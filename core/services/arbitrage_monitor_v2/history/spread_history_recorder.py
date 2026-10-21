"""
历史数据记录器

职责：
- 时间窗口采样
- 异步批量写入CSV文件
- 完全异步化，不阻塞核心流程

性能保证：
- 内存写入：< 0.001ms
- 采样操作：< 0.1ms（每1分钟一次）
- 队列操作：< 0.001ms
- 总体影响：< 0.01ms
"""

import asyncio
import time
import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict

# 🔥 使用统一日志系统配置（参考网格系统）
from core.adapters.exchanges.utils.setup_logging import LoggingConfig

# 🔥 配置日志记录器（写入文件）
logger = LoggingConfig.setup_logger(
    name=__name__,
    log_file='spread_history.log',
    console_formatter=None,  # 不输出到控制台，避免干扰UI
    file_formatter='detailed',
    level=logging.INFO
)
logger.propagate = False

try:
    import aiofiles
except ImportError:
    aiofiles = None
    logger.warning("⚠️  aiofiles未安装，历史记录功能将无法使用。请运行: pip install aiofiles")

try:
    import aiosqlite
except ImportError:
    aiosqlite = None
    logger.warning("⚠️  aiosqlite未安装，SQLite功能将无法使用。请运行: pip install aiosqlite")


class SpreadHistoryRecorder:
    """历史记录器（完全异步，不阻塞核心流程）
    
    采用时间窗口采样策略：每1分钟采样一次数据点
    """
    
    def __init__(
        self, 
        data_dir: str = "data/spread_history",
        sample_interval_seconds: int = 60,
        sample_strategy: str = "max",
        batch_size: int = 10,
        batch_timeout: float = 60.0,
        queue_maxsize: int = 500,
        compress_after_days: int = 10,
        archive_after_days: int = 30,
        cleanup_interval_hours: int = 24,
        db_retention_hours: int = 48
    ):
        """
        初始化历史记录器
        
        Args:
            data_dir: 数据存储目录
            sample_interval_seconds: 采样间隔（秒），默认1分钟
            sample_strategy: 采样策略：max(最大值), mean(平均值), latest(最新值)
            batch_size: 批量写入大小
            batch_timeout: 批量写入超时（秒）
            queue_maxsize: 写入队列最大大小
            compress_after_days: 压缩天数（10天后压缩）
            archive_after_days: 归档天数（30天后归档）
            cleanup_interval_hours: 清理任务执行间隔（小时）
            db_retention_hours: 数据库保留时长（小时），默认48小时
        """
        if aiofiles is None:
            raise ImportError("aiofiles未安装，无法使用历史记录功能")
        
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        (self.data_dir / "raw").mkdir(parents=True, exist_ok=True)
        (self.data_dir / "archive").mkdir(parents=True, exist_ok=True)
        
        # SQLite数据库路径
        self.db_path = self.data_dir / "spread_history.db"
        self.sqlite_enabled = aiosqlite is not None
        
        # 压缩和归档配置
        self.compress_after_days = compress_after_days
        self.archive_after_days = archive_after_days
        self.cleanup_interval_hours = cleanup_interval_hours
        
        # 🔥 数据库清理配置（保持数据库性能）
        self.db_retention_hours = db_retention_hours
        
        # 时间窗口采样配置
        self.sample_interval_seconds = sample_interval_seconds
        self.sample_strategy = sample_strategy
        
        # 时间窗口数据缓存（用于采样，限制大小）
        self.window_data: Dict[str, List[dict]] = defaultdict(list)
        self.window_data_maxsize = 100  # 每个代币最多保留100条
        self.last_sample_time = time.time()
        
        # 🔥 每个价差方向的最后记录时间（用于去重，1分钟内只记录一次）
        # 🔥 修改：同一个代币可能有2个方向的价差（ex1买->ex2卖 和 ex2买->ex1卖），需要分别去重
        # 键格式：f"{symbol}_{exchange_buy}_{exchange_sell}"
        self._last_record_time: Dict[str, float] = {}
        self._record_interval_seconds = 60  # 每个价差方向1分钟内只记录一次
        
        # 写入队列（异步）
        self.write_queue = asyncio.Queue(maxsize=queue_maxsize)
        
        # 当前日期（用于文件轮转）
        self.current_date = datetime.now().date()
        self.current_file: Optional[Path] = None
        
        # 批量写入配置
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        
        # 写入任务
        self.write_task: Optional[asyncio.Task] = None
        self.sample_task: Optional[asyncio.Task] = None
        self.cleanup_task: Optional[asyncio.Task] = None
        
        # 运行状态
        self.running = False
        
        # 统计信息
        self.stats = {
            'records_received': 0,
            'samples_taken': 0,
            'batches_written': 0,
            'sqlite_batches_written': 0,
            'queue_drops': 0,
            'files_compressed': 0,
            'files_archived': 0,
        }
    
    async def start(self):
        """启动异步写入任务和采样任务"""
        if self.running:
            return
        
        # 初始化SQLite数据库（如果启用）
        if self.sqlite_enabled:
            await self._init_sqlite_database()
        
        self.running = True
        self.write_task = asyncio.create_task(self._async_write_loop())
        self.sample_task = asyncio.create_task(self._time_window_sampler())
        self.cleanup_task = asyncio.create_task(self._cleanup_loop())
        
        sqlite_status = "（SQLite已启用）" if self.sqlite_enabled else "（SQLite未启用）"
        logger.info(f"✅ [历史记录] 历史记录器已启动 {sqlite_status}")
        logger.info(f"📁 [历史记录] 数据目录: {self.data_dir}")
        logger.info(f"⏱️  [历史记录] 采样间隔: {self.sample_interval_seconds}秒，采样策略: {self.sample_strategy}")
        logger.info(f"💾 [历史记录] 批量写入配置: batch_size={self.batch_size}, batch_timeout={self.batch_timeout}秒")
        logger.info(f"🗑️  [历史记录] 数据库保留时长: {self.db_retention_hours}小时（超过此时长的数据将被自动清理）")
        logger.info(f"📦 [历史记录] 清理任务间隔: {self.cleanup_interval_hours}小时")
        # 🔥 诊断：确认日志配置
        logger.info(f"🔍 [历史记录] 日志配置检查: handlers={[type(h).__name__ for h in logger.handlers]}, level={logger.level}")
    
    async def stop(self):
        """停止历史记录器"""
        self.running = False
        
        if self.sample_task:
            self.sample_task.cancel()
            try:
                await self.sample_task
            except asyncio.CancelledError:
                pass
        
        if self.write_task:
            self.write_task.cancel()
            try:
                await self.write_task
            except asyncio.CancelledError:
                pass
        
        if self.cleanup_task:
            self.cleanup_task.cancel()
            try:
                await self.cleanup_task
            except asyncio.CancelledError:
                pass
        
        # 写入剩余数据
        await self._flush_remaining_data()
        
        logger.info("🛑 [历史记录] 历史记录器已停止")
    
    async def record_spread(self, data: dict):
        """记录价差（非阻塞，只写入时间窗口缓存）
        
        性能保证：< 0.001ms，不阻塞
        
        🔥 去重机制：每个代币在1分钟内只记录一次
        
        Args:
            data: 套利机会数据，包含：
                - symbol: 代币符号
                - exchange_buy: 买入交易所
                - exchange_sell: 卖出交易所
                - price_buy: 买入价格
                - price_sell: 卖出价格
                - spread_pct: 价差百分比（主要数据）
                - funding_rate_buy: 买入交易所资金费率（主要数据）
                - funding_rate_sell: 卖出交易所资金费率（主要数据）
                - funding_rate_diff: 资金费率差（主要数据，8小时费率差）
                - funding_rate_diff_annual: 年化资金费率差（可选）
                - size_buy: 买入数量（可选）
                - size_sell: 卖出数量（可选）
        """
        if not self.running:
            logger.warning(f"⚠️  [历史记录] 历史记录器未运行，跳过记录: {data.get('symbol', 'unknown')}")
            return
        
        symbol = data.get('symbol')
        exchange_buy = data.get('exchange_buy')
        exchange_sell = data.get('exchange_sell')
        if not symbol or not exchange_buy or not exchange_sell:
            return
        
        # 🔥 去重检查：每个价差方向在1分钟内只记录一次
        # 🔥 修改：同一个代币可能有2个方向的价差，需要分别去重
        # 键格式：f"{symbol}_{exchange_buy}_{exchange_sell}"
        spread_key = f"{symbol}_{exchange_buy}_{exchange_sell}"
        current_time = time.time()
        last_record_time = self._last_record_time.get(spread_key, 0)
        if current_time - last_record_time < self._record_interval_seconds:
            # 在1分钟内已记录过，跳过（静默跳过，不记录日志）
            return
        
        # 更新最后记录时间
        self._last_record_time[spread_key] = current_time
        
        # 添加到时间窗口缓存（不阻塞）
        # 🔥 使用symbol作为key，因为同一个symbol的2个方向价差需要一起采样
        self.window_data[symbol].append({
            **data,
            'timestamp': datetime.now().isoformat()
        })
        
        # 限制缓存大小（避免内存积累）
        if len(self.window_data[symbol]) > self.window_data_maxsize:
            self.window_data[symbol] = self.window_data[symbol][-self.window_data_maxsize:]
        
        self.stats['records_received'] += 1
        
        # 🔥 调试日志：每100条记录输出一次统计
        if self.stats['records_received'] % 100 == 0:
            total_cached = sum(len(v) for v in self.window_data.values())
            logger.info(f"📊 [历史记录] 已接收 {self.stats['records_received']} 条记录，内存缓存 {total_cached} 条，等待采样...")
    
    async def _time_window_sampler(self):
        """时间窗口采样器（每1分钟采样一次，非阻塞）"""
        logger.info(f"🕐 [历史记录] 采样器已启动，采样间隔: {self.sample_interval_seconds}秒")
        while self.running:
            try:
                await asyncio.sleep(self.sample_interval_seconds)
                
                current_time = time.time()
                if current_time - self.last_sample_time >= self.sample_interval_seconds:
                    # 采样当前时间窗口的数据（< 0.1ms）
                    total_cached = sum(len(v) for v in self.window_data.values())
                    sampled_data = self._sample_window_data()
                    
                    if sampled_data:
                        self.stats['samples_taken'] += len(sampled_data)
                        logger.info(f"📊 [历史记录] 采样完成: 从 {total_cached} 条缓存中采样 {len(sampled_data)} 条数据")
                        
                        # 非阻塞放入写入队列
                        try:
                            self.write_queue.put_nowait(sampled_data)
                            logger.info(f"✅ [历史记录] 已放入写入队列，队列大小: {self.write_queue.qsize()}")
                        except asyncio.QueueFull:
                            # 队列满时丢弃（不影响核心流程）
                            self.stats['queue_drops'] += 1
                            logger.warning(f"⚠️  [历史记录] 写入队列已满，丢弃数据")
                    else:
                        if total_cached > 0:
                            logger.warning(f"⚠️  [历史记录] 采样器运行，但采样结果为空（缓存中有 {total_cached} 条数据）")
                        else:
                            logger.debug(f"💤 [历史记录] 采样器运行，但缓存为空（未检测到套利机会）")
                    
                    # 清空时间窗口缓存
                    self.window_data.clear()
                    self.last_sample_time = current_time
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                # 错误隔离：不影响核心流程
                logger.error(f"⚠️  [历史记录] 采样错误（已隔离）: {e}", exc_info=True)
    
    def _sample_window_data(self) -> List[dict]:
        """采样时间窗口数据（< 0.1ms）
        
        🔥 修改：按 (symbol, exchange_buy, exchange_sell) 分组采样
        确保同一个symbol的2个方向价差都能被正确采样和保存
        """
        sampled = []
        
        # 🔥 按 (symbol, exchange_buy, exchange_sell) 分组
        grouped_by_direction = defaultdict(list)
        
        for symbol, data_list in self.window_data.items():
            if not data_list:
                continue
            
            # 按方向分组
            for data in data_list:
                exchange_buy = data.get('exchange_buy')
                exchange_sell = data.get('exchange_sell')
                if exchange_buy and exchange_sell:
                    direction_key = (symbol, exchange_buy, exchange_sell)
                    grouped_by_direction[direction_key].append(data)
        
        # 对每个方向独立采样
        for (symbol, exchange_buy, exchange_sell), data_list in grouped_by_direction.items():
            if not data_list:
                continue
            
            if self.sample_strategy == "max":
                # 选择价差最大的数据点（绝对值最大）
                best = max(data_list, key=lambda x: abs(x.get('spread_pct', 0)))
                # 🔥 确保所有数值字段都转换为Python原生类型（避免Decimal类型）
                best = self._convert_data_types(best)
                sampled.append(best)
            elif self.sample_strategy == "mean":
                # 计算平均值
                # 🔥 计算资金费率平均值（如果存在）
                funding_rate_buy_list = [d.get('funding_rate_buy') for d in data_list if d.get('funding_rate_buy') is not None]
                funding_rate_sell_list = [d.get('funding_rate_sell') for d in data_list if d.get('funding_rate_sell') is not None]
                funding_rate_diff_list = [d.get('funding_rate_diff') for d in data_list if d.get('funding_rate_diff') is not None]
                
                avg_data = {
                    'symbol': symbol,
                    'timestamp': datetime.now().isoformat(),
                    'exchange_buy': exchange_buy,
                    'exchange_sell': exchange_sell,
                    'price_buy': sum(d.get('price_buy', 0) for d in data_list) / len(data_list),
                    'price_sell': sum(d.get('price_sell', 0) for d in data_list) / len(data_list),
                    'spread_pct': sum(d.get('spread_pct', 0) for d in data_list) / len(data_list),  # 🔥 主要数据：价差百分比
                    'funding_rate_buy': sum(funding_rate_buy_list) / len(funding_rate_buy_list) if funding_rate_buy_list else None,  # 🔥 主要数据：买入交易所资金费率
                    'funding_rate_sell': sum(funding_rate_sell_list) / len(funding_rate_sell_list) if funding_rate_sell_list else None,  # 🔥 主要数据：卖出交易所资金费率
                    'funding_rate_diff': sum(funding_rate_diff_list) / len(funding_rate_diff_list) if funding_rate_diff_list else None,  # 🔥 主要数据：资金费率差（8小时费率差）
                    'funding_rate_diff_annual': sum(d.get('funding_rate_diff_annual', 0) for d in data_list) / len(data_list) if data_list[0].get('funding_rate_diff_annual') is not None else None,
                    'size_buy': sum(d.get('size_buy', 0) for d in data_list) / len(data_list),
                    'size_sell': sum(d.get('size_sell', 0) for d in data_list) / len(data_list),
                }
                # 🔥 确保所有数值字段都转换为Python原生类型（避免Decimal类型）
                avg_data = self._convert_data_types(avg_data)
                sampled.append(avg_data)
            elif self.sample_strategy == "latest":
                # 选择最新的数据点
                latest = data_list[-1]
                # 🔥 确保所有数值字段都转换为Python原生类型（避免Decimal类型）
                latest = self._convert_data_types(latest)
                sampled.append(latest)
        
        return sampled
    
    def _convert_data_types(self, data: dict) -> dict:
        """转换数据中的Decimal类型为float（确保SQLite兼容）"""
        from decimal import Decimal
        converted = {}
        for key, value in data.items():
            if value is None:
                converted[key] = None
            elif isinstance(value, Decimal):
                converted[key] = float(value)
            elif isinstance(value, (int, float)):
                converted[key] = float(value)
            else:
                converted[key] = value
        return converted
    
    async def _init_sqlite_database(self):
        """初始化SQLite数据库和表结构"""
        if not self.sqlite_enabled:
            return
        
        try:
            async with aiosqlite.connect(self.db_path) as db:
                # 创建表
                await db.execute("""
                    CREATE TABLE IF NOT EXISTS spread_history_sampled (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp DATETIME NOT NULL,
                        symbol TEXT NOT NULL,
                        exchange_buy TEXT NOT NULL,
                        exchange_sell TEXT NOT NULL,
                        price_buy REAL NOT NULL,
                        price_sell REAL NOT NULL,
                        spread_pct REAL NOT NULL,
                        funding_rate_buy REAL,
                        funding_rate_sell REAL,
                        funding_rate_diff REAL,
                        funding_rate_diff_annual REAL,
                        size_buy REAL NOT NULL,
                        size_sell REAL NOT NULL,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # 🔥 检查并添加新字段（如果表已存在但字段不存在）
                try:
                    await db.execute("ALTER TABLE spread_history_sampled ADD COLUMN funding_rate_buy REAL")
                except Exception:
                    pass  # 字段已存在，忽略错误
                
                try:
                    await db.execute("ALTER TABLE spread_history_sampled ADD COLUMN funding_rate_sell REAL")
                except Exception:
                    pass  # 字段已存在，忽略错误
                
                try:
                    await db.execute("ALTER TABLE spread_history_sampled ADD COLUMN funding_rate_diff REAL")
                except Exception:
                    pass  # 字段已存在，忽略错误
                
                # 创建索引（如果不存在）
                await db.execute("""
                    CREATE INDEX IF NOT EXISTS idx_sampled_timestamp 
                    ON spread_history_sampled(timestamp)
                """)
                
                await db.execute("""
                    CREATE INDEX IF NOT EXISTS idx_sampled_symbol 
                    ON spread_history_sampled(symbol)
                """)
                
                await db.execute("""
                    CREATE INDEX IF NOT EXISTS idx_sampled_symbol_timestamp 
                    ON spread_history_sampled(symbol, timestamp)
                """)
                
                await db.execute("""
                    CREATE INDEX IF NOT EXISTS idx_sampled_spread_pct 
                    ON spread_history_sampled(spread_pct)
                """)
                
                await db.commit()
                
        except Exception as e:
            logger.error(f"⚠️  [历史记录] SQLite数据库初始化失败（已隔离）: {e}", exc_info=True)
            self.sqlite_enabled = False
    
    async def _async_write_loop(self):
        """异步写入循环（独立任务，不阻塞主流程）"""
        batch = []
        last_write_time = time.time()
        
        while self.running:
            try:
                # 等待数据或超时
                timeout = self.batch_timeout - (time.time() - last_write_time)
                if timeout <= 0:
                    timeout = 0.1
                
                data = await asyncio.wait_for(
                    self.write_queue.get(),
                    timeout=timeout
                )
                
                # data可能是单个dict或list
                if isinstance(data, list):
                    batch.extend(data)
                else:
                    batch.append(data)
                
                # 批量写入（积累batch_size条或batch_timeout秒）
                if len(batch) >= self.batch_size:
                    await self._write_batch(batch)
                    batch = []
                    last_write_time = time.time()
                    
            except asyncio.TimeoutError:
                # 超时，写入当前批次
                if batch:
                    await self._write_batch(batch)
                    batch = []
                    last_write_time = time.time()
            except asyncio.CancelledError:
                break
            except Exception as e:
                # 错误隔离：不影响核心流程
                logger.error(f"⚠️  [历史记录] 写入错误（已隔离）: {e}", exc_info=True)
    
    async def _write_batch(self, batch: List[dict]):
        """批量写入CSV和SQLite（异步IO，不阻塞）"""
        if not batch:
            return
        
        logger.info(f"💾 [历史记录] 开始批量写入 {len(batch)} 条数据到CSV和SQLite...")
        
        # 并行写入CSV和SQLite
        tasks = [self._write_batch_to_csv(batch)]
        if self.sqlite_enabled:
            tasks.append(self._write_batch_to_sqlite(batch))
        
        await asyncio.gather(*tasks, return_exceptions=True)
        self.stats['batches_written'] += 1
    
    async def _write_batch_to_csv(self, batch: List[dict]):
        """批量写入CSV文件（异步IO）"""
        if not batch:
            return
        
        try:
            # 检查日期变化（文件轮转）
            today = datetime.now().date()
            if today != self.current_date:
                self.current_date = today
                self.current_file = None
            
            # 获取今天的文件路径
            if self.current_file is None:
                csv_file = self.data_dir / "raw" / f"{today.strftime('%Y-%m-%d')}.csv"
                self.current_file = csv_file
                
                # 如果是新文件，写入表头
                if not csv_file.exists():
                    header = "timestamp,symbol,exchange_buy,exchange_sell,price_buy,price_sell,spread_pct,funding_rate_buy,funding_rate_sell,funding_rate_diff,funding_rate_diff_annual,size_buy,size_sell\n"
                    async with aiofiles.open(csv_file, 'a') as f:
                        await f.write(header)
            
            # 批量写入数据
            lines = []
            for data in batch:
                line = (
                    f"{data.get('timestamp', '')},"
                    f"{data.get('symbol', '')},"
                    f"{data.get('exchange_buy', '')},"
                    f"{data.get('exchange_sell', '')},"
                    f"{data.get('price_buy', 0):.8f},"
                    f"{data.get('price_sell', 0):.8f},"
                    f"{data.get('spread_pct', 0):.6f},"
                    f"{data.get('funding_rate_buy', 0) if data.get('funding_rate_buy') is not None else 0:.8f},"
                    f"{data.get('funding_rate_sell', 0) if data.get('funding_rate_sell') is not None else 0:.8f},"
                    f"{data.get('funding_rate_diff', 0) if data.get('funding_rate_diff') is not None else 0:.8f},"
                    f"{data.get('funding_rate_diff_annual', 0) if data.get('funding_rate_diff_annual') is not None else 0:.2f},"
                    f"{data.get('size_buy', 0):.8f},"
                    f"{data.get('size_sell', 0):.8f}\n"
                )
                lines.append(line)
            
            # 异步写入文件
            async with aiofiles.open(self.current_file, 'a') as f:
                await f.writelines(lines)
            
        except Exception as e:
            # 错误隔离：不影响核心流程
            logger.error(f"⚠️  [历史记录] CSV写入错误（已隔离）: {e}", exc_info=True)
    
    async def _write_batch_to_sqlite(self, batch: List[dict]):
        """批量写入SQLite数据库（异步）"""
        if not batch or not self.sqlite_enabled:
            return
        
        logger.info(f"💾 [历史记录] 写入SQLite: {len(batch)} 条数据")
        
        try:
            # 🔥 转换 decimal.Decimal 为 float（SQLite不支持Decimal类型）
            def convert_value(v):
                """转换值类型，确保SQLite兼容"""
                if v is None:
                    return None
                from decimal import Decimal
                if isinstance(v, Decimal):
                    return float(v)
                # 确保所有数值类型都转换为Python原生类型
                if isinstance(v, (int, float)):
                    return float(v)
                return v
            
            async with aiosqlite.connect(self.db_path) as db:
                # 使用executemany批量插入，提高性能
                await db.executemany("""
                    INSERT INTO spread_history_sampled 
                    (timestamp, symbol, exchange_buy, exchange_sell, 
                     price_buy, price_sell, spread_pct, 
                     funding_rate_buy, funding_rate_sell, funding_rate_diff, funding_rate_diff_annual, 
                     size_buy, size_sell)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    (
                        data.get('timestamp', ''),
                        data.get('symbol', ''),
                        data.get('exchange_buy', ''),
                        data.get('exchange_sell', ''),
                        convert_value(data.get('price_buy', 0)),
                        convert_value(data.get('price_sell', 0)),
                        convert_value(data.get('spread_pct', 0)),  # 🔥 主要数据：价差百分比
                        convert_value(data.get('funding_rate_buy')) if data.get('funding_rate_buy') is not None else None,  # 🔥 主要数据：买入交易所资金费率
                        convert_value(data.get('funding_rate_sell')) if data.get('funding_rate_sell') is not None else None,  # 🔥 主要数据：卖出交易所资金费率
                        convert_value(data.get('funding_rate_diff')) if data.get('funding_rate_diff') is not None else None,  # 🔥 主要数据：资金费率差（8小时费率差）
                        convert_value(data.get('funding_rate_diff_annual')) if data.get('funding_rate_diff_annual') is not None else None,
                        convert_value(data.get('size_buy', 0)),
                        convert_value(data.get('size_sell', 0)),
                    )
                    for data in batch
                ])
                await db.commit()
                self.stats['sqlite_batches_written'] += 1
                
        except Exception as e:
            # 错误隔离：不影响核心流程
            logger.error(f"⚠️  [历史记录] SQLite写入错误（已隔离）: {e}", exc_info=True)
    
    async def _flush_remaining_data(self):
        """刷新剩余数据"""
        # 采样剩余数据
        sampled_data = self._sample_window_data()
        if sampled_data:
            try:
                self.write_queue.put_nowait(sampled_data)
            except asyncio.QueueFull:
                pass
        
        # 写入队列中剩余的数据
        batch = []
        while not self.write_queue.empty():
            try:
                data = self.write_queue.get_nowait()
                if isinstance(data, list):
                    batch.extend(data)
                else:
                    batch.append(data)
            except asyncio.QueueEmpty:
                break
        
        if batch:
            await self._write_batch(batch)
    
    async def _cleanup_loop(self):
        """清理循环（定期压缩和归档旧文件 + 清理数据库）"""
        # 启动时等待一段时间再执行第一次清理
        await asyncio.sleep(3600)  # 等待1小时
        
        while self.running:
            try:
                # 1. 归档旧文件（压缩和归档）
                await self._archive_old_files()
                
                # 2. 🔥 清理数据库旧数据（保持性能）
                await self._cleanup_database()
                
                # 等待清理间隔
                await asyncio.sleep(self.cleanup_interval_hours * 3600)
            except asyncio.CancelledError:
                break
            except Exception as e:
                # 错误隔离：不影响核心流程
                logger.error(f"⚠️  [历史记录] 清理任务错误（已隔离）: {e}", exc_info=True)
                await asyncio.sleep(3600)  # 出错后等待1小时再重试
    
    async def _archive_old_files(self):
        """归档旧文件（压缩和归档）"""
        import gzip
        import shutil
        
        raw_dir = self.data_dir / "raw"
        archive_dir = self.data_dir / "archive"
        
        if not raw_dir.exists():
            return
        
        compress_cutoff = datetime.now().date() - timedelta(days=self.compress_after_days)
        archive_cutoff = datetime.now().date() - timedelta(days=self.archive_after_days)
        
        compressed_count = 0
        archived_count = 0
        
        for csv_file in raw_dir.glob("*.csv"):
            try:
                # 提取日期（文件名格式：YYYY-MM-DD.csv）
                file_date_str = csv_file.stem
                file_date = datetime.strptime(file_date_str, "%Y-%m-%d").date()
                
                if file_date < archive_cutoff:
                    # 归档：移动到归档目录并压缩
                    gz_file = archive_dir / f"{file_date_str}.csv.gz"
                    
                    # 异步压缩（使用线程池避免阻塞）
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(
                        None,
                        self._compress_file,
                        csv_file,
                        gz_file
                    )
                    
                    csv_file.unlink()
                    archived_count += 1
                    
                elif file_date < compress_cutoff:
                    # 压缩：原地压缩
                    gz_file = csv_file.with_suffix('.csv.gz')
                    
                    # 异步压缩
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(
                        None,
                        self._compress_file,
                        csv_file,
                        gz_file
                    )
                    
                    csv_file.unlink()
                    compressed_count += 1
                    
            except ValueError:
                # 文件名格式不正确，跳过
                continue
            except Exception as e:
                # 错误隔离：不影响核心流程
                logger.error(f"⚠️  [历史记录] 文件处理错误（已隔离）: {csv_file.name}, {e}", exc_info=True)
                continue
        
        if compressed_count > 0 or archived_count > 0:
            self.stats['files_compressed'] += compressed_count
            self.stats['files_archived'] += archived_count
            logger.info(f"📦 [历史记录] 清理完成: 压缩 {compressed_count} 个文件，归档 {archived_count} 个文件")
    
    async def _cleanup_database(self):
        """清理数据库旧数据（删除超过保留期的数据）
        
        为了保持数据库性能，定期删除超过保留期（默认48小时）的历史数据。
        """
        if not self.sqlite_enabled or not self.db_path.exists():
            return
        
        try:
            cutoff_time = datetime.now() - timedelta(hours=self.db_retention_hours)
            
            async with aiosqlite.connect(self.db_path) as db:
                # 1. 查询即将删除的数据量
                async with db.execute("""
                    SELECT COUNT(*) FROM spread_history_sampled
                    WHERE timestamp < ?
                """, (cutoff_time.isoformat(),)) as cursor:
                    row = await cursor.fetchone()
                    rows_to_delete = row[0] if row else 0
                
                if rows_to_delete == 0:
                    logger.debug(
                        f"🗑️  [数据库清理] 无需清理（所有数据均在保留期内：{self.db_retention_hours}小时）"
                    )
                    return
                
                # 2. 删除旧数据
                async with db.execute("""
                    DELETE FROM spread_history_sampled
                    WHERE timestamp < ?
                """, (cutoff_time.isoformat(),)) as cursor:
                    await db.commit()
                
                # 3. 优化数据库（释放空间）
                await db.execute("VACUUM")
                await db.commit()
                
                # 4. 查询清理后的数据量
                async with db.execute("""
                    SELECT COUNT(*) FROM spread_history_sampled
                """) as cursor:
                    row = await cursor.fetchone()
                    remaining_rows = row[0] if row else 0
                
                logger.info(
                    f"🗑️  [数据库清理] 清理完成\n"
                    f"   删除记录: {rows_to_delete:,} 条\n"
                    f"   保留记录: {remaining_rows:,} 条\n"
                    f"   保留时长: {self.db_retention_hours}小时\n"
                    f"   截止时间: {cutoff_time.strftime('%Y-%m-%d %H:%M:%S')}"
                )
                
                # 更新统计信息
                self.stats['db_cleanup_count'] = self.stats.get('db_cleanup_count', 0) + 1
                self.stats['db_rows_deleted'] = self.stats.get('db_rows_deleted', 0) + rows_to_delete
                
        except Exception as e:
            # 错误隔离：不影响核心流程
            logger.error(f"⚠️  [数据库清理] 清理失败（已隔离）: {e}", exc_info=True)
    
    @staticmethod
    def _compress_file(source_file: Path, target_file: Path):
        """压缩文件（同步操作，在线程池中执行）"""
        import gzip
        import shutil
        
        with open(source_file, 'rb') as f_in:
            with gzip.open(target_file, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
    
    def get_stats(self) -> dict:
        """获取统计信息"""
        return {
            **self.stats,
            'queue_size': self.write_queue.qsize(),
            'window_data_count': sum(len(v) for v in self.window_data.values()),
        }

