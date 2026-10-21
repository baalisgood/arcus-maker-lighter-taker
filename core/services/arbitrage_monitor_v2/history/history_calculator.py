"""
历史数据计算器

职责：
- 从数据库提取历史数据
- 计算所有交易所对的天然价差（Natural Spread）
- 将计算结果存储到内存
- 跟随历史数据存储频率（存储→计算→提供）

天然价差（Natural Spread）：
- 不同交易所间天然存在的基准价差
- 通过历史数据的中位数计算（保留正负号）
- 正数：历史上此方向有利可图
- 负数：历史上此方向无法套利

性能要求：
- 数据库查询：异步，不阻塞主流程
- 计算结果存储：内存存储，O(1)查询
- 动态时间窗口：3分钟 - 24小时
- 更新频率：跟随存储频率（每次存储后立即计算）
"""

import asyncio
import logging
import statistics
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from pathlib import Path

try:
    import aiosqlite
except ImportError:
    aiosqlite = None

# 🔥 使用统一日志系统
from core.adapters.exchanges.utils.setup_logging import LoggingConfig

logger = LoggingConfig.setup_logger(
    name=__name__,
    log_file='history_calculator.log',
    console_formatter=None,
    file_formatter='detailed',
    level=logging.INFO
)
logger.propagate = False


@dataclass
class ExchangePairResult:
    """交易所对的计算结果（存储在内存）"""
    exchange_pair: Tuple[str, str]  # (exchange_buy, exchange_sell)
    symbol: str
    
    # 计算结果
    natural_spread: Optional[float] = None  # 天然价差（百分比，可正可负）
    natural_funding_rate_diff: Optional[float] = None  # 🔥 天然资金费率差（百分比，永远≥0）
    data_points_count: int = 0  # 数据点数量
    window_hours: float = 0.0  # 实际使用的时间窗口（小时）
    funding_rate_stable: bool = False  # 历史资金费率差稳定性（稳定/不稳定）
    spread_stable: bool = False  # 历史价差稳定性（稳定/不稳定）
    current_funding_rate_diff: float = 0.0  # 🔥 已弃用：请使用 natural_funding_rate_diff
    
    last_update: datetime = field(default_factory=datetime.now)


class HistoryDataCalculator:
    """历史数据计算器"""
    
    def __init__(
        self,
        db_path: str = "data/spread_history/spread_history.db",
        update_interval_minutes: int = 1,
        max_history_hours: int = 24,
        min_data_points: int = 10,
        min_runtime_minutes: float = 3.0,
        # 🔥 新增：稳定性判断参数（从配置读取）
        funding_rate_stability_threshold: float = 0.5,
        funding_rate_min_threshold: float = 0.01,
        funding_rate_duration_ratio: float = 0.8,
        funding_rate_extreme_ratio: float = 0.1,
        funding_rate_extreme_multiplier: float = 1.5,
        spread_stability_threshold: float = 0.5,
        spread_extreme_ratio: float = 0.15,
        spread_extreme_multiplier: float = 2.0,
        enable_shared_memory: bool = True  # 🔥 新增：是否启用共享内存文件
    ):
        """
        初始化历史数据计算器
        
        Args:
            db_path: SQLite数据库路径
            update_interval_minutes: 更新间隔（分钟）
            max_history_hours: 最大历史数据时间范围（小时）
            min_data_points: 最少数据点要求
            min_runtime_minutes: 最少运行时间要求（分钟）
            
            # 稳定性判断参数（可配置）
            funding_rate_stability_threshold: 资金费率波动系数阈值
            funding_rate_min_threshold: 资金费率差最小阈值
            funding_rate_duration_ratio: 持续时间占比阈值
            funding_rate_extreme_ratio: 极端波动次数占比阈值
            funding_rate_extreme_multiplier: 极端波动倍数
            spread_stability_threshold: 价差波动系数阈值
            spread_extreme_ratio: 价差极端波动次数占比阈值
            spread_extreme_multiplier: 价差极端波动倍数
        """
        if aiosqlite is None:
            raise ImportError("aiosqlite未安装，无法使用历史数据计算功能。请运行: pip install aiosqlite")
        
        self.db_path = Path(db_path)
        self.update_interval_minutes = update_interval_minutes
        self.max_history_hours = max_history_hours
        self.min_data_points = min_data_points
        self.min_runtime_minutes = min_runtime_minutes
        
        # 🔥 稳定性判断参数（从配置读取）
        self.funding_rate_stability_threshold = funding_rate_stability_threshold
        self.funding_rate_min_threshold = funding_rate_min_threshold
        self.funding_rate_duration_ratio = funding_rate_duration_ratio
        self.funding_rate_extreme_ratio = funding_rate_extreme_ratio
        self.funding_rate_extreme_multiplier = funding_rate_extreme_multiplier
        self.spread_stability_threshold = spread_stability_threshold
        self.spread_extreme_ratio = spread_extreme_ratio
        self.spread_extreme_multiplier = spread_extreme_multiplier
        
        # 🔥 记录实际进程启动时间（用于日志）
        self.process_start_time = datetime.now()
        
        # 🔥 系统参考时间：默认向前预置 max_history_hours，确保重启后立即可用历史数据
        self.system_start_time = self.process_start_time - timedelta(hours=self.max_history_hours)
        
        # 内存存储：计算结果
        # 键格式：f"{symbol}_{exchange_buy}_{exchange_sell}"（带方向）
        self.results: Dict[str, ExchangePairResult] = {}
        
        # 更新任务
        self.update_task: Optional[asyncio.Task] = None
        self.running = False
        
        # 🔥 共享内存文件配置
        self.enable_shared_memory = enable_shared_memory
        self.shared_memory_path = Path("data/spread_history/calculator_results.json")
        
        logger.info(f"✅ [天然价差] 天然价差计算器初始化完成")
        logger.info(f"📁 [天然价差] 数据库路径: {self.db_path}")
        logger.info(f"⏱️  [天然价差] 更新间隔: {self.update_interval_minutes}分钟（跟随存储频率）")
        logger.info(f"📊 [天然价差] 动态时间窗口: {self.min_runtime_minutes}分钟 - {self.max_history_hours}小时")
        logger.info(f"🔢 [天然价差] 最少数据点: {self.min_data_points}")
        logger.info(
            f"🕐 [天然价差] 进程启动时间: {self.process_start_time.strftime('%Y-%m-%d %H:%M:%S')} "
            f"(历史窗口基准: 最近 {self.max_history_hours} 小时)"
        )
    
    def _get_dynamic_window_hours(self) -> float:
        """获取动态时间窗口（小时）
        
        根据系统实际运行时间自动调整窗口大小：
        - 系统刚启动：使用实际运行时间
        - 系统运行超过24小时：使用24小时
        
        Returns:
            实际使用的时间窗口（小时）
        """
        # 计算实际运行时间（小时）
        runtime_hours = (datetime.now() - self.system_start_time).total_seconds() / 3600
        
        # 动态调整窗口（最小3分钟，最大24小时）
        min_hours = self.min_runtime_minutes / 60
        actual_window_hours = min(self.max_history_hours, max(min_hours, runtime_hours))
        
        return actual_window_hours
    
    async def start(self):
        """启动历史数据计算器"""
        if self.running:
            return
        
        self.running = True
        self.update_task = asyncio.create_task(self._update_loop())
        logger.info("✅ [天然价差] 天然价差计算器已启动")
    
    async def stop(self):
        """停止历史数据计算器"""
        self.running = False
        
        if self.update_task:
            self.update_task.cancel()
            try:
                await self.update_task
            except asyncio.CancelledError:
                pass
        
        logger.info("🛑 [天然价差] 天然价差计算器已停止")
    
    async def _write_to_shared_memory(self):
        """🔥 将计算结果写入共享内存文件（JSON格式）"""
        try:
            import json
            
            # 确保目录存在
            self.shared_memory_path.parent.mkdir(parents=True, exist_ok=True)
            
            # 序列化结果
            serialized_results = {
                "last_update": datetime.now().isoformat(),
                "system_start_time": self.system_start_time.isoformat(),
                "results_count": len(self.results),
                "results": {}
            }
            
            for key, result in self.results.items():
                exchange_buy, exchange_sell = result.exchange_pair
                serialized_results["results"][key] = {
                    "symbol": result.symbol,
                    "exchange_buy": exchange_buy,
                    "exchange_sell": exchange_sell,
                    "natural_spread": result.natural_spread,
                    "natural_funding_rate_diff": result.natural_funding_rate_diff,
                    "data_points_count": result.data_points_count,
                    "window_hours": result.window_hours,
                    "funding_rate_stable": result.funding_rate_stable,
                    "spread_stable": result.spread_stable,
                    "last_update": result.last_update.isoformat() if result.last_update else None
                }
            
            # 写入文件（使用临时文件 + 原子替换，避免读取时文件不完整）
            temp_path = self.shared_memory_path.with_suffix('.tmp')
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(serialized_results, f, indent=2, ensure_ascii=False)
            
            # 原子替换
            temp_path.replace(self.shared_memory_path)
            
            logger.debug(f"✅ [共享内存] 已写入计算结果: {len(self.results)} 组")
            
        except Exception as e:
            logger.error(f"❌ [共享内存] 写入失败: {e}", exc_info=True)
    
    async def _update_loop(self):
        """更新循环（跟随存储频率）"""
        while self.running:
            try:
                await asyncio.sleep(self.update_interval_minutes * 60)
                
                if not self.running:
                    break
                
                # 🔥 为本轮计算准备容器（先保留上一轮结果，若本轮无数据则复用）
                previous_results = self.results
                new_results: Dict[str, ExchangePairResult] = {}
                previous_count = len(previous_results)

                # 获取动态时间窗口
                window_hours = self._get_dynamic_window_hours()
                runtime_minutes = (datetime.now() - self.system_start_time).total_seconds() / 60
                old_count = previous_count
                
                logger.info(
                    f"\n{'='*60}\n"
                    f"🔄 [天然价差] 开始新一轮计算\n"
                    f"   系统运行时间: {runtime_minutes:.1f}分钟\n"
                    f"   动态时间窗口: {window_hours:.2f}小时\n"
                    f"   清空旧数据: {old_count} 个\n"
                    f"{'='*60}"
                )
                
                # 获取所有交易对和交易所（从数据库查询）
                symbols_exchanges = await self._get_symbols_exchanges()
                
                if not symbols_exchanges:
                    logger.warning("⚠️ [天然价差] 数据库中没有找到历史数据")
                    continue
                
                # 更新所有交易对的计算结果
                total_directions = 0
                for symbol, exchanges in symbols_exchanges.items():
                    if len(exchanges) < 2:
                        continue
                    
                    directions_count = await self._update_calculated_results(symbol, exchanges, new_results)
                    total_directions += directions_count

                if new_results:
                    self.results = new_results
                    success_count = len(new_results)
                    insufficient_count = max(total_directions - success_count, 0)
                else:
                    # 本轮未能获得新结果，尝试沿用上一轮
                    if previous_results:
                        self.results = previous_results
                        success_count = previous_count
                        insufficient_count = max(total_directions - success_count, 0)
                        logger.warning("⚠️ [天然价差] 本轮历史数据不足，沿用上一轮计算结果")
                    else:
                        self.results = {}
                        success_count = 0
                        insufficient_count = total_directions
                        logger.warning("⚠️ [天然价差] 历史数据库暂无可用数据，无法提供天然价差结果")
                
                logger.info(
                    f"\n{'='*60}\n"
                    f"✅ [天然价差] 计算完成并提供新数据\n"
                    f"   交易对数量: {len(symbols_exchanges)}\n"
                    f"   交易所组合方向: {total_directions} 个\n"
                    f"   成功计算: {success_count} 个\n"
                    f"   数据不足: {insufficient_count} 个\n"
                    f"{'='*60}\n"
                )
                
                # 🔥 写入共享内存文件（供监控工具读取）
                if self.enable_shared_memory:
                    await self._write_to_shared_memory()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ [天然价差] 更新循环错误: {e}", exc_info=True)
    
    async def _get_symbols_exchanges(self) -> Dict[str, List[str]]:
        """从数据库获取所有交易对和交易所列表"""
        if not self.db_path.exists():
            return {}
        
        symbols_exchanges: Dict[str, List[str]] = {}
        
        try:
            async with aiosqlite.connect(self.db_path) as db:
                # 查询所有唯一的(symbol, exchange_buy, exchange_sell)组合
                async with db.execute("""
                    SELECT DISTINCT symbol, exchange_buy, exchange_sell
                    FROM spread_history_sampled
                    WHERE timestamp >= datetime('now', '-' || ? || ' hours')
                """, (self.max_history_hours,)) as cursor:
                    async for row in cursor:
                        symbol, exchange_buy, exchange_sell = row
                        if symbol not in symbols_exchanges:
                            symbols_exchanges[symbol] = []
                        if exchange_buy not in symbols_exchanges[symbol]:
                            symbols_exchanges[symbol].append(exchange_buy)
                        if exchange_sell not in symbols_exchanges[symbol]:
                            symbols_exchanges[symbol].append(exchange_sell)
        except Exception as e:
            logger.error(f"❌ [历史计算] 获取交易对和交易所列表失败: {e}", exc_info=True)
        
        return symbols_exchanges
    
    async def _update_calculated_results(
        self,
        symbol: str,
        exchanges: List[str],
        results_container: Dict[str, ExchangePairResult],
    ) -> int:
        """更新指定交易对的所有交易所对的计算结果
        
        Returns:
            计算的方向数量
        """
        # 计算所有交易所对（每个组合2个方向）
        from itertools import combinations
        directions_count = 0
        
        for ex1, ex2 in combinations(exchanges, 2):
            await self._calculate_pair_result(symbol, ex1, ex2, results_container)
            await self._calculate_pair_result(symbol, ex2, ex1, results_container)
            directions_count += 2
        
        return directions_count
    
    async def _calculate_pair_result(
        self,
        symbol: str,
        exchange_buy: str,
        exchange_sell: str,
        results_container: Dict[str, ExchangePairResult],
    ):
        """计算指定交易所对的天然价差
        
        Args:
            symbol: 交易对
            exchange_buy: 买入交易所
            exchange_sell: 卖出交易所
        """
        pair_key = f"{symbol}_{exchange_buy}_{exchange_sell}"
        
        # 获取动态时间窗口
        window_hours = self._get_dynamic_window_hours()
        
        # 从数据库提取历史数据（特定方向）
        raw_data = await self._get_history_data_dynamic(
            symbol, exchange_buy, exchange_sell, window_hours
        )
        
        if not raw_data:
            # 如果没有历史数据，不存储（跳过）
            logger.debug(
                f"⚠️ [天然价差] {symbol} {exchange_buy}→{exchange_sell}: "
                f"数据库中没有找到历史数据"
            )
            return
        
        # 提取价差历史（保留正负号）
        spread_history = [row.get('spread_pct', 0.0) for row in raw_data]
        data_points = len(spread_history)
        
        # 检查数据点数量
        if data_points < self.min_data_points:
            logger.debug(
                f"⚠️ [天然价差] {symbol} {exchange_buy}→{exchange_sell}: "
                f"数据点不足（{data_points} < {self.min_data_points}）"
            )
            return
        
        # 🔥 计算天然价差：中位数（保留正负号）
        natural_spread = statistics.median(spread_history)
        
        # 🔥 计算资金费率差历史
        funding_rate_diff_history = [
            row.get('funding_rate_diff') for row in raw_data
            if row.get('funding_rate_diff') is not None
        ]
        
        # 🔥 计算天然资金费率差：中位数（永远≥0，因为存储的是绝对值差值）
        # 数学规则：数值更大的 - 数值更小的 = 差值
        # 例如：+1% - (-1%) = 2%，+2% - +1% = 1%，-1% - (-2%) = 1%
        natural_funding_rate_diff = None
        if len(funding_rate_diff_history) >= self.min_data_points:
            natural_funding_rate_diff = statistics.median(funding_rate_diff_history)
        
        # 计算稳定性
        funding_rate_stable = self._calculate_funding_rate_stability(
            funding_rate_diff_history
        ) if len(funding_rate_diff_history) >= self.min_data_points else False
        
        spread_stable = self._calculate_spread_stability_v2(
            spread_history
        )
        
        # 🔥 保持向后兼容（已弃用）
        current_funding_rate_diff = (
            funding_rate_diff_history[-1]
            if funding_rate_diff_history else 0.0
        )
        
        # 🔥 存储到内存
        results_container[pair_key] = ExchangePairResult(
            exchange_pair=(exchange_buy, exchange_sell),
            symbol=symbol,
            natural_spread=natural_spread,
            natural_funding_rate_diff=natural_funding_rate_diff,  # 🔥 新增
            data_points_count=data_points,
            window_hours=window_hours,
            funding_rate_stable=funding_rate_stable,
            spread_stable=spread_stable,
            current_funding_rate_diff=current_funding_rate_diff,  # 🔥 已弃用，保留兼容性
            last_update=datetime.now()
        )
    
        # 🔥 详细日志
        spread_direction_str = "长期有利" if natural_spread > 0 else ("长期亏损" if natural_spread < 0 else "基本平衡")
        
        # 🔥 资金费率差描述（永远≥0，表示绝对值差异）
        funding_description_str = "无数据"
        if natural_funding_rate_diff is not None:
            if natural_funding_rate_diff > 0.02:  # 大于0.02%，有明显差异
                funding_description_str = "有明显差异"
            elif natural_funding_rate_diff > 0.005:  # 大于0.005%，有差异
                funding_description_str = "有差异"
            else:
                funding_description_str = "差异很小"
        
        # 🔥 构建日志字符串
        funding_rate_line = (
            f"   天然资金费率差: {natural_funding_rate_diff:.6f}% ({funding_description_str})"
            if natural_funding_rate_diff is not None
            else "   天然资金费率差: 无数据"
        )
        
        logger.info(
            f"📊 [天然数据] {symbol} {exchange_buy}→{exchange_sell}\n"
            f"   天然价差: {natural_spread:+.4f}% ({spread_direction_str})\n"
            f"{funding_rate_line}\n"
            f"   数据点数: {data_points}\n"
            f"   时间窗口: {window_hours:.2f}小时\n"
            f"   资金费率稳定: {'✅ 是' if funding_rate_stable else '❌ 否'}\n"
            f"   价差稳定: {'✅ 是' if spread_stable else '❌ 否'}"
        )
    
    async def _get_history_data_dynamic(
        self,
        symbol: str,
        exchange_buy: str,
        exchange_sell: str,
        window_hours: float
    ) -> List[Dict]:
        """从数据库获取历史数据（动态时间窗口）
        
        Args:
            symbol: 交易对
            exchange_buy: 买入交易所
            exchange_sell: 卖出交易所
            window_hours: 时间窗口（小时）
        
        Returns:
            历史数据列表
        """
        if not self.db_path.exists():
            return []
        
        cutoff_time = datetime.now() - timedelta(hours=window_hours)
        
        try:
            async with aiosqlite.connect(self.db_path) as db:
                async with db.execute("""
                    SELECT 
                        spread_pct,
                        funding_rate_diff,
                        timestamp
                    FROM spread_history_sampled
                    WHERE symbol = ?
                      AND exchange_buy = ?
                      AND exchange_sell = ?
                      AND timestamp >= ?
                    ORDER BY timestamp ASC
                """, (symbol, exchange_buy, exchange_sell, cutoff_time.isoformat())) as cursor:
                    rows = await cursor.fetchall()
                    return [
                        {
                            'spread_pct': row[0],
                            'funding_rate_diff': row[1],
                            'timestamp': row[2]
                        }
                        for row in rows
                    ]
        except Exception as e:
            logger.error(
                f"❌ [天然价差] 获取历史数据失败: {symbol} {exchange_buy}→{exchange_sell}: {e}",
                exc_info=True
            )
            return []
    
    def _calculate_funding_rate_stability(
        self,
        funding_rate_diff_history: List[float]
    ) -> bool:
        """
        计算历史资金费率差稳定性（从配置读取参数）
        
        判断逻辑：
        1. 持续时间稳定性：资金费率差 ≥ min_threshold 的时间占比 ≥ duration_ratio
        2. 波动幅度稳定性：波动系数（标准差/平均值）≤ stability_threshold
        3. 趋势稳定性：极端波动次数 ≤ extreme_ratio
        
        Args:
            funding_rate_diff_history: 资金费率差历史数据
        
        Returns:
            是否稳定
        """
        # 🔥 使用配置的最少数据点要求
        if len(funding_rate_diff_history) < self.min_data_points:
            return False
        
        try:
            # 1. 持续时间稳定性（使用配置参数）
            above_threshold_count = sum(
                1 for diff in funding_rate_diff_history
                if diff >= self.funding_rate_min_threshold  # 🔥 从配置读取
            )
            duration_ratio = above_threshold_count / len(funding_rate_diff_history)
            duration_stable = duration_ratio >= self.funding_rate_duration_ratio  # 🔥 从配置读取
            
            # 2. 波动幅度稳定性（使用配置参数）
            mean_diff = statistics.mean(funding_rate_diff_history)
            if mean_diff == 0:
                return False
            
            std_diff = statistics.stdev(funding_rate_diff_history) if len(funding_rate_diff_history) > 1 else 0
            volatility_coefficient = abs(std_diff / mean_diff) if mean_diff != 0 else float('inf')
            volatility_stable = volatility_coefficient <= self.funding_rate_stability_threshold  # 🔥 从配置读取
            
            # 3. 趋势稳定性（使用配置参数）
            if len(funding_rate_diff_history) > 1:
                changes = [
                    abs(funding_rate_diff_history[i] - funding_rate_diff_history[i-1])
                    for i in range(1, len(funding_rate_diff_history))
                ]
                avg_change = statistics.mean(changes) if changes else 0
                extreme_changes = sum(
                    1 for change in changes
                    if change > avg_change * self.funding_rate_extreme_multiplier  # 🔥 从配置读取
                )
                trend_stable = extreme_changes <= len(funding_rate_diff_history) * self.funding_rate_extreme_ratio  # 🔥 从配置读取
            else:
                trend_stable = True
            
            # 综合判断：三个条件都满足才算稳定
            return duration_stable and volatility_stable and trend_stable
            
        except Exception as e:
            logger.error(f"❌ [历史计算] 计算资金费率差稳定性失败: {e}", exc_info=True)
            return False
    
    def _calculate_spread_stability_v2(
        self,
        spread_history: List[float]
    ) -> bool:
        """
        计算历史价差稳定性（V2版本，支持正负价差，从配置读取参数）
        
        判断逻辑：
        1. 波动范围稳定性：标准差/绝对值平均值 ≤ stability_threshold
        2. 极端波动检测：极端波动次数 ≤ extreme_ratio
        
        Args:
            spread_history: 价差历史数据（包含正负价差）
        
        Returns:
            是否稳定
        """
        # 🔥 使用配置的最少数据点要求
        if len(spread_history) < self.min_data_points:
            return False
        
        try:
            # 1. 波动幅度稳定性（使用配置参数）
            mean_spread = statistics.mean(spread_history)
            std_spread = statistics.stdev(spread_history) if len(spread_history) > 1 else 0
            
            # 计算变异系数（使用绝对值平均）
            abs_mean = statistics.mean([abs(s) for s in spread_history])
            if abs_mean == 0:
                return False
            
            volatility_coefficient = std_spread / abs_mean
            volatility_stable = volatility_coefficient <= self.spread_stability_threshold  # 🔥 从配置读取
            
            # 2. 极端波动检测（使用配置参数）
            if len(spread_history) > 1:
                changes = [
                    abs(spread_history[i] - spread_history[i-1])
                    for i in range(1, len(spread_history))
                ]
                avg_change = statistics.mean(changes) if changes else 0
                extreme_changes = sum(
                    1 for change in changes
                    if change > avg_change * self.spread_extreme_multiplier  # 🔥 从配置读取
                )
                extreme_stable = extreme_changes <= len(spread_history) * self.spread_extreme_ratio  # 🔥 从配置读取
            else:
                extreme_stable = True
            
            # 综合判断：两个条件都满足才算稳定
            return volatility_stable and extreme_stable
            
        except Exception as e:
            logger.error(f"❌ [天然价差] 计算价差稳定性失败: {e}", exc_info=True)
            return False
    
    async def get_natural_spread(
        self,
        symbol: str,
        exchange_buy: str,
        exchange_sell: str
    ) -> tuple[Optional[float], Optional[datetime]]:
        """
        获取天然价差（带时间戳）
        
        Args:
            symbol: 交易对
            exchange_buy: 买入交易所
            exchange_sell: 卖出交易所
        
        Returns:
            (天然价差, 更新时间) 元组
            - 天然价差（百分比）:
              * 正数：历史上此方向有利可图
              * 负数：历史上此方向无法套利
              * None：数据不足，无法计算
            - 更新时间：最后更新时间戳（用于检查数据新鲜度）
        """
        pair_key = f"{symbol}_{exchange_buy}_{exchange_sell}"
        result = self.results.get(pair_key)
        
        if result:
            return (result.natural_spread, result.last_update)
        else:
            return (None, None)
    
    async def get_avg_positive_spread(
        self,
        symbol: str,
        exchange1: str,
        exchange2: str
    ) -> float:
        """
        【已废弃】获取历史平均正数价差值
        
        请使用 get_natural_spread() 替代
        
        Args:
            symbol: 交易对
            exchange1: 交易所1
            exchange2: 交易所2
        
        Returns:
            历史平均正数价差值（百分比），如果没有数据则返回0.0
        """
        logger.warning(
            f"⚠️ [天然价差] get_avg_positive_spread() 已废弃，请使用 get_natural_spread()"
        )
        natural_spread, _ = await self.get_natural_spread(symbol, exchange1, exchange2)
        return natural_spread if natural_spread is not None else 0.0
    
    async def is_funding_rate_stable(
        self,
        symbol: str,
        exchange1: str,
        exchange2: str
    ) -> bool:
        """
        获取历史资金费率差稳定性
        
        Args:
            symbol: 交易对
            exchange1: 交易所1
            exchange2: 交易所2
        
        Returns:
            是否稳定，如果没有数据则返回False
        """
        pair_key = f"{symbol}_{exchange1}_{exchange2}"
        result = self.results.get(pair_key)
        return result.funding_rate_stable if result else False
    
    async def is_spread_stable(
        self,
        symbol: str,
        exchange1: str,
        exchange2: str
    ) -> bool:
        """
        获取历史价差稳定性
        
        Args:
            symbol: 交易对
            exchange1: 交易所1
            exchange2: 交易所2
        
        Returns:
            是否稳定，如果没有数据则返回False
        """
        pair_key = f"{symbol}_{exchange1}_{exchange2}"
        result = self.results.get(pair_key)
        return result.spread_stable if result else False
    
    async def get_natural_funding_rate_diff(
        self,
        symbol: str,
        exchange_buy: str,
        exchange_sell: str
    ) -> tuple[Optional[float], Optional[datetime]]:
        """
        获取天然资金费率差（Natural Funding Rate Difference）
        
        天然资金费率差定义：
        - 两个交易所间的长期资金费率差异（中位数）
        - 永远 ≥ 0（绝对值差值）
        - 计算规则：数值更大的费率 - 数值更小的费率
        
        示例：
        - 交易所A费率 +1%，交易所B费率 +2% → 差值 = |2% - 1%| = 1%
        - 交易所A费率 -1%，交易所B费率 +1% → 差值 = |1% - (-1%)| = 2%
        - 交易所A费率 -2%，交易所B费率 -1% → 差值 = |-1% - (-2%)| = 1%
        
        套利方向：
        - 数值更大的交易所做空（收取资金费率）
        - 数值更小的交易所做多（支付更少或收取更多）
        
        Args:
            symbol: 交易对，如 "BTC-USDC-PERP"
            exchange_buy: 买入交易所（做多方）
            exchange_sell: 卖出交易所（做空方）
        
        Returns:
            tuple[Optional[float], Optional[datetime]]: (天然资金费率差, 最后更新时间)
            - 如果数据不足或不存在，返回 (None, None)
            - 如果存在有效数据，返回 (百分比值 ≥ 0, 更新时间)
        
        Example:
            natural_funding_diff, last_update = await calculator.get_natural_funding_rate_diff(
                "BTC-USDC-PERP", "backpack", "lighter"
            )
            if natural_funding_diff is not None:
                print(f"天然资金费率差: {natural_funding_diff:.6f}%")
                # 决策：当前资金费率差 > 天然资金费率差 + 阈值 → 有套利机会
        """
        pair_key = f"{symbol}_{exchange_buy}_{exchange_sell}"
        result = self.results.get(pair_key)
        
        if result and result.natural_funding_rate_diff is not None:
            return result.natural_funding_rate_diff, result.last_update
        
        return None, None
    
    async def get_current_funding_rate_diff(
        self,
        symbol: str,
        exchange1: str,
        exchange2: str
    ) -> float:
        """
        🔥 已弃用：获取当前资金费率差值
        
        请使用 get_natural_funding_rate_diff() 代替
        
        Args:
            symbol: 交易对
            exchange1: 交易所1
            exchange2: 交易所2
        
        Returns:
            当前资金费率差值（百分比），如果没有数据则返回0.0
        """
        logger.warning(
            f"⚠️ [资金费率差] get_current_funding_rate_diff() 已弃用，请使用 get_natural_funding_rate_diff()"
        )
        pair_key = f"{symbol}_{exchange1}_{exchange2}"
        result = self.results.get(pair_key)
        return result.current_funding_rate_diff if result else 0.0
    
    def get_result_count(self) -> int:
        """获取当前存储的计算结果数量"""
        return len(self.results)
    
    def get_results_for_symbol(self, symbol: str) -> List[ExchangePairResult]:
        """获取指定交易对的所有计算结果"""
        return [
            result for key, result in self.results.items()
            if result.symbol == symbol
        ]

