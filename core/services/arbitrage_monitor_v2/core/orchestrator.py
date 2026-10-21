"""
总调度器 - 协调所有模块

职责：
- 初始化和协调各个模块
- 管理系统生命周期
- 提供统一的对外接口
"""

import asyncio
import logging
from typing import Dict, List, Optional
from pathlib import Path

from core.adapters.exchanges.factory import ExchangeFactory
from core.utils.config_loader import ExchangeConfigLoader

from ..config.monitor_config import ConfigManager, MonitorConfig
from ..config.debug_config import DebugConfig
from ..data.data_receiver import DataReceiver
from ..data.data_processor import DataProcessor
from ..analysis.spread_calculator import SpreadCalculator
from ..analysis.opportunity_finder import OpportunityFinder
from ..display.ui_manager import UIManager
from .health_monitor import HealthMonitor
from ..history import SpreadHistoryRecorder

# 🔥 使用统一日志系统配置（参考网格系统）
from core.adapters.exchanges.utils.setup_logging import LoggingConfig

# 🔥 配置日志记录器（写入文件）
logger = LoggingConfig.setup_logger(
    name=__name__,
    log_file='arbitrage_monitor_v2.log',
    console_formatter=None,  # 不输出到控制台，避免干扰UI
    file_formatter='detailed',
    level=logging.INFO
)


class ArbitrageOrchestrator:
    """套利监控总调度器"""
    
    def __init__(
        self,
        config_path: Optional[Path] = None,
        debug_config: Optional[DebugConfig] = None
    ):
        """
        初始化总调度器
        
        Args:
            config_path: 配置文件路径
            debug_config: Debug配置
        """
        # 加载配置
        self.config_manager = ConfigManager(config_path)
        self.config: MonitorConfig = self.config_manager.get_config()
        self.debug = debug_config or DebugConfig()
        
        # 创建队列
        self.orderbook_queue = asyncio.Queue(maxsize=self.config.orderbook_queue_size)
        self.ticker_queue = asyncio.Queue(maxsize=self.config.ticker_queue_size)
        self.analysis_queue = asyncio.Queue(maxsize=self.config.analysis_queue_size)
        
        # 🔥 混合模式：创建实时滚动区管理器
        from ..display.realtime_scroller import RealtimeScroller
        self.scroller = RealtimeScroller(throttle_ms=500)  # 500ms 节流
        
        # 初始化各层模块
        self.data_receiver = DataReceiver(
            self.orderbook_queue,
            self.ticker_queue,
            self.debug
        )
        
        self.data_processor = DataProcessor(
            self.orderbook_queue,
            self.ticker_queue,
            self.debug,
            scroller=self.scroller  # 🔥 传递滚动区管理器
        )
        
        self.spread_calculator = SpreadCalculator(self.debug)
        
        self.opportunity_finder = OpportunityFinder(
            self.config,
            self.debug,
            scroller=self.scroller  # 🔥 传递滚动区管理器
        )
        
        self.ui_manager = UIManager(
            self.debug,
            scroller=self.scroller  # 🔥 传递滚动区管理器
        )
        
        self.health_monitor = HealthMonitor(
            data_timeout_seconds=self.config.data_timeout_seconds
        )
        
        # 🔥 历史记录器（可选，根据配置启用）
        self.history_recorder: Optional[SpreadHistoryRecorder] = None
        if self.config.spread_history_enabled:
            try:
                self.history_recorder = SpreadHistoryRecorder(
                    data_dir=self.config.spread_history_data_dir,
                    sample_interval_seconds=self.config.spread_history_sample_interval_seconds,
                    sample_strategy=self.config.spread_history_sample_strategy,
                    batch_size=self.config.spread_history_batch_size,
                    batch_timeout=self.config.spread_history_batch_timeout,
                    queue_maxsize=self.config.spread_history_queue_maxsize,
                    compress_after_days=self.config.spread_history_compress_after_days,
                    archive_after_days=self.config.spread_history_archive_after_days,
                    cleanup_interval_hours=self.config.spread_history_cleanup_interval_hours
                )
                logger.info("✅ [历史记录] 历史记录功能已启用")
                logger.info(f"📁 [历史记录] 数据目录: {self.config.spread_history_data_dir}")
                logger.info(f"⏱️  [历史记录] 采样间隔: {self.config.spread_history_sample_interval_seconds}秒")
                # 🔥 诊断：确认日志配置
                logger.info(f"🔍 [历史记录] Orchestrator日志配置检查: handlers={[type(h).__name__ for h in logger.handlers]}, level={logger.level}")
            except Exception as e:
                logger.warning(f"⚠️  [历史记录] 历史记录功能初始化失败（已禁用）: {e}", exc_info=True)
                self.history_recorder = None
        
        # 🎯 UI更新节流（避免每次分析都更新UI导致卡顿）
        self.last_ui_update_time: float = 0
        self.ui_update_interval: float = 1.0  # UI数据更新间隔（秒）
        
        # 任务列表
        self.tasks: List[asyncio.Task] = []
        
        # 运行状态
        self.running = False
        
        print("✅ 套利监控系统初始化完成")
    
    async def start(self):
        """启动系统"""
        if self.running:
            print("⚠️  系统已经在运行中")
            return
        
        # 验证配置
        if not self.config_manager.validate():
            raise ValueError("配置验证失败")
        
        self.running = True
        
        # 1. 初始化交易所适配器
        await self._init_adapters()
        
        # 2. 订阅市场数据
        await self._subscribe_data()
        
        # 3. 启动数据处理器
        await self.data_processor.start()
        
        # 🔥 启动历史记录器（如果启用）
        if self.history_recorder:
            logger.info("🚀 [历史记录] 正在启动历史记录器...")
            await self.history_recorder.start()
            logger.info("✅ [历史记录] 历史记录器启动完成")
        else:
            logger.info("ℹ️  [历史记录] 历史记录功能未启用（配置中已禁用）")
        
        # 4. 启动健康监控
        await self.health_monitor.start(self.config.health_check_interval)
        
        # 5. 🔥 混合模式：启动UI（不清屏，让顶部 print() 滚动显示）
        self.ui_manager.start(refresh_rate=5)
        
        # 5.5. 更新UI配置（让UI知道exchanges和symbols）
        self.ui_manager.update_config({
            'exchanges': self.config.exchanges,
            'symbols': self.config.symbols
        })
        
        # 6. 启动分析任务
        self.tasks.append(asyncio.create_task(self._analysis_loop()))
        
        # 7. 启动UI更新任务
        self.tasks.append(asyncio.create_task(
            self.ui_manager.update_loop(self.config.ui_refresh_interval_ms)
        ))
        
        print("🚀 套利监控系统已启动")
        print(f"📊 监控交易所: {', '.join(self.config.exchanges)}")
        print(f"💰 监控代币: {', '.join(self.config.symbols)}")
        print(f"🎯 最小价差: {self.config.min_spread_pct}%")
    
    async def stop(self):
        """停止系统"""
        if not self.running:
            return
        
        print("\n🛑 正在停止套利监控系统...")
        
        self.running = False
        
        # 🔥 停止历史记录器（如果启用）
        if self.history_recorder:
            try:
                logger.info("🛑 [历史记录] 正在停止历史记录器...")
                await asyncio.wait_for(self.history_recorder.stop(), timeout=3.0)
                logger.info("✅ [历史记录] 历史记录器已停止")
            except asyncio.TimeoutError:
                logger.warning("⏱️  [历史记录] 历史记录器停止超时")
        
        # 停止所有任务
        for task in self.tasks:
            task.cancel()
        
        await asyncio.gather(*self.tasks, return_exceptions=True)
        
        # 停止各个模块
        await self.data_processor.stop()
        await self.health_monitor.stop()
        self.ui_manager.stop()
        await self.data_receiver.cleanup()
        
        print("✅ 套利监控系统已停止")
    
    async def _init_adapters(self):
        """初始化交易所适配器（并行连接优化）"""
        print("🔌 正在连接交易所...")
        
        # 创建工厂实例
        factory = ExchangeFactory()
        config_loader = ExchangeConfigLoader()
        
        # 🚀 第1步：并行创建所有适配器（配置解析）
        adapters_to_connect = []
        
        for exchange in self.config.exchanges:
            try:
                # 尝试加载交易所特定配置文件
                config_path = Path(f"config/exchanges/{exchange}_config.yaml")
                exchange_config = None
                
                if config_path.exists():
                    try:
                        import yaml
                        with open(config_path, 'r', encoding='utf-8') as f:
                            config_data = yaml.safe_load(f)
                        
                        # 配置文件结构是 {exchange: {config...}}，需要获取正确的层级
                        if exchange in config_data:
                            config_data = config_data[exchange]
                        
                        # 转换为ExchangeConfig对象
                        from core.adapters.exchanges.interface import ExchangeConfig
                        from core.adapters.exchanges.models import ExchangeType
                        
                        # 映射交易所类型
                        type_map = {
                            'edgex': ExchangeType.SPOT,  # EdgeX是现货交易所
                            'lighter': ExchangeType.SPOT,
                            'hyperliquid': ExchangeType.PERPETUAL,
                            'binance': ExchangeType.PERPETUAL,
                            'backpack': ExchangeType.SPOT,
                            'paradex': ExchangeType.PERPETUAL,
                            'grvt': ExchangeType.PERPETUAL,
                        }
                        
                        extra_params = dict(config_data.get('extra_params', {}))
                        auth = config_loader.load_auth_config(
                            exchange,
                            use_env=True,
                            config_file=str(config_path)
                        )

                        api_key = auth.api_key or config_data.get('api_key', '')
                        api_secret = (
                            auth.api_secret
                            or auth.private_key
                            or config_data.get('api_secret', '')
                        )
                        private_key = auth.private_key
                        wallet_address = auth.wallet_address or config_data.get('wallet_address')
                        if wallet_address:
                            extra_params.setdefault('wallet_address', wallet_address)
                        if auth.jwt_token:
                            extra_params['jwt_token'] = auth.jwt_token
                        if auth.l2_address:
                            extra_params['l2_address'] = auth.l2_address
                        if auth.sub_account_id:
                            extra_params['sub_account_id'] = auth.sub_account_id

                        exchange_config = ExchangeConfig(
                            exchange_id=exchange,
                            name=config_data.get('name', exchange),
                            exchange_type=type_map.get(exchange, ExchangeType.SPOT),
                            api_key=api_key,
                            api_secret=api_secret,
                            api_passphrase=config_data.get('api_passphrase') or auth.api_passphrase,
                            private_key=private_key,
                            wallet_address=wallet_address,
                            testnet=config_data.get('testnet', False),
                            base_url=config_data.get('base_url'),
                            extra_params=extra_params
                        )
                        print(f"📄 [{exchange}] 已加载配置文件")
                    except Exception as e:
                        print(f"⚠️  [{exchange}] 配置文件解析失败: {e}，使用默认配置")
                        exchange_config = None
                else:
                    print(f"⚠️  [{exchange}] 配置文件不存在，使用默认配置")
                
                # 创建适配器（如果没有配置，工厂会使用默认配置）
                adapter = factory.create_adapter(
                    exchange_id=exchange,
                    config=exchange_config
                )
                
                adapters_to_connect.append((exchange, adapter))
                
            except Exception as e:
                print(f"❌ [{exchange}] 适配器创建失败: {e}")
                raise
        
        # 🚀 第2步：并行连接所有交易所（性能优化）
        async def connect_adapter(exchange: str, adapter):
            """连接单个适配器"""
            try:
                await adapter.connect()
                self.data_receiver.register_adapter(exchange, adapter)
                print(f"✅ [{exchange}] 连接成功")
                return (exchange, adapter, None)
            except Exception as e:
                print(f"❌ [{exchange}] 连接失败: {e}")
                return (exchange, None, e)
        
        # 并行执行所有连接
        results = await asyncio.gather(
            *[connect_adapter(exchange, adapter) for exchange, adapter in adapters_to_connect],
            return_exceptions=True
        )
        
        # 检查连接结果
        failed_exchanges = []
        for result in results:
            if isinstance(result, Exception):
                failed_exchanges.append(str(result))
            elif result[2] is not None:  # 有错误
                failed_exchanges.append(f"{result[0]}: {result[2]}")
        
        if failed_exchanges:
            raise Exception(f"部分交易所连接失败: {', '.join(failed_exchanges)}")
    
    async def _subscribe_data(self):
        """订阅市场数据"""
        print("📡 正在订阅市场数据...")
        
        await self.data_receiver.subscribe_all(self.config.symbols)
        
        print(f"✅ 已订阅 {len(self.config.symbols)} 个代币")
    
    async def _analysis_loop(self):
        """分析循环 - 高频扫描套利机会"""
        print("🔍 分析引擎已启动")
        
        try:
            while self.running:
                try:
                    # 获取所有订单簿数据
                    all_orderbooks = self.data_processor.get_all_orderbooks()
                    all_tickers = self.data_processor.get_all_tickers()
                    
                    # 遍历所有交易对
                    all_opportunities = []
                    # 🔥 保存每个交易对的最佳价差（用于UI表格显示，保证数据一致性）
                    symbol_spreads: Dict[str, float] = {}  # {symbol: best_spread_pct}
                    
                    for symbol in self.config.symbols:
                        # 收集该交易对在各交易所的订单簿
                        orderbooks = {}
                        for exchange in self.config.exchanges:
                            ob = self.data_processor.get_orderbook(exchange, symbol)
                            if ob:
                                orderbooks[exchange] = ob
                                # 更新健康监控
                                self.health_monitor.update_data_time(exchange, symbol)
                        
                        # 至少需要2个交易所有数据
                        if len(orderbooks) < 2:
                            continue
                        
                        # 计算价差（现在包含所有价差，包括正负差价）
                        spreads = self.spread_calculator.calculate_spreads(symbol, orderbooks)
                        
                        # 🔥 修改：保存所有价差数据（用于UI表格显示2个方向的价差）
                        # 同一个代币可能有2个方向的价差，都需要显示
                        if spreads:
                            # 保存所有价差数据，格式：{symbol: [SpreadData, ...]}
                            symbol_spreads[symbol] = spreads
                        else:
                            # 如果没有价差数据，设置为空列表
                            symbol_spreads[symbol] = []
                        
                        # 收集资金费率
                        funding_rates = {}
                        for exchange in self.config.exchanges:
                            ticker = self.data_processor.get_ticker(exchange, symbol)
                            if ticker and hasattr(ticker, 'funding_rate'):
                                funding_rates[exchange] = {symbol: ticker.funding_rate}
                        
                        # 识别机会
                        opportunities = self.opportunity_finder.find_opportunities(spreads, funding_rates)
                        all_opportunities.extend(opportunities)
                        
                        # 🔥 历史记录（非阻塞，只写入内存，性能影响 < 0.01ms）
                        # 🔥 修改：记录所有价差数据（包括正负差价），而不是只记录opportunities
                        # 🔥 同一个代币可能有2个方向的价差（ex1买->ex2卖 和 ex2买->ex1卖），都需要记录
                        if self.config.spread_history_enabled and self.history_recorder:
                            for spread in spreads:
                                # 🔥 从spread数据中提取资金费率（如果可用）
                                funding_rate_buy = funding_rates.get(spread.exchange_buy, {}).get(symbol)
                                funding_rate_sell = funding_rates.get(spread.exchange_sell, {}).get(symbol)
                                funding_rate_diff = None
                                if funding_rate_buy is not None and funding_rate_sell is not None:
                                    # 🔥 资金费率差应该永远为正数（绝对值差值）
                                    funding_rate_diff = abs(funding_rate_sell - funding_rate_buy)
                                
                                # 非阻塞写入时间窗口缓存（< 0.001ms）
                                # 🔥 主要记录：价差百分比（包括正负差价）、资金费率、资金费率差
                                await self.history_recorder.record_spread({
                                    'symbol': spread.symbol,
                                    'exchange_buy': spread.exchange_buy,
                                    'exchange_sell': spread.exchange_sell,
                                    'price_buy': float(spread.price_buy),
                                    'price_sell': float(spread.price_sell),
                                    'spread_pct': spread.spread_pct,  # 🔥 主要数据：价差百分比（正数表示有利可图，负数表示亏损）
                                    'funding_rate_buy': funding_rate_buy,  # 🔥 主要数据：买入交易所资金费率
                                    'funding_rate_sell': funding_rate_sell,  # 🔥 主要数据：卖出交易所资金费率
                                    'funding_rate_diff': funding_rate_diff,  # 🔥 主要数据：资金费率差（8小时费率差，小数形式，如0.0001表示0.01%）
                                    'funding_rate_diff_annual': funding_rate_diff * 1095 * 100 if funding_rate_diff else None,  # 🔥 年化资金费率差（8小时费率差 × 1095 × 100，转换为百分比形式，如54.71%）
                                    'size_buy': float(spread.size_buy),
                                    'size_sell': float(spread.size_sell),
                                })
                    
                    # 更新UI（传递价差数据，保证数据一致性）
                    self._update_ui(all_opportunities, symbol_spreads=symbol_spreads)
                    
                    # 短暂休眠
                    await asyncio.sleep(self.config.analysis_interval_ms / 1000)
                    
                except Exception as e:
                    if self.debug.is_debug_enabled():
                        self.ui_manager.add_debug_message(f"❌ 分析错误: {e}")
                    await asyncio.sleep(0.1)
                    
        except asyncio.CancelledError:
            # 🔥 UI模式下不打印，避免界面闪动（使用UI的debug消息）
            pass
        except Exception as e:
            # 🔥 UI模式下使用debug消息，不直接print
            self.ui_manager.add_debug_message(f"❌ 分析引擎错误: {e}")
    
    def _update_ui(self, opportunities: List, symbol_spreads: Optional[Dict[str, float]] = None):
        """
        更新UI数据（带节流，避免卡顿）
        
        Args:
            opportunities: 机会列表
            symbol_spreads: 每个交易对的所有价差 {symbol: [SpreadData, ...]}（用于UI表格显示，保证数据一致性）
        """
        import time
        current_time = time.time()
        
        # 🎯 节流检查：只在间隔时间到了才更新UI数据
        should_update_data = (current_time - self.last_ui_update_time) >= self.ui_update_interval
        
        # 收集统计信息（轻量级，每次都更新）
        stats = {
            'exchanges': self.config.exchanges,
            'symbols_count': len(self.config.symbols),
            'active_opportunities': len(opportunities),
            **self.data_receiver.get_stats(),
            **self.data_processor.get_stats(),
            **self.opportunity_finder.get_stats(),
        }
        
        # 添加分析延迟（简单估算）
        stats['analysis_latency_ms'] = self.config.analysis_interval_ms
        
        # 🎯 添加UI抽样频率
        stats['ui_update_interval'] = self.ui_manager.price_update_interval
        
        # 始终更新机会和统计（轻量级）
        self.ui_manager.update_opportunities(opportunities)
        self.ui_manager.update_stats(stats)
        
        # 🎯 订单簿数据收集（重量级操作，只在需要时执行）
        if should_update_data:
            orderbook_data = {}
            ticker_data = {}  # 🔥 收集 Ticker 数据（用于资金费率显示）
            
            for exchange in self.config.exchanges:
                orderbook_data[exchange] = {}
                ticker_data[exchange] = {}
                
                for symbol in self.config.symbols:
                    ob = self.data_processor.get_orderbook(exchange, symbol)
                    if ob:
                        orderbook_data[exchange][symbol] = ob
                    
                    # 🔥 获取 Ticker 数据（用于资金费率）
                    ticker = self.data_processor.get_ticker(exchange, symbol)
                    if ticker:
                        ticker_data[exchange][symbol] = ticker
            
            # 更新订单簿数据（包含 Ticker 数据和价差数据，保证数据一致性）
            self.ui_manager.update_orderbook_data(
                orderbook_data, 
                ticker_data=ticker_data,
                symbol_spreads=symbol_spreads  # 🔥 传递后台计算的价差数据
            )
            self.last_ui_update_time = current_time
    
    def get_opportunities(self) -> List:
        """获取当前的套利机会"""
        return self.opportunity_finder.get_all_opportunities()
    
    def get_stats(self) -> Dict:
        """获取系统统计信息"""
        return {
            'data_receiver': self.data_receiver.get_stats(),
            'data_processor': self.data_processor.get_stats(),
            'opportunity_finder': self.opportunity_finder.get_stats(),
            'health': self.health_monitor.get_all_status(),
        }


async def main():
    """主函数 - 用于测试"""
    # 创建基础Debug配置
    debug_config = DebugConfig.create_basic()
    
    # 创建调度器
    config_path = Path("config/arbitrage/monitor_v2.yaml")
    orchestrator = ArbitrageOrchestrator(config_path, debug_config)
    
    try:
        # 启动系统
        await orchestrator.start()
        
        # 持续运行
        while True:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        print("\n收到停止信号...")
    finally:
        await orchestrator.stop()


if __name__ == "__main__":
    asyncio.run(main())
