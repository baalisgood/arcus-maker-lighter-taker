"""
套利监控服务实现

实现多交易所实时价差和资金费率监控功能。
"""

import asyncio
import logging
from decimal import Decimal
from typing import Dict, List, Optional
from datetime import datetime
from collections import defaultdict
from itertools import combinations

# 修复导入路径：OrderBookData, TickerData 在 adapters 模块中
from core.adapters.exchanges.models import OrderBookData, TickerData
from ..interfaces.arbitrage_monitor_service import IArbitrageMonitorService
from ..models.arbitrage_models import (
    ArbitrageOpportunity,
    PriceSpread,
    FundingRateSpread,
    ArbitrageConfig
)
# 🔥 极简符号转换器（套利系统专用，~150行代码）
from ..utils.symbol_converter import SimpleSymbolConverter


class ArbitrageMonitorService(IArbitrageMonitorService):
    """套利监控服务实现"""
    
    def __init__(
        self,
        adapters: Dict[str, object],          # {exchange_name: adapter}
        config: ArbitrageConfig,
        logger: Optional[logging.Logger] = None,
        symbol_converter: Optional[SimpleSymbolConverter] = None  # 🔥 极简符号转换器
    ):
        """
        初始化套利监控服务
        
        Args:
            adapters: 交易所适配器字典
            config: 监控配置
            logger: 日志记录器
            symbol_converter: 极简符号转换器（可选）
        """
        self.adapters = adapters
        self.config = config
        self.logger = logger or logging.getLogger(__name__)
        
        # 🔥 极简符号转换器（可选）
        self.symbol_converter = symbol_converter or SimpleSymbolConverter(self.logger)
        self.logger.info("✅ 使用极简符号转换器（~150行代码，零冗余）")
        
        # 🔥 数据缓存（改造：使用订单簿数据）
        self.orderbook_data: Dict[str, Dict[str, OrderBookData]] = defaultdict(dict)  # {exchange: {symbol: orderbook}}
        self.ticker_data: Dict[str, Dict[str, TickerData]] = defaultdict(dict)  # {exchange: {symbol: ticker}} - 仅用于获取资金费率
        
        # 套利机会缓存
        self.opportunities: List[ArbitrageOpportunity] = []
        
        # 运行状态
        self.running = False
        self.monitor_task = None
        
        # 回调函数
        self.opportunity_callbacks = []
        
        # 🔥 WebSocket连接监控（新增 - 解决数据停止更新问题）
        self.last_data_time: Dict[str, Dict[str, datetime]] = defaultdict(dict)  # {exchange: {symbol: 最后更新时间}}
        self.data_timeout_seconds = 90  # 🔧 数据超时阈值（90秒，平衡灵敏度和稳定性）
        self.connection_monitor_task = None  # 连接监控任务
        self.connection_check_interval = 45  # 🔧 连接检查间隔（45秒，更及时发现问题）
        self.max_reconnect_attempts = 3  # 🔧 最大重连次数（3次，避免频繁重连）
        self.reconnect_attempts: Dict[str, int] = defaultdict(int)  # {exchange: 重连次数}
        self.reconnecting: Dict[str, bool] = defaultdict(bool)  # 🔧 正在重连标志（防止并发）
        self.start_time = datetime.now()  # 🔧 系统启动时间（用于启动缓冲期）
        self.startup_grace_period = 120  # 🔧 启动缓冲期（120秒内不检查，给足够时间接收首次数据）
        self.last_health_check_log = datetime.now()  # 🔧 上次健康检查日志时间
        self.health_check_log_interval = 300  # 🔧 健康检查日志间隔（5分钟输出一次状态）
        
        # 🔥 订阅信息缓存（用于重连后恢复订阅）
        self.subscribed_callbacks: Dict[str, Dict[str, object]] = defaultdict(dict)  # {exchange: {symbol: callback}}
        
        # 🚀 性能优化：异步队列架构
        self.orderbook_queue: Optional[asyncio.Queue] = None  # 订单簿数据队列
        self.ticker_queue: Optional[asyncio.Queue] = None     # Ticker数据队列
        self.analysis_result_queue: Optional[asyncio.Queue] = None  # 分析结果队列（给UI用）
        self.data_processor_task = None  # 数据处理任务
        self.analysis_task = None        # 差价分析任务
        
        # 🚀 性能指标
        self.metrics = {
            'orderbook_queue_size': 0,
            'ticker_queue_size': 0,
            'analysis_queue_size': 0,
            'orderbook_processed': 0,
            'ticker_processed': 0,
            'last_analysis_latency_ms': 0.0,
        }
    
    async def start(self) -> bool:
        """启动监控服务"""
        if self.running:
            self.logger.warning("监控服务已经在运行")
            return False
        
        try:
            self.logger.info("🚀 启动套利监控服务...")
            self.running = True
            
            # 🚀 初始化异步队列（容量限制防止内存溢出）
            self.orderbook_queue = asyncio.Queue(maxsize=500)  # 订单簿队列
            self.ticker_queue = asyncio.Queue(maxsize=500)     # Ticker队列
            self.analysis_result_queue = asyncio.Queue(maxsize=100)  # 分析结果队列
            self.logger.info("✅ 异步队列已初始化 (订单簿队列:500, Ticker队列:500, 结果队列:100)")
            
            # 订阅所有交易所的ticker数据
            await self._subscribe_all()
            
            # 🚀 启动数据处理任务（独立任务，高优先级）
            self.data_processor_task = asyncio.create_task(self._process_data_queue())
            self.logger.info("✅ 数据处理任务已启动")
            
            # 🚀 启动差价分析任务（独立任务，高频扫描）
            self.analysis_task = asyncio.create_task(self._analysis_loop())
            self.logger.info("✅ 差价分析任务已启动")
            
            # 保留原有监控任务（向后兼容，但频率降低）
            self.monitor_task = asyncio.create_task(self._monitor_loop())
            
            # 🔥 启动连接监控任务（新增 - 防止WebSocket静默断开）
            self.connection_monitor_task = asyncio.create_task(self._monitor_connections())
            self.logger.info(f"✅ 连接监控已启动（每{self.connection_check_interval}秒检查一次）")
            
            self.logger.info("✅ 套利监控服务启动成功（异步队列架构）")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 套利监控服务启动失败: {e}", exc_info=True)
            self.running = False
            return False
    
    async def stop(self) -> None:
        """停止监控服务"""
        if not self.running:
            return
        
        self.logger.info("🛑 停止套利监控服务...")
        self.running = False
        
        # 🚀 取消所有任务
        tasks_to_cancel = [
            ("监控任务", self.monitor_task),
            ("数据处理任务", self.data_processor_task),
            ("差价分析任务", self.analysis_task),
            ("连接监控任务", self.connection_monitor_task),
        ]
        
        for task_name, task in tasks_to_cancel:
            if task:
                task.cancel()
            try:
                    await task
                    self.logger.info(f"✅ {task_name}已停止")
            except asyncio.CancelledError:
                pass
        
        # 取消所有订阅
        await self._unsubscribe_all()
        
        self.logger.info("✅ 套利监控服务已停止")
    
    def get_opportunities(self) -> List[ArbitrageOpportunity]:
        """获取当前所有套利机会"""
        return self.opportunities.copy()
    
    def get_current_prices(self, symbol: str) -> Dict[str, Dict[str, Decimal]]:
        """
        获取当前订单簿价格（买1/卖1）
        
        🔥 改造后返回格式：
        {
            "edgex": {"bid": Decimal("100.5"), "ask": Decimal("100.6"), "bid_size": Decimal("10"), "ask_size": Decimal("5")},
            "lighter": {"bid": Decimal("100.4"), "ask": Decimal("100.7"), ...}
        }
        
        Returns:
            字典，key为交易所名称，value为订单簿买卖价和数量
        """
        prices = {}
        for exchange_name in self.adapters.keys():
            orderbook = self.orderbook_data[exchange_name].get(symbol)
            if orderbook and orderbook.best_bid and orderbook.best_ask:
                prices[exchange_name] = {
                    "bid": orderbook.best_bid.price,      # 买1价
                    "ask": orderbook.best_ask.price,      # 卖1价
                    "bid_size": orderbook.best_bid.size,  # 买1数量
                    "ask_size": orderbook.best_ask.size   # 卖1数量
                }
        return prices
    
    def get_current_funding_rates(self, symbol: str) -> Dict[str, Decimal]:
        """获取当前资金费率"""
        rates = {}
        for exchange_name in self.adapters.keys():
            ticker = self.ticker_data[exchange_name].get(symbol)
            if ticker and ticker.funding_rate is not None:
                rates[exchange_name] = ticker.funding_rate
        return rates
    
    def get_statistics(self) -> Dict:
        """获取统计信息"""
        # 🔥 计算每个交易所的连接健康状态
        exchange_health = {}
        current_time = datetime.now()
        
        for exchange_name in self.adapters.keys():
            healthy_count = 0
            total_count = len(self.config.symbols)
            
            for symbol in self.config.symbols:
                if not self._is_data_stale(exchange_name, symbol, current_time):
                    healthy_count += 1
            
            health_ratio = healthy_count / total_count if total_count > 0 else 0
            reconnect_count = self.reconnect_attempts.get(exchange_name, 0)
            is_reconnecting = self.reconnecting.get(exchange_name, False)
            
            exchange_health[exchange_name] = {
                "healthy_count": healthy_count,
                "total_count": total_count,
                "health_ratio": health_ratio,
                "reconnect_count": reconnect_count,
                "is_reconnecting": is_reconnecting,
                "status": "reconnecting" if is_reconnecting else 
                         "healthy" if health_ratio >= 0.8 else 
                         "degraded" if health_ratio >= 0.5 else 
                         "unhealthy"
            }
        
        return {
            "total_exchanges": len(self.adapters),
            "monitored_symbols": len(self.config.symbols),
            "active_opportunities": len(self.opportunities),
            "ticker_data_count": sum(len(tickers) for tickers in self.ticker_data.values()),
            "running": self.running,
            "exchange_health": exchange_health,  # 🔥 交易所健康状态
            "performance_metrics": self.metrics.copy()  # 🚀 性能指标
        }
    
    def add_opportunity_callback(self, callback) -> None:
        """添加套利机会回调函数"""
        self.opportunity_callbacks.append(callback)
    
    # === 私有方法 ===
    
    async def _subscribe_all(self):
        """
        订阅所有交易所的数据
        
        🔥 改造后：
        1. 订阅订单簿数据（用于价格）
        2. 订阅ticker数据（用于资金费率）
        """
        for exchange_name, adapter in self.adapters.items():
            self.logger.info(f"📡 订阅 {exchange_name} 的订单簿和资金费率数据...")
            
            # 🔥 Lighter 特殊处理：使用统一回调
            if exchange_name == "lighter":
                # === 1. 订单簿订阅（Lighter统一回调） ===
                def lighter_orderbook_callback(orderbook):
                    """Lighter 订单簿统一回调"""
                    try:
                        std_symbol = self.symbol_converter.convert_from_exchange(orderbook.symbol, "lighter")
                        if std_symbol in self.config.symbols:
                            self._on_orderbook_update("lighter", std_symbol, orderbook)
                    except Exception as e:
                        self.logger.error(f"❌ Lighter 订单簿回调失败 (symbol={orderbook.symbol}): {e}", exc_info=True)
                
                # === 2. Ticker订阅（用于资金费率，Lighter统一回调） ===
                def lighter_ticker_callback(ticker):
                    """Lighter ticker统一回调（仅用于资金费率）"""
                    try:
                        std_symbol = self.symbol_converter.convert_from_exchange(ticker.symbol, "lighter")
                        if std_symbol in self.config.symbols:
                            self._on_ticker_update("lighter", std_symbol, ticker)
                    except Exception as e:
                        self.logger.error(f"❌ Lighter ticker回调失败 (symbol={ticker.symbol}): {e}", exc_info=True)
                
                # 逐个订阅所有监控的 symbol
                for idx, symbol in enumerate(self.config.symbols):
                    try:
                        exchange_symbol = self.symbol_converter.convert_to_exchange(symbol, "lighter")
                        
                        # 🔥 订单簿订阅（首次注册回调，后续传None）
                        if idx == 0:
                            await adapter.subscribe_orderbook(exchange_symbol, lighter_orderbook_callback)
                            self.logger.info(f"✅ 已订阅 lighter.{exchange_symbol} 订单簿 (首次注册回调)")
                        else:
                            await adapter.subscribe_orderbook(exchange_symbol, None)
                            self.logger.debug(f"✅ 已订阅 lighter.{exchange_symbol} 订单簿")
                        
                        # 🔥 Ticker订阅（用于资金费率）
                        if idx == 0:
                            await adapter.subscribe_ticker(exchange_symbol, lighter_ticker_callback)
                            self.logger.info(f"✅ 已订阅 lighter.{exchange_symbol} ticker (资金费率)")
                        else:
                            await adapter.subscribe_ticker(exchange_symbol, None)
                            self.logger.debug(f"✅ 已订阅 lighter.{exchange_symbol} ticker")
                    except Exception as e:
                        self.logger.error(f"❌ 订阅失败 lighter.{symbol}: {e}")
                
                self.logger.info(f"✅ Lighter 订阅完成，共 {len(self.config.symbols)} 个symbol (订单簿+资金费率)")
                continue
            
            # 🔥 其他交易所（EdgeX, Backpack）：逐个订阅
            for symbol in self.config.symbols:
                try:
                    exchange_symbol = self.symbol_converter.convert_to_exchange(symbol, exchange_name)
                    
                    # === 1. 订单簿回调 ===
                    def create_orderbook_callback(ex, std_symbol):
                        """创建订单簿回调函数工厂"""
                        def callback_wrapper(*args, **kwargs):
                            if len(args) == 1:
                                orderbook = args[0]
                            elif len(args) == 2:
                                _, orderbook = args
                            else:
                                self.logger.error(f"⚠️  未知的订单簿回调参数格式: {len(args)} 个参数")
                                return
                            self._on_orderbook_update(ex, std_symbol, orderbook)
                        return callback_wrapper
                    
                    # === 2. Ticker回调（仅用于资金费率） ===
                    def create_ticker_callback(ex, std_symbol):
                        """创建ticker回调函数工厂"""
                        def callback_wrapper(*args, **kwargs):
                            if len(args) == 1:
                                ticker = args[0]
                            elif len(args) == 2:
                                _, ticker = args
                            else:
                                self.logger.error(f"⚠️  未知的ticker回调参数格式: {len(args)} 个参数")
                                return
                            self._on_ticker_update(ex, std_symbol, ticker)
                        return callback_wrapper
                    
                    # 🔥 订阅订单簿
                    await adapter.subscribe_orderbook(
                        exchange_symbol,
                        create_orderbook_callback(exchange_name, symbol)
                    )
                    self.logger.info(f"✅ 已订阅 {exchange_name}.{exchange_symbol} 订单簿 (标准: {symbol})")
                    
                    # 🔥 订阅ticker（用于资金费率）
                    await adapter.subscribe_ticker(
                        exchange_symbol,
                        create_ticker_callback(exchange_name, symbol)
                    )
                    self.logger.debug(f"✅ 已订阅 {exchange_name}.{exchange_symbol} ticker (资金费率)")
                    
                except Exception as e:
                    self.logger.error(f"❌ 订阅失败 {exchange_name}.{symbol}: {e}")
    
    async def _unsubscribe_all(self):
        """取消所有订阅"""
        for exchange_name, adapter in self.adapters.items():
            try:
                if hasattr(adapter, 'disconnect'):
                    await adapter.disconnect()
            except Exception as e:
                self.logger.error(f"❌ 断开连接失败 {exchange_name}: {e}")
    
    def _on_orderbook_update(self, exchange: str, symbol: str, orderbook: OrderBookData):
        """
        🚀 处理订单簿更新（异步队列模式 - 零延迟接收）
        
        Args:
            exchange: 交易所名称
            symbol: 标准化symbol
            orderbook: 订单簿数据
        """
        # 🚀 快速验证后立即入队，不做复杂处理
        if not orderbook.best_bid or not orderbook.best_ask:
            return  # 静默忽略，不记录日志
        
        if orderbook.best_bid.price <= 0 or orderbook.best_ask.price <= 0:
            return  # 静默忽略，不记录日志
        
        # 🚀 立即放入队列（非阻塞）
        try:
            self.orderbook_queue.put_nowait({
                'exchange': exchange,
                'symbol': symbol,
                'orderbook': orderbook,
                'timestamp': datetime.now()
            })
        except asyncio.QueueFull:
            # 队列满了，说明处理不过来，记录告警
            self.logger.warning(f"⚠️  订单簿队列已满，丢弃 {exchange}.{symbol} 的更新")
            pass
    
    def _on_ticker_update(self, exchange: str, symbol: str, ticker: TickerData):
        """
        🚀 处理ticker更新（异步队列模式 - 零延迟接收）
        
        Args:
            exchange: 交易所名称
            symbol: 标准化symbol
            ticker: ticker数据
        """
        # 🚀 快速验证后立即入队
        if ticker.funding_rate is None:
            return  # 静默忽略，不记录日志
        
        # 🚀 立即放入队列（非阻塞）
        try:
            self.ticker_queue.put_nowait({
                'exchange': exchange,
                'symbol': symbol,
                'ticker': ticker,
                'timestamp': datetime.now()
            })
        except asyncio.QueueFull:
            self.logger.warning(f"⚠️  Ticker队列已满，丢弃 {exchange}.{symbol} 的更新")
            pass
    
    async def _process_data_queue(self):
        """
        🚀 数据处理任务（独立任务，高优先级）
        
        从队列消费数据，更新本地缓存，不做复杂分析
        """
        self.logger.info("🚀 数据处理任务启动...")
        
        while self.running:
            try:
                # 🚀 并发处理订单簿和ticker队列
                tasks = []
                
                # 处理订单簿队列（批量）
                orderbook_batch = []
                while not self.orderbook_queue.empty() and len(orderbook_batch) < 50:
                    try:
                        data = self.orderbook_queue.get_nowait()
                        orderbook_batch.append(data)
                        self.orderbook_queue.task_done()
                    except asyncio.QueueEmpty:
                        break
                
                # 处理ticker队列（批量）
                ticker_batch = []
                while not self.ticker_queue.empty() and len(ticker_batch) < 50:
                    try:
                        data = self.ticker_queue.get_nowait()
                        ticker_batch.append(data)
                        self.ticker_queue.task_done()
                    except asyncio.QueueEmpty:
                        break
                
                # 更新本地缓存（订单簿）
                for data in orderbook_batch:
                    exchange = data['exchange']
                    symbol = data['symbol']
                    orderbook = data['orderbook']
                    
                    # 记录数据更新时间
                    self.last_data_time[exchange][symbol] = data['timestamp']
                    
                    # 重置重连计数
                    if self.reconnect_attempts[exchange] > 0:
                        self.reconnect_attempts[exchange] = 0
                    
                    # 存储订单簿数据
                    self.orderbook_data[exchange][symbol] = orderbook
                    self.metrics['orderbook_processed'] += 1
                
                # 更新本地缓存（ticker）
                for data in ticker_batch:
                    exchange = data['exchange']
                    symbol = data['symbol']
                    ticker = data['ticker']
                    
                    # 记录数据更新时间
                    self.last_data_time[exchange][symbol] = data['timestamp']
                    
                    # 存储ticker数据
                    self.ticker_data[exchange][symbol] = ticker
                    self.metrics['ticker_processed'] += 1
                
                # 更新性能指标
                self.metrics['orderbook_queue_size'] = self.orderbook_queue.qsize()
                self.metrics['ticker_queue_size'] = self.ticker_queue.qsize()
                
                # 短暂休眠，避免CPU占用过高
                await asyncio.sleep(0.001)  # 1ms
                
            except Exception as e:
                self.logger.error(f"❌ 数据处理错误: {e}", exc_info=True)
                await asyncio.sleep(0.1)
    
    async def _analysis_loop(self):
        """
        🚀 差价分析任务（独立任务，高频扫描）
        
        高频扫描所有交易对，计算套利机会，将结果放入结果队列
        """
        self.logger.info("🚀 差价分析任务启动...")
        
        while self.running:
            try:
                analysis_start = datetime.now()
                
                # 🚀 快速扫描所有交易对
                all_opportunities = []
                for symbol in self.config.symbols:
                    opportunities = await self._check_arbitrage_opportunity(symbol)
                    all_opportunities.extend(opportunities)
                
                # 更新机会缓存
                self.opportunities = all_opportunities
                
                # 🚀 将结果放入结果队列（供UI使用）
                try:
                    # 非阻塞放入，如果队列满了就丢弃旧数据
                    while not self.analysis_result_queue.empty():
                        try:
                            self.analysis_result_queue.get_nowait()
                            self.analysis_result_queue.task_done()
                        except asyncio.QueueEmpty:
                            break
                    
                    self.analysis_result_queue.put_nowait(all_opportunities)
                except asyncio.QueueFull:
                    pass
                
                # 计算分析延迟
                analysis_latency = (datetime.now() - analysis_start).total_seconds() * 1000
                self.metrics['last_analysis_latency_ms'] = analysis_latency
                self.metrics['analysis_queue_size'] = self.analysis_result_queue.qsize()
                
                # 调用回调函数
                if all_opportunities:
                    for callback in self.opportunity_callbacks:
                        try:
                            await callback(all_opportunities)
                        except Exception as e:
                            self.logger.error(f"❌ 回调函数错误: {e}")
                
                # 🚀 高频扫描：每10ms一次（100Hz）
                await asyncio.sleep(0.01)
                
            except Exception as e:
                self.logger.error(f"❌ 差价分析错误: {e}", exc_info=True)
                await asyncio.sleep(0.1)
    
    async def _monitor_loop(self):
        """监控循环"""
        self.logger.info("🔄 启动监控循环...")
        
        while self.running:
            try:
                await asyncio.sleep(self.config.update_interval)
                
                # 计算所有交易对的套利机会
                all_opportunities = []
                
                for symbol in self.config.symbols:
                    opportunities = await self._check_arbitrage_opportunity(symbol)
                    all_opportunities.extend(opportunities)
                
                # 更新机会缓存
                self.opportunities = all_opportunities
                
                # 调用回调函数
                if all_opportunities:
                    for callback in self.opportunity_callbacks:
                        try:
                            if asyncio.iscoroutinefunction(callback):
                                await callback(all_opportunities)
                            else:
                                callback(all_opportunities)
                        except Exception as e:
                            self.logger.error(f"❌ 回调函数执行失败: {e}")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"❌ 监控循环错误: {e}", exc_info=True)
    
    async def _check_arbitrage_opportunity(self, symbol: str) -> List[ArbitrageOpportunity]:
        """
        检查单个交易对的套利机会
        
        🔥 改造后：使用订单簿买1/卖1价格
        """
        # 🔥 收集所有交易所的订单簿价格
        orderbook_prices = {}  # {exchange: {"bid": ..., "ask": ..., "bid_size": ..., "ask_size": ...}}
        
        for exchange_name in self.adapters.keys():
            orderbook = self.orderbook_data[exchange_name].get(symbol)
            if orderbook and orderbook.best_bid and orderbook.best_ask:
                orderbook_prices[exchange_name] = {
                    "bid": orderbook.best_bid.price,
                    "ask": orderbook.best_ask.price,
                    "bid_size": orderbook.best_bid.size,
                    "ask_size": orderbook.best_ask.size
                }
        
        # 🔥 收集资金费率（从ticker获取）
        funding_rates = {}
        for exchange_name in self.adapters.keys():
            ticker = self.ticker_data[exchange_name].get(symbol)
            if ticker and ticker.funding_rate is not None:
                    funding_rates[exchange_name] = ticker.funding_rate
        
        # 至少需要2个交易所有价格数据
        if len(orderbook_prices) < 2:
            return []
        
        # 🔥 识别套利机会（使用订单簿价格）
        opportunities = self._identify_opportunities(
            symbol=symbol,
            orderbook_prices=orderbook_prices,
            funding_rates=funding_rates if len(funding_rates) >= 2 else None
        )
        
        return opportunities
    
    def _identify_opportunities(
        self,
        symbol: str,
        orderbook_prices: Dict[str, Dict[str, Decimal]],
        funding_rates: Optional[Dict[str, Decimal]] = None
    ) -> List[ArbitrageOpportunity]:
        """
        识别套利机会
        
        🔥 改造后：使用订单簿买1/卖1价格
        
        Args:
            symbol: 交易对符号
            orderbook_prices: 订单簿价格 {exchange: {"bid": ..., "ask": ..., ...}}
            funding_rates: 资金费率（可选）
        """
        opportunities = []
        
        # 1. 🔥 价差套利机会（基于订单簿）
        price_spreads = self._calculate_price_spreads(symbol, orderbook_prices)
        for spread in price_spreads:
            # 🔥 只保留正差价（有利可图的）
            if spread.spread_pct >= self.config.price_spread_threshold:
                opportunities.append(ArbitrageOpportunity(
                    symbol=symbol,
                    opportunity_type="price_spread",
                    price_spread=spread
                ))
        
        # 2. 资金费率套利机会
        if funding_rates:
            funding_spreads = self._calculate_funding_rate_spreads(symbol, funding_rates)
            for spread in funding_spreads:
                if spread.spread_abs >= self.config.funding_rate_threshold:
                    opportunities.append(ArbitrageOpportunity(
                        symbol=symbol,
                        opportunity_type="funding_rate",
                        funding_rate_spread=spread
                    ))
        
        # 3. 组合套利机会（价差 + 资金费率）
        if price_spreads and funding_rates:
            best_price_spread = price_spreads[0] if price_spreads else None
            
            if best_price_spread:
                buy_ex = best_price_spread.exchange_buy
                sell_ex = best_price_spread.exchange_sell
                
                if buy_ex in funding_rates and sell_ex in funding_rates:
                    rate_buy = funding_rates[buy_ex]
                    rate_sell = funding_rates[sell_ex]
                    
                    # 理想情况：买入交易所费率高，卖出交易所费率低
                    if rate_buy > rate_sell:
                        funding_spread = FundingRateSpread(
                            symbol=symbol,
                            exchange_high=buy_ex,
                            exchange_low=sell_ex,
                            rate_high=rate_buy,
                            rate_low=rate_sell,
                            spread_abs=rate_buy - rate_sell,
                            spread_pct=Decimal("0"),
                            timestamp=datetime.now()
                        )
                        
                        # 检查是否都超过阈值
                        if (best_price_spread.spread_pct >= self.config.price_spread_threshold and
                            funding_spread.spread_abs >= self.config.funding_rate_threshold):
                            opportunities.append(ArbitrageOpportunity(
                                symbol=symbol,
                                opportunity_type="combined",
                                price_spread=best_price_spread,
                                funding_rate_spread=funding_spread
                            ))
        
        # 按评分降序排列
        opportunities.sort(key=lambda x: x.score, reverse=True)
        
        return opportunities
    
    def _calculate_price_spreads(
        self,
        symbol: str,
        orderbook_prices: Dict[str, Dict[str, Decimal]]
    ) -> List[PriceSpread]:
        """
        计算价差（基于订单簿买1/卖1）
        
        🔥 改造后：
        - 使用订单簿买1/卖1价格
        - 只计算有利可图的价差（正向套利：B买1价 > A卖1价）
        
        Args:
            symbol: 交易对符号
            orderbook_prices: 订单簿价格 {exchange: {"bid": ..., "ask": ..., "bid_size": ..., "ask_size": ...}}
        
        Returns:
            有利可图的价差列表，按价差百分比降序排列
        """
        spreads = []
        
        # 🔥 对所有交易所两两组合计算套利机会
        for exchange1, exchange2 in combinations(orderbook_prices.keys(), 2):
            book1 = orderbook_prices[exchange1]  # {bid, ask, bid_size, ask_size}
            book2 = orderbook_prices[exchange2]
            
            # === 正向套利1：在exchange1买入（ask1），在exchange2卖出（bid2） ===
            # 有利可图条件：bid2 > ask1
            if book2["bid"] > book1["ask"]:
                spread_abs = book2["bid"] - book1["ask"]
                spread_pct = (spread_abs / book1["ask"]) * Decimal("100")
                
                spreads.append(PriceSpread(
                    symbol=symbol,
                    exchange_buy=exchange1,      # 在exchange1以ask1价格买入
                    exchange_sell=exchange2,     # 在exchange2以bid2价格卖出
                    price_buy=book1["ask"],      # 买入价格（exchange1的卖1价）
                    price_sell=book2["bid"],     # 卖出价格（exchange2的买1价）
                    size_buy=book1["ask_size"],  # 买入深度
                    size_sell=book2["bid_size"], # 卖出深度
                    spread_abs=spread_abs,       # 绝对价差
                    spread_pct=spread_pct,       # 百分比价差
                    timestamp=datetime.now()
                ))
            
            # === 正向套利2：在exchange2买入（ask2），在exchange1卖出（bid1） ===
            # 有利可图条件：bid1 > ask2
            if book1["bid"] > book2["ask"]:
                spread_abs = book1["bid"] - book2["ask"]
                spread_pct = (spread_abs / book2["ask"]) * Decimal("100")
            
            spreads.append(PriceSpread(
                symbol=symbol,
                    exchange_buy=exchange2,      # 在exchange2以ask2价格买入
                    exchange_sell=exchange1,     # 在exchange1以bid1价格卖出
                    price_buy=book2["ask"],      # 买入价格（exchange2的卖1价）
                    price_sell=book1["bid"],     # 卖出价格（exchange1的买1价）
                    size_buy=book2["ask_size"],  # 买入深度
                    size_sell=book1["bid_size"], # 卖出深度
                    spread_abs=spread_abs,       # 绝对价差
                    spread_pct=spread_pct,       # 百分比价差
                timestamp=datetime.now()
            ))
        
        # 🔥 按价差百分比降序排列（最大收益排在前面）
        spreads.sort(key=lambda x: x.spread_pct, reverse=True)
        
        return spreads
    
    def _calculate_funding_rate_spreads(
        self,
        symbol: str,
        funding_rates: Dict[str, Decimal]
    ) -> List[FundingRateSpread]:
        """计算资金费率差"""
        spreads = []
        
        # 对所有交易所两两组合计算费率差
        for exchange1, exchange2 in combinations(funding_rates.keys(), 2):
            rate1 = funding_rates[exchange1]
            rate2 = funding_rates[exchange2]
            
            # 确定高费率和低费率交易所
            if rate1 > rate2:
                exchange_high = exchange1
                exchange_low = exchange2
                rate_high = rate1
                rate_low = rate2
            else:
                exchange_high = exchange2
                exchange_low = exchange1
                rate_high = rate2
                rate_low = rate1
            
            # 计算费率差
            spread_abs = rate_high - rate_low
            
            # 计算百分比差
            if rate_low != 0:
                spread_pct = (spread_abs / abs(rate_low)) * Decimal("100")
            else:
                spread_pct = Decimal("0")
            
            spreads.append(FundingRateSpread(
                symbol=symbol,
                exchange_high=exchange_high,
                exchange_low=exchange_low,
                rate_high=rate_high,
                rate_low=rate_low,
                spread_abs=spread_abs,
                spread_pct=spread_pct,
                timestamp=datetime.now()
            ))
        
        # 按绝对费率差降序排列
        spreads.sort(key=lambda x: x.spread_abs, reverse=True)
        
        return spreads
    
    # === 🔥 WebSocket连接监控相关方法（新增 - 解决数据停止更新问题）===
    
    async def _monitor_connections(self):
        """
        监控WebSocket连接健康状态
        
        定期检查每个交易所的数据更新情况：
        - 🔧 启动后120秒内不检查（给足够时间接收首次数据）
        - 如果数据超过阈值时间未更新，触发重连
        - 🔧 防止并发重连（同一交易所同时只能有一个重连任务）
        """
        self.logger.info(
            f"🔄 WebSocket连接监控循环已启动 "
            f"(检查间隔: {self.connection_check_interval}秒, "
            f"启动缓冲期: {self.startup_grace_period}秒)"
        )
        
        while self.running:
            try:
                current_time = datetime.now()
                
                # 🔧 启动缓冲期检查
                elapsed_since_start = (current_time - self.start_time).total_seconds()
                if elapsed_since_start < self.startup_grace_period:
                    remaining = self.startup_grace_period - elapsed_since_start
                    # 🔧 改用INFO级别，让用户看到监控循环在工作
                    if remaining > 60:  # 只在缓冲期前半段输出
                        self.logger.info(
                            f"⏳ 连接监控启动缓冲期中，剩余 {remaining:.0f} 秒后开始检查"
                        )
                    await asyncio.sleep(self.connection_check_interval)
                    continue
                
                # 检查每个交易所的数据时效性
                for exchange_name in self.adapters.keys():
                    # 🔧 检查是否正在重连
                    if self.reconnecting.get(exchange_name, False):
                        self.logger.debug(f"⏳ {exchange_name} 正在重连中，跳过本次检查")
                        continue
                    
                    # 🔧 检查重连次数
                    if self.reconnect_attempts[exchange_name] >= self.max_reconnect_attempts:
                        # 已达上限，不再检查
                        continue
                    
                    # 检查该交易所的所有监控符号
                    stale_symbols = []
                    
                    for symbol in self.config.symbols:
                        if self._is_data_stale(exchange_name, symbol, current_time):
                            stale_symbols.append(symbol)
                    
                    # 🔧 只有当大部分符号都超时时才重连（避免误判）
                    total_symbols = len(self.config.symbols)
                    stale_ratio = len(stale_symbols) / total_symbols if total_symbols > 0 else 0
                    
                    if stale_ratio > 0.5:  # 超过50%的符号超时才重连
                        self.logger.warning(
                            f"⚠️  {exchange_name} 检测到 {len(stale_symbols)}/{total_symbols} "
                            f"个符号数据超时 ({stale_ratio*100:.0f}%)"
                        )
                        
                        # 🔧 触发重连（异步执行，但标记正在重连）
                        asyncio.create_task(self._reconnect_exchange(exchange_name))
                
                # 🔧 定期输出健康检查日志（每5分钟一次）
                time_since_last_log = (current_time - self.last_health_check_log).total_seconds()
                if time_since_last_log >= self.health_check_log_interval:
                    self._log_connection_health(current_time)
                    self.last_health_check_log = current_time
                
                # 等待下次检查
                await asyncio.sleep(self.connection_check_interval)
                
            except asyncio.CancelledError:
                self.logger.info("连接监控循环已取消")
                break
            except Exception as e:
                self.logger.error(f"❌ 连接监控循环异常: {e}", exc_info=True)
                await asyncio.sleep(10)  # 出错后等待10秒再继续
    
    def _is_data_stale(self, exchange: str, symbol: str, current_time: datetime) -> bool:
        """
        检查指定交易所和符号的数据是否过期
        
        Args:
            exchange: 交易所名称
            symbol: 交易对符号
            current_time: 当前时间
            
        Returns:
            bool: 数据是否过期
        """
        last_update = self.last_data_time.get(exchange, {}).get(symbol)
        
        # 如果从未收到数据，认为是过期的
        if not last_update:
            return True
        
        # 计算距离上次更新的时间
        elapsed = (current_time - last_update).total_seconds()
        
        # 超过阈值认为过期
        return elapsed > self.data_timeout_seconds
    
    def _log_connection_health(self, current_time: datetime):
        """
        输出连接健康状态日志
        
        定期输出每个交易所的数据更新情况，帮助用户了解系统状态
        
        Args:
            current_time: 当前时间
        """
        self.logger.info("=" * 60)
        self.logger.info("📊 WebSocket 连接健康检查")
        
        for exchange_name in self.adapters.keys():
            # 统计该交易所的数据状态
            total_symbols = len(self.config.symbols)
            stale_count = 0
            min_elapsed = None
            max_elapsed = None
            
            for symbol in self.config.symbols:
                last_update = self.last_data_time.get(exchange_name, {}).get(symbol)
                
                if not last_update:
                    stale_count += 1
                    continue
                
                elapsed = (current_time - last_update).total_seconds()
                
                if self._is_data_stale(exchange_name, symbol, current_time):
                    stale_count += 1
                
                # 更新最小/最大时间差
                if min_elapsed is None or elapsed < min_elapsed:
                    min_elapsed = elapsed
                if max_elapsed is None or elapsed > max_elapsed:
                    max_elapsed = elapsed
            
            # 输出状态
            healthy_count = total_symbols - stale_count
            status = "✅ 正常" if stale_count == 0 else f"⚠️  异常 ({stale_count}个超时)"
            
            if min_elapsed is not None and max_elapsed is not None:
                self.logger.info(
                    f"  {exchange_name:10s}: {status} | "
                    f"健康: {healthy_count}/{total_symbols} | "
                    f"数据延迟: {min_elapsed:.0f}~{max_elapsed:.0f}秒"
                )
            else:
                self.logger.info(
                    f"  {exchange_name:10s}: {status} | "
                    f"健康: {healthy_count}/{total_symbols} | "
                    f"数据延迟: 无数据"
                )
            
            # 显示重连次数
            reconnect_count = self.reconnect_attempts.get(exchange_name, 0)
            if reconnect_count > 0:
                self.logger.info(
                    f"    ↳ 重连次数: {reconnect_count}/{self.max_reconnect_attempts}"
                )
        
        self.logger.info("=" * 60)
    
    async def _reconnect_exchange(self, exchange_name: str):
        """
        重连指定交易所
        
        重连流程：
        1. 🔧 设置重连标志（防止并发）
        2. 断开现有连接
        3. 等待一段时间
        4. 重新连接
        5. 恢复所有订阅
        6. 🔧 清除重连标志
        
        Args:
            exchange_name: 交易所名称
        """
        try:
            # 🔧 防止并发重连
            if self.reconnecting.get(exchange_name, False):
                self.logger.warning(f"⚠️  {exchange_name} 已经在重连中，跳过")
                return
            
            # 设置重连标志
            self.reconnecting[exchange_name] = True
            
            self.reconnect_attempts[exchange_name] += 1
            attempt = self.reconnect_attempts[exchange_name]
            
            self.logger.info(
                f"🔄 开始重连 {exchange_name} "
                f"(第 {attempt}/{self.max_reconnect_attempts} 次尝试)..."
            )
            
            adapter = self.adapters.get(exchange_name)
            if not adapter:
                self.logger.error(f"❌ {exchange_name} 适配器不存在")
                return
            
            # 1. 断开现有连接
            try:
                if hasattr(adapter, 'disconnect'):
                    await adapter.disconnect()
                    self.logger.info(f"✅ {exchange_name} 已断开连接")
            except Exception as e:
                self.logger.warning(f"⚠️  {exchange_name} 断开连接时出错: {e}")
            
            # 2. 等待一段时间（指数退避）
            wait_time = min(5 * attempt, 30)  # 最多等30秒
            self.logger.info(f"⏳ 等待 {wait_time} 秒后重连...")
            await asyncio.sleep(wait_time)
            
            # 3. 重新连接
            try:
                if hasattr(adapter, 'connect'):
                    await adapter.connect()
                    self.logger.info(f"✅ {exchange_name} 已重新连接")
            except Exception as e:
                self.logger.error(f"❌ {exchange_name} 重新连接失败: {e}")
                return
            
            # 4. 恢复所有订阅
            self.logger.info(f"📡 恢复 {exchange_name} 的订阅...")
            
            # 🔥 Lighter 特殊处理
            if exchange_name == "lighter":
                # === 1. 订单簿回调 ===
                def lighter_orderbook_callback(orderbook):
                    """Lighter 订单簿统一回调"""
                    try:
                        std_symbol = self.symbol_converter.convert_from_exchange(
                            orderbook.symbol, "lighter"
                        )
                        if std_symbol in self.config.symbols:
                            self._on_orderbook_update("lighter", std_symbol, orderbook)
                    except Exception as e:
                        self.logger.error(
                            f"❌ Lighter 订单簿回调失败 (symbol={orderbook.symbol}): {e}"
                        )
                
                # === 2. Ticker回调（用于资金费率） ===
                def lighter_ticker_callback(ticker):
                    """Lighter ticker统一回调"""
                    try:
                        std_symbol = self.symbol_converter.convert_from_exchange(
                            ticker.symbol, "lighter"
                        )
                        if std_symbol in self.config.symbols:
                            self._on_ticker_update("lighter", std_symbol, ticker)
                    except Exception as e:
                        self.logger.error(
                            f"❌ Lighter ticker回调失败 (symbol={ticker.symbol}): {e}"
                        )
                
                # 重新订阅所有符号
                for idx, symbol in enumerate(self.config.symbols):
                    try:
                        exchange_symbol = self.symbol_converter.convert_to_exchange(
                            symbol, "lighter"
                        )
                        
                        # 订单簿订阅
                        if idx == 0:
                            await adapter.subscribe_orderbook(exchange_symbol, lighter_orderbook_callback)
                        else:
                            await adapter.subscribe_orderbook(exchange_symbol, None)
                        
                        # Ticker订阅
                        if idx == 0:
                            await adapter.subscribe_ticker(exchange_symbol, lighter_ticker_callback)
                        else:
                            await adapter.subscribe_ticker(exchange_symbol, None)
                        
                        self.logger.debug(f"✅ 已重新订阅 lighter.{exchange_symbol} (订单簿+ticker)")
                    except Exception as e:
                        self.logger.error(f"❌ 重新订阅失败 lighter.{symbol}: {e}")
            
            # 🔥 其他交易所
            else:
                for symbol in self.config.symbols:
                    try:
                        exchange_symbol = self.symbol_converter.convert_to_exchange(
                            symbol, exchange_name
                        )
                        
                        # === 1. 订单簿回调 ===
                        def create_orderbook_callback(ex, std_symbol):
                            def callback_wrapper(*args, **kwargs):
                                if len(args) == 1:
                                    orderbook = args[0]
                                elif len(args) == 2:
                                    _, orderbook = args
                                else:
                                    return
                                self._on_orderbook_update(ex, std_symbol, orderbook)
                            return callback_wrapper
                        
                        # === 2. Ticker回调 ===
                        def create_ticker_callback(ex, std_symbol):
                            def callback_wrapper(*args, **kwargs):
                                if len(args) == 1:
                                    ticker = args[0]
                                elif len(args) == 2:
                                    _, ticker = args
                                else:
                                    return
                                self._on_ticker_update(ex, std_symbol, ticker)
                            return callback_wrapper
                        
                        # 重新订阅订单簿
                        await adapter.subscribe_orderbook(
                            exchange_symbol,
                            create_orderbook_callback(exchange_name, symbol)
                        )
                        
                        # 重新订阅ticker
                        await adapter.subscribe_ticker(
                            exchange_symbol,
                            create_ticker_callback(exchange_name, symbol)
                        )
                        
                        self.logger.debug(
                            f"✅ 已重新订阅 {exchange_name}.{exchange_symbol} (订单簿+ticker)"
                        )
                    except Exception as e:
                        self.logger.error(
                            f"❌ 重新订阅失败 {exchange_name}.{symbol}: {e}"
                        )
            
            self.logger.info(
                f"✅ {exchange_name} 重连完成，已恢复所有订阅 "
                f"(共 {len(self.config.symbols)} 个符号)"
            )
            
        except Exception as e:
            self.logger.error(f"❌ {exchange_name} 重连过程失败: {e}", exc_info=True)
        finally:
            # 🔧 清除重连标志（无论成功还是失败）
            self.reconnecting[exchange_name] = False

