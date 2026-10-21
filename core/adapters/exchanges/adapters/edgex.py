"""
EdgeX交易所适配器 - 重构版本

基于EdgeX交易所API实现的适配器，使用模块化设计
官方端点：
- HTTP: https://pro.edgex.exchange/
- WebSocket: wss://quote.edgex.exchange/

注意：由于EdgeX官方API文档不可用，此实现基于标准交易所API模式
"""

import asyncio
import time
import json
from typing import Dict, List, Optional, Any, Union, Callable
from decimal import Decimal
from datetime import datetime
import copy

from ..adapter import ExchangeAdapter
from ..interface import ExchangeConfig
from ..models import (
    ExchangeType, OrderBookData, TradeData, TickerData, BalanceData, OrderData,
    OrderSide, OrderType, OrderStatus, PositionData, ExchangeInfo, OHLCVData
)
from ....services.events import Event

# 导入分离的模块
from .edgex_base import EdgeXBase
from .edgex_rest import EdgeXRest
from .edgex_websocket import EdgeXWebSocket
from ..subscription_manager import SubscriptionManager, DataType, create_subscription_manager


class EdgeXAdapter(ExchangeAdapter):
    """EdgeX交易所适配器 - 基于MESA架构的统一接口实现"""

    def __init__(self, config: ExchangeConfig, event_bus=None):
        """初始化EdgeX适配器"""
        super().__init__(config, event_bus)
        if self.logger and hasattr(self.logger, "logger"):
            # ExchangeAdapter 的 BaseLogger
            self.logger = self.logger.logger
        
        # 初始化组件模块
        self.base = EdgeXBase(config)
        
        # 🔥 先设置logger，确保REST初始化时能正确记录日志
        self.rest = EdgeXRest(config, self.logger)  # logger已经在super().__init__中设置
        self.websocket = EdgeXWebSocket(config, self.logger)

        # 🔗 共享持仓缓存，便于UI等模块直接读取 adapter._position_cache
        shared_position_cache: Dict[str, PositionData] = {}
        self._position_cache = shared_position_cache
        if hasattr(self.websocket, "_position_cache"):
            self.websocket._position_cache = shared_position_cache
        if hasattr(self.rest, "_position_cache"):
            self.rest._position_cache = shared_position_cache
        self._ws_position_warned = False
        self._ws_balance_warned = False
        self._order_cache: Dict[str, OrderData] = {}
        self._terminal_order_cache: Dict[str, Dict[str, Any]] = {}
        self._terminal_cache_ttl: float = 10.0  # seconds
        
        # 复制基础配置到实例
        self.base_url = self.base.DEFAULT_BASE_URL
        self.ws_url = self.base.DEFAULT_WS_URL
        self.symbols_info = {}
        
        # 确保日志器已设置（防御性编程）
        self.base.logger = self.logger
        self.rest.logger = self.logger
        self.websocket.logger = self.logger
        
        # 🔥 余额刷新配置（从配置文件读取）
        self._balance_use_websocket = False  # 默认使用 REST
        self._balance_rest_interval = 60     # 默认 60 秒刷新间隔
        self._balance_refresh_task = None
        self._last_balance_refresh_time = 0
        
        # 🔥 设置WebSocket回调：收到trade-event时自动刷新余额缓存
        # （暂时注释掉，使用 REST 定时刷新）
        # self._setup_balance_auto_refresh()

        # 🔁 内部回调：记录订单最新状态
        if hasattr(self.websocket, "_order_callbacks"):
            self.websocket._order_callbacks.append(self._handle_internal_order_update)
        
        # 🔥 关键：阻止EdgeX日志输出到终端（与Lighter保持一致）
        # 移除所有StreamHandler，只保留文件输出
        if self.logger:
            self.logger.propagate = False
            handlers_to_remove = []
            for handler in self.logger.handlers:
                from logging import StreamHandler
                from logging.handlers import RotatingFileHandler
                if isinstance(handler, StreamHandler) and not isinstance(handler, RotatingFileHandler):
                    handlers_to_remove.append(handler)
            for handler in handlers_to_remove:
                self.logger.removeHandler(handler)
        
        # 🚀 初始化订阅管理器 - 加载EdgeX配置文件
        try:
            # 尝试加载YAML配置文件
            config_dict = self._load_edgex_config()
            
            # 🔥 修复：获取符号缓存服务实例
            symbol_cache_service = self._get_symbol_cache_service()
            
            self._subscription_manager = create_subscription_manager(
                exchange_config=config_dict,
                symbol_cache_service=symbol_cache_service,
                logger=self.logger
            )
            
            if self.logger:
                mode = config_dict.get('subscription_mode', {}).get('mode', 'unknown')
                self.logger.info(f"[EdgeX] 订阅管理器: 初始化成功 (模式: {mode})")
                
        except Exception as e:
            self.logger.warning(f"⚠️ [EdgeX] 创建订阅管理器失败，使用默认配置: {e}")
            # 使用默认配置
            default_config = {
                'exchange_id': 'edgex',
                'subscription_mode': {
                    'mode': 'predefined',
                    'predefined': {
                        'symbols': ['BTC_USDT_PERP', 'ETH_USDT_PERP', 'SOL_USDT_PERP'],
                        'data_types': {'ticker': True, 'orderbook': True, 'trades': False, 'user_data': False}
                    }
                }
            }
            # 🔥 修复：获取符号缓存服务实例
            symbol_cache_service = self._get_symbol_cache_service()
            
            self._subscription_manager = create_subscription_manager(
                exchange_config=default_config,
                symbol_cache_service=symbol_cache_service,
                logger=self.logger
            )

    def _load_edgex_config(self) -> Dict[str, Any]:
        """加载EdgeX配置文件"""
        import yaml
        import os
        from pathlib import Path
        
        # 尝试多个可能的配置文件路径（统一使用Path，避免属性访问错误）
        base_dir = Path(__file__).resolve().parents[3]
        config_paths = [
            Path('config/exchanges/edgex_config.yaml'),
            Path('config/exchanges/edgex.yaml'),
            base_dir / 'config/exchanges/edgex_config.yaml'
        ]
        
        for config_path in config_paths:
            try:
                if config_path.exists():
                    with config_path.open('r', encoding='utf-8') as f:
                        config_data = yaml.safe_load(f)
                        
                    # 提取EdgeX配置
                    edgex_config = config_data.get('edgex', {})
                    edgex_config['exchange_id'] = 'edgex'
                    
                    # 🔥 读取余额刷新配置
                    balance_refresh_config = edgex_config.get('balance_refresh', {})
                    self._balance_use_websocket = balance_refresh_config.get('use_websocket', False)
                    self._balance_rest_interval = balance_refresh_config.get('rest_interval', 60)
                    
                    if self.logger:
                        self.logger.debug(f"[EdgeX] 配置文件: 已加载 ({config_path.name})")
                        self.logger.info(
                            f"📊 [EdgeX] 余额刷新策略: "
                            f"{'WebSocket' if self._balance_use_websocket else 'REST'} "
                            f"(REST间隔: {self._balance_rest_interval}秒)"
                        )
                    
                    return edgex_config
                    
            except Exception as e:
                if self.logger:
                    self.logger.debug(f"加载配置文件失败 {config_path}: {e}")
                continue
        
        # 如果所有路径都失败，返回默认配置
        if self.logger:
            self.logger.warning("未找到EdgeX配置文件，使用默认配置")
        
        return {
            'exchange_id': 'edgex',
            'subscription_mode': {
                'mode': 'predefined',
                'predefined': {
                    'symbols': ['BTC_USDT_PERP', 'ETH_USDT_PERP', 'SOL_USDT_PERP'],
                    'data_types': {'ticker': True, 'orderbook': True, 'trades': False, 'user_data': False}
                }
            },
            'custom_subscriptions': {
                'active_combination': 'major_coins',
                'combinations': {
                    'major_coins': {
                        'description': '主流币种永续合约订阅',
                        'symbols': ['BTC_USDT_PERP', 'ETH_USDT_PERP', 'SOL_USDT_PERP'],
                        'data_types': {'ticker': True, 'orderbook': True, 'trades': False}
                    }
                }
            }
        }

    # === 生命周期管理实现 ===
    
    async def _do_connect(self) -> bool:
        """执行具体的连接逻辑"""
        try:
            # 建立REST连接
            await self.rest.setup_session()
            
            # 建立WebSocket连接
            await self.websocket.connect()
            
            # 获取支持的交易对
            await self.websocket.fetch_supported_symbols()
            
            # 同步支持的交易对到其他模块
            self.base._supported_symbols = self.websocket._supported_symbols
            self.base._contract_mappings = self.websocket._contract_mappings
            self.base._symbol_contract_mappings = self.websocket._symbol_contract_mappings
            # 🔄 同步映射到REST，确保REST查询的symbol一致
            self.rest._supported_symbols = self.websocket._supported_symbols
            self.rest._contract_mappings = self.websocket._contract_mappings
            self.rest._symbol_contract_mappings = self.websocket._symbol_contract_mappings
            
            self.logger.info("✅ [EdgeX] 连接成功")
            return True

        except Exception as e:
            self.logger.warning(f"❌ [EdgeX] 连接失败: {str(e)}")
            return False

    async def _do_disconnect(self) -> None:
        """执行具体的断开连接逻辑"""
        try:
            # 关闭WebSocket连接
            await self.websocket.disconnect()
            
            # 关闭REST会话
            await self.rest.close_session()
            
            # 清理订阅管理器
            self._subscription_manager.clear_subscriptions()
            
            self.logger.info("✅ [EdgeX] 连接已断开")

        except Exception as e:
            self.logger.warning(f"❌ [EdgeX] 断开连接时出错: {e}")

    async def _do_authenticate(self) -> bool:
        """执行具体的认证逻辑"""
        try:
            # 使用REST模块进行认证
            return await self.rest.authenticate()
        except Exception as e:
            self.logger.warning(f"EdgeX认证失败: {str(e)}")
            return False

    async def _do_health_check(self) -> Dict[str, Any]:
        """执行具体的健康检查"""
        try:
            # 使用REST模块进行健康检查
            return await self.rest.health_check()
        except Exception as e:
            health_data = {
                'exchange_time': datetime.now(),
                'market_count': len(self.base._supported_symbols),
                'api_accessible': False,
                'error': str(e)
            }
            return health_data

    async def _do_heartbeat(self) -> None:
        """执行心跳检测"""
        pass

    # === 市场数据接口实现 ===

    async def get_exchange_info(self) -> ExchangeInfo:
        """获取交易所信息"""
        try:
            # 获取支持的交易对列表
            supported_symbols = await self.get_supported_symbols()
            
            # 构建markets字典
            markets = {}
            for symbol in supported_symbols:
                # 解析symbol获取base和quote
                if '_' in symbol:
                    base, quote = symbol.split('_', 1)
                else:
                    # 回退处理
                    if symbol.endswith('USDT'):
                        base = symbol[:-4]
                        quote = 'USDT'
                    else:
                        base = symbol
                        quote = 'USDT'
                
                markets[symbol] = {
                    'id': symbol,
                    'symbol': symbol,
                    'base': base,
                    'quote': quote,
                    'baseId': base,
                    'quoteId': quote,
                    'active': True,
                    'type': 'swap',
                    'spot': False,
                    'margin': False,
                    'future': False,
                    'swap': True,
                    'option': False,
                    'contract': True,
                    'contractSize': 1,
                    'linear': True,
                    'inverse': False,
                    'expiry': None,
                    'expiryDatetime': None,
                    'strike': None,
                    'optionType': None,
                    'precision': {
                        'amount': 8,
                        'price': 8,
                        'cost': 8,
                        'base': 8,
                        'quote': 8
                    },
                    'limits': {
                        'amount': {'min': 0.001, 'max': 1000000},
                        'price': {'min': 0.01, 'max': 1000000},
                        'cost': {'min': 10, 'max': 10000000},
                        'leverage': {'min': 1, 'max': 100}
                    },
                    'info': {
                        'symbol': symbol,
                        'exchange': 'edgex',
                        'type': 'perpetual'
                    }
                }
            
            self.logger.info(f"✅ EdgeX交易所信息: {len(markets)}个市场")
            
            return ExchangeInfo(
                name="EdgeX",
                id="edgex",
                type=ExchangeType.PERPETUAL,
                supported_features=[
                    "spot_trading", "perpetual_trading", "websocket",
                    "orderbook", "ticker", "ohlcv", "user_stream"
                ],
                rate_limits=self.config.rate_limits,
                precision=self.config.precision,
                fees={},
                markets=markets,
                status="operational",
                timestamp=datetime.now()
            )
            
        except Exception as e:
            self.logger.error(f"❌ 获取EdgeX交易所信息失败: {e}")
            # 返回空markets的基本信息
            return ExchangeInfo(
                name="EdgeX",
                id="edgex",
                type=ExchangeType.PERPETUAL,
                supported_features=[
                    "spot_trading", "perpetual_trading", "websocket",
                    "orderbook", "ticker", "ohlcv", "user_stream"
                ],
                rate_limits=self.config.rate_limits,
                precision=self.config.precision,
                fees={},
                markets={},
                status="operational",
                timestamp=datetime.now()
            )

    async def get_ticker(self, symbol: str) -> TickerData:
        """获取单个交易对行情数据"""
        try:
            mapped_symbol = self.base._map_symbol(symbol)
            ticker_data = await self.rest.fetch_ticker(mapped_symbol)
            return self.base._parse_ticker(ticker_data, symbol)
        except Exception as e:
            self.logger.warning(f"获取ticker数据失败: {e}")
            raise

    async def get_orderbook(self, symbol: str, limit: Optional[int] = None) -> OrderBookData:
        """获取订单簿数据"""
        try:
            mapped_symbol = self.base._map_symbol(symbol)
            orderbook_data = await self.rest.fetch_orderbook(mapped_symbol, limit)
            return self.base._parse_orderbook(orderbook_data, symbol)
        except Exception as e:
            self.logger.warning(f"获取orderbook数据失败: {e}")
            raise

    async def get_orderbook_snapshot(self, symbol: str, limit: Optional[int] = None) -> Dict[str, Any]:
        """
        获取订单簿完整快照 - 通过REST API
        
        Args:
            symbol: 交易对符号
            limit: 深度限制 (EdgeX支持15或200档)
            
        Returns:
            Dict: 完整的订单簿快照数据
        """
        try:
            return await self.rest.get_orderbook_snapshot(symbol, limit)
        except Exception as e:
            self.logger.warning(f"获取订单簿快照失败: {e}")
            return {
                "data": [{
                    "asks": [],
                    "bids": [],
                    "depthType": "SNAPSHOT"
                }]
            }

    async def get_trades(self, symbol: str, since: Optional[datetime] = None, limit: Optional[int] = None) -> List[TradeData]:
        """获取最近成交记录"""
        try:
            mapped_symbol = self.base._map_symbol(symbol)
            since_timestamp = int(since.timestamp() * 1000) if since else None
            trades_data = await self.rest.fetch_trades(mapped_symbol, since_timestamp, limit)
            return [self.base._parse_trade(trade, symbol) for trade in trades_data]
        except Exception as e:
            self.logger.warning(f"获取trades数据失败: {e}")
            return []

    async def get_tickers(self, symbols: Optional[List[str]] = None) -> List[TickerData]:
        """获取多个交易对行情"""
        try:
            if symbols is None:
                symbols = await self.get_supported_symbols()

            # 并发获取所有ticker数据
            tasks = [self.get_ticker(symbol) for symbol in symbols]
            tickers = await asyncio.gather(*tasks, return_exceptions=True)

            # 过滤掉异常结果
            valid_tickers = [ticker for ticker in tickers if isinstance(ticker, TickerData)]
            return valid_tickers

        except Exception as e:
            self.logger.warning(f"获取多个行情数据失败: {e}")
            return []

    async def get_supported_symbols(self) -> List[str]:
        """获取交易所实际支持的交易对列表"""
        return await self.websocket.get_supported_symbols()

    async def get_balances(self) -> List[BalanceData]:
        """
        获取账户余额
        
        策略（根据配置文件 balance_refresh 决定）：
        1. use_websocket=true: 使用 WebSocket 实时推送
        2. use_websocket=false: 使用 REST API 定时刷新（推荐，更稳定）
        """
        import time
        
        # 🔥 策略1：使用 WebSocket（已注释，改用 REST）
        # if self._balance_use_websocket:
        #     balances = self.websocket.get_cached_balances()
        #     if balances:
        #         return balances
        #     
        #     if not hasattr(self, '_ws_balance_warned') or not self._ws_balance_warned:
        #         if self.logger:
        #             self.logger.info("ℹ️ [EdgeX] WS余额推送暂无，等待账户更新...")
        #         self._ws_balance_warned = True
        #     return []
        
        # 🔥 策略2：使用 REST API 定时刷新（默认，推荐）
        current_time = time.time()
        time_since_last_refresh = current_time - self._last_balance_refresh_time
        
        # 如果距离上次刷新时间未达到间隔，且有缓存，返回缓存
        if time_since_last_refresh < self._balance_rest_interval:
            cached_balances = getattr(self, '_cached_rest_balances', None)
            if cached_balances:
                return cached_balances
        
        # 刷新余额
        try:
            rest_balances = await self.rest.get_balances()
            if rest_balances:
                self._cached_rest_balances = rest_balances
                self._last_balance_refresh_time = current_time
                if self.logger:
                    self.logger.debug(
                        f"✅ [EdgeX] REST余额已刷新 "
                        f"({len(rest_balances)}个资产，下次刷新: {self._balance_rest_interval}秒后)"
                    )
                return rest_balances
        except Exception as e:
            if self.logger:
                self.logger.debug(f"REST余额查询失败: {e}")
            
            # 失败时返回缓存（如果有）
            cached_balances = getattr(self, '_cached_rest_balances', None)
            if cached_balances:
                return cached_balances
        
        return []

    async def get_ohlcv(self, symbol: str, timeframe: str, since: Optional[datetime] = None, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """获取K线数据"""
        try:
            mapped_symbol = self.base._map_symbol(symbol)
            
            # 映射时间框架
            interval_map = {
                '1m': '1m', '5m': '5m', '15m': '15m', '30m': '30m',
                '1h': '1h', '4h': '4h', '1d': '1d'
            }
            interval = interval_map.get(timeframe, '1h')
            
            return await self.rest.get_klines(mapped_symbol, interval, since, limit)
        except Exception as e:
            self.logger.warning(f"获取K线数据失败: {e}")
            return []

    # === 交易接口实现 ===

    async def place_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        quantity: Decimal,
        price: Optional[Decimal] = None,
        time_in_force: str = "GTC",
        client_order_id: Optional[str] = None,
        reduce_only: bool = False
    ) -> OrderData:
        """下单"""
        try:
            mapped_symbol = self.base._map_symbol(symbol)
            order = await self.rest.place_order(
                mapped_symbol,
                side,
                order_type,
                quantity,
                price,
                time_in_force,
                client_order_id,
                reduce_only=reduce_only
            )
            self._cache_order_metadata(order)
            return order
        except Exception as e:
            self.logger.warning(f"下单失败: {e}")
            raise

    async def cancel_order(self, order_id: str, symbol: str, client_order_id: str = None) -> OrderData:
        """
        取消订单（ExchangeInterface标准方法）
        
        🔥 方案二：智能处理取消结果
        - 如果取消失败但订单已成交，返回成交数据而非抛错
        - 如果取消成功，返回取消后的订单状态
        
        Args:
            order_id: 订单ID（注意：EdgeX SDK下单后，这实际上可能是client_id）
            symbol: 交易对符号
            client_order_id: 客户端订单ID（可选）
            
        Returns:
            OrderData: 被取消的订单数据或已成交的订单数据
        """
        try:
            mapped_symbol = self.base._map_symbol(symbol)
            
            # 🔥 智能判断：如果order_id看起来像client_id（时间戳格式，13位数字）
            # 则使用client_order_id参数取消
            actual_order_id = None
            actual_client_id = client_order_id
            
            if len(order_id) == 13 and order_id.isdigit():
                # 这很可能是client_id（时间戳格式）
                actual_client_id = order_id
                actual_order_id = None
                self.logger.debug(f"[EdgeX] 检测到client_id格式: {order_id}，使用client_order_id取消")
            else:
                # 这是真正的order_id（EdgeX的order_id通常是长数字）
                actual_order_id = order_id
            
            # 调用REST API取消订单（现在返回详细的结果）
            cancel_result = await self.rest.cancel_order(
                symbol=mapped_symbol,
                order_id=actual_order_id,
                client_order_id=actual_client_id
            )
            
            # 🔥 情况1：订单已成交（取消失败但这是好结果）
            if cancel_result.get("status") == "FILLED":
                filled_data = cancel_result.get("filled_data")
                
                if filled_data:
                    # 有完整的成交数据，解析并返回
                    self.logger.info(
                        f"✅ [EdgeX] 取消失败但订单已成交: {order_id}，返回成交数据"
                    )
                    return self.base._parse_order(filled_data)
                else:
                    # 没有成交数据（查询失败），使用缓存构造占位对象
                    self.logger.warning(
                        f"⚠️ [EdgeX] 取消失败订单已成交但无法获取详情: {order_id}，"
                        "使用缓存构造占位对象"
                    )
                    cached_order = self._get_cached_order(order_id, client_order_id)
                    if cached_order:
                        # 修改缓存订单的状态为已成交
                        cached_order.status = OrderStatus.FILLED
                        cached_order.filled = cached_order.amount
                        cached_order.remaining = Decimal("0")
                        return cached_order
                    else:
                        # 无缓存，构造最小占位对象
                        return self._build_filled_placeholder(symbol, order_id, client_order_id)
            
            # 🔥 情况2：取消成功
            elif cancel_result.get("success"):
                cached_order = self._get_cached_order(order_id, client_order_id)
                if cached_order:
                    cached_order.status = OrderStatus.CANCELED
                    cached_order.remaining = Decimal("0")
                    self._store_terminal_order(cached_order)
                    return cached_order
                placeholder = self._build_cancel_placeholder(symbol, order_id, client_order_id)
                self._store_terminal_order(placeholder)
                return placeholder
            
            # 🔥 情况3：取消失败（其他原因）
            else:
                raise Exception(f"Failed to cancel order {order_id}: {cancel_result}")
                
        except Exception as e:
            self.logger.warning(f"取消订单失败: {e}")
            raise

    async def get_order_status(self, symbol: str, order_id: str = None, client_order_id: str = None) -> OrderData:
        """
        查询订单状态
        
        Args:
            symbol: 交易对符号
            order_id: 订单ID
            client_order_id: 客户端订单ID
            
        Returns:
            OrderData: 订单数据对象
        """
        cached_terminal = self._consume_recent_terminal_order(order_id, client_order_id)
        if cached_terminal:
            self.logger.debug(
                f"[EdgeX] 命中终态订单缓存: order_id={order_id or client_order_id}, "
                f"status={cached_terminal.status.value}"
            )
            return cached_terminal

        try:
            # 使用 REST 的 fetch_order_status 方法
            raw_order = await self.rest.fetch_order_status(
                symbol=symbol,
                order_id=order_id,
                client_order_id=client_order_id
            )
            
            # 转换为 OrderData
            return self.rest._normalize_order_data(raw_order)
            
        except Exception as e:
            self.logger.warning(f"查询订单状态失败: order_id={order_id}, error={e}")
            # 尝试从缓存获取
            cached_order = self._get_cached_order(order_id, client_order_id)
            if cached_order:
                self.logger.info(f"从缓存返回订单数据: order_id={order_id}")
                return cached_order
            raise

    def _cache_order_metadata(self, order: OrderData) -> None:
        """缓存下单时的订单元数据，便于取消失败时兜底"""
        cache_key = str(order.id) if order.id is not None else None
        if cache_key:
            self._order_cache[cache_key] = order
        if order.client_id:
            self._order_cache[str(order.client_id)] = order

    def _get_cached_order(self, order_id: Optional[str], client_order_id: Optional[str]) -> Optional[OrderData]:
        """根据订单ID或客户端ID获取缓存元数据"""
        if order_id and order_id in self._order_cache:
            return self._order_cache[order_id]
        if client_order_id and client_order_id in self._order_cache:
            return self._order_cache[client_order_id]
        return None

    def _store_terminal_order(self, order: OrderData) -> None:
        """缓存最近的终态订单，避免立即触发REST查询"""
        if not order or order.id is None:
            return
        snapshot = copy.deepcopy(order)
        timestamp = time.monotonic()
        order_key = str(order.id)
        self._terminal_order_cache[order_key] = {"order": snapshot, "timestamp": timestamp}
        if order.client_id:
            self._terminal_order_cache[str(order.client_id)] = {
                "order": snapshot,
                "timestamp": timestamp,
            }

    def _consume_recent_terminal_order(
        self,
        order_id: Optional[str],
        client_order_id: Optional[str] = None,
    ) -> Optional[OrderData]:
        """
        命中最近的终态订单缓存（WS推送或撤单后已确认），命中后立即返回以避免REST查询。
        """
        if not self._terminal_order_cache:
            return None
        now = time.monotonic()

        def _lookup(key: Optional[str]) -> Optional[OrderData]:
            if not key:
                return None
            entry = self._terminal_order_cache.get(str(key))
            if not entry:
                return None
            if now - entry["timestamp"] > self._terminal_cache_ttl:
                self._terminal_order_cache.pop(str(key), None)
                return None
            return copy.deepcopy(entry["order"])

        order = _lookup(order_id)
        if order:
            return order
        return _lookup(client_order_id)

    def _build_cancel_placeholder(
        self,
        symbol: str,
        order_id: Optional[str],
        client_order_id: Optional[str] = None
    ) -> OrderData:
        """当查询订单状态失败时，构造占位的取消结果，避免抛错"""
        cached_order = self._get_cached_order(order_id, client_order_id)
        now = datetime.utcnow()
        placeholder_params = {}
        if cached_order and cached_order.params:
            placeholder_params.update(cached_order.params)
        placeholder_params["placeholder"] = True

        placeholder_raw = {"source": "cancel_placeholder"}
        if cached_order and cached_order.raw_data:
            placeholder_raw.update(cached_order.raw_data)

        return OrderData(
            id=str(order_id or client_order_id or f"cancel_{int(now.timestamp()*1000)}"),
            client_id=cached_order.client_id if cached_order else client_order_id,
            symbol=cached_order.symbol if cached_order else symbol,
            side=cached_order.side if cached_order else OrderSide.BUY,
            type=cached_order.type if cached_order else OrderType.LIMIT,
            amount=cached_order.amount if cached_order else Decimal("0"),
            price=cached_order.price if cached_order else None,
            filled=cached_order.filled if cached_order else Decimal("0"),
            remaining=cached_order.remaining if cached_order else Decimal("0"),
            cost=cached_order.cost if cached_order else Decimal("0"),
            average=cached_order.average if cached_order else None,
            status=OrderStatus.CANCELED,
            timestamp=cached_order.timestamp if cached_order else now,
            updated=now,
            fee=cached_order.fee if cached_order else None,
            trades=cached_order.trades if cached_order else [],
            params=placeholder_params,
            raw_data=placeholder_raw
        )

    async def _handle_internal_order_update(self, order: OrderData) -> None:
        """
        内部订单更新回调：记录WS推送的最新状态，用于后续缓存命中。
        """
        try:
            await super()._handle_order_update(order)
        except Exception:
            # 父类只是日志输出，失败时忽略
            pass

        if order:
            self._cache_order_metadata(order)
            if order.status in (OrderStatus.CANCELED, OrderStatus.FILLED):
                self._store_terminal_order(order)
    
    def _build_filled_placeholder(
        self,
        symbol: str,
        order_id: Optional[str],
        client_order_id: Optional[str] = None
    ) -> OrderData:
        """
        当取消失败且订单已成交，但无法获取成交详情时，构造占位的成交结果
        
        🔥 用于方案二：订单已成交但REST查询失败的兜底逻辑
        """
        cached_order = self._get_cached_order(order_id, client_order_id)
        now = datetime.utcnow()
        placeholder_params = {}
        if cached_order and cached_order.params:
            placeholder_params.update(cached_order.params)
        placeholder_params["placeholder"] = True
        placeholder_params["placeholder_reason"] = "filled_but_no_details"

        placeholder_raw = {"source": "filled_placeholder"}
        if cached_order and cached_order.raw_data:
            placeholder_raw.update(cached_order.raw_data)

        return OrderData(
            id=str(order_id or client_order_id or f"filled_{int(now.timestamp()*1000)}"),
            client_id=cached_order.client_id if cached_order else client_order_id,
            symbol=cached_order.symbol if cached_order else symbol,
            side=cached_order.side if cached_order else OrderSide.BUY,
            type=cached_order.type if cached_order else OrderType.LIMIT,
            amount=cached_order.amount if cached_order else Decimal("0"),
            price=cached_order.price if cached_order else None,
            filled=cached_order.amount if cached_order else Decimal("0"),  # 假设全部成交
            remaining=Decimal("0"),  # 已成交，无剩余
            cost=cached_order.cost if cached_order else Decimal("0"),
            average=cached_order.price if cached_order else None,  # 假设按原价成交
            status=OrderStatus.FILLED,  # 🔥 关键：状态为已成交
            timestamp=cached_order.timestamp if cached_order else now,
            updated=now,
            fee=cached_order.fee if cached_order else None,
            trades=cached_order.trades if cached_order else [],
            params=placeholder_params,
            raw_data=placeholder_raw
        )

    async def get_open_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        """获取开放订单"""
        try:
            mapped_symbol = self.base._map_symbol(symbol) if symbol else None
            return await self.rest.get_open_orders(mapped_symbol)
        except Exception as e:
            self.logger.warning(f"获取开放订单失败: {e}")
            return []

    async def get_order_history(self, symbol: Optional[str] = None, since: Optional[datetime] = None, limit: Optional[int] = None) -> List[OrderData]:
        """获取订单历史"""
        try:
            mapped_symbol = self.base._map_symbol(symbol) if symbol else None
            return await self.rest.get_order_history(mapped_symbol, since, limit)
        except Exception as e:
            self.logger.warning(f"获取订单历史失败: {e}")
            return []

    async def cancel_all_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        """取消所有订单"""
        try:
            mapped_symbol = self.base._map_symbol(symbol) if symbol else None
            orders_data = await self.rest.cancel_all_orders(mapped_symbol)
            return [self.base._parse_order(order) for order in orders_data]
        except Exception as e:
            self.logger.warning(f"取消所有订单失败: {e}")
            return []

    async def get_positions(self, symbols: Optional[List[str]] = None) -> List[PositionData]:
        """获取持仓信息（优先使用私有WebSocket缓存）"""
        try:
            positions = self.websocket.get_cached_positions()
            if symbols:
                target_symbols = {
                    self._normalize_symbol_filter(symbol)
                    for symbol in symbols
                    if symbol
                }
                positions = [
                    pos for pos in positions
                    if pos.symbol and self._normalize_symbol_filter(pos.symbol) in target_symbols
                ]
            
            if not positions:
                if self.logger and not self._ws_position_warned:
                    self.logger.info(
                        "ℹ️ [EdgeX] WS持仓仍为空，等待 POSITION_UPDATE/ACCOUNT_UPDATE 推送..."
                    )
                    self._ws_position_warned = True
            else:
                self._ws_position_warned = False
            
            return positions
        except Exception as e:
            if self.logger:
                self.logger.warning(f"获取持仓信息失败: {e}")
            return []

    async def set_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        """设置杠杆倍数"""
        try:
            mapped_symbol = self.base._map_symbol(symbol)
            return await self.rest.set_leverage(mapped_symbol, leverage)
        except Exception as e:
            self.logger.warning(f"设置杠杆失败: {e}")
            return {'symbol': symbol, 'leverage': leverage, 'error': str(e)}

    async def set_margin_mode(self, symbol: str, margin_mode: str) -> Dict[str, Any]:
        """设置保证金模式"""
        try:
            mapped_symbol = self.base._map_symbol(symbol)
            return await self.rest.set_margin_mode(mapped_symbol, margin_mode)
        except Exception as e:
            self.logger.warning(f"设置保证金模式失败: {e}")
            return {'symbol': symbol, 'margin_mode': margin_mode, 'error': str(e)}

    @staticmethod
    def _normalize_symbol_filter(symbol: str) -> str:
        return (
            str(symbol)
            .upper()
            .replace('_', '-')
            .replace('/', '-')
        )

    # === WebSocket订阅接口实现 ===

    async def subscribe_ticker(self, symbol: str, callback: Callable[[TickerData], None]) -> None:
        """订阅行情数据流"""
        await self.websocket.subscribe_ticker(symbol, callback)

    async def subscribe_orderbook(self, symbol: str, callback: Callable[[OrderBookData], None]) -> None:
        """订阅订单簿数据流"""
        await self.websocket.subscribe_orderbook(symbol, callback)

    async def subscribe_trades(self, symbol: str, callback: Callable[[TradeData], None]) -> None:
        """订阅成交数据流"""
        await self.websocket.subscribe_trades(symbol, callback)

    async def subscribe_user_data(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """订阅用户数据流"""
        await self.websocket.subscribe_user_data(callback)

    async def batch_subscribe_tickers(self, symbols: Optional[List[str]] = None, callback: Optional[Callable[[str, TickerData], None]] = None) -> None:
        """批量订阅ticker数据（支持硬编码和动态两种模式）
        
        Args:
            symbols: 要订阅的交易对符号列表（None时使用配置文件中的设置）
            callback: ticker数据回调函数 (symbol, ticker_data)
        """
        try:
            # 🚀 使用订阅管理器确定要订阅的交易对
            if symbols is None:
                # 没有提供symbols，使用订阅管理器
                if self._subscription_manager.mode.value == "predefined":
                    # 硬编码模式：使用配置文件中的交易对
                    symbols = self._subscription_manager.get_subscription_symbols()
                    if self.logger:
                        self.logger.info(f"🔧 EdgeX硬编码模式：使用配置文件中的 {len(symbols)} 个交易对")
                else:
                    # 动态模式：从市场发现交易对
                    symbols = await self._subscription_manager.discover_symbols(self.get_supported_symbols)
                    if self.logger:
                        self.logger.info(f"🔧 EdgeX动态模式：发现 {len(symbols)} 个交易对")
            
            # 检查是否应该订阅ticker数据
            if not self._subscription_manager.should_subscribe_data_type(DataType.TICKER):
                if self.logger:
                    self.logger.info("[EdgeX] 配置中禁用了ticker数据订阅，跳过")
                return
            
            if not symbols:
                if self.logger:
                    self.logger.warning("⚠️ [EdgeX] 没有找到要订阅的交易对")
                return
            
            # 将订阅添加到管理器
            for symbol in symbols:
                self._subscription_manager.add_subscription(
                    symbol=symbol,
                    data_type=DataType.TICKER,
                    callback=callback
                )
            
            # 委托给websocket模块执行实际订阅
            await self.websocket.batch_subscribe_tickers(symbols, callback)
            
            if self.logger:
                self.logger.info(f"✅ [EdgeX] 批量订阅ticker完成: {len(symbols)}个交易对")
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ [EdgeX] 批量订阅ticker失败: {str(e)}")
            raise

    async def batch_subscribe_orderbooks(self, symbols: Optional[List[str]] = None, depth: int = 15, callback: Optional[Callable[[str, OrderBookData], None]] = None) -> None:
        """批量订阅订单簿数据（支持硬编码和动态两种模式）
        
        Args:
            symbols: 要订阅的交易对符号列表（None时使用配置文件中的设置）
            depth: 订单簿深度
            callback: 订单簿数据回调函数
        """
        try:
            # 🚀 使用订阅管理器确定要订阅的交易对
            if symbols is None:
                # 没有提供symbols，使用订阅管理器
                if self._subscription_manager.mode.value == "predefined":
                    # 硬编码模式：使用配置文件中的交易对
                    symbols = self._subscription_manager.get_subscription_symbols()
                    if self.logger:
                        self.logger.info(f"🔧 EdgeX硬编码模式：使用配置文件中的 {len(symbols)} 个交易对")
                else:
                    # 动态模式：从市场发现交易对
                    symbols = await self._subscription_manager.discover_symbols(self.get_supported_symbols)
                    if self.logger:
                        self.logger.info(f"🔧 EdgeX动态模式：发现 {len(symbols)} 个交易对")
            
            # 检查是否应该订阅orderbook数据
            if not self._subscription_manager.should_subscribe_data_type(DataType.ORDERBOOK):
                if self.logger:
                    self.logger.info("[EdgeX] 配置中禁用了orderbook数据订阅，跳过")
                return
            
            if not symbols:
                if self.logger:
                    self.logger.warning("⚠️ [EdgeX] 没有找到要订阅的交易对")
                return
            
            # 将订阅添加到管理器
            for symbol in symbols:
                self._subscription_manager.add_subscription(
                    symbol=symbol,
                    data_type=DataType.ORDERBOOK,
                    callback=callback
                )
            
            # 委托给websocket模块执行实际订阅
            await self.websocket.batch_subscribe_orderbooks(symbols, depth, callback)
            
            if self.logger:
                self.logger.info(f"✅ [EdgeX] 批量订阅orderbook完成: {len(symbols)}个交易对")
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ [EdgeX] 批量订阅orderbook失败: {str(e)}")
            raise

    async def batch_subscribe_mixed(self, 
                                   symbols: Optional[List[str]] = None,
                                   ticker_callback: Optional[Callable[[str, TickerData], None]] = None,
                                   orderbook_callback: Optional[Callable[[str, OrderBookData], None]] = None,
                                   trades_callback: Optional[Callable[[str, TradeData], None]] = None,
                                   user_data_callback: Optional[Callable[[str, Dict[str, Any]], None]] = None,
                                   depth: int = 15) -> None:
        """批量订阅混合数据类型（支持任意组合）
        
        Args:
            symbols: 要订阅的交易对符号列表（None时使用配置文件中的设置）
            ticker_callback: ticker数据回调函数
            orderbook_callback: orderbook数据回调函数
            trades_callback: trades数据回调函数
            user_data_callback: user_data回调函数
            depth: 订单簿深度
        """
        try:
            # 🚀 使用订阅管理器确定要订阅的交易对
            if symbols is None:
                if self._subscription_manager.mode.value == "predefined":
                    symbols = self._subscription_manager.get_subscription_symbols()
                    if self.logger:
                        self.logger.info(f"🔧 EdgeX硬编码模式：使用配置文件中的 {len(symbols)} 个交易对")
                else:
                    symbols = await self._subscription_manager.discover_symbols(self.get_supported_symbols)
                    if self.logger:
                        self.logger.info(f"🔧 EdgeX动态模式：发现 {len(symbols)} 个交易对")
            
            if not symbols:
                if self.logger:
                    self.logger.warning("⚠️ [EdgeX] 没有找到要订阅的交易对")
                return
            
            # 根据配置决定订阅哪些数据类型
            subscription_count = 0
            
            # 订阅ticker数据
            if (ticker_callback is not None and 
                self._subscription_manager.should_subscribe_data_type(DataType.TICKER)):
                await self.batch_subscribe_tickers(symbols, ticker_callback)
                subscription_count += 1
                if self.logger:
                    self.logger.info(f"✅ [EdgeX] 已订阅ticker数据: {len(symbols)}个交易对")
            
            # 订阅orderbook数据
            if (orderbook_callback is not None and 
                self._subscription_manager.should_subscribe_data_type(DataType.ORDERBOOK)):
                await self.batch_subscribe_orderbooks(symbols, depth, orderbook_callback)
                subscription_count += 1
                if self.logger:
                    self.logger.info(f"✅ [EdgeX] 已订阅orderbook数据: {len(symbols)}个交易对")
            
            # 订阅trades数据
            if (trades_callback is not None and 
                self._subscription_manager.should_subscribe_data_type(DataType.TRADES)):
                for symbol in symbols:
                    await self.subscribe_trades(symbol, trades_callback)
                subscription_count += 1
                if self.logger:
                    self.logger.info(f"✅ [EdgeX] 已订阅trades数据: {len(symbols)}个交易对")
            
            # 订阅user_data数据
            if (user_data_callback is not None and 
                self._subscription_manager.should_subscribe_data_type(DataType.USER_DATA)):
                await self.subscribe_user_data(user_data_callback)
                subscription_count += 1
                if self.logger:
                    self.logger.info(f"✅ [EdgeX] 已订阅user_data数据")
            
            # 获取订阅统计信息
            stats = self._subscription_manager.get_subscription_stats()
            if self.logger:
                self.logger.info(f"🎯 [EdgeX] 混合订阅完成: {subscription_count}种数据类型, {len(symbols)}个交易对")
                self.logger.info(f"📊 [EdgeX] 订阅统计: {stats}")
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ [EdgeX] 批量混合订阅失败: {e}")
            raise

    def get_subscription_manager(self) -> SubscriptionManager:
        """获取订阅管理器实例"""
        return self._subscription_manager

    def get_subscription_stats(self) -> Dict[str, Any]:
        """获取订阅统计信息"""
        return self._subscription_manager.get_subscription_stats()

    async def unsubscribe(self, symbol: Optional[str] = None) -> None:
        """取消订阅"""
        await self.websocket.unsubscribe(symbol)

    async def unsubscribe_all(self) -> None:
        """取消所有订阅"""
        await self.websocket.unsubscribe_all()

    # === 向后兼容的接口 ===

    async def subscribe_order_book(self, symbol: str, callback, depth: int = 20):
        """订阅订单簿数据 - 向后兼容"""
        await self.websocket.subscribe_order_book(symbol, callback, depth)

    async def get_recent_trades(self, symbol: str, limit: int = 500) -> List[TradeData]:
        """获取最近成交记录 - 向后兼容"""
        return await self.rest.get_recent_trades(symbol, limit)

    async def create_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        amount: Decimal,
        price: Optional[Decimal] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> OrderData:
        """创建订单 - 向后兼容"""
        reduce_only = False
        if params:
            reduce_only = params.get('reduceOnly', params.get('reduce_only', False))
        return await self.place_order(
            symbol=symbol,
            side=side,
            order_type=order_type,
            quantity=amount,
            price=price,
            time_in_force=params.get('timeInForce', 'GTC') if params else 'GTC',
            client_order_id=params.get('clientOrderId') if params else None,
            reduce_only=reduce_only
        )

    async def get_order(self, order_id: str, symbol: str) -> OrderData:
        """获取单个订单信息 - 向后兼容"""
        return await self.get_order_status(symbol, order_id)

    async def authenticate(self) -> bool:
        """进行身份认证 - 向后兼容"""
        return await self._do_authenticate()

    async def health_check(self) -> Dict[str, Any]:
        """健康检查 - 向后兼容"""
        return await self._do_health_check()

    async def get_exchange_status(self) -> Dict[str, Any]:
        """获取交易所状态 - 向后兼容"""
        return {
            'status': 'online' if self.connected else 'offline',
            'timestamp': int(time.time() * 1000)
        }

    async def get_symbol_info(self, symbol: str) -> Optional[Dict[str, Any]]:
        """获取交易对信息 - 向后兼容"""
        return self.symbols_info.get(symbol)

    # === 工具方法 ===

    def format_quantity(self, symbol: str, quantity: Decimal) -> Decimal:
        """格式化数量精度"""
        symbol_info = self.symbols_info.get(symbol)
        return self.base.format_quantity(symbol, quantity, symbol_info)

    def format_price(self, symbol: str, price: Decimal) -> Decimal:
        """格式化价格精度"""
        symbol_info = self.symbols_info.get(symbol)
        return self.base.format_price(symbol, price, symbol_info)

    # === 事件处理方法 ===

    async def _handle_ticker_update(self, ticker: TickerData) -> None:
        """处理ticker更新事件"""
        try:
            # 在新架构中简化事件处理
            self.logger.debug(f"Ticker更新: {ticker.symbol}@edgex, 价格: {ticker.last}")
        except Exception as e:
            self.logger.warning(f"处理ticker更新事件失败: {e}")

    async def _handle_orderbook_update(self, orderbook: OrderBookData) -> None:
        """处理orderbook更新事件"""
        try:
            # 在新架构中简化事件处理
            self.logger.debug(f"订单簿更新: {orderbook.symbol}@edgex")
        except Exception as e:
            self.logger.warning(f"处理orderbook更新事件失败: {e}")

    # === 属性代理 ===

    @property
    def api_key(self) -> str:
        """获取API密钥"""
        return self.rest.api_key

    @property
    def api_secret(self) -> str:
        """获取API密钥"""
        return self.rest.api_secret

    @property
    def is_authenticated(self) -> bool:
        """获取认证状态"""
        return self.rest.is_authenticated

    @property
    def symbol_mapping(self) -> Dict[str, str]:
        """获取符号映射"""
        return getattr(self.config, 'symbol_mapping', {})

    def _normalize_symbol(self, symbol: str) -> str:
        """标准化符号 - 向后兼容"""
        return self.base._normalize_symbol(symbol)

    def _map_symbol(self, symbol: str) -> str:
        """映射符号 - 向后兼容"""
        return self.base._map_symbol(symbol)

    def _reverse_map_symbol(self, exchange_symbol: str) -> str:
        """反向映射符号 - 向后兼容"""
        return self.base._reverse_map_symbol(exchange_symbol)

    def _safe_decimal(self, value: Any) -> Decimal:
        """安全转换为Decimal - 向后兼容"""
        return self.base._safe_decimal(value)

    async def batch_subscribe_all_tickers(self, callback: Optional[Callable[[str, TickerData], None]] = None) -> None:
        """订阅所有交易对的ticker数据（使用ticker.all频道）"""
        try:
            self.logger.info("[EdgeX] 开始订阅所有交易对的ticker数据")
            
            # 建立WebSocket连接
            if not self.websocket._ws_connection:
                await self.websocket.connect()
            
            # 订阅所有ticker
            subscribe_msg = {
                "type": "subscribe",
                "channel": "ticker.all"
            }
            
            if self.websocket._ws_connection:
                await self.websocket._ws_connection.send_str(json.dumps(subscribe_msg))
                self.logger.info("已订阅所有交易对的ticker数据")
            
            # 如果提供了回调函数，保存它
            if callback:
                self.websocket.ticker_callback = callback

        except Exception as e:
            self.logger.warning(f"订阅所有ticker时出错: {e}")

    async def _fetch_supported_symbols(self) -> None:
        """通过WebSocket获取支持的交易对 - 向后兼容"""
        await self.websocket.fetch_supported_symbols()

    async def _process_metadata_response(self, data: Dict[str, Any]) -> None:
        """处理metadata响应数据 - 向后兼容"""
        await self.websocket._process_metadata_response(data)

    async def _close_websocket(self) -> None:
        """关闭WebSocket连接 - 向后兼容"""
        await self.websocket.disconnect()

    async def _safe_callback(self, callback: Callable, data: Any) -> None:
        """安全调用回调函数 - 向后兼容"""
        await self.websocket._safe_callback(callback, data)

    async def _request(self, method: str, endpoint: str, params: Optional[Dict] = None, 
                      data: Optional[Dict] = None, signed: bool = False) -> Dict[str, Any]:
        """执行HTTP请求 - 向后兼容"""
        return await self.rest._request(method, endpoint, params, data, signed)

    async def _fetch_ticker(self, symbol: str) -> Dict[str, Any]:
        """获取单个交易对行情数据 - 向后兼容"""
        return await self.rest.fetch_ticker(symbol)

    async def _fetch_orderbook(self, symbol: str, limit: Optional[int] = None) -> Dict[str, Any]:
        """获取订单簿数据 - 向后兼容"""
        return await self.rest.fetch_orderbook(symbol, limit)

    async def _fetch_trades(self, symbol: str, since: Optional[int] = None, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """获取交易记录 - 向后兼容"""
        return await self.rest.fetch_trades(symbol, since, limit)

    async def _fetch_balances(self) -> Dict[str, Any]:
        """获取账户余额数据 - 向后兼容"""
        return await self.rest.fetch_balances()

    # === 数据解析方法 - 向后兼容 ===

    def _parse_ticker(self, data: Dict[str, Any], symbol: str) -> TickerData:
        """解析行情数据 - 向后兼容"""
        return self.base._parse_ticker(data, symbol)

    def _parse_orderbook(self, data: Dict[str, Any], symbol: str) -> OrderBookData:
        """解析订单簿数据 - 向后兼容"""
        return self.base._parse_orderbook(data, symbol)

    def _parse_trade(self, data: Dict[str, Any], symbol: str) -> TradeData:
        """解析交易数据 - 向后兼容"""
        return self.base._parse_trade(data, symbol)

    def _parse_balance(self, data: Dict[str, Any]) -> BalanceData:
        """解析余额数据 - 向后兼容"""
        return self.base._parse_balance(data)

    def _parse_order(self, data: Dict[str, Any]) -> OrderData:
        """解析订单数据 - 向后兼容"""
        return self.base._parse_order(data)

    def _parse_order_status(self, status: str) -> OrderStatus:
        """解析订单状态 - 向后兼容"""
        return self.base._parse_order_status(status)

    def _normalize_contract_symbol(self, symbol: str) -> str:
        """将EdgeX合约symbol转换为标准格式 - 向后兼容"""
        return self.base._normalize_contract_symbol(symbol)

    def _get_auth_headers(self) -> Dict[str, str]:
        """获取认证请求头 - 向后兼容"""
        return self.base.get_auth_headers(self.api_key)

    # === 属性访问 - 向后兼容 ===

    @property
    def _supported_symbols(self) -> List[str]:
        """获取支持的交易对 - 向后兼容"""
        return self.base._supported_symbols

    @property
    def _contract_mappings(self) -> Dict[str, str]:
        """获取合约映射 - 向后兼容"""
        return self.base._contract_mappings

    @property
    def _symbol_contract_mappings(self) -> Dict[str, str]:
        """获取符号到合约映射 - 向后兼容"""
        return self.base._symbol_contract_mappings

    @property
    def _ws_connection(self):
        """获取WebSocket连接 - 向后兼容"""
        # 如果websocket已经初始化，返回其连接
        if hasattr(self, 'websocket') and self.websocket is not None:
            return self.websocket._ws_connection
        # 否则返回临时存储的值或None
        else:
            return getattr(self, '_ws_connection_value', None)

    @_ws_connection.setter  
    def _ws_connection(self, value):
        """设置WebSocket连接 - 向后兼容"""
        # 检查websocket属性是否已经初始化
        if hasattr(self, 'websocket') and self.websocket is not None:
            self.websocket._ws_connection = value
        # 如果websocket还未初始化，直接设置为实例属性
        else:
            object.__setattr__(self, '_ws_connection_value', value)

    @property
    def _ws_subscriptions(self):
        """获取WebSocket订阅 - 向后兼容"""
        return self.websocket._ws_subscriptions

    @property
    def session(self):
        """获取HTTP会话 - 向后兼容"""
        return self.rest.session

    @property
    def ws_connections(self):
        """获取WebSocket连接字典 - 向后兼容"""
        return getattr(self.websocket, 'ws_connections', {})
    
    def _setup_balance_auto_refresh(self):
        """设置WebSocket回调（保留占位，余额直接由WS推送维护）"""
        async def on_trade_event(_: Dict[str, Any]):
            return
        
        # 注册回调到WebSocket
        if not hasattr(self.websocket, '_user_data_callbacks'):
            self.websocket._user_data_callbacks = []
        self.websocket._user_data_callbacks.append(on_trade_event)

    def _get_symbol_cache_service(self):
        """获取符号缓存服务实例"""
        try:
            # 尝试从依赖注入容器获取符号缓存服务
            from ....di.container import get_container
            from ....services.symbol_manager.interfaces.symbol_cache import ISymbolCacheService
            
            container = get_container()
            symbol_cache_service = container.get(ISymbolCacheService)
            
            if self.logger:
                self.logger.debug("[EdgeX] 符号缓存服务: 已获取")
            return symbol_cache_service
            
        except Exception as e:
            if self.logger:
                self.logger.warning(f"⚠️ 获取符号缓存服务失败: {e}，返回None")
            return None
