"""
Paradex REST API 模块

提供REST API接口实现，包括：
- 市场数据获取
- 账户信息查询
- 订单管理
- 持仓查询

使用官方 paradex-py SDK 处理订单签名
"""

import asyncio
import aiohttp
import logging
from typing import Dict, List, Optional, Any
import importlib
from decimal import Decimal
from datetime import datetime

from ..models import (
    TickerData,
    OrderBookData,
    OrderBookLevel,
    TradeData,
    OrderData,
    PositionData,
    BalanceData,
    ExchangeInfo,
    ExchangeType,
    OrderSide,
    OrderType,
    OrderStatus,
    PositionSide,
    MarginMode
)

from .paradex_base import ParadexBase
from ..utils.logger_factory import get_exchange_logger

logger = get_exchange_logger("ExchangeAdapter.paradex")

# 尝试导入 Paradex 官方 SDK（使用 importlib 以避免静态检查报错）
try:
    paradex_module = importlib.import_module("paradex_py")
    env_module = importlib.import_module("paradex_py.environment")
    order_module = importlib.import_module("paradex_py.common.order")

    ParadexSDK = getattr(paradex_module, "Paradex")
    Environment = getattr(env_module, "Environment")
    SDKOrder = getattr(order_module, "Order")
    SDKOrderSide = getattr(order_module, "OrderSide")
    SDKOrderType = getattr(order_module, "OrderType")

    PARADEX_SDK_AVAILABLE = True
    PARADEX_SDK_IMPORT_ERROR = ""
except Exception as import_error:
    PARADEX_SDK_AVAILABLE = False
    PARADEX_SDK_IMPORT_ERROR = str(import_error)
    ParadexSDK = None
    Environment = None
    SDKOrder = None
    SDKOrderSide = None
    SDKOrderType = None


class ParadexRest(ParadexBase):
    """
    Paradex REST API 实现
    
    认证方式：
    - JWT Token: 用于读取操作（通过Authorization: Bearer {JWT}头）
    - Private Key: 用于签名订单操作
    """
    
    def __init__(self, config=None, logger_param=None):
        """
        初始化REST API客户端
        
        Args:
            config: 交易所配置
            logger_param: 日志器（从适配器传入）
        """
        super().__init__(config)
        # 🔥 使用模块级 logger，确保日志写入文件
        # 如果传入了 logger，将其 handlers 添加到模块级 logger
        self.logger = logger
        if logger_param and hasattr(logger_param, 'handlers'):
            for handler in logger_param.handlers:
                if handler not in logger.handlers:
                    logger.addHandler(handler)
        self.session: Optional[aiohttp.ClientSession] = None
        
        # 认证信息
        self.api_key = getattr(config, 'api_key', '') if config else ''
        self.api_secret = getattr(config, 'api_secret', '') if config else ''
        extra_params = getattr(config, 'extra_params', {}) if config else {}
        config_wallet = getattr(config, 'wallet_address', '') if config else ''
        self.jwt_token = extra_params.get('jwt_token', '')
        self.l2_address = extra_params.get('l2_address', '')
        self.wallet_address = (
            extra_params.get('wallet_address')
            or extra_params.get('l1_address')
            or extra_params.get('wallet')
            or config_wallet
            or self.l2_address
        )
        
        # 是否为只读模式（只有JWT Token，没有私钥）
        self.readonly_mode = bool(self.jwt_token) and not bool(self.api_key)
        
        # 初始化 Paradex 官方 SDK 客户端（用于订单签名）
        self._paradex_client = None
        self._paradex_api_client = None
        if PARADEX_SDK_AVAILABLE and config and self.api_key:
            if not self.wallet_address:
                if self.logger:
                    self.logger.warning("未配置 wallet_address（以太坊地址），无法初始化 Paradex SDK 客户端")
            else:
                try:
                    env = "testnet" if getattr(config, 'testnet', False) else "prod"
                    self._paradex_client = ParadexSDK(
                        env=env,
                        l1_address=self.wallet_address,
                        l2_private_key=self.api_key,
                        logger=self.logger,
                    )
                    self._paradex_api_client = self._paradex_client.api_client
                    if self.logger:
                        self.logger.info("✅ Paradex SDK 客户端初始化成功（支持订单签名）")
                except Exception as e:
                    if self.logger:
                        self.logger.warning(f"Paradex SDK 客户端初始化失败: {e}")
                        self.logger.warning("订单创建功能将不可用，但不影响查询功能")
        elif not PARADEX_SDK_AVAILABLE:
            if self.logger:
                self.logger.warning("未能导入 paradex-py SDK，订单创建功能不可用")
                if PARADEX_SDK_IMPORT_ERROR:
                    self.logger.warning(f"导入错误: {PARADEX_SDK_IMPORT_ERROR}")
                self.logger.warning("请运行: pip install paradex-py")
        
    async def connect(self) -> bool:
        """
        建立REST连接
        
        Returns:
            bool: 是否成功
        """
        try:
            if self.session is None:
                # 🔥 SSL 配置：从环境变量读取
                import ssl
                import os
                verify_ssl = os.getenv('PARADEX_VERIFY_SSL', 'true').lower() == 'true'
                
                # 创建 SSL 上下文
                connector = None
                if not verify_ssl:
                    ssl_context = ssl.create_default_context()
                    ssl_context.check_hostname = False
                    ssl_context.verify_mode = ssl.CERT_NONE
                    connector = aiohttp.TCPConnector(ssl=ssl_context)
                
                timeout = aiohttp.ClientTimeout(total=30)
                self.session = aiohttp.ClientSession(timeout=timeout, connector=connector)
                
            # 测试连接
            markets = await self.get_markets()
            
            if self.logger:
                self.logger.info(f"Paradex REST API连接成功，共{len(markets)}个市场")
                
            return True
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"Paradex REST API连接失败: {e}")
            return False
            
    async def disconnect(self) -> None:
        """断开REST连接"""
        if self.session:
            await self.session.close()
            self.session = None
            
    async def authenticate(self) -> bool:
        """
        API认证测试
        
        Returns:
            bool: 认证是否成功
        """
        try:
            # 尝试获取账户信息来测试认证
            if self.jwt_token:
                await self.get_account()
                if self.logger:
                    self.logger.info("Paradex JWT Token认证成功")
                return True
            else:
                if self.logger:
                    self.logger.warning("Paradex未配置JWT Token，仅支持公共API")
                return True  # 公共API不需要认证
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"Paradex认证失败: {e}")
            return False
            
    async def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict] = None,
        data: Optional[Dict] = None,
        auth_required: bool = False
    ) -> Any:
        """
        执行HTTP请求
        
        Args:
            method: HTTP方法
            endpoint: API端点
            params: URL查询参数
            data: 请求体数据
            auth_required: 是否需要认证
            
        Returns:
            响应数据
        """
        if not self.session:
            raise Exception("REST会话未初始化")
            
        url = f"{self.get_base_url()}{endpoint}"
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        
        # 添加认证头
        if auth_required and self.jwt_token:
            headers['Authorization'] = f'Bearer {self.jwt_token}'
            
        try:
            async with self.session.request(
                method=method,
                url=url,
                params=params,
                json=data,
                headers=headers
            ) as response:
                
                # 处理204 No Content（取消订单）
                if response.status == 204:
                    return {}
                    
                # 处理其他状态码
                if response.status not in [200, 201]:
                    error_text = await response.text()
                    raise Exception(f"API请求失败 [{response.status}]: {error_text}")
                    
                return await response.json()
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"REST请求失败 {method} {endpoint}: {e}")
            raise
            
    # ===== 市场数据接口 =====
    
    async def get_markets(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        获取市场列表
        
        Args:
            symbol: 可选的市场符号过滤
            
        Returns:
            List[Dict]: 市场信息列表
        """
        params = {}
        if symbol:
            # 转换为Paradex格式
            paradex_symbol = self.convert_to_paradex_symbol(symbol)
            params['market'] = paradex_symbol
            
        response = await self._request('GET', '/markets', params=params)
        return response.get('results', [])
        
    async def get_exchange_info(self) -> ExchangeInfo:
        """
        获取交易所信息
        
        Returns:
            ExchangeInfo: 交易所信息
        """
        markets = await self.get_markets()
        
        normalized_markets: Dict[str, Any] = {}
        for market in markets:
            symbol = self.normalize_symbol(market.get('symbol', ''))
            if not symbol:
                continue
            normalized_markets[symbol] = market
        
        # 更新基础类中的可用交易对缓存
        self._supported_symbols = list(normalized_markets.keys())
        
        return ExchangeInfo(
            id='paradex',
            name='Paradex',
            type=ExchangeType.PERPETUAL,
            supported_features=['perpetual', 'websocket', 'trading'],
            rate_limits={},
            precision={},
            fees={},
            markets=normalized_markets,
            status='online',
            timestamp=datetime.now()
        )
        
    async def get_ticker(self, symbol: str) -> TickerData:
        """
        获取单个交易对行情（通过markets接口）
        
        注意：Paradex没有单独的ticker接口，建议使用WebSocket
        
        Args:
            symbol: 交易对符号（标准格式）
            
        Returns:
            TickerData: 行情数据
        """
        # Paradex建议使用WebSocket获取实时行情
        # 这里通过markets接口获取静态数据
        markets = await self.get_markets(symbol)
        
        if not markets:
            raise Exception(f"未找到市场: {symbol}")
            
        market = markets[0]
        return self._build_market_ticker(symbol, market)
        
    async def get_tickers(self, symbols: Optional[List[str]] = None) -> List[TickerData]:
        """
        获取多个交易对行情
        
        Args:
            symbols: 交易对列表，None表示获取所有
            
        Returns:
            List[TickerData]: 行情数据列表
        """
        markets = await self.get_markets()
        
        tickers = []
        for market in markets:
            symbol = self.normalize_symbol(market['symbol'])
            
            # 如果指定了symbols，则过滤
            if symbols and symbol not in symbols:
                continue
                
            tickers.append(self._build_market_ticker(symbol, market))
            
        return tickers

    def _build_market_ticker(self, symbol: str, market: Dict[str, Any]) -> TickerData:
        """将 /markets 元数据转换为 TickerData（含资金费率信息）"""

        def _pick(*keys: str) -> Any:
            for key in keys:
                if key in market and market[key] not in (None, ''):
                    return market[key]
            return None

        last_price = self._safe_decimal(
            _pick('last_traded_price', 'lastPrice', 'mark_price', 'markPrice'),
            default=None
        )
        mark_price = self._safe_decimal(_pick('mark_price', 'markPrice'), default=None)
        index_price = self._safe_decimal(
            _pick('index_price', 'indexPrice', 'underlying_price', 'underlyingPrice'),
            default=None
        )
        funding_rate = self._safe_decimal(_pick('funding_rate', 'fundingRate'), default=None)
        predicted_funding_rate = self._safe_decimal(
            _pick('future_funding_rate', 'predicted_funding_rate'),
            default=None
        )
        funding_period_hours = _pick('funding_period_hours')
        funding_interval = None
        if funding_period_hours is not None:
            try:
                funding_interval = int(float(funding_period_hours) * 3600)
            except (ValueError, TypeError):
                funding_interval = None

        ticker = TickerData(
            symbol=symbol,
            timestamp=datetime.now(),
            last=last_price,
            bid=None,
            ask=None,
            volume=self._safe_decimal(_pick('volume_24h', 'total_volume'), default=None),
            quote_volume=self._safe_decimal(_pick('quote_volume_24h'), default=None),
            change=self._safe_decimal(_pick('price_change_24h'), default=None),
            percentage=self._safe_decimal(_pick('price_change_rate_24h'), default=None),
            funding_rate=funding_rate,
            predicted_funding_rate=predicted_funding_rate,
            funding_interval=funding_interval,
            index_price=index_price,
            mark_price=mark_price,
            open_interest=self._safe_decimal(_pick('open_interest'), default=None),
            contract_id=market.get('id'),
            contract_name=market.get('symbol'),
            base_currency=market.get('base_currency'),
            quote_currency=_pick('quote_currency', 'settlement_currency'),
            contract_size=self._safe_decimal(_pick('contract_size', 'position_limit'), default=None),
            tick_size=self._safe_decimal(_pick('price_tick_size'), default=None),
            lot_size=self._safe_decimal(_pick('order_size_increment'), default=None),
            raw_data=market
        )

        return ticker
        
    async def get_orderbook(self, symbol: str, limit: int = 20) -> OrderBookData:
        """
        获取订单簿
        
        注意：Paradex建议使用WebSocket订阅订单簿
        这里返回空数据，实际数据应通过WebSocket获取
        
        Args:
            symbol: 交易对符号
            limit: 深度限制
            
        Returns:
            OrderBookData: 订单簿数据
        """
        # Paradex没有REST订单簿接口，需要使用WebSocket
        if self.logger:
            self.logger.warning(f"Paradex建议使用WebSocket订阅订单簿，REST接口不提供订单簿数据")
            
        return OrderBookData(
            symbol=symbol,
            timestamp=datetime.now(),
            bids=[],
            asks=[],
            nonce=None,
            raw_data={}
        )
        
    # ===== 账户接口 =====
    
    async def get_account(self) -> Dict[str, Any]:
        """
        获取账户信息
        
        Returns:
            Dict: 账户信息
        """
        return await self._request('GET', '/account', auth_required=True)
        
    async def get_balances(self) -> List[BalanceData]:
        """
        获取账户余额（包含未实现盈亏，返回账户权益）
        
        使用 /account API 的 account_value 字段（账户权益）
        
        Returns:
            List[BalanceData]: 余额列表（total 字段 = account_value）
        """
        # 1. 获取账户信息（包含 account_value）
        account_info = await self.get_account()
        account_value = self._safe_decimal(account_info.get('account_value', '0'))
        
        # 2. 获取各币种余额详情
        response = await self._request('GET', '/balance', auth_required=True)
        results = response.get('results', [])
        
        balances = []
        for item in results:
            currency = item.get('token', '')
            raw_balance = self._safe_decimal(item.get('size', '0'))
            
            # 🔥 对于 USDC（主要结算货币），使用 account_value 作为总额
            # account_value 已经包含了未实现盈亏
            if currency == 'USDC':
                total = account_value  # 使用账户权益
                self.logger.debug(
                    f"💰 账户权益: account_value={float(account_value):.2f} USDC "
                    f"(余额={float(raw_balance):.2f})"
                )
            else:
                total = raw_balance
            
            balance = BalanceData(
                currency=currency,
                free=total,  # 可用余额 = 账户权益（Paradex不区分冻结/可用）
                used=Decimal('0'),  # Paradex不区分冻结/可用
                total=total,  # 🔥 总额 = account_value（已包含未实现盈亏）
                usd_value=total if currency == 'USDC' else None,
                timestamp=self._timestamp_to_datetime(item.get('last_updated_at')),
                raw_data={
                    **item,
                    '_account_value': str(account_value) if currency == 'USDC' else '0'
                }
            )
            balances.append(balance)
            
        return balances
        
    async def get_positions(self, symbols: Optional[List[str]] = None) -> List[PositionData]:
        """
        获取持仓信息
        
        Args:
            symbols: 可选的交易对过滤
            
        Returns:
            List[PositionData]: 持仓列表
        """
        response = await self._request('GET', '/positions', auth_required=True)
        results = response.get('results', [])
        
        positions = []
        for item in results:
            symbol = self.normalize_symbol(item.get('market', ''))
            
            # 过滤符号
            if symbols and symbol not in symbols:
                continue
                
            # 解析持仓方向
            side_str = item.get('side', 'LONG').upper()
            side = PositionSide.LONG if side_str == 'LONG' else PositionSide.SHORT
            
            position = PositionData(
                symbol=symbol,
                side=side,
                size=self._safe_decimal(item.get('size', '0')),
                entry_price=self._safe_decimal(item.get('average_entry_price', '0')),
                mark_price=Decimal('0'),  # Paradex在positions中不提供
                current_price=None,
                unrealized_pnl=self._safe_decimal(item.get('unrealized_pnl', '0')),
                realized_pnl=self._safe_decimal(item.get('realized_positional_pnl', '0')),
                percentage=None,
                leverage=int(self._safe_decimal(item.get('leverage', '1'))),
                margin_mode=MarginMode.CROSS,  # Paradex默认全仓
                margin=self._safe_decimal(item.get('margin', '0')),
                liquidation_price=self._safe_decimal(item.get('liquidation_price')),
                timestamp=self._timestamp_to_datetime(item.get('last_updated_at')),
                raw_data=item
            )
            positions.append(position)
            
        return positions
        
    # ===== 订单管理接口 =====
    
    async def create_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        amount: Decimal,
        price: Optional[Decimal] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> OrderData:
        """
        创建订单（使用官方 SDK 自动签名）
        
        Args:
            symbol: 交易对符号（标准格式，如 BTC/USDC:PERP）
            side: 订单方向
            order_type: 订单类型
            amount: 数量
            price: 价格（限价单必需）
            params: 额外参数
            
        Returns:
            OrderData: 订单数据
        """
        # 检查是否为只读模式
        if self.readonly_mode:
            raise Exception("只读模式不支持创建订单，需要配置 L2 私钥")
            
        # 检查是否有 SDK 客户端
        if not self._paradex_api_client:
            raise Exception(
                "订单创建功能不可用。请确保：\n"
                "1. 已安装 paradex-py SDK: pip install paradex-py\n"
                "2. 已配置 api_key (L2 私钥) 和钱包地址\n"
                "3. 查看配置文件: config/exchanges/paradex_config.yaml"
            )
            
        try:
            # 转换符号格式（标准格式 -> Paradex格式）
            paradex_symbol = self.convert_to_paradex_symbol(symbol)
            
            sdk_order = self._build_sdk_order(
                market=paradex_symbol,
                side=side,
                order_type=order_type,
                amount=amount,
                price=price,
                params=params or {},
            )
            
            if self.logger:
                self.logger.info(f"创建 Paradex 订单: {paradex_symbol} {side.value} {amount} @ {price}")
                
            response = await asyncio.to_thread(
                self._paradex_api_client.submit_order,
                sdk_order
            )
            
            if self.logger:
                self.logger.info(f"✅ 订单创建成功: {response.get('id', 'unknown')}")
                
            # 解析并返回订单数据
            return self._parse_order(response)
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"创建订单失败: {e}")
            raise Exception(f"Paradex 订单创建失败: {str(e)}")
        
    async def cancel_order(self, order_id: str, symbol: str) -> OrderData:
        """
        取消订单
        
        Args:
            order_id: 订单ID
            symbol: 交易对符号
            
        Returns:
            OrderData: 订单数据
        """
        if not self._paradex_api_client:
            raise Exception("未初始化 paradex SDK 客户端，无法取消订单")
        
        try:
            await asyncio.to_thread(
                self._paradex_api_client.cancel_order,
                order_id
            )
            if self.logger:
                self.logger.info(f"✅ 订单取消成功: {order_id}")
            return self._create_cancelled_order(order_id, symbol)
        except Exception as e:
            if self.logger:
                self.logger.error(f"取消订单失败 {order_id}: {e}")
            raise
            
    def _create_cancelled_order(self, order_id: str, symbol: str) -> OrderData:
        """创建一个已取消状态的订单对象（用于204响应）"""
        return OrderData(
            id=str(order_id),
            client_id=None,
            symbol=symbol,
            side=OrderSide.BUY,  # 占位
            type=OrderType.LIMIT,  # 占位
            amount=Decimal('0'),
            price=Decimal('0'),
            filled=Decimal('0'),
            remaining=Decimal('0'),
            cost=Decimal('0'),
            average=None,
            status=OrderStatus.CANCELED,
            timestamp=datetime.now(),
            updated=datetime.now(),
            fee=None,
            trades=[],
            params={},
            raw_data={}
        )
        
    async def cancel_all_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        """
        取消所有订单
        
        Args:
            symbol: 可选的交易对过滤
            
        Returns:
            List[OrderData]: 被取消的订单列表
        """
        if not self._paradex_api_client:
            raise Exception("未初始化 paradex SDK 客户端，无法批量取消订单")
        
        try:
            params = {}
            if symbol:
                params['market'] = self.convert_to_paradex_symbol(symbol)
            
            await asyncio.to_thread(
                self._paradex_api_client.cancel_all_orders,
                params if params else None
            )
            
            if self.logger:
                self.logger.info("✅ 批量取消订单成功")
            
            return []
        except Exception as e:
            if self.logger:
                self.logger.error(f"批量取消订单失败: {e}")
            raise
        
    async def get_order(self, order_id: str, symbol: str) -> OrderData:
        """
        获取订单信息
        
        Args:
            order_id: 订单ID
            symbol: 交易对符号
            
        Returns:
            OrderData: 订单数据
        """
        if self.logger:
            self.logger.info(f"[Paradex] 查询订单状态: order_id={order_id}")
        
        response = await self._request('GET', f'/orders/{order_id}', auth_required=True)
        order_data = self._parse_order(response)
        
        if self.logger:
            # 构建日志信息，包含成交价格
            log_msg = (
                f"✅ [Paradex] 订单查询结果: order_id={order_id}, "
                f"status={order_data.status.value}, filled={order_data.filled}/{order_data.amount}"
            )
            
            # 如果订单已成交，显示平均成交价
            if order_data.average and order_data.filled > 0:
                log_msg += f", 成交价={order_data.average}"
            
            # 如果是限价单且有委托价，也显示委托价
            if order_data.price and order_data.type != OrderType.MARKET:
                log_msg += f", 委托价={order_data.price}"
            
            self.logger.info(log_msg)
        
        return order_data
        
    async def get_open_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        """
        获取开放订单
        
        Args:
            symbol: 可选的交易对过滤
            
        Returns:
            List[OrderData]: 开放订单列表
        """
        params = {}
        if symbol:
            paradex_symbol = self.convert_to_paradex_symbol(symbol)
            params['market'] = paradex_symbol
            
        response = await self._request('GET', '/orders', params=params, auth_required=True)
        results = response.get('results', [])
        
        return [self._parse_order(order) for order in results]
        
    # ===== 辅助方法 =====
    
    def _parse_order_status(self, status_str: str) -> OrderStatus:
        """解析订单状态"""
        mapping = {
            'NEW': OrderStatus.PENDING,
            'OPEN': OrderStatus.OPEN,
            'CLOSED': OrderStatus.FILLED,
            'UNTRIGGERED': OrderStatus.PENDING,
            'CANCELLED': OrderStatus.CANCELED,
            'CANCELED': OrderStatus.CANCELED,
        }
        return mapping.get(status_str, OrderStatus.UNKNOWN)
        
    def _parse_order(self, data: Dict[str, Any]) -> OrderData:
        """解析订单数据"""
        symbol = self.normalize_symbol(data.get('market', ''))
        side_str = data.get('side', 'BUY')
        type_str = data.get('type', 'LIMIT')
        status_str = data.get('status', 'OPEN')
        
        amount = self._safe_decimal(data.get('size', '0'))
        remaining = self._safe_decimal(data.get('remaining_size', '0'))
        filled = amount - remaining
        
        price_value = data.get('price')
        price = self._safe_decimal(price_value) if price_value is not None else None
        
        avg_price_value = data.get('avg_fill_price')
        average_price = self._safe_decimal(avg_price_value) if avg_price_value is not None else None
        
        quote_amount = data.get('quote_amount')
        if quote_amount is not None:
            cost = self._safe_decimal(quote_amount)
        elif price is not None:
            cost = filled * price
        else:
            cost = Decimal('0')
        
        params = {
            'reduce_only': data.get('reduce_only'),
            'post_only': data.get('post_only'),
            'trigger_price': data.get('trigger_price'),
            'instruction': data.get('instruction')
        }
        
        return OrderData(
            id=str(data.get('id', '')),
            client_id=data.get('client_id'),
            symbol=symbol,
            side=OrderSide.BUY if side_str == 'BUY' else OrderSide.SELL,
            type=self._map_order_type_reverse(type_str),
            amount=amount,
            price=price,
            filled=filled,
            remaining=remaining,
            cost=cost,
            average=average_price,
            status=self._parse_order_status(status_str),
            timestamp=self._timestamp_to_datetime(data.get('created_at')),
            updated=self._timestamp_to_datetime(data.get('last_updated_at')),
            fee=data.get('fee'),
            trades=data.get('fills', []) or data.get('trades', []),
            params={k: v for k, v in params.items() if v is not None},
            raw_data=data
        )
        
    def _map_order_type_reverse(self, type_str: str) -> OrderType:
        """
        反向映射订单类型（Paradex 格式 -> 系统格式）
        """
        mapping = {
            'LIMIT': OrderType.LIMIT,
            'MARKET': OrderType.MARKET,
            'STOP_LIMIT': OrderType.STOP_LIMIT,
            'STOP_MARKET': OrderType.STOP_MARKET,
            'STOP_LOSS_LIMIT': OrderType.STOP_LOSS_LIMIT,
            'STOP_LOSS_MARKET': OrderType.STOP_LOSS,
            'TAKE_PROFIT': OrderType.TAKE_PROFIT_LIMIT,
            'TAKE_PROFIT_MARKET': OrderType.TAKE_PROFIT,
        }
        return mapping.get(type_str, OrderType.LIMIT)

    def _map_sdk_order_type(self, order_type: OrderType) -> SDKOrderType:
        mapping = {
            OrderType.LIMIT: SDKOrderType.Limit,
            OrderType.MARKET: SDKOrderType.Market,
            OrderType.STOP_LIMIT: SDKOrderType.StopLimit,
            OrderType.STOP_MARKET: SDKOrderType.StopMarket,
            OrderType.STOP_LOSS: SDKOrderType.StopMarket,
            OrderType.STOP_LOSS_LIMIT: SDKOrderType.StopLimit,
            OrderType.TAKE_PROFIT: SDKOrderType.TakeProfitMarket,
            OrderType.TAKE_PROFIT_LIMIT: SDKOrderType.TakeProfitLimit,
        }
        result = mapping.get(order_type)
        if result is None:
            raise ValueError(f"不支持的订单类型: {order_type}")
        return result

    def _map_sdk_order_side(self, side: OrderSide) -> SDKOrderSide:
        return SDKOrderSide.Buy if side == OrderSide.BUY else SDKOrderSide.Sell

    def _build_sdk_order(
        self,
        market: str,
        side: OrderSide,
        order_type: OrderType,
        amount: Decimal,
        price: Optional[Decimal],
        params: Dict[str, Any],
    ) -> SDKOrder:
        if order_type == OrderType.LIMIT and price is None:
            raise ValueError("限价单必须指定价格")
        
        instruction = params.get('instruction', 'GTC')
        reduce_only = bool(params.get('reduce_only', False))
        trigger_price = params.get('trigger_price')
        trigger_decimal = None
        if trigger_price is not None:
            trigger_decimal = self._safe_decimal(trigger_price)
        recv_window = params.get('recv_window')
        stp = params.get('stp')
        client_id = params.get('client_id', '')
        
        return SDKOrder(
            market=market,
            order_type=self._map_sdk_order_type(order_type),
            order_side=self._map_sdk_order_side(side),
            size=amount,
            limit_price=price if price is not None else Decimal('0'),
            client_id=client_id,
            instruction=instruction.upper(),
            reduce_only=reduce_only,
            recv_window=recv_window,
            stp=stp,
            trigger_price=trigger_decimal,
        )

