"""
Backpack REST API模块

包含HTTP请求处理、ED25519签名认证、私有API操作等功能
"""

import asyncio
import aiohttp
import time
import json
import base64
import hashlib
from typing import Dict, List, Optional, Any
from decimal import Decimal
from datetime import datetime

try:
    import nacl.signing  # type: ignore
    _PY_NACL_AVAILABLE = True
except ImportError:  # pragma: no cover
    nacl = None  # type: ignore
    _PY_NACL_AVAILABLE = False

from .backpack_base import BackpackBase, BackpackSymbolInfo
from ..models import (
    BalanceData, OrderData, OrderSide, OrderType, OrderStatus,
    TickerData, OrderBookData, OrderBookLevel, TradeData, PositionData, PositionSide,
    MarginMode, ExchangeInfo, ExchangeType, OHLCVData
)


class BackpackRest(BackpackBase):
    """Backpack REST API接口"""

    def __init__(self, config=None, logger=None):
        super().__init__(config)
        self.logger = logger
        self.session = None

        # API认证信息
        self.api_key = getattr(config, 'api_key', '') if config else ''
        # 🔥 Backpack使用ED25519签名，需要private_key（兼容api_secret）
        self.api_secret = getattr(config, 'api_secret', '') if config else ''
        private_key = getattr(config, 'private_key', '') if config else ''
        # 🔥 优先使用private_key，如果没有则使用api_secret
        if not self.api_secret and private_key:
            self.api_secret = private_key
        self.is_authenticated = bool(self.api_key and self.api_secret)
        
        if self.logger:
            if self.is_authenticated:
                self.logger.debug(f"✅ [Backpack REST] 认证信息已配置 (api_key={bool(self.api_key)}, private_key={bool(self.api_secret)})")
            else:
                self.logger.warning(f"⚠️  [Backpack REST] 认证信息未配置 (api_key={bool(self.api_key)}, private_key={bool(self.api_secret)})")

        # 精度缓存：symbol -> (price_precision, qty_precision)
        self._precision_cache: Dict[str, tuple[int, int]] = {}

        # 余额缓存
        self._balance_cache: List[BalanceData] = []
        self._balance_cache_time: float = 0.0
        self._balance_cache_ttl: float = 30.0  # 默认缓存30秒，可通过配置调整

        self._signing_available = _PY_NACL_AVAILABLE
        self._signing_warning_emitted = False
        if not self._signing_available and self.logger:
            self.logger.warning(
                "⚠️  [Backpack REST] 未检测到 PyNaCl，私有API（余额/下单）将被禁用。"
                "如需启用，请执行: pip install PyNaCl"
            )

    # === 连接管理 ===

    async def connect(self) -> bool:
        """连接到Backpack REST API"""
        try:
            # 创建HTTP session
            self.session = aiohttp.ClientSession()

            # 测试API连接并获取市场数据（一次性完成）
            if self.logger:
                self.logger.info("测试Backpack API连接并获取市场数据...")

            async with self.session.get(f"{self.base_url}api/v1/markets", timeout=10) as response:
                if response.status == 200:
                    if self.logger:
                        self.logger.info("Backpack API连接成功")

                    # 解析响应数据并直接处理
                    try:
                        markets_data = await response.json()
                        if self.logger:
                            self.logger.info(f"获取到 {len(markets_data)} 个市场数据")

                        # 直接处理市场数据，避免重复API调用
                        supported_symbols = []
                        market_info = {}

                        # 统计数据
                        total_markets = len(markets_data)
                        perpetual_count = 0
                        spot_count = 0

                        for market in markets_data:
                            symbol = market.get("symbol")
                            if symbol:
                                # 🔥 修改：只获取永续合约，排除现货
                                if symbol.endswith('_PERP'):
                                    # 永续合约
                                    normalized_symbol = self._normalize_backpack_symbol(
                                        symbol)
                                    supported_symbols.append(normalized_symbol)
                                    market_info[normalized_symbol] = market
                                    perpetual_count += 1
                                else:
                                    # 现货交易对 - 跳过
                                    spot_count += 1

                        # 更新内部状态
                        self._supported_symbols = supported_symbols
                        self._market_info = market_info

                        if self.logger:
                            self.logger.info(f"✅ Backpack连接成功，市场数据统计:")
                            self.logger.info(f"  - 总市场数量: {total_markets}")
                            self.logger.info(f"  - 永续合约: {perpetual_count}")
                            self.logger.info(f"  - 现货交易对: {spot_count} (已跳过)")
                            self.logger.info(
                                f"  - 最终可用: {len(supported_symbols)} 个永续合约")

                        if len(supported_symbols) > 0:
                            return True
                        else:
                            if self.logger:
                                self.logger.error("未找到任何交易对")
                            return False

                    except Exception as parse_e:
                        if self.logger:
                            self.logger.error(f"解析市场数据失败: {parse_e}")
                        return False
                else:
                    error_text = await response.text()
                    if self.logger:
                        self.logger.error(
                            f"API连接失败，状态码: {response.status}, 响应: {error_text[:200]}")
                    return False

        except Exception as e:
            if self.logger:
                if "timeout" in str(e).lower():
                    self.logger.error("Backpack API连接超时")
                else:
                    self.logger.error(f"Backpack连接异常: {type(e).__name__}: {e}")
            return False

    async def disconnect(self) -> None:
        """断开REST API连接"""
        try:
            if self.session and not self.session.closed:
                await self.session.close()
                self.session = None
                if self.logger:
                    self.logger.info("Backpack REST会话已关闭")
        except Exception as e:
            if self.logger:
                self.logger.warning(f"关闭Backpack REST会话时出错: {e}")

    async def authenticate(self) -> bool:
        """执行认证验证"""
        try:
            if not self.is_authenticated:
                if self.logger:
                    self.logger.warning("Backpack API密钥未配置")
                return False

            # 测试需要认证的API调用
            await self._make_authenticated_request("GET", "/api/v1/capital")
            if self.logger:
                self.logger.info("Backpack API认证成功")
            return True
        except Exception as e:
            if self.logger:
                self.logger.warning(f"Backpack API认证失败: {e}")
            return False

    async def health_check(self) -> Dict[str, Any]:
        """健康检查"""
        try:
            async with self.session.get(f"{self.base_url}api/v1/markets", timeout=5) as response:
                if response.status == 200:
                    return {
                        "status": "healthy",
                        "api_accessible": True,
                        "timestamp": datetime.now()
                    }
                else:
                    return {
                        "status": "unhealthy",
                        "api_accessible": False,
                        "error": f"HTTP {response.status}",
                        "timestamp": datetime.now()
                    }
        except Exception as e:
            return {
                "status": "error",
                "api_accessible": False,
                "error": str(e),
                "timestamp": datetime.now()
            }

    async def heartbeat(self) -> None:
        """心跳检查"""
        if self.session:
            try:
                await self.session.get(f"{self.base_url}api/v1/markets", timeout=5)
            except Exception as e:
                if self.logger:
                    self.logger.warning(f"心跳检查失败: {e}")

    # === 认证请求 ===

    def _determine_instruction_type(self, method: str, endpoint: str) -> str:
        """
        根据请求方法和端点确定指令类型，用于生成签名
        """
        upper_method = method.upper()

        # 规范化端点，确保以/开头
        if not endpoint.startswith('/'):
            endpoint = '/' + endpoint
        if endpoint.endswith('/') and len(endpoint) > 1:
            endpoint = endpoint[:-1]

        # 账户查询
        if endpoint == '/api/v1/account':
            if upper_method == 'GET':
                return 'accountQuery'

        # 余额查询
        elif endpoint == '/api/v1/capital':
            if upper_method == 'GET':
                return 'balanceQuery'

        # 保证金查询
        elif endpoint == '/api/v1/capital/collateral':
            if upper_method == 'GET':
                return 'collateralQuery'

        # 持仓查询
        elif endpoint == '/api/v1/position':
            if upper_method == 'GET':
                return 'positionQuery'

        # 订单相关端点
        elif endpoint == '/api/v1/orders':
            if upper_method == 'GET':
                return 'orderQueryAll'
            elif upper_method == 'DELETE':
                return 'orderCancelAll'

        elif endpoint == '/api/v1/order':
            if upper_method == 'POST':
                return 'orderExecute'
            elif upper_method == 'DELETE':
                return 'orderCancel'
            elif upper_method == 'GET':
                return 'orderQuery'

        # 行情查询
        elif endpoint == '/api/v1/ticker':
            return 'marketdataQuery'

        # 历史查询（History API）
        elif endpoint == '/wapi/v1/history/fills':
            if upper_method == 'GET':
                return 'fillHistoryQueryAll'
        elif endpoint == '/wapi/v1/history/orders':
            if upper_method == 'GET':
                return 'orderHistoryQueryAll'

        # 未知端点使用默认生成的指令类型
        if self.logger:
            self.logger.warning(f"未知的API端点: {method} {endpoint}，使用默认指令类型")
        return f"{upper_method.lower()}{endpoint.replace('/', '_')}"

    def _generate_signature(self, method: str, endpoint: str, params: Dict = None, data: Dict = None) -> Dict:
        """
        为API请求生成必要的头部和签名，基于参考脚本实现
        """
        if not self.api_key or not self.api_secret:
            if self.logger:
                self.logger.warning("API密钥未设置，无法生成签名")
                self.logger.warning(
                    f"api_key长度: {len(self.api_key) if self.api_key else 0}")
                self.logger.warning(
                    f"api_secret长度: {len(self.api_secret) if self.api_secret else 0}")
            return {}

        if not self._signing_available:
            raise RuntimeError("PyNaCl 未安装，无法生成Backpack签名")

        # 获取指令类型
        instruction_type = self._determine_instruction_type(method, endpoint)

        # 当前时间戳，毫秒
        timestamp = int(time.time() * 1000)
        window = 5000

        # 构建签名字符串，从指令类型开始
        signature_str = f"instruction={instruction_type}"

        # 添加查询参数 - 按字母顺序排序
        if params and len(params) > 0:
            filtered_params = {k: v for k,
                               v in params.items() if v is not None}
            sorted_keys = sorted(filtered_params.keys())
            for key in sorted_keys:
                signature_str += f"&{key}={filtered_params[key]}"

        # 处理请求体数据
        if data and len(data) > 0:
            filtered_data = {k: v for k, v in data.items() if v is not None}
            def _normalize_value(val):
                if isinstance(val, bool):
                    return str(val).lower()
                return val
            sorted_keys = sorted(filtered_data.keys())
            for key in sorted_keys:
                signature_str += f"&{key}={_normalize_value(filtered_data[key])}"

        # 添加时间戳和窗口
        signature_str += f"&timestamp={timestamp}&window={window}"

        if self.logger:
            self.logger.debug(f"签名字符串: {signature_str}")

        # 使用私钥进行签名
        # 确保私钥是正确的base64格式，如果不是则直接返回错误
        try:
            private_key_bytes = base64.b64decode(self.api_secret)
        except Exception as e:
            raise ValueError(f"私钥必须是有效的base64格式: {e}")

        # 确保私钥长度是32字节，如果不是则使用SHA256处理
        if len(private_key_bytes) != 32:
            private_key_bytes = hashlib.sha256(private_key_bytes).digest()

        # 使用ED25519算法签名
        signing_key = nacl.signing.SigningKey(private_key_bytes)  # type: ignore[attr-defined]
        message_bytes = signature_str.encode('utf-8')
        signature_bytes = signing_key.sign(message_bytes).signature

        # Base64编码签名
        signature_base64 = base64.b64encode(signature_bytes).decode('utf-8')

        # 构建头部
        headers = {
            'X-API-KEY': self.api_key,
            'X-SIGNATURE': signature_base64,
            'X-TIMESTAMP': str(timestamp),
            'X-WINDOW': str(window),
            'Content-Type': 'application/json'
        }

        return headers

    async def _make_authenticated_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict] = None,
        data: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """发起需要认证的API请求，使用ED25519签名"""
        if not self.is_authenticated:
            raise RuntimeError("Exchange not authenticated")

        # 生成签名头部
        headers = self._generate_signature(method, endpoint, params, data)
        if not headers:
            raise RuntimeError("签名生成失败")

        # 发送请求
        url = f"{self.base_url.rstrip('/')}{endpoint}"

        async with self.session.request(
            method=method.upper(),
            url=url,
            params=params if method.upper() == 'GET' else None,
            # 修复：DELETE也需要传递JSON数据
            json=data if method.upper() in ['POST', 'PUT', 'DELETE'] else None,
            headers=headers,
            timeout=30
        ) as response:
            if response.status == 200:
                # Backpack API 有时返回纯文本字符串（如订单状态）
                content_type = response.headers.get('Content-Type', '')

                if 'application/json' in content_type:
                    # 标准JSON响应
                    return await response.json()
                else:
                    # 可能是纯文本响应（如 "New", "PartiallyFilled"）
                    text_response = await response.text()

                    # 尝试解析为JSON
                    try:
                        import json as json_lib
                        return json_lib.loads(text_response)
                    except (ValueError, json_lib.JSONDecodeError):
                        # 纯字符串响应，直接返回
                        if self.logger:
                            self.logger.info(f"API返回纯文本响应: {text_response}")
                        return text_response
            else:
                error_text = await response.text()
                if self.logger:
                    self.logger.warning(
                        f"API请求失败 {response.status}: {error_text}")
                raise RuntimeError(
                    f"API request failed: {response.status} - {error_text}")

    # === 市场数据接口 ===

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
                    parts = symbol.split('_')
                    if len(parts) >= 2:
                        base = parts[0]
                        quote = '_'.join(parts[1:])  # 处理类似 USDC_PERP 的情况
                    else:
                        base = symbol
                        quote = 'USDC'
                else:
                    # 回退处理
                    if symbol.endswith('PERP'):
                        base = symbol[:-4]
                        quote = 'USDC'
                    else:
                        base = symbol
                        quote = 'USDC'

                # 尝试从API获取该交易对的精度信息
                price_precision, amount_precision = await self._get_symbol_precision_from_api(symbol)

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
                        'amount': amount_precision,
                        'price': price_precision,
                        'cost': price_precision,
                        'base': amount_precision,
                        'quote': price_precision
                    },
                    'limits': {
                        'amount': {'min': 0.001, 'max': 1000000},
                        'price': {'min': 0.01, 'max': 1000000},
                        'cost': {'min': 10, 'max': 10000000},
                        'leverage': {'min': 1, 'max': 100}
                    },
                    'info': {
                        'symbol': symbol,
                        'exchange': 'backpack',
                        'type': 'perpetual'
                    }
                }

            if self.logger:
                self.logger.info(f"✅ Backpack交易所信息: {len(markets)}个市场")

            return ExchangeInfo(
                name="Backpack",
                id="backpack",
                type=ExchangeType.PERPETUAL,
                supported_features=["trading", "orderbook", "ticker"],
                rate_limits=getattr(self.config, 'rate_limits',
                                    {}) if self.config else {},
                precision=getattr(self.config, 'precision',
                                  {}) if self.config else {},
                fees={},
                markets=markets,
                status="active",
                timestamp=datetime.now()
            )

        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 获取Backpack交易所信息失败: {e}")
            # 返回空markets的基本信息
            return ExchangeInfo(
                name="Backpack",
                id="backpack",
                type=ExchangeType.PERPETUAL,
                supported_features=["trading", "orderbook", "ticker"],
                rate_limits=getattr(self.config, 'rate_limits',
                                    {}) if self.config else {},
                precision=getattr(self.config, 'precision',
                                  {}) if self.config else {},
                fees={},
                markets={},
                status="active",
                timestamp=datetime.now()
            )

    async def _get_symbol_precision_from_api(self, symbol: str) -> tuple[int, int]:
        """
        从Backpack API获取交易对的精度信息

        Args:
            symbol: 交易对符号

        Returns:
            (价格精度, 数量精度)
        """
        try:
            # 尝试从已缓存的市场信息中获取精度
            market_info = self._market_info.get(symbol, {})

            if market_info:
                # 检查是否有filters字段（Backpack使用嵌套结构）
                if 'filters' in market_info:
                    filters = market_info['filters']
                    price_precision = 8  # 默认值
                    amount_precision = 8  # 默认值

                    # 从price过滤器获取价格精度
                    if 'price' in filters and isinstance(filters['price'], dict):
                        price_filter = filters['price']
                        if 'tickSize' in price_filter:
                            tick_size = price_filter['tickSize']
                            price_precision = self._calculate_precision_from_tick_size(
                                tick_size)

                    # 从quantity过滤器获取数量精度
                    if 'quantity' in filters and isinstance(filters['quantity'], dict):
                        quantity_filter = filters['quantity']
                        if 'stepSize' in quantity_filter:
                            step_size = quantity_filter['stepSize']
                            amount_precision = self._calculate_precision_from_tick_size(
                                step_size)

                    if self.logger:
                        self.logger.debug(
                            f"从API获取 {symbol} 精度: 价格={price_precision}位, 数量={amount_precision}位")

                    return price_precision, amount_precision

                # 检查是否有直接的precision字段
                if 'precision' in market_info:
                    precision_data = market_info['precision']
                    price_precision = precision_data.get('price', 8)
                    amount_precision = precision_data.get('amount', 8)

                    if self.logger:
                        self.logger.debug(
                            f"从API precision字段获取 {symbol} 精度: 价格={price_precision}位, 数量={amount_precision}位")

                    return price_precision, amount_precision

                # 检查是否有tickSize和stepSize字段
                if 'tickSize' in market_info and 'stepSize' in market_info:
                    tick_size = market_info['tickSize']
                    step_size = market_info['stepSize']

                    price_precision = self._calculate_precision_from_tick_size(
                        tick_size)
                    amount_precision = self._calculate_precision_from_tick_size(
                        step_size)

                    if self.logger:
                        self.logger.debug(
                            f"从API tick/step获取 {symbol} 精度: 价格={price_precision}位, 数量={amount_precision}位")

                    return price_precision, amount_precision

            # 如果无法从API获取，记录警告并返回默认值
            if self.logger:
                self.logger.warning(f"无法从API获取 {symbol} 的精度信息，使用默认值(8位)")

            return 8, 8

        except Exception as e:
            if self.logger:
                self.logger.error(f"从API获取 {symbol} 精度信息失败: {e}")
            return 8, 8

    def get_symbol_info(self, symbol: str) -> Optional[BackpackSymbolInfo]:
        """
        获取交易对信息（重写父类方法，添加从API获取精度的能力）

        Args:
            symbol: 交易对符号

        Returns:
            包含真实精度信息的 BackpackSymbolInfo
        """
        # 解析symbol获取base和quote
        if '_' in symbol:
            parts = symbol.split('_')
            if len(parts) >= 2:
                base = parts[0]
                quote = '_'.join(parts[1:])
            else:
                base = symbol
                quote = 'USDC'
        else:
            base = symbol
            quote = 'USDC'

        # 创建基础信息对象
        symbol_info = BackpackSymbolInfo(symbol, base, quote)

        # 尝试从缓存或API获取真实精度
        if symbol in self._precision_cache:
            price_precision, qty_precision = self._precision_cache[symbol]
            symbol_info.price_precision = price_precision  # ✅ 应用缓存的精度
            symbol_info.qty_precision = qty_precision
        else:
            # 从_market_info中获取精度
            market_info = self._market_info.get(symbol, {})

            if market_info:
                # 检查 filters 字段
                if 'filters' in market_info:
                    filters = market_info['filters']

                    # 从 quantity 过滤器获取数量精度（stepSize）
                    if 'quantity' in filters and isinstance(filters['quantity'], dict):
                        quantity_filter = filters['quantity']
                        if 'stepSize' in quantity_filter:
                            step_size = quantity_filter['stepSize']
                            qty_precision = self._calculate_precision_from_tick_size(
                                step_size)
                            symbol_info.qty_precision = qty_precision

                    # 从 price 过滤器获取价格精度（tickSize）
                    if 'price' in filters and isinstance(filters['price'], dict):
                        price_filter = filters['price']
                        if 'tickSize' in price_filter:
                            tick_size = price_filter['tickSize']
                            price_precision = self._calculate_precision_from_tick_size(
                                tick_size)
                            symbol_info.price_precision = price_precision

                # 缓存精度信息
                self._precision_cache[symbol] = (
                    symbol_info.price_precision, symbol_info.qty_precision)

        return symbol_info

    def _calculate_precision_from_tick_size(self, tick_size: str) -> int:
        """
        根据tick_size计算精度位数

        Args:
            tick_size: tick大小，如 "0.01"

        Returns:
            精度位数
        """
        try:
            tick_value = float(tick_size)
            if tick_value >= 1:
                return 0

            # 计算小数点后的位数
            import math
            precision = -int(math.log10(tick_value))
            return max(0, precision)

        except Exception:
            return 8  # 默认值

    async def get_ticker(self, symbol: str) -> TickerData:
        """获取行情数据"""
        mapped_symbol = self._map_symbol(symbol)

        try:
            # 确保session已创建
            if not self.session:
                await self.connect()

            if not self.session:
                raise Exception("无法建立Backpack连接")

            # 使用公开API获取ticker数据
            async with self.session.get(f"{self.base_url}api/v1/ticker?symbol={mapped_symbol}") as response:
                if response.status == 200:
                    data = await response.json()
                    # 检查data是否为None
                    if data is None:
                        if self.logger:
                            self.logger.warning(f"API返回空数据: {symbol}")
                        data = {}

                    # ✅ 改为debug级别，避免终端刷屏
                    if self.logger:
                        self.logger.debug(f"Ticker API返回: {data}")

                    return self._parse_ticker(data)
                else:
                    raise Exception(f"HTTP {response.status}")

        except Exception as e:
            if self.logger:
                self.logger.error(f"获取行情失败 {symbol}: {e}")
            # 返回空行情数据
            return TickerData(
                symbol=symbol,
                bid=None, ask=None, last=None,
                open=None, high=None, low=None, close=None,
                volume=None, quote_volume=None,
                change=None, percentage=None,
                timestamp=datetime.now(),
                raw_data={}
            )

    async def get_tickers(self, symbols: Optional[List[str]] = None) -> List[TickerData]:
        """获取多个行情数据"""
        try:
            if symbols:
                # 获取指定交易对的ticker
                tasks = [self.get_ticker(symbol) for symbol in symbols]
                return await asyncio.gather(*tasks)
            else:
                # 确保session已创建
                if not self.session:
                    await self.connect()

                if not self.session:
                    raise Exception("无法建立Backpack连接")

                # 获取所有ticker数据
                async with self.session.get(f"{self.base_url}api/v1/tickers") as response:
                    if response.status == 200:
                        data = await response.json()
                        tickers = []
                        for ticker_data in data:
                            symbol = ticker_data.get('symbol', '')
                            if symbol:
                                tickers.append(
                                    self._parse_ticker(ticker_data, symbol))
                        return tickers
                    else:
                        if self.logger:
                            self.logger.error(
                                f"获取所有ticker失败: HTTP {response.status}")
                        return []
        except Exception as e:
            if self.logger:
                self.logger.error(f"获取ticker数据失败: {e}")
            return []

    async def get_orderbook(self, symbol: str, limit: Optional[int] = None) -> OrderBookData:
        """获取订单簿数据 - 使用公开API"""
        try:
            # 直接调用公开API快照方法
            snapshot = await self.get_orderbook_snapshot(symbol)
            if not snapshot:
                return OrderBookData(
                    symbol=symbol,
                    bids=[],
                    asks=[],
                    timestamp=datetime.now(),
                    nonce=None,
                    raw_data={}
                )

            # 转换为OrderBookData格式
            bids = []
            asks = []

            for bid in snapshot.get('bids', []):
                if len(bid) >= 2:
                    bids.append(OrderBookLevel(
                        price=Decimal(str(bid[0])),
                        size=Decimal(str(bid[1]))
                    ))

            for ask in snapshot.get('asks', []):
                if len(ask) >= 2:
                    asks.append(OrderBookLevel(
                        price=Decimal(str(ask[0])),
                        size=Decimal(str(ask[1]))
                    ))

            return OrderBookData(
                symbol=symbol,
                bids=bids,
                asks=asks,
                timestamp=datetime.now(),
                nonce=None,
                raw_data=snapshot
            )

        except Exception as e:
            if self.logger:
                self.logger.error(f"获取订单簿失败 {symbol}: {e}")
            return OrderBookData(
                symbol=symbol,
                bids=[],
                asks=[],
                timestamp=datetime.now(),
                nonce=None,
                raw_data={}
            )

    async def get_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        since: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[OHLCVData]:
        """获取K线数据"""
        # TODO: 实现K线数据获取
        return []

    async def get_trades(
        self,
        symbol: str,
        since: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[TradeData]:
        """
        获取用户成交记录（私有API）

        Args:
            symbol: 交易对符号 (如 'BTC_USDC_PERP')
            since: 开始时间
            limit: 返回数量限制

        Returns:
            List[TradeData]: 成交记录列表
        """
        try:
            # 🔥 Backpack History API 使用原始 symbol 格式
            # 永续合约: BTC_USDC_PERP (保留 _PERP 后缀)
            # 现货: BTC_USDC
            # 经过实际测试验证：API 返回的 symbol 就是 BTC_USDC_PERP
            mapped_symbol = symbol

            # 转换时间戳
            since_timestamp = int(since.timestamp() * 1000) if since else None

            # 调用API获取原始数据
            trades_data = await self.fetch_trades(
                mapped_symbol,
                since=since_timestamp,
                limit=limit
            )

            # 解析为TradeData对象
            result = []
            for trade in trades_data:
                try:
                    trade_data = self._parse_trade(trade, symbol)
                    result.append(trade_data)
                except Exception as e:
                    if self.logger:
                        self.logger.warning(f"解析成交记录失败: {e}, 原始数据: {trade}")
                    continue

            return result

        except Exception as e:
            if self.logger:
                self.logger.error(f"获取成交记录失败 {symbol}: {e}")
            return []

    def _parse_trade(self, trade: Dict[str, Any], symbol: str) -> TradeData:
        """
        解析Backpack成交记录

        Backpack API 返回格式示例（实际测试）:
        {
            "tradeId": 35412870,                       # 成交ID
            "orderId": "15106947971",                  # 订单ID  
            "symbol": "BTC_USDC_PERP",                 # 交易对（永续合约保留_PERP）
            "side": "Ask",                             # 方向: Bid/Ask
            "price": "111409.6",                       # 成交价格
            "quantity": "0.00804",                     # 成交数量
            "fee": "0.232891",                         # 手续费
            "feeSymbol": "USDC",                       # 手续费币种
            "timestamp": "2025-10-24T13:05:14.548",    # ISO 8601 时间戳
            "isMaker": false,                          # 是否 Maker
            "clientId": null,                          # 客户端ID
            "systemOrderType": null                    # 系统订单类型
        }
        """
        try:
            # 解析方向
            side_str = trade.get('side', '').lower()
            if side_str == 'bid' or side_str == 'buy':
                side = OrderSide.BUY
            elif side_str == 'ask' or side_str == 'sell':
                side = OrderSide.SELL
            else:
                side = OrderSide.BUY  # 默认

            # 解析价格和数量
            price = Decimal(str(trade.get('price', 0)))
            amount = Decimal(str(trade.get('quantity', 0)))
            cost = Decimal(str(trade.get('quoteQuantity', 0)))

            # 解析手续费
            fee_amount = trade.get('fee')
            fee_symbol = trade.get('feeSymbol', 'USDC')
            fee = {
                'cost': Decimal(str(fee_amount)) if fee_amount else Decimal('0'),
                'currency': fee_symbol
            } if fee_amount else None

            # 解析时间戳（支持两种格式）
            timestamp_value = trade.get('timestamp')
            if timestamp_value:
                if isinstance(timestamp_value, str):
                    # ISO 8601 格式: "2025-10-24T13:05:14.548"
                    timestamp = datetime.fromisoformat(
                        timestamp_value.replace('Z', '+00:00'))
                elif isinstance(timestamp_value, (int, float)):
                    # 毫秒时间戳格式: 1729767891574
                    timestamp = datetime.fromtimestamp(timestamp_value / 1000)
                else:
                    timestamp = datetime.now()
            else:
                timestamp = datetime.now()

            # 创建TradeData对象
            return TradeData(
                # tradeId 或 id
                id=str(trade.get('tradeId', trade.get('id', ''))),
                symbol=symbol,
                side=side,
                amount=amount,
                price=price,
                cost=cost,
                fee=fee,
                timestamp=timestamp,
                order_id=str(trade.get('orderId', '')),
                raw_data=trade
            )

        except Exception as e:
            if self.logger:
                self.logger.error(f"解析成交记录失败: {e}, 数据: {trade}")
            raise

    # === 账户接口 ===

    async def get_balances(self, force_refresh: bool = False) -> List[BalanceData]:
        """获取账户余额（带缓存）

        🔥 重要：Backpack统一账户，资金在保证金账户中
        使用 /api/v1/capital/collateral 而不是 /api/v1/capital
        
        🎯 缓存策略：
        - 缓存有效期：5秒
        - WebSocket 更新时自动刷新缓存

        API返回格式:
        {
            "collateral": [
                {
                    "symbol": "USDC",
                    "totalQuantity": "1997.44",
                    "availableQuantity": "0",
                    "lendQuantity": "1997.44",
                    "openOrderQuantity": "0",
                    ...
                }
            ],
            ...
        }
        """
        import time
        from datetime import datetime
        
        # 🔥 优先使用缓存（TTL内有效，除非 force_refresh）
        current_time = time.time()
        cache_ttl = getattr(self, "_balance_cache_ttl", 30.0)
        if (
            not force_refresh
            and self._balance_cache
            and (current_time - self._balance_cache_time) < cache_ttl
        ):
            if self.logger:
                self.logger.debug(
                    f"✅ [Backpack] 使用余额缓存 (年龄: {current_time - self._balance_cache_time:.1f}秒)"
                )
            return self._balance_cache
        
        if not self._signing_available:
            if self.logger and not self._signing_warning_emitted:
                self.logger.warning(
                    "⚠️  [Backpack REST] PyNaCl 未安装，无法访问余额接口（已跳过私有API请求）"
                )
                self._signing_warning_emitted = True
            return self._balance_cache

        try:
            # 🔥 使用 collateral 端点获取真实余额
            if self.logger:
                self.logger.debug(f"🔍 [Backpack] 开始查询余额...")
            
            data = await self._make_authenticated_request("GET", "/api/v1/capital/collateral")
            
            if self.logger:
                self.logger.debug(f"🔍 [Backpack] API响应: {type(data)}, keys={list(data.keys()) if isinstance(data, dict) else 'N/A'}")

            balances = []

            # 解析 collateral 数组
            if isinstance(data, dict) and 'collateral' in data:
                collateral_list = data['collateral']

                for item in collateral_list:
                    if not isinstance(item, dict):
                        continue

                    currency = item.get('symbol', '')
                    total_quantity = self._safe_decimal(
                        item.get('totalQuantity', '0'))
                    available_quantity = self._safe_decimal(
                        item.get('availableQuantity', '0'))
                    lend_quantity = self._safe_decimal(
                        item.get('lendQuantity', '0'))
                    open_order_quantity = self._safe_decimal(
                        item.get('openOrderQuantity', '0'))

                    # available = 可用余额
                    # used = 借出余额 + 订单冻结
                    # 🔁 Backpack 统一账户：totalQuantity 才是真实可用余额
                    # availableQuantity 会被借出流程占用，长期为0
                    free = total_quantity
                    # 记录借出/冻结明细到 raw_data，而不是映射到 used 字段
                    used = Decimal("0")

                    # 🔥 将顶层账户字段附加到 raw_data，便于后续使用
                    # 这样每个币种的余额都能访问账户级别的数据
                    raw_data_with_account_info = {
                        **item,  # 币种自己的数据
                        # 附加账户级别的顶层字段
                        '_account_netEquityAvailable': data.get('netEquityAvailable', '0'),
                        '_account_netEquity': data.get('netEquity', '0'),
                        '_account_netEquityLocked': data.get('netEquityLocked', '0'),
                        '_account_unsettledEquity': data.get('unsettledEquity', '0'),
                        '_account_pnlUnrealized': data.get('pnlUnrealized', '0'),
                        '_account_assetsValue': data.get('assetsValue', '0'),
                        '_account_liabilitiesValue': data.get('liabilitiesValue', '0'),
                        '_account_marginFraction': data.get('marginFraction', '0'),
                        '_lendQuantity': str(lend_quantity),
                        '_openOrderQuantity': str(open_order_quantity),
                        '_availableQuantity': str(available_quantity),
                    }

                    # 🔥 重要：total字段必须使用totalQuantity（总余额），而不是availableQuantity（可用余额）
                    # Backpack统一账户中，资金可能在借出（lend）或订单冻结中
                    # 使用总余额才能正确判断账户是否有资金进行交易
                    balance = BalanceData(
                        currency=currency,
                        free=free,  # 可用余额（可能为0，但账户仍有资金）
                        used=used,  # 已用余额（借出+订单冻结）
                        total=total_quantity,  # 🔥 总余额（用于判断账户是否有资金）
                        usd_value=self._safe_decimal(
                            item.get('balanceNotional', '0')),
                        timestamp=datetime.now(),
                        raw_data={'source': 'rest', **raw_data_with_account_info}  # 🔥 标记来源
                    )
                    balances.append(balance)

            # 兼容旧格式（如果 collateral 端点失败，回退到 capital）
            elif isinstance(data, dict):
                for currency, balance_info in data.items():
                    if not isinstance(balance_info, dict):
                        continue

                    available = self._safe_decimal(
                        balance_info.get('available', '0'))
                    locked = self._safe_decimal(
                        balance_info.get('locked', '0'))
                    staked = self._safe_decimal(
                        balance_info.get('staked', '0'))

                    total = available + locked + staked

                    balance = BalanceData(
                        currency=currency,
                        free=available,
                        used=locked + staked,
                        total=total,
                        usd_value=None,
                        timestamp=datetime.now(),
                        raw_data={'source': 'rest', **balance_info}  # 🔥 标记来源
                    )
                    balances.append(balance)
            
            # 🔥 更新缓存
            self._balance_cache = balances
            self._balance_cache_time = time.time()
            
            if self.logger:
                if balances:
                    # 🔥 优化：简化日志格式，减少重复日志
                    self.logger.info(f"[Backpack] 余额查询成功: {len(balances)}个币种")
                    for bal in balances:
                        self.logger.debug(f"  {bal.currency}: 可用={bal.free}, 冻结={bal.used}, 总计={bal.total}")
                else:
                    self.logger.warning(f"[Backpack] 余额查询成功但未找到余额数据")

            return balances

        except Exception as e:
            if self.logger:
                # 🔥 记录详细错误信息（包括认证错误）
                error_msg = str(e)
                if "Exchange not authenticated" in error_msg or "未配置API" in error_msg or "无法获取" in error_msg:
                    self.logger.warning(f"⚠️  [Backpack] 余额查询失败（认证问题）: {e}")
                else:
                    self.logger.error(f"❌ [Backpack] 余额查询失败: {e}", exc_info=True)
            # 🔥 失败时返回空列表，但保留旧缓存（如果有）
            if hasattr(self, '_balance_cache') and self._balance_cache:
                if self.logger:
                    self.logger.debug(f"🔄 [Backpack] 使用旧缓存余额")
                return self._balance_cache
            return []

    async def get_positions(self, symbols: Optional[List[str]] = None) -> List[PositionData]:
        """获取持仓信息"""
        try:
            data = await self._make_authenticated_request("GET", "/api/v1/position")

            # 根据参考脚本，Backpack API可能返回单个dict或list
            if isinstance(data, dict):
                position_list = [data]
            elif isinstance(data, list):
                position_list = data
            else:
                if self.logger:
                    self.logger.warning(f"持仓API返回格式不正确: {type(data)}")
                return []

            positions = []
            for position_info in position_list:
                if not isinstance(position_info, dict):
                    continue

                symbol = self._reverse_map_symbol(
                    position_info.get('symbol', ''))

                # 过滤指定符号
                if symbols and symbol not in symbols:
                    continue

                # 🔥 修复：Backpack API 实际字段名是 netQuantity，不是 size
                # netQuantity 是带符号的：正数=多仓，负数=空仓
                net_quantity_raw = position_info.get(
                    'netQuantity', position_info.get('size'))
                position_size = self._safe_decimal(net_quantity_raw)

                if position_size is None:
                    if self.logger:
                        self.logger.warning(
                            f"持仓 {symbol} 的 netQuantity 字段为None，使用默认值0")
                    position_size = Decimal('0')

                # 🔥 跳过0持仓（避免"幽灵持仓"问题）
                if position_size == 0:
                    if self.logger:
                        self.logger.debug(
                            f"跳过0持仓: {symbol} (API返回netQuantity=0，视为无持仓)")
                    continue

                # 🔥 修复：根据 netQuantity 的符号判断持仓方向
                # Backpack API 没有 side 字段，需要根据 netQuantity 判断
                if position_size > 0:
                    side = PositionSide.LONG
                    size_abs = position_size
                elif position_size < 0:
                    side = PositionSide.SHORT
                    size_abs = abs(position_size)
                else:
                    # 已经被上面的 if position_size == 0 过滤了
                    continue

                # 🔥 修复：Backpack API 字段名是 pnlUnrealized，不是 unrealizedPnl
                unrealized_pnl = self._safe_decimal(
                    position_info.get(
                        'pnlUnrealized', position_info.get('unrealizedPnl'))
                ) or Decimal('0')

                # 🔥 修复：Backpack API 字段名是 pnlRealized
                realized_pnl = self._safe_decimal(
                    position_info.get(
                        'pnlRealized', position_info.get('realizedPnl'))
                ) or Decimal('0')

                # 🔥 修复：清算价格字段名是 estLiquidationPrice
                liquidation_price = self._safe_decimal(
                    position_info.get('estLiquidationPrice',
                                      position_info.get('liquidationPrice'))
                )

                position = PositionData(
                    symbol=symbol,
                    side=side,
                    size=size_abs,  # 使用绝对值
                    entry_price=self._safe_decimal(
                        position_info.get('entryPrice')) or Decimal('0'),
                    mark_price=self._safe_decimal(
                        position_info.get('markPrice')),
                    current_price=self._safe_decimal(
                        position_info.get('markPrice')),
                    unrealized_pnl=unrealized_pnl,
                    realized_pnl=realized_pnl,
                    percentage=None,
                    leverage=self._safe_int(position_info.get('leverage', 1)),
                    margin_mode=MarginMode.CROSS,
                    margin=self._safe_decimal(
                        position_info.get('margin')) or Decimal('0'),
                    liquidation_price=liquidation_price,
                    timestamp=datetime.now(),
                    raw_data=position_info
                )

                if self.logger:
                    # 改为debug级别，避免终端刷屏（每秒1次查询）
                    self.logger.debug(
                        f"📊 REST API持仓解析: {symbol} {side.value} "
                        f"数量={size_abs} (原始netQuantity={position_size}), "
                        f"成本={position.entry_price}, 未实现盈亏={unrealized_pnl}"
                    )

                positions.append(position)

            return positions

        except Exception as e:
            if self.logger:
                # 🔥 只写入日志文件，不输出到控制台（避免UI抖动）
                # 如果是因为未配置API密钥导致的错误，使用debug级别
                error_msg = str(e)
                if "Exchange not authenticated" in error_msg or "未配置API" in error_msg or "无法获取" in error_msg:
                    self.logger.debug(f"获取持仓失败: {e}")
                else:
                    self.logger.error(f"获取持仓失败: {e}")
            return []

    # === 辅助方法 ===

    def _parse_ticker(self, data: Dict[str, Any]) -> TickerData:
        """解析行情数据（Backpack格式）"""
        from datetime import datetime

        symbol = data.get('symbol', '')

        return TickerData(
            symbol=symbol,
            last=self._safe_decimal(data.get('lastPrice')),  # Backpack字段名
            bid=None,  # Backpack API不提供bid
            ask=None,  # Backpack API不提供ask
            high=self._safe_decimal(data.get('high')),
            low=self._safe_decimal(data.get('low')),
            volume=self._safe_decimal(data.get('volume')),
            quote_volume=self._safe_decimal(data.get('quoteVolume')),
            open=self._safe_decimal(data.get('firstPrice')),  # Backpack的开盘价字段
            change=self._safe_decimal(data.get('priceChange')),
            percentage=self._safe_decimal(data.get('priceChangePercent')),
            trades_count=int(data.get('trades', 0)) if data.get(
                'trades') else None,
            timestamp=datetime.now(),
            raw_data=data
        )

    def _parse_timestamp(self, timestamp_value: Any) -> Optional[datetime]:
        """
        解析时间戳（支持多种格式）

        Backpack API 返回格式:
        - ISO 8601 字符串: "2025-10-24T13:49:48.045"
        - 毫秒时间戳: 1729768293700

        Returns:
            datetime 对象，或 None（如果解析失败）
        """
        if not timestamp_value:
            return None

        try:
            if isinstance(timestamp_value, str):
                # ISO 8601 格式: "2025-10-24T13:49:48.045"
                return datetime.fromisoformat(timestamp_value.replace('Z', '+00:00'))
            elif isinstance(timestamp_value, (int, float)):
                # 毫秒时间戳格式: 1729768293700
                return datetime.fromtimestamp(timestamp_value / 1000)
        except Exception as e:
            if self.logger:
                self.logger.warning(f"时间戳解析失败: {timestamp_value}, 错误: {e}")

        return None

    def _calculate_average_price(self, order_data: Dict[str, Any]) -> Optional[Decimal]:
        """
        智能计算订单平均成交价格

        Backpack API 返回格式:
        - avgPrice: 直接返回的平均价格（某些端点有）
        - executedQuoteQuantity: 成交总金额
        - executedQuantity: 成交数量

        优先级:
        1. avgPrice（如果存在）
        2. executedQuoteQuantity / executedQuantity（计算）
        3. None
        """
        # 🔥 优先使用 avgPrice
        avg_price = order_data.get('avgPrice')
        if avg_price:
            return self._safe_decimal(avg_price)

        # 🔥 如果没有 avgPrice，从成交金额和数量计算
        executed_quote = order_data.get('executedQuoteQuantity')
        executed_qty = order_data.get('executedQuantity')

        if executed_quote and executed_qty:
            try:
                quote_decimal = self._safe_decimal(executed_quote)
                qty_decimal = self._safe_decimal(executed_qty)

                if quote_decimal and qty_decimal and qty_decimal > 0:
                    # 计算平均价格 = 成交金额 / 成交数量
                    avg_price_calc = quote_decimal / qty_decimal
                    return avg_price_calc
            except Exception as e:
                if self.logger:
                    self.logger.warning(f"计算平均价格失败: {e}")

        return None

    def _parse_order(self, data: Dict[str, Any]) -> OrderData:
        """解析订单数据（Backpack格式）"""
        # 如果直接返回状态字符串，说明创建订单成功但只返回了状态
        # 这是正常的（订单已挂到交易所），需要构造一个合理的 OrderData
        if isinstance(data, str):
            if self.logger:
                self.logger.warning(f"订单API返回简单字符串: {data}，视为订单已挂")
            # 返回一个最小的 OrderData（订单ID将在后续查询中获取）
            return OrderData(
                id="",  # 稍后补充
                client_id=None,
                symbol="",
                side=OrderSide.BUY,
                type=OrderType.LIMIT,
                amount=Decimal('0'),
                price=Decimal('0'),
                filled=Decimal('0'),
                remaining=Decimal('0'),
                cost=Decimal('0'),
                average=None,
                status=OrderStatus.OPEN if data == "New" else OrderStatus.UNKNOWN,
                timestamp=datetime.now(),
                updated=None,
                fee=None,
                trades=[],
                params={},
                raw_data={'status': data}
            )

        # 状态映射
        status_mapping = {
            'New': OrderStatus.OPEN,
            'PartiallyFilled': OrderStatus.OPEN,  # 部分成交也视为OPEN状态
            'Filled': OrderStatus.FILLED,
            'Canceled': OrderStatus.CANCELED,
            'Cancelled': OrderStatus.CANCELED,
            'Rejected': OrderStatus.REJECTED,
            'Expired': OrderStatus.EXPIRED
        }

        status = status_mapping.get(data.get('status'), OrderStatus.UNKNOWN)

        # 方向映射
        side = OrderSide.BUY if data.get('side') == 'Bid' else OrderSide.SELL

        # 类型映射
        order_type_str = data.get('orderType', 'Limit')
        order_type = OrderType.LIMIT if order_type_str == 'Limit' else OrderType.MARKET

        # 解析数量
        quantity = self._safe_decimal(data.get('quantity'))
        executed_quantity = self._safe_decimal(data.get('executedQuantity'))
        remaining = quantity - \
            executed_quantity if quantity and executed_quantity else Decimal(
                '0')

        return OrderData(
            id=str(data.get('id', '')),
            client_id=data.get('clientId'),
            symbol=data.get('symbol'),
            side=side,
            type=order_type,
            amount=quantity,
            price=self._safe_decimal(data.get('price')),
            filled=executed_quantity,
            remaining=remaining,
            cost=self._safe_decimal(data.get('executedQuoteQuantity')),
            average=self._calculate_average_price(data),  # 🔥 智能计算平均价格
            status=status,
            timestamp=self._parse_timestamp(
                data.get('createdAt')) or datetime.now(),
            updated=self._parse_timestamp(data.get('updatedAt')),
            fee=None,
            trades=[],
            params={},
            raw_data=data
        )

    # === 交易接口 ===

    async def create_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        amount: Decimal,
        price: Optional[Decimal] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> OrderData:
        """创建订单"""
        mapped_symbol = self._map_symbol(symbol)

        # 🔥 确保市场数据已加载（获取真实精度）
        if not self._market_info:
            await self._fetch_supported_symbols()

        # 🔥 格式化数量和价格精度，避免小数位过长问题
        symbol_info = self.get_symbol_info(symbol)
        formatted_amount = self.format_quantity(
            symbol, amount, symbol_info)  # 已经是字符串

        order_data = {
            "symbol": mapped_symbol,
            # Bid/Ask (Backpack格式)
            "side": "Bid" if side == OrderSide.BUY else "Ask",
            "orderType": order_type.value.title(),  # Market/Limit
            "quantity": formatted_amount  # 直接使用，已经是字符串
        }

        if price:
            formatted_price = self.format_price(
                symbol, price, symbol_info)  # 已经是字符串
            order_data["price"] = formatted_price  # 直接使用，已经是字符串

        if params:
            # 处理 reduce_only 参数（Backpack使用驼峰 reduceOnly）
            reduce_only_flag = False
            if "reduce_only" in params:
                reduce_only_flag = bool(params.pop("reduce_only"))
            elif "reduceOnly" in params:
                reduce_only_flag = bool(params.pop("reduceOnly"))

            order_data.update(params)

            if reduce_only_flag:
                order_data["reduceOnly"] = True

        try:
            response = await self._make_authenticated_request("POST", "/api/v1/order", data=order_data)

            # Backpack 订单 API 可能直接返回字符串状态（如 "New", "PartiallyFilled"）
            # 这是正常的，表示订单已成功创建
            if isinstance(response, str):
                if self.logger:
                    self.logger.info(f"订单创建成功，状态: {response}")
                # 订单已创建，但需要通过 get_open_orders 获取完整信息
                # 暂时返回一个占位 OrderData
                return OrderData(
                    id="pending",  # 临时ID，稍后通过 get_open_orders 更新
                    client_id=None,
                    symbol=order_data.get('symbol', ''),
                    side=OrderSide.BUY if order_data.get(
                        'side') == 'Bid' else OrderSide.SELL,
                    type=OrderType.LIMIT if order_data.get(
                        'orderType') == 'Limit' else OrderType.MARKET,
                    amount=Decimal(str(order_data.get('quantity', '0'))),
                    price=Decimal(str(order_data.get('price', '0'))
                                  ) if 'price' in order_data else None,
                    filled=Decimal('0'),
                    remaining=Decimal(str(order_data.get('quantity', '0'))),
                    cost=Decimal('0'),
                    average=None,
                    status=OrderStatus.OPEN if response == "New" else OrderStatus.UNKNOWN,
                    timestamp=datetime.now(),
                    updated=None,
                    fee=None,
                    trades=[],
                    params={},
                    raw_data={'api_response': response,
                              'submitted_order': order_data}
                )

            # 检查响应是否为字典类型
            if not isinstance(response, dict):
                if self.logger:
                    self.logger.warning(
                        f"创建订单返回非预期类型数据: {type(response)} = {response}")
                raise ValueError(f"API返回了非预期类型数据: {response}")

            return self._parse_order(response)

        except Exception as e:
            if self.logger:
                # 🔥 只写入日志文件，不输出到控制台（避免UI抖动）
                # 如果是因为未配置API密钥导致的错误，使用debug级别
                error_msg = str(e)
                if "Exchange not authenticated" in error_msg or "未配置API" in error_msg:
                    self.logger.debug(f"创建订单失败: {e}")
                    self.logger.debug(f"异常类型: {type(e).__name__}")
                    self.logger.debug(f"订单数据: {order_data}")
                else:
                    self.logger.error(f"创建订单失败: {e}")
                    self.logger.error(f"异常类型: {type(e).__name__}")
                    self.logger.error(f"订单数据: {order_data}")
                    import traceback
                    self.logger.error(f"异常堆栈: {traceback.format_exc()}")
            raise

    async def cancel_order(self, order_id: str, symbol: str) -> OrderData:
        """取消订单"""
        mapped_symbol = self._map_symbol(symbol)

        try:
            response = await self._make_authenticated_request(
                "DELETE",
                "/api/v1/order",
                data={"orderId": order_id, "symbol": mapped_symbol}
            )

            return self._parse_order(response)

        except Exception as e:
            if self.logger:
                self.logger.error(f"取消订单失败: {e}")
            raise

    async def cancel_all_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        """
        取消所有订单（增强版）

        策略：
        1. 先尝试使用 Backpack 的批量取消 API（只使用 symbol 参数）
        2. 如果批量取消失败或返回空，则获取所有未成交订单并逐个取消

        注意：Backpack API 不需要 cancelAll 参数，只需要 symbol 即可取消该交易对的所有订单
        """
        try:
            # 方法1: 尝试批量取消（只使用 symbol 参数）
            if not symbol:
                if self.logger:
                    self.logger.error("取消所有订单需要指定 symbol 参数")
                return []

            data = {"symbol": self._map_symbol(symbol)}

            response = await self._make_authenticated_request("DELETE", "/api/v1/orders", data=data)

            # 解析返回的订单列表
            canceled_orders = []

            # 处理不同的响应格式
            if isinstance(response, dict):
                # 格式1: {"orders": [...]}
                if 'orders' in response:
                    for order_data in response['orders']:
                        order = self._parse_order(order_data)
                        canceled_orders.append(order)
                # 格式2: 直接是订单对象 {"orderId": ..., "status": ...}
                elif 'orderId' in response or 'id' in response:
                    order = self._parse_order(response)
                    canceled_orders.append(order)
            elif isinstance(response, list):
                # 格式3: 直接是订单数组 [...]
                for order_data in response:
                    order = self._parse_order(order_data)
                    canceled_orders.append(order)
            elif isinstance(response, str):
                # 格式4: 纯文本响应（如 "Cancelled"）
                if self.logger:
                    self.logger.info(f"批量取消API返回文本: {response}")

            if self.logger:
                self.logger.info(f"批量取消API返回: {len(canceled_orders)} 个订单")

            # 如果批量取消返回0个订单，尝试获取所有未成交订单并逐个取消
            if len(canceled_orders) == 0 and symbol:
                if self.logger:
                    self.logger.warning("批量取消返回0个订单，尝试获取所有未成交订单并逐个取消...")

                # 获取所有未成交订单
                open_orders = await self.get_open_orders(symbol)
                if self.logger:
                    self.logger.info(f"获取到 {len(open_orders)} 个未成交订单")

                # 逐个取消
                for order in open_orders:
                    try:
                        await self.cancel_order(order.id, symbol)
                        canceled_orders.append(order)
                        if self.logger:
                            self.logger.debug(f"已取消订单: {order.id}")
                    except Exception as cancel_error:
                        if self.logger:
                            self.logger.warning(
                                f"取消订单 {order.id} 失败: {cancel_error}")

                if self.logger:
                    self.logger.info(
                        f"逐个取消完成: 成功取消 {len(canceled_orders)} 个订单")

            return canceled_orders

        except Exception as e:
            if self.logger:
                self.logger.error(f"取消所有订单失败: {e}")
                import traceback
                self.logger.error(traceback.format_exc())
            return []

    async def get_order(self, order_id: str, symbol: str) -> OrderData:
        """获取订单信息"""
        mapped_symbol = self._map_symbol(symbol)

        try:
            response = await self._make_authenticated_request(
                "GET",
                "/api/v1/order",
                params={"orderId": order_id, "symbol": mapped_symbol}
            )

            if isinstance(response, dict):
                order_payload = response.get('order', response)
            else:
                order_payload = response

            if not isinstance(order_payload, dict):
                raise ValueError(f"API返回了非预期类型数据: {order_payload}")

            return self._parse_order(order_payload)

        except Exception as e:
            if self.logger:
                self.logger.error(f"获取订单信息失败 {order_id}: {e}")
            # 返回基础订单信息
            return OrderData(
                id=order_id,
                client_id=None,
                symbol=symbol,
                side=OrderSide.BUY,
                type=OrderType.LIMIT,
                amount=Decimal('0'),
                price=Decimal('0'),
                filled=Decimal('0'),
                remaining=Decimal('0'),
                cost=Decimal('0'),
                average=None,
                status=OrderStatus.UNKNOWN,
                timestamp=datetime.now(),
                updated=None,
                fee=None,
                trades=[],
                params={},
                raw_data={}
            )

    async def get_open_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        """获取开放订单"""
        try:
            endpoint = "/api/v1/orders"
            params = {}
            if symbol:
                params["symbol"] = self._map_symbol(symbol)

            response = await self._make_authenticated_request("GET", endpoint, params=params)

            # 确保返回列表格式（根据参考脚本）
            order_list = response if isinstance(response, list) else [
                response] if response else []

            # 解析订单列表
            orders = []
            for order_data in order_list:
                if not isinstance(order_data, dict):
                    continue

                # 只处理未完成的订单
                if order_data.get('status') in ['New', 'PartiallyFilled']:
                    order = self._parse_order(order_data)
                    orders.append(order)

            return orders

        except Exception as e:
            if self.logger:
                self.logger.error(f"获取开放订单失败: {e}")
            return []

    async def get_order_history(
        self,
        symbol: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[OrderData]:
        """
        获取历史订单（包括已完全成交的订单）

        使用 /wapi/v1/history/orders 端点

        Args:
            symbol: 交易对（可选）
            since: 开始时间（可选）
            limit: 限制数量（默认100，最大1000）

        Returns:
            历史订单列表

        注意：
        - 使用 /wapi/v1/history/orders（不是 /api/v1/history/orders）
        - 返回的订单包含 avgPrice（平均成交价）
        - 适合查询已完全成交的市价订单
        """
        try:
            params = {}

            # 🔥 symbol 参数（必填）
            if symbol:
                params['symbol'] = self._map_symbol(symbol)

            # 时间范围（可选）
            if since:
                # 转换为毫秒时间戳
                params['startTime'] = int(since.timestamp() * 1000)

            # 限制数量（可选，默认100，最大1000）
            if limit:
                params['limit'] = min(limit, 1000)

            # 🔥 调用新的 /wapi/v1/history/orders 端点
            result = await self._make_authenticated_request(
                'GET',
                '/wapi/v1/history/orders',
                params=params
            )

            # 解析订单列表
            if isinstance(result, list):
                if self.logger:
                    self.logger.info(f"🔍 原始API返回 {len(result)} 条订单")

                orders = []
                for i, order_data in enumerate(result, 1):
                    try:
                        # 使用现有的 _parse_order 方法
                        order = self._parse_order(order_data)
                        orders.append(order)
                        if self.logger and i <= 3:  # 只打印前3条
                            self.logger.debug(
                                f"✅ 订单 {i} 解析成功 - ID: {order.id}, "
                                f"Status: {order.status.value}, Average: {order.average}")
                    except Exception as e:
                        if self.logger:
                            self.logger.error(f"❌ 解析订单 {i} 失败: {e}")
                            self.logger.error(f"   订单数据: {order_data}")
                        import traceback
                        if self.logger:
                            self.logger.error(traceback.format_exc())

                if self.logger:
                    self.logger.info(
                        f"✅ 成功解析 {len(orders)}/{len(result)} 条历史订单")

                return orders

            return []

        except Exception as e:
            if self.logger:
                self.logger.error(f"获取历史订单失败: {e}")
            return []

    # === 设置接口 ===

    async def set_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        """设置杠杆倍数"""
        # Backpack可能不支持动态设置杠杆
        return {"success": True, "message": "Leverage setting not supported"}

    async def set_margin_mode(self, symbol: str, margin_mode: str) -> Dict[str, Any]:
        """设置保证金模式"""
        # Backpack可能不支持动态设置保证金模式
        return {"success": True, "message": "Margin mode setting not supported"}

    # === 符号管理 ===

    async def get_supported_symbols(self) -> List[str]:
        """获取交易所实际支持的交易对列表"""
        if not self._supported_symbols:
            await self._fetch_supported_symbols()
        return self._supported_symbols.copy()

    async def _fetch_supported_symbols(self) -> None:
        """通过API获取支持的交易对 - 🔥 修改：只获取永续合约"""
        try:
            if self.logger:
                self.logger.info("开始获取Backpack支持的交易对列表...")

            # 调用市场API获取所有交易对
            async with self.session.get(f"{self.base_url}api/v1/markets") as response:
                if response.status == 200:
                    markets_data = await response.json()

                    supported_symbols = []
                    market_info = {}

                    # 统计数据
                    total_markets = len(markets_data)
                    perpetual_count = 0
                    spot_count = 0

                    for market in markets_data:
                        symbol = market.get("symbol")
                        if symbol:
                            # 🔥 修改：只获取永续合约，排除现货
                            if symbol.endswith('_PERP'):
                                # 永续合约
                                normalized_symbol = self._normalize_backpack_symbol(
                                    symbol)
                                supported_symbols.append(normalized_symbol)
                                market_info[normalized_symbol] = market
                                perpetual_count += 1

                                if self.logger:
                                    self.logger.debug(
                                        f"添加永续合约: {normalized_symbol}")
                            else:
                                # 现货交易对 - 跳过
                                spot_count += 1
                                if self.logger:
                                    self.logger.debug(f"跳过现货交易对: {symbol}")

                    self._supported_symbols = supported_symbols
                    self._market_info = market_info

                    if self.logger:
                        self.logger.info(f"✅ Backpack市场数据统计:")
                        self.logger.info(f"  - 总市场数量: {total_markets}")
                        self.logger.info(f"  - 永续合约: {perpetual_count}")
                        self.logger.info(f"  - 现货交易对: {spot_count} (已跳过)")
                        self.logger.info(
                            f"  - 最终订阅: {len(supported_symbols)} 个永续合约")

                else:
                    if self.logger:
                        self.logger.error(f"获取市场数据失败: {response.status}")
                    await self._use_default_symbols()

        except Exception as e:
            if self.logger:
                self.logger.error(f"获取支持的交易对时出错: {e}")
            await self._use_default_symbols()

    async def get_market_info(self, symbol: str) -> Optional[Dict[str, Any]]:
        """获取市场信息"""
        if not self._market_info:
            await self._fetch_supported_symbols()
        return self._market_info.get(symbol)

    # === 其他API方法 ===

    async def get_orderbook_snapshot(self, symbol: str, limit: Optional[int] = None) -> Dict[str, Any]:
        """
        获取订单簿完整快照 - 通过公共REST API (修复价格排序问题)

        Args:
            symbol: 交易对符号 (如 SOL_USDC_PERP)
            limit: 深度限制 (可选，Backpack可能不支持)

        Returns:
            Dict: 包含正确排序的买卖盘数据
            {
                "asks": [["价格", "数量"], ...],  # 按价格从低到高排序
                "bids": [["价格", "数量"], ...],  # 按价格从高到低排序(修复后)
                "lastUpdateId": int,
                "timestamp": int
            }
        """
        try:
            # 映射符号到Backpack格式
            mapped_symbol = self._map_symbol(symbol)

            # 构建参数
            params = {"symbol": mapped_symbol}
            if limit:
                params["limit"] = limit

            # 调用公共API - 不需要认证
            async with self.session.get(f"{self.base_url}api/v1/depth", params=params) as response:
                if response.status == 200:
                    data = await response.json()

                    # 修复Backpack的价格排序问题
                    # 原始买盘：按价格从低到高排序 -> 需要反转为从高到低
                    # 原始卖盘：按价格从低到高排序 -> 保持不变
                    fixed_data = data.copy()

                    # 修复买盘排序：反转使最高买价在前
                    if 'bids' in fixed_data:
                        fixed_data['bids'] = list(reversed(fixed_data['bids']))

                    # 卖盘排序正确，无需修改
                    # asks 已经按价格从低到高排序，最低卖价在前

                    if self.logger:
                        bids_count = len(fixed_data.get('bids', []))
                        asks_count = len(fixed_data.get('asks', []))
                        best_bid = fixed_data.get('bids', [[0]])[
                            0][0] if fixed_data.get('bids') else 0
                        best_ask = fixed_data.get('asks', [[0]])[
                            0][0] if fixed_data.get('asks') else 0
                        self.logger.debug(
                            f"📊 {symbol} 订单簿快照: 买盘{bids_count}档, 卖盘{asks_count}档, 最优买价:{best_bid}, 最优卖价:{best_ask}")

                    return fixed_data
                else:
                    error_text = await response.text()
                    raise Exception(f"HTTP {response.status}: {error_text}")

        except Exception as e:
            if self.logger:
                self.logger.error(f"获取 {symbol} 订单簿快照失败: {e}")
            raise

    async def fetch_ticker(self, symbol: str) -> Dict[str, Any]:
        """获取单个交易对行情数据"""
        try:
            async with self.session.get(f"{self.base_url}api/v1/ticker?symbol={symbol}") as response:
                if response.status == 200:
                    return await response.json()
                else:
                    error_text = await response.text()
                    raise Exception(f"HTTP {response.status}: {error_text}")
        except Exception as e:
            if self.logger:
                self.logger.warning(f"获取ticker数据失败 {symbol}: {e}")
            raise

    async def fetch_all_tickers(self) -> List[Dict[str, Any]]:
        """获取所有交易对行情数据"""
        try:
            async with self.session.get(f"{self.base_url}api/v1/tickers") as response:
                if response.status == 200:
                    return await response.json()
                else:
                    error_text = await response.text()
                    raise Exception(f"HTTP {response.status}: {error_text}")
        except Exception as e:
            if self.logger:
                self.logger.warning(f"获取所有ticker数据失败: {e}")
            raise

    async def fetch_orderbook(self, symbol: str, limit: Optional[int] = None) -> Dict[str, Any]:
        """获取订单簿原始数据"""
        try:
            params = {"symbol": symbol}
            if limit:
                params["limit"] = limit

            data = await self._make_authenticated_request("GET", "/api/v1/orderbook", params=params)
            return data
        except Exception as e:
            if self.logger:
                self.logger.warning(f"获取orderbook数据失败 {symbol}: {e}")
            raise

    async def fetch_trades(self, symbol: Optional[str] = None, since: Optional[int] = None, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        获取用户成交历史（私有API）

        参考文档: https://docs.backpack.exchange/#tag/History/operation/get_fill_history
        使用 /wapi/v1/history/fills 获取用户成交记录

        API 参数:
        - symbol: 交易对符号 (可选，不传则获取所有)
        - startTime: 开始时间戳(毫秒) (可选)
        - endTime: 结束时间戳(毫秒) (可选)
        - limit: 返回数量限制，默认500 (可选)
        """
        try:
            params = {}
            if symbol:
                params["symbol"] = symbol
            if since:
                params["startTime"] = since  # 开始时间戳（毫秒）
            if limit:
                params["limit"] = limit

            # 🔥 使用 /wapi/v1/history/fills 获取用户成交记录
            data = await self._make_authenticated_request("GET", "/wapi/v1/history/fills", params=params)
            return data if isinstance(data, list) else []
        except Exception as e:
            if self.logger:
                self.logger.warning(
                    f"获取fills数据失败 {symbol if symbol else 'all'}: {e}")
            raise

    async def get_klines(self, symbol: str, interval: str, since: Optional[datetime] = None, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """获取K线数据"""
        try:
            params = {"symbol": symbol, "interval": interval}
            if since:
                params["startTime"] = int(since.timestamp() * 1000)
            if limit:
                params["limit"] = limit

            # 使用公开API获取K线数据
            async with self.session.get(f"{self.base_url}api/v1/klines", params=params) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    error_text = await response.text()
                    raise Exception(f"HTTP {response.status}: {error_text}")
        except Exception as e:
            if self.logger:
                self.logger.warning(f"获取K线数据失败 {symbol}: {e}")
            raise

    async def fetch_balances(self) -> Dict[str, Any]:
        """获取账户余额原始数据"""
        try:
            return await self._make_authenticated_request("GET", "/api/v1/capital")
        except Exception as e:
            if self.logger:
                self.logger.warning(f"获取余额数据失败: {e}")
            raise

    async def place_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        quantity: Decimal,
        price: Optional[Decimal] = None,
        time_in_force: str = "GTC",
        client_order_id: Optional[str] = None
    ) -> OrderData:
        """下单"""
        order_data = {
            "symbol": symbol,
            "side": side.value.title(),
            "orderType": order_type.value.title(),
            "quantity": str(quantity)
        }

        if price:
            order_data["price"] = str(price)
        if time_in_force:
            order_data["timeInForce"] = time_in_force
        if client_order_id:
            order_data["clientOrderId"] = client_order_id

        try:
            response = await self._make_authenticated_request("POST", "/api/v1/order", data=order_data)
            return self._parse_order(response)
        except Exception as e:
            if self.logger:
                self.logger.warning(f"下单失败: {e}")
            raise

    async def cancel_order_by_id(self, symbol: str, order_id: Optional[str] = None, client_order_id: Optional[str] = None) -> bool:
        """取消订单"""
        try:
            data = {"symbol": symbol}
            if order_id:
                data["orderId"] = order_id
            if client_order_id:
                data["clientOrderId"] = client_order_id

            await self._make_authenticated_request("DELETE", "/api/v1/order", data=data)
            return True
        except Exception as e:
            if self.logger:
                self.logger.warning(f"取消订单失败: {e}")
            return False

    async def get_order_status(self, symbol: str, order_id: Optional[str] = None, client_order_id: Optional[str] = None) -> OrderData:
        """获取订单状态"""
        params = {"symbol": symbol}
        if order_id:
            params["orderId"] = order_id
        if client_order_id:
            params["clientOrderId"] = client_order_id

        try:
            response = await self._make_authenticated_request("GET", "/api/v1/order", params=params)

            # 检查响应是否为字典类型
            if not isinstance(response, dict):
                if self.logger:
                    self.logger.warning(f"订单状态查询返回非字典类型数据: {response}")
                raise ValueError(f"API返回了非字典类型数据: {response}")

            return self._parse_order(response)
        except Exception as e:
            message = str(e)
            is_not_found = "RESOURCE_NOT_FOUND" in message or "404" in message

            if is_not_found:
                target_order_id = str(order_id) if order_id else None
                target_client_id = str(client_order_id) if client_order_id else None

                if self.logger:
                    self.logger.info(
                        "🔁 [Backpack] 实时接口无此订单，改用历史接口回查: "
                        f"order_id={target_order_id}, client_id={target_client_id}"
                    )

                history_orders = await self.get_order_history(symbol=symbol, limit=200)
                for hist_order in history_orders:
                    hist_id = str(hist_order.id) if hist_order.id is not None else None
                    hist_client = str(hist_order.client_id) if hist_order.client_id else None
                    if (target_order_id and hist_id == target_order_id) or (
                        target_client_id and hist_client == target_client_id
                    ):
                        if self.logger:
                            self.logger.info(
                                "✅ [Backpack] 历史接口命中订单，返回解析结果"
                            )
                        return hist_order

                if self.logger:
                    self.logger.warning(
                        "⚠️ [Backpack] 历史订单列表也未找到目标订单，保持原异常抛出"
                    )

            if self.logger:
                self.logger.warning(f"获取订单状态失败: {e}")
            raise

    async def get_recent_trades(self, symbol: str, limit: int = 500) -> List[Dict[str, Any]]:
        """获取最近成交"""
        try:
            params = {"symbol": symbol, "limit": limit}
            async with self.session.get(f"{self.base_url}api/v1/trades", params=params) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    error_text = await response.text()
                    raise Exception(f"HTTP {response.status}: {error_text}")
        except Exception as e:
            if self.logger:
                self.logger.warning(f"获取最近成交失败 {symbol}: {e}")
            raise
