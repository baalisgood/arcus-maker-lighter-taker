"""
EdgeX REST API模块

包含HTTP请求、认证、私有数据获取、交易操作等功能

重构说明（2025-11-11）：
- 添加官方SDK支持（edgex_sdk）
- 实现POST_ONLY订单（Maker费率优化）
- 添加合约属性管理（contract_id, tick_size）
- 实现BBO价格获取和智能价格调整
- 添加重试机制
"""

import time
import aiohttp
import asyncio
import logging
import ssl
import os
from typing import Dict, List, Optional, Any, Tuple, Type, Union
from decimal import Decimal, ROUND_HALF_UP, ROUND_DOWN, ROUND_UP
from datetime import datetime
from functools import wraps

# 🔥 使用统一日志系统（参考日志编写操作指南）
from ..utils.setup_logging import LoggingConfig
from ..utils.logger_factory import get_exchange_logger

module_logger = get_exchange_logger("ExchangeAdapter.edgex")

# 尝试导入tenacity重试库
try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False
    # 如果tenacity不可用，定义简单的装饰器
    def retry(*args, **kwargs):
        def decorator(func):
            return func
        return decorator

# 尝试导入官方SDK
try:
    from edgex_sdk import Client, WebSocketManager, GetOrderBookDepthParams
    from edgex_sdk.order.types import (
        CreateOrderParams as SDKCreateOrderParams,
        OrderSide as SDKOrderSide,
        OrderType as SDKOrderType,
        TimeInForce as SDKTimeInForce,
        CancelOrderParams as SDKCancelOrderParams,
    )
    EDGEX_SDK_AVAILABLE = True
except ImportError as e:
    EDGEX_SDK_AVAILABLE = False
    # 如果SDK不可用，定义占位符
    Client = None
    WebSocketManager = None
    GetOrderBookDepthParams = None
    SDKCreateOrderParams = None
    SDKOrderSide = None
    SDKOrderType = None
    SDKTimeInForce = None
    SDKCancelOrderParams = None

from .edgex_base import EdgeXBase
from ..models import (
    BalanceData,
    OrderData,
    OrderStatus,
    OrderSide,
    OrderType,
    PositionData,
    PositionSide,
    MarginMode,
    TradeData,
)


# ========================================================================
# 🔥 重试装饰器：基于EdgeX文档实现
# ========================================================================

def query_retry(
    default_return: Any = None,
    exception_type: Union[Type[Exception], Tuple[Type[Exception], ...]] = (Exception,),
    max_attempts: int = 5,
    min_wait: float = 1,
    max_wait: float = 10,
    reraise: bool = False
):
    """
    通用重试装饰器（参考文档实现）
    
    Args:
        default_return: 失败时的默认返回值
        exception_type: 需要重试的异常类型
        max_attempts: 最大重试次数
        min_wait: 最小等待时间（秒）
        max_wait: 最大等待时间（秒）
        reraise: 是否在失败后抛出异常
        
    参考文档：EDGEX_ADAPTER_GUIDE.md 第1320-1377行
    """
    def decorator(func):
        if TENACITY_AVAILABLE:
            # 使用tenacity库
            @retry(
                stop=stop_after_attempt(max_attempts),
                wait=wait_exponential(multiplier=1, min=min_wait, max=max_wait),
                retry=retry_if_exception_type(exception_type),
                reraise=reraise
            )
            @wraps(func)
            async def wrapper(*args, **kwargs):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    module_logger.warning("操作 [%s] 失败: %s", func.__name__, e)
                    if not reraise:
                        return default_return
                    raise
            return wrapper
        else:
            # 简单重试实现
            @wraps(func)
            async def wrapper(*args, **kwargs):
                last_exception = None
                for attempt in range(1, max_attempts + 1):
                    try:
                        return await func(*args, **kwargs)
                    except exception_type as e:
                        last_exception = e
                        if attempt < max_attempts:
                            wait_time = min(min_wait * (2 ** (attempt - 1)), max_wait)
                            await asyncio.sleep(wait_time)
                        else:
                            module_logger.warning(
                                "操作 [%s] 失败 (尝试 %s/%s): %s",
                                func.__name__,
                                attempt,
                                max_attempts,
                                e,
                            )
                            if reraise:
                                raise
                            return default_return
                return default_return
            return wrapper
    return decorator


class EdgeXRest(EdgeXBase):
    """EdgeX REST API接口（重构版）"""

    def __init__(self, config=None, logger=None):
        super().__init__(config)
        
        # 🔥 使用统一日志系统（参考日志编写操作指南）
        # 🔥 关键修复：始终使用LoggingConfig创建logger，确保有文件handler
        # 即使传入了logger，也要检查是否有文件handler，如果没有就创建新的
        from logging.handlers import RotatingFileHandler
        
        has_file_handler = False
        if logger:
            # 检查传入的logger是否有文件handler
            for handler in getattr(logger, "handlers", []):
                if isinstance(handler, RotatingFileHandler):
                    has_file_handler = True
                    break
        
        if logger and has_file_handler:
            # 传入的logger有文件handler，使用它
            self.logger = logger
        else:
            # 🔥 使用统一日志系统创建logger（确保有文件handler）
            self.logger = LoggingConfig.setup_logger(
                name=__name__,
                log_file='ExchangeAdapter.log',  # 与WebSocket使用同一个日志文件
                console_formatter=None,  # 不输出到控制台，避免干扰UI
                file_formatter='detailed',
                level=logging.INFO
            )
        
        # 🔥 关键初始化日志（统一格式）
        self.logger.info("[EdgeX] REST: 开始初始化")
        
        self.session = None
        self.api_key = getattr(config, 'api_key', '') if config else ''
        self.api_secret = getattr(config, 'api_secret', '') if config else ''
        self.base_url = getattr(config, 'base_url', self.DEFAULT_BASE_URL) if config else self.DEFAULT_BASE_URL
        self.is_authenticated = False
        
        # 🔥 新增：官方SDK客户端（如果SDK可用）
        self.sdk_client = None
        self._contract_cache = {}  # 缓存合约信息 {ticker: (contract_id, tick_size, step_size, min_order_size)}
        self._metadata_cache: Optional[Dict[str, Any]] = None
        self._metadata_lock = asyncio.Lock()
        self._metadata_retry_attempts = 3
        
        # 🔥 新增：从配置文件或环境变量读取认证信息
        import os
        import yaml
        from pathlib import Path
        
        # 🔥 优先级1：从config对象的authentication属性读取（与测试脚本保持一致）
        config_obj_account_id = None
        config_obj_stark_key = None
        if config and hasattr(config, 'authentication'):
            try:
                auth = config.authentication
                config_obj_account_id = getattr(auth, 'account_id', None)
                config_obj_stark_key = getattr(auth, 'stark_private_key', None)
                # 认证信息读取成功，不单独记录（在最后统一记录）
            except Exception as e:
                self.logger.warning(f"⚠️  [EdgeX] 从config对象读取认证失败: {e}")
        
        # 🔥 优先级2：从YAML配置文件读取
        config_file_account_id = None
        config_file_stark_key = None
        try:
            config_path = Path(__file__).parent.parent.parent.parent.parent / 'config' / 'exchanges' / 'edgex_config.yaml'
            if config_path.exists():
                with open(config_path, 'r', encoding='utf-8') as f:
                    edgex_yaml = yaml.safe_load(f)
                    # 🔥 修复：authentication 在 edgex 下，与 api 平级
                    edgex_section = edgex_yaml.get('edgex', {})
                    auth = edgex_section.get('authentication', {})
                    config_file_account_id = auth.get('account_id')
                    config_file_stark_key = auth.get('stark_private_key')
                    # 认证信息读取成功，不单独记录（在最后统一记录）
        except Exception as e:
            self.logger.warning(f"⚠️  [EdgeX] 读取配置文件失败: {e}")
        
        # 🔥 优先级3：环境变量作为备用
        env_account_id = os.getenv('EDGEX_ACCOUNT_ID')
        env_stark_key = os.getenv('EDGEX_STARK_PRIVATE_KEY')
        
        # 按优先级选择认证信息
        self.account_id = config_obj_account_id or config_file_account_id or env_account_id
        self.stark_private_key = config_obj_stark_key or config_file_stark_key or env_stark_key
        
        # 🔥 统一记录认证信息状态
        auth_status = "已配置" if (self.account_id and self.stark_private_key) else "未配置"
        self.logger.info(f"[EdgeX] REST: 认证信息 {auth_status}")
        
        # 🔥 初始化官方SDK客户端（如果SDK可用且有认证信息）
        self.logger.debug(f"[EdgeX REST] 初始化检查: SDK可用={EDGEX_SDK_AVAILABLE}, account_id={bool(self.account_id)}, stark_key={bool(self.stark_private_key)}")
        
        if EDGEX_SDK_AVAILABLE and self.account_id and self.stark_private_key:
            try:
                self.sdk_client = Client(
                    base_url=self.base_url.rstrip('/'),
                    account_id=int(self.account_id),
                    stark_private_key=self.stark_private_key
                )
                self.logger.info("[EdgeX] REST: SDK客户端初始化成功")
            except Exception as e:
                self.logger.error(f"[EdgeX] REST: SDK客户端初始化失败: {e}", exc_info=True)
                self.sdk_client = None
        elif not EDGEX_SDK_AVAILABLE:
            self.logger.info("[EdgeX] REST: SDK未安装，使用基础功能")
        else:
            self.logger.info("[EdgeX] REST: SDK未初始化（认证信息未配置）")

        # 🔥 日志限流标志（避免刷屏）
        self._positions_warning_logged = False
        self._balances_warning_logged = False
        self._whitelist_warning_logged = False
        
        # 🔥 余额缓存（WebSocket实时更新）
        self._cached_balances: Optional[List[Dict[str, Any]]] = None
        self._balance_cache_time: float = 0
        self._balance_cache_ttl: float = 5.0  # 缓存5秒有效期

    async def setup_session(self):
        """设置HTTP会话"""
        if not self.session:
            # 🔥 SSL 配置：从环境变量读取
            verify_ssl = os.getenv('EDGEX_VERIFY_SSL', 'true').lower() == 'true'
            
            # 创建 SSL 上下文
            connector = None
            if not verify_ssl:
                ssl_context = ssl.create_default_context()
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
                connector = aiohttp.TCPConnector(ssl=ssl_context)
            
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
                connector=connector,
                headers={
                    'User-Agent': 'EdgeX-Adapter/1.0',
                    'Content-Type': 'application/json'
                }
            )

    async def close_session(self):
        """关闭HTTP会话"""
        if self.session:
            await self.session.close()
            self.session = None

    def _generate_signature(self, message: str) -> str:
        """
        生成ECDSA签名（基于EdgeX官方SDK实现）
        
        Args:
            message: 待签名的消息字符串 (timestamp + method + path + params)
            
        Returns:
            签名的hex字符串 (r + s)，128个字符
            
        参考: EdgeX官方SDK AsyncClient.make_authenticated_request
        """
        try:
            from Crypto.Hash import keccak
            from ecdsa import SigningKey, SECP256k1
            from ecdsa.util import sigencode_string
            
            # Step 1: 对消息进行UTF-8编码
            message_bytes = message.encode('utf-8')
            
            # Step 2: 使用Keccak256哈希（PyCryptodome实现）
            keccak_hash = keccak.new(digest_bits=256)
            keccak_hash.update(message_bytes)
            content_hash = keccak_hash.digest()
            
            # Step 3: 使用私钥进行ECDSA签名
            # 移除0x前缀（如果有）
            private_key_hex = self.stark_private_key
            if private_key_hex.startswith('0x') or private_key_hex.startswith('0X'):
                private_key_hex = private_key_hex[2:]
            
            # 转换为字节
            private_key_bytes = bytes.fromhex(private_key_hex)
            
            # 创建签名密钥
            signing_key = SigningKey.from_string(private_key_bytes, curve=SECP256k1)
            
            # 签名（获取r和s）
            signature = signing_key.sign_digest(
                content_hash,
                sigencode=sigencode_string
            )
            
            # 提取r和s (每个32字节)
            r = int.from_bytes(signature[:32], byteorder='big')
            s = int.from_bytes(signature[32:64], byteorder='big')
            
            # Step 4: 构造最终签名 (r + s)
            # 每个值都编码为64位hex字符串（32字节），总共128字符
            r_hex = format(r, '064x')
            s_hex = format(s, '064x')
            
            final_signature = r_hex + s_hex  # ✅ 只有r+s，不包含public_key.y
            
            if self.logger:
                self.logger.debug(f"✅ EdgeX签名生成成功 (长度: {len(final_signature)})")
                self.logger.debug(f"   r={r_hex[:16]}...{r_hex[-8:]}")
                self.logger.debug(f"   s={s_hex[:16]}...{s_hex[-8:]}")
            
            return final_signature
            
        except ImportError as e:
            error_msg = f"❌ EdgeX签名生成失败: 缺少必要的库 ({e})\n请安装: pip install pycryptodome ecdsa"
            if self.logger:
                self.logger.error(error_msg)
            raise Exception(error_msg)
        except Exception as e:
            error_msg = f"❌ EdgeX签名生成失败: {e}"
            if self.logger:
                self.logger.error(error_msg)
            raise Exception(error_msg)

    def get_auth_headers(self, method: str, path: str, params: Optional[Dict] = None, 
                         data: Optional[Dict] = None) -> Dict[str, str]:
        """
        生成EdgeX认证请求头
        
        Args:
            method: HTTP方法 (GET, POST, DELETE)
            path: 请求路径 (如 /api/v1/private/account/getAccountById)
            params: 查询参数 (用于GET请求)
            data: 请求体 (用于POST请求)
            
        Returns:
            包含认证头的字典
            
        参考文档: docs/adapters/edgex Authentication.md
        """
        if not self.account_id or not self.stark_private_key:
            raise Exception("EdgeX认证信息未配置 (account_id或stark_private_key)")
        
        # 生成时间戳（毫秒）
        timestamp = str(int(time.time() * 1000))
        
        # 构造签名消息
        # 格式: timestamp + method(大写) + path + sorted_params
        method_upper = method.upper()
        
        # 处理参数/Body
        def serialize_value(v):
            """序列化参数值（列表转逗号分隔字符串）"""
            if isinstance(v, list):
                # 列表转为逗号分隔的字符串
                return ','.join(str(item) for item in v)
            return str(v)
        
        param_str = ""
        if params:
            # GET请求：按字母顺序排序参数
            sorted_params = sorted(params.items())
            param_str = '&'.join([f"{k}={serialize_value(v)}" for k, v in sorted_params])
        elif data:
            # POST请求：按字母顺序排序Body字段
            sorted_data = sorted(data.items())
            param_str = '&'.join([f"{k}={serialize_value(v)}" for k, v in sorted_data])
        
        # 构造完整消息
        message = f"{timestamp}{method_upper}{path}{param_str}"
        
        if self.logger:
            self.logger.debug(f"🔐 EdgeX签名消息: {message}")
            self.logger.debug(f"   timestamp={timestamp}, method={method_upper}, path={path}")
            self.logger.debug(f"   param_str={param_str}")
        
        # 生成签名
        signature = self._generate_signature(message)
        
        # 返回认证头
        return {
            'X-edgeX-Api-Timestamp': timestamp,
            'X-edgeX-Api-Signature': signature
        }

    async def _request(self, method: str, endpoint: str, params: Optional[Dict] = None, 
                      data: Optional[Dict] = None, signed: bool = False) -> Dict[str, Any]:
        """执行HTTP请求"""
        await self.setup_session()
        
        # 🔥 修复：正确处理URL拼接，避免双斜杠
        base_url = self.base_url.rstrip('/')
        endpoint = endpoint.lstrip('/')
        url = f"{base_url}/{endpoint}"
        
        # 构造请求路径（用于签名）
        request_path = f"/{endpoint}"
        
        headers = {}
        
        if signed:
            # 🔥 修复：使用新的认证方法
            headers.update(self.get_auth_headers(method, request_path, params, data))
            
        try:
            if method.upper() == 'GET':
                async with self.session.get(url, params=params, headers=headers) as response:
                    result = await response.json()
                    if response.status != 200:
                        raise Exception(f"EdgeX API错误: {result}")
                    return result
            elif method.upper() == 'POST':
                async with self.session.post(url, json=data, headers=headers) as response:
                    result = await response.json()
                    if response.status != 200:
                        raise Exception(f"EdgeX API错误: {result}")
                    return result
            elif method.upper() == 'DELETE':
                async with self.session.delete(url, params=params, headers=headers) as response:
                    result = await response.json()
                    if response.status != 200:
                        raise Exception(f"EdgeX API错误: {result}")
                    return result
            else:
                raise Exception(f"不支持的HTTP方法: {method}")
                
        except Exception as e:
            if self.logger:
                self.logger.warning(f"EdgeX HTTP请求失败: {e}")
            raise

    # === 公共数据接口 ===

    async def fetch_ticker(self, symbol: str) -> Dict[str, Any]:
        """
        获取单个交易对行情数据
        
        根据EdgeX Quote API文档：
        GET /api/v1/public/quote/getTicker?contractId={contract_id}
        """
        # 获取合约ID
        contract_id = self._get_contract_id(symbol)
        if not contract_id:
            raise Exception(f"无法获取合约ID: {symbol}")
        
        params = {'contractId': contract_id}
        response = await self._request('GET', 'api/v1/public/quote/getTicker', params=params)
        
        # EdgeX API返回格式: {"code": "SUCCESS", "data": [ticker_data]}
        if response.get('code') == 'SUCCESS' and response.get('data'):
            # 返回第一个ticker数据
            if isinstance(response['data'], list) and len(response['data']) > 0:
                return response['data'][0]
        
        return response

    async def fetch_orderbook(self, symbol: str, limit: Optional[int] = None) -> Dict[str, Any]:
        """获取订单簿数据"""
        contract_id = self._get_contract_id(symbol)
        if not contract_id:
            raise Exception(f"无法获取合约ID: {symbol}")
        params = {'contractId': contract_id, 'limit': limit or 15}
        return await self._request('GET', 'api/v1/public/quote/getDepth', params=params)

    async def get_orderbook_snapshot(self, symbol: str, limit: Optional[int] = None) -> Dict[str, Any]:
        """
        获取订单簿完整快照 - 通过公共REST API
        
        Args:
            symbol: 交易对符号 (如 BTC-USDT)
            limit: 深度限制 (支持15或200档)
            
        Returns:
            Dict: 完整的订单簿快照数据
            {
                "data": [
                    {
                        "asks": [["价格", "数量"], ...],
                        "bids": [["价格", "数量"], ...],
                        "depthType": "SNAPSHOT"
                    }
                ]
            }
        """
        try:
            # 映射符号到EdgeX合约ID
            contract_id = self._get_contract_id(symbol)
            
            # 确定深度级别 (EdgeX只支持15或200)
            level = 200 if limit is None or limit > 15 else 15
            
            # 构建参数
            params = {
                "contractId": contract_id,
                "level": level
            }
            
            # 使用特殊的EdgeX公共API端点
            url = f"https://pro.edgex.exchange/api/v1/public/quote/getDepth"
            
            await self.setup_session()
            
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    if self.logger:
                        if data.get('data') and len(data['data']) > 0:
                            snapshot = data['data'][0]
                            bids_count = len(snapshot.get('bids', []))
                            asks_count = len(snapshot.get('asks', []))
                            self.logger.debug(f"📊 {symbol} 订单簿快照: 买盘{bids_count}档, 卖盘{asks_count}档")
                    
                    return data
                else:
                    error_text = await response.text()
                    raise Exception(f"HTTP {response.status}: {error_text}")
                    
        except Exception as e:
            if self.logger:
                self.logger.error(f"获取 {symbol} 订单簿快照失败: {e}")
            raise

    def _get_contract_id(self, symbol: str) -> str:
        """获取交易对对应的合约ID"""
        # 基于之前的测试，BTCUSDT的合约ID是10000001
        # 这里需要根据实际情况映射
        symbol_to_contract = {
            "BTC-USDT": "10000001",
            "BTCUSDT": "10000001",
            "BTC_USDT": "10000001",
            # 可以根据需要添加更多映射
        }
        
        # 标准化符号格式
        normalized_symbol = symbol.replace("-", "").replace("_", "").upper()
        
        # 查找合约ID
        for key, contract_id in symbol_to_contract.items():
            if key.replace("-", "").replace("_", "").upper() == normalized_symbol:
                return contract_id
        
        # 如果没找到，返回默认值或抛出错误
        if self.logger:
            self.logger.warning(f"未找到 {symbol} 的合约ID，使用默认值")
        return "10000001"  # 默认使用BTCUSDT

    async def fetch_trades(self, symbol: str, since: Optional[int] = None, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """获取交易记录"""
        params = {'symbol': symbol}
        if limit:
            params['limit'] = min(limit, 1000)
        if since:
            params['startTime'] = since
        return await self._request('GET', 'api/v1/trades', params=params)

    async def fetch_klines(self, symbol: str, interval: str, since: Optional[int] = None, limit: Optional[int] = None) -> List[List]:
        """获取K线数据"""
        params = {
            'symbol': symbol,
            'interval': interval
        }
        if limit:
            params['limit'] = min(limit, 1000)
        if since:
            params['startTime'] = since
        return await self._request('GET', 'api/v1/klines', params=params)

    # === 私有数据接口 ===

    def _map_coin_id_to_currency(self, coin_id: Any) -> str:
        """
        将 EdgeX 的 coinId 映射为标准货币符号
        
        EdgeX coinId 映射：
        - 1000 = USDC
        - 1001 = USDT
        - 1002 = ETH
        - 1003 = BTC
        """
        if coin_id is None:
            return 'USDC'
        
        coin_id_str = str(coin_id)
        
        # 🔥 EdgeX 标准 coinId 映射
        COIN_ID_MAP = {
            '1000': 'USDC',
            '1001': 'USDT',
            '1002': 'ETH',
            '1003': 'BTC',
        }
        
        currency = COIN_ID_MAP.get(coin_id_str)
        if currency:
            return currency
        
        # 如果未知，使用原值但记录警告
        if self.logger:
            self.logger.warning(f"⚠️ [EdgeX REST] 未知的 coinId: {coin_id_str}，使用原值")
        return coin_id_str

    async def fetch_balances(self) -> Dict[str, Any]:
        """获取账户余额数据（使用官方SDK + 缓存）"""
        import time
        try:
            # 🔥 优先使用缓存（如果未过期）
            current_time = time.time()
            if self._cached_balances and (current_time - self._balance_cache_time) < self._balance_cache_ttl:
                if self.logger:
                    self.logger.debug(f"✅ EdgeX余额使用缓存 (年龄: {current_time - self._balance_cache_time:.1f}秒)")
                return {"balances": self._cached_balances}
            
            # 🔥 使用官方SDK查询账户资产
            if not self.sdk_client:
                if self.logger and not self._balances_warning_logged:
                    self.logger.info("ℹ️  EdgeX SDK客户端未配置，跳过余额查询（仅监控模式）。")
                    self._balances_warning_logged = True
                return {"balances": []}
            
            # 🔥 SDK的get_account_asset()是异步方法，直接await
            response = await self.sdk_client.get_account_asset()
            
            if response and response.get('code') == 'SUCCESS':
                asset_data = response.get('data', {})
                
                # 🔥 EdgeX使用collateralAssetModelList来获取可用余额
                collateral_assets = asset_data.get('collateralAssetModelList', [])
                
                # 格式化为标准余额格式
                balances = []
                for asset in collateral_assets:
                    coin_id = asset.get('coinId')
                    currency = self._map_coin_id_to_currency(coin_id)  # 🔥 映射 coinId 为货币符号
                    available = asset.get('availableAmount', '0')
                    total_equity = asset.get('totalEquity', '0')
                    
                    # 🔥 EdgeX的余额数据格式
                    balances.append({
                        'asset': currency,  # 🔥 使用映射后的货币符号
                        'free': available,
                        'locked': str(Decimal(total_equity) - Decimal(available)),
                        'total': total_equity,
                        'source': 'rest'  # 🔥 标记来源（SDK查询也属于REST）
                    })
                
                # 🔥 如果collateralAssetModelList为空，尝试从collateralList获取（参考测试脚本）
                if not balances:
                    collaterals = asset_data.get('collateralList', [])
                    for collateral in collaterals:
                        coin_id = collateral.get('coinId')
                        currency = self._map_coin_id_to_currency(coin_id)  # 🔥 映射 coinId 为货币符号
                        amount = collateral.get('amount', '0')
                        if amount and Decimal(str(amount)) > 0:
                            balances.append({
                                'asset': currency,  # 🔥 使用映射后的货币符号
                                'free': amount,  # collateralList只有总金额，没有细分可用/冻结
                                'locked': '0',
                                'total': amount,
                                'source': 'rest'
                            })
                            if self.logger:
                                self.logger.debug(f"✅ EdgeX从collateralList获取余额: {currency}={amount}")
                
                # 🔥 更新缓存
                self._cached_balances = balances
                self._balance_cache_time = current_time
                
                if self.logger:
                    self.logger.debug(f"✅ EdgeX余额查询成功并缓存: {len(balances)}个资产")
                
                return {"balances": balances}
            else:
                # 针对白名单未开通的账户，给出更清晰的提示并避免反复报错
                if self._is_whitelist_error(response):
                    self._handle_whitelist_error(response)
                    return {"balances": self._cached_balances or []}
                
                if self.logger:
                    self.logger.warning(f"⚠️  EdgeX余额查询失败: {response}")
                # 🔥 失败时使用旧缓存（如果有）
                if self._cached_balances:
                    return {"balances": self._cached_balances}
                return {"balances": []}
                
        except Exception as e:
            if self._is_whitelist_error(e):
                self._handle_whitelist_error(e)
                return {"balances": self._cached_balances or []}
            
            if self.logger and not self._balances_warning_logged:
                self.logger.error(f"❌ EdgeX余额查询异常: {e}", exc_info=True)
                self._balances_warning_logged = True
            # 🔥 失败时使用旧缓存（如果有）
            if self._cached_balances:
                return {"balances": self._cached_balances}
            return {"balances": []}

    async def fetch_positions(self, symbols: Optional[List[str]] = None) -> Dict[str, Any]:
        """获取持仓信息（使用官方SDK）"""
        try:
            # 🔥 使用官方SDK查询账户持仓
            if not self.sdk_client:
                if self.logger and not self._positions_warning_logged:
                    self.logger.warning("⚠️  EdgeX SDK客户端未初始化，无法查询持仓")
                    self._positions_warning_logged = True
                return {"positions": []}
            
            # 🔥 SDK的get_account_positions()是异步方法，直接await
            response = await self.sdk_client.get_account_positions()
            
            if response and response.get('code') == 'SUCCESS':
                position_data = response.get('data', {})
                position_list = position_data.get('positionList', [])
                
                # 格式化为标准持仓格式
                positions = []
                for pos in position_list:
                    contract_id = pos.get('contractId', '')
                    open_size = Decimal(str(pos.get('openSize', '0')))
                    
                    # 只返回有持仓的合约
                    if abs(open_size) > 0:
                        # 🔥 获取合约符号（通过合约ID映射）
                        symbol = self._contract_mappings.get(str(contract_id), f"CONTRACT_{contract_id}")
                        
                        positions.append({
                            'symbol': symbol,
                            'contract_id': contract_id,
                            'size': str(open_size),  # 正数=Long，负数=Short
                            'side': 'long' if open_size > 0 else 'short',
                            'entry_price': str(pos.get('entryPrice', '0')),
                            'mark_price': str(pos.get('markPrice', '0')),
                            'unrealized_pnl': str(pos.get('unrealizedPnl', '0')),
                            'realized_pnl': str(pos.get('realizedPnl', '0')),
                            'raw_data': pos
                        })
                
                if self.logger:
                    self.logger.debug(f"✅ EdgeX持仓查询成功: {len(positions)}个持仓")
                
                return {"positions": positions}
            else:
                if self.logger:
                    self.logger.warning(f"⚠️  EdgeX持仓查询失败: {response}")
                return {"positions": []}
                
        except Exception as e:
            if self.logger and not self._positions_warning_logged:
                self.logger.error(f"❌ EdgeX持仓查询异常: {e}", exc_info=True)
                self._positions_warning_logged = True
            return {"positions": []}

    async def fetch_open_orders(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """获取开放订单"""
        params = {}
        if symbol:
            params['symbol'] = symbol
        return await self._request('GET', 'api/v1/private/order/openOrders', params=params, signed=True)

    async def fetch_order_history(self, symbol: Optional[str] = None, since: Optional[int] = None, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """获取订单历史"""
        params = {}
        if symbol:
            params['symbol'] = symbol
        if since:
            params['startTime'] = since
        if limit:
            params['limit'] = min(limit, 1000)
        return await self._request('GET', 'api/v1/private/order/allOrders', params=params, signed=True)

    async def fetch_order_status(
        self,
        symbol: str,
        order_id: Optional[str] = None,
        client_order_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        获取订单状态（支持开放订单和历史订单双重回查）
        """
        if not order_id and not client_order_id:
            raise ValueError("EdgeX查询订单需要提供 order_id 或 client_order_id")

        target_order_id = str(order_id) if order_id else None
        target_client_id = str(client_order_id) if client_order_id else None
        fallback_symbol = symbol or ""

        def _match(payload: Dict[str, Any]) -> bool:
            if not isinstance(payload, dict):
                return False

            payload_order_id = str(
                payload.get('id')
                or payload.get('orderId')
                or payload.get('origClientOrderId')
                or payload.get('clientOrderId')
                or ""
            )
            payload_client_id = str(
                payload.get('clientOrderId')
                or payload.get('clientId')
                or payload.get('origClientOrderId')
                or ""
            )

            if target_order_id and payload_order_id == target_order_id:
                return True
            if target_client_id and payload_client_id == target_client_id:
                return True
            return False

        async def _search(
            source_name: str,
            fetch_coro
        ) -> Optional[Dict[str, Any]]:
            try:
                raw = await fetch_coro
            except Exception as exc:  # pylint: disable=broad-except
                if self.logger:
                    self.logger.warning(
                        f"⚠️ [EdgeX] 查询{source_name}订单列表失败: {exc}"
                    )
                return None

            entries: List[Dict[str, Any]]
            if isinstance(raw, list):
                entries = raw
            elif isinstance(raw, dict):
                data_field = raw.get('data')
                if isinstance(data_field, list):
                    entries = data_field
                else:
                    entries = []
            else:
                entries = []

            for payload in entries:
                if _match(payload):
                    return payload
            return None

        # 1) 先检查开放订单，覆盖“部分成交或仍在挂单”场景
        match = await _search("open", self.fetch_open_orders(symbol))
        if match:
            if self.logger:
                self.logger.debug(
                    f"✅ [EdgeX] 在开放订单列表中找到订单 {target_order_id or target_client_id}"
                )
            return self._normalize_private_order_payload(match, fallback_symbol)

        # 2) 再检查历史订单，捕获“已成交 / 已取消”场景
        history_limit = 200
        match = await _search(
            "history",
            self.fetch_order_history(symbol, None, history_limit)
        )
        if match:
            if self.logger:
                self.logger.debug(
                    f"✅ [EdgeX] 在历史订单列表中找到订单 {target_order_id or target_client_id}"
                )
            return self._normalize_private_order_payload(match, fallback_symbol)

        raise ValueError(
            f"EdgeX无法定位订单: order_id={order_id}, client_order_id={client_order_id}"
        )

    # === 交易操作接口 ===

    async def create_order(
        self,
        symbol: str,
        side: str,
        order_type: str,
        quantity: Decimal,
        price: Optional[Decimal] = None,
        time_in_force: str = "GTC",
        client_order_id: Optional[str] = None,
        reduce_only: bool = False
    ) -> Dict[str, Any]:
        """创建订单"""
        data = {
            'symbol': symbol,
            'side': side,
            'type': order_type,
            'quantity': str(quantity),
            'timeInForce': time_in_force
        }
        
        if price:
            data['price'] = str(price)
        if client_order_id:
            data['newClientOrderId'] = client_order_id
        if reduce_only:
            data['reduceOnly'] = True
            
        return await self._request('POST', 'api/v1/order', data=data, signed=True)

    async def cancel_order(
        self,
        symbol: str,
        order_id: Optional[str] = None,
        client_order_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        取消订单（优先使用SDK，回退到私有REST端点）
        
        🔥 方案二：智能解析取消结果
        - 如果取消失败且返回 FAILED_ORDER_FILLED，查询订单获取成交详情
        - 返回格式：{"success": bool, "status": str, "filled_data": Optional[Dict]}
        """
        # 🔥 优先使用SDK取消（支持 order_id 或 client_order_id）
        if self.sdk_client and EDGEX_SDK_AVAILABLE and SDKCancelOrderParams:
            cancel_params = SDKCancelOrderParams(
                order_id=str(order_id or ""),
                client_order_id=str(client_order_id or "")
            )
            sdk_result = await self.sdk_client.cancel_order(cancel_params)
            # SDK返回的结果格式可能不同，统一处理
            return {
                "success": True,
                "status": "CANCELED",
                "order_id": order_id or client_order_id,
                "sdk_result": sdk_result
            }
        
        # 🔥 REST API取消：必须提供 order_id 或 client_order_id
        if not order_id and not client_order_id:
            raise ValueError("EdgeX取消订单需要提供 order_id 或 client_order_id")
        
        account_id = self._require_account_id()
        payload = {
            "accountId": account_id,
            "orderIdList": [str(order_id)]
        }
        response = await self._request(
            'POST',
            'api/v1/private/order/cancelOrderById',
            data=payload,
            signed=True
        )
        
        cancel_map = (response.get('data') or {}).get('cancelResultMap', {})
        status = (cancel_map.get(str(order_id)) or '').upper()
        
        # 🔥 取消成功
        if status == 'SUCCESS':
            return {
                "success": True,
                "status": "CANCELED",
                "order_id": order_id
            }
        
        # 🔥 订单已成交（取消失败但这是预期的好结果）
        if status == 'FAILED_ORDER_FILLED':
            if self.logger:
                self.logger.info(
                    f"✅ [EdgeX REST] 取消订单失败：订单已成交 (order_id={order_id})，"
                    "查询订单获取成交详情"
                )
            
            try:
                # 查询订单获取完整的成交数据
                filled_order_data = await self.fetch_order_status(
                    symbol=symbol,
                    order_id=order_id
                )
                return {
                    "success": True,  # 虽然取消失败，但订单成交是好结果
                    "status": "FILLED",
                    "order_id": order_id,
                    "filled_data": filled_order_data
                }
            except Exception as query_error:
                if self.logger:
                    self.logger.warning(
                        f"⚠️ [EdgeX REST] 查询已成交订单失败: {query_error}，"
                        "返回占位数据"
                    )
                # 即使查询失败，也返回成功，因为订单确实成交了
                return {
                    "success": True,
                    "status": "FILLED",
                    "order_id": order_id,
                    "filled_data": None
                }
        
        # 🔥 其他取消失败情况（如订单不存在、已取消等）
        if self.logger:
            self.logger.warning(
                f"⚠️ [EdgeX REST] 取消订单失败: order_id={order_id}, "
                f"status={status}, cancel_map={cancel_map}"
            )
        
        raise Exception(f"EdgeX取消订单失败: {status or cancel_map}")

    async def cancel_all_orders(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """取消所有订单"""
        if self.sdk_client and EDGEX_SDK_AVAILABLE and SDKCancelOrderParams and symbol:
            ticker = self._extract_ticker_from_symbol(symbol)
            contract_id, _, _, _ = await self.get_contract_attributes(ticker)
            cancel_params = SDKCancelOrderParams(contract_id=contract_id)
            result = await self.sdk_client.cancel_order(cancel_params)
            return result.get('data', []) if isinstance(result, dict) else []
        
        account_id = self._require_account_id()
        payload = {"accountId": account_id}
        if symbol:
            payload["symbol"] = symbol
        response = await self._request(
            'POST',
            'api/v1/private/order/cancelAllOrder',
            data=payload,
            signed=True
        )
        cancel_map = (response.get('data') or {}).get('cancelResultMap', {})
        return [
            {"order_id": key, "status": value}
            for key, value in cancel_map.items()
        ]

    # === 账户设置接口 ===

    async def set_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        """设置杠杆倍数"""
        data = {
            'symbol': symbol,
            'leverage': leverage
        }
        return await self._request('POST', 'api/v1/leverage', data=data, signed=True)

    async def set_margin_mode(self, symbol: str, margin_mode: str) -> Dict[str, Any]:
        """设置保证金模式"""
        data = {
            'symbol': symbol,
            'marginType': margin_mode.upper()
        }
        return await self._request('POST', 'api/v1/marginType', data=data, signed=True)

    # === 数据解析接口 ===

    async def get_balances(self) -> List[BalanceData]:
        """获取账户余额"""
        try:
            balance_data = await self.fetch_balances()
            balances = []
            for balance in balance_data.get('balances', []):
                free = Decimal(str(balance.get('free', '0')))
                locked = Decimal(str(balance.get('locked', '0')))
                total = Decimal(str(balance.get('total', '0')))
                
                # 只返回有余额的资产
                if free > 0 or locked > 0:
                    balances.append(BalanceData(
                        currency=balance.get('asset', 'USDC'),
                        free=free,
                        used=locked,  # EdgeX使用locked表示冻结余额
                        total=total,
                        usd_value=total,  # EdgeX余额已经是USD计价
                        timestamp=datetime.now(),
                        raw_data={'source': balance.get('source', 'rest'), **balance}
                    ))
            return balances
        except Exception as e:
            if self.logger:
                self.logger.warning(f"获取账户余额失败: {e}", exc_info=True)
            return []

    async def get_positions(self, symbols: Optional[List[str]] = None) -> List[PositionData]:
        """获取持仓信息"""
        try:
            positions_data = await self.fetch_positions(symbols)
            positions: List[PositionData] = []
            for pos in positions_data.get('positions', []):
                if not isinstance(pos, dict):
                    continue

                symbol = pos.get('symbol')
                contract_id = pos.get('contract_id') or pos.get('contractId')
                contract_id_str = str(contract_id) if contract_id is not None else None
                if not symbol and contract_id_str:
                    symbol = (
                        self._contract_mappings.get(contract_id_str)
                        or self._contract_mappings.get(contract_id)
                    )
                if not symbol:
                    symbol = pos.get('contractSymbol') or (f"CONTRACT_{contract_id}" if contract_id is not None else None)
                symbol = self._standardize_symbol(symbol, contract_id)
                if not symbol:
                    continue

                size_raw = self._safe_decimal(
                    pos.get('size') or pos.get('openSize') or pos.get('positionAmt')
                )
                if size_raw == 0:
                    continue

                side = PositionSide.LONG if size_raw > 0 else PositionSide.SHORT
                size_abs = abs(size_raw)

                entry_price = self._safe_decimal(
                    pos.get('entry_price') or pos.get('entryPrice')
                )
                mark_price = self._safe_decimal(
                    pos.get('mark_price') or pos.get('markPrice')
                )
                current_price = mark_price if mark_price != 0 else entry_price

                unrealized_pnl = self._safe_decimal(
                    pos.get('unrealized_pnl') or pos.get('unrealizedPnl')
                )
                realized_pnl = self._safe_decimal(
                    pos.get('realized_pnl') or pos.get('realizedPnl')
                )

                percentage = self._safe_decimal(pos.get('percentage') or pos.get('roe'))
                percentage_value = percentage if percentage != 0 else None

                margin_mode_str = str(
                    pos.get('margin_mode') or pos.get('marginMode') or 'cross'
                ).lower()
                margin_mode = MarginMode.ISOLATED if margin_mode_str == 'isolated' else MarginMode.CROSS

                leverage = self._safe_int(pos.get('leverage')) or 1
                margin_value = self._safe_decimal(
                    pos.get('margin') or pos.get('initialMargin') or pos.get('positionMargin')
                )
                liquidation_price = self._safe_decimal(
                    pos.get('liquidation_price') or pos.get('liquidationPrice')
                )
                if liquidation_price == 0:
                    liquidation_price = None

                positions.append(
                    PositionData(
                        symbol=symbol,
                        side=side,
                        size=size_abs,
                        entry_price=entry_price,
                        mark_price=mark_price if mark_price != 0 else None,
                        current_price=current_price if current_price != 0 else None,
                        unrealized_pnl=unrealized_pnl,
                        realized_pnl=realized_pnl,
                        percentage=percentage_value,
                        leverage=leverage,
                        margin_mode=margin_mode,
                        margin=margin_value,
                        liquidation_price=liquidation_price,
                        timestamp=datetime.now(),
                        raw_data=pos,
                    )
                )

            return positions
        except Exception as e:
            if self.logger:
                self.logger.warning(f"获取持仓信息失败: {e}", exc_info=True)
            return []

    def _standardize_symbol(self, symbol: Optional[str], contract_id: Optional[str] = None) -> Optional[str]:
        """
        将EdgeX返回的symbol转换为统一格式（如 ETH-USDC-PERP）
        """
        if not symbol and contract_id is not None:
            symbol = self._contract_mappings.get(str(contract_id)) or self._contract_mappings.get(contract_id)
        if not symbol:
            return symbol
        
        normalized = str(symbol).upper()
        
        # CONTRACT_123456 -> 通过合约映射转换
        if normalized.startswith("CONTRACT_"):
            contract_key = normalized.split("_", 1)[1]
            mapped = self._contract_mappings.get(contract_key)
            if mapped:
                normalized = mapped.upper()
        
        # EdgeX常见格式：BTCUSD / BTC_USDC
        if normalized.endswith("USD"):
            base = normalized[:-3]
            return f"{base}-USDC-PERP"
        if normalized.endswith("USDC"):
            base = normalized[:-4]
            return f"{base}-USDC-PERP"
        if "_" in normalized:
            parts = normalized.split("_")
            if len(parts) >= 2:
                base = parts[0]
                quote = parts[1]
                if quote in ("USD", "USDC", "USDT"):
                    quote = "USDC"
                return f"{base}-{quote}-PERP"
        
        return normalized

    async def get_open_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        """获取开放订单"""
        try:
            orders_data = await self.fetch_open_orders(symbol)
            return [self._parse_order(order) for order in orders_data]
        except Exception as e:
            if self.logger:
                self.logger.warning(f"获取开放订单失败: {e}")
            return []

    async def get_order_history(self, symbol: Optional[str] = None, since: Optional[datetime] = None, limit: Optional[int] = None) -> List[OrderData]:
        """获取订单历史"""
        try:
            since_timestamp = int(since.timestamp() * 1000) if since else None
            orders_data = await self.fetch_order_history(symbol, since_timestamp, limit)
            return [self._parse_order(order) for order in orders_data]
        except Exception as e:
            if self.logger:
                self.logger.warning(f"获取订单历史失败: {e}")
            return []

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
            if self.sdk_client and EDGEX_SDK_AVAILABLE:
                return await self._place_order_via_sdk(
                    symbol=symbol,
                    side=side,
                    order_type=order_type,
                    quantity=quantity,
                    price=price,
                    time_in_force=time_in_force,
                    client_order_id=client_order_id,
                    reduce_only=reduce_only
                )

            side_str = 'BUY' if side == OrderSide.BUY else 'SELL'
            type_str = 'LIMIT' if order_type == OrderType.LIMIT else 'MARKET'
            
            order_data = await self.create_order(
                symbol=symbol,
                side=side_str,
                order_type=type_str,
                quantity=quantity,
                price=price,
                time_in_force=time_in_force,
                client_order_id=client_order_id,
                reduce_only=reduce_only
            )
            return self._parse_order(order_data)
        except Exception as e:
            if self.logger:
                self.logger.warning(f"下单失败: {e}")
            raise

    async def cancel_order_by_id(self, symbol: str, order_id: Optional[str] = None, client_order_id: Optional[str] = None) -> bool:
        """取消订单"""
        try:
            await self.cancel_order(symbol, order_id, client_order_id)
            return True
        except Exception as e:
            if self.logger:
                self.logger.warning(f"取消订单失败: {e}")
            return False

    async def get_order_status(self, symbol: str, order_id: Optional[str] = None, client_order_id: Optional[str] = None) -> OrderData:
        """获取订单状态"""
        try:
            order_data = await self.fetch_order_status(symbol, order_id, client_order_id)
            return self._parse_order(order_data)
        except Exception as e:
            if self.logger:
                self.logger.warning(f"获取订单状态失败: {e}")
            raise

    async def get_recent_trades(self, symbol: str, limit: int = 500) -> List[TradeData]:
        """获取最近成交记录"""
        try:
            trades_data = await self.fetch_trades(symbol, limit=limit)
            return [self._parse_trade(trade, symbol) for trade in trades_data]
        except Exception as e:
            if self.logger:
                self.logger.warning(f"获取最近成交记录失败: {e}")
            return []

    def _normalize_private_order_payload(self, data: Dict[str, Any], fallback_symbol: str) -> Dict[str, Any]:
        """将私有订单接口返回的数据转换为通用订单字典"""
        symbol = data.get('symbol')
        if not symbol:
            contract_id = data.get('contractId')
            if contract_id:
                symbol = self.get_symbol_by_contract(str(contract_id)) or f"CONTRACT_{contract_id}"
            else:
                symbol = fallback_symbol
        
        size = data.get('size') or data.get('quantity') or data.get('amount')
        filled = data.get('cumFillSize') or data.get('filled') or '0'
        
        remaining = data.get('remaining')
        if remaining is None and size is not None:
            try:
                remaining = str(Decimal(str(size)) - Decimal(str(filled or 0)))
            except Exception:
                remaining = '0'
        
        cost = (
            data.get('cumMatchValue')
            or data.get('cumFillValue')
            or data.get('cost')
            or '0'
        )
        
        return {
            'id': data.get('id') or data.get('orderId') or data.get('clientOrderId'),
            'symbol': symbol,
            'side': data.get('side'),
            'type': data.get('type'),
            'amount': size,
            'price': data.get('price'),
            'filled': filled,
            'remaining': remaining,
            'cost': cost,
            'status': data.get('status') or data.get('orderStatus'),
            'timestamp': data.get('createdTime') or data.get('timestamp'),
            'clientOrderId': data.get('clientOrderId'),
            'average': data.get('avgPrice') or data.get('averagePrice'),
            'updatedTime': data.get('updatedTime'),
            'info': data
        }

    async def get_klines(self, symbol: str, interval: str, since: Optional[datetime] = None, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """获取K线数据"""
        try:
            since_timestamp = int(since.timestamp() * 1000) if since else None
            klines_data = await self.fetch_klines(symbol, interval, since_timestamp, limit)
            
            # 转换数据格式
            klines = []
            for kline in klines_data:
                if len(kline) >= 6:
                    klines.append({
                        'timestamp': kline[0],
                        'open': float(kline[1]),
                        'high': float(kline[2]),
                        'low': float(kline[3]),
                        'close': float(kline[4]),
                        'volume': float(kline[5])
                    })
            return klines
        except Exception as e:
            if self.logger:
                self.logger.warning(f"获取K线数据失败: {e}")
            return []

    async def authenticate(self) -> bool:
        """进行身份认证"""
        try:
            # 🔥 简化：EdgeX主要用于WebSocket数据订阅，跳过REST API认证
            # EdgeX不需要复杂的认证过程，直接标记为已认证
            if self.logger:
                self.logger.info("EdgeX认证跳过 - 主要用于WebSocket数据订阅")
            self.is_authenticated = True
            return True
        except Exception as e:
            if self.logger:
                self.logger.warning(f"EdgeX认证失败: {e}")
            self.is_authenticated = False
            return False

    async def health_check(self) -> Dict[str, Any]:
        """健康检查"""
        try:
            # 🔥 简化：EdgeX主要用于WebSocket，健康检查直接返回成功
            # 避免REST API调用可能的问题
            if self.logger:
                self.logger.debug("EdgeX健康检查跳过 - 主要用于WebSocket")
            api_accessible = True
            error = None
        except Exception as e:
            # EdgeX API不可访问时的处理
            api_accessible = False
            error = str(e)

        return {
            "status": "ok" if api_accessible else "error",
            "api_accessible": api_accessible,
            "authentication": "enabled" if self.is_authenticated else "disabled",
            "timestamp": time.time(),
            "error": error
        }
    
    # ========================================================================
    # 🔥 新增方法：基于EdgeX文档的实盘验证功能
    # ========================================================================
    
    async def get_contract_attributes(self, ticker: str) -> Tuple[str, Decimal, Decimal, Decimal]:
        """
        获取合约属性（基于文档实现）
        
        Args:
            ticker: 交易对代码（如 "ETH", "BTC", "SOL"）
            
        Returns:
            Tuple[str, Decimal, Decimal, Decimal]: (contract_id, tick_size, step_size, min_order_size)
            - contract_id: 合约ID
            - tick_size: 价格最小变动单位（用于调整价格精度）
            - step_size: 数量最小变动单位（用于调整数量精度）
            - min_order_size: 最小订单数量（交易所要求的最小下单量）
            
        Raises:
            ValueError: 如果找不到合约或ticker为空
            
        参考文档：EDGEX_ADAPTER_GUIDE.md 第260-329行
        
        注意：
        - stepSize: 数量精度（最小步长），如 0.01 表示数量必须是 0.01 的倍数
        - minOrderSize: 最小订单数量，如 1 表示最少下单 1 个币
        """
        # 检查缓存
        if ticker in self._contract_cache:
            return self._contract_cache[ticker]
        
        if not ticker:
            raise ValueError("Ticker不能为空")
        
        try:
            response = await self._ensure_metadata_loaded()
            
            data = response.get('data', {})
            if not data:
                raise ValueError("无法获取元数据")
            
            contract_list = data.get('contractList', [])
            if not contract_list:
                raise ValueError("无法获取合约列表")
            
            # 查找匹配的合约（ETH → ETHUSD）
            contract_name = f"{ticker.upper()}USD"
            current_contract = None
            
            for contract in contract_list:
                if contract.get('contractName') == contract_name:
                    current_contract = contract
                    break
            
            if not current_contract:
                response = await self._ensure_metadata_loaded(force_refresh=True)
                data = response.get('data', {})
                contract_list = data.get('contractList', [])
                for contract in contract_list:
                    if contract.get('contractName') == contract_name:
                        current_contract = contract
                        break
            
            if not current_contract:
                raise ValueError(f"找不到合约: {contract_name}")
            
            # 提取合约属性
            contract_id = current_contract.get('contractId')
            tick_size = Decimal(str(current_contract.get('tickSize', '0.01')))
            # 🔥 stepSize 用于数量精度，minOrderSize 用于最小订单数量
            # stepSize=0.01 表示数量精度（最小步长）
            # minOrderSize=1 表示最小订单数量（最少下单量）
            step_size = Decimal(str(current_contract.get('stepSize', '0.01')))
            min_order_size = Decimal(str(current_contract.get('minOrderSize', '0')))
            
            if not contract_id:
                raise ValueError(f"合约ID为空: {contract_name}")
            
            # 缓存结果（包含 min_order_size）
            self._contract_cache[ticker] = (contract_id, tick_size, step_size, min_order_size)
            
            if self.logger:
                self.logger.info(
                    f"✅ 合约属性: {ticker} -> ID={contract_id}, "
                    f"tick_size={tick_size}, step_size={step_size}, min_order_size={min_order_size}"
                )
            
            return contract_id, tick_size, step_size, min_order_size
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 获取合约属性失败 ({ticker}): {e}")
            raise
    
    def _extract_ticker_from_symbol(self, symbol: str) -> str:
        """从交易所符号中提取基础资产（例如 ETH_USDC → ETH）"""
        if not symbol:
            return ""
        normalized = symbol.replace(':', '_').replace('-', '_')
        return normalized.split('_')[0].upper()

    def _require_account_id(self) -> str:
        """确保已配置accountId，供私有接口使用"""
        if not self.account_id:
            raise ValueError("EdgeX accountId未配置，无法调用私有订单接口")
        return str(self.account_id)

    def _generate_client_order_id(self) -> str:
        """生成唯一的客户端订单ID"""
        return str(int(time.time() * 1000))

    def _map_time_in_force(self, tif: Optional[str]) -> Optional[str]:
        """将通用TIF映射为EdgeX SDK枚举值"""
        if not tif or not SDKTimeInForce:
            return None
        mapping = {
            'GTC': SDKTimeInForce.GOOD_TIL_CANCEL.value,
            'IOC': SDKTimeInForce.IMMEDIATE_OR_CANCEL.value,
            'FOK': SDKTimeInForce.FILL_OR_KILL.value,
            'POST_ONLY': SDKTimeInForce.POST_ONLY.value,
        }
        return mapping.get(tif.upper())

    def _is_whitelist_error(self, error: Any) -> bool:
        """检测是否为EdgeX账户白名单错误"""
        if error is None:
            return False
        if isinstance(error, dict):
            code_text = str(error.get('code', '')).upper()
            msg_text = str(error.get('msg', '')).upper()
            return 'ACCOUNT_ID_WHITELIST_ERROR' in code_text or 'ACCOUNT_ID_WHITELIST_ERROR' in msg_text
        return 'ACCOUNT_ID_WHITELIST_ERROR' in str(error).upper()

    def _handle_whitelist_error(self, detail: Any):
        """统一处理账户未加入白名单的场景，避免重复刷日志"""
        if self._whitelist_warning_logged or not self.logger:
            return
        
        msg = ""
        if isinstance(detail, dict):
            msg = detail.get('msg') or ''
        else:
            msg = str(detail)
        
        account_id_hint = self.account_id or "未配置"
        self.logger.warning(
            "⚠️ [EdgeX] 账户未加入白名单，无法查询余额 (account_id=%s)。EdgeX返回: %s",
            account_id_hint,
            msg
        )
        self.logger.warning("   请联系EdgeX管理员开通白名单或使用已授权账户。")
        self._whitelist_warning_logged = True

    async def _place_order_via_sdk(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        quantity: Decimal,
        price: Optional[Decimal],
        time_in_force: Optional[str],
        client_order_id: Optional[str],
        reduce_only: bool = False,
    ) -> OrderData:
        """使用官方SDK下单，避免REST私有端点404"""
        if not (self.sdk_client and EDGEX_SDK_AVAILABLE and SDKCreateOrderParams):
            raise Exception("EdgeX SDK客户端未初始化，无法执行下单操作")

        ticker = self._extract_ticker_from_symbol(symbol)
        contract_id, tick_size, step_size, min_order_size = await self.get_contract_attributes(ticker)

        sdk_side = SDKOrderSide.BUY if side == OrderSide.BUY else SDKOrderSide.SELL
        sdk_type = SDKOrderType.MARKET if order_type == OrderType.MARKET else SDKOrderType.LIMIT
        tif_value = self._map_time_in_force(time_in_force)
        client_id = client_order_id or self._generate_client_order_id()

        price_decimal = Decimal('0')
        if sdk_type == SDKOrderType.LIMIT:
            if price is None:
                raise ValueError("限价单必须指定价格")
            price_decimal = Decimal(str(price))
            if tick_size and tick_size > 0:
                price_decimal = price_decimal.quantize(tick_size, rounding=ROUND_HALF_UP)

        # 🔥 处理订单数量：使用 step_size 调整精度，使用 min_order_size 确保最小值
        quantity_decimal = Decimal(str(quantity))
        try:
            quantity_step = Decimal(str(step_size)) if step_size and step_size > 0 else Decimal("0.01")
        except Exception:
            quantity_step = Decimal("0.01")

        # 先按 step_size 调整数量精度
        if quantity_step > 0:
            try:
                quantity_decimal = quantity_decimal.quantize(quantity_step, rounding=ROUND_DOWN)
            except Exception:
                # 如果quantize失败，退化为直接向下取整到2位
                quantity_decimal = quantity_decimal.quantize(Decimal("0.01"), rounding=ROUND_DOWN)

            if quantity_decimal <= 0:
                quantity_decimal = quantity_step
        
        # 🔥 检查并确保满足最小订单数量要求（minOrderSize）
        if min_order_size > 0 and quantity_decimal < min_order_size:
            if self.logger:
                self.logger.warning(
                    f"⚠️  订单数量 {quantity_decimal} 小于交易所最小要求 {min_order_size}，"
                    f"已调整为 {min_order_size}"
                )
            quantity_decimal = min_order_size
            # 确保调整后的数量仍然符合 step_size 精度
            if quantity_step > 0:
                try:
                    quantity_decimal = quantity_decimal.quantize(quantity_step, rounding=ROUND_UP)
                except Exception:
                    pass

        params = SDKCreateOrderParams(
            contract_id=contract_id,
            price=str(price_decimal),
            size=str(quantity_decimal),
            type=sdk_type,
            side=sdk_side,
            client_order_id=client_id,
            time_in_force=tif_value,
            reduce_only=reduce_only,
        )

        result = await self.sdk_client.create_order(params)

        if self.logger:
            self.logger.info(
                f"✅ [EdgeX SDK] 下单成功: contract={contract_id}, side={sdk_side.value}, "
                f"type={sdk_type.value}, size={quantity}, client_id={client_id}"
            )

        return OrderData(
            id=client_id,
            client_id=client_id,
            symbol=symbol,
            side=side,
            type=order_type,
            amount=quantity,
            price=price if order_type == OrderType.LIMIT else None,
            filled=Decimal('0'),
            remaining=quantity,
            cost=Decimal('0'),
            average=None,
            status=OrderStatus.OPEN,
            timestamp=datetime.now(),
            updated=None,
            fee=None,
            trades=[],
            params={},
            raw_data={'source': 'sdk', 'response': result}
        )
    
    async def _ensure_metadata_loaded(self, force_refresh: bool = False) -> Dict[str, Any]:
        if self._metadata_cache and not force_refresh:
            return self._metadata_cache
        
        async with self._metadata_lock:
            if self._metadata_cache and not force_refresh:
                return self._metadata_cache
            
            last_error: Optional[Exception] = None
            for attempt in range(1, self._metadata_retry_attempts + 1):
                try:
                    if self.sdk_client:
                        response = await self.sdk_client.get_metadata()
                    else:
                        response = await self._request('GET', 'api/v1/metadata')
                    
                    if response:
                        self._metadata_cache = response
                        return response
                except Exception as e:
                    last_error = e
                    if self.logger:
                        self.logger.warning(
                            f"⚠️ [EdgeX REST] 获取元数据失败 (尝试{attempt}/{self._metadata_retry_attempts}): {e}"
                        )
                    await asyncio.sleep(min(2 * attempt, 5))
            
            raise RuntimeError(f"EdgeX获取元数据失败: {last_error}")
    
    @query_retry(default_return=(Decimal('0'), Decimal('0')))
    async def fetch_bbo_prices(self, contract_id: str) -> Tuple[Decimal, Decimal]:
        """
        获取订单簿最佳买卖价（BBO - Best Bid and Offer）
        
        Args:
            contract_id: 合约ID
            
        Returns:
            Tuple[Decimal, Decimal]: (best_bid, best_ask)
            - best_bid: 最高买价
            - best_ask: 最低卖价
            
        参考文档：EDGEX_ADAPTER_GUIDE.md 第335-398行
        """
        try:
            # 使用官方SDK或HTTP请求获取订单簿
            if self.sdk_client and EDGEX_SDK_AVAILABLE:
                depth_params = GetOrderBookDepthParams(
                    contract_id=contract_id,
                    limit=15
                )
                order_book = await self.sdk_client.quote.get_order_book_depth(depth_params)
                order_book_data = order_book.get('data', [])
            else:
                # 使用HTTP请求
                params = {'contractId': contract_id, 'limit': 15}
                response = await self._request('GET', 'api/v1/public/quote/getDepth', params=params)
                order_book_data = response.get('data', [])
            
            if not order_book_data:
                return Decimal('0'), Decimal('0')
            
            # 解析第一个订单簿条目
            order_book_entry = order_book_data[0]
            bids = order_book_entry.get('bids', [])
            asks = order_book_entry.get('asks', [])
            
            # 提取最佳价格
            best_bid = Decimal(str(bids[0]['price'])) if bids else Decimal('0')
            best_ask = Decimal(str(asks[0]['price'])) if asks else Decimal('0')
            
            return best_bid, best_ask
            
        except Exception as e:
            if self.logger:
                self.logger.warning(f"获取BBO价格失败 (contract_id={contract_id}): {e}")
            return Decimal('0'), Decimal('0')
    
    def round_to_tick(self, price: Decimal, tick_size: Decimal) -> Decimal:
        """
        将价格取整到tick_size
        
        Args:
            price: 原始价格
            tick_size: 价格最小变动单位
            
        Returns:
            Decimal: 取整后的价格
        """
        try:
            price = Decimal(str(price))
            tick_size = Decimal(str(tick_size))
            return price.quantize(tick_size, rounding=ROUND_HALF_UP)
        except Exception:
            return price
    
    async def calculate_maker_price(
        self,
        contract_id: str,
        side: str,
        tick_size: Decimal
    ) -> Decimal:
        """
        计算Maker订单价格（确保不会立即成交）
        
        策略：
        - Buy订单：略低于best_ask（best_ask - tick_size）
        - Sell订单：略高于best_bid（best_bid + tick_size）
        
        Args:
            contract_id: 合约ID
            side: 订单方向（'buy' 或 'sell'）
            tick_size: 价格最小变动单位
            
        Returns:
            Decimal: 调整后的Maker价格
            
        参考文档：EDGEX_ADAPTER_GUIDE.md 第400-453行
        """
        best_bid, best_ask = await self.fetch_bbo_prices(contract_id)
        
        if best_bid <= 0 or best_ask <= 0:
            raise ValueError(f"无效的BBO价格: bid={best_bid}, ask={best_ask}")
        
        if side.lower() == 'buy':
            # Buy订单：略低于best_ask
            order_price = best_ask - tick_size
        else:
            # Sell订单：略高于best_bid
            order_price = best_bid + tick_size
        
        return self.round_to_tick(order_price, tick_size) 