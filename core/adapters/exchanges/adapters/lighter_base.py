"""
Lighter交易所适配器 - 基础模块

提供Lighter交易所的基础配置、工具方法和数据解析功能  
"""

from typing import Dict, Any, Optional, List
from decimal import Decimal
from datetime import datetime

from ..utils.logger_factory import get_exchange_logger

logger = get_exchange_logger("ExchangeAdapter.lighter")


class LighterBase:
    """Lighter交易所基础类"""

    # Lighter API端点
    MAINNET_URL = "https://mainnet.zklighter.elliot.ai"
    TESTNET_URL = "https://testnet.zklighter.elliot.ai"
    MAINNET_WS_URL = "wss://mainnet.zklighter.elliot.ai/stream"
    TESTNET_WS_URL = "wss://testnet.zklighter.elliot.ai/stream"

    # 交易类型常量（来自 SignerClient）
    TX_TYPE_CREATE_ORDER = 14
    TX_TYPE_CANCEL_ORDER = 15
    TX_TYPE_CANCEL_ALL_ORDERS = 16
    TX_TYPE_MODIFY_ORDER = 17
    TX_TYPE_WITHDRAW = 13
    TX_TYPE_TRANSFER = 12

    # 订单类型常量
    ORDER_TYPE_LIMIT = 0
    ORDER_TYPE_MARKET = 1
    ORDER_TYPE_STOP_LOSS = 2
    ORDER_TYPE_STOP_LOSS_LIMIT = 3
    ORDER_TYPE_TAKE_PROFIT = 4
    ORDER_TYPE_TAKE_PROFIT_LIMIT = 5
    ORDER_TYPE_TWAP = 6

    # 时间生效常量
    ORDER_TIME_IN_FORCE_IOC = 0  # Immediate or Cancel
    ORDER_TIME_IN_FORCE_GTT = 1  # Good Till Time
    ORDER_TIME_IN_FORCE_POST_ONLY = 2

    # 保证金模式
    CROSS_MARGIN_MODE = 0
    ISOLATED_MARGIN_MODE = 1

    # 订单状态映射
    ORDER_STATUS_MAP = {
        "pending": "open",
        "active": "open",
        "filled": "filled",
        "canceled": "canceled",
        "expired": "expired",
        "rejected": "rejected",
    }

    # 订单方向映射
    ORDER_SIDE_MAP = {
        True: "sell",   # is_ask=True -> sell
        False: "buy",   # is_ask=False -> buy
    }

    def __init__(self, config: Dict[str, Any]):
        """
        初始化Lighter基础类

        Args:
            config: 配置字典，包含API密钥、URL等信息
        """
        self.config = config
        self.testnet = config.get("testnet", False)

        # API 配置
        # 🔥 支持嵌套配置（从 api_config.auth 读取）或直接配置
        api_config = config.get("api_config", {})
        auth_config = api_config.get("auth", {}) if isinstance(api_config, dict) else {}
        
        # 优先从 auth 配置读取，如果没有则从顶层读取（向后兼容）
        self.auth_enabled = auth_config.get("enabled", False) if auth_config else config.get("auth_enabled", False)
        self.api_key_private_key = auth_config.get("api_key_private_key", "") if auth_config else config.get("api_key_private_key", "")
        self.account_index = auth_config.get("account_index", 0) if auth_config else config.get("account_index", 0)
        self.api_key_index = auth_config.get("api_key_index", 0) if auth_config else config.get("api_key_index", 0)
        
        # 🔥 auth_enabled 只控制WebSocket账户订阅，不影响REST API查询能力
        # 如果 auth_enabled 为 False，WebSocket将不订阅账户数据，但REST API仍可使用认证信息查询余额
        if not self.auth_enabled:
            logger.info("ℹ️  [Lighter] WebSocket账户订阅已禁用，将只订阅公共数据（market_stats和order_book）")
            # 🔥 注意：不清空account_index，因为REST API仍可能需要它来查询余额

        # URL配置
        self.base_url = self.TESTNET_URL if self.testnet else self.MAINNET_URL
        self.ws_url = self.TESTNET_WS_URL if self.testnet else self.MAINNET_WS_URL

        # 覆盖URL（如果配置中提供）
        if "api_url" in config:
            self.base_url = config["api_url"]
        
        # 🔥 支持多种ws_url字段名（兼容不同配置格式）
        if "ws_url" in config and config["ws_url"]:
            self.ws_url = config["ws_url"]
        elif self.testnet and "ws_testnet_url" in config and config["ws_testnet_url"]:
            self.ws_url = config["ws_testnet_url"]
        elif not self.testnet and "ws_mainnet_url" in config and config["ws_mainnet_url"]:
            self.ws_url = config["ws_mainnet_url"]

        # 🔥 确保ws_url不为None（兜底保护）
        if not self.ws_url:
            default_ws = self.TESTNET_WS_URL if self.testnet else self.MAINNET_WS_URL
            logger.warning(f"⚠️ ws_url为空，使用默认值: {default_ws}")
            self.ws_url = default_ws

        if config.get('official_transport_only'):
            from ..official_transport import validate_lighter_endpoints
            validate_lighter_endpoints(self.base_url,self.ws_url)

        # 市场信息缓存
        self._markets_cache: Dict[int, Dict[str, Any]] = {}
        self._symbol_to_market_index: Dict[str, int] = {}
        # 符号映射（用于兼容不同命名方式）
        self._symbol_mapping: Dict[str, str] = {}
        symbol_mapping_cfg = config.get("symbol_mapping")
        if isinstance(symbol_mapping_cfg, dict):
            self._symbol_mapping.update(symbol_mapping_cfg)

        # 初始化SignerClient（用于REST API认证和签名）
        # 🔥 即使auth_enabled=False，如果配置了认证信息，也初始化SignerClient用于REST API查询
        # auth_enabled只控制WebSocket是否订阅账户数据，不影响REST API的余额查询能力
        self.signer_client = None
        
        # 🔥 优先从环境变量读取认证信息，其次使用配置文件
        import os
        api_key_private_key = os.getenv('LIGHTER_API_KEY_PRIVATE_KEY') or self.api_key_private_key
        
        env_account_index = os.getenv('LIGHTER_ACCOUNT_INDEX')
        if env_account_index is not None:
            account_index = int(env_account_index)
        elif self.account_index is not None:
            account_index = self.account_index
        else:
            account_index = None
        
        env_api_key_index = os.getenv('LIGHTER_API_KEY_INDEX')
        if env_api_key_index is not None:
            api_key_index = int(env_api_key_index)
        elif self.api_key_index is not None:
            api_key_index = self.api_key_index
        else:
            api_key_index = None
        
        if account_index is not None and api_key_private_key:
            try:
                # 更新实例属性（使用环境变量的值）
                self.api_key_private_key = api_key_private_key
                self.account_index = account_index
                self.api_key_index = api_key_index
                
                # 兼容不同 lighter-sdk 版本的 SignerClient 参数
                self.signer_client = self._create_signer_client(
                    base_url=self.base_url,
                    account_index=account_index,
                    api_key_index=api_key_index,
                    api_key_private_key=api_key_private_key,
                )
                
                key_source = "环境变量" if os.getenv('LIGHTER_API_KEY_PRIVATE_KEY') else "配置文件"
                logger.info(
                    f"✅ [LighterBase] SignerClient初始化成功: "
                    f"account_index={account_index}, api_key_index={api_key_index} (从{key_source})"
                )
            except Exception as e:
                logger.warning(f"⚠️ [LighterBase] SignerClient初始化失败: {e}")
        else:
            logger.info("ℹ️  [LighterBase] 未配置认证信息，WebSocket将运行在公共模式")

        logger.info(
            f"Lighter基础配置初始化完成 - URL: {self.base_url}, 测试网: {self.testnet}")

    async def _close_signer_client(self):
        """每个模块只关闭自己持有的 signer，重复断开不会再次关闭。"""
        client = self.signer_client
        self.signer_client = None
        if client is not None:
            close = getattr(client, "close", None)
            if close:
                try:
                    await close()
                except Exception as exc:
                    logger.warning("关闭Lighter SignerClient时出错: %s", exc)

    def _create_signer_client(self, base_url: str, account_index: int, api_key_index: Optional[int], api_key_private_key: str):
        """
        兼容 lighter-sdk 旧/新版的 SignerClient 构造参数：
        - 新版(>=1.0.1): 需要 api_private_keys: Dict[int, str]
        - 旧版: 接受 private_key / api_key_index / account_index
        """
        from lighter import SignerClient

        api_key_index_val = api_key_index if api_key_index is not None else 0
        api_private_keys = {api_key_index_val: api_key_private_key}

        try:
            # 新版签名
            client = SignerClient(
                url=base_url,
                account_index=account_index,
                api_private_keys=api_private_keys,
            )
        except TypeError:
            # 兼容旧版签名
            client = SignerClient(
                url=base_url,
                private_key=api_key_private_key,
                account_index=account_index,
                api_key_index=api_key_index,
            )
        if self.config.get('official_transport_only'):
            from ..official_transport import harden_api_client
            harden_api_client(client.api_client)
        return client

    def get_base_url(self) -> str:
        """获取REST API基础URL"""
        return self.base_url

    def get_ws_url(self) -> str:
        """获取WebSocket URL"""
        return self.ws_url

    # ============= 符号转换方法 =============

    def normalize_symbol(self, symbol: str) -> str:
        """
        标准化交易对符号

        Args:
            symbol: 原始符号，如 "BTC-USD", "BTCUSD", "PAXGUSD", "XAUUSD"

        Returns:
            标准化后的符号
            
        注意：
        - 如果symbol已在markets缓存中，直接返回（避免破坏正确的symbol）
        - 否则尝试自动转换为 BASE-QUOTE 格式
        """
        if not symbol:
            return symbol
            
        symbol_upper = symbol.upper().replace("_", "-")

        def _match_candidate(candidate: str) -> Optional[str]:
            if not candidate:
                return None
            if candidate in self._symbol_to_market_index or candidate in self._markets_cache:
                return candidate
            return None

        match = _match_candidate(symbol_upper)
        if match:
            return match

        candidates = []

        # 无分隔符情况，例如 PAXGUSD
        if "-" not in symbol_upper and len(symbol_upper) > 3:
            if symbol_upper.endswith("USD"):
                base = symbol_upper[:-3]
                candidates.append(f"{base}-USD")
                candidates.append(f"{base}USD")
            elif symbol_upper.endswith("USDC"):
                base = symbol_upper[:-4]
                candidates.append(f"{base}-USDC")
                candidates.append(f"{base}USDC")
            elif symbol_upper.endswith("USDT"):
                base = symbol_upper[:-4]
                candidates.append(f"{base}-USDT")
                candidates.append(f"{base}USDT")

        suffixes = ["-USD-PERP", "-USDC-PERP", "-USD", "-USDC", "-USDT"]
        for suffix in suffixes:
            if symbol_upper.endswith(suffix):
                base = symbol_upper[: -len(suffix)]
                if not base:
                    continue
                if "PERP" in suffix:
                    candidates.extend([
                        base,
                        f"{base}USD",
                        f"{base}-USD",
                        f"{base}USDC",
                        f"{base}-USDC",
                        f"{base}USDT",
                        f"{base}-USDT",
                    ])
                else:
                    quote = suffix.replace("-", "")
                    candidates.extend([
                        f"{base}-{quote}",
                        f"{base}{quote}"
                    ])

        candidates.append(symbol_upper)
        for candidate in candidates:
            match = _match_candidate(candidate)
            if match:
                return match

        return symbol_upper

    def get_market_index(self, symbol: str) -> Optional[int]:
        """
        获取交易对的市场索引

        Args:
            symbol: 交易对符号

        Returns:
            市场索引，如果不存在返回None
        """
        if not symbol:
            return None

        # 特殊直连映射：已知 ETH/USDC 现货 market_id=2048
        symbol_upper = symbol.upper().replace(":", "/").replace("-", "/")
        # 现货标准格式可能带有 SPOT 后缀，先去掉再匹配
        symbol_upper_clean = symbol_upper
        for suffix in ("/SPOT", "-SPOT", "SPOT"):
            if symbol_upper_clean.endswith(suffix):
                symbol_upper_clean = symbol_upper_clean[: -len(suffix)]
                if symbol_upper_clean.endswith("/"):
                    symbol_upper_clean = symbol_upper_clean[:-1]
                break
        if symbol_upper in {"ETH/USDC", "ETHUSDC", "ETH-USD", "ETHUSD"}:
            return 2048
        if symbol_upper_clean in {"ETH/USDC", "ETHUSDC", "ETH-USD", "ETHUSD"}:
            return 2048

        normalized = self.normalize_symbol(symbol)
        mapped = self._symbol_mapping.get(symbol) or self._symbol_mapping.get(normalized)
        if mapped:
            normalized = self.normalize_symbol(mapped)
            logger.debug(f"🔄 符号映射: {symbol} -> {normalized}")

        market_index = self._symbol_to_market_index.get(normalized)
        if market_index is None:
            # 尝试直接匹配基础币种
            base = normalized.split('-')[0] if '-' in normalized else normalized
            market_index = self._symbol_to_market_index.get(base)
            
            # 🔥 添加详细日志来诊断查找失败
            logger.warning(
                f"❌ 未找到市场索引: symbol={symbol}, normalized={normalized}, "
                f"base={base}, cached_keys={list(self._symbol_to_market_index.keys())[:5]}"
            )
        else:
            logger.debug(f"✅ 找到市场索引: {symbol} -> {market_index}")

        return market_index

    def update_markets_cache(self, markets: List[Dict[str, Any]]):
        """
        更新市场信息缓存

        Args:
            markets: 市场信息列表
        """
        for market in markets:
            # 🔥 注意：不能用 or，因为 market_index=0 会被视为 False（如ETH的market_index=0）
            market_index = market.get("market_index")
            if market_index is None:
                market_index = market.get("market_id")
            if market_index is not None:
                self._markets_cache[market_index] = market
                # 构建符号到索引的映射
                symbol = market.get("symbol", "")
                if symbol:
                    self._symbol_to_market_index[symbol] = market_index

        logger.info(f"更新市场缓存完成: {len(self._markets_cache)} 个市场")

    # ============= 数据解析辅助方法 =============

    @staticmethod
    def _safe_decimal(value: Any, default: Decimal = Decimal("0")) -> Decimal:
        """
        安全转换为Decimal类型

        Args:
            value: 待转换的值
            default: 默认值

        Returns:
            Decimal类型的值
        """
        if value is None or value == "":
            return default
        try:
            return Decimal(str(value))
        except (ValueError, TypeError):
            return default

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        """
        安全转换为float类型

        Args:
            value: 待转换的值
            default: 默认值

        Returns:
            float类型的值
        """
        if value is None or value == "":
            return default
        try:
            return float(value)
        except (ValueError, TypeError):
            return default

    @staticmethod
    def _safe_int(value: Any, default: int = 0) -> int:
        """
        安全转换为int类型

        Args:
            value: 待转换的值
            default: 默认值

        Returns:
            int类型的值
        """
        if value is None or value == "":
            return default
        try:
            return int(value)
        except (ValueError, TypeError):
            return default

    @staticmethod
    def _parse_timestamp(timestamp: Any) -> Optional[int]:
        """
        解析时间戳

        Args:
            timestamp: 时间戳（秒或毫秒）

        Returns:
            毫秒级时间戳
        """
        if timestamp is None:
            return None

        try:
            ts = int(timestamp)
            # 如果是秒级时间戳，转换为毫秒
            if ts < 10000000000:
                ts = ts * 1000
            return ts
        except (ValueError, TypeError):
            return None

    def _parse_order_side(self, is_ask: bool) -> str:
        """
        解析订单方向

        Args:
            is_ask: Lighter的is_ask字段

        Returns:
            标准化的订单方向 ("buy" 或 "sell")
        """
        return self.ORDER_SIDE_MAP.get(is_ask, "buy")

    def _parse_order_status(self, status: str) -> str:
        """
        解析订单状态

        Args:
            status: Lighter的订单状态

        Returns:
            标准化的订单状态
        """
        status_lower = status.lower() if status else "unknown"
        return self.ORDER_STATUS_MAP.get(status_lower, status_lower)

    def _parse_order_type(self, order_type: int) -> str:
        """
        解析订单类型

        Args:
            order_type: Lighter的订单类型常量

        Returns:
            标准化的订单类型字符串
        """
        type_map = {
            self.ORDER_TYPE_LIMIT: "limit",
            self.ORDER_TYPE_MARKET: "market",
            self.ORDER_TYPE_STOP_LOSS: "stop_loss",
            self.ORDER_TYPE_STOP_LOSS_LIMIT: "stop_loss_limit",
            self.ORDER_TYPE_TAKE_PROFIT: "take_profit",
            self.ORDER_TYPE_TAKE_PROFIT_LIMIT: "take_profit_limit",
            self.ORDER_TYPE_TWAP: "twap",
        }
        return type_map.get(order_type, "unknown")

    def _parse_time_in_force(self, tif: int) -> str:
        """
        解析时间生效类型

        Args:
            tif: Lighter的时间生效常量

        Returns:
            标准化的时间生效类型字符串
        """
        tif_map = {
            self.ORDER_TIME_IN_FORCE_IOC: "IOC",
            self.ORDER_TIME_IN_FORCE_GTT: "GTT",
            self.ORDER_TIME_IN_FORCE_POST_ONLY: "POST_ONLY",
        }
        return tif_map.get(tif, "GTT")

    # ============= 精度和数量处理 =============

    def format_quantity(self, quantity: Decimal, symbol: str) -> str:
        """
        格式化数量

        Args:
            quantity: 数量
            symbol: 交易对符号

        Returns:
            格式化后的数量字符串
        """
        # Lighter使用精度自适应，这里保留8位小数
        return f"{quantity:.8f}".rstrip('0').rstrip('.')

    def format_price(self, price: Decimal, symbol: str) -> str:
        """
        格式化价格

        Args:
            price: 价格
            symbol: 交易对符号

        Returns:
            格式化后的价格字符串
        """
        # Lighter使用精度自适应，这里保留8位小数
        return f"{price:.8f}".rstrip('0').rstrip('.')

    # ============= 错误处理 =============

    def parse_error(self, error: Any) -> str:
        """
        解析错误信息

        Args:
            error: 错误对象

        Returns:
            错误信息字符串
        """
        if error is None:
            return ""

        if isinstance(error, str):
            return error

        if isinstance(error, Exception):
            return str(error).strip().split("\n")[-1]

        return str(error)

    def __repr__(self) -> str:
        """字符串表示"""
        return f"LighterBase(testnet={self.testnet}, url={self.base_url})"
