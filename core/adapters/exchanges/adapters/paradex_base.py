"""
Paradex 交易所基础模块

提供基础配置、符号映射和工具方法
"""

import logging
from typing import Dict, List, Optional, Any
from decimal import Decimal
from datetime import datetime


class ParadexBase:
    """
    Paradex 基础配置和工具类
    
    职责：
    - 基础配置管理
    - 符号格式转换（标准格式 <-> Paradex格式）
    - 数据格式标准化
    - 工具方法和常量定义
    """
    
    # 默认配置
    DEFAULT_REST_URL = "https://api.prod.paradex.trade/v1"
    DEFAULT_WS_URL = "wss://ws.api.prod.paradex.trade/v1"
    DEFAULT_TESTNET_REST_URL = "https://api.testnet.paradex.trade/v1"
    DEFAULT_TESTNET_WS_URL = "wss://ws.api.testnet.paradex.trade/v1"
    
    # WebSocket配置
    WS_PING_INTERVAL = 55  # Paradex服务器每55秒发送一次ping
    WS_PONG_TIMEOUT = 5    # 必须在5秒内回复pong
    
    # 符号格式说明：
    # 标准格式: BTC/USDC:PERP (统一格式，用于系统内部)
    # Paradex格式: BTC-USD-PERP (交易所原生格式)
    
    def __init__(self, config=None):
        """
        初始化Paradex基础类
        
        Args:
            config: 交易所配置对象
        """
        self.config = config
        self.logger: Optional[logging.Logger] = None
        self._supported_symbols: List[str] = []
        
        # 符号映射缓存（标准格式 -> Paradex格式）
        self._symbol_mapping: Dict[str, str] = {}
        # 反向映射缓存（Paradex格式 -> 标准格式）
        self._reverse_symbol_mapping: Dict[str, str] = {}
        
    def set_logger(self, logger: logging.Logger):
        """设置日志器"""
        self.logger = logger
        
    def get_base_url(self) -> str:
        """
        获取REST API基础URL
        
        Returns:
            str: API基础URL
        """
        if self.config and hasattr(self.config, 'testnet') and self.config.testnet:
            return self.DEFAULT_TESTNET_REST_URL
        return self.DEFAULT_REST_URL
        
    def get_ws_url(self) -> str:
        """
        获取WebSocket URL
        
        Returns:
            str: WebSocket URL
        """
        if self.config and hasattr(self.config, 'testnet') and self.config.testnet:
            return self.DEFAULT_TESTNET_WS_URL
        return self.DEFAULT_WS_URL
        
    def normalize_symbol(self, paradex_symbol: str) -> str:
        """
        将Paradex格式符号转换为标准格式
        
        Paradex格式: BTC-USD-PERP, ETH-USD-PERP
        标准格式: BTC/USDC:PERP, ETH/USDC:PERP
        
        Args:
            paradex_symbol: Paradex格式的交易对符号
            
        Returns:
            str: 标准格式的交易对符号
        """
        # 检查缓存
        if paradex_symbol in self._reverse_symbol_mapping:
            return self._reverse_symbol_mapping[paradex_symbol]
            
        try:
            # Paradex格式: BTC-USD-PERP -> 拆分为 ['BTC', 'USD', 'PERP']
            parts = paradex_symbol.split('-')
            
            if len(parts) < 3:
                # 如果格式不符合预期，返回原值
                if self.logger:
                    self.logger.warning(f"⚠️ [normalize] 无法解析Paradex符号格式: {paradex_symbol}, parts={parts}")
                return paradex_symbol
                
            base = parts[0]      # BTC
            quote = parts[1]     # USD -> USDC
            market_type = parts[2]  # PERP
            
            # Paradex使用USD但实际结算币种是USDC
            if quote == "USD":
                quote = "USDC"
                
            # 标准格式: BTC/USDC:PERP
            standard_symbol = f"{base}/{quote}:{market_type}"
            
            # 缓存映射
            self._reverse_symbol_mapping[paradex_symbol] = standard_symbol
            self._symbol_mapping[standard_symbol] = paradex_symbol
            
            return standard_symbol
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ [normalize] 符号格式转换失败 {paradex_symbol}: {e}", exc_info=True)
            return paradex_symbol
            
    def convert_to_paradex_symbol(self, standard_symbol: str) -> str:
        """
        将标准格式符号转换为Paradex格式
        
        标准格式支持两种：
        1. BTC/USDC:PERP（标准格式1，使用/和:）
        2. BTC-USDC-PERP（标准格式2，使用-分隔）
        Paradex格式: BTC-USD-PERP, ETH-USD-PERP
        
        Args:
            standard_symbol: 标准格式的交易对符号
            
        Returns:
            str: Paradex格式的交易对符号
        """
        # 检查缓存
        if standard_symbol in self._symbol_mapping:
            return self._symbol_mapping[standard_symbol]
            
        try:
            # 🔥 新增：支持BTC-USDC-PERP格式（监控系统使用的格式）
            # 检测格式：如果有冒号，使用格式1；否则尝试格式2
            if ':' in standard_symbol:
                # 标准格式1: BTC/USDC:PERP
                # 第一步：按冒号分割 -> ['BTC/USDC', 'PERP']
                pair_part, market_type = standard_symbol.split(':')
                # 第二步：按斜杠分割 -> ['BTC', 'USDC']
                if '/' in pair_part:
                    base, quote = pair_part.split('/')
                else:
                    base = pair_part
                    quote = "USDC"
            elif '-' in standard_symbol:
                # 标准格式2: BTC-USDC-PERP
                # 按连字符分割 -> ['BTC', 'USDC', 'PERP']
                parts = standard_symbol.split('-')
                if len(parts) >= 3:
                    base = parts[0]
                    quote = parts[1]
                    market_type = parts[2]
                elif len(parts) == 2:
                    base = parts[0]
                    quote = parts[1]
                    market_type = "PERP"
                else:
                    # 只有一个部分，当作base
                    base = parts[0]
                    quote = "USDC"
                    market_type = "PERP"
            else:
                # 没有分隔符，整个当作base
                base = standard_symbol
                quote = "USDC"
                market_type = "PERP"
                
            # Paradex使用USD而不是USDC
            if quote == "USDC":
                quote = "USD"
                
            # Paradex格式: BTC-USD-PERP
            paradex_symbol = f"{base}-{quote}-{market_type}"
            
            # 缓存映射
            self._symbol_mapping[standard_symbol] = paradex_symbol
            self._reverse_symbol_mapping[paradex_symbol] = standard_symbol
            
            return paradex_symbol
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"符号格式转换失败 {standard_symbol}: {e}")
            return standard_symbol
            
    def map_symbol(self, symbol: str) -> str:
        """
        符号映射（兼容方法）
        
        自动检测符号格式并转换为Paradex格式
        
        Args:
            symbol: 交易对符号（任意格式）
            
        Returns:
            str: Paradex格式符号
        """
        # 如果包含斜杠，认为是标准格式，转换为Paradex格式
        if '/' in symbol:
            return self.convert_to_paradex_symbol(symbol)
        # 否则认为已经是Paradex格式，直接返回
        return symbol
        
    def reverse_map_symbol(self, exchange_symbol: str) -> str:
        """
        反向符号映射（兼容方法）
        
        将Paradex格式转换为标准格式
        
        Args:
            exchange_symbol: Paradex格式符号
            
        Returns:
            str: 标准格式符号
        """
        return self.normalize_symbol(exchange_symbol)
        
    def _safe_decimal(self, value: Any, default: Decimal = Decimal('0')) -> Decimal:
        """
        安全转换为Decimal类型
        
        Args:
            value: 待转换的值
            default: 转换失败时的默认值
            
        Returns:
            Decimal: 转换后的值
        """
        if value is None:
            return default
            
        try:
            if isinstance(value, Decimal):
                return value
            elif isinstance(value, (int, float)):
                return Decimal(str(value))
            elif isinstance(value, str):
                if value == '' or value == 'null':
                    return default
                return Decimal(value)
            else:
                return default
        except Exception as e:
            if self.logger:
                self.logger.warning(f"Decimal转换失败 {value}: {e}")
            return default
            
    def _safe_int(self, value: Any, default: int = 0) -> int:
        """
        安全转换为int类型
        
        Args:
            value: 待转换的值
            default: 转换失败时的默认值
            
        Returns:
            int: 转换后的值
        """
        if value is None:
            return default
            
        try:
            if isinstance(value, int):
                return value
            elif isinstance(value, (float, Decimal)):
                return int(value)
            elif isinstance(value, str):
                if value == '' or value == 'null':
                    return default
                return int(float(value))
            else:
                return default
        except Exception as e:
            if self.logger:
                self.logger.warning(f"int转换失败 {value}: {e}")
            return default
            
    def _timestamp_to_datetime(self, timestamp: Any) -> Optional[datetime]:
        """
        将时间戳转换为datetime对象
        
        Paradex使用纳秒时间戳或秒时间戳
        
        Args:
            timestamp: 时间戳（秒或毫秒或纳秒）
            
        Returns:
            Optional[datetime]: datetime对象，转换失败返回None
        """
        if timestamp is None:
            return None
            
        try:
            ts_int = int(timestamp)
            
            # 判断时间戳精度
            if ts_int > 1e15:  # 纳秒（18位）
                return datetime.fromtimestamp(ts_int / 1e9)
            elif ts_int > 1e12:  # 毫秒（13位）
                return datetime.fromtimestamp(ts_int / 1000)
            else:  # 秒（10位）
                return datetime.fromtimestamp(ts_int)
                
        except Exception as e:
            if self.logger:
                self.logger.warning(f"时间戳转换失败 {timestamp}: {e}")
            return None
            
    def get_market_type_from_symbol(self, symbol: str) -> str:
        """
        从符号中提取市场类型
        
        Args:
            symbol: 交易对符号（标准格式或Paradex格式）
            
        Returns:
            str: 市场类型（perpetual, spot等）
        """
        try:
            # 标准格式: BTC/USDC:PERP
            if ':' in symbol:
                market_type = symbol.split(':')[1].upper()
                if market_type == 'PERP':
                    return 'perpetual'
                return market_type.lower()
                
            # Paradex格式: BTC-USD-PERP
            elif '-' in symbol:
                parts = symbol.split('-')
                if len(parts) >= 3:
                    market_type = parts[2].upper()
                    if market_type == 'PERP':
                        return 'perpetual'
                    return market_type.lower()
                    
            return 'perpetual'  # 默认为永续合约
            
        except Exception:
            return 'perpetual'

