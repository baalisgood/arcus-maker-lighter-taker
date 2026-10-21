"""
交易所适配器工具模块

提供日志优化、格式化、缓存管理、错误处理、重连管理等工具函数和类

使用状态说明：
- ✅ cache_config: 已使用（统一缓存配置）
- ⚠️ adapter_logger: 未使用（可选，统一日志工具）
- ⚠️ cache_manager: 未使用（可选，统一缓存管理器）
- ⚠️ reconnect_manager: 未使用（可选，统一重连管理器）
- ⚠️ error_handler: 未使用（可选，统一错误处理）

详细说明请参考 README.md
"""

from .setup_logging import (
    LoggingConfig,
    setup_optimized_logging,
    format_order_log,
    format_ws_log,
    format_sync_log,
    simplify_order_id,
)

from .log_formatter import (
    CompactFormatter,
    DetailedFormatter,
    ColoredFormatter,
)

# ✅ 已使用的工具
from .cache_config import (
    CACHE_TTL_CONFIG,
    BALANCE_REFRESH_INTERVAL,
    get_cache_ttl,
    get_balance_refresh_interval,
)

# ⚠️ 可选工具（未使用，但可以直接使用）
from .adapter_logger import AdapterLogger
from .cache_manager import ExchangeCacheManager, CacheEntry
from .reconnect_manager import (
    WebSocketReconnectManager,
    ReconnectConfig,
    ReconnectStrategy,
)
from .error_handler import (
    exchange_api_retry,
    ErrorCategory,
    categorize_error,
    handle_exchange_error,
)

__all__ = [
    # 日志配置（已使用）
    'LoggingConfig',
    'setup_optimized_logging',

    # 格式化函数（已使用）
    'format_order_log',
    'format_ws_log',
    'format_sync_log',
    'simplify_order_id',

    # 格式化器类（已使用）
    'CompactFormatter',
    'DetailedFormatter',
    'ColoredFormatter',

    # 缓存配置（已使用）
    'CACHE_TTL_CONFIG',
    'BALANCE_REFRESH_INTERVAL',
    'get_cache_ttl',
    'get_balance_refresh_interval',

    # 可选工具（未使用，但可以直接使用）
    'AdapterLogger',
    'ExchangeCacheManager',
    'CacheEntry',
    'WebSocketReconnectManager',
    'ReconnectConfig',
    'ReconnectStrategy',
    'exchange_api_retry',
    'ErrorCategory',
    'categorize_error',
    'handle_exchange_error',
]
