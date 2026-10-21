"""
交易所适配器统一错误处理

提供统一的API错误处理和重试机制
"""

import asyncio
import logging
from typing import Callable, TypeVar, Tuple, Optional, Any
from functools import wraps
from enum import Enum

logger = logging.getLogger(__name__)
# 🔥 关键修复：阻止日志传播到父logger（避免输出到终端UI）
logger.propagate = False

T = TypeVar('T')


class ErrorCategory(Enum):
    """错误分类"""
    NETWORK = "network"  # 网络错误
    AUTHENTICATION = "authentication"  # 认证错误
    RATE_LIMIT = "rate_limit"  # 限流错误
    SERVER_ERROR = "server_error"  # 服务器错误
    CLIENT_ERROR = "client_error"  # 客户端错误
    UNKNOWN = "unknown"  # 未知错误


def categorize_error(error: Exception) -> ErrorCategory:
    """
    分类错误
    
    Args:
        error: 异常对象
    
    Returns:
        错误分类
    """
    error_str = str(error).lower()
    error_type = type(error).__name__
    
    # 网络错误
    if any(keyword in error_str for keyword in ['connection', 'timeout', 'network', 'unreachable']):
        return ErrorCategory.NETWORK
    
    # 认证错误
    if any(keyword in error_str for keyword in ['auth', 'unauthorized', 'forbidden', 'invalid key']):
        return ErrorCategory.AUTHENTICATION
    
    # 限流错误
    if any(keyword in error_str for keyword in ['rate limit', '429', 'too many requests']):
        return ErrorCategory.RATE_LIMIT
    
    # 服务器错误
    if any(keyword in error_str for keyword in ['500', '502', '503', '504', 'server error']):
        return ErrorCategory.SERVER_ERROR
    
    # 客户端错误
    if any(keyword in error_str for keyword in ['400', '404', 'bad request']):
        return ErrorCategory.CLIENT_ERROR
    
    return ErrorCategory.UNKNOWN


def exchange_api_retry(
    max_retries: int = 3,
    backoff_base: float = 1.0,
    backoff_max: float = 10.0,
    exceptions: Tuple[type, ...] = (Exception,),
    retry_on: Optional[Tuple[ErrorCategory, ...]] = None,
    logger_instance: Optional[logging.Logger] = None
):
    """
    统一API重试装饰器
    
    Args:
        max_retries: 最大重试次数
        backoff_base: 基础退避时间（秒）
        backoff_max: 最大退避时间（秒）
        exceptions: 需要重试的异常类型
        retry_on: 需要重试的错误分类（如果为None则重试所有错误）
        logger_instance: 日志记录器（如果为None则使用默认logger）
    
    Example:
        @exchange_api_retry(max_retries=3, retry_on=(ErrorCategory.NETWORK, ErrorCategory.SERVER_ERROR))
        async def fetch_balance(self):
            ...
    """
    if retry_on is None:
        retry_on = (
            ErrorCategory.NETWORK,
            ErrorCategory.RATE_LIMIT,
            ErrorCategory.SERVER_ERROR
        )
    
    log = logger_instance or logger
    
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            last_error = None
            
            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                
                except exceptions as e:
                    last_error = e
                    error_category = categorize_error(e)
                    
                    # 检查是否需要重试
                    if error_category not in retry_on:
                        log.warning(
                            f"[API重试] {func.__name__} 遇到不可重试的错误: "
                            f"{error_category.value} - {e}"
                        )
                        raise
                    
                    # 检查是否达到最大重试次数
                    if attempt >= max_retries:
                        log.error(
                            f"[API重试] {func.__name__} 达到最大重试次数 ({max_retries})，"
                            f"最后错误: {error_category.value} - {e}"
                        )
                        raise
                    
                    # 计算退避时间
                    delay = min(backoff_base * (2 ** attempt), backoff_max)
                    
                    log.warning(
                        f"[API重试] {func.__name__} 第 {attempt + 1}/{max_retries} 次重试，"
                        f"错误: {error_category.value} - {e}，"
                        f"等待 {delay:.1f}秒后重试"
                    )
                    
                    await asyncio.sleep(delay)
            
            # 如果所有重试都失败，抛出最后一个错误
            if last_error:
                raise last_error
        
        return wrapper
    
    return decorator


def handle_exchange_error(
    error: Exception,
    operation: str,
    exchange_id: str = "unknown",
    logger_instance: Optional[logging.Logger] = None
) -> None:
    """
    统一处理交易所错误
    
    Args:
        error: 异常对象
        operation: 操作名称
        exchange_id: 交易所ID
        logger_instance: 日志记录器
    """
    log = logger_instance or logger
    error_category = categorize_error(error)
    
    error_messages = {
        ErrorCategory.NETWORK: f"网络错误: {error}",
        ErrorCategory.AUTHENTICATION: f"认证错误: {error}",
        ErrorCategory.RATE_LIMIT: f"API限流: {error}",
        ErrorCategory.SERVER_ERROR: f"服务器错误: {error}",
        ErrorCategory.CLIENT_ERROR: f"客户端错误: {error}",
        ErrorCategory.UNKNOWN: f"未知错误: {error}",
    }
    
    message = f"[{exchange_id}] {operation} - {error_messages.get(error_category, str(error))}"
    
    # 根据错误分类选择日志级别
    if error_category in (ErrorCategory.AUTHENTICATION, ErrorCategory.CLIENT_ERROR):
        log.error(message)
    elif error_category == ErrorCategory.RATE_LIMIT:
        log.warning(message)
    else:
        log.error(message, exc_info=True)

