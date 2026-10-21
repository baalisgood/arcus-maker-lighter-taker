"""
交易所适配器统一缓存管理器

提供统一的缓存管理功能，包括：
- 缓存存储和检索
- 自动过期检查
- 缓存统计和清理
"""

from typing import Dict, Any, Optional, TypeVar, Generic
from datetime import datetime, timedelta
from dataclasses import dataclass
import logging

from .cache_config import get_cache_ttl, CACHE_TTL_CONFIG

logger = logging.getLogger(__name__)
# 🔥 关键修复：阻止日志传播到父logger（避免输出到终端UI）
logger.propagate = False

T = TypeVar('T')


@dataclass
class CacheEntry(Generic[T]):
    """缓存条目"""
    data: T
    timestamp: datetime
    ttl: int  # 生存时间（秒）
    
    def is_expired(self) -> bool:
        """检查缓存是否过期"""
        age = (datetime.now() - self.timestamp).total_seconds()
        return age >= self.ttl


class ExchangeCacheManager:
    """
    交易所适配器统一缓存管理器
    
    提供统一的缓存管理功能，支持多种缓存类型：
    - balance: 余额缓存
    - position: 持仓缓存
    - orderbook: 订单簿缓存
    - ticker: Ticker缓存
    - market_info: 市场信息缓存
    """
    
    def __init__(self, exchange_id: str = "unknown"):
        """
        初始化缓存管理器
        
        Args:
            exchange_id: 交易所ID（用于日志标识）
        """
        self.exchange_id = exchange_id
        self._caches: Dict[str, Dict[str, CacheEntry]] = {
            'balance': {},
            'position': {},
            'orderbook': {},
            'ticker': {},
            'market_info': {},
            'user_stats': {},
        }
        self._stats = {
            'hits': 0,
            'misses': 0,
            'expired': 0,
            'updates': 0,
        }
    
    def get(self, cache_type: str, key: str) -> Optional[Any]:
        """
        获取缓存数据
        
        Args:
            cache_type: 缓存类型（balance, position, orderbook等）
            key: 缓存键（如symbol, currency等）
        
        Returns:
            缓存数据，如果不存在或已过期则返回None
        """
        if cache_type not in self._caches:
            logger.warning(f"[{self.exchange_id}] 未知的缓存类型: {cache_type}")
            return None
        
        cache = self._caches[cache_type]
        
        if key not in cache:
            self._stats['misses'] += 1
            return None
        
        entry = cache[key]
        
        if entry.is_expired():
            # 缓存过期，删除
            del cache[key]
            self._stats['expired'] += 1
            self._stats['misses'] += 1
            return None
        
        self._stats['hits'] += 1
        return entry.data
    
    def set(self, cache_type: str, key: str, data: Any, ttl: Optional[int] = None) -> None:
        """
        设置缓存数据
        
        Args:
            cache_type: 缓存类型
            key: 缓存键
            data: 要缓存的数据
            ttl: 生存时间（秒），如果为None则使用默认TTL
        """
        if cache_type not in self._caches:
            logger.warning(f"[{self.exchange_id}] 未知的缓存类型: {cache_type}")
            return
        
        if ttl is None:
            ttl = get_cache_ttl(cache_type)
        
        cache = self._caches[cache_type]
        cache[key] = CacheEntry(
            data=data,
            timestamp=datetime.now(),
            ttl=ttl
        )
        self._stats['updates'] += 1
    
    def delete(self, cache_type: str, key: str) -> None:
        """删除缓存条目"""
        if cache_type in self._caches:
            self._caches[cache_type].pop(key, None)
    
    def clear(self, cache_type: Optional[str] = None) -> None:
        """
        清空缓存
        
        Args:
            cache_type: 缓存类型，如果为None则清空所有缓存
        """
        if cache_type:
            if cache_type in self._caches:
                self._caches[cache_type].clear()
        else:
            for cache in self._caches.values():
                cache.clear()
    
    def cleanup_expired(self, cache_type: Optional[str] = None) -> int:
        """
        清理过期缓存
        
        Args:
            cache_type: 缓存类型，如果为None则清理所有类型的过期缓存
        
        Returns:
            清理的缓存条目数量
        """
        cleaned = 0
        
        cache_types = [cache_type] if cache_type else self._caches.keys()
        
        for ct in cache_types:
            if ct not in self._caches:
                continue
            
            cache = self._caches[ct]
            expired_keys = [
                key for key, entry in cache.items()
                if entry.is_expired()
            ]
            
            for key in expired_keys:
                del cache[key]
                cleaned += 1
                self._stats['expired'] += 1
        
        return cleaned
    
    def get_stats(self) -> Dict[str, Any]:
        """
        获取缓存统计信息
        
        Returns:
            统计信息字典
        """
        total_entries = sum(len(cache) for cache in self._caches.values())
        hit_rate = (
            self._stats['hits'] / (self._stats['hits'] + self._stats['misses'])
            if (self._stats['hits'] + self._stats['misses']) > 0
            else 0.0
        )
        
        return {
            'exchange_id': self.exchange_id,
            'total_entries': total_entries,
            'hits': self._stats['hits'],
            'misses': self._stats['misses'],
            'expired': self._stats['expired'],
            'updates': self._stats['updates'],
            'hit_rate': hit_rate,
            'cache_sizes': {
                cache_type: len(cache)
                for cache_type, cache in self._caches.items()
            }
        }
    
    def reset_stats(self) -> None:
        """重置统计信息"""
        self._stats = {
            'hits': 0,
            'misses': 0,
            'expired': 0,
            'updates': 0,
        }

