"""
交易所适配器统一重连管理器

提供统一的WebSocket重连逻辑，包括：
- 指数退避策略
- 重连次数限制
- 网络连通性检查
- 重连状态管理
"""

import asyncio
import logging
from typing import Callable, Optional, Dict, Any
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)
# 🔥 关键修复：阻止日志传播到父logger（避免输出到终端UI）
logger.propagate = False


class ReconnectStrategy(Enum):
    """重连策略"""
    EXPONENTIAL = "exponential"  # 指数退避
    LINEAR = "linear"  # 线性退避
    FIXED = "fixed"  # 固定延迟


@dataclass
class ReconnectConfig:
    """重连配置"""
    max_retries: int = 10  # 最大重试次数（-1表示无限重试）
    base_delay: float = 2.0  # 基础延迟（秒）
    max_delay: float = 300.0  # 最大延迟（秒）
    backoff_multiplier: float = 2.0  # 退避乘数
    strategy: ReconnectStrategy = ReconnectStrategy.EXPONENTIAL
    enable_network_check: bool = True  # 是否启用网络连通性检查


class WebSocketReconnectManager:
    """
    统一WebSocket重连管理器
    
    提供统一的重连逻辑，支持：
    - 指数退避策略
    - 重连次数限制
    - 网络连通性检查（可选）
    - 重连状态管理
    """
    
    def __init__(
        self,
        exchange_id: str,
        config: Optional[ReconnectConfig] = None,
        network_check_func: Optional[Callable[[], bool]] = None
    ):
        """
        初始化重连管理器
        
        Args:
            exchange_id: 交易所ID（用于日志标识）
            config: 重连配置，如果为None则使用默认配置
            network_check_func: 网络连通性检查函数（可选）
        """
        self.exchange_id = exchange_id
        self.config = config or ReconnectConfig()
        self.network_check_func = network_check_func
        
        self._reconnect_attempts = 0
        self._reconnecting = False
        self._last_reconnect_time: Optional[float] = None
        
        self._stats = {
            'total_attempts': 0,
            'successful_reconnects': 0,
            'failed_reconnects': 0,
            'network_check_failures': 0,
        }
    
    async def reconnect(
        self,
        connect_func: Callable[[], Any],
        cleanup_func: Optional[Callable[[], Any]] = None
    ) -> bool:
        """
        执行重连
        
        Args:
            connect_func: 连接函数（异步）
            cleanup_func: 清理函数（异步，可选）
        
        Returns:
            bool: True表示重连成功，False表示重连失败或达到最大重试次数
        """
        if self._reconnecting:
            logger.debug(f"[{self.exchange_id}] 重连已在进行中，跳过")
            return False
        
        self._reconnecting = True
        
        try:
            while True:
                # 检查重试次数限制
                if self.config.max_retries > 0 and self._reconnect_attempts >= self.config.max_retries:
                    logger.warning(
                        f"[{self.exchange_id}] 达到最大重试次数 ({self.config.max_retries})，停止重连"
                    )
                    return False
                
                self._reconnect_attempts += 1
                self._stats['total_attempts'] += 1
                
                # 计算延迟时间
                delay = self._calculate_delay()
                
                logger.info(
                    f"🔄 [{self.exchange_id}] 重连尝试 #{self._reconnect_attempts}，"
                    f"延迟 {delay:.1f}秒"
                )
                
                # 等待延迟时间
                await asyncio.sleep(delay)
                
                # 网络连通性检查（可选）
                if self.config.enable_network_check and self.network_check_func:
                    try:
                        network_ok = await self._check_network()
                        if not network_ok:
                            logger.warning(
                                f"⚠️ [{self.exchange_id}] 网络连通性检查失败，"
                                f"跳过本次重连尝试"
                            )
                            self._stats['network_check_failures'] += 1
                            continue  # 继续下一次重试
                    except Exception as e:
                        logger.warning(
                            f"⚠️ [{self.exchange_id}] 网络检查异常: {e}，继续重连"
                        )
                
                # 清理旧连接（可选）
                if cleanup_func:
                    try:
                        await cleanup_func()
                    except Exception as e:
                        logger.warning(
                            f"⚠️ [{self.exchange_id}] 清理旧连接失败: {e}，继续重连"
                        )
                
                # 尝试重连
                try:
                    result = await connect_func()
                    
                    if result:
                        # 重连成功
                        logger.info(
                            f"✅ [{self.exchange_id}] 重连成功（第 {self._reconnect_attempts} 次尝试）"
                        )
                        self._stats['successful_reconnects'] += 1
                        self._reconnect_attempts = 0  # 重置重试计数
                        self._last_reconnect_time = asyncio.get_event_loop().time()
                        return True
                    else:
                        # 连接函数返回False，继续重试
                        logger.warning(
                            f"⚠️ [{self.exchange_id}] 连接函数返回False，继续重试"
                        )
                        self._stats['failed_reconnects'] += 1
                        
                except Exception as e:
                    logger.error(
                        f"❌ [{self.exchange_id}] 重连失败: {type(e).__name__}: {e}"
                    )
                    self._stats['failed_reconnects'] += 1
                    # 继续下一次重试
        
        finally:
            self._reconnecting = False
        
        return False
    
    def _calculate_delay(self) -> float:
        """计算延迟时间"""
        if self.config.strategy == ReconnectStrategy.EXPONENTIAL:
            # 指数退避
            delay = self.config.base_delay * (
                self.config.backoff_multiplier ** (self._reconnect_attempts - 1)
            )
        elif self.config.strategy == ReconnectStrategy.LINEAR:
            # 线性退避
            delay = self.config.base_delay * self._reconnect_attempts
        else:
            # 固定延迟
            delay = self.config.base_delay
        
        # 限制最大延迟
        return min(delay, self.config.max_delay)
    
    async def _check_network(self) -> bool:
        """检查网络连通性"""
        if not self.network_check_func:
            return True
        
        try:
            if asyncio.iscoroutinefunction(self.network_check_func):
                return await self.network_check_func()
            else:
                return self.network_check_func()
        except Exception as e:
            logger.warning(f"[{self.exchange_id}] 网络检查异常: {e}")
            return False
    
    def reset(self) -> None:
        """重置重连状态"""
        self._reconnect_attempts = 0
        self._reconnecting = False
    
    def get_stats(self) -> Dict[str, Any]:
        """获取重连统计信息"""
        return {
            'exchange_id': self.exchange_id,
            'current_attempts': self._reconnect_attempts,
            'is_reconnecting': self._reconnecting,
            'last_reconnect_time': self._last_reconnect_time,
            **self._stats
        }

