"""
网络状态上报工具

用于在适配器/执行层检测到网络异常或恢复时，统一通知风控模块。
"""

from __future__ import annotations

import logging
from typing import Callable, Dict, Optional

logger = logging.getLogger(__name__)
# 🔥 关键修复：阻止日志传播到父logger（避免输出到终端UI）
logger.propagate = False

_failure_handler: Optional[Callable[[str, str], None]] = None
_recovery_handler: Optional[Callable[[str], None]] = None
_active_failures: Dict[str, bool] = {}


def register_network_handlers(
    on_failure: Callable[[str, str], None],
    on_recovered: Callable[[str], None],
) -> None:
    """
    注册网络故障/恢复回调
    """

    global _failure_handler, _recovery_handler
    _failure_handler = on_failure
    _recovery_handler = on_recovered
    logger.info("✅ [NetworkState] 风控回调已注册")


def notify_network_failure(exchange: str, reason: str) -> None:
    """
    上报网络故障（仅在状态从健康→故障时触发回调）
    """

    exchange = exchange or "unknown"
    reason = reason or "未知原因"

    previous_state = _active_failures.get(exchange, False)
    _active_failures[exchange] = True

    if previous_state:
        logger.debug(
            "🟡 [NetworkState] %s 仍处于故障状态，略过重复上报 (%s)",
            exchange,
            reason,
        )
        return

    logger.warning("🔴 [NetworkState] %s 网络故障: %s", exchange, reason)
    if _failure_handler:
        try:
            _failure_handler(exchange, reason)
        except Exception as exc:
            logger.error("❌ [NetworkState] 处理故障回调失败: %s", exc, exc_info=True)


def notify_network_recovered(exchange: str) -> None:
    """
    上报网络恢复（仅在全部交易所恢复后触发回调）
    """

    exchange = exchange or "unknown"

    if exchange in _active_failures:
        _active_failures[exchange] = False
        logger.info("🟢 [NetworkState] %s 标记为已恢复", exchange)
    else:
        logger.debug("🟢 [NetworkState] 收到 %s 的恢复通知（此前未记录故障）", exchange)

    if _recovery_handler and not any(_active_failures.values()):
        try:
            _recovery_handler(exchange)
        except Exception as exc:
            logger.error("❌ [NetworkState] 处理恢复回调失败: %s", exc, exc_info=True)

