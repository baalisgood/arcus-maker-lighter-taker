"""
Paradex 专属风控控制器（仅用于套利系统）
====================================

设计目标：
- ✅ 触发源仅来自交易所 "paradex"（仅统计/解析 paradex 侧的异常与状态）
- ✅ 与现有 GlobalRiskController 完全独立：不修改其 is_paused / pause_reason（不进入“全局暂停状态机”）
- ✅ 作为“补充风控层”：当判定需阻断时，会在执行器入口**整体阻断本次套利请求**
     - 即：若本次套利包含 paradex，则两腿（paradex + 对手交易所）都不下单，避免单腿风险
     - 但这属于“请求级别阻断”，不是 GlobalRiskController 的全局暂停
- ✅ 默认不绑定任何入口：只有 Orchestrator 检测到套利交易所包含 paradex 时才注入执行器

触发场景（来自套利程序与 Paradex 前端交易链路的真实痛点）：
- 401/403：interactive Authorization/JWT 失效或权限不足
- 429：接口限流（避免风暴式重试/刷新）
- 4xx 参数/模板错误（如签名/价格步进/vwap_price 等导致的持续拒单）

说明：
- 本模块只负责“是否允许对 paradex 发起开/平仓”的判断与节流；
- 具体的 token 刷新、WS 私有订阅等逻辑仍由 ParadexAdapter 内部实现；
- 若发现 401/403，本模块可选地调用 `adapter.refresh_frontend_authorization_once()` 做一次兜底自愈（带冷却与互斥）。
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Optional, Tuple

from core.adapters.exchanges.utils.setup_logging import LoggingConfig


logger = LoggingConfig.setup_logger(
    name=__name__,
    log_file="paradex_risk_controller.log",
    console_formatter=None,
    file_formatter="detailed",
    level=logging.INFO,
)
logger.propagate = False


@dataclass
class ParadexRiskControlConfig:
    """Paradex 专属风控配置（执行层局部风控，不影响全局风控）。"""

    enabled: bool = True

    # ========== 是否允许“只禁止开仓但仍允许平仓” ==========
    # - 对于 auth/429 等问题，优先保护：不再新增仓位，但允许你平掉已有仓位
    allow_close_when_open_blocked: bool = True

    # ========== Auth/JWT 类问题（401/403/readonly 等） ==========
    auth_failure_window_seconds: float = 120.0
    auth_failure_threshold: int = 2
    auth_open_pause_seconds: float = 60.0
    auth_close_pause_seconds: float = 10.0

    # 若遇到 401/403：是否尝试调用 adapter.refresh_frontend_authorization_once() 自愈
    auto_refresh_on_auth_failure: bool = True
    auto_refresh_cooldown_seconds: float = 15.0

    # ========== 限流（429） ==========
    rate_limit_window_seconds: float = 30.0
    rate_limit_threshold: int = 3
    rate_limit_open_pause_seconds: float = 30.0
    rate_limit_close_pause_seconds: float = 5.0

    # ========== 模板/参数错误（持续 4xx） ==========
    bad_request_window_seconds: float = 300.0
    bad_request_threshold: int = 3
    bad_request_open_pause_seconds: float = 300.0
    bad_request_close_pause_seconds: float = 30.0

    # ========== 交易所状态：CANCEL_ONLY（只允许撤单） ==========
    # 典型返回：{"error":"SYSTEM_STATUS_CANCEL_ONLY","message":"system status is CANCEL_ONLY, only cancel orders are allowed"}
    # 这不是“网络异常”，也不是“参数错误”，而是交易所侧临时不可交易状态。
    cancel_only_open_pause_seconds: float = 900.0
    cancel_only_close_pause_seconds: float = 900.0

    # ========== 交易所状态：POST_ONLY（只允许挂单，禁止市价/吃单） ==========
    # 典型返回：{"error":"SYSTEM_STATUS_POST_ONLY","message":"system status is POST_ONLY, only POST_ONLY or RPI orders are allowed"}
    post_only_open_pause_seconds: float = 900.0
    post_only_close_pause_seconds: float = 900.0


class ParadexRiskController:
    """
    Paradex 局部风控控制器：
    - 只做“对 paradex 是否放行下单（开/平仓）”
    - 不修改全局风控状态
    """

    def __init__(self, cfg: ParadexRiskControlConfig, *, paradex_adapter: object):
        self.cfg = cfg
        self._adapter = paradex_adapter

        # 事件窗口计数（按时间窗口滚动）
        self._auth_fail_ts: Deque[float] = deque()
        self._rate_limit_ts: Deque[float] = deque()
        self._bad_request_ts: Deque[float] = deque()

        # 局部暂停状态（分别控制开/平）
        self._block_open_until: float = 0.0
        self._block_close_until: float = 0.0
        self._block_reason_open: Optional[str] = None
        self._block_reason_close: Optional[str] = None

        # refresh 自愈保护：避免并发/风暴
        self._refresh_lock = asyncio.Lock()
        self._last_refresh_attempt_ts: float = 0.0

        logger.info("✅ [Paradex风控] 初始化完成：enabled=%s", bool(cfg.enabled))

    # ------------------------------------------------------------------ #
    # 对外：执行器注入
    # ------------------------------------------------------------------ #

    def should_block_trade(self, *, exchange: str, is_open: bool) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        仅对 exchange=paradex 生效。

        Returns:
            (blocked, reason, failure_code)
        """
        if not self.cfg.enabled:
            return False, None, None
        if (exchange or "").strip().lower() != "paradex":
            return False, None, None

        now = time.time()
        if bool(is_open):
            if now < float(self._block_open_until):
                return True, self._block_reason_open or "paradex_open_blocked", "paradex_open_blocked"
            return False, None, None

        # 平仓：若配置允许“开仓阻断时仍可平仓”，则不使用 open 的暂停
        if self.cfg.allow_close_when_open_blocked:
            if now < float(self._block_close_until):
                return True, self._block_reason_close or "paradex_close_blocked", "paradex_close_blocked"
            return False, None, None

        # 不允许 close 放行时：close 也受 open_block 影响
        if now < max(float(self._block_close_until), float(self._block_open_until)):
            reason = self._block_reason_close or self._block_reason_open or "paradex_blocked"
            return True, reason, "paradex_blocked"
        return False, None, None

    def record_order_error(self, *, exchange: str, order_type_label: str, exception: Exception, attempt: int) -> None:
        """由执行器统一上报：解析异常类型并触发局部风控。"""
        if not self.cfg.enabled:
            return
        if (exchange or "").strip().lower() != "paradex":
            return

        status = _extract_http_status(exception)
        msg = str(exception or "")
        msg_l = msg.lower()

        # CANCEL_ONLY：交易所只允许撤单（属于交易所维护/状态切换）
        if _is_paradex_cancel_only_error(status=status, message=msg_l):
            self._apply_pause(
                open_pause_s=float(self.cfg.cancel_only_open_pause_seconds),
                close_pause_s=float(self.cfg.cancel_only_close_pause_seconds),
                reason_open=f"Paradex 系统状态=CANCEL_ONLY（只允许撤单）: {order_type_label}单(attempt={attempt}) {msg[:200]}",
                reason_close=f"Paradex 系统状态=CANCEL_ONLY（只允许撤单）: {order_type_label}单(attempt={attempt}) {msg[:200]}",
            )
            return

        # POST_ONLY：交易所只允许挂单（吃单/市价单会被拒绝）
        if _is_paradex_post_only_error(status=status, message=msg_l):
            self._apply_pause(
                open_pause_s=float(self.cfg.post_only_open_pause_seconds),
                close_pause_s=float(self.cfg.post_only_close_pause_seconds),
                reason_open=f"Paradex 系统状态=POST_ONLY（仅允许挂单）: {order_type_label}单(attempt={attempt}) {msg[:200]}",
                reason_close=f"Paradex 系统状态=POST_ONLY（仅允许挂单）: {order_type_label}单(attempt={attempt}) {msg[:200]}",
            )
            return

        # 429
        if status == 429 or "too many requests" in msg_l or "rate limit" in msg_l:
            self._record_rate_limit(
                reason=f"{order_type_label}单触发429限流(attempt={attempt}): {msg[:200]}"
            )
            return

        # 401/403：鉴权/权限
        if status in (401, 403) or "unauthorized" in msg_l or "forbidden" in msg_l:
            self._record_auth_failure(
                reason=f"{order_type_label}单触发鉴权失败({status})(attempt={attempt}): {msg[:200]}"
            )
            # 尝试一次自愈刷新（不阻塞执行器主逻辑）
            if self.cfg.auto_refresh_on_auth_failure:
                asyncio.create_task(self._maybe_refresh_frontend_authorization())
            return

        # 4xx 参数/模板类（排除 429/401/403）
        if status is not None and 400 <= int(status) < 500:
            self._record_bad_request(
                reason=f"{order_type_label}单被4xx拒绝({status})(attempt={attempt}): {msg[:200]}"
            )
            return

    # ------------------------------------------------------------------ #
    # 内部：窗口计数 & 暂停策略
    # ------------------------------------------------------------------ #

    def _record_auth_failure(self, *, reason: str) -> None:
        now = time.time()
        _append_ts(self._auth_fail_ts, now, window_s=self.cfg.auth_failure_window_seconds)
        count = len(self._auth_fail_ts)
        if count >= max(1, int(self.cfg.auth_failure_threshold or 1)):
            self._apply_pause(
                open_pause_s=float(self.cfg.auth_open_pause_seconds),
                close_pause_s=float(self.cfg.auth_close_pause_seconds),
                reason_open=f"Paradex鉴权失败触发开仓暂停: {reason}",
                reason_close=f"Paradex鉴权失败触发平仓暂停: {reason}",
            )
            # 触发后清空窗口，避免暂停结束后被旧计数立刻再触发
            self._auth_fail_ts.clear()
        else:
            logger.warning(
                "⚠️ [Paradex风控] 鉴权失败计数: %s/%s window=%ss reason=%s",
                count,
                int(self.cfg.auth_failure_threshold or 0),
                int(self.cfg.auth_failure_window_seconds),
                (reason or "")[:200],
            )

    def _record_rate_limit(self, *, reason: str) -> None:
        now = time.time()
        _append_ts(self._rate_limit_ts, now, window_s=self.cfg.rate_limit_window_seconds)
        count = len(self._rate_limit_ts)
        if count >= max(1, int(self.cfg.rate_limit_threshold or 1)):
            self._apply_pause(
                open_pause_s=float(self.cfg.rate_limit_open_pause_seconds),
                close_pause_s=float(self.cfg.rate_limit_close_pause_seconds),
                reason_open=f"Paradex 429限流触发开仓暂停: {reason}",
                reason_close=f"Paradex 429限流触发平仓暂停: {reason}",
            )
            self._rate_limit_ts.clear()
        else:
            logger.warning(
                "⚠️ [Paradex风控] 429计数: %s/%s window=%ss reason=%s",
                count,
                int(self.cfg.rate_limit_threshold or 0),
                int(self.cfg.rate_limit_window_seconds),
                (reason or "")[:200],
            )

    def _record_bad_request(self, *, reason: str) -> None:
        now = time.time()
        _append_ts(self._bad_request_ts, now, window_s=self.cfg.bad_request_window_seconds)
        count = len(self._bad_request_ts)
        if count >= max(1, int(self.cfg.bad_request_threshold or 1)):
            self._apply_pause(
                open_pause_s=float(self.cfg.bad_request_open_pause_seconds),
                close_pause_s=float(self.cfg.bad_request_close_pause_seconds),
                reason_open=f"Paradex 4xx拒单触发开仓暂停: {reason}",
                reason_close=f"Paradex 4xx拒单触发平仓暂停: {reason}",
            )
            self._bad_request_ts.clear()
        else:
            logger.warning(
                "⚠️ [Paradex风控] 4xx拒单计数: %s/%s window=%ss reason=%s",
                count,
                int(self.cfg.bad_request_threshold or 0),
                int(self.cfg.bad_request_window_seconds),
                (reason or "")[:200],
            )

    def _apply_pause(
        self,
        *,
        open_pause_s: float,
        close_pause_s: float,
        reason_open: str,
        reason_close: str,
    ) -> None:
        now = time.time()
        if open_pause_s and open_pause_s > 0:
            until = now + float(open_pause_s)
            if until > float(self._block_open_until):
                self._block_open_until = until
                self._block_reason_open = reason_open
        if close_pause_s and close_pause_s > 0:
            until = now + float(close_pause_s)
            if until > float(self._block_close_until):
                self._block_close_until = until
                self._block_reason_close = reason_close

        logger.error(
            "⛔ [Paradex风控] 已触发局部暂停：open=%ss close=%ss reason=%s",
            int(open_pause_s or 0),
            int(close_pause_s or 0),
            (reason_open or "")[:200],
        )

    async def _maybe_refresh_frontend_authorization(self) -> None:
        """
        尝试调用 ParadexAdapter.refresh_frontend_authorization_once() 做一次兜底刷新：
        - 有冷却时间，避免高并发触发
        - 有互斥锁，避免并发刷新
        """
        cooldown = max(0.0, float(self.cfg.auto_refresh_cooldown_seconds or 0.0))
        now = time.time()
        if cooldown > 0 and (now - float(self._last_refresh_attempt_ts)) < cooldown:
            return

        # adapter 不一定具备该方法（例如只走 SDK 模式），因此用反射兜底
        refresh = getattr(self._adapter, "refresh_frontend_authorization_once", None)
        if not callable(refresh):
            return

        async with self._refresh_lock:
            now2 = time.time()
            if cooldown > 0 and (now2 - float(self._last_refresh_attempt_ts)) < cooldown:
                return
            self._last_refresh_attempt_ts = now2
            try:
                ok = await refresh()
                if ok:
                    logger.warning("🔐 [Paradex风控] 已触发一次 Authorization 刷新（兜底自愈）")
                else:
                    logger.warning("⚠️ [Paradex风控] Authorization 刷新未成功（adapter返回False）")
            except Exception as exc:
                logger.warning("⚠️ [Paradex风控] Authorization 刷新异常: %s", exc)

    def get_status_snapshot(self) -> dict:
        """
        返回“局部风控”状态快照（仅用于 UI 展示，不参与任何交易逻辑）。
        """
        now = time.time()
        open_remaining = int(max(0.0, float(self._block_open_until) - now))
        close_remaining = int(max(0.0, float(self._block_close_until) - now))
        return {
            "enabled": bool(self.cfg.enabled),
            "open_blocked": bool(open_remaining > 0),
            "close_blocked": bool(close_remaining > 0),
            "open_remaining_seconds": open_remaining,
            "close_remaining_seconds": close_remaining,
            "reason_open": self._block_reason_open,
            "reason_close": self._block_reason_close,
        }


def _append_ts(dq: Deque[float], now_ts: float, *, window_s: float) -> None:
    window_s = max(0.0, float(window_s or 0.0))
    dq.append(float(now_ts))
    if window_s <= 0:
        return
    cutoff = float(now_ts) - window_s
    while dq and float(dq[0]) < cutoff:
        dq.popleft()


def _extract_http_status(exc: Exception) -> Optional[int]:
    """
    从常见异常中提取 HTTP status：
    - aiohttp.ClientResponseError: .status
    - 自定义异常：可能包含 .status / .code
    - 文本兜底
    """
    try:
        status = getattr(exc, "status", None)
        if isinstance(status, int):
            return status
    except Exception:
        pass
    try:
        code = getattr(exc, "code", None)
        if isinstance(code, int):
            return code
    except Exception:
        pass
    s = str(exc or "")
    # 极简兜底（避免误判，不做复杂正则）
    for candidate in (401, 403, 404, 409, 422, 429):
        if str(candidate) in s:
            return candidate
    return None


def _is_paradex_cancel_only_error(*, status: Optional[int], message: str) -> bool:
    """
    识别 Paradex 的 “SYSTEM_STATUS_CANCEL_ONLY / CANCEL_ONLY” 状态。
    - 该错误通常是 HTTP 400
    - 文本里会包含 SYSTEM_STATUS_CANCEL_ONLY 或 cancel_only
    """
    msg = (message or "").lower()
    if "system_status_cancel_only" in msg:
        return True
    # 兜底：同时出现 system status + cancel_only，避免误判一般性的“cancel”
    if "cancel_only" in msg and ("system status" in msg or "system_status" in msg):
        return True
    if status == 400 and "cancel_only" in msg:
        return True
    return False


def _is_paradex_post_only_error(*, status: Optional[int], message: str) -> bool:
    """
    识别 Paradex 的 “SYSTEM_STATUS_POST_ONLY / POST_ONLY” 状态。
    - 通常是 HTTP 400
    - 文本里会包含 SYSTEM_STATUS_POST_ONLY 或 post_only
    """
    msg = (message or "").lower()
    if "system_status_post_only" in msg:
        return True
    if "post_only" in msg and ("system status" in msg or "system_status" in msg):
        return True
    if status == 400 and "post_only" in msg:
        return True
    return False

