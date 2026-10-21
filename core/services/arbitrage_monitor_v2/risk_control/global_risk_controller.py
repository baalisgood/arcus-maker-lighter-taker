"""
全局风险控制模块

职责：
- 仓位管理（单一代币最大持仓、所有代币最大持仓）
- 账户余额管理（余额不足警告、余额不足平仓）
- 网络故障处理（自动暂停、重连、恢复）
- 交易所维护检测（检测维护状态、暂停操作、恢复）
- 脚本崩溃恢复（持久化存储、自动恢复）
- 其他风险控制（价格异常、订单异常、数据一致性等）

注意：此模块负责全局风险控制，不涉及具体的套利决策逻辑
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional, Set, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
import time
from collections import deque
from collections import defaultdict

from ..config.arbitrage_config import RiskControlConfig, QuantityConfig
from core.adapters.exchanges.models import BalanceData, PositionData
from core.adapters.exchanges.interface import ExchangeInterface

# 数据模型
from ..models import RiskStatus

# 🔥 使用统一日志系统
from core.adapters.exchanges.utils.setup_logging import LoggingConfig

logger = LoggingConfig.setup_logger(
    name=__name__,
    log_file='global_risk_controller.log',
    console_formatter=None,
    file_formatter='detailed',
    level=logging.INFO
)
logger.propagate = False


class GlobalRiskController:
    """全局风险控制器"""

    def __init__(
        self,
        risk_config: RiskControlConfig,
        exchange_adapters: Dict[str, ExchangeInterface],
        symbol_quantity_config: Optional[Dict[str, QuantityConfig]] = None,
        allowed_symbols: Optional[Set[str]] = None,
    ):
        """
        初始化全局风险控制器

        Args:
            risk_config: 风险控制配置
            exchange_adapters: 交易所适配器字典 {exchange_name: adapter}
        """
        self.config = risk_config
        self.exchange_adapters = exchange_adapters
        self.symbol_quantity_config = symbol_quantity_config or {}
        self.allowed_symbols = {s.upper()
                                for s in (allowed_symbols or set())} or None

        # 风险状态
        self.risk_status = RiskStatus()

        # 监控任务
        self.monitor_tasks: List[asyncio.Task] = []
        self.running = False

        # 回调函数
        self.on_pause: Optional[Callable[[str], None]] = None  # 暂停回调
        self.on_resume: Optional[Callable[[], None]] = None  # 恢复回调
        # 平仓所有仓位回调
        self.on_close_all_positions: Optional[Callable[[], None]] = None
        self.position_times_provider = None
        self.on_close_position = None
        # 🔥 主动重连回调（非 async）：用于“高滑点连续出现”触发重连交易所连接栈
        self.on_reconnect_exchange_stack: Optional[Callable[[str], None]] = None

        # 统计数据
        self.daily_trade_count: Dict[str, int] = {}  # {date: count}
        self.last_trade_date: Optional[str] = None

        # 执行失败暂停（全局）
        self._exec_failure_streak: int = 0
        self._exec_pause_until: Optional[datetime] = None
        self._exec_pause_last_log_ts: float = 0.0
        self._exec_pause_reason: Optional[str] = None

        # 滑点触发暂停（全局）
        self._slippage_pause_until: Optional[datetime] = None
        self._slippage_pause_reason: Optional[str] = None
        self._slippage_pause_last_trigger_ts: float = 0.0
        self._slippage_pause_last_log_ts: float = 0.0
        # 负滑点“连续多次”计数：滚动窗口内满足条件的触发时间戳（秒）
        self._burst_negative_slippage_ts: deque = deque()

        # 🔥 高滑点“近N次成功成交”触发重连（滚动窗口：按成交条数）
        self._high_slippage_recent_success: deque = deque()
        self._high_slippage_reconnect_last_trigger_ts: float = 0.0

        # API失败暂停（全局）
        self._api_failure_pause_until: Optional[datetime] = None
        self._api_failure_pause_reason: Optional[str] = None
        self._api_failure_pause_last_log_ts: float = 0.0
        # 按交易所统计：exchange -> deque[timestamps]
        self._api_failure_ts_by_exchange: Dict[str, deque] = {}
        # 🔥 失败计数节流：exchange -> last_record_ts
        # 用途：避免“同一瞬间多个symbol同类请求失败+重试”导致失败次数瞬间堆满阈值。
        self._api_failure_last_record_ts_by_exchange: Dict[str, float] = {}
        # WS/API失败累计次数（不节流）：(exchange, api) -> total_count
        # 用途：你希望“重连/断连发生多少次，就打印多少次”，这里由风控统一记录与输出。
        self._api_failure_total_count: Dict[tuple[str, str], int] = defaultdict(int)

        # WS成交延迟暂停（全局暂停，禁止开/平仓/纠偏；到期自动恢复）
        self._order_ws_latency_pause_until: Optional[datetime] = None
        self._order_ws_latency_pause_reason: Optional[str] = None
        self._order_ws_latency_pause_last_log_ts: float = 0.0

        # 持仓持续不一致暂停（全局，禁止开/平仓；直到恢复一致）
        self._position_mismatch_pause_active: bool = False
        self._position_mismatch_pause_reason: Optional[str] = None
        self._position_mismatch_pause_last_log_ts: float = 0.0

        # 余额严重不足强平节流（避免反复触发“平仓所有仓位”）
        self._last_force_close_ts: float = 0.0

        logger.info("✅ [风险控制] 全局风险控制器初始化完成")

    # ============================================================================
    # Helpers: per-exchange balance thresholds
    # ============================================================================

    @staticmethod
    def _to_decimal(v: Any) -> Decimal:
        try:
            return Decimal(str(v))
        except Exception:
            return Decimal("0")

    def _min_balance_warning_threshold(self, exchange: str) -> Decimal:
        ex = str(exchange or "").strip().lower()
        bm = getattr(self.config, "balance_management", None)
        if bm is None:
            return Decimal("0")
        overrides = getattr(bm, "min_balance_warning_by_exchange", None) or {}
        if isinstance(overrides, dict):
            for k, v in overrides.items():
                if str(k or "").strip().lower() == ex:
                    return self._to_decimal(float(v))
        return self._to_decimal(float(getattr(bm, "min_balance_warning", 0.0) or 0.0))

    def _min_balance_close_threshold(self, exchange: str) -> Decimal:
        ex = str(exchange or "").strip().lower()
        bm = getattr(self.config, "balance_management", None)
        if bm is None:
            return Decimal("0")
        overrides = getattr(bm, "min_balance_close_position_by_exchange", None) or {}
        if isinstance(overrides, dict):
            for k, v in overrides.items():
                if str(k or "").strip().lower() == ex:
                    return self._to_decimal(float(v))
        return self._to_decimal(float(getattr(bm, "min_balance_close_position", 0.0) or 0.0))

    # ============================================================================
    # 滑点触发暂停（全局暂停，禁止开/平仓）
    # ============================================================================

    def record_slippage_pause(
        self,
        *,
        symbol: str,
        is_open: bool,
        slippage_pct: Optional[float],
    ) -> Dict[str, Any]:
        """
        记录一次成交后的滑点，并在满足阈值时触发“滑点暂停”。

        重要口径：
        - slippage_pct = 实际价差% - 理论价差%
        - 通常为负数，且越负亏损越大
        - 仅对“负滑点”触发暂停：slippage_pct <= -threshold
        """
        cfg = getattr(self.config, "slippage_pause", None)
        if not cfg or not bool(getattr(cfg, "enabled", False)):
            return {"enabled": False}

        # 先处理到期自动恢复
        self._maybe_resume_slippage_pause()

        if slippage_pct is None:
            return {"enabled": True, "observed": False}

        # 方向开关
        if bool(is_open) and not bool(getattr(cfg, "apply_to_open", True)):
            return {"enabled": True, "observed": True, "applied": False}
        if (not bool(is_open)) and not bool(getattr(cfg, "apply_to_close", True)):
            return {"enabled": True, "observed": True, "applied": False}

        # 负滑点“连续多次”触发（滚动窗口计数）
        burst_cfg = getattr(cfg, "burst_negative_slippage_pause", None)
        burst_enabled = bool(burst_cfg and bool(getattr(burst_cfg, "enabled", False)))

        try:
            threshold = float(
                getattr(cfg, "negative_slippage_threshold_pct", 0.0) or 0.0)
        except Exception:
            threshold = 0.0
        threshold = abs(threshold)
        is_threshold_enabled = bool(threshold > 0)
        if (not is_threshold_enabled) and (not burst_enabled):
            return {"enabled": True, "observed": True, "applied": False}

        try:
            pause_seconds = float(getattr(cfg, "pause_seconds", 0.0) or 0.0)
        except Exception:
            pause_seconds = 0.0
        pause_seconds = max(0.0, pause_seconds)
        if pause_seconds <= 0:
            return {"enabled": True, "observed": True, "applied": False}

        try:
            cooldown_seconds = float(
                getattr(cfg, "cooldown_seconds", 0.0) or 0.0)
        except Exception:
            cooldown_seconds = 0.0
        cooldown_seconds = max(0.0, cooldown_seconds)

        now_ts = time.time()

        # 维护/统计负滑点连续触发（固定阈值：-0.01%）
        burst_triggered = False
        fixed_negative_threshold_pct = 0.01
        if burst_enabled:
            try:
                window_seconds = float(getattr(burst_cfg, "window_seconds", 0.0) or 0.0)
            except Exception:
                window_seconds = 0.0
            window_seconds = max(0.0, window_seconds)
            try:
                count_threshold = int(getattr(burst_cfg, "count_threshold", 0) or 0)
            except Exception:
                count_threshold = 0
            count_threshold = max(0, count_threshold)

            if window_seconds <= 0 or count_threshold <= 0:
                self._burst_negative_slippage_ts.clear()
            else:
                cutoff_ts = now_ts - window_seconds
                while self._burst_negative_slippage_ts and float(self._burst_negative_slippage_ts[0]) < cutoff_ts:
                    self._burst_negative_slippage_ts.popleft()

                try:
                    # 只统计：亏损方向且达到固定阈值（负的万分之一及更差）
                    if float(slippage_pct) <= -fixed_negative_threshold_pct:
                        self._burst_negative_slippage_ts.append(now_ts)
                except Exception:
                    pass

                burst_triggered = bool(len(self._burst_negative_slippage_ts) >= count_threshold)
        if cooldown_seconds > 0 and (now_ts - float(self._slippage_pause_last_trigger_ts)) < cooldown_seconds:
            return {
                "enabled": True,
                "observed": True,
                "triggered": False,
                "cooldown": True,
                "remaining_seconds": self.get_slippage_pause_remaining_seconds(),
            }

        if burst_triggered:
            # 触发后清空计数，避免恢复后立刻再次触发
            self._burst_negative_slippage_ts.clear()

            self._slippage_pause_last_trigger_ts = now_ts
            self._slippage_pause_until = datetime.now() + timedelta(seconds=pause_seconds)
            action = "开仓" if bool(is_open) else "平仓"
            self._slippage_pause_reason = (
                f"滑点暂停触发：负滑点连续触发（slippage<=-{fixed_negative_threshold_pct:.4f}%）"
                f" (action={action}, symbol={symbol}, pause={int(pause_seconds)}s)"
            )

            # 若当前未处于其他原因的全局暂停，则直接进入暂停；若已暂停则不覆盖原因，但仍保持 slippage_pause_until
            if not self.risk_status.is_paused:
                self.risk_status.is_paused = True
                self.risk_status.pause_reason = self._slippage_pause_reason
                logger.warning("⏸️ [风险控制] %s", self.risk_status.pause_reason or "")
                if self.on_pause:
                    self.on_pause(self.risk_status.pause_reason or "")
            else:
                # 节流提示：避免暂停期间刷屏
                if now_ts - float(self._slippage_pause_last_log_ts) >= 60.0:
                    self._slippage_pause_last_log_ts = now_ts
                    logger.warning(
                        "⏸️ [风险控制] 滑点暂停仍在生效: remaining=%ss (symbol=%s)",
                        int(self.get_slippage_pause_remaining_seconds() or 0),
                        symbol,
                    )

            return {
                "enabled": True,
                "observed": True,
                "triggered": True,
                "paused": True,
                "remaining_seconds": self.get_slippage_pause_remaining_seconds(),
            }

        if not is_threshold_enabled:
            return {
                "enabled": True,
                "observed": True,
                "triggered": False,
                "remaining_seconds": self.get_slippage_pause_remaining_seconds(),
            }

        # 仅对“亏损方向”的滑点触发：越负越糟
        if float(slippage_pct) > -threshold:
            return {
                "enabled": True,
                "observed": True,
                "triggered": False,
                "remaining_seconds": self.get_slippage_pause_remaining_seconds(),
            }

        self._slippage_pause_last_trigger_ts = now_ts
        self._slippage_pause_until = datetime.now() + timedelta(seconds=pause_seconds)
        action = "开仓" if bool(is_open) else "平仓"
        self._slippage_pause_reason = (
            f"滑点过大触发暂停：slippage={float(slippage_pct):.4f}% <= -{threshold:.4f}%"
            f" (action={action}, symbol={symbol}, pause={int(pause_seconds)}s)"
        )

        # 若当前未处于其他原因的全局暂停，则直接进入暂停；若已暂停则不覆盖原因，但仍保持 slippage_pause_until
        if not self.risk_status.is_paused:
            self.risk_status.is_paused = True
            self.risk_status.pause_reason = self._slippage_pause_reason
            logger.warning("⏸️ [风险控制] %s", self.risk_status.pause_reason or "")
            if self.on_pause:
                self.on_pause(self.risk_status.pause_reason or "")
        else:
            # 节流提示：避免暂停期间刷屏
            if now_ts - float(self._slippage_pause_last_log_ts) >= 60.0:
                self._slippage_pause_last_log_ts = now_ts
                logger.warning(
                    "⏸️ [风险控制] 滑点暂停仍在生效: remaining=%ss (symbol=%s)",
                    int(self.get_slippage_pause_remaining_seconds() or 0),
                    symbol,
                )

        return {
            "enabled": True,
            "observed": True,
            "triggered": True,
            "paused": True,
            "remaining_seconds": self.get_slippage_pause_remaining_seconds(),
        }

    def record_high_slippage_reconnect(
        self,
        *,
        symbol: str,
        is_open: bool,
        slippage_pct: Optional[float],
        success: bool,
        partial_failure: bool,
    ) -> Dict[str, Any]:
        """
        高滑点“近N次成功成交”触发重连（并进入滑点暂停）：
        - 仅统计【成交成功】（success=True 且 partial_failure=False）的记录
        - 仅统计亏损方向：slippage_pct <= -threshold_pct
        - 最近 window_trades 次成功成交中，高滑点次数 >= count_threshold -> 触发一次重连回调
        - 触发后同时进入【滑点暂停】（全局暂停，禁止开/平仓），暂停时长复用 slippage_pause.pause_seconds
        """
        sp = getattr(self.config, "slippage_pause", None)
        cfg = getattr(sp, "high_slippage_reconnect", None) if sp else None
        if not cfg or not bool(getattr(cfg, "enabled", False)):
            return {"enabled": False}

        # 仅统计：成交成功（两腿成功）
        if (not bool(success)) or bool(partial_failure):
            return {"enabled": True, "observed": False, "success": False}

        # 方向开关：复用 slippage_pause 的开/平仓开关口径
        try:
            if bool(is_open) and sp is not None and not bool(getattr(sp, "apply_to_open", True)):
                return {"enabled": True, "observed": False, "skipped": "apply_to_open=false"}
            if (not bool(is_open)) and sp is not None and not bool(getattr(sp, "apply_to_close", True)):
                return {"enabled": True, "observed": False, "skipped": "apply_to_close=false"}
        except Exception:
            pass

        if slippage_pct is None:
            return {"enabled": True, "observed": False, "reason": "slippage_pct_none"}

        # 参数
        try:
            window_trades = int(getattr(cfg, "window_trades", 10) or 10)
        except Exception:
            window_trades = 10
        window_trades = max(1, int(window_trades))
        try:
            count_threshold = int(getattr(cfg, "count_threshold", 5) or 5)
        except Exception:
            count_threshold = 5
        count_threshold = max(1, int(count_threshold))
        try:
            threshold_pct = float(getattr(cfg, "high_slippage_threshold_pct", 0.01) or 0.01)
        except Exception:
            threshold_pct = 0.01
        threshold_pct = abs(float(threshold_pct))
        if threshold_pct <= 0:
            threshold_pct = 0.01

        now_ts = time.time()
        try:
            cooldown_seconds = float(getattr(cfg, "cooldown_seconds", 300.0) or 300.0)
        except Exception:
            cooldown_seconds = 300.0
        cooldown_seconds = max(0.0, float(cooldown_seconds))

        # 维护窗口（按条数，保留最近 window_trades 条成功成交）
        try:
            self._high_slippage_recent_success.append(float(slippage_pct))
            while len(self._high_slippage_recent_success) > window_trades:
                self._high_slippage_recent_success.popleft()
        except Exception:
            return {"enabled": True, "observed": False, "reason": "append_failed"}

        high_cnt = 0
        try:
            for v in list(self._high_slippage_recent_success):
                if float(v) <= -threshold_pct:
                    high_cnt += 1
        except Exception:
            high_cnt = 0

        # 冷却：避免频繁重连
        if cooldown_seconds > 0 and (now_ts - float(self._high_slippage_reconnect_last_trigger_ts)) < cooldown_seconds:
            return {
                "enabled": True,
                "observed": True,
                "triggered": False,
                "cooldown": True,
                "high_count": high_cnt,
                "window_trades": window_trades,
            }

        if high_cnt >= count_threshold:
            self._high_slippage_reconnect_last_trigger_ts = now_ts
            action = "开仓" if bool(is_open) else "平仓"
            reason = (
                f"高滑点连续出现：最近{window_trades}次成功成交中高滑点次数={high_cnt}>={count_threshold} "
                f"(slippage<=-{threshold_pct:.4f}%) action={action} symbol={symbol}"
            )
            logger.warning("🔌 [风险控制] %s -> 请求重连交易所连接栈", reason)
            # 触发后清空窗口，避免下一条成交立刻再次触发
            try:
                self._high_slippage_recent_success.clear()
            except Exception:
                pass

            # 🔥 同时进入“滑点暂停”（与 burst_negative_slippage_pause 共用 pause_seconds 配置）
            paused = False
            entered_slippage_pause = False
            try:
                try:
                    pause_seconds = float(getattr(sp, "pause_seconds", 0.0) or 0.0) if sp is not None else 0.0
                except Exception:
                    pause_seconds = 0.0
                pause_seconds = max(0.0, float(pause_seconds))
                if pause_seconds > 0:
                    # 使用同一套滑点暂停状态机（保证 should_block_execution_pause 可自动恢复）
                    self._slippage_pause_last_trigger_ts = now_ts
                    self._slippage_pause_until = datetime.now() + timedelta(seconds=pause_seconds)
                    self._slippage_pause_reason = (
                        f"滑点暂停触发：{reason} (pause={int(pause_seconds)}s)"
                    )

                    # 若当前未处于其他原因的全局暂停，则直接进入暂停；若已暂停则不覆盖原因，但仍保持 slippage_pause_until
                    if not self.risk_status.is_paused:
                        self.risk_status.is_paused = True
                        self.risk_status.pause_reason = self._slippage_pause_reason
                        logger.warning("⏸️ [风险控制] %s", self.risk_status.pause_reason or "")
                        if self.on_pause:
                            self.on_pause(self.risk_status.pause_reason or "")
                        entered_slippage_pause = True
                    else:
                        # 节流提示：避免暂停期间刷屏
                        if now_ts - float(self._slippage_pause_last_log_ts) >= 60.0:
                            self._slippage_pause_last_log_ts = now_ts
                            logger.warning(
                                "⏸️ [风险控制] 滑点暂停仍在生效: remaining=%ss (symbol=%s)",
                                int(self.get_slippage_pause_remaining_seconds() or 0),
                                symbol,
                            )
                    paused = True
            except Exception:
                paused = False

            # ✅ 你要求的语义：如果本次“高滑点”已经成功进入滑点暂停，则把重连延后到“暂停结束 on_resume”再执行，
            # 避免触发时立刻重连造成更大的抖动。
            # 兜底：若系统本来就处于其他原因的 paused（无法走滑点暂停的 on_resume 链路），则仍立即触发重连回调。
            reconnect_deferred = False
            reconnect_called = False
            try:
                if bool(entered_slippage_pause):
                    reconnect_deferred = True
                else:
                    if self.on_reconnect_exchange_stack:
                        self.on_reconnect_exchange_stack(reason)
                        reconnect_called = True
            except Exception:
                reconnect_called = False
            return {
                "enabled": True,
                "observed": True,
                "triggered": True,
                "paused": bool(paused),
                "remaining_seconds": self.get_slippage_pause_remaining_seconds(),
                "reconnect_deferred": bool(reconnect_deferred),
                "reconnect_called": bool(reconnect_called),
                "high_count": high_cnt,
                "window_trades": window_trades,
                "threshold_pct": threshold_pct,
            }

        return {
            "enabled": True,
            "observed": True,
            "triggered": False,
            "high_count": high_cnt,
            "window_trades": window_trades,
            "threshold_pct": threshold_pct,
        }

    def get_slippage_pause_remaining_seconds(self) -> Optional[float]:
        """返回“滑点暂停”的剩余秒数；若未处于该暂停则返回 None。"""
        if not self._slippage_pause_until:
            return None
        remaining = (self._slippage_pause_until -
                     datetime.now()).total_seconds()
        return max(0.0, float(remaining))

    def _maybe_resume_slippage_pause(self) -> None:
        """若滑点暂停已到期，自动恢复（不影响余额/网络等其他暂停原因）。"""
        if not self._slippage_pause_until:
            return
        if datetime.now() < self._slippage_pause_until:
            return

        self._slippage_pause_until = None
        self._slippage_pause_reason = None

        # 仅当全局暂停原因就是“滑点暂停”时才恢复全局 paused 状态
        reason = (self.risk_status.pause_reason or "")
        if self.risk_status.is_paused and ("滑点过大触发暂停" in reason or "滑点暂停" in reason or "滑点过大" in reason):
            self.risk_status.is_paused = False
            self.risk_status.pause_reason = None
            logger.warning("▶️ [风险控制] 滑点暂停到期，已自动恢复交易")
            if self.on_resume:
                self.on_resume()

    def should_block_slippage_pause(self) -> tuple[bool, Optional[str]]:
        """仅用于“滑点暂停”的交易拦截（开仓/平仓都阻止）。"""
        self._maybe_resume_slippage_pause()
        remaining = self.get_slippage_pause_remaining_seconds()
        if remaining is None or remaining <= 0:
            return False, None
        reason = self._slippage_pause_reason or "滑点暂停中"
        return True, f"{reason} (remaining={int(remaining)}s)"

    def is_slippage_pause_enabled(self) -> bool:
        cfg = getattr(self.config, "slippage_pause", None)
        return bool(cfg and bool(getattr(cfg, "enabled", False)))

    # ============================================================================
    # 执行失败暂停（全局暂停，禁止开/平仓）
    # ============================================================================

    def record_execution_outcome(
        self,
        *,
        symbol: str,
        success: bool,
        partial_failure: bool = False,
        reason: Optional[str] = None,
        risk_events: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        记录一次“真实执行”的结果，用于“连续失败 -> 暂停”风控。

        ⚠️ 计数口径（与用户需求一致）：
        - 仅当“最终结果返回失败”（success=False）才计为一次失败轮次。
          （包含“单腿成功另一腿失败且补救/重试也失败”的场景：通常表现为 success=False 且 partial_failure=True）
        - 只要本轮 success=True，则立即清零连续失败计数。
        """
        cfg = getattr(self.config, "execution_pause", None)
        if not cfg or not bool(getattr(cfg, "enabled", False)):
            return {"enabled": False}

        # 先处理暂停到期自动恢复
        self._maybe_resume_execution_pause()

        # ✅ 成功：清零连续失败计数
        if bool(success):
            self._exec_failure_streak = 0
            return {
                "enabled": True,
                "success": True,
                "streak": 0,
                "threshold": self.get_execution_failure_threshold(),
                "triggered": False,
                "paused": False,
                "remaining_seconds": self.get_execution_pause_remaining_seconds(),
            }

        # ✅ 失败：计数 + 触发阈值暂停
        self._exec_failure_streak = max(0, int(self._exec_failure_streak)) + 1
        threshold = max(1, int(getattr(cfg, "consecutive_failures", 3) or 3))
        pause_minutes = float(getattr(cfg, "pause_minutes", 0.0) or 0.0)
        pause_minutes = max(0.0, pause_minutes)
        triggered = False
        streak_now = int(self._exec_failure_streak)

        # 达到阈值 -> 进入暂停
        if streak_now >= threshold and pause_minutes > 0:
            triggered = True
            # 触发后清零，避免恢复后立刻再次触发
            self._exec_failure_streak = 0
            self._exec_pause_until = datetime.now() + timedelta(minutes=pause_minutes)
            self._exec_pause_reason = (
                f"执行失败连续触发≥{threshold}轮，已暂停{pause_minutes:.1f}分钟"
                + (f" (symbol={symbol})" if symbol else "")
                + (f" 原因={reason}" if reason else "")
            )

            # 若当前已因其他原因暂停，不覆盖原因，但仍保持 exec_pause_until（到期会尝试恢复）
            if not self.risk_status.is_paused:
                self.risk_status.is_paused = True
                self.risk_status.pause_reason = self._exec_pause_reason
                logger.warning(
                    "⏸️ [风险控制] %s", self.risk_status.pause_reason or "")
                if self.on_pause:
                    self.on_pause(self.risk_status.pause_reason or "")
            else:
                # 节流提示：避免暂停期间刷屏
                now_ts = datetime.now().timestamp()
                if now_ts - float(self._exec_pause_last_log_ts) >= 60.0:
                    self._exec_pause_last_log_ts = now_ts
                    logger.warning(
                        "⏸️ [风险控制] 执行失败暂停仍在生效: remaining=%ss (symbol=%s)",
                        int(max(0.0, (self._exec_pause_until - datetime.now()
                                      ).total_seconds())) if self._exec_pause_until else -1,
                        symbol,
                    )
        return {
            "enabled": True,
            "success": False,
            "streak": streak_now,
            "threshold": threshold,
            "triggered": triggered,
            "paused": bool(self._exec_pause_until and self.get_execution_pause_remaining_seconds() and self.get_execution_pause_remaining_seconds() > 0),
            "remaining_seconds": self.get_execution_pause_remaining_seconds(),
        }

    def should_block_execution_pause(self) -> tuple[bool, Optional[str]]:
        """
        交易拦截（开仓/平仓都阻止）。

        说明：
        - 历史上该方法只拦截“执行失败暂停”
        - 为保证最小改动与向后兼容，新增的“持仓持续不一致暂停”也复用该闸门返回
        """
        # 先处理到期自动恢复
        self._maybe_resume_execution_pause()
        self._maybe_resume_slippage_pause()
        self._maybe_resume_api_failure_pause()
        self._maybe_resume_order_ws_latency_pause()

        # WS成交延迟暂停：到期自动恢复
        blocked0, reason0 = self.should_block_order_ws_latency_pause()
        if blocked0:
            return True, reason0

        # 持仓持续不一致暂停：不依赖时间到期，由外部在“恢复一致”时调用 clear
        blocked, reason = self.should_block_position_mismatch_pause()
        if blocked:
            return True, reason

        # API失败暂停：到期自动恢复
        blocked2, reason2 = self.should_block_api_failure_pause()
        if blocked2:
            return True, reason2

        remaining = self.get_execution_pause_remaining_seconds()
        if remaining is None or remaining <= 0:
            return False, None
        reason = self._exec_pause_reason or "执行失败暂停中"
        return True, f"{reason} (remaining={int(remaining)}s)"

    def should_block_position_alignment_auto_fix(self) -> tuple[bool, Optional[str]]:
        """
        交易拦截（仅用于“持仓纠偏下单”）。

        设计目标（与你的要求一致）：
        - ✅ 当“持仓持续不一致暂停(position_mismatch_pause)”触发时：
          - 继续禁止开仓/平仓（由 should_block_execution_pause 负责）
          - 但允许持仓纠偏继续下单尝试（由本方法忽略 position_mismatch_pause）
        - ✅ 仍然拦截更高风险/更不可信的状态：
          - API失败暂停（接口不可用/部分可用）
          - 滑点暂停（市场异常）
          - 执行失败暂停（连续失败）
          - 其它非“持仓持续不一致暂停”的全局 paused 原因（如网络故障/交易所维护/余额类暂停）
        """
        # 先处理到期自动恢复
        self._maybe_resume_execution_pause()
        self._maybe_resume_slippage_pause()
        self._maybe_resume_api_failure_pause()
        self._maybe_resume_order_ws_latency_pause()

        # API失败暂停：到期自动恢复（该状态下连纠偏也应禁止）
        blocked_api, reason_api = self.should_block_api_failure_pause()
        if blocked_api:
            return True, reason_api

        # WS成交延迟暂停：到期自动恢复（该状态下连纠偏也应禁止）
        blocked_ws, reason_ws = self.should_block_order_ws_latency_pause()
        if blocked_ws:
            try:
                cfg = getattr(self.config, "order_ws_latency_pause", None)
                block_fix = bool(getattr(cfg, "block_position_alignment_auto_fix", True))
            except Exception:
                block_fix = True
            if block_fix:
                return True, reason_ws

        # 滑点暂停：到期自动恢复（纠偏也是下单，需拦截）
        blocked_slip, reason_slip = self.should_block_slippage_pause()
        if blocked_slip:
            return True, reason_slip

        # 执行失败暂停：到期自动恢复
        remaining = self.get_execution_pause_remaining_seconds()
        if remaining is not None and remaining > 0:
            reason = self._exec_pause_reason or "执行失败暂停中"
            return True, f"{reason} (remaining={int(remaining)}s)"

        # 其它全局 paused：默认拦截；但“持仓持续不一致暂停”要放行纠偏
        if bool(self.risk_status.is_paused):
            reason = self.risk_status.pause_reason or "系统暂停中"
            if "持仓持续不一致暂停" in reason:
                return False, None
            return True, reason

        return False, None

    # ============================================================================
    # WS成交延迟暂停（全局暂停，禁止开/平仓/纠偏）
    # ============================================================================

    def is_order_ws_latency_pause_enabled(self) -> bool:
        cfg = getattr(self.config, "order_ws_latency_pause", None)
        return bool(cfg and bool(getattr(cfg, "enabled", False)))

    def get_order_ws_latency_pause_remaining_seconds(self) -> Optional[float]:
        if not self._order_ws_latency_pause_until:
            return None
        remaining = (self._order_ws_latency_pause_until -
                     datetime.now()).total_seconds()
        return max(0.0, float(remaining))

    def _maybe_resume_order_ws_latency_pause(self) -> None:
        if not self._order_ws_latency_pause_until:
            return
        if datetime.now() < self._order_ws_latency_pause_until:
            return

        self._order_ws_latency_pause_until = None
        self._order_ws_latency_pause_reason = None

        reason = (self.risk_status.pause_reason or "")
        if self.risk_status.is_paused and ("WS成交延迟" in reason or "成交推送延迟" in reason):
            self.risk_status.is_paused = False
            self.risk_status.pause_reason = None
            logger.warning("▶️ [风险控制] WS成交延迟暂停到期，已自动恢复交易")
            if self.on_resume:
                self.on_resume()

    def should_block_order_ws_latency_pause(self) -> tuple[bool, Optional[str]]:
        self._maybe_resume_order_ws_latency_pause()
        remaining = self.get_order_ws_latency_pause_remaining_seconds()
        if remaining is None or remaining <= 0:
            return False, None
        reason = self._order_ws_latency_pause_reason or "WS成交延迟暂停中"
        return True, f"{reason} (remaining={int(remaining)}s)"

    def record_order_ws_latency_pause(
        self,
        *,
        symbol: str,
        exchange: str,
        order_id: str,
        latency_seconds: float,
    ) -> Dict[str, Any]:
        """
        记录一次“下单提交 -> WS成交推送”的耗时，超阈值则触发全局暂停。
        """
        cfg = getattr(self.config, "order_ws_latency_pause", None)
        if not cfg or not bool(getattr(cfg, "enabled", False)):
            return {"enabled": False}

        # 先处理到期自动恢复
        self._maybe_resume_order_ws_latency_pause()

        try:
            threshold = float(getattr(cfg, "threshold_seconds", 0.0) or 0.0)
        except Exception:
            threshold = 0.0
        threshold = max(0.0, threshold)
        if threshold <= 0:
            return {"enabled": True, "observed": True, "triggered": False}

        try:
            pause_seconds = float(getattr(cfg, "pause_seconds", 0.0) or 0.0)
        except Exception:
            pause_seconds = 0.0
        pause_seconds = max(0.0, pause_seconds)
        if pause_seconds <= 0:
            return {"enabled": True, "observed": True, "triggered": False}

        observed = max(0.0, float(latency_seconds or 0.0))
        if observed < threshold:
            return {"enabled": True, "observed": True, "triggered": False}

        self._order_ws_latency_pause_until = datetime.now() + timedelta(seconds=pause_seconds)
        self._order_ws_latency_pause_reason = (
            f"WS成交延迟触发暂停：latency={observed:.2f}s >= {threshold:.2f}s "
            f"(exchange={exchange}, symbol={symbol}, order_id={order_id}, pause={int(pause_seconds)}s)"
        )

        if not self.risk_status.is_paused:
            self.risk_status.is_paused = True
            self.risk_status.pause_reason = self._order_ws_latency_pause_reason
            logger.warning("⏸️ [风险控制] %s", self.risk_status.pause_reason or "")
            if self.on_pause:
                self.on_pause(self.risk_status.pause_reason or "")
        else:
            now_ts = time.time()
            if now_ts - float(self._order_ws_latency_pause_last_log_ts) >= 60.0:
                self._order_ws_latency_pause_last_log_ts = now_ts
                logger.warning(
                    "⏸️ [风险控制] WS成交延迟暂停仍在生效: remaining=%ss (symbol=%s, exchange=%s)",
                    int(self.get_order_ws_latency_pause_remaining_seconds() or 0),
                    symbol,
                    exchange,
                )

        return {
            "enabled": True,
            "observed": True,
            "triggered": True,
            "paused": True,
            "remaining_seconds": self.get_order_ws_latency_pause_remaining_seconds(),
        }

    # ============================================================================
    # API失败暂停（全局暂停，禁止开/平仓/纠偏）
    # ============================================================================

    def is_api_failure_pause_enabled(self) -> bool:
        cfg = getattr(self.config, "api_failure_pause", None)
        return bool(cfg and bool(getattr(cfg, "enabled", False)))

    def get_api_failure_pause_remaining_seconds(self) -> Optional[float]:
        if not self._api_failure_pause_until:
            return None
        remaining = (self._api_failure_pause_until -
                     datetime.now()).total_seconds()
        return max(0.0, float(remaining))

    def should_block_api_failure_pause(self) -> tuple[bool, Optional[str]]:
        self._maybe_resume_api_failure_pause()
        remaining = self.get_api_failure_pause_remaining_seconds()
        if remaining is None or remaining <= 0:
            return False, None
        reason = self._api_failure_pause_reason or "API失败暂停中"
        return True, f"{reason} (remaining={int(remaining)}s)"

    def _maybe_resume_api_failure_pause(self) -> None:
        if not self._api_failure_pause_until:
            return
        if datetime.now() < self._api_failure_pause_until:
            return
        self._api_failure_pause_until = None
        self._api_failure_pause_reason = None
        # 若全局暂停原因就是“API失败暂停”，则恢复全局 paused 状态
        reason = (self.risk_status.pause_reason or "")
        if self.risk_status.is_paused and ("API失败暂停" in reason or "api_failure_pause" in reason):
            self.risk_status.is_paused = False
            self.risk_status.pause_reason = None
            logger.warning("▶️ [风险控制] API失败暂停到期，已自动恢复交易")
            if self.on_resume:
                self.on_resume()

    def record_api_failure(
        self,
        *,
        exchange: str,
        api: str,
        reason: str,
    ) -> Dict[str, Any]:
        """
        记录一次交易所API失败（REST/WS/持仓/余额/行情等均可）。
        在滚动窗口内达到阈值则触发全局暂停一段时间。
        """
        cfg = getattr(self.config, "api_failure_pause", None)
        if not cfg or not bool(getattr(cfg, "enabled", False)):
            return {"enabled": False}

        self._maybe_resume_api_failure_pause()

        ex = (exchange or "").lower().strip() or "unknown"
        api_name = (api or "").strip() or "unknown"
        try:
            threshold = max(1, int(getattr(cfg, "consecutive_failures", 2) or 2))
        except Exception:
            threshold = 2
        try:
            window_s = max(0.0, float(getattr(cfg, "window_seconds", 30.0) or 30.0))
        except Exception:
            window_s = 30.0
        try:
            pause_s = max(0.0, float(getattr(cfg, "pause_seconds", 60.0) or 60.0))
        except Exception:
            pause_s = 60.0
        try:
            min_interval_s = max(0.0, float(getattr(cfg, "min_interval_seconds", 0.0) or 0.0))
        except Exception:
            min_interval_s = 0.0
        if pause_s <= 0 or window_s <= 0:
            return {"enabled": True, "recorded": False, "configured": False}

        now_ts = time.time()

        # ------------------------------------------------------------------
        # 🔥 失败计数节流（按交易所）：同一交易所两次“失败记账”最短间隔
        #
        # 背景：ticker/quotes 失败时往往是“多个symbol同时失败 + 每个symbol多次重试”，
        #       若不节流会在几秒内把 failures_in_window 堆满，从而误触发长时间暂停。
        # ------------------------------------------------------------------
        if min_interval_s > 0:
            last_ts = float(self._api_failure_last_record_ts_by_exchange.get(ex, 0.0) or 0.0)
            if (now_ts - last_ts) < min_interval_s:
                return {
                    "enabled": True,
                    "exchange": ex,
                    "api": api_name,
                    "recorded": False,
                    "throttled": True,
                    "min_interval_seconds": min_interval_s,
                }
            self._api_failure_last_record_ts_by_exchange[ex] = now_ts

        dq = self._api_failure_ts_by_exchange.get(ex)
        if dq is None:
            dq = deque()
            self._api_failure_ts_by_exchange[ex] = dq
        # 清理窗口外
        while dq and (now_ts - float(dq[0])) > window_s:
            dq.popleft()
        dq.append(now_ts)

        failures = len(dq)
        triggered = False

        # ------------------------------------------------------------------
        # 🔥 记录累计次数并输出日志（不节流）
        #
        # 说明：
        # - 适配器只负责上报事件，不负责计数与输出；
        # - 这里统一输出“累计次数 + 当前窗口次数/阈值”，便于你现场断网测试确认是否在计数。
        # - 默认仅对 WS 相关事件输出（ws_disconnect/ws_recv/ws_run），避免 ticker 失败刷屏过大。
        #   若你希望所有 API 失败都输出，把 if 条件去掉即可。
        # ------------------------------------------------------------------
        try:
            is_ws_related = api_name.startswith("ws_") or ("ws_disconnect" in api_name.lower())
        except Exception:
            is_ws_related = False
        if is_ws_related:
            try:
                key = (ex, api_name)
                self._api_failure_total_count[key] += 1
                total = int(self._api_failure_total_count[key])
                logger.warning(
                    "🔁 [风控] WS异常记录: exchange=%s api=%s total=%s failures_in_window=%s/%s window=%ss reason=%s",
                    ex,
                    api_name,
                    total,
                    failures,
                    threshold,
                    int(window_s),
                    (reason or "")[:200],
                )
            except Exception:
                pass

        if failures >= threshold:
            triggered = True
            # 触发后清空，避免暂停结束后立刻被旧失败触发
            try:
                dq.clear()
            except Exception:
                pass
            self._api_failure_pause_until = datetime.now() + timedelta(seconds=pause_s)
            self._api_failure_pause_reason = (
                f"API失败暂停：exchange={ex} api={api_name} "
                f"failures≥{threshold}/{int(window_s)}s pause={int(pause_s)}s"
                + (f" reason={reason}" if reason else "")
            )
            if not self.risk_status.is_paused:
                self.risk_status.is_paused = True
                self.risk_status.pause_reason = self._api_failure_pause_reason
                logger.warning("⏸️ [风险控制] %s", self.risk_status.pause_reason or "")
                if self.on_pause:
                    self.on_pause(self.risk_status.pause_reason or "")
            else:
                # 节流提示：避免暂停期间刷屏
                if now_ts - float(self._api_failure_pause_last_log_ts) >= 60.0:
                    self._api_failure_pause_last_log_ts = now_ts
                    logger.warning("⏸️ [风险控制] API失败暂停仍在生效: %s", self._api_failure_pause_reason or "")

        return {
            "enabled": True,
            "exchange": ex,
            "api": api_name,
            "failures_in_window": failures,
            "threshold": threshold,
            "window_seconds": window_s,
            "triggered": triggered,
            "paused": bool(self.get_api_failure_pause_remaining_seconds() and self.get_api_failure_pause_remaining_seconds() > 0),
            "remaining_seconds": self.get_api_failure_pause_remaining_seconds(),
        }

    # ============================================================================
    # 持仓持续不一致暂停（全局暂停，禁止开/平仓；直到恢复一致）
    # ============================================================================

    def is_position_mismatch_pause_enabled(self) -> bool:
        cfg = getattr(self.config, "position_mismatch_pause", None)
        return bool(cfg and bool(getattr(cfg, "enabled", False)))

    def activate_position_mismatch_pause(self, reason: str) -> None:
        """进入“持仓持续不一致暂停”（不设置到期时间，直到 clear）。"""
        cfg = getattr(self.config, "position_mismatch_pause", None)
        if not cfg or not bool(getattr(cfg, "enabled", False)):
            return
        self._position_mismatch_pause_active = True
        self._position_mismatch_pause_reason = reason or "持仓持续不一致暂停"

        # 若当前未处于其他原因的全局暂停，则直接进入暂停；若已暂停则不覆盖原因
        if not self.risk_status.is_paused:
            self.risk_status.is_paused = True
            self.risk_status.pause_reason = self._position_mismatch_pause_reason
            logger.warning("⏸️ [风险控制] %s", self.risk_status.pause_reason or "")
            if self.on_pause:
                self.on_pause(self.risk_status.pause_reason or "")
        else:
            # 节流提示：避免暂停期间刷屏
            now_ts = time.time()
            if now_ts - float(self._position_mismatch_pause_last_log_ts) >= 60.0:
                self._position_mismatch_pause_last_log_ts = now_ts
                logger.warning("⏸️ [风险控制] 持仓不一致暂停仍在生效: %s", self._position_mismatch_pause_reason or "")

    def clear_position_mismatch_pause(self) -> None:
        """清除“持仓持续不一致暂停”。若当前全局暂停原因就是它，则恢复交易。"""
        if not self._position_mismatch_pause_active:
            return
        self._position_mismatch_pause_active = False
        self._position_mismatch_pause_reason = None

        reason = (self.risk_status.pause_reason or "")
        if self.risk_status.is_paused and ("持仓持续不一致暂停" in reason or "持仓不一致暂停" in reason or "持仓不一致" in reason):
            self.risk_status.is_paused = False
            self.risk_status.pause_reason = None
            logger.warning("▶️ [风险控制] 持仓不一致已恢复一致，已自动恢复交易")
            if self.on_resume:
                self.on_resume()

    def should_block_position_mismatch_pause(self) -> tuple[bool, Optional[str]]:
        """仅用于“持仓持续不一致暂停”的交易拦截（开仓/平仓都阻止）。"""
        if not self._position_mismatch_pause_active:
            return False, None
        reason = self._position_mismatch_pause_reason or "持仓持续不一致暂停"
        return True, reason

    def get_execution_pause_remaining_seconds(self) -> Optional[float]:
        """返回“执行失败暂停”的剩余秒数；若未处于该暂停则返回 None。"""
        if not self._exec_pause_until:
            return None
        remaining = (self._exec_pause_until - datetime.now()).total_seconds()
        return max(0.0, float(remaining))

    def get_execution_failure_streak(self) -> int:
        """当前连续失败计数（仅 success=False 才计数）。"""
        try:
            return max(0, int(self._exec_failure_streak))
        except Exception:
            return 0

    def get_execution_failure_threshold(self) -> int:
        """连续失败阈值（consecutive_failures）。"""
        cfg = getattr(self.config, "execution_pause", None)
        try:
            if not cfg:
                return 0
            return max(0, int(getattr(cfg, "consecutive_failures", 0) or 0))
        except Exception:
            return 0

    def is_execution_pause_enabled(self) -> bool:
        cfg = getattr(self.config, "execution_pause", None)
        return bool(cfg and bool(getattr(cfg, "enabled", False)))

    def get_execution_pause_minutes(self) -> float:
        cfg = getattr(self.config, "execution_pause", None)
        try:
            if not cfg:
                return 0.0
            return max(0.0, float(getattr(cfg, "pause_minutes", 0.0) or 0.0))
        except Exception:
            return 0.0

    def _maybe_resume_execution_pause(self) -> None:
        """若执行失败暂停已到期，自动恢复（不影响余额/网络等其他暂停原因）。"""
        if not self._exec_pause_until:
            return
        if datetime.now() < self._exec_pause_until:
            return
        # 先清理本模块的暂停状态
        self._exec_pause_until = None
        self._exec_pause_reason = None

        # 若全局暂停原因就是“执行失败暂停”，则恢复全局 paused 状态
        reason = (self.risk_status.pause_reason or "")
        if self.risk_status.is_paused and ("执行失败连续触发" in reason or "执行失败暂停" in reason or "执行失败" in reason):
            self.risk_status.is_paused = False
            self.risk_status.pause_reason = None
            logger.warning("▶️ [风险控制] 执行失败暂停到期，已自动恢复交易")
            if self.on_resume:
                self.on_resume()

    async def start(self):
        """启动风险控制器"""
        if self.running:
            return

        self.running = True

        # 启动监控任务
        if self.config.balance_management.check_interval > 0:
            self.monitor_tasks.append(
                asyncio.create_task(self._balance_monitor_loop())
            )

        if self.config.position_duration.enabled:
            self.monitor_tasks.append(
                asyncio.create_task(self._position_duration_monitor_loop())
            )

        if self.config.daily_trade_limit.enabled:
            self.monitor_tasks.append(
                asyncio.create_task(self._daily_trade_limit_monitor_loop())
            )

        logger.info("✅ [风险控制] 全局风险控制器已启动")

    async def stop(self):
        """停止风险控制器"""
        self.running = False

        for task in self.monitor_tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        self.monitor_tasks.clear()
        logger.info("🛑 [风险控制] 全局风险控制器已停止")

    # ============================================================================
    # 仓位管理
    # ============================================================================

    async def check_position_limits(
        self, symbol: str, exchange: str, new_position_quantity: Decimal, *,
        exchange_sell=None, prices=None, local_positions=None, price_lookup=None, slippage=0,
    ) -> tuple[bool, Optional[str]]:
        """Evaluate one account snapshot per exchange; unavailable reads deny new exposure."""
        try:
            quantity = Decimal(str(new_position_quantity))
            if not quantity.is_finite() or quantity <= 0:
                raise ValueError("开仓数量无效")
            snapshots = {}
            balance_snapshots = {}
            collection_started = datetime.now()
            for name, adapter in self.exchange_adapters.items():
                try:
                    snapshots[name] = await self._validated_positions(name, adapter)
                    balance_snapshots[name] = await adapter.get_balances()
                    balance = self._validate_balance_snapshot(balance_snapshots[name])
                    if balance < Decimal(str(self.config.balance_management.min_balance_close_position)):
                        raise ValueError("账户余额不足")
                except Exception:
                    self.risk_status.account_data_unavailable.add(name)
                    raise
                self.risk_status.account_data_unavailable.discard(name)

            for name in snapshots:
                try:
                    if (datetime.now() - collection_started).total_seconds() > 5:
                        raise ValueError("账户采集耗时过长，需重新读取")
                    self._validate_positions_snapshot(snapshots[name])
                    self._validate_balance_snapshot(balance_snapshots[name])
                except Exception:
                    self.risk_status.account_data_unavailable.add(name)
                    raise

            prices = prices() if callable(prices) else prices

            # Local confirmed fills may precede the exchange's account projection.
            # Keep the larger quantity for each leg instead of counting it twice.
            exposure = {}
            for name, positions in snapshots.items():
                for position in positions:
                    key = (name, position.symbol)
                    exposure[key] = exposure.get(key, Decimal(0)) + Decimal(str(position.size))
            for position in local_positions or []:
                if not position.is_open:
                    continue
                for name, size in ((position.exchange_buy, position.quantity_buy),
                                   (position.exchange_sell, position.quantity_sell)):
                    key = (name, position.symbol)
                    exposure[key] = max(exposure.get(key, Decimal(0)), Decimal(str(size)))

            pm = self.config.position_management
            occupied = {s for (_, s), size in exposure.items() if size > 0}
            if pm.max_open_symbols and len(occupied | {symbol}) > pm.max_open_symbols:
                return False, "同时开放交易对超过限制"
            symbol_limit = self._get_symbol_quantity_limit(symbol)
            if symbol_limit is None:
                symbol_limit = Decimal(str(pm.max_single_token_position))
            # Explicit USD profiles cap each leg; legacy profiles retain aggregate base limits.
            if pm.max_gross_notional_usd is None:
                existing = sum((size for (_, sym), size in exposure.items() if sym == symbol), Decimal(0))
                if symbol_limit > 0 and existing + quantity > symbol_limit:
                    return False, "单一代币持仓超过限制"
            leg_exchanges = (exchange, exchange_sell) if exchange_sell else (exchange,)
            for name in leg_exchanges:
                if symbol_limit > 0 and exposure.get((name, symbol), Decimal(0)) + quantity > symbol_limit:
                    return False, "单腿持仓数量超过限制"

            if pm.max_gross_notional_usd is not None:
                def valid_price(value):
                    value = Decimal(str(value))
                    if not value.is_finite() or value <= 0:
                        raise ValueError("价格不可用，拒绝开仓")
                    return value
                if not exchange_sell or not prices:
                    raise ValueError("双腿报价不可用")
                gross = Decimal(0)
                for (name, existing_symbol), size in exposure.items():
                    if size <= 0:
                        continue
                    price = valid_price(price_lookup(name, existing_symbol) if price_lookup else None)
                    gross += size * price
                for name in leg_exchanges:
                    price = valid_price(prices.get(name))
                    slip = Decimal(str(slippage))
                    if not slip.is_finite() or not 0 <= slip < 1:
                        raise ValueError("滑点参数无效")
                    notional = quantity * price * (1 + slip)
                    minimum = Decimal(str(pm.min_leg_notional_usd.get(name, 0)))
                    cap = Decimal(str(pm.max_leg_notional_usd)) if pm.max_leg_notional_usd is not None else None
                    if minimum > 0 and quantity * price * (1 - slip) < minimum:
                        return False, f"{name} 单腿名义价值低于最低下单金额"
                    if cap is not None and (not cap.is_finite() or cap <= 0 or notional > cap):
                        return False, f"{name} 单腿名义价值超过美元上限"
                    gross += notional
                cap = Decimal(str(pm.max_gross_notional_usd))
                if not cap.is_finite() or cap <= 0 or gross > cap:
                    return False, "双腿总名义价值超过美元上限"
            else:
                total_limit = Decimal(str(pm.max_total_position))
                if total_limit > 0 and sum(exposure.values(), Decimal(0)) + quantity > total_limit:
                    return False, "总持仓超过限制"
            return True, None
        except Exception as exc:
            return False, f"账户数据或风险参数不可用: {exc}"

    def _get_symbol_quantity_limit(self, symbol: str) -> Optional[Decimal]:
        """获取代币特定的最大持仓数量"""
        if not self.symbol_quantity_config:
            return None

        cfg = self.symbol_quantity_config.get(
            symbol) or self.symbol_quantity_config.get('default')
        if not cfg:
            return None

        max_qty = getattr(cfg, 'max_position_quantity', None)
        if max_qty is None:
            return None

        max_qty_decimal = Decimal(str(max_qty))
        return max_qty_decimal if max_qty_decimal > 0 else None

    async def _get_single_token_position_quantity(self, symbol: str) -> Decimal:
        """获取单一代币的持仓数量（绝对值累计）"""
        total_quantity = Decimal('0')
        symbol_upper = (symbol or "").upper()

        allowed = self.allowed_symbols
        for exchange_name, adapter in self.exchange_adapters.items():
            try:
                positions = await adapter.get_positions()
                for position in positions:
                    pos_symbol = (position.symbol or "").upper()
                    if allowed is not None and pos_symbol not in allowed:
                        continue
                    if pos_symbol == symbol_upper and getattr(position, "size", Decimal('0')) != 0:
                        total_quantity += self._calc_position_quantity(
                            position)
            except Exception as e:
                # 🔥 记录API失败（持仓接口）
                try:
                    self.record_api_failure(
                        exchange=str(exchange_name),
                        api="get_positions",
                        reason=str(e),
                    )
                except Exception:
                    pass
                # 🔥 只写入日志文件，不输出到控制台（避免UI抖动）
                # 如果是因为未配置API密钥导致的错误，使用debug级别
                error_msg = str(e)
                if "未配置SignerClient" in error_msg or "未配置API" in error_msg or "无法获取" in error_msg:
                    logger.debug(f"[风险控制] 获取{exchange_name}持仓失败: {e}")
                else:
                    logger.error(
                        f"[风险控制] 获取{exchange_name}持仓失败: {e}", exc_info=True)

        return total_quantity

    async def _get_total_position_quantity(self) -> Decimal:
        """获取所有代币的总持仓数量（绝对值之和）"""
        total_quantity = Decimal('0')
        allowed = self.allowed_symbols

        for exchange_name, adapter in self.exchange_adapters.items():
            try:
                positions = await adapter.get_positions()
                for position in positions:
                    if allowed is not None:
                        pos_symbol = (position.symbol or "").upper()
                        if pos_symbol not in allowed:
                            continue
                    if getattr(position, "size", Decimal('0')) != 0:
                        total_quantity += self._calc_position_quantity(
                            position)
            except Exception as e:
                # 🔥 记录API失败（持仓接口）
                try:
                    self.record_api_failure(
                        exchange=str(exchange_name),
                        api="get_positions",
                        reason=str(e),
                    )
                except Exception:
                    pass
                # 🔥 只写入日志文件，不输出到控制台（避免UI抖动）
                # 如果是因为未配置API密钥导致的错误，使用debug级别
                error_msg = str(e)
                if "未配置SignerClient" in error_msg or "未配置API" in error_msg or "无法获取" in error_msg:
                    logger.debug(f"[风险控制] 获取{exchange_name}持仓失败: {e}")
                else:
                    logger.error(
                        f"[风险控制] 获取{exchange_name}持仓失败: {e}", exc_info=True)

        return total_quantity

    @staticmethod
    def _calc_position_quantity(position: PositionData) -> Decimal:
        """根据position数据安全计算仓位数量（绝对值）"""
        size = getattr(position, "size", None)
        if size is None:
            return Decimal('0')
        try:
            return abs(Decimal(str(size)))
        except Exception:
            return Decimal('0')

    # ============================================================================
    # 账户余额管理
    # ============================================================================

    async def _balance_monitor_loop(self):
        """余额监控循环"""
        while self.running:
            try:
                await asyncio.sleep(self.config.balance_management.check_interval)

                if not self.running:
                    break

                await self._check_all_balances()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[风险控制] 余额监控错误: {e}", exc_info=True)

    async def _check_all_balances(self):
        """检查所有交易所的余额"""
        low_balance_exchanges = set()
        critical_balance_exchanges = set()

        for exchange_name, adapter in self.exchange_adapters.items():
            try:
                usdc_balance = await self._validated_balance(adapter)
                await self._validated_positions(exchange_name, adapter)
                self.risk_status.account_data_unavailable.discard(exchange_name)

                if usdc_balance is None:
                    logger.warning(f"[风险控制] {exchange_name}: 未找到USDC余额")
                    continue

                # 检查余额不足平仓阈值
                close_th = self._min_balance_close_threshold(exchange_name)
                warn_th = self._min_balance_warning_threshold(exchange_name)
                if close_th > Decimal("0") and usdc_balance < close_th:
                    critical_balance_exchanges.add(exchange_name)
                    logger.error(
                        f"🚨 [风险控制] {exchange_name}: 余额严重不足 "
                        f"({usdc_balance} < {close_th})"
                    )

                # 检查余额不足警告阈值
                elif warn_th > Decimal("0") and usdc_balance < warn_th:
                    low_balance_exchanges.add(exchange_name)
                    logger.warning(
                        f"⚠️  [风险控制] {exchange_name}: 余额不足 "
                        f"({usdc_balance} < {warn_th})"
                    )

            except Exception as e:
                self.risk_status.account_data_unavailable.add(exchange_name)
                # 🔥 记录API失败（余额接口）
                try:
                    self.record_api_failure(
                        exchange=str(exchange_name),
                        api="get_balances",
                        reason=str(e),
                    )
                except Exception:
                    pass
                # 🔥 只写入日志文件，不输出到控制台（避免UI抖动）
                # 如果是因为未配置API密钥导致的错误，使用debug级别
                error_msg = str(e)
                if "未配置SignerClient" in error_msg or "未配置API" in error_msg or "无法获取" in error_msg:
                    logger.debug(f"[风险控制] 检查{exchange_name}余额失败: {e}")
                else:
                    logger.error(
                        f"[风险控制] 检查{exchange_name}余额失败: {e}", exc_info=True)

        # 更新风险状态
        self.risk_status.low_balance_exchanges = low_balance_exchanges
        self.risk_status.critical_balance_exchanges = critical_balance_exchanges

        # 处理余额不足
        if critical_balance_exchanges:
            await self._handle_critical_balance(critical_balance_exchanges)
        elif low_balance_exchanges:
            await self._handle_low_balance(low_balance_exchanges)
        else:
            # 余额恢复正常
            if self.risk_status.low_balance_exchanges or self.risk_status.critical_balance_exchanges:
                await self._handle_balance_recovered()

    def _get_usdc_balance(self, balances: List[BalanceData]) -> Optional[Decimal]:
        """
        从余额列表中获取USDC余额

        🔥 重要：必须使用总余额（total），而不是可用余额（free）
        原因：
        - Backpack统一账户中，资金可能在借出（lend）或订单冻结中
        - 可用余额（free）可能为0，但总余额（total）不为0
        - 使用总余额才能正确判断账户是否有资金进行交易
        """
        for balance in balances:
            currency = (balance.currency or '').upper()
            if not currency:
                continue

            # 🔥 兼容 USDC 及其变体（如 USDC.E），以及部分交易所使用的 USD 标识
            if currency == 'USDC' or currency.startswith('USDC') or currency == 'USD':
                total = balance.total
                if total is not None:
                    return total

                # 某些交易所可能只提供free/used字段
                free = balance.free or Decimal('0')
                used = balance.used or Decimal('0')
                return free + used
        return None

    async def _handle_critical_balance(self, exchanges: Set[str]):
        """处理余额严重不足"""
        # 先确保进入暂停（不覆盖其他暂停原因，但会输出关键错误日志）
        critical_reason = f"余额严重不足: {', '.join(exchanges)}"
        if not self.risk_status.is_paused:
            self.risk_status.is_paused = True
            self.risk_status.pause_reason = critical_reason
            logger.error(f"🚨 [风险控制] 暂停套利: {self.risk_status.pause_reason}")
            if self.on_pause:
                self.on_pause(self.risk_status.pause_reason)
        else:
            # 已暂停：仍然需要在“严重不足”场景触发强平（避免被 warning/其他原因的 pause 挡住）
            logger.error("🚨 [风险控制] 检测到余额严重不足（系统已暂停）：%s", critical_reason)

        # 🔥 无论是否已 paused，只要检测到“严重不足”，都尝试触发强平（带节流）
        try:
            now_ts = datetime.now().timestamp()
            if self.on_close_all_positions and (now_ts - float(self._last_force_close_ts) >= 60.0):
                self._last_force_close_ts = now_ts
                self.on_close_all_positions()
            elif self.on_close_all_positions is None:
                logger.error(
                    "🚨 [风险控制] 余额严重不足但未绑定强平回调(on_close_all_positions)，无法自动平仓。")
        except Exception as exc:
            logger.error("🚨 [风险控制] 触发强制平仓回调失败: %s", exc, exc_info=True)

    async def _handle_low_balance(self, exchanges: Set[str]):
        """处理余额不足"""
        if not self.risk_status.is_paused:
            self.risk_status.is_paused = True
            self.risk_status.pause_reason = f"余额不足: {', '.join(exchanges)}"

            logger.warning(f"⚠️  [风险控制] 暂停套利: {self.risk_status.pause_reason}")

            # 触发暂停回调
            if self.on_pause:
                self.on_pause(self.risk_status.pause_reason)

    async def _handle_balance_recovered(self):
        """处理余额恢复"""
        if self.risk_status.is_paused and "余额" in (self.risk_status.pause_reason or ""):
            self.risk_status.is_paused = False
            self.risk_status.pause_reason = None

            logger.info("✅ [风险控制] 余额已恢复，恢复套利操作")

            # 触发恢复回调
            if self.on_resume:
                self.on_resume()

    # ============================================================================
    # 持仓时间限制
    # ============================================================================

    async def _position_duration_monitor_loop(self):
        """持仓时间监控循环"""
        while self.running:
            try:
                await asyncio.sleep(60)  # 每分钟检查一次

                if not self.running:
                    break

                await self._check_position_duration()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[风险控制] 持仓时间监控错误: {e}", exc_info=True)

    async def _check_position_duration(self, positions_info: Optional[Dict[str, datetime]] = None):
        """
        检查持仓时间

        Args:
            positions_info: {symbol: open_time}，如果为None则从持仓信息中获取
        """
        if not self.config.position_duration.enabled:
            return

        if positions_info is None:
            positions_info = self.position_times_provider() if self.position_times_provider else {}

        current_time = datetime.now()
        max_duration_hours = self.config.position_duration.max_position_duration

        for symbol, open_time in positions_info.items():
            duration_hours = (current_time - open_time).total_seconds() / 3600

            if duration_hours > max_duration_hours:
                logger.warning(
                    f"⚠️  [风险控制] {symbol}: 持仓时间过长 "
                    f"({duration_hours:.1f}小时 > {max_duration_hours}小时)"
                )

                if self.config.position_duration.auto_close_on_timeout:
                    if self.on_close_position:
                        await self.on_close_position(symbol, "持仓时间过长")

    # ============================================================================
    # 每日交易次数限制
    # ============================================================================

    async def _daily_trade_limit_monitor_loop(self):
        """每日交易次数监控循环"""
        while self.running:
            try:
                await asyncio.sleep(3600)  # 每小时检查一次

                if not self.running:
                    break

                await self._reset_daily_trade_count_if_needed()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[风险控制] 每日交易次数监控错误: {e}", exc_info=True)

    async def _reset_daily_trade_count_if_needed(self):
        """如果需要，重置每日交易次数"""
        today = datetime.now().strftime('%Y-%m-%d')

        if self.last_trade_date != today:
            self.daily_trade_count.clear()
            self.last_trade_date = today
            logger.debug(f"[风险控制] 重置每日交易次数: {today}")

    def check_daily_trade_limit(self) -> tuple[bool, Optional[str]]:
        """
        检查每日交易次数限制

        Returns:
            (是否允许, 拒绝原因)
        """
        if not self.config.daily_trade_limit.enabled:
            return True, None

        today = datetime.now().strftime('%Y-%m-%d')
        current_count = self.daily_trade_count.get(today, 0)

        if current_count >= self.config.daily_trade_limit.max_daily_trades:
            return False, f"每日交易次数已达上限: {current_count} >= {self.config.daily_trade_limit.max_daily_trades}"

        return True, None

    def record_trade(self):
        """记录交易"""
        today = datetime.now().strftime('%Y-%m-%d')
        self.daily_trade_count[today] = self.daily_trade_count.get(
            today, 0) + 1

    # ============================================================================
    # 交易对风险限制
    # ============================================================================

    def is_symbol_disabled(self, symbol: str) -> bool:
        """检查交易对是否被禁用"""
        return symbol in self.config.symbol_risk.disabled_symbols

    def is_symbol_high_risk(self, symbol: str) -> bool:
        """检查交易对是否为高风险"""
        return symbol in self.config.symbol_risk.high_risk_symbols

    # ============================================================================
    # 网络故障处理
    # ============================================================================

    def mark_network_failure(self, reason: str):
        """标记网络故障"""
        if not self.risk_status.network_failure:
            self.risk_status.network_failure = True
            self.risk_status.is_paused = True
            self.risk_status.pause_reason = f"网络故障: {reason}"

            logger.error(f"🚨 [风险控制] 网络故障: {reason}")

            if self.on_pause:
                self.on_pause(self.risk_status.pause_reason)

    def mark_network_recovered(self):
        """标记网络恢复"""
        if self.risk_status.network_failure:
            self.risk_status.network_failure = False

            # 如果只有网络故障导致暂停，则恢复
            if self.risk_status.pause_reason and "网络故障" in self.risk_status.pause_reason:
                self.risk_status.is_paused = False
                self.risk_status.pause_reason = None

                logger.info("✅ [风险控制] 网络已恢复，恢复套利操作")

                if self.on_resume:
                    self.on_resume()

    # ============================================================================
    # 交易所维护检测
    # ============================================================================

    def mark_exchange_maintenance(self, exchange: str):
        """标记交易所维护"""
        self.risk_status.exchange_maintenance.add(exchange)
        self.risk_status.is_paused = True
        self.risk_status.pause_reason = f"交易所维护: {exchange}"

        logger.warning(f"⚠️  [风险控制] 交易所维护: {exchange}")

        if self.on_pause:
            self.on_pause(self.risk_status.pause_reason)

    def mark_exchange_recovered(self, exchange: str):
        """标记交易所恢复"""
        self.risk_status.exchange_maintenance.discard(exchange)

        # 如果没有其他维护中的交易所，则恢复
        if not self.risk_status.exchange_maintenance:
            if self.risk_status.pause_reason and "交易所维护" in self.risk_status.pause_reason:
                self.risk_status.is_paused = False
                self.risk_status.pause_reason = None

                logger.info("✅ [风险控制] 所有交易所已恢复，恢复套利操作")

                if self.on_resume:
                    self.on_resume()

    # ============================================================================
    # 状态查询
    # ============================================================================

    def is_paused(self) -> bool:
        """检查是否暂停"""
        # 到期自动恢复（执行失败暂停 / 滑点暂停）
        self._maybe_resume_execution_pause()
        self._maybe_resume_slippage_pause()
        self._maybe_resume_order_ws_latency_pause()
        return self.risk_status.is_paused

    def get_pause_reason(self) -> Optional[str]:
        """获取暂停原因"""
        return self.risk_status.pause_reason

    def get_risk_status(self) -> RiskStatus:
        """获取风险状态"""
        # 重要：主循环会频繁调用 get_risk_status() 来决定是否 sleep/跳过。
        # 若不在这里做“到期恢复”，像滑点暂停这种纯时间闸门会卡死在 paused 状态。
        self._maybe_resume_execution_pause()
        self._maybe_resume_slippage_pause()
        self._maybe_resume_order_ws_latency_pause()
        return self.risk_status

    async def _validated_positions(self, exchange_name, adapter):
        positions = await adapter.get_positions()
        return self._validate_positions_snapshot(positions)

    @staticmethod
    def _validate_positions_snapshot(positions):
        if not isinstance(positions, list):
            raise ValueError("账户持仓数据不可用")
        for position in positions:
            size = Decimal(str(getattr(position, "size", None)))
            if not size.is_finite() or size < 0 or not getattr(position, "symbol", None):
                raise ValueError("账户持仓数据无效")
            timestamp = getattr(position, "timestamp", None)
            if timestamp is None or (datetime.now() - timestamp).total_seconds() > 5:
                raise ValueError("账户持仓数据过期")
        return positions

    async def _validated_balance(self, adapter):
        balances = await adapter.get_balances()
        return self._validate_balance_snapshot(balances)

    def _validate_balance_snapshot(self, balances):
        if not isinstance(balances, list):
            raise ValueError("账户余额数据不可用")
        for balance in balances:
            total = getattr(balance, "total", None)
            if total is None:
                free, used = getattr(balance, "free", None), getattr(balance, "used", None)
                if free is None or used is None:
                    raise ValueError("账户余额字段缺失")
                total = Decimal(str(free)) + Decimal(str(used))
            if not Decimal(str(total)).is_finite():
                raise ValueError("账户余额无效")
            timestamp = getattr(balance, "timestamp", None)
            if timestamp is None or (datetime.now() - timestamp).total_seconds() > 5:
                raise ValueError("账户余额数据过期")
        value = self._get_usdc_balance(balances)
        if value is None or not value.is_finite():
            raise ValueError("账户余额不可用")
        return value

    async def check_startup_account_state(self):
        """Read-only gate: never adopt, close, reset or resubmit pre-existing exposure."""
        for name, adapter in self.exchange_adapters.items():
            try:
                positions = await self._validated_positions(name, adapter)
                orders = await adapter.get_open_orders()
                balance = await self._validated_balance(adapter)
                if (not isinstance(orders, list) or balance is None or not balance.is_finite()
                        or getattr(adapter, "_uncertain", None)):
                    raise ValueError("账户状态不可用或订单状态不确定")
                if any(Decimal(str(p.size)) > 0 for p in positions) or orders:
                    raise ValueError("存在已有持仓或挂单")
            except Exception as exc:
                self.risk_status.account_data_unavailable.add(name)
                return False, f"{name}: {exc}；新开仓被锁定，请人工核对后重新启动"
        return True, None
