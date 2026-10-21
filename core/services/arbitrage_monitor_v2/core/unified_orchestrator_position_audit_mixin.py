"""
⚠️ 本文件由 `unified_orchestrator.py` 拆分生成（只做文件拆分，不改功能逻辑）。

职责：持仓校验/对账与差异格式化（含紧急平仓反馈的日志处理）。

来源：`core/services/arbitrage_monitor_v2/core/unified_orchestrator.py`
"""

from __future__ import annotations

import asyncio
import time
import logging
import time
import re
from typing import Dict, Optional, Set, List, Tuple, Any
from collections import defaultdict, deque
from pathlib import Path
from decimal import Decimal, InvalidOperation
from dataclasses import asdict
from datetime import datetime, time as dt_time, timedelta
from zoneinfo import ZoneInfo
import yaml

from ..config.symbol_config import SegmentedConfigManager
from ..config.monitor_config import ConfigManager as MonitorConfigManager
from ..config.debug_config import DebugConfig
from ..config.unified_config_manager import UnifiedConfigManager
from ..config.arbitrage_config import RiskControlConfig, ExecutionConfig, ExchangeFeeConfig
from ..config.multi_leg_pairs_config import MultiLegPairsConfigManager, MultiLegPairConfig
from ..config.multi_exchange_config import (
    MultiExchangeArbitrageConfigManager,
    TradingPair,
)
# 🔥 使用V2统一决策引擎（总量驱动算法）
from ..decision.unified_decision_engine import UnifiedDecisionEngine
from ..execution.arbitrage_executor import ArbitrageExecutor, ExecutionRequest, ExecutionResult
from ..analysis.spread_calculator import SpreadCalculator, SpreadData
from ..guards.reduce_only_guard import ReduceOnlyGuard
from ..data.data_receiver import DataReceiver
from ..data.data_processor import DataProcessor
from ..data.latest_store import LatestStore
from ..display.ui_manager import UIManager, UIMode
from ..display.realtime_scroller import RealtimeScroller
from ..utils.orchestrator_utils import ThrottledLogger, LiquidityFailureLogger
from ..utils.risk_control_utils import RiskControlUtils
from ..utils.ema_spread_gate import EmaSpreadGateState, EmaUpdateResult
from .orchestrator_ui_controller import OrchestratorUIController
from .spread_pipeline import SpreadPipeline
from .reduce_only_probe_service import ReduceOnlyProbeService
from .orchestrator_bootstrap import OrchestratorBootstrap
from ..models import FundingRateData, PositionSegment, SegmentedPosition
from ..risk_control.global_risk_controller import GlobalRiskController
from ..risk_control.open_only_close_guard import (
    OpenOnlyCloseGuard,
    OpenOnlyCloseConfig,
)
from .debug_state_printer import DebugStatePrinter
from ..state.symbol_state_manager import SymbolStateManager

from core.adapters.exchanges.interface import ExchangeInterface
from core.adapters.exchanges.models import OrderBookData

from .unified_orchestrator_logging import logger


class UnifiedOrchestratorPositionAuditMixin:
    """由 unified_orchestrator.py 拆分出的 mixin。"""

    # 持仓校验轮询参数（quiet_window：最近无开/平仓活动的最小窗口）
    _position_alignment_quiet_window_seconds: float = 10.0

    def _mark_open_close_activity(self, *, action: str, symbol: str) -> None:
        """
        记录最近一次“开仓/平仓”完成时间，用于持仓校验轮询的安静窗口判断。
        - action: "open"/"close"
        - symbol: 触发来源（仅用于诊断）
        """
        try:
            self._last_open_close_activity_ts = time.time()
            self._last_open_close_activity_action = str(action or "")
            self._last_open_close_activity_symbol = str(symbol or "")
        except Exception:
            # 不影响主流程
            return

    def _can_run_position_alignment_poll(self) -> bool:
        """轮询校验前置条件：最近 quiet_window 内没有开/平仓活动，且当前无开/平仓 in-flight 任务。"""
        # ✅ 方案B（最小改动）：复用既有 in-flight 锁结构，避免在执行开/平仓过程中进行持仓校验
        try:
            pending_open = getattr(self, "_pending_open_pairs", None)
            pending_close = getattr(self, "_pending_close_symbols", None)
            if (pending_open and len(pending_open) > 0) or (pending_close and len(pending_close) > 0):
                return False
        except Exception:
            # 保守：异常时不阻塞轮询
            pass

        quiet = float(getattr(self, "_position_alignment_quiet_window_seconds", 10.0) or 10.0)
        last_ts = float(getattr(self, "_last_open_close_activity_ts", 0.0) or 0.0)
        now = time.time()
        return (now - last_ts) >= max(0.0, quiet)

    def _position_alignment_poll_interval_seconds(self) -> float:
        """
        轮询间隔（秒）：
        直接复用配置 `position_alignment_auto_fix.cooldown_seconds`。
        """
        try:
            v = float(self._position_alignment_auto_fix_cooldown_seconds())
            return max(1.0, v)
        except Exception:
            return 10.0

    # ============================================================================
    # 持仓持续不一致 -> 全局暂停（禁止开仓 & 平仓）
    # ============================================================================

    def _is_position_mismatch_pause_enabled(self) -> bool:
        # 监控模式下，决策引擎仓位可能是“模拟持仓”，与交易所真实仓位天然不一致。
        # 为避免误触发“持仓持续不一致暂停”，这里默认禁用该机制。
        # 若未来确实需要在 monitor-only 也启用，可再增加显式开关。
        if bool(getattr(self, "monitor_only_mode", True)):
            return False
        try:
            rc = getattr(self, "risk_controller", None)
            cfg = getattr(rc, "config", None)
            pmp = getattr(cfg, "position_mismatch_pause", None)
            return bool(getattr(pmp, "enabled", False))
        except Exception:
            return False

    def _position_mismatch_pause_seconds(self) -> float:
        try:
            rc = getattr(self, "risk_controller", None)
            cfg = getattr(rc, "config", None)
            pmp = getattr(cfg, "position_mismatch_pause", None)
            return max(0.0, float(getattr(pmp, "mismatch_seconds", 10.0) or 10.0))
        except Exception:
            return 10.0

    def _update_position_mismatch_pause_state(self, *, consistent: Optional[bool]) -> None:
        """
        当“全局持仓校验不一致”持续超过阈值时，触发全局暂停（禁止开仓 & 平仓）。
        一旦恢复一致，自动恢复。
        """
        if not self._is_position_mismatch_pause_enabled():
            return
        rc = getattr(self, "risk_controller", None)
        if not rc:
            return
        # consistent=None 通常表示无法形成有效对账（例如两侧都无数据），不做暂停计时
        if consistent is None:
            return

        threshold = self._position_mismatch_pause_seconds()
        now = time.time()

        if consistent is False:
            # 仅当能解析出明确 mismatch_keys 时才计时（避免数据缺失/格式异常导致误触发）
            ui = getattr(self, "_last_alignment_ui_data", None) or {}
            delta_str = ui.get("delta")
            mismatch_keys = self._parse_alignment_delta_keys(delta_str)
            if not mismatch_keys:
                try:
                    setattr(self, "_position_alignment_mismatch_first_seen_ts_global", None)
                except Exception:
                    pass
                return

            first_ts = getattr(self, "_position_alignment_mismatch_first_seen_ts_global", None)
            if first_ts is None:
                setattr(self, "_position_alignment_mismatch_first_seen_ts_global", now)
                first_ts = now

            duration = max(0.0, now - float(first_ts))
            if duration >= threshold:
                delta_show = (str(delta_str).strip() if delta_str else "")
                reason = f"持仓持续不一致暂停：不一致持续{duration:.1f}s≥{threshold:.1f}s"
                if delta_show:
                    reason += f" | delta={delta_show}"
                try:
                    rc.activate_position_mismatch_pause(reason)  # type: ignore[attr-defined]
                except Exception:
                    pass
            return

        # consistent=True：恢复一致 -> 清理计时并解除暂停
        try:
            setattr(self, "_position_alignment_mismatch_first_seen_ts_global", None)
        except Exception:
            pass
        try:
            rc.clear_position_mismatch_pause()  # type: ignore[attr-defined]
        except Exception:
            pass

    def _parse_alignment_delta_keys(self, delta: Optional[str]) -> Tuple[Tuple[str, str], ...]:
        """
        从 _format_position_deltas 输出的字符串中提取不一致键集合：
        e.g. "lighter/BTC:决策=+0.1500 交易所=+0.1350 差=-0.0150 | variational/ETH:..."
        -> (("lighter","BTC"), ("variational","ETH"))
        """
        if not delta:
            return tuple()
        s = str(delta).strip()
        if not s or s == "-":
            return tuple()
        keys: List[Tuple[str, str]] = []
        for part in s.split("|"):
            p = part.strip()
            if not p:
                continue
            # 只取前缀 "exchange/symbol:"
            m = re.match(r"^([^/]+)/([^:]+):", p)
            if not m:
                continue
            ex = (m.group(1) or "").strip().lower()
            sym = (m.group(2) or "").strip().upper()
            if ex and sym:
                keys.append((ex, sym))
        return tuple(sorted(set(keys)))

    async def _position_alignment_poll_loop(self) -> None:
        """
        后台轮询持仓校验（全局）：
        - 定时触发 `_audit_position_alignment("__global__")`
        - 仅当最近 quiet_window 秒内没有开/平仓活动，才执行一次
        """
        last_run = 0.0
        while bool(getattr(self, "running", False)):
            try:
                await asyncio.sleep(1.0)
                if not bool(getattr(self, "running", False)):
                    break

                # ------------------------------------------------------------
                # 🔍 监控模式：默认跳过“交易所持仓对账/纠偏/暂停”
                #
                # 背景：
                # - 监控模式下执行器会返回“模拟成功”，决策引擎会维护一份“模拟持仓”
                # - 交易所真实仓位通常为 0，此时对账必然“不一致”，并会误触发暂停/纠偏
                #
                # 行为：
                # - 默认不执行 _audit_position_alignment("__global__")
                # - 仅给 UI 提供一个解释性面板，避免用户误解为真实仓位异常
                #
                # 可选开关（来自 config_manager.system_mode dict）：
                # - system_mode.position_alignment_in_monitor_mode: true 时强制开启对账（高级用法）
                # ------------------------------------------------------------
                if bool(getattr(self, "monitor_only_mode", True)):
                    sys_mode: Dict[str, Any] = {}
                    try:
                        cfg_mgr = getattr(self, "config_manager", None)
                        sys_mode = (cfg_mgr.get_system_mode() if cfg_mgr else None) or {}
                    except Exception:
                        sys_mode = {}
                    if not bool(sys_mode.get("position_alignment_in_monitor_mode", False)):
                        try:
                            self._last_alignment_ui_data = {
                                "title": "🧪 持仓校验 - 监控模式（已跳过对账）",
                                "consistent": True,
                                "decision": "模拟持仓已启用：下方“套利对持仓明细”展示的是决策引擎的模拟仓位。",
                                "exchange": "监控模式默认不对账交易所真实仓位（避免与模拟仓位天然不一致导致误判）。",
                                "delta": "-",
                                "timestamp": datetime.now().strftime("%m-%d %H:%M:%S"),
                            }
                        except Exception:
                            pass
                        continue

                if not self._can_run_position_alignment_poll():
                    continue
                interval = float(self._position_alignment_poll_interval_seconds() or 10.0)
                now = time.time()
                if interval > 0 and now - last_run < interval:
                    continue
                last_run = now
                await self._refresh_arcus_position_snapshot_for_audit()
                if not self._can_run_position_alignment_poll():
                    continue
                consistent = self._audit_position_alignment("__global__")

                # 🔥 新增：持仓持续不一致 -> 全局暂停（禁止开仓 & 平仓），避免交易所/WS故障导致越纠越乱
                try:
                    self._update_position_mismatch_pause_state(consistent=consistent)
                except Exception:
                    pass

                # 若不一致且允许自动纠偏，则由纠偏模块决定是否执行（内部有冷却与 inflight 保护）
                if consistent is False and self._is_position_alignment_auto_fix_enabled():
                    # ✅ 仅当“连续两次轮询都发现同一组(exchange/token)不一致”时，才触发纠偏（抑制缓存抖动）
                    try:
                        ui = getattr(self, "_last_alignment_ui_data", None) or {}
                        delta_str = ui.get("delta")
                        mismatch_keys = self._parse_alignment_delta_keys(delta_str)
                        if not mismatch_keys:
                            setattr(self, "_position_alignment_last_mismatch_keys", None)
                            setattr(self, "_position_alignment_same_mismatch_count", 0)
                            continue

                        last_keys = getattr(self, "_position_alignment_last_mismatch_keys", None)
                        last_count = int(getattr(self, "_position_alignment_same_mismatch_count", 0) or 0)
                        if mismatch_keys == last_keys:
                            last_count += 1
                        else:
                            last_keys = mismatch_keys
                            last_count = 1

                        setattr(self, "_position_alignment_last_mismatch_keys", last_keys)
                        setattr(self, "_position_alignment_same_mismatch_count", last_count)

                        # ✅ 满足“连续两次确认”后即可触发；后续若仍不一致，则由全局 cooldown 控制触发频率
                        # （之前用 last_count==2 会导致：不一致持续存在时 last_count 变成3/4/...，反而永远不再触发）
                        if last_count >= 2 and self._should_run_position_alignment_auto_fix_global():
                            # ✅ 更严格的全局熔断：只要 UI 对账里出现 >=2 个不一致 token，本轮绝不纠偏
                            # 设计意图：多 token 同时不一致，几乎总是缓存/解析/交易所侧系统性问题；纠偏风险更高。
                            try:
                                token_set = {sym for (_ex, sym) in (mismatch_keys or tuple()) if sym}
                            except Exception:
                                token_set = set()
                            if len(token_set) >= 2:
                                # 节流：避免持续不一致时刷屏
                                try:
                                    now3 = time.time()
                                    last_log3 = float(
                                        getattr(self, "_position_alignment_auto_fix_circuit_last_log_ts", 0.0) or 0.0
                                    )
                                    if now3 - last_log3 >= 30.0:
                                        setattr(self, "_position_alignment_auto_fix_circuit_last_log_ts", now3)
                                        logger.warning(
                                            "🧮 [持仓纠偏] 熔断：UI对账出现>=2个不一致token，本轮跳过纠偏：tokens=%s mismatches=%s",
                                            sorted(list(token_set)),
                                            mismatch_keys,
                                        )
                                except Exception:
                                    pass
                                continue
                            asyncio.create_task(self._run_position_alignment_auto_fix_all())
                        elif last_count >= 2:
                            # 轻量诊断：已连续确认不一致，但因门禁（cooldown/inflight/monitor_only）未触发
                            # 这条日志会节流，避免刷屏
                            try:
                                now2 = time.time()
                                last_log = float(
                                    getattr(self, "_position_alignment_auto_fix_skip_last_log_ts", 0.0) or 0.0
                                )
                                if now2 - last_log >= 30.0:
                                    setattr(self, "_position_alignment_auto_fix_skip_last_log_ts", now2)
                                    logger.warning(
                                        "🧮 [持仓纠偏] 已连续确认不一致，但本轮未触发（门禁）：reason=%s mismatches=%s",
                                        self._why_block_position_alignment_auto_fix_global(),
                                        mismatch_keys,
                                    )
                            except Exception:
                                pass
                    except Exception:
                        pass
                else:
                    # 一致时重置连续计数
                    try:
                        setattr(self, "_position_alignment_last_mismatch_keys", None)
                        setattr(self, "_position_alignment_same_mismatch_count", 0)
                    except Exception:
                        pass
            except asyncio.CancelledError:
                break
            except Exception:
                # 轮询异常不影响主循环；下次再试
                continue

    def _is_position_alignment_auto_fix_enabled(self) -> bool:
        """是否启用“持仓校验不一致 -> 自动纠偏下单”"""
        try:
            rc = getattr(self, "risk_controller", None)
            cfg = getattr(rc, "config", None)
            paf = getattr(cfg, "position_alignment_auto_fix", None)
            return bool(getattr(paf, "enabled", False))
        except Exception:
            return False

    def _position_alignment_auto_fix_cooldown_seconds(self) -> float:
        try:
            rc = getattr(self, "risk_controller", None)
            cfg = getattr(rc, "config", None)
            paf = getattr(cfg, "position_alignment_auto_fix", None)
            return float(getattr(paf, "cooldown_seconds", 900.0) or 900.0)
        except Exception:
            return 900.0

    def _position_alignment_auto_fix_window_seconds(self) -> float:
        """纠偏执行限频窗口（秒）；0 表示不限制。"""
        try:
            rc = getattr(self, "risk_controller", None)
            cfg = getattr(rc, "config", None)
            paf = getattr(cfg, "position_alignment_auto_fix", None)
            return float(getattr(paf, "window_seconds", 0.0) or 0.0)
        except Exception:
            return 0.0

    def _position_alignment_auto_fix_max_exec_per_window(self) -> int:
        """纠偏执行限频：窗口内最大执行次数；0 表示不限制。"""
        try:
            rc = getattr(self, "risk_controller", None)
            cfg = getattr(rc, "config", None)
            paf = getattr(cfg, "position_alignment_auto_fix", None)
            return max(0, int(getattr(paf, "max_executions_per_window", 0) or 0))
        except Exception:
            return 0

    def _can_run_position_alignment_auto_fix_by_window_limit(self) -> bool:
        """
        限制“纠偏下单执行”的频率（不影响校验轮询）。
        计数口径：每次开始执行 _run_position_alignment_auto_fix_all() 计 1 次（无论最终是否成功）。
        """
        max_exec = self._position_alignment_auto_fix_max_exec_per_window()
        window_s = max(0.0, float(self._position_alignment_auto_fix_window_seconds() or 0.0))
        if max_exec <= 0 or window_s <= 0:
            return True
        now = time.time()
        dq = getattr(self, "_position_alignment_auto_fix_exec_ts", None)
        if dq is None:
            dq = deque()
            setattr(self, "_position_alignment_auto_fix_exec_ts", dq)
        # 清理窗口外记录
        try:
            while dq and (now - float(dq[0])) > window_s:
                dq.popleft()
        except Exception:
            try:
                dq.clear()
            except Exception:
                pass
        return len(dq) < max_exec

    def _record_position_alignment_auto_fix_execution(self) -> None:
        """记录一次纠偏执行（用于窗口限频）。"""
        now = time.time()
        dq = getattr(self, "_position_alignment_auto_fix_exec_ts", None)
        if dq is None:
            dq = deque()
            setattr(self, "_position_alignment_auto_fix_exec_ts", dq)
        try:
            dq.append(now)
        except Exception:
            pass

    def _why_block_position_alignment_auto_fix_global(self) -> str:
        """返回“为何本轮不允许触发全局纠偏”的简短原因（用于轻量 warning 日志）。"""
        try:
            if not self._is_position_alignment_auto_fix_enabled():
                return "disabled"
            if bool(getattr(self, "monitor_only_mode", True)):
                return "monitor_only"
            # 🔥 纠偏闸门：允许“持仓不一致暂停”下继续纠偏，但仍受更高优先级暂停约束（API失败/滑点/执行失败等）
            try:
                rc = getattr(self, "risk_controller", None)
                if rc and hasattr(rc, "should_block_position_alignment_auto_fix"):
                    blocked, reason = rc.should_block_position_alignment_auto_fix()  # type: ignore[attr-defined]
                    if blocked:
                        r = (reason or "").strip() or "paused"
                        # 避免过长刷屏
                        if len(r) > 90:
                            r = r[:90] + "…"
                        return f"risk_pause:{r}"
            except Exception:
                pass
            if not self._can_run_position_alignment_auto_fix_by_window_limit():
                max_exec = self._position_alignment_auto_fix_max_exec_per_window()
                window_s = self._position_alignment_auto_fix_window_seconds()
                return f"window_limit_reached=max{max_exec}/per{window_s:.0f}s"
            if bool(getattr(self, "_position_alignment_auto_fix_inflight_global", False)):
                return "inflight"
            now = time.time()
            cooldown = max(0.0, self._position_alignment_auto_fix_cooldown_seconds())
            last_ts = float(getattr(self, "_position_alignment_auto_fix_last_ts_global", 0.0) or 0.0)
            if cooldown > 0 and now - last_ts < cooldown:
                remaining = max(0.0, cooldown - (now - last_ts))
                return f"cooldown_remaining={remaining:.1f}s"
            return "unknown"
        except Exception:
            return "unknown"

    def _should_run_position_alignment_auto_fix_global(self) -> bool:
        """全局冷却与并发保护：避免反复触发“全局纠偏”"""
        if not self._is_position_alignment_auto_fix_enabled():
            return False
        if bool(getattr(self, "monitor_only_mode", True)):
            return False
        # 🔥 若系统处于“全局暂停”，则按“纠偏专用闸门”决定是否允许纠偏下单：
        # - 允许：持仓持续不一致暂停（只禁开/平仓，不禁纠偏）
        # - 禁止：API失败暂停/滑点暂停/执行失败暂停/其它原因暂停
        try:
            rc = getattr(self, "risk_controller", None)
            if rc:
                if hasattr(rc, "should_block_position_alignment_auto_fix"):
                    blocked, _ = rc.should_block_position_alignment_auto_fix()  # type: ignore[attr-defined]
                else:
                    blocked, _ = rc.should_block_execution_pause()  # type: ignore[attr-defined]
                if bool(blocked):
                    return False
        except Exception:
            pass
        # 🔥 新增：窗口限频（优先级高于 cooldown）
        if not self._can_run_position_alignment_auto_fix_by_window_limit():
            return False
        now = time.time()
        cooldown = max(0.0, self._position_alignment_auto_fix_cooldown_seconds())
        last_ts = float(getattr(self, "_position_alignment_auto_fix_last_ts_global", 0.0) or 0.0)
        if cooldown > 0 and now - last_ts < cooldown:
            return False
        if bool(getattr(self, "_position_alignment_auto_fix_inflight_global", False)):
            return False
        return True

    def _normalize_position_map(
        self,
        raw: Dict[Tuple[str, str], Decimal],
    ) -> Dict[Tuple[str, str], Decimal]:
        """对 (exchange, symbol) 做标准化聚合，确保与校验逻辑一致。"""
        totals: Dict[Tuple[str, str], Decimal] = {}
        for (exchange, symbol), qty in (raw or {}).items():
            ex = (exchange or "").lower()
            sym = self._normalize_symbol_for_comparison(symbol or "")
            if not ex or not sym:
                continue
            totals[(ex, sym)] = totals.get((ex, sym), Decimal("0")) + (qty or Decimal("0"))
        return totals

    def _resolve_trade_symbol_for_exchange_token(
        self,
        *,
        exchange: str,
        token: str,
    ) -> Optional[str]:
        """
        将 (exchange, token) 映射为可下单的交易对 symbol。
        优先级：
        1) 交易所持仓缓存中出现过的 symbol（最可信）
        2) 决策引擎 pair_positions 里该交易所对应腿的 symbol（用于“需要补仓但当前无持仓缓存”的场景）
        """
        ex_key = (exchange or "").lower()
        tok = (token or "").upper()
        if not ex_key or not tok:
            return None

        # 1) 先从交易所持仓缓存找
        cache = getattr(self.ui_manager, "exchange_position_cache", None) or {}
        ex_cache = cache.get(ex_key) or cache.get(exchange) or {}
        if isinstance(ex_cache, dict):
            for sym_raw in ex_cache.keys():
                try:
                    if self._normalize_symbol_for_comparison(str(sym_raw).upper()) == tok:
                        return str(sym_raw)
                except Exception:
                    continue

        # 2) 再从决策引擎的活跃套利对找
        pair_positions = getattr(self.decision_engine, "pair_positions", {}) or {}
        for sym_key, pair_map in pair_positions.items():
            if not isinstance(pair_map, dict):
                continue
            for pp in pair_map.values():
                try:
                    buy_ex = (getattr(pp, "exchange_buy", "") or "").lower()
                    sell_ex = (getattr(pp, "exchange_sell", "") or "").lower()
                    buy_sym = (getattr(pp, "buy_symbol", None) or getattr(pp, "symbol", None) or sym_key or "")
                    sell_sym = (getattr(pp, "sell_symbol", None) or getattr(pp, "symbol", None) or sym_key or "")
                    if buy_ex == ex_key and self._normalize_symbol_for_comparison(str(buy_sym).upper()) == tok:
                        return str(buy_sym)
                    if sell_ex == ex_key and self._normalize_symbol_for_comparison(str(sell_sym).upper()) == tok:
                        return str(sell_sym)
                except Exception:
                    continue
        return None

    def _one_grid_quantity_for_token(self, token: str) -> Optional[Decimal]:
        """
        获取“一个网格的数量”（每格数量），用于纠偏安全阀：
        - 你的要求：只有当持仓不一致数量 <= 该 token 的单格数量时才允许纠偏
        - 数量来源：策略配置 quantity_config.base_quantity（不同 token 不同）
        """
        tok = self._normalize_symbol_for_comparison(token or "")
        if not tok:
            return None
        try:
            cm = getattr(self.decision_engine, "config_manager", None)
            symbol_configs = getattr(cm, "symbol_configs", None) if cm else None
            if isinstance(symbol_configs, dict):
                for sym_key, cfg in symbol_configs.items():
                    try:
                        sym_key_str = str(sym_key).upper().strip()
                        token_candidate = None
                        if "_" in sym_key_str:
                            parts = [p for p in sym_key_str.split("_") if p]
                            if len(parts) >= 3:
                                token_candidate = parts[-1]
                        if token_candidate is None:
                            token_candidate = sym_key_str

                        if self._normalize_symbol_for_comparison(token_candidate) != tok:
                            continue
                        qty_cfg = getattr(cfg, "quantity_config", None)
                        base = getattr(qty_cfg, "base_quantity", None) if qty_cfg else None
                        if base is None:
                            continue
                        return Decimal(str(base))
                    except Exception:
                        continue
            # fallback: default_config
            default_cfg = getattr(cm, "default_config", None) if cm else None
            qty_cfg2 = getattr(default_cfg, "quantity_config", None) if default_cfg else None
            base2 = getattr(qty_cfg2, "base_quantity", None) if qty_cfg2 else None
            if base2 is None:
                return None
            return Decimal(str(base2))
        except Exception:
            return None

    async def _execute_single_leg_adjustment(
        self,
        *,
        exchange: str,
        symbol: str,
        quantity: Decimal,
        is_buy: bool,
        reduce_only: bool,
        label: str,
    ) -> bool:
        """对单一交易所/单一交易对执行补仓/减仓（市价单优先）"""
        try:
            qty = Decimal(str(quantity))
        except Exception:
            return False
        if qty <= Decimal("0"):
            return False

        adapter = (getattr(self.executor, "exchange_adapters", None) or {}).get((exchange or "").lower())
        if not adapter:
            logger.warning("🧮 [持仓纠偏] %s: 未找到交易所适配器，跳过: %s %s", label, exchange, symbol)
            return False

        # 市价单确认超时：lighter 可单独配置，否则默认10秒
        timeout_cfg = getattr(getattr(self.executor, "config", None), "order_execution", None)
        timeout = 10
        try:
            if exchange.lower() == "lighter":
                t = getattr(timeout_cfg, "lighter_market_order_timeout", None)
                if t not in (None, 0):
                    timeout = max(1, int(t))
        except Exception:
            timeout = 10

        logger.warning(
            "🧮 [持仓纠偏] %s: 下单调整 exchange=%s symbol=%s side=%s qty=%s reduce_only=%s",
            label,
            exchange,
            symbol,
            "BUY" if is_buy else "SELL",
            qty,
            bool(reduce_only),
        )
        order = await self.executor._place_market_order(  # type: ignore[attr-defined]
            adapter=adapter,
            symbol=symbol,
            quantity=qty,
            is_buy=is_buy,
            reduce_only=bool(reduce_only),
            request=None,
        )
        if not order:
            logger.error("🧮 [持仓纠偏] %s: 下单失败 exchange=%s symbol=%s", label, exchange, symbol)
            return False

        filled = await self.executor.order_monitor.wait_for_order_fill(  # type: ignore[attr-defined]
            order,
            adapter,
            timeout=timeout,
            is_market_order=True,
            require_full_fill=False,
        )
        if filled is None:
            logger.error(
                "🧮 [持仓纠偏] %s: 成交未确认(超时/无回执) exchange=%s symbol=%s order_id=%s",
                label, exchange, symbol, getattr(order, "id", None) or "-"
            )
            return False
        logger.warning(
            "🧮 [持仓纠偏] %s: 成交确认 exchange=%s symbol=%s filled=%s order_id=%s",
            label, exchange, symbol, filled, getattr(order, "id", None) or "-"
        )
        try:
            ex_key = (exchange or "").lower()
            sym_key = self._normalize_symbol_for_comparison(symbol)
            if ex_key and sym_key:
                if not hasattr(self, "_position_alignment_last_adjust_ts"):
                    self._position_alignment_last_adjust_ts = {}
                self._position_alignment_last_adjust_ts[(ex_key, sym_key)] = time.time()
        except Exception:
            pass
        return True

    async def _run_position_alignment_auto_fix_all(self) -> None:
        """
        全局纠偏：以“决策引擎”为准，把交易所实际持仓调整到一致。
        核心逻辑（与你描述一致）：
        - delta = expected(decision) - actual(exchange)
        - delta > 0：补仓/回补空头（BUY）
        - delta < 0：减仓/加空（SELL）
        - 若需要跨过0（从多到空或从空到多），拆成两步：先平到0(reduce-only)，再开到目标
        """
        if not self._should_run_position_alignment_auto_fix_global():
            return
        # ✅ 双保险：只要 UI 对账里出现 >=2 个不一致 token，本轮绝不纠偏（即便内部筛选后只剩 1 个可纠偏 token）
        try:
            ui = getattr(self, "_last_alignment_ui_data", None) or {}
            delta_str = ui.get("delta")
            mismatch_keys = self._parse_alignment_delta_keys(delta_str)
            token_set = {sym for (_ex, sym) in (mismatch_keys or tuple()) if sym}
            if len(token_set) >= 2:
                logger.warning(
                    "🧮 [持仓纠偏] 熔断：UI对账出现>=2个不一致token，跳过本轮纠偏：tokens=%s mismatches=%s",
                    sorted(list(token_set)),
                    mismatch_keys,
                )
                return
        except Exception:
            pass
        # 🔥 计入一次“纠偏执行尝试”（无论最终是否成功）
        self._record_position_alignment_auto_fix_execution()
        self._position_alignment_auto_fix_inflight_global = True
        self._position_alignment_auto_fix_last_ts_global = time.time()
        try:
            tolerance = getattr(self.decision_engine, "quantity_epsilon", Decimal("0.0001"))
            if tolerance <= Decimal("0"):
                tolerance = Decimal("0.0001")

            decision_raw = self._collect_decision_net_positions()
            exchange_raw = self._collect_exchange_net_positions()
            expected_map = self._normalize_position_map(decision_raw)
            actual_map = self._normalize_position_map(exchange_raw)

            keys = set(expected_map.keys()) | set(actual_map.keys())
            if not keys:
                return

            # ------------------------------------------------------------------
            # ✅ 安全门：只有“两边都有持仓（且都已读到非零）”才允许纠偏
            #
            # 你提出的约束：
            # - 若某个 token 在两边交易所中，一边有持仓另一边为0/缺失/未读取成功 -> 不执行纠偏
            # - 只有双方都非零，但数量不同（例如 1 vs 2）才执行纠偏
            #
            # 实现策略：
            # - 用 decision_engine.pair_positions 反推“每个 token 涉及哪些交易所”（通常=2边）
            # - 对每个 token，若参与交易所数 < 2 -> 不纠偏（避免单边误操作）
            # - 若任意参与交易所的 expected 或 actual 的绝对值 ≤ tolerance -> 整个 token 跳过
            #   （把 0/缺失/未读到 与 “显著非零” 区分开）
            # ------------------------------------------------------------------
            token_to_exchanges: Dict[str, Set[str]] = defaultdict(set)
            try:
                pair_positions = getattr(self.decision_engine, "pair_positions", {}) or {}
                for sym_key, pair_map in (pair_positions or {}).items():
                    if not isinstance(pair_map, dict):
                        continue
                    for pp in pair_map.values():
                        try:
                            buy_ex = (getattr(pp, "exchange_buy", "") or "").lower()
                            sell_ex = (getattr(pp, "exchange_sell", "") or "").lower()
                            buy_sym = (getattr(pp, "buy_symbol", None) or getattr(pp, "symbol", None) or sym_key or "")
                            sell_sym = (getattr(pp, "sell_symbol", None) or getattr(pp, "symbol", None) or sym_key or "")
                            buy_tok = self._normalize_symbol_for_comparison(str(buy_sym).upper())
                            sell_tok = self._normalize_symbol_for_comparison(str(sell_sym).upper())
                            if buy_ex and buy_tok:
                                token_to_exchanges[buy_tok].add(buy_ex)
                            if sell_ex and sell_tok:
                                token_to_exchanges[sell_tok].add(sell_ex)
                        except Exception:
                            continue
            except Exception:
                token_to_exchanges = defaultdict(set)

            # 计算哪些 token 具备“纠偏前置条件”（两边都非零）
            eligible_tokens: Set[str] = set()
            skipped_tokens_reason_logged: Set[str] = set()
            for tok, ex_set in (token_to_exchanges or {}).items():
                try:
                    exs = {str(x).lower() for x in (ex_set or set()) if str(x).strip()}
                    if len(exs) < 2:
                        continue
                    ok = True
                    for ex in exs:
                        exp_v = expected_map.get((ex, tok), Decimal("0"))
                        act_v = actual_map.get((ex, tok), Decimal("0"))
                        # 任意一边 expected 或 actual 为 0/缺失/过小，都认为“不满足双边持仓前置条件”
                        if abs(exp_v) <= tolerance or abs(act_v) <= tolerance:
                            ok = False
                            break
                    if ok:
                        eligible_tokens.add(tok)
                except Exception:
                    continue

            # ------------------------------------------------------------------
            # ✅ 安全阀：若“需要纠偏的 token 数量 >= 2”，则本轮不执行任何纠偏下单
            #
            # 设计意图：
            # - 同时出现多个 token 不一致，往往是“缓存不完整/解析失败/交易所侧返回异常/系统性故障”
            # - 为避免误把“未读到=0”当成缺仓去补仓，直接熔断整轮纠偏更安全
            # - 仍会打印日志，便于你人工确认后再处理
            # ------------------------------------------------------------------
            need_fix_tokens: Set[str] = set()
            try:
                for (ex, tok) in sorted(keys):
                    expected = expected_map.get((ex, tok), Decimal("0"))
                    actual = actual_map.get((ex, tok), Decimal("0"))
                    delta = expected - actual
                    if abs(delta) <= tolerance:
                        continue

                    # 必须能映射到两边交易所关系，且满足“双边非零”条件
                    if not (token_to_exchanges and tok in token_to_exchanges and tok in eligible_tokens):
                        continue

                    one_grid = self._one_grid_quantity_for_token(tok)
                    if one_grid is None or one_grid <= Decimal("0"):
                        continue
                    if abs(delta) > (abs(one_grid) + tolerance):
                        continue

                    symbol = self._resolve_trade_symbol_for_exchange_token(exchange=ex, token=tok)
                    if not symbol:
                        continue

                    need_fix_tokens.add(tok)
                    if len(need_fix_tokens) >= 2:
                        break
            except Exception:
                need_fix_tokens = set()

            if len(need_fix_tokens) >= 2:
                logger.warning(
                    "🧮 [持仓纠偏] 跳过本轮纠偏（需纠偏token数量>=2）：tokens=%s",
                    sorted(list(need_fix_tokens)),
                )
                return

            adjusted = 0
            skipped = 0
            skipped_tokens_grid_logged: Set[str] = set()
            logged_safety_valves_tokens: Set[str] = set()
            for (ex, tok) in sorted(keys):
                expected = expected_map.get((ex, tok), Decimal("0"))
                actual = actual_map.get((ex, tok), Decimal("0"))
                delta = expected - actual
                if abs(delta) <= tolerance:
                    continue

                # ✅ 计算安全阀状态（用于统一诊断日志）
                has_pair_relation = bool(token_to_exchanges and (tok in token_to_exchanges))
                is_eligible_token = bool(has_pair_relation and (tok in eligible_tokens))
                one_grid = self._one_grid_quantity_for_token(tok)
                one_grid_ok = bool(one_grid is not None and one_grid > Decimal("0"))
                abs_delta = abs(delta)
                within_one_grid = bool(one_grid_ok and abs_delta <= (abs(one_grid) + tolerance))

                # ✅ 统一打印“安全阀检查状态”，便于排查（每个 token 每轮只打印一次）
                if tok not in logged_safety_valves_tokens:
                    logged_safety_valves_tokens.add(tok)
                    logger.warning(
                        "🧮 [持仓纠偏] 安全阀检查：代币=%s 交易所=%s 差额=%s 绝对差额=%s "
                        "是否识别到双腿=%s 双腿持仓是否齐全=%s 单格数量=%s 单格数量有效=%s "
                        "差额是否在单格内=%s 容忍值=%s",
                        tok,
                        ex,
                        delta,
                        abs_delta,
                        has_pair_relation,
                        is_eligible_token,
                        one_grid if one_grid is not None else "-",
                        one_grid_ok,
                        within_one_grid,
                        tolerance,
                    )

                # ✅ 双边持仓前置条件：不满足则整组 token 跳过（避免“读错为0”导致误纠偏）
                if has_pair_relation:
                    if not is_eligible_token:
                        # 限流：同一 token 每次纠偏尝试最多提示一次
                        if tok not in skipped_tokens_reason_logged:
                            skipped_tokens_reason_logged.add(tok)
                            exs = sorted({str(x).lower() for x in (token_to_exchanges.get(tok) or set())})
                            # 汇总两边值，便于你从日志直接判断“哪边为0/缺失”
                            exp_show = {e: str(expected_map.get((e, tok), Decimal("0"))) for e in exs}
                            act_show = {e: str(actual_map.get((e, tok), Decimal("0"))) for e in exs}
                            logger.warning(
                                "🧮 [持仓纠偏] 跳过token（双边持仓不齐全/可能未读到）：token=%s exchanges=%s expected=%s actual=%s tolerance=%s",
                                tok,
                                exs,
                                exp_show,
                                act_show,
                                tolerance,
                            )
                        skipped += 1
                        continue
                else:
                    # 没有在 pair_positions 中找到该 token 的两边交易所关系（保守：不纠偏）
                    skipped += 1
                    continue

                # ✅ 安全阀：单腿差额不得超过“一个网格的数量”（不同 token 不同）
                if not one_grid_ok:
                    # 无法确定单格数量时，保守跳过，避免下单风险
                    if tok not in skipped_tokens_grid_logged:
                        skipped_tokens_grid_logged.add(tok)
                        logger.warning(
                            "🧮 [持仓纠偏] 跳过token（无法确定单格数量）：token=%s exchange=%s delta=%s",
                            tok,
                            ex,
                            delta,
                        )
                    skipped += 1
                    continue
                if not within_one_grid:
                    if tok not in skipped_tokens_grid_logged:
                        skipped_tokens_grid_logged.add(tok)
                        logger.warning(
                            "🧮 [持仓纠偏] 跳过token（差额超过单格数量）：token=%s one_grid=%s tolerance=%s",
                            tok,
                            one_grid,
                            tolerance,
                        )
                    skipped += 1
                    continue

                # ✅ 若交易所持仓缓存时间戳未更新（仍早于纠偏成交时间），跳过本轮
                try:
                    last_adj_map = getattr(self, "_position_alignment_last_adjust_ts", {})
                    last_adj_ts = last_adj_map.get((ex, tok))
                    cache_map = getattr(self, "_exchange_position_ts", {})
                    cache_ts = cache_map.get((ex, tok))
                    if last_adj_ts is not None and cache_ts is not None and cache_ts <= last_adj_ts:
                        skipped += 1
                        continue
                except Exception:
                    pass

                symbol = self._resolve_trade_symbol_for_exchange_token(exchange=ex, token=tok)
                if not symbol:
                    logger.warning(
                        "🧮 [持仓纠偏] 无法映射交易对，跳过: exchange=%s token=%s expected=%s actual=%s delta=%s",
                        ex, tok, expected, actual, delta
                    )
                    skipped += 1
                    continue

                # 计算是否跨0：若跨0则拆两单
                if actual != 0 and (actual > 0) != (expected > 0) and expected != 0:
                    # 先平到0（reduce-only）
                    close_qty = abs(actual)
                    close_is_buy = True if actual < 0 else False  # short -> BUY, long -> SELL
                    ok1 = await self._execute_single_leg_adjustment(
                        exchange=ex,
                        symbol=symbol,
                        quantity=close_qty,
                        is_buy=close_is_buy,
                        reduce_only=True,
                        label=f"{ex}:{tok}:close_to_zero",
                    )
                    # 再开到目标（非 reduce-only）
                    open_qty = abs(expected)
                    open_is_buy = True if expected > 0 else False
                    ok2 = await self._execute_single_leg_adjustment(
                        exchange=ex,
                        symbol=symbol,
                        quantity=open_qty,
                        is_buy=open_is_buy,
                        reduce_only=False,
                        label=f"{ex}:{tok}:open_to_target",
                    )
                    if ok1 and ok2:
                        adjusted += 1
                    else:
                        skipped += 1
                    continue

                # 不跨0：单笔修正
                qty = abs(delta)
                is_buy = True if delta > 0 else False
                # reduce-only 判定：若目标更接近0，属于减仓方向
                reduce_only = abs(expected) < abs(actual) and actual != 0
                ok = await self._execute_single_leg_adjustment(
                    exchange=ex,
                    symbol=symbol,
                    quantity=qty,
                    is_buy=is_buy,
                    reduce_only=bool(reduce_only),
                    label=f"{ex}:{tok}:delta",
                )
                if ok:
                    adjusted += 1
                else:
                    skipped += 1

            logger.warning(
                "🧮 [持仓纠偏] 全局纠偏完成: adjusted=%s skipped=%s cooldown=%ss",
                adjusted,
                skipped,
                int(self._position_alignment_auto_fix_cooldown_seconds()),
            )

            # ✅ 纠偏结束后主动再做一次“全局持仓校验”，刷新 UI 的校验面板；
            # 同时再延迟复查一次，容忍 WS 推送/缓存更新延迟，避免 UI 仍显示旧的不一致状态。
            try:
                self._audit_position_alignment("__global__")
            except Exception:
                pass

            async def _delayed_refresh() -> None:
                try:
                    await asyncio.sleep(2.0)
                    self._audit_position_alignment("__global__")
                except Exception:
                    pass

            try:
                asyncio.create_task(_delayed_refresh())
            except Exception:
                pass
        except Exception as exc:
            logger.error("🧮 [持仓纠偏] 全局扫描调度异常: %s", exc, exc_info=True)
        finally:
            self._position_alignment_auto_fix_inflight_global = False

    def _handle_emergency_close_feedback(
        self,
        symbol: str,
        request: ExecutionRequest,
        result: Optional[ExecutionResult],
        action_label: str,
    ) -> None:
        if not result or not getattr(result, "emergency_closes", None):
            return
        for entry in result.emergency_closes:
            qty_decimal = Decimal("0")
            try:
                qty_decimal = Decimal(str(entry.get("quantity", "0")))
            except Exception:
                qty_decimal = Decimal("0")
            logger.warning(
                "🧯 [紧急平仓反馈] %s %s | 交易所=%s | 数量=%s | 上下文=%s/%s | 状态=%s",
                symbol,
                action_label,
                entry.get("exchange", "-"),
                qty_decimal,
                entry.get("context", "-"),
                entry.get("exchange_role", "-"),
                entry.get("status", "-"),
            )

    async def _refresh_arcus_position_snapshot_for_audit(self) -> None:
        rc = getattr(self, "risk_controller", None)
        if rc is None:
            return
        for exchange, adapter in getattr(self, "exchange_adapters", {}).items():
            if exchange.lower() != "arcus":
                continue
            if (getattr(adapter, "_positions_snapshot_valid", False)
                    and time.monotonic() - getattr(adapter, "_positions_snapshot_at", 0.0) <= 5):
                continue
            try:
                await rc._validated_positions(exchange, adapter)
            except Exception as exc:
                adapter._positions_snapshot_valid = False
                logger.debug("[持仓校验] Arcus 快照未确认: %s", exc)

    def _audit_position_alignment(self, symbol: str) -> Optional[bool]:
        decision_map = self._collect_decision_net_positions()
        exchange_map = self._collect_exchange_net_positions()
        rc = getattr(self, "risk_controller", None)
        unavailable = set(getattr(getattr(rc, "risk_status", None), "account_data_unavailable", set()))
        for exchange, adapter in getattr(self, "exchange_adapters", {}).items():
            if exchange.lower() == "arcus" and (
                not getattr(adapter, "_positions_snapshot_valid", False)
                or time.monotonic() - getattr(adapter, "_positions_snapshot_at", 0.0) > 5
            ):
                unavailable.add(exchange)
        if unavailable:
            self._last_alignment_ui_data = {
                "title": "持仓校验 - 未确认（账户数据未就绪、失败或过期）",
                "consistent": None,
                "decision": self._format_position_map(decision_map),
                "exchange": "未确认: " + ", ".join(sorted(unavailable)),
                "delta": None,
                "timestamp": datetime.now().strftime("%m-%d %H:%M:%S"),
            }
            return None
        audit_key = symbol or "__global__"
        consistent = self._position_maps_consistent(decision_map, exchange_map)
        decision_items = tuple(sorted(decision_map.items()))
        exchange_items = tuple(sorted(exchange_map.items()))
        snapshot = f"{decision_items}|{exchange_items}|{consistent}"
        last_snapshot = self._last_alignment_snapshot.get(audit_key)
        last_time = self._last_alignment_log_time.get(audit_key, 0.0)
        now = time.time()

        # 若状态一致且未变化且间隔未到，跳过重复打印
        if (
            consistent
            and snapshot == last_snapshot
            and now - last_time < self._alignment_log_interval
        ):
            # ✅ 但 UI 仍需要按轮询频率刷新“时间”，否则面板看起来像卡住了
            try:
                if getattr(self, "_last_alignment_ui_data", None):
                    self._last_alignment_ui_data["timestamp"] = datetime.now().strftime("%m-%d %H:%M:%S")
            except Exception:
                pass
            return

        decision_display = self._format_position_map(decision_map)
        exchange_display = self._format_position_map(exchange_map)
        header = "✅ 一致" if consistent else "⚠️ 不一致"

        # 🔥 提取当前涉及的代币（用于显示上下文）
        symbol_tokens = set()
        if symbol:
            # 从 LIGHTER_PARADEX_SOL 提取 SOL
            parts = symbol.split('_')
            if len(parts) >= 3:
                symbol_tokens.add(parts[-1])  # 最后一个通常是代币名

        # 如果当前操作的代币有持仓，显示具体的交易对；否则显示全局
        has_relevant_position = any(
            token in symbol_tokens
            for _, token in list(decision_map.keys()) + list(exchange_map.keys())
        )

        if has_relevant_position and symbol_tokens:
            title = f"🧮 持仓校验 - {symbol} | {header}"
        else:
            title = f"🧮 持仓校验 - 全局 (触发自: {symbol}) | {header}"

        base_lines = [
            "",
            "=" * 80,
            title,
            "=" * 80,
            "📊 决策引擎:",
            f"   {decision_display}",
            "",
            "📟 交易所缓存:",
            f"   {exchange_display}",
        ]
        mismatch_display = None
        if consistent:
            base_lines.append("=" * 80)
            # 常规运行不需要频繁打印持仓校验明细：降为 debug（需手动开启 debug 才可见）
            logger.debug("\n".join(base_lines))
        else:
            mismatch_display = self._format_position_deltas(
                decision_map, exchange_map)
            base_lines.extend(
                [
                    "",
                    "📋 差异明细:",
                    f"   {mismatch_display}" if mismatch_display else "   -",
                    "=" * 80,
                ]
            )
            # 常规运行不需要频繁打印持仓校验明细：降为 debug（需手动开启 debug 才可见）
            logger.debug("\n".join(base_lines))

        # 供 UI 使用的简洁快照（不直接打印）
        self._last_alignment_ui_data = {
            "title": title,
            "consistent": consistent,
            "decision": decision_display,
            "exchange": exchange_display,
            "delta": mismatch_display,
            "timestamp": datetime.now().strftime("%m-%d %H:%M:%S"),
        }

        self._last_alignment_snapshot[audit_key] = snapshot
        self._last_alignment_log_time[audit_key] = now
        return bool(consistent)

    def get_alignment_ui_data(self) -> Optional[Dict[str, Any]]:
        """获取最近一次持仓校验的简洁快照，供UI显示。"""
        # 兜底：如果 monitor-only 且未开启对账，则始终返回解释性面板，避免 UI 显示空白/误解。
        if bool(getattr(self, "monitor_only_mode", True)):
            sys_mode: Dict[str, Any] = {}
            try:
                cfg_mgr = getattr(self, "config_manager", None)
                sys_mode = (cfg_mgr.get_system_mode() if cfg_mgr else None) or {}
            except Exception:
                sys_mode = {}
            if not bool(sys_mode.get("position_alignment_in_monitor_mode", False)):
                return {
                    "title": "🧪 持仓校验 - 监控模式（已跳过对账）",
                    "consistent": True,
                    "decision": "模拟持仓已启用：下方“套利对持仓明细”展示的是决策引擎的模拟仓位。",
                    "exchange": "监控模式默认不对账交易所真实仓位（避免与模拟仓位天然不一致导致误判）。",
                    "delta": "-",
                    "timestamp": datetime.now().strftime("%m-%d %H:%M:%S"),
                }
        return self._last_alignment_ui_data

    def _collect_decision_net_positions(self) -> Dict[Tuple[str, str], Decimal]:
        """
        收集决策引擎的净持仓，按 (交易所, 标的) 分组

        对于多腿套利（例如 LIGHTER_PAXG_XAU），会拆分成：
        - ('lighter', 'XAU'): +0.043  (买入腿，标准化后)
        - ('lighter', 'PAXG'): -0.043 (卖出腿，标准化后)
        """
        totals: Dict[Tuple[str, str], Decimal] = defaultdict(Decimal)
        epsilon = getattr(self.decision_engine,
                          "quantity_epsilon", Decimal("0.0001"))

        pair_positions = getattr(self.decision_engine,
                                 "pair_positions", {}) or {}
        for pair_map in pair_positions.values():
            for pair_position in pair_map.values():
                qty = getattr(pair_position, "total_quantity", Decimal("0"))
                if qty <= epsilon:
                    continue

                buy_exchange = (pair_position.exchange_buy or "").lower()
                sell_exchange = (pair_position.exchange_sell or "").lower()
                buy_symbol_raw = (
                    pair_position.buy_symbol or pair_position.symbol or "").upper()
                sell_symbol_raw = (
                    pair_position.sell_symbol or pair_position.symbol or "").upper()

                # 🔥 标准化symbol名称，确保与交易所侧一致（去除 -USD-PERP 等后缀）
                buy_symbol = self._normalize_symbol_for_comparison(
                    buy_symbol_raw)
                sell_symbol = self._normalize_symbol_for_comparison(
                    sell_symbol_raw)

                if buy_exchange and buy_symbol:
                    totals[(buy_exchange, buy_symbol)] += qty
                if sell_exchange and sell_symbol:
                    totals[(sell_exchange, sell_symbol)] -= qty

        if not totals:
            raw_positions = getattr(
                self.decision_engine, "positions", {}) or {}
            for position in raw_positions.values():
                qty = getattr(position, "total_quantity", Decimal("0"))
                if qty <= epsilon:
                    continue

                buy_exchange = (position.exchange_buy or "").lower()
                sell_exchange = (position.exchange_sell or "").lower()
                buy_symbol_raw = (
                    getattr(position, "buy_symbol", None) or position.symbol or "").upper()
                sell_symbol_raw = (
                    getattr(position, "sell_symbol", None) or position.symbol or "").upper()

                # 🔥 标准化symbol名称，确保与交易所侧一致
                buy_symbol = self._normalize_symbol_for_comparison(
                    buy_symbol_raw)
                sell_symbol = self._normalize_symbol_for_comparison(
                    sell_symbol_raw)

                if buy_exchange and buy_symbol:
                    totals[(buy_exchange, buy_symbol)] += qty
                if sell_exchange and sell_symbol:
                    totals[(sell_exchange, sell_symbol)] -= qty

        return {
            key: qty for key, qty in totals.items() if abs(qty) > epsilon
        }

    def _collect_exchange_net_positions(self) -> Dict[Tuple[str, str], Decimal]:
        """
        收集交易所的实际持仓，按 (交易所, 标的) 分组

        对于多腿套利，会分别记录每个标的的持仓：
        - ('lighter', 'XAU-USD-PERP'): +0.043
        - ('lighter', 'PAXG-USD-PERP'): -0.0453
        """
        totals: Dict[Tuple[str, str], Decimal] = defaultdict(Decimal)
        cache: Dict[str, Dict[str, Dict[str, Any]]] = {}
        try:
            cache = self._collect_exchange_position_cache()
            if cache:
                try:
                    self.ui_manager.update_exchange_position_cache(cache)
                except Exception as exc:
                    logger.debug("⚠️ [持仓校验] 无法刷新 UI 持仓缓存: %s", exc)
        except Exception as exc:
            logger.debug("⚠️ [持仓校验] 采集交易所持仓缓存失败: %s", exc)
            cache = {}

        if not cache:
            cache = getattr(self.ui_manager,
                            "exchange_position_cache", None) or {}

        for exchange, positions in cache.items():
            exchange_key = (exchange or "").lower()
            if not exchange_key:
                continue
            for symbol, payload in positions.items():
                symbol_key_raw = (symbol or "").upper()
                symbol_key = self._normalize_symbol_for_comparison(
                    symbol_key_raw)
                if not symbol_key:
                    continue
                try:
                    ts = payload.get("timestamp")
                    ts_val = None
                    if isinstance(ts, datetime):
                        ts_val = float(ts.timestamp())
                    elif isinstance(ts, (int, float)):
                        ts_val = float(ts)
                    if ts_val is not None:
                        if not hasattr(self, "_exchange_position_ts"):
                            self._exchange_position_ts = {}
                        self._exchange_position_ts[(exchange_key, symbol_key)] = ts_val
                except Exception:
                    pass

                size_raw = payload.get("size", 0)
                try:
                    size = Decimal(str(size_raw))
                except Exception:
                    size = Decimal("0")
                side = str(payload.get("side") or "").strip().lower()
                if side.startswith("short") or side.startswith("sell"):
                    size = -abs(size)
                elif side.startswith("long") or side.startswith("buy"):
                    size = abs(size)
                totals[(exchange_key, symbol_key)] += size

        epsilon = getattr(self.decision_engine,
                          "quantity_epsilon", Decimal("0.0001"))
        return {
            key: qty for key, qty in totals.items() if abs(qty) > epsilon
        }

    def _normalize_symbol_for_comparison(self, symbol: str) -> str:
        """
        标准化标的名称用于比较

        将 PAXG-USD-PERP 和 PAXG 都标准化为 PAXG，
        让它们在校验时能够匹配上
        """
        if not symbol:
            return ""
        # 统一分隔符，兼容 BTC/USDC:PERP、BTC-USDC:PERP 等形式
        base = symbol.upper().replace("/", "-").replace(":", "-")
        # 通过非字母数字切分，保留关键 Token，做到“包含即匹配”
        tokens = [tok for tok in re.split(r"[^A-Z0-9]+", base) if tok]
        if not tokens:
            return base
        suffix_whitelist = {"USD", "USDC", "USDT", "PERP", "SPOT", "FUTURES"}
        for token in tokens:
            if token not in suffix_whitelist:
                return token
        return tokens[0]

    def _position_maps_consistent(
        self,
        left: Dict[Tuple[str, str], Decimal],
        right: Dict[Tuple[str, str], Decimal],
    ) -> bool:
        """
        检查两个持仓映射是否一致（支持标的名称标准化匹配）

        例如：
        - left: ('lighter', 'PAXG-USD-PERP')
        - right: ('lighter', 'PAXG')
        会被标准化为同一个键进行比较
        """
        tolerance = getattr(self.decision_engine,
                            "quantity_epsilon", Decimal("0.0001"))
        if tolerance <= Decimal("0"):
            tolerance = Decimal("0.0001")

        # 🔥 标准化所有键，让 PAXG 和 PAXG-USD-PERP 能够匹配
        left_normalized: Dict[Tuple[str, str], Decimal] = {}
        for (exchange, symbol), qty in left.items():
            normalized_key = (
                exchange, self._normalize_symbol_for_comparison(symbol))
            left_normalized[normalized_key] = left_normalized.get(
                normalized_key, Decimal("0")) + qty

        right_normalized: Dict[Tuple[str, str], Decimal] = {}
        for (exchange, symbol), qty in right.items():
            normalized_key = (
                exchange, self._normalize_symbol_for_comparison(symbol))
            right_normalized[normalized_key] = right_normalized.get(
                normalized_key, Decimal("0")) + qty

        # 使用标准化后的键进行比较
        keys = set(left_normalized.keys()) | set(right_normalized.keys())
        for key in keys:
            if abs(left_normalized.get(key, Decimal("0")) - right_normalized.get(key, Decimal("0"))) > tolerance:
                return False
        return True

    def _format_position_deltas(
        self,
        left: Dict[Tuple[str, str], Decimal],
        right: Dict[Tuple[str, str], Decimal],
    ) -> str:
        # 🔥 标准化所有键，确保 ETH 和 ETH-USDC-PERP 能够匹配
        left_normalized: Dict[Tuple[str, str], Decimal] = {}
        for (exchange, symbol), qty in left.items():
            normalized_key = (
                exchange, self._normalize_symbol_for_comparison(symbol))
            left_normalized[normalized_key] = left_normalized.get(
                normalized_key, Decimal("0")) + qty

        right_normalized: Dict[Tuple[str, str], Decimal] = {}
        for (exchange, symbol), qty in right.items():
            normalized_key = (
                exchange, self._normalize_symbol_for_comparison(symbol))
            right_normalized[normalized_key] = right_normalized.get(
                normalized_key, Decimal("0")) + qty

        # 使用标准化后的键进行格式化
        tolerance = getattr(self.decision_engine,
                            "quantity_epsilon", Decimal("0.0001"))
        if tolerance <= Decimal("0"):
            tolerance = Decimal("0.0001")
        keys = sorted(set(left_normalized.keys()) |
                      set(right_normalized.keys()))
        mismatch_parts: List[str] = []
        for key in keys:
            l_val = left_normalized.get(key, Decimal("0"))
            r_val = right_normalized.get(key, Decimal("0"))
            delta = r_val - l_val
            if abs(delta) <= tolerance:
                continue
            exchange, symbol = key
            mismatch_parts.append(
                f"{exchange}/{symbol}:决策={l_val:+.4f} 交易所={r_val:+.4f} 差={delta:+.4f}"
            )
        return " | ".join(mismatch_parts) if mismatch_parts else "-"

    def _format_position_map(self, data: Dict[Tuple[str, str], Decimal]) -> str:
        if not data:
            return "-"

        # 🔥 标准化所有键，确保 ETH 和 ETH-USDC-PERP 能够匹配
        normalized: Dict[Tuple[str, str], Decimal] = {}
        for (exchange, symbol), qty in data.items():
            normalized_key = (
                exchange, self._normalize_symbol_for_comparison(symbol))
            normalized[normalized_key] = normalized.get(
                normalized_key, Decimal("0")) + qty

        parts: List[str] = []
        for (exchange, symbol) in sorted(normalized.keys()):
            qty = normalized[(exchange, symbol)]
            parts.append(f"{exchange}/{symbol}:{qty:+.4f}")
        return " ".join(parts) if parts else "-"
