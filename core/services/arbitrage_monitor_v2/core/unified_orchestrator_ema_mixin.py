"""
⚠️ 本文件由 `unified_orchestrator.py` 拆分生成（只做文件拆分，不改功能逻辑）。

职责：EMA 价差门槛（状态管理/采样/开仓与平仓门槛判断）。

来源：`core/services/arbitrage_monitor_v2/core/unified_orchestrator.py`
"""

from __future__ import annotations

import asyncio
import time
import logging
import time
import re
from typing import Dict, Optional, Set, List, Tuple, Any
from collections import defaultdict
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


class UnifiedOrchestratorEmaMixin:
    """由 unified_orchestrator.py 拆分出的 mixin。"""

    def _get_ema_gate_state(self, *, symbol: str, action: str, window_seconds: float, strict: bool) -> EmaSpreadGateState:
            key = (symbol, action)
            last = self._ema_spread_gate_states.get(key)
            if last is not None:
                last_window, last_strict, state = last
                if abs(last_window - float(window_seconds)) < 1e-9 and bool(last_strict) == bool(strict):
                    return state
            state = EmaSpreadGateState(window_seconds=float(window_seconds), strict_per_second=bool(strict))
            self._ema_spread_gate_states[key] = (float(window_seconds), bool(strict), state)
            return state

    def _observe_spread_ema(
            self,
            *,
            symbol: str,
            action: str,
            value_pct: float,
            window_seconds: float,
            strict: bool,
        ):
            state = self._get_ema_gate_state(
                symbol=symbol,
                action=action,
                window_seconds=window_seconds,
                strict=strict,
            )
            return state.observe(float(value_pct))

    @staticmethod
    def _get_ema_delta_pct_from_cfg(cfg: Any) -> float:
        """允许偏离绝对值Δ（单位：百分比值，例如 0.004 表示 0.004%）。"""
        try:
            delta = float(getattr(cfg, "ema_spread_deviation_pct", 0.0) or 0.0)
            return max(0.0, delta)
        except Exception:
            return 0.0

    def _passes_ema_spread_gate_open(
            self,
            *,
            symbol: str,
            spread_pct: float,
            grid_level: int,
            observed: Optional[EmaUpdateResult] = None,
        ) -> Tuple[bool, Optional[str]]:
            """
            EMA门槛（开仓）：
            - EMA(open_spread_pct) >= Tn - Δ
            - 严格模式断秒 -> 拦截
            """
            try:
                cfg = self.config_manager.get_config(symbol).grid_config
            except Exception:
                return True, None
            if not bool(getattr(cfg, "ema_spread_gate_enabled", False)):
                return True, None
            window = float(getattr(cfg, "ema_spread_window_seconds", 30.0) or 30.0)
            strict = bool(getattr(cfg, "ema_spread_strict_samples_per_second", False))
            res = observed
            if res is None:
                res = self._observe_spread_ema(
                    symbol=symbol,
                    action="open",
                    value_pct=float(spread_pct),
                    window_seconds=window,
                    strict=strict,
                )
            if not res.strict_ok:
                return False, res.strict_reason or "EMA严格模式断秒"

            initial = float(getattr(cfg, "initial_spread_threshold", 0.0) or 0.0)
            delta = self._get_ema_delta_pct_from_cfg(cfg)
            # Tn
            n = max(1, int(grid_level))
            threshold = initial + (n - 1) * float(getattr(cfg, "grid_step", 0.0) or 0.0)
            min_ema = threshold - delta
            if res.ema is None:
                return False, f"EMA尚未初始化(window={window}s)"
            if float(res.ema) < float(min_ema):
                return False, (
                    f"EMA开仓门槛未满足: ema={float(res.ema):.4f}% < {min_ema:.4f}% "
                    f"(T{n}={threshold:.4f}%, Δ={delta:.4f}%, window={window}s)"
                )
            return True, None

    def _passes_ema_spread_gate_close(
            self,
            *,
            symbol: str,
            relative_spread_pct: float,
            observed: Optional[EmaUpdateResult] = None,
        ) -> Tuple[bool, Optional[str]]:
            """
            EMA门槛（平仓）：
            - EMA(relative_spread_pct) <= Tclose + Δ
            - 其中 Tclose 使用决策引擎的“平仓持续性阈值”口径（基于当前持仓段数，符合 T(n-1) 逻辑）
            - 严格模式断秒 -> 拦截
            """
            try:
                cfg_obj = self.config_manager.get_config(symbol)
            except Exception:
                return True, None
            cfg = cfg_obj.grid_config
            if not bool(getattr(cfg, "ema_spread_gate_enabled", False)):
                return True, None
            window = float(getattr(cfg, "ema_spread_window_seconds", 30.0) or 30.0)
            strict = bool(getattr(cfg, "ema_spread_strict_samples_per_second", False))
            res = observed
            if res is None:
                res = self._observe_spread_ema(
                    symbol=symbol,
                    action="close",
                    value_pct=float(relative_spread_pct),
                    window_seconds=window,
                    strict=strict,
                )
            if not res.strict_ok:
                return False, res.strict_reason or "EMA严格模式断秒"

            delta = self._get_ema_delta_pct_from_cfg(cfg)
            # 使用决策引擎同口径计算当前持仓对应的平仓阈值（T(n-1)/T0）
            pos = self.decision_engine.get_position(symbol)
            actual_qty = pos.total_quantity if pos else Decimal("0")
            try:
                close_threshold = float(self.decision_engine._get_close_persistence_threshold(actual_qty, cfg_obj))  # type: ignore[attr-defined]
            except Exception:
                close_threshold = float(getattr(cfg, "initial_spread_threshold", 0.0) or 0.0) * 0.4
            max_ema = close_threshold + delta
            if res.ema is None:
                return False, f"EMA尚未初始化(window={window}s)"
            if float(res.ema) > float(max_ema):
                return False, (
                    f"EMA平仓门槛未满足: ema={float(res.ema):.4f}% > {max_ema:.4f}% "
                    f"(Tclose={close_threshold:.4f}%, Δ={delta:.4f}%, window={window}s)"
                )
            return True, None
