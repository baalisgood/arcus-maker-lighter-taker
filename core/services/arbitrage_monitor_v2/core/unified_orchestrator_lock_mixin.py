"""
⚠️ 本文件由 `unified_orchestrator.py` 拆分生成（只做文件拆分，不改功能逻辑）。

职责：并发去重锁（防止同一组合重复开仓/同一symbol重复平仓触发）。

来源：`core/services/arbitrage_monitor_v2/core/unified_orchestrator.py`
"""

from __future__ import annotations

import asyncio
import time
import logging
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


class UnifiedOrchestratorLockMixin:
    """由 unified_orchestrator.py 拆分出的 mixin。"""

    def _build_open_pair_key(
        self,
        symbol: str,
        exchange_buy: Optional[str],
        exchange_sell: Optional[str]
    ) -> str:
        symbol_key = (symbol or "").upper()
        buy_key = (exchange_buy or "").lower()
        sell_key = (exchange_sell or "").lower()
        return f"{symbol_key}:{buy_key}->{sell_key}"

    async def _try_register_open_pair(self, key: str) -> bool:
        # These single-loop set operations never await the long account-budget lock.
        if self._pending_open_lock.locked() or key in self._pending_open_pairs:
            return False
        self._pending_open_pairs.add(key)
        try:
            getattr(self, "_pending_open_pairs_started_at", {})[key] = time.time()
        except Exception:
            pass
        return True

    def _release_open_pair(self, key: str) -> None:
        if not key:
            return
        self._pending_open_pairs.discard(key)
        try:
            getattr(self, "_pending_open_pairs_started_at", {}).pop(key, None)
        except Exception:
            pass

    async def _release_open_pair_async(self, key: str) -> None:
        """Release immediately so a different pair's order wait cannot delay risk closes."""
        self._release_open_pair(key)

    async def _try_register_close_symbol(self, key: str) -> bool:
        async with self._pending_close_lock:
            if key in self._pending_close_symbols:
                return False
            self._pending_close_symbols.add(key)
            return True

    def _release_close_symbol(self, key: str) -> None:
        if not key:
            return
        self._pending_close_symbols.discard(key)
