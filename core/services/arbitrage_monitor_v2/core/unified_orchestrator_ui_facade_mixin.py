"""
⚠️ 本文件由 `unified_orchestrator.py` 拆分生成（只做文件拆分，不改功能逻辑）。

职责：UI/展示层薄封装（委托 UIController 收集数据与执行更新循环）。

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


class UnifiedOrchestratorUiFacadeMixin:
    """由 unified_orchestrator.py 拆分出的 mixin。"""

    def _update_ui(self):
            """兼容旧接口，委托 UI 控制器执行。"""
            self.ui_controller.update_ui()

    async def _ui_update_loop(self):
            await self.ui_controller._ui_update_loop()

    async def _update_ui_comprehensive(self):
            """兼容旧接口，委托 UI 控制器执行。"""
            await self.ui_controller.update_comprehensive()

    def _collect_multi_leg_data(self) -> List[Dict[str, Any]]:
            return self.ui_controller._collect_multi_leg_data()

    def _collect_grid_ui_data(self) -> Dict[str, Any]:
            return self.ui_controller._collect_grid_ui_data()

    def _collect_exchange_position_cache(self) -> Dict[str, Dict[str, Dict]]:
            return self.ui_controller._collect_exchange_position_cache()

    def _build_local_position_rows(self) -> List[Dict]:
            return self.ui_controller._build_local_position_rows()

    def _build_position_rows(self) -> List[Dict]:
            return self.ui_controller._build_position_rows()

    async def _update_account_balances_ui(self):
            await self.ui_controller._update_account_balances_ui()

    def _collect_ui_market_data(
            self
        ) -> Tuple[
            Dict[str, Dict[str, OrderBookData]],
            Dict[str, Dict[str, object]],
            Dict[str, List]
        ]:
            return self.ui_controller._collect_ui_market_data()
