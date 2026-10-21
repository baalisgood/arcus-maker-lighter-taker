"""
⚠️ 本文件由 `unified_orchestrator.py` 拆分生成（只做文件拆分，不改功能逻辑）。

职责：数量/精度与最小下单量处理（数量格式化、拆单数量计算、按交易所最小量映射）。

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


class UnifiedOrchestratorQuantityMixin:
    """由 unified_orchestrator.py 拆分出的 mixin。"""

    def _get_min_order_size(self, symbol: str) -> Optional[Decimal]:
            try:
                config = self.config_manager.get_config(symbol)
                min_size = getattr(config.quantity_config, 'min_order_size', None)
                if min_size in (None, 0):
                    return None
                return Decimal(str(min_size))
            except Exception:
                return None

    def _build_min_exchange_order_qty_map(self, symbol: str) -> Dict[str, Decimal]:
            """
            根据 symbol 配置构建 {exchange: min_qty} 映射，供执行器约束最小下单量。
            """
            result: Dict[str, Decimal] = {}
            try:
                config = self.config_manager.get_config(symbol)
                qty_cfg = getattr(config, "quantity_config", None)
                raw_map = getattr(qty_cfg, "exchange_min_order_qty", None) or {}
                if isinstance(raw_map, dict):
                    for exch, value in raw_map.items():
                        if value in (None, 0):
                            continue
                        try:
                            result[str(exch).lower()] = Decimal(str(value))
                        except (InvalidOperation, ValueError, TypeError):
                            logger.warning(
                                "⚠️ [配置] %s exchange_min_order_qty[%s]=%s 无法解析，已忽略",
                                symbol,
                                exch,
                                value,
                            )
            except Exception as exc:
                logger.warning("⚠️ [配置] 读取 %s 最小下单量失败: %s", symbol, exc)
            return result

    def _get_quantity_config(self, symbol: str):
            """获取交易对的数量配置"""
            from ..config.arbitrage_config import QuantityConfig
            symbol_upper = symbol.upper()
            config = self.config_manager.get_config(symbol_upper).quantity_config
            if not config:
                config = QuantityConfig()
            return config

    def _calculate_segment_target_quantity(
            self,
            symbol: str,
            quantity_config
        ) -> Decimal:
            """计算单段应持有的目标数量"""
            base_quantity = Decimal(str(quantity_config.base_quantity or 0))
            ratio = Decimal('1')
            target_quantity = base_quantity * ratio
            return self._format_quantity(symbol, target_quantity)

    def _calculate_partial_order_quantity(
            self,
            symbol: str,
            target_quantity: Decimal,
            remaining_quantity: Decimal
        ) -> Decimal:
            """根据拆单配置计算本次应下单的数量"""
            if remaining_quantity <= Decimal('0'):
                return Decimal('0')

            grid_cfg = self.config_manager.get_config(symbol).grid_config
            ratio_value = grid_cfg.segment_partial_order_ratio or 1.0
            ratio = Decimal(str(ratio_value))
            if ratio <= Decimal('0'):
                chunk = remaining_quantity
            else:
                ratio = min(ratio, Decimal('1'))
                chunk = target_quantity * ratio

            if chunk <= Decimal('0'):
                chunk = remaining_quantity

            order_quantity = min(remaining_quantity, chunk)

            min_partial = Decimal(str(grid_cfg.min_partial_order_quantity or 0))
            if min_partial > Decimal('0') and order_quantity < min_partial:
                if remaining_quantity < min_partial:
                    order_quantity = remaining_quantity
                else:
                    order_quantity = min(remaining_quantity, max(chunk, min_partial))

            order_quantity = self._format_quantity(symbol, order_quantity)
            if order_quantity <= Decimal('0') and remaining_quantity > Decimal('0'):
                order_quantity = self._format_quantity(symbol, remaining_quantity)
            return order_quantity

    def _format_quantity(self, symbol: str, quantity: Decimal) -> Decimal:
            """按交易对精度格式化数量"""
            from decimal import ROUND_DOWN, InvalidOperation
            config = self._get_quantity_config(symbol)
            precision = max(0, getattr(config, 'quantity_precision', 4) or 0)
            step = Decimal('1').scaleb(-precision)
            try:
                formatted = quantity.quantize(step, rounding=ROUND_DOWN)
            except InvalidOperation:
                formatted = quantity
            return formatted
