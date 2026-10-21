"""
⚠️ 本文件由 `unified_orchestrator.py` 拆分生成（只做文件拆分，不改功能逻辑）。

职责：执行结果解析工具（成交量/成交价推断、数量安全转换等）。

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


class UnifiedOrchestratorExecutionUtilsMixin:
    """由 unified_orchestrator.py 拆分出的 mixin。"""

    def _extract_filled_quantity(
            self,
            execution_result: ExecutionResult,
            fallback: Decimal
        ) -> Decimal:
            """根据执行结果推断实际成交数量"""
            qty_decimal = self._safe_decimal(getattr(execution_result, "success_quantity", None))
            if qty_decimal > Decimal("0"):
                return qty_decimal

            derived = self._derive_quantity_from_orders(execution_result)
            if derived > Decimal("0"):
                return derived

            if fallback > Decimal("0"):
                logger.warning(
                    "⚠️ [统一调度] 执行结果未返回实际成交量，已按0处理 (预期=%s)",
                    fallback,
                )
            return Decimal("0")

    @staticmethod
    def _safe_decimal(value: Any) -> Decimal:
        if value is None:
            return Decimal("0")
        try:
            return Decimal(str(value))
        except Exception:
            return Decimal("0")

    def _derive_quantity_from_orders(self, execution_result: ExecutionResult) -> Decimal:
            quantities: List[Decimal] = []
            for order in (execution_result.order_buy, execution_result.order_sell):
                if order is None:
                    continue
                filled_value = getattr(order, "filled", None)
                filled_decimal = self._safe_decimal(filled_value)
                if filled_decimal > Decimal("0"):
                    quantities.append(filled_decimal)
            if len(quantities) == 2 and quantities[0] == quantities[1]:
                return quantities[0]
            return Decimal("0")

    def _resolve_execution_price(
            self,
            order,
            fallback: Decimal
        ) -> Decimal:
            """
            获取订单的实际成交价，若不存在则回退到理论价格
            """
            from typing import Any, Optional

            def _safe_decimal(value: Any) -> Optional[Decimal]:
                """安全地将任意值转换为 Decimal"""
                if value is None:
                    return None
                if isinstance(value, Decimal):
                    return value
                try:
                    return Decimal(str(value))
                except:
                    return None

            price_candidates: List[Optional[Decimal]] = []

            if order:
                price_candidates.extend([
                    order.average,
                    order.cost / order.filled if order.cost and order.filled else None,
                    order.price if self.monitor_only_mode else None,
                ])

                # 兼容 trades / raw_data 中的价格
                for trade in order.trades or []:
                    price_candidates.append(_safe_decimal(trade.get("price")))

                if order.raw_data:
                    price_candidates.append(_safe_decimal(order.raw_data.get("price")))

            for candidate in price_candidates:
                if candidate is not None and candidate > Decimal("0"):
                    return candidate

            return fallback if self.monitor_only_mode else Decimal("0")

    async def _validate_segmented_open(self, request):
        if request.symbol in getattr(self, "_position_risk_close_reasons", {}):
            return False, "该套利对风控退出中或失败锁存"
        if self._bootstrap_positions_block_open:
            return False, "启动持仓恢复未通过"
        try:
            snapshots = {}
            expected = getattr(request, "_expected_position_quantity", Decimal("0"))
            for exchange, leg_symbol, expected_side in ((request.exchange_buy, request.buy_symbol, "long"), (request.exchange_sell, request.sell_symbol, "short")):
                adapter = self.exchange_adapters[exchange]
                balances = await adapter.get_balances()
                self.risk_controller._validate_balance_snapshot(balances)
                positions = await self.risk_controller._validated_positions(exchange, adapter)
                orders = await adapter.get_open_orders()
                if not isinstance(orders, list) or orders:
                    return False, "挂单状态不可用或存在未结束挂单"
                snapshots[exchange] = (balances, positions)
                matching = [row for row in positions if self._normalize_symbol_for_comparison(row.symbol) == self._normalize_symbol_for_comparison(leg_symbol or request.symbol)]
                size = sum((Decimal(str(row.size)) for row in matching if str(getattr(row.side, "value", row.side)).lower() == expected_side), Decimal("0"))
                if size != expected or any(Decimal(str(row.size)) > 0 and str(getattr(row.side, "value", row.side)).lower() != expected_side for row in matching):
                    return False, "实仓与分段意图不一致"
                if getattr(adapter, "_uncertain", None):
                    return False, "订单状态不确定"
            if request.symbol in self._position_risk_close_reasons:
                return False, "账户读取期间触发风控退出"
            current = self.decision_engine.get_position(request.symbol)
            if (current.total_quantity if current else Decimal("0")) != expected:
                return False, "账户读取期间分段持仓发生变化"
            if current and (current.exchange_buy != request.exchange_buy or current.exchange_sell != request.exchange_sell or (current.buy_symbol or request.symbol) != (request.buy_symbol or request.symbol) or (current.sell_symbol or request.symbol) != (request.sell_symbol or request.symbol)):
                return False, "账户读取期间分段方向发生变化"
            for balances, positions in snapshots.values():
                self.risk_controller._validate_balance_snapshot(balances)
                self.risk_controller._validate_positions_snapshot(positions)
            blocked, reason, _ = self._open_only_close_guard.should_block_open(exchange_buy=request.exchange_buy, exchange_sell=request.exchange_sell, symbol=request.symbol)
            if blocked:
                return False, reason
            from dataclasses import replace
            buy = self.executor._get_live_market_price(request.exchange_buy, request.buy_symbol or request.symbol, True)
            sell = self.executor._get_live_market_price(request.exchange_sell, request.sell_symbol or request.symbol, False)
            if buy is None or sell is None or not buy.is_finite() or not sell.is_finite() or buy <= 0 or sell <= 0:
                return False, "账户读取后行情不可用或过期"
            view = replace(request.spread_data, price_buy=buy, price_sell=sell, spread_abs=sell-buy, spread_pct=float((sell-buy)/buy*100))
            threshold = request.grid_threshold_pct
            if threshold is None:
                threshold = Decimal(str(self.config_manager.get_config(request.symbol).grid_config.initial_spread_threshold))
            if Decimal(str(self.decision_engine._calculate_net_spread(view))) < Decimal(str(threshold)):
                return False, "账户读取后净价差低于本格阈值"
            # Reconcile confirmed fills against both account projections; never add them twice.
            actual, confirmed, leg_symbols = {}, {}, {}
            normalize = self._normalize_symbol_for_comparison
            for exchange, (_, positions) in snapshots.items():
                for row in positions:
                    size = Decimal(str(row.size))
                    if size > 0:
                        side = str(getattr(row.side, "value", row.side)).lower()
                        if side not in ("long", "short"):
                            return False, "持仓方向不可用"
                        key = (exchange, normalize(row.symbol), side)
                        actual[key] = actual.get(key, Decimal("0")) + size
            for symbol, position in self.decision_engine.iter_pair_positions():
                size = Decimal(str(position.total_quantity))
                if not size.is_finite() or size < 0:
                    return False, "本地持仓数量无效"
                if not size:
                    continue
                for exchange, leg_symbol, side in ((position.exchange_buy, position.buy_symbol or symbol, "long"),
                                                   (position.exchange_sell, position.sell_symbol or symbol, "short")):
                    if exchange in snapshots:
                        key = (exchange, normalize(leg_symbol), side)
                        confirmed[key] = confirmed.get(key, Decimal("0")) + size
                        leg_symbols[key] = leg_symbol
            if actual != confirmed:
                return False, "账户总持仓与已确认分段持仓不一致"
            if getattr(self.executor, "_unknown_execution_pairs", None):
                return False, "订单状态不确定"

            def positive(value):
                value = Decimal(str(value))
                if not value.is_finite() or value <= 0:
                    raise ValueError("名义价值限额、数量或报价无效")
                return value

            guard = self._open_only_close_guard
            cap = positive(self.config_manager.get_config(request.symbol).risk_config.max_position_value)
            quantity = positive(request.quantity)
            for exchange, leg_symbol, price in ((request.exchange_buy, request.buy_symbol or request.symbol, buy),
                                                (request.exchange_sell, request.sell_symbol or request.symbol, sell)):
                equity = self.risk_controller._validate_balance_snapshot(snapshots[exchange][0])
                if guard._cfg.low_balance.enabled and equity < guard._min_equity_threshold(exchange):
                    return False, f"{exchange} 实时账户余额低于开仓门槛"
                gross, symbol_gross = Decimal("0"), Decimal("0")
                for key, size in actual.items():
                    if key[0] != exchange:
                        continue
                    # Use the larger fresh side quote so spread direction cannot understate gross exposure.
                    live = max(positive(self.executor._get_live_market_price(exchange, leg_symbols[key], True)),
                               positive(self.executor._get_live_market_price(exchange, leg_symbols[key], False)))
                    value = size * live
                    gross += value
                    if key[1] == normalize(leg_symbol):
                        symbol_gross += value
                added = quantity * positive(price)
                if symbol_gross + added > cap:
                    return False, f"{exchange} 本币开仓后名义价值超过 {cap} USDC"
                limits = []
                if guard._cfg.max_exchange_position_value.enabled:
                    limits.append(positive(guard._max_value_threshold(exchange)))
                if guard._cfg.dynamic_max_exchange_position_value.enabled:
                    limits.append(positive(equity) * positive(guard._dynamic_multiplier(exchange)))
                if limits and gross + added > min(limits):
                    return False, f"{exchange} 开仓后总名义价值超过 {min(limits)} USDC"
            request.price_buy, request.price_sell, request.spread_data = buy, sell, view
            self.executor._validate_pair_quantity(request)
            return True, ""
        except Exception as exc:
            return False, f"账户状态不可用: {exc}"

    async def _validate_segmented_close(self, request):
        position = self.decision_engine.get_position(request.symbol)
        if not position or request.quantity <= 0 or request.quantity > position.total_quantity:
            return False, "平仓数量超过已确认持仓"
        if request.exchange_buy != position.exchange_sell or request.exchange_sell != position.exchange_buy:
            return False, "平仓交易所方向不匹配"
        try:
            snapshots = []
            for exchange, is_buy, leg_symbol in ((request.exchange_buy, True, request.buy_symbol), (request.exchange_sell, False, request.sell_symbol)):
                adapter = self.exchange_adapters[exchange]
                rows = await self.risk_controller._validated_positions(exchange, adapter)
                matching = [p for p in rows if self._normalize_symbol_for_comparison(str(p.symbol)) == self._normalize_symbol_for_comparison(leg_symbol or request.symbol)]
                expected_side = "short" if is_buy else "long"
                available = sum((Decimal(str(p.size)) for p in matching if str(getattr(p.side, "value", p.side)).lower() == expected_side), Decimal("0"))
                if available < request.quantity or getattr(adapter, "_uncertain", None):
                    return False, "实仓不足或订单状态不确定"
                snapshots.append(rows)
            current = self.decision_engine.get_position(request.symbol)
            if current is not position or request.quantity > current.total_quantity or current.exchange_sell != request.exchange_buy or current.exchange_buy != request.exchange_sell:
                return False, "账户读取期间分段持仓发生变化"
            for rows in snapshots:
                self.risk_controller._validate_positions_snapshot(rows)
            for exchange, is_buy, leg_symbol in ((request.exchange_buy, True, request.buy_symbol), (request.exchange_sell, False, request.sell_symbol)):
                live_price = self.executor._get_live_market_price(exchange, leg_symbol or request.symbol, is_buy)
                if live_price is None or not live_price.is_finite() or live_price <= 0:
                    return False, "可成交行情过期或缺失"
                if is_buy:
                    request.price_buy = live_price
                else:
                    request.price_sell = live_price
            if not request.allow_when_global_paused:
                from dataclasses import replace
                exit_view = replace(request.spread_data, price_buy=request.price_buy, price_sell=request.price_sell)
                profit = self._estimate_fifo_close_profit(position, request.quantity, exit_view)
                if profit is None or profit < 0:
                    return False, "正常平仓净收益不足或入场成本缺失"
            return True, ""
        except Exception as exc:
            return False, f"实仓/行情核验失败: {exc}"
