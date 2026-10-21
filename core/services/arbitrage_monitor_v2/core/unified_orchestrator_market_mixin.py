"""
⚠️ 本文件由 `unified_orchestrator.py` 拆分生成（只做文件拆分，不改功能逻辑）。

职责：行情/价差构造与补单前置校验（实时盘口取价、从请求构造价差、重试前门槛复用）。

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


class UnifiedOrchestratorMarketMixin:
    """由 unified_orchestrator.py 拆分出的 mixin。"""

    def _resolve_live_price_from_cache(
            self,
            exchange: str,
            symbol: str,
            is_buy: bool,
        ) -> Optional[Decimal]:
            """
            提供给执行器的实时盘口价格解析器，优先读取本地WS缓存。
            """
            if not getattr(self, "data_processor", None):
                return None

            exchange_candidates = [exchange, exchange.upper(), exchange.lower()]
            symbol_candidates = self._build_symbol_aliases(symbol)

            for exchange_key in dict.fromkeys(exchange_candidates):
                if not exchange_key:
                    continue
                for symbol_key in symbol_candidates:
                    orderbook = self.data_processor.get_orderbook(
                        exchange_key,
                        symbol_key,
                        max_age_seconds=self.get_data_freshness_seconds(exchange_key),
                    )
                    if not orderbook:
                        continue
                    side = getattr(orderbook, "best_ask", None) if is_buy else getattr(orderbook, "best_bid", None)
                    price = getattr(side, "price", None) if side else None
                    if price in (None, 0):
                        continue
                    try:
                        return Decimal(str(price))
                    except (InvalidOperation, ValueError, TypeError):
                        continue
            return None

    def _build_live_spread_from_request(
            self,
            request: "ExecutionRequest",
        ) -> Optional[SpreadData]:
            if not request:
                return None
            exchange_buy = request.exchange_buy
            exchange_sell = request.exchange_sell
            buy_symbol = request.buy_symbol or request.symbol
            sell_symbol = request.sell_symbol or request.symbol
            if not exchange_buy or not exchange_sell or not buy_symbol or not sell_symbol:
                return None
            buy_ob = self.data_processor.get_orderbook(
                exchange_buy,
                buy_symbol,
                max_age_seconds=self.get_data_freshness_seconds(exchange_buy),
            )
            sell_ob = self.data_processor.get_orderbook(
                exchange_sell,
                sell_symbol,
                max_age_seconds=self.get_data_freshness_seconds(exchange_sell),
            )
            if not buy_ob or not sell_ob:
                return None
            best_ask = getattr(buy_ob, "best_ask", None)
            best_bid = getattr(sell_ob, "best_bid", None)
            if not best_ask or not best_bid:
                return None
            price_buy = self.executor._to_decimal_value(getattr(best_ask, "price", None))
            price_sell = self.executor._to_decimal_value(getattr(best_bid, "price", None))
            if price_buy <= Decimal("0") or price_sell <= Decimal("0"):
                return None
            size_buy = self.executor._to_decimal_value(getattr(best_ask, "size", None))
            size_sell = self.executor._to_decimal_value(getattr(best_bid, "size", None))
            spread_abs = price_sell - price_buy
            spread_pct = (
                float((spread_abs / price_buy) * Decimal("100"))
                if price_buy > Decimal("0")
                else 0.0
            )
            return SpreadData(
                symbol=request.symbol,
                exchange_buy=exchange_buy,
                exchange_sell=exchange_sell,
                price_buy=price_buy,
                price_sell=price_sell,
                size_buy=size_buy,
                size_sell=size_sell,
                spread_abs=spread_abs,
                spread_pct=spread_pct,
                buy_symbol=buy_symbol,
                sell_symbol=sell_symbol,
            )

    async def _validate_retry_preconditions(
            self,
            request: "ExecutionRequest",
        ) -> bool:
            """
            供执行器重试前调用，复用三道门槛（价差→稳定性→流动性）。
            """
            spread = self._build_live_spread_from_request(request)
            if not spread:
                logger.warning(
                    "⛔️ [重试门槛] %s: 无最新盘口数据，放弃重新挂单",
                    request.symbol,
                )
                return False

            symbol = (request.symbol or "").upper()
            action = "开仓" if request.is_open else "平仓"
            funding_rate = None
            try:
                funding_rate = self._get_funding_rate_data(
                    symbol,
                    spread.exchange_buy,
                    spread.exchange_sell,
                )
            except Exception:
                funding_rate = None

            if request.is_open:
                should_open, open_quantity = await self.decision_engine.should_open(
                    symbol,
                    spread,
                    funding_rate,
                )
                if not should_open or open_quantity <= Decimal("0"):
                    # 🔥 检查是否检测到反向开仓（实为平仓信号）
                    if getattr(self.decision_engine, '_reverse_open_detected', False):
                        self.decision_engine._reverse_open_detected = False  # 重置标记
                        logger.info(f"🔄 [重试门槛] {symbol}: 反向开仓被拦截，但这是平仓信号，允许继续")
                        # 不终止补单，让平仓逻辑处理
                        return True
                    logger.info(
                        "⛔️ [重试门槛] %s: 价差已不满足开仓条件，终止补单",
                        symbol,
                    )
                    return False
            else:
                should_close, close_quantity, reason, _ = await self.decision_engine.should_close(
                    symbol,
                    spread,
                    funding_rate,
                )
                if not should_close or close_quantity <= Decimal("0"):
                    logger.info(
                        "⛔️ [重试门槛] %s: 平仓条件未满足（%s），终止补单",
                        symbol,
                        reason or "无原因",
                    )
                    return False

            if not self._passes_price_stability(symbol, spread, action=action):
                logger.info(
                    "⛔️ [重试门槛] %s: %s价格稳定性未通过，终止补单",
                    symbol,
                    action,
                )
                return False

            if self._should_enforce_orderbook_liquidity(symbol):
                min_ob_qty = self._get_min_orderbook_quantity(symbol)
                target_quantity = self.executor._to_decimal_value(
                    request.quantity or Decimal("0")
                )
                if request.is_open:
                    if target_quantity <= Decimal("0"):
                        target_quantity = self.executor._to_decimal_value(open_quantity)
                else:
                    target_quantity = max(
                        target_quantity,
                        self.executor._to_decimal_value(close_quantity),
                    )
                legs = [
                    {
                        "exchange": spread.exchange_buy,
                        "symbol": spread.buy_symbol or symbol,
                        "quantity": target_quantity,
                        "is_buy": True,
                        "desc": "开仓买入腿" if request.is_open else "平仓买回腿",
                        "min_quantity": min_ob_qty,
                    },
                    {
                        "exchange": spread.exchange_sell,
                        "symbol": spread.sell_symbol or symbol,
                        "quantity": target_quantity,
                        "is_buy": False,
                        "desc": "开仓卖出腿" if request.is_open else "平仓卖出腿",
                        "min_quantity": min_ob_qty,
                    },
                ]
                liquidity_ok, failure_detail = self._verify_orderbook_liquidity(
                    symbol,
                    legs,
                    action=action,
                )
                if not liquidity_ok:
                    logger.warning(
                        "⛔️ [重试门槛] %s: %s 对手盘不足，终止补单",
                        symbol,
                        failure_detail,
                    )
                    return False

            return True
