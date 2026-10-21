"""
⚠️ 本文件由 `unified_orchestrator.py` 拆分生成（只做文件拆分，不改功能逻辑）。

职责：通用门槛/风控工具与节流日志（流动性/稳定性/local-spread/双限价避让/持久性确认等）。

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


class UnifiedOrchestratorGatesMixin:
    """由 unified_orchestrator.py 拆分出的 mixin。"""

    def _should_enforce_orderbook_liquidity(self, symbol: str) -> bool:
        """
        判断是否需要对该symbol启用对手盘深度校验
        """
        try:
            config = self.config_manager.get_config(symbol)
            return bool(
                getattr(config.grid_config,
                        'require_orderbook_liquidity', False)
            )
        except Exception:
            return False

    def _get_min_orderbook_quantity(self, symbol: str) -> Optional[Decimal]:
        """
        获取配置的盘口最小可用数量门槛
        """
        try:
            config = self.config_manager.get_config(symbol)
            min_qty = getattr(config.grid_config,
                              'min_orderbook_quantity', None)
            if min_qty is None:
                return None
            return Decimal(str(min_qty))
        except Exception:
            return None

    def _get_orderbook_aggregate_levels(self, symbol: str) -> int:
        """
        获取订单簿聚合档位数（Top-N）。

        说明：
        - N=1：等价于只用买1/卖1（best_bid/best_ask）
        - N>1：可用于 L2 订单簿交易所的 VWAP 价格与数量聚合
        - 不支持多档/无数量的交易所会在下游自动降级
        """
        try:
            config = self.config_manager.get_config(symbol)
            raw = getattr(config.grid_config, "orderbook_aggregate_levels", 1)
            n = int(raw) if raw is not None else 1
            return max(1, n)
        except Exception:
            return 1

    def _get_limit_price_offset_for_symbol(self, symbol: Optional[str]) -> Optional[Decimal]:
        """
        获取指定交易对的限价偏移配置（绝对价格增量）
        """
        if not symbol:
            return None
        try:
            config = self.config_manager.get_config(symbol)
        except Exception:
            return None
        raw_offset = getattr(config.grid_config, 'limit_price_offset', None)
        if raw_offset in (None, 0):
            return None
        try:
            offset_value = Decimal(str(raw_offset))
        except Exception:
            logger.warning(
                "⚠️ [配置] %s limit_price_offset=%s 无法解析为Decimal，忽略该配置",
                symbol,
                raw_offset
            )
            return None
        if offset_value <= Decimal('0'):
            return None
        return offset_value

    def _resolve_grid_threshold_pct(
        self,
        symbol: Optional[str],
        grid_level: Optional[int]
    ) -> Optional[Decimal]:
        """
        根据配置计算指定网格级别的开仓阈值（百分比）。
        """
        if not symbol or not grid_level or grid_level <= 0:
            return None
        try:
            config = self.config_manager.get_config(symbol)
        except Exception:
            return None
        grid_cfg = getattr(config, "grid_config", None)
        if not grid_cfg:
            return None
        initial = getattr(grid_cfg, "initial_spread_threshold", None)
        step = getattr(grid_cfg, "grid_step", None)
        if initial is None or step is None:
            return None
        try:
            initial_dec = Decimal(str(initial))
            step_dec = Decimal(str(step))
        except (InvalidOperation, TypeError, ValueError):
            return None
        if initial_dec <= Decimal("0"):
            return None
        if step_dec < Decimal("0"):
            return None
        threshold = initial_dec + step_dec * Decimal(grid_level - 1)
        if threshold <= Decimal("0"):
            return None
        # 量化到4位小数，方便显示
        try:
            return threshold.quantize(Decimal("0.0001"))
        except InvalidOperation:
            return threshold

    def _resolve_grid_close_threshold_pct(
        self,
        *,
        symbol: Optional[str],
        actual_position: Optional[Decimal],
    ) -> Optional[Decimal]:
        """
        根据当前实际持仓，计算“平仓持续性阈值”（百分比，供日志展示）。

        说明（以代码为准）：
        - 平仓门槛来自决策引擎的 `_get_close_persistence_threshold(actual_position, config)`
        - 对称网格下：T5 平仓应显示 T4；T1 平仓应显示 T0（T1*0.4）
        """
        if not symbol:
            return None
        if actual_position is None:
            return None
        try:
            cfg = self.config_manager.get_config(symbol)
        except Exception:
            return None
        try:
            th = float(self.decision_engine._get_close_persistence_threshold(actual_position, cfg))  # type: ignore[attr-defined]
        except Exception:
            return None
        if th <= 0:
            return None
        try:
            return Decimal(str(th)).quantize(Decimal("0.0001"))
        except Exception:
            try:
                return Decimal(str(th))
            except Exception:
                return None

    def _resolve_slippage_pct(
        self,
        symbol: str,
        symbol_config
    ) -> Optional[Decimal]:
        """
        统一解析滑点配置：优先使用套利对自定义值，缺失时回退到执行层默认值。
        """
        raw_value: Optional[Any] = None
        if symbol_config and symbol_config.grid_config.slippage_tolerance is not None:
            raw_value = symbol_config.grid_config.slippage_tolerance
        else:
            raw_value = getattr(
                self.executor.config.order_execution,
                "max_slippage",
                None,
            )
        if raw_value is None:
            return None
        try:
            return Decimal(str(raw_value))
        except (InvalidOperation, TypeError, ValueError):
            logger.warning(
                "⚠️ [配置] %s 的滑点参数无法解析，将忽略该值: %s",
                symbol,
                raw_value,
            )
            return None

    def _record_price_sample(self, symbol: str, spread_data: SpreadData) -> None:
        self.risk_utils.record_price_sample(symbol, spread_data)

    def _reset_price_history(self, symbol: str, spread_data: SpreadData) -> None:
        self.risk_utils.reset_price_history(symbol, spread_data)

    def _passes_price_stability(
        self,
        symbol: str,
        spread_data: SpreadData,
        *,
        action: str
    ) -> bool:
        return self.risk_utils.passes_price_stability(
            symbol,
            spread_data,
            action=action,
        )

    def _passes_local_orderbook_spread(
        self,
        *,
        symbol: str,
        spread_data: SpreadData,
        threshold_pct: Decimal,
    ) -> bool:
        """
        第四道门槛：校验每条实际交易腿的自身 bid-ask 点差是否低于阈值。
        """
        legs = [
            ("买入腿", spread_data.exchange_buy, spread_data.buy_symbol or symbol),
            ("卖出腿", spread_data.exchange_sell, spread_data.sell_symbol or symbol),
        ]
        checked: Set[Tuple[str, str]] = set()
        epsilon = Decimal("0.00000001")

        for desc, exchange, leg_symbol in legs:
            if not exchange or not leg_symbol:
                continue
            key = (exchange.lower(), leg_symbol.upper())
            if key in checked:
                continue
            checked.add(key)

            spread_pct = self._calculate_local_orderbook_spread_pct(
                exchange=exchange,
                symbol=leg_symbol,
            )
            if spread_pct is None:
                self._log_with_throttle(
                    key=f"local_spread_missing:{symbol}:{exchange}:{leg_symbol}",
                    message=(
                        f"⏸️ [V2开仓] {symbol}: {desc} {exchange}/{leg_symbol} "
                        "缺少盘口数据或价差异常，无法计算bid-ask点差，跳过本次信号。"
                    ),
                    level="warning",
                    throttle_seconds=5.0,
                )
                return False

            if (spread_pct - threshold_pct) > epsilon:
                self._log_with_throttle(
                    key=f"local_spread_block:{symbol}:{exchange}:{leg_symbol}",
                    message=(
                        f"⏸️ [V2开仓] {symbol}: {desc} {exchange}/{leg_symbol} "
                        f"自有点差 {float(spread_pct):.4f}% 高于阈值 {float(threshold_pct):.4f}%，"
                        "跳过本次机会。"
                    ),
                    # 该模块 logger 默认 WARNING；用 INFO 会导致“有拦截但看不到原因”
                    level="warning",
                    throttle_seconds=10.0,
                )
                return False

        return True

    def _calculate_local_orderbook_spread_pct(
        self,
        *,
        exchange: str,
        symbol: str,
    ) -> Optional[Decimal]:
        """
        计算指定交易对在某交易所的 bid-ask 点差百分比。
        """
        orderbook = self.data_processor.get_orderbook(
            exchange,
            symbol,
            max_age_seconds=self.get_data_freshness_seconds(exchange),
        )
        if not orderbook or not orderbook.best_ask or not orderbook.best_bid:
            return None

        try:
            ask_price = Decimal(str(orderbook.best_ask.price))
            bid_price = Decimal(str(orderbook.best_bid.price))
        except (InvalidOperation, TypeError, ValueError):
            return None

        if ask_price <= Decimal("0") or bid_price <= Decimal("0"):
            return None

        spread_abs = ask_price - bid_price
        if spread_abs <= Decimal("0"):
            return Decimal("0")

        try:
            return (spread_abs / ask_price) * Decimal("100")
        except (InvalidOperation, ZeroDivisionError):
            return None

    def _is_symbol_market_open(self, symbol: str, base_symbol: Optional[str] = None) -> bool:
        """
        判断符号是否处于可交易时间。
        当前仅对包含 XAU 的国际黄金符号进行周末休市限制：
        - 北京时间周六 06:00 起休市
        - 北京时间周一 07:00 前不开仓
        - 周日全日不可开仓
        """
        check_symbol = base_symbol or symbol
        upper_symbol = (check_symbol or "").upper()
        if "XAU" not in upper_symbol:
            return True

        now = datetime.now(ZoneInfo("Asia/Shanghai"))
        weekday = now.weekday()  # Monday=0 ... Sunday=6
        current_time = now.time()
        sat_cutoff = dt_time(hour=5, minute=59)
        mon_cutoff = dt_time(hour=7, minute=0, second=5)

        # 北京时间周六 06:00 之后到周一 07:00 前休市
        if weekday == 5 and current_time >= sat_cutoff:
            return False
        if weekday == 6:
            return False
        if weekday == 0 and current_time < mon_cutoff:
            return False
        return True

    def _log_with_throttle(
        self,
        key: str,
        message: str,
        *,
        signature: Optional[str] = None,
        level: str = "info",
        throttle_seconds: float = 0.5
    ) -> None:
        self._throttled_logger.log(
            key=key,
            message=message,
            signature=signature,
            level=level,
            throttle_seconds=throttle_seconds,
        )

    def _should_log_persistence_confirmation(
        self,
        symbol: str,
        action: str,
        spread_pct: float,
        *,
        time_window: float = 2.0,
        spread_epsilon: float = 0.003,
    ) -> bool:
        key = f"{symbol}:{action}"
        now = time.time()
        last_entry = self._persistence_log_records.get(key)
        if last_entry:
            last_spread, last_time = last_entry
            if (now - last_time) < time_window and abs(last_spread - spread_pct) < spread_epsilon:
                return False
        self._persistence_log_records[key] = (spread_pct, now)
        return True

    def _should_log_open_intent(
        self,
        symbol: str,
        spread_pct: float,
        *,
        time_window: float = 1.5,
        spread_epsilon: float = 0.002,
    ) -> bool:
        now = time.time()
        last_entry = self._open_intent_log_records.get(symbol)
        if last_entry:
            last_spread, last_time = last_entry
            if abs(last_spread - spread_pct) < spread_epsilon and (now - last_time) < time_window:
                return False
        self._open_intent_log_records[symbol] = (spread_pct, now)
        return True

    def _should_skip_due_to_dual_limit_backoff(self, symbol: str) -> bool:
        return self.risk_utils.should_skip_due_to_dual_limit_backoff(symbol)

    def _schedule_dual_limit_backoff(self, symbol: str) -> None:
        self.risk_utils.schedule_dual_limit_backoff(symbol)

    def _clear_dual_limit_backoff(self, symbol: str) -> None:
        self.risk_utils.clear_dual_limit_backoff(symbol)

    def _log_liquidity_failure_summary(
        self,
        symbol: str,
        *,
        reason: str,
        failure_detail: Optional[Dict[str, Any]],
        base_message: str,
        throttle_seconds: float = 1.0
    ) -> None:
        self._liquidity_failure_logger.log(
            symbol,
            reason=reason,
            failure_detail=failure_detail,
            base_message=base_message,
            throttle_seconds=throttle_seconds,
        )

    def _clear_liquidity_failure_summary(self, reason: str, symbol: str) -> None:
        self._liquidity_failure_logger.clear(reason, symbol)

    def _verify_orderbook_liquidity(
        self,
        symbol: str,
        legs: List[Dict[str, Any]],
        action: str = "开仓"
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        return self.risk_utils.verify_orderbook_liquidity(symbol, legs, action=action)

    def _log_persistence_confirmation(
        self,
        symbol: str,
        action: str,
        spread_pct: float
    ):
        """在实际下单前输出持续性检查结果，帮助验证严格/宽松模式"""
        try:
            config = self.config_manager.get_config(symbol)
        except Exception:
            return

        seconds = config.grid_config.spread_persistence_seconds
        if seconds <= 1:
            return

        mode = "严格" if config.grid_config.strict_persistence_check else "宽松"
        if not self._should_log_persistence_confirmation(symbol, action, spread_pct):
            return
        logger.info(
            "🛡️ [%s] %s持续性确认(%s/%ds) - 当前价差 %.4f%%",
            symbol,
            action,
            mode,
            seconds,
            spread_pct
        )

    def _select_reverse_spread(
        self,
        reference: SpreadData,
        candidates: List[SpreadData]
    ) -> SpreadData:
        """在候选列表中寻找 reference 的反向价差，若不存在则构造一份。"""
        for candidate in candidates:
            if self._is_reverse_spread(candidate, reference):
                return candidate

        # 🔥 未找到反向价差，使用兜底逻辑（字段交换）
        logger.warning(
            f"⚠️ [价差计算] {reference.symbol}: 未找到反向价差，"
            f"使用字段交换兜底逻辑（可能导致平仓数据不准确）"
        )
        return self._build_reverse_spread(reference)

    @staticmethod
    def _is_reverse_spread(candidate: SpreadData, reference: SpreadData) -> bool:
        """判断 candidate 是否为 reference 的反向价差。"""
        def _norm(value: Optional[str]) -> str:
            return (value or "").lower()

        return (
            _norm(candidate.exchange_buy) == _norm(reference.exchange_sell)
            and _norm(candidate.exchange_sell) == _norm(reference.exchange_buy)
            and _norm(candidate.buy_symbol) == _norm(reference.sell_symbol)
            and _norm(candidate.sell_symbol) == _norm(reference.buy_symbol)
        )

    @staticmethod
    def _build_reverse_spread(spread: SpreadData) -> SpreadData:
        """基于给定价差构造其反向视角的 SpreadData。"""
        return SpreadData(
            symbol=spread.symbol,
            exchange_buy=spread.exchange_sell,
            exchange_sell=spread.exchange_buy,
            price_buy=spread.price_sell,
            price_sell=spread.price_buy,
            size_buy=spread.size_sell,
            size_sell=spread.size_buy,
            spread_abs=-spread.spread_abs,
            spread_pct=-spread.spread_pct,
            buy_symbol=spread.sell_symbol,
            sell_symbol=spread.buy_symbol
        )
