"""
⚠️ 本文件由 `unified_orchestrator.py` 拆分生成（只做文件拆分，不改功能逻辑）。

职责：平仓决策与执行链路（平仓门槛、方向修正、构造请求、异步执行与记录）。

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
from dataclasses import asdict, replace
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


class UnifiedOrchestratorCloseMixin:
    """由 unified_orchestrator.py 拆分出的 mixin。"""

    async def _check_and_close(
        self,
        symbol: str,
        spread_data: SpreadData,
        funding_rate_data: Optional[FundingRateData],
        *,
        config_symbol: Optional[str] = None
    ):
        """检查并执行平仓（V2总量驱动算法）"""
        try:
            position = self.decision_engine.get_position(symbol)
            reasons = self._position_risk_close_reasons
            if not position or position.total_quantity <= 0:
                if symbol not in self._position_risk_close_failed:
                    reasons.pop(symbol, None)
            elif not self.monitor_only_mode:
                config = self.config_manager.get_config(symbol)
                reason = reasons.get(symbol) or self._position_loss_reason(position, spread_data, config)
                if reason:
                    await self._close_position_for_risk(symbol, reason)
                    return

            # 监控模式=模拟盘：禁用账户/交易所状态类风控拦截（避免阻断模拟下单闭环）
            if not bool(getattr(self, "monitor_only_mode", True)):
                # 执行失败暂停（仅该风控会同时禁止开仓/平仓）
                if getattr(self, "risk_controller", None):
                    # type: ignore[attr-defined]
                    blocked, pause_reason = self.risk_controller.should_block_execution_pause()
                    if blocked:
                        self._log_with_throttle(
                            key=f"global_pause_close:{symbol}",
                            signature="global_pause_close",
                            message=f"⏸️ [风控] {symbol}: 执行失败暂停中，已禁止平仓。原因={pause_reason or 'unknown'}",
                            level="warning",
                            throttle_seconds=60.0,
                        )
                        return

                if self.reduce_only_guard.is_pair_closing_blocked(symbol):
                    self._log_with_throttle(
                        key=f"reduce_only_close_blocked:{symbol}",
                        message=f"⏸️ [V2平仓] {symbol}: 交易所仍处于 reduce-only 限制，等待恢复后再尝试平仓。",
                        throttle_seconds=60.0,
                    )
                    return
                if self.reduce_only_guard.is_pair_blocked(symbol):
                    self._log_with_throttle(
                        key=f"reduce_only_close:{symbol}",
                        message=f"⏸️ [V2平仓] {symbol}: 交易所在 reduce-only 模式，暂停开平仓，等待整点探针恢复。",
                        throttle_seconds=60.0,
                    )
                    return
            symbol_config = self.config_manager.get_config(symbol)
            slippage_pct = self._resolve_slippage_pct(symbol, symbol_config)

            # ===== EMA价差门槛：持续喂样本（平仓用 should_close 同口径 relative_spread）=====
            ema_close_obs: Optional[EmaUpdateResult] = None
            relative_spread_for_ema: Optional[float] = None
            try:
                gc = symbol_config.grid_config
                if bool(getattr(gc, "ema_spread_gate_enabled", False)):
                    # 一次性确认：该交易对启用了 EMA 门槛（用于用户验证功能已启用）
                    # 🔥 使用标志位确保每个交易对只打印一次，避免重复打印
                    log_key = (symbol, "close")
                    if not hasattr(self, "_ema_gate_enabled_logged"):
                        self._ema_gate_enabled_logged = set()
                    if log_key not in self._ema_gate_enabled_logged:
                        logger.warning(
                            f"🧮 [EMA门槛] {symbol}: 已启用(平仓) "
                            f"window={float(getattr(gc, 'ema_spread_window_seconds', 30.0) or 30.0)}s "
                            f"Δ={float(getattr(gc, 'ema_spread_deviation_pct', 0.0) or 0.0):.4f}% "
                            f"strict={bool(getattr(gc, 'ema_spread_strict_samples_per_second', False))}"
                        )
                        self._ema_gate_enabled_logged.add(log_key)
                    pos_for_dir = self.decision_engine.get_position(symbol)
                    if pos_for_dir:
                        pk = self.decision_engine._build_position_key(  # type: ignore[attr-defined]
                            symbol,
                            pos_for_dir.exchange_buy,
                            pos_for_dir.exchange_sell,
                            pos_for_dir.buy_symbol or symbol,
                            pos_for_dir.sell_symbol or symbol,
                        )
                        direction = self.decision_engine.open_direction.get(
                            pk, 1)
                    else:
                        direction = 1
                    closing_spread_pct = float(spread_data.spread_pct)
                    relative_spread_for_ema = -closing_spread_pct * \
                        (1 if direction >= 0 else -1)
                    ema_close_obs = self._observe_spread_ema(
                        symbol=symbol,
                        action="close",
                        value_pct=float(relative_spread_for_ema),
                        window_seconds=float(
                            getattr(gc, "ema_spread_window_seconds", 30.0) or 30.0),
                        strict=bool(
                            getattr(gc, "ema_spread_strict_samples_per_second", False)),
                    )
            except Exception:
                ema_close_obs = None
                relative_spread_for_ema = None
            # 🔥 V2接口：返回(是否平仓, 平仓数量, 平仓原因, _)
            # segment_id现在总是返回None，因为我们使用总量驱动，不关心具体段
            should_close, close_quantity, reason, _ = await self.decision_engine.should_close(
                symbol,
                spread_data,
                funding_rate_data
            )

            if not should_close or close_quantity <= Decimal('0'):
                return

            try:
                anchor_ts = self._analysis_anchor_ts.get(symbol)
                if anchor_ts:
                    self.decision_signal_latency_ms = (time.time() - anchor_ts) * 1000.0
                    self.decision_signal_latency_label = "close"
            except Exception:
                pass

            # ===== 新增门槛：EMA价差门槛（平仓；严格模式断秒会拦截平仓）=====
            if relative_spread_for_ema is None:
                # 兜底：尽量对齐 should_close 口径；无法获取方向则取绝对值
                try:
                    relative_spread_for_ema = abs(
                        float(spread_data.spread_pct))
                except Exception:
                    relative_spread_for_ema = 0.0
            ema_ok, ema_reason = self._passes_ema_spread_gate_close(
                symbol=symbol,
                relative_spread_pct=float(relative_spread_for_ema),
                observed=ema_close_obs,
            )
            if not ema_ok:
                self._log_with_throttle(
                    key=f"ema_spread_gate:close:{symbol}",
                    signature="ema_spread_gate_close",
                    message=f"⛔️ [EMA门槛] {symbol}: {ema_reason}，已拦截平仓。",
                    level="warning",
                    throttle_seconds=60.0,
                )
                return
            # 低频放行日志（帮助确认功能在运行且在放行）
            try:
                gc = symbol_config.grid_config
                if bool(getattr(gc, "ema_spread_gate_enabled", False)) and ema_close_obs is not None and ema_close_obs.ema is not None:
                    delta = float(
                        getattr(gc, "ema_spread_deviation_pct", 0.0) or 0.0)
                    pos = self.decision_engine.get_position(symbol)
                    actual_qty = pos.total_quantity if pos else Decimal("0")
                    try:
                        close_th = float(self.decision_engine._get_close_persistence_threshold(
                            # type: ignore[attr-defined]
                            actual_qty, symbol_config))
                    except Exception:
                        close_th = float(
                            getattr(gc, "initial_spread_threshold", 0.0) or 0.0) * 0.4
                    max_ema = close_th + delta
                    self._log_with_throttle(
                        key=f"ema_spread_gate:close_pass:{symbol}",
                        signature="ema_spread_gate_close_pass",
                        message=(
                            f"✅ [EMA门槛] {symbol}: 平仓放行 "
                            f"ema={float(ema_close_obs.ema):.4f}% ≤ {max_ema:.4f}% "
                            f"(Tclose={close_th:.4f}%, Δ={delta:.4f}%)"
                        ),
                        level="warning",
                        throttle_seconds=300.0,
                    )
            except Exception:
                pass

            if not self._passes_price_stability(symbol, spread_data, action="平仓"):
                return

            local_spread_threshold = getattr(
                symbol_config.grid_config,
                "max_local_orderbook_spread_pct",
                None,
            )
            local_spread_threshold_dec: Optional[Decimal] = None
            if local_spread_threshold is not None:
                try:
                    local_spread_threshold_dec = Decimal(
                        str(local_spread_threshold))
                except (InvalidOperation, TypeError, ValueError):
                    logger.warning(
                        "⚠️ [配置] %s: max_local_orderbook_spread_pct=%s 无法解析，已忽略平仓第四门槛",
                        symbol,
                        local_spread_threshold,
                    )
                    local_spread_threshold_dec = None
            if (
                local_spread_threshold_dec is not None
                and local_spread_threshold_dec > Decimal("0")
            ):
                if not self._passes_local_orderbook_spread(
                    symbol=symbol,
                    spread_data=spread_data,
                    threshold_pct=local_spread_threshold_dec,
                ):
                    return

            # 🔥 获取持仓方向（用于正确计算平仓价差）
            position = self.decision_engine.get_position(symbol)
            if not position:
                logger.warning(f"⚠️  [V2平仓] {symbol}: 无法获取持仓信息，取消平仓")
                return

            # 平仓时需要反向交易：开仓时买入的交易所，平仓时卖出
            position_exchange_buy = position.exchange_buy    # 开仓时的买入方
            position_exchange_sell = position.exchange_sell  # 开仓时的卖出方
            buy_back_symbol = position.sell_symbol or symbol
            sell_leg_symbol = position.buy_symbol or symbol
            config_limit_offset = None
            if config_symbol:
                config_limit_offset = self._get_limit_price_offset_for_symbol(
                    config_symbol)
            buy_limit_offset = config_limit_offset or self._get_limit_price_offset_for_symbol(
                buy_back_symbol)
            sell_limit_offset = config_limit_offset or self._get_limit_price_offset_for_symbol(
                sell_leg_symbol)

            position_segments = self.decision_engine.get_current_segments(
                symbol)

            # ------------------------------------------------------------------
            # 🔥 多档盘口聚合（Top-N）：用前 N 档计算 VWAP 买入价/卖出价（平仓视角）
            # 目的：买1/卖1数量不足时，用买1..买N / 卖1..卖N 合并后的均价评估是否仍满足平仓门槛
            # 注意：按“每条腿”独立启用：
            # - 支持多档/有数量/能覆盖下单量 -> 该腿用 VWAP
            # - 不支持（例如 Variational/BBO/无数量）-> 该腿保持原样（best_bid/best_ask）
            # ------------------------------------------------------------------
            price_mode = "BBO"
            try:
                agg_n = self._get_orderbook_aggregate_levels(config_symbol or symbol)
                if agg_n > 1 and close_quantity and close_quantity > Decimal("0"):
                    from ..utils.orderbook_aggregation import aggregate_top_n_vwap

                    buy_ob = self.data_processor.get_orderbook(
                        spread_data.exchange_buy,
                        spread_data.buy_symbol or symbol,
                        max_age_seconds=self.get_data_freshness_seconds(spread_data.exchange_buy),
                    )
                    sell_ob = self.data_processor.get_orderbook(
                        spread_data.exchange_sell,
                        spread_data.sell_symbol or symbol,
                        max_age_seconds=self.get_data_freshness_seconds(spread_data.exchange_sell),
                    )
                    if buy_ob and sell_ob:
                        buy_agg = aggregate_top_n_vwap(
                            buy_ob, is_buy=True, max_levels=agg_n, target_qty=close_quantity
                        )
                        sell_agg = aggregate_top_n_vwap(
                            sell_ob, is_buy=False, max_levels=agg_n, target_qty=close_quantity
                        )
                        buy_mode = "BBO"
                        sell_mode = "BBO"

                        if buy_agg.vwap_price is not None and buy_agg.fully_covered:
                            spread_data.price_buy = buy_agg.vwap_price
                            if buy_agg.total_available is not None:
                                spread_data.size_buy = buy_agg.total_available
                            buy_mode = f"VWAP{agg_n}"

                        if sell_agg.vwap_price is not None and sell_agg.fully_covered:
                            spread_data.price_sell = sell_agg.vwap_price
                            if sell_agg.total_available is not None:
                                spread_data.size_sell = sell_agg.total_available
                            sell_mode = f"VWAP{agg_n}"

                        if spread_data.price_buy and spread_data.price_buy > 0 and spread_data.price_sell:
                            spread_data.spread_abs = spread_data.price_sell - spread_data.price_buy
                            spread_data.spread_pct = float(
                                ((spread_data.price_sell - spread_data.price_buy) / spread_data.price_buy) * 100
                            )

                        price_mode = f"buy={buy_mode} sell={sell_mode}"
            except Exception:
                price_mode = "BBO"

            # 🔥 平仓意图日志 - 单行格式,包含关键信息
            # 平仓视角：使用当前订单簿价格（spread_data 已经是从当前订单簿计算的）
            # spread_data 已经是平仓视角，直接使用其 exchange 和 price
            closing_view = (
                f"平仓视角: 买{spread_data.exchange_buy}@{spread_data.price_buy:.2f} "
                f"卖{spread_data.exchange_sell}@{spread_data.price_sell:.2f} "
                f"(价差={spread_data.spread_pct:.4f}%) 价格模式={price_mode}"
            )

            # 🔥 开仓视角：从持仓记录中计算加权平均开仓价格
            # 因为可能分多次开仓，需要计算加权平均
            open_segments = position.get_open_segments()
            if open_segments:
                total_qty = sum(float(seg.open_quantity)
                                for seg in open_segments)
                if total_qty > 0:
                    # 加权平均买入价格
                    opening_buy_price = Decimal(str(sum(
                        float(seg.open_price_buy) * float(seg.open_quantity)
                        for seg in open_segments
                    ) / total_qty))

                    # 加权平均卖出价格
                    opening_sell_price = Decimal(str(sum(
                        float(seg.open_price_sell) * float(seg.open_quantity)
                        for seg in open_segments
                    ) / total_qty))
                else:
                    # 兜底：使用当前价格的反向
                    opening_buy_price = spread_data.price_sell
                    opening_sell_price = spread_data.price_buy
            else:
                # 兜底：使用当前价格的反向
                opening_buy_price = spread_data.price_sell
                opening_sell_price = spread_data.price_buy

            opening_spread_abs = opening_sell_price - opening_buy_price
            opening_spread_pct = float(
                (opening_spread_abs / opening_buy_price) * 100) if opening_buy_price > 0 else 0.0

            opening_view = (
                f"开仓视角: 买{position_exchange_buy}@{opening_buy_price:.2f} "
                f"卖{position_exchange_sell}@{opening_sell_price:.2f} "
                f"(价差={opening_spread_pct:.4f}%)"
            )
            close_message = (
                f"📉 [平仓意图] {symbol} | "
                f"数量={close_quantity} | "
                f"剩余={position.total_quantity} | "
                f"{closing_view} | {opening_view} | 原因={reason}"
            )
            self._log_with_throttle(
                key=f"close_intent:{symbol}",
                message=close_message,
                throttle_seconds=3.0
            )

            # 🔥 平仓详细数据 - 仅在DEBUG模式或大额平仓时输出
            is_reverse_view = (
                (spread_data.exchange_buy or "").lower() == (
                    position_exchange_sell or "").lower()
                and (spread_data.exchange_sell or "").lower() == (position_exchange_buy or "").lower()
            )

            if not is_reverse_view:
                rebuilt = None
                if getattr(self, "spread_pipeline", None):
                    rebuilt = self.spread_pipeline._build_spreads_from_position(
                        symbol, position)
                if rebuilt:
                    _, closing_from_memory = rebuilt
                    if closing_from_memory:
                        logger.warning(
                            "⚠️ [V2平仓] %s: 接收到的价差方向与持仓不符，已按照记忆方向重新计算平仓视角。",
                            symbol,
                        )
                        spread_data = closing_from_memory
                        is_reverse_view = True
                    else:
                        logger.warning(
                            "⚠️ [V2平仓] %s: 记忆方向缺少盘口数据，无法修正平仓视角，暂停本次平仓。",
                            symbol,
                        )
                        return
                else:
                    logger.warning(
                        "⚠️ [V2平仓] %s: 平仓视角与持仓方向不符且无法回溯记忆盘口，暂停本次平仓。",
                        symbol,
                    )
                    return
            if is_reverse_view and (self.debug.is_debug_enabled() or close_quantity >= position.total_quantity * Decimal('0.5')):
                logger.info(
                    "🔁 [平仓详情] %s | "
                    "平仓视角: 买%s/%s@%.2f→卖%s/%s@%.2f (价差=%.4f%%) | "
                    "开仓视角: 价差=%.4f%%",
                    symbol,
                    spread_data.exchange_buy,
                    spread_data.buy_symbol or symbol,
                    float(spread_data.price_buy),
                    spread_data.exchange_sell,
                    spread_data.sell_symbol or symbol,
                    float(spread_data.price_sell),
                    spread_data.spread_pct,
                    -spread_data.spread_pct
                )

            # 🔥 判断是否为最后一笔拆单
            is_last_split = self.decision_engine.is_last_split_order(
                symbol=symbol,
                order_quantity=close_quantity,
                is_open=False
            )

            if self._should_enforce_orderbook_liquidity(symbol):
                min_ob_qty = self._get_min_orderbook_quantity(symbol)
                # 🔥 使用 spread_data 的交易所和symbol（已经是平仓视角）
                legs = [
                    {
                        'exchange': spread_data.exchange_buy,
                        'symbol': spread_data.buy_symbol or symbol,
                        'quantity': close_quantity,
                        'is_buy': True,
                        'desc': "平仓买回腿",
                        'min_quantity': min_ob_qty
                    },
                    {
                        'exchange': spread_data.exchange_sell,
                        'symbol': spread_data.sell_symbol or symbol,
                        'quantity': close_quantity,
                        'is_buy': False,
                        'desc': "平仓卖出腿",
                        'min_quantity': min_ob_qty
                    },
                ]
                liquidity_ok, failure_detail = self._verify_orderbook_liquidity(
                    symbol,
                    legs,
                    action="平仓"
                )
                if not liquidity_ok:
                    self._log_liquidity_failure_summary(
                        symbol,
                        reason="V2平仓",
                        failure_detail=failure_detail,
                        base_message=f"⚠️ [V2平仓] {symbol}: 对手盘流动性不足，等待下次机会",
                        throttle_seconds=5.0,
                    )
                    return
                self._clear_liquidity_failure_summary("V2平仓", symbol)

            self._log_persistence_confirmation(
                symbol=symbol,
                action="平仓",
                spread_pct=float(spread_data.spread_pct)
            )

            # 🔥 V2平仓：直接使用 spread_data（已经是平仓视角）
            close_key = symbol.upper()
            if not await self._try_register_close_symbol(close_key):
                logger.debug("🔁 [V2平仓] %s 已有执行任务，跳过重复触发", symbol)
                return
            # spread_data 是从 build_closing_spread_from_orderbooks() 返回的
            execution_task = asyncio.create_task(
                self._execute_close_with_lock(
                    symbol=symbol,
                    close_quantity=close_quantity,
                    spread_data=spread_data,
                    reason=reason,
                    is_last_split=is_last_split,
                    position_segments=position_segments,
                    slippage_pct=slippage_pct,
                    buy_limit_offset=buy_limit_offset,
                    sell_limit_offset=sell_limit_offset,
                    position=position,
                    close_key=close_key,
                    funding_rate_data=funding_rate_data,
                )
            )
            # 🔥 不等待执行完成，让任务在后台运行
            return

        except Exception as e:
            logger.error(f"❌ [统一调度] 平仓检查异常: {symbol}: {e}", exc_info=True)

    async def _execute_and_record_close(
        self,
        symbol: str,
        close_quantity: Decimal,
        spread_data: SpreadData,
        reason: str,
        is_last_split: bool,
        position_segments: int,
        slippage_pct: Optional[Decimal],
        buy_limit_offset: Optional[Decimal],
        sell_limit_offset: Optional[Decimal],
        position: Optional[SegmentedPosition],
        funding_rate_data: Optional[FundingRateData],
    ):
        """异步执行平仓并记录结果"""
        # 🔥 关键修复：spread_data 已经是平仓视角的数据（由 build_closing_spread_from_orderbooks
        # 或 calculate_multi_leg_closing_spread 计算），其 exchange/price/symbol 都是平仓时应该使用的，
        # 不需要再次反转！
        #
        # build_closing_spread_from_orderbooks() 的逻辑：
        # - 平仓 exchange_buy = 开仓 exchange_sell（已反转）
        # - 平仓 price_buy = 当前 exchange_sell 的 Ask（正确的平仓买入价）
        # - 平仓 exchange_sell = 开仓 exchange_buy（已反转）
        # - 平仓 price_sell = 当前 exchange_buy 的 Bid（正确的平仓卖出价）
        #
        # 如果再次反转，会导致价格匹配错误的交易所，无法成交！

        if not position or close_quantity <= 0 or close_quantity > position.total_quantity:
            return
        if (spread_data.exchange_buy != position.exchange_sell
                or spread_data.exchange_sell != position.exchange_buy):
            return
        net_profit = self._estimate_fifo_close_profit(position, close_quantity, spread_data)
        if net_profit is None or net_profit < 0:
            logger.warning("⏸️ [净收益门槛] %s: FIFO成本缺失或预计收益不足: %s", symbol, net_profit)
            return

        # 🔥 获取完整盘口数据（4组：买入腿Ask/Bid + 卖出腿Ask/Bid）
        buy_leg_symbol = spread_data.buy_symbol or symbol
        sell_leg_symbol = spread_data.sell_symbol or symbol
        orderbook_buy = self.data_processor.get_orderbook(
            spread_data.exchange_buy, buy_leg_symbol)
        orderbook_sell = self.data_processor.get_orderbook(
            spread_data.exchange_sell, sell_leg_symbol)

        orderbook_buy_ask = spread_data.price_buy if orderbook_buy and orderbook_buy.best_ask else None
        orderbook_buy_bid = orderbook_buy.best_bid.price if orderbook_buy and orderbook_buy.best_bid else None
        orderbook_sell_ask = orderbook_sell.best_ask.price if orderbook_sell and orderbook_sell.best_ask else None
        orderbook_sell_bid = spread_data.price_sell if orderbook_sell and orderbook_sell.best_bid else None

        # ✅ 平仓：日志展示应使用“平仓阈值”（T(n-1)），而不是开仓阈值（Tn）
        grid_threshold_pct = self._resolve_grid_close_threshold_pct(
            symbol=symbol,
            actual_position=getattr(position, "total_quantity", None) if position else None,
        )

        exec_request = ExecutionRequest(
            symbol=symbol,
            exchange_buy=spread_data.exchange_buy,      # ✅ 直接使用平仓视角的交易所
            exchange_sell=spread_data.exchange_sell,    # ✅ 直接使用平仓视角的交易所
            price_buy=spread_data.price_buy,            # ✅ 直接使用平仓视角的价格（Ask）
            price_sell=spread_data.price_sell,          # ✅ 直接使用平仓视角的价格（Bid）
            quantity=close_quantity,
            is_open=False,
            spread_data=spread_data,
            is_last_split=is_last_split,
            buy_symbol=buy_leg_symbol,                  # ✅ 平仓时买入的标的
            sell_symbol=sell_leg_symbol,                # ✅ 平仓时卖出的标的
            grid_action="close",
            grid_level=position_segments,
            grid_threshold_pct=grid_threshold_pct,
            slippage_tolerance_pct=slippage_pct,
            limit_price_offset_buy=buy_limit_offset,
            limit_price_offset_sell=sell_limit_offset,
            min_exchange_order_qty=self._build_min_exchange_order_qty_map(
                symbol),
            orderbook_buy_ask=orderbook_buy_ask,
            orderbook_buy_bid=orderbook_buy_bid,
            orderbook_sell_ask=orderbook_sell_ask,
            orderbook_sell_bid=orderbook_sell_bid,
        )
        result: Optional[ExecutionResult] = None
        try:
            result = await self.executor.execute_arbitrage(exec_request)

            if result.success:
                filled_qty = self._extract_filled_quantity(
                    result, close_quantity)
                if filled_qty > Decimal("0"):
                    await self.decision_engine.record_close(
                        symbol=symbol,
                        quantity=filled_qty,
                        spread_data=spread_data,
                        reason=reason
                    )
                    # 🔥 record_close() 已经更新了 position.total_quantity，直接读取即可
                    remaining_qty = position.total_quantity if position else Decimal(
                        '0')
                    logger.info(
                        "✅ [平仓成交] %s | 数量=%s | 剩余=%s | 原因=%s",
                        symbol,
                        filled_qty,
                        remaining_qty,
                        reason
                    )
                    self._clear_dual_limit_backoff(symbol)
                else:
                    logger.warning(
                        "⚠️ [平仓成交] %s | 执行器报告成交量为0，决策引擎未更新",
                        symbol
                    )
                    self._clear_dual_limit_backoff(symbol)
            else:
                if getattr(result, "failure_code", None) == "dual_limit_no_fill":
                    self._schedule_dual_limit_backoff(symbol)
                else:
                    self._clear_dual_limit_backoff(symbol)
        except Exception as e:
            logger.error(f"❌ [统一调度] 平仓执行异常: {symbol}: {e}", exc_info=True)
            # 将异常也计入“执行失败暂停”风控
            try:
                if getattr(self, "risk_controller", None):
                    self.risk_controller.record_execution_outcome(  # type: ignore[attr-defined]
                        symbol=symbol,
                        success=False,
                        partial_failure=False,
                        reason=str(e),
                        risk_events=["exception"],
                    )
            except Exception:
                pass
        finally:
            # 将执行结果上报到全局风控：连续失败 -> 暂停
            try:
                if getattr(self, "risk_controller", None) and result is not None:
                    # 若是“全局暂停闸门”主动拦截，不计入连续失败风控（避免暂停期间把失败计数刷满）
                    if getattr(result, "failure_code", None) == "global_paused":
                        return
                    # ✅ 特例：限价+市价模式下，限价腿 0 成交→撤单结束（未产生仓位），属于“正常未成交”
                    # - 不应计入“连续失败暂停”
                    # - 市价单 0 成交仍视为真实失败（不在此豁免范围）
                    if getattr(result, "failure_code", None) == "limit_zero_fill":
                        return
                    update = self.risk_controller.record_execution_outcome(  # type: ignore[attr-defined]
                        symbol=symbol,
                        success=bool(getattr(result, "success", False)),
                        partial_failure=bool(
                            getattr(result, "partial_failure", False)),
                        reason=str(getattr(result, "error_message", "") or ""),
                        risk_events=list(
                            getattr(result, "risk_events", []) or []),
                    )
                    # 失败计数日志（1/3、2/3...），成功会在风控内部清零
                    if isinstance(update, dict) and not bool(update.get("success", True)):
                        streak = int(update.get("streak", 0) or 0)
                        th = int(update.get("threshold", 0) or 0)
                        self._log_with_throttle(
                            key=f"execution_pause:fail_streak:close:{symbol}",
                            signature=f"{streak}/{th}",
                            message=f"🧯 [风控] 执行失败计数: {streak}/{th} (平仓) symbol={symbol} reason={getattr(result, 'error_message', '') or ''}",
                            level="warning",
                            throttle_seconds=3.0,
                        )
                    # 达到阈值触发暂停：打印一次确认日志
                    if isinstance(update, dict) and bool(update.get("triggered", False)):
                        self._log_with_throttle(
                            key="execution_pause:triggered",
                            signature="execution_pause_triggered",
                            # type: ignore[attr-defined]
                            message=f"⏸️ [风控] 已触发执行失败暂停：{self.risk_controller.should_block_execution_pause()[1]}",
                            level="warning",
                            throttle_seconds=60.0,
                        )
            except Exception:
                pass

            # 滑点触发暂停（全局暂停，禁止开/平仓）
            try:
                if getattr(self, "risk_controller", None) and result is not None:
                    self.risk_controller.record_slippage_pause(  # type: ignore[attr-defined]
                        symbol=symbol,
                        is_open=False,
                        slippage_pct=getattr(result, "slippage_pct", None),
                    )
            except Exception:
                pass

            # 🔥 高滑点“近N次成功成交”触发重连（不暂停）：仅统计成交成功
            try:
                if getattr(self, "risk_controller", None) and result is not None:
                    getattr(self.risk_controller, "record_high_slippage_reconnect", None) and self.risk_controller.record_high_slippage_reconnect(  # type: ignore[attr-defined]
                        symbol=symbol,
                        is_open=False,
                        slippage_pct=getattr(result, "slippage_pct", None),
                        success=bool(getattr(result, "success", False)),
                        partial_failure=bool(getattr(result, "partial_failure", False)),
                    )
            except Exception:
                pass
            self._handle_emergency_close_feedback(
                symbol, exec_request, result, action_label="平仓")
            # ✅ 仅记录“开/平仓活动时间”，持仓校验改为后台定时轮询（避免交易刚完成时 WS/缓存未刷新导致误判）
            try:
                self._mark_open_close_activity(action="close", symbol=symbol)
            except Exception:
                pass

    async def _execute_close_with_lock(
        self,
        *,
        symbol: str,
        close_quantity: Decimal,
        spread_data: SpreadData,
        reason: str,
        is_last_split: bool,
        position_segments: List[PositionSegment],
        slippage_pct: Optional[Decimal],
        buy_limit_offset: Optional[Decimal],
        sell_limit_offset: Optional[Decimal],
        position: SegmentedPosition,
        close_key: str,
        funding_rate_data: Optional[FundingRateData],
    ) -> None:
        try:
            await self._execute_and_record_close(
                symbol=symbol,
                close_quantity=close_quantity,
                spread_data=spread_data,
                reason=reason,
                is_last_split=is_last_split,
                position_segments=position_segments,
                slippage_pct=slippage_pct,
                buy_limit_offset=buy_limit_offset,
                sell_limit_offset=sell_limit_offset,
                position=position,
                funding_rate_data=funding_rate_data,
            )
        finally:
            self._release_close_symbol(close_key)

    def _fee_rate_for_exchange(self, exchange: str) -> Decimal:
        fees = self.decision_engine.exchange_fee_config
        config = fees.get(exchange, fees.get("default"))
        mode = self.executor._get_exchange_mode_config(exchange, None)
        attribute = "limit_fee_rate" if mode and mode.order_mode == "limit" else "market_fee_rate"
        return Decimal(str(getattr(config, attribute, 0)))

    def _estimate_fifo_close_profit(self, position, quantity: Decimal, spread_data) -> Optional[Decimal]:
        remaining = quantity
        profit = Decimal("0")
        if spread_data.price_buy <= 0 or spread_data.price_sell <= 0:
            return None
        buy_fee = self._fee_rate_for_exchange(position.exchange_buy)
        sell_fee = self._fee_rate_for_exchange(position.exchange_sell)
        for segment in position.get_open_segments():
            take = min(remaining, segment.open_quantity)
            if take <= 0:
                continue
            if not segment.open_price_buy or not segment.open_price_sell or segment.open_price_buy <= 0 or segment.open_price_sell <= 0:
                return None
            entry_buy_fee = segment.entry_fee_buy_per_unit
            entry_sell_fee = segment.entry_fee_sell_per_unit
            if entry_buy_fee is None:
                entry_buy_fee = segment.open_price_buy * buy_fee
            if entry_sell_fee is None:
                entry_sell_fee = segment.open_price_sell * sell_fee
            profit += take * (spread_data.price_sell - segment.open_price_buy
                              + segment.open_price_sell - spread_data.price_buy
                              - entry_buy_fee - entry_sell_fee
                              - spread_data.price_sell * buy_fee
                              - spread_data.price_buy * sell_fee)
            remaining -= take
            if remaining <= 0:
                return profit
        return None

    def _position_loss_reason(self, position, spread_data, config) -> Optional[str]:
        try:
            limit = Decimal(str(config.risk_config.max_loss_percent))
            if not limit.is_finite() or limit <= 0:
                return None
            if (spread_data.exchange_buy != position.exchange_sell
                    or spread_data.exchange_sell != position.exchange_buy):
                return None
            if not all(price.is_finite() and price > 0 for price in (spread_data.price_buy, spread_data.price_sell)):
                return None
            threshold = getattr(config.grid_config, "max_local_orderbook_spread_pct", None)
            threshold = Decimal(str(threshold)) if threshold is not None else None
            if threshold is not None and not threshold.is_finite():
                return None
            prices = []
            for exchange, symbol in (
                (position.exchange_sell, position.sell_symbol or position.symbol),
                (position.exchange_buy, position.buy_symbol or position.symbol),
            ):
                book = self.data_processor.get_orderbook(
                    exchange, symbol, max_age_seconds=self.get_data_freshness_seconds(exchange))
                if not book or not book.best_ask or not book.best_bid:
                    return None
                ask, bid = Decimal(str(book.best_ask.price)), Decimal(str(book.best_bid.price))
                if not all(price.is_finite() and price > 0 for price in (ask, bid)) or bid > ask:
                    return None
                if (threshold is not None and threshold > 0
                        and (ask - bid) / ask * 100 - threshold > Decimal("0.00000001")):
                    return None
                prices.append((ask, bid))
            # Estimate from the exact quotes that passed validation.
            spread_data = replace(spread_data, price_buy=prices[0][0], price_sell=prices[1][1])
            profit = self._estimate_fifo_close_profit(position, position.total_quantity, spread_data)
            notional = sum((segment.open_quantity * (segment.open_price_buy + segment.open_price_sell) / 2
                            for segment in position.get_open_segments()), Decimal("0"))
            if profit is None or not profit.is_finite() or not notional.is_finite() or notional <= 0:
                return None
            if -profit >= notional * limit / 100:
                return f"最大净亏损达到{limit}%"
        except (InvalidOperation, TypeError, ValueError):
            return None
        return None
