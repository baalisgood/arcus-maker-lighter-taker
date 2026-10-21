"""
⚠️ 本文件由 `unified_orchestrator.py` 拆分生成（只做文件拆分，不改功能逻辑）。

职责：开仓决策与执行链路（开仓门槛、构造请求、异步执行与记录）。

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


class UnifiedOrchestratorOpenMixin:
    """由 unified_orchestrator.py 拆分出的 mixin。"""

    async def _check_and_open(
        self,
        symbol: str,
        spread_data: SpreadData,
        funding_rate_data: Optional[FundingRateData],
        *,
        config_symbol: Optional[str] = None
    ):
        """检查并执行开仓（V2总量驱动算法）"""
        try:
            # 监控模式=模拟盘：禁用账户/交易所状态类风控拦截（避免阻断模拟下单闭环）
            if not bool(getattr(self, "monitor_only_mode", True)):
                # 执行失败暂停（仅该风控会同时禁止开仓/平仓）
                if getattr(self, "risk_controller", None):
                    # type: ignore[attr-defined]
                    blocked, pause_reason = self.risk_controller.should_block_execution_pause()
                    if blocked:
                        self._log_with_throttle(
                            key=f"global_pause_open:{symbol}",
                            signature="global_pause_open",
                            message=f"⏸️ [风控] {symbol}: 执行失败暂停中，已禁止开仓。原因={pause_reason or 'unknown'}",
                            level="warning",
                            throttle_seconds=60.0,
                        )
                        return
                if self.reduce_only_guard.is_pair_blocked(symbol):
                    self._log_with_throttle(
                        key=f"reduce_only_open:{symbol}",
                        message=f"⏸️ [V2开仓] {symbol}: 交易所在 reduce-only 模式，仅允许平仓，跳过开仓。",
                        # 该模块 logger 默认 WARNING；若不显式指定，INFO 会被过滤导致“看不到拦截原因”
                        level="warning",
                        throttle_seconds=60.0,
                    )
                    return
            if self._should_skip_due_to_dual_limit_backoff(symbol):
                return
            symbol_config = self.config_manager.get_config(symbol)
            slippage_pct = self._resolve_slippage_pct(symbol, symbol_config)

            # ===== EMA价差门槛：持续喂样本（低开销：仅在启用时执行一次 O(1) 更新）=====
            ema_open_obs: Optional[EmaUpdateResult] = None
            try:
                gc = symbol_config.grid_config
                if bool(getattr(gc, "ema_spread_gate_enabled", False)):
                    # 一次性确认：该交易对启用了 EMA 门槛（用于用户验证功能已启用）
                    # 🔥 使用标志位确保每个交易对只打印一次，避免重复打印
                    log_key = (symbol, "open")
                    if not hasattr(self, "_ema_gate_enabled_logged"):
                        self._ema_gate_enabled_logged = set()
                    if log_key not in self._ema_gate_enabled_logged:
                        logger.warning(
                            f"🧮 [EMA门槛] {symbol}: 已启用(开仓) "
                            f"window={float(getattr(gc, 'ema_spread_window_seconds', 30.0) or 30.0)}s "
                            f"Δ={float(getattr(gc, 'ema_spread_deviation_pct', 0.0) or 0.0):.4f}% "
                            f"strict={bool(getattr(gc, 'ema_spread_strict_samples_per_second', False))}"
                        )
                        self._ema_gate_enabled_logged.add(log_key)
                    ema_open_obs = self._observe_spread_ema(
                        symbol=symbol,
                        action="open",
                        value_pct=float(spread_data.spread_pct),
                        window_seconds=float(
                            getattr(gc, "ema_spread_window_seconds", 30.0) or 30.0),
                        strict=bool(
                            getattr(gc, "ema_spread_strict_samples_per_second", False)),
                    )
            except Exception:
                ema_open_obs = None
            if not self._is_symbol_market_open(symbol, base_symbol=config_symbol):
                self._log_with_throttle(
                    key=f"market_closed:{symbol}",
                    message=(
                        f"⏸️ [V2开仓] {symbol}: 当前处于休市时间，跳过开仓。"
                        "仅允许reduce-only平仓。"
                    ),
                    # 该模块 logger 级别默认为 WARNING（见文件顶部 setup_logger），
                    # 若这里用 info 会被过滤，导致“休市功能已触发但日志看不到”的错觉。
                    level="warning",
                    throttle_seconds=60.0,
                )
                return
            # 🔥 V2接口：返回(是否开仓, 开仓数量)
            should_open, open_quantity = await self.decision_engine.should_open(
                symbol,
                spread_data,
                funding_rate_data
            )

            if not should_open or open_quantity <= Decimal('0'):
                # 🔥 检查是否检测到反向开仓（实为平仓信号）
                if getattr(self.decision_engine, '_reverse_open_detected', False):
                    self.decision_engine._reverse_open_detected = False  # 重置标记
                    logger.info(f"🔄 [{symbol}] 反向开仓被拦截，立即触发平仓检查")
                    # 立即执行平仓检查
                    await self._check_and_close(
                        symbol,
                        spread_data,
                        funding_rate_data
                    )
                return

            try:
                anchor_ts = self._analysis_anchor_ts.get(symbol)
                if anchor_ts:
                    self.decision_signal_latency_ms = (time.time() - anchor_ts) * 1000.0
                    self.decision_signal_latency_label = "open"
            except Exception:
                pass

            # 🔒 启动持仓对齐未完成/失败时的安全策略：禁止开仓（仅允许平仓）
            # - 避免“重启时持仓尚未恢复/快照不完整，就开始继续开仓”引发重复仓位
            if bool(getattr(self, "_bootstrap_positions_block_open", False)):
                self._log_with_throttle(
                    key=f"bootstrap_block_open:{symbol}",
                    signature="bootstrap_block_open",
                    message=(
                        f"⏸️ [启动对齐] {symbol}: 启动持仓对齐尚未成功，已禁止开仓（仅允许平仓）。"
                        f" last_error={getattr(self, '_bootstrap_positions_last_error', None) or '-'}"
                    ),
                    level="warning",
                    throttle_seconds=60.0,
                )
                return

            # 🔢 当前网格层级（后续日志与EMA门槛复用）
            grid_level = self.decision_engine.get_grid_level(
                symbol, self.decision_engine._calculate_net_spread(spread_data))

            # ===== 风控闸门：仅禁止开仓（平仓不受影响）=====
            # 1) 余额不足（权益不足）-> 只允许平仓
            # 2) 交易所总持仓价值超阈值 -> 只允许平仓
            if not bool(getattr(self, "monitor_only_mode", True)):
                blocked, reason, block_type = self._open_only_close_guard.should_block_open(
                    exchange_buy=spread_data.exchange_buy,
                    exchange_sell=spread_data.exchange_sell,
                    symbol=(config_symbol or symbol),
                )
                if blocked:
                    # 🔔 余额/持仓价值风控触发：声音报警（异步）
                    # - 余额不足：默认 5 次（不改）
                    # - 总持仓价值过高（max_exchange_position_value）：仅提醒 1 次（用户需求）
                    try:
                        sound_enabled = True
                        try:
                            rc = getattr(self, "risk_controller", None)
                            cfg = getattr(getattr(rc, "config", None),
                                          "open_only_close", None)
                            if cfg is not None:
                                if block_type == "low_balance":
                                    sound_enabled = bool(
                                        getattr(getattr(cfg, "low_balance", None), "sound_alarm_enabled", True))
                                elif block_type == "max_exchange_position_value":
                                    sound_enabled = bool(
                                        getattr(
                                            getattr(cfg, "max_exchange_position_value", None), "sound_alarm_enabled", True)
                                    )
                                elif block_type == "price_volatility":
                                    sound_enabled = bool(
                                        getattr(
                                            getattr(cfg, "price_volatility", None), "sound_alarm_enabled", False
                                        )
                                    )
                        except Exception:
                            sound_enabled = True

                        repeat_override = None
                        if isinstance(reason, str) and ("总持仓价值过高" in reason or "持仓价值过高" in reason):
                            repeat_override = 1
                        if sound_enabled:
                            getattr(self, "_sound_alarm", None) and self._sound_alarm.trigger(
                                signature="open_only_close_blocked",
                                reason=reason,
                                repeat_times_override=repeat_override,
                            )
                    except Exception:
                        pass
                    self._log_with_throttle(
                        key=f"open_only_close:{symbol}",
                        message=(
                            f"⏸️ [风控] {symbol}: {reason}，已禁止开仓（仅允许平仓）。"
                        ),
                        # 用固定签名按 symbol 节流，避免数值微小变化导致刷屏
                        signature="open_only_close_blocked",
                        # 该模块 logger 默认 WARNING，若不显式指定 level，这条关键信息会被 INFO 过滤掉
                        level="warning",
                        throttle_seconds=120.0,
                    )
                    return

            # ===== 新增门槛：EMA价差门槛（严格模式断秒同时拦截开仓/平仓）=====
            ema_ok, ema_reason = self._passes_ema_spread_gate_open(
                symbol=symbol,
                spread_pct=float(spread_data.spread_pct),
                grid_level=grid_level,
                observed=ema_open_obs,
            )
            if not ema_ok:
                self._log_with_throttle(
                    key=f"ema_spread_gate:open:{symbol}",
                    signature="ema_spread_gate_open",
                    message=f"⛔️ [EMA门槛] {symbol}: {ema_reason}，已拦截开仓。",
                    level="warning",
                    throttle_seconds=60.0,
                )
                return
            # 低频放行日志（帮助确认功能在运行且在放行）
            try:
                gc = symbol_config.grid_config
                if bool(getattr(gc, "ema_spread_gate_enabled", False)) and ema_open_obs is not None and ema_open_obs.ema is not None:
                    initial = float(
                        getattr(gc, "initial_spread_threshold", 0.0) or 0.0)
                    step = float(getattr(gc, "grid_step", 0.0) or 0.0)
                    delta = float(
                        getattr(gc, "ema_spread_deviation_pct", 0.0) or 0.0)
                    th = initial + (max(1, int(grid_level)) - 1) * step
                    min_ema = th - delta
                    self._log_with_throttle(
                        key=f"ema_spread_gate:open_pass:{symbol}",
                        signature="ema_spread_gate_open_pass",
                        message=(
                            f"✅ [EMA门槛] {symbol}: 开仓放行 "
                            f"ema={float(ema_open_obs.ema):.4f}% ≥ {min_ema:.4f}% "
                            f"(T{grid_level}={th:.4f}%, Δ={delta:.4f}%)"
                        ),
                        level="warning",
                        throttle_seconds=300.0,
                    )
            except Exception:
                pass

            if not self._passes_price_stability(symbol, spread_data, action="开仓"):
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
                        "⚠️ [配置] %s: max_local_orderbook_spread_pct=%s 无法解析，已忽略该门槛",
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

            if self.symbol_state_manager:
                blocked, state = self.symbol_state_manager.should_block(
                    symbol, grid_level)
                if blocked:
                    reason = state.reason if state else "等待中"
                    # 该模块 logger 默认 WARNING；若不显式指定，INFO 会被过滤导致“看不到拦截原因”
                    self._log_with_throttle(
                        key=f"symbol_waiting_open:{symbol}",
                        signature="symbol_waiting_open",
                        message=(
                            f"⏸️ [V2开仓] {symbol}: 当前处于等待状态，原因={reason}，保持跳过 "
                            f"(T{state.grid_level if state else grid_level})"
                        ),
                        level="warning",
                        throttle_seconds=30.0,
                    )
                    return

            buy_symbol = spread_data.buy_symbol or symbol
            sell_symbol = spread_data.sell_symbol or symbol

            # 🔥 获取真实的平仓视角价格（从当前订单簿重新计算，而不是简单对调）
            # 这样日志中显示的平仓价格才是真实的市场价格
            closing_buy_exchange = spread_data.exchange_sell
            closing_sell_exchange = spread_data.exchange_buy
            closing_buy_symbol = sell_symbol
            closing_sell_symbol = buy_symbol

            # 🔥 尝试从当前订单簿获取真实的平仓价格
            # 平仓时：在 exchange_sell 买回，在 exchange_buy 卖出
            closing_buy_price = spread_data.price_sell  # 默认值（兜底）
            closing_sell_price = spread_data.price_buy  # 默认值（兜底）
            closing_spread_pct = -spread_data.spread_pct  # 默认值（兜底）

            try:
                # 尝试获取当前订单簿数据
                closing_buy_ob = self.data_processor.get_orderbook(
                    closing_buy_exchange,
                    closing_buy_symbol,
                    max_age_seconds=self.get_data_freshness_seconds(closing_buy_exchange),
                )
                closing_sell_ob = self.data_processor.get_orderbook(
                    closing_sell_exchange,
                    closing_sell_symbol,
                    max_age_seconds=self.get_data_freshness_seconds(closing_sell_exchange),
                )

                # 如果订单簿数据可用，使用真实的当前价格
                if closing_buy_ob and closing_buy_ob.best_ask:
                    closing_buy_price = closing_buy_ob.best_ask.price
                if closing_sell_ob and closing_sell_ob.best_bid:
                    closing_sell_price = closing_sell_ob.best_bid.price

                # 重新计算平仓价差
                if closing_buy_ob and closing_sell_ob and closing_buy_ob.best_ask and closing_sell_ob.best_bid:
                    closing_spread_abs = closing_sell_price - closing_buy_price
                    closing_spread_pct = float(
                        (closing_spread_abs / closing_buy_price) * 100)
            except Exception as e:
                logger.debug(f"[开仓意图] {symbol}: 无法获取实时平仓价格，使用兜底值: {e}")

            position = self.decision_engine.get_position(symbol)
            current_qty = position.total_quantity if position else Decimal('0')

            buy_offset_str = "0"
            sell_offset_str = "0"
            config_limit_offset = None
            if config_symbol:
                config_limit_offset = self._get_limit_price_offset_for_symbol(
                    config_symbol)
            buy_limit_offset = config_limit_offset or self._get_limit_price_offset_for_symbol(
                buy_symbol)
            sell_limit_offset = config_limit_offset or self._get_limit_price_offset_for_symbol(
                sell_symbol)
            if buy_limit_offset:
                buy_offset_str = f"{buy_limit_offset:+.4f}"
            if sell_limit_offset:
                sell_offset_str = f"{sell_limit_offset:+.4f}"

            # ------------------------------------------------------------------
            # 🔥 多档盘口聚合（Top-N）：用前 N 档计算 VWAP 买入价/卖出价
            # 目的：买1/卖1数量不足时，用买1..买N / 卖1..卖N 合并后的均价评估是否仍满足套利门槛
            # 注意：按“每条腿”独立启用：
            # - 支持多档/有数量/能覆盖下单量 -> 该腿用 VWAP
            # - 不支持（例如 Variational/BBO/无数量）-> 该腿保持原样（best_bid/best_ask）
            # ------------------------------------------------------------------
            price_mode = "BBO"
            try:
                agg_n = self._get_orderbook_aggregate_levels(config_symbol or symbol)
                if agg_n > 1 and open_quantity and open_quantity > Decimal("0"):
                    from ..utils.orderbook_aggregation import aggregate_top_n_vwap

                    buy_ob = self.data_processor.get_orderbook(
                        spread_data.exchange_buy,
                        buy_symbol,
                        max_age_seconds=self.get_data_freshness_seconds(spread_data.exchange_buy),
                    )
                    sell_ob = self.data_processor.get_orderbook(
                        spread_data.exchange_sell,
                        sell_symbol,
                        max_age_seconds=self.get_data_freshness_seconds(spread_data.exchange_sell),
                    )
                    if buy_ob and sell_ob:
                        buy_agg = aggregate_top_n_vwap(
                            buy_ob, is_buy=True, max_levels=agg_n, target_qty=open_quantity
                        )
                        sell_agg = aggregate_top_n_vwap(
                            sell_ob, is_buy=False, max_levels=agg_n, target_qty=open_quantity
                        )
                        buy_mode = "BBO"
                        sell_mode = "BBO"

                        # 买入腿：吃卖盘 asks
                        if buy_agg.vwap_price is not None and buy_agg.fully_covered:
                            spread_data.price_buy = buy_agg.vwap_price
                            if buy_agg.total_available is not None:
                                spread_data.size_buy = buy_agg.total_available
                            buy_mode = f"VWAP{agg_n}"

                        # 卖出腿：吃买盘 bids
                        if sell_agg.vwap_price is not None and sell_agg.fully_covered:
                            spread_data.price_sell = sell_agg.vwap_price
                            if sell_agg.total_available is not None:
                                spread_data.size_sell = sell_agg.total_available
                            sell_mode = f"VWAP{agg_n}"

                        # 无论是否混合模式，都按最终 price_buy/price_sell 重算价差
                        if spread_data.price_buy and spread_data.price_buy > 0 and spread_data.price_sell:
                            spread_data.spread_abs = spread_data.price_sell - spread_data.price_buy
                            spread_data.spread_pct = float(
                                ((spread_data.price_sell - spread_data.price_buy) / spread_data.price_buy) * 100
                            )

                        price_mode = f"buy={buy_mode} sell={sell_mode}"
            except Exception:
                price_mode = "BBO"

            if self._should_log_open_intent(symbol, spread_data.spread_pct):
                open_message = (
                    f"📈 [开仓意图] {symbol} | "
                    f"数量={open_quantity} | "
                    f"当前持仓={current_qty} | "
                    f"买{spread_data.exchange_buy}@{spread_data.price_buy:.2f}(偏移{buy_offset_str}) "
                    f"卖{spread_data.exchange_sell}@{spread_data.price_sell:.2f}(偏移{sell_offset_str}) | "
                    f"开仓价差={spread_data.spread_pct:.4f}% | "
                    f"价格模式={price_mode} | "
                    f"平仓视角: 买{closing_buy_exchange}@{closing_buy_price:.2f} "
                    f"卖{closing_sell_exchange}@{closing_sell_price:.2f} "
                    f"(价差={closing_spread_pct:.4f}%) | "
                    f"网格T{grid_level}"
                )
                self._log_with_throttle(
                    key=f"open_intent:{symbol}",
                    message=open_message,
                    throttle_seconds=3.0
                )

            # 🔥 详细价差数据 - 仅在DEBUG模式或首次开仓时输出
            if self.debug.is_debug_enabled() or current_qty <= Decimal('0'):
                detail_msg = (
                    "🔀 [开仓详情] "
                    f"{symbol} | 买{spread_data.exchange_buy}/{buy_symbol}@{float(spread_data.price_buy):.2f}(偏移{buy_offset_str}) "
                    f"卖{spread_data.exchange_sell}/{sell_symbol}@{float(spread_data.price_sell):.2f}(偏移{sell_offset_str}) | "
                    f"开仓价差=+{spread_data.spread_pct:.4f}% 平仓视角={-spread_data.spread_pct:.4f}%"
                )
                # 开仓详情在无持仓时最容易重复刷屏，增加节流保护
                self._log_with_throttle(
                    key=f"open_detail:{symbol}",
                    message=detail_msg,
                    throttle_seconds=10.0
                )

            # 🔥 判断是否为最后一笔拆单
            is_last_split = self.decision_engine.is_last_split_order(
                symbol=symbol,
                order_quantity=open_quantity,
                is_open=True
            )

            if self._should_enforce_orderbook_liquidity(symbol):
                min_ob_qty = self._get_min_orderbook_quantity(symbol)
                legs = [
                    {
                        'exchange': spread_data.exchange_buy,
                        'symbol': buy_symbol,
                        'quantity': open_quantity,
                        'is_buy': True,
                        'desc': "开仓买入腿",
                        'min_quantity': min_ob_qty
                    },
                    {
                        'exchange': spread_data.exchange_sell,
                        'symbol': sell_symbol,
                        'quantity': open_quantity,
                        'is_buy': False,
                        'desc': "开仓卖出腿",
                        'min_quantity': min_ob_qty
                    },
                ]
                liquidity_ok, failure_detail = self._verify_orderbook_liquidity(
                    symbol,
                    legs,
                    action="开仓"
                )
                if not liquidity_ok:
                    self._log_liquidity_failure_summary(
                        symbol,
                        reason="V2开仓",
                        failure_detail=failure_detail,
                        base_message=f"⚠️ [V2开仓] {symbol}: 对手盘流动性不足，跳过本次拆单",
                        throttle_seconds=5.0,
                    )
                    return
                self._clear_liquidity_failure_summary("V2开仓", symbol)

            # 🔐 持续性确认日志（仅在实际下单前打印一次）
            self._log_persistence_confirmation(
                symbol=symbol,
                action="开仓",
                spread_pct=float(spread_data.spread_pct)
            )

            # 🔥 V2使用简化执行：直接执行开仓数量，不需要segment_id
            # 🔥 使用异步任务执行，避免阻塞主循环和UI更新
            open_key = self._build_open_pair_key(
                symbol, spread_data.exchange_buy, spread_data.exchange_sell)
            if not await self._try_register_open_pair(open_key):
                age_s = None
                try:
                    started_at = getattr(
                        self, "_pending_open_pairs_started_at", {}).get(open_key)
                    if started_at:
                        age_s = time.time() - float(started_at)
                except Exception:
                    age_s = None
                # 降噪：in-flight 重复触发改为 debug，避免高频刷屏
                self._log_with_throttle(
                    key=f"open_inflight:{open_key}",
                    signature="open_inflight",
                    message=(
                        f"🔁 [V2开仓] {symbol}: {spread_data.exchange_buy}→{spread_data.exchange_sell} "
                        f"已有执行任务(in-flight){f' age={age_s:.1f}s' if isinstance(
                            age_s, (int, float)) else ''}，跳过重复触发。"
                    ),
                    level="debug",
                    throttle_seconds=10.0,
                )
                return
            execution_task = asyncio.create_task(
                self._execute_open_with_lock(
                    symbol=symbol,
                    open_quantity=open_quantity,
                    spread_data=spread_data,
                    funding_rate_data=funding_rate_data,
                    is_last_split=is_last_split,
                    grid_level=grid_level,
                    slippage_pct=slippage_pct,
                    buy_limit_offset=buy_limit_offset,
                    sell_limit_offset=sell_limit_offset,
                    position=position,
                    open_key=open_key
                )
            )
            # ✅ 兜底：任务结束/取消/异常时，确保 open_key 不会残留导致后续永远 in-flight
            # 说明：理论上 `_execute_open_with_lock` 的 finally 一定会释放，但如果 task 在首次运行前
            # 就被 cancel（例如重连/退出时统一取消 task），finally 不会执行，因此需要 done_callback 兜底。

            def _on_open_task_done(task: asyncio.Task) -> None:
                try:
                    if task.cancelled():
                        logger.warning(
                            "🛑 [V2开仓] 开仓任务被取消，已强制释放in-flight锁: key=%s",
                            open_key,
                        )
                    else:
                        exc = task.exception()
                        if exc is not None:
                            logger.error(
                                "❌ [V2开仓] 开仓任务异常结束，已强制释放in-flight锁: key=%s err=%s",
                                open_key,
                                exc,
                                exc_info=exc,
                            )
                except Exception:
                    # 避免 done_callback 自身抛异常污染事件循环
                    pass
                try:
                    # done_callback 中不要 await，避免阻塞事件循环；discard/pop 是幂等的
                    self._pending_open_pairs.discard(open_key)
                    getattr(self, "_pending_open_pairs_started_at",
                            {}).pop(open_key, None)
                except Exception:
                    pass

            try:
                execution_task.add_done_callback(_on_open_task_done)
            except Exception:
                pass
            # 🔥 不等待执行完成，让任务在后台运行
            return

        except Exception as e:
            logger.error(f"❌ [统一调度] 开仓检查异常: {symbol}: {e}", exc_info=True)

    async def _execute_and_record_open(
        self,
        symbol: str,
        open_quantity: Decimal,
        spread_data: SpreadData,
        funding_rate_data: Optional[FundingRateData],
        is_last_split: bool,
        grid_level: int,
        slippage_pct: Optional[Decimal],
        buy_limit_offset: Optional[Decimal],
        sell_limit_offset: Optional[Decimal],
        position: Optional[SegmentedPosition]
    ):
        """异步执行开仓并记录结果"""
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

        grid_threshold_pct = self._resolve_grid_threshold_pct(
            symbol, grid_level)

        exec_request = ExecutionRequest(
            symbol=symbol,
            exchange_buy=spread_data.exchange_buy,
            exchange_sell=spread_data.exchange_sell,
            price_buy=spread_data.price_buy,
            price_sell=spread_data.price_sell,
            quantity=open_quantity,
            is_open=True,
            spread_data=spread_data,
            is_last_split=is_last_split,
            buy_symbol=buy_leg_symbol,
            sell_symbol=sell_leg_symbol,
            grid_action="open",
            grid_level=grid_level,
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
        exec_request._expected_position_quantity = position.total_quantity if position else Decimal("0")
        result: Optional[ExecutionResult] = None
        try:
            # ✅ 仅对“限价+市价(limit_market)”启用的最小下单量拦截：
            # - 目标：避免“残差(dust)”反复触发下单，但数量永远达不到交易所最小下单量，造成无意义执行/噪音日志。
            # - 约束：其他下单模式完全不受影响。
            try:
                plan = self.executor.order_strategy_executor.determine_execution_plan(  # type: ignore[attr-defined]
                    exec_request
                )
                if (
                    isinstance(plan, dict)
                    and str(plan.get("mode", "")).strip().lower() == "limit_market"
                ):
                    epsilon = Decimal("0.00000001")
                    buy_min = self.executor._resolve_min_order_qty(  # type: ignore[attr-defined]
                        exec_request,
                        exec_request.exchange_buy,
                    )
                    sell_min = self.executor._resolve_min_order_qty(  # type: ignore[attr-defined]
                        exec_request,
                        exec_request.exchange_sell,
                    )
                    threshold = None
                    for v in (buy_min, sell_min):
                        try:
                            if v is not None and v > Decimal("0"):
                                threshold = v if threshold is None else max(threshold, v)
                        except Exception:
                            continue
                    if (
                        threshold is not None
                        and exec_request.quantity + epsilon < threshold
                    ):
                        self._log_with_throttle(
                            key=f"limit_market_min_qty_block:open:{symbol}:{exec_request.exchange_buy}:{exec_request.exchange_sell}",
                            signature="limit_market_min_qty_block",
                            message=(
                                f"⛔️ [限价+市价] {symbol}: 本次开仓数量 {exec_request.quantity} < 交易所最小下单量阈值 {threshold}，"
                                "已拦截执行（等待后续网格/差量累积到可下单数量）。"
                            ),
                            level="warning",
                            throttle_seconds=10.0,
                        )
                        return
            except Exception:
                # 拦截逻辑失败不应影响主流程（宁可放行也不误杀）
                pass

            result = await self.executor.execute_arbitrage(exec_request)

            if result.success or (result.failure_code == "EXECUTION_COST_UNKNOWN" and result.success_quantity > 0):
                self._clear_dual_limit_backoff(symbol)
                entry_price_buy = self._resolve_execution_price(
                    result.order_buy,
                    spread_data.price_buy
                )
                entry_price_sell = self._resolve_execution_price(
                    result.order_sell,
                    spread_data.price_sell
                )

                filled_quantity = self._extract_filled_quantity(
                    result, open_quantity)
                if filled_quantity <= 0:
                    return
                if filled_quantity + Decimal('0') < open_quantity:
                    logger.warning(
                        "⚠️ [V2开仓] %s: 实际成交 %s 低于目标 %s，剩余部分将延后补齐",
                        symbol,
                        filled_quantity,
                        open_quantity
                    )
                await self.decision_engine.record_open(
                    symbol=symbol,
                    quantity=open_quantity,
                    spread_data=spread_data,
                    funding_rate_data=funding_rate_data,
                    entry_fee_buy_per_unit=self._entry_fee_per_unit(result.order_buy, filled_quantity),
                    entry_fee_sell_per_unit=self._entry_fee_per_unit(result.order_sell, filled_quantity),
                    entry_price_buy=entry_price_buy,
                    entry_price_sell=entry_price_sell,
                    filled_quantity=filled_quantity
                )
                self.decision_engine.report_open_shortfall(
                    symbol=symbol,
                    requested_quantity=open_quantity,
                    actual_quantity=filled_quantity
                )

                if entry_price_buy <= 0 or entry_price_sell <= 0 or result.failure_code == "EXECUTION_COST_UNKNOWN":
                    logger.error("[开仓对账] %s: 已记录实际匹配量，成本未知，自动执行保持锁定", symbol)
                    return
                actual_spread = entry_price_sell - entry_price_buy
                actual_spread_pct = (actual_spread / entry_price_buy) * 100
                profit_estimate = float(actual_spread * open_quantity)
                logger.info(
                    "✅ [开仓成交] %s | 数量=%s | "
                    "买%s@%.2f 卖%s@%.2f | "
                    "实际价差=%.4f%% 理论=%.4f%% | "
                    "预期盈利=$%.2f | "
                    "新持仓=%s",
                    symbol,
                    open_quantity,
                    spread_data.exchange_buy,
                    entry_price_buy,
                    spread_data.exchange_sell,
                    entry_price_sell,
                    actual_spread_pct,
                    spread_data.spread_pct,
                    profit_estimate,
                    position.total_quantity + open_quantity if position else open_quantity
                )
            else:
                if getattr(result, "failure_code", None) == "dual_limit_no_fill":
                    self._schedule_dual_limit_backoff(symbol)
                else:
                    self._clear_dual_limit_backoff(symbol)
        except Exception as e:
            logger.error(f"❌ [统一调度] 开仓执行异常: {symbol}: {e}", exc_info=True)
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
                    if (getattr(result, "failure_code", None) == "RISK_REJECTED"
                            and not result.partial_failure and not result.success_quantity
                            and not result.order_buy and not result.order_sell
                            and not getattr(self.executor, "_unknown_execution_pairs", None)
                            and not any(getattr(self.exchange_adapters.get(ex), "_uncertain", None)
                                        for ex in (exec_request.exchange_buy, exec_request.exchange_sell))):
                        self._log_with_throttle(
                            key=f"open_precheck_rejected:{symbol}",
                            signature=str(result.error_message or ""),
                            message=f"⏸️ [开仓前检查] {symbol}: {result.error_message or ''}；本轮未下单",
                            level="warning", throttle_seconds=3.0,
                        )
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
                    # 失败计数日志（1/3、2/3...），成功会清零
                    if isinstance(update, dict) and not bool(update.get("success", True)):
                        streak = int(update.get("streak", 0) or 0)
                        th = int(update.get("threshold", 0) or 0)
                        self._log_with_throttle(
                            key=f"execution_pause:fail_streak:open:{symbol}",
                            signature=f"{streak}/{th}",
                            message=f"🧯 [风控] 执行失败计数: {streak}/{th} (开仓) symbol={symbol} reason={getattr(result, 'error_message', '') or ''}",
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
                        is_open=True,
                        slippage_pct=getattr(result, "slippage_pct", None),
                    )
            except Exception:
                pass

            # 🔥 高滑点“近N次成功成交”触发重连（不暂停）：仅统计成交成功
            try:
                if getattr(self, "risk_controller", None) and result is not None:
                    getattr(self.risk_controller, "record_high_slippage_reconnect", None) and self.risk_controller.record_high_slippage_reconnect(  # type: ignore[attr-defined]
                        symbol=symbol,
                        is_open=True,
                        slippage_pct=getattr(result, "slippage_pct", None),
                        success=bool(getattr(result, "success", False)),
                        partial_failure=bool(getattr(result, "partial_failure", False)),
                    )
            except Exception:
                pass
            self._handle_emergency_close_feedback(
                symbol, exec_request, result, action_label="开仓")
            # ✅ 仅记录“开/平仓活动时间”，持仓校验改为后台定时轮询（避免交易刚完成时 WS/缓存未刷新导致误判）
            try:
                self._mark_open_close_activity(action="open", symbol=symbol)
            except Exception:
                pass

    async def _execute_open_with_lock(
        self,
        *,
        symbol: str,
        open_quantity: Decimal,
        spread_data: SpreadData,
        funding_rate_data: Optional[FundingRateData],
        is_last_split: bool,
        grid_level: int,
        slippage_pct: Optional[Decimal],
        buy_limit_offset: Optional[Decimal],
        sell_limit_offset: Optional[Decimal],
        position: Optional[SegmentedPosition],
        open_key: str
    ) -> None:
        try:
            # Keep confirmed fills visible before another pair checks the shared account budget.
            async with self._pending_open_lock:
                await self._execute_and_record_open(
                    symbol=symbol,
                    open_quantity=open_quantity,
                    spread_data=spread_data,
                    funding_rate_data=funding_rate_data,
                    is_last_split=is_last_split,
                    grid_level=grid_level,
                    slippage_pct=slippage_pct,
                    buy_limit_offset=buy_limit_offset,
                    sell_limit_offset=sell_limit_offset,
                    position=position
                )
        finally:
            # 立即释放去重 key，不等待其他币正在使用的预算锁。
            try:
                await self._release_open_pair_async(open_key)
            except Exception:
                self._release_open_pair(open_key)

    @staticmethod
    def _entry_fee_per_unit(order, quantity):
        fee = getattr(order, "fee", None)
        if not fee or quantity <= 0:
            return None
        if isinstance(fee, dict) and fee.get("cost") is not None and str(fee.get("currency", "")).upper() in {"USDC", "USD"}:
            return Decimal(str(fee["cost"])) / quantity
        return None
