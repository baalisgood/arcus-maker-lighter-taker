from __future__ import annotations

"""
Orchestrator UI 控制器
--------------------
封装统一调度器的 UI 更新、数据汇总、余额刷新等纯展示逻辑：
- 负责周期性刷新 UI、收集统计/持仓/盘口数据
- 对外提供 start/stop 方法，不改变原有日志与行为
- 所有业务状态依旧由 `UnifiedOrchestrator` 持有，本类仅作只读访问
"""

import asyncio
import time
import logging
from datetime import datetime
from decimal import Decimal, ROUND_CEILING
from typing import TYPE_CHECKING, Dict, Any, Optional, List, Tuple
from itertools import combinations

from dataclasses import asdict

from core.adapters.exchanges.models import (
    OrderBookData,
    PositionData,
    PositionSide,
)
from ..models import PositionSegment, SegmentedPosition
from ..display.ui_manager import UIMode

if TYPE_CHECKING:
    from .unified_orchestrator import UnifiedOrchestrator


class OrchestratorUIController:
    def __init__(self, orchestrator: "UnifiedOrchestrator", logger: logging.Logger) -> None:
        self._orc = orchestrator
        self._logger = logger
        self._ui_manager_task: Optional[asyncio.Task] = None
        self._ui_update_task: Optional[asyncio.Task] = None
        self._ui_render_task: Optional[asyncio.Task] = None

        min_interval = float(
            getattr(
                orchestrator.monitor_config,
                "ui_min_refresh_interval_seconds",
                0.5,
            )
            or 0.5
        )
        refresh_interval = max(
            orchestrator.monitor_config.ui_refresh_interval_ms / 1000,
            max(0.2, min_interval),  # 最小允许 200ms，避免把终端打爆
        )
        self.ui_update_interval: float = refresh_interval
        self.last_ui_update_time: float = 0.0
        self.balance_update_interval: float = 30.0
        self.last_balance_update_time: float = 0.0
        self._balance_cache: Dict[str, List[Dict[str, Any]]] = {}

    def start(self, enable_ui_render_loop: bool = True) -> None:
        """启动 UI 相关的后台任务。"""
        if enable_ui_render_loop and hasattr(self._orc.ui_manager, "update_loop"):
            self._ui_manager_task = asyncio.create_task(
                self._orc.ui_manager.update_loop(
                    self._orc.monitor_config.ui_refresh_interval_ms
                )
            )
        self._ui_update_task = asyncio.create_task(self._ui_update_loop())

    async def stop(self) -> None:
        """停止 UI 背景任务。"""
        for task in (self._ui_update_task, self._ui_manager_task):
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self._ui_update_task = None
        self._ui_manager_task = None

    async def _ui_update_loop(self):
        """UI更新循环"""
        self._logger.info("[统一调度] UI更新循环已启动")

        try:
            while self._orc.running:
                try:
                    await self._update_ui_comprehensive()
                    await asyncio.sleep(self.ui_update_interval)
                except asyncio.CancelledError:
                    break
                except KeyboardInterrupt:
                    raise
                except Exception as exc:
                    self._logger.error(
                        f"❌ [统一调度] UI更新异常: {exc}", exc_info=True
                    )
        except KeyboardInterrupt:
            raise
        finally:
            self._logger.info("[统一调度] UI更新循环已停止")

    async def _update_ui_comprehensive(self):
        """完整更新UI数据"""
        orc = self._orc

        try:
            snapshot = await self._collect_ui_snapshot()
            # 避免渲染阻塞收集：若上轮渲染未结束则跳过本轮，防止堆积
            if self._ui_render_task and not self._ui_render_task.done():
                return
            self._ui_render_task = asyncio.create_task(
                self._render_ui_snapshot(snapshot)
            )

        except Exception as exc:
            self._logger.debug(f"[统一调度] UI更新失败: {exc}")

    async def _collect_ui_snapshot(self) -> Dict[str, Any]:
        """
        采集 UI 所需数据的轻量快照，避免渲染阻塞主流程。
        返回浅拷贝的字典，后续在独立协程渲染。
        """
        orc = self._orc
        current_time = time.time()
        should_refresh_market_data = (
            current_time - self.last_ui_update_time
        ) >= self.ui_update_interval

        stats = orc.data_processor.get_stats()
        receiver_stats = orc.data_receiver.get_stats()
        stats.update(receiver_stats)
        stats.setdefault("orderbook_queue_size", orc.orderbook_queue.qsize())
        stats.setdefault("ticker_queue_size", orc.ticker_queue.qsize())
        stats.setdefault("analysis_queue_size", 0)
        stats["exchanges"] = orc.monitor_config.exchanges
        stats["symbols_count"] = len(orc.monitor_config.symbols)
        stats["ui_update_interval"] = self.ui_update_interval
        stats["analysis_fallback_loop_count"] = int(
            getattr(orc, "analysis_fallback_loop_count", 0) or 0
        )
        stats["decision_signal_latency_ms"] = getattr(
            orc, "decision_signal_latency_ms", None
        )
        stats["decision_signal_latency_label"] = getattr(
            orc, "decision_signal_latency_label", None
        )

        # 🔥 仅用于UI显示：记录部分交易所的“账户数据源”（例如 variational: rest/ws）
        # 注意：这不影响任何业务逻辑，只用于终端UI展示用户当前选择。
        try:
            sources: Dict[str, str] = {}
            ex_adapters = getattr(orc, "exchange_adapters", None) or {}
            # 兼容 key：variational / var
            for k in ("variational", "var"):
                adapter = ex_adapters.get(k)
                if not adapter:
                    continue
                cfg = getattr(adapter, "config", None)
                extra = getattr(cfg, "extra_params", None) if cfg else None
                if isinstance(extra, dict):
                    v = str(extra.get("account_data_source", "")).strip().lower().replace(" ", "")
                    if v in ("rest", "ws", "rest+ws", "ws+rest", "both", "hybrid"):
                        sources["variational"] = "rest+ws" if v in ("rest+ws", "ws+rest", "both", "hybrid") else v
                        break
            if sources:
                stats["exchange_account_sources"] = sources
        except Exception:
            pass

        stats_snapshot = dict(stats)  # 浅拷贝

        risk_status = orc.risk_controller.get_risk_status()
        today = datetime.now().strftime("%Y-%m-%d")
        daily_trade_count = getattr(
            orc.risk_controller, "daily_trade_count", {})
        daily_trade_count_value = (
            daily_trade_count.get(today, 0)
            if isinstance(daily_trade_count, dict)
            else 0
        )
        # 🔥 收集错误避让状态
        backoff_exchanges = {}
        if hasattr(orc, 'error_backoff_controller') and orc.error_backoff_controller:
            backoff_exchanges = orc.error_backoff_controller.get_all_paused_exchanges()

        risk_status_data = {
            "is_paused": risk_status.is_paused,
            "pause_reason": risk_status.pause_reason,
            "network_failure": risk_status.network_failure,
            "exchange_maintenance": list(risk_status.exchange_maintenance),
            "low_balance_exchanges": list(risk_status.low_balance_exchanges),
            "critical_balance_exchanges": list(
                risk_status.critical_balance_exchanges
            ),
            "daily_trade_count": daily_trade_count_value,
            "daily_trade_limit": 0,
            "backoff_exchanges": backoff_exchanges,  # 🔥 新增：错误避让状态
        }

        # 🔥 UI展示：强平失败锁存（仅展示，不影响业务逻辑）
        try:
            latched = bool(getattr(orc, "_force_close_failed_latched", False))
            reason = getattr(orc, "_force_close_failed_latch_reason", None)
            risk_status_data["force_close_latch"] = {
                "latched": latched,
                "reason": (str(reason) if reason else None),
            }
        except Exception:
            pass

        # 🔥 UI展示：Paradex 局部风控状态（不影响全局 paused，仅展示）
        try:
            prc = getattr(orc, "_paradex_risk_controller", None)
            if prc and hasattr(prc, "get_status_snapshot"):
                risk_status_data["paradex_local_risk"] = prc.get_status_snapshot()
        except Exception:
            pass

        # 🔥 open_only_close（只允许平仓）属于“开仓闸门”，不会体现在 is_paused。
        # 这里仅做 UI 展示：对配置交易所两两组合做一次判断，展示“是否触发/原因”。
        try:
            guard = getattr(orc, "_open_only_close_guard", None)
            exchanges = list(getattr(orc.monitor_config, "exchanges", []) or [])
            blocks: List[Dict[str, Any]] = []
            if guard and len(exchanges) >= 2:
                # 1) symbol 维度的开仓闸门（价格波动幅度）
                #    放在最前面：避免被“权益/持仓价值”原因遮住
                try:
                    symbols: List[str] = []
                    mx_syms = getattr(orc, "multi_exchange_symbols", None)
                    if isinstance(mx_syms, set):
                        symbols = sorted([str(s).upper() for s in mx_syms if s])
                    if not symbols:
                        symbols = sorted([str(s).upper() for s in (getattr(orc.monitor_config, "symbols", []) or []) if s])

                    for sym in symbols:
                        b2, r2, t2 = guard.get_volatility_block_status(sym) if hasattr(guard, "get_volatility_block_status") else (False, None, None)
                        if b2:
                            blocks.append(
                                {
                                    "pair": sym,
                                    "block_type": t2,
                                    "reason": r2,
                                }
                            )
                except Exception:
                    pass

                # 2) exchange 维度的开仓闸门（权益/持仓价值）
                for ex_a, ex_b in combinations(exchanges, 2):
                    blocked, reason, block_type = guard.should_block_open(
                        exchange_buy=ex_a, exchange_sell=ex_b
                    )
                    if blocked:
                        blocks.append(
                            {
                                "pair": f"{ex_a.upper()}↔{ex_b.upper()}",
                                "block_type": block_type,
                                "reason": reason,
                            }
                        )

                enabled_any = bool(
                    getattr(getattr(guard, "_cfg", None), "low_balance", None)
                    and getattr(guard._cfg.low_balance, "enabled", False)
                ) or bool(
                    getattr(getattr(guard, "_cfg", None), "max_exchange_position_value", None)
                    and getattr(guard._cfg.max_exchange_position_value, "enabled", False)
                ) or bool(
                    getattr(getattr(guard, "_cfg", None), "dynamic_max_exchange_position_value", None)
                    and getattr(guard._cfg.dynamic_max_exchange_position_value, "enabled", False)
                ) or bool(
                    getattr(getattr(guard, "_cfg", None), "price_volatility", None)
                    and getattr(guard._cfg.price_volatility, "enabled", False)
                )

                risk_status_data["open_only_close"] = {
                    "enabled_any": enabled_any,
                    "triggered": bool(blocks),
                    "triggered_count": len(blocks),
                    "checked_pairs": max(0, len(exchanges) * (len(exchanges) - 1) // 2),
                    # 交给 UI 自行决定展示数量（UI 会做截断并显示“+N条”）
                    "blocks": blocks,
                }
        except Exception:
            # UI 展示用：失败则忽略，避免影响主流程
            pass

        position_rows = self._build_position_rows()
        local_position_rows = self._build_local_position_rows()
        exchange_position_cache = self._collect_exchange_position_cache()

        # 🔥 UI展示：每个代币的“单格数量(base_quantity)”映射，用于计算“当前持仓=多少格”
        # 说明：
        # - 仅用于终端 UI 展示，不参与任何交易逻辑
        # - 以 SegmentedConfigManager 的 quantity_config.base_quantity 为准
        base_quantity_by_token: Dict[str, str] = {}
        try:
            cfg_mgr = getattr(orc, "config_manager", None)
            ui_mgr = getattr(orc, "ui_manager", None)
            if cfg_mgr and ui_mgr and hasattr(cfg_mgr, "symbol_configs"):
                for sym_key, sym_cfg in (cfg_mgr.symbol_configs or {}).items():
                    try:
                        token = ui_mgr._normalize_token_key(str(sym_key))
                        base_qty = getattr(getattr(sym_cfg, "quantity_config", None), "base_quantity", None)
                        if not token or token == "-" or base_qty is None:
                            continue
                        # 以字符串保存，避免 Decimal/对象序列化问题
                        base_quantity_by_token[str(token).upper()] = str(base_qty)
                    except Exception:
                        continue
        except Exception:
            base_quantity_by_token = {}

        alignment_ui_data = None
        if hasattr(orc, "get_alignment_ui_data"):
            alignment_ui_data = orc.get_alignment_ui_data()

        await self._update_account_balances_ui()
        balance_cache = dict(self._balance_cache)

        orderbook_data = ticker_data = symbol_spreads = None
        if should_refresh_market_data:
            (
                orderbook_data,
                ticker_data,
                symbol_spreads,
            ) = self._collect_ui_market_data()

        if hasattr(orc, "get_execution_records_snapshot"):
            exec_records = orc.get_execution_records_snapshot()
        else:
            exec_records = list(getattr(orc, "execution_records", []) or [])

        multi_leg_rows = None
        if orc.multi_leg_pairs:
            multi_leg_rows = self._collect_multi_leg_data()

        grid_ui_data = None
        if orc.ui_manager.ui_mode == UIMode.SEGMENTED_GRID:
            grid_ui_data = self._collect_grid_ui_data()

        snapshot = {
            "stats": stats_snapshot,
            "risk_status": risk_status_data,
            "positions": position_rows,
            "local_positions": local_position_rows,
            "exchange_position_cache": exchange_position_cache,
            "base_quantity_by_token": base_quantity_by_token,
            "alignment": alignment_ui_data,
            "balance_cache": balance_cache,
            "should_refresh_market_data": should_refresh_market_data,
            "orderbook_data": orderbook_data,
            "ticker_data": ticker_data,
            "symbol_spreads": symbol_spreads,
            "exec_records": exec_records,
            "multi_leg_rows": multi_leg_rows,
            "grid_ui_data": grid_ui_data,
            "timestamp": current_time,
        }
        return snapshot

    async def _render_ui_snapshot(self, snapshot: Dict[str, Any]) -> None:
        """使用快照数据渲染 UI（独立协程，避免阻塞采集）。"""
        orc = self._orc
        try:
            orc.ui_manager.update_stats(snapshot["stats"])
            orc.ui_manager.update_risk_status(snapshot["risk_status"])
            orc.ui_manager.update_positions(snapshot["positions"])
            orc.ui_manager.update_local_positions(snapshot["local_positions"])
            orc.ui_manager.update_exchange_position_cache(
                snapshot["exchange_position_cache"]
            )
            try:
                orc.ui_manager.update_base_quantity_by_token(
                    snapshot.get("base_quantity_by_token") or {}
                )
            except Exception:
                pass
            orc.ui_manager.update_alignment_summary(snapshot["alignment"])

            if snapshot["balance_cache"]:
                orc.ui_manager.account_balances = snapshot["balance_cache"]

            if snapshot["should_refresh_market_data"] and snapshot["orderbook_data"] is not None:
                orc.ui_manager.update_orderbook_data(
                    snapshot["orderbook_data"],
                    ticker_data=snapshot["ticker_data"],
                    symbol_spreads=snapshot["symbol_spreads"],
                )
                self.last_ui_update_time = snapshot["timestamp"]

            orc.ui_manager.update_opportunities([])
            orc.ui_manager.update_execution_records(snapshot["exec_records"])

            if snapshot["multi_leg_rows"] is not None:
                orc.ui_manager.update_multi_leg_pairs(
                    snapshot["multi_leg_rows"])

            if snapshot["grid_ui_data"] is not None:
                grid_ui_data = snapshot["grid_ui_data"]
                orc.ui_manager.update_grid_data(
                    grid_data={"segments": grid_ui_data["segments"]},
                    current_spread_pct=grid_ui_data["current_spread_pct"],
                    spread_duration=grid_ui_data["spread_duration"],
                    spread_trend=grid_ui_data["spread_trend"],
                    next_action=grid_ui_data["next_action"],
                )
        except Exception as exc:
            self._logger.debug(f"[统一调度] UI渲染失败: {exc}")

    def update_ui(self):
        """同步更新一次 UI（历史函数保留调用入口）。"""
        orc = self._orc
        try:
            stats = self._collect_stats()
            orc.ui_manager.update_stats(stats)

            orderbook_data = orc.data_processor.get_all_orderbooks()
            ticker_data = orc.data_processor.get_all_tickers()
            orc.ui_manager.update_orderbook_data(
                orderbook_data,
                ticker_data=ticker_data,
                symbol_spreads=orc.symbol_spreads,
            )

            positions = []
            for symbol in orc.monitor_config.symbols:
                position = orc.decision_engine.get_position(symbol)
                if position:
                    positions.append(
                        {
                            "symbol": symbol,
                            "quantity": float(position.total_quantity),
                            "avg_spread": position.avg_open_spread_pct,
                            "segments": len(position.segments),
                            "is_scalping": orc.decision_engine.is_scalping_active(
                                symbol
                            ),
                        }
                    )
            orc.ui_manager.update_positions(positions)

            orc.ui_manager.update_risk_status(
                orc.risk_controller.get_risk_status().__dict__
            )
            orc.ui_manager.update_opportunities([])
        except Exception as exc:
            self._logger.error(f"❌ [统一调度] UI更新异常: {exc}")

    async def update_comprehensive(self) -> None:
        """对外公开的完整 UI 刷新入口。"""
        await self._update_ui_comprehensive()

    def _collect_stats(self) -> Dict:
        orc = self._orc
        processor_stats = orc.data_processor.get_stats()
        receiver_stats = orc.data_receiver.get_stats()

        stats = {
            **processor_stats,
            "network_bytes_received": receiver_stats.get(
                "network_bytes_received", 0
            ),
            "network_bytes_sent": receiver_stats.get("network_bytes_sent", 0),
            "reconnect_stats": receiver_stats.get("reconnect_stats", {}),
            "exchanges": list(orc.exchange_adapters.keys()),
            "symbols_count": len(orc.monitor_config.symbols),
            "ui_update_interval": self.ui_update_interval,
        }
        return stats

    def _collect_grid_ui_data(self) -> Dict[str, Any]:
        orc = self._orc
        grid_data = {
            "segments": [],
            "current_spread_pct": 0.0,
            "spread_duration": 0.0,
            "spread_trend": "-",
            "next_action": "等待价差触发条件",
        }

        try:
            if not orc.monitor_config.symbols:
                return grid_data

            symbol = orc.monitor_config.symbols[0]

            if (
                hasattr(orc.decision_engine, "grid_configs")
                and symbol in orc.decision_engine.grid_configs
            ):
                grid_config = orc.decision_engine.grid_configs[symbol]

                for i, segment_size in enumerate(grid_config.segment_quantities):
                    level = f"T{i}"
                    open_threshold = (
                        grid_config.min_profit_pct +
                        (i * grid_config.grid_step)
                    )
                    if i == 0:
                        # 🔥 T0 = T1 * 0.4（40%）
                        close_threshold = grid_config.min_profit_pct * 0.4
                    else:
                        close_threshold = (
                            grid_config.min_profit_pct
                            + ((i - 1) * grid_config.grid_step)
                        )

                    actual_quantity = Decimal("0")
                    if (
                        hasattr(orc.decision_engine, "positions")
                        and symbol in orc.decision_engine.positions
                    ):
                        for seg in orc.decision_engine.positions[symbol]:
                            if seg.grid_level == i:
                                actual_quantity += seg.size

                    grid_data["segments"].append(
                        {
                            "level": level,
                            "open_threshold": float(open_threshold),
                            "close_threshold": float(close_threshold),
                            "target_quantity": float(segment_size),
                            "actual_quantity": float(actual_quantity),
                        }
                    )

            if symbol in orc.symbol_spreads and orc.symbol_spreads[symbol]:
                latest_spread = orc.symbol_spreads[symbol][-1]
                grid_data["current_spread_pct"] = float(
                    latest_spread.spread_pct)

                if len(orc.symbol_spreads[symbol]) >= 2:
                    prev_spread = orc.symbol_spreads[symbol][-2].spread_pct
                    curr_spread = latest_spread.spread_pct
                    if curr_spread > prev_spread:
                        grid_data["spread_trend"] = "↗"
                    elif curr_spread < prev_spread:
                        grid_data["spread_trend"] = "↘"
                    else:
                        grid_data["spread_trend"] = "→"

            if (
                hasattr(orc.decision_engine, "_spread_start_time")
                and symbol in orc.decision_engine._spread_start_time
            ):
                start_time = orc.decision_engine._spread_start_time[symbol]
                if start_time:
                    grid_data["spread_duration"] = time.time() - start_time

            if hasattr(orc.decision_engine, "should_open"):
                should_open, quantity, reason = orc.decision_engine.should_open(
                    symbol)
                if should_open:
                    remaining_time = max(0, 3.0 - grid_data["spread_duration"])
                    grid_data["next_action"] = f"等待{remaining_time:.1f}s后开仓"
                elif hasattr(orc.decision_engine, "should_close"):
                    should_close, quantity, reason, _ = orc.decision_engine.should_close(
                        symbol
                    )
                    if should_close:
                        remaining_time = max(
                            0, 3.0 - grid_data["spread_duration"])
                        grid_data["next_action"] = f"等待{remaining_time:.1f}s后平仓"
                    else:
                        grid_data["next_action"] = reason or "等待价差触发条件"

        except Exception as exc:
            self._logger.debug(f"[统一调度] 收集网格UI数据失败: {exc}")

        return grid_data

    def _collect_multi_leg_data(self) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        orc = self._orc

        try:
            orderbooks: Dict[Tuple[str, str], OrderBookData] = {}

            for pair in orc.multi_leg_pairs:
                leg1_symbol = pair.leg_primary.normalized_symbol()
                leg1_exchange = pair.leg_primary.normalized_exchange()
                leg1_ob = orc.data_processor.get_orderbook(
                    leg1_exchange,
                    leg1_symbol,
                    max_age_seconds=orc.get_data_freshness_seconds(leg1_exchange),
                )
                if leg1_ob:
                    orderbooks[(leg1_exchange, leg1_symbol)] = leg1_ob

                leg2_symbol = pair.leg_secondary.normalized_symbol()
                leg2_exchange = pair.leg_secondary.normalized_exchange()
                leg2_ob = orc.data_processor.get_orderbook(
                    leg2_exchange,
                    leg2_symbol,
                    max_age_seconds=orc.get_data_freshness_seconds(leg2_exchange),
                )
                if leg2_ob:
                    orderbooks[(leg2_exchange, leg2_symbol)] = leg2_ob

                spreads = orc.spread_calculator.calculate_multi_leg_spread(
                    pair_id=pair.pair_id,
                    leg_primary_exchange=leg1_exchange,
                    leg_primary_symbol=leg1_symbol,
                    leg_secondary_exchange=leg2_exchange,
                    leg_secondary_symbol=leg2_symbol,
                    orderbooks=orderbooks,
                    allow_reverse=pair.allow_reverse,
                )

                if spreads:
                    best_spread = max(spreads, key=lambda s: s.spread_pct)

                    # 🔥 计算平仓方向的价差（反向）
                    closing_spread = orc.spread_calculator.calculate_multi_leg_closing_spread(
                        pair_id=pair.pair_id,
                        leg_primary_exchange=leg1_exchange,
                        leg_primary_symbol=leg1_symbol,
                        leg_secondary_exchange=leg2_exchange,
                        leg_secondary_symbol=leg2_symbol,
                        orderbooks=orderbooks,
                        opening_direction=best_spread,
                    )

                    # 🔥 使用 best_spread 中的交易所和交易对信息（已经是最优方向）
                    buy_exchange = best_spread.exchange_buy.upper()
                    sell_exchange = best_spread.exchange_sell.upper()
                    buy_symbol = best_spread.buy_symbol or pair.leg_primary.normalized_symbol()
                    sell_symbol = best_spread.sell_symbol or pair.leg_secondary.normalized_symbol()

                    # 🔥 重要：根据 best_spread 的方向，确定哪条腿是买入，哪条腿是卖出
                    # best_spread.buy_symbol 告诉我们买入的是哪条腿
                    if best_spread.buy_symbol.upper() == leg1_symbol.upper():
                        # 买leg1，卖leg2
                        buying_leg_exchange = leg1_exchange
                        buying_leg_symbol = leg1_symbol
                        selling_leg_exchange = leg2_exchange
                        selling_leg_symbol = leg2_symbol
                    else:
                        # 买leg2，卖leg1
                        buying_leg_exchange = leg2_exchange
                        buying_leg_symbol = leg2_symbol
                        selling_leg_exchange = leg1_exchange
                        selling_leg_symbol = leg1_symbol

                    # 🔥 获取买入腿和卖出腿的完整价格信息（Ask 和 Bid）
                    buying_leg_ask = None
                    buying_leg_bid = None
                    buying_leg_ask_size = None
                    buying_leg_bid_size = None
                    selling_leg_ask = None
                    selling_leg_bid = None
                    selling_leg_ask_size = None
                    selling_leg_bid_size = None

                    buying_leg_key = (buying_leg_exchange, buying_leg_symbol)
                    selling_leg_key = (selling_leg_exchange,
                                       selling_leg_symbol)

                    if buying_leg_key in orderbooks:
                        buying_leg_ob = orderbooks[buying_leg_key]
                        if buying_leg_ob.best_ask:
                            buying_leg_ask = float(
                                buying_leg_ob.best_ask.price)
                            buying_leg_ask_size = float(
                                buying_leg_ob.best_ask.size)
                        if buying_leg_ob.best_bid:
                            buying_leg_bid = float(
                                buying_leg_ob.best_bid.price)
                            buying_leg_bid_size = float(
                                buying_leg_ob.best_bid.size)

                    if selling_leg_key in orderbooks:
                        selling_leg_ob = orderbooks[selling_leg_key]
                        if selling_leg_ob.best_ask:
                            selling_leg_ask = float(
                                selling_leg_ob.best_ask.price)
                            selling_leg_ask_size = float(
                                selling_leg_ob.best_ask.size)
                        if selling_leg_ob.best_bid:
                            selling_leg_bid = float(
                                selling_leg_ob.best_bid.price)
                            selling_leg_bid_size = float(
                                selling_leg_ob.best_bid.size)

                    rows.append(
                        {
                            "pair_id": pair.pair_id,
                            "description": pair.description or "",
                            "enabled": pair.enabled,
                            "buy_exchange": buy_exchange,
                            "buy_symbol": buy_symbol,
                            "sell_exchange": sell_exchange,
                            "sell_symbol": sell_symbol,
                            "buy_price": float(best_spread.price_buy),
                            "sell_price": float(best_spread.price_sell),
                            "buy_size": float(best_spread.size_buy),
                            "sell_size": float(best_spread.size_sell),
                            "spread_pct": best_spread.spread_pct,
                            "closing_spread_pct": closing_spread.spread_pct if closing_spread else None,
                            "min_spread_pct": pair.min_spread_pct or 0.0,
                            "status": "✅"
                            if best_spread.spread_pct >= (pair.min_spread_pct or 0.0)
                            else "⏸️",
                            # 🔥 新增：买入腿和卖出腿的完整价格信息（根据最优方向）
                            "buying_leg_exchange": buying_leg_exchange.upper(),
                            "buying_leg_symbol": buying_leg_symbol,
                            "buying_leg_ask": buying_leg_ask,
                            "buying_leg_bid": buying_leg_bid,
                            "buying_leg_ask_size": buying_leg_ask_size,
                            "buying_leg_bid_size": buying_leg_bid_size,
                            "selling_leg_exchange": selling_leg_exchange.upper(),
                            "selling_leg_symbol": selling_leg_symbol,
                            "selling_leg_ask": selling_leg_ask,
                            "selling_leg_bid": selling_leg_bid,
                            "selling_leg_ask_size": selling_leg_ask_size,
                            "selling_leg_bid_size": selling_leg_bid_size,
                        }
                    )
                else:
                    rows.append(
                        {
                            "pair_id": pair.pair_id,
                            "description": pair.description or "",
                            "enabled": pair.enabled,
                            "buy_exchange": pair.leg_primary.normalized_exchange().upper(),
                            "buy_symbol": pair.leg_primary.normalized_symbol(),
                            "sell_exchange": pair.leg_secondary.normalized_exchange().upper(),
                            "sell_symbol": pair.leg_secondary.normalized_symbol(),
                            "buy_price": None,
                            "sell_price": None,
                            "buy_size": None,
                            "sell_size": None,
                            "spread_pct": 0.0,
                            "closing_spread_pct": None,
                            "min_spread_pct": pair.min_spread_pct or 0.0,
                            "status": "⏸️",
                        }
                    )
        except Exception as exc:
            self._logger.error(f"❌ [统一调度] 收集多腿套利数据失败: {exc}", exc_info=True)

        return rows

    def _collect_exchange_position_cache(self) -> Dict[str, Dict[str, Dict]]:
        exchange_cache: Dict[str, Dict[str, Dict]] = {}
        try:
            for exchange_name, adapter in self._orc.exchange_adapters.items():
                position_cache = getattr(adapter, "_position_cache", None)
                if position_cache is None:
                    position_cache = getattr(adapter, "_positions", None)
                if position_cache is None:
                    continue
                cleaned_positions: Dict[str, Dict[str, Any]] = {}

                for symbol, entry in position_cache.items():
                    try:
                        size, side, entry_price, timestamp = self._extract_position_payload(
                            entry
                        )
                        cleaned_positions[symbol] = {
                            "symbol": symbol,
                            "size": float(size),
                            "side": side,
                            "entry_price": float(entry_price),
                            "timestamp": timestamp,
                        }
                    except Exception:
                        self._logger.debug(
                            "[统一调度] 无法解析 %s 持仓缓存类型: %s",
                            exchange_name,
                            type(entry),
                        )

                exchange_cache[exchange_name] = cleaned_positions
        except Exception as exc:
            self._logger.debug(f"[统一调度] 收集交易所持仓缓存失败: {exc}")
        return exchange_cache

    def _build_position_rows(self) -> List[Dict]:
        position_rows: List[Dict] = []
        pair_positions = getattr(
            self._orc.decision_engine, "pair_positions", None)

        def _weighted_price(position: SegmentedPosition, attr: str) -> float:
            numerator = Decimal("0")
            denominator = Decimal("0")
            for seg in position.get_open_segments():
                price = getattr(seg, attr, None)
                if price is None:
                    continue
                qty = seg.open_quantity
                if qty <= Decimal("0"):
                    continue
                numerator += Decimal(str(price)) * qty
                denominator += qty
            if denominator <= Decimal("0"):
                return 0.0
            return float(numerator / denominator)

        if pair_positions:
            for symbol, pair_map in pair_positions.items():
                for pair_position in pair_map.values():
                    if not pair_position or not pair_position.is_open:
                        continue
                    total_qty = float(pair_position.total_quantity)
                    if total_qty <= 0:
                        continue
                    open_segments = pair_position.get_open_segments()
                    open_time = (
                        open_segments[-1].open_time if open_segments else pair_position.create_time
                    )
                    position_rows.append(
                        {
                            "symbol": f"{pair_position.exchange_buy}↔{pair_position.exchange_sell}",
                            "exchange_buy": pair_position.exchange_buy,
                            "exchange_sell": pair_position.exchange_sell,
                            "buy_symbol": pair_position.buy_symbol or pair_position.symbol,
                            "sell_symbol": pair_position.sell_symbol or pair_position.symbol,
                            "quantity_buy": total_qty,
                            "quantity_sell": total_qty,
                            "open_spread_pct": pair_position.avg_open_spread_pct,
                            "open_time": open_time.strftime("%m-%d %H:%M")
                            if open_time
                            else "N/A",
                            "open_mode": "Grid",
                            "open_price_buy": _weighted_price(pair_position, "open_price_buy"),
                            "open_price_sell": _weighted_price(pair_position, "open_price_sell"),
                        }
                    )
            return position_rows

        # 回退逻辑：尚未建立pair级别数据时，使用实时缓存估算
        for exchange_name, adapter in self._orc.exchange_adapters.items():
            if not hasattr(adapter, "_position_cache"):
                continue
            position_cache = adapter._position_cache
            if not position_cache:
                continue

            for symbol, position_info in position_cache.items():
                existing_row = next(
                    (row for row in position_rows if row["symbol"]
                     == symbol), None
                )
                size, side, entry_price, timestamp = self._extract_position_payload(
                    position_info
                )

                if existing_row:
                    if side == "long":
                        existing_row["exchange_buy"] = exchange_name
                        existing_row["quantity_buy"] = float(abs(size))
                        existing_row["open_price_buy"] = float(entry_price)
                    else:
                        existing_row["exchange_sell"] = exchange_name
                        existing_row["quantity_sell"] = float(abs(size))
                        existing_row["open_price_sell"] = float(entry_price)
                else:
                    position_rows.append(
                        {
                            "symbol": symbol,
                            "exchange_buy": exchange_name if side == "long" else "",
                            "exchange_sell": exchange_name if side == "short" else "",
                            "quantity_buy": float(abs(size)) if side == "long" else 0,
                            "quantity_sell": float(abs(size)) if side == "short" else 0,
                            "open_spread_pct": 0.0,
                            "open_time": timestamp.strftime("%m-%d %H:%M")
                            if timestamp
                            else "N/A",
                            "open_mode": "Real-time",
                            "open_price_buy": float(entry_price)
                            if side == "long"
                            else 0,
                            "open_price_sell": float(entry_price)
                            if side == "short"
                            else 0,
                        }
                    )
        return position_rows

    def _build_local_position_rows(self) -> List[Dict]:
        local_positions: List[Dict] = []
        orc = self._orc
        state_manager = getattr(orc, "symbol_state_manager", None)

        def _calculate_weighted_price(
            segments: List[PositionSegment], price_attr: str
        ) -> float:
            numerator = Decimal("0")
            denominator = Decimal("0")
            for seg in segments:
                price = getattr(seg, price_attr, None)
                if price is None:
                    continue
                qty = seg.open_quantity
                if qty <= Decimal("0"):
                    continue
                numerator += Decimal(price) * qty
                denominator += qty
            if denominator <= Decimal("0"):
                return 0.0
            return float(numerator / denominator)

        try:
            position_iterable: List[Tuple[str, Any]] = []
            pair_positions = getattr(
                orc.decision_engine, "pair_positions", None)
            if pair_positions:
                for symbol, pair_map in pair_positions.items():
                    for pair_position in pair_map.values():
                        position_iterable.append((symbol, pair_position))
            elif hasattr(orc.decision_engine, "positions"):
                position_iterable = list(orc.decision_engine.positions.items())
            else:
                return local_positions

            qty_epsilon = getattr(orc.decision_engine,
                                  "quantity_epsilon", Decimal("0"))

            for symbol, position in position_iterable:
                if not position or not position.is_open:
                    continue

                open_segments = position.get_open_segments()
                total_qty = sum(
                    (seg.open_quantity for seg in open_segments), Decimal("0")
                )
                # 无持仓则不展示在终端表格（避免平仓后残留空行）
                if total_qty <= qty_epsilon:
                    continue
                segment_count = position.get_segment_count()

                # 🔢 统计真实的开/平次数（以段为单位，拆单=1次）
                total_segment_executions = len(
                    getattr(position, "segments", []) or [])
                closed_segment_count = sum(
                    1
                    for seg in getattr(position, "segments", []) or []
                    if getattr(seg, "is_closed", False)
                )

                # ------------------------------------------------------------------
                # 🔥 UI“级别(Tn)”显示逻辑：支持小数（仅展示）
                #
                # 用户期望：当持仓处于“网格1~网格2之间”时，能显示 T1.50 这种中间态。
                # 口径与“交易所总持仓表”的网格数一致：grid_count = abs(quantity) / base_quantity
                # - 仅用于展示，不写回决策引擎，不影响策略逻辑
                # ------------------------------------------------------------------
                level = "-"
                try:
                    cfg = orc.config_manager.get_config(getattr(position, "symbol", symbol))
                    base_qty = getattr(getattr(cfg, "quantity_config", None), "base_quantity", None)
                    max_segments = getattr(getattr(cfg, "grid_config", None), "max_segments", None)
                    if isinstance(base_qty, Decimal) and base_qty > qty_epsilon:
                        grid_count = (abs(total_qty) / base_qty) if base_qty > 0 else Decimal("0")
                        # 上限保护：最多显示到 max_segments（避免异常数据刷屏）
                        if isinstance(max_segments, int) and max_segments > 0:
                            grid_count = min(grid_count, Decimal(str(max_segments)))
                        level = f"T{float(grid_count):.2f}"
                    else:
                        # base_quantity 不可用时：保守显示（有持仓就至少T1）
                        level = "T1.00"
                except Exception:
                    level = "T1.00"

                avg_buy_price = _calculate_weighted_price(
                    open_segments, "open_price_buy"
                )
                avg_sell_price = _calculate_weighted_price(
                    open_segments, "open_price_sell"
                )
                buy_symbol = position.buy_symbol or symbol
                sell_symbol = position.sell_symbol or symbol
                quantity_float = float(total_qty) if total_qty else 0.0

                symbol_label = symbol
                if getattr(position, "pair_key", ""):
                    symbol_label = f"{position.exchange_buy}↔{position.exchange_sell}"

                # 监控模式下的持仓是“决策引擎模拟持仓”，为避免误解在 UI 上显式标注
                if bool(getattr(orc, "monitor_only_mode", True)) and total_qty > Decimal("0"):
                    status_text = "🧪 模拟持仓"
                else:
                    status_text = "✓ 持仓" if total_qty > Decimal("0") else "- 空仓"
                wait_state = (
                    state_manager.get_state(symbol)
                    if state_manager else None
                )
                if wait_state and wait_state.status == "waiting":
                    status_text = self._format_wait_status(wait_state.reason)

                local_positions.append(
                    {
                        "raw_symbol": symbol.upper(),
                        "symbol": symbol_label,
                        "pair": f"{position.exchange_buy}↔{position.exchange_sell}",
                        "exchange_buy": position.exchange_buy,
                        "exchange_sell": position.exchange_sell,
                        "quantity": quantity_float,
                        "level": level,
                        "segment_count": segment_count,
                        "open_trade_count": total_segment_executions,
                        "close_trade_count": closed_segment_count,
                        "avg_spread": position.calculate_avg_spread(),
                        "create_time": position.create_time.strftime("%m-%d %H:%M"),
                        "last_update": position.last_update_time.strftime("%m-%d %H:%M"),
                        "status": status_text,
                        "legs": [
                            {
                                "role": "买入腿",
                                "exchange": position.exchange_buy,
                                "direction": "Long",
                                "symbol": buy_symbol,
                                "quantity": quantity_float,
                                "avg_price": avg_buy_price,
                            },
                            {
                                "role": "卖出腿",
                                "exchange": position.exchange_sell,
                                "direction": "Short",
                                "symbol": sell_symbol,
                                "quantity": quantity_float,
                                "avg_price": avg_sell_price,
                            },
                        ],
                    }
                )
        except Exception as exc:
            # 🔥 修复：将日志级别提升为 warning，方便排查 UI 显示问题
            self._logger.warning(f"⚠️ [统一调度] 构建本地持仓数据失败: {exc}", exc_info=True)

        return local_positions

    def _extract_position_payload(
        self, position_info: Any
    ) -> Tuple[float, str, float, Optional[datetime]]:
        """
        兼容 dict / PositionData / 任意对象的持仓结构，返回统一字段。
        """
        try:
            if isinstance(position_info, dict):
                size = float(position_info.get(
                    "size", position_info.get("quantity", 0)))
                side_raw = position_info.get("side")
                if side_raw is None or str(side_raw).strip() == "":
                    side = "long" if size >= 0 else "short"
                else:
                    side = str(side_raw).lower()
                entry_price = float(position_info.get(
                    "entry_price", position_info.get("price", 0)))
                timestamp = position_info.get("timestamp")
                return size, side, entry_price, timestamp

            from core.adapters.exchanges.models import PositionData

            if isinstance(position_info, PositionData):
                size = (
                    float(position_info.size)
                    if getattr(position_info, "size", None) is not None
                    else 0.0
                )
                if getattr(position_info, "side", None):
                    side_value = position_info.side.value
                else:
                    side_value = "long" if size >= 0 else "short"
                entry_price = (
                    float(position_info.entry_price)
                    if getattr(position_info, "entry_price", None) is not None
                    else 0.0
                )
                timestamp = getattr(position_info, "timestamp", None)
                return size, side_value.lower(), entry_price, timestamp

            # dataclass 或其他对象
            size = float(getattr(position_info, "size", getattr(
                position_info, "quantity", 0)) or 0)
            entry_price = float(
                getattr(position_info, "entry_price",
                        getattr(position_info, "price", 0))
                or 0
            )
            raw_side = getattr(position_info, "side", None)
            side_value = raw_side.value if hasattr(
                raw_side, "value") else raw_side
            if not side_value:
                side_value = "long" if size >= 0 else "short"
            timestamp = getattr(position_info, "timestamp", None)
            return size, str(side_value).lower(), entry_price, timestamp

        except Exception:
            return 0.0, "long", 0.0, None

    def _append_waiting_rows(
        self,
        local_positions: List[Dict],
        state_manager,
    ) -> None:
        """为处于等待状态且无持仓的套利对补充占位行。"""
        existing_symbols = {
            row.get("raw_symbol") for row in local_positions if row.get("raw_symbol")
        }
        waiting_states = state_manager.list_states()
        for symbol, state in waiting_states.items():
            if state.get("status") != "waiting":
                continue
            reason = state.get("reason", "等待中")
            pair_label = "-"
            exchange_buy = state.get("exchange_buy") or "-"
            exchange_sell = state.get("exchange_sell") or "-"
            if exchange_buy != "-" or exchange_sell != "-":
                pair_label = f"{exchange_buy}↔{exchange_sell}"
            status_text = self._format_wait_status(reason)
            grid_level = state.get("grid_level")
            level_text = f"T{grid_level}" if grid_level is not None else "-"

            if symbol in existing_symbols:
                for row in local_positions:
                    if row.get("raw_symbol") == symbol:
                        row["status"] = status_text
                continue

            local_positions.append(
                {
                    "raw_symbol": symbol,
                    "symbol": symbol,
                    "pair": pair_label,
                    "exchange_buy": exchange_buy,
                    "exchange_sell": exchange_sell,
                    "quantity": 0.0,
                    "level": level_text,
                    "segment_count": 0,
                    "avg_spread": 0.0,
                    "create_time": "-",
                    "last_update": "-",
                    "status": status_text,
                    "legs": [
                        {
                            "role": "买入腿",
                            "exchange": exchange_buy,
                            "direction": "Long",
                            "symbol": symbol,
                            "quantity": 0.0,
                            "avg_price": 0.0,
                        },
                        {
                            "role": "卖出腿",
                            "exchange": exchange_sell,
                            "direction": "Short",
                            "symbol": symbol,
                            "quantity": 0.0,
                            "avg_price": 0.0,
                        },
                    ],
                }
            )

    def _format_wait_status(self, reason: Optional[str]) -> str:
        """
        压缩等待状态描述，避免在UI中换行。
        """
        if not reason:
            return "⏱ 等待"
        text = reason.strip()
        replacements = (
            ("订单", "单"),
            ("提交", "提"),
            ("失败", "失败"),
            ("等待", ""),
        )
        for old, new in replacements:
            text = text.replace(old, new)
        max_len = 20
        if len(text) > max_len:
            text = text[:max_len] + "…"
        return f"⏱ {text or '等待'}"

    async def _update_account_balances_ui(self):
        current_time = time.time()
        if (
            current_time - self.last_balance_update_time
            < self.balance_update_interval
        ):
            return

        self.last_balance_update_time = current_time
        account_balances: Dict[str, List[Dict[str, Any]]] = {}

        for exchange_name, adapter in self._orc.exchange_adapters.items():
            if not hasattr(adapter, "get_balances"):
                account_balances[exchange_name] = []
                continue

            try:
                balances = await adapter.get_balances()
                balance_list: List[Dict[str, Any]] = []
                for balance in balances:
                    source = "rest"
                    raw_data = getattr(balance, "raw_data", None)
                    if isinstance(raw_data, dict):
                        source = raw_data.get("source", "rest")

                    balance_list.append(
                        {
                            "currency": balance.currency,
                            "free": float(balance.free) if balance.free else 0.0,
                            "used": float(balance.used) if balance.used else 0.0,
                            "total": float(balance.total) if balance.total else 0.0,
                            "source": source,
                        }
                    )
                account_balances[exchange_name] = balance_list
            except Exception as exc:
                if not hasattr(self, "_balance_error_logged"):
                    self._balance_error_logged = set()
                error_key = f"{exchange_name}_{type(exc).__name__}"
                if error_key not in self._balance_error_logged:
                    self._balance_error_logged.add(error_key)
                    self._logger.warning(
                        f"[统一调度] 获取{exchange_name}余额失败: {exc}")
                account_balances[exchange_name] = self._balance_cache.get(
                    exchange_name, []
                )

        self._balance_cache = account_balances

    def _collect_ui_market_data(
        self,
    ) -> Tuple[
        Dict[str, Dict[str, OrderBookData]],
        Dict[str, Dict[str, object]],
        Dict[str, List],
    ]:
        orc = self._orc
        orderbook_data: Dict[str, Dict[str, OrderBookData]] = {}
        ticker_data: Dict[str, Dict[str, object]] = {}
        symbol_orderbooks: Dict[str, Dict[str, OrderBookData]] = {}

        # 🔥 收集需要查询盘口的所有符号（包括持仓中的 buy_symbol 和 sell_symbol）
        symbols_to_fetch = set(s.upper() for s in orc.monitor_config.symbols)

        # 🔥 添加持仓中涉及的符号（多腿套利场景）
        pair_positions = getattr(orc.decision_engine, "pair_positions", {})
        for pair_map in pair_positions.values():
            for pair_position in pair_map.values():
                if pair_position.buy_symbol:
                    symbols_to_fetch.add(pair_position.buy_symbol.upper())
                if pair_position.sell_symbol:
                    symbols_to_fetch.add(pair_position.sell_symbol.upper())

        for exchange_name in orc.monitor_config.exchanges:
            exchange_orderbooks: Dict[str, OrderBookData] = {}
            exchange_tickers: Dict[str, object] = {}

            for symbol_upper in symbols_to_fetch:
                orderbook = orc.data_processor.get_orderbook(
                    exchange_name,
                    symbol_upper,
                    max_age_seconds=orc.get_data_freshness_seconds(exchange_name),
                )
                if orderbook:
                    exchange_orderbooks[symbol_upper] = orderbook
                    symbol_orderbooks.setdefault(symbol_upper, {})[
                        exchange_name] = orderbook

                ticker = orc.data_processor.get_ticker(
                    exchange_name, symbol_upper)
                if ticker:
                    exchange_tickers[symbol_upper] = ticker
                    pair_key = (exchange_name, symbol_upper)
                    if pair_key in orc._missing_ticker_logged:
                        orc._missing_ticker_logged.remove(pair_key)
                    if not hasattr(orc, "_ticker_found_logged"):
                        orc._ticker_found_logged = set()
                    if pair_key not in orc._ticker_found_logged:
                        orc._ticker_found_logged.add(pair_key)
                        self._logger.info(
                            "✅ [统一调度] 找到ticker数据: %s %s, funding_rate=%s",
                            exchange_name,
                            symbol_upper,
                            getattr(ticker, "funding_rate", None),
                        )
                else:
                    pair_key = (exchange_name, symbol_upper)
                    if pair_key not in orc._missing_ticker_logged:
                        available_tickers = orc.data_processor.get_all_tickers()
                        self._logger.warning(
                            "⚠️ [统一调度] 未找到 %s %s 的Ticker数据，资金费率无法显示。可用的tickers: %s",
                            exchange_name,
                            symbol_upper,
                            list(available_tickers.get(
                                exchange_name, {}).keys())
                            if available_tickers
                            else [],
                        )
                        orc._missing_ticker_logged.add(pair_key)

            if exchange_orderbooks:
                orderbook_data[exchange_name] = exchange_orderbooks
            if exchange_tickers:
                ticker_data[exchange_name] = exchange_tickers

        symbol_spreads: Dict[str, List] = {}
        for symbol, orderbooks in symbol_orderbooks.items():
            if len(orderbooks) >= 2:
                spreads = orc.spread_calculator.calculate_spreads_multi_exchange_directions(
                    symbol,
                    orderbooks,
                )
                symbol_spreads[symbol] = spreads
            else:
                symbol_spreads[symbol] = []

        return orderbook_data, ticker_data, symbol_spreads
