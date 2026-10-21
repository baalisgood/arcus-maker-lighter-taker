"""
⚠️ 本文件由 `unified_orchestrator.py` 拆分生成（只做文件拆分，不改功能逻辑）。

职责：运行期生命周期与调度（start/stop/主循环、多腿/多交易所处理、紧急平仓入口、状态汇总心跳）。

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
from decimal import ROUND_UP
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
from core.adapters.exchanges.models import OrderBookData, PositionData, PositionSide

from .unified_orchestrator_logging import logger


class UnifiedOrchestratorRuntimeMixin:
    """由 unified_orchestrator.py 拆分出的 mixin。"""

    def _startup_sanity_check_and_warn(self) -> None:
        """
        启动自检（只打印一次）：
        - 当多交易所套利配置未生效/缺配置时，用户常见感知是“行情有，但完全没信号、没套利日志”
        - 这里用 WARNING 级别给出明确提示（因为 unified_orchestrator 默认过滤 INFO）
        """
        if getattr(self, "_startup_sanity_checked", False):
            return
        self._startup_sanity_checked = True

        issues_found = 0

        # 1) 读取 monitor_config.symbols（只取启用的 symbol）
        try:
            monitor_symbols = [
                str(s).upper().strip()
                for s in (getattr(self.monitor_config, "symbols", None) or [])
                if isinstance(s, str) and str(s).strip()
            ]
        except Exception:
            monitor_symbols = []

        try:
            enabled_symbols = [
                s for s in monitor_symbols
                if bool(getattr(self.config_manager, "is_symbol_enabled", lambda _s: True)(s))
            ]
        except Exception:
            enabled_symbols = list(monitor_symbols)

        # 2) 多交易所配置：pairs + 文件是否 enabled
        pairs: List[TradingPair] = list(
            getattr(self, "multi_exchange_pairs", []) or [])
        cfg_path = getattr(
            getattr(self, "multi_exchange_config_manager", None), "config_path", None)
        cfg_enabled: Optional[bool] = None
        cfg_hint: str = "config/arbitrage/multi_exchange_arbitrage.yaml"
        try:
            if cfg_path:
                cfg_hint = str(cfg_path)
                path_obj = Path(cfg_path)
                if path_obj.exists():
                    raw = yaml.safe_load(
                        path_obj.read_text(encoding="utf-8")) or {}
                    if isinstance(raw, dict):
                        cfg_enabled = bool(raw.get("enabled", False))
        except Exception:
            # 自检失败不影响启动
            pass

        # 3) 若配置启用但未生成 pairs：给出明确提示
        if cfg_enabled is True and not pairs:
            issues_found += 1
            logger.warning(
                "🧭 [启动自检] 多交易所套利已启用但未生成任何交易所组合(pairs=0)。"
                "这会导致“行情有但没有套利信号/日志”。请检查 %s 的配置是否完整："
                "mode/center_exchange/counter_exchanges/symbols（或 many_to_many 的 exchanges/symbols）。",
                cfg_hint,
            )

        # 4) 若存在 pairs：检查是否缺少交易所适配器（缺腿会导致永远拿不到两边盘口）
        if pairs:
            required_exchanges = {
                p.normalized_exchange_a() for p in pairs
            } | {
                p.normalized_exchange_b() for p in pairs
            }
            try:
                monitor_exchanges = {
                    str(e).lower().strip()
                    for e in (getattr(self.monitor_config, "exchanges", None) or [])
                    if isinstance(e, str) and str(e).strip()
                }
            except Exception:
                monitor_exchanges = set()
            available_exchanges = set(
                (getattr(self, "exchange_adapters", None) or {}).keys())
            missing_exchanges = sorted(
                required_exchanges - available_exchanges)
            if missing_exchanges:
                issues_found += 1
                logger.warning(
                    "🧭 [启动自检] 多交易所套利需要交易所=%s，但当前仅初始化=%s。"
                    "缺少的交易所会导致对应组合永远无法计算价差/触发信号。"
                    "请检查统一配置(arbitrage_unified.yaml)或启动参数选择的交易所。",
                    ",".join(missing_exchanges),
                    ",".join(sorted(available_exchanges)
                             ) if available_exchanges else "-",
                )

            # 4.1) 配置了 pairs，但 monitor_config.exchanges 未包含所需交易所
            # 这会导致数据层不订阅/不缓存该交易所盘口，从而“行情有但没信号”
            missing_in_monitor = sorted(required_exchanges - monitor_exchanges)
            if missing_in_monitor:
                issues_found += 1
                logger.warning(
                    "🧭 [启动自检] 多交易所套利需要交易所=%s，但 monitor_config.exchanges=%s 未包含它们。"
                    "请检查 monitor_config（启动参数 --monitor-config 对应的文件）里 exchanges 列表。",
                    ",".join(missing_in_monitor),
                    ",".join(sorted(monitor_exchanges)
                             ) if monitor_exchanges else "-",
                )

        # 5) 对启用的 monitor symbols 做“覆盖检查”
        # - 当前架构下：多交易所组合由 spread_pipeline._process_single_trading_pair 驱动
        # - symbol 级 process_symbol 在 SpreadPipeline 中标注为废弃（误配时会表现为“完全无信号”）
        if enabled_symbols:
            covered_symbols = {p.normalized_symbol() for p in pairs}
            uncovered = sorted(set(enabled_symbols) - covered_symbols)
            if uncovered and not pairs:
                issues_found += 1
                logger.warning(
                    "🧭 [启动自检] 当前未加载任何 multi_exchange_pairs。"
                    "而本版本 symbol 级处理链路已废弃，以下启用的 symbols 可能不会产生套利信号：%s。"
                    "建议将这些 symbols 加入 %s 并启用，或在 monitor_config 中移除/禁用。",
                    ",".join(uncovered),
                    cfg_hint,
                )
            elif uncovered and pairs:
                issues_found += 1
                logger.warning(
                    "🧭 [启动自检] 以下启用的 symbols 未被多交易所套利配置覆盖：%s。"
                    "它们不会走多交易所套利链路；若你期望它们参与跨所套利，请将其加入 %s 的 symbols 列表。",
                    ",".join(uncovered),
                    cfg_hint,
                )

        # 6) 正常态也输出一条（便于确认自检确实执行过）
        if issues_found == 0:
            try:
                required_exchanges = {
                    p.normalized_exchange_a() for p in pairs
                } | {
                    p.normalized_exchange_b() for p in pairs
                } if pairs else set()
            except Exception:
                required_exchanges = set()
            try:
                monitor_exchanges = {
                    str(e).lower().strip()
                    for e in (getattr(self.monitor_config, "exchanges", None) or [])
                    if isinstance(e, str) and str(e).strip()
                }
            except Exception:
                monitor_exchanges = set()
            available_exchanges = set(
                (getattr(self, "exchange_adapters", None) or {}).keys())
            logger.warning(
                "🧭 [启动自检] OK: pairs=%s | symbols=%s/%s | exchanges(required/monitor/available)=%s/%s/%s | cfg=%s",
                len(pairs),
                len(enabled_symbols),
                len(monitor_symbols),
                ",".join(sorted(required_exchanges)
                         ) if required_exchanges else "-",
                ",".join(sorted(monitor_exchanges)
                         ) if monitor_exchanges else "-",
                ",".join(sorted(available_exchanges)
                         ) if available_exchanges else "-",
                cfg_hint,
            )

    async def _bootstrap_positions_from_exchanges_once(self, *, recovery: bool = False) -> None:
        """
        启动一次性“持仓对齐”：
        - 从交易所拉取当前真实持仓（仅一次）
        - 将“跨所两腿（long+short）”注入到决策引擎（不拆段，只注入总量）
        - 建立开仓记忆（open_direction），确保后续平仓/归一价差口径一致
        - 若快照为空/缺腿/不一致，可按配置重试，并在失败时进入“禁止开仓”安全状态（仅影响开仓）
        """
        sys_mode = self.config_manager.get_system_mode() or {}
        enabled = bool(sys_mode.get("bootstrap_positions_on_startup", False))
        if not enabled:
            # 显式禁用：不影响当前稳定流程
            self._bootstrap_positions_done = True
            self._bootstrap_positions_success = True
            self._bootstrap_positions_block_open = False
            return

        # 初始化状态（避免属性缺失）
        self._bootstrap_positions_done = False
        self._bootstrap_positions_success = False
        self._bootstrap_positions_block_open = recovery or bool(
            sys_mode.get("bootstrap_positions_block_open_until_success", True)
        )
        self._bootstrap_positions_last_error = None

        # 仅在多交易所套利开启时有意义
        pairs: List[TradingPair] = list(
            getattr(self, "multi_exchange_pairs", []) or [])
        if not pairs:
            self._bootstrap_positions_done = True
            self._bootstrap_positions_success = True
            self._bootstrap_positions_block_open = False
            return

        # 配置参数（全部可配置，默认偏安全）
        fetch_timeout_s = float(sys_mode.get(
            "bootstrap_positions_fetch_timeout_seconds", 30) or 30)
        retry_interval_s = float(sys_mode.get(
            "bootstrap_positions_retry_interval_seconds", 10) or 10)
        retry_max_attempts = int(sys_mode.get(
            "bootstrap_positions_retry_max_attempts", 5) or 0)
        empty_confirm_attempts = int(sys_mode.get(
            "bootstrap_positions_empty_confirm_attempts", 10) or 0)
        empty_confirm_interval_s = float(sys_mode.get(
            "bootstrap_positions_empty_confirm_interval_seconds", 1) or 1)
        block_on_mismatch = bool(sys_mode.get(
            "bootstrap_positions_block_open_on_mismatch", True))
        qty_tolerance = Decimal(
            str(sys_mode.get("bootstrap_positions_qty_tolerance", "0") or "0"))
        # 数量差容忍（用于判断“数量不一致”）：
        # - qty_tolerance == 0：关闭数量不一致检查（按用户期望：即使数量不一致也继续注入）
        # - qty_tolerance > 0：启用数量不一致检查（差值 > qty_tolerance 则判 mismatch）
        # 是否要求“方向记忆文件”必须存在/可恢复：
        # - 按用户最新确认：方向（买腿/卖腿）可由持仓直接推导，因此默认不要求 JSON 记忆文件。
        # - 若未来确实需要恢复“开仓时价差符号”等额外记忆，可再开启该开关。
        require_direction_memory = bool(sys_mode.get(
            "bootstrap_positions_require_direction_memory", False))

        # 需要查询的底层 symbol（标准格式，如 BTC-USDC-PERP）
        required_std_symbols = sorted(
            {p.normalized_symbol() for p in pairs if p.normalized_symbol()})
        print(
            f"🧩 [启动对齐] 正在核对余额、挂单和持仓：{', '.join(sorted(self.exchange_adapters))}；"
            f"每项读取超时={fetch_timeout_s:g}s，空仓需确认{max(1, empty_confirm_attempts)}轮。界面将在核对结束后启动。",
            flush=True,
        )

        # 复用 DataReceiver 的符号转换器（与行情订阅一致）
        converter = getattr(getattr(self, "data_receiver",
                            None), "symbol_converter", None)
        # 仅用于启动对齐失败时的诊断信息（避免默认刷屏）
        bootstrap_debug: Dict[str, Any] = {
            "required_std_symbols": list(required_std_symbols),
            "requested_symbols_by_exchange": {},
            "raw_positions_by_exchange": {},
            "net_map": {},
        }

        def _to_exchange_symbol(std_symbol: str, exchange: str) -> str:
            if not converter:
                return std_symbol
            try:
                return converter.convert_to_exchange(std_symbol, exchange)
            except Exception:
                return std_symbol

        def _to_std_symbol(ex_symbol: str, exchange: str) -> str:
            if not converter:
                return ex_symbol
            try:
                return converter.convert_from_exchange(ex_symbol, exchange)
            except Exception:
                return ex_symbol

        async def _fetch_all_positions_once() -> Tuple[Dict[Tuple[str, str], Decimal], Dict[Tuple[str, str], Dict[str, Any]], List[str]]:
            """
            Returns:
                net_map: (exchange, std_symbol)->net_qty (long=+ / short=-)
                leg_meta: (exchange, std_symbol)->{abs_qty, entry_price}
                errors: list[str]
            """
            net_map: Dict[Tuple[str, str], Decimal] = defaultdict(Decimal)
            # 记录各腿 entry_price（加权均价）
            leg_meta_acc: Dict[Tuple[str, str], Dict[str, Decimal]] = defaultdict(lambda: {
                "abs_qty": Decimal("0"),
                "price_sum": Decimal("0"),
            })
            errors: List[str] = []

            async def _fetch_one(exchange: str, adapter: ExchangeInterface) -> List[Any]:
                # 只查策略涉及的 symbol，减少启动开销；对 Lighter/Variational 可映射成 BTC/ETH 等
                ex_syms = [_to_exchange_symbol(s, exchange)
                           for s in required_std_symbols]
                try:
                    bootstrap_debug["requested_symbols_by_exchange"][exchange] = list(
                        ex_syms)
                except Exception:
                    pass
                try:
                    if getattr(adapter, "strict_order_state", False):
                        await asyncio.wait_for(self.risk_controller._validated_balance(adapter), timeout=fetch_timeout_s)
                        orders = await asyncio.wait_for(adapter.get_open_orders(), timeout=fetch_timeout_s)
                        if not isinstance(orders, list):
                            raise ValueError("启动挂单数据类型无效")
                        if orders:
                            raise ValueError(f"启动存在未终态挂单: count={len(orders)}")
                        uncertain = getattr(adapter, "_uncertain", None)
                        if uncertain:
                            raise ValueError(f"启动订单状态不确定: count={len(uncertain)}")
                    positions = await asyncio.wait_for(adapter.get_positions(symbols=ex_syms), timeout=fetch_timeout_s)
                    return self.risk_controller._validate_positions_snapshot(positions)
                except Exception as e:
                    raise e

            tasks: Dict[str, asyncio.Task] = {}
            for ex, adapter in (self.exchange_adapters or {}).items():
                exn = str(ex).lower()
                if not adapter:
                    continue
                tasks[exn] = asyncio.create_task(_fetch_one(exn, adapter))

            results: Dict[str, List[Any]] = {}
            try:
                for exn, t in tasks.items():
                    try:
                        results[exn] = await t
                    except Exception as e:
                        errors.append(f"{exn}: {type(e).__name__}: {e}")
                        results[exn] = []
            finally:
                for task in tasks.values():
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks.values(), return_exceptions=True)

            for exn, pos_list in results.items():
                # 记录原始返回（仅用于启动对齐失败诊断）
                try:
                    bootstrap_debug["raw_positions_by_exchange"][exn] = []
                except Exception:
                    pass
                for p in pos_list or []:
                    try:
                        sym_raw = getattr(p, "symbol", None)
                        if not sym_raw:
                            continue
                        std_sym = _to_std_symbol(
                            str(sym_raw), exn).upper().strip()
                        if not std_sym:
                            continue
                        side = getattr(p, "side", None)
                        size_raw = Decimal(str(getattr(p, "size", "0") or "0"))
                        if size_raw == Decimal("0"):
                            continue

                        # long=+ / short=- （尽量兼容不同交易所返回口径）
                        # - 优先使用 side（PositionSide.LONG/SHORT）
                        # - 若 side 缺失/异常，则回退使用 size 的符号（有些交易所 size 为 signed）
                        side_str = str(
                            getattr(side, "value", side) or "").lower()
                        if "long" in side_str:
                            sign = Decimal("1")
                        elif "short" in side_str:
                            sign = Decimal("-1")
                        else:
                            sign = Decimal("1") if size_raw > Decimal(
                                "0") else Decimal("-1")

                        size = abs(size_raw)
                        if size <= Decimal("0"):
                            continue
                        net_map[(exn, std_sym)] += sign * size

                        entry_price = Decimal(
                            str(getattr(p, "entry_price", "0") or "0"))
                        # abs_qty & price_sum 用于加权均价
                        key = (exn, std_sym)
                        leg_meta_acc[key]["abs_qty"] += size
                        leg_meta_acc[key]["price_sum"] += size * \
                            max(entry_price, Decimal("0"))

                        # 仅记录精简字段，避免日志过大
                        try:
                            bootstrap_debug["raw_positions_by_exchange"][exn].append({
                                "symbol_raw": str(sym_raw),
                                "symbol_std": str(std_sym),
                                "side": side_str,
                                "size": str(size),
                                "entry_price": str(entry_price),
                            })
                        except Exception:
                            pass
                    except Exception:
                        continue

            leg_meta: Dict[Tuple[str, str], Dict[str, Any]] = {}
            for key, acc in leg_meta_acc.items():
                abs_qty = acc.get("abs_qty", Decimal("0"))
                price_sum = acc.get("price_sum", Decimal("0"))
                avg_price = (
                    price_sum / abs_qty) if abs_qty > Decimal("0") else Decimal("0")
                leg_meta[key] = {"abs_qty": abs_qty, "entry_price": avg_price}
            # net_map 也做一份可序列化快照（仅用于失败诊断）
            try:
                bootstrap_debug["net_map"] = {
                    f"{ex}/{sym}": str(qty) for (ex, sym), qty in (net_map or {}).items()
                    if qty is not None
                }
            except Exception:
                pass
            return net_map, leg_meta, errors

        async def _maybe_confirm_empty_snapshot() -> Tuple[Dict[Tuple[str, str], Decimal], Dict[Tuple[str, str], Dict[str, Any]], List[str]]:
            print(f"🧩 [启动对齐] 第{attempt}次核对：账户快照第1/{max(1, empty_confirm_attempts)}轮（余额、挂单、持仓）", flush=True)
            net_map, leg_meta, errors = await _fetch_all_positions_once()
            has_any = any(abs(v) > Decimal("0") for v in net_map.values())
            if errors or has_any or empty_confirm_attempts <= 1:
                return net_map, leg_meta, errors

            # 全为空：在确认窗口内重复确认（避免 WS/缓存未就绪导致误判“无仓”）
            for confirmation in range(2, max(1, empty_confirm_attempts) + 1):
                print(f"🧩 [启动对齐] 快照暂无持仓，{empty_confirm_interval_s:g}s后核对第{confirmation}/{empty_confirm_attempts}轮", flush=True)
                await asyncio.sleep(empty_confirm_interval_s)
                print(f"🧩 [启动对齐] 第{attempt}次核对：账户快照第{confirmation}/{empty_confirm_attempts}轮（余额、挂单、持仓）", flush=True)
                net_map2, leg_meta2, errors2 = await _fetch_all_positions_once()
                if errors2:
                    return net_map2, leg_meta2, errors2
                has_any2 = any(abs(v) > Decimal("0")
                               for v in net_map2.values())
                if has_any2:
                    return net_map2, leg_meta2, errors
                net_map, leg_meta = net_map2, leg_meta2
            return net_map, leg_meta, errors

        attempt = 0

        while True:
            attempt += 1
            activity_before = getattr(self, "_last_open_close_activity_ts", 0.0)
            try:
                net_map, leg_meta, errors = await _maybe_confirm_empty_snapshot()
            except asyncio.CancelledError:
                print("⚠️ [启动对齐] 核对已取消，尚未完成账户确认。", flush=True)
                raise
            required_exchanges = {exchange for pair in pairs for exchange in (pair.normalized_exchange_a(), pair.normalized_exchange_b())}
            missing_exchanges = required_exchanges - {str(exchange).lower() for exchange in self.exchange_adapters}
            if errors or missing_exchanges:
                self._bootstrap_positions_success = False
                self._bootstrap_positions_block_open = True
                self._bootstrap_positions_last_error = f"账户读取失败: {errors}; 缺少交易所: {sorted(missing_exchanges)}"
                print(f"⚠️ [启动对齐] 第{attempt}次核对失败，禁止开仓：{self._bootstrap_positions_last_error}", flush=True)
                if retry_max_attempts and attempt >= retry_max_attempts:
                    break
                print(f"🧩 [启动对齐] {retry_interval_s:g}s后重试账户核对", flush=True)
                await asyncio.sleep(retry_interval_s)
                continue

            # Recovery must not add the same restored position twice or race a close.
            if recovery and (
                self._bootstrap_recovery_busy()
                or activity_before != getattr(self, "_last_open_close_activity_ts", 0.0)
            ):
                self._bootstrap_positions_last_error = "恢复核对期间存在持仓或交易活动，保留禁止开仓"
                self._bootstrap_positions_done = True
                return

            # 建立每个 trading_pair 的候选注入
            injected: List[Dict[str, Any]] = []
            incomplete: List[str] = []
            mismatched: List[str] = []

            # ----------------------------------------------------------------
            # one_to_many：按“非中心分配”生成 AB/AC，并用中心剩余校验一致性
            # ----------------------------------------------------------------
            mx_mgr = getattr(self, "multi_exchange_config_manager", None)
            mx_mode = ""
            center_ex = None
            counter_exs: List[str] = []
            try:
                mx_mode = mx_mgr.get_mode() if mx_mgr and hasattr(mx_mgr, "get_mode") else ""
                center_ex = mx_mgr.get_center_exchange() if mx_mgr and hasattr(
                    mx_mgr, "get_center_exchange") else None
                counter_exs = mx_mgr.get_counter_exchanges() if mx_mgr and hasattr(mx_mgr,
                                                                                   "get_counter_exchanges") else []
            except Exception:
                mx_mode = ""
                center_ex = None
                counter_exs = []
            one_to_many = (str(mx_mode).strip().lower() == "one_to_many" and bool(
                center_ex) and bool(counter_exs))

            # pair 索引： (std_symbol, {ex1,ex2}) -> TradingPair
            pair_index: Dict[Tuple[str, frozenset], TradingPair] = {}
            try:
                for p in pairs:
                    s = p.normalized_symbol()
                    a = p.normalized_exchange_a()
                    b = p.normalized_exchange_b()
                    if not s or not a or not b:
                        continue
                    pair_index[(s, frozenset({a, b}))] = p
            except Exception:
                pair_index = {}

            if one_to_many:
                A = str(center_ex).strip().lower()
                counters = [str(x).strip().lower()
                            for x in (counter_exs or []) if str(x).strip()]
                eps = getattr(self.decision_engine,
                              "quantity_epsilon", Decimal("0.00000001"))

                # 用于失败诊断（仅 mismatch 时输出）
                alloc_debug: Dict[str, Any] = {}

                for std_sym in required_std_symbols:
                    if not std_sym:
                        continue

                    qA = net_map.get((A, std_sym), Decimal("0"))
                    sum_qe = Decimal("0")
                    allocations: List[Dict[str, Any]] = []

                    for E in counters:
                        qE = net_map.get((E, std_sym), Decimal("0"))
                        if abs(qE) <= eps:
                            continue
                        sum_qe += qE
                        qty = abs(qE)
                        if qE > 0:
                            buy_ex, sell_ex = E, A
                        else:
                            buy_ex, sell_ex = A, E

                        tp = pair_index.get((std_sym, frozenset({A, E})))
                        if not tp:
                            mismatched.append(
                                f"{std_sym}:{A}<->{E}:pair_missing")
                            continue

                        allocations.append({
                            "pair": tp,
                            "qty": qty,
                            "buy_ex": buy_ex,
                            "sell_ex": sell_ex,
                        })

                    # 中心剩余一致性：qA 应等于 -sum(qE)
                    qA_expected = -sum_qe
                    residual = qA - qA_expected
                    if abs(residual) > max(qty_tolerance, self.decision_engine.quantity_epsilon):
                        mismatched.append(
                            f"{std_sym}:{A}:residual={str(residual)}")
                        try:
                            alloc_debug[std_sym] = {
                                "qA": str(qA),
                                "qA_expected": str(qA_expected),
                                "residual": str(residual),
                                "tolerance": str(qty_tolerance),
                                "allocations": [
                                    {
                                        "pair": a["pair"].trading_pair_id,
                                        "buy": a["buy_ex"],
                                        "sell": a["sell_ex"],
                                        "qty": str(a["qty"]),
                                    } for a in allocations
                                ],
                            }
                        except Exception:
                            pass

                    # 将 allocations 转为注入（先不注入，等确认无 mismatch）
                    try:
                        alloc_debug.setdefault(std_sym, {})
                    except Exception:
                        pass

                    # 可选：若未来开启 require_direction_memory，可在此处补充校验逻辑
                    # 当前默认不要求方向记忆文件存在。

                # 1对多：mismatch 统一按 block_on_mismatch 处理（与旧逻辑一致）
                if block_on_mismatch and mismatched:
                    try:
                        bootstrap_debug["one_to_many_alloc_debug"] = alloc_debug
                    except Exception:
                        pass
                    logger.warning(
                        "🧩 [启动对齐][detail] one_to_many_mismatch: mismatched=%s errors=%s debug=%s",
                        len(mismatched),
                        len(errors),
                        {"alloc": alloc_debug} if alloc_debug else {},
                    )
                    self._bootstrap_positions_last_error = (
                        f"one_to_many_mismatched={len(mismatched)} errors={len(errors)}"
                    )
                    self._bootstrap_positions_success = False
                    if self._bootstrap_positions_block_open:
                        self._bootstrap_positions_block_open = True
                    if retry_max_attempts and attempt >= retry_max_attempts:
                        logger.warning(
                            "🧩 [启动对齐] one_to_many 持仓不一致，已到最大重试次数：attempt=%s mismatched=%s errors=%s",
                            attempt,
                            len(mismatched),
                            len(errors),
                        )
                        break
                    logger.warning(
                        "🧩 [启动对齐] one_to_many 持仓不一致，等待重试：attempt=%s mismatched=%s errors=%s",
                        attempt,
                        len(mismatched),
                        len(errors),
                    )
                    print(f"⚠️ [启动对齐] 第{attempt}次持仓核对不一致，{retry_interval_s:g}s后重试", flush=True)
                    await asyncio.sleep(retry_interval_s)
                    continue

                # 无 mismatch：正式注入（按非中心分配）
                for std_sym in required_std_symbols:
                    qA = net_map.get((A, std_sym), Decimal("0"))
                    _ = qA  # 仅用于对称性，避免 lint
                    sum_qe = Decimal("0")
                    for E in counters:
                        qE = net_map.get((E, std_sym), Decimal("0"))
                        if abs(qE) <= eps:
                            continue
                        sum_qe += qE
                        qty = abs(qE)
                        if qty <= eps:
                            continue
                        if qE > 0:
                            buy_ex, sell_ex = E, A
                        else:
                            buy_ex, sell_ex = A, E

                        tp = pair_index.get((std_sym, frozenset({A, E})))
                        if not tp:
                            continue

                        # 推算网格层级（仅用于日志）
                        config = self.config_manager.get_config(
                            tp.trading_pair_id)
                        base_qty = Decimal(
                            str(config.quantity_config.base_quantity))
                        max_segments = int(config.grid_config.max_segments)
                        inferred = 1
                        try:
                            if base_qty > Decimal("0"):
                                inferred = int(
                                    (qty / base_qty).to_integral_value(rounding=ROUND_UP))
                                inferred = max(1, min(inferred, max_segments))
                        except Exception:
                            inferred = 1

                        entry_buy = leg_meta.get((buy_ex, std_sym), {}).get(
                            "entry_price", Decimal("0"))
                        entry_sell = leg_meta.get((sell_ex, std_sym), {}).get(
                            "entry_price", Decimal("0"))

                        spread_data = SpreadData(
                            symbol=std_sym,
                            exchange_buy=buy_ex,
                            exchange_sell=sell_ex,
                            price_buy=Decimal(str(entry_buy or "0")),
                            price_sell=Decimal(str(entry_sell or "0")),
                            size_buy=qty,
                            size_sell=qty,
                            spread_abs=Decimal("0"),
                            spread_pct=0.0,
                            buy_symbol=std_sym,
                            sell_symbol=std_sym,
                        )

                        await self.decision_engine.record_open(
                            symbol=tp.trading_pair_id,
                            quantity=qty,
                            spread_data=spread_data,
                            funding_rate_data=None,
                            buy_order_id=None,
                            sell_order_id=None,
                            entry_price_buy=Decimal(
                                str(entry_buy or "0")) if entry_buy else None,
                            entry_price_sell=Decimal(
                                str(entry_sell or "0")) if entry_sell else None,
                            filled_quantity=qty,
                        )

                        injected.append({
                            "symbol": tp.trading_pair_id,
                            "exchange_buy": buy_ex,
                            "exchange_sell": sell_ex,
                            "qty": str(qty),
                            "grid": f"T{inferred}",
                        })

                self._bootstrap_positions_done = True
                self._bootstrap_positions_success = True
                self._bootstrap_positions_block_open = False
                logger.warning(
                    "🧩 [启动对齐] one_to_many 持仓同步完成: injected=%s errors=%s attempt=%s",
                    len(injected),
                    len(errors),
                    attempt,
                )
                if injected:
                    logger.warning("🧩 [启动对齐] injected_detail=%s", injected)
                break

            # ----------------------------------------------------------------
            # 非 one_to_many：保持现有逐 pair 校验/注入逻辑（兼容回退）
            # ----------------------------------------------------------------

            # 先扫描快照一致性（缺腿/数量不一致）
            for pair in pairs:
                ex_a = pair.normalized_exchange_a()
                ex_b = pair.normalized_exchange_b()
                std_sym = pair.normalized_symbol()
                if not ex_a or not ex_b or not std_sym:
                    continue
                net_a = net_map.get((ex_a, std_sym), Decimal("0"))
                net_b = net_map.get((ex_b, std_sym), Decimal("0"))

                # 无持仓：跳过
                if abs(net_a) <= self.decision_engine.quantity_epsilon and abs(net_b) <= self.decision_engine.quantity_epsilon:
                    continue

                # 缺腿：只要一边接近0，视为不完整（不注入，避免“半仓继续开仓”）
                if abs(net_a) <= self.decision_engine.quantity_epsilon or abs(net_b) <= self.decision_engine.quantity_epsilon:
                    incomplete.append(pair.trading_pair_id)
                    continue

                # 必须一多一空
                if (net_a > Decimal("0") and net_b > Decimal("0")) or (net_a < Decimal("0") and net_b < Decimal("0")):
                    mismatched.append(pair.trading_pair_id)
                    continue

                qty_a = abs(net_a)
                qty_b = abs(net_b)
                diff = abs(qty_a - qty_b)
                if diff > max(qty_tolerance, self.decision_engine.quantity_epsilon):
                    mismatched.append(pair.trading_pair_id)
                    continue

            # 若快照不完整/不一致：按配置阻止开仓并重试
            if incomplete or (block_on_mismatch and mismatched):
                # 失败诊断：只在“不完整/不一致”时输出一条 detail，帮助定位 symbol 映射/缺腿原因
                try:
                    pair_detail: List[Dict[str, Any]] = []
                    for pair in pairs:
                        ex_a = pair.normalized_exchange_a()
                        ex_b = pair.normalized_exchange_b()
                        std_sym = pair.normalized_symbol()
                        if not ex_a or not ex_b or not std_sym:
                            continue
                        net_a = net_map.get((ex_a, std_sym), Decimal("0"))
                        net_b = net_map.get((ex_b, std_sym), Decimal("0"))
                        # 只收集“异常/有仓但没法注入”的 pair，避免刷屏
                        if (
                            abs(net_a) > self.decision_engine.quantity_epsilon
                            or abs(net_b) > self.decision_engine.quantity_epsilon
                            or pair.trading_pair_id in (incomplete or [])
                            or pair.trading_pair_id in (mismatched or [])
                        ):
                            pair_detail.append({
                                "pair": pair.trading_pair_id,
                                "std_symbol": std_sym,
                                "ex_a": ex_a,
                                "ex_b": ex_b,
                                "net_a": str(net_a),
                                "net_b": str(net_b),
                                "epsilon": str(self.decision_engine.quantity_epsilon),
                            })
                    bootstrap_debug["pair_detail"] = pair_detail
                except Exception:
                    pass
                logger.warning(
                    "🧩 [启动对齐][detail] snapshot_not_ready: debug=%s",
                    bootstrap_debug,
                )
                self._bootstrap_positions_last_error = (
                    f"incomplete={len(incomplete)} mismatched={len(mismatched)} errors={len(errors)}"
                )
                # 失败：保持阻止开仓（若开启了该安全策略）
                self._bootstrap_positions_success = False
                if self._bootstrap_positions_block_open:
                    self._bootstrap_positions_block_open = True
                # 达到最大次数则放弃重试（但仍可保持 block_open）
                if retry_max_attempts and attempt >= retry_max_attempts:
                    logger.warning(
                        "🧩 [启动对齐] 持仓快照不完整/不一致，已到最大重试次数：attempt=%s incomplete=%s mismatched=%s errors=%s",
                        attempt,
                        len(incomplete),
                        len(mismatched),
                        len(errors),
                    )
                    break
                logger.warning(
                    "🧩 [启动对齐] 持仓快照不完整/不一致，等待重试：attempt=%s incomplete=%s mismatched=%s errors=%s",
                    attempt,
                    len(incomplete),
                    len(mismatched),
                    len(errors),
                )
                print(f"⚠️ [启动对齐] 第{attempt}次持仓核对不完整/不一致，{retry_interval_s:g}s后重试", flush=True)
                await asyncio.sleep(retry_interval_s)
                continue

            # 正式注入
            for pair in pairs:
                ex_a = pair.normalized_exchange_a()
                ex_b = pair.normalized_exchange_b()
                std_sym = pair.normalized_symbol()
                if not ex_a or not ex_b or not std_sym:
                    continue
                net_a = net_map.get((ex_a, std_sym), Decimal("0"))
                net_b = net_map.get((ex_b, std_sym), Decimal("0"))
                if abs(net_a) <= self.decision_engine.quantity_epsilon or abs(net_b) <= self.decision_engine.quantity_epsilon:
                    continue
                # 方向：long所在交易所为 buy
                if net_a > Decimal("0") and net_b < Decimal("0"):
                    buy_ex, sell_ex = ex_a, ex_b
                    qty = min(abs(net_a), abs(net_b))
                elif net_a < Decimal("0") and net_b > Decimal("0"):
                    buy_ex, sell_ex = ex_b, ex_a
                    qty = min(abs(net_a), abs(net_b))
                else:
                    continue

                if qty <= self.decision_engine.quantity_epsilon:
                    continue

                # 推算网格层级（仅用于日志）
                config = self.config_manager.get_config(pair.trading_pair_id)
                base_qty = Decimal(str(config.quantity_config.base_quantity))
                max_segments = int(config.grid_config.max_segments)
                inferred = 1
                try:
                    if base_qty > Decimal("0"):
                        inferred = int(
                            (qty / base_qty).to_integral_value(rounding=ROUND_UP))
                        inferred = max(1, min(inferred, max_segments))
                except Exception:
                    inferred = 1

                # entry_price 取各腿加权均价（若无则0）
                entry_buy = leg_meta.get((buy_ex, std_sym), {}).get(
                    "entry_price", Decimal("0"))
                entry_sell = leg_meta.get((sell_ex, std_sym), {}).get(
                    "entry_price", Decimal("0"))

                spread_data = SpreadData(
                    symbol=std_sym,
                    exchange_buy=buy_ex,
                    exchange_sell=sell_ex,
                    price_buy=Decimal(str(entry_buy or "0")),
                    price_sell=Decimal(str(entry_sell or "0")),
                    size_buy=qty,
                    size_sell=qty,
                    spread_abs=Decimal("0"),
                    spread_pct=0.0,
                    buy_symbol=std_sym,
                    sell_symbol=std_sym,
                )

                # 关键：symbol 使用 trading_pair_id，保持决策引擎唯一键一致
                await self.decision_engine.record_open(
                    symbol=pair.trading_pair_id,
                    quantity=qty,
                    spread_data=spread_data,
                    funding_rate_data=None,
                    buy_order_id=None,
                    sell_order_id=None,
                    entry_price_buy=Decimal(
                        str(entry_buy or "0")) if entry_buy else None,
                    entry_price_sell=Decimal(
                        str(entry_sell or "0")) if entry_sell else None,
                    filled_quantity=qty,
                )

                injected.append({
                    "symbol": pair.trading_pair_id,
                    "exchange_buy": buy_ex,
                    "exchange_sell": sell_ex,
                    "qty": str(qty),
                    "grid": f"T{inferred}",
                })

            self._bootstrap_positions_done = True
            self._bootstrap_positions_success = True
            self._bootstrap_positions_block_open = False
            logger.warning(
                "🧩 [启动对齐] 持仓同步完成: injected=%s errors=%s attempt=%s",
                len(injected),
                len(errors),
                attempt,
            )
            if injected:
                # 注意：该 logger 默认写文件，避免刷屏，最多一条 detail
                logger.warning("🧩 [启动对齐] injected_detail=%s", injected)
            break

        self._bootstrap_positions_done = True
        if self._bootstrap_positions_success:
            print(f"✅ [启动对齐] 核对完成：已恢复{len(injected)}组持仓，第{attempt}次核对通过。", flush=True)
        else:
            print(
                f"⚠️ [启动对齐] 核对结束：{self._bootstrap_positions_last_error}；"
                f"{'禁止开仓' if self._bootstrap_positions_block_open else '未启用开仓闭锁'}。",
                flush=True,
            )

    async def _start_variational_ws_events_in_monitor(self) -> None:
        """
        监控模式下也启动 Variational WS events（仅用于观测手动下单推送，不触发任何交易逻辑）。
        - 默认启用（ws_events_in_monitor 默认为 True）
        - 可在 variational.extra_params.ws_events_in_monitor=false 关闭
        """
        if self._variational_monitor_ws_started:
            return

        # 找到 variational 适配器（兼容大小写）
        adapter = None
        for name, a in (self.exchange_adapters or {}).items():
            if str(name).lower() == "variational":
                adapter = a
                break
        if not adapter:
            return

        try:
            cfg = getattr(adapter, "config", None) or getattr(
                adapter, "get_config", lambda: None)()
            extra = getattr(cfg, "extra_params", {}) if cfg else {}
            enabled = bool(extra.get("ws_events_in_monitor", True))
            if not enabled:
                return
        except Exception:
            enabled = True

        # 需要适配器支持 subscribe_user_data（VariationalAdapter 已实现）
        if not hasattr(adapter, "subscribe_user_data"):
            return

        # 节流：同一类型事件每秒最多打印一次（避免刷屏）
        last_log_ts: Dict[str, float] = {}

        async def _cb(event: Dict[str, Any]) -> None:
            try:
                if not isinstance(event, dict):
                    return
                et = str(event.get("type") or "").strip()
                if et not in ("trade", "clearing_event"):
                    return
                now = time.time()
                if now - float(last_log_ts.get(et, 0.0)) < 1.0:
                    return
                last_log_ts[et] = now

                data = event.get("data") if isinstance(
                    event.get("data"), dict) else {}
                if et == "trade":
                    quote_id = data.get("source_quote") or data.get(
                        "parent_quote_id") or data.get("quote_id")
                    side = data.get("side")
                    qty = data.get("qty")
                    price = data.get("price")
                    status = data.get("status")
                    # 用 adapter 自己的 logger 写入 ExchangeAdapter.variational.log
                    if hasattr(adapter, "logger"):
                        adapter.logger.info(
                            "[variational-ws][monitor] trade: quote_id=%s side=%s qty=%s price=%s status=%s",
                            quote_id,
                            side,
                            qty,
                            price,
                            status,
                        )
                else:
                    rfq_id = data.get("rfq_id")
                    clearing_status = data.get("clearing_status")
                    parent_quote_id = data.get("parent_quote_id")
                    if hasattr(adapter, "logger"):
                        adapter.logger.info(
                            "[variational-ws][monitor] clearing_event: rfq_id=%s status=%s parent_quote_id=%s",
                            rfq_id,
                            clearing_status,
                            parent_quote_id,
                        )
            except Exception:
                # 不影响主流程
                return

        await adapter.subscribe_user_data(_cb)
        self._variational_monitor_ws_started = True
        try:
            if hasattr(adapter, "logger"):
                adapter.logger.info(
                    "[variational-ws][monitor] 已启动 events 订阅（用于手动下单推送测试）")
        except Exception:
            pass

    def _bootstrap_recovery_busy(self) -> bool:
        return bool(
            getattr(self, "_pending_open_pairs", None)
            or getattr(self, "_pending_close_symbols", None)
            or getattr(self, "_force_close_in_progress", False)
            or getattr(self, "_force_close_residual_in_progress", False)
            or getattr(self, "_position_alignment_auto_fix_inflight_global", False)
            or any(position.total_quantity > 0 for position in self.decision_engine.positions.values())
        )

    async def _bootstrap_positions_recovery_loop(self) -> None:
        sys_mode = self.config_manager.get_system_mode() or {}
        interval = max(1.0, float(sys_mode.get("bootstrap_positions_retry_interval_seconds", 10) or 10))
        while self.running and not self._bootstrap_positions_success:
            await asyncio.sleep(interval)
            if not self.running:
                return
            if self._bootstrap_recovery_busy():
                continue
            try:
                await self._bootstrap_positions_from_exchanges_once(recovery=True)
            except Exception as e:
                self._bootstrap_positions_done = True
                self._bootstrap_positions_success = False
                self._bootstrap_positions_block_open = True
                self._bootstrap_positions_last_error = str(e)
                logger.warning("🧩 [启动对齐] 后台恢复核对失败，保留禁止开仓：%s", e)

    async def start(self):
        """启动调度器"""
        logger.info("🚀 [统一调度] 启动调度器...")

        # 检查交易所适配器
        if not self.exchange_adapters:
            logger.error("❌ [统一调度] 没有可用的交易所适配器，无法启动")
            raise RuntimeError("没有可用的交易所适配器")

        logger.info(
            f"✅ [统一调度] 已加载 {len(self.exchange_adapters)} 个交易所适配器: {list(self.exchange_adapters.keys())}")

        # 启动风险控制器（仅实盘需要）
        # 监控模式=模拟盘：禁用账户/交易所状态风控，避免无意义的余额/持仓轮询与误阻断。
        if not bool(getattr(self, "monitor_only_mode", True)):
            await self.risk_controller.start()
            # 注意：本模块 logger 默认级别为 WARNING（见文件顶部），INFO 会被过滤；
            # 这里用 WARNING 仅用于让用户在 unified_orchestrator.log 中明确看到“风控已启动”这一关键状态。
            logger.warning("✅ [统一调度] 风险控制器已启动")
        else:
            logger.warning("🧪 [模拟盘] 监控模式已禁用账户类风控（不拉余额/仓位，不触发暂停/强平）")

        # 事件驱动：绑定 running loop 到 LatestStore（必须在 data_processor.start 之前）
        if self.latest_store is not None:
            if self.latest_event is None:
                self.latest_event = asyncio.Event()
            try:
                logger.info(
                    "✅ [DataFlow] event-driven 已绑定: poll_fallback_ms=%s",
                    getattr(self.monitor_config,
                            "latest_poll_interval_ms", 50),
                )
                self.latest_store.bind_event_loop(
                    asyncio.get_running_loop(),
                    self.latest_event,
                    on_first_notify=lambda: logger.info(
                        "✅ [DataFlow] event-driven 已触发（确认生效）"),
                )
            except Exception:
                pass
            # 同步给 DataProcessor（用于 await event）
            self.data_processor._latest_event = self.latest_event

        # 启动数据处理器
        await self.data_processor.start()
        logger.info("✅ [统一调度] 数据处理器已启动")

        # 事件驱动分析：绑定 DataProcessor 触发事件
        try:
            if self.event_driven_analysis_enabled:
                if self.analysis_event is None:
                    self.analysis_event = asyncio.Event()
                self.data_processor.bind_analysis_event(
                    self.analysis_event,
                    min_interval_ms=self.event_driven_analysis_min_interval_ms,
                )
        except Exception:
            pass

        # 连接所有交易所并订阅数据
        await self.bootstrapper.connect_all_exchanges()
        logger.info("✅ [统一调度] 交易所连接和订阅完成")

        # 🔥 初始化WebSocket订单追踪（实盘模式）
        if not self.monitor_only_mode:
            logger.info("📡 [统一调度] 初始化WebSocket订单追踪...")
            await self.executor.initialize_websocket_subscriptions()
            logger.info("✅ [统一调度] WebSocket订单追踪初始化完成")
        else:
            logger.info("🔍 [统一调度] 监控模式，跳过WebSocket订单追踪初始化")
            await self._start_variational_ws_events_in_monitor()

        # Monitor simulates only its own positions; no account bootstrap.
        if self.monitor_only_mode:
            self._bootstrap_positions_done = True
            self._bootstrap_positions_success = True
            self._bootstrap_positions_block_open = False

        # 🔥 启动一次性持仓对齐（重启恢复）
        # 放在 UI 启动前：确保 UI 首屏就是“已恢复后的持仓状态”
        try:
            if not self.monitor_only_mode:
                await self._bootstrap_positions_from_exchanges_once()
        except Exception as e:
            # 兜底：对齐失败不影响启动；若配置为 block_open，则开仓路径会被拦截
            self._bootstrap_positions_last_error = str(e)
            self._bootstrap_positions_done = True
            self._bootstrap_positions_success = False
            sys_mode = self.config_manager.get_system_mode() or {}
            self._bootstrap_positions_block_open = bool(
                sys_mode.get(
                    "bootstrap_positions_block_open_until_success", True)
            )
            logger.warning("🧩 [启动对齐] 持仓对齐异常：%s", e)
            print(f"⚠️ [启动对齐] 核对异常：{e}；{'禁止开仓' if self._bootstrap_positions_block_open else '未启用开仓闭锁'}。", flush=True)

        debug_cli_enabled = getattr(
            self.monitor_config, "debug_cli_mode", False)
        if debug_cli_enabled:
            logger.info("🛠️ [统一调度] Debug CLI 模式启用，跳过富UI渲染")
            self.ui_update_task = None
        else:
            self.ui_manager.start(refresh_rate=5)
            if self.ui_manager.ui_mode != UIMode.SEGMENTED_GRID:
                self.ui_manager.set_v3_mode(True, self.monitor_only_mode)
            else:
                self.ui_manager.is_v3_mode = True
                self.ui_manager.monitor_only_mode = self.monitor_only_mode
            self.ui_manager.update_config({
                'exchanges': self.monitor_config.exchanges,
                'symbols': self.monitor_config.symbols,
                'ui_max_symbols': getattr(self.monitor_config, "ui_max_symbols", 30),
                'multi_leg_symbols': [
                    pair.pair_id for pair in self.multi_leg_pairs
                ] if self.multi_leg_pairs else []
            })
            logger.info("✅ [统一调度] UI管理器已启动")

            self.ui_update_task = asyncio.create_task(
                self.ui_manager.update_loop(
                    self.monitor_config.ui_refresh_interval_ms)
            )
            logger.info("✅ [统一调度] UI渲染循环已启动")

        # UI数据更新循环 / Debug CLI
        self.ui_controller.start(enable_ui_render_loop=not debug_cli_enabled)
        if debug_cli_enabled:
            self.debug_cli_printer = DebugStatePrinter(
                self,
                interval_seconds=getattr(
                    self.monitor_config, "debug_cli_interval_seconds", 1.0
                ),
            )
            self.debug_cli_printer.start()
        logger.info("✅ [统一调度] UI数据控制器已启动")

        # 状态汇总移到独立协程，避免占用主循环
        try:
            self.status_summary_task = asyncio.create_task(
                self._status_summary_loop()
            )
        except Exception:
            self.status_summary_task = None

        # 3. Reduce-only探测服务（仅实盘需要）
        if not bool(getattr(self, "monitor_only_mode", True)):
            self.reduce_only_probe_service.start()

        # 启动主循环
        self.running = True
        if not self.monitor_only_mode and not self._bootstrap_positions_success:
            task = getattr(self, "bootstrap_positions_recovery_task", None)
            if task is None or task.done():
                self.bootstrap_positions_recovery_task = asyncio.create_task(
                    self._bootstrap_positions_recovery_loop())
        logger.info("✅ [统一调度] 分段套利系统已启动")

        # ✅ 启动自检（一次性提示常见误配导致“没信号”）
        try:
            self._startup_sanity_check_and_warn()
        except Exception:
            pass

        # ✅ 持仓校验改为后台定时轮询：仅在“最近10秒无开/平仓完成”时执行一次全局校验
        # - 避免每次开/平仓刚完成时，WS/缓存尚未刷新导致误判与反向纠偏
        try:
            self.position_alignment_poll_task = asyncio.create_task(
                self._position_alignment_poll_loop())
        except Exception:
            self.position_alignment_poll_task = None

        await self._main_loop()

    async def check_has_positions(self) -> bool:
        """
        检查是否有未平仓位

        Returns:
            bool: True表示有持仓，False表示无持仓
        """
        try:
            positions = self.decision_engine.positions

            # 检查是否有任何持仓
            for symbol, position in positions.items():
                if position.is_open and position.total_quantity > 0:
                    return True

            return False
        except Exception as e:
            logger.error(f"❌ [统一调度] 检查持仓失败: {e}")
            return False

    async def emergency_close_all_positions(self, *, timeout_seconds: float = 60.0) -> bool:
        """
        紧急平仓所有持仓（市价单）

        用于系统退出时的风险控制
        """
        try:
            overall_success = True
            if getattr(self.executor, "monitor_only", True):
                logger.warning("ℹ️  [紧急平仓] 当前为监控模式（不下单），跳过紧急平仓。")
                return False
            positions = self.decision_engine.positions

            if not positions:
                logger.warning("ℹ️  [紧急平仓] 无持仓需要平仓")
                return True

            close_tasks: List[Tuple[str, Decimal, SpreadData,
                                    "asyncio.Task[ExecutionResult]"]] = []

            for symbol, position in positions.items():
                if not position.is_open or position.total_quantity <= 0:
                    continue

                logger.warning(
                    f"🔄 [紧急平仓] {symbol}: "
                    f"数量={position.total_quantity}, "
                    f"买入方={position.exchange_buy}, "
                    f"卖出方={position.exchange_sell}"
                )

                # 基于实时订单簿构造“平仓视角”价差（不依赖不存在的 get_latest_spread）
                buy_ex = position.exchange_buy
                sell_ex = position.exchange_sell
                if not buy_ex or not sell_ex:
                    logger.warning(f"⚠️  [紧急平仓] {symbol}: 持仓缺少交易所信息，跳过")
                    overall_success = False
                    continue
                buy_symbol_open = (position.buy_symbol or symbol).upper()
                sell_symbol_open = (position.sell_symbol or symbol).upper()
                ob_buy = self.data_processor.get_orderbook(
                    buy_ex,
                    buy_symbol_open,
                    max_age_seconds=self.get_data_freshness_seconds(buy_ex),
                )
                ob_sell = self.data_processor.get_orderbook(
                    sell_ex,
                    sell_symbol_open,
                    max_age_seconds=self.get_data_freshness_seconds(sell_ex),
                )
                if not ob_buy or not ob_sell or not ob_buy.best_ask or not ob_sell.best_bid:
                    logger.warning(
                        f"⚠️  [紧急平仓] {symbol}: 缺少实时盘口({buy_ex}/{sell_ex})，跳过")
                    overall_success = False
                    continue
                opening_spread = SpreadData(
                    symbol=symbol,
                    exchange_buy=buy_ex,
                    exchange_sell=sell_ex,
                    price_buy=ob_buy.best_ask.price,
                    price_sell=ob_sell.best_bid.price,
                    size_buy=ob_buy.best_ask.size,
                    size_sell=ob_sell.best_bid.size,
                    spread_abs=ob_sell.best_bid.price - ob_buy.best_ask.price,
                    spread_pct=float(
                        ((ob_sell.best_bid.price - ob_buy.best_ask.price) / ob_buy.best_ask.price) * 100),
                    buy_symbol=buy_symbol_open,
                    sell_symbol=sell_symbol_open,
                )
                closing_spread = self.spread_calculator.build_closing_spread_from_orderbooks(
                    opening_spread,
                    {buy_ex: ob_buy, sell_ex: ob_sell},
                )
                if not closing_spread:
                    logger.warning(f"⚠️  [紧急平仓] {symbol}: 无法生成平仓视角价差，跳过")
                    overall_success = False
                    continue

                # 创建平仓请求（反向交易）
                from ..execution.arbitrage_executor import ExecutionRequest
                buy_symbol = closing_spread.buy_symbol or (
                    position.sell_symbol or symbol)
                sell_symbol = closing_spread.sell_symbol or (
                    position.buy_symbol or symbol)

                # 🔥 获取完整盘口数据
                orderbook_buy = self.data_processor.get_orderbook(
                    closing_spread.exchange_buy, buy_symbol)
                orderbook_sell = self.data_processor.get_orderbook(
                    closing_spread.exchange_sell, sell_symbol)

                emergency_grid_level = self.decision_engine.get_current_segments(
                    symbol)
                grid_threshold_pct = self._resolve_grid_threshold_pct(
                    symbol, emergency_grid_level)

                close_request = ExecutionRequest(
                    symbol=symbol,
                    exchange_buy=closing_spread.exchange_buy,  # 平仓视角：买回
                    exchange_sell=closing_spread.exchange_sell,  # 平仓视角：卖出
                    price_buy=closing_spread.price_buy,
                    price_sell=closing_spread.price_sell,
                    quantity=position.total_quantity,
                    is_open=False,
                    spread_data=closing_spread,
                    buy_symbol=buy_symbol,
                    sell_symbol=sell_symbol,
                    grid_action="close",
                    grid_level=emergency_grid_level or None,
                    grid_threshold_pct=grid_threshold_pct,
                    limit_price_offset_buy=self._get_limit_price_offset_for_symbol(
                        buy_symbol),
                    limit_price_offset_sell=self._get_limit_price_offset_for_symbol(
                        sell_symbol),
                    min_exchange_order_qty=self._build_min_exchange_order_qty_map(
                        symbol),
                    orderbook_buy_ask=closing_spread.price_buy if orderbook_buy and orderbook_buy.best_ask else None,
                    orderbook_buy_bid=orderbook_buy.best_bid.price if orderbook_buy and orderbook_buy.best_bid else None,
                    orderbook_sell_ask=orderbook_sell.best_ask.price if orderbook_sell and orderbook_sell.best_ask else None,
                    orderbook_sell_bid=closing_spread.price_sell if orderbook_sell and orderbook_sell.best_bid else None,
                    allow_when_global_paused=True,
                )

                # 并发执行平仓
                task = asyncio.create_task(
                    self.executor.execute_arbitrage(close_request))
                close_tasks.append(
                    (symbol, Decimal(str(position.total_quantity)), closing_spread, task))

            # 等待所有平仓完成
            if close_tasks:
                logger.warning(
                    f"⏳ [紧急平仓] 等待 {len(close_tasks)} 个持仓平仓... (timeout={timeout_seconds}s)")
                tasks_only = [t for (_, _, _, t) in close_tasks]
                try:
                    results = await asyncio.wait_for(
                        asyncio.gather(*tasks_only, return_exceptions=True),
                        timeout=timeout_seconds,
                    )
                except asyncio.TimeoutError:
                    logger.warning("⚠️  [紧急平仓] 等待超时，继续退出（请手动检查是否仍有残余持仓）")
                    results = []
                    overall_success = False
                for (symbol, qty, spread_data, _task), result in zip(close_tasks, results):
                    if isinstance(result, Exception):
                        logger.error(f"❌ [紧急平仓] {symbol}: 平仓异常 - {result}")
                        overall_success = False
                        continue
                    if getattr(result, "success", False):
                        actual_closed = min(qty, self._extract_filled_quantity(result, qty))
                        if actual_closed < qty:
                            overall_success = False
                        logger.warning("[紧急平仓] %s: 已确认平仓量=%s 计划=%s", symbol, actual_closed, qty)
                        try:
                            await self.decision_engine.record_close(
                                symbol=symbol,
                                quantity=min(qty, self._extract_filled_quantity(result, qty)),
                                spread_data=spread_data,
                                reason="系统退出紧急平仓",
                            )
                        except Exception:
                            pass
                    else:
                        logger.error(
                            f"❌ [紧急平仓] {symbol}: 平仓失败 - {getattr(result, 'error_message', '')}")
                        overall_success = False
                logger.warning("✅ [紧急平仓] 紧急平仓流程结束")
            else:
                # 有持仓但全部因缺信息/盘口等原因被跳过
                # overall_success 已在上方按跳过原因置为 False
                pass

            return bool(overall_success)

        except Exception as e:
            logger.error(f"❌ [紧急平仓] 批量平仓失败: {e}", exc_info=True)
            raise

    async def _close_position_for_risk(self, symbol: str, reason: str) -> bool:
        if self.monitor_only_mode or getattr(self.executor, "monitor_only", True):
            return False
        self._position_risk_close_reasons[symbol] = reason
        if symbol in self._position_risk_close_failed:
            return False
        # Let an already submitted open/close settle before reducing this pair.
        if getattr(self, "_force_close_in_progress", False) or any(
            key.startswith(symbol.upper() + ":") for key in self._pending_open_pairs
        ):
            return False
        close_key = symbol.upper()
        if not await self._try_register_close_symbol(close_key):
            return False
        try:
            ok = await self.force_close_all_positions_by_grid_sequential(
                reason=reason, target_keys={symbol})
            if ok is None:
                return False
            if not ok:
                self._position_risk_close_failed.add(symbol)
                logger.error("风控退出失败，%s 已锁存，需核对残余持仓后恢复。", symbol)
            return ok
        except (Exception, asyncio.CancelledError):
            self._position_risk_close_failed.add(symbol)
            raise
        finally:
            self._release_close_symbol(close_key)

    async def force_close_all_positions_by_grid_sequential(
        self,
        *,
        timeout_seconds: float = 60.0,
        reason: str = "余额严重不足：强制分批平仓",
        target_keys: Optional[Set[str]] = None,
    ) -> Optional[bool]:
        """
        余额严重不足触发的“强制平仓”（分批、按币种依次）：
        - 每次平仓数量=单个网格数量（base_quantity，不足则全平）
        - 必须本次平仓两腿都成功（ExecutionResult.success=True 且 partial_failure=False）才继续
        - 若失败（含单腿成功/单腿失败），停止后续平仓（系统仍处于风控暂停中）
        - 定向风控在提交前被拒绝时返回 None，由下一轮重新核对残余仓位
        """
        if getattr(self.executor, "monitor_only", True):
            logger.warning("🧯 [强制分批平仓] 当前为监控模式（不下单），已跳过。")
            return False

        # 防重入：避免风控循环每 60s 再次触发时并发跑多个平仓任务
        if bool(getattr(self, "_force_close_in_progress", False)):
            logger.warning("🧯 [强制分批平仓] 任务已在执行中，忽略重复触发。")
            return True
        self._force_close_in_progress = True

        try:
            # trading_pair_id -> 标准 symbol（如 LIGHTER_VARIATIONAL_BTC -> BTC-USDC-PERP）
            pair_symbol_map: Dict[str, str] = {}
            try:
                for p in (getattr(self, "multi_exchange_pairs", []) or []):
                    pid = getattr(p, "trading_pair_id", None)
                    sym = getattr(p, "normalized_symbol", lambda: None)()
                    if pid and sym:
                        pair_symbol_map[str(pid).upper().strip()] = str(
                            sym).upper().strip()
            except Exception:
                pair_symbol_map = {}

            def _resolve_base_symbol(symbol_key: str, position: Any) -> str:
                # 优先：多交易所交易对映射
                sk = (symbol_key or "").upper().strip()
                mapped = pair_symbol_map.get(sk)
                if mapped:
                    return mapped
                # 兜底：持仓里若已记住腿的实际 symbol，用它推回标准 symbol（买腿/卖腿通常同底层）
                for attr in ("buy_symbol", "sell_symbol", "symbol"):
                    try:
                        v = getattr(position, attr, None)
                        if v:
                            return str(v).upper().strip()
                    except Exception:
                        continue
                return sk

            # 依次处理：按 monitor_config.symbols 的顺序优先；其余未知 key 放最后
            positions = getattr(self.decision_engine, "positions", {}) or {}
            if not positions:
                logger.warning("🧯 [强制分批平仓] 无持仓需要平仓。")
                return True

            desired_order = [str(s).upper().strip()
                             for s in (getattr(self.monitor_config, "symbols", []) or [])
                             if isinstance(s, str) and str(s).strip()]
            desired_set = set(desired_order)

            def _sort_key(symbol_key: str) -> tuple[int, int, str]:
                base = _resolve_base_symbol(
                    symbol_key, positions.get(symbol_key))
                if base in desired_set:
                    return (0, desired_order.index(base), symbol_key)
                return (1, 10**9, symbol_key)

            symbol_keys = sorted(
                [k for k in positions.keys() if isinstance(k, str) and (target_keys is None or k in target_keys)], key=_sort_key)

            logger.warning("🧯 [强制分批平仓] 开始：symbols=%s reason=%s",
                           len(symbol_keys), reason)

            for symbol_key in symbol_keys:
                pos = self.decision_engine.get_position(symbol_key)
                if not pos or not getattr(pos, "is_open", True) or getattr(pos, "total_quantity", Decimal("0")) <= Decimal("0"):
                    continue

                base_symbol = _resolve_base_symbol(symbol_key, pos)
                try:
                    # ✅ 优先按“持仓 key(交易所套利对/底层ID)”取配置，避免 exchange_pair_symbol_config_enabled 时回退到 default_config
                    # 例如：symbol_key=LIGHTER_VARIATIONAL_BTC
                    try:
                        cfg = self.config_manager.get_config(symbol_key)
                    except Exception:
                        cfg = self.config_manager.get_config(base_symbol)
                    base_qty = Decimal(str(cfg.quantity_config.base_quantity))
                except Exception:
                    base_qty = Decimal("0")

                if base_qty <= Decimal("0"):
                    # 无法解析 base_quantity：退化为一次性全平该币（避免卡死）
                    base_qty = Decimal(
                        str(getattr(pos, "total_quantity", Decimal("0")) or "0"))

                logger.warning(
                    "🧯 [强制分批平仓] %s: 开始分批平仓 base_symbol=%s base_qty=%s total=%s",
                    symbol_key,
                    base_symbol,
                    base_qty,
                    getattr(pos, "total_quantity", Decimal("0")),
                )

                # ✅ 余额不足强制平仓：同一币种分批平到清零（每次一格，直到剩余为0）
                last_remaining: Optional[Decimal] = None
                while True:
                    pos2 = self.decision_engine.get_position(symbol_key)
                    if not pos2 or getattr(pos2, "total_quantity", Decimal("0")) <= self.decision_engine.quantity_epsilon:
                        break

                    remaining = Decimal(
                        str(getattr(pos2, "total_quantity", Decimal("0")) or "0"))
                    # 防止异常：若连续两次 remaining 不变，避免死循环
                    if last_remaining is not None and remaining + self.decision_engine.quantity_epsilon >= last_remaining:
                        logger.error(
                            "❌ [强制分批平仓] %s: 剩余仓位未下降(remaining=%s)，停止后续分批平仓。",
                            symbol_key,
                            remaining,
                        )
                        return False
                    last_remaining = remaining
                    step_qty = min(base_qty, remaining)
                    if step_qty <= self.decision_engine.quantity_epsilon:
                        break

                    buy_ex = getattr(pos2, "exchange_buy", None)
                    sell_ex = getattr(pos2, "exchange_sell", None)
                    if not buy_ex or not sell_ex:
                        logger.error(
                            "❌ [强制分批平仓] %s: 持仓缺少交易所信息，停止后续分批平仓。", symbol_key)
                        return False

                    buy_symbol_open = (
                        getattr(pos2, "buy_symbol", None) or base_symbol).upper()
                    sell_symbol_open = (
                        getattr(pos2, "sell_symbol", None) or base_symbol).upper()

                    ob_buy = self.data_processor.get_orderbook(
                        buy_ex,
                        buy_symbol_open,
                        max_age_seconds=self.get_data_freshness_seconds(
                            buy_ex),
                    )
                    ob_sell = self.data_processor.get_orderbook(
                        sell_ex,
                        sell_symbol_open,
                        max_age_seconds=self.get_data_freshness_seconds(
                            sell_ex),
                    )
                    if not ob_buy or not ob_sell or not ob_buy.best_ask or not ob_sell.best_bid:
                        logger.error(
                            "❌ [强制分批平仓] %s: 缺少实时盘口(%s/%s)，停止后续分批平仓。",
                            symbol_key,
                            buy_ex,
                            sell_ex,
                        )
                        return False

                    opening_spread = SpreadData(
                        symbol=base_symbol,
                        exchange_buy=buy_ex,
                        exchange_sell=sell_ex,
                        price_buy=ob_buy.best_ask.price,
                        price_sell=ob_sell.best_bid.price,
                        size_buy=ob_buy.best_ask.size,
                        size_sell=ob_sell.best_bid.size,
                        spread_abs=ob_sell.best_bid.price - ob_buy.best_ask.price,
                        spread_pct=float(
                            ((ob_sell.best_bid.price - ob_buy.best_ask.price) / ob_buy.best_ask.price) * 100),
                        buy_symbol=buy_symbol_open,
                        sell_symbol=sell_symbol_open,
                    )
                    closing_spread = self.spread_calculator.build_closing_spread_from_orderbooks(
                        opening_spread,
                        {buy_ex: ob_buy, sell_ex: ob_sell},
                    )
                    if not closing_spread:
                        logger.error(
                            "❌ [强制分批平仓] %s: 无法生成平仓视角价差，停止后续分批平仓。", symbol_key)
                        return False

                    from ..execution.arbitrage_executor import ExecutionRequest
                    buy_symbol = closing_spread.buy_symbol or sell_symbol_open
                    sell_symbol = closing_spread.sell_symbol or buy_symbol_open

                    emergency_grid_level = self.decision_engine.get_current_segments(
                        symbol_key)
                    grid_threshold_pct = self._resolve_grid_threshold_pct(
                        symbol_key, emergency_grid_level)
                    orderbook_buy = self.data_processor.get_orderbook(
                        closing_spread.exchange_buy, buy_symbol)
                    orderbook_sell = self.data_processor.get_orderbook(
                        closing_spread.exchange_sell, sell_symbol)

                    close_request = ExecutionRequest(
                        symbol=symbol_key,
                        exchange_buy=closing_spread.exchange_buy,
                        exchange_sell=closing_spread.exchange_sell,
                        price_buy=closing_spread.price_buy,
                        price_sell=closing_spread.price_sell,
                        quantity=step_qty,
                        is_open=False,
                        spread_data=closing_spread,
                        buy_symbol=buy_symbol,
                        sell_symbol=sell_symbol,
                        grid_action="close",
                        grid_level=emergency_grid_level or None,
                        grid_threshold_pct=grid_threshold_pct,
                        force_order_mode="market_market",
                        limit_price_offset_buy=self._get_limit_price_offset_for_symbol(
                            buy_symbol),
                        limit_price_offset_sell=self._get_limit_price_offset_for_symbol(
                            sell_symbol),
                        min_exchange_order_qty=self._build_min_exchange_order_qty_map(
                            base_symbol),
                        orderbook_buy_ask=closing_spread.price_buy if orderbook_buy and orderbook_buy.best_ask else None,
                        orderbook_buy_bid=orderbook_buy.best_bid.price if orderbook_buy and orderbook_buy.best_bid else None,
                        orderbook_sell_ask=orderbook_sell.best_ask.price if orderbook_sell and orderbook_sell.best_ask else None,
                        orderbook_sell_bid=closing_spread.price_sell if orderbook_sell and orderbook_sell.best_bid else None,
                        allow_when_global_paused=True,
                    )

                    logger.warning(
                        "🧯 [强制分批平仓] %s: 下单平仓 step=%s remaining=%s",
                        symbol_key,
                        step_qty,
                        remaining,
                    )

                    # 必须等待本次平仓完成后再继续（依次、强一致）
                    # 重试策略：
                    # - 若出现 partial_failure（单腿成功单腿失败）：只重试失败腿（按你要求：哪个失败重试哪个）
                    #   - 重试成功：继续分批平仓
                    #   - 重试仍失败：停止后续分批平仓（系统仍处于余额风控暂停中）
                    # - 若两腿均未成功下出/未产生订单：按配置做“外层重试”
                    try:
                        max_outer_attempts = int(
                            getattr(self.executor, "max_retry_count", 1) or 1)
                    except Exception:
                        max_outer_attempts = 1
                    max_outer_attempts = max(1, max_outer_attempts)

                    ok = False
                    closed_qty_for_record = step_qty
                    last_result = None
                    for attempt in range(1, max_outer_attempts + 1):
                        last_result = await asyncio.wait_for(
                            self.executor.execute_arbitrage(close_request),
                            timeout=timeout_seconds,
                        )

                        if target_keys is not None and getattr(last_result, "failure_code", None) == "ORDER_STATE_UNKNOWN":
                            return False
                        if target_keys is not None and getattr(last_result, "failure_code", None) == "CLOSE_REJECTED":
                            # The executor emits this only before either leg is submitted.
                            return None

                        # 成功口径：两腿都成功（success=True 且 partial_failure=False）
                        if getattr(last_result, "success", False) and (not bool(getattr(last_result, "partial_failure", False))):
                            closed_qty_for_record = min(step_qty, self._extract_filled_quantity(last_result, step_qty))
                            if closed_qty_for_record <= self.decision_engine.quantity_epsilon:
                                return False
                            ok = True
                            break

                        # 单腿成功/单腿失败：只重试失败腿（按你要求）
                        if bool(getattr(last_result, "partial_failure", False)):
                            failed_exchange = getattr(
                                last_result, "failed_exchange", None)
                            if not failed_exchange:
                                logger.error(
                                    "❌ [强制分批平仓] %s: 单腿失败但 failed_exchange 缺失，停止后续分批平仓。error=%s",
                                    symbol_key,
                                    getattr(last_result,
                                            "error_message", None),
                                )
                                return False
                            try:
                                success_qty = Decimal(
                                    str(getattr(last_result, "success_quantity", "0") or "0"))
                            except Exception:
                                success_qty = Decimal("0")
                            retry_qty = min(step_qty, max(
                                Decimal("0"), success_qty))
                            if getattr(last_result, "failure_code", None) == "ORDER_STATE_UNKNOWN" or retry_qty <= self.decision_engine.quantity_epsilon:
                                return False

                            # 对失败腿做顺序重试：成功则继续；失败则停止后续平仓
                            fixed = await self._retry_force_close_failed_leg_only(
                                close_request=close_request,
                                failed_exchange=str(failed_exchange),
                                retry_qty=retry_qty,
                                timeout_seconds=timeout_seconds,
                            )
                            if not fixed:
                                logger.error(
                                    "❌ [强制分批平仓] %s: 失败腿重试仍失败，停止后续分批平仓。failed_exchange=%s error=%s",
                                    symbol_key,
                                    failed_exchange,
                                    getattr(last_result,
                                            "error_message", None),
                                )
                                return False
                            ok = True
                            closed_qty_for_record = retry_qty
                            break

                        # 若两腿都没产生订单（更安全）：允许外层重试
                        buy_order = getattr(last_result, "order_buy", None)
                        sell_order = getattr(last_result, "order_sell", None)
                        can_retry = (buy_order is None and sell_order is None)
                        if attempt < max_outer_attempts and can_retry:
                            self._log_with_throttle(
                                key=f"force_close_retry:{symbol_key}",
                                signature="force_close_retry",
                                message=(
                                    f"🔁 [强制分批平仓] {symbol_key}: 平仓失败将重试 "
                                    f"(attempt={attempt}/{max_outer_attempts}) error={getattr(last_result, 'error_message', None)}"
                                ),
                                level="warning",
                                throttle_seconds=0.0,
                            )
                            await asyncio.sleep(float(getattr(self, "loop_interval", 1.0) or 1.0))
                            continue

                        # 其余失败：停止后续平仓
                        logger.error(
                            "❌ [强制分批平仓] %s: 本次平仓失败，已停止后续分批平仓。success=%s error=%s",
                            symbol_key,
                            getattr(last_result, "success", False),
                            getattr(last_result, "error_message", None),
                        )
                        return False

                    if not ok:
                        logger.error(
                            "❌ [强制分批平仓] %s: 平仓重试后仍失败，停止后续分批平仓。", symbol_key)
                        return False

                    try:
                        await self.decision_engine.record_close(
                            symbol=symbol_key,
                            quantity=closed_qty_for_record,
                            spread_data=closing_spread,
                            reason=reason,
                        )
                    except Exception:
                        # record_close 失败不应导致继续下单，避免“状态不同步”
                        logger.error(
                            "❌ [强制分批平仓] %s: record_close失败，停止后续分批平仓。", symbol_key, exc_info=True)
                        return False
                    continue

            logger.warning("✅ [强制分批平仓] 所有可见持仓已处理完毕。")
            return True
        except asyncio.TimeoutError:
            logger.error("❌ [强制分批平仓] 执行超时，已停止后续分批平仓。")
            return False
        except Exception as exc:
            logger.error("❌ [强制分批平仓] 异常：%s", exc, exc_info=True)
            return False
        finally:
            self._force_close_in_progress = False

    async def _force_close_exchange_cache_residual_positions(
        self,
        *,
        timeout_seconds: float = 60.0,
        reason: str = "强制平仓后残余持仓清理",
    ) -> bool:
        """
        强制分批平仓结束后，基于交易所适配器的持仓缓存进行一次残余清理。
        - 只读取缓存，不依赖决策引擎
        - 仅执行一次扫描（不上线循环）
        """
        adapters = getattr(self, "exchange_adapters", None) or {}
        if not adapters:
            return True

        epsilon = getattr(self.decision_engine, "quantity_epsilon", Decimal("0.00000001"))
        overall_ok = True
        residuals: List[Tuple[str, str, Decimal, bool]] = []

        for ex_name, adapter in adapters.items():
            cache = getattr(adapter, "_position_cache", None)
            if not cache:
                continue
            # cache 可能是 dict 或 list
            items = cache.items() if isinstance(cache, dict) else enumerate(cache)
            for key, entry in items:
                symbol = None
                size = Decimal("0")
                is_buy_to_close = None
                try:
                    if isinstance(entry, PositionData):
                        symbol = str(entry.symbol)
                        size = Decimal(str(entry.size or "0"))
                        is_buy_to_close = bool(entry.side == PositionSide.SHORT)
                    elif isinstance(entry, dict):
                        symbol = str(entry.get("symbol") or key or "")
                        raw_size = entry.get("size") or entry.get("position") or 0
                        size = Decimal(str(raw_size))
                        if size < Decimal("0"):
                            is_buy_to_close = True
                            size = abs(size)
                        else:
                            is_buy_to_close = False
                    else:
                        symbol = str(getattr(entry, "symbol", "") or key or "")
                        size = Decimal(str(getattr(entry, "size", "0") or "0"))
                        side = getattr(entry, "side", None)
                        if side is not None:
                            is_buy_to_close = bool(side == PositionSide.SHORT)
                except Exception:
                    continue

                if not symbol or size <= epsilon or is_buy_to_close is None:
                    continue
                residuals.append((str(ex_name), symbol, size, bool(is_buy_to_close)))

        if not residuals:
            logger.warning("✅ [强制分批平仓] 交易所缓存检查完成：未发现残余持仓。")
            return True

        logger.warning(
            "🧯 [强制分批平仓] 交易所缓存发现残余持仓：count=%s，将尝试市价平仓。",
            len(residuals),
        )

        for ex_name, symbol, qty, is_buy_to_close in residuals:
            adapter = adapters.get(ex_name)
            if not adapter:
                overall_ok = False
                logger.error("❌ [强制分批平仓] 未找到适配器，无法平仓: ex=%s symbol=%s", ex_name, symbol)
                continue
            ok = await self.executor._emergency_close_position(
                exchange=ex_name,
                adapter=adapter,
                symbol=symbol,
                quantity=qty,
                is_buy_to_close=is_buy_to_close,
                request=None,
                max_attempts_override=1,
            )
            if not ok:
                overall_ok = False
                logger.error(
                    "❌ [强制分批平仓] 残余持仓平仓失败: ex=%s symbol=%s qty=%s reason=%s",
                    ex_name,
                    symbol,
                    qty,
                    reason,
                )
            await asyncio.sleep(0.2)

        if overall_ok:
            logger.warning("✅ [强制分批平仓] 残余持仓清理完成。")
        return overall_ok

    async def _retry_force_close_failed_leg_only(
        self,
        *,
        close_request: "ExecutionRequest",
        failed_exchange: str,
        retry_qty: Decimal,
        timeout_seconds: float,
    ) -> bool:
        """
        强制分批平仓：单腿失败后的“失败腿重试”。

        规则（按你的口径）：
        - 只重试失败的那一腿（不重复下另一腿）
        - 顺序重试：一次只重试一个交易所
        - 重试成功才继续后续分批平仓；重试失败则停止后续分批平仓
        """
        if retry_qty <= self.decision_engine.quantity_epsilon:
            return False

        failed_ex = (failed_exchange or "").strip().lower()
        if not failed_ex:
            return False

        buy_ex = (close_request.exchange_buy or "").strip().lower()
        sell_ex = (close_request.exchange_sell or "").strip().lower()
        if failed_ex not in (buy_ex, sell_ex):
            logger.error(
                "❌ [强制分批平仓] failed_exchange=%s 不在本次请求两腿中(%s/%s)，停止重试。",
                failed_ex,
                buy_ex,
                sell_ex,
            )
            return False

        is_buy_leg = bool(failed_ex == buy_ex)
        leg_symbol = (
            close_request.buy_symbol if is_buy_leg else close_request.sell_symbol) or close_request.symbol
        leg_symbol = str(leg_symbol).upper().strip()
        if not leg_symbol:
            return False

        adapter = getattr(self.executor, "exchange_adapters",
                          {}).get(failed_ex)
        if adapter is None:
            logger.error("❌ [强制分批平仓] %s: 未找到适配器，无法重试失败腿。", failed_ex)
            return False

        # 滑点覆盖：复用执行器口径（放大倍数用于“强制补单”）
        base_slippage = None
        try:
            base_slippage = self.executor._get_slippage_percent(close_request)
        except Exception:
            base_slippage = None
        retry_slippage = (base_slippage * Decimal("20")
                          ) if base_slippage is not None else None

        try:
            max_leg_attempts = int(
                getattr(getattr(self.executor, "config", None).order_execution,
                        "max_retry_count", 3)  # type: ignore[union-attr]
                or 3
            )
        except Exception:
            max_leg_attempts = 3
        max_leg_attempts = max(1, max_leg_attempts)

        # 逐次重试失败腿（一次一单），直到成交到目标或失败
        for attempt in range(1, max_leg_attempts + 1):
            logger.warning(
                "🔁 [强制分批平仓] 失败腿重试: ex=%s leg=%s qty=%s attempt=%s/%s",
                failed_ex,
                "买腿" if is_buy_leg else "卖腿",
                retry_qty,
                attempt,
                max_leg_attempts,
            )
            try:
                order = await asyncio.wait_for(
                    self.executor._place_market_order(
                        adapter=adapter,
                        symbol=leg_symbol,
                        quantity=retry_qty,
                        is_buy=is_buy_leg,
                        reduce_only=True,
                        request=close_request,
                        slippage_override=retry_slippage,
                        force_rest=False,
                    ),
                    timeout=timeout_seconds,
                )
            except Exception as exc:
                logger.error(
                    "❌ [强制分批平仓] 失败腿下单异常：ex=%s leg=%s err=%s",
                    failed_ex,
                    "买腿" if is_buy_leg else "卖腿",
                    exc,
                    exc_info=True,
                )
                return False

            if not order:
                # 下单失败：继续下一次重试（顺序）
                if attempt < max_leg_attempts:
                    await asyncio.sleep(0.5)
                    continue
                return False

            # 等待成交确认（不依赖内部 resubmit；我们外层自己重试）
            try:
                _o, filled = await asyncio.wait_for(
                    self.executor.order_strategy_executor._confirm_market_fill_with_retries(  # type: ignore[attr-defined]
                        initial_order=order,
                        adapter=adapter,
                        symbol=leg_symbol,
                        quantity=retry_qty,
                        is_buy=is_buy_leg,
                        reduce_only=True,
                        request=close_request,
                        exchange_name=failed_ex,
                        allow_resubmit=False,
                        max_attempts=1,
                        enable_internal_aggressive_retry=True,
                    ),
                    timeout=timeout_seconds,
                )
            except Exception as exc:
                logger.error(
                    "❌ [强制分批平仓] 失败腿成交确认异常：ex=%s leg=%s err=%s",
                    failed_ex,
                    "买腿" if is_buy_leg else "卖腿",
                    exc,
                    exc_info=True,
                )
                return False

            if filled + self.decision_engine.quantity_epsilon >= retry_qty:
                return True

            # 未成交到目标：继续下一次重试
            if attempt < max_leg_attempts:
                await asyncio.sleep(0.5)
                continue
            return False

        return False

    async def stop(self):
        """停止调度器"""
        logger.info("🛑 [统一调度] 停止调度器...")
        self.running = False

        task = getattr(self, "bootstrap_positions_recovery_task", None)
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self.bootstrap_positions_recovery_task = None

        # 停止持仓校验轮询
        try:
            task = getattr(self, "position_alignment_poll_task", None)
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            self.position_alignment_poll_task = None
        except Exception:
            pass

        # 停止状态汇总协程
        try:
            task = getattr(self, "status_summary_task", None)
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            self.status_summary_task = None
        except Exception:
            pass

        # 停止数据处理器和风险控制器
        await self.data_processor.stop()
        await self.risk_controller.stop()

        # 停止UI更新任务
        if hasattr(self, 'ui_update_task') and self.ui_update_task:
            self.ui_update_task.cancel()
            try:
                await self.ui_update_task
            except asyncio.CancelledError:
                pass
            self.ui_update_task = None

        if self.debug_cli_printer:
            await self.debug_cli_printer.stop()
            self.debug_cli_printer = None

        # 停止UI
        if not getattr(self.monitor_config, "debug_cli_mode", False):
            self.ui_manager.stop()
        await self.ui_controller.stop()
        await self.reduce_only_probe_service.stop()

        # 断开所有交易所
        await self.bootstrapper.disconnect_all_exchanges()

        logger.info("✅ [统一调度] 调度器已停止")

    async def _main_loop(self):
        """主循环：处理套利决策和执行"""
        logger.info("▶️  [统一调度] 主循环启动")

        try:
            while self.running:
                try:
                    # 检查风险控制状态
                    risk_status = self.risk_controller.get_risk_status()
                    if risk_status.is_paused:
                        if not self.monitor_only_mode:
                            for symbol, position in list(self.decision_engine.positions.items()):
                                if position.total_quantity <= 0:
                                    continue
                                try:
                                    views = self.spread_pipeline._build_spreads_from_position(symbol, position)
                                    if not views:
                                        continue
                                    reason = self._position_risk_close_reasons.get(symbol) or self._position_loss_reason(
                                        position, views[1], self.config_manager.get_config(symbol))
                                    if reason:
                                        await self._close_position_for_risk(symbol, reason)
                                except Exception:
                                    logger.exception("暂停期间检查 %s 风控退出失败", symbol)
                        await asyncio.sleep(self.loop_interval)
                        continue

                    # 仅处理“本轮有更新的交易对”
                    dirty_keys: Set[Tuple[str, str]] = set()
                    try:
                        dirty_keys = self.data_processor.consume_dirty_keys()
                    except Exception:
                        dirty_keys = set()
                    dirty_symbols = {str(sym).upper() for (_, sym) in dirty_keys}
                    run_full = not getattr(self, "_first_analysis_done", False)
                    run_filtered = bool(dirty_keys) and bool(getattr(self, "_first_analysis_done", False))
                    if not run_full and not run_filtered:
                        # 无新数据：只做心跳与等待
                        if self.event_driven_analysis_enabled and self.analysis_event is not None:
                            try:
                                await asyncio.wait_for(
                                    self.analysis_event.wait(),
                                    timeout=float(self.loop_interval or 0.1),
                                )
                                self.analysis_event.clear()
                            except asyncio.TimeoutError:
                                self.analysis_fallback_loop_count += 1
                        else:
                            await asyncio.sleep(self.loop_interval)
                        continue

                    # 并行处理所有交易对，提升多symbol时的吞吐
                    symbol_tasks = []
                    symbol_list: List[str] = []
                    for symbol in self.monitor_config.symbols:
                        symbol_upper = symbol.upper()
                        if run_filtered and symbol_upper not in dirty_symbols:
                            continue
                        if symbol_upper in self.multi_exchange_symbols:
                            continue
                        if not self.config_manager.is_symbol_enabled(symbol_upper):
                            continue
                        symbol_list.append(symbol_upper)
                        symbol_tasks.append(
                            self.spread_pipeline.process_symbol(symbol_upper))

                    if symbol_tasks:
                        results = await asyncio.gather(*symbol_tasks, return_exceptions=True)
                        for symbol, result in zip(symbol_list, results):
                            if isinstance(result, Exception):
                                logger.error(
                                    "❌ [统一调度] 处理 %s 异常: %s",
                                    symbol,
                                    result,
                                    exc_info=result,
                                )

                    # 🔥 处理多腿套利组合
                    if self.multi_leg_pairs:
                        await self._process_multi_leg_pairs(
                            dirty_keys if run_filtered else None
                        )

                    # 🔥 处理多交易所套利组合
                    if self.multi_exchange_pairs:
                        await self._process_trading_pairs(
                            dirty_keys if run_filtered else None
                        )

                    if run_full:
                        self._first_analysis_done = True

                    # 等待下一次循环
                    if self.event_driven_analysis_enabled and self.analysis_event is not None:
                        try:
                            await asyncio.wait_for(
                                self.analysis_event.wait(),
                                timeout=float(self.loop_interval or 0.1),
                            )
                            self.analysis_event.clear()
                        except asyncio.TimeoutError:
                            self.analysis_fallback_loop_count += 1
                    else:
                        await asyncio.sleep(self.loop_interval)

                except asyncio.CancelledError:
                    logger.info("⚠️  [统一调度] 主循环收到取消信号")
                    break
                except KeyboardInterrupt:
                    # 🔥 重要：重新抛出 KeyboardInterrupt，让外层捕获
                    logger.info("⚠️  [统一调度] 主循环收到中断信号 (Ctrl+C)")
                    raise
                except Exception as e:
                    logger.error(f"❌ [统一调度] 主循环异常: {e}", exc_info=True)
                    await asyncio.sleep(1)
        except KeyboardInterrupt:
            # 🔥 确保 KeyboardInterrupt 被传播到 start() 方法
            logger.info("⚠️  [统一调度] 主循环正在退出...")
            raise
        finally:
            logger.info("✅ [统一调度] 主循环已停止")

    async def _process_multi_leg_pairs(
        self,
        dirty_keys: Optional[Set[Tuple[str, str]]] = None,
    ):
        await self.spread_pipeline.process_multi_leg_pairs(dirty_keys)

    async def _process_trading_pairs(
        self,
        dirty_keys: Optional[Set[Tuple[str, str]]] = None,
    ):
        await self.spread_pipeline.process_trading_pairs(dirty_keys)

    def _get_funding_rate_data(
        self,
        symbol: str,
        exchange_buy: str,
        exchange_sell: str
    ) -> Optional[FundingRateData]:
        """兼容旧接口：委托 SpreadPipeline 获取资金费率。"""
        return self.spread_pipeline._get_funding_rate_data(symbol, exchange_buy, exchange_sell)

    def _capture_decision_snapshot(self, symbol: str, spread_data: SpreadData) -> None:
        """记录一次决策引擎状态，供心跳日志使用"""
        position = self.decision_engine.positions.get(symbol)
        quantity_epsilon = getattr(
            self.decision_engine, "quantity_epsilon", Decimal("0"))
        open_segments = position.get_segment_count() if position else 0
        completed_segments = (
            position.get_completed_segment_count(
                quantity_epsilon) if position else 0
        )
        active_segment = (
            position.get_active_incomplete_segment(
                quantity_epsilon) if position else None
        )
        if active_segment:
            next_segment_id = active_segment.segment_id
        elif position:
            next_segment_id = position.get_next_segment_id()
        else:
            next_segment_id = 1

        config = self.config_manager.get_config(symbol)
        grid_cfg = config.grid_config
        segment_index = max(0, next_segment_id - 1)
        next_threshold = grid_cfg.initial_spread_threshold + \
            segment_index * grid_cfg.grid_step

        net_spread_pct = spread_data.spread_pct
        calc_net = getattr(self.decision_engine, "_calculate_net_spread", None)
        if callable(calc_net):
            try:
                net_spread_pct = calc_net(spread_data)
            except Exception:
                pass

        self._decision_snapshots[symbol] = {
            "timestamp": datetime.utcnow().isoformat(timespec="seconds"),
            "spread_pct": spread_data.spread_pct,
            "net_spread_pct": net_spread_pct,
            "next_threshold_pct": next_threshold,
            "next_segment_id": next_segment_id,
            "open_segments": open_segments,
            "completed_segments": completed_segments,
            "max_segments": grid_cfg.max_segments,
        }

    def _log_status_summary(self) -> None:
        """
        周期性输出状态汇总,让用户快速了解系统当前状态
        包括:持仓情况、价差状态、流动性状态、风险控制状态
        """
        current_time = time.time()
        if current_time - self._last_status_summary_time < self.status_summary_interval:
            return

        self._last_status_summary_time = current_time

        # 构建符号池：单交易对 + 多腿 + 多交易所 + 决策引擎已有持仓
        symbol_pool: Set[str] = set(self.monitor_config.symbols)
        if self.multi_leg_pairs:
            symbol_pool.update(pair.pair_id for pair in self.multi_leg_pairs)
        if self.multi_exchange_pairs:
            symbol_pool.update(
                pair.trading_pair_id for pair in self.multi_exchange_pairs)
        symbol_pool.update(self.decision_engine.positions.keys())

        # 收集持仓信息
        positions_summary = []
        for symbol in sorted(symbol_pool):
            position = self.decision_engine.get_position(symbol)
            if position and position.total_quantity > self.decision_engine.quantity_epsilon:
                positions_summary.append(
                    f"{symbol}={position.total_quantity.normalize() if hasattr(position.total_quantity, 'normalize') else position.total_quantity}"
                )

        # 收集价差信息
        spread_summary = []
        for symbol in sorted(symbol_pool):
            spreads = self.symbol_spreads.get(symbol, [])
            if spreads:
                best_spread = max(spreads, key=lambda s: s.spread_pct)
                spread_summary.append(
                    f"{symbol}={best_spread.spread_pct:.4f}%"
                )

        # 收集reduce-only状态
        blocked_pairs = []
        if self.reduce_only_guard:
            for symbol in sorted(symbol_pool):
                if self.reduce_only_guard.is_pair_blocked(symbol):
                    blocked_pairs.append(symbol)

        # 构建汇总信息
        status_parts = []

        if positions_summary:
            status_parts.append(f"持仓: {', '.join(positions_summary)}")
        else:
            status_parts.append("持仓: 无")

        if spread_summary:
            status_parts.append(f"价差: {', '.join(spread_summary)}")

        if blocked_pairs:
            status_parts.append(f"受限: {', '.join(blocked_pairs)}")

        # 输出汇总日志
        logger.info("📊 [状态汇总] %s", " | ".join(status_parts))

    async def _status_summary_loop(self) -> None:
        """独立协程：输出状态汇总，避免阻塞主循环。"""
        try:
            while self.running:
                try:
                    self._log_status_summary()
                    await asyncio.sleep(0.2)
                except asyncio.CancelledError:
                    break
                except Exception:
                    await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            return

    def _log_decision_heartbeat(self) -> None:
        """按照固定间隔输出分段决策摘要，避免长时间无日志"""
        current_time = time.time()
        if current_time - self._last_decision_log_time < self.decision_log_interval:
            return

        self._last_decision_log_time = current_time

        if not self._decision_snapshots:
            logger.info("🫀 [分段决策] 心跳：暂无有效价差数据，等待订单簿更新")
            return

        lines = []
        for symbol in sorted(self._decision_snapshots.keys()):
            snap = self._decision_snapshots[symbol]
            lines.append(
                "  - {symbol}: spread={spread:.4f}% | net={net:.4f}% | "
                "next=第{next_id}段≥{threshold:.4f}% | segments {open_cnt}/{max_cnt}".format(
                    symbol=symbol,
                    spread=snap["spread_pct"],
                    net=snap["net_spread_pct"],
                    next_id=snap["next_segment_id"],
                    threshold=snap["next_threshold_pct"],
                    open_cnt=snap["open_segments"],
                    max_cnt=snap["max_segments"],
                )
            )

        logger.info("🫀 [分段决策] 心跳概览\n" + "\n".join(lines))
