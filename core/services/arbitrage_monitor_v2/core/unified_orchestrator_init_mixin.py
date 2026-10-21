"""
⚠️ 本文件由 `unified_orchestrator.py` 拆分生成（只做文件拆分，不改功能逻辑）。

职责：调度器初始化与基础工具（__init__/UI模式选择/symbol别名生成）。

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
from ..utils.sound_alarm import SoundAlarm
from .orchestrator_ui_controller import OrchestratorUIController
from .spread_pipeline import SpreadPipeline
from .reduce_only_probe_service import ReduceOnlyProbeService
from .orchestrator_bootstrap import OrchestratorBootstrap
from ..models import FundingRateData, PositionSegment, SegmentedPosition
from ..risk_control.global_risk_controller import GlobalRiskController
from ..risk_control.paradex_risk_controller import ParadexRiskController
from ..risk_control.open_only_close_guard import (
    OpenOnlyCloseGuard,
    OpenOnlyCloseConfig,
)
from .debug_state_printer import DebugStatePrinter
from ..state.symbol_state_manager import SymbolStateManager

from core.adapters.exchanges.interface import ExchangeInterface
from core.adapters.exchanges.models import OrderBookData

from .unified_orchestrator_logging import logger


class UnifiedOrchestratorInitMixin:
    """由 unified_orchestrator.py 拆分出的 mixin。"""

    async def _restart_exchange_stack_after_pause(self, *, trigger: str) -> None:
        """
        “暂停到期恢复”后的恢复动作：完整重启交易所连接栈（WS + REST + 订阅）。

        背景：
        - 仅做 WS 层 reconnect 往往只能恢复行情推送，不一定恢复账户/订单/余额等订阅与轮询。
        - 因此这里直接复用启动流程：disconnect_all_exchanges → connect_all_exchanges（含订阅行情）。
        """
        bootstrapper = getattr(self, "bootstrapper", None)
        do_disconnect_all = getattr(bootstrapper, "disconnect_all_exchanges", None) if bootstrapper else None
        do_connect_all = getattr(bootstrapper, "connect_all_exchanges", None) if bootstrapper else None
        if callable(do_disconnect_all) and callable(do_connect_all):
            logger.warning("🔌 [风控恢复] 开始完整重启交易所连接栈: trigger=%s", trigger)
            try:
                await do_disconnect_all()
            except Exception as exc:
                logger.error("❌ [风控恢复] disconnect_all_exchanges失败：%s", exc, exc_info=True)
            try:
                await do_connect_all()
            except Exception as exc:
                logger.error("❌ [风控恢复] connect_all_exchanges失败：%s", exc, exc_info=True)
            logger.warning("✅ [风控恢复] 完整重启流程完成: trigger=%s", trigger)
            return

        # 兜底：若 bootstrapper 不可用，则仅尝试重连适配器（可能不完整）
        adapters = getattr(self, "exchange_adapters", None) or {}
        if not adapters:
            logger.warning("🔌 [风控恢复] 未找到 exchange_adapters/bootstrapper，跳过恢复动作")
            return
        logger.warning(
            "🔌 [风控恢复] bootstrapper不可用，降级为逐交易所 disconnect+connect: trigger=%s, exchanges=%s",
            trigger,
            ",".join(sorted([str(k) for k in adapters.keys()])),
        )
        for ex, adapter in adapters.items():
            ex_name = (ex or "").strip()
            try:
                do_disconnect = getattr(adapter, "disconnect", None)
                do_connect = getattr(adapter, "connect", None)
                if callable(do_disconnect) and callable(do_connect):
                    await do_disconnect()
                    await do_connect()
                    logger.warning("🔌 [风控恢复] %s 已重连(disconnect+connect)", ex_name)
            except Exception as exc:
                logger.error("❌ [风控恢复] %s 重连失败：%s", ex_name, exc, exc_info=True)

    def _bind_global_risk_controller_callbacks(self) -> None:
        """
        绑定 GlobalRiskController 的回调：
        - on_pause/on_resume：用于在统一调度日志中可见
        - on_close_all_positions：余额严重不足时触发“强制平仓所有仓位”
        
        说明：
        - GlobalRiskController 在内部任务里调用这些回调（非 async/不 await），
          因此这里采用 create_task 的方式调度异步平仓，避免阻塞风险控制循环。
        - 监控模式（monitor_only_mode=True）下不做任何平仓动作。
        """
        rc = getattr(self, "risk_controller", None)
        if not rc:
            return

        self._position_risk_close_reasons = {}
        self._position_risk_close_failed = set()
        rc.position_times_provider = lambda: {
            key: min(segment.open_time for segment in position.get_open_segments())
            for key, position in self.decision_engine.positions.items()
            if position.total_quantity > 0 and position.get_open_segments()
        }
        rc.on_close_position = self._close_position_for_risk

        def _on_pause(reason: str) -> None:
            try:
                # 记录“本次暂停来源”，供 on_resume 决定是否需要“恢复时重连WS”
                try:
                    self._last_pause_reason = reason or ""
                    self._last_pause_was_slippage = ("滑点" in (reason or ""))
                except Exception:
                    pass
                # 🔔 仅对“余额相关”风控触发报警（避免执行失败暂停等场景刷响）
                if "余额" in (reason or ""):
                    # 支持 balance_management 声音开关（默认启用）
                    try:
                        bm = getattr(getattr(rc, "config", None), "balance_management", None)
                        if bm is not None and not bool(getattr(bm, "sound_alarm_enabled", True)):
                            logger.warning("🔕 [风控] 余额风控声音报警已禁用：%s", reason)
                            logger.warning("⏸️ [风控] 已暂停：%s", reason)
                            return
                    except Exception:
                        pass
                    try:
                        getattr(self, "_sound_alarm", None) and self._sound_alarm.trigger(
                            signature="balance_pause",
                            reason=reason,
                        )
                    except Exception:
                        pass
                logger.warning("⏸️ [风控] 已暂停：%s", reason)
            except Exception:
                return

        def _on_resume() -> None:
            try:
                logger.warning("▶️ [风控] 已恢复交易")

                # 若刚刚结束的是“滑点暂停”，恢复前先重连交易所WS适配器。
                # 注意：GlobalRiskController 的 on_resume 是非 async 回调，因此这里用 create_task。
                try:
                    if bool(getattr(self, "_last_pause_was_slippage", False)):
                        # 防抖：避免短时间反复恢复导致重复重连
                        now_ts = time.time()
                        last_ts = float(getattr(self, "_ws_reconnect_last_ts", 0.0) or 0.0)
                        if now_ts - last_ts >= 3.0:
                            self._ws_reconnect_last_ts = now_ts
                            # 🔥 打开“恢复闸门”：重连期间禁止开/平仓下单
                            self._ws_reconnect_in_progress = True

                            try:
                                loop = asyncio.get_running_loop()
                            except RuntimeError:
                                loop = None

                            async def _do_reconnect() -> None:
                                try:
                                    await self._restart_exchange_stack_after_pause(trigger="slippage_pause_resume")
                                finally:
                                    # 关闭“恢复闸门”
                                    self._ws_reconnect_in_progress = False

                            if loop is not None:
                                loop.create_task(_do_reconnect())
                            else:
                                # 无运行事件循环则无法重连：直接关闭闸门，避免永久阻塞
                                self._ws_reconnect_in_progress = False
                                logger.warning("🔌 [风控恢复] 当前无运行事件循环，跳过WS重连")
                        # 消费掉标记：只对一次恢复生效
                        self._last_pause_was_slippage = False
                except Exception:
                    # 不影响恢复日志
                    pass
            except Exception:
                return

        def _on_reconnect_exchange_stack(reason: str) -> None:
            """
            🔥 主动重连交易所连接栈（非暂停恢复场景）：
            - 由风控触发（例如高滑点连续出现）
            - 打开 _ws_reconnect_in_progress 闸门：重连期间禁止开/平仓下单
            """
            try:
                # 防抖：避免短时间反复触发导致重复重连
                now_ts = time.time()
                last_ts = float(getattr(self, "_ws_reconnect_last_ts", 0.0) or 0.0)
                if now_ts - last_ts < 3.0:
                    return
                self._ws_reconnect_last_ts = now_ts

                # 🔥 打开“恢复闸门”：重连期间禁止开/平仓下单
                self._ws_reconnect_in_progress = True

                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    # 无运行事件循环则无法重连：直接关闭闸门，避免永久阻塞
                    self._ws_reconnect_in_progress = False
                    logger.warning("🔌 [风控重连] 当前无运行事件循环，跳过WS重连。reason=%s", reason)
                    return

                async def _do_reconnect() -> None:
                    try:
                        await self._restart_exchange_stack_after_pause(trigger=f"high_slippage_reconnect:{reason}")
                    finally:
                        self._ws_reconnect_in_progress = False

                loop.create_task(_do_reconnect())
            except Exception:
                # 回调失败不影响风控主流程
                try:
                    self._ws_reconnect_in_progress = False
                except Exception:
                    pass
                return

        def _on_close_all_positions() -> None:
            try:
                # 🔔 余额严重不足触发强平：报警（若 on_pause 已触发也会被节流挡住）
                try:
                    bm = getattr(getattr(rc, "config", None), "balance_management", None)
                    if bm is not None and not bool(getattr(bm, "sound_alarm_enabled", True)):
                        logger.warning("🔕 [风控] 余额风控声音报警已禁用：force_close_all_positions")
                    else:
                        getattr(self, "_sound_alarm", None) and self._sound_alarm.trigger(
                            signature="balance_force_close",
                            reason="force_close_all_positions",
                        )
                except Exception:
                    pass
                # 监控模式下不执行平仓
                if bool(getattr(self, "monitor_only_mode", True)):
                    logger.warning("🧯 [风控] 触发强制平仓，但当前为监控模式，已跳过。")
                    return

                # 🔥 强平失败锁存：若之前强平失败/跳过/超时，则不再重复触发强平任务
                if bool(getattr(self, "_force_close_failed_latched", False)):
                    try:
                        now_ts = time.time()
                        last_ts = float(getattr(self, "_force_close_failed_latch_last_log_ts", 0.0) or 0.0)
                        if now_ts - last_ts >= 60.0:
                            self._force_close_failed_latch_last_log_ts = now_ts
                            reason = getattr(self, "_force_close_failed_latch_reason", None) or "force_close_failed_latched"
                            logger.error(
                                "⛔ [风控] 强制平仓已失败并锁存，后续将不再自动触发强平。reason=%s",
                                reason,
                            )
                    except Exception:
                        pass
                    return

                # 必须在运行中的事件循环内调度异步任务
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    logger.warning("🧯 [风控] 触发强制平仓，但当前无运行事件循环，已跳过。")
                    return

                async def _do_close() -> None:
                    if getattr(self, "_force_close_in_progress", False) or getattr(self, "_force_close_residual_in_progress", False):
                        return
                    try:
                        bm = getattr(getattr(rc, "config", None), "balance_management", None)
                        mode = str(getattr(bm, "force_close_mode", "all") or "all").strip().lower()
                    except Exception:
                        mode = "all"
                    try:
                        timeout_seconds = float(getattr(bm, "force_close_timeout_seconds", 300.0) or 300.0)
                    except Exception:
                        timeout_seconds = 300.0
                    timeout_seconds = max(1.0, timeout_seconds)

                    try:
                        if mode in ("grid_sequential", "grid", "sequential"):
                            logger.warning("🧯 [风控] 余额严重不足：开始强制分批平仓（按币种依次/每次一格）...")
                            ok = await self.force_close_all_positions_by_grid_sequential(
                                timeout_seconds=timeout_seconds,
                                reason="余额严重不足：强制分批平仓",
                            )
                            if ok:
                                logger.warning("✅ [风控] 强制分批平仓任务已结束（全部成功）。")
                                self._force_close_residual_in_progress = True
                                try:
                                    await asyncio.sleep(5.0)
                                    await self._force_close_exchange_cache_residual_positions(
                                        timeout_seconds=timeout_seconds,
                                        reason="强制分批平仓后残余持仓清理",
                                    )
                                finally:
                                    self._force_close_residual_in_progress = False
                                    self._mark_open_close_activity(action="close", symbol="__residual__")
                            else:
                                self._force_close_failed_latched = True
                                self._force_close_failed_latch_reason = "force_close_grid_sequential_failed"
                                logger.error("⛔ [风控] 强制分批平仓失败，已锁存：后续将不再自动触发强平。")
                        else:
                            if mode not in ("all", "", None):
                                logger.warning("⚠️ [风控] 未识别的 force_close_mode=%s，已回退为 all(一次性全平)", mode)
                            logger.warning("🧯 [风控] 余额严重不足：开始强制平仓所有仓位（一次性全平）...")
                            ok = await self.emergency_close_all_positions(timeout_seconds=timeout_seconds)
                            if ok:
                                logger.warning("✅ [风控] 强制平仓任务已完成（全部成功）。")
                            else:
                                self._force_close_failed_latched = True
                                self._force_close_failed_latch_reason = "force_close_all_failed"
                                logger.error("⛔ [风控] 强制平仓失败/跳过/超时，已锁存：后续将不再自动触发强平。")
                    except Exception as exc:
                        try:
                            self._force_close_failed_latched = True
                            self._force_close_failed_latch_reason = f"force_close_exception:{type(exc).__name__}"
                        except Exception:
                            pass
                        logger.error("❌ [风控] 强制平仓失败：%s", exc, exc_info=True)

                loop.create_task(_do_close())
            except Exception:
                # 不影响风控主流程
                return

        # 绑定回调（覆盖旧值）
        try:
            rc.on_pause = _on_pause
            rc.on_resume = _on_resume
            rc.on_close_all_positions = _on_close_all_positions
            if hasattr(rc, "on_reconnect_exchange_stack"):
                rc.on_reconnect_exchange_stack = _on_reconnect_exchange_stack
        except Exception:
            return

    def __init__(
        self,
        segmented_config_path: Optional[Path] = None,
        execution_config_path: Optional[Path] = None,
        monitor_config_path: Optional[Path] = None,
        multi_exchange_config_path: Optional[Path] = None,
        debug_config: Optional[DebugConfig] = None
    ):
        """
        初始化统一调度器

        Args:
            segmented_config_path: 分段配置文件路径
            monitor_config_path: 监控配置文件路径
            debug_config: Debug配置
        """
        # 加载配置
        self.execution_config_path = execution_config_path or Path("config/arbitrage/arbitrage_lighter_arcus.yaml")
        self.config_manager = SegmentedConfigManager(segmented_config_path)
        # 🔔 声音报警（用于余额风控等关键事件；可在 system_mode 中覆盖参数）
        self._sound_alarm = SoundAlarm.from_system_mode(self.config_manager.get_system_mode())
        self.monitor_config_manager = MonitorConfigManager(monitor_config_path)
        self.monitor_config = self.monitor_config_manager.get_config()
        self.debug = debug_config or DebugConfig.create_production()
        self.symbol_spreads: Dict[str, List[SpreadData]] = {}
        # 持仓一致性日志节流（仅在变更或超过间隔时打印）
        self._alignment_log_interval: float = 120.0
        self._last_alignment_snapshot: Dict[str, str] = {}
        self._last_alignment_log_time: Dict[str, float] = {}
        self._last_alignment_ui_data: Optional[Dict[str, Any]] = None
        self._unified_execution_config_manager: Optional[UnifiedConfigManager] = None
        # 🔥 强平失败锁存：一旦强平任务出现失败/跳过/超时，则锁存，后续不再重复触发强平
        self._force_close_failed_latched: bool = False
        self._force_close_failed_latch_reason: Optional[str] = None
        self._force_close_failed_latch_last_log_ts: float = 0.0
        # 首轮全量计算标记（后续按更新键增量计算）
        self._first_analysis_done: bool = False

        # 🔥 加载多腿套利配置
        self.multi_leg_pairs_manager = MultiLegPairsConfigManager()
        self.multi_leg_pairs: List[MultiLegPairConfig] = self.multi_leg_pairs_manager.get_pairs(
        )
        if self.multi_leg_pairs:
            logger.info(f"✅ [统一调度] 加载多腿套利配置: {len(self.multi_leg_pairs)} 对")
            for pair in self.multi_leg_pairs:
                logger.info(f"  - {pair.pair_id}: {pair.description}")

        # 🔥 加载多交易所套利配置
        mx_path: Optional[Path] = None
        try:
            # 优先级：启动参数 > monitor_config.multi_exchange_config_path > 默认文件
            if multi_exchange_config_path:
                mx_path = Path(str(multi_exchange_config_path))
            else:
                raw_path = getattr(self.monitor_config, "multi_exchange_config_path", None)
                if raw_path:
                    mx_path = Path(str(raw_path))
        except Exception:
            mx_path = None
        self.multi_exchange_config_manager = MultiExchangeArbitrageConfigManager(
            config_path=mx_path
        )
        self.multi_exchange_pairs: List[TradingPair] = self.multi_exchange_config_manager.get_pairs(
        )
        self.multi_exchange_symbols: Set[str] = set()
        if self.multi_exchange_pairs:
            for pair in self.multi_exchange_pairs:
                self.config_manager.register_config_alias(
                    pair.trading_pair_id, pair.symbol)
                self.multi_exchange_symbols.add(pair.normalized_symbol())
            logger.info("✅ [统一调度] 加载多交易所套利配置: %d 对",
                        len(self.multi_exchange_pairs))

        # 验证配置
        if not self.monitor_config_manager.validate():
            raise ValueError("监控配置验证失败")

        logger.info("✅ [统一调度] 配置加载完成")

        # 创建队列
        self.orderbook_queue = asyncio.Queue(
            maxsize=self.monitor_config.orderbook_queue_size)
        self.ticker_queue = asyncio.Queue(
            maxsize=self.monitor_config.ticker_queue_size)

        # “恢复阶段：WS重连中”闸门（用于滑点暂停到期恢复前的安全重连）
        self._ws_reconnect_in_progress: bool = False
        self._ws_reconnect_last_ts: float = 0.0
        self._last_pause_reason: str = ""
        self._last_pause_was_slippage: bool = False

        # 方案A：latest-only 数据通道（可选）
        self.latest_store: Optional[LatestStore] = None
        self.latest_event: Optional[asyncio.Event] = None
        if getattr(self.monitor_config, "latest_only_mode", False):
            self.latest_store = LatestStore()
            logger.info(
                "✅ [DataFlow] latest-only 模式已启用: poll_interval_ms=%s",
                getattr(self.monitor_config, "latest_poll_interval_ms", 50),
            )

        # 初始化交易所适配器
        self.exchange_adapters: Dict[str, ExchangeInterface] = {}
        self.bootstrapper = OrchestratorBootstrap(self)
        self._root_config: Optional[Dict[str, Any]] = None
        self.bootstrapper.init_exchange_adapters()
        # 监控模式下也可选启用 Variational WS events（用于手动下单测试）
        self._variational_monitor_ws_started: bool = False
        self.reduce_only_guard = ReduceOnlyGuard(ZoneInfo("Asia/Shanghai"))

        # 初始化数据分析模块
        self.spread_calculator = SpreadCalculator(self.debug)

        # 🔥 初始化错误避让控制器
        from ..risk_control.error_backoff_controller import ErrorBackoffController
        self.error_backoff_controller = ErrorBackoffController()
        logger.info("✅ [统一调度] 错误避让控制器已初始化")

        # 🔥 初始化V2统一决策引擎（总量驱动 + 剥头皮状态机）
        self.decision_engine = UnifiedDecisionEngine(
            config_manager=self.config_manager
        )
        # 注入避让控制器
        self.decision_engine._backoff_controller = self.error_backoff_controller
        logger.info("✅ [统一调度] V2统一决策引擎已初始化（总量驱动算法）")

        # 🔥 将避让控制器注入到交易所适配器
        for exchange_name, adapter in self.exchange_adapters.items():
            # 适配器自身
            if hasattr(adapter, '_backoff_controller'):
                adapter._backoff_controller = self.error_backoff_controller
                logger.info(f"✅ [统一调度] 错误避让控制器已注入到 {exchange_name}")
                # 若适配器提供“局部重启”能力，则注册重启钩子（例如 nonce 异常时重建 REST/WS）
                if hasattr(adapter, "restart_connections"):
                    try:
                        self.error_backoff_controller.set_restart_hook(
                            exchange_name,
                            adapter.restart_connections
                        )
                        logger.info("✅ [统一调度] %s 已注册局部重启钩子 (21104 时触发)", exchange_name)
                    except Exception as e:
                        logger.warning(f"⚠️ [统一调度] 注册 {exchange_name} 重启钩子失败: {e}")

            # Lighter REST 层
            if hasattr(adapter, '_rest') and adapter._rest:
                rest = adapter._rest
                if hasattr(rest, '_backoff_controller'):
                    rest._backoff_controller = self.error_backoff_controller
                    logger.info(f"✅ [统一调度] 错误避让控制器已注入到 {exchange_name}.rest")

                # Lighter WebSocket 层
                if hasattr(rest, '_websocket') and rest._websocket:
                    ws = rest._websocket
                    if hasattr(ws, '_backoff_controller'):
                        ws._backoff_controller = self.error_backoff_controller
                        logger.info(
                            f"✅ [统一调度] 错误避让控制器已注入到 {exchange_name}.websocket")

            # 若适配器本身就包含 websocket（如直接使用 REST 适配器实例）
            if hasattr(adapter, '_websocket') and adapter._websocket:
                ws = adapter._websocket
                if hasattr(ws, '_backoff_controller'):
                    ws._backoff_controller = self.error_backoff_controller
                    logger.info(
                        f"✅ [统一调度] 错误避让控制器已注入到 {exchange_name}.websocket")

        # 初始化风险控制器
        risk_config = self._load_risk_control_config()
        self.risk_controller = GlobalRiskController(
            risk_config=risk_config,
            exchange_adapters=self.exchange_adapters
        )

        # ------------------------------------------------------------------
        # 🔥 将“API失败上报回调”注入到交易所适配器（以及其内部 REST/WS 子模块）
        #
        # 目标：
        # - 让 WS断连 / 行情quote/ticker失败 等事件也能触发 api_failure_pause；
        # - 不破坏分层：适配器层不直接依赖 GlobalRiskController，仅调用注入的回调。
        # ------------------------------------------------------------------
        try:
            reporter = getattr(self.risk_controller, "record_api_failure", None)
            if callable(reporter):
                for ex_name, adapter in (self.exchange_adapters or {}).items():
                    try:
                        setattr(adapter, "_api_failure_reporter", reporter)
                    except Exception:
                        pass
                    # 常见子模块：_websocket / _rest / _base（不同交易所有不同组合）
                    for comp in (
                        getattr(adapter, "_websocket", None),
                        getattr(adapter, "_rest", None),
                        getattr(adapter, "_base", None),
                    ):
                        if comp is None:
                            continue
                        try:
                            setattr(comp, "_api_failure_reporter", reporter)
                        except Exception:
                            pass
        except Exception:
            # 注入失败不影响主流程
            pass
        # ✅ 启动时明确打印：执行失败暂停风控是否启用
        try:
            if getattr(self.risk_controller, "is_execution_pause_enabled", lambda: False)():
                logger.warning(
                    "🧷 [风控] execution_pause已启用: consecutive_failures=%s | pause_minutes=%s",
                    getattr(self.risk_controller,
                            "get_execution_failure_threshold", lambda: None)(),
                    getattr(self.risk_controller,
                            "get_execution_pause_minutes", lambda: None)(),
                )
        except Exception:
            pass

        # ✅ 启动时明确打印：滑点暂停/高滑点重连风控当前生效值（避免“改了YAML但没重启/没生效”不易排查）
        try:
            sp = getattr(risk_config, "slippage_pause", None)
            if sp is not None and bool(getattr(sp, "enabled", False)):
                hsr = getattr(sp, "high_slippage_reconnect", None)
                bns = getattr(sp, "burst_negative_slippage_pause", None)
                logger.warning(
                    "🧷 [风控] slippage_pause已启用: threshold=%.4f%% | pause_seconds=%s | cooldown_seconds=%s | apply_to_open=%s | apply_to_close=%s | burst_negative_slippage_pause=%s(win=%ss cnt=%s) | high_slippage_reconnect=%s(win=%s cnt=%s th=%.6f%% cd=%ss)",
                    float(getattr(sp, "negative_slippage_threshold_pct", 0.0) or 0.0),
                    float(getattr(sp, "pause_seconds", 0.0) or 0.0),
                    float(getattr(sp, "cooldown_seconds", 0.0) or 0.0),
                    "ON" if bool(getattr(sp, "apply_to_open", True)) else "OFF",
                    "ON" if bool(getattr(sp, "apply_to_close", True)) else "OFF",
                    "ON" if (bns is not None and bool(getattr(bns, "enabled", False))) else "OFF",
                    float(getattr(bns, "window_seconds", 0.0) or 0.0) if bns is not None else 0.0,
                    int(getattr(bns, "count_threshold", 0) or 0) if bns is not None else 0,
                    "ON" if (hsr is not None and bool(getattr(hsr, "enabled", False))) else "OFF",
                    int(getattr(hsr, "window_trades", 0) or 0) if hsr is not None else 0,
                    int(getattr(hsr, "count_threshold", 0) or 0) if hsr is not None else 0,
                    float(getattr(hsr, "high_slippage_threshold_pct", 0.0) or 0.0) if hsr is not None else 0.0,
                    float(getattr(hsr, "cooldown_seconds", 0.0) or 0.0) if hsr is not None else 0.0,
                )
        except Exception:
            pass

        # 分段网格：开仓限制（只允许平仓）
        self._open_only_close_guard = OpenOnlyCloseGuard(
            cfg=getattr(risk_config, "open_only_close", OpenOnlyCloseConfig()),
            get_exchange_equity_usdc=self._get_cached_exchange_equity_usdc,
            get_exchange_position_value_usdc=self._get_cached_exchange_position_value_usdc,
        )
        try:
            ooc = getattr(risk_config, "open_only_close", None)
            low_cfg = getattr(ooc, "low_balance", None)
            max_cfg = getattr(ooc, "max_exchange_position_value", None)
            dyn_cfg = getattr(ooc, "dynamic_max_exchange_position_value", None)
            vol_cfg = getattr(ooc, "price_volatility", None)
            low_enabled = bool(getattr(low_cfg, "enabled", False))
            max_enabled = bool(getattr(max_cfg, "enabled", False))
            dyn_enabled = bool(getattr(dyn_cfg, "enabled", False))
            vol_enabled = bool(getattr(vol_cfg, "enabled", False))
            if low_enabled or max_enabled or dyn_enabled or vol_enabled:
                logger.warning(
                    "🧷 [风控] open_only_close已启用: "
                    "low_balance=%s(th=%s) | "
                    "max_exchange_position_value=%s(th=%s) | "
                    "dynamic_max_exchange_position_value=%s(mult=%s) | "
                    "price_volatility=%s(win=%ss th=%s%% block=%ss)",
                    "ON" if low_enabled else "OFF",
                    getattr(low_cfg, "min_equity_usdc", 0.0),
                    "ON" if max_enabled else "OFF",
                    getattr(max_cfg, "max_value_usdc", 0.0),
                    "ON" if dyn_enabled else "OFF",
                    getattr(dyn_cfg, "equity_multiplier", 0.0),
                    "ON" if vol_enabled else "OFF",
                    getattr(vol_cfg, "window_seconds", 0.0),
                    getattr(vol_cfg, "threshold_pct", 0.0),
                    getattr(vol_cfg, "block_seconds", 0.0),
                )
        except Exception:
            # 不影响主流程
            pass

        # 单套利对状态管理（运行/等待）
        self.symbol_state_manager = SymbolStateManager()

        # 初始化执行器
        execution_config = self._load_execution_module_config()
        self.decision_engine.exchange_order_modes = execution_config.exchange_order_modes
        self.decision_engine.exchange_fee_config = dict(execution_config.exchange_fee_config)
        self.decision_engine.exchange_fee_config.update(self._load_decision_settings()["exchange_fee_config"])
        self.decision_engine.deduct_fees = bool(self._load_root_config().get("decision", {}).get("deduct_fees", True))
        # 获取monitor_only配置
        monitor_only = self.config_manager.get_system_mode().get('monitor_only', True)
        self.monitor_only_mode = monitor_only

        # 🔥 绑定全局风控回调（余额严重不足 -> 强制平仓等）
        self._bind_global_risk_controller_callbacks()

        self.executor = ArbitrageExecutor(
            execution_config=execution_config,
            exchange_adapters=self.exchange_adapters,
            monitor_only=monitor_only,
            is_segmented_mode=True,  # 🔥 分段模式：禁用轮次间隔，允许快速拆单补仓
            reduce_only_guard=self.reduce_only_guard,
            symbol_state_manager=self.symbol_state_manager,
        )

        # ------------------------------------------------------------------
        # 🛡️ Paradex 专属风控（局部风控，不影响全局风控）
        # - 仅当 exchanges 包含 paradex 时启用
        # - 通过执行器的 per-exchange 钩子注入：互相独立、互相补充
        # ------------------------------------------------------------------
        try:
            paradex_adapter = None
            for k, v in (self.exchange_adapters or {}).items():
                if (k or "").strip().lower() == "paradex":
                    paradex_adapter = v
                    break
            prc_cfg = getattr(execution_config, "paradex_risk_control", None)
            if paradex_adapter is not None and prc_cfg is not None and bool(getattr(prc_cfg, "enabled", False)):
                self._paradex_risk_controller = ParadexRiskController(
                    prc_cfg, paradex_adapter=paradex_adapter
                )

                def _exchange_blocker(ex: str, req) -> tuple[bool, Optional[str], Optional[str]]:
                    # ✅ 重要：套利是“两腿整体行为”，任何一腿触发风控都必须整体阻断，
                    # 否则会出现单腿下单风险（例如只下了 VAR）。
                    #
                    # 做法：
                    # - 仅当本次请求涉及 paradex 时才检查 Paradex 风控
                    # - 一旦 Paradex 风控触发：对任意 exchange 都返回 blocked=True，
                    #   让执行器在入口处直接终止本次执行计划（两腿都不下单）
                    buy_ex = str(getattr(req, "exchange_buy", "") or "").strip().lower()
                    sell_ex = str(getattr(req, "exchange_sell", "") or "").strip().lower()
                    if "paradex" not in {buy_ex, sell_ex}:
                        return False, None, None

                    blocked, reason, code = self._paradex_risk_controller.should_block_trade(
                        exchange="paradex",
                        is_open=bool(getattr(req, "is_open", False)),
                    )
                    if not blocked:
                        return False, None, None
                    # 全局阻断：对任意 ex 返回 blocked
                    return True, f"[GLOBAL_BLOCK] {reason or 'paradex_blocked'}", (code or "paradex_blocked")

                if hasattr(self.executor, "set_exchange_trade_blocker"):
                    self.executor.set_exchange_trade_blocker(_exchange_blocker)
                if hasattr(self.executor, "set_exchange_order_error_reporter"):
                    self.executor.set_exchange_order_error_reporter(
                        lambda ex, label, exc, attempt: self._paradex_risk_controller.record_order_error(
                            exchange=ex,
                            order_type_label=label,
                            exception=exc,
                            attempt=attempt,
                        )
                    )
                logger.warning(
                    "🛡️ [风控] 已启用 Paradex 专属风控：触发时将整体阻断本次套利请求（避免单腿风险）"
                )
        except Exception:
            pass
        # ------------------------------------------------------------------
        # 🔥 注入“下单提交 -> WS成交推送”耗时上报回调（触发风控：WS成交延迟暂停）
        # - 执行器不直接依赖 GlobalRiskController，仅调用注入回调
        # ------------------------------------------------------------------
        try:
            reporter = getattr(
                self.risk_controller, "record_order_ws_latency_pause", None)
            if callable(reporter) and hasattr(self.executor, "set_order_ws_latency_reporter"):
                self.executor.set_order_ws_latency_reporter(reporter)
        except Exception:
            pass

        # ------------------------------------------------------------------
        # 🔥 注入“全局暂停闸门”：用于暂停触发后立刻阻断新的开/平仓下单
        # - 触发点可能发生在同一轮循环的其他任务中（并发），因此需要在执行器入口再做一次检查
        # ------------------------------------------------------------------
        try:
            # 使用 risk_status.is_paused 作为统一闸门（覆盖所有全局暂停原因）：
            # - 滑点暂停 / WS成交延迟暂停 / API失败暂停 / 执行失败暂停 / 网络故障暂停 / 维护暂停 等
            # 目标：一旦系统进入“全局暂停”，后续新开/平仓下单立即被拦截
            if hasattr(self.executor, "set_global_pause_checker"):
                rc = self.risk_controller

                def _pause_checker():
                    try:
                        # 恢复阶段：WS重连中（比 risk_status.is_paused 更高优先级）
                        if bool(getattr(self, "_ws_reconnect_in_progress", False)):
                            return True, "恢复阶段：重连WS中"
                        status = rc.get_risk_status()
                        if bool(getattr(status, "is_paused", False)):
                            reason = getattr(status, "pause_reason", None)
                            return True, (reason or "全局暂停中")
                        return False, None
                    except Exception:
                        return False, None

                self.executor.set_global_pause_checker(_pause_checker)
        except Exception:
            pass

        # 初始化展示组件
        self.scroller = RealtimeScroller()

        # 初始化数据模块
        self.data_receiver = DataReceiver(
            self.orderbook_queue,
            self.ticker_queue,
            self.debug,
            latest_store=self.latest_store,
            latest_only_mode=getattr(
                self.monitor_config, "latest_only_mode", False),
        )

        self.data_processor = DataProcessor(
            self.orderbook_queue,
            self.ticker_queue,
            self.debug,
            scroller=self.scroller,
            latest_store=self.latest_store,
            latest_only_mode=getattr(
                self.monitor_config, "latest_only_mode", False),
            latest_poll_interval_ms=getattr(
                self.monitor_config, "latest_poll_interval_ms", 50),
            latest_event=self.latest_event,
            time_budget_ms=getattr(
                self.monitor_config, "data_processor_time_budget_ms", 5),
        )
        # ✅ 将 DataProcessor 注入执行器，供 limit_market 等路径直接读取盘口缓存
        try:
            setattr(self.executor, "data_processor", self.data_processor)
        except Exception:
            pass
        self.executor.before_open_check = self._validate_segmented_open
        self.executor.before_close_check = self._validate_segmented_close
        self.executor.market_constraints = self._load_root_config().get("market_constraints", {})
        self.executor.set_live_price_resolver(
            self._resolve_live_price_from_cache)

        # 初始化UI
        self.ui_manager = UIManager(self.debug, self.scroller)
        self.ui_controller = OrchestratorUIController(self, logger)
        self.spread_pipeline = SpreadPipeline(self)
        self.reduce_only_probe_service = ReduceOnlyProbeService(self)
        self.debug_cli_printer: Optional[DebugStatePrinter] = None

        # 运行状态
        self.running = False
        self.loop_interval = (
            float(getattr(self.monitor_config, "loop_interval_ms", 100)) / 1000.0
        )  # 主循环间隔（秒）
        # 事件驱动分析配置
        self.event_driven_analysis_enabled = bool(
            getattr(self.monitor_config, "event_driven_analysis_enabled", False)
        )
        self.event_driven_analysis_min_interval_ms = int(
            getattr(self.monitor_config, "event_driven_analysis_min_interval_ms", 0)
            or 0
        )
        self.analysis_event: Optional[asyncio.Event] = None
        self.analysis_fallback_loop_count: int = 0
        # 端到端信号延迟（用于UI观测，不写日志）
        self._analysis_anchor_ts: Dict[str, float] = {}
        self.decision_signal_latency_ms: Optional[float] = None
        self.decision_signal_latency_label: Optional[str] = None

        # 数据新鲜度配置
        self.data_freshness_seconds = self.config_manager.get_system_mode().get(
            'data_freshness_seconds', 3.0)
        # 数据新鲜度（按交易所覆盖，可选）
        # - 仅当 system_mode.data_freshness_seconds_by_exchange 里显式配置某交易所时生效
        # - 未配置的交易所：沿用全局 data_freshness_seconds
        self.data_freshness_seconds_by_exchange: Dict[str, float] = {}
        try:
            raw_map = (self.config_manager.get_system_mode() or {}).get(
                'data_freshness_seconds_by_exchange', {}
            )
            if isinstance(raw_map, dict):
                for k, v in raw_map.items():
                    ex = str(k or "").strip().lower()
                    if not ex:
                        continue
                    try:
                        self.data_freshness_seconds_by_exchange[ex] = float(v)
                    except Exception:
                        continue
        except Exception:
            self.data_freshness_seconds_by_exchange = {}

        # 🔧 把“数据新鲜度阈值”同步给执行器（执行器/limit_market 会读取）
        # - 避免执行器只看到全局值，无法使用 by_exchange 覆盖（例如 grvt=5s）
        try:
            setattr(self.executor, "data_freshness_seconds", float(self.data_freshness_seconds))
            setattr(
                self.executor,
                "data_freshness_seconds_by_exchange",
                dict(self.data_freshness_seconds_by_exchange or {}),
            )
        except Exception:
            pass

        # ✅ 启动时打印一次“数据新鲜度(秒)”最终生效值，方便确认配置是否生效
        try:
            ex_list = list(getattr(self.monitor_config, "exchanges", []) or [])
            resolved = [
                f"{str(ex).strip().lower()}={self.get_data_freshness_seconds(str(ex)):.3f}s"
                for ex in ex_list
            ]
            overrides = ",".join(
                [f"{k}={v:.3f}s" for k, v in sorted((self.data_freshness_seconds_by_exchange or {}).items())]
            )
            logger.warning(
                "📊 [统一调度] 数据新鲜度: global=%.3fs, overrides={%s}, resolved=[%s]",
                float(self.data_freshness_seconds),
                overrides,
                ", ".join(resolved),
            )
        except Exception:
            pass

        # Ticker日志控制
        self._missing_ticker_logged: Set[Tuple[str, str]] = set()

        # 决策心跳日志控制
        self.decision_log_interval: float = 60.0  # 默认每分钟输出一次
        self._last_decision_log_time: float = 0.0
        self._decision_snapshots: Dict[str, Dict[str, Any]] = {}

        # 🔥 状态汇总日志控制
        self.status_summary_interval: float = 60.0  # 每60秒输出一次状态汇总
        self._last_status_summary_time: float = 0.0
        self._liquidity_failure_logger = LiquidityFailureLogger(logger)
        self._throttled_logger = ThrottledLogger(logger)

        # EMA价差门槛：按 (symbol, action) 维护独立状态
        # action: "open" / "close"
        self._ema_spread_gate_states: Dict[
            Tuple[str, str],
            Tuple[float, bool, EmaSpreadGateState],
        ] = {}
        self._persistence_log_records: Dict[str, Tuple[float, float]] = {}
        self._open_intent_log_records: Dict[str, Tuple[float, float]] = {}
        # EMA门槛启用日志标志位：记录每个交易对的启用日志是否已打印（避免重复打印）
        # (symbol, action)
        self._ema_gate_enabled_logged: Set[Tuple[str, str]] = set()

        # 风控工具（供门槛/节流复用）
        self.risk_utils = RiskControlUtils(self)

        # 🔐 组合级执行锁，防止同一交易所组合重复触发
        self._pending_open_pairs: Set[str] = set()
        # 记录每个 open_key 的开始时间（用于诊断/清理“残留in-flight”）
        self._pending_open_pairs_started_at: Dict[str, float] = {}
        self._pending_close_symbols: Set[str] = set()
        self._pending_open_lock = asyncio.Lock()
        self._pending_close_lock = asyncio.Lock()

        # 🔥 根据决策引擎类型设置UI模式
        self._determine_ui_mode()

        logger.info("✅ [统一调度] 统一调度器初始化完成")

    def get_data_freshness_seconds(self, exchange: str) -> float:
        """
        获取“数据新鲜度阈值(秒)”：
        - 若 system_mode.data_freshness_seconds_by_exchange 显式配置了该交易所：用覆盖值
        - 否则：用全局 data_freshness_seconds
        """
        ex = (exchange or "").strip().lower()
        try:
            v = (self.data_freshness_seconds_by_exchange or {}).get(ex)
            return float(v) if v is not None else float(self.data_freshness_seconds)
        except Exception:
            return float(self.data_freshness_seconds)

    def _determine_ui_mode(self):
        """
        根据决策引擎类型确定UI模式

        逻辑：
        - 如果使用 UnifiedDecisionEngine (V2) -> SEGMENTED_GRID 模式
        - 其他情况保持原有行为（ARBITRAGE_V3 或 MONITOR）
        """
        from ..display.ui_manager import UIMode

        # 检查决策引擎类型
        if hasattr(self, "decision_engine") and isinstance(self.decision_engine, UnifiedDecisionEngine):
            # 🔥 分段网格模式
            self.ui_manager.set_ui_mode(UIMode.SEGMENTED_GRID)
            logger.info("🎨 [统一调度] UI模式: 分段网格模式 (SEGMENTED_GRID)")
        else:
            # 保持原有逻辑（V3基础模式或监控模式）
            # 这里不需要额外设置，已经在 set_v3_mode 中处理
            logger.info(
                f"🎨 [统一调度] UI模式: {'V3基础模式' if self.ui_manager.is_v3_mode else '监控模式'}")

    @staticmethod
    def _build_symbol_aliases(symbol: Optional[str]) -> List[str]:
        """
        针对不同交易所的交易对命名差异，生成若干别名用于缓存匹配。
        """
        if not symbol:
            return []
        candidates = {
            symbol,
            symbol.upper(),
            symbol.lower(),
            symbol.replace("-", "_"),
            symbol.replace("_", "-"),
            symbol.replace("/", "-"),
            symbol.replace("/", "_"),
        }
        return [item for item in candidates if item]
