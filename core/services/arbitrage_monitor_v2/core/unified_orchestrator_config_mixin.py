"""
⚠️ 本文件由 `unified_orchestrator.py` 拆分生成（只做文件拆分，不改功能逻辑）。

职责：配置读取/解析与缓存读取（执行层/风控/根配置、UI缓存的余额与持仓价值）。

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


class UnifiedOrchestratorConfigMixin:
    """由 unified_orchestrator.py 拆分出的 mixin。"""

    def _load_execution_module_config(self) -> ExecutionConfig:
        """
        载入执行层配置（含下单模式、双限价开关等）

        优先读取统一配置文件 `config/arbitrage/arbitrage_unified.yaml`，
        仅当文件缺失或解析失败时才退回默认配置，确保分段模式也能
        复用同一套执行参数。
        """
        config_path = self.execution_config_path
        if not config_path.exists():
            logger.warning(
                "⚠️ [统一调度] 未找到统一执行配置文件 (%s)，"
                "将使用默认执行配置（市价优先）",
                config_path
            )
            return ExecutionConfig()

        try:
            self._unified_execution_config_manager = UnifiedConfigManager(
                config_path=config_path
            )
            unified_config = self._unified_execution_config_manager.get_unified_config()
            exec_config = unified_config.execution
            dual_limit_flag = getattr(
                exec_config.order_execution,
                "enable_dual_limit_mode",
                False
            )
            logger.info(
                "✅ [统一调度] 执行层配置加载完成: dual_limit=%s, order_modes=%d",
                "ON" if dual_limit_flag else "OFF",
                len(exec_config.exchange_order_modes)
            )
            return exec_config
        except Exception as exc:
            logger.warning(
                "⚠️ [统一调度] 读取统一执行配置失败，改用默认执行配置: %s",
                exc,
                exc_info=True
            )
            return ExecutionConfig()

    def _load_risk_control_config(self) -> RiskControlConfig:
        """从配置文件加载全局风险控制参数（如余额阈值）"""
        risk_config = RiskControlConfig()

        data = self._load_root_config()
        rc_data = data.get('risk_control', {})
        duration_data = rc_data.get('position_duration', {})
        duration = risk_config.position_duration
        duration.enabled = bool(duration_data.get('enabled', duration.enabled))
        duration.max_position_duration = int(duration_data.get('max_position_duration', duration.max_position_duration))
        duration.auto_close_on_timeout = bool(duration_data.get('auto_close_on_timeout', duration.auto_close_on_timeout))
        bm_data = rc_data.get('balance_management', {})

        balance_cfg = risk_config.balance_management
        balance_cfg.min_balance_warning = float(
            bm_data.get('min_balance_warning', balance_cfg.min_balance_warning)
        )
        balance_cfg.min_balance_close_position = float(
            bm_data.get('min_balance_close_position',
                        balance_cfg.min_balance_close_position)
        )
        # 按交易所覆盖（可选）
        try:
            by_ex_warn = bm_data.get("min_balance_warning_by_exchange", {}) or {}
            if isinstance(by_ex_warn, dict):
                balance_cfg.min_balance_warning_by_exchange = {
                    str(k): float(v) for k, v in by_ex_warn.items()
                }
        except Exception:
            pass
        try:
            by_ex_close = bm_data.get("min_balance_close_position_by_exchange", {}) or {}
            if isinstance(by_ex_close, dict):
                balance_cfg.min_balance_close_position_by_exchange = {
                    str(k): float(v) for k, v in by_ex_close.items()
                }
        except Exception:
            pass
        balance_cfg.check_interval = int(
            bm_data.get('check_interval', balance_cfg.check_interval)
        )
        try:
            balance_cfg.sound_alarm_enabled = bool(
                bm_data.get('sound_alarm_enabled',
                            balance_cfg.sound_alarm_enabled)
            )
        except Exception:
            pass
        try:
            mode = bm_data.get("force_close_mode", balance_cfg.force_close_mode)
            balance_cfg.force_close_mode = str(mode).strip().lower() if mode is not None else balance_cfg.force_close_mode
        except Exception:
            pass

        # 分段网格：只允许平仓开关/阈值
        ooc = rc_data.get("open_only_close", {}) or {}
        low_balance = ooc.get("low_balance", {}) or {}
        max_value = ooc.get("max_exchange_position_value", {}) or {}
        dyn_max_value = ooc.get(
            "dynamic_max_exchange_position_value", {}) or {}
        vol_data = ooc.get("price_volatility", {}) or {}
        try:
            risk_config.open_only_close.low_balance.enabled = bool(
                low_balance.get("enabled", False))
            risk_config.open_only_close.low_balance.min_equity_usdc = float(
                low_balance.get("min_equity_usdc", 0.0))
            risk_config.open_only_close.low_balance.sound_alarm_enabled = bool(
                low_balance.get(
                    "sound_alarm_enabled", risk_config.open_only_close.low_balance.sound_alarm_enabled)
            )
            by_ex = low_balance.get("min_equity_usdc_by_exchange", {}) or {}
            if isinstance(by_ex, dict):
                risk_config.open_only_close.low_balance.min_equity_usdc_by_exchange = {
                    str(k): float(v) for k, v in by_ex.items()
                }
        except Exception:
            pass
        try:
            risk_config.open_only_close.max_exchange_position_value.enabled = bool(
                max_value.get("enabled", False))
            risk_config.open_only_close.max_exchange_position_value.max_value_usdc = float(
                max_value.get("max_value_usdc", 0.0)
            )
            risk_config.open_only_close.max_exchange_position_value.sound_alarm_enabled = bool(
                max_value.get(
                    "sound_alarm_enabled",
                    risk_config.open_only_close.max_exchange_position_value.sound_alarm_enabled,
                )
            )
            by_ex2 = max_value.get("max_value_usdc_by_exchange", {}) or {}
            if isinstance(by_ex2, dict):
                risk_config.open_only_close.max_exchange_position_value.max_value_usdc_by_exchange = {
                    str(k): float(v) for k, v in by_ex2.items()
                }
        except Exception:
            pass
        try:
            risk_config.open_only_close.dynamic_max_exchange_position_value.enabled = bool(
                dyn_max_value.get("enabled", False)
            )
            risk_config.open_only_close.dynamic_max_exchange_position_value.equity_multiplier = float(
                dyn_max_value.get("equity_multiplier", 0.0)
            )
            by_ex3 = dyn_max_value.get("equity_multiplier_by_exchange", {}) or {}
            if isinstance(by_ex3, dict):
                risk_config.open_only_close.dynamic_max_exchange_position_value.equity_multiplier_by_exchange = {
                    str(k): float(v) for k, v in by_ex3.items()
                }
        except Exception:
            pass
        try:
            pv = risk_config.open_only_close.price_volatility
            pv.enabled = bool(vol_data.get("enabled", pv.enabled))
            pv.window_seconds = float(vol_data.get("window_seconds", pv.window_seconds))
            pv.threshold_pct = float(vol_data.get("threshold_pct", pv.threshold_pct))
            pv.block_seconds = float(vol_data.get("block_seconds", pv.block_seconds))
            pv.apply_to_all_symbols = bool(
                vol_data.get("apply_to_all_symbols", pv.apply_to_all_symbols)
            )
            pv.sound_alarm_enabled = bool(
                vol_data.get("sound_alarm_enabled", pv.sound_alarm_enabled)
            )
            raw_triggers = vol_data.get("trigger_symbols", pv.trigger_symbols)
            trigger_list: List[str] = []
            if isinstance(raw_triggers, str):
                trigger_list = [s.strip() for s in raw_triggers.split(",") if s.strip()]
            elif isinstance(raw_triggers, list):
                trigger_list = [str(s).strip() for s in raw_triggers if str(s).strip()]
            pv.trigger_symbols = [s.upper() for s in trigger_list]
        except Exception:
            pass

        # 执行失败暂停（全局）
        ep = rc_data.get("execution_pause", {}) or {}
        try:
            risk_config.execution_pause.enabled = bool(
                ep.get("enabled", risk_config.execution_pause.enabled))
            risk_config.execution_pause.consecutive_failures = int(
                ep.get("consecutive_failures",
                       risk_config.execution_pause.consecutive_failures)
            )
            risk_config.execution_pause.pause_minutes = float(
                ep.get("pause_minutes", risk_config.execution_pause.pause_minutes)
            )
        except Exception:
            # 不影响主流程
            pass

        # 滑点触发暂停（全局暂停，禁止开仓 & 平仓）
        sp = rc_data.get("slippage_pause", {}) or {}
        try:
            risk_config.slippage_pause.enabled = bool(
                sp.get("enabled", risk_config.slippage_pause.enabled)
            )
            risk_config.slippage_pause.negative_slippage_threshold_pct = float(
                sp.get(
                    "negative_slippage_threshold_pct",
                    risk_config.slippage_pause.negative_slippage_threshold_pct,
                )
            )
            risk_config.slippage_pause.pause_seconds = float(
                sp.get("pause_seconds", risk_config.slippage_pause.pause_seconds)
            )
            risk_config.slippage_pause.cooldown_seconds = float(
                sp.get("cooldown_seconds",
                       risk_config.slippage_pause.cooldown_seconds)
            )
            risk_config.slippage_pause.apply_to_open = bool(
                sp.get("apply_to_open", risk_config.slippage_pause.apply_to_open)
            )
            risk_config.slippage_pause.apply_to_close = bool(
                sp.get("apply_to_close", risk_config.slippage_pause.apply_to_close)
            )

            # 负滑点“连续多次”触发暂停（滚动窗口计数）
            bns = sp.get("burst_negative_slippage_pause", {}) or {}
            risk_config.slippage_pause.burst_negative_slippage_pause.enabled = bool(
                bns.get(
                    "enabled",
                    risk_config.slippage_pause.burst_negative_slippage_pause.enabled,
                )
            )
            risk_config.slippage_pause.burst_negative_slippage_pause.window_seconds = float(
                bns.get(
                    "window_seconds",
                    risk_config.slippage_pause.burst_negative_slippage_pause.window_seconds,
                )
            )
            risk_config.slippage_pause.burst_negative_slippage_pause.count_threshold = int(
                bns.get(
                    "count_threshold",
                    risk_config.slippage_pause.burst_negative_slippage_pause.count_threshold,
                )
            )
        except Exception:
            pass

        # 🔥 高滑点“近N次成功成交”触发重连（不暂停）
        hsr = sp.get("high_slippage_reconnect", {}) or {}
        try:
            cfg = getattr(risk_config.slippage_pause, "high_slippage_reconnect", None)
            if cfg is not None and isinstance(hsr, dict):
                cfg.enabled = bool(hsr.get("enabled", getattr(cfg, "enabled", True)))
                cfg.window_trades = int(hsr.get("window_trades", getattr(cfg, "window_trades", 10)) or 10)
                cfg.count_threshold = int(hsr.get("count_threshold", getattr(cfg, "count_threshold", 5)) or 5)
                cfg.high_slippage_threshold_pct = float(
                    hsr.get(
                        "high_slippage_threshold_pct",
                        getattr(cfg, "high_slippage_threshold_pct", 0.01),
                    )
                    or 0.01
                )
                cfg.cooldown_seconds = float(hsr.get("cooldown_seconds", getattr(cfg, "cooldown_seconds", 300.0)) or 300.0)
        except Exception:
            pass

        # API失败暂停（全局暂停，禁止开仓 & 平仓 & 纠偏）
        afp = rc_data.get("api_failure_pause", {}) or {}
        try:
            risk_config.api_failure_pause.enabled = bool(
                afp.get("enabled", risk_config.api_failure_pause.enabled)
            )
            risk_config.api_failure_pause.consecutive_failures = int(
                afp.get(
                    "consecutive_failures",
                    risk_config.api_failure_pause.consecutive_failures,
                )
            )
            risk_config.api_failure_pause.window_seconds = float(
                afp.get(
                    "window_seconds",
                    risk_config.api_failure_pause.window_seconds,
                )
            )
            risk_config.api_failure_pause.pause_seconds = float(
                afp.get(
                    "pause_seconds",
                    risk_config.api_failure_pause.pause_seconds,
                )
            )
            # 🔥 失败计数节流：同一交易所两次“失败记账”最短间隔（秒）
            risk_config.api_failure_pause.min_interval_seconds = float(
                afp.get(
                    "min_interval_seconds",
                    risk_config.api_failure_pause.min_interval_seconds,
                )
            )
        except Exception:
            pass

        # WS成交延迟暂停（全局暂停，禁止开仓 & 平仓 & 纠偏）
        owlp = rc_data.get("order_ws_latency_pause", {}) or {}
        try:
            risk_config.order_ws_latency_pause.enabled = bool(
                owlp.get("enabled", risk_config.order_ws_latency_pause.enabled)
            )
            risk_config.order_ws_latency_pause.threshold_seconds = float(
                owlp.get(
                    "threshold_seconds",
                    risk_config.order_ws_latency_pause.threshold_seconds,
                )
            )
            risk_config.order_ws_latency_pause.pause_seconds = float(
                owlp.get(
                    "pause_seconds",
                    risk_config.order_ws_latency_pause.pause_seconds,
                )
            )
            risk_config.order_ws_latency_pause.block_position_alignment_auto_fix = bool(
                owlp.get(
                    "block_position_alignment_auto_fix",
                    risk_config.order_ws_latency_pause.block_position_alignment_auto_fix,
                )
            )
        except Exception:
            pass

        # 持仓校验：第二次校验不一致 -> 自动纠偏（实盘下单）
        paf = rc_data.get("position_alignment_auto_fix", {}) or {}
        try:
            risk_config.position_alignment_auto_fix.enabled = bool(
                paf.get("enabled", risk_config.position_alignment_auto_fix.enabled)
            )
            risk_config.position_alignment_auto_fix.cooldown_seconds = float(
                paf.get("cooldown_seconds",
                        risk_config.position_alignment_auto_fix.cooldown_seconds)
            )
            # 🔥 纠偏下单限频：滚动窗口内最多执行 N 次（0=不限制）
            risk_config.position_alignment_auto_fix.max_executions_per_window = int(
                paf.get(
                    "max_executions_per_window",
                    risk_config.position_alignment_auto_fix.max_executions_per_window,
                )
            )
            risk_config.position_alignment_auto_fix.window_seconds = float(
                paf.get(
                    "window_seconds",
                    risk_config.position_alignment_auto_fix.window_seconds,
                )
            )
        except Exception:
            pass

        # 持仓持续不一致暂停（全局暂停，禁止开仓 & 平仓）
        pmp = rc_data.get("position_mismatch_pause", {}) or {}
        try:
            risk_config.position_mismatch_pause.enabled = bool(
                pmp.get("enabled", risk_config.position_mismatch_pause.enabled)
            )
            risk_config.position_mismatch_pause.mismatch_seconds = float(
                pmp.get(
                    "mismatch_seconds",
                    risk_config.position_mismatch_pause.mismatch_seconds,
                )
            )
        except Exception:
            pass

        return risk_config

    def _load_decision_settings(self) -> Dict[str, Any]:
        """加载决策相关配置（手续费、是否扣费等）"""
        data = self._load_root_config()
        decision_data = data.get('decision', {})
        settings = {
            'deduct_fees': bool(decision_data.get('deduct_fees', True)),
            'exchange_fee_config': {}
        }

        fee_data: Dict[str, Any] = decision_data.get('exchange_fee_config', {})
        for exchange, cfg in fee_data.items():
            if not isinstance(cfg, dict):
                continue
            settings['exchange_fee_config'][exchange] = ExchangeFeeConfig(
                limit_fee_rate=float(
                    cfg.get('limit_fee_rate', ExchangeFeeConfig().limit_fee_rate)),
                market_fee_rate=float(
                    cfg.get('market_fee_rate', ExchangeFeeConfig().market_fee_rate))
            )

        return settings

    def _load_root_config(self) -> Dict[str, Any]:
        """读取分段配置文件的原始数据并缓存"""
        if self._root_config is not None:
            return self._root_config

        config_path = self.config_manager.config_path
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                self._root_config = yaml.safe_load(f) or {}
        except FileNotFoundError:
            logger.warning(f"⚠️ [统一调度] 未找到分段配置文件: {config_path}")
            self._root_config = {}
        except Exception as exc:
            logger.warning(f"⚠️ [统一调度] 读取配置失败: {exc}")
            self._root_config = {}

        return self._root_config

    def _get_cached_exchange_equity_usdc(self, exchange: str) -> Optional[Decimal]:
        """
        从 UI 已缓存的账户余额里取“权益/USDC total”。
        - 优先使用 self.ui_manager.account_balances（由 UI 更新循环定期刷新）
        - 若没有数据返回 None（由风控模块决定是否拦截）
        """
        try:
            ex = (exchange or "").strip().lower()
            if not ex:
                return None
            balances = (getattr(self.ui_manager, "account_balances", {}) or {}).get(ex) or \
                       (getattr(self.ui_manager, "account_balances", {})
                        or {}).get(ex.upper()) or []
            for item in balances:
                if not isinstance(item, dict):
                    continue
                currency = (item.get("currency") or "").upper()
                if currency == "USDC" or currency.startswith("USDC") or currency == "USD":
                    total = item.get("total")
                    if total is None:
                        free = item.get("free") or 0
                        used = item.get("used") or 0
                        total = free + used
                    return Decimal(str(total))
            return None
        except Exception:
            return None

    def _get_cached_exchange_position_value_usdc(self, exchange: str) -> Optional[Decimal]:
        """
        从 UI 已计算的“交易所总价值（估算）”缓存里取值。
        - 该缓存来自 UIManager._build_exchange_value_table()
        """
        try:
            ex = (exchange or "").strip().upper()
            if not ex:
                return None
            cache = getattr(self.ui_manager,
                            "exchange_value_cache", None) or {}
            v = cache.get(ex)
            if v is None:
                return None
            return Decimal(str(v))
        except Exception:
            return None
