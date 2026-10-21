"""
UI管理器

职责：
- 管理UI布局和渲染
- 协调各个UI组件
- 控制UI刷新频率
"""

import asyncio
import logging
import time
import os
import re
from enum import Enum
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from collections import defaultdict, deque, OrderedDict
from pathlib import Path
from rich.console import Console, Group
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.text import Text
from rich.columns import Columns
from rich.cells import cell_len
from rich.segment import Segment

from .ui_components import UIComponents
from ..analysis.opportunity_finder import ArbitrageOpportunity
from ..config.debug_config import DebugConfig
from core.adapters.exchanges.utils.setup_logging import LoggingConfig

# 设置logger，仅写入文件，避免终端抖动
logger = LoggingConfig.setup_logger(
    name=__name__,
    log_file='arbitrage_monitor_v2.log',
    console_formatter=None,
    file_formatter='detailed',
    level=logging.INFO
)
logger.propagate = False


class _DashboardConsole(Console):
    """tmux和Rich的emoji宽度可能不同；只替换冗余图标，保留文字和样式。"""
    _icons = re.compile(r"[\U0001f000-\U0001faff\u2600-\u27bf\ufe0f]")
    _status_symbols = {"✓": "+", "✗": "x", "✅": "+", "❌": "x", "⚠": "!"}

    def render(self, renderable, options=None):
        for segment in super().render(renderable, options):
            if not segment.control:
                text = self._icons.sub(
                    lambda match: self._status_symbols.get(match.group(), "").ljust(cell_len(match.group())),
                    segment.text,
                )
                segment = Segment(text, segment.style, segment.control)
            yield segment


class UIMode(Enum):
    """
    UI显示模式

    - MONITOR: 监控模式（原V2，只监控不交易）
    - ARBITRAGE_V3: 套利V3基础模式（执行系统）
    - SEGMENTED_GRID: 分段网格模式（多交易所网格套利）
    """
    MONITOR = "monitor"
    ARBITRAGE_V3 = "v3_basic"
    SEGMENTED_GRID = "segmented"


class UIManager:
    """UI管理器"""

    def __init__(self, debug_config: DebugConfig, scroller=None):
        """
        初始化UI管理器

        Args:
            debug_config: Debug配置
            scroller: 实时滚动区管理器（可选）
        """
        self.debug = debug_config
        self.console = _DashboardConsole()
        self.components = UIComponents()
        self.scroller = scroller  # 🔥 混合模式：滚动区管理器

        # 数据缓存
        self.opportunities: List[ArbitrageOpportunity] = []
        self.stats: Dict = {}
        self.debug_messages: List[str] = []
        self.orderbook_data: Dict = {}  # 订单簿数据（实时接收）
        self.cached_orderbook_data: Dict = {}  # 订单簿数据（UI显示用，抽样）
        self.ticker_data: Dict = {}  # 🔥 Ticker数据（用于资金费率显示）
        self.cached_ticker_data: Dict = {}  # 🔥 Ticker数据（UI显示用，抽样）
        # 🔥 每个交易对的所有价差（后台计算，保证数据一致性）格式：{symbol: [SpreadData, ...]}
        self.symbol_spreads: Dict[str, List] = {}
        self.config: Dict = {}  # 配置信息（exchanges, symbols）
        self.multi_leg_symbols: List[str] = []
        self.multi_leg_rows: List[Dict[str, Any]] = []

        # V3系统新增数据缓存
        self.execution_records: List[Dict] = []  # 执行记录列表（最近N条）
        self.positions_data: List[Dict] = []  # 持仓信息列表（交易所实时持仓）
        self.local_positions_data: List[Dict] = []  # 🔥 本地套利对持仓列表（决策引擎维护）
        # 账户余额 {exchange: [BalanceData, ...]}
        self.account_balances: Dict[str, List[Dict]] = {}
        # 首次余额快照，用于前端盈亏
        self._initial_balance_snapshot: Dict[str, Decimal] = {}
        self.is_v3_mode: bool = False  # 是否为V3模式（执行系统）
        self.monitor_only_mode: bool = True  # 🔥 监控模式（True=只监控，False=实盘）
        self.risk_status: Optional[Dict] = None  # 🔥 风险控制状态数据
        self.alignment_summary: Optional[Dict[str, Any]] = None  # 持仓校验简报

        # 🔥 UI模式管理（新增）
        self.ui_mode: UIMode = UIMode.MONITOR  # 默认监控模式

        # 🔥 数据时间戳跟踪（用于检测过期数据）
        # {exchange: {symbol: timestamp}}
        self.orderbook_data_timestamps: Dict[str, Dict[str, float]] = {}
        # {exchange: {symbol: timestamp}}
        self.ticker_data_timestamps: Dict[str, Dict[str, float]] = {}
        self.data_timeout_seconds: float = 30.0  # 数据超时时间（30秒无更新则视为过期）

        # 🎯 UI更新节流配置
        self.last_price_update_time: float = 0  # 上次价格更新时间
        self.price_update_interval: float = 1.0  # 价格UI更新间隔（秒）
        self._last_ui_log_time: float = 0.0
        self._ui_log_interval_seconds: float = 5.0  # UI刷新日志间隔
        self.enable_ui_refresh_log: bool = False  # 默认关闭UI刷新日志

        # 运行状态
        self.running = False
        self.live: Optional[Live] = None
        self.ui_task: Optional[asyncio.Task] = None

        # 启动时间
        self.start_time = datetime.now()

        # 🔥 UI层持续时间容差和出现次数统计（不影响后台数据）
        # {opportunity_key: {'ui_duration_start': datetime, 'last_seen': datetime}}
        self._ui_opportunity_tracking: Dict[str, Dict] = {}
        # {symbol: [timestamp1, timestamp2, ...]} - 过去15分钟的出现时间戳
        self._symbol_occurrence_timestamps: Dict[str, List[datetime]] = {}
        self._ui_tolerance_seconds: float = 2.0  # 2秒容差
        self._occurrence_window_minutes: int = 15  # 15分钟窗口

        # 🔥 UI层显示延迟（5秒停留时间，仅用于显示）
        # {opportunity_key: {'opportunity': ArbitrageOpportunity, 'disappeared_at': datetime}}
        self._disappeared_opportunities: Dict[str, Dict] = {}
        self._display_delay_seconds: float = 5.0  # 5秒显示延迟

        # 🔥 分段网格模式专用数据缓存
        self.grid_data: Dict[str, Any] = {}  # 网格配置和状态数据
        self.current_spread_pct: float = 0.0  # 当前价差百分比
        self.spread_duration: float = 0.0  # 价差持续时间（秒）
        self.spread_trend: str = "-"  # 价差趋势（↗ ↘ →）
        self.next_action: str = ""  # 下次操作提示

        # 🔥 执行器日志缓存（避免每次整文件扫描）
        self._executor_log_path: Path = Path("logs/arbitrage_executor.log")
        self._executor_log_inode: Optional[int] = None
        self._executor_log_pos: int = 0
        self._executor_log_cache: deque = deque(maxlen=200)
        self._executor_log_pattern = re.compile(
            r"^(?P<time>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) "
            r"\[(?P<level>[A-Z]+)\s+\] "
            r"\[(?P<source>[^\]]+)\] "
            r"(?P<message>.+)$"
        )
        # 🔥 交易所原始持仓缓存（用于显示方向和入场价）
        self.exchange_position_cache: Dict[str, Dict[str, Dict]] = {}
        # 🔥 交易所总价值缓存（估值），提供给其他功能复用
        self.exchange_value_cache: Dict[str, float] = {}
        self.exchange_value_timestamp: Optional[float] = None  # Unix 时间戳

    def start(self, refresh_rate: int = 5):
        """
        启动UI（使用Rich Live模式）

        Args:
            refresh_rate: 刷新频率（Hz）
        """
        self.running = True
        self.start_time = datetime.now()

        # 🔥 生成初始布局并填充内容
        initial_layout = self._generate_layout()
        self._fill_layout_content(initial_layout)

        # 🔥 混合模式：使用 screen=True（全屏模式，滚动区在 Rich UI 内部）
        self.live = Live(
            initial_layout,
            console=self.console,
            screen=True,  # ← 全屏模式，滚动区在底部
            refresh_per_second=refresh_rate
        )

        print("✅ UI管理器已启动（顶部：汇总表 | 底部：实时滚动）")

    def stop(self):
        """停止UI"""
        self.running = False
        if self.live:
            self.live.stop()
        print("🛑 UI管理器已停止")

    def get_exchange_value_cache(self) -> Tuple[Dict[str, float], Optional[float]]:
        """
        获取最近计算的交易所总价值缓存及时间戳（Unix）。
        返回(价值映射, 时间戳)；若无数据，返回({}, None)。
        """
        return dict(self.exchange_value_cache), self.exchange_value_timestamp

    def set_v3_mode(self, enabled: bool, monitor_only: bool = True):
        """
        设置V3模式（执行系统模式）

        Args:
            enabled: 是否启用V3模式
            monitor_only: 是否为监控模式（True=只监控，False=实盘）
        """
        self.is_v3_mode = enabled
        self.monitor_only_mode = monitor_only
        # 🔥 兼容：自动设置UI模式
        if enabled:
            self.ui_mode = UIMode.ARBITRAGE_V3

    def set_ui_mode(self, mode: UIMode):
        """
        设置UI显示模式

        Args:
            mode: UI模式（MONITOR/ARBITRAGE_V3/SEGMENTED_GRID）
        """
        self.ui_mode = mode

        # 🔥 同步旧标志位（保持兼容性）
        if mode == UIMode.ARBITRAGE_V3:
            self.is_v3_mode = True
        elif mode == UIMode.SEGMENTED_GRID:
            self.is_v3_mode = True  # 分段网格也算执行系统
        else:
            self.is_v3_mode = False

    async def update_loop(self, interval_ms: int = 200):
        """
        UI更新循环

        Args:
            interval_ms: 更新间隔（毫秒）
        """
        if not self.live:
            raise RuntimeError("UI未启动，请先调用start()")

        with self.live:
            while self.running:
                try:
                    # 🔥 生成布局（根据UI模式）
                    layout = self._generate_layout()

                    # 🔥 填充内容（根据UI模式）
                    self._fill_layout_content(layout)

                    self.live.update(layout)
                    self._log_ui_refresh(layout)

                    # 等待下一次更新
                    await asyncio.sleep(interval_ms / 1000)

                except asyncio.CancelledError:
                    break
                except Exception as e:
                    self.add_debug_message(f"❌ UI更新错误: {e}")
                    await asyncio.sleep(1)

        print("🛑 UI更新循环已停止")

    def _generate_layout(self) -> Layout:
        """
        生成UI布局（根据UI模式选择布局策略）

        Returns:
            Rich Layout对象
        """
        # 🔥 新架构：根据UI模式分发到不同的布局生成器
        if self.ui_mode == UIMode.SEGMENTED_GRID:
            return self._generate_segmented_grid_layout()
        elif self.ui_mode == UIMode.ARBITRAGE_V3 or self.is_v3_mode:
            return self._generate_v3_layout()
        else:
            return self._generate_monitor_layout()

    def _generate_v3_layout(self) -> Layout:
        """
        生成V3基础模式布局（保持原有布局不变）

        Returns:
            Rich Layout对象
        """
        layout = Layout()

        # 分割为三部分：头部、主体、底部滚动区
        # 说明：V3 的“执行记录”改为只显示最近5条，因此无需为其预留过高的固定区域
        layout.split_column(
            Layout(name="header", size=8),
            Layout(name="body", ratio=2),
            Layout(name="scroller", size=24)
        )

        # 头部分为三部分：系统状态 + 性能统计 + 风险控制
        layout["header"].split_row(
            Layout(name="summary", ratio=1),
            Layout(name="performance", ratio=1),
            Layout(name="risk_control", ratio=3)
        )

        # V3模式身体部分：价格表格 + (持仓+账户) + (执行记录+套利机会)
        layout["body"].split_column(
            Layout(name="prices", ratio=3),  # 价格表格
            Layout(name="v3_info_row", ratio=2),  # V3信息行
            # 执行记录仅展示最近5条：固定更紧凑的高度，避免面板出现大量空白
            Layout(name="v3_records_row", size=9)  # 执行记录+套利机会行（约=表头+5行+边框）
        )
        # V3信息行：持仓信息 + 账户信息
        layout["v3_info_row"].split_row(
            Layout(name="positions", ratio=1),
            Layout(name="accounts", ratio=1)
        )
        # V3记录行：套利机会 + 执行记录（并排显示）
        layout["v3_records_row"].split_row(
            Layout(name="opportunities", ratio=1),  # 套利机会表格（左侧）
            Layout(name="execution_records", ratio=1)  # 执行记录表格（右侧）
        )

        return layout

    def _generate_monitor_layout(self) -> Layout:
        """
        生成监控模式布局（保持原有布局不变）

        Returns:
            Rich Layout对象
        """
        layout = Layout()

        # 分割为三部分：头部、主体、底部滚动区
        layout.split_column(
            Layout(name="header", size=8),
            Layout(name="body", ratio=2),
            Layout(name="scroller", size=24)  # 与V3一致，预留充足行高
        )

        # 头部分为三部分：系统状态 + 性能统计 + 风险控制
        layout["header"].split_row(
            Layout(name="summary", ratio=1),
            Layout(name="performance", ratio=1),
            Layout(name="risk_control", ratio=3)
        )

        # 监控模式身体部分：价格表格 + 套利机会
        if self.debug.is_debug_enabled():
            # Debug模式：价格表格 + (套利机会 + Debug面板)
            layout["body"].split_column(
                Layout(name="prices", ratio=1),
                Layout(name="opportunities_row", size=10)  # 🔥 降低套利机会区域高度
            )
            layout["opportunities_row"].split_row(
                Layout(name="opportunities", ratio=2),
                Layout(name="debug", ratio=1)
            )
        else:
            # 普通模式：价格表格 + 套利机会
            layout["body"].split_column(
                Layout(name="prices", ratio=1),
                Layout(name="opportunities", size=10)  # 🔥 降低套利机会表格高度
            )

        return layout

    def _generate_segmented_grid_layout(self) -> Layout:
        """
        生成分段网格模式布局

        布局结构：
        - 头部：系统状态 + 性能统计 + 风险控制
        - 主体：网格状态 + 持仓详情
        - 底部：实时日志滚动

        Returns:
            Rich Layout对象
        """
        layout = Layout()
        layout.split_column(
            Layout(name="header", size=8),
            Layout(name="body", ratio=2),
            Layout(name="scroller", size=12)
        )

        # 头部分割（与其他模式相同）
        layout["header"].split_row(
            Layout(name="summary", ratio=1),
            Layout(name="performance", ratio=1),
            Layout(name="risk_control", ratio=3)
        )

        # 🔥 分段网格模式的主体布局
        layout["body"].split_column(
            Layout(name="prices", ratio=2),       # 价格/价差表
            Layout(name="holdings_overview", ratio=1)  # 持仓总览（套利对 + 交易所）
        )

        # 持仓总览分为两列（调整比例，给交易所总览更多空间）
        layout["holdings_overview"].split_row(
            Layout(name="pair_holdings", ratio=5),    # 套利对持仓明细
            Layout(name="exchange_holdings", ratio=3)  # 交易所总览（增加空间）
        )

        return layout

    def _fill_layout_content(self, layout: Layout):
        """
        填充布局内容（根据模式填充不同内容）

        Args:
            layout: 待填充的布局对象
        """
        # 填充头部（所有模式共用）
        self._fill_summary(layout)
        self._fill_performance(layout)
        self._fill_risk_control(layout)

        # 🔥 根据UI模式填充不同的内容
        if self.ui_mode == UIMode.SEGMENTED_GRID:
            # 分段网格模式：填充价格、套利对持仓、交易所总览
            self._fill_prices(layout)
            self._fill_pair_holdings(layout)
            self._fill_exchange_holdings(layout)
        elif self.ui_mode == UIMode.ARBITRAGE_V3 or self.is_v3_mode:
            # V3模式：填充价格、持仓、账户、套利机会、执行记录
            self._fill_prices(layout)
            self._fill_positions(layout)
            self._fill_accounts(layout)
            self._fill_opportunities(layout)
            self._fill_execution_records(layout)
        else:
            # 监控模式：填充价格、套利机会
            self._fill_prices(layout)
            self._fill_opportunities(layout)
            if self.debug.is_debug_enabled():
                self._fill_debug(layout)

        # 填充底部滚动区（所有模式共用）
        self._fill_scroller(layout)

    def set_ui_refresh_logging(self, enabled: bool) -> None:
        """开启/关闭 UI刷新日志输出"""
        self.enable_ui_refresh_log = enabled

    def _log_ui_refresh(self, layout: Layout) -> None:
        """周期性输出UI刷新日志，便于诊断是否卡住"""
        if not self.enable_ui_refresh_log:
            return
        now = time.time()
        if now - self._last_ui_log_time < self._ui_log_interval_seconds:
            return
        self._last_ui_log_time = now

        price_rows = len(self.orderbook_data) if self.orderbook_data else 0
        opp_rows = len(self.opportunities) if self.opportunities else 0
        exec_rows = len(
            self.execution_records) if self.execution_records else 0
        summary = (
            f"价格源={price_rows} 套利机会={opp_rows} 执行记录={exec_rows} "
            f"布局={layout.name if layout.name else 'root'}"
        )
        logger.info("🖥️ [UI] 刷新完成 %s", summary)

    def _fill_summary(self, layout: Layout):
        """填充摘要面板"""
        uptime = (datetime.now() - self.start_time).total_seconds()

        summary_stats = {
            'uptime_seconds': uptime,
            'exchanges': self.stats.get('exchanges', []),
            'symbols_count': self.stats.get('symbols_count', 0),
            'active_opportunities': len(self.opportunities),
            # 🔥 传递重连统计
            'reconnect_stats': self.stats.get('reconnect_stats', {}),
            'is_v3_mode': self.is_v3_mode,  # 🔥 传递V3模式标识
            'monitor_only_mode': self.monitor_only_mode,  # 🔥 传递监控模式标识
            # 仅用于显示：交易所账户数据源（例如 variational: rest/ws）
            'exchange_account_sources': self.stats.get('exchange_account_sources', {}) or {},
        }

        layout["summary"].update(
            self.components.create_summary_panel(summary_stats))

    def _fill_performance(self, layout: Layout):
        """填充性能面板"""
        layout["performance"].update(
            self.components.create_performance_panel(self.stats))

    def _fill_risk_control(self, layout: Layout):
        """填充风险控制面板"""
        risk_panel_data = self._build_risk_panel_payload()
        layout["risk_control"].update(
            self.components.create_risk_control_panel(risk_panel_data)
        )

    def _build_risk_panel_payload(self) -> Dict[str, Any]:
        """组合风险控制面板所需的数据"""
        payload = dict(self.risk_status or {})
        balance_summary = self._format_exchange_balance_summary()
        if balance_summary:
            payload["exchange_balance_summary"] = balance_summary
        return payload

    def _format_exchange_balance_summary(self) -> str:
        """将各交易所USDC余额格式化为易读字符串"""
        if not self.account_balances:
            return ""

        summary_parts: List[str] = []
        total_pnl: Decimal = Decimal("0")
        total_current_balance: Decimal = Decimal("0")
        total_initial_balance: Decimal = Decimal("0")
        has_snapshot = False

        # 🔥 以“启动配置中的交易所列表”为准，保证“启动了几个交易所就统计几个”
        expected_exchanges = list(self.config.get("exchanges", []) or [])
        if expected_exchanges:
            exchange_names = [str(x).strip() for x in expected_exchanges if str(x).strip()]
        else:
            # 兜底：退回到当前已经拿到余额的交易所
            exchange_names = sorted(self.account_balances.keys())

        expected_count = len(exchange_names)
        mixed_account_fields = {name.lower() for name in exchange_names} == {"arcus", "lighter"}
        snap_count = 0  # 已有“初始快照”的交易所数量

        for exchange_name in exchange_names:
            balances = self.account_balances.get(exchange_name) or []
            usdc_total: Optional[Decimal] = None

            for item in balances:
                currency = (item.get("currency") or "").upper()
                if currency == "USDC" or currency.startswith("USDC") or currency == "USD":
                    total_value = item.get("total")
                    if total_value is None:
                        free = item.get("free") or 0.0
                        used = item.get("used") or 0.0
                        total_value = free + used
                    usdc_total = Decimal(str(total_value))
                    break

            if usdc_total is not None:
                initial = self._initial_balance_snapshot.get(exchange_name)
                if initial is None:
                    # 记录第一次看到的余额作为初始资金
                    self._initial_balance_snapshot[exchange_name] = usdc_total
                    initial = usdc_total
                pnl_value = usdc_total - initial
                total_pnl += pnl_value
                total_current_balance += usdc_total
                total_initial_balance += initial
                has_snapshot = True
                snap_count += 1
                pnl_str = f"{pnl_value:+,.2f}"
                ex_short = str(exchange_name)[:3].upper()
                field_label = {"arcus": "权益", "lighter": "抵押品"}.get(str(exchange_name).lower(), "") if mixed_account_fields else ""
                ex_label = str(exchange_name).upper() if mixed_account_fields else ex_short
                summary_parts.append(
                    f"{ex_label}{field_label}: {usdc_total:,.2f} ({pnl_str})"
                )
            else:
                ex_short = str(exchange_name)[:3].upper()
                summary_parts.append(f"{ex_short}: -")

        summary_text = " | ".join(summary_parts)
        if mixed_account_fields:
            return f"{summary_text} || 策略净收益：未核算"
        # 🔥 初始总余额：显示“已统计/应统计”数量，避免误解“只统计了2个交易所”
        # - 当某交易所余额尚未取到时，初始与当前都只统计已拿到快照的交易所（并在括号提示缺失）
        if has_snapshot:
            count_hint = f"{snap_count}/{expected_count}" if expected_count > 0 else f"{snap_count}"
            summary_text = (
                f"{summary_text} || "
                f"初始({count_hint}): {total_initial_balance:,.2f} | "
                f"当前: {total_current_balance:,.2f} | "
                f"盈亏: {total_pnl:+,.2f}"
            )
        return summary_text

    def _fill_prices(self, layout: Layout):
        """填充价格表格（使用抽样缓存数据，包含资金费率和后台计算的价差）"""
        exchanges = self.config.get('exchanges', [])
        symbols = self.config.get('symbols', [])
        ui_max_symbols = int(self.config.get('ui_max_symbols', 30) or 30)

        # 🎯 使用缓存数据，而不是实时数据（抽样显示，避免UI卡顿）
        # 🔥 传递 ticker_data 以显示资金费率，传递 symbol_spreads 以保证数据一致性
        base_panel = self.components.create_price_table(
            self.cached_orderbook_data,
            symbols,
            exchanges,
            ticker_data=self.cached_ticker_data,
            symbol_spreads=self.symbol_spreads,
            max_symbols=ui_max_symbols,
        )

        if self.multi_leg_symbols:
            from rich.layout import Layout as RichLayout
            multi_panel = self.components.create_multi_leg_table(
                self.multi_leg_rows,
                total_pairs=len(self.multi_leg_symbols)
            )
            price_layout = RichLayout()
            price_layout.split_column(
                RichLayout(name="base_prices", ratio=3),
                RichLayout(name="multi_leg_prices", size=max(
                    6, len(self.multi_leg_symbols) * 3))
            )
            price_layout["base_prices"].update(base_panel)
            price_layout["multi_leg_prices"].update(multi_panel)
            layout["prices"].update(price_layout)
        else:
            layout["prices"].update(base_panel)

        self._adjust_prices_section_height(layout, symbols, ui_max_symbols)

    def _fill_opportunities(self, layout: Layout):
        """填充机会表格"""
        # 🔥 合并当前机会和已消失但仍在5秒显示延迟内的机会
        display_opportunities = list(self.opportunities)

        # 添加已消失但仍在5秒内的机会（仅用于显示）
        current_time = datetime.now()
        for key, disappeared_info in self._disappeared_opportunities.items():
            time_since_disappeared = (
                current_time - disappeared_info['disappeared_at']).total_seconds()
            if time_since_disappeared <= self._display_delay_seconds:
                # 仍在5秒显示延迟内，添加到显示列表
                display_opportunities.append(disappeared_info['opportunity'])

        # 🔥 按价差排序（从大到小），确保表格显示顺序与实时数据流一致
        display_opportunities.sort(key=lambda x: x.spread_pct, reverse=True)

        # 🔥 显示所有机会（不限制数量，确保实时数据流中的机会都能显示）
        # 🔥 传递UI层的持续时间容差和出现次数统计
        layout["opportunities"].update(
            self.components.create_opportunities_table(
                display_opportunities,  # 🔥 使用合并后的机会列表（已排序）
                limit=50,  # 🔥 增加显示数量，确保所有机会都能显示（从10增加到50）
                ui_opportunity_tracking=self._ui_opportunity_tracking,
                symbol_occurrence_timestamps=self._symbol_occurrence_timestamps
            )
        )

    def _fill_scroller(self, layout: Layout):
        """显示成交统计（使用内存执行记录，避免磁盘读取日志）。"""
        from rich.text import Text
        from rich.panel import Panel

        records = self.execution_records or []
        record_count = len(records)

        # 若内存为空，尝试直接从执行器的内存快照兜底（不读日志）
        if record_count == 0:
            try:
                from core.services.arbitrage_monitor_v2.execution.arbitrage_executor import (
                    get_recent_execution_summaries,
                )

                # 兜底获取最近执行摘要（UI 仅展示最近5条；这里取 5 条即可）
                fallback = get_recent_execution_summaries(limit=5)
                if fallback:
                    records = fallback
                    record_count = len(records)
            except Exception:
                records = self.execution_records or []
                record_count = len(records)

        if record_count == 0:
            tip = Text("暂无成交记录（等待首次交易执行）", style="dim yellow")
            panel = Panel(
                tip, title="[bold white]📊 成交记录 (0)[/bold white]", border_style="blue")
        else:
            panel = self.components.create_execution_records_table(records)

        # UI 的执行记录表格只展示最近 5 条，按“可见行数”动态收缩高度，避免空白过多
        visible_rows = max(min(record_count, 5), 1)
        # 经验值：边框/标题/表头等大约占 4 行；给一点余量避免截断
        wanted_height = min(18, max(8, visible_rows + 4))
        if self.ui_mode == UIMode.SEGMENTED_GRID and self.console.height >= 70:
            wanted_height = 28
        layout["scroller"].size = wanted_height
        layout["scroller"].update(panel)

    def _fill_debug(self, layout: Layout):
        """填充Debug面板"""
        layout["debug"].update(
            self.components.create_debug_panel(self.debug_messages))

    def _adjust_prices_section_height(self, layout: Layout, symbols: List[str], ui_max_symbols: int) -> None:
        """
        根据交易对数量动态调整价格表高度，避免空白过大。
        目标：
        - 实时订单簿按监控代币数量自适应高度
        - 最大支持展示 30 个代币（超过会截断显示）
        """
        try:
            symbol_count = len(symbols or [])
            if symbol_count <= 0:
                layout["prices"].size = 8
                return
            cap = max(1, int(ui_max_symbols or 30))
            effective_rows = min(max(symbol_count, 4), cap)
            # Rich Table show_lines=True 时，每行之间会多一条分隔线，因此高度约为 2 * rows
            approx_height = 8 + effective_rows * 2
            layout["prices"].size = min(90, max(12, approx_height))
        except Exception as exc:
            logger.debug("[UI] 调整价格表高度失败: %s", exc)

    def _calculate_arbitrage_pair_positions(self) -> List[Dict[str, str]]:
        """
        从本地决策引擎数据构建套利对持仓（按套利对分组）

        🔥 使用 local_positions_data（决策引擎维护的套利对持仓）

        Returns:
            套利对持仓列表，每项包含: pair, quantity, level, segments, status
        """
        pair_positions = []

        try:
            # 🔥 优先使用本地套利对数据
            if self.local_positions_data:
                for pos in self.local_positions_data:
                    legs = pos.get('legs') or []
                    if not legs:
                        continue

                    level = pos.get('level', '-')
                    open_trade_count = pos.get('open_trade_count')
                    close_trade_count = pos.get('close_trade_count')
                    segments_count = pos.get('segment_count', 0)

                    def _format_trade_count(open_count, close_count, fallback) -> str:
                        if open_count is None and close_count is None:
                            return str(fallback)

                        def _fmt(value):
                            if value is None:
                                return "?"
                            try:
                                return str(int(value))
                            except (ValueError, TypeError):
                                return str(value)
                        return f"{_fmt(open_count)}/{_fmt(close_count)}"

                    segments = _format_trade_count(
                        open_trade_count,
                        close_trade_count,
                        segments_count,
                    )
                    status = pos.get('status', '-')
                    pair_label = pos.get('pair', '-')

                    # 初始化买卖腿占位
                    row = {
                        'pair': pair_label,
                        'buy': {
                            'exchange': '-',
                            'direction': '-',
                            'symbol': '-',
                            'quantity': '-',
                            'price': '-'
                        },
                        'sell': {
                            'exchange': '-',
                            'direction': '-',
                            'symbol': '-',
                            'quantity': '-',
                            'price': '-'
                        },
                        'level': level,
                        'segments': segments,
                        'status': status
                    }

                    for leg in legs:
                        quantity = leg.get('quantity')
                        quantity_str = f"{quantity:.4f}" if isinstance(
                            quantity, (int, float)) else "-"
                        price = leg.get('avg_price')
                        price_str = f"{price:.2f}" if isinstance(
                            price, (int, float)) and price > 0 else "-"
                        direction = leg.get('direction', '-')
                        target = row['buy'] if direction == 'Long' else row['sell']
                        target.update({
                            'exchange': leg.get('exchange', '-'),
                            'direction': direction,
                            'symbol': leg.get('symbol', '-'),
                            'quantity': quantity_str,
                            'price': price_str
                        })

                    pair_positions.append(row)
                return pair_positions

            # 🔥 如果没有本地数据，尝试从交易所实时持仓推断（兼容旧逻辑）
            if not self.positions_data:
                return pair_positions

            for pos in self.positions_data:
                exchange_buy = pos.get('exchange_buy', '')
                exchange_sell = pos.get('exchange_sell', '')
                qty_buy = pos.get('quantity_buy', 0)
                qty_sell = pos.get('quantity_sell', 0)

                # 如果两个交易所都有持仓，说明是一个套利对
                if exchange_buy and exchange_sell and qty_buy > 0 and qty_sell > 0:
                    pair_qty = min(Decimal(str(qty_buy)),
                                   Decimal(str(qty_sell)))
                    quantity_str = f"{float(pair_qty):.4f}"

                    pair_positions.append({
                        'pair': f"{exchange_buy}↔{exchange_sell}",
                        'buy': {
                            'exchange': exchange_buy,
                            'direction': 'Long',
                            'symbol': pos.get('buy_symbol') or pos.get('symbol', '-'),
                            'quantity': quantity_str,
                            'price': "-"
                        },
                        'sell': {
                            'exchange': exchange_sell,
                            'direction': 'Short',
                            'symbol': pos.get('sell_symbol') or pos.get('symbol', '-'),
                            'quantity': quantity_str,
                            'price': "-"
                        },
                        'level': "推断",
                        'segments': "?",
                        'status': "✓"
                    })

        except Exception as e:
            logger.debug(f"[UI] 计算套利对持仓失败: {e}")

        return pair_positions

    def _read_executor_log_tail(self, max_lines: int = 20) -> List[Dict[str, str]]:
        """
        读取 arbitrage_executor.log 最近若干行，并解析为表格数据。
        """
        path = self._executor_log_path
        if not path.exists():
            self._executor_log_pos = 0
            self._executor_log_inode = None
            self._executor_log_cache.clear()
            return []

        try:
            stat_result = os.stat(path)
        except OSError as exc:
            logger.warning("⚠️ [UI] 无法获取执行器日志状态: %s", exc)
            return list(self._executor_log_cache)[-max_lines:]

        inode = getattr(stat_result, "st_ino", None)
        size = stat_result.st_size
        if self._executor_log_inode != inode or size < self._executor_log_pos:
            self._executor_log_inode = inode
            self._executor_log_pos = 0
            self._executor_log_cache.clear()

        try:
            with path.open("r", encoding="utf-8", errors="ignore") as log_file:
                if self._executor_log_pos:
                    log_file.seek(self._executor_log_pos)
                else:
                    if size > 200_000:
                        start_pos = size - 200_000
                        log_file.seek(start_pos)
                        log_file.readline()
                    self._executor_log_pos = log_file.tell()

                for line in log_file:
                    stripped = line.strip()
                    if not stripped:
                        continue
                    record = self._parse_executor_log_line(stripped)
                    self._executor_log_cache.append(record)

                self._executor_log_pos = log_file.tell()
        except Exception as exc:
            logger.warning("⚠️ [UI] 读取执行器日志失败: %s", exc)

        cache = list(self._executor_log_cache)
        if not cache:
            return []
        return cache[-max_lines:]

    def _parse_executor_log_line(self, raw: str) -> Dict[str, str]:
        match = self._executor_log_pattern.match(raw)
        if not match:
            return {
                "time": "-",
                "level": "-",
                "source": "-",
                "message": raw[:160],
            }

        message = match.group("message").strip()
        if len(message) > 160:
            message = message[:157] + "..."
        return {
            "time": match.group("time")[11:],
            "level": match.group("level"),
            "source": match.group("source").split(".")[-1],
            "message": message,
        }

    def _normalize_token_key(self, symbol: Optional[str]) -> str:
        """
        将不同格式的交易对符号归一化到统一的代币键，用于校验多空是否对冲。
        逻辑：
            - 先统一分隔符（/, :, _ → -）
            - 取第一个分段作为候选代币
            - 剥离常见的合约/计价后缀（PERP、USDC、USDT、USD 等）
        """
        if not symbol:
            return "-"
        token = str(symbol).upper().strip()
        if not token:
            return "-"
        for sep in ("/", ":", "_"):
            token = token.replace(sep, "-")
        parts = [part for part in token.split("-") if part]
        candidate = parts[0] if parts else token

        suffixes = ("PERP", "FUT", "SWAP", "USD", "USDT", "USDC", "EUR", "BTC")

        def strip_suffix(value: str) -> str:
            changed = True
            while changed:
                changed = False
                for suffix in suffixes:
                    if (
                        value.endswith(suffix)
                        and len(value) > len(suffix) + 1
                    ):
                        value = value[: -len(suffix)]
                        changed = True
                        break
            return value

        normalized = strip_suffix(candidate)
        if not normalized:
            normalized = candidate
        return normalized or "-"

    def _calculate_exchange_total_positions(self) -> List[Dict[str, str]]:
        """
        从交易所实时持仓缓存构建交易所总持仓（直接读取交易所数据）

        Returns:
            交易所持仓列表，每项包含: exchange, direction, quantity, entry_price, verification
        """
        exchange_positions = []

        try:
            # 🔥 如果有交易所原始持仓数据（从 orchestrator 传递）
            if hasattr(self, 'exchange_position_cache') and self.exchange_position_cache:
                # [(exchange, symbol, size, side, entry_price)]
                position_rows_by_symbol = []

                for exchange, positions in self.exchange_position_cache.items():
                    for symbol, pos_info in positions.items():
                        raw_size = Decimal(str(pos_info.get('size', 0)))
                        if abs(raw_size) < Decimal('0.0001'):
                            continue
                        entry_price = pos_info.get('entry_price', 0)
                        side_text = str(pos_info.get('side', '')).lower()

                        if side_text == 'short':
                            signed_size = -abs(raw_size)
                        elif side_text == 'long':
                            signed_size = abs(raw_size)
                        else:
                            signed_size = raw_size

                        token_key = self._normalize_token_key(symbol)
                        position_rows_by_symbol.append({
                            'exchange': exchange,
                            'symbol': symbol,
                            'signed_size': signed_size,
                            'entry_price': float(entry_price) if entry_price else 0,
                            'token_key': token_key,
                        })

                # 🔥 按代币计算净仓位（多空独立平衡）
                symbol_balances: Dict[str, Decimal] = defaultdict(Decimal)
                for row in position_rows_by_symbol:
                    token_key = row.get('token_key') or self._normalize_token_key(
                        row.get('symbol'))
                    symbol_balances[token_key] += row['signed_size']

                # 🔥 识别多腿套利对：同一交易所的两个标的，如果数量相等（符号相反），认为是对冲的
                # {(exchange, symbol1): (exchange, symbol2)}
                multi_leg_pairs: Dict[Tuple[str, str], Tuple[str, str]] = {}

                for i, row1 in enumerate(position_rows_by_symbol):
                    for j, row2 in enumerate(position_rows_by_symbol):
                        if i >= j:
                            continue
                        # 同一交易所，不同代币，数量相等（符号相反）
                        if (row1['exchange'] == row2['exchange'] and
                            row1['token_key'] != row2['token_key'] and
                                abs(row1['signed_size'] + row2['signed_size']) < Decimal('0.0001')):
                            # 找到一对对冲的标的
                            key1 = (row1['exchange'], row1['symbol'])
                            key2 = (row2['exchange'], row2['symbol'])
                            multi_leg_pairs[key1] = key2
                            multi_leg_pairs[key2] = key1

                # 🔥 格式化输出（每个symbol一行）
                for row in position_rows_by_symbol:
                    token_key = row.get('token_key') or self._normalize_token_key(
                        row.get('symbol'))
                    signed_size = row['signed_size']

                    # 检查是否是多腿套利的一部分
                    position_key = (row['exchange'], row['symbol'])
                    if position_key in multi_leg_pairs:
                        # 多腿套利：只要数量匹配就是一致的
                        is_balanced = True
                    else:
                        # 单独持仓：净仓位接近0才是一致的
                        is_balanced = abs(
                            symbol_balances[token_key]) < Decimal('0.0001')

                    verification = "[green]✓一致[/green]" if is_balanced else "[red]✗不一致[/red]"
                    direction = "Long" if signed_size > 0 else "Short"
                    quantity = f"{abs(float(signed_size)):.4f}"
                    entry_price_str = f"{row['entry_price']:.2f}" if row['entry_price'] else "-"

                    exchange_positions.append({
                        'exchange': row['exchange'].upper(),
                        'symbol': row.get('symbol') or "-",
                        'direction': direction,
                        'quantity': quantity,
                        'entry_price': entry_price_str,
                        'verification': verification
                    })

                return exchange_positions

        except Exception as e:
            logger.debug(f"[UI] 计算交易所总持仓失败: {e}")

        return exchange_positions

    def _format_wait_status(self, reason: Optional[str]) -> str:
        """
        将等待状态描述压缩成更短的文本，适配表格宽度。
        """
        if not reason:
            return "⏱ 等待"
        text = reason.strip()
        replacements = (
            ("订单", "单"),
            ("提交", "提"),
            ("等待", ""),
            ("限价", "限价"),
            ("市价", "市价"),
            ("失败", "失败"),
        )
        for old, new in replacements:
            text = text.replace(old, new)
        max_len = 22
        if len(text) > max_len:
            text = text[:max_len] + "…"
        # 清理由替换产生的多余空格
        text = " ".join(text.split())
        return f"⏱ {text or '等待'}"

    def _fill_pair_holdings(self, layout: Layout):
        """填充套利对持仓明细表"""
        from rich.table import Table
        from rich.panel import Panel

        # === 简单统计（仅UI展示用，不影响业务）===
        pair_positions = self._calculate_arbitrage_pair_positions()
        pair_count = len(pair_positions) if isinstance(pair_positions, list) else 0

        pair_table = Table(
            title=f"[bold cyan]📦 套利对持仓明细（{pair_count}）[/bold cyan]",
            show_header=True,
            header_style="bold white",
            border_style="cyan",
            padding=(0, 1),
            show_edge=True
        )

        # 添加列（紧凑宽度）
        pair_table.add_column("套利对", justify="left", style="cyan", width=15)
        pair_table.add_column("买-交易所", justify="left", style="green", width=8)
        pair_table.add_column("买-方向", justify="center", style="white", width=6)
        pair_table.add_column("买-代币", justify="left", style="white", width=15)
        pair_table.add_column("买-数量", justify="right",
                              style="yellow", width=8)
        pair_table.add_column("买-均价", justify="right", style="cyan", width=10)
        pair_table.add_column("卖-交易所", justify="left", style="green", width=8)
        pair_table.add_column("卖-方向", justify="center", style="white", width=6)
        pair_table.add_column("卖-代币", justify="left", style="white", width=15)
        pair_table.add_column("卖-数量", justify="right",
                              style="yellow", width=8)
        pair_table.add_column("卖-均价", justify="right", style="cyan", width=10)
        pair_table.add_column("价差(开/平)", justify="right",
                              style="white", width=14, overflow="fold")
        pair_table.add_column("级别", justify="center", style="green", width=6)
        pair_table.add_column("次数(开/平)", justify="center",
                              style="white", width=9)
        pair_table.add_column("状态", justify="left",
                              style="white", width=24, overflow="fold")

        exchange_positions = self._calculate_exchange_total_positions()

        # 🔧 将交易所持仓按代币归类，生成简洁摘要
        def _resolve_token_key(pair_data: Dict[str, Any]) -> str:
            symbol = pair_data['buy'].get(
                'symbol') or pair_data['sell'].get('symbol')
            return self._normalize_token_key(symbol)

        def _symbol_aliases(symbol: Optional[str]) -> List[str]:
            if not symbol:
                return []
            base = str(symbol).strip()

            # 去掉可能的交易所前缀，如 lighter/ETH-USDC-PERP 或 LIGHTER:ETH-USDC-PERP
            if "/" in base:
                parts = base.split("/", 1)
                if parts[0].upper() in {"LIGHTER", "BINANCE", "PARADEX", "EDGE", "EDGEX", "BACKPACK"}:
                    base = parts[1]
            if ":" in base:
                parts = base.split(":", 1)
                if parts[0].upper() in {"LIGHTER", "BINANCE", "PARADEX", "EDGE", "EDGEX", "BACKPACK"}:
                    base = parts[1]

            variants = {
                base,
                base.upper(),
                base.lower(),
                base.replace("-", "_"),
                base.replace("_", "-"),
                base.replace("/", "-"),
                base.replace("/", "_"),
            }

            # 🔧 兼容标准化符号（-PERP / -SPOT 后缀）与交易所原生符号
            suffixes = ("-PERP", "/PERP", "PERP", "-SPOT", "/SPOT", "SPOT")
            more = set()
            for item in list(variants):
                for suf in suffixes:
                    if item.endswith(suf):
                        stripped = item[: -len(suf)]
                        stripped = stripped.rstrip("-_/")  # 清理尾部分隔符
                        if stripped:
                            more.add(stripped)
                            # 只保留基础币种（ETH-USDC-PERP -> ETH）
                            more.add(stripped.split("-")
                                     [0].split("/")[0].split("_")[0])
            variants |= more

            return [item for item in variants if item]

        def _get_cached_orderbook(exchange: Optional[str], symbol: Optional[str]):
            if not exchange or not symbol:
                return None

            exchange_candidates = [
                exchange,
                exchange.lower(),
                exchange.upper(),
            ]

            # 若抽样缓存缺失，尝试实时缓存（不经过节流）
            data_sources = [
                self.cached_orderbook_data,
                getattr(self, "orderbook_data", None) or {},
            ]

            # 🔥 调试：查看可用的 keys（节流避免刷屏）
            debug_key = f"orderbook_keys_{exchange}_{symbol}"
            if not hasattr(self, '_debug_printed'):
                self._debug_printed = set()

            if debug_key not in self._debug_printed:
                self._debug_printed.add(debug_key)
                for idx, source in enumerate(data_sources):
                    if not source:
                        continue
                    for ex_key in exchange_candidates:
                        books = source.get(ex_key)
                        if books:
                            all_keys = list(books.keys())
                            logger.info(
                                f"🔍 查找 {exchange}:{symbol} | 数据源{idx} {ex_key} 可用符号: "
                                f"{all_keys[:10]}... (共{len(books)}个)"
                            )
                            break
                    break  # 只打印第一个有数据的源

            for source in data_sources:
                if not source:
                    continue

                # 🔥 优先尝试直接匹配（原始 symbol）
                for ex_key in exchange_candidates:
                    books = source.get(ex_key)
                    if not books:
                        continue
                    if symbol in books:
                        return books[symbol]

                # 🔥 其次尝试别名匹配
                symbol_candidates = _symbol_aliases(symbol)
                for ex_key in exchange_candidates:
                    books = source.get(ex_key)
                    if not books:
                        continue
                    for sym_key in symbol_candidates:
                        if sym_key in books:
                            return books[sym_key]

            return None

        def _safe_decimal(value: Any) -> Optional[Decimal]:
            if value is None:
                return None
            if isinstance(value, Decimal):
                return value
            try:
                return Decimal(str(value))
            except (InvalidOperation, ValueError, TypeError):
                return None

        def _compute_pair_spread_text(pair_data: Dict[str, Any]) -> str:
            buy_exchange = pair_data['buy'].get('exchange')
            buy_symbol = pair_data['buy'].get('symbol')
            sell_exchange = pair_data['sell'].get('exchange')
            sell_symbol = pair_data['sell'].get('symbol')

            buy_book = _get_cached_orderbook(buy_exchange, buy_symbol)
            sell_book = _get_cached_orderbook(sell_exchange, sell_symbol)

            # 🔥 调试日志（节流）
            if not buy_book or not sell_book:
                missing_key = f"missing_{buy_exchange}_{buy_symbol}_{sell_exchange}_{sell_symbol}"
                if not hasattr(self, '_missing_logged'):
                    self._missing_logged = set()
                if missing_key not in self._missing_logged:
                    self._missing_logged.add(missing_key)
                    logger.info(
                        f"⬜️ 价差计算缺失盘口: buy={buy_exchange}:{buy_symbol} "
                        f"(found={buy_book is not None}), sell={sell_exchange}:{sell_symbol} (found={sell_book is not None})"
                    )

            def _extract_prices(book):
                if not book:
                    return None, None
                bid = _safe_decimal(
                    getattr(getattr(book, "best_bid", None), "price", None))
                ask = _safe_decimal(
                    getattr(getattr(book, "best_ask", None), "price", None))
                return bid, ask

            buy_bid, buy_ask = _extract_prices(buy_book)
            sell_bid, sell_ask = _extract_prices(sell_book)

            open_pct = None
            close_pct = None

            if buy_ask and buy_ask > 0 and sell_bid:
                open_pct = (sell_bid - buy_ask) / buy_ask * Decimal("100")
            if sell_ask and sell_ask > 0 and buy_bid:
                close_pct = (buy_bid - sell_ask) / sell_ask * Decimal("100")

            def _fmt(value: Optional[Decimal]) -> str:
                if value is None:
                    return "-"
                return f"{value:+.3f}%"

            if open_pct is None and close_pct is None:
                return "-/- ⚠"

            return f"{_fmt(open_pct)}/{_fmt(close_pct)}"

        if pair_positions:
            # ------------------------------------------------------------------
            # 🔥 排序/分组：按“交易所套利对”聚合显示（更适配 one_to_many）
            # - 目标：A 为中心交易所时，将 A-B 的所有币放在一堆，A-C 放在一堆
            # - 仅影响显示，不影响决策引擎/执行逻辑
            # - center_exchange 优先从“出现频次最高的交易所”推断（one_to_many 下通常为中心所）
            # ------------------------------------------------------------------
            def _norm_ex(v: Any) -> str:
                return str(v or "").strip().lower()

            def _infer_center_exchange(rows: List[Dict[str, Any]]) -> str:
                try:
                    counts: Dict[str, int] = defaultdict(int)
                    for r in rows or []:
                        a = _norm_ex((r.get("buy") or {}).get("exchange"))
                        b = _norm_ex((r.get("sell") or {}).get("exchange"))
                        if a:
                            counts[a] += 1
                        if b and b != a:
                            counts[b] += 1
                    if not counts:
                        return ""
                    # 频次最高者视为中心；若并列则不强行推断
                    best_ex, best_cnt = max(counts.items(), key=lambda kv: kv[1])
                    ties = [k for k, v in counts.items() if v == best_cnt]
                    if len(ties) != 1:
                        return ""
                    return best_ex
                except Exception:
                    return ""

            center_ex = _infer_center_exchange(pair_positions)

            def _exchange_group_key(row: Dict[str, Any]) -> Tuple[Tuple[int, str, str], str]:
                buy_ex = _norm_ex((row.get("buy") or {}).get("exchange"))
                sell_ex = _norm_ex((row.get("sell") or {}).get("exchange"))
                ex_set = {e for e in (buy_ex, sell_ex) if e}
                token_key = _resolve_token_key(row)

                # 优先：center_ex 参与时按 center->other 分组（忽略方向）
                if center_ex and center_ex in ex_set and len(ex_set) == 2:
                    other = next(iter(ex_set - {center_ex}))
                    # (0, other, center) 让 “中心-对手” 的组排在前，并按对手排序
                    return (0, other, center_ex), token_key

                # 回退：按两腿交易所名排序分组
                ex_sorted = "-".join(sorted(ex_set)) if ex_set else "-"
                return (1, ex_sorted, ""), token_key

            # 先整体排序：group -> token -> pair 字符串（稳定）
            sorted_rows = sorted(
                pair_positions,
                key=lambda r: (_exchange_group_key(r)[0], _exchange_group_key(r)[1], str(r.get("pair") or "")),
            )

            last_group: Optional[Tuple[int, str, str]] = None
            for pair_data in sorted_rows:
                grp, _tk = _exchange_group_key(pair_data)
                if last_group is not None and grp != last_group:
                    pair_table.add_section()
                last_group = grp

                spread_display = _compute_pair_spread_text(pair_data)
                pair_table.add_row(
                    pair_data['pair'],
                    pair_data['buy'].get('exchange', '-'),
                    pair_data['buy'].get('direction', '-'),
                    pair_data['buy'].get('symbol', '-'),
                    pair_data['buy'].get('quantity', '-'),
                    pair_data['buy'].get('price', '-'),
                    pair_data['sell'].get('exchange', '-'),
                    pair_data['sell'].get('direction', '-'),
                    pair_data['sell'].get('symbol', '-'),
                    pair_data['sell'].get('quantity', '-'),
                    pair_data['sell'].get('price', '-'),
                    spread_display,
                    pair_data.get('level', '-'),
                    pair_data.get('segments', '-'),
                    pair_data.get('status', '-')
                )
        else:
            # 没有持仓时显示占位符
            pair_table.add_row("无活跃套利对", *["-"] * 14)

        alignment_panel = self._build_alignment_panel()
        if alignment_panel:
            layout["pair_holdings"].update(Group(pair_table, alignment_panel))
        else:
            layout["pair_holdings"].update(pair_table)

    def _build_alignment_panel(self) -> Optional[Panel]:
        """构建持仓校验简报（同步日志但紧凑显示）。"""
        from rich.table import Table

        summary = self.alignment_summary
        if not summary:
            return None

        status_icon = "✅" if summary.get("consistent") else "⚠️"
        title = summary.get("title") or "持仓校验"
        decision = summary.get("decision") or "-"
        exchange = summary.get("exchange") or "-"
        delta = summary.get("delta")
        timestamp = summary.get("timestamp") or "-"

        table = Table(show_header=False, box=None, padding=(0, 1))
        table.add_column("项", style="cyan", width=6, no_wrap=True)
        table.add_column("内容", style="white", overflow="fold")

        table.add_row("状态", f"{status_icon} {title}")
        table.add_row("决策", decision)
        table.add_row("交易所", exchange)
        table.add_row("差异", delta or "-")
        table.add_row("时间", timestamp)

        return Panel(table, title="[bold cyan]🧮 持仓校验[/bold cyan]", border_style="cyan", expand=False)

    def _fill_exchange_holdings(self, layout: Layout):
        """填充交易所总持仓表（含校验）"""
        from rich.table import Table
        from decimal import Decimal

        # === 简单统计（仅UI展示用，不影响业务）===
        exchange_positions = self._calculate_exchange_total_positions()
        token_set = set()
        if isinstance(exchange_positions, list):
            for row in exchange_positions:
                raw_symbol = row.get("symbol")
                token = self._normalize_token_key(raw_symbol)
                if token and token != "-":
                    token_set.add(token)
        token_count = len(token_set)

        exchange_table = Table(
            title=f"[bold green]🏦 交易所总持仓（{token_count}）[/bold green]",
            show_header=True,
            header_style="bold white",
            border_style="green",
            padding=(0, 1),
            show_edge=True
        )

        # 添加列
        exchange_table.add_column(
            "交易所", justify="left", style="green", width=10)
        exchange_table.add_column("代币", justify="left", style="cyan", width=14)
        exchange_table.add_column(
            "方向", justify="center", style="white", width=5)
        exchange_table.add_column(
            "总持仓", justify="right", style="yellow", width=7)
        # 🔥 新增：网格数（abs(持仓) / base_quantity），仅用于展示
        exchange_table.add_column(
            "网格数", justify="right", style="yellow", width=7)
        exchange_table.add_column(
            "入场价", justify="right", style="cyan", width=10)
        # 🔥 新增：总价值（数量 × 入场价，仅用于展示）
        exchange_table.add_column(
            "总价值", justify="right", style="yellow", width=12)
        exchange_table.add_column(
            "校验", justify="center", style="white", width=8)

        if exchange_positions:
            grouped_positions: "OrderedDict[str, Dict[str, Any]]" = OrderedDict(
            )
            for ex_data in exchange_positions:
                raw_symbol = ex_data.get('symbol')
                token_key = self._normalize_token_key(raw_symbol)
                display_token = token_key if token_key and token_key != "-" else (
                    raw_symbol or "未知")
                grouped_positions.setdefault(
                    token_key or display_token,
                    {
                        "display": display_token,
                        "rows": []
                    }
                )["rows"].append(ex_data)

            for idx, group in enumerate(grouped_positions.values()):
                display_token = group["display"]
                exchange_table.add_row(
                    "",
                    f"[bold cyan]{display_token}[/bold cyan]",
                    "",
                    "",
                    "",
                    "",
                    "",
                    ""
                )
                for ex_data in group["rows"]:
                    # 计算网格数：abs(quantity) / base_quantity（保留2位小数）
                    grids_str = "-"
                    try:
                        qty_for_grids = Decimal(str(ex_data.get("quantity", "0")).replace(",", ""))
                        token_key = self._normalize_token_key(ex_data.get("symbol"))
                        base_map = getattr(self, "base_quantity_by_token", {}) or {}
                        base_raw = base_map.get(str(token_key).upper())
                        base_qty = Decimal(str(base_raw)) if base_raw not in (None, "", "-") else None
                        if base_qty is not None and base_qty > 0:
                            grids = abs(qty_for_grids) / base_qty
                            grids_str = f"{float(grids):.2f}"
                    except Exception:
                        grids_str = "-"

                    # 计算总价值：abs(quantity) * entry_price
                    total_value_str = "-"
                    try:
                        qty = Decimal(str(ex_data.get("quantity", "0")).replace(",", ""))
                        price_raw = ex_data.get("entry_price", "-")
                        price = Decimal(str(price_raw).replace(",", "")) if price_raw not in (None, "-", "") else None
                        if price is not None:
                            total_value = abs(qty) * price
                            total_value_str = f"{total_value:,.2f}"
                    except Exception:
                        total_value_str = "-"

                    exchange_table.add_row(
                        ex_data['exchange'],
                        ex_data.get('symbol', '-'),
                        ex_data['direction'],
                        ex_data['quantity'],
                        grids_str,
                        ex_data.get('entry_price', '-'),
                        total_value_str,
                        ex_data['verification']
                    )
                if idx < len(grouped_positions) - 1:
                    exchange_table.add_section()
        else:
            # 没有持仓时显示占位符
            exchange_table.add_row("-", "-", "-", "-", "-", "-", "-", "-")

        # 🔥 构建价值表格（返回 Table，不带 Panel）
        value_table = self._build_exchange_value_table()

        if value_table is None:
            value_table = Table(title="交易所总价值（估算）", border_style="green", padding=(0, 1))
            value_table.add_column("交易所", width=10)
            value_table.add_column("总价值(USDC)", width=14)
            value_table.add_row("-", "估值不可用")

        # 盘口缺失时保留相同容器，避免持仓面板尺寸跳变。
        grid = Table.grid(expand=True)
        grid.add_column(width=80)
        grid.add_column(width=30)
        grid.add_row(exchange_table, value_table)
        combined_panel = Panel(
            grid,
            title=f"[bold green]🏦 交易所总持仓 & 总价值（{token_count}）[/bold green]",
            border_style="green",
            padding=(0, 1)
        )
        layout["exchange_holdings"].update(combined_panel)

    def _normalize_symbol_for_comparison(self, symbol: str) -> str:
        """
        标准化标的名称用于比较（与持仓校验逻辑一致）

        将 PAXG-USD-PERP、HYPE-USDC-PERP、BTC/USDC:PERP 等都标准化为核心代币名（PAXG、HYPE、BTC）
        """
        if not symbol:
            return ""
        # 统一分隔符，兼容 BTC/USDC:PERP、BTC-USDC:PERP 等形式
        base = symbol.upper().replace("/", "-").replace(":", "-")
        # 通过非字母数字切分，保留关键 Token
        tokens = [tok for tok in re.split(r"[^A-Z0-9]+", base) if tok]
        if not tokens:
            return base
        # 过滤掉常见后缀
        suffix_whitelist = {"USD", "USDC", "USDT", "PERP", "SPOT", "FUTURES"}
        for token in tokens:
            if token not in suffix_whitelist:
                return token
        return tokens[0]

    def _build_exchange_value_table(self) -> Optional[Any]:
        """
        计算各交易所总持仓价值（USDC 计价）：
        - 使用 cached_orderbook_data 的中间价作为估值
        - 未找到盘口时跳过该标的
        - 🔥 使用标准化symbol匹配，确保能找到盘口价格
        - 返回 Table 对象（不带外层 Panel）
        """
        from rich.table import Table
        from decimal import Decimal, InvalidOperation

        positions = self._calculate_exchange_total_positions()
        if not positions:
            # 清空缓存
            self.exchange_value_cache = {}
            self.exchange_value_timestamp = None
            return None

        # 汇总：exchange -> total_value
        exchange_totals: Dict[str, Decimal] = defaultdict(Decimal)

        # 调试计数器
        total_positions = len(positions)
        priced_positions = 0
        failed_positions = []

        def _mid_price_from_books(books: Optional[Dict[str, Any]], symbol_normalized: str) -> Optional[Decimal]:
            """
            从盘口数据中查找匹配的价格
            symbol_normalized: 已标准化的代币名（如 HYPE、PAXG、BTC）
            """
            if not books:
                return None

            # 遍历所有盘口，找到标准化后匹配的
            for book_key, book in books.items():
                book_key_normalized = self._normalize_symbol_for_comparison(
                    book_key)
                if book_key_normalized == symbol_normalized:
                    if book and book.best_bid and book.best_ask:
                        try:
                            bid = Decimal(str(book.best_bid.price))
                            ask = Decimal(str(book.best_ask.price))
                            if bid > 0 and ask > 0:
                                return (bid + ask) / Decimal("2")
                        except (InvalidOperation, TypeError):
                            continue
            return None

        def _mid_price(exchange: str, symbol_raw: str) -> Optional[Decimal]:
            """
            获取中间价，优先用目标交易所盘口；若缺失，降级使用任意交易所
            symbol_raw: 原始symbol（如 HYPE-USDC-PERP）
            """
            symbol_normalized = self._normalize_symbol_for_comparison(
                symbol_raw)

            # 优先用目标交易所盘口
            books = self.cached_orderbook_data.get(
                exchange) if self.cached_orderbook_data else None
            price = _mid_price_from_books(books, symbol_normalized)
            if price is not None:
                return price

            # 降级：使用任意交易所的盘口
            if self.cached_orderbook_data:
                for ex_books in self.cached_orderbook_data.values():
                    price = _mid_price_from_books(ex_books, symbol_normalized)
                    if price is not None:
                        return price
            return None

        for item in positions:
            ex = (item.get("exchange") or "").lower()
            sym = item.get("symbol")
            qty = item.get("quantity")
            if not ex or sym is None or qty is None:
                continue
            try:
                qty_dec = Decimal(str(qty))
            except (InvalidOperation, TypeError):
                continue

            price = _mid_price(ex, sym)
            if price is None:
                failed_positions.append(f"{ex}:{sym}")
                continue

            priced_positions += 1
            exchange_totals[ex] += qty_dec * price

        if not exchange_totals:
            return None

        table = Table(
            title="[bold green]💵 交易所总价值（估算）[/bold green]",
            show_header=True,
            header_style="bold white",
            border_style="green",
            padding=(0, 1),
            show_edge=True,
        )
        table.add_column("交易所", justify="left", style="green", width=10)
        table.add_column("总价值(USDC)", justify="right",
                         style="yellow", width=14)

        # 缓存供其他功能复用
        try:
            self.exchange_value_cache = {ex.upper(): float(
                val) for ex, val in exchange_totals.items()}
            self.exchange_value_timestamp = time.time()
        except Exception:
            self.exchange_value_cache = {}
            self.exchange_value_timestamp = None

        for ex, val in sorted(exchange_totals.items()):
            table.add_row(ex.upper(), f"{val:.4f}")

        return table

    def _fill_positions(self, layout: Layout):
        """填充持仓信息面板（V3模式 + 分段网格模式）"""
        layout["positions"].update(
            self.components.create_positions_table(self.positions_data)
        )

    def _fill_accounts(self, layout: Layout):
        """填充账户信息面板（V3模式）"""
        layout["accounts"].update(
            self.components.create_accounts_table(self.account_balances)
        )

    def _fill_execution_records(self, layout: Layout):
        """填充执行记录面板（V3模式）"""
        layout["execution_records"].update(
            self.components.create_execution_records_table(
                self.execution_records)
        )

    def update_opportunities(self, opportunities: List[ArbitrageOpportunity]):
        """
        更新机会数据（带UI层持续时间容差和出现次数统计）

        Args:
            opportunities: 机会列表
        """
        current_time = datetime.now()

        # 🔥 保存旧的机会列表（用于查找已消失的机会）
        old_opportunities = self.opportunities.copy()

        # 🔥 清理超过15分钟的时间戳
        cutoff_time = current_time - \
            timedelta(minutes=self._occurrence_window_minutes)
        for symbol in list(self._symbol_occurrence_timestamps.keys()):
            self._symbol_occurrence_timestamps[symbol] = [
                ts for ts in self._symbol_occurrence_timestamps[symbol]
                if ts > cutoff_time
            ]
            if not self._symbol_occurrence_timestamps[symbol]:
                del self._symbol_occurrence_timestamps[symbol]

        # 🔥 更新UI层持续时间容差和出现次数统计
        current_keys = set()
        current_symbols = set()  # 🔥 当前出现的代币集合（用于重置5秒显示延迟）
        for opp in opportunities:
            key = opp.get_opportunity_key()
            current_keys.add(key)
            current_symbols.add(opp.symbol)  # 🔥 记录当前出现的代币

            # 记录出现时间戳（用于统计出现次数）
            if opp.symbol not in self._symbol_occurrence_timestamps:
                self._symbol_occurrence_timestamps[opp.symbol] = []
            # 检查是否是新出现（避免重复记录）
            if not self._symbol_occurrence_timestamps[opp.symbol] or \
               (current_time - self._symbol_occurrence_timestamps[opp.symbol][-1]).total_seconds() > 1.0:
                self._symbol_occurrence_timestamps[opp.symbol].append(
                    current_time)

            # UI层持续时间容差逻辑
            if key in self._ui_opportunity_tracking:
                # 现有机会：检查是否在容差范围内
                tracking = self._ui_opportunity_tracking[key]
                time_since_last_seen = (
                    current_time - tracking['last_seen']).total_seconds()

                if time_since_last_seen <= self._ui_tolerance_seconds:
                    # 在容差范围内，继续累计时间
                    tracking['last_seen'] = current_time
                else:
                    # 超过容差，重新开始计时
                    tracking['ui_duration_start'] = current_time
                    tracking['last_seen'] = current_time
            else:
                # 新机会：开始计时
                self._ui_opportunity_tracking[key] = {
                    'ui_duration_start': current_time,
                    'last_seen': current_time
                }

        # 🔥 处理已消失的机会（保留5秒显示时间）
        expired_keys = set(self._ui_opportunity_tracking.keys()) - current_keys
        for key in list(expired_keys):
            tracking = self._ui_opportunity_tracking[key]
            time_since_last_seen = (
                current_time - tracking['last_seen']).total_seconds()

            # 🔥 如果机会不在当前列表中，立即添加到已消失列表（开始5秒计时）
            # 不再等待2秒容差，因为5秒显示延迟是独立的UI功能
            if key not in self._disappeared_opportunities:
                # 找到对应的机会对象（从旧的机会列表中）
                disappeared_opp = None
                for opp in old_opportunities:
                    if opp.get_opportunity_key() == key:
                        disappeared_opp = opp
                        break

                if disappeared_opp:
                    self._disappeared_opportunities[key] = {
                        'opportunity': disappeared_opp,
                        'disappeared_at': current_time
                    }

            # 🔥 如果超过2秒容差，从跟踪中移除（但保留在已消失列表中）
            if time_since_last_seen > self._ui_tolerance_seconds:
                del self._ui_opportunity_tracking[key]

        # 🔥 清理超过5秒显示延迟的已消失机会
        for key in list(self._disappeared_opportunities.keys()):
            disappeared_info = self._disappeared_opportunities[key]
            time_since_disappeared = (
                current_time - disappeared_info['disappeared_at']).total_seconds()
            if time_since_disappeared > self._display_delay_seconds:
                # 超过5秒，从已消失列表中删除
                del self._disappeared_opportunities[key]

        # 🔥 如果已消失的机会重新出现，从已消失列表中移除
        for key in current_keys:
            if key in self._disappeared_opportunities:
                del self._disappeared_opportunities[key]

        # 🔥 如果同一个代币在5秒内接收到多次套利机会，重置该代币所有已消失机会的5秒计时
        # 这仅影响UI显示延迟，不影响次数统计和持续时间等数据
        for symbol in current_symbols:
            # 查找该代币的所有已消失机会
            for key, disappeared_info in self._disappeared_opportunities.items():
                if disappeared_info['opportunity'].symbol == symbol:
                    # 重置该已消失机会的消失时间，重新开始5秒计时
                    disappeared_info['disappeared_at'] = current_time

        self.opportunities = opportunities

    def update_stats(self, stats: Dict):
        """
        更新统计数据

        Args:
            stats: 统计字典
        """
        self.stats = stats

    def update_orderbook_data(
        self,
        orderbook_data: Dict,
        ticker_data: Optional[Dict] = None,
        symbol_spreads: Optional[Dict[str, List]] = None
    ):
        """
        更新订单簿数据（带抽样节流和数据过期清理）

        Args:
            orderbook_data: 订单簿数据 {exchange: {symbol: OrderBookData}}
            ticker_data: Ticker数据 {exchange: {symbol: TickerData}}，用于资金费率显示（可选）
            symbol_spreads: 每个交易对的所有价差 {symbol: [SpreadData, ...]}（后台计算，保证数据一致性）
        """
        import time
        current_time = time.time()

        # 🔥 更新数据时间戳（用于检测过期数据）
        for exchange, symbols_data in orderbook_data.items():
            if exchange not in self.orderbook_data_timestamps:
                self.orderbook_data_timestamps[exchange] = {}
            for symbol in symbols_data.keys():
                self.orderbook_data_timestamps[exchange][symbol] = current_time

        if ticker_data is not None:
            for exchange, symbols_data in ticker_data.items():
                if exchange not in self.ticker_data_timestamps:
                    self.ticker_data_timestamps[exchange] = {}
                for symbol in symbols_data.keys():
                    self.ticker_data_timestamps[exchange][symbol] = current_time

        # 🔥 清理过期数据（超过30秒未更新的数据）
        self._cleanup_stale_data(current_time)

        # 🎯 始终接收数据（不丢弃）
        self.orderbook_data = orderbook_data
        if ticker_data is not None:
            self.ticker_data = ticker_data
        if symbol_spreads is not None:
            self.symbol_spreads = symbol_spreads  # 🔥 保存后台计算的价差数据

        # 🎯 但只按固定频率更新UI缓存（抽样显示）
        if current_time - self.last_price_update_time >= self.price_update_interval:
            # 更新UI缓存（只包含未过期的数据）
            self.cached_orderbook_data = self._filter_stale_data(
                orderbook_data, self.orderbook_data_timestamps, current_time)
            if ticker_data is not None:
                self.cached_ticker_data = self._filter_stale_data(
                    ticker_data, self.ticker_data_timestamps, current_time)
            if symbol_spreads is not None:
                self.symbol_spreads = symbol_spreads.copy()  # 🔥 更新价差缓存
            self.last_price_update_time = current_time

    def update_multi_leg_pairs(self, rows: List[Dict[str, Any]]):
        """更新多腿套利实时展示数据"""
        self.multi_leg_rows = rows

    def _cleanup_stale_data(self, current_time: float):
        """
        清理过期数据的时间戳

        Args:
            current_time: 当前时间戳
        """
        # 清理订单簿数据时间戳
        for exchange in list(self.orderbook_data_timestamps.keys()):
            for symbol in list(self.orderbook_data_timestamps[exchange].keys()):
                timestamp = self.orderbook_data_timestamps[exchange][symbol]
                if current_time - timestamp > self.data_timeout_seconds:
                    del self.orderbook_data_timestamps[exchange][symbol]
            if not self.orderbook_data_timestamps[exchange]:
                del self.orderbook_data_timestamps[exchange]

        # 清理Ticker数据时间戳
        for exchange in list(self.ticker_data_timestamps.keys()):
            for symbol in list(self.ticker_data_timestamps[exchange].keys()):
                timestamp = self.ticker_data_timestamps[exchange][symbol]
                if current_time - timestamp > self.data_timeout_seconds:
                    del self.ticker_data_timestamps[exchange][symbol]
            if not self.ticker_data_timestamps[exchange]:
                del self.ticker_data_timestamps[exchange]

    def _filter_stale_data(self, data: Dict, timestamps: Dict[str, Dict[str, float]], current_time: float) -> Dict:
        """
        过滤过期数据，只保留未过期的数据

        Args:
            data: 数据字典 {exchange: {symbol: Data}}
            timestamps: 时间戳字典 {exchange: {symbol: timestamp}}
            current_time: 当前时间戳

        Returns:
            过滤后的数据字典（只包含未过期的数据）
        """
        filtered_data = {}
        for exchange, symbols_data in data.items():
            if exchange not in timestamps:
                continue
            filtered_symbols = {}
            for symbol, symbol_data in symbols_data.items():
                if symbol in timestamps[exchange]:
                    timestamp = timestamps[exchange][symbol]
                    if current_time - timestamp <= self.data_timeout_seconds:
                        filtered_symbols[symbol] = symbol_data
            if filtered_symbols:
                filtered_data[exchange] = filtered_symbols
        return filtered_data

    def update_config(self, config: Dict):
        """
        更新配置信息

        Args:
            config: 配置字典（exchanges, symbols）
        """
        self.config = config
        self.multi_leg_symbols = config.get('multi_leg_symbols', [])

    def update_grid_data(
        self,
        grid_data: Dict[str, Any],
        current_spread_pct: float = 0.0,
        spread_duration: float = 0.0,
        spread_trend: str = "-",
        next_action: str = ""
    ):
        """
        更新分段网格数据

        Args:
            grid_data: 网格配置和状态（包含各段的阈值、持仓等）
            current_spread_pct: 当前价差百分比
            spread_duration: 价差持续时间（秒）
            spread_trend: 价差趋势（↗ ↘ →）
            next_action: 下次操作提示
        """
        self.grid_data = grid_data
        self.current_spread_pct = current_spread_pct
        self.spread_duration = spread_duration
        self.spread_trend = spread_trend
        self.next_action = next_action

    def update_positions(self, positions: List[Dict]):
        """
        更新持仓信息（V3模式 - 交易所实时持仓）

        Args:
            positions: 持仓信息列表（来自交易所WebSocket）
        """
        self.positions_data = positions

    def update_local_positions(self, positions: List[Dict]):
        """
        更新本地套利对持仓信息

        Args:
            positions: 本地持仓列表（来自决策引擎）
        """
        self.local_positions_data = positions

    def update_exchange_position_cache(self, cache: Dict[str, Dict[str, Dict]]):
        """
        更新交易所原始持仓缓存

        Args:
            cache: 交易所持仓缓存 {exchange: {symbol: position_info}}
        """
        self.exchange_position_cache = cache

    def update_base_quantity_by_token(self, mapping: Dict[str, str]) -> None:
        """
        更新“每个代币的单格数量(base_quantity)”映射（仅用于 UI 展示）。

        Args:
            mapping: {TOKEN: "base_quantity"}，例如 {"BTC": "0.005", "ETH": "0.3"}
        """
        try:
            self.base_quantity_by_token = dict(mapping or {})
        except Exception:
            self.base_quantity_by_token = {}

    def update_execution_records(self, records: List[Dict]):
        """
        更新执行记录（V3模式）

        Args:
            records: 执行记录列表
        """
        self.execution_records = records

    def update_risk_status(self, risk_status_data: Dict):
        """
        更新风险控制状态

        Args:
            risk_status_data: 风险状态数据字典
        """
        self.risk_status = risk_status_data

    def update_alignment_summary(self, summary: Optional[Dict[str, Any]]):
        """更新持仓校验摘要（来自 orchestrator 的 alignment 快照）"""
        self.alignment_summary = summary

    def add_debug_message(self, message: str):
        """
        添加Debug消息

        Args:
            message: 消息内容
        """
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.debug_messages.append(f"[{timestamp}] {message}")

        # 只保留最近100条消息
        if len(self.debug_messages) > 100:
            self.debug_messages = self.debug_messages[-100:]

    def clear_debug_messages(self):
        """清空Debug消息"""
        self.debug_messages.clear()
