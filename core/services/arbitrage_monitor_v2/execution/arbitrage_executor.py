"""
套利执行模块

职责：
- 接收套利决策指令
- 执行订单提交（限价+市场、市价+市价模式）
- 监控订单执行状态
- 处理部分成交、超时等异常情况
- 反馈执行结果

注意：此模块负责订单执行，不涉及决策逻辑
拆分子模块：
- reduce_only_handler：集中管理 reduce-only 状态与回滚
- order_monitor：负责订单等待、撤单、WS/REST 状态合并
- order_strategy_executor：负责限价/市价/双限价等策略执行
"""

import asyncio
from core.adapters.exchanges.order_state import OrderStateUnknownError
import logging
import time
import re
from contextlib import AsyncExitStack, asynccontextmanager
from contextvars import ContextVar
from typing import Optional, Dict, Callable, Any, List, Tuple, Set, Iterable
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, ROUND_DOWN, ROUND_UP, InvalidOperation

from ..config.arbitrage_config import (
    ExecutionConfig,
    QuantityConfig,
    ExchangeOrderModeConfig,
    ExchangeRateLimitConfig,
)
from ..analysis.spread_calculator import SpreadData
from ..models import PositionInfo
from ..guards.reduce_only_guard import ReduceOnlyGuard
from .reduce_only_handler import ReduceOnlyHandler
from .order_monitor import OrderMonitor
from .order_strategy_executor import OrderStrategyExecutor
from ..risk_control.network_state import (
    notify_network_failure,
    notify_network_recovered,
)
from core.adapters.exchanges.interface import ExchangeInterface
from core.adapters.exchanges.models import OrderData, OrderStatus, OrderSide, OrderType
from ..state.symbol_state_manager import SymbolStateManager

# 🔥 使用统一日志系统
from core.adapters.exchanges.utils.setup_logging import LoggingConfig

logger = LoggingConfig.setup_logger(
    name=__name__,
    log_file='arbitrage_executor.log',
    console_formatter=None,  # 🔥 不输出到终端
    file_formatter='detailed',
    level=logging.INFO
)
# 🔥 额外确保不传播到父logger，防止终端抖动
logger.propagate = False

# 内存执行快照（避免依赖日志文件）
_EXECUTION_SUMMARIES: deque = deque(maxlen=50)


def _push_execution_summary(summary: Dict[str, Any]) -> None:
    """将成交概要存入内存队列，供 UI 兜底显示。"""
    try:
        _EXECUTION_SUMMARIES.append(summary)
    except Exception:
        # 兜底保证执行路径不抛出
        pass


def get_recent_execution_summaries(limit: int = 20) -> List[Dict[str, Any]]:
    """获取最近的内存成交概要（用于 UI 兜底）。"""
    if limit <= 0:
        limit = 1
    return list(_EXECUTION_SUMMARIES)[-limit:]


CLOSED_STATUSES: Set[OrderStatus] = set()
status_canceled = getattr(OrderStatus, "CANCELED", None)
if status_canceled is not None:
    CLOSED_STATUSES.add(status_canceled)
status_cancelled = getattr(OrderStatus, "CANCELLED", None)
if status_cancelled is not None:
    CLOSED_STATUSES.add(status_cancelled)
status_filled = getattr(OrderStatus, "FILLED", None)
if status_filled is not None:
    CLOSED_STATUSES.add(status_filled)
status_closed = getattr(OrderStatus, "CLOSED", None)
if status_closed is not None:
    CLOSED_STATUSES.add(status_closed)

try:  # pragma: no cover
    import aiohttp
except Exception:  # pragma: no cover
    aiohttp = None


@dataclass
class ExecutionResult:
    """执行结果"""
    success: bool
    order_buy: Optional[OrderData] = None  # 买入订单
    order_sell: Optional[OrderData] = None  # 卖出订单
    error_message: Optional[str] = None
    execution_time: datetime = field(default_factory=datetime.now)
    # 🔥 部分失败标记（用于风险控制）
    partial_failure: bool = False  # 一个交易所成功，一个失败
    failed_exchange: Optional[str] = None  # 失败的交易所
    success_exchange: Optional[str] = None  # 成功的交易所
    success_quantity: Decimal = Decimal('0')  # 成功成交的数量
    failure_code: Optional[str] = None  # 扩展错误码（用于上层策略）
    emergency_closes: List[Dict[str, Any]] = field(default_factory=list)
    # 🔥 风控事件（用于上层统计/暂停策略；即便 success=True 也可能包含风险事件，例如单腿异常但已补单成功）
    risk_events: List[str] = field(default_factory=list)
    # 🔥 成交滑点（用于滑点风控暂停；单位：百分比）
    # slippage_pct = 实际价差% - 理论价差%（通常为负数；越负亏损越大）
    slippage_pct: Optional[float] = None
    high_slippage_threshold_pct: Optional[float] = None


@dataclass
class ExecutionRequest:
    """执行请求"""
    symbol: str
    exchange_buy: str
    exchange_sell: str
    price_buy: Decimal
    price_sell: Decimal
    quantity: Decimal
    is_open: bool = True  # True=开仓, False=平仓
    spread_data: Optional[SpreadData] = None
    is_last_split: bool = False  # 🔥 是否为最后一笔拆单（用于打印平均统计）
    buy_symbol: Optional[str] = None  # 针对多腿套利：买入腿的具体交易对
    sell_symbol: Optional[str] = None  # 针对多腿套利：卖出腿的具体交易对
    grid_action: Optional[str] = None  # "open"/"close"
    grid_level: Optional[int] = None   # 对应的网格级别(Tn)
    # 本次动作对应的阈值（百分比，供日志展示）：
    # - open : Tn 开仓阈值（>=）
    # - close: T(n-1) 平仓阈值（<=），例如平仓T5应显示T4阈值
    grid_threshold_pct: Optional[Decimal] = None
    # 市价单允许滑点（小数，如0.0005=0.05%）
    slippage_tolerance_pct: Optional[Decimal] = None
    limit_price_offset_buy: Optional[Decimal] = None  # 限价买单价格偏移（绝对值）
    limit_price_offset_sell: Optional[Decimal] = None  # 限价卖单价格偏移（绝对值）
    min_exchange_order_qty: Dict[str, Decimal] = field(
        default_factory=dict)  # 交易所最小下单量映射
    # 🔥 完整盘口数据（用于日志显示）
    orderbook_buy_ask: Optional[Decimal] = None   # 买入腿的Ask价格
    orderbook_buy_bid: Optional[Decimal] = None   # 买入腿的Bid价格
    orderbook_sell_ask: Optional[Decimal] = None  # 卖出腿的Ask价格
    orderbook_sell_bid: Optional[Decimal] = None  # 卖出腿的Bid价格
    # 🔥 强制平仓/强平专用：允许在“全局暂停”状态下继续执行（仅对平仓有效）
    allow_when_global_paused: bool = False
    # 🔥 强制执行模式（仅在风控强平等场景使用）：例如 "market_market"
    force_order_mode: Optional[str] = None
    # 🔥 “最后闸门”辅助标记：本请求是否已经开始尝试发单（用于方案1的放行逻辑）
    _has_submitted_order: bool = False


class ExchangeRateLimiter:
    """单个交易所的限速控制器"""

    def __init__(
        self,
        exchange_name: str,
        *,
        max_concurrent: int,
        min_interval_ms: int,
        cooldown_ms: int,
    ):
        self.exchange = exchange_name
        self._semaphore = asyncio.Semaphore(max(1, max_concurrent))
        self._min_interval = max(0.0, min_interval_ms / 1000.0)
        self._default_cooldown = max(0.0, cooldown_ms / 1000.0)
        self._lock = asyncio.Lock()
        self._next_available_time: float = 0.0
        self._cooldown_until: float = 0.0

    async def acquire(self) -> None:
        """
        占用限流资源，并遵守最小间隔/冷却。

        重要：必须对取消/异常安全。
        - 本方法会先拿到 semaphore，再 await `sleep()` 来对齐节奏；
        - 若在 sleep 期间 task 被 cancel，必须立即释放 semaphore，
          否则会造成“信号还在出，但后续永远卡 in-flight、执行层无日志”的永久死锁。
        """
        await self._semaphore.acquire()
        try:
            await self._respect_interval()
        except asyncio.CancelledError:
            # 取消发生在节奏等待期间：必须释放占用，否则 semaphore 永久泄漏
            try:
                self._semaphore.release()
            except Exception:
                pass
            raise
        except Exception:
            # 任何异常都要释放占用，避免后续请求永久卡住
            try:
                self._semaphore.release()
            except Exception:
                pass
            raise

    async def _respect_interval(self) -> None:
        wait_seconds: float = 0.0
        async with self._lock:
            now = time.monotonic()
            target = max(self._next_available_time, self._cooldown_until)
            wait_seconds = max(0.0, target - now)
        if wait_seconds > 0:
            logger.debug(
                "⏱️ [限流] %s 等待 %.3fs 以满足节奏限制",
                self.exchange,
                wait_seconds
            )
            await asyncio.sleep(wait_seconds)
        async with self._lock:
            now = time.monotonic()
            if self._min_interval > 0:
                self._next_available_time = now + self._min_interval
            else:
                self._next_available_time = now

    def release(self) -> None:
        self._semaphore.release()

    def register_cooldown(self, seconds: Optional[float] = None) -> None:
        cooldown = seconds if seconds and seconds > 0 else self._default_cooldown
        if cooldown <= 0:
            return
        target = time.monotonic() + cooldown
        if target > self._cooldown_until:
            self._cooldown_until = target

    @property
    def default_cooldown(self) -> float:
        return self._default_cooldown

    @asynccontextmanager
    async def reserve(self):
        await self.acquire()
        try:
            yield self
        finally:
            self.release()


class RateLimitReservation:
    """封装多交易所限流资源的占用"""

    def __init__(self, limiters: List[ExchangeRateLimiter]):
        self._limiters = limiters
        self._stack = AsyncExitStack()

    async def __aenter__(self):
        for limiter in self._limiters:
            await self._stack.enter_async_context(limiter.reserve())
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return await self._stack.__aexit__(exc_type, exc, tb)


class ReduceOnlyRestrictionError(Exception):
    """当交易所返回 invalid reduce only mode 时抛出。"""

    def __init__(self, exchange: str, symbol: str, original: Optional[Exception] = None):
        self.exchange = exchange
        self.symbol = symbol
        self.original = original
        message = f"{exchange}:{symbol} reduce-only restriction"
        if original:
            message += f" ({original})"
        super().__init__(message)


class GlobalPausedError(Exception):
    """
    全局暂停中：用于在“真正发单前”阻断尚未开始下单的套利请求。

    设计目标（方案1）：
    - 暂停触发后：后续还没开始下单的套利应立刻被拦截（避免继续开/平仓）。
    - 已经开始下单（至少一次发单尝试）的那一笔：允许继续完成（包含补救/紧急平仓），避免扩大单腿风险。
    """

    def __init__(self, reason: str, *, stage: str):
        self.reason = reason or "全局暂停中"
        self.stage = stage or "unknown"
        super().__init__(f"{self.reason} (stage={self.stage})")

class ArbitrageExecutor:
    """套利执行器"""

    def __init__(
        self,
        execution_config: ExecutionConfig,
        exchange_adapters: Dict[str, ExchangeInterface],
        monitor_only: bool = True,  # 🔥 监控模式开关
        is_segmented_mode: bool = False,  # 🔥 是否为分段模式
        reduce_only_guard: Optional[ReduceOnlyGuard] = None,
        symbol_state_manager: Optional[SymbolStateManager] = None,
    ):
        """
        初始化套利执行器

        Args:
            execution_config: 执行配置
            exchange_adapters: 交易所适配器字典 {exchange_name: adapter}
            monitor_only: 监控模式开关（True=只监控不下单，False=正常执行订单）
            is_segmented_mode: 是否为分段模式（True=分段模式不使用轮次间隔，False=基础模式使用轮次间隔）
        """
        self.config = execution_config
        self.exchange_adapters = exchange_adapters
        self._execution_request_ctx = ContextVar("execution_request", default=None)
        self._order_submit_ts = {}
        self._order_ws_latency_info = {}
        self._order_ws_latency_keys = deque()
        self._order_ws_latency_max_keys = 5000
        self._global_pause_checker = None
        self._exchange_trade_blocker = None
        self._exchange_order_error_reporter = None
        self._order_ws_latency_reporter = None
        self.monitor_only = monitor_only
        self._unknown_execution_pairs = set()  # 🔥 保存监控模式状态
        self.is_segmented_mode = is_segmented_mode  # 🔥 保存分段模式状态
        self.reduce_only_guard = reduce_only_guard
        self.symbol_state_manager = symbol_state_manager
        self._live_price_resolver: Optional[
            Callable[[str, str, bool], Optional[Decimal]]
        ] = None
        self.reduce_only_handler = ReduceOnlyHandler(self, reduce_only_guard)
        self.order_monitor = OrderMonitor(self)
        self.order_strategy_executor = OrderStrategyExecutor(
            self,
            ExecutionResult,
            ReduceOnlyRestrictionError,
        )

        # 轮次间隔控制（分段模式不使用轮次间隔，允许快速拆单补仓）
        if is_segmented_mode:
            self._round_pause_seconds: float = 0.0
            logger.info("🔥 [套利执行] 分段模式：已禁用轮次间隔，允许快速拆单补仓")
        else:
            self._round_pause_seconds: float = (
                getattr(self.config.order_execution,
                        'round_pause_seconds', 0) or 0
            )
            if self._round_pause_seconds > 0:
                logger.info(
                    f"⏸️ [套利执行] 基础模式：轮次间隔 {self._round_pause_seconds} 秒")
        self._next_round_time: float = 0.0

        # 订单状态跟踪
        self.pending_orders: Dict[str, OrderData] = {}  # {order_id: OrderData}
        self._pending_orders_by_client: Dict[str, OrderData] = {}
        self._order_fill_progress: Dict[str, Decimal] = {}

        # 订单回调
        self.order_fill_callbacks: Dict[str,
                                        Callable] = {}  # {order_id: callback}

        # 🔥 WebSocket订单追踪（事件驱动）
        self._order_fill_events: Dict[str,
                                      asyncio.Event] = {}  # {order_id: Event}
        # {order_id/client_id: OrderData}
        self._order_fill_results: Dict[str, OrderData] = {}
        # {order_id/client_id: exchange_name}
        self._order_exchange_map: Dict[str, str] = {}
        # {lighter_ws_order_id: exchange_name}
        self._lighter_ws_order_map: Dict[str, str] = {}
        self._quantity_log_times: Dict[str, float] = {}
        self._quantity_log_values: Dict[str, Decimal] = {}
        self._quantity_log_interval: float = 180.0  # 3分钟节流
        self._network_retry_max_attempts: int = max(
            1,
            getattr(self.config.order_execution, "max_retry_count", 3)
        )
        self._network_retry_base_delay: float = 2.0
        self._network_retry_max_delay: float = 15.0
        # 🔒 lighter REST 下单串行锁，确保 nonce 递增
        self._lighter_order_lock: asyncio.Lock = asyncio.Lock()

        # 🔥 lighter 单腿成交计数器 {symbol: count}
        self._lighter_single_leg_counter: Dict[str, int] = {}

        # 交易所限流控制
        self._rate_limit_config_map: Dict[str, ExchangeRateLimitConfig] = (
            self._build_rate_limit_config_map()
        )
        self._default_rate_limit_config: ExchangeRateLimitConfig = (
            self._rate_limit_config_map.get(
                "default", ExchangeRateLimitConfig())
        )
        self._rate_limiters: Dict[str, ExchangeRateLimiter] = {}
        self._initialize_rate_limiters()

        # 🔥 拆单统计缓存（用于计算平均价差）
        # 格式: {symbol: {'open': [trade_data, ...], 'close': [trade_data, ...]}}
        # trade_data: {'buy_price': Decimal, 'sell_price': Decimal, 'quantity': Decimal, 'spread_pct': Decimal}
        self._split_order_cache: Dict[str, Dict[str, list]] = {}
        self._emergency_tracker_ctx: ContextVar[Optional[List[Dict[str, Any]]]] = ContextVar(
            "arbitrage_emergency_tracker",
            default=None,
        )

        mode_str = "🔍 监控模式（不执行真实订单）" if self.monitor_only else "⚡ 实盘模式（执行真实订单）"
        logger.info(f"✅ [套利执行] 套利执行器初始化完成 - {mode_str}")

        # 🔥 WebSocket订阅将由主编排器在适当时机调用 initialize_websocket_subscriptions()
        # 不在 __init__ 中自动订阅，确保时序正确（先连接WebSocket，再订阅）

    def _extract_raw_order_status(self, order: Optional[OrderData]) -> str:
        """提取lighter原始状态字符串"""
        if not order:
            return ""
        params = getattr(order, "params", None)
        if isinstance(params, dict):
            raw = params.get("lighter_status") or params.get("status_raw")
            if raw:
                return str(raw)
        raw_data = getattr(order, "raw_data", None)
        if isinstance(raw_data, dict):
            raw_status = raw_data.get("status") or raw_data.get("state")
            if raw_status:
                return str(raw_status)
        return ""

    def _is_slippage_cancel(self, order: Optional[OrderData]) -> bool:
        """判断订单是否因滑点保护被取消"""
        if not order:
            return False
        status = getattr(order, "status", None)
        if status not in CLOSED_STATUSES:
            return False
        raw_status = self._extract_raw_order_status(order)
        return bool(raw_status and "slippage" in raw_status.lower())

    def _get_slippage_percent(self, request: Optional[ExecutionRequest] = None) -> Optional[Decimal]:
        """
        获取当前订单允许的最大滑点（小数，例如0.0005=0.05%）
        """
        if request and request.slippage_tolerance_pct is not None:
            return request.slippage_tolerance_pct

        fallback = getattr(self.config.order_execution, "max_slippage", None)
        if fallback is None:
            return None
        try:
            return Decimal(str(fallback))
        except (InvalidOperation, TypeError, ValueError):
            return None

    def _record_emergency_close_event(
        self,
        *,
        exchange: str,
        symbol: str,
        quantity: Decimal,
        is_buy_to_close: bool,
        request: Optional[ExecutionRequest],
        status: str,
    ) -> None:
        tracker = self._emergency_tracker_ctx.get(None)
        if tracker is None:
            return
        context = "close"
        if request is not None and request.is_open:
            context = "open"
        exchange_role = "unknown"
        if request and request.exchange_buy:
            if exchange.lower() == request.exchange_buy.lower():
                exchange_role = "buy"
        if request and request.exchange_sell:
            if exchange.lower() == request.exchange_sell.lower():
                exchange_role = "sell"
        entry = {
            "exchange": exchange,
            "symbol": symbol,
            "quantity": str(quantity),
            "is_buy_to_close": is_buy_to_close,
            "context": context,
            "exchange_role": exchange_role,
            "grid_level": request.grid_level if request else None,
            "status": status,
            "timestamp": datetime.utcnow().isoformat(timespec="seconds"),
        }
        tracker.append(entry)

    def _resolve_min_order_qty(
        self,
        request: Optional[ExecutionRequest],
        exchange_name: Optional[str],
    ) -> Optional[Decimal]:
        """解析执行请求中指定交易所的最小下单量限制。"""
        if not request or not exchange_name:
            return None
        mapping = getattr(request, "min_exchange_order_qty", None) or {}
        key = str(exchange_name).lower()
        raw_value = mapping.get(key, mapping.get("default"))
        if raw_value in (None, 0, Decimal("0")):
            return None
        try:
            return self._to_decimal_value(raw_value)
        except Exception:
            return None

    async def initialize_websocket_subscriptions(self):
        """
        初始化WebSocket订阅（公共方法，由主编排器调用）

        功能：
        1. 确保WebSocket连接已建立
        2. 订阅用户数据流（订单/持仓更新的前提）
        3. 注册订单成交回调

        注意：
        - 必须在 adapter.connect() 之后调用
        - 只在实盘模式下调用
        """
        if self.monitor_only:
            logger.info("🔍 [套利执行] 监控模式，跳过WebSocket订阅")
            return

        logger.info("📡 [套利执行] 开始初始化WebSocket订阅...")

        try:
            for exchange_name, adapter in self.exchange_adapters.items():
                exchange_tag = self._format_exchange_tag(exchange_name)
                try:
                    # 🔥 步骤1：获取WebSocket组件
                    ws = None
                    if hasattr(adapter, 'websocket'):
                        ws = adapter.websocket
                    elif hasattr(adapter, '_websocket'):
                        ws = adapter._websocket

                    if not ws:
                        logger.info(
                            f"ℹ️ [套利执行] [{exchange_tag}] 无WebSocket组件，将使用REST轮询")
                        continue

                    # 🔥 步骤2：确保WebSocket已连接（兼容不同属性名）
                    ws_connected = getattr(ws, '_ws_connected', None) or getattr(
                        ws, '_connected', None) or getattr(ws, 'connected', None)
                    if not ws_connected:
                        logger.warning(
                            f"⚠️ [套利执行] [{exchange_tag}] WebSocket未连接，跳过订阅")
                        continue

                    # 🔥 步骤3：订阅用户数据流（关键！EdgeX和Backpack都需要此步骤）
                    if hasattr(ws, 'subscribe_user_data'):
                        try:
                            # 注册一个空回调（实际处理在 subscribe_order_fills 中）
                            # 注意：不同交易所的回调签名可能不同，使用 *args 兼容
                            async def _user_data_callback(*args, **kwargs):
                                pass  # WebSocket内部会自动分发给订单/持仓回调

                            await ws.subscribe_user_data(_user_data_callback)
                            logger.info(f"✅ [套利执行] [{exchange_tag}] 用户数据流已订阅")
                        except OrderStateUnknownError:
                            raise
                        except Exception as e:
                            logger.warning(
                                f"⚠️ [套利执行] [{exchange_tag}] 用户数据流订阅失败: {e}")

                    # 🔥 步骤4：注册订单状态回调（包括部分成交和完全成交）
                    # 优先订阅 subscribe_orders（接收所有订单状态更新，包括部分成交和完全成交）
                    # 如果不支持 subscribe_orders，则降级到 subscribe_order_fills（只接收完全成交）
                    order_subscription_success = False

                    if hasattr(ws, 'subscribe_orders'):
                        try:
                            await ws.subscribe_orders(self._on_order_update)
                            logger.info(
                                f"✅ [套利执行] [{exchange_tag}] 订单状态回调已注册（含部分成交+完全成交）")
                            order_subscription_success = True
                        except OrderStateUnknownError:
                            raise
                        except Exception as e:
                            logger.warning(
                                f"⚠️ [套利执行] [{exchange_tag}] 订单状态回调注册失败: {e}")

                    # 🔥 只有在 subscribe_orders 不可用时才使用 subscribe_order_fills
                    if not order_subscription_success and hasattr(ws, 'subscribe_order_fills'):
                        try:
                            await ws.subscribe_order_fills(self._on_order_filled)
                            logger.info(
                                f"✅ [套利执行] [{exchange_tag}] 订单成交回调已注册（仅完全成交）")
                            order_subscription_success = True
                        except OrderStateUnknownError:
                            raise
                        except Exception as e:
                            logger.warning(
                                f"⚠️ [套利执行] [{exchange_tag}] 订单成交回调注册失败: {e}")

                    if not order_subscription_success:
                        logger.info(
                            f"ℹ️ [套利执行] [{exchange_tag}] 不支持WebSocket订单追踪，将使用REST轮询")

                    # 🔥 步骤5：订阅持仓更新（用于UI显示）
                    if hasattr(ws, 'subscribe_positions'):
                        async def _position_callback(*args, **kwargs):
                            """持仓更新回调（用于UI刷新）"""
                            # 这里可以添加持仓缓存更新逻辑
                            pass

                        await ws.subscribe_positions(_position_callback)
                        logger.info(f"✅ [套利执行] [{exchange_tag}] 持仓更新回调已注册")

                except (OrderStateUnknownError, GlobalPausedError):
                    raise
                except Exception as e:
                    logger.warning(
                        f"⚠️ [套利执行] [{exchange_tag}] WebSocket订阅失败: {e}，将使用REST轮询",
                        exc_info=True
                    )

            logger.info("✅ [套利执行] WebSocket订阅初始化完成")

        except OrderStateUnknownError:
            raise
        except Exception as e:
            logger.error(f"❌ [套利执行] WebSocket订阅初始化失败: {e}", exc_info=True)

    def set_live_price_resolver(
        self,
        resolver: Optional[Callable[[str, str, bool], Optional[Decimal]]],
    ) -> None:
        """
        注册实时盘口价格解析器，供限价重试刷新价格使用。
        """
        self._live_price_resolver = resolver

    def _get_live_market_price(
        self,
        exchange: Optional[str],
        symbol: Optional[str],
        is_buy: Optional[bool],
    ) -> Optional[Decimal]:
        """
        通过外部解析器获取最新盘口价格，失败则返回 None。
        """
        if (
            not self._live_price_resolver
            or not exchange
            or not symbol
            or is_buy is None
        ):
            return None
        try:
            price_value = self._live_price_resolver(
                exchange, symbol, bool(is_buy))
        except Exception as exc:  # pragma: no cover - 仅日志
            logger.debug(
                "⚠️ [盘口刷新] 获取 %s %s 最新价格失败: %s",
                exchange,
                symbol,
                exc,
            )
            return None
        if price_value in (None, 0, Decimal("0")):
            return None
        try:
            price_decimal = self._to_decimal_value(price_value)
        except Exception:
            return None
        if price_decimal <= Decimal("0"):
            return None

        # 打印对应订单簿的快照，方便复盘激进限价/补单的定价依据
        try:
            orderbook = self.data_processor.get_orderbook(
                exchange,
                symbol,
                max_age_seconds=self.data_freshness_seconds,
            )
        except Exception as snapshot_err:  # pragma: no cover - 仅日志
            orderbook = None
            logger.debug(
                "⚠️ [盘口刷新] 无法获取 %s/%s 的盘口快照: %s",
                exchange,
                symbol,
                snapshot_err,
            )

        ask_price = getattr(
            getattr(orderbook, "best_ask", None), "price", None)
        bid_price = getattr(
            getattr(orderbook, "best_bid", None), "price", None)
        try:
            ask_display = self._to_decimal_value(
                ask_price) if ask_price else None
        except Exception:
            ask_display = None
        try:
            bid_display = self._to_decimal_value(
                bid_price) if bid_price else None
        except Exception:
            bid_display = None

        logger.debug(
            "[盘口刷新] %s/%s Snapshot -> Ask:%s | Bid:%s | is_buy=%s | price=%s",
            exchange.upper(),
            symbol,
            f"{ask_display:.5f}" if isinstance(ask_display, Decimal) else "-",
            f"{bid_display:.5f}" if isinstance(bid_display, Decimal) else "-",
            bool(is_buy),
            price_decimal,
        )

        return price_decimal

    async def _on_order_update(self, order: OrderData):
        """
        WebSocket订单状态更新回调（由 orders 累计推送触发）

        - 记录部分成交进度，累积 filled 数量
        - 在达到目标数量或收到 FILLED 状态时唤醒等待协程
        """
        try:
            if order is None:
                logger.warning("⚠️ [套利执行] 收到空的订单推送，已忽略")
                return

            order_id = str(order.id) if getattr(
                order, "id", None) is not None else None
            client_id = str(order.client_id) if getattr(
                order, "client_id", None) else None
            exchange_name = (self._resolve_exchange_from_order(
                order) or "unknown").upper()
            status = getattr(order, "status", None)
            debug_enabled = logger.isEnabledFor(logging.DEBUG)
            if debug_enabled:
                filled_price = getattr(order, "average", None) or getattr(
                    order, "price", None)
                price_suffix = f", price={filled_price}" if filled_price is not None else ""
                logger.debug(
                    "📨 [套利执行][%s] WS订单更新: order_id=%s, client_id=%s, status=%s, filled=%s/%s%s",
                    exchange_name,
                    order_id,
                    client_id,
                    status.value if status else "unknown",
                    order.filled,
                    getattr(order, "amount", None),
                    price_suffix,
                )
            # 确保变量在后续使用前定义，避免非 debug 模式下引用未绑定
            price_suffix = ""

            candidate_ids: Set[str] = set()
            if order_id:
                candidate_ids.add(order_id)
            if client_id:
                candidate_ids.add(client_id)
            if not candidate_ids:
                logger.warning(
                    "⚠️ [套利执行][%s] WS订单更新缺少可识别ID，忽略: raw=%s",
                    exchange_name,
                    order,
                )
                return

            # 🔥 使用交易所提供的累计成交量
            accumulated = self._to_decimal_value(
                getattr(order, "filled", Decimal("0")))

            # 🔥 更新填充进度缓存（用于跟踪部分成交）
            for key in candidate_ids:
                self._order_fill_progress[key] = accumulated

            pending_order: Optional[OrderData] = None
            for key in candidate_ids:
                pending_order = self._lookup_pending_order(key)
                if pending_order:
                    break

            latency_keys = candidate_ids | (self._iter_order_keys(pending_order) if pending_order else set())

            target_quantity = Decimal("0")
            if pending_order and getattr(pending_order, "amount", None) is not None:
                target_quantity = self._to_decimal_value(pending_order.amount)
            if target_quantity <= Decimal("0"):
                target_quantity = self._to_decimal_value(
                    getattr(order, "amount", Decimal("0")))

            order_object = pending_order or order
            ws_average = getattr(order, "average", None)
            ws_price = getattr(order, "price", None)
            if ws_average is not None:
                order_object.average = self._to_decimal_value(ws_average)
            elif ws_price is not None:
                order_object.average = self._to_decimal_value(ws_price)
            if ws_price is not None:
                order_object.price = self._to_decimal_value(ws_price)
            if pending_order and order_id:
                pending_order.id = str(order_id)
                self.pending_orders[str(order_id)] = pending_order
            order_object.filled = accumulated
            if target_quantity > Decimal("0"):
                order_object.remaining = max(
                    target_quantity - accumulated, Decimal("0"))

            epsilon = Decimal("0.00000001")
            is_ws_marked_filled = bool(status and status == OrderStatus.FILLED)
            is_ws_cancelled = bool(status and status == OrderStatus.CANCELED)
            is_closed_status = is_ws_marked_filled or is_ws_cancelled
            reached_target = target_quantity > Decimal(
                "0") and accumulated + epsilon >= target_quantity

            if not reached_target:
                if is_ws_marked_filled and accumulated <= epsilon:
                    logger.warning(
                        "⚠️ [套利执行][%s] WS标记FILLED但成交量为0: order_id=%s，视为未成交关闭",
                        exchange_name,
                        order_id or client_id or "unknown",
                    )
                elif not is_closed_status:
                    logger.info(
                        "⏳ [套利执行][%s] WS部分成交: order_id=%s, filled=%s/%s (%.1f%%)%s",
                        exchange_name,
                        order_id or client_id or "unknown",
                        accumulated,
                        target_quantity or "?",
                        (accumulated / target_quantity *
                         100) if target_quantity > 0 else 0,
                        price_suffix,
                    )
                    return

            # 🔥 订单进入关闭状态（全部成交 / 部分成交后关闭 / 取消）
            final_quantity = accumulated
            if target_quantity > Decimal("0"):
                final_quantity = min(accumulated, target_quantity)
                order_object.amount = target_quantity
            else:
                actual_amount = self._to_decimal_value(
                    order_object.amount or accumulated)
                order_object.amount = actual_amount if actual_amount > Decimal(
                    "0") else accumulated

            # 当交易所缺少可靠成交量时才允许回退（当前所有交易所均能提供成交数量）
            if final_quantity <= epsilon:
                final_quantity = Decimal("0")

            order_object.filled = final_quantity
            order_object.remaining = max(
                self._to_decimal_value(order_object.amount) - final_quantity,
                Decimal("0"),
            )

            # 如果WS推送了真实的order_id（与REST返回的client_id不同），更新缓存
            if order_id and pending_order and str(pending_order.id) != order_id:
                logger.debug(
                    "ℹ️ [套利执行] 更新订单ID: %s -> %s", pending_order.id, order_id
                )
                pending_order.id = order_id
                self._register_pending_order(pending_order)

            # ------------------------------------------------------------------
            # 🔥 计算并缓存：下单提交 -> WS成交推送耗时（必须在清理 pending/submit_ts 之前）
            # - 仅在“有成交”时记录（filled>0），避免 0成交 IOC/撤单误伤
            # ------------------------------------------------------------------
            try:
                if final_quantity > epsilon:
                    ws_ts = time.time()
                    submit_ts: Optional[float] = None
                    for k in latency_keys:
                        ts = self._order_submit_ts.pop(str(k), None)
                        if ts is None:
                            continue
                        submit_ts = ts if submit_ts is None else min(submit_ts, ts)
                    # Consume submit times once; reconnect replays must not become new latency samples.
                    already_measured = any(k in self._order_ws_latency_info for k in latency_keys)
                    if not already_measured and submit_ts is not None and submit_ts > 0:
                        latency_s = max(0.0, float(ws_ts - float(submit_ts)))
                        for k in latency_keys:
                            key = str(k)
                            self._order_ws_latency_info[key] = (
                                float(submit_ts), float(ws_ts), float(latency_s)
                            )
                            # FIFO 限制容量
                            if key not in self._order_ws_latency_keys:
                                self._order_ws_latency_keys.append(key)
                                if len(self._order_ws_latency_keys) > int(self._order_ws_latency_max_keys):
                                    old = self._order_ws_latency_keys.popleft()
                                    self._order_ws_latency_info.pop(str(old), None)

                        # 上报给全局风控（触发WS延迟暂停）
                        # ✅ 仅对“市价单”上报：
                        # - 市价单正常应快速完成（WS推送过慢更可能代表连接/推送异常）
                        # - 限价单可能长时间挂单等待成交，不能用该阈值一刀切误伤
                        from core.adapters.exchanges.models import OrderType as _OrderType
                        order_type = getattr(order_object, "type", None)
                        is_market = (
                            order_type == _OrderType.MARKET
                            or str(getattr(order_type, "value", order_type) or "").lower() == "market"
                        )
                        if is_market and callable(self._order_ws_latency_reporter):
                            sym = (getattr(order_object, "symbol", None) or "").strip() or (
                                getattr(order, "symbol", None) or ""
                            ).strip()
                            exch = (exchange_name or "unknown").strip().lower()
                            oid = str(order_id or client_id or "unknown")
                            self._order_ws_latency_reporter(
                                symbol=sym or "unknown",
                                exchange=exch or "unknown",
                                order_id=oid,
                                latency_seconds=float(latency_s),
                            )
            except Exception:
                # 仅用于观测/风控，不影响WS回调主流程
                pass

            self._remove_pending_order(order_object, keys=candidate_ids)
            self._clear_fill_progress(candidate_ids)

            # 🔥 只在有等待事件时才触发和打印日志（说明是我们主动等待的订单）
            # 忽略"推送早于注册"的情况（这些是 Lighter 的重复推送）
            event_triggered = False
            triggered_keys = []  # 记录触发的 key，用于后续删除事件
            for key in candidate_ids:
                if key in self._order_fill_events:
                    # 找到等待的订单，触发事件并打印日志
                    self._order_fill_results[key] = order_object
                    self._order_fill_events[key].set()
                    event_triggered = True
                    triggered_keys.append(key)
                    logger.debug("✅ [套利执行] 订单事件已触发: %s", key)
                else:
                    # 推送早于注册，或者是重复推送，缓存但不打印
                    self._order_fill_results[key] = order_object
                    logger.debug("ℹ️ [套利执行] 成交推送早于事件注册或为重复推送，已缓存: %s", key)

            # 🔥 只有触发了等待事件才打印日志（避免 UNKNOWN 重复日志）
            if event_triggered:
                if reached_target:
                    log_label = "订单完全成交"
                elif final_quantity > Decimal("0"):
                    log_label = "订单部分成交后关闭"
                else:
                    log_label = "订单未成交已关闭"
                logger.info(
                    "✅ [套利执行][%s] %s: order_id=%s, filled=%s/%s%s",
                    exchange_name,
                    log_label,
                    order_id or client_id or "unknown",
                    final_quantity,
                    target_quantity or final_quantity,
                    price_suffix,
                )
                # 🔥 立即删除已触发的事件，防止重复推送再次触发
                for key in triggered_keys:
                    self._order_fill_events.pop(key, None)
                    logger.debug("🔥 [套利执行] 已删除事件: %s（防止重复触发）", key)

                # 🔥 在打印日志后再清理映射关系，避免后续重复推送找不到交易所名称
                for key in candidate_ids:
                    self._order_exchange_map.pop(key, None)
                    self._lighter_ws_order_map.pop(key, None)

        except Exception as e:
            logger.error("❌ [套利执行] WebSocket订单更新回调异常: %s", e, exc_info=True)

    async def _on_order_filled(self, order: OrderData):
        """
        WebSocket订单完全成交回调（仅在status=FILLED时触发）

        注意：此回调仅作为备份，主要处理逻辑在 _on_order_update 中
        某些交易所可能只推送完全成交事件，因此保留此回调以确保兼容性
        """
        # 🔥 直接调用 _on_order_update 处理，避免代码重复
        await self._on_order_update(order)

    def _format_quantity(
        self,
        raw_quantity: Decimal,
        precision: int
    ) -> Decimal:
        """
        根据配置的小数精度格式化数量（向下取整，兼容所有交易所步进）
        """
        if precision < 0:
            precision = 0

        quantize_unit = Decimal(
            '1').scaleb(-precision) if precision > 0 else Decimal('1')
        formatted_quantity = raw_quantity.quantize(
            quantize_unit, rounding=ROUND_DOWN)

        if formatted_quantity <= 0:
            formatted_quantity = quantize_unit

        return formatted_quantity

    def calculate_order_quantity(
        self,
        symbol: str
    ) -> Tuple[bool, Decimal, str]:
        """
        计算下单数量

        Args:
            symbol: 交易对

        Returns:
            (是否可以下单, 格式化后的数量, 错误信息)
        """
        try:
            # 1. 获取配置（优先使用代币特定配置，否则使用默认配置）
            config = self.config.quantity_config.get(
                symbol,
                self.config.quantity_config.get('default')
            )

            if not config:
                logger.error(f"❌ [数量计算] {symbol}: 未找到数量配置")
                return False, Decimal("0"), "未找到数量配置"

            target_quantity = Decimal(str(config.single_order_quantity))
            if target_quantity <= 0:
                logger.error(f"❌ [数量计算] {symbol}: single_order_quantity 必须大于0")
                return False, Decimal("0"), "数量配置无效"

            raw_quantity = target_quantity

            precision = getattr(config, 'quantity_precision', 4)
            quantity = self._format_quantity(raw_quantity, precision)

            # 3. 检查是否为0（无法格式化）
            if quantity == 0:
                logger.warning(f"⚠️ [数量计算] {symbol}: 无法格式化数量，放弃下单")
                return False, Decimal("0"), "无法格式化数量"

            # 4. 最大持仓限制由全局风险控制统一校验（参见 GlobalRiskController）

            now = time.time()
            last_log_ts = self._quantity_log_times.get(symbol, 0.0)
            last_qty = self._quantity_log_values.get(symbol)
            if (now - last_log_ts) >= self._quantity_log_interval or last_qty != quantity:
                max_qty = getattr(config, 'max_position_quantity', None)
                log_msg = (
                    f"✅ [数量计算] {symbol}: 目标数量={target_quantity}, "
                    f"格式化后={quantity} (精度={precision}位)"
                )
                if max_qty:
                    log_msg += f", 最大持仓={max_qty}"
                logger.info(log_msg)
                self._quantity_log_times[symbol] = now
                self._quantity_log_values[symbol] = quantity
            return True, quantity, ""

        except Exception as e:
            logger.error(f"❌ [数量计算] {symbol}: 计算失败: {e}", exc_info=True)
            return False, Decimal("0"), str(e)

    async def execute_arbitrage(
        self,
        request: ExecutionRequest
    ) -> ExecutionResult:
        """
        执行套利订单

        Args:
            request: 执行请求

        Returns:
            执行结果
        """
        pair_key = (request.symbol, tuple(sorted((request.exchange_buy, request.exchange_sell))))
        if not self.monitor_only and (self._unknown_execution_pairs or any(
            getattr(self.exchange_adapters.get(exchange), '_uncertain', None)
            for exchange in (request.exchange_buy, request.exchange_sell)
        )):
            reason = '订单状态不确定，需人工核对双腿，自动交易保持暂停'
            self.order_strategy_executor._mark_manual_intervention(request, reason)
            return ExecutionResult(success=False, error_message=reason, failure_code='ORDER_STATE_UNKNOWN')
        await self._await_round_pause_window()
        grid_desc = ""
        if request.grid_level and request.grid_level > 0:
            action_label = "开仓" if request.is_open else "平仓"
            grid_desc = f"{action_label}T{request.grid_level}"
        emergency_tracker: List[Dict[str, Any]] = []
        request_token = self._execution_request_ctx.set(request)
        request._confirmed_fills = {}
        tracker_token = self._emergency_tracker_ctx.set(emergency_tracker)
        try:
            # 🔥 检查监控模式
            if self.monitor_only:
                # ------------------------------------------------------------------
                # 模拟盘（监控模式）：
                # - 不发真实订单
                # - 构造“模拟已成交”订单对象，复用现有成交统计打印与上层持仓记录逻辑
                # - 执行计划与实盘一致（用于显示/识别 limit/market/双限价等）
                # ------------------------------------------------------------------
                execution_plan = self.order_strategy_executor.determine_execution_plan(request)

                # ✅ 极简：仅用于 tools/follow_executor_fills.py 识别 “开/平仓Tn”
                # 约束："[套利执行]" 后必须紧跟 "开仓Tn/平仓Tn"（正则只允许空白）
                if grid_desc:
                    buy_leg = request.buy_symbol or request.symbol
                    sell_leg = request.sell_symbol or request.symbol
                    logger.info(
                        f"[套利执行] {grid_desc} 执行计划: "
                        f"买入={request.exchange_buy}/{buy_leg}@{request.price_buy:.2f}, "
                        f"卖出={request.exchange_sell}/{sell_leg}@{request.price_sell:.2f}, "
                        f"数量={request.quantity}"
                    )

                buy_symbol = request.buy_symbol or request.symbol
                sell_symbol = request.sell_symbol or request.symbol
                now = datetime.now()

                def _to_order_type(mode: Optional[str]) -> OrderType:
                    s = str(mode or "").strip().lower()
                    if s == "limit":
                        return OrderType.LIMIT
                    return OrderType.MARKET

                def _leg_order_mode(is_buy_leg: bool) -> str:
                    mode = str(execution_plan.get("mode") or "").strip().lower()
                    if mode == "market_market":
                        return "market"
                    if mode == "limit_limit":
                        return "limit"
                    # limit_market（或其他变体）：按 first/second_is_buy 对应腿取 mode
                    if execution_plan.get("first_is_buy") is is_buy_leg:
                        return str(execution_plan.get("first_order_mode") or "market")
                    if execution_plan.get("second_is_buy") is is_buy_leg:
                        return str(execution_plan.get("second_order_mode") or "market")
                    return "market"

                buy_order_type = _to_order_type(_leg_order_mode(True))
                sell_order_type = _to_order_type(_leg_order_mode(False))

                qty = self._to_decimal_value(getattr(request, "quantity", None))
                price_buy = self._to_decimal_value(getattr(request, "price_buy", None))
                price_sell = self._to_decimal_value(getattr(request, "price_sell", None))

                # 模拟订单：必须包含 average/price/filled，确保 _log_execution_summary 可复用
                buy_order = OrderData(
                    id=f"SIM-{request.exchange_buy}-{int(time.time() * 1000)}-BUY",
                    client_id=f"SIM-CLIENT-{request.exchange_buy}-{int(time.time() * 1000)}",
                    symbol=str(buy_symbol),
                    side=OrderSide.BUY,
                    type=buy_order_type,
                    amount=qty,
                    price=price_buy if buy_order_type == OrderType.LIMIT else None,
                    filled=qty,
                    remaining=Decimal("0"),
                    cost=(price_buy * qty) if price_buy and qty else Decimal("0"),
                    average=price_buy,
                    status=OrderStatus.FILLED,
                    timestamp=now,
                    updated=now,
                    fee=None,
                    trades=[],
                    params={},
                    raw_data={"simulated": True, "paper_trade": True},
                )
                sell_order = OrderData(
                    id=f"SIM-{request.exchange_sell}-{int(time.time() * 1000)}-SELL",
                    client_id=f"SIM-CLIENT-{request.exchange_sell}-{int(time.time() * 1000)}",
                    symbol=str(sell_symbol),
                    side=OrderSide.SELL,
                    type=sell_order_type,
                    amount=qty,
                    price=price_sell if sell_order_type == OrderType.LIMIT else None,
                    filled=qty,
                    remaining=Decimal("0"),
                    cost=(price_sell * qty) if price_sell and qty else Decimal("0"),
                    average=price_sell,
                    status=OrderStatus.FILLED,
                    timestamp=now,
                    updated=now,
                    fee=None,
                    trades=[],
                    params={},
                    raw_data={"simulated": True, "paper_trade": True},
                )

                slip = self._log_execution_summary(
                    request=request,
                    order_buy=buy_order,
                    order_sell=sell_order,
                    is_open=bool(getattr(request, "is_open", True)),
                    is_last_split=bool(getattr(request, "is_last_split", False)),
                ) or {}

                result = ExecutionResult(
                    success=True,
                    order_buy=buy_order,
                    order_sell=sell_order,
                    success_quantity=qty,
                    error_message="",
                    slippage_pct=slip.get("slippage_pct"),
                    high_slippage_threshold_pct=slip.get("high_slippage_threshold_pct"),
                )
                result.emergency_closes = []
                return result

            async def run_reserved_plan():
                # A queued request may have passed the entrance check before another plan failed.
                if self._unknown_execution_pairs or any(
                    getattr(self.exchange_adapters.get(exchange), '_uncertain', None)
                    for exchange in (request.exchange_buy, request.exchange_sell)
                ):
                    raise OrderStateUnknownError('arcus')
                if self.is_segmented_mode:
                    self._validate_pair_quantity(request)
                if not request.is_open and getattr(self, "before_close_check", None):
                    allowed, reason = await self.before_close_check(request)
                    if not allowed:
                        return ExecutionResult(success=False, error_message=reason, failure_code="CLOSE_REJECTED")
                if request.is_open and getattr(self, "before_open_check", None):
                    allowed, reason = await self.before_open_check(request)
                    if not allowed:
                        return ExecutionResult(success=False, error_message=reason, failure_code="RISK_REJECTED")
                try:
                    if callable(self._global_pause_checker) and (request.is_open or not request.allow_when_global_paused):
                        blocked, pause_reason = self._global_pause_checker()
                        if blocked:
                            return ExecutionResult(success=False, error_message=pause_reason, failure_code="global_paused")
                    outcome = await self._run_execution_plan(request, grid_desc)
                    self._apply_confirmed_fills(request, outcome)
                except OrderStateUnknownError:
                    self._unknown_execution_pairs.add(pair_key)
                    raise
                if outcome.failure_code == 'ORDER_STATE_UNKNOWN':
                    self._unknown_execution_pairs.add(pair_key)
                return outcome

            exchanges = self._collect_request_exchanges(request)
            reservation = self._reserve_exchange_slots(exchanges)
            if reservation:
                async with reservation:
                    result = await run_reserved_plan()
            else:
                result = await run_reserved_plan()
            result.emergency_closes = list(emergency_tracker)
            if result.failure_code == 'ORDER_STATE_UNKNOWN':
                self._unknown_execution_pairs.add(pair_key)
            return result

        except OrderStateUnknownError as exc:
            self._unknown_execution_pairs.add(pair_key)
            self.order_strategy_executor._mark_manual_intervention(request, str(exc))
            return ExecutionResult(success=False, error_message=str(exc), failure_code='ORDER_STATE_UNKNOWN')
        except Exception as e:
            logger.error(f"[套利执行] 执行套利失败: {e}", exc_info=True)
            result = ExecutionResult(
                success=False,
                error_message=str(e)
            )
            result.emergency_closes = list(emergency_tracker)
            return result
        finally:
            self._execution_request_ctx.reset(request_token)
            self._emergency_tracker_ctx.reset(tracker_token)
            self._schedule_next_round_pause()

    def _mark_symbol_waiting(
        self,
        request: Optional[ExecutionRequest],
        reason: str
    ) -> None:
        """
        将符号标记为“等待”，仅针对开仓失败的情况，避免继续触发同一网格。
        """
        if (
            not request
            or not request.is_open
            or not self.symbol_state_manager
        ):
            return
        self.symbol_state_manager.defer(
            symbol=request.symbol,
            reason=reason,
            grid_level=request.grid_level,
            exchange_buy=request.exchange_buy,
            exchange_sell=request.exchange_sell,
        )

    async def _run_execution_plan(
        self,
        request: ExecutionRequest,
        grid_desc: str
    ) -> ExecutionResult:
        """根据策略计划执行实际下单"""
        execution_plan = self.order_strategy_executor.determine_execution_plan(
            request)

        # 🔢 追加网格阈值说明
        threshold_suffix = ""
        if request.grid_threshold_pct is not None:
            threshold_suffix = f"(>={request.grid_threshold_pct:.4f}%)"

        # 🔥 构建日志：将 grid_desc 放在前面，添加完整盘口数据
        action_label = ""
        if grid_desc:
            action_label = f"{grid_desc}{threshold_suffix} "
        elif threshold_suffix:
            action_label = f"{threshold_suffix} "

        # 🔥 构建4组盘口数据：买入腿(Ask/Bid) + 卖出腿(Ask/Bid)
        buy_leg_label = request.buy_symbol or request.symbol
        sell_leg_label = request.sell_symbol or request.symbol

        def _fmt_price(value: Optional[Decimal]) -> str:
            try:
                return f"{value:.2f}"
            except (TypeError, ValueError):
                return "--"

        buy_ask_display = (
            request.orderbook_buy_ask
            if request.orderbook_buy_ask is not None
            else getattr(request, "price_buy", None)
        )
        buy_bid_display = request.orderbook_buy_bid
        sell_ask_display = request.orderbook_sell_ask
        sell_bid_display = (
            request.orderbook_sell_bid
            if request.orderbook_sell_bid is not None
            else getattr(request, "price_sell", None)
        )

        orderbook_info = (
            "\n   盘口: "
            f"买入腿{buy_leg_label}[Ask:{_fmt_price(buy_ask_display)}/Bid:{_fmt_price(buy_bid_display)}] "
            f"卖出腿{sell_leg_label}[Ask:{_fmt_price(sell_ask_display)}/Bid:{_fmt_price(sell_bid_display)}]"
        )

        # 构建执行订单摘要
        order_summary = (
            f"买入={request.exchange_buy}/{buy_leg_label}@{request.price_buy:.2f}, "
            f"卖出={request.exchange_sell}/{sell_leg_label}@{request.price_sell:.2f}, "
            f"数量={request.quantity}"
        )

        plan_msg = (
            f"[套利执行] {action_label}执行计划: "
            f"mode={execution_plan['mode']}, "
            f"parallel={execution_plan['parallel']} | "
            f"{order_summary}{orderbook_info}"
        )
        logger.info(plan_msg)

        if execution_plan["mode"] == "limit_market":
            return await self.order_strategy_executor.execute_limit_market_mode(request, execution_plan)
        if execution_plan["mode"] == "market_market":
            return await self.order_strategy_executor.execute_market_market_mode(request)
        if execution_plan["mode"] == "limit_limit":
            return await self.order_strategy_executor.execute_limit_limit_mode(request)
        return ExecutionResult(
            success=False,
            error_message=f"未知的下单模式: {execution_plan['mode']}"
        )

    def _get_exchange_mode_config(
        self,
        exchange: Optional[str],
        symbol: Optional[str]
    ) -> Optional[ExchangeOrderModeConfig]:
        """
        获取指定交易所/交易对的下单模式配置

        优先顺序：
        1. exchange:symbol（原始大小写）
        2. exchange:symbol.upper()
        3. exchange:symbol.lower()
        4. exchange（仅交易所维度）
        5. exchange.lower()
        6. default
        """
        if not exchange:
            return self.config.exchange_order_modes.get("default")

        candidates = []
        if symbol:
            candidates.extend([
                f"{exchange}:{symbol}",
                f"{exchange}:{symbol.upper()}",
                f"{exchange}:{symbol.lower()}",
            ])
        candidates.append(exchange)
        candidates.append(exchange.lower())
        candidates.append("default")

        for key in candidates:
            config = self.config.exchange_order_modes.get(key)
            if config:
                return config
        return None

    async def _handle_single_leg_shortfall(
        self,
        *,
        request: ExecutionRequest,
        buy_order: OrderData,
        sell_order: OrderData,
        buy_adapter: ExchangeInterface,
        sell_adapter: ExchangeInterface,
        buy_symbol: str,
        sell_symbol: str,
        missing_buy: bool,
        shortfall: Decimal,
        target_quantity: Decimal,
        epsilon: Decimal,
        cancel_opposite_order: bool = False,
        result_quantity: Optional[Decimal] = None
    ) -> ExecutionResult:
        """委托 OrderStrategyExecutor 处理双限价值差补齐逻辑。"""
        return await self.order_strategy_executor._handle_single_leg_shortfall(
            request=request,
            buy_order=buy_order,
            sell_order=sell_order,
            buy_adapter=buy_adapter,
            sell_adapter=sell_adapter,
            buy_symbol=buy_symbol,
            sell_symbol=sell_symbol,
            missing_buy=missing_buy,
            shortfall=shortfall,
            target_quantity=target_quantity,
            epsilon=epsilon,
            cancel_opposite_order=cancel_opposite_order,
            result_quantity=result_quantity,
        )

    async def _execute_lighter_ws_batch(
        self,
        request: ExecutionRequest,
        adapter: ExchangeInterface,
        buy_symbol: str,
        sell_symbol: str
    ) -> ExecutionResult:
        """委托 OrderStrategyExecutor 执行 lighter 批量模式。"""
        return await self.order_strategy_executor._execute_lighter_ws_batch(
            request=request,
            adapter=adapter,
            buy_symbol=buy_symbol,
            sell_symbol=sell_symbol,
        )

    def _select_order_by_side(
        self,
        orders: List[OrderData],
        side: OrderSide
    ) -> Optional[OrderData]:
        for order in orders:
            if order.side == side:
                return order
        return None

    async def _place_limit_order(
        self,
        adapter: ExchangeInterface,
        symbol: str,
        price: Decimal,
        quantity: Decimal,
        is_buy: bool,
        absolute_offset: Optional[Decimal] = None,
        request: Optional[ExecutionRequest] = None,
        override_price: Optional[Decimal] = None,
    ) -> Optional[OrderData]:
        """下限价订单"""
        side = OrderSide.BUY if is_buy else OrderSide.SELL
        exchange_name = self._get_exchange_name(adapter)
        exchange_tag = self._format_exchange_tag(exchange_name)
        attempt = 0

        async def _place_with_retry() -> Optional[OrderData]:
            nonlocal attempt
            while True:
                try:
                    exchange_config = self.config.exchange_order_modes.get(
                        exchange_name)
                    use_tick_precision = (
                        getattr(exchange_config, 'use_tick_precision', False)
                        if exchange_config else False
                    )

                    limit_price = price
                    price_forced = override_price is not None
                    if price_forced:
                        limit_price = override_price

                    abs_offset_value: Optional[Decimal] = None
                    if absolute_offset is not None:
                        try:
                            abs_offset_value = Decimal(str(absolute_offset))
                        except Exception:
                            abs_offset_value = None

                    absolute_offset_applied = False
                    if price_forced:
                        use_tick_precision = False
                        absolute_offset_applied = True

                    if (
                        abs_offset_value is not None
                        and abs_offset_value > Decimal('0')
                        and not price_forced
                    ):
                        limit_price = price + abs_offset_value if is_buy else price - abs_offset_value
                        if limit_price <= 0:
                            limit_price = price
                        absolute_offset_applied = True
                        use_tick_precision = False
                        logger.info(
                            f"[套利执行] [{exchange_tag}] 使用绝对价差偏移: "
                            f"基础价={price}, 偏移={abs_offset_value}, 下单价={limit_price}"
                        )

                    # 策略1：tick 精度
                    if use_tick_precision:
                        try:
                            if is_buy:
                                best_ask = price
                                step = self._infer_price_step(best_ask)
                                limit_price = best_ask - step
                                if limit_price <= 0:
                                    limit_price = step
                                limit_price = limit_price.quantize(
                                    step, rounding=ROUND_DOWN)
                                logger.info(
                                    f"[套利执行] [{exchange_tag}] Tick定价(买): "
                                    f"卖一={best_ask}, step={step}, 下单价={limit_price}, 数量={quantity}"
                                )
                            else:
                                best_bid = price
                                step = self._infer_price_step(best_bid)
                                limit_price = best_bid + step
                                limit_price = limit_price.quantize(
                                    step, rounding=ROUND_UP)
                                logger.info(
                                    f"[套利执行] [{exchange_tag}] Tick定价(卖): "
                                    f"买一={best_bid}, step={step}, 下单价={limit_price}, 数量={quantity}"
                                )
                        except Exception as tick_error:
                            logger.warning(
                                f"[套利执行] [{exchange_tag}] tick策略失败，使用百分比偏移: {tick_error}"
                            )
                            use_tick_precision = False

                    # 策略2：百分比偏移
                    if not use_tick_precision and not absolute_offset_applied and not price_forced:
                        price_offset = (
                            exchange_config.limit_price_offset
                            if exchange_config else 0.001
                        )
                        if is_buy:
                            limit_price = price * \
                                (1 + Decimal(str(price_offset)))
                        else:
                            limit_price = price * \
                                (1 - Decimal(str(price_offset)))
                        logger.debug(
                            f"[套利执行] [{exchange_tag}] 百分比偏移策略: "
                            f"市场价={price}, 偏移={price_offset}, 下单价={limit_price}"
                        )

                    if exchange_name == 'arcus':
                        # Quantize strategy-generated prices; adapter still rejects invalid direct inputs.
                        from decimal import ROUND_DOWN, ROUND_UP
                        market = adapter._market(symbol)
                        rounding = ROUND_DOWN if is_buy else ROUND_UP
                        for _ in range(len(market.get('tickTiers', [])) + 2):
                            tick = adapter._price_tick(market, limit_price)
                            rounded = (limit_price / tick).to_integral_value(rounding=rounding) * tick
                            if rounded == limit_price:
                                break
                            limit_price = rounded
                    params = {'timeInForce': 'GTC'}
                    if (request and self._should_use_reduce_only(exchange_name, request.is_open)
                            and not (exchange_name == 'lighter' and ('SPOT' in symbol.upper() or 'SPOT' in request.symbol.upper()))):
                        params['reduce_only'] = True
                    order = await adapter.create_order(
                        symbol=symbol,
                        side=side,
                        order_type=OrderType.LIMIT,
                        amount=quantity,
                        price=limit_price,
                        params=params
                    )

                    if order:
                        self._register_pending_order(order)
                        self._register_order_exchange_mapping(
                            order, exchange_name)
                        notify_network_recovered(exchange_name)

                    return order

                except (OrderStateUnknownError, GlobalPausedError):
                    raise
                except Exception as e:
                    if self.reduce_only_handler.is_reduce_only_error(e):
                        if request:
                            self.reduce_only_handler.register_reduce_only_event(
                                request,
                                exchange_name,
                                symbol,
                                closing_issue=(not request.is_open),
                                reason=str(e)
                            )
                        raise ReduceOnlyRestrictionError(
                            exchange_name, symbol, e) from e
                    if await self._handle_order_error_and_wait(exchange_name, "限价", e, attempt):
                        attempt += 1
                        continue
                    return None

        if exchange_name and exchange_name.lower() == "lighter":
            async with self._lighter_order_lock:
                return await _place_with_retry()
        return await _place_with_retry()

    async def _place_market_order(
        self,
        adapter: ExchangeInterface,
        symbol: str,
        quantity: Decimal,
        is_buy: bool,
        reduce_only: bool = False,
        request: Optional[ExecutionRequest] = None,
        *,
        slippage_override: Optional[Decimal] = None,
        force_rest: bool = False,
    ) -> Optional[OrderData]:
        """下市价订单"""
        side = OrderSide.BUY if is_buy else OrderSide.SELL
        exchange_name = self._get_exchange_name(adapter)
        is_open_context = True
        if request is not None:
            is_open_context = request.is_open
        else:
            is_open_context = False
        if reduce_only:
            reduce_only = self._should_use_reduce_only(
                exchange_name, is_open_context)
        attempt = 0
        use_lighter_ws = (
            exchange_name
            and exchange_name.lower() == "lighter"
            and hasattr(adapter, "place_market_orders_ws_batch")
            and not force_rest
        )

        async def _place_with_retry() -> Optional[OrderData]:
            nonlocal attempt
            while True:
                try:
                    if use_lighter_ws:
                        order = await self._place_lighter_market_order_ws(
                            adapter=adapter,
                            symbol=symbol,
                            quantity=quantity,
                            is_buy=is_buy,
                            reduce_only=reduce_only,
                            request=request,
                            slippage_override=slippage_override,
                        )
                    else:
                        params = {}
                        if reduce_only:
                            params["reduce_only"] = True

                        if exchange_name == 'arcus':
                            params['slippage_tolerance_pct'] = slippage_override or self._get_slippage_percent(request)

                        # 🔥 Lighter: REST 市价单支持 slippage_multiplier（用于复用现有“滑点倍数放大”方案）
                        if (
                            exchange_name
                            and exchange_name.lower() == "lighter"
                            and slippage_override is not None
                        ):
                            try:
                                base_slippage = getattr(getattr(adapter, "_rest", None), "base_slippage", None)
                                if base_slippage and base_slippage > Decimal("0"):
                                    params["slippage_multiplier"] = Decimal(str(slippage_override)) / Decimal(str(base_slippage))
                                else:
                                    logger.warning(
                                        "⚠️ [套利执行] Lighter REST 市价单无法计算 slippage_multiplier（base_slippage缺失或为0），将忽略滑点覆盖: symbol=%s",
                                        symbol,
                                    )
                            except Exception as exc:
                                logger.warning(
                                    "⚠️ [套利执行] Lighter REST 市价单设置 slippage_multiplier 失败，将忽略滑点覆盖: symbol=%s err=%s",
                                    symbol,
                                    exc,
                                )
                        self._raise_if_global_paused_before_submit(request=request, stage="submit")
                        if request is not None:
                            request._has_submitted_order = True
                        order = await adapter.create_order(
                            symbol=symbol,
                            side=side,
                            order_type=OrderType.MARKET,
                            amount=quantity,
                            params=params or None
                        )

                    if order:
                        self._register_pending_order(order)
                        self._register_order_exchange_mapping(
                            order, exchange_name)
                        notify_network_recovered(exchange_name)

                    return order

                except (OrderStateUnknownError, GlobalPausedError):
                    raise
                except Exception as e:
                    if self.reduce_only_handler.is_reduce_only_error(e):
                        if request:
                            self.reduce_only_handler.register_reduce_only_event(
                                request,
                                exchange_name,
                                symbol,
                                closing_issue=(not request.is_open),
                                reason=str(e)
                            )
                        raise ReduceOnlyRestrictionError(
                            exchange_name, symbol, e) from e
                    if await self._handle_order_error_and_wait(exchange_name, "市价", e, attempt):
                        attempt += 1
                        continue
                    return None

        should_lock = (
            exchange_name
            and exchange_name.lower() == "lighter"
            and not use_lighter_ws
        )
        if should_lock:
            async with self._lighter_order_lock:
                return await _place_with_retry()
        return await _place_with_retry()

    async def _place_lighter_market_order_ws(
        self,
        adapter: ExchangeInterface,
        symbol: str,
        quantity: Decimal,
        is_buy: bool,
        reduce_only: bool,
        request: Optional[ExecutionRequest] = None,
        slippage_override: Optional[Decimal] = None,
    ) -> OrderData:
        """lighter专用：通过WS批量接口发送单笔市价单"""
        # 现货不支持 reduce_only，兜底关闭以避免签名错误
        is_spot_symbol = "SPOT" in str(symbol).upper()
        if is_spot_symbol:
            reduce_only = False
        payload = [{
            "symbol": symbol,
            "side": "buy" if is_buy else "sell",
            "quantity": quantity
        }]
        if reduce_only:
            payload[0]["reduce_only"] = True

        # 🔥 传递决策引擎的目标价格（用于滑点保护）
        target_price = None
        price_source = "实时市场盘口价"
        if request:
            target_price = self._to_decimal_value(
                request.price_buy if is_buy else request.price_sell
            )
            if target_price and target_price > Decimal("0"):
                payload[0]["target_price"] = target_price
                price_source = "决策引擎信号价"

        # 🔥 获取滑点参数
        slippage_percent = slippage_override
        slippage_source = "手动覆盖"
        if slippage_percent is None:
            slippage_percent = self._get_slippage_percent(request)
            if request and request.slippage_tolerance_pct is not None:
                slippage_source = f"套利对配置[{request.symbol}]"
            else:
                slippage_source = "全局默认配置[order_execution.max_slippage]"
        else:
            # 🔥 如果滑点很大（>1%），判定为紧急平仓场景
            if slippage_percent and slippage_percent > Decimal("0.01"):
                slippage_source = "紧急平仓配置滑点"
            else:
                slippage_source = "手动覆盖"

        # 🔥 输出详细的市价单参数日志
        slippage_display = f"{float(slippage_percent or 0) * 100:.4f}%" if slippage_percent else "未设置"
        logger.info(
            f"📊 [套利执行] 市价单参数: 方向={'买入' if is_buy else '卖出'}, "
            f"数量={quantity}, 价格基准=[{price_source}]{f'({target_price})' if target_price else ''}, "
            f"滑点={slippage_display}(来源: {slippage_source})"
        )
        try:
            response = await adapter.place_market_orders_ws_batch(
                payload,
                slippage_percent=slippage_percent
            )
            orders = response.get("orders") if isinstance(response, dict) else None
            if (not isinstance(orders, list) or len(orders) != 1
                    or not isinstance(orders[0], OrderData)
                    or not orders[0].id
                    or orders[0].symbol != (adapter._normalize_symbol(symbol)
                                              if hasattr(adapter, "_normalize_symbol") else symbol)
                    or orders[0].side != (OrderSide.BUY if is_buy else OrderSide.SELL)
                    or orders[0].amount != quantity):
                raise OrderStateUnknownError("lighter")
            order = orders[0]
            self._register_order_exchange_mapping(order, "lighter")
            return order
        except OrderStateUnknownError:
            raise
        except Exception as e:
            exchange_name = self._get_exchange_name(adapter)
            if self.reduce_only_handler.is_reduce_only_error(e):
                if request:
                    self.reduce_only_handler.register_reduce_only_event(
                        request,
                        exchange_name,
                        symbol,
                        closing_issue=(not request.is_open),
                        reason=str(e)
                    )
                raise ReduceOnlyRestrictionError(
                    exchange_name, symbol, e) from e
            raise

    async def _place_aggressive_limit_order_lighter_rest(
        self,
        *,
        adapter: ExchangeInterface,
        symbol: str,
        quantity: Decimal,
        is_buy: bool,
        reduce_only: bool,
        slippage_override: Decimal,
    ) -> Optional[OrderData]:
        """
        Lighter 专用：用 REST 下“激进限价 IOC”来替代最后一次市价补单。

        - 价格使用 Lighter REST 的滑点保护价计算逻辑（盘口价 * (1±slippage)）
        - time_in_force=IOC：尽量贴近市价行为，但仍可在链上/撮合层面更“可见”
        """
        exchange_name = self._get_exchange_name(adapter)
        if not exchange_name or exchange_name.lower() != "lighter":
            return None

        # 计算 slippage_multiplier（LighterRest 使用 base_slippage * multiplier）
        try:
            rest = getattr(adapter, "_rest", None)
            base_slippage = getattr(rest, "base_slippage", None)
            if not base_slippage or base_slippage <= Decimal("0"):
                logger.warning(
                    "⚠️ [套利执行] Lighter 激进限价无法计算 slippage_multiplier（base_slippage缺失或为0），放弃限价补单: %s",
                    symbol,
                )
                return None
            slippage_multiplier = Decimal(str(slippage_override)) / Decimal(str(base_slippage))
        except Exception as exc:
            logger.warning(
                "⚠️ [套利执行] Lighter 激进限价计算 slippage_multiplier 失败: %s",
                exc,
            )
            return None

        # 用 LighterRest 内部逻辑计算“滑点保护价”（作为激进限价）
        try:
            side_str = "buy" if is_buy else "sell"
            price = await rest._calculate_slippage_protection_price(  # noqa: SLF001（此处为适配器内部排障/策略特例）
                symbol=symbol,
                side=side_str,
                provided_price=None,
                slippage_multiplier=slippage_multiplier,
            )
        except OrderStateUnknownError:
            raise
        except Exception as exc:
            logger.warning(
                "⚠️ [套利执行] Lighter 激进限价计算保护价失败: %s",
                exc,
            )
            return None

        if not price:
            logger.error("❌ [套利执行] Lighter 激进限价补单失败：无法计算价格: %s", symbol)
            return None

        side = OrderSide.BUY if is_buy else OrderSide.SELL
        params: Dict[str, Any] = {"time_in_force": "IOC"}
        if reduce_only:
            params["reduce_only"] = True

        logger.info(
            "📌 [套利执行] Lighter 第3次补单改用激进限价(IOC): symbol=%s side=%s qty=%s price=%s slippage=%s",
            symbol,
            "buy" if is_buy else "sell",
            quantity,
            price,
            f"{float(slippage_override)*100:.4f}%",
        )

        # 强制走 REST：create_order 会走 adapter._rest.place_order -> signer_client.create_order
        order = await adapter.create_order(
            symbol=symbol,
            side=side,
            order_type=OrderType.LIMIT,
            amount=quantity,
            price=price,
            params=params,
        )
        if order:
            self._register_pending_order(order)
            self._register_order_exchange_mapping(order, exchange_name)
        return order

    async def _handle_order_error_and_wait(
        self,
        exchange_name: str,
        order_type_label: str,
        exception: Exception,
        attempt: int
    ) -> bool:
        """
        处理下单异常并根据类型决定是否重试

        Returns:
            bool: True 表示应继续重试，False 表示停止重试
        """
        next_attempt = attempt + 1
        rate_limited, retry_after = self._is_rate_limit_error(exception)

        if rate_limited:
            wait_seconds = retry_after
            if wait_seconds is None:
                wait_seconds = self._get_rate_limit_default_cooldown(
                    exchange_name)
            wait_seconds = max(wait_seconds, 0.1)
            self._apply_rate_limit_cooldown(exchange_name, wait_seconds)
            if next_attempt <= self._network_retry_max_attempts:
                logger.warning(
                    "⚠️ [限流] %s %s单触发API限速，第%s/%s次重试将在 %.2fs 后进行：%s",
                    exchange_name,
                    order_type_label,
                    next_attempt,
                    self._network_retry_max_attempts,
                    wait_seconds,
                    exception
                )
                await asyncio.sleep(wait_seconds)
                return True

            logger.error(
                "❌ [限流] %s %s单多次触发API限速，暂停自动重试：%s",
                exchange_name,
                order_type_label,
                exception,
                exc_info=True
            )
            return False

        is_network_issue = self._is_network_error(exception)

        if is_network_issue:
            notify_network_failure(exchange_name, str(exception))
            if next_attempt <= self._network_retry_max_attempts:
                wait_seconds = self._calc_network_retry_delay(next_attempt)
                logger.warning(
                    f"⚠️ [套利执行] {exchange_name} {order_type_label}单网络异常，"
                    f"第{next_attempt}/{self._network_retry_max_attempts}次重试，等待 {wait_seconds:.1f}s：{exception}"
                )
                await asyncio.sleep(wait_seconds)
                return True

            logger.error(
                f"❌ [套利执行] {exchange_name} {order_type_label}单因网络异常多次失败，暂停自动重试，等待人工检查。",
                exc_info=True
            )
            return False

        logger.error(
            f"❌ [套利执行] {exchange_name} {order_type_label}单被交易所拒绝或参数错误：{exception}。"
            "请人工检查。",
            exc_info=True
        )
        return False

    def _is_network_error(self, exc: Exception) -> bool:
        """识别常见网络类异常"""
        keywords = (
            "timeout",
            "timed out",
            "temporarily unavailable",
            "cannot connect",
            "connection reset",
            "network is unreachable",
            "ssl",
            "name or service not known",
            "dns",
            "deadline_exceeded",  # 🔥 gRPC 超时错误（EdgeX SDK）
            "deadline exceeded",
        )
        seen = set()
        current = exc
        while current and current not in seen:
            if isinstance(current, (asyncio.TimeoutError, TimeoutError, ConnectionError, OSError)):
                return True
            if aiohttp and isinstance(current, aiohttp.ClientError):
                return True
            message = str(current).lower()
            if any(keyword in message for keyword in keywords):
                return True
            seen.add(current)
            current = current.__cause__ or current.__context__
        return False

    def _calc_network_retry_delay(self, attempt: int) -> float:
        """按指数回退计算网络重试等待时间"""
        delay = self._network_retry_base_delay * (2 ** max(0, attempt - 1))
        return min(delay, self._network_retry_max_delay)

    # ------------------------------------------------------------------ #
    # Reduce-only 管理
    # ------------------------------------------------------------------ #

    async def probe_reduce_only_leg(
        self,
        exchange: str,
        symbol: str,
        quantity: Decimal,
        *,
        price: Decimal,
    ) -> bool:
        """在整点探测时发送最小量限价单，用于检测交易所是否解锁。"""
        return await self.reduce_only_handler.probe_reduce_only_leg(
            exchange=exchange,
            symbol=symbol,
            quantity=quantity,
            price=price,
        )

    async def _cancel_order(
        self,
        adapter: ExchangeInterface,
        order_id: str,
        symbol: str
    ) -> bool:
        """
        取消订单

        注意：对于EdgeX，如果order_id实际上是client_id（SDK下单的情况），
        EdgeX适配器内部会正确处理
        """
        exchange_name = self._get_exchange_name(adapter)

        async def _do_cancel() -> bool:
            try:
                result = await adapter.cancel_order(order_id, symbol)
                if getattr(adapter, 'strict_order_state', False) and (not result or getattr(result, 'status', None) not in (OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED, OrderStatus.EXPIRED)):
                    raise OrderStateUnknownError(exchange_name, order_id=order_id)
                if isinstance(result, OrderData):
                    pending = self._lookup_pending_order(order_id)
                    if pending is not None:
                        self.order_monitor._merge_order_state(pending, result)
                        self._remember_confirmed_fill(adapter, pending)
                    else:
                        self._remember_confirmed_fill(adapter, result)
                    for key in self._iter_order_keys(result) | {str(order_id)}:
                        self._order_fill_results[key] = result
                logger.info(f"✅ [套利执行] 取消订单成功: {order_id}")
                self._remove_pending_order(None, keys=[order_id])
                return True
            except OrderStateUnknownError:
                raise
            except Exception as e:
                if self._is_order_already_closed(adapter, order_id, symbol):
                    logger.info(
                        "ℹ️ [套利执行] 订单已通过WebSocket确认取消: %s (REST返回: %s)",
                        order_id,
                        e
                    )
                    self._remove_pending_order(None, keys=[order_id])
                    return True
                if self._is_order_missing_from_exchange(e):
                    logger.info(
                        "ℹ️ [套利执行] 交易所返回不存在的订单，视为已关闭: %s (REST返回: %s)",
                        order_id,
                        e
                    )
                    self._remove_pending_order(None, keys=[order_id])
                    return True
                logger.error(f"[套利执行] 取消订单失败: {e}", exc_info=True)
                return False

        if exchange_name and exchange_name.lower() == "lighter":
            async with self._lighter_order_lock:
                return await _do_cancel()
        return await _do_cancel()

    def _is_order_already_closed(
        self,
        adapter: ExchangeInterface,
        order_id: str,
        symbol: str
    ) -> bool:
        ws = getattr(adapter, 'websocket', None) or getattr(
            adapter, '_websocket', None)
        if ws and hasattr(ws, 'lookup_cached_order'):
            try:
                cached = ws.lookup_cached_order(order_id, symbol)
            except TypeError:
                cached = ws.lookup_cached_order(order_id)
            if cached and getattr(cached, 'status', None) in CLOSED_STATUSES:
                return True
        cached_ws = self._order_fill_results.get(order_id)
        if cached_ws and getattr(cached_ws, 'status', None) in CLOSED_STATUSES:
            return True
        return False

    @staticmethod
    def _is_order_missing_from_exchange(error: Exception) -> bool:
        """检查交易所返回是否为“订单不存在/已完成”之类的提示。"""
        message = str(error).lower()
        keywords = [
            "order not found",
            "order does not exist",
            "unknown order",
            "already closed",
            "order is closed",
            "order_is_closed",
            "already filled",
            "order already filled",
            "order already done",
            "order already cancelled",
            "order already canceled",
            "cannot find order",
            "failed_order_not_found",
            "cannot cancel closed order",
            "order status is done",
        ]
        return any(keyword in message for keyword in keywords)

    def _get_exchange_name(self, adapter: ExchangeInterface) -> str:
        """获取交易所名称"""
        for name, adpt in self.exchange_adapters.items():
            if adpt == adapter:
                return name
        return "unknown"

    @staticmethod
    def _format_exchange_tag(exchange_name: Optional[str]) -> str:
        """标准化日志中的交易所标签"""
        return (exchange_name or "unknown").upper()

    @staticmethod
    def _should_use_reduce_only(exchange_name: Optional[str], is_open: bool) -> bool:
        """
        lighter / Arcus 在平仓场景使用 reduce_only。
        """
        if not exchange_name:
            return False
        return exchange_name.lower() in ("lighter", "arcus") and not is_open

    @staticmethod
    def _iter_order_keys(order: OrderData) -> Set[str]:
        keys: Set[str] = set()
        if getattr(order, "id", None):
            keys.add(str(order.id))
        if getattr(order, "client_id", None):
            keys.add(str(order.client_id))
        return keys

    def _register_pending_order(self, order: Optional[OrderData]) -> None:
        """注册挂单到缓存，同时支持通过order_id和client_id查询"""
        if not order:
            return
        order_id = getattr(order, "id", None)
        client_id = getattr(order, "client_id", None)
        if (not client_id) and order_id is not None:
            # Lighter REST 会把 client_id 塞在 id 字段里，WebSocket 才返回真实 order_id
            client_id = str(order_id)
            order.client_id = client_id
        if order_id is not None:
            self.pending_orders[str(order_id)] = order
        if client_id:
            self._pending_orders_by_client[str(client_id)] = order
        for key in self._iter_order_keys(order):
            self._order_submit_ts.setdefault(key, time.time())

    def _lookup_pending_order(self, key: Optional[str]) -> Optional[OrderData]:
        """通过order_id或client_id查找挂单"""
        if not key:
            return None
        order = self.pending_orders.get(str(key))
        if order:
            return order
        return self._pending_orders_by_client.get(str(key))

    def _remove_pending_order(
        self,
        order: Optional[OrderData] = None,
        *,
        keys: Optional[Iterable[str]] = None,
    ) -> None:
        """从缓存中移除挂单并清理相关累计进度"""
        target_keys: Set[str] = set()
        if order:
            target_keys.update(self._iter_order_keys(order))
        if keys:
            target_keys.update({str(k) for k in keys if k})
        for key in list(target_keys):
            cached = self.pending_orders.pop(key, None)
            if not cached:
                cached = self._pending_orders_by_client.pop(key, None)
            else:
                client_id = getattr(cached, "client_id", None)
                if client_id:
                    self._pending_orders_by_client.pop(str(client_id), None)
            if cached:
                for alt_key in self._iter_order_keys(cached):
                    self.pending_orders.pop(alt_key, None)
                    self._pending_orders_by_client.pop(alt_key, None)
                    self._order_fill_progress.pop(alt_key, None)
            self._order_fill_progress.pop(key, None)

    def _clear_fill_progress(self, keys: Iterable[str]) -> None:
        """清除订单的累计成交进度记录"""
        for key in keys:
            if key:
                self._order_fill_progress.pop(str(key), None)

    def _register_order_exchange_mapping(self, order: Optional[OrderData], exchange_name: Optional[str]) -> None:
        """记录订单ID与交易所的映射关系，用于日志追踪"""
        if not order or not exchange_name:
            return
        for key in self._iter_order_keys(order):
            self._order_exchange_map[key] = exchange_name

    def _register_lighter_ws_orders(self, orders_payload: Any) -> None:
        """注册Lighter批量订单的ID映射"""
        if not orders_payload:
            return
        if not isinstance(orders_payload, (list, tuple)):
            orders = [orders_payload]
        else:
            orders = orders_payload
        for entry in orders:
            if not isinstance(entry, dict):
                continue
            candidates: List[str] = []
            for key in ("orderId", "order_id", "id", "clientId", "client_id", "clientOrderId", "client_order_id"):
                value = entry.get(key)
                if value is not None:
                    candidates.append(str(value))
            if not candidates:
                continue
            for cand in candidates:
                self._lighter_ws_order_map[cand] = "lighter"

    # ------------------------------------------------------------------ #
    # 交易所限流管理
    # ------------------------------------------------------------------ #

    def _build_rate_limit_config_map(self) -> Dict[str, ExchangeRateLimitConfig]:
        """构建交易所限流配置映射"""
        config_map: Dict[str, ExchangeRateLimitConfig] = {}
        raw_limits = getattr(self.config, "exchange_rate_limits", {}) or {}
        for key, value in raw_limits.items():
            normalized = str(key).lower()
            config_map[normalized] = self._ensure_rate_limit_config(value)
        return config_map

    def _initialize_rate_limiters(self) -> None:
        """初始化所有交易所的限流控制器"""
        for exchange_name in self.exchange_adapters.keys():
            limiter = self._create_rate_limiter_for_exchange(exchange_name)
            cfg = self._resolve_rate_limit_config(exchange_name)
            logger.info(
                f"⚙️ [限流初始化] {exchange_name}: "
                f"并发={cfg.max_concurrent_orders}, "
                f"间隔={cfg.min_interval_ms}ms, "
                f"冷却={cfg.rate_limit_cooldown_ms}ms"
            )

    def _ensure_rate_limit_config(
        self,
        value: Any
    ) -> ExchangeRateLimitConfig:
        """确保限流配置为正确的数据类型"""
        if isinstance(value, ExchangeRateLimitConfig):
            return value
        if isinstance(value, dict):
            try:
                return ExchangeRateLimitConfig(
                    max_concurrent_orders=int(
                        value.get('max_concurrent_orders', 1) or 1),
                    min_interval_ms=int(value.get('min_interval_ms', 0) or 0),
                    rate_limit_cooldown_ms=int(
                        value.get('rate_limit_cooldown_ms', 1000) or 0),
                )
            except Exception:
                return ExchangeRateLimitConfig()
        return ExchangeRateLimitConfig()

    def _resolve_rate_limit_config(
        self,
        exchange_name: Optional[str]
    ) -> ExchangeRateLimitConfig:
        """获取指定交易所的限流配置"""
        if not exchange_name:
            return self._default_rate_limit_config
        key = exchange_name.lower()
        cfg = self._rate_limit_config_map.get(key)
        if cfg:
            return cfg
        self._rate_limit_config_map[key] = self._default_rate_limit_config
        return self._default_rate_limit_config

    def _create_rate_limiter_for_exchange(
        self,
        exchange_name: str
    ) -> ExchangeRateLimiter:
        """为指定交易所创建限流控制器"""
        cfg = self._resolve_rate_limit_config(exchange_name)
        limiter = ExchangeRateLimiter(
            exchange_name=exchange_name,
            max_concurrent=cfg.max_concurrent_orders or 1,
            min_interval_ms=cfg.min_interval_ms or 0,
            cooldown_ms=cfg.rate_limit_cooldown_ms or 0,
        )
        self._rate_limiters[exchange_name.lower()] = limiter
        return limiter

    def _get_rate_limiter(
        self,
        exchange_name: Optional[str]
    ) -> Optional[ExchangeRateLimiter]:
        """获取指定交易所的限流控制器"""
        if not exchange_name:
            return None
        key = exchange_name.lower()
        limiter = self._rate_limiters.get(key)
        if limiter is None and exchange_name:
            limiter = self._create_rate_limiter_for_exchange(exchange_name)
        return limiter

    def _collect_request_exchanges(
        self,
        request: ExecutionRequest
    ) -> List[str]:
        """从执行请求中提取所有涉及的交易所"""
        exchanges: List[str] = []
        seen: Set[str] = set()
        for name in (request.exchange_buy, request.exchange_sell):
            if not name:
                continue
            key = name.lower()
            if key in seen:
                continue
            seen.add(key)
            exchanges.append(name)
        return exchanges

    def _reserve_exchange_slots(
        self,
        exchanges: List[str]
    ) -> Optional[RateLimitReservation]:
        """预约所有相关交易所的限流资源"""
        limiters: List[ExchangeRateLimiter] = []
        seen: Set[str] = set()
        for name in exchanges:
            limiter = self._get_rate_limiter(name)
            if not limiter:
                continue
            key = limiter.exchange.lower()
            if key in seen:
                continue
            seen.add(key)
            limiters.append(limiter)
        if not limiters:
            return None
        return RateLimitReservation(limiters)

    def _apply_rate_limit_cooldown(
        self,
        exchange_name: Optional[str],
        wait_seconds: Optional[float]
    ) -> None:
        """对指定交易所应用限流冷却时间"""
        limiter = self._get_rate_limiter(exchange_name)
        if limiter:
            limiter.register_cooldown(wait_seconds)

    def _get_rate_limit_default_cooldown(
        self,
        exchange_name: Optional[str]
    ) -> float:
        """获取交易所的默认限流冷却时间"""
        limiter = self._get_rate_limiter(exchange_name)
        if limiter:
            default_value = limiter.default_cooldown
            return default_value if default_value > 0 else 1.0
        return 1.0

    def _is_rate_limit_error(
        self,
        exc: Exception
    ) -> Tuple[bool, Optional[float]]:
        """判断异常是否为限流错误，并提取建议等待时间"""
        message = self._flatten_exception_message(exc).lower()
        keywords = (
            "rate limit",
            "ratelimit",
            "too many requests",
            "429",
            "max request",
            "retryafter",
            "retry_after",
            "retry after",
            "burst limit",
            "ops per",
            "per second",
        )
        if any(keyword in message for keyword in keywords):
            return True, self._parse_retry_hint_seconds(message)
        return False, None

    def _flatten_exception_message(self, exc: Exception) -> str:
        """将异常链展开为单行消息"""
        parts: List[str] = []
        seen_ids: Set[int] = set()
        current: Optional[Exception] = exc
        while current and id(current) not in seen_ids:
            parts.append(str(current))
            seen_ids.add(id(current))
            current = current.__cause__ or current.__context__
        return " | ".join(parts)

    def _parse_retry_hint_seconds(self, message: str) -> Optional[float]:
        """从错误消息中提取建议重试等待秒数"""
        retry_match = re.search(
            r"retry[_ ]?after(?:seconds)?['\":=\s]+([0-9]+(?:\.[0-9]+)?)",
            message
        )
        if retry_match:
            try:
                return float(retry_match.group(1))
            except ValueError:
                pass
        window_match = re.search(
            r"([0-9]+)\s*ops?\s*per\s*([0-9]+)\s*second",
            message
        )
        if window_match:
            try:
                ops = int(window_match.group(1))
                seconds = int(window_match.group(2))
                if ops > 0:
                    return seconds / ops
            except ValueError:
                return None
        return None

    def _cleanup_order_exchange_mapping(self, order: Optional[OrderData], exclude_key: Optional[str] = None) -> None:
        """清理订单与交易所的映射关系"""
        if not order:
            return
        for key in self._iter_order_keys(order):
            if exclude_key and key == exclude_key:
                continue
            self._order_exchange_map.pop(key, None)

    def _resolve_exchange_from_order(self, order: Optional[OrderData]) -> Optional[str]:
        """从订单对象反查所属交易所名称"""
        if not order:
            return None
        for key in self._iter_order_keys(order):
            exchange = self._order_exchange_map.get(key)
            if exchange:
                return exchange
        for key in self._iter_order_keys(order):
            exchange = self._lighter_ws_order_map.get(key)
            if exchange:
                return exchange
        return getattr(order, "exchange", None)

    async def _await_round_pause_window(self):
        """在执行前确保满足轮次间隔（不阻塞日志输出）"""
        if self.monitor_only or self._round_pause_seconds <= 0:
            return

        remaining = self._next_round_time - time.time()
        if remaining <= 0:
            return

        logger.info(
            f"⏸️ [套利执行] 等待 {remaining:.1f} 秒以满足轮次间隔 "
            f"(配置: {self._round_pause_seconds} 秒)"
        )
        try:
            await asyncio.sleep(remaining)
        except asyncio.CancelledError:
            logger.warning("⚠️ [套利执行] 轮次间隔等待被取消")

    def _schedule_next_round_pause(self):
        """当前轮次完成后，设定下一轮允许执行的时间点"""
        if self.monitor_only or self._round_pause_seconds <= 0:
            return

        self._next_round_time = time.time() + self._round_pause_seconds

    @staticmethod
    def _to_decimal_value(value: Any) -> Decimal:
        """安全地将任意类型的数值转换为Decimal"""
        if isinstance(value, Decimal):
            return value
        try:
            return Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError):
            return Decimal('0')

    @staticmethod
    def _infer_price_step(value: Any) -> Decimal:
        """
        根据盘口价格推导最小步进（去除末尾0后的小数位数）
        """
        price_decimal = ArbitrageExecutor._to_decimal_value(value)
        price_str = format(price_decimal, 'f')
        if '.' in price_str:
            fractional = price_str.rstrip('0').split('.')[1]
            if fractional:
                return Decimal('1').scaleb(-len(fractional))
        # 没有小数位时按个位步进（例如 93983 -> step=1，符合“取最后一位数”设计）
        return Decimal('1')

    # ========================================================================
    # 风险控制：紧急平仓
    # ========================================================================

    async def _emergency_close_position(
        self,
        exchange: str,
        adapter: ExchangeInterface,
        symbol: str,
        quantity: Decimal,
        is_buy_to_close: bool,
        request: Optional[ExecutionRequest] = None,
    ) -> bool:
        """
        紧急平仓（用于单边持仓风险控制）

        当一个交易所下单失败，另一个成功时，需要立即平掉成功的订单

        Args:
            exchange: 交易所名称
            adapter: 交易所适配器
            symbol: 交易对
            quantity: 平仓数量
            is_buy_to_close: True=买入平仓(平掉空头), False=卖出平仓(平掉多头)
            request: 原始执行请求，用于回传上下文

        Returns:
            bool: True=平仓成功, False=平仓失败
        """
        quantity_epsilon = Decimal("0.00000001")

        if request and any(getattr(self.exchange_adapters.get(name), 'strict_order_state', False)
                           for name in (request.exchange_buy, request.exchange_sell)):
            # Confirmed exposure after a definitive rejection: one close submission, no replay.
            try:
                params = {'reduce_only': self._should_use_reduce_only(exchange, is_open=False)}
                if exchange.lower() == 'arcus':
                    slippage = self._get_slippage_percent(request)
                    if slippage is not None:
                        params['slippage_tolerance_pct'] = slippage
                close_order = await adapter.create_order(
                    symbol=symbol, side=OrderSide.BUY if is_buy_to_close else OrderSide.SELL,
                    order_type=OrderType.MARKET, amount=quantity, params=params,
                )
                if not close_order:
                    raise OrderStateUnknownError(exchange)
                if close_order.status in CLOSED_STATUSES:
                    filled = self._to_decimal_value(close_order.filled)
                else:
                    filled = await self.order_monitor.wait_for_order_fill(
                        close_order, adapter,
                        timeout=getattr(self.config.order_execution, 'limit_order_timeout', 60) or 60,
                        is_market_order=True,
                    )
                if filled is None:
                    raise OrderStateUnknownError(exchange, order_id=close_order.id)
                if filled > quantity_epsilon:
                    self._record_emergency_close_event(exchange=exchange, symbol=symbol,
                        quantity=filled, is_buy_to_close=is_buy_to_close, request=request, status='filled')
                if filled < quantity - quantity_epsilon:
                    raise OrderStateUnknownError(exchange, order_id=close_order.id)
                return True
            except OrderStateUnknownError:
                raise
            except Exception as exc:
                # An exception after submission cannot establish whether the close was accepted.
                raise OrderStateUnknownError(exchange) from exc

        # 其他交易所保持原有逻辑（重试3次）
        limit_timeout = getattr(
            self.config.order_execution,
            "limit_order_timeout",
            60,
        ) or 60
        max_attempts = getattr(self.config.order_execution,
                               "emergency_close_retry", 3) or 3
        remaining_qty = quantity

        for attempt in range(1, max_attempts + 1):
            if remaining_qty <= quantity_epsilon:
                logger.info(f"✅ [紧急平仓] {exchange} 剩余仓位已清零，停止重试")
                return True

            try:
                logger.warning(
                    f"🚨 [紧急平仓] 开始平仓 {exchange} (尝试 {attempt}/{max_attempts}): "
                    f"{'买入' if is_buy_to_close else '卖出'} {remaining_qty} {symbol}"
                )

                close_order = await self._place_market_order(
                    adapter=adapter,
                    symbol=symbol,
                    quantity=remaining_qty,
                    is_buy=is_buy_to_close,
                    reduce_only=self._should_use_reduce_only(
                        exchange, is_open=False),
                    request=None
                )

                if not close_order:
                    logger.error(
                        f"❌ [紧急平仓] {exchange} 平仓订单提交失败（尝试 {attempt}/{max_attempts}）"
                    )
                    continue

                logger.info(
                    f"✅ [紧急平仓] {exchange} 平仓订单已提交: 订单ID={close_order.id}"
                )

                filled = await self.order_monitor.wait_for_order_fill(
                    close_order,
                    adapter,
                    timeout=limit_timeout,
                    is_market_order=True,
                )
                filled_amount = filled or Decimal("0")

                if filled_amount > quantity_epsilon:
                    remaining_qty = max(
                        Decimal("0"), remaining_qty - filled_amount)
                    self._record_emergency_close_event(
                        exchange=exchange,
                        symbol=symbol,
                        quantity=Decimal(str(filled_amount)),
                        is_buy_to_close=is_buy_to_close,
                        request=request,
                        status="filled",
                    )
                    logger.info(
                        f"✅ [紧急平仓] {exchange} 本次成交 {filled_amount} {symbol} "
                        f"(剩余 {remaining_qty})"
                    )
                    if remaining_qty <= quantity_epsilon:
                        logger.info(f"✅ [紧急平仓] {exchange} 平仓完成")
                        self._record_emergency_close_event(
                            exchange=exchange,
                            symbol=symbol,
                            quantity=Decimal("0"),
                            is_buy_to_close=is_buy_to_close,
                            request=request,
                            status="completed",
                        )
                        return True
                    logger.warning(
                        f"⚠️ [紧急平仓] {exchange} 仍有 {remaining_qty} 未成交，将继续重试"
                    )
                else:
                    logger.warning(
                        f"⚠️ [紧急平仓] {exchange} 本次未成交，订单ID={close_order.id}"
                    )

            except OrderStateUnknownError:
                raise
            except Exception as e:
                logger.error(
                    f"❌ [紧急平仓] {exchange} 平仓异常（尝试 {attempt}/{max_attempts}）: {e}",
                    exc_info=True
                )
                await asyncio.sleep(min(2, attempt))

        logger.error(
            f"❌ [紧急平仓] {exchange} 连续 {max_attempts} 次尝试后仍未完全平仓 "
            f"请立即人工干预，手动平仓 {'多头' if not is_buy_to_close else '空头'} {remaining_qty} {symbol}"
        )
        self._record_emergency_close_event(
            exchange=exchange,
            symbol=symbol,
            quantity=remaining_qty,
            is_buy_to_close=is_buy_to_close,
            request=request,
            status="failed",
        )
        return False

    # ========================================================================
    # 分段套利模式执行方法
    # ========================================================================

    async def execute_segmented_open(
        self,
        symbol: str,
        segment_id: int,
        spread_data: 'SpreadData',
        quantity: Decimal
    ) -> ExecutionResult:
        """
        执行分段开仓

        Args:
            symbol: 交易对
            segment_id: 段序号
            spread_data: 价差数据
            quantity: 开仓数量

        Returns:
            执行结果
        """
        # 创建执行请求
        request = ExecutionRequest(
            symbol=symbol,
            exchange_buy=spread_data.exchange_buy,
            exchange_sell=spread_data.exchange_sell,
            price_buy=spread_data.price_buy,
            price_sell=spread_data.price_sell,
            quantity=quantity,
            is_open=True,
            spread_data=spread_data,
            buy_symbol=spread_data.buy_symbol or symbol,
            sell_symbol=spread_data.sell_symbol or symbol
        )

        logger.info(
            f"[分段执行] 开始执行第{segment_id}段开仓:\n"
            f"   - 交易对: {symbol}\n"
            f"   - 买入: {spread_data.exchange_buy} @ {spread_data.price_buy}\n"
            f"   - 卖出: {spread_data.exchange_sell} @ {spread_data.price_sell}\n"
            f"   - 数量: {quantity}\n"
            f"   - 价差: {spread_data.spread_pct:.3f}%"
        )

        # 复用现有的执行逻辑
        result = await self.execute_arbitrage(request)

        if result.success:
            logger.info(
                f"✅ [分段执行] 第{segment_id}段开仓成功"
            )
        else:
            logger.error(
                f"❌ [分段执行] 第{segment_id}段开仓失败: {result.error_message}"
            )

        return result

    async def execute_segmented_close(
        self,
        symbol: str,
        segment_ids: List[int],
        spread_data: 'SpreadData',
        total_quantity: Decimal
    ) -> ExecutionResult:
        """
        执行分段平仓

        Args:
            symbol: 交易对
            segment_ids: 平仓的段序号列表
            spread_data: 价差数据
            total_quantity: 总平仓数量

        Returns:
            执行结果
        """
        # 🔥 创建平仓执行请求（交易所方向与开仓相反）
        # 开仓：exchange_buy买入，exchange_sell卖出
        # 平仓：exchange_sell买入，exchange_buy卖出（反向）
        request = ExecutionRequest(
            symbol=symbol,
            exchange_buy=spread_data.exchange_sell,  # 🔥 反向：原来卖出的交易所现在买入
            exchange_sell=spread_data.exchange_buy,  # 🔥 反向：原来买入的交易所现在卖出
            price_buy=spread_data.price_sell,         # 🔥 反向：使用原卖出价作为买入价
            price_sell=spread_data.price_buy,         # 🔥 反向：使用原买入价作为卖出价
            quantity=total_quantity,
            is_open=False,
            spread_data=spread_data,
            buy_symbol=spread_data.sell_symbol or symbol,
            sell_symbol=spread_data.buy_symbol or symbol
        )

        logger.info(
            f"[分段执行] 开始执行分段平仓:\n"
            f"   - 交易对: {symbol}\n"
            f"   - 平仓段: {segment_ids}\n"
            f"   - 买入: {spread_data.exchange_sell} @ {spread_data.price_sell}\n"
            f"   - 卖出: {spread_data.exchange_buy} @ {spread_data.price_buy}\n"
            f"   - 总数量: {total_quantity}\n"
            f"   - 价差: {spread_data.spread_pct:.3f}%"
        )

        # 复用现有的执行逻辑
        result = await self.execute_arbitrage(request)

        if result.success:
            logger.info(
                f"✅ [分段执行] 分段平仓成功，已平仓段: {segment_ids}"
            )
        else:
            logger.error(
                f"❌ [分段执行] 分段平仓失败: {result.error_message}"
            )

        return result

    def _log_execution_summary(
        self,
        request: ExecutionRequest,
        order_buy: Optional[OrderData],
        order_sell: Optional[OrderData],
        is_open: bool,
        is_last_split: bool = False
    ):
        """
        打印实际成交统计（开仓/平仓）

        Args:
            request: 执行请求
            order_buy: 买入订单
            order_sell: 卖出订单
            is_open: 是否开仓
            is_last_split: 是否为最后一笔拆单（用于打印平均统计）
        """
        if not order_buy or not order_sell:
            return

        # 获取实际成交价格（优先 average，其次 cost/filled，再次 trades）
        actual_price_buy = self._resolve_actual_price(order_buy)
        actual_price_sell = self._resolve_actual_price(order_sell)

        if not actual_price_buy or not actual_price_sell:
            logger.warning(f"⚠️  无法获取实际成交价格，跳过统计日志")
            return

        def _order_type_label(order_obj: OrderData) -> str:
            order_type = getattr(order_obj, 'type', None)
            if isinstance(order_type, OrderType):
                return order_type.value.lower()
            if isinstance(order_type, str):
                return order_type.lower()
            return ""

        buy_type_label = _order_type_label(order_buy)
        sell_type_label = _order_type_label(order_sell)
        buy_exec_note = " 【市价补单】" if buy_type_label == "market" else ""
        sell_exec_note = " 【市价补单】" if sell_type_label == "market" else ""

        # 🔥 计算实际价差（根据持仓方向）
        # 平仓时：request.exchange_buy/sell 已经是反向的（基于持仓方向）
        # 因此这里的计算始终是从持仓视角看的价差
        actual_spread = float(actual_price_sell - actual_price_buy)
        actual_spread_pct = (actual_spread / float(actual_price_buy)) * 100

        # 计算理论价差（从请求中获取）
        theory_spread = float(request.price_sell - request.price_buy)
        theory_spread_pct = (theory_spread / float(request.price_buy)) * 100
        theory_price_buy = float(
            request.price_buy) if request.price_buy else 0.0
        theory_price_sell = float(
            request.price_sell) if request.price_sell else 0.0

        # 计算成交数量
        filled_quantity = float(order_buy.filled) if order_buy.filled else 0

        # 计算盈亏
        profit_usdt = actual_spread * filled_quantity

        # 🔥 记录到拆单缓存
        trade_data = {
            'buy_price': actual_price_buy,
            'sell_price': actual_price_sell,
            'quantity': filled_quantity,
            'spread_pct': actual_spread_pct,
            'spread_usd': actual_spread,
            'profit_usdt': profit_usdt,
            'symbol': request.buy_symbol or request.sell_symbol or request.symbol
        }

        symbol = request.symbol
        direction_key = 'open' if is_open else 'close'

        if symbol not in self._split_order_cache:
            self._split_order_cache[symbol] = {'open': [], 'close': []}

        self._split_order_cache[symbol][direction_key].append(trade_data)

        # 🔥 计算滑点（实际-理论）
        slippage_pct = actual_spread_pct - theory_spread_pct

        # 🔥 动态滑点阈值：
        # - 至少 0.01%（万分之1），避免因为微小报价抖动触发警告
        # - 同时随着理论价差增大而提高容忍度（25% 的相对偏差）
        relative_component = abs(theory_spread_pct) * 0.25
        slippage_threshold = max(0.01, relative_component)

        # 🔥 判断是否需要滑点警告
        is_high_slippage = abs(slippage_pct) > slippage_threshold

        # 🔥 打印详细统计
        direction = "📈 开仓" if is_open else "📉 平仓"

        # 构建滑点显示（带警告标识）
        if is_high_slippage:
            slippage_line = f"   滑点:     {slippage_pct:.4f}%  ⚠️  【高滑点警告】\n"
        else:
            slippage_line = f"   滑点:     {slippage_pct:.4f}%\n"

        symbol_label = trade_data['symbol']

        lines = [
            "",
            f"{'='*80}",
            f"{direction} 成交统计 - {request.symbol}",
            f"{'='*80}",
            "🎯 实际成交:",
            f"   买入方: {request.exchange_buy} @ {actual_price_buy:.4f} USDC{buy_exec_note}",
            f"   卖出方: {request.exchange_sell} @ {actual_price_sell:.4f} USDC{sell_exec_note}",
            f"   成交量: {filled_quantity:.6f} {symbol_label}",
            f"   价差:   {actual_spread_pct:.4f}% (${actual_spread:.2f})",
            "",
            "📊 理论vs实际:",
            f"   理论价差: {theory_spread_pct:.4f}% (${theory_spread:.2f})",
            f"   理论买入: {request.exchange_buy} @ {theory_price_buy:.4f} USDC",
            f"   理论卖出: {request.exchange_sell} @ {theory_price_sell:.4f} USDC",
            f"   实际价差: {actual_spread_pct:.4f}% (${actual_spread:.2f})",
            slippage_line.rstrip("\n"),
            ""
        ]
        if not is_open:
            lines.extend([
                "💰 盈亏预估:",
                f"   平仓盈亏: ${profit_usdt:.2f} USDT",
                f"   ROI实际: {actual_spread_pct:.4f}%",
                ""
            ])
        lines.extend([
            "📋 订单详情:",
            f"   买入订单ID: {order_buy.id}",
            f"   卖出订单ID: {order_sell.id}",
            f"{'='*80}"
        ])
        logger.info("\n".join(lines))

        # 将成交概要写入内存队列，供 UI 直接使用（避免读日志）
        summary = {
            "execution_time": datetime.now(),
            "symbol": request.symbol,
            "is_open": is_open,
            "exchange_buy": request.exchange_buy,
            "exchange_sell": request.exchange_sell,
            "quantity": float(filled_quantity),
            "price_buy": float(actual_price_buy),
            "price_sell": float(actual_price_sell),
            "spread_pct": float(theory_spread_pct),
            "actual_spread_pct": float(actual_spread_pct),
            "success": True,
            "error_message": "",
        }
        _push_execution_summary(summary)

        # 🔥 如果是最后一笔拆单，打印平均统计
        if is_last_split and len(self._split_order_cache[symbol][direction_key]) > 1:
            self._log_split_average_summary(symbol, direction_key, is_open)

    def _log_split_average_summary(
        self,
        symbol: str,
        direction_key: str,
        is_open: bool
    ):
        """
        打印拆单平均统计（最后一笔拆单时调用）

        Args:
            symbol: 交易对
            direction_key: 'open' 或 'close'
            is_open: 是否开仓
        """
        trades = self._split_order_cache[symbol][direction_key]

        if not trades:
            return

        # 计算总量加权平均价格（统一转换为Decimal类型）
        total_quantity = Decimal(
            str(sum(Decimal(str(t['quantity'])) for t in trades)))

        if total_quantity == 0:
            return

        # 加权平均买入价
        avg_buy_price = sum(
            Decimal(str(t['buy_price'])) * Decimal(str(t['quantity']))
            for t in trades
        ) / total_quantity

        # 加权平均卖出价
        avg_sell_price = sum(
            Decimal(str(t['sell_price'])) * Decimal(str(t['quantity']))
            for t in trades
        ) / total_quantity

        # 平均价差
        avg_spread = avg_sell_price - avg_buy_price
        avg_spread_pct = (avg_spread / avg_buy_price) * Decimal('100')

        # 总盈亏
        total_profit = sum(Decimal(str(t['profit_usdt'])) for t in trades)

        # 打印小总结
        direction = "开仓" if is_open else "平仓"

        symbol_label = trades[0].get('symbol', symbol) if trades else symbol

        logger.info(
            f"\n"
            f"{'─'*80}\n"
            f"📊 本轮拆单平均统计 - {symbol} ({direction})\n"
            f"{'─'*80}\n"
            f"📦 拆单明细:\n"
            f"   拆单笔数: {len(trades)} 笔\n"
            f"   总成交量: {total_quantity:.6f} {symbol_label}\n"
            f"\n"
            f"📈 平均价差:\n"
            f"   平均买入价: {avg_buy_price:.2f} USDC\n"
            f"   平均卖出价: {avg_sell_price:.2f} USDC\n"
            f"   平均价差:   {avg_spread_pct:.4f}% (${avg_spread:.2f})\n"
            f"\n"
            f"💰 {'累计盈亏' if is_open else '实际盈亏'}:\n"
            f"   总盈亏: ${total_profit:.2f} USDT\n"
            f"   平均ROI: {avg_spread_pct:.4f}%\n"
            f"{'─'*80}\n"
        )

        # 🔥 清空缓存，准备下一轮
        self._split_order_cache[symbol][direction_key] = []

    def _resolve_actual_price(self, order: Optional[OrderData]) -> Optional[Decimal]:
        """尝试获取订单的真实成交均价。"""
        if not order:
            return None

        epsilon = Decimal("0.00000001")
        avg_price = getattr(order, "average", None)
        if avg_price:
            price_dec = self._to_decimal_value(avg_price)
            if price_dec > epsilon:
                return price_dec

        cost = getattr(order, "cost", None)
        filled = getattr(order, "filled", None)
        cost_dec = self._to_decimal_value(cost)
        filled_dec = self._to_decimal_value(filled)
        if filled_dec > epsilon and cost_dec > epsilon:
            return cost_dec / filled_dec

        trades = getattr(order, "trades", None) or []
        total_qty = Decimal("0")
        total_cost = Decimal("0")
        for trade in trades:
            if not isinstance(trade, dict):
                continue
            qty_candidate = (
                trade.get("quantity")
                or trade.get("amount")
                or trade.get("filled")
                or trade.get("size")
            )
            price_candidate = trade.get("price")
            if qty_candidate is None or price_candidate is None:
                continue
            qty_dec = self._to_decimal_value(qty_candidate)
            price_dec = self._to_decimal_value(price_candidate)
            if qty_dec <= epsilon or price_dec <= epsilon:
                continue
            total_qty += qty_dec
            total_cost += qty_dec * price_dec
        if total_qty > epsilon:
            return total_cost / total_qty

        if getattr(order, "price", None):
            return self._to_decimal_value(order.price)

        return None

    def set_global_pause_checker(
        self,
        checker: Optional[Callable[[], Tuple[bool, Optional[str]]]],
    ) -> None:
        """
        注入“全局暂停闸门”：
        - 用于暂停触发后，阻断新的开/平仓下单
        - 执行器不直接依赖 GlobalRiskController，避免耦合
        """
        self._global_pause_checker = checker

    def set_exchange_trade_blocker(
        self,
        checker: Optional[
            Callable[[str, "ExecutionRequest"], Tuple[bool, Optional[str], Optional[str]]]
        ],
    ) -> None:
        """
        注入“按交易所的下单阻断闸门”：
        - 典型用途：只对某一家交易所做局部熔断/降级（例如 paradex）
        - 执行器保持与具体风控模块解耦
        """
        self._exchange_trade_blocker = checker

    def set_exchange_order_error_reporter(
        self,
        reporter: Optional[Callable[[str, str, Exception, int], Any]],
    ) -> None:
        """
        注入“下单异常上报回调”：
        - 执行器内部依旧使用原有重试/限流/网络处理
        - 额外把异常事件抛给外部模块做局部风控记账（例如 paradex）
        """
        self._exchange_order_error_reporter = reporter

    def set_order_ws_latency_reporter(self, reporter: Optional[Callable[..., Any]]) -> None:
        """
        注入“下单提交 -> WS成交推送”耗时上报回调。
        设计：执行器不直接依赖风控实现，只调用回调（避免耦合）。
        """
        self._order_ws_latency_reporter = reporter

    def _get_data_freshness_seconds(self, exchange: Optional[str]) -> float:
        """
        获取“数据新鲜度阈值(秒)”（执行器侧）：
        - 优先读取 orchestrator 同步进来的 data_freshness_seconds_by_exchange
        - 否则回退到 data_freshness_seconds
        """
        try:
            ex = str(exchange or "").strip().lower()
            by_ex = getattr(self, "data_freshness_seconds_by_exchange", None) or {}
            if ex and isinstance(by_ex, dict):
                v = by_ex.get(ex)
                if v is not None:
                    return float(v)
        except Exception:
            pass
        try:
            return float(getattr(self, "data_freshness_seconds", 2.0))
        except Exception:
            return 2.0

    def _raise_if_global_paused_before_submit(
        self,
        *,
        request: Optional[ExecutionRequest],
        stage: str,
    ) -> None:
        """
        “最后闸门”：在真正发单前检查全局暂停。

        规则（方案1）：
        - 暂停触发后：后续还没开始下单的套利应立刻被拦截。
        - 已经开始下单（至少一次发单尝试）的那一笔：允许继续完成（包含补救/紧急平仓）。
        """
        if request is None:
            return

        # 强平专用：允许“平仓”在全局暂停中继续执行（开仓一律禁止）
        if (not bool(getattr(request, "is_open", True))) and bool(getattr(request, "allow_when_global_paused", False)):
            return

        # 已经开始下单的请求：允许继续跑完
        if bool(getattr(request, "_has_submitted_order", False)):
            return

        try:
            if callable(self._global_pause_checker):
                blocked, reason = self._global_pause_checker()
                if bool(blocked):
                    # ✅ 余额相关暂停：允许“平仓”继续发单（开仓仍一律禁止）
                    # 说明：余额风控场景下，暂停的目的是禁止继续加仓，但平仓应尽量放行以降低风险。
                    if not bool(getattr(request, "is_open", True)):
                        try:
                            r = str(reason or "")
                            if ("余额" in r) or ("balance" in r.lower()):
                                return
                        except Exception:
                            # 保守：解析失败时不额外放行
                            pass
                    raise GlobalPausedError(reason or "全局暂停中", stage=stage)
        except GlobalPausedError:
            raise
        except Exception:
            raise GlobalPausedError("风险闸门不可用", stage=stage)

    def _validate_pair_quantity(self, request):
        constraints = getattr(self, "market_constraints", {}).get((request.buy_symbol or request.symbol).split("-")[0])
        if not constraints:
            return
        quantity = request.quantity
        step = Decimal(str(constraints["quantity_step"]))
        minimum = Decimal(str(constraints["min_quantity"]))
        if not quantity.is_finite() or quantity < minimum or quantity % step:
            raise ValueError("双腿数量不符合共同精度或最小量")
        for exchange, price in ((request.exchange_buy, request.price_buy), (request.exchange_sell, request.price_sell)):
            if not price.is_finite() or price <= 0 or price * quantity < Decimal(str(constraints["min_notional_by_exchange"].get(exchange, 0))):
                raise ValueError("双腿订单金额低于交易所最小金额")

    def _remember_confirmed_fill(self, adapter, order):
        from dataclasses import replace
        request = self._execution_request_ctx.get()
        if not request or not order or order.status not in (OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED, OrderStatus.EXPIRED):
            return
        exchange = self._get_exchange_name(adapter)
        side = str(getattr(order.side, "value", order.side)).lower()
        is_buy = side == "buy"
        expected = request.exchange_buy if is_buy else request.exchange_sell
        if exchange != expected:
            return
        key = (exchange, is_buy, str(order.client_id or order.id))
        old = request._confirmed_fills.get(key)
        if old is None or Decimal(str(order.filled or 0)) >= Decimal(str(old.filled or 0)):
            request._confirmed_fills[key] = replace(order)


    def _apply_confirmed_fills(self, request, result):
        if not self.is_segmented_mode:
            return
        from dataclasses import replace
        pair_key = (request.symbol, tuple(sorted((request.exchange_buy, request.exchange_sell))))
        valuation_missing = False
        for exchange, is_buy, attribute in ((request.exchange_buy, True, "order_buy"), (request.exchange_sell, False, "order_sell")):
            rows = [row for (ex, side, _), row in request._confirmed_fills.items() if ex == exchange and side == is_buy and row.filled > 0]
            original = getattr(result, attribute)
            if not rows and original and original.status in (OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED, OrderStatus.EXPIRED) and original.filled > 0:
                rows = [original]
            if not rows:
                continue
            qty = sum((row.filled for row in rows), Decimal("0"))
            costs = [row.cost if row.cost and row.cost.is_finite() and row.cost > 0 else (row.average * row.filled if row.average and row.average.is_finite() and row.average > 0 else None) for row in rows]
            if any(cost is None for cost in costs):
                valuation_missing = True
                setattr(result, attribute, replace(rows[-1], filled=qty, cost=Decimal("0"), average=Decimal("0")))
                continue
            cost = sum(costs, Decimal("0"))
            fee_cost = Decimal("0")
            config = self.config.exchange_fee_config.get(exchange, self.config.exchange_fee_config.get("default"))
            for row, row_cost in zip(rows, costs):
                fee = row.fee
                if fee is None or (isinstance(fee, dict) and fee.get("cost") is None):
                    rate = getattr(config, "limit_fee_rate" if row.type == OrderType.LIMIT else "market_fee_rate", 0)
                    fee_cost += row_cost * Decimal(str(rate))
                elif isinstance(fee, dict) and fee.get("cost") is not None:
                    known_fee = Decimal(str(fee["cost"]))
                    currency = str(fee.get("currency", "")).upper()
                    if not known_fee.is_finite() or (known_fee != 0 and currency not in {"USDC", "USD"}):
                        valuation_missing = True
                    else:
                        fee_cost += known_fee
                else:
                    valuation_missing = True
            if len(rows) == 1 and original is not None:
                original.filled, original.cost, original.average = qty, cost, cost / qty
                original.remaining = Decimal("0")
                original.fee = {"currency":"USDC", "cost":fee_cost}
            else:
                setattr(result, attribute, replace(rows[-1], filled=qty, cost=cost, average=cost / qty, remaining=Decimal("0"), fee={"currency":"USDC", "cost":fee_cost}))
        buy = Decimal(str(getattr(result.order_buy, "filled", 0) or 0))
        sell = Decimal(str(getattr(result.order_sell, "filled", 0) or 0))
        if valuation_missing:
            self._unknown_execution_pairs.add(pair_key)
            result.success = False
            result.failure_code = "EXECUTION_COST_UNKNOWN"
            result.error_message = "已成交订单成本或费用币种缺失，保留实仓且需人工核对"
            result.success_quantity = buy if buy == sell and buy <= request.quantity else Decimal("0")
        elif result.success:
            if buy != sell or buy <= 0 or buy > request.quantity:
                self._unknown_execution_pairs.add(pair_key)
                result.success = False
                result.failure_code = "ORDER_STATE_UNKNOWN"
                result.error_message = "双腿累计成交不一致或超过计划数量，需人工核对"
            else:
                result.success_quantity = buy
