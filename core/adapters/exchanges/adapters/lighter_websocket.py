"""
Lighter交易所适配器 - WebSocket模块

使用直接WebSocket连接提供实时数据流（订单、持仓、余额、市场数据）

========================
维护说明（请务必阅读）
========================

本文件历史迭代较多，容易出现“同一业务多套实现并存”的情况。为避免未来再次分叉，
请遵守以下约定：

- **当前主流程（订单推送解析）**：
  - `account_all_orders` / `account_all` 的 `orders` 推送：使用 **`_parse_order_any()`** 作为统一入口；
  - 其中“完整 JSON”订单结构（包含 `market_index/order_index/client_order_index`）由
    **`_parse_order_from_direct_ws()`** 负责解析（这是主力解析器）。

- **历史兼容（不建议新代码使用）**：
  - `_parse_orders()` / `_parse_order_from_ws()` / `_parse_order()` 仅用于旧路径/文档兼容；
  - 新功能/新逻辑请不要直接调用上述三个方法，统一走 `_parse_order_any()`。

⚠️ **关键注意事项**：
1. **应用层心跳**: Lighter 使用 JSON 消息心跳（`{"type": "ping"}` / `{"type": "pong"}`），
   必须在 `_handle_direct_ws_message` 中拦截并回复，否则120秒后断开连接
2. **价格订阅**: 使用 `market_stats` 频道获取实时价格（13次/秒），不要用 `orderbook`
3. **WebSocket参数**: 设置 `ping_interval=None` 禁用客户端ping，避免与应用层心跳冲突
4. **统一WebSocket**: 完全使用直接WebSocket订阅，不再使用SDK的WsClient
   - 订单数据：`account_all_orders` 频道
   - 持仓数据：`account_all` + `account_all_positions` 频道
   - 余额数据：`user_stats` 频道
   - 市场数据：`market_stats` 和 `order_book` 频道 1 
"""

from typing import Dict, Any, Optional, List, Callable, Set
from decimal import Decimal
from datetime import datetime
from collections import deque
import asyncio
import logging
import math
import json
import time
import uuid
import ssl
import os

from ..utils.logger_factory import get_exchange_logger

logger = get_exchange_logger("ExchangeAdapter.lighter")

try:
    import lighter
    LIGHTER_AVAILABLE = True
except ImportError:
    LIGHTER_AVAILABLE = False
    logger.warning("lighter SDK未安装（仅用于签名，WebSocket使用直接连接）")

try:
    import websockets
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False
    logger.warning("websockets库未安装，无法使用直接订阅功能")

from .lighter_base import LighterBase
from ..models import (
    TickerData, OrderBookData, TradeData, OrderData, PositionData,
    OrderBookLevel, OrderStatus, OrderSide, OrderType
)


class LighterWebSocket(LighterBase):
    """Lighter WebSocket客户端"""

    def __init__(self, config: Dict[str, Any]):
        """
        初始化Lighter WebSocket客户端

        Args:
            config: 配置字典
        """
        if not LIGHTER_AVAILABLE:
            raise ImportError("lighter SDK未安装，无法使用Lighter WebSocket")

        super().__init__(config)

        # 🔥 直接WebSocket连接（统一使用直接WebSocket，不再使用SDK的WsClient）
        self._direct_ws = None
        self._reader_connected = False
        self._reader_socket = None
        self._reader_generation = 0
        self._private_ready_generation = None
        self._private_subscription_requested = False
        self._direct_ws_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None  # 🚀 主动心跳任务
        self._subscribed_market_stats: List[int] = []  # 订阅的market_stats市场

        # 保存事件循环引用
        self._event_loop: Optional[asyncio.AbstractEventLoop] = None

        # 订阅的市场和账户
        self._subscribed_markets: List[int] = []

        # 数据缓存
        self._order_books: Dict[str, OrderBookData] = {}
        # 🔥 本地订单簿状态维护（参考 test_sol_orderbook.py）
        # {market_index: {'bids': {price: size}, 'asks': {price: size}}}
        self._local_orderbooks: Dict[int, Dict[str, Dict[str, str]]] = {}
        self._account_data: Dict[str, Any] = {}
        # 🔥 持仓缓存（供position_monitor使用）
        self._position_cache: Dict[str, Dict[str, Any]] = {}
        # 🔥 余额缓存（供balance_monitor使用）
        self._balance_cache: Dict[str, Dict[str, Any]] = {}
        # 🔥 订单缓存（供REST降级/状态查询使用）
        self._order_cache: Dict[str, OrderData] = {}
        self._order_cache_by_symbol: Dict[str, Dict[str, OrderData]] = {}
        self._order_cache_ready: bool = False

        # 回调函数
        self._ticker_callbacks: List[Callable] = []
        self._orderbook_callbacks: List[Callable] = []
        self._order_callbacks: List[Callable] = []
        self._order_fill_callbacks: List[Callable] = []  # 🔥 新增：订单成交回调
        self._position_callbacks: List[Callable] = []

        # 错误避让控制器
        self._backoff_controller = None
        try:
            from core.services.arbitrage_monitor_v2.risk_control.error_backoff_controller import ErrorBackoffController
            self._backoff_controller = ErrorBackoffController()
        except ImportError:
            logger.warning("⚠️ 错误避让控制器未初始化（模块未找到）")
        
        # 连接状态
        self._connected = False
        self._running = True  # WebSocket运行状态
        self._reconnect_attempts = 0
        # 🔥 重连次数统计（实际重连成功的次数）
        self._reconnect_count = 0  # 重连成功次数

        # 价格日志控制（每10秒打印一次）
        self._last_price_log_time: Dict[str, float] = {}  # symbol -> 上次打印时间
        
        # 🔥 订单簿日志限流（避免刷屏）
        self._orderbook_log_count: Dict[str, int] = {}  # {symbol: count}
        self._orderbook_log_interval = 1000  # 每1000次更新打印一次日志
        self._empty_callbacks_warning_logged = False  # 回调列表为空的警告只打印一次
        
        # 🔥 心跳日志频率控制（每30秒记录一次）
        self._last_heartbeat_log_time: float = 0  # 上次心跳日志时间
        self._heartbeat_log_interval: float = 30.0  # 心跳日志间隔（秒）

        # 🔥 订单簿订阅去重缓存：market_index -> True
        self._subscribed_orderbooks: Set[int] = set()

        # 🔥 Trade去重：防止WebSocket重复推送导致重复处理
        self._processed_trade_ids: deque = deque(
            maxlen=1000)  # 保留最近1000个trade_id

        # 🔥 订单推送去重：按照 (order_id, client_id, filled) 组合记录
        self._processed_order_update_keys: deque = deque(maxlen=2000)
        self._processed_order_update_set: Set[str] = set()

        # 🔥 订单推送日志策略（解决：一腿成交、一腿未成交时缺少可见原因）
        # - 默认：只在“状态/成交变化”时打印，OPEN 订单按间隔重复提示（避免长时间无感知）
        # - 可配置强制打印每条推送（用于排障）
        ws_orders_log_cfg = config.get("ws_orders_log", {})
        if not isinstance(ws_orders_log_cfg, dict):
            ws_orders_log_cfg = {}
        self._ws_orders_log_all_pushes: bool = bool(
            ws_orders_log_cfg.get("log_all_pushes", False)
        )
        # OPEN 状态重复提示间隔（秒）
        try:
            self._ws_orders_open_log_interval_sec: float = float(
                ws_orders_log_cfg.get("open_log_interval_sec", 8.0)
            )
        except Exception:
            self._ws_orders_open_log_interval_sec = 8.0
        # 订单推送日志级别（INFO/WARNING/ERROR/DEBUG）；默认 INFO
        self._ws_orders_log_level: str = str(
            ws_orders_log_cfg.get("level", "INFO")
        ).upper()
        # 记录每个订单上次打印的签名与时间（用于状态变化检测与OPEN限频）
        self._ws_orders_last_signature: Dict[str, str] = {}
        self._ws_orders_last_log_time: Dict[str, float] = {}

        # 🔥 网络流量统计（字节数）
        self._network_bytes_received = 0
        self._network_bytes_sent = 0
        self._ws_manual_health_ping_sent = False

        # 🔥 数据超时检测（参考 test_sol_orderbook.py）
        self._last_message_time: float = 0  # 最后接收消息的时间戳（包含心跳）
        self._last_business_message_time: float = 0  # 最后一次业务消息时间戳
        self._connection_start_time: float = 0  # 🔥 连接建立时的时间戳（用于诊断）
        self._data_timeout_task: Optional[asyncio.Task] = None  # 数据超时检测任务
        self._data_timeout_seconds: float = 60.0  # 数据超时阈值（60秒，提高检测响应速度）
        manual_ping_env = float(os.getenv("LIGHTER_WS_PING_THRESHOLD", "30"))
        timeout_floor = max(30.0, self._data_timeout_seconds)
        if manual_ping_env >= timeout_floor:
            manual_ping_env = max(timeout_floor / 2, 5.0)
        self._ws_manual_ping_threshold = manual_ping_env

        # 🔥 确保logger有文件handler，写入ExchangeAdapter.log
        self._setup_logger()

        # 🔥 创建专门的价格日志器（只输出到文件，不显示在终端）
        self._price_logger = self._setup_price_logger()

        logger.info("[Lighter] WebSocket: 初始化完成")

    def _get_order_raw_status(self, order: Optional[OrderData]) -> str:
        """从 OrderData 中提取 lighter 原始状态字符串（用于排障/原因分析）"""
        if not order:
            return ""
        params = getattr(order, "params", None)
        if isinstance(params, dict):
            raw = params.get("lighter_status") or params.get("status_raw")
            if raw:
                return str(raw)
        raw_data = getattr(order, "raw_data", None)
        if isinstance(raw_data, dict):
            raw = raw_data.get("status") or raw_data.get("state")
            if raw:
                return str(raw)
        return ""

    def _get_order_log_level_func(self, *, is_error: bool = False, is_warning: bool = False):
        """根据配置返回 logger 的日志函数（info/warning/error/debug）"""
        if is_error:
            return logger.error
        if is_warning:
            return logger.warning
        level = (self._ws_orders_log_level or "INFO").upper()
        if level == "DEBUG":
            return logger.debug
        if level in ("WARN", "WARNING"):
            return logger.warning
        if level == "ERROR":
            return logger.error
        return logger.info

    def _build_order_signature(self, order: OrderData, raw_status: str) -> str:
        """构建用于判断‘是否需要再次打印’的签名"""
        status = getattr(order, "status", None)
        status_str = status.value if hasattr(status, "value") else str(status or "")
        filled = getattr(order, "filled", None)
        remaining = getattr(order, "remaining", None)
        amount = getattr(order, "amount", None)
        price = getattr(order, "price", None)
        average = getattr(order, "average", None)
        side = getattr(order, "side", None)
        side_str = side.value if hasattr(side, "value") else str(side or "")
        return (
            f"{status_str}|raw={raw_status}|side={side_str}|"
            f"filled={filled}|remaining={remaining}|amount={amount}|"
            f"price={price}|avg={average}"
        )

    def _log_order_push(
        self,
        *,
        order: Optional[OrderData],
        order_info: Optional[Dict[str, Any]],
        source: str,
        market_index: Optional[Any] = None,
    ) -> None:
        """
        统一记录订单推送日志：
        - 状态/成交变化：必打
        - OPEN：首次必打；之后按间隔重复提示（用于定位“另一腿为什么没成交”）
        - canceled-too-much-slippage：ERROR + 解决建议
        """
        if not order:
            if order_info:
                logger.warning("⚠️ [Lighter订单推送][%s] 订单解析失败: %s", source, order_info)
            else:
                logger.warning("⚠️ [Lighter订单推送][%s] 订单解析失败（无原始数据）", source)
            return

        raw_status = self._get_order_raw_status(order)
        raw_key = (raw_status or "").lower()
        order_id = str(getattr(order, "id", "") or "")
        client_id = str(getattr(order, "client_id", "") or "")
        symbol = str(getattr(order, "symbol", "") or "")

        # 构建签名，用于变化检测/限频
        sig = self._build_order_signature(order, raw_status)
        last_sig = self._ws_orders_last_signature.get(order_id)
        now = time.time()

        # 若要求打印“所有推送”，则跳过变化检测与限频
        force_log = bool(self._ws_orders_log_all_pushes)

        # slippage 取消：无条件打印（ERROR）并给出解决建议
        if raw_key and "slippage" in raw_key:
            logf = self._get_order_log_level_func(is_error=True)
            logf(
                "❌ [Lighter订单滑点保护触发][%s] symbol=%s id=%s client_id=%s raw_status=%s | "
                "已成交=%s/%s remaining=%s price=%s avg=%s | "
                "建议：提高 slippage（如 slippage_tolerance_pct/适配器 slippage），或减小下单数量，或等待流动性恢复/改用限价单",
                source,
                symbol,
                order_id or "N/A",
                client_id or "N/A",
                raw_status or "N/A",
                getattr(order, "filled", None),
                getattr(order, "amount", None),
                getattr(order, "remaining", None),
                getattr(order, "price", None),
                getattr(order, "average", None),
            )
            self._ws_orders_last_signature[order_id] = sig
            self._ws_orders_last_log_time[order_id] = now
            return

        # 变化检测：状态/成交变化必打
        changed = (last_sig is None) or (sig != last_sig)

        status = getattr(order, "status", None)
        status_str = status.value if hasattr(status, "value") else str(status or "")

        # OPEN：首次/变化必打；非变化按间隔重复提示
        if status_str.upper() == "OPEN" and not force_log and not changed:
            last_t = self._ws_orders_last_log_time.get(order_id, 0.0)
            if now - last_t < self._ws_orders_open_log_interval_sec:
                return

        if not force_log and not changed:
            return

        # 选择日志等级：CANCELED/REJECTED 等用 warning，其它用配置默认等级
        is_warning = status_str.upper() in ("CANCELED", "CANCELLED", "REJECTED", "EXPIRED")
        logf = self._get_order_log_level_func(is_warning=is_warning)

        side = getattr(order, "side", None)
        side_str = side.value if hasattr(side, "value") else str(side or "")
        display_price = getattr(order, "average", None) or getattr(order, "price", None)
        price_label = "成交均价" if getattr(order, "average", None) else "价格"

        logf(
            "🧾 [Lighter订单推送][%s] market=%s symbol=%s id=%s client_id=%s "
            "side=%s status=%s raw_status=%s | %s=%s | 已成交=%s/%s remaining=%s",
            source,
            str(market_index) if market_index is not None else "N/A",
            symbol,
            order_id or "N/A",
            client_id or "N/A",
            side_str or "N/A",
            status_str or "N/A",
            raw_status or "N/A",
            price_label,
            display_price,
            getattr(order, "filled", None),
            getattr(order, "amount", None),
            getattr(order, "remaining", None),
        )

        self._ws_orders_last_signature[order_id] = sig
        self._ws_orders_last_log_time[order_id] = now

    def _update_balance_from_stats(
        self,
        stats_data: Dict[str, Any],
        *,
        source: str,
        is_init: bool,
    ) -> None:
        """
        统一处理余额缓存更新 + 限频日志（避免 account_all / user_stats 多处重复代码）。

        Args:
            stats_data: WS 推送的 stats 字段
            source: "account_all" 或 "user_stats"（用于日志文案区分）
            is_init: 是否为订阅确认阶段的初始化日志
        """
        try:
            collateral = Decimal(str(stats_data.get("collateral", "0")))
            portfolio_value = Decimal(str(stats_data.get("portfolio_value", "0")))
            available_balance = Decimal(str(stats_data.get("available_balance", "0")))
            buying_power = Decimal(str(stats_data.get("buying_power", "0")))

            self._balance_cache["USDC"] = {
                "currency": "USDC",
                "free": available_balance,
                "total": portfolio_value,
                "used": portfolio_value - available_balance,
                "collateral": collateral,
                "buying_power": buying_power,
                "timestamp": datetime.now(),
                "raw_data": stats_data,
            }

            # 初始化阶段：始终打印一次
            if is_init:
                if source == "account_all":
                    logger.info(
                        "✅ [Lighter] 余额初始化成功(account_all): USDC "
                        "可用=$%s, 总额=$%s, 抵押=$%s, 购买力=$%s",
                        f"{float(available_balance):,.2f}",
                        f"{float(portfolio_value):,.2f}",
                        f"{float(collateral):,.2f}",
                        f"{float(buying_power):,.2f}",
                    )
                else:
                    logger.info(
                        "✅ [Lighter] 余额初始化成功: USDC "
                        "可用=$%s, 总额=$%s, 抵押=$%s, 购买力=$%s",
                        f"{float(available_balance):,.2f}",
                        f"{float(portfolio_value):,.2f}",
                        f"{float(collateral):,.2f}",
                        f"{float(buying_power):,.2f}",
                    )
                return

            # 更新阶段：INFO 限频，其他用 DEBUG
            current_time = time.time()
            if not hasattr(self, "_last_balance_log_time"):
                self._last_balance_log_time = 0
            balance_log_interval = 30
            if current_time - self._last_balance_log_time >= balance_log_interval:
                if source == "account_all":
                    logger.info(
                        "✅ [Lighter] 余额更新(account_all): USDC "
                        "可用=$%s, 总额=$%s, 抵押=$%s, 购买力=$%s",
                        f"{float(available_balance):,.2f}",
                        f"{float(portfolio_value):,.2f}",
                        f"{float(collateral):,.2f}",
                        f"{float(buying_power):,.2f}",
                    )
                else:
                    logger.info(
                        "✅ [Lighter] 余额更新: USDC "
                        "可用=$%s, 总额=$%s, 抵押=$%s, 购买力=$%s",
                        f"{float(available_balance):,.2f}",
                        f"{float(portfolio_value):,.2f}",
                        f"{float(collateral):,.2f}",
                        f"{float(buying_power):,.2f}",
                    )
                self._last_balance_log_time = current_time
            else:
                # Debug 兜底：确保缓存已更新，但不刷屏
                if source == "account_all":
                    logger.debug(
                        "📡 [WS] 余额缓存已更新(account_all): USDC 可用=$%s, 总额=$%s, 抵押=$%s",
                        f"{float(available_balance):,.2f}",
                        f"{float(portfolio_value):,.2f}",
                        f"{float(collateral):,.2f}",
                    )
                else:
                    logger.debug(
                        "📡 [WS] 余额缓存已更新: USDC 可用=$%s, 总额=$%s, 抵押=$%s",
                        f"{float(available_balance):,.2f}",
                        f"{float(portfolio_value):,.2f}",
                        f"{float(collateral):,.2f}",
                    )
        except Exception as exc:
            logger.warning("⚠️ [Lighter] 处理余额 stats 失败: %s", exc)

    async def _process_orders_payload(
        self,
        orders_data: Any,
        *,
        source: str,
        verified_account: bool = False,
    ) -> None:
        """
        统一处理 orders 推送（account_all / account_all_orders）：
        parse -> 去重 -> 打印 -> 更新持仓缓存 -> 回调 -> 成交回调
        """
        if not isinstance(orders_data, dict):
            logger.warning("⚠️ [Lighter订单推送][%s] orders 数据格式异常: %s", source, type(orders_data))
            return

        for market_index, order_list in orders_data.items():
            if not isinstance(order_list, list):
                continue

            for order_info in order_list:
                strict_rest = getattr(self, "_strict_rest", None)
                if strict_rest and strict_rest.strict_order_state:
                    if not verified_account:
                        continue
                    order = strict_rest._accept_terminal_order(order_info, market_index)
                    if order is None:
                        continue
                else:
                    order = self._parse_order_any(order_info)
                if not order:
                    logger.warning("⚠️ [Lighter] 订单解析失败: %s", order_info)
                    continue

                # 默认仍保留去重；排障时可配置强制打印所有推送
                if (not self._ws_orders_log_all_pushes) and self._should_skip_order_update(order):
                    continue

                self._log_order_push(
                    order=order,
                    order_info=order_info,
                    source=source,
                    market_index=market_index,
                )

                # 根据订单更新持仓缓存（避免等待 account_all 推送）
                if not (strict_rest and strict_rest.strict_order_state):
                    self._adjust_position_cache_from_order(order)

                # 触发订单回调
                if self._order_callbacks:
                    for callback in self._order_callbacks:
                        if asyncio.iscoroutinefunction(callback):
                            await callback(order)
                        else:
                            callback(order)

                # 成交回调
                if order.status == OrderStatus.FILLED and self._order_fill_callbacks:
                    logger.debug(
                        "📣 [Lighter] 触发订单成交回调(from %s): order_id=%s, filled=%s, 回调数量=%s",
                        source,
                        getattr(order, "id", "?"),
                        getattr(order, "filled", None),
                        len(self._order_fill_callbacks),
                    )
                    for callback in self._order_fill_callbacks:
                        if asyncio.iscoroutinefunction(callback):
                            await callback(order)
                        else:
                            callback(order)

    @staticmethod
    def _extract_market_index_from_channel(channel: str) -> Optional[int]:
        """
        从 channel 中提取 market_index（兼容 "order_book:0" / "order_book/0"）。
        """
        if not channel:
            return None
        try:
            if ":" in channel:
                return int(channel.split(":")[1])
            if "/" in channel:
                return int(channel.split("/")[1])
        except Exception:
            return None
        return None

    def _handle_orderbook_data(
        self,
        *,
        symbol: str,
        market_index: int,
        order_book_data: Optional[OrderBookData],
        raw_order_book: Optional[Dict[str, Any]] = None,
        is_snapshot: bool,
    ) -> None:
        """
        统一处理订单簿：缓存 + 限频日志 + 触发回调 +（可选）从盘口推导 ticker。

        - subscribed/order_book 与 update/order_book 使用同一条路径
        - 旧入口 _on_order_book_update 也复用，避免逻辑分叉
        """
        if not symbol or not order_book_data:
            logger.warning(
                "⚠️ [Lighter] 订单簿数据为空: symbol=%s market_index=%s",
                symbol,
                market_index,
            )
            return

        # 缓存
        self._order_books[symbol] = order_book_data

        # 日志：快照阶段额外打印一次订阅成功；更新阶段按 interval 限频
        if symbol not in self._orderbook_log_count:
            self._orderbook_log_count[symbol] = 0
        self._orderbook_log_count[symbol] += 1
        should_log = (self._orderbook_log_count[symbol] % self._orderbook_log_interval == 0)

        if is_snapshot:
            # 用户侧默认不需要订单簿细节；保持 debug，避免刷屏
            logger.debug(
                "✅ [Lighter] %s 订单簿订阅成功 (快照已加载): bid=%s, ask=%s",
                symbol,
                order_book_data.best_bid.price if order_book_data.best_bid else "N/A",
                order_book_data.best_ask.price if order_book_data.best_ask else "N/A",
            )
        else:
            # 更新阶段：仅在限频点打印或数据异常时打印
            if order_book_data.best_bid and order_book_data.best_ask:
                if should_log:
                    # 用户侧默认不需要订单簿细节；保持 debug，避免刷屏
                    logger.debug(
                        "📥 [Lighter] %s 订单簿更新 (第%s次): bid=%s×%s, ask=%s×%s, 回调数量=%s",
                        symbol,
                        self._orderbook_log_count[symbol],
                        order_book_data.best_bid.price,
                        order_book_data.best_bid.size,
                        order_book_data.best_ask.price,
                        order_book_data.best_ask.size,
                        len(self._orderbook_callbacks),
                    )
            else:
                logger.warning(
                    "⚠️ [Lighter] %s 订单簿数据不完整 (第%s次): best_bid=%s, best_ask=%s",
                    symbol,
                    self._orderbook_log_count[symbol],
                    order_book_data.best_bid if order_book_data else None,
                    order_book_data.best_ask if order_book_data else None,
                )

        # 触发订单簿回调（内部会做 order_book -> ticker 的兜底）
        self._trigger_orderbook_callbacks(order_book_data)

        # 保留旧行为：如果给了 raw_order_book，则尝试生成 ticker 并触发
        if raw_order_book and self._ticker_callbacks:
            ticker = self._extract_ticker_from_orderbook(symbol, raw_order_book, order_book_data)
            if ticker:
                self._trigger_ticker_callbacks(ticker)
    
    def get_network_stats(self) -> Dict[str, int]:
        """获取网络流量统计"""
        return {
            'bytes_received': self._network_bytes_received,
            'bytes_sent': self._network_bytes_sent,
        }
    
    def get_reconnect_stats(self) -> Dict[str, int]:
        """获取重连统计"""
        return {
            'reconnect_count': self._reconnect_count,
        }

    def _setup_logger(self):
        """设置logger的文件handler"""
        from logging.handlers import RotatingFileHandler
        from pathlib import Path

        # 🔥 关键修复：阻止日志传播到父logger（避免输出到终端UI）
        logger.propagate = False

        # 确保logs目录存在
        Path("logs").mkdir(parents=True, exist_ok=True)

        # 检查是否已有文件handler
        has_file_handler = any(
            isinstance(h, RotatingFileHandler) and 'ExchangeAdapter.log' in str(
                h.baseFilename)
            for h in logger.handlers
        )

        if not has_file_handler:
            # 添加文件handler
            file_handler = RotatingFileHandler(
                'logs/ExchangeAdapter.log',
                maxBytes=10*1024*1024,  # 10MB
                backupCount=3,
                encoding='utf-8'
            )
            file_handler.setLevel(logging.INFO)
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
            )
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            logger.setLevel(logging.INFO)  # 确保logger级别至少是INFO
            logger.debug("[Lighter] WebSocket: 日志文件handler已配置")

    def _setup_price_logger(self):
        """创建专门的价格日志器（只输出到文件，不显示在终端）"""
        from logging.handlers import RotatingFileHandler
        from pathlib import Path

        # 创建独立的logger
        price_logger = logging.getLogger(f"{__name__}.price")
        price_logger.setLevel(logging.INFO)
        
        # 🔥 关键修复：阻止日志传播到父logger（避免输出到终端UI）
        price_logger.propagate = False  # 🔥 关键：不传播到父logger，避免输出到控制台

        # 只添加文件handler
        if not price_logger.handlers:
            Path("logs").mkdir(parents=True, exist_ok=True)
            file_handler = RotatingFileHandler(
                'logs/ExchangeAdapter.log',
                maxBytes=10*1024*1024,  # 10MB
                backupCount=3,
                encoding='utf-8'
            )
            file_handler.setLevel(logging.INFO)
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
            )
            file_handler.setFormatter(formatter)
            price_logger.addHandler(file_handler)

        return price_logger

    # ============= 连接管理 =============

    async def connect(self):
        """建立WebSocket连接"""
        try:
            if self._connected:
                logger.warning("[Lighter] WebSocket已连接")
                return

            if self.signer_client is None and self.account_index is not None and self.api_key_private_key:
                self.signer_client = self._create_signer_client(
                    base_url=self.base_url,
                    account_index=self.account_index,
                    api_key_index=self.api_key_index,
                    api_key_private_key=self.api_key_private_key,
                )

            # 🔥 保存事件循环引用（用于线程安全的回调调度）
            self._event_loop = asyncio.get_event_loop()

            # 🔧 重要：重置运行标志，允许WebSocket任务运行
            self._running = True
            self._connected = True
            logger.info("[Lighter] Lighter WebSocket准备就绪")

        except (Exception, asyncio.CancelledError) as e:
            self._connected = False
            self._running = False
            await self._close_signer_client()
            logger.error(f"❌ [Lighter] WebSocket连接失败: {e}")
            raise

    async def disconnect(self):
        """断开WebSocket连接"""
        self._invalidate_private_socket(self._direct_ws)
        try:
            self._connected = False
            self._running = False  # 停止所有WebSocket循环

            # 🚀 取消心跳任务和数据超时检测任务
            if self._heartbeat_task and not self._heartbeat_task.done():
                self._heartbeat_task.cancel()
                try:
                    await self._heartbeat_task
                except asyncio.CancelledError:
                    pass
            
            if self._data_timeout_task and not self._data_timeout_task.done():
                self._data_timeout_task.cancel()
                try:
                    await self._data_timeout_task
                except asyncio.CancelledError:
                    pass

            # 关闭直接WebSocket连接
            if self._direct_ws_task and not self._direct_ws_task.done():
                self._direct_ws_task.cancel()
                try:
                    await self._direct_ws_task
                except asyncio.CancelledError:
                    pass

            if self._direct_ws:
                try:
                    await self._direct_ws.close()
                except:
                    pass
                self._direct_ws = None

            logger.info("✅ [Lighter] WebSocket已断开（包括直接订阅）")

        except Exception as e:
            logger.error(f"❌ [Lighter] 断开WebSocket时出错: {e}")
        finally:
            await self._close_signer_client()

    async def reconnect(self):
        """重新连接WebSocket"""
        logger.info("[Lighter] 尝试重新连接WebSocket...")

        await self.disconnect()
        await asyncio.sleep(min(self._reconnect_attempts * 2, 30))

        try:
            await self.connect()
            await self._resubscribe_all()
            self._reconnect_attempts = 0
            logger.info("WebSocket重连成功")
        except Exception as e:
            self._reconnect_attempts += 1
            logger.error(
                f"WebSocket重连失败 (尝试 {self._reconnect_attempts}): {e}")
            asyncio.create_task(self.reconnect())

    async def _resubscribe_all(self):
        """重新订阅所有频道"""
        # 🔥 重新订阅市场数据（账户数据由直接WebSocket自动处理，无需手动重订阅）
        if self._private_subscription_requested:
            await self._subscribe_account_all_orders()
        for market_index in self._subscribed_markets.copy():
            await self.subscribe_orderbook(market_index)

    # ============= 订阅管理 =============

    async def subscribe_ticker(self, symbol: str, callback: Optional[Callable] = None):
        """
        订阅ticker数据（使用market_stats频道）

        Args:
            symbol: 交易对符号
            callback: 数据回调函数
        """
        market_index = self.get_market_index(symbol)
        if market_index is None:
            logger.warning(f"未找到交易对 {symbol} 的市场索引")
            return

        # 🔥 修复：只在回调不为None且不在列表中时才添加
        if callback and callback not in self._ticker_callbacks:
            self._ticker_callbacks.append(callback)
            logger.debug(f"✅ [Lighter] 已注册ticker回调函数（总数: {len(self._ticker_callbacks)}）")

        # 🔥 使用market_stats代替orderbook
        await self.subscribe_market_stats(market_index, symbol)

        # 🔥 兜底：同时订阅order_book（带去重），用盘口中间价更新ticker缓存
        # 一些环境下 market_stats 不推送价格，这里用盘口推送补充价格
        try:
            if market_index not in self._subscribed_orderbooks:
                await self.subscribe_orderbook(market_index, None)
                self._subscribed_orderbooks.add(market_index)
        except Exception as e:
            logger.warning(f"⚠️ 订阅order_book失败（用于价格兜底）: {e}")

    async def subscribe_orderbook(self, market_index_or_symbol, callback: Optional[Callable] = None):
        """
        订阅订单簿（使用直接WebSocket）

        Args:
            market_index_or_symbol: 市场索引或交易对符号
            callback: 订单簿回调函数（可选，None表示使用统一回调）
        """
        logger.debug(
            "🔍 [Lighter WebSocket] subscribe_orderbook 调用: market=%s callback=%s",
            market_index_or_symbol,
            callback is not None,
        )
        
        # 🔥 注册回调（改造：支持统一回调模式）
        if callback is not None:
            # 如果提供了回调且不在列表中，添加它
            if callback not in self._orderbook_callbacks:
                self._orderbook_callbacks.append(callback)
                # 🔥 去重优化：合并重复日志
                logger.info(
                    "✅ [Lighter] 已注册订单簿回调函数（总数: %s）",
                    len(self._orderbook_callbacks),
                )
            else:
                logger.debug("⚠️ [Lighter WebSocket] 回调函数已存在，跳过注册")
        
        if isinstance(market_index_or_symbol, str):
            symbol = market_index_or_symbol
            market_index = self.get_market_index(symbol)
            if market_index is None:
                # 🔥 去重优化：合并重复日志
                logger.warning(
                    "❌ [Lighter] 未找到交易对 %s 的市场索引", symbol
                )
                return
            logger.debug(
                "🔍 [Lighter WebSocket] 市场索引匹配: %s -> market_index=%s",
                symbol,
                market_index,
            )
        else:
            market_index = market_index_or_symbol
            symbol = self._get_symbol_from_market_index(market_index)
            logger.debug(
                "🔍 [Lighter WebSocket] 使用市场索引: market_index=%s -> symbol=%s",
                market_index,
                symbol,
            )

        # 🔥 改用直接WebSocket订阅（参考 test_sol_orderbook.py）
        if market_index not in self._subscribed_markets:
            self._subscribed_markets.append(market_index)
            # 🔥 去重优化：合并重复日志
            logger.info(
                "📡 [Lighter] 已订阅订单簿: %s (market_index=%s), 当前订阅=%s",
                symbol,
                market_index,
                list(self._subscribed_markets),
            )

            # 🔥 确保直接WebSocket连接已启动，然后发送订阅
            await self._ensure_direct_ws_running()
        else:
            logger.debug(
                "ℹ️ [Lighter WebSocket] market_index=%s 已订阅，跳过", market_index
            )

    async def subscribe_market_stats(self, market_index_or_symbol, symbol: Optional[str] = None):
        """
        订阅市场统计数据（market_stats频道，用于获取价格）

        Args:
            market_index_or_symbol: 市场索引或交易对符号
            symbol: 交易对符号（如果第一个参数是市场索引）
        """
        if isinstance(market_index_or_symbol, str):
            symbol = market_index_or_symbol
            market_index = self.get_market_index(symbol)
            if market_index is None:
                logger.warning(f"未找到交易对 {symbol} 的市场索引")
                return
        else:
            market_index = market_index_or_symbol
            if symbol is None:
                symbol = self._get_symbol_from_market_index(market_index)

        if market_index not in self._subscribed_market_stats:
            self._subscribed_market_stats.append(market_index)
            logger.info(
                f"🔔 已订阅market_stats: {symbol} (market_index={market_index})")

            # 启动直接WebSocket订阅（如果尚未启动）
            await self._ensure_direct_ws_running()

    async def _ensure_direct_ws_running(self):
        """确保直接WebSocket订阅任务正在运行"""
        if not WEBSOCKETS_AVAILABLE:
            logger.warning("⚠️ websockets库未安装，无法直接订阅market_stats")
            return

        if self._direct_ws_task and not self._direct_ws_task.done():
            # 任务已在运行，发送新的订阅消息
            logger.debug("✅ [Lighter] WebSocket任务已在运行，发送新订阅")
            if self._direct_ws:
                await self._send_market_stats_subscriptions()
                await self._send_orderbook_subscriptions()
            else:
                logger.debug(
                    "⚠️ [Lighter] WebSocket任务运行中但连接未建立，订阅将在连接建立后自动发送")
        else:
            # 启动新任务
            logger.info(
                f"🚀 [Lighter] 启动新WebSocket任务（待订阅: {len(self._subscribed_market_stats)} 个market_stats, {len(self._subscribed_markets)} 个orderbook）")
            self._direct_ws_task = asyncio.create_task(
                self._run_direct_ws_subscription())

    def _register_backoff_error(self, error_code: str, error_message: str = ""):
        """
        注册错误到避让控制器
        
        Args:
            error_code: 错误码
            error_message: 错误信息
        """
        if self._backoff_controller:
            self._backoff_controller.register_error("lighter", error_code, error_message)
    
    async def send_tx_batch(
        self,
        tx_types: List[int],
        tx_infos: List[str],
        request_id: Optional[str] = None,
        timeout: float = 10.0
    ) -> Dict[str, Any]:
        """
        通过 WebSocket 发送批量交易（jsonapi/sendtxbatch）
        """
        if not WEBSOCKETS_AVAILABLE:
            raise ImportError("websockets 库未安装，无法发送WS批量交易")

        if not tx_types or not tx_infos:
            raise ValueError("tx_types 和 tx_infos 不能为空")
        if len(tx_types) != len(tx_infos):
            raise ValueError("tx_types 与 tx_infos 长度不一致")

        request_id = request_id or f"tx_batch_{uuid.uuid4().hex}"

        payload = {
            "type": "jsonapi/sendtxbatch",
            "data": {
                "id": request_id,
                "tx_types": json.dumps(tx_types),
                "tx_infos": json.dumps(tx_infos)
            }
        }

        logger.info(
            f"📨 [Lighter] WS批量交易请求: id={request_id}, 笔数={len(tx_types)}")

        try:
            async with websockets.connect(self.ws_url, ping_interval=None) as ws:
                try:
                    greeting = await asyncio.wait_for(ws.recv(), timeout=5.0)
                    logger.debug(f"[Lighter] WS握手: {greeting}")
                except asyncio.TimeoutError:
                    logger.debug("[Lighter] WS握手超时，继续发送交易")

                await ws.send(json.dumps(payload))
                response_raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
                logger.debug(f"[Lighter] WS批量交易响应: {response_raw}")

                try:
                    response = json.loads(response_raw)
                except json.JSONDecodeError:
                    response = {"raw": response_raw}

                if isinstance(response, dict) and "error" in response:
                    error_info = response.get("error", {})
                    error_code = error_info.get("code") if isinstance(error_info, dict) else None
                    error_message = error_info.get("message", str(response)) if isinstance(error_info, dict) else str(response)
                    
                    # 🔥 注册错误到避让控制器
                    if error_code:
                        self._register_backoff_error(str(error_code), error_message)
                    
                    logger.error(f"❌ [Lighter] WS批量交易失败: {response}")
                    raise RuntimeError(response)

                return response

        except Exception as e:
            # 🔥 尝试从异常中提取错误码
            error_str = str(e)
            if "21104" in error_str or "invalid nonce" in error_str.lower():
                self._register_backoff_error("21104", error_str)
            
            logger.error(f"❌ [Lighter] WS批量交易发送失败: {e}")
            raise

    async def _send_market_stats_subscriptions(self):
        """发送market_stats订阅消息（支持批量发送）"""
        if not self._direct_ws:
            logger.warning(
                f"⚠️ WebSocket未连接，暂不发送market_stats订阅 (待订阅数量: {len(self._subscribed_market_stats)})")
            return

        if not self._subscribed_market_stats:
            logger.debug("无待订阅的market_stats")
            return

        # 🔥 简化订阅方式：直接发送，参考测试脚本
        total_count = len(self._subscribed_market_stats)
        if total_count == 0:
            return
        
        logger.info(f"📨 [Lighter] 正在订阅 {total_count} 个market_stats...")
        
        for market_index in self._subscribed_market_stats:
            subscribe_msg = {
                "type": "subscribe",
                "channel": f"market_stats/{market_index}"
            }
            try:
                await self._direct_ws.send(json.dumps(subscribe_msg))
                await asyncio.sleep(0.1)  # 小延迟避免过快（参考测试脚本）
            except Exception as e:
                logger.error(f"❌ [Lighter] 发送market_stats订阅失败 (market_index={market_index}): {e}")
        
        logger.info(f"✅ [Lighter] market_stats订阅完成: {total_count}个")

    async def _send_orderbook_subscriptions(self):
        """发送订单簿订阅消息（支持批量发送，参考 test_sol_orderbook.py）"""
        if not self._direct_ws:
            logger.warning(
                f"⚠️ WebSocket未连接，暂不发送订单簿订阅 (待订阅数量: {len(self._subscribed_markets)})")
            return

        if not self._subscribed_markets:
            logger.debug("无待订阅的订单簿")
            return

        # 🔥 简化订阅方式：直接发送，参考测试脚本
        total_count = len(self._subscribed_markets)
        if total_count == 0:
            return
        
        logger.info(f"📨 [Lighter] 正在订阅 {total_count} 个order_book...")
        
        for market_index in self._subscribed_markets:
            subscribe_msg = {
                "type": "subscribe",
                "channel": f"order_book/{market_index}"
            }
            try:
                await self._direct_ws.send(json.dumps(subscribe_msg))
                await asyncio.sleep(0.1)  # 小延迟避免过快（参考测试脚本）
            except Exception as e:
                logger.error(f"❌ [Lighter] 发送订单簿订阅失败 (market_index={market_index}): {e}")
        
        logger.info(f"✅ [Lighter] order_book订阅完成: {total_count}个")

    async def _send_account_subscriptions(self, auth_token: str) -> None:
        """发送账户相关订阅（account_all_orders / account_all / user_stats）"""
        if not self._direct_ws:
            logger.info("ℹ️  [Lighter] WebSocket未连接，跳过账户订阅")
            return

        if self.account_index is None:
            logger.warning("⚠️ [Lighter] account_index 未配置，无法订阅账户频道")
            return

        account_socket = self._direct_ws

        account_channels = [
            ("account_all_orders", "订单状态推送"),
            ("account_all", "订单/持仓混合推送"),
            ("account_all_positions", "持仓专用推送"),
            ("user_stats", "余额统计"),
        ]
        for channel_name, desc in account_channels:
            subscribe_msg = {
                "type": "subscribe",
                "channel": f"{channel_name}/{self.account_index}",
                "auth": auth_token,
            }
            try:
                await account_socket.send(json.dumps(subscribe_msg))
                if (auth_token and channel_name in ("account_all_orders", "account_all")
                        and self._direct_ws is account_socket):
                    self._authenticated_account_socket = account_socket
                await asyncio.sleep(0.1)
                logger.info(f"✅ [Lighter] 已订阅{desc}: {channel_name}")
            except Exception as exc:
                logger.error(f"❌ [Lighter] 订阅{desc}失败: {exc}")

    def record_received(self):
        now=time.monotonic();last=getattr(self,'last_received_monotonic',0)
        stale=getattr(self,'liveness_seconds',None)
        if stale is not None and last and now-last>=stale+getattr(self,'quick_resume_seconds',10):
            self.liveness_revision=getattr(self,'liveness_revision',0)+1
        self.last_received_monotonic=now

    async def _listen_websocket(self, ws) -> None:
        """监听WebSocket消息（贴近 test_sol_orderbook 实现）"""
        message_count = 0
        try:
            while self._running:
                try:
                    message = await ws.recv()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning(f"⚠️ [Lighter] 接收消息失败: {exc}")
                    break

                if message is None:
                    continue
                if ws is not getattr(self,'_reader_socket',None) or self._reader_generation!=self.generation:
                    break

                self._network_bytes_received += len(message.encode("utf-8"))
                if ws is self._reader_socket and getattr(self,'_reader_connected',False):
                    self.record_received()
                self._last_message_time = time.time()
                message_count += 1

                if message_count % 5000 == 0:
                    elapsed = time.time() - self._connection_start_time
                    logger.info(
                        f"📊 [Lighter] 已接收 {message_count} 条消息 | 连接持续 {elapsed:.0f} 秒"
                    )

                try:
                    data = json.loads(message)
                except json.JSONDecodeError as exc:
                    logger.error(f"❌ [Lighter] JSON解析失败: {exc}")
                    continue

                try:
                    await self._handle_direct_ws_message(data)
                except Exception as exc:
                    logger.error(f"❌ [Lighter] 处理消息失败: {exc}", exc_info=True)
                    # 不直接 break，继续接收后续消息
        finally:
            self._invalidate_private_socket(ws)

    def _invalidate_private_socket(self,socket):
        # Readiness is invalid BEFORE socket close/backoff can await; an old reader cannot invalidate a new one.
        if socket is getattr(self,'_reader_socket',None):
            self._reader_connected=False
            self._reader_socket=None
            self._private_ready_socket=None
            self._private_ready_generation=None
            self._authenticated_account_socket=None

    async def subscribe_orders(self, callback: Optional[Callable] = None):
        """
        订阅订单更新

        使用直接WebSocket连接订阅account_all_orders频道
        这样可以接收挂单状态推送，而不仅仅是成交推送

        Args:
            callback: 数据回调函数
        """
        if callback:
            self._order_callbacks.append(callback)

        # 🔥 启动直接订阅account_all_orders（包含挂单状态和成交推送）
        await self._subscribe_account_all_orders()

    async def subscribe_order_fills(self, callback: Callable) -> None:
        """
        订阅订单成交（专门监控FILLED状态的订单）

        🔥 使用直接WebSocket订阅account_all_orders，自动处理订单成交

        Args:
            callback: 订单成交回调函数，参数为OrderData
        """
        if callback:
            self._order_fill_callbacks.append(callback)

        # 🔥 统一使用直接WebSocket订阅订单数据（包含成交状态）
        await self._subscribe_account_all_orders()

    async def subscribe_positions(self, callback: Optional[Callable] = None):
        """
        订阅持仓更新

        🔥 使用直接WebSocket订阅account_all + account_all_positions频道（双重保障）

        Args:
            callback: 数据回调函数
        """
        if callback:
            self._position_callbacks.append(callback)

        # 🔥 启动直接WebSocket订阅（包含account_all频道，含持仓数据）
        # 如果已经通过subscribe_orders启动，这里会复用同一个连接
        await self._subscribe_account_all_orders()

        logger.info("✅ 持仓订阅已启用（通过account_all频道）")

    async def unsubscribe_ticker(self, symbol: str):
        """取消订阅ticker"""
        market_index = self.get_market_index(symbol)
        if market_index and market_index in self._subscribed_markets:
            self._subscribed_markets.remove(market_index)
            # 🔥 直接WebSocket会自动处理取消订阅，无需额外操作

    async def unsubscribe_orderbook(self, symbol: str):
        """取消订阅订单簿"""
        await self.unsubscribe_ticker(symbol)

    # ============= 消息处理 =============

    def _on_order_book_update(self, market_id: str, order_book: Dict[str, Any]):
        """
        订单簿更新回调

        Args:
            market_id: 市场ID
            order_book: 订单簿数据
        """
        try:
            market_index = int(market_id)
            symbol = self._get_symbol_from_market_index(market_index)

            if not symbol:
                logger.warning(f"未找到market_index={market_index}对应的符号")
                return

            # 解析订单簿
            order_book_data = self._parse_order_book(symbol, order_book)
            self._handle_orderbook_data(
                symbol=symbol,
                market_index=market_index,
                order_book_data=order_book_data,
                raw_order_book=order_book,
                is_snapshot=False,
            )

        except Exception as e:
            logger.error(f"❌ [Lighter] 处理订单簿更新失败: {e}")

    # ============= 数据解析 =============

    def _initialize_orderbook(self, market_index: int, orderbook_data: Dict[str, Any]):
        """
        初始化本地订单簿（处理完整快照，参考 test_sol_orderbook.py）
        
        Args:
            market_index: 市场索引
            orderbook_data: 订单簿数据（包含 bids 和 asks）
        """
        try:
            # 🔥 调试：打印原始timestamp
            raw_timestamp = orderbook_data.get('timestamp')
            if raw_timestamp:
                logger.debug(f"[Lighter] 订单簿timestamp原始值: {raw_timestamp} (type={type(raw_timestamp)})")
            else:
                logger.warning(f"⚠️ [Lighter] 订单簿缺少timestamp字段！data keys={list(orderbook_data.keys())}")
            
            parsed_timestamp = self._parse_orderbook_timestamp(orderbook_data)
            if parsed_timestamp:
                logger.debug(f"[Lighter] 订单簿timestamp解析成功: {parsed_timestamp}")
            else:
                logger.warning(f"⚠️ [Lighter] 订单簿timestamp解析失败！原始值={raw_timestamp}")
            
            # 初始化本地缓存
            self._local_orderbooks[market_index] = {
                'bids': {},  # {price: size}
                'asks': {},  # {price: size}
                'timestamp': parsed_timestamp,
                'offset': orderbook_data.get('offset'),
                'nonce': orderbook_data.get('nonce')
            }
            
            # 处理 bids（只添加 size > 0 的条目）
            bids = orderbook_data.get('bids', [])
            for bid in bids:
                if isinstance(bid, dict):
                    price = bid.get('price')
                    size = bid.get('size')
                elif isinstance(bid, (list, tuple)) and len(bid) >= 2:
                    price, size = bid[0], bid[1]
                else:
                    continue
                
                # 🔥 只添加 price 和 size 都存在且 size > 0 的条目
                if price and size:
                    try:
                        size_float = float(size)
                        if size_float > 0:
                            self._local_orderbooks[market_index]['bids'][str(price)] = str(size)
                    except (ValueError, TypeError):
                        continue  # 忽略无效的 size 值
            
            # 处理 asks（只添加 size > 0 的条目）
            asks = orderbook_data.get('asks', [])
            for ask in asks:
                if isinstance(ask, dict):
                    price = ask.get('price')
                    size = ask.get('size')
                elif isinstance(ask, (list, tuple)) and len(ask) >= 2:
                    price, size = ask[0], ask[1]
                else:
                    continue
                
                # 🔥 只添加 price 和 size 都存在且 size > 0 的条目
                if price and size:
                    try:
                        size_float = float(size)
                        if size_float > 0:
                            self._local_orderbooks[market_index]['asks'][str(price)] = str(size)
                    except (ValueError, TypeError):
                        continue  # 忽略无效的 size 值
                    
        except Exception as e:
            logger.error(f"❌ [Lighter] 初始化订单簿失败 (market_index={market_index}): {e}", exc_info=True)
    
    def _apply_orderbook_update(self, market_index: int, orderbook_data: Dict[str, Any]):
        """
        应用增量订单簿更新（参考 test_sol_orderbook.py）
        
        Args:
            market_index: 市场索引
            orderbook_data: 增量更新数据（包含 bids 和 asks）
        """
        try:
            # 增量不能替代完整快照（首次连接和重连后均须重新初始化）。
            if market_index not in self._local_orderbooks:
                return
            
            local_book = self._local_orderbooks[market_index]
            
            # 处理 bids 更新
            bids = orderbook_data.get('bids', [])
            for bid in bids:
                if isinstance(bid, dict):
                    price = bid.get('price')
                    size = bid.get('size')
                elif isinstance(bid, (list, tuple)) and len(bid) >= 2:
                    price, size = bid[0], bid[1]
                else:
                    continue
                
                if price:
                    price_str = str(price)
                    try:
                        size_float = float(size) if size is not None else 0.0
                    except (ValueError, TypeError):
                        size_float = 0.0
                    
                    # size = 0 表示删除该价格档位
                    if size_float == 0:
                        local_book['bids'].pop(price_str, None)
                    elif size_float > 0:
                        # 更新或添加（只保存 size > 0 的条目）
                        local_book['bids'][price_str] = str(size)
            
            # 处理 asks 更新
            asks = orderbook_data.get('asks', [])
            for ask in asks:
                if isinstance(ask, dict):
                    price = ask.get('price')
                    size = ask.get('size')
                elif isinstance(ask, (list, tuple)) and len(ask) >= 2:
                    price, size = ask[0], ask[1]
                else:
                    continue
                
                if price:
                    price_str = str(price)
                    try:
                        size_float = float(size) if size is not None else 0.0
                    except (ValueError, TypeError):
                        size_float = 0.0
                    
                    # size = 0 表示删除该价格档位
                    if size_float == 0:
                        local_book['asks'].pop(price_str, None)
                    elif size_float > 0:
                        # 更新或添加（只保存 size > 0 的条目）
                        local_book['asks'][price_str] = str(size)
            
            # 更新元数据（timestamp/offset/nonce）
            raw_timestamp = orderbook_data.get('timestamp')
            parsed_timestamp = self._parse_orderbook_timestamp(orderbook_data)
            if parsed_timestamp:
                local_book['timestamp'] = parsed_timestamp
                logger.debug(f"[Lighter] 更新订单簿timestamp: {parsed_timestamp}")
            elif raw_timestamp:
                logger.warning(f"⚠️ [Lighter] 订单簿更新timestamp解析失败: {raw_timestamp}")
            if 'offset' in orderbook_data:
                local_book['offset'] = orderbook_data.get('offset')
            if 'nonce' in orderbook_data:
                local_book['nonce'] = orderbook_data.get('nonce')
                        
        except Exception as e:
            logger.error(f"❌ [Lighter] 应用订单簿更新失败 (market_index={market_index}): {e}", exc_info=True)
    
    def _build_orderbook_from_local(self, symbol: str, market_index: int) -> Optional[OrderBookData]:
        """
        从本地订单簿状态构建 OrderBookData（参考 test_sol_orderbook.py）
        
        Args:
            symbol: 交易对符号
            market_index: 市场索引
            
        Returns:
            OrderBookData 对象，如果本地订单簿不存在或没有有效数据则返回 None
        """
        try:
            if market_index not in self._local_orderbooks:
                return None
            
            local_book = self._local_orderbooks[market_index]
            bids = []
            asks = []
            
            # 构建 bids（从高到低排序，过滤掉 size=0 的条目）
            if local_book.get('bids'):
                sorted_bids = sorted(local_book['bids'].items(), key=lambda x: float(x[0]), reverse=True)
                for price_str, size_str in sorted_bids:
                    size_decimal = self._safe_decimal(size_str)
                    # 🔥 只添加 size > 0 的条目
                    if size_decimal and size_decimal > 0:
                        bids.append(OrderBookLevel(
                            price=self._safe_decimal(price_str),
                            size=size_decimal
                        ))
            
            # 构建 asks（从低到高排序，过滤掉 size=0 的条目）
            if local_book.get('asks'):
                sorted_asks = sorted(local_book['asks'].items(), key=lambda x: float(x[0]))
                for price_str, size_str in sorted_asks:
                    size_decimal = self._safe_decimal(size_str)
                    # 🔥 只添加 size > 0 的条目
                    if size_decimal and size_decimal > 0:
                        asks.append(OrderBookLevel(
                            price=self._safe_decimal(price_str),
                            size=size_decimal
                        ))
            
            # 🔥 如果 bids 或 asks 为空，返回 None（避免返回无效的订单簿）
            if not bids or not asks:
                return None
            
            timestamp = local_book.get('timestamp') or datetime.now()
            return OrderBookData(
                symbol=symbol,
                bids=bids,
                asks=asks,
                timestamp=timestamp,
                nonce=local_book.get('nonce'),
                exchange_timestamp=local_book.get('timestamp'),
                raw_data={
                    'source': 'local_cache',
                    'market_index': market_index,
                    'offset': local_book.get('offset'),
                    'nonce': local_book.get('nonce')
                }
            )
        except Exception as e:
            logger.error(f"❌ [Lighter] 构建订单簿失败 (symbol={symbol}, market_index={market_index}): {e}", exc_info=True)
            return None

    def _parse_order_book(self, symbol: str, order_book: Dict[str, Any]) -> OrderBookData:
        """解析订单簿数据（保留用于兼容性）"""
        bids = []
        asks = []

        if "bids" in order_book:
            for bid in order_book["bids"]:
                # Lighter WebSocket返回字典格式：{'price': '...', 'size': '...'}
                if isinstance(bid, dict):
                    bids.append(OrderBookLevel(
                        price=self._safe_decimal(bid.get('price', 0)),
                        size=self._safe_decimal(bid.get('size', 0))
                    ))
                # 兼容列表/元组格式：['price', 'size']
                elif isinstance(bid, (list, tuple)) and len(bid) >= 2:
                    bids.append(OrderBookLevel(
                        price=self._safe_decimal(bid[0]),
                        size=self._safe_decimal(bid[1])
                    ))

        if "asks" in order_book:
            for ask in order_book["asks"]:
                # Lighter WebSocket返回字典格式：{'price': '...', 'size': '...'}
                if isinstance(ask, dict):
                    asks.append(OrderBookLevel(
                        price=self._safe_decimal(ask.get('price', 0)),
                        size=self._safe_decimal(ask.get('size', 0))
                    ))
                # 兼容列表/元组格式：['price', 'size']
                elif isinstance(ask, (list, tuple)) and len(ask) >= 2:
                    asks.append(OrderBookLevel(
                        price=self._safe_decimal(ask[0]),
                        size=self._safe_decimal(ask[1])
                    ))

        parsed_timestamp = self._parse_orderbook_timestamp(order_book)
        timestamp = parsed_timestamp or datetime.now()

        return OrderBookData(
            symbol=symbol,
            bids=bids,
            asks=asks,
            timestamp=timestamp,
            nonce=order_book.get('nonce'),
            exchange_timestamp=parsed_timestamp,
            raw_data=order_book
        )

    def _extract_ticker_from_orderbook(self, symbol: str, raw_data: Dict[str, Any], order_book: OrderBookData) -> Optional[TickerData]:
        """从订单簿中提取ticker数据"""
        try:
            best_bid = order_book.bids[0].price if order_book.bids else Decimal(
                "0")
            best_ask = order_book.asks[0].price if order_book.asks else Decimal(
                "0")

            # 最新价格取中间价
            last_price = (best_bid + best_ask) / \
                2 if best_bid > 0 and best_ask > 0 else best_bid or best_ask

            return TickerData(
                symbol=symbol,
                timestamp=datetime.now(),
                bid=best_bid,
                ask=best_ask,
                last=last_price,
                volume=self._safe_decimal(raw_data.get("volume_24h", 0)),
                high=self._safe_decimal(
                    raw_data.get("high_24h", last_price)),
                low=self._safe_decimal(
                    raw_data.get("low_24h", last_price))
            )
        except Exception as e:
            logger.error(f"❌ [Lighter] 提取ticker数据失败: {e}")
            return None

    def _parse_orders(self, orders_data: Dict[str, Any]) -> List[OrderData]:
        """
        【历史兼容】解析订单列表（不建议新代码使用）

        - 新代码请优先使用 `_process_orders_payload()` 或 `_parse_order_any()`
        - 保留该方法是为了兼容旧文档/旧分支引用
        """
        orders = []
        for market_index_str, order_list in orders_data.items():
            try:
                market_index = int(market_index_str)
                symbol = self._get_symbol_from_market_index(market_index)

                for order_info in order_list:
                    parsed = self._parse_order_any(order_info, symbol_hint=symbol)
                    if parsed:
                        orders.append(parsed)
            except Exception as e:
                logger.error(f"❌ [Lighter] 解析订单失败: {e}")

        return orders

    def _parse_order_any(
        self,
        order_info: Dict[str, Any],
        *,
        symbol_hint: Optional[str] = None,
    ) -> Optional[OrderData]:
        """
        统一订单解析入口（避免 _parse_order_from_ws / _parse_order_from_direct_ws / _parse_order 三套逻辑长期分叉）。

        - account_all_orders / account_all 的 orders：通常是“完整 JSON”（包含 market_index / order_index）
        - 部分历史路径/文档：可能是“缩写字段”（包含 i/u/is/rs/p/ia/st）
        """
        if not isinstance(order_info, dict):
            return None

        # 优先判断“完整 JSON”结构
        if "market_index" in order_info or "order_index" in order_info or "client_order_index" in order_info:
            return self._parse_order_from_direct_ws(order_info)

        # 再判断“缩写字段”结构
        if "i" in order_info or "u" in order_info or "st" in order_info:
            return self._parse_order_from_ws(order_info)

        # 无法识别：返回 None（调用方决定是否打印原始数据）
        _ = symbol_hint  # 预留：未来可用于兜底 symbol
        return None

    def _parse_order_from_ws(self, order_info: Dict[str, Any]) -> Optional[OrderData]:
        """
        【历史兼容】解析“缩写字段”订单结构（不建议新代码使用）

        说明：
        - 该方法用于兼容早期/文档中的缩写字段（i/u/is/rs/p/ia/st）。
        - 当前主流程的 orders 推送通常是“完整 JSON”，请走 `_parse_order_any()`，
          由 `_parse_order_from_direct_ws()` 解析。

        解析WebSocket推送的Order JSON

        ⚠️ 根据Lighter官方Go结构文档，实际字段名是缩写形式：
        - "i":  OrderIndex (int64) - 订单ID
        - "u":  ClientOrderIndex (int64) - 客户端订单ID  
        - "is": InitialBaseAmount (int64) - 初始数量（单位1e5）
        - "rs": RemainingBaseAmount (int64) - 剩余数量（单位1e5）
        - "p":  Price (uint32) - 价格（需要除以price_multiplier）
        - "ia": IsAsk (uint8) - 是否卖单 (0=buy, 1=sell)
        - "st": Status (uint8) - 状态码 (0=Failed, 1=Pending, 2=Executed, 3=Pending-Final)
        """
        try:
            # 🔥 使用实际的缩写字段名
            order_index = order_info.get("i")  # OrderIndex
            client_order_index = order_info.get("u")  # ClientOrderIndex

            if order_index is None:
                logger.warning(
                    "⚠️ 订单数据缺少OrderIndex(i): keys=%s",
                    list(order_info.keys()),
                )
                return None

            order_id = str(order_index)

            # 获取市场索引和符号（字段名可能是 "m"）
            market_index = order_info.get("m")
            symbol = self._get_symbol_from_market_index(market_index) if market_index is not None else "UNKNOWN"

            # 动态获取精度
            market_info = self._markets_cache.get(market_index, {}) if market_index is not None else {}
            price_decimals = market_info.get("price_decimals", 1)
            price_multiplier = Decimal(10 ** int(price_decimals))
            quantity_multiplier = Decimal(10 ** (6 - int(price_decimals)))

            initial_amount_raw = order_info.get("is", 0)
            remaining_amount_raw = order_info.get("rs", 0)
            initial_amount = self._safe_decimal(initial_amount_raw) / quantity_multiplier
            remaining_amount = self._safe_decimal(remaining_amount_raw) / quantity_multiplier
            filled_amount = initial_amount - remaining_amount
            if filled_amount < 0:
                filled_amount = Decimal("0")

            price_raw = order_info.get("p", 0)
            price = self._safe_decimal(price_raw) / price_multiplier
            average_price = price if filled_amount > 0 and price > 0 else None

            is_ask = bool(order_info.get("ia", 0))
            side = OrderSide.SELL if is_ask else OrderSide.BUY

            status_code = int(order_info.get("st", 1) or 1)
            if status_code == 2:
                status = OrderStatus.FILLED
            elif status_code == 0:
                status = OrderStatus.CANCELED
            elif status_code in (1, 3):
                status = OrderStatus.OPEN
            else:
                status = OrderStatus.PENDING

            return OrderData(
                id=order_id,
                client_id=str(client_order_index) if client_order_index is not None else "",
                symbol=symbol,
                side=side,
                type=OrderType.LIMIT,
                amount=initial_amount,
                filled=filled_amount,
                remaining=remaining_amount,
                price=price,
                average=average_price,
                cost=Decimal("0"),
                status=status,
                timestamp=datetime.now(),
                updated=None,
                fee=None,
                trades=[],
                params={},
                raw_data=order_info,
            )
        except Exception as e:
            logger.error("❌ [Lighter] 解析WebSocket订单失败: %s", e, exc_info=True)
            return None

    def _parse_order(self, order_info: Dict[str, Any], symbol: str) -> OrderData:
        """
        【历史兼容】解析单个订单（不建议新代码使用）

        说明：
        - 新代码请使用 `_parse_order_any()`（它会自动识别订单结构并分发到正确解析器）。
        - 本方法保留是为了兼容旧调用路径；内部已转为调用 `_parse_order_any()`。
        """
        # 🔥 兼容旧路径：统一走 _parse_order_any
        parsed = self._parse_order_any(order_info, symbol_hint=symbol)
        if parsed:
            return parsed

        # 兜底：不识别时返回一个最小可用的 OPEN 订单对象，避免上层直接崩溃
        return OrderData(
            id=str(order_info.get("order_index", "") or ""),
            client_id=str(order_info.get("client_order_index", "") or ""),
            symbol=symbol,
            side=OrderSide.BUY,
            type=OrderType.LIMIT,
            amount=Decimal("0"),
            price=None,
            filled=Decimal("0"),
            remaining=Decimal("0"),
            cost=Decimal("0"),
            average=None,
            status=OrderStatus.OPEN,
            timestamp=datetime.now(),
            updated=None,
            fee=None,
            trades=[],
            params={"lighter_status": str(order_info.get("status", "unknown") or "unknown")},
            raw_data=order_info,
        )

    def _parse_trade_as_order(self, trade_info: Dict[str, Any]) -> Optional[OrderData]:
        """
        将trade数据解析为OrderData（用于WebSocket订单成交通知）

        Lighter WebSocket中，交易成交数据在'trades'键中

        Trade JSON格式（根据文档）:
        {
            "trade_id": INTEGER,
            "tx_hash": STRING,
            "market_id": INTEGER,
            "size": STRING,
            "price": STRING,
            "ask_id": INTEGER,        # 卖单订单ID
            "bid_id": INTEGER,        # 买单订单ID
            "ask_account_id": INTEGER, # 卖方账户ID
            "bid_account_id": INTEGER, # 买方账户ID
            "is_maker_ask": BOOLEAN   # maker是卖方(true)还是买方(false)
        }
        """
        try:
            # 🔥 获取市场ID
            market_id = trade_info.get("market_id")
            if market_id is None:
                logger.warning(f"交易数据缺少market_id: {trade_info}")
                return None

            symbol = self._get_symbol_from_market_index(market_id)

            # 🔥 判断当前账户是买方还是卖方
            ask_account_id = trade_info.get("ask_account_id")
            bid_account_id = trade_info.get("bid_account_id")

            # 根据账户ID判断是买还是卖
            is_sell = (ask_account_id == self.account_index)
            is_buy = (bid_account_id == self.account_index)

            if not (is_sell or is_buy):
                # 这个trade不属于当前账户
                return None

            # 🔥 获取正确的订单ID（ask_id或bid_id）
            order_id = trade_info.get(
                "ask_id") if is_sell else trade_info.get("bid_id")
            if order_id is None:
                logger.warning(f"交易数据缺少订单ID: {trade_info}")
                return None

            # 🔥 获取正确的 client_id（ask_client_id或bid_client_id）
            client_id = trade_info.get(
                "ask_client_id") if is_sell else trade_info.get("bid_client_id")
            logger.debug(
                f"🔍 [Lighter] 从trade提取: order_id={order_id}, client_id={client_id}, "
                f"方向={'卖' if is_sell else '买'}"
            )
            
            # 解析交易数量和价格
            size_str = trade_info.get("size", "0")
            price_str = trade_info.get("price", "0")
            usd_amount_str = trade_info.get("usd_amount", "0")

            base_amount = self._safe_decimal(size_str)
            trade_price = self._safe_decimal(price_str)
            usd_amount = self._safe_decimal(usd_amount_str)

            # 🔥 构造OrderData
            from ..models import OrderSide, OrderType, OrderStatus

            order_data = OrderData(
                id=str(order_id),  # ✅ 使用ask_id或bid_id作为订单ID
                client_id=str(client_id) if client_id else "",  # ✅ 使用ask_client_id或bid_client_id
                symbol=symbol,
                side=OrderSide.SELL if is_sell else OrderSide.BUY,
                type=OrderType.LIMIT,  # Trade可能来自限价单
                amount=base_amount,
                price=trade_price,
                filled=base_amount,  # 交易全部成交
                remaining=Decimal("0"),  # 已全部成交
                cost=usd_amount,  # 成交金额
                average=trade_price,  # 成交价
                status=OrderStatus.FILLED,  # 已成交
                timestamp=self._parse_timestamp(trade_info.get("timestamp")),
                updated=self._parse_timestamp(trade_info.get("timestamp")),
                fee=None,
                trades=[],
                params={},
                raw_data=trade_info
            )

            return order_data

        except Exception as e:
            logger.error(f"❌ [Lighter] 解析交易数据失败: {e}", exc_info=True)
            return None

    def _parse_positions(self, positions_data: Dict[str, Any]) -> List[PositionData]:
        """解析持仓列表"""
        # datetime已在文件顶部导入
        from ..models import PositionSide, MarginMode

        positions = []
        for market_index_str, position_info in positions_data.items():
            try:
                market_index = int(market_index_str)
                symbol = self._get_symbol_from_market_index(market_index)

                position_size = self._safe_decimal(
                    position_info.get("position", 0))
                if position_size == 0:
                    continue

                # 🔥 Lighter持仓方向：优先使用 sign 字段（1=Long, -1=Short）
                sign_value = position_info.get("sign")
                position_side = None
                if sign_value is not None:
                    try:
                        sign_int = int(sign_value)
                        position_side = PositionSide.LONG if sign_int >= 0 else PositionSide.SHORT
                    except (ValueError, TypeError):
                        position_side = None

                # 🔥 回退：如果sign缺失或解析失败，使用position字段符号
                if position_side is None:
                    position_side = PositionSide.LONG if position_size > 0 else PositionSide.SHORT

                positions.append(PositionData(
                    symbol=symbol,
                    side=position_side,
                    size=abs(position_size),
                    entry_price=self._safe_decimal(
                        position_info.get("avg_entry_price", 0)),
                    mark_price=None,
                    current_price=None,
                    unrealized_pnl=self._safe_decimal(
                        position_info.get("unrealized_pnl", 0)),
                    realized_pnl=self._safe_decimal(
                        position_info.get("realized_pnl", 0)),
                    percentage=None,
                    leverage=1,  # Lighter杠杆为1
                    margin_mode=MarginMode.CROSS,  # Lighter使用全仓模式
                    margin=Decimal("0"),  # WebSocket不提供保证金信息
                    liquidation_price=self._safe_decimal(
                        position_info.get("liquidation_price")),
                    timestamp=datetime.now(),
                    raw_data=position_info
                ))
            except Exception as e:
                logger.error(f"❌ [Lighter] 解析持仓失败: {e}")
                import traceback
                logger.error(traceback.format_exc())

        return positions

    def _refresh_position_cache_from_positions(self, positions_data: Dict[str, Any]) -> List[PositionData]:
        """
        使用WebSocket推送的positions数据刷新本地缓存，并返回 PositionData 列表
        """
        if not positions_data:
            return []

        positions = self._parse_positions(positions_data)

        if not hasattr(self, '_position_cache'):
            self._position_cache = {}

        # 先根据本次推送清理数量为0的市场
        for market_index_str, position_info in positions_data.items():
            try:
                market_index = int(market_index_str)
            except (TypeError, ValueError):
                logger.warning(f"⚠️ [WS] 无法解析持仓market_index: {market_index_str}")
                continue

            symbol = self._get_symbol_from_market_index(market_index)
            if not symbol:
                continue

            raw_position = position_info.get("position", 0)
            try:
                position_value = Decimal(str(raw_position))
            except (ArithmeticError, ValueError, TypeError):
                position_value = Decimal('0')

            if position_value == 0 and symbol in self._position_cache:
                logger.info(f"📡 [WS] 持仓已平仓，缓存清空: {symbol}")
                self._position_cache.pop(symbol, None)

        if not positions:
            return []

        from ..models import PositionSide

        for position in positions:
            signed_size = position.size if position.side == PositionSide.LONG else -position.size
            previous = self._position_cache.get(position.symbol)
            self._position_cache[position.symbol] = {
                'symbol': position.symbol,
                'size': signed_size,
                'side': position.side.value,
                'entry_price': position.entry_price,
                'unrealized_pnl': position.unrealized_pnl or Decimal('0'),
                'timestamp': datetime.now(),
            }

            if not previous or previous.get('size') != signed_size:
                logger.info(
                    f"💰 [Lighter持仓] 更新: {position.symbol} "
                    f"side={position.side.value}, size={signed_size}, "
                    f"entry_price={position.entry_price}, "
                    f"unrealized_pnl={position.unrealized_pnl or Decimal('0')}"
                )

        return positions

    def _get_symbol_from_market_index(self, market_index: int) -> str:
        """从市场索引获取符号"""
        market_info = self._markets_cache.get(market_index)
        if market_info:
            return market_info.get("symbol", "")
        return f"MARKET_{market_index}"

    def _adjust_position_cache_from_order(self, order: OrderData) -> None:
        """根据订单推送更新持仓缓存（补偿account_all延迟）"""
        try:
            if not hasattr(self, '_position_cache'):
                self._position_cache = {}
            
            symbol = order.symbol
            if not symbol:
                return
            
            filled_amount = order.filled or Decimal('0')
            if filled_amount == 0:
                return
            
            delta = filled_amount if order.side == OrderSide.BUY else -filled_amount
            current_entry = self._position_cache.get(symbol)
            current_size = Decimal(str(current_entry.get('size', 0))) if current_entry else Decimal('0')
            new_size = current_size + delta
            
            tolerance = Decimal('0.0000005')
            if abs(new_size) <= tolerance:
                if symbol in self._position_cache:
                    self._position_cache.pop(symbol, None)
                    logger.info(f"📡 [WS] 订单推送表明持仓已清零: {symbol}")
                return
            
            entry_price = order.price or order.average or current_entry.get('entry_price') if current_entry else order.price
            cache_entry = {
                'symbol': symbol,
                'size': new_size,
                'side': 'long' if new_size > 0 else 'short',
                'entry_price': entry_price or Decimal('0'),
                'unrealized_pnl': Decimal('0'),
                'timestamp': datetime.now(),
            }
            self._position_cache[symbol] = cache_entry
            logger.debug(f"📡 [WS] 订单推送更新持仓: {symbol} size={new_size}")
        except Exception as exc:
            logger.warning(f"⚠️ [WS] 根据订单更新持仓缓存失败: {exc}")

    # ============= 回调触发 =============

    def _trigger_ticker_callbacks(self, ticker: TickerData):
        """触发ticker回调（线程安全）"""
        for callback in self._ticker_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    # 🔥 WebSocket在同步线程中运行，需要线程安全地调度协程
                    if self._event_loop and self._event_loop.is_running():
                        asyncio.run_coroutine_threadsafe(
                            callback(ticker), self._event_loop)
                    else:
                        logger.debug("⚠️ 事件循环未运行，跳过ticker回调")
                else:
                    callback(ticker)
            except Exception as e:
                logger.error(f"❌ [Lighter] ticker回调执行失败: {e}", exc_info=True)

    def _trigger_orderbook_callbacks(self, orderbook: OrderBookData):
        """触发订单簿回调（线程安全）"""
        if not self._orderbook_callbacks:
            if not self._empty_callbacks_warning_logged:
                logger.warning(f"⚠️ [Lighter] 订单簿回调列表为空，无法触发回调！symbol={orderbook.symbol if orderbook else 'N/A'}")
                self._empty_callbacks_warning_logged = True
            return
        
        for callback in self._orderbook_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    # 🔥 WebSocket在同步线程中运行，需要线程安全地调度协程
                    if self._event_loop and self._event_loop.is_running():
                        asyncio.run_coroutine_threadsafe(
                            callback(orderbook), self._event_loop)
                    else:
                        logger.warning(f"⚠️ [Lighter] 事件循环未运行，跳过订单簿回调: symbol={orderbook.symbol if orderbook else 'N/A'}")
                else:
                    callback(orderbook)
            except Exception as e:
                logger.error(f"❌ [Lighter] 订单簿回调执行失败: {e}", exc_info=True)

        # 🔥 如果存在ticker回调，但market_stats可能未推送价格，则用盘口中间价补充价格回调
        if self._ticker_callbacks and orderbook and orderbook.best_bid and orderbook.best_ask:
            try:
                mid_price = (Decimal(str(orderbook.best_bid.price)) +
                             Decimal(str(orderbook.best_ask.price))) / Decimal("2")
                ticker = TickerData(
                    symbol=orderbook.symbol,
                    timestamp=datetime.now(),
                    last=mid_price,
                    bid=Decimal(str(orderbook.best_bid.price)),
                    ask=Decimal(str(orderbook.best_ask.price)),
                )
                self._trigger_ticker_callbacks(ticker)
            except Exception as e:
                logger.debug(f"order_book→ticker 价格回调失败: {e}")

    def _trigger_order_callbacks(self, order: OrderData):
        """触发订单回调（线程安全，带错误捕获）"""
        for callback in self._order_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    # 🔥 WebSocket在同步线程中运行，需要线程安全地调度协程
                    if self._event_loop and self._event_loop.is_running():
                        future = asyncio.run_coroutine_threadsafe(
                            callback(order), self._event_loop)

                        # 🔥 添加完成回调来捕获异常
                        def log_error(fut):
                            try:
                                fut.result()  # 获取结果，如果有异常会抛出
                            except Exception as e:
                                logger.error(
                                    f"❌ 订单回调执行出错: order_id={order.id}, "
                                    f"side={order.side}, status={order.status}, "
                                    f"error={e}",
                                    exc_info=True
                                )

                        future.add_done_callback(log_error)
                    else:
                        logger.warning("⚠️ 事件循环未运行，跳过订单回调")
                else:
                    callback(order)
            except Exception as e:
                logger.error(f"❌ [Lighter] 订单回调执行失败: {e}", exc_info=True)

    def _trigger_order_fill_callbacks(self, order: OrderData):
        """触发订单成交回调（线程安全，带错误捕获）"""
        for callback in self._order_fill_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    # 🔥 WebSocket在同步线程中运行，需要线程安全地调度协程
                    if self._event_loop and self._event_loop.is_running():
                        future = asyncio.run_coroutine_threadsafe(
                            callback(order), self._event_loop)

                        # 🔥 添加完成回调来捕获异常
                        def log_error(fut):
                            try:
                                fut.result()
                            except Exception as e:
                                logger.error(
                                    f"❌ 订单成交回调执行出错: order_id={order.id}, "
                                    f"error={e}",
                                    exc_info=True
                                )

                        future.add_done_callback(log_error)
                    else:
                        logger.warning("⚠️ 事件循环未运行，无法调度异步回调")
                else:
                    callback(order)
            except Exception as e:
                logger.error(f"❌ [Lighter] 订单成交回调执行失败: {e}", exc_info=True)

    def _build_order_update_key(self, order: OrderData) -> Optional[str]:
        """构建订单推送去重键"""
        if order is None:
            return None
        order_id = getattr(order, "id", None)
        client_id = getattr(order, "client_id", None)
        filled = getattr(order, "filled", None)
        if order_id is None and client_id is None:
            return None
        return f"{order_id or ''}:{client_id or ''}:{filled or '0'}"

    def _should_skip_order_update(self, order: OrderData) -> bool:
        """基于 (order_id, client_id, filled) 去重，避免重复推送"""
        dedup_key = self._build_order_update_key(order)
        if dedup_key is None:
            return False
        if dedup_key in self._processed_order_update_set:
            logger.debug("⚠️ [Lighter] 跳过重复订单推送: key=%s", dedup_key)
            return True

        self._processed_order_update_keys.append(dedup_key)
        self._processed_order_update_set.add(dedup_key)

        if len(self._processed_order_update_keys) > self._processed_order_update_keys.maxlen:
            old_key = self._processed_order_update_keys.popleft()
            self._processed_order_update_set.discard(old_key)

        return False

    def _trigger_position_callbacks(self, position: PositionData):
        """触发持仓回调（线程安全）"""
        for callback in self._position_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    # 🔥 WebSocket在同步线程中运行，需要线程安全地调度协程
                    if self._event_loop and self._event_loop.is_running():
                        asyncio.run_coroutine_threadsafe(
                            callback(position), self._event_loop)
                    else:
                        logger.debug("⚠️ 事件循环未运行，跳过持仓回调")
                else:
                    callback(position)
            except Exception as e:
                logger.error(f"❌ [Lighter] 持仓回调执行失败: {e}")

    # ============= 直接订阅account_all_orders =============

    async def _subscribe_account_all_orders(self):
        """
        直接订阅account_all_orders频道

        根据Lighter WebSocket文档，订阅account_all_orders需要：
        1. 建立WebSocket连接到 wss://mainnet.zklighter.elliot.ai/stream
        2. 发送订阅消息，包含auth token
        3. 接收订单推送（包括挂单状态）
        """
        self._private_subscription_requested = True
        if not WEBSOCKETS_AVAILABLE:
            logger.warning("⚠️ websockets库未安装，无法直接订阅订单")
            return

        if self._direct_ws_task and not self._direct_ws_task.done():
            logger.debug("⚠️ [Lighter] 直接订阅任务已在运行")
            return

        # 启动直接订阅任务
        self._direct_ws_task = asyncio.create_task(
            self._run_direct_ws_subscription())
        logger.debug("🚀 [Lighter] 已启动直接订阅account_all_orders任务")

    async def _check_exchange_connectivity(self) -> bool:
        """检查Lighter交易所服务器连通性"""
        try:
            import aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    "https://mainnet.zklighter.elliot.ai/",
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as response:
                    return response.status == 200
        except Exception:
            return False

    async def _create_auth_token(self) -> Optional[str]:
        """根据配置生成认证Token（公共模式下返回None）"""
        if not (self.auth_enabled and self.account_index is not None and self.signer_client):
            return None

        try:
            token, err = self.signer_client.create_auth_token_with_expiry(
                lighter.SignerClient.DEFAULT_10_MIN_AUTH_EXPIRY
            )
            if err:
                logger.error(f"❌ [Lighter] 生成认证token失败: {err}")
                return None
            logger.info("🔑 [Lighter] 认证token生成成功")
            return token
        except Exception as exc:
            logger.error(f"❌ [Lighter] 生成认证token异常: {exc}", exc_info=True)
            return None

    async def _run_direct_ws_subscription(self):
        """运行直接WebSocket订阅（贴近 test_sol_orderbook.py 的流程）"""
        logger.info(f"🔧 _run_direct_ws_subscription 启动 (_running={self._running})")
        retry_delay = 5

        while self._running:
            if self.config.get('official_transport_only'):
                from ..official_transport import validate_lighter_endpoints
                validate_lighter_endpoints(self.base_url,self.ws_url)
            auth_token = await self._create_auth_token()

            try:
                # 🔥 SSL 配置：从环境变量读取
                ssl_context = None
                verify_ssl = os.getenv('LIGHTER_VERIFY_SSL', 'true').lower() == 'true'
                if not verify_ssl:
                    ssl_context = ssl.create_default_context()
                    ssl_context.check_hostname = False
                    ssl_context.verify_mode = ssl.CERT_NONE
                elif self.ws_url.startswith('wss://'):
                    # 🔥 修复：wss:// 协议必须提供 ssl context，不能为 None
                    # 使用默认上下文（会加载系统/certifi证书）
                    ssl_context = ssl.create_default_context()
                
                from ..official_transport import websocket_connect,ReceivedConnection
                options={}
                if self.config.get('official_transport_only'):
                    generation=getattr(self,'generation',0)+1
                    def received(_generation=generation):
                        if (getattr(self,'_reader_connected',False) and self._reader_generation==_generation
                                and self._reader_socket is self._direct_ws):self.record_received()
                    seconds=getattr(self,'liveness_seconds',5)
                    options=dict(ping_interval=seconds/2,ping_timeout=seconds+getattr(self,'quick_resume_seconds',10),
                        create_connection=lambda *args,_received=received,**kwargs:ReceivedConnection(*args,on_received=_received,**kwargs))
                async with websocket_connect(
                    self.ws_url,
                    proxy=None,
                    close_timeout=10,
                    ssl=ssl_context,
                    **({'ping_interval':None,'ping_timeout':None}|options),
                ) as ws:
                    self._direct_ws = ws
                    self.generation = getattr(self, 'generation', 0) + 1
                    self._reader_socket=ws
                    self._reader_generation=self.generation
                    self._reader_connected=True
                    self.last_received_monotonic=time.monotonic()
                    self._private_ready_socket = None
                    self._private_ready_generation = None
                    self._ws_manual_health_ping_sent = False
                    self._connection_start_time = time.time()
                    self._last_message_time = self._connection_start_time
                    self._last_business_message_time = self._connection_start_time
                    self._network_bytes_received = 0
                    self._network_bytes_sent = 0
                    self._heartbeat_task = asyncio.create_task(self._heartbeat_loop(ws))
                    logger.info("✅ [Lighter] WebSocket已连接（直接订阅模式）")

                    # 启动数据超时检测
                    if self._data_timeout_task and not self._data_timeout_task.done():
                        self._data_timeout_task.cancel()
                    self._data_timeout_task = asyncio.create_task(self._data_timeout_monitor())
                    logger.info(
                        f"⏱️  [Lighter] 数据超时检测任务已启动 (阈值={self._data_timeout_seconds}s)"
                    )

                    # 先订阅market_stats / order_book，再订阅账户频道
                    await self._send_market_stats_subscriptions()
                    await self._send_orderbook_subscriptions()
                    if auth_token:
                        await self._send_account_subscriptions(auth_token)
                    else:
                        logger.info("ℹ️  [Lighter] 公共模式运行，仅订阅行情频道")

                    await self._listen_websocket(ws)

            except asyncio.CancelledError:
                self._invalidate_private_socket(self._direct_ws)
                logger.info("🛑 [Lighter] WebSocket订阅任务取消")
                break
            except Exception as exc:
                self._invalidate_private_socket(self._direct_ws)
                logger.error(f"❌ [Lighter] WebSocket运行异常: {exc}", exc_info=True)
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 30)
            finally:
                await self._cleanup_old_connection()

        logger.info("🛑 WebSocket订阅任务已停止")
    
    async def _cleanup_old_connection(self):
        """
        🔥 清理旧连接：确保完全退出并清理所有旧连接资源
        
        在重连前调用此方法，确保：
        1. 关闭旧的WebSocket连接
        2. 取消所有相关任务
        3. 清理连接状态
        """
        self._invalidate_private_socket(self._direct_ws)
        try:
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                logger.warning("⚠️ [Lighter重连] 当前无活动事件循环，跳过异步清理")
                return

            logger.info("🧹 [Lighter重连] 开始清理旧连接...")
            
            # 1. 取消数据超时检测任务
            if self._data_timeout_task and not self._data_timeout_task.done():
                self._data_timeout_task.cancel()
                try:
                    await self._data_timeout_task
                except asyncio.CancelledError:
                    pass
                logger.debug("✅ [Lighter重连] 数据超时检测任务已取消")
            
            # 2. 取消心跳任务（如果存在）
            if self._heartbeat_task and not self._heartbeat_task.done():
                self._heartbeat_task.cancel()
                try:
                    await self._heartbeat_task
                except asyncio.CancelledError:
                    pass
                logger.debug("✅ [Lighter重连] 心跳任务已取消")
            
            # 3. 关闭旧的WebSocket连接
            # 🔥 关键修复：确保完全关闭WebSocket连接，包括发送close frame
            # 重启时连接自然关闭，重连时需要显式关闭并等待服务器确认
            if self._direct_ws:
                try:
                    # 🔥 修复：直接尝试关闭，不检查closed属性（websockets库没有这个属性）
                    await self._direct_ws.close(code=1000, reason="Reconnecting")
                    logger.debug("✅ [Lighter重连] 旧WebSocket连接已关闭（已发送close frame）")
                except Exception as close_error:
                    logger.warning(f"⚠️ [Lighter重连] 关闭旧连接时出错: {close_error}")
                finally:
                    # 🔥 关键：等待一小段时间，确保服务器端收到close frame并清理状态
                    # 这可能是重启和重连的关键区别：重启时服务器有足够时间清理状态
                    await asyncio.sleep(0.5)  # 等待500ms，让服务器端处理close frame
                    
                    # 确保引用被清空
                    self._direct_ws = None
                    logger.debug("✅ [Lighter重连] WebSocket引用已清空")
            
            # 4. 重置连接状态
            self._connection_start_time = 0
            self._last_message_time = 0
            self._last_business_message_time = 0
            
            # 🔥 关键修复：清理订阅列表，避免服务器端状态不一致
            # 重启时这些列表是空的，重连时也应该清空，让服务器重新建立订阅状态
            # 这可能是导致重连后服务器不发送数据的关键原因
            old_market_stats_count = len(self._subscribed_market_stats)
            old_markets_count = len(self._subscribed_markets)
            
            # 🔥 不清空订阅列表，而是保留它们，因为重连后需要重新订阅
            # 但我们需要确保服务器端知道这是新连接，所以会在重连时重新发送订阅请求
            # 实际上，订阅列表应该保留，因为重连后需要重新订阅相同的市场
            
            # 🔥 但是，我们需要清理本地订单簿缓存，避免使用旧数据
            self._local_orderbooks.clear()
            logger.debug(f"✅ [Lighter重连] 已清理本地订单簿缓存")
            
            logger.info(
                f"✅ [Lighter重连] 旧连接清理完成 "
                f"(保留订阅: {old_market_stats_count}个market_stats, {old_markets_count}个markets)"
            )
            
        except Exception as e:
            logger.error(f"❌ [Lighter重连] 清理旧连接时出错: {e}", exc_info=True)
    
    async def _heartbeat_loop(self, socket=None):
        """
        🚀 主动心跳任务：定期发送 pong 保持连接活跃
        
        参考测试脚本 test_sol_orderbook.py 的稳定连接方案：
        - 首次等待30秒后发送pong
        - 然后每30秒主动发送一次pong
        - 确保即使在低活跃期也能保持连接
        - 🔥 增强：检查连接状态和数据活跃度
        """
        try:
            await asyncio.sleep(30)  # 首次等待 30 秒（提前发送）
            
            socket = socket or self._direct_ws
            while self._running and socket is self._direct_ws:
                try:
                    if self._direct_ws:
                        # 🔥 修复：发送心跳前更新最后活动时间（关键修复）
                        current_time = time.time()
                        # 先检查数据活跃度（在更新前）
                        silence_time_before = current_time - self._last_message_time if self._last_message_time > 0 else 0
                        
                        if silence_time_before > 60:
                            logger.warning(
                                f"⚠️  [主动心跳] 数据静默时间: {silence_time_before:.1f}秒 "
                                f"(最后消息: {self._last_message_time:.1f})"
                            )
                        
                        # 主动发送 pong
                        try:
                            pong_msg = json.dumps({"type": "pong"})
                            await socket.send(pong_msg)
                            self._network_bytes_sent += len(pong_msg.encode('utf-8'))
                            # 🔥 INFO级别，但控制频率（每30秒记录一次）
                            current_time = time.time()
                            if current_time - self._last_heartbeat_log_time >= self._heartbeat_log_interval:
                                logger.info(f"[Lighter] 心跳: 发送pong (静默时间: {silence_time_before:.1f}s)")
                                self._last_heartbeat_log_time = current_time
                        except Exception as send_error:
                            # 🔥 心跳发送失败，记录错误
                            logger.error(f"❌ [主动心跳] 发送 pong 失败: {send_error}")
                            raise  # 重新抛出异常，让外层处理
                    else:
                        logger.debug("⚠️  [主动心跳] WebSocket 已关闭，停止心跳")
                        break
                        
                except Exception as e:
                    logger.warning(f"❌ [主动心跳] 发送失败: {e}")
                    break
                
                # 每 30 秒发送一次（更激进，确保不超时）
                await asyncio.sleep(30)
                
        except asyncio.CancelledError:
            logger.debug("💔 主动心跳任务已取消")
        except Exception as e:
            logger.error(f"❌ 主动心跳任务异常: {e}")
    
    def _parse_orderbook_timestamp(self, order_book: Dict[str, Any]) -> Optional[datetime]:
        for field in ("timestamp", "last_updated_at"):
            parsed = self._parse_timestamp_value(order_book.get(field))
            if parsed is not None:
                return parsed
        return None

    def _parse_timestamp_value(self, value: Any) -> Optional[datetime]:
        """
        将Lighter原始时间戳（秒/毫秒/微秒/纳秒/字符串）转换为datetime
        """
        if value is None:
            return None
        try:
            if isinstance(value, str):
                value = value.strip()
                if not value:
                    return None
                value = float(value)
            elif isinstance(value, Decimal):
                value = float(value)

            if isinstance(value, (int, float)):
                if not math.isfinite(value) or value <= 0:
                    return None
                if value >= 1e17:     # 纳秒
                    value = value / 1e9
                elif value >= 1e14:   # 微秒
                    value = value / 1e6
                elif value >= 1e11:   # 毫秒
                    value = value / 1e3
                return datetime.fromtimestamp(value)
        except Exception:
            return None

        return None

    async def _data_timeout_monitor(self):
        """
        🔥 数据超时检测任务：监控数据接收情况，超时后主动重连
        
        参考 test_sol_orderbook.py 的实现：
        - 每30秒检查一次最后消息时间
        - 如果超过阈值（60秒）没有收到任何消息，主动断开并触发重连
        - 这样可以及时发现静默断开的情况
        """
        try:
            while self._running:
                try:
                    await asyncio.sleep(30)
                    
                    if not self._running:
                        break
                    
                    if not self._direct_ws:
                        logger.info("⏱️  [数据超时检测] WebSocket已关闭，停止监控")
                        break
                    
                    current_time = time.time()
                    if self._last_business_message_time > 0:
                        silence_time = current_time - self._last_business_message_time
                    elif self._last_message_time > 0:
                        silence_time = current_time - self._last_message_time
                    else:
                        silence_time = 0
                    
                    timeout_threshold = max(30.0, self._data_timeout_seconds)
                    ping_threshold = min(self._ws_manual_ping_threshold, timeout_threshold - 5.0)
                    if ping_threshold <= 0:
                        ping_threshold = max(timeout_threshold / 2, 5.0)
                    
                    if silence_time >= timeout_threshold:
                        logger.error(
                            f"❌ [数据超时检测] 静默 {silence_time:.1f}秒 > 阈值 {timeout_threshold:.0f}秒，"
                            " 主动断开连接并重连..."
                        )
                        try:
                            if self._direct_ws:
                                self._invalidate_private_socket(self._direct_ws)
                                await self._direct_ws.close()
                                logger.info("🔌 [数据超时检测] 已主动关闭WebSocket连接")
                        except Exception as e:
                            logger.warning(f"⚠️  [数据超时检测] 关闭连接时出错: {e}")
                        
                        self._last_message_time = 0
                        self._last_business_message_time = 0
                        self._ws_manual_health_ping_sent = False
                        continue
                    
                    if silence_time >= ping_threshold:
                        if not self._ws_manual_health_ping_sent:
                            try:
                                if self._direct_ws:
                                    await self._direct_ws.send(json.dumps({"type": "ping"}))
                                    self._ws_manual_health_ping_sent = True
                                    logger.warning(
                                        "⚠️ [数据超时检测] 静默超时，已发送保活 ping，等待响应..."
                                    )
                                    continue
                            except Exception as ping_error:
                                logger.warning(
                                    f"⚠️ [数据超时检测] 保活 ping 发送失败: {ping_error}"
                                )
                                self._ws_manual_health_ping_sent = False
                        else:
                            logger.warning(
                                f"⚠️ [数据超时检测] 静默 {silence_time:.1f}秒，持续等待"
                                " 保活 ping 响应..."
                            )
                        continue
                    
                    self._ws_manual_health_ping_sent = False
                    logger.debug(
                        f"✅ [数据超时检测] 数据正常，静默时间: {silence_time:.1f}秒"
                    )
                
                except Exception as e:
                    logger.error(f"❌ [数据超时检测] 检查失败: {e}")
                    await asyncio.sleep(30)
                    
        except asyncio.CancelledError:
            logger.debug("💔 数据超时检测任务已取消")
        except Exception as e:
            logger.error(f"❌ 数据超时检测任务异常: {e}")

    async def _handle_direct_ws_message(self, data: Dict[str, Any]):
        """
        处理直接WebSocket消息

        根据文档，account_all_orders返回：
        {
            "channel": "account_all_orders:{ACCOUNT_ID}",
            "orders": {
                "{MARKET_INDEX}": [Order]
            },
            "type": "update/account_all_orders"
        }
        """
        try:
            msg_type = data.get("type", "")
            normalized_msg_type = (msg_type or "").lower()
            channel = data.get("channel", "")
            private_channel = str(channel).replace(":", "/").split("/")
            verified_account = (bool(self.auth_enabled and self.signer_client)
                and getattr(self, "_authenticated_account_socket", None) is not None
                and self._authenticated_account_socket is self._direct_ws
                and len(private_channel) == 2
                and private_channel[0] == normalized_msg_type.split("/")[-1]
                and private_channel[0] in ("account_all", "account_all_orders")
                and private_channel[1] == str(self.account_index)
                and str(data.get("account", self.account_index)) == str(self.account_index))
            # 🔥 修复：在函数开始处定义channel，确保所有分支都能使用
            self._last_message_time = time.time()
            if not getattr(self,'config',{}).get('official_transport_only'):
                self.record_received()
            if verified_account and str(msg_type).startswith('subscribed/'):
                if (getattr(self,'_reader_connected',False) and self._reader_socket is self._direct_ws
                        and self._reader_generation==self.generation):
                    self._private_ready_socket = self._direct_ws
                    self._private_ready_generation=self.generation
            if normalized_msg_type != "ping":
                self._last_business_message_time = self._last_message_time
                self._ws_manual_health_ping_sent = False
            
            # 🔥 处理应用层心跳 ping/pong（最高优先级！）
            # Lighter协议：服务器发送ping → 客户端必须回复pong
            # 参考测试脚本：简单直接，立即回复
            if normalized_msg_type == "ping":
                # 🔥 更新最后消息时间（ping也算消息）
                self._last_message_time = time.time()
                
                if self._direct_ws:
                    try:
                        # 🔥 立即回复pong，简单直接（参考测试脚本）
                        pong_msg = {"type": "pong"}
                        await self._direct_ws.send(json.dumps(pong_msg))
                        # 🔥 简化日志：每30秒记录一次
                        current_time = time.time()
                        if current_time - self._last_heartbeat_log_time >= self._heartbeat_log_interval:
                            logger.info("[Lighter] 心跳: 收到ping，已回复pong")
                            self._last_heartbeat_log_time = current_time
                    except Exception as e:
                        logger.error(f"❌ [Lighter心跳] 回复pong失败: {e}")
                return  # ping/pong 不需要进一步处理，立即返回

            # 🔥 处理订阅确认消息（subscribed/account_all）- 包含初始余额数据
            if msg_type == "subscribed/account_all":
                # 🔥 订阅确认消息中也包含初始余额数据
                if "stats" in data:
                        self._update_balance_from_stats(
                            data["stats"],
                            source="account_all",
                            is_init=True,
                        )
                
                # 继续处理其他字段（positions, orders等）
                # 注意：这里不return，继续处理positions和orders
            
            # 🔥 处理account_all更新（包含订单、成交、持仓完整数据）
            if msg_type == "update/account_all":
                # 🔥 调试：检查account_all是否包含余额数据
                if "stats" in data:
                    logger.debug(f"📡 [Lighter] account_all消息包含stats字段: {data.get('stats')}")
                
                account_id = str(
                    data.get("account", self.account_index or "unknown"))

                # 🔥 解析持仓更新（datetime已在文件顶部导入）
                if "positions" in data:
                    positions_data = data["positions"]
                    positions = self._refresh_position_cache_from_positions(positions_data)

                    # 触发持仓回调
                    if positions and self._position_callbacks:
                        for position in positions:
                            self._trigger_position_callbacks(position)

                # 🔥 解析成交数据（trades字段，Lighter通过这个推送订单成交）
                # 这是最重要的！Lighter的订单成交通知主要通过trades字段，而不是orders字段
                if "trades" in data and data["trades"]:
                    trades_data = data["trades"]
                    if isinstance(trades_data, dict):
                        for market_index, trade_list in trades_data.items():
                            if isinstance(trade_list, list):
                                # 成交推送是“最终证据”，用户排障需要看到。
                                # 这里仅在存在 trades 时打印一次摘要（trade_id 去重仍然生效）。
                                logger.info(
                                    "💰 [Lighter成交] 市场%s 收到 %s 条成交记录(trades)",
                                    market_index,
                                    len(trade_list),
                                )
                                
                                # 🔥 遍历每个trade，解析为OrderData并触发回调
                                for trade_info in trade_list:
                                    # 🔥 去重检查：防止WebSocket重复推送
                                    trade_id = trade_info.get("trade_id")
                                    if trade_id:
                                        if trade_id in self._processed_trade_ids:
                                            logger.debug(
                                                f"⚠️ 跳过重复的trade: trade_id={trade_id}")
                                            continue
                                        self._processed_trade_ids.append(trade_id)
                                    
                                    # 解析trade为OrderData
                                    order = self._parse_trade_as_order(trade_info)
                                    if order:
                                        # 成交详情：只在 trades 真实到达时打印（不会刷屏，因为 trade_id 去重）
                                        logger.info(
                                            "✅ [订单成交详情] symbol=%s order_id=%s client_id=%s side=%s "
                                            "filled=%s price=%s market=%s",
                                            getattr(order, "symbol", None) or "UNKNOWN",
                                            getattr(order, "id", None),
                                            getattr(order, "client_id", None),
                                            getattr(getattr(order, "side", None), "value", None),
                                            getattr(order, "filled", None),
                                            getattr(order, "average", None) or getattr(order, "price", None),
                                            market_index,
                                        )
                                        
                                        # 🔥 触发订单成交回调（套利系统通过 subscribe_order_fills 注册）
                                        if self._order_fill_callbacks:
                                            for callback in self._order_fill_callbacks:
                                                if asyncio.iscoroutinefunction(callback):
                                                    await callback(order)
                                                else:
                                                    callback(order)

                # 🔥 解析订单更新（如果有）
                if "orders" in data:
                    orders_data = data["orders"]
                    logger.debug(f"📦 [Lighter] account_all包含订单数据: {len(orders_data)} 个市场")
                    await self._process_orders_payload(orders_data, source="account_all", verified_account=verified_account)
                
                # 🔥 处理account_all中的余额数据（如果存在）
                if "stats" in data:
                    self._update_balance_from_stats(
                        data["stats"],
                        source="account_all",
                        is_init=False,
                    )

                return  # account_all已经包含所有数据，不需要继续处理

            # 🔥 处理account_all_positions订阅/更新（专门的持仓推送）
            if msg_type in ("subscribed/account_all_positions", "update/account_all_positions"):
                positions_data = data.get("positions") or {}
                positions = self._refresh_position_cache_from_positions(positions_data)

                if positions and self._position_callbacks:
                    for position in positions:
                        self._trigger_position_callbacks(position)

                if msg_type.startswith("subscribed/"):
                    # 🔥 去重优化：订阅确认降级为 debug
                    logger.debug(f"✅ [Lighter] 订阅成功: account_all_positions/{self.account_index}")
                else:
                    logger.debug(
                        f"📡 [Lighter] account_all_positions推送: {len(positions)} 条持仓更新"
                    )

                return

            # 🔥 处理订阅确认消息（subscribed/user_stats）- 包含初始余额数据
            if msg_type == "subscribed/user_stats":
                # 🔥 订阅确认消息中包含初始余额数据
                if "stats" in data:
                    self._update_balance_from_stats(
                        data["stats"],
                        source="user_stats",
                        is_init=True,
                    )
                
                return  # user_stats订阅确认只包含余额数据
            
            # 🔥 处理user_stats更新（账户余额统计）
            if msg_type == "update/user_stats":
                if "stats" not in data:
                    logger.warning(f"⚠️ [Lighter] user_stats消息缺少stats字段: {data}")
                    return
                
                self._update_balance_from_stats(
                    data["stats"],
                    source="user_stats",
                    is_init=False,
                )

                return

            # 处理订单更新（account_all_orders）
            if msg_type == "update/account_all_orders" and "orders" in data:
                orders_data = data["orders"]
                # 🔥 去重优化：只在 debug 级别记录推送统计，减少日志冗余
                logger.debug(f"📦 [Lighter] account_all_orders订单推送: {len(orders_data)} 个市场")
                await self._process_orders_payload(orders_data, source="account_all_orders", verified_account=verified_account)

            # 处理订阅确认
            elif msg_type.startswith("subscribed/") and msg_type != "subscribed/order_book":
                # 🔥 去重优化：订阅确认降级为 debug，避免日志过多
                logger.debug(f"✅ [Lighter] 订阅成功: {channel or msg_type}")

            # 🔥 处理market_stats更新
            elif msg_type in ("subscribed/market_stats", "update/market_stats") and "market_stats" in data:
                await self._handle_market_stats_update(data["market_stats"])

            # 🔥 处理订单簿更新（参考 test_sol_orderbook.py）
            elif msg_type == "subscribed/order_book":
                # 初始快照
                raw_book = data.get("order_book", {}) or {}
                try:
                    market_index = self._extract_market_index_from_channel(channel)
                    if market_index is not None and any(isinstance(raw_book.get(side), list) for side in ("bids", "asks")):
                        symbol = self._get_symbol_from_market_index(market_index)
                        if symbol:
                            self._initialize_orderbook(market_index, raw_book)
                            order_book_data = self._build_orderbook_from_local(symbol, market_index)
                            self._handle_orderbook_data(
                                symbol=symbol,
                                market_index=market_index,
                                order_book_data=order_book_data,
                                raw_order_book=raw_book,
                                is_snapshot=True,
                            )
                except Exception as e:
                    logger.error(f"❌ [Lighter] 处理订单簿订阅确认失败: {e}", exc_info=True)
            
            elif msg_type == "update/order_book":
                # 增量更新
                raw_book = data.get("order_book", {}) or {}
                try:
                    market_index = self._extract_market_index_from_channel(channel)
                    if market_index in self._local_orderbooks and raw_book:
                        symbol = self._get_symbol_from_market_index(market_index)
                        if symbol:
                            self._apply_orderbook_update(market_index, raw_book)
                            order_book_data = self._build_orderbook_from_local(symbol, market_index)
                            self._handle_orderbook_data(
                                symbol=symbol,
                                market_index=market_index,
                                order_book_data=order_book_data,
                                raw_order_book=raw_book,
                                is_snapshot=False,
                            )
                except Exception as e:
                    logger.error(f"❌ 处理订单簿更新失败: {e}", exc_info=True)

            # 处理未知消息类型
            else:
                logger.warning(
                    f"⚠️ 未处理的消息类型: type={msg_type}, channel={channel}")

        except Exception as e:
            logger.error(f"❌ 处理直接WebSocket消息失败: {e}", exc_info=True)

    async def _handle_market_stats_update(self, market_stats: Dict[str, Any]):
        """
        处理market_stats更新

        market_stats格式:
        {
            "market_id": 1,
            "index_price": "110687.2",
            "mark_price": "110660.1",
            "last_trade_price": "110657.5",
            "open_interest": "308919704.542476",
            "current_funding_rate": "0.0012",
            ...
        }
        """
        try:
            market_id = market_stats.get("market_id")
            if market_id is None:
                logger.debug("⚠️ market_stats缺少market_id字段")
                return

            symbol = self._get_symbol_from_market_index(market_id)
            if not symbol:
                logger.warning(f"⚠️ market_id={market_id} 无法找到对应的symbol")
                return

            # 🔥 提取价格数据
            last_price = self._safe_decimal(
                market_stats.get("last_trade_price", 0))
            
            # 🔥 详细日志：诊断BTC等主流币为什么没有收到价格推送（DEBUG级别，减少日志写入）
            if market_id in [1, 0, 2, 3, 4] or not last_price:  # BTC=1, ETH=0, 其他主流币
                logger.debug(
                    f"🔍 market_stats诊断: market_id={market_id}, symbol={symbol}, "
                    f"last_price={last_price}, "
                    f"has_callbacks={len(self._ticker_callbacks) > 0}"
                )
            
            if not last_price:
                if market_id in [1, 0, 2, 3, 4]:
                    logger.warning(f"⚠️ {symbol}(market_id={market_id}) last_price为0或None")
                return

            # 构造TickerData（使用正确的字段名）
            ticker = TickerData(
                symbol=symbol,
                timestamp=datetime.now(),  # ✅ 必需字段，使用datetime对象
                last=last_price,  # ✅ 最新成交价
                bid=self._safe_decimal(market_stats.get(
                    "mark_price", last_price)),  # 使用mark_price作为bid近似值
                ask=self._safe_decimal(market_stats.get(
                    "index_price", last_price)),  # 使用index_price作为ask近似值
                volume=self._safe_decimal(
                    market_stats.get("daily_base_token_volume", 0)),  # 24小时成交量
                high=self._safe_decimal(
                    market_stats.get("daily_price_high", 0)),  # 24小时最高价
                low=self._safe_decimal(
                    market_stats.get("daily_price_low", 0)),  # 24小时最低价
                # Lighter: current_funding_rate 是百分比形式的1小时费率
                # 需要先÷100转为小数，再×8转为8小时费率
                funding_rate=(
                    self._safe_decimal(market_stats.get(
                        "current_funding_rate")) / 100 * 8
                    if market_stats.get("current_funding_rate") is not None
                    else None
                )  # 资金费率（0.12% → 0.0012 → 0.0096）
            )

            # 🔥 每60秒（1分钟）打印一次价格日志（只输出到文件，不显示在终端）
            current_time = time.time()
            last_log_time = self._last_price_log_time.get(symbol, 0)
            if current_time - last_log_time >= 60:
                self._price_logger.info(
                    f"📊 market_stats更新: {symbol}, 价格={last_price}")
                self._last_price_log_time[symbol] = current_time

            # 触发ticker回调
            if self._ticker_callbacks:
                # 🔥 只在第一次收到数据时打印INFO，后续使用DEBUG
                if symbol not in getattr(self, '_ticker_first_log', set()):
                    if not hasattr(self, '_ticker_first_log'):
                        self._ticker_first_log = set()
                    self._ticker_first_log.add(symbol)
                    logger.info(f"🔔 首次触发ticker回调: {symbol}, 回调数量={len(self._ticker_callbacks)}, funding_rate={ticker.funding_rate}")
                
                for idx, callback in enumerate(self._ticker_callbacks):
                    try:
                        if asyncio.iscoroutinefunction(callback):
                            await callback(ticker)
                        else:
                            callback(ticker)
                    except Exception as cb_err:
                        logger.error(f"❌ [Lighter] ticker回调 #{idx} 执行失败: {cb_err}", exc_info=True)
            else:
                logger.warning(f"⚠️ {symbol} market_stats更新，但没有ticker回调注册（回调列表为空）")

        except Exception as e:
            logger.error(f"❌ 处理market_stats更新失败: {e}", exc_info=True)

    def lookup_cached_order(self, order_id: str, symbol: Optional[str] = None) -> Optional[OrderData]:
        """
        从缓存中查找订单（供REST降级/执行层使用）
        """
        if not order_id:
            return None
        order_cache = getattr(self, "_order_cache", {})
        cached = order_cache.get(str(order_id))
        if cached:
            return cached
        if symbol:
            normalized_symbol = symbol
            order_cache_by_symbol = getattr(self, "_order_cache_by_symbol", {})
            symbol_cache = order_cache_by_symbol.get(normalized_symbol, {})
            return symbol_cache.get(str(order_id))
        return None

    def _update_order_cache(self, order: OrderData) -> None:
        """
        使用最新的订单状态刷新缓存
        """
        if not order:
            return
        
        cache_key = order.id or order.client_id
        if not cache_key:
            return
        
        order_cache: Dict[str, OrderData] = getattr(self, "_order_cache", None)
        if order_cache is None:
            self._order_cache = {}
            order_cache = self._order_cache
        
        order_cache_by_symbol: Dict[str, Dict[str, OrderData]] = getattr(
            self, "_order_cache_by_symbol", None
        )
        if order_cache_by_symbol is None:
            self._order_cache_by_symbol = {}
            order_cache_by_symbol = self._order_cache_by_symbol
        
        symbol = order.symbol or "UNKNOWN"
        symbol_cache = order_cache_by_symbol.setdefault(symbol, {})
        
        if order.status in (OrderStatus.FILLED, OrderStatus.CANCELED):
            order_cache.pop(cache_key, None)
            symbol_cache.pop(cache_key, None)
            if not symbol_cache:
                order_cache_by_symbol.pop(symbol, None)
            return
        
        order_cache[cache_key] = order
        symbol_cache[cache_key] = order
        self._order_cache_ready = True

    def _parse_order_from_direct_ws(self, order_info: Dict[str, Any]) -> Optional[OrderData]:
        """
        解析来自account_all_orders的订单数据

        根据文档，Order JSON格式：
        {
            "order_index": INTEGER,
            "client_order_index": INTEGER,
            "market_index": INTEGER,
            "initial_base_amount": STRING,
            "price": STRING,
            "remaining_base_amount": STRING,
            "filled_base_amount": STRING,
            "filled_quote_amount": STRING,
            "is_ask": BOOL,
            "status": STRING,  # "open", "filled", "canceled"
            ...
        }
        """
        try:
            # 获取市场符号
            market_index = order_info.get("market_index")
            if market_index is None:
                return None

            symbol = self._get_symbol_from_market_index(market_index)

            # 订单ID
            order_index = order_info.get("order_index")
            client_order_index = order_info.get("client_order_index")
            order_id = str(order_index) if order_index is not None else ""

            # 数量和价格
            initial_amount = self._safe_decimal(
                order_info.get("initial_base_amount", "0"))
            remaining_amount = self._safe_decimal(
                order_info.get("remaining_base_amount", "0"))
            filled_amount = self._safe_decimal(
                order_info.get("filled_base_amount", "0"))
            price = self._safe_decimal(order_info.get("price", "0"))

            # 成交金额和均价
            filled_quote = self._safe_decimal(
                order_info.get("filled_quote_amount", "0"))
            average_price = filled_quote / filled_amount if filled_amount > 0 else None

            # 方向
            is_ask = order_info.get("is_ask", False)
            side = OrderSide.SELL if is_ask else OrderSide.BUY

            # 状态
            status_raw = order_info.get("status", "unknown") or "unknown"
            status_key = status_raw.lower()
            if status_key == "filled":
                status = OrderStatus.FILLED
            elif status_key.startswith("canceled") or status_key.startswith("cancelled"):
                status = OrderStatus.CANCELED
            elif status_key == "open":
                status = OrderStatus.OPEN
            else:
                status = OrderStatus.OPEN  # 默认为OPEN

            # 创建OrderData
            params_extra: Dict[str, Any] = {}
            if status_raw:
                params_extra["lighter_status"] = status_raw
            order = OrderData(
                id=order_id,
                client_id=str(
                    client_order_index) if client_order_index is not None else "",
                symbol=symbol,
                side=side,
                type=OrderType.LIMIT,
                amount=initial_amount,
                price=price,
                filled=filled_amount,
                remaining=remaining_amount,
                cost=filled_quote,
                average=average_price,
                status=status,
                timestamp=self._parse_timestamp(order_info.get("timestamp")),
                updated=None,
                fee=None,
                trades=[],
                params=params_extra,
                raw_data=order_info
            )
            self._update_order_cache(order)
            return order

        except Exception as e:
            logger.error(f"解析订单失败: {e}", exc_info=True)
            return None

    def get_cached_orderbook(self, symbol: str) -> Optional[OrderBookData]:
        """获取缓存的订单簿"""
        return self._order_books.get(symbol)
