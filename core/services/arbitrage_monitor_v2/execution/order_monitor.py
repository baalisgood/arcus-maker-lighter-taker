from __future__ import annotations

"""
订单监控模块
------------
专责处理订单提交后的状态跟踪：
- 优先监听 WebSocket，必要时退回 REST 轮询
- 负责 WS 超时的快速撤单、缓存合并与日志输出
- 保持 `ArbitrageExecutor` 原有行为和日志格式，功能零改动
"""

import asyncio
from core.adapters.exchanges.order_state import OrderStateUnknownError
import logging
from typing import Optional, Dict, Any, TYPE_CHECKING, Set
from decimal import Decimal

from core.adapters.exchanges.interface import ExchangeInterface
from core.adapters.exchanges.models import OrderData, OrderStatus, OrderType
from core.adapters.exchanges.utils.setup_logging import LoggingConfig

if TYPE_CHECKING:
    from .arbitrage_executor import ArbitrageExecutor

logger = LoggingConfig.setup_logger(
    name=__name__,
    log_file='arbitrage_executor.log',
    console_formatter=None,
    file_formatter='detailed',
    level=logging.INFO
)
logger.propagate = False


class OrderMonitor:
    """封装订单等待、撤单与缓存处理逻辑，供 ArbitrageExecutor 复用。"""

    def __init__(self, executor: "ArbitrageExecutor"):
        self.executor = executor
        self._ws_cancelled_orders: Set[str] = set()

    async def _wait_for_order_fill(
        self,
        order: OrderData,
        adapter: ExchangeInterface,
        timeout: int = 60,
        *,
        is_market_order: bool = False,
        require_full_fill: bool = False,
        cancel_on_ws_timeout: bool = True,
    ) -> Optional[Decimal]:
        order_id = str(order.id)
        exchange_name = self.executor._get_exchange_name(adapter)

        if self.executor.monitor_only:
            logger.info(f"🔍 [监控模式] 模拟订单成交: {order_id}, 数量={order.amount}")
            return order.amount

        if getattr(adapter, "strict_order_state", False):
            # ACK and partial fills never prove an IOC has finished. Use authoritative reads.
            deadline = asyncio.get_running_loop().time() + timeout
            while True:
                try:
                    latest = await adapter.get_order(order_id, order.symbol)
                    if latest:
                        # Preserve authoritative partial exposure even if confirmation later times out.
                        known_filled = Decimal(str(order.filled or 0))
                        latest_filled = Decimal(str(latest.filled or 0))
                        if latest_filled >= known_filled:
                            order.filled = latest_filled
                            order.remaining = min(order.remaining, latest.remaining)
                            for field in ("average", "fee"):
                                value = getattr(latest, field)
                                if value is not None:
                                    setattr(order, field, value)
                            order.cost = max(order.cost or Decimal("0"), latest.cost or Decimal("0"))
                            order.raw_data = {**(order.raw_data or {}), **(latest.raw_data or {})}
                        order.status = latest.status
                        if latest.status in (OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED, OrderStatus.EXPIRED):
                            # REST has resolved this submission; a later WS replay is not latency evidence.
                            keys = self.executor._iter_order_keys(order) | self.executor._iter_order_keys(latest)
                            for key in list(keys):
                                pending = self.executor._lookup_pending_order(key)
                                if pending:
                                    keys.update(self.executor._iter_order_keys(pending))
                            for key in keys:
                                self.executor._order_submit_ts.pop(key, None)
                            return order.filled
                except OrderStateUnknownError:
                    # Read retries cannot repeat a transaction; allow delayed projections to settle.
                    pass
                if asyncio.get_running_loop().time() >= deadline:
                    if order.type == OrderType.LIMIT and cancel_on_ws_timeout:
                        await self.executor._cancel_order(adapter, order_id, order.symbol)
                        final = self.executor._order_fill_results.get(order_id)
                        if final is not None:
                            self._merge_order_state(order, final)
                            return order.filled
                    raise OrderStateUnknownError(exchange_name, order_id=order_id, client_id=order.client_id)
                await asyncio.sleep(min(0.2, max(0, deadline - asyncio.get_running_loop().time())))

        if order.status == OrderStatus.FILLED and order.filled and order.filled > Decimal('0'):
            logger.info(
                f"✅ [{exchange_name}] 订单提交时已成交（API直接返回）: {order_id}, "
                f"数量={order.filled}/{order.amount}"
            )
            return order.filled

        ws = getattr(adapter, 'websocket', None) or getattr(adapter, '_websocket', None)
        ws_supported = bool(ws) and hasattr(ws, 'subscribe_order_fills')
        ws_connected = False
        order_fill_ready = getattr(ws, 'order_fill_ws_enabled', True)
        use_websocket = ws_supported and order_fill_ready

        if use_websocket:
            ws_connected = (
                getattr(ws, '_ws_connected', None)
                or getattr(ws, '_connected', None)
                or getattr(ws, 'connected', None)
            )

        if use_websocket and ws_connected:
            try:
                logger.debug(f"📡 [{exchange_name}] 使用WebSocket等待订单成交: {order_id}")
                result = await self._wait_for_order_fill_websocket(order, timeout)
                if result is not None:
                    return result
                logger.debug(
                    f"[{exchange_name}] WebSocket未返回 {order_id} 的成交信息，准备执行兜底流程"
                )
                if is_market_order:
                    # 市价单无法通过撤单确认，只能依赖REST兜底
                    if hasattr(adapter, "get_order"):
                        rest_once = await self._rest_confirm_once(
                            order,
                            adapter,
                            prefer_history=True,
                            is_market_order=True,
                        )
                        if rest_once is not None:
                            return rest_once
                    tx_hash = None
                    try:
                        raw_data = getattr(order, "raw_data", None) or {}
                        if isinstance(raw_data, dict):
                            tx_hash = raw_data.get("tx_hash")
                    except Exception:
                        tx_hash = None
                    client_id = getattr(order, "client_id", None)
                    symbol = getattr(order, "symbol", None) or getattr(order, "market", None) or ""
                    logger.warning(
                        "⚠️ [%s] 市价订单WS超时且REST未确认成交: order_id=%s client_id=%s symbol=%s tx_hash=%s "
                        "(说明：可能为0成交IOC取消/滑点保护取消/订单未生成/WS推送延迟或缺失)",
                        exchange_name,
                        order_id,
                        str(client_id) if client_id else "-",
                        symbol or "-",
                        str(tx_hash) if tx_hash else "-",
                    )
                    return None

                # 限价单：先尝试直接撤单，只有在失败或仍无结果时才查询REST
                if not cancel_on_ws_timeout:
                    return None
                if await self._cancel_after_ws_timeout(order, adapter):
                    logger.debug(
                        f"⚠️ [{exchange_name}] WS超时 {order_id} → 已撤单，视为未成交"
                    )
                    return None

                logger.warning(
                    f"⚠️ [{exchange_name}] WS超时 {order_id} → 撤单失败，改用REST确认"
                )
                if hasattr(adapter, "get_order"):
                    return await self._rest_confirm_once(
                        order,
                        adapter,
                        prefer_history=False,
                        is_market_order=is_market_order,
                    )
                return None
            except OrderStateUnknownError:
                raise
            except Exception as e:
                logger.error(f"❌ [{exchange_name}] WebSocket等待订单成交失败: {e}", exc_info=True)

        logger.debug(
            f"🛰️ [{exchange_name}] 使用REST轮询确认订单成交: {order_id}"
        )
        return await self._wait_for_order_fill_rest(
            order,
            adapter,
            timeout,
            require_full_fill=require_full_fill,
            is_market_order=is_market_order,
        )

    async def _wait_for_order_fill_websocket(
        self,
        order: OrderData,
        timeout: int = 60
    ) -> Optional[Decimal]:
        """等待订单通过WebSocket成交，支持通过order_id或client_id查询"""
        order_id = str(order.id) if getattr(order, "id", None) is not None else None
        client_id = str(order.client_id) if getattr(order, "client_id", None) else None
        executor = self.executor
        exchange_name = executor._resolve_exchange_from_order(order)
        exchange_tag = (exchange_name or "套利执行").upper()

        # 🔥 尝试从order_id或client_id获取提前到达的成交结果
        early_order = None
        if order_id:
            early_order = executor._order_fill_results.pop(order_id, None)
        if not early_order and client_id:
            early_order = executor._order_fill_results.pop(client_id, None)
        
        if early_order is not None:
            self._cleanup_order_fill_cache(early_order, exclude_key=order_id)
            self._merge_order_state(order, early_order)
            price_info = getattr(order, 'average', None) or getattr(order, 'price', None)
            price_suffix = f", 价格={price_info}" if price_info is not None else ""
            logger.debug(
                f"✅ [{exchange_tag}] WebSocket订单成交(提前收到): {order_id}, "
                f"数量={order.filled}{price_suffix}"
            )
            self._log_slippage_cancel(order, exchange_tag)
            return order.filled

        # 注册事件（同时支持order_id和client_id）
        event = asyncio.Event()
        if order_id:
            executor._order_fill_events[order_id] = event
        if client_id:
            executor._order_fill_events[client_id] = event

        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
            # 🔥 尝试从order_id或client_id获取成交结果
            filled_order = None
            if order_id:
                filled_order = executor._order_fill_results.pop(order_id, None)
            if not filled_order and client_id:
                filled_order = executor._order_fill_results.pop(client_id, None)
            
            if filled_order:
                self._cleanup_order_fill_cache(filled_order, exclude_key=order_id)
                self._merge_order_state(order, filled_order)
                price_info = getattr(order, 'average', None) or getattr(order, 'price', None)
                price_suffix = f", 价格={price_info}" if price_info is not None else ""
                logger.debug(
                    f"✅ [{exchange_tag}] WebSocket订单成交: {order_id}, "
                    f"数量={order.filled}{price_suffix}"
                )
                self._log_slippage_cancel(order, exchange_tag)
                return order.filled
            else:
                logger.warning(f"⚠️ [套利执行] 订单事件触发但无成交数据: {order_id}")
                return None
        except asyncio.TimeoutError:
            logger.debug(f"⏱️ [套利执行] WebSocket等待超时: {order_id}")
            # 🔥 超时后检查 _order_fill_progress 缓存，获取部分成交量
            partial_filled = None
            if order_id and order_id in executor._order_fill_progress:
                partial_filled = executor._order_fill_progress[order_id]
            elif client_id and client_id in executor._order_fill_progress:
                partial_filled = executor._order_fill_progress[client_id]
            
            if partial_filled and partial_filled > Decimal("0"):
                order.filled = partial_filled
                target_quantity = executor._to_decimal_value(getattr(order, "amount", Decimal("0")))
                if target_quantity > Decimal("0"):
                    order.remaining = max(target_quantity - partial_filled, Decimal("0"))
                logger.info(
                    f"⏳ [{exchange_tag}] WebSocket超时但检测到部分成交: {order_id}, "
                    f"已成交={partial_filled}/{target_quantity or '?'} ({(partial_filled/target_quantity*100) if target_quantity > 0 else 0:.1f}%)"
                )
                # 返回部分成交量，让调用方决定如何处理（取消订单或继续等待）
                return partial_filled
            
            logger.debug(f"⏱️ [{exchange_tag}] WebSocket超时且无部分成交: {order_id}")
            return None
        finally:
            if order_id:
                executor._order_fill_events.pop(order_id, None)
            if client_id:
                executor._order_fill_events.pop(client_id, None)
                executor._order_fill_results.pop(client_id, None)

    async def _rest_confirm_once(
        self,
        order: OrderData,
        adapter: ExchangeInterface,
        *,
        prefer_history: bool = False,
        is_market_order: bool = False,
    ) -> Optional[Decimal]:
        """在WS撤单失败后，通过REST单次查询确认最终状态。"""
        if not hasattr(adapter, "get_order"):
            logger.error(
                f"❌ [套利执行] {self.executor._get_exchange_name(adapter)} 不支持REST订单查询，无法确认成交: {order.id}"
            )
            return None

        symbol = getattr(order, "symbol", None) or getattr(order, "market", None) or ""
        exchange_name = self.executor._get_exchange_name(adapter)
        exchange_tag = (exchange_name or "套利执行").upper()

        target_amount = self._normalize_decimal(getattr(order, "amount", None))

        if prefer_history:
            history_fn = getattr(adapter, "get_order_history", None)
            if callable(history_fn):
                try:
                    history_orders = await history_fn(symbol=symbol, limit=50)
                    matched = self._match_order_from_list(history_orders, order.id)
                    if matched:
                        self._merge_order_state(order, matched)
                        if order.filled and order.filled > Decimal("0"):
                            logger.debug(
                                f"✅ [套利执行] 历史记录确认订单成交: {order.id}, 数量={order.filled}"
                            )
                            return order.filled
                        status = getattr(order, "status", None)
                        status_text = ""
                        if status:
                            raw = status.value if hasattr(status, "value") else str(status)
                            status_text = raw.upper()
                        if status_text in {"CANCELED", "CANCELLED", "EXPIRED"}:
                            logger.debug(
                                f"[套利执行] 历史记录显示订单已关闭: {order.id}, 状态={status_text}"
                            )
                            self._log_slippage_cancel(order, exchange_tag)
                            return None
                except OrderStateUnknownError:
                    raise
                except Exception as exc:
                    logger.warning(
                        f"⚠️ [套利执行] 查询历史订单失败({order.id}): {exc}"
                    )
        try:
            latest = await adapter.get_order(str(order.id), symbol)
        except OrderStateUnknownError:
            raise
        except Exception as exc:
            logger.warning(
                f"⚠️ [套利执行] 单次REST确认失败({order.id}): {exc}"
            )
            return None

        if not latest:
            logger.debug(f"ℹ️ [套利执行] 单次REST确认未找到订单: {order.id}")
            return None

        self._merge_order_state(order, latest)

        normalized_fill = self._normalize_decimal(getattr(order, "filled", None))
        if normalized_fill and normalized_fill > Decimal("0"):
            order.filled = normalized_fill
            logger.debug(
                f"✅ [套利执行] 单次REST确认订单成交: {order.id}, 数量={normalized_fill}"
            )
            return normalized_fill

        inferred_fill = self._infer_fill_from_status(
            order,
            target_amount,
            allow_fallback=not is_market_order,
        )
        if inferred_fill and inferred_fill > Decimal("0"):
            logger.info(
                f"✅ [套利执行] 单次REST确认订单成交(无活跃订单): {order.id}, 数量={inferred_fill}"
            )
            return inferred_fill

        status = getattr(order, "status", None)
        status_text = ""
        if status:
            raw = status.value if hasattr(status, "value") else str(status)
            status_text = raw.upper()
        if status_text in {"CANCELED", "CANCELLED", "EXPIRED"}:
            logger.warning(
                f"⚠️ [套利执行] 单次REST确认订单已关闭: {order.id}, 状态={status_text}"
            )
            self._log_slippage_cancel(order, exchange_tag)
            return None

        logger.debug(
            f"ℹ️ [套利执行] 单次REST确认未检测到成交: {order.id}, 状态={status_text or 'UNKNOWN'}"
        )
        return None

    @staticmethod
    def _match_order_from_list(
        orders: Optional[Any],
        target_id: Any,
    ) -> Optional[OrderData]:
        if not orders:
            return None
        target = str(target_id)
        for item in orders:
            if not item:
                continue
            if getattr(item, "id", None) and str(item.id) == target:
                return item
            if getattr(item, "client_id", None) and str(item.client_id) == target:
                return item
        return None

    async def _wait_for_order_fill_rest(
        self,
        order: OrderData,
        adapter: ExchangeInterface,
        timeout: int = 60,
        poll_interval: float = 2.0,
        *,
        require_full_fill: bool = False,
        is_market_order: bool = False,
    ) -> Optional[Decimal]:
        if not hasattr(adapter, 'get_order'):
            logger.error(
                f"❌ [套利执行] {self.executor._get_exchange_name(adapter)} 不支持REST订单查询，无法确认成交: {order.id}"
            )
            return None

        exchange_name = self.executor._get_exchange_name(adapter)
        exchange_tag = (exchange_name or "套利执行").upper()
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        symbol = getattr(order, 'symbol', None) or getattr(order, 'market', None) or ''
        safe_interval = max(1.0, poll_interval)
        epsilon = Decimal("0.00000001")
        target_amount: Optional[Decimal] = None
        if getattr(order, "amount", None) is not None:
            try:
                target_amount = Decimal(str(order.amount))
            except Exception:
                target_amount = None
        last_filled = Decimal("0")

        while loop.time() < deadline:
            try:
                latest = await adapter.get_order(str(order.id), symbol)
                if latest:
                    self._merge_order_state(order, latest)
                    filled_value = self._normalize_decimal(getattr(order, "filled", None)) or Decimal("0")
                    if filled_value > Decimal("0"):
                        last_filled = filled_value
                        if (
                            not require_full_fill
                            or (
                                target_amount is not None
                                and filled_value + epsilon >= target_amount
                            )
                        ):
                            logger.info(
                                f"✅ [套利执行] REST确认订单成交: {order.id}, 数量={filled_value}"
                            )
                            return filled_value
                    if (
                        latest.status
                        and latest.status.value.upper() in {"CANCELED", "CANCELLED"}
                    ):
                        logger.warning(
                            f"⚠️ [套利执行] 订单已被取消: {order.id}"
                        )
                        self._log_slippage_cancel(order, exchange_tag)
                        return last_filled if last_filled > Decimal("0") else None

                    inferred_fill = self._infer_fill_from_status(
                        order,
                        target_amount,
                        allow_fallback=not is_market_order,
                    )
                    if inferred_fill and inferred_fill > Decimal("0"):
                        logger.info(
                            f"✅ [套利执行] REST确认订单成交(状态已关闭): {order.id}, 数量={inferred_fill}"
                        )
                        return inferred_fill
            except OrderStateUnknownError:
                raise
            except Exception as exc:
                logger.warning(
                    f"⚠️ [套利执行] REST查询订单状态失败({order.id}): {exc}"
                )

            await asyncio.sleep(min(safe_interval, max(0.5, deadline - loop.time())))

        logger.warning(
            f"⏱️ [套利执行] REST轮询等待超时: {order.id}"
        )
        return last_filled if last_filled > Decimal("0") else None

    async def _cancel_after_ws_timeout(
        self,
        order: OrderData,
        adapter: ExchangeInterface
    ) -> bool:
        if not hasattr(adapter, 'cancel_order'):
            return False

        symbol = getattr(order, 'symbol', None) or getattr(order, 'market', None)
        if not symbol:
            logger.warning("⚠️ [套利执行] WS超时后无法定位交易对，跳过直接撤单")
            return False

        order_id = str(order.id)
        cancel_success = await self.executor._cancel_order(adapter, order_id, symbol)
        if cancel_success:
            self._ws_cancelled_orders.add(order_id)
        return cancel_success

    def consume_ws_cancelled(self, order_id: str) -> bool:
        """
        若该订单已由 OrderMonitor 在WS超时阶段撤单，则返回True并移除标记。
        """
        key = str(order_id)
        if key in self._ws_cancelled_orders:
            self._ws_cancelled_orders.discard(key)
            return True
        return False

    def _cleanup_order_fill_cache(self, filled_order: OrderData, exclude_key: Optional[str] = None) -> None:
        if not filled_order:
            return
        executor = self.executor
        order_key = str(filled_order.id) if filled_order.id else None
        client_key = str(filled_order.client_id) if filled_order.client_id else None

        if order_key and order_key != exclude_key:
            executor._order_fill_results.pop(order_key, None)
        if client_key:
            executor._order_fill_results.pop(client_key, None)
        executor._cleanup_order_exchange_mapping(filled_order, exclude_key=exclude_key)

    def _merge_order_state(self, target: OrderData, source: OrderData) -> None:
        if not target or not source:
            return

        target.filled = source.filled or target.filled
        if source.remaining is not None:
            target.remaining = source.remaining
        if source.cost is not None:
            target.cost = source.cost
        if source.price is not None:
            target.price = source.price
        if source.average is not None:
            target.average = source.average
        if source.status is not None:
            target.status = source.status
        if source.updated is not None:
            target.updated = source.updated
        if source.trades:
            target.trades = source.trades
        if source.fee:
            target.fee = source.fee
        if source.raw_data:
            if not target.raw_data:
                target.raw_data = source.raw_data
            else:
                target.raw_data.update(source.raw_data)

        source_params = getattr(source, "params", None)
        if source_params:
            if not getattr(target, "params", None):
                target.params = dict(source_params)
            else:
                target.params.update(source_params)

    def _log_slippage_cancel(self, order: Optional[OrderData], exchange_tag: str) -> None:
        if not order:
            return
        if self.executor._is_slippage_cancel(order):
            raw_status = self.executor._extract_raw_order_status(order) or "canceled-too-much-slippage"
            logger.error(
                "❌ [%s] 市价订单因滑点保护被取消: order_id=%s, status=%s",
                exchange_tag,
                getattr(order, "id", "?"),
                raw_status,
            )

    @staticmethod
    def _normalize_decimal(value: Optional[Any]) -> Optional[Decimal]:
        if value is None:
            return None
        try:
            return Decimal(str(value))
        except Exception:
            return None

    def _infer_fill_from_status(
        self,
        order: OrderData,
        target_amount: Optional[Decimal],
        *,
        allow_fallback: bool = True,
    ) -> Optional[Decimal]:
        """
        当REST返回状态为FILLED但缺少filled字段时，推断为全部成交。
        """
        status = getattr(order, "status", None)
        status_text = ""
        if status:
            status_text = status.value if hasattr(status, "value") else str(status)
            status_text = status_text.upper()

        if status_text != "FILLED":
            return None

        normalized_fill = self._normalize_decimal(getattr(order, "filled", None))
        if normalized_fill is not None:
            order.filled = normalized_fill
            if normalized_fill > Decimal("0"):
                return normalized_fill
            # 显式的0表示交易所确认未成交，直接返回0避免误判
            return Decimal("0")

        if not allow_fallback:
            return None

        fallback = target_amount or self._normalize_decimal(getattr(order, "amount", None))
        if fallback and fallback > Decimal("0"):
            order.filled = fallback
            order.remaining = Decimal("0")
            return fallback
        return None


    async def wait_for_order_fill(self, order, adapter, timeout=60, *, is_market_order=False, require_full_fill=False, cancel_on_ws_timeout=True):
        filled = await self._wait_for_order_fill(order, adapter, timeout, is_market_order=is_market_order, require_full_fill=require_full_fill, cancel_on_ws_timeout=cancel_on_ws_timeout)
        self.executor._remember_confirmed_fill(adapter, order)
        return filled
