from __future__ import annotations

"""
订单策略执行器模块
------------------
负责承接 `ArbitrageExecutor` 中所有与“下单模式选择 + 执行流程”有关的纯逻辑：
- 解析执行计划（限价/市价/双限价/批量WS）
- 调度各执行模式并处理部分成交、fallback、市价补单
- 保持原有日志与异常格式，确保外层行为一致

此模块不做依赖注入或状态管理，所有外部依赖通过 `executor` 传入，做到“可读、可复用、不改功能”。
"""

import asyncio
from core.adapters.exchanges.order_state import OrderStateUnknownError
import time
from decimal import Decimal, ROUND_DOWN, ROUND_UP, InvalidOperation
import logging
from typing import Any, Dict, Optional, Type, TYPE_CHECKING, Tuple, List

from core.adapters.exchanges.interface import ExchangeInterface
from core.adapters.exchanges.models import OrderData, OrderStatus, OrderSide
from core.adapters.exchanges.utils.setup_logging import LoggingConfig

from ..risk_control.network_state import notify_network_recovered

if TYPE_CHECKING:
    from .arbitrage_executor import (
        ArbitrageExecutor,
        ExecutionRequest,
        ExecutionResult,
        ReduceOnlyRestrictionError,
    )


logger = LoggingConfig.setup_logger(
    name=__name__,
    log_file='arbitrage_executor.log',
    console_formatter=None,
    file_formatter='detailed',
    level=logging.INFO
)
logger.propagate = False


class OrderStrategyExecutor:
    """封装下单模式选择与执行逻辑，降低 ArbitrageExecutor 体积。"""

    def __init__(
        self,
        executor: "ArbitrageExecutor",
        execution_result_cls: Type["ExecutionResult"],
        reduce_only_exception_cls: Type["ReduceOnlyRestrictionError"],
    ) -> None:
        self.executor = executor
        self.ExecutionResult = execution_result_cls
        self.ReduceOnlyRestrictionError = reduce_only_exception_cls

    def _mark_symbol_waiting(self, request: Optional["ExecutionRequest"], reason: str) -> None:
        """通知执行器将符号标记为等待状态。"""
        if not request:
            return
        self.executor._mark_symbol_waiting(request, reason)

    def _mark_manual_intervention(self, request: Optional["ExecutionRequest"], reason_suffix: str) -> None:
        """
        将套利对标记为需要人工介入，终端状态栏会显示对应提示。
        """
        if not request:
            return
        symbol = getattr(request, "symbol", None)
        state_manager = getattr(self.executor, "symbol_state_manager", None)
        if not symbol or not state_manager:
            return
        reason = f"需人工介入：{reason_suffix}".strip()
        state_manager.defer(
            symbol=symbol,
            reason=reason,
            grid_level=getattr(request, "grid_level", None),
            exchange_buy=getattr(request, "exchange_buy", None),
            exchange_sell=getattr(request, "exchange_sell", None),
        )
    
    def _apply_second_leg_overrides(
        self,
        request: "ExecutionRequest",
        plan: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        针对限价+市价模式的第二腿做额外配置覆写，
        例如 lighter 设置 force_limit_when_second_leg 时改用激进限价。
        """
        if plan.get("mode") != "limit_market":
            return plan
        if plan.get("second_order_mode") != "market":
            return plan
        
        second_exchange = plan.get("second_exchange")
        if not second_exchange:
            return plan
        
        second_symbol = (
            (request.buy_symbol or request.symbol)
            if plan.get("second_is_buy")
            else (request.sell_symbol or request.symbol)
        )
        exchange_config = self.executor._get_exchange_mode_config(
            second_exchange,
            second_symbol,
        )
        if exchange_config and getattr(exchange_config, "force_limit_when_second_leg", False):
            updated_plan = dict(plan)
            updated_plan["second_order_mode"] = "limit"
            logger.info(
                f"[执行计划] {second_exchange} 启用 force_limit_when_second_leg，"
                "第二腿改为激进限价顺序执行。"
            )
            return updated_plan
        return plan

    async def _wait_fill_after_cancel_failure(
        self,
        order: Optional[OrderData],
        adapter: ExchangeInterface,
        symbol: str,
        target_quantity: Decimal,
        exchange_tag: str,
        *,
        wait_seconds: float = 2.0,
        interval_seconds: float = 0.2,
    ) -> Optional[Decimal]:
        """
        撤单失败后额外等待 WebSocket/REST 更新，以捕捉“刚成交但尚未推送”的情况。
        返回检测到的最新成交量（若无更新则返回 None）。
        """
        if not order or not getattr(order, "id", None):
            return None

        epsilon = Decimal("0.00000001")
        interval = max(0.05, interval_seconds)
        remaining = max(0.0, wait_seconds)

        refreshed_total = await self._resolve_filled_quantity(
            order=order,
            adapter=adapter,
            symbol=symbol,
            reported_filled=None,
        )
        best_fill = self.executor._to_decimal_value(refreshed_total or Decimal("0"))
        if best_fill + epsilon >= target_quantity:
            return best_fill

        loop = asyncio.get_running_loop()
        deadline = loop.time() + remaining
        while loop.time() < deadline:
            await asyncio.sleep(interval)
            refreshed_total = await self._resolve_filled_quantity(
                order=order,
                adapter=adapter,
                symbol=symbol,
                reported_filled=None,
            )
            current_fill = self.executor._to_decimal_value(refreshed_total or Decimal("0"))
            if current_fill > best_fill + epsilon:
                logger.info(
                    f"⏳ [套利执行] [{exchange_tag}] 撤单失败后检测到新增成交 {current_fill}"
                )
            best_fill = max(best_fill, current_fill)
            if best_fill + epsilon >= target_quantity:
                return best_fill

        return best_fill if best_fill > Decimal("0") else None

    async def _retry_limit_submission(
        self,
        *,
        adapter: ExchangeInterface,
        symbol: str,
        price: Decimal,
        quantity: Decimal,
        is_buy: bool,
        absolute_offset: Optional[Decimal],
        request: "ExecutionRequest",
        exchange_name: str,
    ) -> Optional[OrderData]:
        logger.warning(
            "🔁 [套利执行] %s 限价订单提交失败，启动失败腿重试（方向=%s，数量=%s）",
            exchange_name,
            "买入" if is_buy else "卖出",
            quantity,
        )
        refreshed_price = self.executor._get_live_market_price(
            exchange_name,
            symbol,
            is_buy,
        )
        effective_price = refreshed_price or price
        if refreshed_price and refreshed_price != price:
            logger.info(
                f"[套利执行] [{self.executor._format_exchange_tag(exchange_name)}] "
                f"限价重试使用最新盘口价: {effective_price} (原价={price})"
            )
        return await self.executor._place_limit_order(
            adapter,
            symbol,
            effective_price,
            quantity,
            is_buy=is_buy,
            absolute_offset=absolute_offset,
            request=request,
        )

    async def _handle_first_leg_min_qty_failure(
        self,
        *,
        request: "ExecutionRequest",
        execution_plan: Dict[str, Any],
        first_exchange: str,
        first_adapter: ExchangeInterface,
        first_symbol: str,
        accumulated_fill: Decimal,
        submit_quantity: Decimal,
        min_first_leg_qty: Decimal,
    ) -> "ExecutionResult":
        """
        处理第一腿限价单因剩余数量低于最小下单量而无法提交的场景。
        """
        executor = self.executor
        Result = self.ExecutionResult
        epsilon = Decimal("0.00000001")

        qty_text = f"{submit_quantity:.8f}"
        min_text = f"{min_first_leg_qty:.8f}"

        logger.error(
            f"❌ [套利执行] [{executor._format_exchange_tag(first_exchange)}] "
            f"剩余数量 {qty_text} 低于最小下单量 {min_text}，限价单提交失败，触发紧急平仓流程。"
        )

        close_success = True
        if accumulated_fill > epsilon:
            try:
                close_success = await executor._emergency_close_position(
                    exchange=first_exchange,
                    adapter=first_adapter,
                    symbol=first_symbol,
                    quantity=accumulated_fill,
                    is_buy_to_close=not execution_plan["first_is_buy"],
                    request=request,
                )
            except OrderStateUnknownError:
                raise
            except Exception as exc:
                logger.error(
                    f"❌ [套利执行] [{executor._format_exchange_tag(first_exchange)}] "
                    f"紧急平仓异常: {exc}",
                    exc_info=True,
                )
                close_success = False

        if close_success:
            logger.warning(
                f"⚠️ [套利执行] [{executor._format_exchange_tag(first_exchange)}] "
                "已撤回已成交部分，等待重新调整最小下单量后再尝试。"
            )
        else:
            logger.error(
                f"🚨 [套利执行] [{executor._format_exchange_tag(first_exchange)}] "
                "紧急平仓失败，需人工介入处理剩余仓位。"
            )
            self._mark_manual_intervention(
                request,
                f"{first_exchange} 最小下单量 {min_text}，当前剩余 {qty_text}",
            )

        return self._result_failure(
            request,
            f"{first_exchange} 限价单剩余数量低于最小下单量，已停止本次执行并等待人工介入",
            failure_code="limit_min_qty_violation",
        )

    async def _retry_market_submission(
        self,
        *,
        adapter: ExchangeInterface,
        symbol: str,
        quantity: Decimal,
        is_buy: bool,
        reduce_only: bool,
        request: "ExecutionRequest",
        exchange_name: str,
        slippage_override: Optional[Decimal] = None,
    ) -> Optional[OrderData]:
        logger.warning(
            "🔁 [套利执行] %s 市价订单提交失败，启动失败腿重试（方向=%s，数量=%s）",
            exchange_name,
            "买入" if is_buy else "卖出",
            quantity,
        )
        return await self.executor._place_market_order(
            adapter,
            symbol,
            quantity,
            is_buy=is_buy,
            reduce_only=reduce_only,
            request=request,
            slippage_override=slippage_override,
        )

    async def _place_aggressive_limit_retry_order(
        self,
        *,
        adapter: ExchangeInterface,
        symbol: str,
        quantity: Decimal,
        is_buy: bool,
        request: Optional["ExecutionRequest"],
        exchange_name: str,
        slippage_pct: Optional[Decimal],
        base_slippage: Optional[Decimal],
    ) -> Optional[OrderData]:
        """市价多次失败后，改用最新盘口价 + 加倍偏移的激进限价单吃单。"""
        if not request:
            return None

        executor = self.executor

        base_price_raw = request.price_buy if is_buy else request.price_sell
        fallback_price_raw = request.price_sell if is_buy else request.price_buy

        live_price = executor._get_live_market_price(exchange_name, symbol, is_buy)
        if live_price is not None:
            price_decimal = live_price
        else:
            price_source = base_price_raw if base_price_raw not in (None, 0) else fallback_price_raw
            price_decimal = executor._to_decimal_value(price_source or Decimal("0"))

        if price_decimal <= Decimal("0"):
            return None

        multiplier = Decimal("2")

        absolute_offset: Optional[Decimal] = None
        raw_absolute_offset = None
        if request:
            raw_absolute_offset = (
                request.limit_price_offset_buy if is_buy else request.limit_price_offset_sell
            )
        if raw_absolute_offset is not None:
            absolute_offset = executor._to_decimal_value(raw_absolute_offset)
            if absolute_offset <= Decimal("0"):
                absolute_offset = None
        if absolute_offset is not None:
            absolute_offset *= multiplier

        offset_pct = slippage_pct or base_slippage or Decimal("0.001")
        if offset_pct <= Decimal("0"):
            offset_pct = Decimal("0.001")
        offset_pct = executor._to_decimal_value(offset_pct) * multiplier

        if is_buy:
            if absolute_offset:
                target_price = price_decimal + absolute_offset
            else:
                target_price = price_decimal * (Decimal("1") + offset_pct)
        else:
            if absolute_offset:
                target_price = price_decimal - absolute_offset
            else:
                target_price = price_decimal * (Decimal("1") - offset_pct)
            if target_price <= Decimal("0"):
                target_price = price_decimal * Decimal("0.999")

        step = executor._infer_price_step(price_decimal)
        if step > Decimal("0"):
            rounding_mode = ROUND_UP if is_buy else ROUND_DOWN
            target_price = target_price.quantize(step, rounding=rounding_mode)

        exchange_tag = executor._format_exchange_tag(exchange_name)
        leg_label = "买腿" if is_buy else "卖腿"
        offset_hint = (
            f"{absolute_offset} (abs)"
            if absolute_offset
            else f"{float((offset_pct * Decimal('100'))):.4f}%"
        )
        logger.info(
            f"[套利执行] [{exchange_tag}] 激进限价补仓[{leg_label}]: 基准价={price_decimal}, 目标价={target_price}, 偏移={offset_hint}"
        )

        return await executor._place_limit_order(
            adapter=adapter,
            symbol=symbol,
            price=price_decimal,
            quantity=quantity,
            is_buy=is_buy,
            absolute_offset=None,
            request=request,
            override_price=target_price,
        )

    async def _confirm_market_fill_with_retries(
        self,
        *,
        initial_order: Optional[OrderData],
        adapter: ExchangeInterface,
        symbol: str,
        quantity: Decimal,
        is_buy: bool,
        reduce_only: bool,
        request: Optional["ExecutionRequest"],
        exchange_name: str,
        allow_resubmit: bool = True,
        max_attempts: Optional[int] = None,
        enable_internal_aggressive_retry: bool = True,
    ) -> Tuple[Optional[OrderData], Decimal]:
        """
        等待市价订单成交，必要时重新提交，避免长期无成交造成单腿风险。
        enable_internal_aggressive_retry=False 时禁用内部激进限价兜底（供双限价补仓自定义流程使用）。
        """
        executor = self.executor
        leg_label = "买腿" if is_buy else "卖腿"
        epsilon = Decimal("0.00000001")
        limit_timeout_cfg = getattr(
            executor.config.order_execution,
            "limit_order_timeout",
            60,
        ) or 60
        try:
            limit_timeout = max(1, int(limit_timeout_cfg))
        except (ValueError, TypeError):
            limit_timeout = 60
        lighter_market_timeout_cfg = getattr(
            executor.config.order_execution,
            "lighter_market_order_timeout",
            None,
        )
        lighter_market_timeout: Optional[int] = None
        if lighter_market_timeout_cfg not in (None, 0):
            try:
                lighter_market_timeout = max(1, int(lighter_market_timeout_cfg))
            except (ValueError, TypeError):
                lighter_market_timeout = None
        configured_attempt_limit = max_attempts or getattr(
            executor.config.order_execution,
            "max_retry_count",
            3,
        )
        if configured_attempt_limit != 1:
            logger.debug(
                "⚙️ 市价补单最大重试次数配置=%s，但当前策略强制使用单次尝试。",
                configured_attempt_limit,
            )
        # ⚠️ 需求变更：所有场景下市价仅允许一次尝试，失败即进入下一流程
        market_attempt_limit = 1
        market_attempts_used = 1 if initial_order is not None else 0
        current_order = initial_order
        current_is_market = True
        last_order = initial_order
        target_quantity = executor._to_decimal_value(quantity)
        base_slippage = executor._get_slippage_percent(request)
        last_slippage_override: Optional[Decimal] = base_slippage
        lighter_retry_mode = bool(exchange_name and exchange_name.lower() == "lighter")
        aggressive_limit_attempted = False

        while True:
            if current_order is None:
                if not allow_resubmit and not aggressive_limit_attempted:
                    break
                slippage_override = None
                if market_attempts_used < market_attempt_limit:
                    if base_slippage is not None:
                        multiplier = Decimal(2) ** (market_attempts_used)
                        slippage_override = base_slippage * multiplier
                        last_slippage_override = slippage_override
                    slip_hint = (
                        f"，滑点={float(slippage_override * Decimal('100')):.4f}%"
                        if slippage_override is not None
                        else ""
                    )
                    logger.warning(
                        "⏱️ [套利执行] %s %s 市价补单尝试(%s/%s)%s，若失败将直接进入下一流程",
                        exchange_name,
                        leg_label,
                        market_attempts_used + 1,
                        market_attempt_limit,
                        slip_hint,
                    )
                    try:
                        current_order = await executor._place_market_order(
                            adapter,
                            symbol,
                            target_quantity,
                            is_buy=is_buy,
                            reduce_only=reduce_only,
                            request=request,
                            slippage_override=slippage_override,
                        )
                        current_is_market = True
                        market_attempts_used += 1
                    except self.ReduceOnlyRestrictionError:
                        raise

                    if not current_order:
                        current_order = await self._retry_market_submission(
                            adapter=adapter,
                            symbol=symbol,
                            quantity=target_quantity,
                            is_buy=is_buy,
                            reduce_only=reduce_only,
                            request=request,
                            exchange_name=exchange_name,
                            slippage_override=slippage_override,
                        )
                        current_is_market = True
                        if not current_order:
                            continue
                elif enable_internal_aggressive_retry and not aggressive_limit_attempted:
                    logger.warning(
                        "🔁 [套利执行] %s %s 市价补单多次失败，尝试激进限价吃单",
                        exchange_name,
                        leg_label,
                    )
                    current_order = await self._place_aggressive_limit_retry_order(
                        adapter=adapter,
                        symbol=symbol,
                        quantity=target_quantity,
                        is_buy=is_buy,
                        request=request,
                        exchange_name=exchange_name,
                        slippage_pct=last_slippage_override or base_slippage,
                        base_slippage=base_slippage,
                    )
                    aggressive_limit_attempted = True
                    current_is_market = False
                    if not current_order:
                        break
                else:
                    break

                last_order = current_order

            wait_timeout = (
                lighter_market_timeout
                if lighter_retry_mode and current_is_market and lighter_market_timeout
                else limit_timeout
            )
            filled = await executor.order_monitor.wait_for_order_fill(
                current_order,
                adapter,
                timeout=wait_timeout,
                is_market_order=current_is_market,
            )
            if (filled is None and getattr(adapter, 'strict_order_state', False)
                    and current_order.status not in (OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED, OrderStatus.EXPIRED)):
                raise OrderStateUnknownError(exchange_name, order_id=current_order.id)
            filled_decimal = executor._to_decimal_value(filled or Decimal("0"))
            if filled_decimal > epsilon:
                price_info = getattr(current_order, "average", None) or getattr(current_order, "price", None)
                price_text = (
                    f"{executor._to_decimal_value(price_info):f}"
                    if price_info is not None
                    else "--"
                )
                # 🔄 同步订单对象的最新成交信息，便于后续统计
                current_order.filled = filled_decimal
                normalized_amount = executor._to_decimal_value(
                    getattr(current_order, "amount", None) or filled_decimal
                )
                current_order.amount = normalized_amount
                current_order.remaining = max(normalized_amount - filled_decimal, Decimal("0"))
                price_decimal = executor._to_decimal_value(price_info) if price_info is not None else None
                if price_decimal is not None:
                    current_order.average = price_decimal
                    current_order.price = price_decimal
                    current_cost = executor._to_decimal_value(getattr(current_order, "cost", None))
                    if current_cost <= Decimal("0"):
                        current_order.cost = price_decimal * filled_decimal

                logger.info(
                    "✅ [套利执行] %s %s 市价补单成交: 尝试#%s, 数量=%s, 价格=%s",
                    exchange_name,
                    leg_label,
                    market_attempts_used,
                    filled_decimal,
                    price_text,
                )
                return current_order, filled_decimal

            if executor._is_slippage_cancel(current_order):
                logger.error(
                    "❌ [套利执行] %s %s 市价补单因滑点保护被取消: order_id=%s",
                    exchange_name,
                    leg_label,
                    current_order.id if current_order else "-",
                )
                if aggressive_limit_attempted:
                    break
                current_order = None
                current_is_market = True
                continue

            logger.warning(
                "%s [套利执行] %s %s 补单未确认成交: order_id=%s",
                "⚠️" if current_is_market else "❌",
                exchange_name,
                leg_label,
                current_order.id if current_order else "-",
            )
            if aggressive_limit_attempted:
                break
            current_order = None
            current_is_market = True

        return last_order, Decimal("0")

    def _result_failure(
        self,
        request: Optional["ExecutionRequest"],
        message: str,
        *,
        mark_waiting: bool = True,
        **kwargs: Any,
    ) -> "ExecutionResult":
        if mark_waiting:
            self._mark_symbol_waiting(request, message)
        return self.ExecutionResult(
            success=False,
            error_message=message,
            **kwargs,
        )

    def _unknown_result(self, request, exc, order_buy=None, order_sell=None):
        order_buy = order_buy if isinstance(order_buy, OrderData) else None
        order_sell = order_sell if isinstance(order_sell, OrderData) else None
        buy_filled = self.executor._to_decimal_value(getattr(order_buy, 'filled', 0))
        sell_filled = self.executor._to_decimal_value(getattr(order_sell, 'filled', 0))
        known_quantity = max(buy_filled, sell_filled)
        return self.ExecutionResult(success=False, error_message=str(exc), failure_code='ORDER_STATE_UNKNOWN',
            order_buy=order_buy, order_sell=order_sell, partial_failure=known_quantity > 0,
            success_quantity=known_quantity,
            success_exchange=(request.exchange_buy if buy_filled >= sell_filled else request.exchange_sell) if known_quantity else None)

    def determine_execution_plan(self, request: "ExecutionRequest") -> Dict[str, Any]:
        executor = self.executor
        dual_limit_enabled = getattr(
            executor.config.order_execution,
            "enable_dual_limit_mode",
            False,
        )

        buy_exchange_config = executor._get_exchange_mode_config(
            request.exchange_buy,
            request.buy_symbol or request.symbol,
        )
        buy_mode = buy_exchange_config.order_mode if buy_exchange_config else "market"
        buy_priority = buy_exchange_config.priority if buy_exchange_config else 0

        sell_exchange_config = executor._get_exchange_mode_config(
            request.exchange_sell,
            request.sell_symbol or request.symbol,
        )
        sell_mode = sell_exchange_config.order_mode if sell_exchange_config else "market"
        sell_priority = sell_exchange_config.priority if sell_exchange_config else 0

        logger.debug(
            f"[执行计划] {request.exchange_buy}(买入): mode={buy_mode}, priority={buy_priority} | "
            f"{request.exchange_sell}(卖出): mode={sell_mode}, priority={sell_priority}"
        )

        if request.force_order_mode == "market_market":
            buy_mode = sell_mode = "market"
        elif request.force_order_mode == "limit_limit":
            buy_mode = sell_mode = "limit"
            dual_limit_enabled = True

        if buy_mode == "market" and sell_mode == "market":
            return self._apply_second_leg_overrides(
                request,
                {
                    "mode": "market_market",
                    "first_exchange": None,
                    "first_is_buy": None,
                    "first_order_mode": None,
                    "second_exchange": None,
                    "second_is_buy": None,
                    "second_order_mode": None,
                    "parallel": True,
                },
            )

        if buy_mode == "limit" and sell_mode == "limit":
            # 🔥 检查是否有任一方启用了 force_sequential_limit
            buy_force_sequential = getattr(buy_exchange_config, "force_sequential_limit", False) if buy_exchange_config else False
            sell_force_sequential = getattr(sell_exchange_config, "force_sequential_limit", False) if sell_exchange_config else False
            
            logger.debug(
                f"[执行计划] 双限价检查: {request.exchange_buy}(buy_force={buy_force_sequential}) | "
                f"{request.exchange_sell}(sell_force={sell_force_sequential})"
            )
            
            # 如果任一方启用了强制顺序执行，采用 limit+market 模式
            if buy_force_sequential or sell_force_sequential:
                # 优先执行没有启用 force_sequential_limit 的一方（让它先挂限价单）
                # 启用 force_sequential_limit 的一方后执行（用激进限价单吃单）
                if buy_force_sequential and not sell_force_sequential:
                    # buy方强制顺序，先执行sell（限价），再执行buy（激进限价吃单）
                    first_exchange = request.exchange_sell
                    first_is_buy = False
                    second_exchange = request.exchange_buy
                    second_is_buy = True
                    logger.info(
                        f"[执行计划] {request.exchange_buy} 启用force_sequential_limit，"
                        f"采用先限价({first_exchange})后激进限价吃单({second_exchange})模式"
                    )
                elif sell_force_sequential and not buy_force_sequential:
                    # sell方强制顺序，先执行buy（限价），再执行sell（激进限价吃单）
                    first_exchange = request.exchange_buy
                    first_is_buy = True
                    second_exchange = request.exchange_sell
                    second_is_buy = False
                    logger.info(
                        f"[执行计划] {request.exchange_sell} 启用force_sequential_limit，"
                        f"采用先限价({first_exchange})后激进限价吃单({second_exchange})模式"
                    )
                else:
                    # 双方都启用了强制顺序，根据priority决定
                    if buy_priority >= sell_priority:
                        first_exchange = request.exchange_buy
                        first_is_buy = True
                        second_exchange = request.exchange_sell
                        second_is_buy = False
                    else:
                        first_exchange = request.exchange_sell
                        first_is_buy = False
                        second_exchange = request.exchange_buy
                        second_is_buy = True
                    logger.info(
                        f"[执行计划] 双方均启用force_sequential_limit，"
                        f"根据priority采用先限价({first_exchange})后激进限价吃单({second_exchange})模式"
                    )
                
                return self._apply_second_leg_overrides(
                    request,
                    {
                        "mode": "limit_market",
                        "first_exchange": first_exchange,
                        "first_is_buy": first_is_buy,
                        "first_order_mode": "limit",
                        "second_exchange": second_exchange,
                        "second_is_buy": second_is_buy,
                        "second_order_mode": "limit",  # 🔥 第二腿也是limit，但会用激进价格偏移
                        "parallel": False,
                    },
                )
            
            # 没有启用强制顺序，走原有的双限价或limit+market逻辑
            if dual_limit_enabled:
                return {
                    "mode": "limit_limit",
                    "first_exchange": None,
                    "first_is_buy": None,
                    "first_order_mode": "limit",
                    "second_exchange": None,
                    "second_is_buy": None,
                    "second_order_mode": "limit",
                    "parallel": True,
                }
            if buy_priority >= sell_priority:
                first_exchange = request.exchange_buy
                first_is_buy = True
                second_exchange = request.exchange_sell
                second_is_buy = False
            else:
                first_exchange = request.exchange_sell
                first_is_buy = False
                second_exchange = request.exchange_buy
                second_is_buy = True

            logger.warning(
                f"[执行计划] {request.exchange_buy}/{request.exchange_sell} 均为limit，"
                f"根据priority调整为先限价({first_exchange})后市价({second_exchange})"
            )

            return self._apply_second_leg_overrides(
                request,
                {
                    "mode": "limit_market",
                    "first_exchange": first_exchange,
                    "first_is_buy": first_is_buy,
                    "first_order_mode": "limit",
                    "second_exchange": second_exchange,
                    "second_is_buy": second_is_buy,
                    "second_order_mode": "market",
                    "parallel": False,
                },
            )

        if buy_mode == "limit":
            return self._apply_second_leg_overrides(
                request,
                {
                    "mode": "limit_market",
                    "first_exchange": request.exchange_buy,
                    "first_is_buy": True,
                    "first_order_mode": "limit",
                    "second_exchange": request.exchange_sell,
                    "second_is_buy": False,
                    "second_order_mode": "market",
                    "parallel": False,
                },
            )

        return self._apply_second_leg_overrides(
            request,
            {
                "mode": "limit_market",
                "first_exchange": request.exchange_sell,
                "first_is_buy": False,
                "first_order_mode": "limit",
                "second_exchange": request.exchange_buy,
                "second_is_buy": True,
                "second_order_mode": "market",
                "parallel": False,
            },
        )

    async def execute_limit_market_mode(
        self,
        request: "ExecutionRequest",
        execution_plan: Dict[str, Any],
    ) -> "ExecutionResult":
        executor = self.executor
        Result = self.ExecutionResult
        epsilon = Decimal("0.00000001")
        
        # 🔥 获取限价单超时配置
        limit_timeout_cfg = getattr(
            executor.config.order_execution,
            "limit_order_timeout",
            60,
        ) or 60
        try:
            limit_timeout = max(1, int(limit_timeout_cfg))
        except (ValueError, TypeError):
            limit_timeout = 60

        buy_adapter = executor.exchange_adapters.get(request.exchange_buy)
        sell_adapter = executor.exchange_adapters.get(request.exchange_sell)

        buy_symbol = request.buy_symbol or request.symbol
        sell_symbol = request.sell_symbol or request.symbol

        if not buy_adapter or not sell_adapter:
            return self._result_failure(
                request,
                f"交易所适配器不存在: {request.exchange_buy} 或 {request.exchange_sell}",
                mark_waiting=False,
            )

        limit_order = market_order = None
        try:
            first_adapter = buy_adapter if execution_plan["first_is_buy"] else sell_adapter
            second_adapter = sell_adapter if execution_plan["first_is_buy"] else buy_adapter
            first_exchange = execution_plan["first_exchange"]
            second_exchange = execution_plan["second_exchange"]
            first_tag = self.executor._format_exchange_tag(first_exchange)
            second_tag = self.executor._format_exchange_tag(second_exchange)
            first_price = request.price_buy if execution_plan["first_is_buy"] else request.price_sell
            first_offset = (
                request.limit_price_offset_buy
                if execution_plan["first_is_buy"]
                else request.limit_price_offset_sell
            )

            first_symbol = buy_symbol if execution_plan["first_is_buy"] else sell_symbol
            first_exchange_config = self.executor._get_exchange_mode_config(
                first_exchange,
                first_symbol,
            )
            first_uses_tick_precision = bool(
                getattr(first_exchange_config, "use_tick_precision", False)
            )
            first_absolute_offset = None if first_uses_tick_precision else first_offset
            requested_quantity = executor._to_decimal_value(request.quantity or Decimal('0'))
            if requested_quantity <= epsilon:
                return self._result_failure(
                    request,
                    f"限价订单数量异常 ({first_exchange})",
                    mark_waiting=False,
                )

            # 预取第二腿上下文，避免回调路径重复计算
            second_is_buy = execution_plan["second_is_buy"]
            second_symbol = buy_symbol if second_is_buy else sell_symbol
            second_exchange_config = self.executor._get_exchange_mode_config(
                second_exchange,
                second_symbol,
            )
            second_uses_tick_precision = bool(
                getattr(second_exchange_config, "use_tick_precision", False)
            )
            prefetched_second_ctx = {
                "symbol": second_symbol,
                "is_buy": second_is_buy,
                "tag": second_tag,
                "uses_tick_precision": second_uses_tick_precision,
            }

            min_second_leg_qty = executor._resolve_min_order_qty(request, second_exchange)
            min_first_leg_qty = executor._resolve_min_order_qty(request, first_exchange)
            accumulated_fill = Decimal("0")
            max_partial_attempts = max(
                1,
                getattr(executor.config.order_execution, "max_retry_count", 3),
            )
            limit_attempt = 0
            limit_order: Optional[OrderData] = None
            zero_fill_abort = False
            zero_fill_order_id: Optional[str] = None

            while accumulated_fill + epsilon < requested_quantity:
                if limit_attempt >= max_partial_attempts:
                    break
                limit_attempt += 1
                remaining_quantity = max(Decimal("0"), requested_quantity - accumulated_fill)
                submit_quantity = remaining_quantity

                refreshed_price = self.executor._get_live_market_price(
                    first_exchange,
                    first_symbol,
                    execution_plan["first_is_buy"],
                )
                if refreshed_price and refreshed_price > Decimal("0"):
                    if first_price != refreshed_price:
                        logger.info(
                            f"[套利执行] [{first_tag}] 第{limit_attempt}次限价重试使用最新盘口价: "
                            f"{refreshed_price} (原价={first_price})"
                        )
                        first_price = refreshed_price

                logger.info(
                    f"[套利执行] [{first_tag}] 提交限价订单: "
                    f"方向={'买入' if execution_plan['first_is_buy'] else '卖出'}, "
                    f"价格={first_price}, 数量={submit_quantity} (第{limit_attempt}次)"
                )

                limit_order = await executor._place_limit_order(
                    first_adapter,
                    first_symbol,
                    first_price,
                    submit_quantity,
                    is_buy=execution_plan["first_is_buy"],
                    absolute_offset=first_absolute_offset,
                    request=request,
                )
                if not limit_order:
                    limit_order = await self._retry_limit_submission(
                        adapter=first_adapter,
                        symbol=first_symbol,
                        price=first_price,
                        quantity=submit_quantity,
                        is_buy=execution_plan["first_is_buy"],
                        absolute_offset=first_absolute_offset,
                        request=request,
                        exchange_name=first_exchange,
                    )

                if not limit_order:
                    if (
                        min_first_leg_qty
                        and submit_quantity + epsilon < min_first_leg_qty
                    ):
                        return await self._handle_first_leg_min_qty_failure(
                            request=request,
                            execution_plan=execution_plan,
                            first_exchange=first_exchange,
                            first_adapter=first_adapter,
                            first_symbol=first_symbol,
                            accumulated_fill=accumulated_fill,
                            submit_quantity=submit_quantity,
                            min_first_leg_qty=min_first_leg_qty,
                        )
                    return self._result_failure(
                        request,
                        f"限价订单提交失败 ({first_exchange})",
                    )

                submitted_price = getattr(limit_order, "price", None) or first_price
                logger.info(
                    f"[套利执行] [{first_tag}] 限价订单已提交: order_id={limit_order.id}, "
                    f"价格={submitted_price}, 数量={submit_quantity}"
                )

                filled_quantity = await executor.order_monitor.wait_for_order_fill(
                    limit_order,
                    first_adapter,
                    timeout=limit_timeout,
                    require_full_fill=True,
                )
                filled_decimal = await self._resolve_filled_quantity(
                    order=limit_order,
                    adapter=first_adapter,
                    symbol=first_symbol,
                    reported_filled=filled_quantity,
                )

                if filled_decimal > epsilon:
                    accumulated_fill += filled_decimal
                    has_reached_target = accumulated_fill + epsilon >= requested_quantity
                    if has_reached_target:
                        logger.info(
                            f"✅ [套利执行] [{first_tag}] 限价腿已累计成交 {accumulated_fill}，达到拆单目标 {requested_quantity}"
                        )
                        break
                    logger.info(
                        f"[套利执行] [{first_tag}] 限价订单成交: order_id={limit_order.id}, "
                        f"成交数量={filled_decimal} (累计={accumulated_fill})"
                    )
                else:
                    if accumulated_fill <= epsilon:
                        # 🔥 0成交场景：先尝试取消订单，再判断是否真的0成交
                        if limit_order and limit_order.id:
                            cancel_success = False
                            extra_wait_fill: Optional[Decimal] = None
                            
                            # 检查 WS 是否已推送取消
                            if not executor.order_monitor.consume_ws_cancelled(limit_order.id):
                                cancel_success = await executor._cancel_order(
                                    first_adapter,
                                    limit_order.id,
                                    first_symbol,
                                )
                                
                                # 如果取消失败，额外等待确认是否已成交（处理竞态问题）
                                if not cancel_success:
                                    extra_wait_fill = await self._wait_fill_after_cancel_failure(
                                        order=limit_order,
                                        adapter=first_adapter,
                                        symbol=first_symbol,
                                        target_quantity=requested_quantity,
                                        exchange_tag=first_tag,
                                    )
                            else:
                                cancel_success = True
                            terminal_fill = await self._resolve_filled_quantity(
                                order=limit_order, adapter=first_adapter,
                                symbol=first_symbol, reported_filled=None,
                            )
                            if terminal_fill > (extra_wait_fill or Decimal("0")):
                                extra_wait_fill = terminal_fill
                            
                            # 🔁 处理撤单失败后检测到的成交
                            if extra_wait_fill is not None and extra_wait_fill > epsilon:
                                accumulated_fill = extra_wait_fill
                                filled_decimal = extra_wait_fill
                                logger.info(
                                    f"✅ [套利执行] [{first_tag}] 撤单失败后检测到订单成交: "
                                    f"order_id={limit_order.id}, 成交数量={accumulated_fill}"
                                )
                                # 检查是否已吃满目标
                                if accumulated_fill + epsilon >= requested_quantity:
                                    logger.info(
                                        f"✅ [套利执行] [{first_tag}] 撤单失败但已完全成交，达到拆单目标 {requested_quantity}"
                                    )
                                    break  # 成功，跳出循环进入第二腿
                                # 如果部分成交，继续下一轮补单
                                logger.info(
                                    f"[套利执行] [{first_tag}] 撤单失败后部分成交 {accumulated_fill}，继续补单 (剩余 {requested_quantity - accumulated_fill})"
                                )
                            elif not cancel_success and extra_wait_fill is None:
                                # 取消失败且无成交确认，订单可能遗留
                                logger.warning(
                                    f"⚠️ [套利执行] [{first_tag}] 限价订单取消失败且无成交确认，"
                                    f"订单可能遗留在交易所: order_id={limit_order.id}"
                                )
                        
                        # 如果仍然是 0 成交，设置中止标志并结束
                        if accumulated_fill <= epsilon:
                            zero_fill_abort = True
                            zero_fill_order_id = limit_order.id
                            break
                    logger.warning(
                        f"[套利执行] [{first_tag}] 本次限价单在超时前未追加成交: order_id={limit_order.id}, "
                        f"累计成交仍为 {accumulated_fill}"
                    )

                remaining_quantity = max(Decimal("0"), requested_quantity - accumulated_fill)
                logger.warning(
                    f"⚠️ [套利执行] [{first_tag}] 限价订单未完全成交，剩余 {remaining_quantity}，准备重新挂单"
                )

                if limit_order and limit_order.id:
                    if not executor.order_monitor.consume_ws_cancelled(limit_order.id):
                        await executor._cancel_order(
                            first_adapter,
                            limit_order.id,
                            first_symbol,
                        )

                # 🔁 撤单过程中可能又吃满，需要重新确认累计成交量
                if (
                    limit_order
                    and accumulated_fill + epsilon < requested_quantity
                ):
                    refreshed_total = await self._resolve_filled_quantity(
                        order=limit_order,
                        adapter=first_adapter,
                        symbol=first_symbol,
                        reported_filled=None,  # 强制刷新
                    )
                    if refreshed_total > filled_decimal + epsilon:
                        extra_fill = refreshed_total - filled_decimal
                        accumulated_fill += extra_fill
                        logger.info(
                            f"ℹ️ [套利执行] [{first_tag}] 限价订单在撤单流程中新增成交 {extra_fill} (累计={accumulated_fill})"
                        )
                        if accumulated_fill + epsilon >= requested_quantity:
                            logger.info(
                                f"✅ [套利执行] [{first_tag}] 撤单期间已吃满拆单目标 {requested_quantity}，结束补单"
                            )
                            break

                if limit_attempt < max_partial_attempts:
                    logger.info(
                        f"[套利执行] [{first_tag}] 累计成交 {accumulated_fill} 未达到拆单目标，继续限价补单 (剩余 {remaining_quantity})"
                    )
                else:
                    logger.warning(
                        f"[套利执行] [{first_tag}] 限价补单次数已用尽，累计成交 {accumulated_fill}，剩余 {remaining_quantity}"
                    )

            if zero_fill_abort:
                logger.warning(
                    f"⚠️ [套利执行] [{first_tag}] 限价订单在超时前无任何成交，结束本轮套利 (order_id={zero_fill_order_id})"
                )
                return self._result_failure(
                    request,
                    f"限价订单未获得有效成交 ({first_exchange})",
                    mark_waiting=False,
                    failure_code="limit_zero_fill",
                )

            filled_decimal = accumulated_fill
            if filled_decimal + epsilon < requested_quantity:
                remaining_quantity = max(Decimal("0"), requested_quantity - filled_decimal)
                logger.warning(
                    f"[套利执行] [{first_tag}] 累计成交 {filled_decimal} 仍低于拆单目标 {requested_quantity}，"
                    f"剩余 {remaining_quantity}，执行紧急平仓"
                )
                await executor._emergency_close_position(
                    exchange=first_exchange,
                    adapter=first_adapter,
                    symbol=first_symbol,
                    quantity=filled_decimal,
                    is_buy_to_close=not execution_plan["first_is_buy"],
                    request=request,
                )
                return self._result_failure(
                    request,
                    f"限价订单累计成交不足拆单目标 ({first_exchange})",
                    partial_failure=True,
                    failed_exchange=second_exchange,
                    success_exchange=first_exchange,
                    success_quantity=filled_decimal,
                    failure_code="limit_incomplete_fill",
                )

            if (
                min_second_leg_qty
                and min_second_leg_qty > Decimal("0")
                and filled_decimal < min_second_leg_qty
            ):
                logger.warning(
                    f"[套利执行] [{first_tag}] 累计成交 {filled_decimal} 低于 {second_exchange} 最小下单量 "
                    f"{min_second_leg_qty}，取消本次拆单"
                )
                await executor._emergency_close_position(
                    exchange=first_exchange,
                    adapter=first_adapter,
                    symbol=first_symbol,
                    quantity=filled_decimal,
                    is_buy_to_close=not execution_plan["first_is_buy"],
                    request=request,
                )
                return self._result_failure(
                    request,
                    f"限价订单累计成交不足最小市价量 ({first_exchange})",
                    partial_failure=True,
                    failed_exchange=second_exchange,
                    success_exchange=first_exchange,
                    success_quantity=filled_decimal,
                    failure_code="limit_min_qty_unmet",
                )

            second_order_mode = execution_plan.get("second_order_mode", "market")
            order_type_label = "激进限价吃单" if second_order_mode == "limit" else "市价订单"

            second_symbol = sell_symbol if execution_plan["first_is_buy"] else buy_symbol
            second_exchange_config = self.executor._get_exchange_mode_config(
                second_exchange,
                second_symbol,
            )
            second_uses_tick_precision = bool(
                getattr(second_exchange_config, "use_tick_precision", False)
            )
            smart_second_leg_price = bool(
                getattr(second_exchange_config, "smart_second_leg_price", False)
            )
            first_leg_clean_fill = limit_attempt == 1
            reuse_signal_price_second_leg = bool(
                getattr(second_exchange_config, "reuse_signal_price_when_second_leg", False)
            )
            if smart_second_leg_price:
                reuse_signal_price_second_leg = first_leg_clean_fill
                logger.info(
                    f"[套利执行] [{second_tag}] 智能价格模式 -> "
                    f"{'信号价' if reuse_signal_price_second_leg else '最新盘口价'} "
                    f"(第一腿尝试次数={limit_attempt})"
                )
            if smart_second_leg_price:
                price_mode_label = f"智能-{ '信号价' if reuse_signal_price_second_leg else '最新盘口'}"
            elif reuse_signal_price_second_leg:
                price_mode_label = "固定-信号价"
            else:
                price_mode_label = "最新盘口"
            effective_second_offset: Optional[Decimal] = None
            signal_price_for_second_leg = executor._to_decimal_value(
                request.price_buy if execution_plan["second_is_buy"] else request.price_sell
            )

            try:
                if second_order_mode == "limit":
                    # 🔥 使用激进限价单吃单（force_sequential_limit 模式）
                    second_price: Optional[Decimal]
                    if reuse_signal_price_second_leg:
                        second_price = signal_price_for_second_leg
                        if not second_price or second_price <= Decimal("0"):
                            second_price = executor._get_live_market_price(
                                second_exchange,
                                second_symbol,
                                execution_plan["second_is_buy"],
                            ) or signal_price_for_second_leg
                    else:
                        second_price = executor._get_live_market_price(
                            second_exchange,
                            second_symbol,
                            execution_plan["second_is_buy"],
                        )
                        if not second_price or second_price <= Decimal("0"):
                            # 如果获取不到实时价格，使用请求中的价格
                            second_price = signal_price_for_second_leg
                    second_price = executor._to_decimal_value(second_price or Decimal("0"))
                    
                    second_offset = (
                        request.limit_price_offset_buy if execution_plan["second_is_buy"]
                        else request.limit_price_offset_sell
                    )
                    effective_second_offset = (
                        None if second_uses_tick_precision else second_offset
                    )
                    
                    # 🔥 计算实际下单价格（用于日志显示）
                    # 获取交易所配置
                    exchange_config = second_exchange_config or executor.config.exchange_order_modes.get(second_exchange)
                    limit_price_offset_pct = (
                        getattr(exchange_config, 'limit_price_offset', 0.001)
                        if exchange_config else 0.001
                    )
                    
                    # 计算激进价格（与 _place_limit_order 中的逻辑一致）
                    if effective_second_offset and effective_second_offset > Decimal('0'):
                        # 如果有绝对偏移，使用绝对偏移
                        if execution_plan["second_is_buy"]:
                            aggressive_price = second_price + effective_second_offset
                        else:
                            aggressive_price = second_price - effective_second_offset
                    else:
                        # 否则使用百分比偏移
                        if execution_plan["second_is_buy"]:
                            aggressive_price = second_price * (Decimal('1') + Decimal(str(limit_price_offset_pct)))
                        else:
                            aggressive_price = second_price * (Decimal('1') - Decimal(str(limit_price_offset_pct)))
                    
                    second_best_bid: Optional[Decimal] = None
                    second_best_ask: Optional[Decimal] = None
                    data_processor = getattr(executor, "data_processor", None)
                    if data_processor:
                        try:
                            second_orderbook = data_processor.get_orderbook(
                                second_exchange,
                                second_symbol,
                                max_age_seconds=executor.data_processor.data_freshness_seconds,
                            )
                            if second_orderbook:
                                if getattr(second_orderbook, "best_bid", None):
                                    second_best_bid = executor._to_decimal_value(
                                        second_orderbook.best_bid.price
                                    )
                                if getattr(second_orderbook, "best_ask", None):
                                    second_best_ask = executor._to_decimal_value(
                                        second_orderbook.best_ask.price
                                    )
                        except Exception as snapshot_err:  # pragma: no cover - snapshot仅用于日志
                            logger.debug(
                                f"[套利执行] [{second_tag}] 记录盘口快照失败: {snapshot_err}"
                            )
                    bid_display = (
                        f"{second_best_bid:.5f}"
                        if isinstance(second_best_bid, Decimal)
                        else "-"
                    )
                    ask_display = (
                        f"{second_best_ask:.5f}"
                        if isinstance(second_best_ask, Decimal)
                        else "-"
                    )

                    logger.info(
                        f"[套利执行] [{second_tag}] 提交{order_type_label}: "
                        f"方向={'买入' if second_is_buy else '卖出'}, "
                        f"盘口Snapshot=Ask:{ask_display}/Bid:{bid_display}, "
                        f"市场价={second_price}, 下单价={aggressive_price}, "
                        f"价格模式={price_mode_label}, "
                        f"数量={filled_decimal} (根据第一腿实际成交数量)"
                    )
                    
                    market_order = await executor._place_limit_order(
                        second_adapter,
                        second_symbol,
                        second_price,
                        filled_decimal,
                        is_buy=second_is_buy,
                        absolute_offset=effective_second_offset,
                        request=request,
                    )
                else:
                    logger.info(
                        f"[套利执行] [{second_tag}] 提交{order_type_label}: "
                        f"方向={'买入' if second_is_buy else '卖出'}, "
                        f"数量={filled_decimal} (根据第一腿实际成交数量)"
                    )
                    # 原有的市价单逻辑
                    market_order = await executor._place_market_order(
                        second_adapter,
                        second_symbol,
                        filled_decimal,
                        is_buy=second_is_buy,
                        reduce_only=(not request.is_open),
                        request=request,
                    )
            except self.ReduceOnlyRestrictionError:
                await executor.reduce_only_handler.handle_reduce_only_after_partial(
                    request=request,
                    completed_exchange=first_exchange,
                    completed_adapter=first_adapter,
                    completed_symbol=first_symbol,
                    completed_quantity=filled_decimal,
                    completed_was_buy=execution_plan["first_is_buy"],
                    failing_exchange=second_exchange,
                    failing_symbol=second_symbol,
                    closing_issue=(not request.is_open),
                )
                return self._result_failure(
                    request,
                    f"{second_exchange} reduce-only 限制，已撤销已成交腿",
                    partial_failure=True,
                    order_buy=limit_order if execution_plan["first_is_buy"] else None,
                    order_sell=limit_order if not execution_plan["first_is_buy"] else None,
                )

            if not market_order:
                if second_order_mode == "limit":
                    # 限价单重试
                    if reuse_signal_price_second_leg:
                        second_price_retry = signal_price_for_second_leg
                    else:
                        second_price_retry = executor._get_live_market_price(
                            second_exchange,
                            second_symbol,
                            second_is_buy,
                        ) or signal_price_for_second_leg
                    second_price_retry = executor._to_decimal_value(second_price_retry or Decimal("0"))
                    
                    retry_market = await self._retry_limit_submission(
                        adapter=second_adapter,
                        symbol=second_symbol,
                        price=second_price_retry,
                        quantity=filled_decimal,
                        is_buy=second_is_buy,
                        absolute_offset=effective_second_offset,
                        request=request,
                        exchange_name=second_exchange,
                    )
                else:
                    # 市价单重试
                    retry_market = await self._retry_market_submission(
                        adapter=second_adapter,
                        symbol=second_symbol,
                        quantity=filled_decimal,
                        is_buy=second_is_buy,
                        reduce_only=(not request.is_open),
                        request=request,
                        exchange_name=second_exchange,
                    )
                market_order = retry_market

            if not market_order:
                return await self._fail_limit_market_second_leg(
                    request=request,
                    limit_order=limit_order,
                    market_order=None,
                    execution_plan=execution_plan,
                    first_adapter=first_adapter,
                    first_exchange=first_exchange,
                    first_symbol=first_symbol,
                    filled_quantity=filled_decimal,
                    failed_exchange=second_exchange,
                    error_message=f"{order_type_label}提交失败 ({second_exchange})",
                )

            logger.info(
                f"[套利执行] [{second_tag}] {order_type_label}已提交: order_id={market_order.id}, 数量={filled_decimal}"
            )
            limit_completion_logged = False
            try:
                if second_order_mode == "limit":
                    # 🔥 激进限价单使用不同的等待逻辑
                    await executor.order_monitor.wait_for_order_fill(
                        market_order,
                        second_adapter,
                        limit_timeout,
                    )
                    market_filled_dec = executor._to_decimal_value(market_order.filled)
                    
                    # 🔥 检查是否完全成交
                    if market_filled_dec >= filled_decimal - epsilon:
                        # 完全成交，成功
                        logger.info(
                            f"[套利执行] [{second_tag}] 激进限价单完全成交: order_id={market_order.id}, 成交数量={market_filled_dec}"
                        )
                        limit_completion_logged = True
                    else:
                        # 🔥 未吃满（包含0成交）：先撤单，再用市价补齐，按市价重试策略逐步放大滑点
                        shortfall = max(Decimal("0"), filled_decimal - market_filled_dec)
                        if shortfall <= epsilon:
                            shortfall = Decimal("0")
                        warn_prefix = "部分成交" if market_filled_dec > epsilon else "未成交"
                        logger.warning(
                            f"⚠️ [套利执行] [{second_tag}] 激进限价单{warn_prefix}，缺口 {shortfall}，将改走市价补齐。"
                        )

                        need_market_supplement = True
                        extra_wait_fill: Optional[Decimal] = None

                        if not executor.order_monitor.consume_ws_cancelled(market_order.id):
                            cancel_success = await executor._cancel_order(
                                second_adapter,
                                market_order.id,
                                second_symbol,
                            )
                            if not cancel_success:
                                extra_wait_fill = await self._wait_fill_after_cancel_failure(
                                    order=market_order,
                                    adapter=second_adapter,
                                    symbol=second_symbol,
                                    target_quantity=filled_decimal,
                                    exchange_tag=second_tag,
                                )
                        else:
                            cancel_success = True

                        if cancel_success:
                            terminal_filled = await self._resolve_filled_quantity(order=market_order, adapter=second_adapter, symbol=second_symbol, reported_filled=None)
                            extra_wait_fill = max(extra_wait_fill or Decimal("0"), terminal_filled)
                        if extra_wait_fill is not None:
                            market_filled_dec = max(market_filled_dec, extra_wait_fill)
                            shortfall = max(Decimal("0"), filled_decimal - market_filled_dec)
                            if market_order:
                                market_order.filled = market_filled_dec
                            if shortfall <= epsilon:
                                need_market_supplement = False
                                limit_completion_logged = True
                                logger.info(
                                    f"✅ [套利执行] [{second_tag}] 撤单失败后的额外等待检测到成交，"
                                    f"order_id={market_order.id}, 成交数量={market_filled_dec}"
                                )

                        if need_market_supplement:
                            try:
                                supplement_order, supplement_filled_dec = await self._confirm_market_fill_with_retries(
                                    initial_order=None,
                                    adapter=second_adapter,
                                    symbol=second_symbol,
                                    quantity=shortfall,
                                    is_buy=execution_plan["second_is_buy"],
                                    reduce_only=(not request.is_open),
                                    request=request,
                                    exchange_name=second_exchange,
                                    enable_internal_aggressive_retry=True,
                                )
                                if supplement_filled_dec + epsilon < shortfall:
                                    logger.error(
                                        f"❌ [套利执行] [{second_tag}] 市价补齐多次失败，"
                                        f"仅成交 {supplement_filled_dec} / 目标 {shortfall}"
                                    )
                                    return await self._fail_limit_market_second_leg(
                                        request=request,
                                        limit_order=limit_order,
                                        market_order=market_order,
                                        execution_plan=execution_plan,
                                        first_adapter=first_adapter,
                                        first_exchange=first_exchange,
                                        first_symbol=first_symbol,
                                        filled_quantity=market_filled_dec,
                                        failed_exchange=second_exchange,
                                        error_message=f"激进限价缺口市价补单失败 ({second_exchange})",
                                    )

                                market_filled_dec += supplement_filled_dec
                                if supplement_order:
                                    market_order = supplement_order
                                if market_order:
                                    market_order.filled = market_filled_dec
                                logger.info(
                                    f"✅ [套利执行] [{second_tag}] 市价补单成功: 补单量={supplement_filled_dec}, "
                                    f"总成交={market_filled_dec}"
                                )
                                limit_completion_logged = True

                            except self.ReduceOnlyRestrictionError:
                                logger.error(f"❌ [套利执行] [{second_tag}] 市价补单遇到 reduce-only 限制")
                                raise
                            except OrderStateUnknownError:
                                raise
                            except Exception as market_err:
                                logger.error(
                                    f"❌ [套利执行] [{second_tag}] 市价补单异常: {market_err}",
                                    exc_info=True,
                                )
                                return await self._fail_limit_market_second_leg(
                                    request=request,
                                    limit_order=limit_order,
                                    market_order=market_order,
                                    execution_plan=execution_plan,
                                    first_adapter=first_adapter,
                                    first_exchange=first_exchange,
                                    first_symbol=first_symbol,
                                    filled_quantity=market_filled_dec,
                                    failed_exchange=second_exchange,
                                    error_message=f"激进限价缺口市价补单异常 ({second_exchange})",
                                )
                else:
                    # 原有的市价单确认逻辑
                    market_order, market_filled_dec = await self._confirm_market_fill_with_retries(
                        initial_order=market_order,
                        adapter=second_adapter,
                        symbol=second_symbol,
                        quantity=filled_decimal,
                        is_buy=execution_plan["second_is_buy"],
                        reduce_only=(not request.is_open),
                        request=request,
                        exchange_name=second_exchange,
                    )
            except self.ReduceOnlyRestrictionError:
                await executor.reduce_only_handler.handle_reduce_only_after_partial(
                    request=request,
                    completed_exchange=first_exchange,
                    completed_adapter=first_adapter,
                    completed_symbol=first_symbol,
                    completed_quantity=filled_decimal,
                    completed_was_buy=execution_plan["first_is_buy"],
                    failing_exchange=second_exchange,
                    failing_symbol=second_symbol,
                    closing_issue=(not request.is_open),
                )
                return self._result_failure(
                    request,
                    f"{second_exchange} reduce-only 限制，已撤销已成交腿",
                    partial_failure=True,
                    order_buy=limit_order if execution_plan["first_is_buy"] else market_order,
                    order_sell=limit_order if not execution_plan["first_is_buy"] else market_order,
                )

            if market_filled_dec <= epsilon:
                order_type_display = "激进限价单" if second_order_mode == "limit" else "市价订单"
                logger.warning(
                    f"[套利执行] [{second_tag}] {order_type_display}未在等待窗口内确认成交: "
                    f"order_id={market_order.id if market_order else 'unknown'}"
                )
                return await self._fail_limit_market_second_leg(
                    request=request,
                    limit_order=limit_order,
                    market_order=market_order,
                    execution_plan=execution_plan,
                    first_adapter=first_adapter,
                    first_exchange=first_exchange,
                    first_symbol=first_symbol,
                    filled_quantity=filled_decimal,
                    failed_exchange=second_exchange,
                    error_message=f"{order_type_display}超时未成交 ({second_exchange})",
            )

            result_order_buy = limit_order if execution_plan["first_is_buy"] else market_order
            result_order_sell = limit_order if not execution_plan["first_is_buy"] else market_order

            order_type_display = "激进限价单" if second_order_mode == "limit" else "市价订单"
            if not (second_order_mode == "limit" and limit_completion_logged):
                logger.info(
                    f"[套利执行] [{second_tag}] {order_type_display}成交确认: "
                    f"order_id={market_order.id if market_order else 'unknown'}, "
                    f"成交数量={market_filled_dec}"
                )
            executor._log_execution_summary(
                request=request,
                order_buy=result_order_buy,
                order_sell=result_order_sell,
                is_open=request.is_open,
                is_last_split=request.is_last_split,
            )

            return Result(
                success=True,
                order_buy=result_order_buy,
                order_sell=result_order_sell,
                success_quantity=filled_decimal,
            )

        except OrderStateUnknownError as exc:
            self._mark_manual_intervention(request, str(exc))
            return self._unknown_result(request, exc,
                limit_order if execution_plan['first_is_buy'] else market_order,
                market_order if execution_plan['first_is_buy'] else limit_order)
        except Exception as exc:  # pylint: disable=broad-except
            logger.error(f"[套利执行] 限价+市场模式执行失败: {exc}", exc_info=True)
            return self._result_failure(
                request,
                str(exc),
            )

    async def execute_limit_limit_mode(
        self,
        request: "ExecutionRequest",
    ) -> "ExecutionResult":
        executor = self.executor
        Result = self.ExecutionResult

        buy_adapter = executor.exchange_adapters.get(request.exchange_buy)
        sell_adapter = executor.exchange_adapters.get(request.exchange_sell)
        if not buy_adapter or not sell_adapter:
            return self._result_failure(
                request,
                f"交易所适配器不存在: {request.exchange_buy} 或 {request.exchange_sell}",
                mark_waiting=False,
            )

        buy_symbol = request.buy_symbol or request.symbol
        sell_symbol = request.sell_symbol or request.symbol
        quantity = request.quantity or Decimal('0')
        epsilon = Decimal("0.00000001")
        limit_timeout = getattr(
            executor.config.order_execution,
            "limit_order_timeout",
            60,
        ) or 60

        buy_order = sell_order = None
        try:
            logger.info(
                f"[套利执行] 双限价模式：同时提交限价订单，"
                f"买入={request.exchange_buy}@{request.price_buy}, "
                f"卖出={request.exchange_sell}@{request.price_sell}, 数量={quantity}"
            )
            buy_offset = request.limit_price_offset_buy
            sell_offset = request.limit_price_offset_sell

            buy_order_task = asyncio.create_task(
                executor._place_limit_order(
                    buy_adapter,
                    buy_symbol,
                    request.price_buy,
                    quantity,
                    is_buy=True,
                    absolute_offset=buy_offset,
                    request=request,
                )
            )
            sell_order_task = asyncio.create_task(
                executor._place_limit_order(
                    sell_adapter,
                    sell_symbol,
                    request.price_sell,
                    quantity,
                    is_buy=False,
                    absolute_offset=sell_offset,
                    request=request,
                )
            )

            buy_order_raw, sell_order_raw = await asyncio.gather(
                buy_order_task,
                sell_order_task,
                return_exceptions=True,
            )

            buy_order = buy_order_raw if not isinstance(buy_order_raw, Exception) else None
            sell_order = sell_order_raw if not isinstance(sell_order_raw, Exception) else None
            for outcome in (buy_order_raw, sell_order_raw):
                if isinstance(outcome, OrderStateUnknownError):
                    raise outcome
            buy_error = buy_order_raw if isinstance(buy_order_raw, self.ReduceOnlyRestrictionError) else None
            sell_error = sell_order_raw if isinstance(sell_order_raw, self.ReduceOnlyRestrictionError) else None

            buy_order = buy_order_raw if not isinstance(buy_order_raw, Exception) else None
            sell_order = sell_order_raw if not isinstance(sell_order_raw, Exception) else None

            if buy_error:
                return await self._handle_reduce_only_submission_failure(
                    request=request,
                    failing_exchange=request.exchange_buy,
                    failing_symbol=buy_symbol,
                    failing_is_buy=True,
                    other_order=sell_order,
                    other_adapter=sell_adapter,
                    other_exchange=request.exchange_sell,
                    other_symbol=sell_symbol,
                    other_is_buy=False,
                    epsilon=epsilon,
                    limit_timeout=limit_timeout,
                )

            if sell_error:
                return await self._handle_reduce_only_submission_failure(
                    request=request,
                    failing_exchange=request.exchange_sell,
                    failing_symbol=sell_symbol,
                    failing_is_buy=False,
                    other_order=buy_order,
                    other_adapter=buy_adapter,
                    other_exchange=request.exchange_buy,
                    other_symbol=buy_symbol,
                    other_is_buy=True,
                    epsilon=epsilon,
                    limit_timeout=limit_timeout,
                )

            if isinstance(buy_order_raw, Exception):
                raise buy_order_raw
            if isinstance(sell_order_raw, Exception):
                raise sell_order_raw

            if not buy_order:
                buy_order = await self._retry_limit_submission(
                    adapter=buy_adapter,
                    symbol=buy_symbol,
                    price=request.price_buy,
                    quantity=quantity,
                    is_buy=True,
                    absolute_offset=buy_offset,
                    request=request,
                    exchange_name=request.exchange_buy,
                )

            if not sell_order:
                sell_order = await self._retry_limit_submission(
                    adapter=sell_adapter,
                    symbol=sell_symbol,
                    price=request.price_sell,
                    quantity=quantity,
                    is_buy=False,
                    absolute_offset=sell_offset,
                    request=request,
                    exchange_name=request.exchange_sell,
                )

            if not buy_order and not sell_order:
                return self._result_failure(
                    request,
                    "双限价订单均提交失败",
                )

            if (not buy_order or not sell_order) and any(
                getattr(adapter, 'strict_order_state', False) for adapter in (buy_adapter, sell_adapter)
            ):
                known = buy_order or sell_order
                known_adapter = buy_adapter if buy_order else sell_adapter
                known_symbol = buy_symbol if buy_order else sell_symbol
                known_exchange = request.exchange_buy if buy_order else request.exchange_sell
                if known.status not in (OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED):
                    if not await executor._cancel_order(known_adapter, known.id, known_symbol):
                        raise OrderStateUnknownError(known_exchange, order_id=known.id)
                    try:
                        latest = await known_adapter.get_order(known.id, known_symbol)
                        if latest is None:
                            raise OrderStateUnknownError(known_exchange, order_id=known.id)
                        previous_filled = executor._to_decimal_value(known.filled)
                        latest_filled = executor._to_decimal_value(latest.filled)
                        if latest_filled >= previous_filled:
                            executor.order_monitor._merge_order_state(known, latest)
                        # A stale cancellation snapshot cannot erase confirmed fills or their valuation.
                        known.filled = max(previous_filled, latest_filled)
                        known.remaining = max(Decimal('0'), known.amount - known.filled)
                    except OrderStateUnknownError:
                        raise
                    except Exception as exc:
                        raise OrderStateUnknownError(known_exchange, order_id=known.id) from exc
                # Cancellation does not erase fills. Preserve the known leg and quarantine the pair.
                raise OrderStateUnknownError(known_exchange, order_id=known.id)

            if not buy_order:
                if sell_order:
                    await executor._cancel_order(sell_adapter, sell_order.id, sell_symbol)
                return self._result_failure(
                    request,
                    f"买入限价订单提交失败 ({request.exchange_buy})",
                )

            if not sell_order:
                await executor._cancel_order(buy_adapter, buy_order.id, buy_symbol)
                return self._result_failure(
                    request,
                    f"卖出限价订单提交失败 ({request.exchange_sell})",
                )

            buy_fill_task = asyncio.create_task(
                executor.order_monitor.wait_for_order_fill(buy_order, buy_adapter, timeout=limit_timeout)
            )
            sell_fill_task = asyncio.create_task(
                executor.order_monitor.wait_for_order_fill(sell_order, sell_adapter, timeout=limit_timeout)
            )
            buy_filled, sell_filled = await asyncio.gather(buy_fill_task, sell_fill_task, return_exceptions=True)
            for outcome in (buy_filled, sell_filled):
                if isinstance(outcome, Exception):
                    raise outcome

            buy_filled_dec = await self._resolve_filled_quantity(
                order=buy_order,
                adapter=buy_adapter,
                symbol=buy_symbol,
                reported_filled=buy_filled,
            )
            sell_filled_dec = await self._resolve_filled_quantity(
                order=sell_order,
                adapter=sell_adapter,
                symbol=sell_symbol,
                reported_filled=sell_filled,
            )

            def _is_completed(filled_dec: Decimal) -> bool:
                return filled_dec + epsilon >= quantity

            buy_completed = _is_completed(buy_filled_dec)
            sell_completed = _is_completed(sell_filled_dec)

            if buy_completed and sell_completed:
                buy_order.filled = buy_filled_dec
                sell_order.filled = sell_filled_dec
                executor._log_execution_summary(
                    request=request,
                    order_buy=buy_order,
                    order_sell=sell_order,
                    is_open=request.is_open,
                    is_last_split=request.is_last_split,
                )
                actual_quantity = self._calculate_success_quantity_from_orders(
                    buy_order,
                    sell_order,
                    fallback=quantity,
                )
                return Result(
                    success=True,
                    order_buy=buy_order,
                    order_sell=sell_order,
                    success_quantity=actual_quantity,
                )

            # ✅ 只有一条腿成交（另一条腿0成交）→ 立即取消未成交腿并启动补仓流程
            if buy_completed ^ sell_completed:
                missing_buy = not buy_completed
                shortfall = (
                    sell_filled_dec - buy_filled_dec
                    if missing_buy else
                    buy_filled_dec - sell_filled_dec
                )
                if shortfall > epsilon:
                    logger.warning(
                        f"⚠️ [套利执行] 双限价模式出现单腿成交，"
                        f"{'买腿' if missing_buy else '卖腿'} 缺口 {shortfall}，"
                        "先激进限价补仓，剩余再市价兜底"
                    )
                    return await self._handle_single_leg_shortfall(
                        request=request,
                        buy_order=buy_order,
                        sell_order=sell_order,
                        buy_adapter=buy_adapter,
                        sell_adapter=sell_adapter,
                        buy_symbol=buy_symbol,
                        sell_symbol=sell_symbol,
                        missing_buy=missing_buy,
                        shortfall=shortfall,
                        target_quantity=quantity,
                        epsilon=epsilon,
                        cancel_opposite_order=False,
                        result_quantity=Decimal(str(quantity)),
                    )

            if not buy_completed and not sell_completed:
                has_buy_fill = buy_filled_dec > epsilon
                has_sell_fill = sell_filled_dec > epsilon

                if not has_buy_fill and not has_sell_fill:
                    await executor._cancel_order(buy_adapter, buy_order.id, buy_symbol)
                    await executor._cancel_order(sell_adapter, sell_order.id, sell_symbol)
                    logger.info(
                        "↩️ [套利执行] 双限价腿均 0 成交，已取消 buy=%s / sell=%s",
                        buy_order.id,
                        sell_order.id,
                    )
                    refreshed_buy = await self._resolve_filled_quantity(
                        order=buy_order,
                        adapter=buy_adapter,
                        symbol=buy_symbol,
                        reported_filled=None,
                    )
                    refreshed_sell = await self._resolve_filled_quantity(
                        order=sell_order,
                        adapter=sell_adapter,
                        symbol=sell_symbol,
                        reported_filled=None,
                    )
                    if refreshed_buy > epsilon or refreshed_sell > epsilon:
                        buy_filled_dec = refreshed_buy
                        sell_filled_dec = refreshed_sell
                        has_buy_fill = buy_filled_dec > epsilon
                        has_sell_fill = sell_filled_dec > epsilon
                        logger.info(
                            "ℹ️ [套利执行] 取消双限价后检测到延迟成交: buy=%s, sell=%s",
                            buy_filled_dec,
                            sell_filled_dec,
                        )
                    if not has_buy_fill and not has_sell_fill:
                        return self._result_failure(
                            request,
                            "双限价订单均未成交",
                            order_buy=buy_order,
                            order_sell=sell_order,
                            failure_code="dual_limit_no_fill",
                            mark_waiting=False,
                        )

                if has_buy_fill ^ has_sell_fill:
                    missing_buy = not has_buy_fill
                    fallback_quantity = (
                        sell_filled_dec - buy_filled_dec
                        if missing_buy else
                        buy_filled_dec - sell_filled_dec
                    )
                    if fallback_quantity > epsilon:
                        return await self._handle_single_leg_shortfall(
                            request=request,
                            buy_order=buy_order,
                            sell_order=sell_order,
                            buy_adapter=buy_adapter,
                            sell_adapter=sell_adapter,
                            buy_symbol=buy_symbol,
                            sell_symbol=sell_symbol,
                            missing_buy=missing_buy,
                            shortfall=fallback_quantity,
                            target_quantity=quantity,
                            epsilon=epsilon,
                            cancel_opposite_order=True,
                            result_quantity=fallback_quantity,
                        )
                    return self._result_failure(
                        request,
                        "单边成交缺口无法识别",
                        order_buy=buy_order,
                        order_sell=sell_order,
                    )

                buy_remaining = (quantity - buy_filled_dec).copy_abs()
                sell_remaining = (quantity - sell_filled_dec).copy_abs()
                fill_failed = False

                async def _complete_leg(remaining: Decimal, *, is_buy: bool) -> Decimal:
                    exchange_name = request.exchange_buy if is_buy else request.exchange_sell
                    adapter = buy_adapter if is_buy else sell_adapter
                    symbol = buy_symbol if is_buy else sell_symbol
                    leg_label = "买腿" if is_buy else "卖腿"
                    if remaining <= epsilon:
                        return Decimal("0")
                    logger.warning(
                        f"⚠️ [套利执行] 双腿部分成交，{leg_label} 剩余 {remaining} 未到达拆单目标，启动补单流程（激进限价优先）"
                    )
                    added = await self._fill_leg_to_target(
                        adapter=adapter,
                        symbol=symbol,
                        exchange_name=exchange_name,
                        is_buy=is_buy,
                        remaining_quantity=remaining,
                        request=request,
                        epsilon=epsilon,
                    )
                    return added

                try:
                    if buy_remaining > epsilon:
                        buy_filled_dec += await _complete_leg(buy_remaining, is_buy=True)
                    if sell_remaining > epsilon:
                        sell_filled_dec += await _complete_leg(sell_remaining, is_buy=False)
                except self.ReduceOnlyRestrictionError:
                    executor.reduce_only_handler.register_reduce_only_event(
                        request,
                        request.exchange_buy if buy_remaining > sell_remaining else request.exchange_sell,
                        buy_symbol if buy_remaining > sell_remaining else sell_symbol,
                        closing_issue=(not request.is_open),
                        reason="fill_to_target_reduce_only",
                    )
                    fill_failed = True
                except OrderStateUnknownError:
                    raise
                except Exception as fill_exc:
                    logger.warning(
                        f"⚠️ [套利执行] 双腿部分成交补齐至拆单量失败，将降级为差值补齐: {fill_exc}",
                        exc_info=True,
                    )
                    fill_failed = True

                still_missing_buy = (quantity - buy_filled_dec) > epsilon
                still_missing_sell = (quantity - sell_filled_dec) > epsilon
                if not fill_failed and not (still_missing_buy or still_missing_sell):
                    executor._log_execution_summary(
                        request=request,
                        order_buy=buy_order,
                        order_sell=sell_order,
                        is_open=request.is_open,
                        is_last_split=request.is_last_split,
                    )
                    return Result(
                        success=True,
                        order_buy=buy_order,
                        order_sell=sell_order,
                        success_quantity=min(buy_filled_dec, sell_filled_dec, quantity),
                    )

                # 若无法补齐拆单目标，则终止本次执行避免进一步混乱
                await executor._cancel_order(buy_adapter, buy_order.id, buy_symbol)
                await executor._cancel_order(sell_adapter, sell_order.id, sell_symbol)

                reason = "双腿部分成交补齐至拆单量失败"
                if fill_failed:
                    reason += "（补单阶段异常或受限）"
                elif still_missing_buy or still_missing_sell:
                    reason += "（市价补单成交量不足）"

                return self._result_failure(
                    request,
                    reason,
                    order_buy=buy_order,
                    order_sell=sell_order,
                    failure_code="dual_partial_fill_incomplete",
                )

            missing_buy = not buy_completed
            shortfall = (
                sell_filled_dec - buy_filled_dec
                if missing_buy else
                buy_filled_dec - sell_filled_dec
            )
            fallback_quantity = shortfall if shortfall > epsilon else Decimal('0')

            if fallback_quantity <= epsilon:
                await executor._cancel_order(buy_adapter, buy_order.id, buy_symbol)
                await executor._cancel_order(sell_adapter, sell_order.id, sell_symbol)
                return self._result_failure(
                    request,
                    "限价订单未产生需要补齐的成交差额",
                    order_buy=buy_order,
                    order_sell=sell_order,
                )

            return await self._handle_single_leg_shortfall(
                request=request,
                buy_order=buy_order,
                sell_order=sell_order,
                buy_adapter=buy_adapter,
                sell_adapter=sell_adapter,
                buy_symbol=buy_symbol,
                sell_symbol=sell_symbol,
                missing_buy=missing_buy,
                shortfall=fallback_quantity,
                target_quantity=quantity,
                epsilon=epsilon,
                cancel_opposite_order=False,
                result_quantity=Decimal(str(quantity)),
            )

        except OrderStateUnknownError as exc:
            self._mark_manual_intervention(request, str(exc))
            return self._unknown_result(request, exc, buy_order, sell_order)
        except Exception as exc:  # pylint: disable=broad-except
            logger.error(f"[套利执行] 双限价模式执行失败: {exc}", exc_info=True)
            return self._result_failure(
                request,
                str(exc),
            )
    
    async def _fail_limit_market_second_leg(
        self,
        *,
        request: "ExecutionRequest",
        limit_order: OrderData,
        market_order: Optional[OrderData],
        execution_plan: Dict[str, Any],
        first_adapter: ExchangeInterface,
        first_exchange: str,
        first_symbol: str,
        filled_quantity: Optional[Decimal],
        failed_exchange: str,
        error_message: str,
    ) -> "ExecutionResult":
        executor = self.executor
        epsilon = Decimal("0.00000001")

        first_leg_qty = executor._to_decimal_value(filled_quantity or Decimal("0"))
        second_leg_qty = Decimal("0")
        if market_order is not None:
            try:
                second_leg_qty = executor._to_decimal_value(
                    getattr(market_order, "filled", Decimal("0"))
                )
            except (InvalidOperation, TypeError, ValueError):
                second_leg_qty = Decimal("0")

        close_first_success = True
        if first_leg_qty > epsilon:
            try:
                close_first_success = await executor._emergency_close_position(
                    exchange=first_exchange,
                    adapter=first_adapter,
                    symbol=first_symbol,
                    quantity=first_leg_qty,
                    is_buy_to_close=not execution_plan["first_is_buy"],
                    request=request,
                )
            except OrderStateUnknownError:
                raise
            except Exception as exc:  # pylint: disable=broad-except
                logger.error(
                    f"❌ [套利执行] [{executor._format_exchange_tag(first_exchange)}] "
                    f"紧急平仓异常: {exc}",
                    exc_info=True,
                )
                close_first_success = False

        close_second_success = True
        if second_leg_qty > epsilon:
            second_exchange = execution_plan.get("second_exchange")
            second_symbol = (
                (request.buy_symbol or request.symbol)
                if execution_plan.get("second_is_buy")
                else (request.sell_symbol or request.symbol)
            )
            second_adapter = (
                executor.exchange_adapters.get(second_exchange)
                if second_exchange else None
            )

            if second_adapter:
                try:
                    close_second_success = await executor._emergency_close_position(
                        exchange=second_exchange,
                        adapter=second_adapter,
                        symbol=second_symbol,
                        quantity=second_leg_qty,
                        is_buy_to_close=not execution_plan.get("second_is_buy", False),
                        request=request,
                    )
                except OrderStateUnknownError:
                    raise
                except Exception as exc:  # pylint: disable=broad-except
                    logger.error(
                        f"❌ [套利执行] [{executor._format_exchange_tag(second_exchange)}] "
                        f"第二腿紧急平仓异常: {exc}",
                        exc_info=True,
                    )
                    close_second_success = False
            else:
                close_second_success = False
                logger.error(
                    f"❌ [套利执行] 第二腿适配器不存在，无法回滚 {second_exchange}"
                )

        if not close_first_success or not close_second_success:
            reason_hint = []
            if not close_first_success:
                reason_hint.append(f"{first_exchange}紧急平仓失败")
            if not close_second_success and second_leg_qty > epsilon:
                reason_hint.append(f"{execution_plan.get('second_exchange')}第二腿回滚失败")
            if reason_hint:
                self._mark_manual_intervention(
                    request,
                    "；".join(reason_hint) or "紧急平仓失败",
                )

        result_order_buy = limit_order if execution_plan["first_is_buy"] else market_order
        result_order_sell = limit_order if not execution_plan["first_is_buy"] else market_order
        if (not close_first_success or not close_second_success) and any(
            getattr(executor.exchange_adapters.get(name), 'strict_order_state', False)
            for name in (request.exchange_buy, request.exchange_sell)
        ):
            raise OrderStateUnknownError(first_exchange)

        return self._result_failure(
            request,
            error_message,
            order_buy=result_order_buy,
            order_sell=result_order_sell,
            partial_failure=True,
            failed_exchange=failed_exchange,
            success_exchange=first_exchange,
            success_quantity=first_leg_qty,
        )

    async def _handle_market_market_submission_failure(
        self,
        *,
        request: "ExecutionRequest",
        buy_order: Optional[OrderData],
        sell_order: Optional[OrderData],
        buy_adapter: ExchangeInterface,
        sell_adapter: ExchangeInterface,
        buy_symbol: str,
        sell_symbol: str,
    ) -> "ExecutionResult":
        executor = self.executor
        if not buy_order and not sell_order:
            return self._result_failure(
                request,
                "两个交易所订单都提交失败",
            )

        buy_failed = buy_order is None
        success_order = sell_order if buy_failed else buy_order
        success_adapter = sell_adapter if buy_failed else buy_adapter
        success_symbol = sell_symbol if buy_failed else buy_symbol
        success_exchange = request.exchange_sell if buy_failed else request.exchange_buy
        failed_exchange = request.exchange_buy if buy_failed else request.exchange_sell

        filled_decimal = executor._to_decimal_value(getattr(success_order, "filled", 0))
        close_failed = False
        close_error = None
        try:
            if success_order:
                _, filled_decimal = await self._confirm_market_fill_with_retries(
                    initial_order=success_order,
                    adapter=success_adapter,
                    symbol=success_symbol,
                    quantity=request.quantity,
                    is_buy=(not buy_failed),
                    reduce_only=(not request.is_open),
                    request=request,
                    exchange_name=success_exchange,
                    allow_resubmit=False,
                    max_attempts=1,
                )
                if filled_decimal > Decimal("0"):
                    closed = await executor._emergency_close_position(
                        exchange=success_exchange,
                        adapter=success_adapter,
                        symbol=success_symbol,
                        quantity=filled_decimal,
                        is_buy_to_close=buy_failed,
                        request=request,
                    )
                    close_failed = not closed
        except OrderStateUnknownError as exc:
            close_failed = True
            close_error = str(exc)
        if close_failed:
            self._mark_manual_intervention(request, close_error or '单腿补偿未确认完成，需人工核对头寸')

        return self._result_failure(
            request,
            f"{failed_exchange} 市价订单提交失败",
            order_buy=buy_order,
            order_sell=sell_order,
            partial_failure=bool(success_order),
            failed_exchange=failed_exchange,
            success_exchange=success_exchange if success_order else None,
            success_quantity=filled_decimal,
            failure_code='ORDER_STATE_UNKNOWN' if close_failed else None,
        )

    async def execute_market_market_mode(
        self,
        request: "ExecutionRequest",
    ) -> "ExecutionResult":
        executor = self.executor
        Result = self.ExecutionResult

        buy_adapter = executor.exchange_adapters.get(request.exchange_buy)
        sell_adapter = executor.exchange_adapters.get(request.exchange_sell)

        if not buy_adapter or not sell_adapter:
            return self._result_failure(
                request,
                f"交易所适配器不存在: {request.exchange_buy} 或 {request.exchange_sell}",
                mark_waiting=False,
            )

        buy_symbol = request.buy_symbol or request.symbol
        sell_symbol = request.sell_symbol or request.symbol

        if self._should_use_lighter_ws_batch(request, buy_adapter, sell_adapter):
            return await self._execute_lighter_ws_batch(
                request=request,
                adapter=buy_adapter,
                buy_symbol=buy_symbol,
                sell_symbol=sell_symbol,
            )

        buy_order = sell_order = None
        try:
            buy_order_task = asyncio.create_task(
                executor._place_market_order(
                    buy_adapter,
                    buy_symbol,
                    request.quantity,
                    is_buy=True,
                    reduce_only=(not request.is_open),
                    request=request,
                )
            )

            sell_order_task = asyncio.create_task(
                executor._place_market_order(
                    sell_adapter,
                    sell_symbol,
                    request.quantity,
                    is_buy=False,
                    reduce_only=(not request.is_open),
                    request=request,
                )
            )

            buy_order, sell_order = await asyncio.gather(buy_order_task, sell_order_task, return_exceptions=True)
            for outcome in (buy_order, sell_order):
                if isinstance(outcome, Exception):
                    raise outcome

            if not buy_order:
                buy_order = await self._retry_market_submission(
                    adapter=buy_adapter,
                    symbol=buy_symbol,
                    quantity=request.quantity,
                    is_buy=True,
                    reduce_only=(not request.is_open),
                    request=request,
                    exchange_name=request.exchange_buy,
                )
            if not sell_order:
                sell_order = await self._retry_market_submission(
                    adapter=sell_adapter,
                    symbol=sell_symbol,
                    quantity=request.quantity,
                    is_buy=False,
                    reduce_only=(not request.is_open),
                    request=request,
                    exchange_name=request.exchange_sell,
                )

            if not buy_order and not sell_order:
                logger.error(
                    f"❌ [风险控制] 两个交易所订单都失败: {request.exchange_buy}, {request.exchange_sell}"
                )
                return self._result_failure(
                    request,
                    "两个交易所订单都提交失败",
                )

            if not buy_order or not sell_order:
                return await self._handle_market_market_submission_failure(
                    request=request,
                    buy_order=buy_order,
                    sell_order=sell_order,
                    buy_adapter=buy_adapter,
                    sell_adapter=sell_adapter,
                    buy_symbol=buy_symbol,
                    sell_symbol=sell_symbol,
                )

            logger.info(
                f"[套利执行] 两个市价订单已提交: "
                f"买入订单={buy_order.id} 卖出订单={sell_order.id}"
            )

            try:
                buy_order, buy_filled_dec = await self._confirm_market_fill_with_retries(
                    initial_order=buy_order,
                    adapter=buy_adapter,
                    symbol=buy_symbol,
                    quantity=request.quantity,
                    is_buy=True,
                    reduce_only=(not request.is_open),
                    request=request,
                    exchange_name=request.exchange_buy,
                )
            except self.ReduceOnlyRestrictionError:
                executor.reduce_only_handler.register_reduce_only_event(
                    request,
                    request.exchange_buy,
                    buy_symbol,
                    closing_issue=(not request.is_open),
                    reason="买单触发 reduce-only",
                )
                return self._result_failure(
                    request,
                    f"{request.exchange_buy} 市价订单因 reduce-only 受限",
                    order_buy=buy_order,
                    order_sell=sell_order,
                )

            try:
                sell_order, sell_filled_dec = await self._confirm_market_fill_with_retries(
                    initial_order=sell_order,
                    adapter=sell_adapter,
                    symbol=sell_symbol,
                    quantity=request.quantity,
                    is_buy=False,
                    reduce_only=(not request.is_open),
                    request=request,
                    exchange_name=request.exchange_sell,
                )
            except self.ReduceOnlyRestrictionError:
                executor.reduce_only_handler.register_reduce_only_event(
                    request,
                    request.exchange_sell,
                    sell_symbol,
                    closing_issue=(not request.is_open),
                    reason="卖单触发 reduce-only",
                )
                return self._result_failure(
                    request,
                    f"{request.exchange_sell} 市价订单因 reduce-only 受限",
                    order_buy=buy_order,
                    order_sell=sell_order,
                )

            epsilon = Decimal("0.00000001")

            if buy_filled_dec <= epsilon and sell_filled_dec <= epsilon:
                logger.error("❌ [风险控制] 两个订单都未成交")
                return self._result_failure(
                    request,
                    "两个订单都未成交",
                    order_buy=buy_order,
                    order_sell=sell_order,
                )

            if buy_filled_dec <= epsilon or sell_filled_dec <= epsilon:
                buy_order.filled = buy_filled_dec
                sell_order.filled = sell_filled_dec
                filled_exchange = request.exchange_sell if buy_filled_dec <= epsilon else request.exchange_buy
                unfilled_exchange = request.exchange_buy if buy_filled_dec <= epsilon else request.exchange_sell
                filled_quantity = sell_filled_dec if buy_filled_dec <= epsilon else buy_filled_dec
                filled_adapter = sell_adapter if buy_filled_dec <= epsilon else buy_adapter
                filled_symbol = sell_symbol if buy_filled_dec <= epsilon else buy_symbol
                is_buy_unfilled = (buy_filled_dec <= epsilon)

                logger.error(
                    f"❌ [风险控制] 部分成交: {filled_exchange} 成交 {filled_quantity}, "
                    f"{unfilled_exchange} 未成交。将平掉 {filled_exchange} 订单"
                )

                await executor._emergency_close_position(
                    exchange=filled_exchange,
                    adapter=filled_adapter,
                    symbol=filled_symbol,
                    quantity=Decimal(str(filled_quantity)),
                    is_buy_to_close=is_buy_unfilled,
                    request=request,
                )

                return self._result_failure(
                    request,
                    f"{unfilled_exchange} 订单未成交，已平掉 {filled_exchange} 订单，等待人工介入",
                    order_buy=buy_order,
                    order_sell=sell_order,
                    partial_failure=True,
                    failed_exchange=unfilled_exchange,
                    success_exchange=filled_exchange,
                    success_quantity=Decimal(str(filled_quantity)),
                )

            if (getattr(buy_adapter, 'strict_order_state', False) or getattr(sell_adapter, 'strict_order_state', False)) and abs(buy_filled_dec - sell_filled_dec) > epsilon:
                # Known unequal IOC fills leave exposure: quarantine the pair for manual reconciliation.
                raise OrderStateUnknownError('arcus', order_id=f'{buy_order.id}/{sell_order.id}')

            buy_order.filled = buy_filled_dec
            sell_order.filled = sell_filled_dec
            executor._log_execution_summary(
                request=request,
                order_buy=buy_order,
                order_sell=sell_order,
                is_open=request.is_open,
                is_last_split=request.is_last_split,
            )
            actual_quantity = self._calculate_success_quantity_from_orders(
                buy_order,
                sell_order,
                fallback=request.quantity,
            )

            return Result(
                success=True,
                order_buy=buy_order,
                order_sell=sell_order,
                success_quantity=actual_quantity,
            )

        except OrderStateUnknownError as exc:
            self._mark_manual_intervention(request, str(exc))
            return self._unknown_result(request, exc, buy_order, sell_order)
        except Exception as exc:  # pylint: disable=broad-except
            logger.error(f"[套利执行] 市价+市价模式执行失败: {exc}", exc_info=True)
            return self._result_failure(
                request,
                str(exc),
            )

    def _should_use_lighter_ws_batch(
        self,
        request: "ExecutionRequest",
        buy_adapter: Optional[ExchangeInterface],
        sell_adapter: Optional[ExchangeInterface],
    ) -> bool:
        if not buy_adapter or not sell_adapter:
            return False
        buy_exchange = (request.exchange_buy or "").lower()
        sell_exchange = (request.exchange_sell or "").lower()
        if buy_exchange != sell_exchange:
            return False
        if buy_exchange != "lighter":
            return False
        if buy_adapter is not sell_adapter:
            return False
        return hasattr(buy_adapter, "place_market_orders_ws_batch")

    async def _execute_lighter_ws_batch(
        self,
        request: "ExecutionRequest",
        adapter: ExchangeInterface,
        buy_symbol: str,
        sell_symbol: str,
    ) -> "ExecutionResult":
        from .lighter_batch_executor import execute_lighter_ws_batch
        return await execute_lighter_ws_batch(
            executor_obj=self,
            request=request,
            adapter=adapter,
            buy_symbol=buy_symbol,
            sell_symbol=sell_symbol,
        )

    async def _handle_reduce_only_submission_failure(
        self,
        *,
        request: "ExecutionRequest",
        failing_exchange: str,
        failing_symbol: str,
        failing_is_buy: bool,
        other_order: Optional[OrderData],
        other_adapter: ExchangeInterface,
        other_exchange: str,
        other_symbol: str,
        other_is_buy: bool,
        epsilon: Decimal,
        limit_timeout: float,
    ) -> "ExecutionResult":
        executor = self.executor
        logger.warning(
            "⚠️ [reduce-only] %s %s 限价下单被拒绝，仅允许平仓，开始处理对腿",
            failing_exchange,
            failing_symbol,
        )

        filled_dec = Decimal("0")
        reported = None
        if other_order:
            try:
                reported = await executor.order_monitor.wait_for_order_fill(
                    other_order,
                    other_adapter,
                    timeout=limit_timeout,
                )
            except OrderStateUnknownError:
                raise
            except Exception:
                reported = None
            filled_dec = await self._resolve_filled_quantity(
                order=other_order,
                adapter=other_adapter,
                symbol=other_symbol,
                reported_filled=reported,
            )

        if filled_dec > epsilon and executor.reduce_only_handler:
            await executor.reduce_only_handler.handle_reduce_only_after_partial(
                request=request,
                completed_exchange=other_exchange,
                completed_adapter=other_adapter,
                completed_symbol=other_symbol,
                completed_quantity=filled_dec,
                completed_was_buy=other_is_buy,
                failing_exchange=failing_exchange,
                failing_symbol=failing_symbol,
                closing_issue=(not request.is_open),
            )
        elif other_order:
            await executor._cancel_order(other_adapter, other_order.id, other_symbol)

        message = (
            f"{failing_exchange} reduce-only 限制，已处理{('买腿' if other_is_buy else '卖腿')}并暂停等待探针"
        )
        return self._result_failure(
            request,
            message,
            order_buy=other_order if other_is_buy else None,
            order_sell=other_order if not other_is_buy else None,
            partial_failure=True,
        )

    async def _handle_single_leg_shortfall(
        self,
        *,
        request: "ExecutionRequest",
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
        result_quantity: Optional[Decimal] = None,
    ) -> "ExecutionResult":
        executor = self.executor
        Result = self.ExecutionResult

        fallback_exchange = request.exchange_buy if missing_buy else request.exchange_sell
        fallback_adapter = buy_adapter if missing_buy else sell_adapter
        fallback_symbol = buy_symbol if missing_buy else sell_symbol
        fallback_is_buy = missing_buy
        failing_order = buy_order if missing_buy else sell_order
        opposite_adapter = sell_adapter if missing_buy else buy_adapter
        opposite_symbol = sell_symbol if missing_buy else buy_symbol
        opposite_order = sell_order if missing_buy else buy_order
        reported_quantity = result_quantity or (target_quantity if not cancel_opposite_order else shortfall)

        leg_label = "买腿" if fallback_is_buy else "卖腿"
        completed_leg_label = "卖腿" if missing_buy else "买腿"

        logger.warning(
            f"⚠️ [套利执行] 双限价模式{'仅' if cancel_opposite_order else ''}"
            f"{completed_leg_label} 完成，{leg_label} 缺口 {shortfall}，"
            f"将在 {fallback_exchange} 启动激进限价补仓"
        )

        cancel_confirmed = await executor._cancel_order(fallback_adapter, failing_order.id, fallback_symbol)
        strict_pair = any(getattr(a, 'strict_order_state', False) for a in (fallback_adapter, opposite_adapter))
        if strict_pair and not cancel_confirmed:
            raise OrderStateUnknownError(fallback_exchange, order_id=failing_order.id)
        if cancel_opposite_order:
            opposite_cancelled = await executor._cancel_order(opposite_adapter, opposite_order.id, opposite_symbol)
            if strict_pair and not opposite_cancelled:
                raise OrderStateUnknownError(executor._get_exchange_name(opposite_adapter), order_id=opposite_order.id)

        if strict_pair:
            # Late fills can arrive during cancellation. Re-read BOTH legs before compensating.
            latest = await asyncio.gather(
                buy_adapter.get_order(buy_order.id, buy_symbol),
                sell_adapter.get_order(sell_order.id, sell_symbol),
                return_exceptions=True,
            )
            for outcome in latest:
                if isinstance(outcome, Exception):
                    raise OrderStateUnknownError('arcus') from outcome
            buy_order, sell_order = latest
            difference = executor._to_decimal_value(sell_order.filled) - executor._to_decimal_value(buy_order.filled)
            if abs(difference) <= epsilon:
                return Result(success=True, order_buy=buy_order, order_sell=sell_order,
                              success_quantity=min(buy_order.filled, sell_order.filled))
            if (difference > 0) != missing_buy:
                raise OrderStateUnknownError('arcus')
            shortfall = abs(difference)

        shortfall_dec = executor._to_decimal_value(shortfall)
        if shortfall_dec <= epsilon:
            return self._result_failure(
                request,
                "补单数量异常（<=0），已终止执行",
                order_buy=buy_order,
                order_sell=sell_order,
            )

        min_qty_limit = executor._resolve_min_order_qty(request, fallback_exchange)
        aggressive_allowed = shortfall_dec > epsilon
        if min_qty_limit and shortfall_dec < min_qty_limit:
            aggressive_allowed = False
            logger.info(
                f"ℹ️ [套利执行] {fallback_exchange} {leg_label} 补单量 {shortfall_dec} < 最小下单量 {min_qty_limit}，跳过激进限价"
            )

        supplemental_filled = Decimal("0")
        remaining_to_fill = shortfall_dec
        final_fallback_order: Optional[OrderData] = None
        aggressive_order: Optional[OrderData] = None

        if aggressive_allowed:
            slippage_hint = executor._get_slippage_percent(request)
            logger.info(
                f"⚡ [套利执行] {fallback_exchange} {leg_label} 缺口 {shortfall_dec}，尝试激进限价补齐"
            )
            aggressive_order = await self._place_aggressive_limit_retry_order(
                adapter=fallback_adapter,
                symbol=fallback_symbol,
                quantity=remaining_to_fill,
                is_buy=fallback_is_buy,
                request=request,
                exchange_name=fallback_exchange,
                slippage_pct=slippage_hint,
                base_slippage=slippage_hint,
            )
            if aggressive_order:
                _, aggressive_filled = await self._confirm_market_fill_with_retries(
                    initial_order=aggressive_order,
                    adapter=fallback_adapter,
                    symbol=fallback_symbol,
                    quantity=remaining_to_fill,
                    is_buy=fallback_is_buy,
                    reduce_only=(not request.is_open),
                    request=request,
                    exchange_name=fallback_exchange,
                    allow_resubmit=False,
                    max_attempts=1,
                    enable_internal_aggressive_retry=False,
                )
                if aggressive_filled > epsilon:
                    supplemental_filled += aggressive_filled
                    remaining_to_fill = max(Decimal("0"), shortfall_dec - supplemental_filled)
                    final_fallback_order = aggressive_order

        if remaining_to_fill > epsilon:
            if aggressive_allowed and aggressive_order and getattr(aggressive_order, "id", None):
                try:
                    await executor._cancel_order(fallback_adapter, aggressive_order.id, fallback_symbol)
                except OrderStateUnknownError:
                    raise
                except Exception as cancel_exc:
                    logger.warning(
                        "⚠️ [套利执行] 取消 %s 激进限价补单失败 (order_id=%s): %s",
                        fallback_exchange,
                        aggressive_order.id,
                        cancel_exc,
                    )
                else:
                    logger.info(
                        "↩️ [套利执行] [%s] 激进限价补单未全成，已先取消原单（剩余 %.6f）",
                        fallback_exchange,
                        float(remaining_to_fill),
                    )
                try:
                    latest = await fallback_adapter.get_order(str(aggressive_order.id), fallback_symbol)
                except OrderStateUnknownError:
                    raise
                except Exception as refresh_exc:
                    logger.warning(
                        "⚠️ [套利执行] 查询 %s 激进限价补单剩余失败: %s",
                        fallback_exchange,
                        refresh_exc,
                    )
                else:
                    refreshed_remaining = executor._to_decimal_value(
                        getattr(latest, "remaining", None)
                    )
                    if refreshed_remaining is not None:
                        # REST 在撤单后通常返回 0，但我们仍然保留本地剩余，避免误以为已补满
                        remaining_to_fill = max(
                            Decimal("0"),
                            refreshed_remaining,
                            remaining_to_fill,
                        )
            if remaining_to_fill <= epsilon:
                logger.info(
                    "✅ [套利执行] %s %s 激进限价补单取消后已确认无剩余，跳过市价补单",
                    fallback_exchange,
                    leg_label,
                )
                final_fallback_order = aggressive_order or final_fallback_order
            else:
                if aggressive_allowed:
                    logger.warning(
                        f"⚠️ [套利执行] {fallback_exchange} {leg_label} 激进限价补单未完全成交，剩余 {remaining_to_fill} 改用市价补齐"
                    )
                else:
                    logger.warning(
                        f"⚠️ [套利执行] {fallback_exchange} {leg_label} 补单量不足最小门槛或禁用激进模式，直接市价补齐 {remaining_to_fill}"
                    )

                try:
                    market_order = await executor._place_market_order(
                        fallback_adapter,
                        fallback_symbol,
                        remaining_to_fill,
                        is_buy=fallback_is_buy,
                        reduce_only=(not request.is_open),
                        request=request,
                    )
                except self.ReduceOnlyRestrictionError:
                    completed_exchange = request.exchange_sell if missing_buy else request.exchange_buy
                    completed_adapter = sell_adapter if missing_buy else buy_adapter
                    completed_symbol = sell_symbol if missing_buy else buy_symbol
                    await executor.reduce_only_handler.handle_reduce_only_after_partial(
                        request=request,
                        completed_exchange=completed_exchange,
                        completed_adapter=completed_adapter,
                        completed_symbol=completed_symbol,
                        completed_quantity=shortfall,
                        completed_was_buy=(not missing_buy),
                        failing_exchange=fallback_exchange,
                        failing_symbol=fallback_symbol,
                        closing_issue=(not request.is_open),
                    )
                    return self._result_failure(
                        request,
                        f"{fallback_exchange} reduce-only 限制，市价补齐被拒绝",
                        order_buy=buy_order,
                        order_sell=sell_order,
                        partial_failure=True,
                        failed_exchange=fallback_exchange,
                        success_exchange=completed_exchange,
                        success_quantity=reported_quantity,
                    )

                if not market_order:
                    success_exchange = request.exchange_sell if missing_buy else request.exchange_buy
                    success_adapter = sell_adapter if missing_buy else buy_adapter
                    success_symbol = sell_symbol if missing_buy else buy_symbol
                    await executor._emergency_close_position(
                        exchange=success_exchange,
                        adapter=success_adapter,
                        symbol=success_symbol,
                        quantity=shortfall,
                        is_buy_to_close=missing_buy,
                        request=request,
                    )
                    return self._result_failure(
                        request,
                        "市价补齐订单提交失败，已触发紧急平仓",
                        order_buy=buy_order,
                        order_sell=sell_order,
                        partial_failure=True,
                        failed_exchange=fallback_exchange,
                        success_exchange=success_exchange,
                        success_quantity=reported_quantity,
                    )

                market_order, market_filled = await self._confirm_market_fill_with_retries(
                    initial_order=market_order,
                    adapter=fallback_adapter,
                    symbol=fallback_symbol,
                    quantity=remaining_to_fill,
                    is_buy=fallback_is_buy,
                    reduce_only=(not request.is_open),
                    request=request,
                    exchange_name=fallback_exchange,
                    enable_internal_aggressive_retry=False,
                )
                if market_filled > epsilon:
                    supplemental_filled += market_filled
                    remaining_to_fill = max(Decimal("0"), shortfall_dec - supplemental_filled)
                    final_fallback_order = market_order

        if supplemental_filled + epsilon < shortfall_dec:
            success_exchange = request.exchange_sell if missing_buy else request.exchange_buy
            success_adapter = sell_adapter if missing_buy else buy_adapter
            success_symbol = sell_symbol if missing_buy else buy_symbol
            await executor._emergency_close_position(
                exchange=success_exchange,
                adapter=success_adapter,
                symbol=success_symbol,
                quantity=shortfall_dec - supplemental_filled,
                is_buy_to_close=missing_buy,
                request=request,
            )
            return self._result_failure(
                request,
                "补单成交量不足目标，已触发紧急平仓",
                order_buy=buy_order,
                order_sell=sell_order,
                partial_failure=True,
                failed_exchange=fallback_exchange,
                success_exchange=success_exchange,
                success_quantity=reported_quantity,
            )

        # 🔥 获取未补齐腿的原始部分成交数量
        failing_order_partial_filled = executor._to_decimal_value(
            getattr(failing_order, "filled", Decimal("0"))
        )
        
        # 🔥 获取对面腿（已完成）的成交数量
        opposite_filled = executor._to_decimal_value(
            getattr(opposite_order, "filled", Decimal("0"))
        )
        
        # 🔥 该腿总成交 = 原始订单部分成交 + 补单累计成交
        total_failing_leg_filled = failing_order_partial_filled + supplemental_filled
        
        # 🔥 调试日志：记录成交明细
        logger.info(
            f"📊 [成交明细] {fallback_exchange} | "
            f"限价部分成交={failing_order_partial_filled} + 补单合计={supplemental_filled} = 总成交={total_failing_leg_filled} | "
            f"对面腿成交={opposite_filled} | "
            f"计算数量=min({total_failing_leg_filled}, {opposite_filled}) | "
            f"缺口目标={shortfall}"
        )
        
        if not final_fallback_order:
            final_fallback_order = failing_order
        
        final_buy_order = final_fallback_order if missing_buy else buy_order
        final_sell_order = sell_order if missing_buy else final_fallback_order
        
        # 🔥 更新最终订单对象的 filled 字段为总成交数
        if final_fallback_order:
            final_fallback_order.filled = total_failing_leg_filled
        
        executor._log_execution_summary(
            request=request,
            order_buy=final_buy_order,
            order_sell=final_sell_order,
            is_open=request.is_open,
            is_last_split=request.is_last_split,
        )
        
        # 🔥 正确计算成交数量：两腿取最小值
        actual_quantity = min(opposite_filled, total_failing_leg_filled)

        return Result(
            success=True,
            order_buy=final_buy_order,
            order_sell=final_sell_order,
            success_quantity=actual_quantity,
        )

    async def _fill_leg_to_target(
        self,
        *,
        adapter: ExchangeInterface,
        symbol: str,
        exchange_name: str,
        is_buy: bool,
        remaining_quantity: Decimal,
        request: "ExecutionRequest",
        epsilon: Decimal,
    ) -> Decimal:
        """
        使用激进限价 + 市价顺序补齐单个腿至拆单目标数量。
        """
        executor = self.executor
        leg_label = "买腿" if is_buy else "卖腿"
        target_quantity = executor._to_decimal_value(remaining_quantity)
        if target_quantity <= epsilon:
            return Decimal("0")

        min_qty_limit = executor._resolve_min_order_qty(request, exchange_name)
        aggressive_allowed = target_quantity > epsilon
        if min_qty_limit and target_quantity < min_qty_limit:
            aggressive_allowed = False
            logger.info(
                f"ℹ️ [套利执行] {exchange_name} remaining={target_quantity} < 最小下单量 {min_qty_limit}，跳过激进限价补齐"
            )

        supplemental_filled = Decimal("0")
        remaining_to_fill = target_quantity

        if aggressive_allowed:
            slippage_hint = executor._get_slippage_percent(request)
            logger.info(
                f"⚡ [套利执行] {exchange_name} {leg_label} 剩余 {remaining_to_fill}，优先尝试激进限价补齐"
            )
            aggressive_order = await self._place_aggressive_limit_retry_order(
                adapter=adapter,
                symbol=symbol,
                quantity=remaining_to_fill,
                is_buy=is_buy,
                request=request,
                exchange_name=exchange_name,
                slippage_pct=slippage_hint,
                base_slippage=slippage_hint,
            )
            if aggressive_order:
                _, aggressive_filled = await self._confirm_market_fill_with_retries(
                    initial_order=aggressive_order,
                    adapter=adapter,
                    symbol=symbol,
                    quantity=remaining_to_fill,
                    is_buy=is_buy,
                    reduce_only=(not request.is_open),
                    request=request,
                    exchange_name=exchange_name,
                    allow_resubmit=False,
                    max_attempts=1,
                    enable_internal_aggressive_retry=False,
                )
                if aggressive_filled > epsilon:
                    supplemental_filled += aggressive_filled
                    remaining_to_fill = max(Decimal("0"), target_quantity - supplemental_filled)

        if remaining_to_fill > epsilon:
            if aggressive_allowed:
                logger.warning(
                    "⚠️ [套利执行] %s %s 激进限价补单未完全成交，改用市价补单 (remaining=%s)",
                    exchange_name,
                    leg_label,
                    remaining_to_fill,
                )
            else:
                logger.warning(
                    "⚠️ [套利执行] %s %s 剩余量不足最小限额，直接使用市价补单 (remaining=%s)",
                    exchange_name,
                    leg_label,
                    remaining_to_fill,
                )

            try:
                market_order = await executor._place_market_order(
                    adapter,
                    symbol,
                    remaining_to_fill,
                    is_buy=is_buy,
                    reduce_only=(not request.is_open),
                    request=request,
                )
            except self.ReduceOnlyRestrictionError:
                logger.warning(
                    f"⚠️ [套利执行] {exchange_name} {leg_label} 市价补单触发 reduce-only 限制，remaining={remaining_to_fill}"
                )
                raise

            if not market_order:
                raise RuntimeError(
                    f"{exchange_name} {leg_label} 市价补单提交失败，无法补齐剩余 {remaining_to_fill}"
                )

            market_order, market_filled = await self._confirm_market_fill_with_retries(
                initial_order=market_order,
                adapter=adapter,
                symbol=symbol,
                quantity=remaining_to_fill,
                is_buy=is_buy,
                reduce_only=(not request.is_open),
                request=request,
                exchange_name=exchange_name,
                enable_internal_aggressive_retry=False,
            )
            if market_filled > epsilon:
                supplemental_filled += market_filled
                remaining_to_fill = max(Decimal("0"), target_quantity - supplemental_filled)

        if supplemental_filled + epsilon < target_quantity:
            raise RuntimeError(
                f"{exchange_name} {leg_label} 激进限价/市价补单仍未成交，剩余 {remaining_to_fill}"
            )

        return supplemental_filled
    def _calculate_success_quantity_from_orders(
        self,
        order_buy: Optional[OrderData],
        order_sell: Optional[OrderData],
        fallback: Optional[Decimal] = None,
    ) -> Decimal:
        """从买卖订单中计算实际成交数量（取两腿最小值）"""
        executor = self.executor
        quantities: List[Decimal] = []
        if order_buy and getattr(order_buy, "filled", None) is not None:
            buy_qty = executor._to_decimal_value(order_buy.filled or Decimal("0"))
            if buy_qty > Decimal("0"):
                quantities.append(buy_qty)
        if order_sell and getattr(order_sell, "filled", None) is not None:
            sell_qty = executor._to_decimal_value(order_sell.filled or Decimal("0"))
            if sell_qty > Decimal("0"):
                quantities.append(sell_qty)
        if len(quantities) == 2:
            return min(quantities)
        if len(quantities) == 1:
            return quantities[0]
        return executor._to_decimal_value(fallback or Decimal("0"))

    async def _resolve_filled_quantity(
        self,
        *,
        order: OrderData,
        adapter: ExchangeInterface,
        symbol: str,
        reported_filled: Optional[Decimal],
    ) -> Decimal:
        """
        确认限价订单的真实成交数量，支持部分成交场景。
        优先使用等待阶段返回的数量；若无，则尝试读取 order.filled，
        仍为空时触发一次 REST 查询刷新状态。
        """
        executor = self.executor
        decimal_value = executor._to_decimal_value(reported_filled or Decimal("0"))
        if decimal_value > Decimal("0"):
            return decimal_value

        if order and reported_filled is None:
            terminal = executor._order_fill_results.get(str(order.id)) or executor._order_fill_results.get(str(order.client_id))
            if terminal is not None and terminal.status in (OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED, OrderStatus.EXPIRED):
                executor.order_monitor._merge_order_state(order, terminal)
                executor._remember_confirmed_fill(adapter, order)
                return executor._to_decimal_value(order.filled)

        if reported_filled is not None and order and getattr(order, "filled", None):
            existing = executor._to_decimal_value(order.filled or Decimal("0"))
            if existing > Decimal("0"):
                return existing

        if not adapter or not hasattr(adapter, "get_order"):
            return Decimal("0")

        try:
            latest = await adapter.get_order(str(order.id), symbol)
        except OrderStateUnknownError:
            raise
        except Exception as exc:  # pragma: no cover - 仅日志提示
            logger.warning(
                "⚠️ [套利执行] 查询 %s 订单 %s 状态失败: %s",
                executor._get_exchange_name(adapter),
                order.id,
                exc,
            )
            return Decimal("0")

        if not latest:
            return Decimal("0")

        try:
            executor.order_monitor._merge_order_state(order, latest)  # type: ignore[attr-defined]
        except AttributeError:
            order.filled = latest.filled or order.filled
            if latest.status:
                order.status = latest.status
            if latest.average:
                order.average = latest.average
            if latest.price:
                order.price = latest.price
            if latest.remaining is not None:
                order.remaining = latest.remaining

        refreshed = executor._to_decimal_value(order.filled or Decimal("0"))
        if refreshed > Decimal("0"):
            logger.info(
                "ℹ️ [套利执行] 刷新成交数量: %s %s -> %s",
                executor._get_exchange_name(adapter),
                order.id,
                refreshed,
            )
        return refreshed

