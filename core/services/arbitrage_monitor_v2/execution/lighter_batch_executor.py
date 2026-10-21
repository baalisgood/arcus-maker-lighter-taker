"""
lighter 批量市价执行模块
从 OrderStrategyExecutor._execute_lighter_ws_batch 拆出，保持接口和行为一致。
"""
from typing import TYPE_CHECKING
from ..risk_control.network_state import notify_network_recovered
from ..state.symbol_state_manager import MANUAL_INTERVENTION_AUTO_RESUME_SECONDS
from core.adapters.exchanges.models import OrderSide
import asyncio
from decimal import Decimal
import logging
from typing import Optional
from core.adapters.exchanges.utils.setup_logging import LoggingConfig

logger = LoggingConfig.setup_logger(
    name=__name__,
    log_file='arbitrage_executor.log',
    console_formatter=None,
    file_formatter='detailed',
    level=logging.INFO
)
logger.propagate = False

# 🔥 修复：OrderSide 在 adapters 模块中，不在 arbitrage_monitor_v2.models 中

# 为类型检查导入（避免循环依赖）

if TYPE_CHECKING:  # pragma: no cover
    from .order_strategy_executor import OrderStrategyExecutor, ExecutionRequest, ExecutionResult
    from ...interfaces import ExchangeInterface


async def execute_lighter_ws_batch(
    executor_obj: "OrderStrategyExecutor",
    request: "ExecutionRequest",
    adapter: "ExchangeInterface",
    buy_symbol: str,
    sell_symbol: str,
) -> "ExecutionResult":
    """
    拆自 OrderStrategyExecutor._execute_lighter_ws_batch
    保持原有行为和返回值不变。
    """
    executor = executor_obj.executor
    Result = executor_obj.ExecutionResult

    # 🔥 lighter 市价单优先使用专用超时配置
    market_timeout = getattr(
        executor.config.order_execution,
        "lighter_market_order_timeout",
        None,
    ) or getattr(
        executor.config.order_execution,
        "limit_order_timeout",
        60,
    ) or 60

    if not adapter or not hasattr(adapter, "place_market_orders_ws_batch"):
        return executor_obj._result_failure(
            request,
            "lighter批量下单未启用",
        )

    reduce_only_flag = not request.is_open
    orders_payload = []
    for side, symbol in (("buy", buy_symbol), ("sell", sell_symbol)):
        payload = {
            "symbol": symbol,
            "side": side,
            "quantity": request.quantity,
        }
        # 仅在非SPOT腿上附加 reduce_only（现货不支持 reduce_only）
        is_spot_leg = "SPOT" in str(symbol).upper()
        if reduce_only_flag and not is_spot_leg:
            payload["reduce_only"] = True
        orders_payload.append(payload)

    try:
        slippage_percent = executor._get_slippage_percent(request)
        response = await adapter.place_market_orders_ws_batch(
            orders_payload,
            slippage_percent=slippage_percent,
        )
    except Exception as exc:  # pylint: disable=broad-except
        logger.error(f"❌ [套利执行] lighter批量市价单失败: {exc}", exc_info=True)

        # 🔥 检查是否是 reduce-only 错误（21740），触发探针机制
        if executor.reduce_only_handler.is_reduce_only_error(exc):
            exchange_name = executor._get_exchange_name(adapter)
            # 为两个 symbol 都注册 reduce-only 事件
            for symbol in [buy_symbol, sell_symbol]:
                executor.reduce_only_handler.register_reduce_only_event(
                    request,
                    exchange_name,
                    symbol,
                    closing_issue=reduce_only_flag,
                    reason=str(exc)
                )
            logger.warning(
                f"⚠️ [套利执行] {exchange_name} 批量市价单遇到 reduce-only 限制（21740），"
                f"已触发探针机制，等待市场恢复"
            )

        return executor_obj._result_failure(request, str(exc))

    if not response or not isinstance(response, dict):
        return executor_obj._result_failure(
            request,
            "lighter批量下单无返回结果",
        )

    skipped_orders = response.get("skipped_orders") or []
    orders_data = response.get("orders") or []
    if not orders_data:
        if skipped_orders:
            logger.info(
                f"ℹ️ [套利执行] lighter批量市价单未发送，原因: 无可平仓持仓 {skipped_orders}"
            )
            return Result(success=True, success_quantity=Decimal('0'))
        return executor_obj._result_failure(
            request,
            "lighter批量下单无返回订单",
        )

    order_buy = executor._select_order_by_side(orders_data, OrderSide.BUY)
    order_sell = executor._select_order_by_side(orders_data, OrderSide.SELL)

    if order_buy:
        executor._register_pending_order(order_buy)
    if order_sell:
        executor._register_pending_order(order_sell)
    notify_network_recovered(request.exchange_buy)

    buy_order_id = order_buy.id if order_buy else "-"
    sell_order_id = order_sell.id if order_sell else "-"
    logger.info(
        f"[套利执行] lighter批量市价单已提交: 买单={buy_order_id}, 卖单={sell_order_id}"
    )

    # 🔥 lighter套利对完全依赖WS，不使用REST兜底
    buy_filled_task = asyncio.create_task(
        executor.order_monitor._wait_for_order_fill_websocket(
            order_buy,
            timeout=market_timeout,
        )
    ) if order_buy else None
    sell_filled_task = asyncio.create_task(
        executor.order_monitor._wait_for_order_fill_websocket(
            order_sell,
            timeout=market_timeout,
        )
    ) if order_sell else None

    # 🔥 完全依赖 WS 推送，等待成交通知
    buy_filled = await buy_filled_task if buy_filled_task else None
    sell_filled = await sell_filled_task if sell_filled_task else None
    buy_filled_dec = executor._to_decimal_value(buy_filled or Decimal("0"))
    sell_filled_dec = executor._to_decimal_value(sell_filled or Decimal("0"))
    if order_buy:
        order_buy.filled = buy_filled_dec
    if order_sell:
        order_sell.filled = sell_filled_dec

    epsilon = Decimal("0.00000001")
    has_buy = order_buy is not None
    has_sell = order_sell is not None

    if has_buy and has_sell:
        # ===== 场景1：双腿都未成交 =====
        if buy_filled_dec <= epsilon and sell_filled_dec <= epsilon:
            logger.error("❌ [Lighter批量市价] 两个方向均未成交（WS超时无推送），继续监控")
            return executor_obj._result_failure(
                request,
                "lighter批量市价单两个订单都未成交，继续监控",
                order_buy=order_buy,
                order_sell=order_sell,
                mark_waiting=False,
            )

        # ===== 场景2：双腿都成交 =====
        if buy_filled_dec > epsilon and sell_filled_dec > epsilon:
            # 🔥 双腿成交，清零计数器
            symbol_key = request.symbol.upper()
            if symbol_key in executor._lighter_single_leg_counter:
                old_count = executor._lighter_single_leg_counter[symbol_key]
                executor._lighter_single_leg_counter[symbol_key] = 0
                logger.info(
                    f"✅ [Lighter批量市价] {symbol_key} 双腿成交成功，计数器清零 (之前={old_count})"
                )

            executor._log_execution_summary(
                request=request,
                order_buy=order_buy,
                order_sell=order_sell,
                is_open=request.is_open,
                is_last_split=request.is_last_split,
            )
            actual_quantity = executor_obj._calculate_success_quantity_from_orders(
                order_buy,
                order_sell,
                fallback=request.quantity,
            )

            return Result(
                success=True,
                order_buy=order_buy,
                order_sell=order_sell,
                success_quantity=actual_quantity,
            )

        # ===== 场景3：单腿成交，另一腿失败 =====
        is_buy_unfilled = buy_filled_dec <= epsilon
        filled_quantity = sell_filled_dec if is_buy_unfilled else buy_filled_dec
        filled_symbol = sell_symbol if is_buy_unfilled else buy_symbol

        # 🔥 计数器管理
        symbol_key = request.symbol.upper()
        current_count = executor._lighter_single_leg_counter.get(symbol_key, 0)
        new_count = current_count + 1
        executor._lighter_single_leg_counter[symbol_key] = new_count

        logger.error(
            f"❌ [Lighter批量市价] 单腿成交(计数:{new_count}/3): "
            f"{'卖单' if is_buy_unfilled else '买单'} 成功 {filled_quantity}, "
            f"{'买单' if is_buy_unfilled else '卖单'} 未成交（WS超时无推送）"
        )

        # 🔥 紧急处理策略变更：
        # - 旧：反向“平掉已成交腿”（完全依赖WS）
        # - 新：不动已成交腿，改为“补单失败腿”（用 REST 市价单重试），让两腿重新回到同一状态
        failed_symbol = buy_symbol if is_buy_unfilled else sell_symbol
        logger.warning(
            f"🚨 [Lighter紧急补单] 开始重试失败腿（REST市价，超时{market_timeout}秒）: "
            f"{'买入' if is_buy_unfilled else '卖出'} {filled_quantity} {failed_symbol} "
            f"(已成交腿不再反向平仓: {filled_symbol})"
        )

        # 🔥 紧急平仓使用滑点放大，提高成交概率（前两次50倍，第3次100倍）
        normal_slippage = executor._get_slippage_percent(request)
        emergency_slippage_configured = (
            normal_slippage if normal_slippage is not None else Decimal("0.001")
        )  # 兜底50%
        emergency_slippage_configured_rest = (
            normal_slippage if normal_slippage is not None else Decimal("0.001")
        )  # 兜底100%
        logger.info(
            f"💰 [Lighter紧急平仓] 滑点配置: 正常={float(normal_slippage or 0)*100:.4f}%, "
            f"紧急={float(emergency_slippage_configured)*100:.4f}% (50倍放大)"
        )

        retry_success = False
        retry_order_final = None
        max_close_attempts = 3  # 🔥 最多尝试3次（初次 + 1次重试 + 5分钟后额外重试一次）

        for attempt in range(1, max_close_attempts + 1):
            try:
                if attempt > 1:
                    logger.warning(
                        f"🔄 [Lighter紧急补单] 第{attempt}次尝试（重新获取最新盘口价格）: "
                        f"{'买入' if is_buy_unfilled else '卖出'} {filled_quantity} {failed_symbol}"
                    )
                # 🔥 额外重试：第3次不再等待，立即进行（避免长时间单边敞口）
                if attempt == 3:
                    logger.warning(
                        f"⏳ [Lighter紧急补单] 第3次尝试将立即执行（不再等待5分钟）："
                        f"方向={'买入' if is_buy_unfilled else '卖出'} 数量={filled_quantity} {failed_symbol}"
                    )

                # 🔥 关键变更：
                # - 第3次（等待5分钟后）不再使用市价单，而是使用“激进限价 IOC”
                # - 激进价格仍使用 50倍滑点（不再使用100倍）
                emergency_slippage = emergency_slippage_configured
                slippage_multiplier = "50倍" if attempt < 3 else "50倍(激进限价)"

                # 🔥 每次尝试都打印滑点信息，让用户明确看到滑点生效
                logger.info(
                    f"🚨 [Lighter紧急补单] 尝试{attempt}/{max_close_attempts} 使用{slippage_multiplier}滑点: "
                    f"{float(emergency_slippage)*100:.4f}% | "
                    f"方向={'买入' if is_buy_unfilled else '卖出'}, 数量={filled_quantity}"
                )

                # 🔥 提交市价“补单”（失败腿）：
                # - 强制走 REST（避免 WS 批量提交时偶发“提交后无推送”导致无法判断）
                # - 滑点仍按现有方案放大（50倍/100倍）
                # - reduce_only：若是平仓场景且为 PERP，则启用 reduce_only；现货不支持
                is_spot_failed_leg = "SPOT" in str(failed_symbol).upper()
                reduce_only_retry = (not request.is_open) and (not is_spot_failed_leg)
                if attempt < 3:
                    # 前两次：REST 市价补单
                    retry_order = await executor._place_market_order(
                        adapter=adapter,
                        symbol=failed_symbol,
                        quantity=filled_quantity,
                        is_buy=is_buy_unfilled,
                        reduce_only=reduce_only_retry,
                        request=None,
                        slippage_override=emergency_slippage,  # 50倍滑点
                        force_rest=True,
                    )
                else:
                    # 第三次：REST 激进限价(IOC)补单，价格按“50倍滑点保护价”计算
                    retry_order = await executor._place_aggressive_limit_order_lighter_rest(
                        adapter=adapter,
                        symbol=failed_symbol,
                        quantity=filled_quantity,
                        is_buy=is_buy_unfilled,
                        reduce_only=reduce_only_retry,
                        slippage_override=emergency_slippage,  # 仍为50倍
                    )

                if retry_order:
                    logger.info(
                        f"✅ [Lighter紧急补单] 补单已提交（尝试{attempt}/{max_close_attempts}），等待WS推送...")

                    # ⚠️ 说明：即便是 REST 提交，Lighter 也需要依赖 WS 推送来确认 order_id/成交（见适配器注释）
                    retry_filled = await executor.order_monitor._wait_for_order_fill_websocket(
                        retry_order,
                        timeout=market_timeout,
                    )
                    retry_filled_dec = executor._to_decimal_value(
                        retry_filled or Decimal("0"))

                    if retry_filled_dec > epsilon:
                        logger.info(
                            f"✅ [Lighter紧急补单] 补单成功（尝试{attempt}/{max_close_attempts}）: {retry_filled_dec} {failed_symbol}")
                        retry_success = True
                        retry_order_final = retry_order
                        break  # 🔥 成功后立即退出循环
                    else:
                        logger.error(
                            f"❌ [Lighter紧急补单] WS超时({market_timeout}秒)未收到成交推送（尝试{attempt}/{max_close_attempts}）")
                        # 第3次使用激进限价(IOC)：若超时仍未成交，主动撤单，避免残留挂单影响后续判断
                        if attempt == 3:
                            try:
                                cancel_id = str(getattr(retry_order, "id", "") or getattr(retry_order, "client_id", "") or "")
                                cancel_symbol = str(getattr(retry_order, "symbol", "") or failed_symbol)
                                if cancel_id and cancel_symbol:
                                    logger.warning(
                                        "🧹 [Lighter紧急补单] 第3次激进限价(IOC)超时未成交，准备撤单: order_id=%s symbol=%s",
                                        cancel_id,
                                        cancel_symbol,
                                    )
                                    await executor._cancel_order(adapter, cancel_id, cancel_symbol)
                            except Exception as cancel_exc:
                                logger.error(
                                    "❌ [Lighter紧急补单] 第3次激进限价(IOC)撤单失败: %s",
                                    cancel_exc,
                                    exc_info=True,
                                )
                        # 🔥 如果不是最后一次尝试，继续下一次循环
                        if attempt < max_close_attempts:
                            continue
                else:
                    logger.error(
                        f"❌ [Lighter紧急补单] 补单提交失败（尝试{attempt}/{max_close_attempts}）")
                    # 🔥 如果提交失败且不是最后一次尝试，继续重试
                    if attempt < max_close_attempts:
                        continue

            except Exception as close_exc:
                logger.error(
                    f"❌ [Lighter紧急补单] 补单异常（尝试{attempt}/{max_close_attempts}）: {close_exc}",
                    exc_info=True
                )
                # 🔥 如果异常且不是最后一次尝试，继续重试
                if attempt < max_close_attempts:
                    continue

        # 🔥 补单失败：仍然存在“单边敞口”，立即暂停并标记人工检查
        if not retry_success:
            auto_resume_minutes = int(MANUAL_INTERVENTION_AUTO_RESUME_SECONDS // 60)
            logger.error(
                f"🚨 [Lighter批量市价] 紧急补单失败，需人工立即检查并手动处理单边敞口 "
                f"(已成交腿={filled_symbol}, 失败腿={failed_symbol}, 数量={filled_quantity}) "
                f"(自动恢复等待={auto_resume_minutes}分钟)"
            )
            executor_obj._mark_manual_intervention(
                request,
                f"lighter单腿补单失败，需手动处理 {failed_symbol}"
            )
            return executor_obj._result_failure(
                request,
                f"lighter批量市价单腿成交，补单失败，需人工检查",
                order_buy=order_buy,
                order_sell=order_sell,
                mark_waiting=False,
            )

        # 🔥 补单成功：此轮交易应按“成功成交”对待（打印成交统计并返回给决策引擎）
        if retry_order_final is None:
            # 极端兜底：理论上不会发生（retry_success=True 一定拿到 retry_order）
            logger.error("❌ [Lighter紧急补单] 补单成功但未拿到订单对象，按失败处理")
            return executor_obj._result_failure(
                request,
                "lighter补单成功但订单对象缺失",
                order_buy=order_buy,
                order_sell=order_sell,
                mark_waiting=False,
            )

        # 用补单订单替换失败腿，使上下游看到“完整双腿”
        order_buy_final = order_buy
        order_sell_final = order_sell
        if is_buy_unfilled:
            order_buy_final = retry_order_final
        else:
            order_sell_final = retry_order_final

        # 打印成交统计（与双腿正常成交一致）
        executor._log_execution_summary(
            request=request,
            order_buy=order_buy_final,
            order_sell=order_sell_final,
            is_open=request.is_open,
            is_last_split=request.is_last_split,
        )
        actual_quantity = executor_obj._calculate_success_quantity_from_orders(
            order_buy_final,
            order_sell_final,
            fallback=request.quantity,
        )

        # 🔥 补单成功后仍保留“单腿计数器”，用于风险控制；本次执行应视为成功
        if new_count >= 3:
            logger.error(
                f"🚨 [Lighter批量市价] {symbol_key} 连续{new_count}次单腿成交（本次已补单成功），后续将暂停执行，需人工检查"
            )
            executor_obj._mark_manual_intervention(
                request,
                f"lighter连续{new_count}次单腿成交（已补单成功但仍需人工检查）"
            )
            # ⚠️ 关键：本次补单成功依然按成功返回（决策引擎需要记录本次成交），暂停仅影响后续执行
            logger.warning(
                f"⚠️ [Lighter批量市价] {symbol_key} 本轮补单成功已计入成交统计，但已触发暂停（计数:{new_count}/3）"
            )

        logger.warning(
            f"⚠️ [Lighter批量市价] {symbol_key} 单腿成交已补单成功(计数:{new_count}/3)，本轮视为成功，继续监控"
        )
        return Result(
            success=True,
            order_buy=order_buy_final,
            order_sell=order_sell_final,
            success_quantity=actual_quantity,
        )

        # （已在上方补单成功分支中处理返回：会打印成交统计并 return Result(success=True)）

    pending_order = order_buy or order_sell
    if pending_order:
        await executor._cancel_order(adapter, pending_order.id, pending_order.symbol)

    return executor_obj._result_failure(
        request,
        "lighter批量市价单未完全提交",
        order_buy=order_buy,
        order_sell=order_sell,
    )
