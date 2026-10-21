from __future__ import annotations

"""
Reduce-Only 处理模块
-------------------
负责在交易所返回 “invalid reduce only mode” 等限制时：
- 记录受影响的腿并更新 `ReduceOnlyGuard`
- 回滚已完成腿、防止裸仓
- 触发定时探测订单，检测何时解除限制

仅承载状态管理与补救流程，实际下单仍由 `ArbitrageExecutor` 执行，确保功能与日志与拆分前一致。
"""

import asyncio
import logging
from decimal import Decimal
from typing import Optional, Set, Tuple, TYPE_CHECKING

from core.adapters.exchanges.interface import ExchangeInterface
from core.adapters.exchanges.models import OrderSide, OrderType
from core.adapters.exchanges.utils.setup_logging import LoggingConfig

from ..guards.reduce_only_guard import ReduceOnlyGuard

if TYPE_CHECKING:
    from .arbitrage_executor import ArbitrageExecutor, ExecutionRequest


logger = LoggingConfig.setup_logger(
    name=__name__,
    log_file='arbitrage_executor.log',
    console_formatter=None,
    file_formatter='detailed',
    level=logging.INFO
)
logger.propagate = False


class ReduceOnlyHandler:
    """负责处理交易所 reduce-only 限制相关的状态记录与回滚逻辑。"""

    def __init__(
        self,
        executor: "ArbitrageExecutor",
        guard: Optional[ReduceOnlyGuard],
    ) -> None:
        self.executor = executor
        self.guard = guard

    @staticmethod
    def is_reduce_only_error(exc: Exception) -> bool:
        message = str(exc).lower()
        return (
            "invalid reduce only mode" in message
            or "code=21740" in message
        )

    @staticmethod
    def collect_request_legs(
        request: Optional["ExecutionRequest"]
    ) -> Set[Tuple[str, str]]:
        if not request:
            return set()
        legs: Set[Tuple[str, str]] = set()
        legs.add((request.exchange_buy, (request.buy_symbol or request.symbol)))
        legs.add((request.exchange_sell, (request.sell_symbol or request.symbol)))
        return legs

    def register_reduce_only_event(
        self,
        request: Optional["ExecutionRequest"],
        exchange: str,
        symbol: str,
        *,
        closing_issue: bool,
        reason: Optional[str] = None,
    ) -> None:
        if not self.guard or not request:
            return
        legs = self.collect_request_legs(request)
        legs.add((exchange, symbol))
        self.guard.mark_pair(
            request.symbol,
            legs,
            reason=reason,
            closing_blocked=closing_issue,
            failed_leg=(exchange, symbol),
        )
        logger.warning(
            "⚠️ [reduce-only] %s 触发限制，交易所=%s，标的=%s，模式=%s",
            request.symbol,
            exchange,
            symbol,
            "平仓阻塞" if closing_issue else "仅允许平仓"
        )

    async def undo_completed_leg(
        self,
        *,
        exchange: str,
        adapter: ExchangeInterface,
        symbol: str,
        quantity: Decimal,
        executed_was_buy: bool,
    ) -> bool:
        """尝试撤销已成交的一条腿，保持仓位对称。"""
        if quantity <= Decimal("0"):
            return True
        revert_is_buy = not executed_was_buy
        try:
            order = await self.executor._place_market_order(
                adapter=adapter,
                symbol=symbol,
                quantity=quantity,
                is_buy=revert_is_buy,
                reduce_only=False,
                request=None
            )
            if not order:
                logger.error(
                    "❌ [套利执行] %s 撤销已成交腿失败（无返回）: %s %s",
                    exchange,
                    symbol,
                    quantity,
                )
                return False
            limit_timeout = getattr(
                self.executor.config.order_execution,
                "limit_order_timeout",
                60,
            ) or 60
            filled = await self.executor.order_monitor.wait_for_order_fill(
                order,
                adapter,
                timeout=limit_timeout,
                is_market_order=True,
            )
            return bool(filled and filled > Decimal("0"))
        except Exception as exc:
            if self.is_reduce_only_error(exc):
                logger.error(
                    "❌ [套利执行] %s 无法撤销已成交腿，交易所仍处于 reduce-only，符号=%s",
                    exchange,
                    symbol,
                )
                return False
            logger.error(
                "❌ [套利执行] 撤销已成交腿异常: %s %s (%s)",
                exchange,
                symbol,
                exc,
                exc_info=True,
            )
            return False

    async def handle_reduce_only_after_partial(
        self,
        *,
        request: "ExecutionRequest",
        completed_exchange: str,
        completed_adapter: ExchangeInterface,
        completed_symbol: str,
        completed_quantity: Decimal,
        completed_was_buy: bool,
        failing_exchange: str,
        failing_symbol: str,
        closing_issue: bool,
    ) -> None:
        self.register_reduce_only_event(
            request,
            failing_exchange,
            failing_symbol,
            closing_issue=closing_issue,
            reason="invalid reduce only mode",
        )
        success = await self.undo_completed_leg(
            exchange=completed_exchange,
            adapter=completed_adapter,
            symbol=completed_symbol,
            quantity=completed_quantity,
            executed_was_buy=completed_was_buy,
        )
        if success:
            logger.info(
                "✅ [套利执行] 已撤销 reduce-only 触发前的成交: %s %s x%s",
                completed_exchange,
                completed_symbol,
                completed_quantity,
            )
        else:
            logger.warning(
                "⚠️ [套利执行] 未能完全撤销 reduce-only 触发前的成交，"
                "请确认持仓是否一致: %s %s x%s",
                completed_exchange,
                completed_symbol,
                completed_quantity,
            )

    async def probe_reduce_only_leg(
        self,
        exchange: str,
        symbol: str,
        quantity: Decimal,
        *,
        price: Decimal,
    ) -> bool:
        """
        在整点探测时发送最小量市价单 → 立即反向平仓 ，以确认是否恢复开仓能力。
        """
        adapter = self.executor.exchange_adapters.get(exchange)
        if not adapter:
            logger.warning("⚠️ [reduce-only] 未找到适配器: %s", exchange)
            return False
        if self.executor.monitor_only:
            logger.info(
                "ℹ️ [reduce-only] 监控模式不执行探测订单: %s %s",
                exchange,
                symbol,
            )
            return False

        qty = max(quantity, Decimal("0.000001"))
        timeout = getattr(
            self.executor.config.order_execution,
            "limit_order_timeout",
            60,
        ) or 60

        try:
            open_order = await self.executor._place_market_order(
                adapter=adapter,
                symbol=symbol,
                quantity=qty,
                is_buy=True,
                reduce_only=False,
                request=None,
            )
        except Exception as exc:
            if self.is_reduce_only_error(exc):
                return False
            logger.error("❌ [reduce-only] 探测开仓失败: %s %s (%s)", exchange, symbol, exc)
            return False

        if not open_order:
            return False

        try:
            filled_open = await self.executor.order_monitor.wait_for_order_fill(
                open_order,
                adapter,
                timeout=timeout,
                is_market_order=True,
            )
            filled_open_dec = self.executor._to_decimal_value(filled_open or qty)
        except Exception:
            filled_open_dec = qty

        try:
            close_order = await self.executor._place_market_order(
                adapter=adapter,
                symbol=symbol,
                quantity=filled_open_dec,
                is_buy=False,
                reduce_only=True,
                request=None,
            )
        except Exception as exc:
            logger.warning(
                "⚠️ [reduce-only] 探测平仓失败: %s %s (%s)",
                exchange,
                symbol,
                exc,
            )
            return False

        if close_order:
            try:
                await self.executor.order_monitor.wait_for_order_fill(
                    close_order,
                    adapter,
                    timeout=timeout,
                    is_market_order=True,
                )
            except Exception:
                logger.warning("⚠️ [reduce-only] 探测平仓确认失败: %s %s", exchange, symbol)

        return True

