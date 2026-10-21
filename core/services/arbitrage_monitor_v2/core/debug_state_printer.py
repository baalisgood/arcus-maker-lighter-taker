from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import TYPE_CHECKING, Optional, List, Dict, Any, Tuple, Union

if TYPE_CHECKING:
    from .unified_orchestrator import UnifiedOrchestrator
    from core.adapters.exchanges.models import OrderData


class DebugStatePrinter:
    """
    单纯在终端打印关键信息的调试工具，用于定位成交后状态变化。
    
    - 输出队列长度、挂锁数量
    - 输出各交易所缓存的持仓摘要
    - 输出最近的订单成交（基于 executor._order_fill_results）
    """

    def __init__(
        self,
        orchestrator: "UnifiedOrchestrator",
        interval_seconds: float = 1.0,
    ) -> None:
        self._orc = orchestrator
        self._interval = max(interval_seconds, 0.5)
        self._task: Optional[asyncio.Task] = None
        self._logger = logging.getLogger(__name__)

    def start(self) -> None:
        if self._task:
            return
        self._task = asyncio.create_task(self._loop())
        self._logger.info(
            "🛠️ [DebugCLI] 调试打印器已启动（间隔 %.1fs）", self._interval
        )

    async def stop(self) -> None:
        if not self._task:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None
        self._logger.info("🛠️ [DebugCLI] 调试打印器已停止")

    async def _loop(self) -> None:
        while self._orc.running:
            try:
                self._print_snapshot()
            except Exception as exc:
                self._logger.warning(
                    "⚠️ [DebugCLI] 打印状态失败: %s", exc, exc_info=True
                )
            await asyncio.sleep(self._interval)

    def _print_snapshot(self) -> None:
        now = datetime.now().strftime("%H:%M:%S")
        orderbook_q = self._orc.orderbook_queue.qsize()
        ticker_q = self._orc.ticker_queue.qsize()
        pending_open = len(getattr(self._orc, "_pending_open_pairs", set()))
        pending_close = len(getattr(self._orc, "_pending_close_symbols", set()))

        position_rows = self._orc._build_position_rows()
        position_summary = (
            " | ".join(
                f"{row['symbol']} "
                f"B:{row.get('quantity_buy', 0)}({row.get('exchange_buy', '-')}) "
                f"S:{row.get('quantity_sell', 0)}({row.get('exchange_sell', '-')})"
                for row in position_rows
            )
            if position_rows
            else "无持仓"
        )

        exchange_cache = self._orc._collect_exchange_position_cache()
        cache_lines: List[str] = []
        for exchange, entries in exchange_cache.items():
            entry_lines: List[str] = []
            for symbol, payload in entries.items():
                size, side, entry_price, ts = self._normalize_entry(payload)
                ts_label = (
                    ts.strftime("%H:%M:%S") if hasattr(ts, "strftime") else str(ts)
                )
                entry_lines.append(
                    f"{symbol}:{size}({side}) @ {entry_price} ts={ts_label}"
                )
            if entry_lines:
                cache_lines.append(f"{exchange}: " + "; ".join(entry_lines))
        cache_summary = " | ".join(cache_lines) if cache_lines else "无交易所缓存"

        recent_fills = self._collect_recent_fills()

        has_activity = any(
            [
                orderbook_q > 0,
                ticker_q > 0,
                pending_open > 0,
                pending_close > 0,
                bool(position_rows),
                bool(cache_lines),
                bool(recent_fills),
            ]
        )
        if not has_activity:
            return

        print(
            f"\n[DebugCLI {now}] 队列 ob={orderbook_q} tk={ticker_q} "
            f"pending_open={pending_open} pending_close={pending_close}"
        )
        print(f"[DebugCLI {now}] 持仓摘要: {position_summary}")
        print(f"[DebugCLI {now}] 交易所缓存: {cache_summary}")
        if recent_fills:
            print(f"[DebugCLI {now}] 最近成交: {recent_fills}")

    def _collect_recent_fills(self) -> str:
        executor = getattr(self._orc, "executor", None)
        if not executor:
            return ""
        fill_map = getattr(executor, "_order_fill_results", {})
        if not fill_map:
            return ""
        fills: List["OrderData"] = list(fill_map.values())
        fills.sort(key=self._normalize_order_timestamp)
        tail = fills[-5:]
        lines: List[str] = []
        for order in tail:
            symbol = getattr(order, "symbol", "?")
            side = getattr(order, "side", "")
            side_value = getattr(side, "value", side)
            filled = getattr(order, "filled", None)
            amount = getattr(order, "amount", None)
            price = getattr(order, "average", None) or getattr(order, "price", None)
            status = getattr(order, "status", "")
            status_value = getattr(status, "value", status)
            lines.append(
                f"{symbol} {side_value}/status={status_value} "
                f"qty={filled}/{amount} price={price}"
            )
        return " | ".join(lines)

    def _normalize_order_timestamp(self, order: "OrderData") -> datetime:
        raw_ts = getattr(order, "timestamp", None)
        return self._coerce_timestamp(raw_ts)

    def _coerce_timestamp(self, value: Union[datetime, int, float, str, None]) -> datetime:
        if isinstance(value, datetime):
            return value
        if isinstance(value, (int, float)):
            ts_value = value / 1000 if value > 10**12 else value
            try:
                return datetime.fromtimestamp(ts_value)
            except Exception:
                return datetime.now()
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            except Exception:
                return datetime.now()
        return datetime.now()

    def _normalize_entry(self, payload: Any) -> Tuple[float, str, float, Any]:
        if isinstance(payload, dict):
            size = float(payload.get("size", payload.get("quantity", 0)) or 0)
            side = str(payload.get("side", "long"))
            price = float(payload.get("entry_price", payload.get("price", 0)) or 0)
            ts = payload.get("timestamp")
            return size, side, price, ts

        size = float(getattr(payload, "size", getattr(payload, "quantity", 0)) or 0)
        side_attr = getattr(payload, "side", None)
        side = side_attr.value if hasattr(side_attr, "value") else side_attr
        if not side:
            side = "long" if size >= 0 else "short"
        price = float(
            getattr(payload, "entry_price", getattr(payload, "price", 0)) or 0
        )
        ts = getattr(payload, "timestamp", None)
        return size, str(side), price, ts

