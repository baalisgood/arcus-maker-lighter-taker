"""
订单簿多档聚合工具（Top-N VWAP）

目的：
- 用前 N 档（买1..买N / 卖1..卖N）计算“可成交均价（VWAP）”与“可用数量”
- 解决买1/卖1数量不足但前几档合计足够的场景，提高可触发的套利机会

注意：
- 只有真实 L2 订单簿交易所（如 Lighter / Paradex 的 L2）才能提供多档数量
- BBO / Variational 这类只有最优一档（或数量不可用）的数据会自动降级
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Optional, Sequence, Tuple

from core.adapters.exchanges.models import OrderBookData, OrderBookLevel


@dataclass
class AggregatedBookSide:
    """聚合结果（单边：买盘 or 卖盘）"""

    vwap_price: Optional[Decimal]
    total_available: Optional[Decimal]
    used_levels: int
    fully_covered: bool  # 是否覆盖了 target_qty（如果给了 target_qty）


def _to_decimal(value: object) -> Optional[Decimal]:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _extract_side_levels(orderbook: OrderBookData, *, is_buy: bool) -> Tuple[Optional[OrderBookLevel], Sequence[OrderBookLevel]]:
    """
    is_buy=True  -> 取 asks（你要买，需要吃卖盘）
    is_buy=False -> 取 bids（你要卖，需要吃买盘）
    """
    if orderbook is None:
        return None, []

    if is_buy:
        best = getattr(orderbook, "best_ask", None)
        levels = getattr(orderbook, "asks", None) or []
    else:
        best = getattr(orderbook, "best_bid", None)
        levels = getattr(orderbook, "bids", None) or []

    # 兜底：如果没有 levels 列表，但 best 存在，则至少返回 best
    if (not levels) and best is not None:
        return best, [best]
    return best, levels


def aggregate_top_n_vwap(
    orderbook: OrderBookData,
    *,
    is_buy: bool,
    max_levels: int,
    target_qty: Optional[Decimal] = None,
) -> AggregatedBookSide:
    """
    使用前 N 档计算 VWAP。

    - 若 target_qty 提供：按“吃单模拟”只消耗到 target_qty 为止（更贴近真实成交均价）
    - 若 target_qty 为 None：按前 N 档全部数量做加权均价
    """
    n = int(max_levels) if max_levels is not None else 1
    n = max(1, n)

    best, levels = _extract_side_levels(orderbook, is_buy=is_buy)
    if not levels:
        return AggregatedBookSide(vwap_price=None, total_available=None, used_levels=0, fully_covered=False)

    # 取前 N 档（levels 理论上已按盘口顺序：bids desc, asks asc；若不保证也不强行排序，避免额外开销/副作用）
    slice_levels = list(levels[:n])
    if not slice_levels and best is not None:
        slice_levels = [best]

    total_available = Decimal("0")
    vwap_numerator = Decimal("0")
    used_levels = 0
    remaining = None
    if target_qty is not None:
        remaining = _to_decimal(target_qty)
        if remaining is None or remaining <= Decimal("0"):
            remaining = None

    for lvl in slice_levels:
        px = _to_decimal(getattr(lvl, "price", None))
        sz = _to_decimal(getattr(lvl, "size", None))
        if px is None or px <= Decimal("0"):
            continue
        if sz is None:
            # 数量缺失：无法做聚合（但上层可选择降级）
            return AggregatedBookSide(vwap_price=None, total_available=None, used_levels=0, fully_covered=False)
        if sz <= Decimal("0"):
            continue

        used_levels += 1
        total_available += sz

        qty_used = sz
        if remaining is not None:
            qty_used = min(sz, remaining)
            remaining -= qty_used

        vwap_numerator += px * qty_used

        if remaining is not None and remaining <= Decimal("0"):
            break

    if used_levels <= 0:
        return AggregatedBookSide(vwap_price=None, total_available=None, used_levels=0, fully_covered=False)

    filled_qty = total_available if target_qty is None else (target_qty - (remaining or Decimal("0")))
    filled_qty = _to_decimal(filled_qty) or Decimal("0")
    if filled_qty <= Decimal("0"):
        return AggregatedBookSide(vwap_price=None, total_available=total_available, used_levels=used_levels, fully_covered=False)

    vwap = vwap_numerator / filled_qty
    fully_covered = (remaining is None) or (remaining <= Decimal("0"))
    return AggregatedBookSide(
        vwap_price=vwap,
        total_available=total_available,
        used_levels=used_levels,
        fully_covered=bool(fully_covered),
    )


