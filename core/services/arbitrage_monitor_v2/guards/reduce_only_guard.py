from __future__ import annotations

"""
Reduce-Only Guard
-----------------
负责统一管理“只允许平仓”状态：
- 记录被限制的套利对与腿信息
- 暂停开仓、可选地阻断平仓
- 为探测任务提供待检测腿、记录整点探测结果
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo


SymbolLeg = Tuple[str, str]  # (exchange, symbol)


@dataclass
class ReduceOnlyPairState:
    """记录单个套利对的 reduce-only 限制状态。"""

    pair_id: str
    legs: Set[SymbolLeg] = field(default_factory=set)
    failed_legs: Set[SymbolLeg] = field(default_factory=set)
    blocked: bool = True
    closing_blocked: bool = False
    first_detected: datetime = field(default_factory=datetime.utcnow)
    last_error: Optional[str] = None
    last_probe: Optional[datetime] = None

    def update_legs(self, legs: Iterable[SymbolLeg]) -> None:
        self.legs.update(legs)

    def probe_legs(self) -> Set[SymbolLeg]:
        return self.failed_legs if self.failed_legs else self.legs


class ReduceOnlyGuard:
    """
    管理“只允许平仓”状态：

    - 一旦交易所返回 invalid reduce only mode，就将对应 pair 标记为 blocked
    - 定时探测成功后自动解除
    - 记录是否允许继续尝试平仓（closing_blocked）
    """

    def __init__(self, timezone: ZoneInfo):
        self._timezone = timezone
        self._pair_states: Dict[str, ReduceOnlyPairState] = {}
        self._leg_to_pairs: Dict[SymbolLeg, Set[str]] = {}

    # ------------------------------------------------------------------ #
    # 基础状态维护
    # ------------------------------------------------------------------ #

    @property
    def timezone(self) -> ZoneInfo:
        return self._timezone

    def mark_pair(
        self,
        pair_id: str,
        legs: Iterable[SymbolLeg],
        *,
        reason: Optional[str] = None,
        closing_blocked: bool = False,
        failed_leg: Optional[SymbolLeg] = None,
    ) -> ReduceOnlyPairState:
        """标记某个套利对进入 reduce-only 模式。"""
        normalized_legs = {self._normalize_leg(leg) for leg in legs}
        now = self._now()
        state = self._pair_states.get(pair_id)
        if not state:
            state = ReduceOnlyPairState(
                pair_id=pair_id,
                legs=normalized_legs,
                first_detected=now,
                blocked=True,
                closing_blocked=closing_blocked,
                last_error=reason,
            )
            self._pair_states[pair_id] = state
        else:
            state.blocked = True
            state.closing_blocked = state.closing_blocked or closing_blocked
            state.last_error = reason or state.last_error
            state.update_legs(normalized_legs)

        if failed_leg:
            normalized_failed = self._normalize_leg(failed_leg)
            state.failed_legs.add(normalized_failed)

        for leg in normalized_legs:
            self._leg_to_pairs.setdefault(leg, set()).add(pair_id)
        return state

    def clear_pair(self, pair_id: str) -> None:
        """恢复某个套利对的正常交易。"""
        state = self._pair_states.pop(pair_id, None)
        if not state:
            return
        for leg in state.legs:
            pairs = self._leg_to_pairs.get(leg)
            if not pairs:
                continue
            pairs.discard(pair_id)
            if not pairs:
                self._leg_to_pairs.pop(leg, None)

    def is_pair_blocked(self, pair_id: str) -> bool:
        state = self._pair_states.get(pair_id)
        return bool(state and state.blocked)

    def is_pair_closing_blocked(self, pair_id: str) -> bool:
        state = self._pair_states.get(pair_id)
        return bool(state and state.closing_blocked)

    def list_blocked_pairs(self) -> List[ReduceOnlyPairState]:
        return [state for state in self._pair_states.values() if state.blocked]

    def get_pair_state(self, pair_id: str) -> Optional[ReduceOnlyPairState]:
        return self._pair_states.get(pair_id)

    # ------------------------------------------------------------------ #
    # 探测逻辑
    # ------------------------------------------------------------------ #

    def legs_to_probe(self) -> List[Tuple[str, str, str]]:
        """
        返回需要探测的腿列表。

        Returns:
            [(pair_id, exchange, symbol), ...]
        """
        targets: List[Tuple[str, str, str]] = []
        for pair_id, state in self._pair_states.items():
            if not state.blocked:
                continue
            probe_legs = state.failed_legs or state.legs
            for exchange, symbol in probe_legs:
                targets.append((pair_id, exchange, symbol))
        return targets

    def record_probe_result(
        self,
        pair_id: str,
        exchange: str,
        symbol: str,
        *,
        success: bool,
    ) -> None:
        state = self._pair_states.get(pair_id)
        if not state:
            return
        state.last_probe = self._now()
        if success:
            self.clear_pair(pair_id)
        else:
            normalized = self._normalize_leg((exchange, symbol))
            state.failed_legs.add(normalized)

    # ------------------------------------------------------------------ #
    # 工具方法
    # ------------------------------------------------------------------ #

    def _normalize_leg(self, leg: SymbolLeg) -> SymbolLeg:
        exchange, symbol = leg
        return exchange.lower(), (symbol or "").upper()

    def _now(self) -> datetime:
        return datetime.now(self._timezone)


