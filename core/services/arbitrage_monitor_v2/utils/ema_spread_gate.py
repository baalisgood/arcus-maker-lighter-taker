from __future__ import annotations

import math
import time
from collections import deque
from dataclasses import dataclass
from typing import Deque, Optional, Tuple


@dataclass
class EmaUpdateResult:
    ema: Optional[float]
    strict_ok: bool
    strict_reason: Optional[str]


class TimeWeightedEma:
    """
    时间加权 EMA（对不规则采样友好）。

    - tau_seconds: 时间常数/窗口尺度，越大越平滑
    - update(x, now): 使用 dt 计算 alpha = 1 - exp(-dt / tau)
    """

    def __init__(self, *, tau_seconds: float) -> None:
        self._tau = float(tau_seconds)
        self._ema: Optional[float] = None
        self._last_ts: Optional[float] = None

    @property
    def value(self) -> Optional[float]:
        return self._ema

    def update(self, x: float, *, now: Optional[float] = None) -> float:
        t = time.time() if now is None else float(now)
        v = float(x)
        if self._ema is None or self._last_ts is None:
            self._ema = v
            self._last_ts = t
            return v
        dt = max(0.0, t - self._last_ts)
        if self._tau <= 0:
            self._ema = v
            self._last_ts = t
            return v
        alpha = 1.0 - math.exp(-dt / self._tau)
        self._ema = self._ema + alpha * (v - self._ema)
        self._last_ts = t
        return float(self._ema)


class StrictPerSecondSampler:
    """
    严格采样：要求在最近 window_seconds 覆盖的每一秒内至少有 1 条样本。

    说明：
    - 以 int(time.time()) 的“秒桶”为准
    - 严格模式用于风控：断秒 => strict_ok=False
    """

    def __init__(self, *, window_seconds: float) -> None:
        self._window = max(1, int(round(float(window_seconds))))
        # 仅保存“出现过样本的秒桶”，严格模式要求这些秒桶覆盖最近 window 秒且连续
        # 这里用单调递增的 deque，避免 set/min/max 的额外开销。
        self._seen: Deque[int] = deque()

    def add_sample(self, *, now: Optional[float] = None) -> None:
        t = time.time() if now is None else float(now)
        sec = int(t)
        if self._seen:
            last = self._seen[-1]
            # 同一秒内重复样本：忽略（只需要“每秒至少一条”）
            if sec <= last:
                return
        self._seen.append(sec)
        # prune
        min_keep = sec - self._window + 1
        while self._seen and self._seen[0] < min_keep:
            self._seen.popleft()

    def is_ok(self, *, now: Optional[float] = None) -> Tuple[bool, Optional[str]]:
        t = time.time() if now is None else float(now)
        sec = int(t)
        min_keep = sec - self._window + 1
        # 如果窗口太小，直接认为ok
        if self._window <= 1:
            return True, None
        if len(self._seen) < self._window:
            return False, f"EMA严格模式断秒: 最近{self._window}s样本不足({len(self._seen)}/{self._window})"
        # 要求窗口正好覆盖 [min_keep, sec]
        if self._seen[-1] != sec:
            return False, f"EMA严格模式断秒: 最新秒桶缺样本(sec={sec})"
        if self._seen[0] != min_keep:
            return False, f"EMA严格模式断秒: 窗口起点缺样本({self._seen[0]}≠{min_keep})"
        # 连续性：deque 单调递增且无重复，检查跨度即可
        span = self._seen[-1] - self._seen[0] + 1
        if span != self._window:
            return False, f"EMA严格模式断秒: 秒桶不连续(span={span}≠{self._window})"
        return True, None


class EmaSpreadGateState:
    """
    单个 gate 状态：EMA + 可选严格采样检测
    """

    def __init__(self, *, window_seconds: float, strict_per_second: bool) -> None:
        self._ema = TimeWeightedEma(tau_seconds=window_seconds)
        self._strict_enabled = bool(strict_per_second)
        self._sampler = StrictPerSecondSampler(window_seconds=window_seconds) if self._strict_enabled else None

    def observe(self, x: float, *, now: Optional[float] = None) -> EmaUpdateResult:
        t = time.time() if now is None else float(now)
        ema = self._ema.update(float(x), now=t)
        if self._sampler is not None:
            self._sampler.add_sample(now=t)
            ok, reason = self._sampler.is_ok(now=t)
            return EmaUpdateResult(ema=ema, strict_ok=ok, strict_reason=reason)
        return EmaUpdateResult(ema=ema, strict_ok=True, strict_reason=None)


