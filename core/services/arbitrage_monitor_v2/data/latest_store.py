"""
Latest-only 数据通道（方案A）
=================================

目标：
- 每个 (exchange, symbol) 只保留“最新一份” ticker / orderbook
- 回调侧 O(1) 覆盖写入，不阻塞、不入队
- 处理侧按固定频率拉取“发生过更新的键”，避免队列积压与 QueueFull

设计要点：
- 回调通常是同步函数，不能 await，因此这里使用 threading.Lock 做线程安全保护
- 使用自增 seq 做“是否更新”判定；同一 key 多次更新只保留最后一次（符合 latest-only 语义）
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from threading import Lock
from typing import Any, Callable, Dict, List, Optional, Tuple

import asyncio


_Key = Tuple[str, str]  # (exchange, symbol)


@dataclass(frozen=True)
class LatestPayload:
    """与队列 item 保持一致的 payload 结构，便于复用 DataProcessor 的处理函数。"""

    exchange: str
    symbol: str
    data: Any
    timestamp: datetime
    exchange_timestamp: Optional[datetime] = None
    received_at: Optional[datetime] = None


class LatestStore:
    """latest-only 存储：分别维护 ticker 与 orderbook 的最新值。"""

    def __init__(self) -> None:
        self._lock = Lock()

        self._ticker_seq: int = 0
        self._orderbook_seq: int = 0

        # key -> (seq, payload_dict)
        self._tickers: Dict[_Key, Tuple[int, Dict[str, Any]]] = {}
        self._orderbooks: Dict[_Key, Tuple[int, Dict[str, Any]]] = {}

        # 事件驱动：由 orchestrator 在运行中的 event loop 里绑定
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._event: Optional[asyncio.Event] = None
        self._on_first_notify: Optional[Callable[[], None]] = None
        self._first_notify_fired: bool = False

    def bind_event_loop(
        self,
        loop: asyncio.AbstractEventLoop,
        event: asyncio.Event,
        *,
        on_first_notify: Optional[Callable[[], None]] = None,
    ) -> None:
        """
        绑定事件循环与通知事件（用于事件驱动，而非轮询）。
        注意：必须在 running loop 中调用（通常在 orchestrator.start() 内）。
        """
        with self._lock:
            self._loop = loop
            self._event = event
            self._on_first_notify = on_first_notify
            # 每次重新绑定都重置一次性确认（便于重连/重启后再次确认）
            self._first_notify_fired = False

    def _notify_update(self) -> None:
        """通知处理端“有新数据”。允许从非 asyncio 上下文调用。"""
        loop = None
        ev = None
        cb = None
        fire_cb = False
        with self._lock:
            loop = self._loop
            ev = self._event
            if not self._first_notify_fired and self._on_first_notify is not None:
                self._first_notify_fired = True
                cb = self._on_first_notify
                fire_cb = True
        if loop is None or ev is None:
            return
        try:
            loop.call_soon_threadsafe(ev.set)
            # 只触发一次性确认回调（在 event loop 线程执行）
            if fire_cb and cb is not None:
                loop.call_soon_threadsafe(cb)
        except Exception:
            # 通知失败不影响行情写入（避免反向影响高频路径）
            return

    def set_ticker(
        self,
        *,
        exchange: str,
        symbol: str,
        data: Any,
        timestamp: Optional[datetime] = None,
    ) -> None:
        ts = timestamp or datetime.now()
        payload = {
            "exchange": exchange,
            "symbol": symbol,
            "data": data,
            "timestamp": ts,
        }
        key = (exchange, symbol)
        with self._lock:
            self._ticker_seq += 1
            self._tickers[key] = (self._ticker_seq, payload)
        self._notify_update()

    def set_orderbook(
        self,
        *,
        exchange: str,
        symbol: str,
        data: Any,
        timestamp: Optional[datetime] = None,
        exchange_timestamp: Optional[datetime] = None,
        received_at: Optional[datetime] = None,
    ) -> None:
        ts = timestamp or datetime.now()
        payload = {
            "exchange": exchange,
            "symbol": symbol,
            "data": data,
            "timestamp": ts,
        }
        if exchange_timestamp is not None:
            payload["exchange_timestamp"] = exchange_timestamp
        if received_at is not None:
            payload["received_at"] = received_at

        key = (exchange, symbol)
        with self._lock:
            self._orderbook_seq += 1
            self._orderbooks[key] = (self._orderbook_seq, payload)
        self._notify_update()

    def collect_ticker_updates(self, last_seen: Dict[_Key, int]) -> List[Tuple[_Key, int, Dict[str, Any]]]:
        """
        返回所有 seq 比 last_seen 更新的 ticker payload（每 key 最多 1 条）。
        注意：这里不修改 last_seen，避免“处理端 time_budget 提前中断”导致未处理更新被误标记为已处理。
        """
        out: List[Tuple[_Key, int, Dict[str, Any]]] = []
        with self._lock:
            for key, (seq, payload) in self._tickers.items():
                if seq <= last_seen.get(key, 0):
                    continue
                out.append((key, seq, payload))
        return out

    def collect_orderbook_updates(self, last_seen: Dict[_Key, int]) -> List[Tuple[_Key, int, Dict[str, Any]]]:
        """
        返回所有 seq 比 last_seen 更新的 orderbook payload（每 key 最多 1 条）。
        注意：这里不修改 last_seen，避免未处理更新丢失。
        """
        out: List[Tuple[_Key, int, Dict[str, Any]]] = []
        with self._lock:
            for key, (seq, payload) in self._orderbooks.items():
                if seq <= last_seen.get(key, 0):
                    continue
                out.append((key, seq, payload))
        return out


