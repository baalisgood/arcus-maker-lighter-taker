"""
GRVT 交易所适配器 - WebSocket 模块

### 设计目标（对齐当前项目的套利/网格/做市调用方式）
- **公共行情 WS**：ticker / book / trade
- **私有交易 WS**：订单推送（v1.order）/ 持仓推送（v1.position）
- 同时兼容两类上层调用：
  - 网格/做市：调用 `adapter.subscribe_user_data(...)`（通常希望收到 OrderData）
  - 套利执行器：直接调用 `adapter._websocket.subscribe_orders(...)` / `subscribe_order_fills(...)`

### GRVT WS 协议要点（来自官方文档 / 离线 mhtml）
1) 订阅/取消订阅走 JSON-RPC wrapper（Full 版本示例）：
{
  "jsonrpc": "2.0",
  "method": "subscribe",
  "params": { "stream": "v1.ticker.s", "selectors": ["BTC_USDT_Perp@500"] },
  "id": 123
}

2) 推送的业务数据通常是“裸 payload”，不是 jsonrpc 包裹：
{
  "stream": "v1.mini.s",
  "selector": "BTC_USDT_Perp",
  "sequence_number": "872634876",
  "feed": {...}
}

3) 交易 WS（trades.*）需要鉴权 header：
- Cookie: gravity=...
- X-Grvt-Account-Id: ...
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from decimal import Decimal
from datetime import datetime, timezone

try:
    import websockets
    WEBSOCKETS_AVAILABLE = True
except Exception:
    WEBSOCKETS_AVAILABLE = False
    websockets = None  # type: ignore

from ..models import TickerData, OrderBookData, TradeData, OrderBookLevel, OrderSide, OrderData, OrderStatus
from .grvt_base import GRVTBase, unix_ns_to_datetime


class GRVTWebSocket(GRVTBase):
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        if not WEBSOCKETS_AVAILABLE:
            raise ImportError("GRVTWebSocket 依赖 websockets 包，请先安装/启用该依赖")

        self._market_ws = None
        self._trade_ws = None
        self._market_task: Optional[asyncio.Task] = None
        self._trade_task: Optional[asyncio.Task] = None

        # 各类行情回调（按合约维度保存）
        self._ticker_cbs: Dict[str, List[Callable[[TickerData], None]]] = {}
        self._book_cbs: Dict[str, List[Callable[[OrderBookData], None]]] = {}
        self._trade_cbs: Dict[str, List[Callable[[TradeData], None]]] = {}
        # 用户/交易类推送回调（来自交易 WS）
        self._user_cbs: List[Callable[..., Any]] = []
        self._order_cbs: List[Callable[..., Any]] = []
        self._order_fill_cbs: List[Callable[..., Any]] = []
        self._position_cbs: List[Callable[..., Any]] = []

        # 可选解析器（由适配器注入）：订单字典 -> OrderData
        self._order_parser: Optional[Callable[[Dict[str, Any]], OrderData]] = None

        # 当前已订阅集合（订阅流/选择器组合）
        self._market_subs: Set[Tuple[str, str]] = set()
        self._trade_subs: Set[Tuple[str, str]] = set()

        self._selector_rate_ms = int(
            (config.get("extra_params", {}) or {}).get("ws_rate_ms", 500)
        )

    def _auth_headers(self) -> List[Tuple[str, str]]:
        """交易 WS 鉴权请求头（来自 REST 登录的会话凭据与账户 ID）。"""
        headers: List[Tuple[str, str]] = []
        if self._cookie_gravity:
            headers.append(("Cookie", f"gravity={self._cookie_gravity}"))
        if self._account_id_header:
            headers.append(("X-Grvt-Account-Id", self._account_id_header))
        return headers

    async def close(self) -> None:
        """关闭 WS 连接与后台接收循环。"""
        for task in (self._market_task, self._trade_task):
            if task:
                task.cancel()
        self._market_task = None
        self._trade_task = None

        if self._market_ws:
            await self._market_ws.close()
            self._market_ws = None
        if self._trade_ws:
            await self._trade_ws.close()
            self._trade_ws = None

    async def _ensure_market_ws(self) -> None:
        """确保公共行情 WS 已连接并启动接收循环。"""
        if self._market_ws:
            return
        if not self.market_ws:
            raise RuntimeError("GRVT market ws endpoint is empty")
        self._market_ws = await websockets.connect(self.market_ws + "/full", ping_interval=20, ping_timeout=20)
        self._market_task = asyncio.create_task(self._market_loop())

    async def _ensure_trade_ws(self) -> None:
        """确保交易/私有 WS 已连接并启动接收循环（需要鉴权请求头）。"""
        if self._trade_ws:
            return
        if not self.trade_ws:
            raise RuntimeError("GRVT trade ws endpoint is empty")
        # 交易 WS 需要带鉴权请求头（会话凭据 + X-Grvt-Account-Id）
        self._trade_ws = await websockets.connect(
            self.trade_ws + "/full",
            extra_headers=self._auth_headers(),
            ping_interval=20,
            ping_timeout=20,
        )
        self._trade_task = asyncio.create_task(self._trade_loop())

    async def _send_subscribe(self, ws, stream: str, selectors: List[str]) -> None:
        """发送 JSON-RPC 订阅请求。"""
        req_id = int(time.time() * 1000) % 1_000_000_000
        payload = {"jsonrpc": "2.0", "method": "subscribe", "params": {"stream": stream, "selectors": selectors}, "id": req_id}
        await ws.send(json.dumps(payload))

    async def _send_unsubscribe(self, ws, stream: str, selectors: List[str]) -> None:
        """发送 JSON-RPC 取消订阅请求。"""
        req_id = int(time.time() * 1000) % 1_000_000_000
        payload = {"jsonrpc": "2.0", "method": "unsubscribe", "params": {"stream": stream, "selectors": selectors}, "id": req_id}
        await ws.send(json.dumps(payload))

    async def subscribe_ticker(self, symbol: str, callback: Callable[[TickerData], None]) -> None:
        """订阅盘口概览（公共行情通道，v1.ticker.s）。"""
        sym = self.normalize_symbol(symbol)
        self._ticker_cbs.setdefault(sym, []).append(callback)
        await self._ensure_market_ws()
        selector = f"{sym}@{self._selector_rate_ms}"
        self._market_subs.add(("v1.ticker.s", selector))
        await self._send_subscribe(self._market_ws, "v1.ticker.s", [selector])

    async def subscribe_orderbook(self, symbol: str, callback: Callable[[OrderBookData], None], depth: int = 50) -> None:
        """订阅深度（公共行情 WS，v1.book.s）。"""
        sym = self.normalize_symbol(symbol)
        self._book_cbs.setdefault(sym, []).append(callback)
        await self._ensure_market_ws()
        depth_val = depth if depth in (10, 50, 100, 500) else 50
        selector = f"{sym}@{self._selector_rate_ms}-{depth_val}"
        self._market_subs.add(("v1.book.s", selector))
        await self._send_subscribe(self._market_ws, "v1.book.s", [selector])

    async def subscribe_trades(self, symbol: str, callback: Callable[[TradeData], None]) -> None:
        """订阅最近成交（公共行情 WS，v1.trade）。"""
        sym = self.normalize_symbol(symbol)
        self._trade_cbs.setdefault(sym, []).append(callback)
        await self._ensure_market_ws()
        selector = f"{sym}@{self._selector_rate_ms}"
        self._market_subs.add(("v1.trade", selector))
        await self._send_subscribe(self._market_ws, "v1.trade", [selector])

    async def subscribe_user_orders(self, symbol: Optional[str], callback: Callable[[Dict[str, Any]], None]) -> None:
        """
        订阅订单推送（交易 WS，v1.order）。

        selector 规则（文档）：
        - 全部订单："<sub_account_id>-all"
        - 单品种："<sub_account_id>-<instrument>"
        """
        if callback not in self._user_cbs:
            self._user_cbs.append(callback)
        if not self.sub_account_id:
            raise ValueError("GRVT 订阅用户推送需要 sub_account_id（交易子账户 ID）")
        await self._ensure_trade_ws()
        if symbol:
            sym = self.normalize_symbol(symbol)
            selector = f"{self.sub_account_id}-{sym}"
        else:
            selector = f"{self.sub_account_id}-all"
        self._trade_subs.add(("v1.order", selector))
        await self._send_subscribe(self._trade_ws, "v1.order", [selector])

    async def subscribe_user_data(self, callback: Callable[..., Any]) -> None:
        """
        兼容接口：上层（套利执行器）会直接调用 ws.subscribe_user_data(...)
        - 本模块会订阅 v1.order
        - 推送原始 msg dict 会透传给 callback（兼容旧逻辑）
        - 如果注入了订单解析器（order dict -> OrderData），则 callback 也可能收到 OrderData（取决于上层调用 subscribe_orders）
        """
        if callback not in self._user_cbs:
            self._user_cbs.append(callback)
        # 确保已订阅订单推送（默认订阅全部品种）
        await self.subscribe_orders(callback)

    async def subscribe_orders(self, callback: Callable[..., Any], symbol: Optional[str] = None) -> None:
        """
        订单推送的标准订阅入口（供套利执行器使用）。

        - 如果 adapter 注入了 `_order_parser`（order dict -> OrderData）：
          - 本模块会把 v1.order 的 feed 解析成 OrderData 再回调
        - 否则：
          - 回调收到原始 msg dict
        """
        if callback not in self._order_cbs:
            self._order_cbs.append(callback)
        # 确保已订阅订单推送流
        await self.subscribe_user_orders(symbol=symbol, callback=lambda *_a, **_k: None)

    async def subscribe_order_fills(self, callback: Callable[..., Any], symbol: Optional[str] = None) -> None:
        """
        兼容接口：有些上层在 subscribe_orders 不可用时会降级使用 subscribe_order_fills。
        我们这里基于 v1.order 的推送做过滤：
        - 当解析出的 OrderData.status == FILLED 时触发回调
        """
        if callback not in self._order_fill_cbs:
            self._order_fill_cbs.append(callback)
        await self.subscribe_orders(callback, symbol=symbol)

    async def subscribe_positions(self, callback: Callable[..., Any], symbol: Optional[str] = None) -> None:
        """
        订阅持仓推送（交易 WS，v1.position）。

        selector 规则与 v1.order 类似：
        - 全部持仓："<sub_account_id>"
        - 单品种："<sub_account_id>-<instrument>"
        """
        if callback not in self._position_cbs:
            self._position_cbs.append(callback)
        if not self.sub_account_id:
            raise ValueError("GRVT 订阅持仓推送需要 sub_account_id（交易子账户 ID）")
        await self._ensure_trade_ws()
        if symbol:
            sym = self.normalize_symbol(symbol)
            selector = f"{self.sub_account_id}-{sym}"
        else:
            selector = str(self.sub_account_id)
        self._trade_subs.add(("v1.position", selector))
        await self._send_subscribe(self._trade_ws, "v1.position", [selector])

    async def unsubscribe_all(self, symbol: Optional[str] = None) -> None:
        """取消订阅（按交易对过滤或全部取消）。"""
        if self._market_ws:
            to_remove = []
            for stream, selector in list(self._market_subs):
                if symbol and self.normalize_symbol(symbol) not in selector:
                    continue
                await self._send_unsubscribe(self._market_ws, stream, [selector])
                to_remove.append((stream, selector))
            for k in to_remove:
                self._market_subs.discard(k)
        if self._trade_ws:
            to_remove = []
            for stream, selector in list(self._trade_subs):
                if symbol and self.normalize_symbol(symbol) not in selector:
                    continue
                await self._send_unsubscribe(self._trade_ws, stream, [selector])
                to_remove.append((stream, selector))
            for k in to_remove:
                self._trade_subs.discard(k)

    async def _market_loop(self) -> None:
        """公共行情 WS 接收循环：逐条解析并分发到各类行情回调。"""
        assert self._market_ws is not None
        async for raw in self._market_ws:
            try:
                msg = json.loads(raw)
            except Exception:
                continue
            await self._handle_market_message(msg)

    async def _trade_loop(self) -> None:
        """交易 WS 接收循环：逐条解析并分发到订单/成交/持仓回调。"""
        assert self._trade_ws is not None
        async for raw in self._trade_ws:
            try:
                msg = json.loads(raw)
            except Exception:
                continue
            await self._handle_trade_message(msg)

    async def _handle_market_message(self, msg: Dict[str, Any]) -> None:
        """处理公共行情推送（v1.ticker.* / v1.book.* / v1.trade）。"""
        if "feed" not in msg or "stream" not in msg:
            return
        stream = str(msg.get("stream", ""))
        feed = msg.get("feed") or {}
        if not isinstance(feed, dict):
            return
        instrument = str(feed.get("instrument") or "")
        if instrument:
            instrument = self.normalize_symbol(instrument)

        if stream.startswith("v1.ticker") or stream.startswith("v1.mini"):
            ticker = self._parse_ticker(feed)
            if ticker and instrument in self._ticker_cbs:
                for cb in list(self._ticker_cbs[instrument]):
                    cb(ticker)
            return

        if stream.startswith("v1.book"):
            ob = self._parse_orderbook(feed)
            if ob and instrument in self._book_cbs:
                for cb in list(self._book_cbs[instrument]):
                    cb(ob)
            return

        if stream.startswith("v1.trade"):
            t = self._parse_trade(feed)
            if t and instrument in self._trade_cbs:
                for cb in list(self._trade_cbs[instrument]):
                    cb(t)
            return

    async def _handle_trade_message(self, msg: Dict[str, Any]) -> None:
        """
        处理交易/私有推送：
        - v1.order：可解析为 OrderData 并分发给订单回调
        - v1.position：持仓推送（这里先把 feed/raw 透传给回调）
        """
        if "feed" not in msg or "stream" not in msg:
            return
        stream = str(msg.get("stream") or "")

        # 1) 永远透传原始消息给用户回调（兼容旧代码以 dict 处理）
        for cb in list(self._user_cbs):
            try:
                cb(msg)
            except Exception:
                continue

        if stream == "v1.order":
            feed = msg.get("feed")
            # 2) 如果注入了解析器，则把 feed -> OrderData（供网格/套利的 OrderData 分支处理）
            if isinstance(feed, dict) and self._order_parser:
                try:
                    order = self._order_parser(feed)
                except Exception:
                    order = None
                if order is not None:
                    for cb in list(self._order_cbs):
                        try:
                            cb(order)
                        except Exception:
                            continue
                    if order.status == OrderStatus.FILLED:
                        for cb in list(self._order_fill_cbs):
                            try:
                                cb(order)
                            except Exception:
                                continue
                    return

            # 解析失败：回退为原始消息
            for cb in list(self._order_cbs):
                try:
                    cb(msg)
                except Exception:
                    continue
            return

        if stream == "v1.position":
            feed = msg.get("feed")
            for cb in list(self._position_cbs):
                try:
                    cb(feed if isinstance(feed, dict) else msg)
                except Exception:
                    continue

    def _parse_ticker(self, feed: Dict[str, Any]) -> Optional[TickerData]:
        try:
            symbol = self.normalize_symbol(str(feed.get("instrument") or ""))
            ts = unix_ns_to_datetime(str(feed.get("event_time") or ""))
            bid = feed.get("best_bid_price")
            ask = feed.get("best_ask_price")
            bid_sz = feed.get("best_bid_size")
            ask_sz = feed.get("best_ask_size")
            last = feed.get("last_price")
            mark = feed.get("mark_price")
            index = feed.get("index_price")
            return TickerData(
                symbol=symbol,
                timestamp=ts,
                bid=Decimal(str(bid)) if bid is not None else None,
                ask=Decimal(str(ask)) if ask is not None else None,
                bid_size=Decimal(str(bid_sz)) if bid_sz is not None else None,
                ask_size=Decimal(str(ask_sz)) if ask_sz is not None else None,
                last=Decimal(str(last)) if last is not None else None,
                close=Decimal(str(last)) if last is not None else None,
                mark_price=Decimal(str(mark)) if mark is not None else None,
                index_price=Decimal(str(index)) if index is not None else None,
                raw_data=feed,
            )
        except Exception:
            return None

    def _parse_orderbook(self, feed: Dict[str, Any]) -> Optional[OrderBookData]:
        try:
            symbol = self.normalize_symbol(str(feed.get("instrument") or ""))
            ts = unix_ns_to_datetime(str(feed.get("event_time") or ""))
            bids = []
            for lvl in feed.get("bids") or []:
                if isinstance(lvl, dict):
                    bids.append(
                        OrderBookLevel(
                            price=Decimal(str(lvl.get("price"))),
                            size=Decimal(str(lvl.get("size"))),
                            count=int(lvl.get("num_orders")) if lvl.get("num_orders") is not None else None,
                        )
                    )
            asks = []
            for lvl in feed.get("asks") or []:
                if isinstance(lvl, dict):
                    asks.append(
                        OrderBookLevel(
                            price=Decimal(str(lvl.get("price"))),
                            size=Decimal(str(lvl.get("size"))),
                            count=int(lvl.get("num_orders")) if lvl.get("num_orders") is not None else None,
                        )
                    )
            return OrderBookData(symbol=symbol, bids=bids, asks=asks, timestamp=ts, raw_data=feed)
        except Exception:
            return None

    def _parse_trade(self, feed: Dict[str, Any]) -> Optional[TradeData]:
        try:
            # 行情成交字段通常包含：
            # event_time / instrument / is_taker_buyer / size / price / trade_id
            symbol = self.normalize_symbol(str(feed.get("instrument") or ""))
            ts = unix_ns_to_datetime(str(feed.get("event_time") or ""))
            side = OrderSide.BUY if bool(feed.get("is_taker_buyer")) else OrderSide.SELL
            price = Decimal(str(feed.get("price")))
            size = Decimal(str(feed.get("size")))
            trade_id = str(feed.get("trade_id") or uuid.uuid4().hex)
            return TradeData(
                id=trade_id,
                symbol=symbol,
                side=side,
                amount=size,
                price=price,
                timestamp=ts,
                cost=price * size,
                fee=None,
                raw_data=feed,
            )
        except Exception:
            return None


