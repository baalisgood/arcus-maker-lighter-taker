"""
Variational (Omni) 交易所适配器 - 行情(BBO)版

背景：
- Variational 前端页面仅展示最优买/卖价（BBO），不提供传统 orderbook 深度
- 通过 `POST /api/quotes/indicative` 可获取 bid/ask/mark_price（插件已验证）

本适配器当前只实现“行情读取（get_ticker/get_tickers）”，
其余交易/账户能力后续如需可再扩展（遵循分层架构与 DI 规范）。
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Callable

from ..adapter import ExchangeAdapter
from ..interface import ExchangeConfig
from ..models import (
    BalanceData,
    ExchangeInfo,
    ExchangeType,
    OHLCVData,
    OrderBookData,
    PositionData,
    TickerData,
    TradeData,
    OrderData,
    OrderSide,
    OrderType,
)

from .variational_rest import VariationalIndicativeQuoteRequest, VariationalRestClient


class VariationalAdapter(ExchangeAdapter):
    """Variational 适配器（只提供 BBO 行情）"""

    def __init__(self, config: ExchangeConfig, event_bus=None):
        super().__init__(config, event_bus)
        self._rest = VariationalRestClient(config=config, logger=self.logger)
        self._connected = False

        # 订阅（轮询）任务：Variational 无公开 WS orderbook，这里用轮询实现 subscribe_ticker
        self._ticker_tasks: Dict[str, asyncio.Task] = {}

        # 默认参数均可通过 config.extra_params 覆盖，避免硬编码
        self._funding_interval_s = int(config.extra_params.get("funding_interval_s", 3600))
        self._settlement_asset = str(config.extra_params.get("settlement_asset", "USDC"))
        self._instrument_type = str(config.extra_params.get("instrument_type", "perpetual_future"))

        # 指定一个“询价数量”，用于得到接近你实际下单量的 bid/ask
        # 说明：Variational 的 quote 是“按 qty 询价”，不同 qty 会有不同滑点
        self._default_quote_qty = str(config.extra_params.get("indicative_quote_qty", "0.001"))

        # 可选：健康检查/心跳用的默认币种
        self._health_symbol = str(config.extra_params.get("health_check_symbol", "BTC")).upper()

        # ticker 轮询间隔（毫秒），可在配置中覆盖
        self._ticker_poll_interval_ms = int(config.extra_params.get("ticker_poll_interval_ms", 500))

    # ===== 生命周期 / 连接 =====

    async def _do_connect(self) -> bool:
        self._connected = True
        return True

    async def _do_disconnect(self) -> None:
        self._connected = False
        await self.unsubscribe(None)
        await self._rest.close()

    async def _do_authenticate(self) -> bool:
        # 仅行情：不需要 API key；交易能力后续扩展再做鉴权
        return True

    async def _do_health_check(self) -> Dict[str, Any]:
        data: Dict[str, Any] = {"connected": self._connected}
        try:
            ticker = await self.get_ticker(self._health_symbol)
            data["ticker_ok"] = True
            data["bid"] = float(ticker.bid) if ticker.bid is not None else None
            data["ask"] = float(ticker.ask) if ticker.ask is not None else None
            data["mark_price"] = float(ticker.mark_price) if ticker.mark_price is not None else None
        except Exception as e:
            data["ticker_ok"] = False
            data["error"] = str(e)
        return data

    async def _do_heartbeat(self) -> None:
        # 用一次轻量的 quote 作为心跳
        await self.get_ticker(self._health_symbol)

    # ===== 市场数据接口 =====

    async def get_exchange_info(self) -> ExchangeInfo:
        return ExchangeInfo(
            name="Variational Omni",
            id="variational",
            type=ExchangeType.PERPETUAL,
            supported_features=[
                "ticker",
                "bbo_quote",
            ],
            rate_limits=self.config.rate_limits if self.config else {},
            precision=self.config.precision if self.config else {},
            fees={},
            markets={},
            status="operational" if self._connected else "disconnected",
            timestamp=datetime.now(),
        )

    async def get_ticker(self, symbol: str) -> TickerData:
        """
        获取单个交易对行情（BBO）

        说明：
        - Variational 不是传统 orderbook 模式
        - bid/ask 来自 quote（按 qty 询价）
        """

        normalized = self._normalize_symbol(symbol)
        qty = self._get_quote_qty_for_symbol(normalized)
        request = VariationalIndicativeQuoteRequest(
            underlying=normalized,
            qty=qty,
            funding_interval_s=self._funding_interval_s,
            settlement_asset=self._settlement_asset,
            instrument_type=self._instrument_type,
        )

        async def _do_call() -> Dict[str, Any]:
            return await self._rest.get_indicative_quote(request)

        raw = await self._execute_with_retry(_do_call, operation_name="get_ticker")

        bid = self._safe_decimal(raw.get("bid"))
        ask = self._safe_decimal(raw.get("ask"))
        mark_price = self._safe_decimal(raw.get("mark_price"))

        # 统一输出：last 取 mark_price（若无则取中间价）
        last = mark_price
        if last == Decimal("0") and bid > 0 and ask > 0:
            last = (bid + ask) / Decimal("2")

        now = datetime.now()
        return TickerData(
            symbol=normalized,
            timestamp=now,
            bid=bid if bid > 0 else None,
            ask=ask if ask > 0 else None,
            last=last if last > 0 else None,
            mark_price=mark_price if mark_price > 0 else None,
            funding_interval=self._funding_interval_s,
            base_currency=normalized,
            quote_currency=self._settlement_asset,
            received_timestamp=now,
            raw_data={"quote_qty": qty, **raw},
        )

    async def get_tickers(self, symbols: Optional[List[str]] = None) -> List[TickerData]:
        # Variational “全市场列表”不在本适配器范围内，必须显式给 symbols
        if not symbols:
            raise ValueError("VariationalAdapter.get_tickers 需要显式提供 symbols 列表")

        tasks = [self.get_ticker(s) for s in symbols]
        return await self._gather_safely(tasks)

    async def get_orderbook(self, symbol: str, limit: Optional[int] = None) -> OrderBookData:
        """
        Variational 不提供深度 orderbook：返回空结构，避免上层误用。
        """
        normalized = self._normalize_symbol(symbol)
        now = datetime.now()
        return OrderBookData(
            symbol=normalized,
            bids=[],
            asks=[],
            timestamp=now,
            nonce=None,
            received_timestamp=now,
            raw_data={"note": "Variational does not provide orderbook depth; use get_ticker() for bid/ask"},
        )

    async def get_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        since: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> List[OHLCVData]:
        raise NotImplementedError("VariationalAdapter 暂不支持 K线数据（仅实现 BBO 行情）")

    async def get_trades(
        self,
        symbol: str,
        since: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> List[TradeData]:
        raise NotImplementedError("VariationalAdapter 暂不支持成交数据（仅实现 BBO 行情）")

    # ===== 账户与交易接口（当前不实现） =====

    async def get_balances(self) -> List[BalanceData]:
        raise NotImplementedError("VariationalAdapter 暂不支持账户余额（仅实现 BBO 行情）")

    async def get_positions(self, symbols: Optional[List[str]] = None) -> List[PositionData]:
        raise NotImplementedError("VariationalAdapter 暂不支持持仓（仅实现 BBO 行情）")

    async def create_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        amount: Decimal,
        price: Optional[Decimal] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> OrderData:
        raise NotImplementedError("VariationalAdapter 暂不支持下单（当前只实现A：行情）")

    async def cancel_order(self, order_id: str, symbol: str) -> OrderData:
        raise NotImplementedError("VariationalAdapter 暂不支持撤单（当前只实现A：行情）")

    async def get_order(self, order_id: str, symbol: str) -> OrderData:
        raise NotImplementedError("VariationalAdapter 暂不支持查询订单（当前只实现A：行情）")

    async def get_open_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        raise NotImplementedError("VariationalAdapter 暂不支持查询开放订单（当前只实现A：行情）")

    async def cancel_all_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        raise NotImplementedError("VariationalAdapter 暂不支持取消全部订单（当前只实现A：行情）")

    async def get_order_history(
        self,
        symbol: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> List[OrderData]:
        raise NotImplementedError("VariationalAdapter 暂不支持历史订单（当前只实现A：行情）")

    async def set_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        raise NotImplementedError("VariationalAdapter 暂不支持设置杠杆（当前只实现A：行情）")

    async def set_margin_mode(self, symbol: str, margin_mode: str) -> Dict[str, Any]:
        raise NotImplementedError("VariationalAdapter 暂不支持设置保证金模式（当前只实现A：行情）")

    # ===== 实时数据流接口（ticker 用轮询实现；orderbook/trades/user_data 暂不支持）=====

    async def subscribe_ticker(self, symbol: str, callback: Callable[[TickerData], None]) -> None:
        """
        订阅 ticker：
        - Variational 无公开 orderbook，BBO 来自 /api/quotes/indicative
        - 用轮询实现实时推送给上层
        """
        normalized = self._normalize_symbol(symbol)
        if normalized in self._ticker_tasks and not self._ticker_tasks[normalized].done():
            return

        async def _loop() -> None:
            interval = max(50, self._ticker_poll_interval_ms) / 1000.0
            backoff = 0.2
            while True:
                try:
                    ticker = await self.get_ticker(normalized)
                    await self._invoke_callback(callback, ticker)
                    backoff = 0.2
                    await asyncio.sleep(interval)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    # 避免刷屏，简单退避
                    self.logger.warning(f"Variational ticker poll error ({normalized}): {e}")
                    await asyncio.sleep(min(5.0, backoff))
                    backoff = min(5.0, backoff * 2)

        self._ticker_tasks[normalized] = asyncio.create_task(_loop())

    async def subscribe_orderbook(self, symbol: str, callback: Callable[[OrderBookData], None]) -> None:
        """
        Variational 不提供深度 orderbook。
        为了不让上层订阅流程直接失败，这里会推送一次“空 orderbook”，并在 raw_data 里注明原因。
        """
        ob = await self.get_orderbook(symbol)
        await self._invoke_callback(callback, ob)

    async def subscribe_trades(self, symbol: str, callback: Callable[[TradeData], None]) -> None:
        raise NotImplementedError("VariationalAdapter 暂不支持订阅成交（当前只实现A：行情）")

    async def subscribe_user_data(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        raise NotImplementedError("VariationalAdapter 暂不支持用户数据流（当前只实现A：行情）")

    async def unsubscribe(self, symbol: Optional[str] = None) -> None:
        """
        取消订阅：
        - symbol=None：取消所有 ticker 轮询任务
        - 否则取消指定币种 ticker 轮询
        """
        if symbol is None:
            tasks = list(self._ticker_tasks.values())
            self._ticker_tasks.clear()
            for t in tasks:
                t.cancel()
            return

        normalized = self._normalize_symbol(symbol)
        task = self._ticker_tasks.pop(normalized, None)
        if task:
            task.cancel()

    # ===== 内部工具 =====

    def _normalize_symbol(self, symbol: str) -> str:
        # Variational 页面与插件都按 underlying（如 BTC/ETH）工作
        # 这里兼容传入 BTC/USDC、BTC-USDC、BTC/USDC:PERP 等格式
        s = (symbol or "").strip().upper()
        if not s:
            raise ValueError("symbol 不能为空")
        base = s.split(":")[0]
        base = base.split("/")[0].split("-")[0]
        return base

    def _get_quote_qty_for_symbol(self, symbol: str) -> str:
        per_symbol = self.config.extra_params.get("indicative_quote_qty_by_symbol")
        if isinstance(per_symbol, dict):
            v = per_symbol.get(symbol)
            if isinstance(v, (int, float, str)) and str(v).strip():
                return str(v)
        return self._default_quote_qty

    async def _gather_safely(self, tasks: List[Any]) -> List[Any]:
        # 避免一个 symbol 失败导致全部失败；失败的任务会被记录并跳过
        results: List[Any] = []
        gathered = await asyncio.gather(*tasks, return_exceptions=True)
        for r in gathered:
            if isinstance(r, Exception):
                self.logger.warning(f"Variational get_ticker failed: {r}")
                continue
            results.append(r)
        return results

    async def _invoke_callback(self, callback: Callable, data: Any) -> None:
        """兼容同步/异步回调"""
        try:
            result = callback(data)
            if asyncio.iscoroutine(result):
                await result
        except Exception as e:
            self.logger.error(f"Variational callback error: {e}")


