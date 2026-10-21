"""
GRVT 交易所适配器（去中心化永续）

本文件实现 `ExchangeInterface`（通过继承 `ExchangeAdapter`），用于让 GRVT 以“统一交易所”的方式接入
当前系统的套利 / 网格 / 做市等模块。

### 文档来源（离线）
- `docs/grvt/source/Market Data API - Gravity Markets API Docs.mhtml`
- `docs/grvt/source/Market Data Websocket - Gravity Markets API Docs.mhtml`
- `docs/grvt/source/Trading API - Gravity Markets API Docs.mhtml`
- `docs/grvt/source/Trading Websocket - Gravity Markets API Docs.mhtml`

### 官方签名参考（离线镜像）
- `docs/grvt/pysdk_ref/grvt_raw_signing.py`

### 关键协议/模型约定（简要）
- **所有 REST 接口统一 POST**
- 行情域：`market-data.*`（无需鉴权）
- 交易域：`trades.*`（需要 `gravity` cookie + `X-Grvt-Account-Id`）
- 下单需要 EIP-712 签名（eth-account encode_typed_data）
- GRVT 账户为两层结构：主账户 + 交易子账户（`sub_account_id`，uint64 字符串）

### 与本项目策略对接的关键点
- 网格引擎要求：`cancel_all_orders()` 返回 `List[OrderData]`，并 `subscribe_user_data` 能推 `OrderData`
- 套利执行器可能会直接访问 `adapter._websocket` 并调用 `subscribe_orders/subscribe_order_fills/subscribe_positions`
  因此 GRVT WS 需要提供这些方法（已在 `grvt_websocket.py` 补齐）。
"""

from __future__ import annotations

import random
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from decimal import ROUND_DOWN
from typing import Any, Callable, Dict, List, Optional

from eth_account import Account
from eth_account.messages import encode_typed_data

from ..adapter import ExchangeAdapter
from ..interface import ExchangeConfig
from ..models import (
    BalanceData,
    ExchangeInfo,
    ExchangeType,
    MarginMode,
    OHLCVData,
    OrderBookData,
    OrderBookLevel,
    OrderData,
    OrderSide,
    OrderStatus,
    OrderType,
    PositionData,
    PositionSide,
    TickerData,
    TradeData,
)

from .grvt_base import GRVTBase, unix_ns_to_datetime, datetime_to_unix_ns
from .grvt_rest import GRVTRest
from .grvt_websocket import GRVTWebSocket


# GRVT 订单签名中价格统一按 9 位小数编码成整数（与官方 Python 开发包保持一致）
PRICE_MULTIPLIER = Decimal("1000000000")  # 1e9


EIP712_ORDER_MESSAGE_TYPE: Dict[str, Any] = {
    "Order": [
        {"name": "subAccountID", "type": "uint64"},
        {"name": "isMarket", "type": "bool"},
        {"name": "timeInForce", "type": "uint8"},
        {"name": "postOnly", "type": "bool"},
        {"name": "reduceOnly", "type": "bool"},
        {"name": "legs", "type": "OrderLeg[]"},
        {"name": "nonce", "type": "uint32"},
        {"name": "expiration", "type": "int64"},
    ],
    "OrderLeg": [
        {"name": "assetID", "type": "uint256"},
        {"name": "contractSize", "type": "uint64"},
        {"name": "limitPrice", "type": "uint64"},
        {"name": "isBuyingContract", "type": "bool"},
    ],
}


TIME_IN_FORCE_TO_SIGN_CODE = {
    "GOOD_TILL_TIME": 1,
    "ALL_OR_NONE": 2,
    "IMMEDIATE_OR_CANCEL": 3,
    "FILL_OR_KILL": 4,
}


TIMEFRAME_TO_GRVT_INTERVAL = {
    "1m": "CI_1_M",
    "3m": "CI_3_M",
    "5m": "CI_5_M",
    "15m": "CI_15_M",
    "30m": "CI_30_M",
    "1h": "CI_1_H",
    "2h": "CI_2_H",
    "4h": "CI_4_H",
    "6h": "CI_6_H",
    "8h": "CI_8_H",
    "12h": "CI_12_H",
    "1d": "CI_1_D",
    "3d": "CI_3_D",
    "1w": "CI_1_W",
}


class GRVTAdapter(ExchangeAdapter):
    """
    GRVT 适配器（永续合约 / DEX）

    结构分层（与其它交易所适配器保持一致）：
    - `GRVTBase`：环境/域名/符号规范化/缓存容器
    - `GRVTRest`：REST 调用（market/trade）+ API Key 登录换 cookie/header
    - `GRVTWebSocket`：WS 订阅（行情/订单/持仓）+ 推送分发

    注意：
    - 下单（create_order）必须使用 `private_key` 做 EIP-712 签名
    - 交易/账户接口必须有 `sub_account_id`
    """

    def __init__(self, config: ExchangeConfig, event_bus: Optional[Any] = None):
        super().__init__(config, event_bus)
        # 与其它适配器保持一致：如果上层注入的是包装过的 logger，这里取出真实 logger
        if self.logger and hasattr(self.logger, "logger"):
            self.logger = self.logger.logger

        # 将统一配置对象转换为轻量 dict（供 GRVT 子模块读取）
        self._config_dict = self._convert_config_to_dict(config)
        self._base = GRVTBase(self._config_dict)
        self._rest = GRVTRest(self._config_dict)
        self._websocket = GRVTWebSocket(self._config_dict) if config.enable_websocket else None
        if self._websocket is not None:
            # 🔥 关键：把“订单推送解析器”注入到 WS 模块
            # WS 收到 v1.order 推送数据时，可解析为本项目统一的 OrderData，
            # 以满足网格/套利对 OrderData 回调的期待。
            self._websocket._order_parser = self._to_order_data  # type: ignore[attr-defined]

        # 暴露 base_url/ws_url（便于外部查看或调试）
        self.base_url = self._base.trade_rpc
        self.ws_url = self._base.trade_ws

        self._supported_symbols: List[str] = []
        self._market_info: Dict[str, Any] = {}

        # 轻量缓存：方便 UI/策略层快速读取（不作为强一致数据源）
        self._order_cache: Dict[str, OrderData] = {}
        self._position_cache: Dict[str, PositionData] = {}

    def _convert_config_to_dict(self, config: ExchangeConfig) -> Dict[str, Any]:
        """
        将统一的 ExchangeConfig 转换为 GRVT 子模块可读的 dict。

        说明：
        - GRVT 的 `sub_account_id` 放在 `extra_params["sub_account_id"]`
        - env/testnet 由 config/testnet 或 extra_params/env 决定
        """
        extra = dict(config.extra_params or {})
        # 允许在 YAML 的 extra_params 里配置 env/sub_account_id（敏感字段仍建议走环境变量）
        if config.testnet and "env" not in extra:
            extra["env"] = "testnet"
        return {
            "exchange_id": config.exchange_id,
            "testnet": bool(config.testnet),
            "request_timeout": int(getattr(config, "request_timeout", 10) or 10),
            "enable_websocket": bool(getattr(config, "enable_websocket", True)),
            "api_key": config.api_key,
            "private_key": config.private_key,
            "extra_params": extra,
        }

    # ========= 生命周期钩子（由 ExchangeAdapter 调用） =========

    async def _do_connect(self) -> bool:
        # HTTP 会话懒加载；连接时预热合约信息，用于：
        # - get_supported_symbols / get_exchange_info
        # - 下单签名：需要 instrument_hash/base_decimals 等字段
        try:
            await self._refresh_instruments()
            return True
        except Exception as e:
            if self.logger:
                self.logger.warning(f"[GRVT] connect：预热 instruments 失败（不影响继续连接）：{e}")
            return True  # 即使预热失败也允许连接继续（避免因为行情端点抖动导致整体启动失败）

    async def _do_disconnect(self) -> None:
        # 关闭 WS 与 HTTP 会话
        if self._websocket:
            await self._websocket.close()
        await self._rest.close()

    async def _do_authenticate(self) -> bool:
        # API Key（接口密钥）登录，获取 gravity 会话凭据与 X-Grvt-Account-Id，并同步到 WS 模块
        try:
            await self._rest.login()
            # 同步会话凭据与账户请求头到 base/websocket（交易 WS 需要用到鉴权请求头）
            self._base._cookie_gravity = self._rest._cookie_gravity
            self._base._cookie_expires_at = self._rest._cookie_expires_at
            self._base._account_id_header = self._rest._account_id_header
            if self._websocket:
                self._websocket._cookie_gravity = self._rest._cookie_gravity
                self._websocket._cookie_expires_at = self._rest._cookie_expires_at
                self._websocket._account_id_header = self._rest._account_id_header
            return True
        except Exception as e:
            if self.logger:
                self.logger.error(f"[GRVT] authenticate failed: {e}")
            return False

    async def _do_health_check(self) -> Dict[str, Any]:
        # 简单健康检查：调用一次行情端点 mini（公共，无需鉴权）
        try:
            if not self._supported_symbols:
                await self._refresh_instruments()
            sym = self._supported_symbols[0] if self._supported_symbols else "BTC_USDT_Perp"
            _ = await self._rest.post_market("full/v1/mini", {"instrument": self._base.normalize_symbol(sym)})
            return {"ok": True}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    # ========= 内部辅助方法 =========

    async def _refresh_instruments(self) -> None:
        """
        拉取所有 instruments，并缓存到 base/rest/ws。

        GRVT 使用 instrument_hash + base_decimals 做签名的资产 ID/数量缩放，因此需要缓存。
        """
        resp = await self._rest.post_market("full/v1/all_instruments", {"is_active": True})
        instruments = (resp.get("result") or []) if isinstance(resp, dict) else []
        by_name: Dict[str, Dict[str, Any]] = {}
        for inst in instruments:
            if not isinstance(inst, dict):
                continue
            name = str(inst.get("instrument") or "").strip()
            if not name:
                continue
            by_name[name] = inst

        self._base._instruments = by_name
        self._rest._instruments = by_name
        if self._websocket:
            self._websocket._instruments = by_name

        # 交易对列表：优先挑选永续（PERPETUAL）合约
        syms: List[str] = []
        for name, inst in by_name.items():
            kind = str(inst.get("kind") or "").upper()
            settlement = str(inst.get("settlement_period") or "").upper()
            if kind == "PERPETUAL" or settlement == "PERPETUAL":
                syms.append(name)
        self._supported_symbols = sorted(syms) if syms else sorted(by_name.keys())

        # 交易所信息对象需要 markets 元信息：这里直接复用合约缓存
        self._market_info = by_name.copy()

    def _get_instrument(self, symbol: str) -> Dict[str, Any]:
        """获取合约详情（用于签名所需字段，例如资产 ID 与精度等）。"""
        sym = self._base.normalize_symbol(symbol)
        inst = self._base._instruments.get(sym)
        if not inst:
            raise ValueError(f"未知的 GRVT 合约：{sym}（是否尚未调用 get_exchange_info / get_supported_symbols 预热？）")
        return inst

    @staticmethod
    def _parse_decimal(val: Any) -> Optional[Decimal]:
        if val is None:
            return None
        try:
            return Decimal(str(val))
        except Exception:
            return None

    @staticmethod
    def _parse_int(val: Any) -> Optional[int]:
        if val is None:
            return None
        try:
            if isinstance(val, str) and val.startswith("0x"):
                return int(val, 16)
            return int(val)
        except Exception:
            return None

    def _domain_data(self) -> Dict[str, Any]:
        # 与官方 pysdk 保持一致：name/version/chainId
        return {"name": "GRVT Exchange", "version": "0", "chainId": int(self._base.chain_id)}

    def _sign_order_payload(self, order: Dict[str, Any]) -> None:
        """
        对订单 payload 进行 EIP-712 签名（核心：与官方 pysdk 对齐）。

        重点：
        - leg.instrument -> instrument_hash（assetID）
        - size -> contractSize（按 base_decimals 缩放为整数）
        - limit_price -> limitPrice（按 1e9 缩放为整数；市价单传 0）
        - nonce/expiration：若未提供则自动生成
        """
        if not self._base.private_key:
            raise ValueError("GRVT private_key is required to sign orders")
        if not self._base.sub_account_id:
            raise ValueError("GRVT sub_account_id is required to create orders")

        account = Account.from_key(self._base.private_key)
        legs_payload = order.get("legs") or []
        if not isinstance(legs_payload, list) or not legs_payload:
            raise ValueError("GRVT order legs is empty")

        typed_legs: List[Dict[str, Any]] = []
        for leg in legs_payload:
            if not isinstance(leg, dict):
                continue
            instrument = str(leg.get("instrument") or "")
            inst = self._get_instrument(instrument)
            base_decimals = int(inst.get("base_decimals") or 0)
            size_multiplier = Decimal(10) ** Decimal(base_decimals)
            instrument_hash = inst.get("instrument_hash")
            asset_id = self._parse_int(instrument_hash)
            if asset_id is None:
                raise ValueError(f"Invalid instrument_hash={instrument_hash} for instrument={instrument}")

            size = Decimal(str(leg.get("size")))
            limit_price_raw = leg.get("limit_price")
            limit_price = Decimal(str(limit_price_raw)) if limit_price_raw not in (None, "", "0") else Decimal("0")

            typed_legs.append(
                {
                    "assetID": asset_id,
                    "contractSize": int((size * size_multiplier).to_integral_value(rounding=ROUND_DOWN)),
                    "limitPrice": int((limit_price * PRICE_MULTIPLIER).to_integral_value(rounding=ROUND_DOWN)),
                    "isBuyingContract": bool(leg.get("is_buying_asset")),
                }
            )

        signature = order.get("signature") or {}
        if not isinstance(signature, dict):
            signature = {}
            order["signature"] = signature

        nonce = signature.get("nonce")
        expiration = signature.get("expiration")
        if nonce is None:
            signature["nonce"] = random.randint(0, 2**32 - 1)
        if expiration is None:
            exp_dt = datetime.now(timezone.utc) + timedelta(minutes=5)
            signature["expiration"] = datetime_to_unix_ns(exp_dt)

        message_data = {
            "subAccountID": int(self._base.sub_account_id),
            "isMarket": bool(order.get("is_market") or False),
            "timeInForce": TIME_IN_FORCE_TO_SIGN_CODE[str(order.get("time_in_force") or "GOOD_TILL_TIME")],
            "postOnly": bool(order.get("post_only") or False),
            "reduceOnly": bool(order.get("reduce_only") or False),
            "legs": typed_legs,
            "nonce": int(signature["nonce"]),
            "expiration": int(signature["expiration"]),
        }

        typed_msg = encode_typed_data(self._domain_data(), EIP712_ORDER_MESSAGE_TYPE, message_data)
        signed = account.sign_message(typed_msg)

        # 文档示例使用 0x 前缀（目前与官方开发包一致）
        signature["r"] = "0x" + hex(signed.r)[2:].zfill(64)
        signature["s"] = "0x" + hex(signed.s)[2:].zfill(64)
        signature["v"] = int(signed.v)
        signature["signer"] = str(account.address)
        signature["chain_id"] = str(self._base.chain_id)

    @staticmethod
    def _order_status_from_grvt(order: Dict[str, Any]) -> OrderStatus:
        state = order.get("state") or {}
        if isinstance(state, dict):
            raw = str(state.get("status") or "").upper()
        else:
            raw = ""
        if raw == "OPEN":
            return OrderStatus.OPEN
        if raw == "FILLED":
            return OrderStatus.FILLED
        if raw in ("CANCELLED", "CANCELED"):
            return OrderStatus.CANCELED
        if raw == "REJECTED":
            return OrderStatus.REJECTED
        if raw == "PENDING":
            return OrderStatus.PENDING
        return OrderStatus.UNKNOWN

    def _to_order_data(self, order: Dict[str, Any]) -> OrderData:
        """
        将 GRVT order dict 映射为本项目统一的 OrderData。

        策略侧主要依赖字段：
        - id / client_id / symbol / side / status / filled / amount
        """
        legs = order.get("legs") or []
        leg0 = legs[0] if isinstance(legs, list) and legs else {}
        symbol = self._base.normalize_symbol(str(leg0.get("instrument") or ""))
        side = OrderSide.BUY if bool(leg0.get("is_buying_asset")) else OrderSide.SELL
        amount = Decimal(str(leg0.get("size") or "0"))
        price = self._parse_decimal(leg0.get("limit_price"))
        is_market = bool(order.get("is_market") or False)
        otype = OrderType.MARKET if is_market else OrderType.LIMIT

        state = order.get("state") or {}
        traded_sizes = []
        if isinstance(state, dict):
            traded_sizes = state.get("traded_size") or []
        filled = Decimal("0")
        if isinstance(traded_sizes, list) and traded_sizes:
            filled = Decimal(str(traded_sizes[0] or "0"))
        remaining = max(Decimal("0"), amount - filled)
        status = self._order_status_from_grvt(order)

        metadata = order.get("metadata") or {}
        client_id = str(metadata.get("client_order_id")) if isinstance(metadata, dict) and metadata.get("client_order_id") is not None else None
        order_id = str(order.get("order_id") or order.get("id") or client_id or "")

        ts = unix_ns_to_datetime(str(metadata.get("create_time") or "")) if isinstance(metadata, dict) else datetime.now(timezone.utc)
        upd = None
        if isinstance(state, dict) and state.get("update_time"):
            upd = unix_ns_to_datetime(str(state.get("update_time")))

        avg = None
        if isinstance(state, dict):
            avg_fill = state.get("avg_fill_price")
            if isinstance(avg_fill, list) and avg_fill:
                avg = self._parse_decimal(avg_fill[0])
        cost = (avg or price or Decimal("0")) * filled

        od = OrderData(
            id=order_id,
            client_id=client_id,
            symbol=symbol,
            side=side,
            type=otype,
            amount=amount,
            price=price if otype == OrderType.LIMIT else None,
            filled=filled,
            remaining=remaining,
            cost=cost,
            average=avg,
            status=status,
            timestamp=ts,
            updated=upd,
            fee=None,
            trades=[],
            params={"grvt": {"raw_state": state}},
            raw_data=order,
        )
        return od

    # ========= ExchangeInterface 接口实现 =========

    async def get_exchange_info(self) -> ExchangeInfo:
        """返回交易所元信息（市场信息/功能特性/限频等）。"""
        if not self._market_info:
            await self._refresh_instruments()
        return ExchangeInfo(
            name=self.config.name,
            id=self.config.exchange_id,
            type=ExchangeType.PERPETUAL,
            supported_features=[
                "perpetual_trading",
                "websocket" if self.config.enable_websocket else "rest_only",
                "ticker",
                "orderbook",
                "trades",
                "user_data",
            ],
            rate_limits=self.config.rate_limits or {},
            precision=self.config.precision or {},
            fees={},
            markets=self._market_info,
            status=self.status.value,
            timestamp=datetime.now(timezone.utc),
        )

    async def get_supported_symbols(self) -> List[str]:
        """返回支持交易对列表（优先永续合约）。"""
        if not self._supported_symbols:
            await self._refresh_instruments()
        return self._supported_symbols.copy()

    async def get_ticker(self, symbol: str) -> TickerData:
        """行情：盘口概览（HTTP 接口）。"""
        sym = self._base.normalize_symbol(symbol)
        resp = await self._rest.post_market("full/v1/ticker", {"instrument": sym, "derived": True})
        result = resp.get("result") if isinstance(resp, dict) else None
        if not isinstance(result, dict):
            raise RuntimeError(f"GRVT ticker response invalid: {resp}")
        ts = unix_ns_to_datetime(str(result.get("event_time") or ""))
        return TickerData(
            symbol=sym,
            timestamp=ts,
            bid=self._parse_decimal(result.get("best_bid_price")),
            ask=self._parse_decimal(result.get("best_ask_price")),
            bid_size=self._parse_decimal(result.get("best_bid_size")),
            ask_size=self._parse_decimal(result.get("best_ask_size")),
            last=self._parse_decimal(result.get("last_price")),
            open=self._parse_decimal(result.get("open_price")),
            high=self._parse_decimal(result.get("high_price")),
            low=self._parse_decimal(result.get("low_price")),
            close=self._parse_decimal(result.get("last_price")),
            volume=(self._parse_decimal(result.get("buy_volume_24h_b")) or Decimal("0"))
            + (self._parse_decimal(result.get("sell_volume_24h_b")) or Decimal("0")),
            quote_volume=(self._parse_decimal(result.get("buy_volume_24h_q")) or Decimal("0"))
            + (self._parse_decimal(result.get("sell_volume_24h_q")) or Decimal("0")),
            funding_rate=(
                (Decimal(str(result.get("funding_rate_8h_curr"))) / Decimal("10000"))
                if result.get("funding_rate_8h_curr") is not None
                else None
            ),
            mark_price=self._parse_decimal(result.get("mark_price")),
            index_price=self._parse_decimal(result.get("index_price")),
            open_interest=self._parse_decimal(result.get("open_interest")),
            raw_data=result,
        )

    async def get_tickers(self, symbols: Optional[List[str]] = None) -> List[TickerData]:
        """批量行情：简单实现为循环调用单个盘口概览接口。"""
        if not symbols:
            # 默认避免“全市场扫描”（对公共行情端点压力较大）
            symbols = await self.get_supported_symbols()
            symbols = symbols[:50]
        out: List[TickerData] = []
        for s in symbols:
            try:
                out.append(await self.get_ticker(s))
            except Exception:
                continue
        return out

    async def get_orderbook(self, symbol: str, limit: Optional[int] = None) -> OrderBookData:
        """行情：深度（HTTP 接口）。"""
        sym = self._base.normalize_symbol(symbol)
        depth = int(limit or 50)
        if depth not in (10, 50, 100, 500):
            depth = 50
        resp = await self._rest.post_market("full/v1/book", {"instrument": sym, "depth": depth})
        result = resp.get("result") if isinstance(resp, dict) else None
        if not isinstance(result, dict):
            raise RuntimeError(f"GRVT book response invalid: {resp}")
        ts = unix_ns_to_datetime(str(result.get("event_time") or ""))
        bids = [
            OrderBookLevel(
                price=Decimal(str(lvl.get("price"))),
                size=Decimal(str(lvl.get("size"))),
                count=int(lvl.get("num_orders")) if lvl.get("num_orders") is not None else None,
            )
            for lvl in (result.get("bids") or [])
            if isinstance(lvl, dict)
        ]
        asks = [
            OrderBookLevel(
                price=Decimal(str(lvl.get("price"))),
                size=Decimal(str(lvl.get("size"))),
                count=int(lvl.get("num_orders")) if lvl.get("num_orders") is not None else None,
            )
            for lvl in (result.get("asks") or [])
            if isinstance(lvl, dict)
        ]
        return OrderBookData(symbol=sym, bids=bids, asks=asks, timestamp=ts, raw_data=result)

    async def get_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        since: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> List[OHLCVData]:
        """行情：K 线（HTTP 接口）。时间周期参数会映射到 GRVT 的区间枚举值。"""
        sym = self._base.normalize_symbol(symbol)
        interval = TIMEFRAME_TO_GRVT_INTERVAL.get(timeframe, "CI_1_M")
        payload: Dict[str, Any] = {
            "instrument": sym,
            "interval": interval,
            "type": "TRADE",
            "limit": int(limit or 500),
        }
        if since:
            payload["start_time"] = datetime_to_unix_ns(since)
        resp = await self._rest.post_market("full/v1/kline", payload)
        result = resp.get("result") if isinstance(resp, dict) else None
        if not isinstance(result, list):
            return []
        out: List[OHLCVData] = []
        for k in result:
            if not isinstance(k, dict):
                continue
            ts = unix_ns_to_datetime(str(k.get("open_time") or ""))
            out.append(
                OHLCVData(
                    symbol=sym,
                    timeframe=timeframe,
                    timestamp=ts,
                    open=Decimal(str(k.get("open"))),
                    high=Decimal(str(k.get("high"))),
                    low=Decimal(str(k.get("low"))),
                    close=Decimal(str(k.get("close"))),
                    volume=Decimal(str(k.get("volume_b") or "0")),
                    quote_volume=Decimal(str(k.get("volume_q") or "0")),
                    trades_count=int(k.get("trades")) if k.get("trades") is not None else None,
                    raw_data=k,
                )
            )
        return out

    async def get_trades(self, symbol: str, since: Optional[datetime] = None, limit: Optional[int] = None) -> List[TradeData]:
        """行情：最近成交（HTTP 接口）。"""
        sym = self._base.normalize_symbol(symbol)
        payload = {"instrument": sym, "limit": int(limit or 500)}
        resp = await self._rest.post_market("full/v1/trade", payload)
        result = resp.get("result") if isinstance(resp, dict) else None
        if not isinstance(result, list):
            return []
        out: List[TradeData] = []
        for t in result:
            if not isinstance(t, dict):
                continue
            ts = unix_ns_to_datetime(str(t.get("event_time") or ""))
            side = OrderSide.BUY if bool(t.get("is_taker_buyer")) else OrderSide.SELL
            price = Decimal(str(t.get("price")))
            amt = Decimal(str(t.get("size")))
            out.append(
                TradeData(
                    id=str(t.get("trade_id") or ""),
                    symbol=sym,
                    side=side,
                    amount=amt,
                    price=price,
                    timestamp=ts,
                    cost=price * amt,
                    fee=None,
                    raw_data=t,
                )
            )
        if since:
            since_dt = since.replace(tzinfo=timezone.utc) if since.tzinfo is None else since
            out = [x for x in out if x.timestamp >= since_dt]
        return out

    async def get_balances(self) -> List[BalanceData]:
        """账户：余额（HTTP 接口，账户概览）。"""
        if not self._base.sub_account_id:
            raise ValueError("GRVT sub_account_id is required for balances")
        resp = await self._rest.post_trade("full/v1/account_summary", {"sub_account_id": str(self._base.sub_account_id)})
        result = resp.get("result") if isinstance(resp, dict) else None
        if not isinstance(result, dict):
            return []
        ts = unix_ns_to_datetime(str(result.get("event_time") or ""))
        balances = []
        for sb in result.get("spot_balances") or []:
            if not isinstance(sb, dict):
                continue
            cur = str(sb.get("currency") or "")
            bal = Decimal(str(sb.get("balance") or "0"))
            idx = self._parse_decimal(sb.get("index_price")) or Decimal("0")
            balances.append(
                BalanceData(
                    currency=cur,
                    free=bal,
                    used=Decimal("0"),
                    total=bal,
                    usd_value=bal * idx,
                    timestamp=ts,
                    raw_data=sb,
                )
            )
        return balances

    async def get_positions(self, symbols: Optional[List[str]] = None) -> List[PositionData]:
        """账户：持仓（HTTP 接口，持仓列表）。"""
        if not self._base.sub_account_id:
            raise ValueError("GRVT sub_account_id is required for positions")
        resp = await self._rest.post_trade("full/v1/positions", {"sub_account_id": str(self._base.sub_account_id)})
        result = resp.get("result") if isinstance(resp, dict) else None
        if not isinstance(result, list):
            return []
        out: List[PositionData] = []
        for p in result:
            if not isinstance(p, dict):
                continue
            sym = self._base.normalize_symbol(str(p.get("instrument") or ""))
            size = Decimal(str(p.get("size") or "0"))
            side = PositionSide.LONG if size >= 0 else PositionSide.SHORT
            ts = unix_ns_to_datetime(str(p.get("event_time") or ""))
            pos = PositionData(
                symbol=sym,
                side=side,
                size=abs(size),
                entry_price=Decimal(str(p.get("entry_price") or "0")),
                mark_price=self._parse_decimal(p.get("mark_price")),
                current_price=self._parse_decimal(p.get("mark_price")),
                unrealized_pnl=Decimal(str(p.get("unrealized_pnl") or "0")),
                realized_pnl=Decimal(str(p.get("realized_pnl") or "0")),
                percentage=self._parse_decimal(p.get("roi")),
                leverage=int(self.config.default_leverage or 1),
                margin_mode=MarginMode.CROSS,
                margin=Decimal("0"),
                liquidation_price=None,
                timestamp=ts,
                raw_data=p,
            )
            out.append(pos)
            self._position_cache[sym] = pos
        if symbols:
            norm = {self._base.normalize_symbol(s) for s in symbols}
            out = [p for p in out if p.symbol in norm]
        return out

    async def create_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        amount: Decimal,
        price: Optional[Decimal] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> OrderData:
        """
        下单（HTTP 接口 + EIP-712 签名）。

        支持的策略参数（从 params 读取）：
        - timeInForce / time_in_force（GOOD_TILL_TIME / IMMEDIATE_OR_CANCEL / FILL_OR_KILL / ALL_OR_NONE）
        - post_only / postOnly
        - reduce_only / reduceOnly
        - client_order_id / clientOrderId（未提供则自动生成，范围 [2^63, 2^64-1]）
        """
        if not self._base.sub_account_id:
            raise ValueError("GRVT sub_account_id is required to create orders")
        params = params or {}
        sym = self._base.normalize_symbol(symbol)

        # 确保已加载合约信息（签名需要 assetID/decimals 等）
        if not self._base._instruments:
            await self._refresh_instruments()

        is_market = order_type == OrderType.MARKET
        if not is_market and price is None:
            raise ValueError("限价单必须提供 price")

        time_in_force = (
            params.get("timeInForce")
            or params.get("time_in_force")
            or params.get("tif")
            or ("IMMEDIATE_OR_CANCEL" if order_type == OrderType.IOC else "GOOD_TILL_TIME")
        )
        if order_type == OrderType.FOK:
            time_in_force = "FILL_OR_KILL"
        if order_type == OrderType.IOC:
            time_in_force = "IMMEDIATE_OR_CANCEL"
        if str(time_in_force) not in TIME_IN_FORCE_TO_SIGN_CODE:
            time_in_force = "GOOD_TILL_TIME"

        post_only = bool(params.get("post_only") or params.get("postOnly") or False)
        reduce_only = bool(params.get("reduce_only") or params.get("reduceOnly") or False)

        # 客户端订单号：客户端侧唯一 ID（不一定上链，但对查单/幂等等非常重要）
        client_order_id = params.get("client_order_id") or params.get("clientOrderId")
        if client_order_id is None:
            # 文档建议：生成区间 [2^63, 2^64-1]
            client_order_id = str(random.randint(2**63, 2**64 - 1))
        else:
            client_order_id = str(client_order_id)

        order_payload: Dict[str, Any] = {
            "sub_account_id": str(self._base.sub_account_id),
            "is_market": bool(is_market),
            "time_in_force": str(time_in_force),
            "post_only": post_only,
            "reduce_only": reduce_only,
            "legs": [
                {
                    "instrument": sym,
                    "size": str(amount),
                    "limit_price": None if is_market else str(price),
                    "is_buying_asset": side == OrderSide.BUY,
                }
            ],
            "signature": {
                # r/s/v/signer/chain_id 会在签名时自动填充
                "signer": "",
                "r": "",
                "s": "",
                "v": 27,
                "expiration": None,
                "nonce": None,
                "chain_id": str(self._base.chain_id),
            },
            "metadata": {"client_order_id": client_order_id},
        }

        # 对订单请求体做 EIP-712 签名（会填充 signature.r/s/v/signer/nonce/expiration）
        self._sign_order_payload(order_payload)

        resp = await self._rest.post_trade("full/v1/create_order", {"order": order_payload})
        result = resp.get("result") if isinstance(resp, dict) else None
        if not isinstance(result, dict):
            raise RuntimeError(f"GRVT create_order response invalid: {resp}")
        od = self._to_order_data(result)
        self._order_cache[od.id] = od
        return od

    async def cancel_order(self, order_id: str, symbol: str) -> OrderData:
        """撤单（HTTP 接口）。"""
        if not self._base.sub_account_id:
            raise ValueError("GRVT sub_account_id is required to cancel orders")
        req = {"sub_account_id": str(self._base.sub_account_id), "order_id": str(order_id)}
        resp = await self._rest.post_trade("full/v1/cancel_order", req)
        result = resp.get("result") if isinstance(resp, dict) else None
        if not isinstance(result, dict):
            raise RuntimeError(f"GRVT cancel_order response invalid: {resp}")
        od = self._to_order_data(result)
        self._order_cache[od.id] = od
        return od

    async def cancel_all_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        """
        批量撤单（兼容网格引擎的返回约定：必须返回 List[OrderData]）。

        GRVT 官方 cancel_all_orders REST 返回的是 count（不是订单列表），
        这里为了兼容网格逻辑，使用“先拉 open_orders 再逐个 cancel”的方式构造返回值。
        """
        if not self._base.sub_account_id:
            raise ValueError("GRVT sub_account_id is required to cancel orders")
        if symbol:
            # 保守策略：先拉取当前未成交订单，再逐个撤单
            orders = await self.get_open_orders(symbol=symbol)
            out = []
            for o in orders:
                try:
                    out.append(await self.cancel_order(o.id, symbol))
                except Exception:
                    continue
            return out
        # 为兼容网格引擎（期望返回 List[OrderData]），这里采用“拉取未成交订单 + 逐个撤单”的实现。
        orders = await self.get_open_orders(symbol=None)
        out: List[OrderData] = []
        for o in orders:
            try:
                out.append(await self.cancel_order(o.id, o.symbol))
            except Exception:
                continue
        return out

    async def get_order(self, order_id: str, symbol: str) -> OrderData:
        """查单（HTTP 接口）。"""
        if not self._base.sub_account_id:
            raise ValueError("GRVT sub_account_id is required for order queries")
        resp = await self._rest.post_trade(
            "full/v1/order",
            {"sub_account_id": str(self._base.sub_account_id), "order_id": str(order_id)},
        )
        result = resp.get("result") if isinstance(resp, dict) else None
        if not isinstance(result, dict):
            raise RuntimeError(f"GRVT get_order response invalid: {resp}")
        od = self._to_order_data(result)
        self._order_cache[od.id] = od
        return od

    async def get_open_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        """查未完成订单（HTTP 接口）。"""
        if not self._base.sub_account_id:
            raise ValueError("GRVT sub_account_id is required for open orders")
        resp = await self._rest.post_trade("full/v1/open_orders", {"sub_account_id": str(self._base.sub_account_id)})
        result = resp.get("result") if isinstance(resp, dict) else None
        if not isinstance(result, list):
            return []
        out = [self._to_order_data(o) for o in result if isinstance(o, dict)]
        if symbol:
            sym = self._base.normalize_symbol(symbol)
            out = [o for o in out if o.symbol == sym]
        for o in out:
            self._order_cache[o.id] = o
        return out

    async def get_order_history(self, symbol: Optional[str] = None, since: Optional[datetime] = None, limit: Optional[int] = None) -> List[OrderData]:
        """查历史订单（HTTP 接口）。"""
        if not self._base.sub_account_id:
            raise ValueError("GRVT sub_account_id is required for order history")
        payload: Dict[str, Any] = {"sub_account_id": str(self._base.sub_account_id), "limit": int(limit or 500)}
        if symbol:
            sym = self._base.normalize_symbol(symbol)
            # 如果接口不支持按交易对精确筛选，这里会在本地做过滤兜底
            payload["kind"] = ["PERPETUAL"]
        if since:
            payload["start_time"] = datetime_to_unix_ns(since)
        resp = await self._rest.post_trade("full/v1/order_history", payload)
        result = resp.get("result") if isinstance(resp, dict) else None
        if not isinstance(result, list):
            return []
        out = [self._to_order_data(o) for o in result if isinstance(o, dict)]
        if symbol:
            sym = self._base.normalize_symbol(symbol)
            out = [o for o in out if o.symbol == sym]
        if since:
            out = [o for o in out if o.timestamp >= (since.replace(tzinfo=timezone.utc) if since.tzinfo is None else since)]
        return out

    async def set_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        # GRVT 确实提供杠杆相关接口，但它与子账户/策略风控强相关；这里暂不实现（保持兼容返回）。
        return {"ok": False, "reason": "not_implemented"}

    async def set_margin_mode(self, symbol: str, margin_mode: str) -> Dict[str, Any]:
        # GRVT 保证金模式通常在子账户级别（simple/portfolio/cross）配置，且可能需走 position config 端点；
        # 为避免误改账户配置，这里暂不实现（保持兼容返回）。
        if margin_mode.lower() in ("cross", "isolated"):
            return {"ok": False, "reason": "not_implemented"}
        return {"ok": False, "reason": f"不支持的 margin_mode={margin_mode}"}

    # ========= WS 订阅相关 =========

    async def subscribe_ticker(self, symbol: str, callback: Callable[[TickerData], None]) -> None:
        if not self._websocket:
            return
        await self._websocket.subscribe_ticker(symbol, callback)

    async def subscribe_orderbook(self, symbol: str, callback: Callable[[OrderBookData], None]) -> None:
        if not self._websocket:
            return
        await self._websocket.subscribe_orderbook(symbol, callback)

    async def subscribe_trades(self, symbol: str, callback: Callable[[TradeData], None]) -> None:
        if not self._websocket:
            return
        await self._websocket.subscribe_trades(symbol, callback)

    async def subscribe_user_data(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """
        订阅订单推送（WS）。

        注意：
        - 网格引擎调用的是 adapter.subscribe_user_data，并且希望收到 OrderData
        - 套利执行器也可能直接调用 ws.subscribe_orders
        所以这里统一走 websocket.subscribe_orders，并提前注入 cookie/header。
        """
        if not self._websocket:
            return
        # 先确保已鉴权：交易 WS 需要会话凭据 + X-Grvt-Account-Id 请求头
        await self._rest.ensure_authenticated()
        self._websocket._cookie_gravity = self._rest._cookie_gravity
        self._websocket._cookie_expires_at = self._rest._cookie_expires_at
        self._websocket._account_id_header = self._rest._account_id_header
        # 优先走 subscribe_orders：下游（网格/套利）能稳定收到 OrderData 对象
        await self._websocket.subscribe_orders(callback, symbol=None)  # type: ignore[arg-type]

    async def unsubscribe(self, symbol: Optional[str] = None) -> None:
        if not self._websocket:
            return
        await self._websocket.unsubscribe_all(symbol=symbol)


