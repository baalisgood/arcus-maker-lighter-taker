"""
GRVT 交易所适配器 - 基础工具模块

GRVT 是去中心化的永续合约交易所。

本模块包含：
- 环境与端点选择（prod/testnet/dev/stg）
- 时间格式转换（unix 纳秒）
- 交易对/合约名称规范化（symbol/instrument）
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from datetime import datetime, timezone
from typing import Any, Dict, Optional


class GrvtEnv(Enum):
    DEV = "dev"
    STG = "stg"
    TESTNET = "testnet"
    PROD = "prod"


@dataclass(frozen=True)
class GrvtEndpointConfig:
    rpc_endpoint: str
    ws_endpoint: Optional[str]


@dataclass(frozen=True)
class GrvtEnvConfig:
    edge: GrvtEndpointConfig
    trade_data: GrvtEndpointConfig
    market_data: GrvtEndpointConfig
    chain_id: int


def get_env_config(environment: GrvtEnv) -> GrvtEnvConfig:
    """
    官方端点配置（来自 GRVT pysdk，离线镜像：docs/grvt/pysdk_ref/grvt_raw_env.py）
    """
    if environment == GrvtEnv.PROD:
        return GrvtEnvConfig(
            edge=GrvtEndpointConfig(rpc_endpoint="https://edge.grvt.io", ws_endpoint=None),
            trade_data=GrvtEndpointConfig(
                rpc_endpoint="https://trades.grvt.io",
                ws_endpoint="wss://trades.grvt.io/ws",
            ),
            market_data=GrvtEndpointConfig(
                rpc_endpoint="https://market-data.grvt.io",
                ws_endpoint="wss://market-data.grvt.io/ws",
            ),
            chain_id=325,
        )
    if environment == GrvtEnv.TESTNET:
        return GrvtEnvConfig(
            edge=GrvtEndpointConfig(
                rpc_endpoint="https://edge.testnet.grvt.io",
                ws_endpoint=None,
            ),
            trade_data=GrvtEndpointConfig(
                rpc_endpoint="https://trades.testnet.grvt.io",
                ws_endpoint="wss://trades.testnet.grvt.io/ws",
            ),
            market_data=GrvtEndpointConfig(
                rpc_endpoint="https://market-data.testnet.grvt.io",
                ws_endpoint="wss://market-data.testnet.grvt.io/ws",
            ),
            chain_id=326,
        )
    if environment in (GrvtEnv.DEV, GrvtEnv.STG):
        # 注意：官方 Python 开发包中 DEV=327，STG=328
        chain_id = 327 if environment == GrvtEnv.DEV else 328
        return GrvtEnvConfig(
            edge=GrvtEndpointConfig(
                rpc_endpoint=f"https://edge.{environment.value}.gravitymarkets.io",
                ws_endpoint=None,
            ),
            trade_data=GrvtEndpointConfig(
                rpc_endpoint=f"https://trades.{environment.value}.gravitymarkets.io",
                ws_endpoint=f"wss://trades.{environment.value}.gravitymarkets.io/ws",
            ),
            market_data=GrvtEndpointConfig(
                rpc_endpoint=f"https://market-data.{environment.value}.gravitymarkets.io",
                ws_endpoint=f"wss://market-data.{environment.value}.gravitymarkets.io/ws",
            ),
            chain_id=chain_id,
        )
    raise ValueError(f"Unknown environment={environment}")


def datetime_to_unix_ns(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return str(int(dt.timestamp() * 1_000_000_000))


def unix_ns_to_datetime(ns: Optional[str]) -> datetime:
    if not ns:
        return datetime.now(timezone.utc)
    try:
        n = int(ns)
        return datetime.fromtimestamp(n / 1_000_000_000, tz=timezone.utc)
    except Exception:
        return datetime.now(timezone.utc)


class GRVTBase:
    """
    GRVT REST / WS 模块共享基类。
    """

    def __init__(self, config: Dict[str, Any]):
        self.config_dict = config

        # 环境选择：优先 extra_params.env，否则根据 testnet 推导
        extra = config.get("extra_params", {}) if isinstance(config.get("extra_params", {}), dict) else {}
        env_str = str(extra.get("env") or "").lower().strip()
        if not env_str:
            env = GrvtEnv.TESTNET if bool(config.get("testnet")) else GrvtEnv.PROD
        else:
            env = GrvtEnv(env_str)

        self.env: GrvtEnv = env
        self.env_config: GrvtEnvConfig = get_env_config(env)
        self.chain_id: int = self.env_config.chain_id

        # 允许通过 extra_params 显式覆盖端点（默认使用官方端点）
        self.edge_rpc = str(extra.get("edge_rpc") or self.env_config.edge.rpc_endpoint).rstrip("/")
        self.trade_rpc = str(extra.get("trade_rpc") or self.env_config.trade_data.rpc_endpoint).rstrip("/")
        self.market_rpc = str(extra.get("market_rpc") or self.env_config.market_data.rpc_endpoint).rstrip("/")

        self.trade_ws = str(extra.get("trade_ws") or (self.env_config.trade_data.ws_endpoint or "")).strip()
        self.market_ws = str(extra.get("market_ws") or (self.env_config.market_data.ws_endpoint or "")).strip()

        # 鉴权与账户模型
        self.api_key: str = str(getattr(config, "api_key", "") or config.get("api_key") or "")
        self.private_key: Optional[str] = getattr(config, "private_key", None) or config.get("private_key")

        # GRVT 为两层账户结构：主账户 + 交易子账户（sub_account_id，uint64 字符串）
        self.sub_account_id: Optional[str] = (
            str(extra.get("sub_account_id") or config.get("sub_account_id") or "").strip() or None
        )

        # 运行期状态：登录后会被填充（会话凭据 + 账户 ID 请求头）
        self._cookie_gravity: Optional[str] = None
        self._cookie_expires_at: Optional[datetime] = None
        self._account_id_header: Optional[str] = None  # X-Grvt-Account-Id

        # 合约信息缓存：合约名 -> dict(instrument_hash/base_decimals/quote_decimals/tick_size/min_size/kind/...)
        self._instruments: Dict[str, Dict[str, Any]] = {}

    def normalize_symbol(self, symbol: str) -> str:
        """
        尽力把外部传入的 symbol 规范化为 GRVT 的 instrument 命名格式。

        GRVT 永续合约的典型命名示例：BTC_USDT_Perp
        """
        if not symbol:
            return symbol
        s = symbol.strip()
        s = s.replace("/", "_").replace("-", "_")
        s = s.upper()

        # 尽量保留 GRVT 约定的混合大小写后缀（例如 _Perp）
        if s.endswith("_PERP"):
            s = s[:-5] + "_Perp"
        elif s.endswith("_PERPETUAL"):
            s = s.replace("_PERPETUAL", "_Perp")

        # 如果是 BASE_QUOTE 形式，且未带后缀，则默认按永续补齐 *_Perp
        if "_PERP" not in s and not s.endswith("_Perp"):
            parts = s.split("_")
            if len(parts) == 2:
                s = f"{parts[0]}_{parts[1]}_Perp"

        return s


