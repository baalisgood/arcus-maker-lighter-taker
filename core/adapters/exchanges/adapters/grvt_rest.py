"""
GRVT 交易所适配器 - REST 模块

核心说明（来自 GRVT 官方文档 / 离线 mhtml）：
- **所有接口统一使用 POST**
- **公共行情域名**：https://market-data.<env>.../(full|lite)/v1/...
- **交易/账户域名（需鉴权）**：https://trades.<env>.../(full|lite)/v1/...
- **鉴权登录**：POST https://edge.<env>.../auth/api_key/login
  - 返回 `gravity` session cookie（后续请求需带 Cookie: gravity=...）
  - 返回 `X-Grvt-Account-Id`（后续请求需带该 header）

本模块职责：
- 管理 aiohttp session
- 处理 API key 登录换取 cookie + account id header
- 封装 market/trade 的 POST 请求（market 不需要鉴权；trade 需要鉴权）
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from http.cookies import SimpleCookie
from typing import Any, Dict, Optional

import aiohttp

from .grvt_base import GRVTBase


class GRVTRest(GRVTBase):
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        timeout_s = int(config.get("request_timeout", 10))
        self._timeout = aiohttp.ClientTimeout(total=timeout_s)
        self._session: Optional[aiohttp.ClientSession] = None
        self._lock: asyncio.Lock = asyncio.Lock()

    async def close(self) -> None:
        """关闭底层 HTTP 会话（退出程序/断开连接时调用）。"""
        if self._session:
            await self._session.close()
            self._session = None

    async def _get_session(self) -> aiohttp.ClientSession:
        """获取/懒加载 aiohttp 会话（复用连接池）。"""
        if self._session and not self._session.closed:
            return self._session
        self._session = aiohttp.ClientSession(timeout=self._timeout, headers={"Content-Type": "application/json"})
        return self._session

    async def login(self) -> Dict[str, Any]:
        """
        使用 API Key 登录获取会话凭据（交易/账户接口必需）。

        产出：
        - `gravity` cookie（以及 expires）
        - `X-Grvt-Account-Id` header（后续鉴权请求必须携带）
        """
        if not self.api_key:
            raise ValueError("GRVT 未配置 api_key（请使用环境变量或统一配置加载器注入）")

        url = f"{self.edge_rpc}/auth/api_key/login"
        session = await self._get_session()

        async with self._lock:
            async with session.post(url, json={"api_key": self.api_key}) as resp:
                text = await resp.text()
                if resp.status >= 400:
                    raise RuntimeError(f"GRVT 登录失败 status={resp.status} body={text[:500]}")

                # 解析 Set-Cookie，提取 gravity 会话凭据
                set_cookie = resp.headers.get("Set-Cookie", "")
                cookie = SimpleCookie()
                cookie.load(set_cookie)
                if "gravity" in cookie:
                    self._cookie_gravity = cookie["gravity"].value
                    expires_raw = cookie["gravity"].get("expires")
                    if expires_raw:
                        try:
                            self._cookie_expires_at = datetime.strptime(
                                expires_raw, "%a, %d %b %Y %H:%M:%S %Z"
                            ).replace(tzinfo=timezone.utc)
                        except Exception:
                            self._cookie_expires_at = None

                # 提取 X-Grvt-Account-Id（注意大小写可能不同）
                account_id = resp.headers.get("X-Grvt-Account-Id") or resp.headers.get("x-grvt-account-id")
                if account_id:
                    self._account_id_header = account_id

                # 响应体不是强依赖，但可能包含额外信息（保留返回）
                try:
                    return await resp.json()
                except Exception:
                    return {"raw": text}

    def _auth_headers(self) -> Dict[str, str]:
        """构建鉴权请求头：会话凭据（Cookie） + X-Grvt-Account-Id。"""
        if not self._cookie_gravity:
            return {}
        headers: Dict[str, str] = {"Cookie": f"gravity={self._cookie_gravity}"}
        if self._account_id_header:
            headers["X-Grvt-Account-Id"] = self._account_id_header
        return headers

    async def ensure_authenticated(self) -> None:
        """
        确保已登录且 cookie 未过期（或即将过期则刷新）。
        该方法会在所有 trade POST 前被调用。
        """
        # 如果会话凭据缺失或即将过期，则重新登录刷新
        if not self._cookie_gravity:
            await self.login()
            return
        if self._cookie_expires_at:
            if (self._cookie_expires_at - datetime.now(timezone.utc)).total_seconds() < 10:
                await self.login()

    async def post_market(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        调用公共行情 REST（不需要鉴权）。

        Args:
            path: 例如 "full/v1/ticker" / "lite/v1/book"
            payload: 请求 JSON body（GRVT 统一 POST）
        """
        session = await self._get_session()
        url = f"{self.market_rpc}/{path.lstrip('/')}"
        async with session.post(url, json=payload) as resp:
            data = await resp.text()
            if resp.status >= 400:
                raise RuntimeError(f"GRVT 行情接口请求失败 status={resp.status} url={url} body={data[:500]}")
            try:
                return await resp.json()
            except Exception:
                return {"raw": data}

    async def post_trade(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        调用交易/账户 REST（需要鉴权）。

        Args:
            path: 例如 "full/v1/create_order" / "full/v1/positions"
            payload: 请求 JSON body（GRVT 统一 POST）
        """
        await self.ensure_authenticated()
        session = await self._get_session()
        url = f"{self.trade_rpc}/{path.lstrip('/')}"
        headers = self._auth_headers()
        async with session.post(url, json=payload, headers=headers) as resp:
            data = await resp.text()
            if resp.status >= 400:
                raise RuntimeError(f"GRVT 交易/账户接口请求失败 status={resp.status} url={url} body={data[:500]}")
            try:
                return await resp.json()
            except Exception:
                return {"raw": data}


