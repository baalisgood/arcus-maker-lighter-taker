"""
Variational (Omni) - REST 客户端

当前 Variational 前端仅展示最优买卖价（BBO），并不提供传统 orderbook 深度。
插件验证可通过 `POST /api/quotes/indicative` 获取 bid/ask/mark_price。

本模块只负责“拉取报价”，不包含任何策略/业务逻辑。
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Dict, Optional

import aiohttp

from ..interface import ExchangeConfig


@dataclass(frozen=True)
class VariationalIndicativeQuoteRequest:
    """Indicative quote 请求参数（与前端一致）"""

    underlying: str
    qty: str
    funding_interval_s: int = 3600
    settlement_asset: str = "USDC"
    instrument_type: str = "perpetual_future"

    def to_payload(self) -> Dict[str, Any]:
        return {
            "instrument": {
                "underlying": self.underlying,
                "funding_interval_s": self.funding_interval_s,
                "settlement_asset": self.settlement_asset,
                "instrument_type": self.instrument_type,
            },
            "qty": self.qty,
        }


class VariationalRestClient:
    """Variational REST 客户端（仅用于获取 BBO quote）"""

    DEFAULT_BASE_URL = "https://omni.variational.io"
    DEFAULT_USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    )

    def __init__(self, config: ExchangeConfig, logger=None):
        self._config = config
        self._logger = logger
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_lock = asyncio.Lock()

        self._base_url = (
            (config.base_url or "").rstrip("/")
            or str(config.extra_params.get("base_url", self.DEFAULT_BASE_URL)).rstrip("/")
        )

        # token：部分接口（如 /api/quotes/indicative）会要求 Authorization token
        # 约定优先级：
        # 1) extra_params.token
        # 2) config.api_key（方便在现有配置结构里复用）
        self._token: Optional[str] = None
        token = config.extra_params.get("token")
        if isinstance(token, str) and token.strip():
            self._token = token.strip()
        elif isinstance(config.api_key, str) and config.api_key.strip():
            self._token = config.api_key.strip()

        # 可扩展请求头：Variational 站点可能有 WAF/风控 + token 鉴权
        # 这里提供“浏览器友好”的默认值，同时允许在配置中覆盖/补充。
        self._headers: Dict[str, str] = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            # 关键：部分站点会基于 UA/Origin/Referer 做简单风控
            "User-Agent": str(config.extra_params.get("user_agent", self.DEFAULT_USER_AGENT)),
            "Origin": str(config.extra_params.get("origin", self._base_url)),
            "Referer": str(config.extra_params.get("referer", f"{self._base_url}/")),
        }
        if self._token:
            # 绝大多数站点是 Bearer token；如果后续发现不是，可通过 extra_params.headers 覆盖
            self._headers["Authorization"] = f"Bearer {self._token}"
        extra_headers = config.extra_params.get("headers")
        if isinstance(extra_headers, dict):
            for k, v in extra_headers.items():
                if isinstance(k, str) and isinstance(v, str) and k.strip():
                    self._headers[k] = v

        # 可选 cookies：如果接口需要登录态/风控 cookie，可在 extra_params 传入
        self._cookies: Optional[Dict[str, str]] = None
        cookies = config.extra_params.get("cookies")
        if isinstance(cookies, dict):
            cleaned: Dict[str, str] = {}
            for k, v in cookies.items():
                if isinstance(k, str) and isinstance(v, str) and k.strip():
                    cleaned[k] = v
            self._cookies = cleaned or None

    async def close(self) -> None:
        async with self._session_lock:
            if self._session:
                await self._session.close()
                self._session = None

    async def _ensure_session(self) -> aiohttp.ClientSession:
        async with self._session_lock:
            if self._session and not self._session.closed:
                return self._session

            timeout = aiohttp.ClientTimeout(total=max(1, int(self._config.request_timeout or 10)))
            cookie_jar = aiohttp.CookieJar()
            if self._cookies:
                cookie_jar.update_cookies(self._cookies)
            self._session = aiohttp.ClientSession(timeout=timeout, headers=self._headers, cookie_jar=cookie_jar)
            return self._session

    async def get_indicative_quote(
        self,
        request: VariationalIndicativeQuoteRequest,
    ) -> Dict[str, Any]:
        """
        获取 BBO（bid/ask/mark_price）

        Returns:
            Dict[str, Any]: 原始响应 JSON
        """
        session = await self._ensure_session()
        url = f"{self._base_url}/api/quotes/indicative"
        payload = request.to_payload()

        if self._logger:
            self._logger.debug(f"Variational indicative quote request: url={url}, payload={payload}")

        async with session.post(url, json=payload) as resp:
            # 403/4xx 时经常会返回 WAF 的 HTML/文本说明，读取出来便于排查
            if resp.status >= 400:
                body = await resp.text()
                # 截断避免日志过长
                body_preview = body[:500]
                raise aiohttp.ClientResponseError(
                    request_info=resp.request_info,
                    history=resp.history,
                    status=resp.status,
                    message=f"{resp.reason}; body={body_preview}",
                    headers=resp.headers,
                )

            data = await resp.json()

        if not isinstance(data, dict):
            raise ValueError(f"Variational quote response is not a JSON object: {type(data)}")

        return data


