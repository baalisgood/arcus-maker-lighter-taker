from __future__ import annotations

from dataclasses import dataclass, field
from collections import deque
from decimal import Decimal
import time
from typing import Callable, Deque, Dict, Optional, Tuple, Iterable, Any, List


@dataclass
class OpenBlockOnLowBalanceConfig:
    """
    余额不足 -> 禁止开仓（只允许平仓）

    - min_equity_usdc: 全局阈值
    - min_equity_usdc_by_exchange: 交易所覆盖（key 不区分大小写）
    """

    enabled: bool = False
    min_equity_usdc: float = 0.0
    min_equity_usdc_by_exchange: Dict[str, float] = field(default_factory=dict)
    # 是否启用该规则的声音报警（仅影响“禁止开仓”提示音）
    sound_alarm_enabled: bool = True


@dataclass
class OpenBlockOnMaxExchangePositionValueConfig:
    """
    交易所“总持仓价值(USDC)”超过阈值 -> 禁止开仓（只允许平仓）

    - max_value_usdc: 全局阈值
    - max_value_usdc_by_exchange: 交易所覆盖（key 不区分大小写）
    """

    enabled: bool = False
    max_value_usdc: float = 0.0
    max_value_usdc_by_exchange: Dict[str, float] = field(default_factory=dict)
    # 是否启用该规则的声音报警（固定阈值 & 动态阈值共用）
    sound_alarm_enabled: bool = True

@dataclass
class OpenBlockOnDynamicMaxExchangePositionValueConfig:
    """
    动态“总持仓价值(USDC)”阈值 -> 禁止开仓（只允许平仓）

    计算方式：
    - 对每个交易所独立计算：
      dynamic_threshold(ex) = equity_usdc(ex) * equity_multiplier(ex)
    - 开仓时按“本次套利对两腿交易所”分别判断：
      若任一交易所满足 position_value_usdc(ex) >= dynamic_threshold(ex)，则禁止该套利对开仓（仍允许平仓）

    注意：
    - 权益来自 UI 缓存，可能缺失；缺失时不做误杀（不强行阻断开仓）
    """

    enabled: bool = False
    equity_multiplier: float = 0.0
    # 可选：按交易所覆盖 multiplier（key 不区分大小写）
    # - 若未配置，则回退到 equity_multiplier
    equity_multiplier_by_exchange: Dict[str, float] = field(default_factory=dict)


@dataclass
class OpenBlockOnPriceVolatilityConfig:
    """
    价格波动幅度过大 -> 禁止开仓（只允许平仓），并进入“冷却/限制”一段时间（可刷新）。

    口径：
    - 在最近 window_seconds 内，取 reference_mid_price 的 max/min
    - amplitude_pct = (max - min) / min * 100
    - amplitude_pct >= threshold_pct -> 触发“禁止开仓”，持续 block_seconds

    注意：
    - 仅影响开仓路径；平仓/紧急平仓不受影响
    - 与其他 open_only_close 规则并列，互不影响（各自计时/解除）
    """

    enabled: bool = False
    window_seconds: float = 60.0
    threshold_pct: float = 0.0
    block_seconds: float = 900.0
    # 是否启用“任意币触发 -> 全部币禁止开仓”
    # - False：仅限制触发的 symbol（默认，兼容历史行为）
    # - True ：任意 symbol 触发后，全局禁止所有 symbol 开仓，直到 block_seconds 到期（可刷新）
    apply_to_all_symbols: bool = False
    # 只允许这些 symbol 触发波动风控（默认仅 BTC/ETH/SOL）
    trigger_symbols: List[str] = field(default_factory=lambda: ["BTC", "ETH", "SOL"])
    # 是否启用该规则的声音报警（仅影响“禁止开仓”提示音）
    sound_alarm_enabled: bool = False


@dataclass
class OpenOnlyCloseConfig:
    """开仓限制（触发后：只允许平仓，不允许开仓）"""

    low_balance: OpenBlockOnLowBalanceConfig = field(default_factory=OpenBlockOnLowBalanceConfig)
    max_exchange_position_value: OpenBlockOnMaxExchangePositionValueConfig = field(
        default_factory=OpenBlockOnMaxExchangePositionValueConfig
    )
    dynamic_max_exchange_position_value: OpenBlockOnDynamicMaxExchangePositionValueConfig = field(
        default_factory=OpenBlockOnDynamicMaxExchangePositionValueConfig
    )
    price_volatility: OpenBlockOnPriceVolatilityConfig = field(
        default_factory=OpenBlockOnPriceVolatilityConfig
    )


class OpenOnlyCloseGuard:
    """
    轻量“只允许平仓”风控闸门：
    - 不直接依赖 UI/Orchestrator 的细节，通过回调获取所需数值
    - 仅用于“开仓前”判断，平仓路径不应调用
    """

    def __init__(
        self,
        cfg: OpenOnlyCloseConfig,
        *,
        get_exchange_equity_usdc: Callable[[str], Optional[Decimal]],
        get_exchange_position_value_usdc: Callable[[str], Optional[Decimal]],
    ) -> None:
        self._cfg = cfg
        self._get_equity = get_exchange_equity_usdc
        self._get_pos_value = get_exchange_position_value_usdc
        # price volatility -------------------------------------------------
        # symbol -> deque[(ts, reference_mid)]
        self._vol_price_history: Dict[str, Deque[Tuple[float, Decimal]]] = {}
        # symbol -> last_sample_ts（用于控制采样频率，避免每轮都 append）
        self._vol_last_sample_ts: Dict[str, float] = {}
        # symbol -> block_until_ts
        self._vol_block_until: Dict[str, float] = {}
        self._vol_block_reason: Dict[str, str] = {}
        # global block (apply_to_all_symbols=True 时启用)
        self._vol_global_block_until: float = 0.0
        self._vol_global_block_reason: str = ""

    @staticmethod
    def _to_decimal(v: float) -> Decimal:
        try:
            return Decimal(str(v))
        except Exception:
            return Decimal("0")

    def _min_equity_threshold(self, exchange: str) -> Decimal:
        ex = (exchange or "").strip().lower()
        overrides = self._cfg.low_balance.min_equity_usdc_by_exchange or {}
        for k, v in overrides.items():
            if (k or "").strip().lower() == ex:
                return self._to_decimal(float(v))
        return self._to_decimal(float(self._cfg.low_balance.min_equity_usdc or 0.0))

    def _max_value_threshold(self, exchange: str) -> Decimal:
        ex = (exchange or "").strip().lower()
        overrides = self._cfg.max_exchange_position_value.max_value_usdc_by_exchange or {}
        for k, v in overrides.items():
            if (k or "").strip().lower() == ex:
                return self._to_decimal(float(v))
        return self._to_decimal(float(self._cfg.max_exchange_position_value.max_value_usdc or 0.0))

    def _dynamic_multiplier(self, exchange: str) -> Decimal:
        """
        返回某交易所的 dynamic multiplier（支持 by_exchange 覆盖）。
        - 未配置覆盖时回退到全局 equity_multiplier
        """
        ex = str(exchange or "").strip().lower()
        cfg = getattr(self._cfg, "dynamic_max_exchange_position_value", None)
        if cfg is None:
            return Decimal("0")
        try:
            overrides = getattr(cfg, "equity_multiplier_by_exchange", None) or {}
            if isinstance(overrides, dict):
                for k, v in overrides.items():
                    if str(k or "").strip().lower() == ex:
                        return self._to_decimal(float(v))
        except Exception:
            pass
        try:
            return self._to_decimal(float(getattr(cfg, "equity_multiplier", 0.0) or 0.0))
        except Exception:
            return Decimal("0")

    def _dynamic_threshold_for_exchange(self, exchange: str) -> Tuple[Optional[Decimal], Optional[Decimal], Optional[Decimal]]:
        """
        计算某交易所的动态阈值：
            dyn_th(ex) = equity(ex) * multiplier(ex)

        Returns:
            (dyn_th, equity, multiplier)
        """
        try:
            if not getattr(self._cfg.dynamic_max_exchange_position_value, "enabled", False):
                return None, None, None
            eq = self._get_equity(str(exchange or "").strip())
            if eq is None or eq <= Decimal("0"):
                return None, None, None
            mult = self._dynamic_multiplier(exchange)
            if mult <= Decimal("0"):
                return None, None, None
            dyn_th = eq * mult
            if dyn_th <= Decimal("0"):
                return None, None, None
            return dyn_th, eq, mult
        except Exception:
            return None, None, None

    # ---------------------------------------------------------------------
    # Price volatility (open gate only)
    # ---------------------------------------------------------------------

    def record_reference_mid_price(
        self,
        *,
        symbol: str,
        orderbooks_by_exchange: Dict[str, Any],
        timestamp: Optional[float] = None,
    ) -> None:
        """
        记录某个 symbol 的参考中间价（reference_mid_price）。
        - reference_mid_price：对当前可用交易所盘口的 mid_price 做平均（尽量不依赖单一交易所）。
        """
        cfg = getattr(self._cfg, "price_volatility", None)
        if not cfg or not bool(getattr(cfg, "enabled", False)):
            return

        sym = (symbol or "").strip().upper()
        if not sym:
            return
        if not self._is_volatility_trigger_symbol(sym):
            return

        now = float(timestamp or time.time())

        # ✅ 采样节奏控制：每个 symbol 最多 1 秒采一次
        # - 目的是减少频繁 min/max 计算与内存 append
        # - 不改变“按时间滑动窗口(min/max)”的语义，只减少采样点密度
        last_ts = float(self._vol_last_sample_ts.get(sym, 0.0) or 0.0)
        if last_ts > 0.0 and (now - last_ts) < 1.0:
            return
        self._vol_last_sample_ts[sym] = now

        mids: list[Decimal] = []
        for ob in (orderbooks_by_exchange or {}).values():
            if not ob:
                continue
            bid = getattr(getattr(ob, "best_bid", None), "price", None)
            ask = getattr(getattr(ob, "best_ask", None), "price", None)
            if bid is None or ask is None:
                continue
            try:
                bid_d = Decimal(str(bid))
                ask_d = Decimal(str(ask))
            except Exception:
                continue
            if bid_d <= Decimal("0") or ask_d <= Decimal("0"):
                continue
            mids.append((bid_d + ask_d) / Decimal("2"))

        if not mids:
            return

        try:
            ref_mid = sum(mids, Decimal("0")) / Decimal(str(len(mids)))
        except Exception:
            return

        hist = self._vol_price_history.setdefault(sym, deque())
        hist.append((now, ref_mid))

        # 保留更长一些，便于窗口覆盖（同时限制内存）
        window = float(getattr(cfg, "window_seconds", 0.0) or 0.0)
        retention = max(window * 4.0, window + 10.0, 120.0)
        cutoff = now - retention
        while hist and hist[0][0] < cutoff:
            hist.popleft()

        self._refresh_volatility_block_if_needed(symbol=sym, now=now)

    def get_volatility_block_status(self, symbol: str) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Returns:
            (是否阻止开仓, 原因, block_type)
        """
        cfg = getattr(self._cfg, "price_volatility", None)
        if not cfg or not bool(getattr(cfg, "enabled", False)):
            # 配置关闭：清理状态，避免 UI 残留
            sym = (symbol or "").strip().upper()
            if sym:
                self._vol_block_until.pop(sym, None)
                self._vol_block_reason.pop(sym, None)
            return False, None, None

        sym = (symbol or "").strip().upper()
        if not sym:
            return False, None, None

        now = time.time()
        # 0) 全局波动阻断（任一币触发 -> 全部币禁止开仓）
        try:
            cfg = getattr(self._cfg, "price_volatility", None)
            if cfg and bool(getattr(cfg, "apply_to_all_symbols", False)):
                if now < float(self._vol_global_block_until or 0.0):
                    rsn = self._vol_global_block_reason or "价格波动过大：已全局禁止开仓"
                    return True, rsn, "price_volatility"
        except Exception:
            # 不影响原有逻辑
            pass

        if not self._is_volatility_trigger_symbol(sym):
            return False, None, None

        until = float(self._vol_block_until.get(sym, 0.0) or 0.0)
        if now < until:
            return True, (self._vol_block_reason.get(sym) or "价格波动过大：已禁止开仓"), "price_volatility"
        return False, None, None

    def _refresh_volatility_block_if_needed(self, *, symbol: str, now: float) -> None:
        cfg = getattr(self._cfg, "price_volatility", None)
        if not cfg or not bool(getattr(cfg, "enabled", False)):
            return
        if not self._is_volatility_trigger_symbol(symbol):
            return

        window = float(getattr(cfg, "window_seconds", 0.0) or 0.0)
        th = float(getattr(cfg, "threshold_pct", 0.0) or 0.0)
        block_s = float(getattr(cfg, "block_seconds", 0.0) or 0.0)
        if window <= 0 or th <= 0 or block_s <= 0:
            return

        hist = self._vol_price_history.get(symbol)
        if not hist:
            return

        # 窗口覆盖判定：
        # - 需要“历史最早样本”已经早于(或等于) now-window，才说明窗口已满
        # - 不能用 relevant[0]（ts>=cutoff）来算 coverage，否则 coverage 永远 <= window，
        #   在实际采样下几乎总是 < window，导致永远不触发。
        cutoff = now - window
        try:
            oldest_ts = float(hist[0][0])
        except Exception:
            return
        if oldest_ts > cutoff:
            # 窗口未满：不触发（避免刚启动时误杀）
            return

        relevant = [(ts, px) for ts, px in hist if ts >= cutoff]
        if not relevant:
            return

        prices = [px for _, px in relevant]
        try:
            p_min = min(prices)
            p_max = max(prices)
        except Exception:
            return

        if p_min <= Decimal("0"):
            return

        try:
            amp_pct = float(((p_max - p_min) / p_min) * Decimal("100"))
        except Exception:
            return

        if amp_pct + 1e-9 < th:
            return

        until = now + block_s
        prev_until = float(self._vol_block_until.get(symbol, 0.0) or 0.0)
        if until > prev_until:
            self._vol_block_until[symbol] = until
            reason = (
                f"{symbol} 价格波动过大(最近{window:.0f}s): "
                f"min={p_min} max={p_max} amp={amp_pct:.4f}% >= {th:.4f}% ，"
                f"禁止开仓{block_s/60:.0f}分钟（可刷新）"
            )
            self._vol_block_reason[symbol] = reason

            # 可选：全局禁止开仓（任意币触发 -> 全部币禁止开仓）
            try:
                if bool(getattr(cfg, "apply_to_all_symbols", False)):
                    prev_g = float(self._vol_global_block_until or 0.0)
                    if until > prev_g:
                        self._vol_global_block_until = until
                        self._vol_global_block_reason = (
                            f"任意币触发价格波动风控：{reason}"
                        )
            except Exception:
                pass

    def should_block_open(
        self,
        *,
        exchange_buy: str,
        exchange_sell: str,
        symbol: Optional[str] = None,
    ) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Returns:
            (是否阻止开仓, 原因, block_type)
            - block_type: "low_balance" | "max_exchange_position_value" | "price_volatility" | None
        """
        # 0) 价格波动幅度（symbol 维度的开仓闸门）
        if symbol:
            blocked, reason, bt = self.get_volatility_block_status(symbol)
            if blocked:
                return True, reason, bt

        exchanges = {str(exchange_buy or "").strip(), str(exchange_sell or "").strip()}
        exchanges = {e for e in exchanges if e}
        if not exchanges:
            return False, None, None

        # 1) 余额不足
        if getattr(self._cfg.low_balance, "enabled", False):
            for ex in sorted(exchanges):
                th = self._min_equity_threshold(ex)
                if th > Decimal("0"):
                    eq = self._get_equity(ex)
                    if eq is None:
                        # 未获取到权益：不强行阻止，但给出原因（便于上层限频提示）
                        continue
                    if eq < th:
                        return True, f"{ex} 权益不足: {eq} < {th} USDC", "low_balance"

        # 2) 总持仓价值过高（固定阈值 / 动态阈值并存，谁先触发执行谁）
        fixed_enabled = bool(getattr(self._cfg.max_exchange_position_value, "enabled", False))
        dyn_enabled = bool(getattr(self._cfg.dynamic_max_exchange_position_value, "enabled", False))
        if fixed_enabled or dyn_enabled:
            for ex in sorted(exchanges):
                val = self._get_pos_value(ex)
                if val is None:
                    continue

                fixed_th = self._max_value_threshold(ex) if fixed_enabled else Decimal("0")
                dyn_th, dyn_eq, dyn_mult = (None, None, None)
                if dyn_enabled:
                    dyn_th, dyn_eq, dyn_mult = self._dynamic_threshold_for_exchange(ex)

                # “先触发” = 当前配置下更小的阈值先被越过（固定阈值 vs 动态阈值）
                candidates: list[Tuple[str, Decimal]] = []
                if fixed_th > Decimal("0"):
                    candidates.append(("fixed", fixed_th))
                if dyn_th is not None and dyn_th > Decimal("0"):
                    candidates.append(("dynamic", dyn_th))
                if not candidates:
                    continue

                mode, th = min(candidates, key=lambda x: x[1])
                if val >= th:
                    if mode == "fixed":
                        return True, f"{ex} 总持仓价值过高: {val} >= {th} USDC", "max_exchange_position_value"
                    return True, (
                        f"{ex} 总持仓价值过高(动态阈值): {val} >= {th} USDC "
                        f"(equity={dyn_eq if dyn_eq is not None else '-'}*{dyn_mult if dyn_mult is not None else '-'})"
                    ), "max_exchange_position_value"

        return False, None, None

    def _is_volatility_trigger_symbol(self, symbol: str) -> bool:
        cfg = getattr(self._cfg, "price_volatility", None)
        if not cfg:
            return True
        lst = getattr(cfg, "trigger_symbols", None) or []
        if not lst:
            return True
        sym = (symbol or "").strip().upper()
        triggers = {str(s).strip().upper() for s in lst if str(s).strip()}
        if sym in triggers:
            return True
        # 兼容：带交易所/环境前缀的 symbol（如 GRVT_VARIATIONAL_SOL）
        # 也兼容：标准合约格式（如 BTC-USDC-PERP）
        parts: list[str] = [sym]
        for sep in ("-", "_", "/"):
            if sep in sym:
                parts = [p for p in sym.split(sep) if p]
                break
        if any(p in triggers for p in parts):
            return True
        base = sym
        for sep in ("-", "_", "/"):
            if sep in base:
                base = base.split(sep)[0]
        return base in triggers


