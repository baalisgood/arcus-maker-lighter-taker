"""Validated maker-mode profile; old profiles default to taker_taker at dispatch."""
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
import math
import yaml

D = Decimal


def number(value, name, *, positive=True):
    result = D(str(value))
    if not result.is_finite() or (positive and result <= 0):
        raise ValueError(f'{name} must be positive and finite')
    return result


@dataclass(frozen=True)
class MakerConfig:
    exchange: str = 'arcus'
    post_only: bool = True
    requote_threshold: D = D('.005')
    max_order_age: float = 30
    max_requotes_per_sec: int = 2
    basis_ema_seconds: float = 30
    quick_resume_seconds: float = 10
    warmup_seconds: float = 60


@dataclass(frozen=True)
class HedgeConfig:
    exchange: str = 'lighter'
    order_type: str = 'market'
    max_retries: int = 3
    max_hedge_latency_ms: float = 500
    request_timeout: float = 5


@dataclass(frozen=True)
class MakerRiskConfig:
    max_unhedged_notional: D = D(500)
    stale_book_seconds: float = 2
    reconcile_interval: float = 30
    allocation_per_venue: D = D(5000)
    leverage: D = D(2)
    reserve_ratio: D = D('.02')


@dataclass(frozen=True)
class MakerSymbolConfig:
    symbol: str
    quantity: D
    open_threshold: D
    close_threshold: D
    grid_step: D
    max_segments: int
    min_close_profit: D = D('.01')
    close_floor_timeout: float = 600


@dataclass(frozen=True)
class MakerProfile:
    execution_mode: str
    maker: MakerConfig
    hedge: HedgeConfig
    risk: MakerRiskConfig
    symbols: dict = field(default_factory=dict)
    disabled: frozenset = frozenset()

    def get_config(self, symbol):
        if symbol not in self.symbols:
            raise KeyError(f'{symbol} is disabled or unconfigured; no default fallback')
        return self.symbols[symbol]


def load_profile(path):
    data = yaml.safe_load(Path(path).read_text())
    if not isinstance(data, dict):
        raise ValueError('Strategy profile must be a mapping')
    mode = data.get('execution_mode', 'taker_taker')
    if mode not in ('taker_taker', 'maker_taker'):
        raise ValueError('Unknown execution_mode')
    m, h, r = (data.get(k, {}) for k in ('maker', 'hedge', 'risk'))
    maker = MakerConfig(**{**m, 'requote_threshold': number(m.get('requote_threshold', '.005'), 'requote_threshold'),
        'quick_resume_seconds':float(number(m.get('quick_resume_seconds',10),'quick_resume_seconds')),
        'warmup_seconds':float(number(m.get('warmup_seconds',60),'warmup_seconds'))})
    hedge = HedgeConfig(**h)
    risk = MakerRiskConfig(**{**r,
        'max_unhedged_notional': number(r.get('max_unhedged_notional', 500), 'max_unhedged_notional'),
        'allocation_per_venue': number(r.get('allocation_per_venue', 5000), 'allocation_per_venue'),
        'leverage': number(r.get('leverage', 2), 'leverage'),
        'reserve_ratio': number(r.get('reserve_ratio', '.02'), 'reserve_ratio', positive=False)})
    if maker.exchange != 'arcus' or maker.post_only is not True or hedge.exchange != 'lighter' or hedge.order_type != 'market':
        raise ValueError('maker_taker requires Arcus PostOnly and Lighter market hedges')
    if not 0 < risk.leverage <= 2 or not 0 <= risk.reserve_ratio < 1:
        raise ValueError('Leverage must be <=2 and reserve_ratio must be in [0,1)')
    for name, value in [('max_order_age', maker.max_order_age), ('stale_book_seconds', risk.stale_book_seconds),
                        ('reconcile_interval', risk.reconcile_interval), ('request_timeout', hedge.request_timeout),
                        ('max_hedge_latency_ms', hedge.max_hedge_latency_ms),
                        ('basis_ema_seconds', maker.basis_ema_seconds),('quick_resume_seconds',maker.quick_resume_seconds),
                        ('warmup_seconds',maker.warmup_seconds)]:
        number(value, name)
        if not math.isfinite(float(value)):raise ValueError(name+' must be finite')
    if type(hedge.max_retries) is not int or not 0 <= hedge.max_retries <= 3:
        raise ValueError('max_retries must be integer 0..3 (additional attempts)')
    if type(maker.max_requotes_per_sec) is not int or not 1 <= maker.max_requotes_per_sec <= 2:
        raise ValueError('max_requotes_per_sec must be integer 1..2')
    defaults = data.get('default_config', {})
    symbols, disabled = {}, set()
    for symbol, config in data.get('symbol_configs', {}).items():
        if type(config.get('enabled', True)) is not bool:
            raise ValueError('enabled must be boolean')
        if not config.get('enabled', True):
            disabled.add(symbol)
            continue
        if config.get('execution_mode', mode) != mode:
            raise ValueError('Mixed modes cannot own the same capital; use separate accounts/processes')
        grid = {**defaults.get('grid_config', {}), **config.get('grid_config', {})}
        qty = {**defaults.get('quantity_config', {}), **config.get('quantity_config', {})}
        segments = grid['max_segments']
        if type(segments) is not int or segments <= 0:
            raise ValueError('max_segments must be positive integer')
        opening = number(grid['initial_spread_threshold'], 'initial_spread_threshold', positive=False)
        close = number(grid.get('close_threshold', opening-D(str(grid.get('profit_per_segment', '.02')))), 'close_threshold', positive=False)
        step = number(grid['grid_step'], 'grid_step', positive=False)
        if opening < 0 or step < 0 or abs(close) >= 100 or opening >= 100:
            raise ValueError('Invalid percent thresholds')
        profit=number(grid.get('min_close_profit','.01'),'min_close_profit',positive=False)
        timeout=number(grid.get('close_floor_timeout',600),'close_floor_timeout',positive=False)
        if not 0<=profit<100 or timeout<0 or not math.isfinite(float(timeout)):
            raise ValueError('min_close_profit must be in [0,100) and close_floor_timeout >=0')
        symbols[symbol] = MakerSymbolConfig(symbol, number(qty['base_quantity'], 'base_quantity'),
                                          opening, close, step, segments,profit,float(timeout))
    if not symbols:
        raise ValueError('No enabled symbols')
    return MakerProfile(mode, maker, hedge, risk, symbols, frozenset(disabled))
