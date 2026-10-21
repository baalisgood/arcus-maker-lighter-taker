"""Pure Decimal quote/lot functions. Percent values are divided by 100 once."""
from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from math import lcm

D = Decimal


def common_step(a, b):
    a, b = D(str(a)), D(str(b))
    if not a.is_finite() or not b.is_finite() or min(a, b) <= 0:
        raise ValueError('Invalid venue size steps')
    scale = 10 ** max(0, -a.as_tuple().exponent, -b.as_tuple().exponent)
    return D(lcm(int(a*scale), int(b*scale))) / scale


def floor_quantity(qty, step):
    return (D(qty)/step).to_integral_value(rounding=ROUND_FLOOR)*step


@dataclass(frozen=True)
class MarketRules:
    arcus_step: D
    lighter_step: D
    arcus_min: D
    lighter_min: D
    arcus_notional: D
    lighter_notional: D
    arcus_max: D
    tick: D
    tick_tiers: tuple = ()

    def __post_init__(self):
        values = (self.arcus_step, self.lighter_step, self.arcus_min, self.lighter_min,
                  self.arcus_notional, self.lighter_notional, self.arcus_max, self.tick)
        if any(not v.is_finite() or v <= 0 for v in values):
            raise ValueError('Both venues require complete positive metadata')
        if self.arcus_max < self.arcus_min:
            raise ValueError('Inconsistent market metadata')

    @property
    def step(self):
        return common_step(self.arcus_step, self.lighter_step)

    def price_tick(self, price):
        for upper, tick in self.tick_tiers:
            if upper is None or price < upper:
                return common_step(self.tick, tick)
        return self.tick

    def legal(self, qty, arcus_price, lighter_price):
        return (qty > 0 and qty % self.step == 0 and self.arcus_min <= qty <= self.arcus_max
                and qty*arcus_price >= max(D(5), self.arcus_notional))


def hedge_quantity(remaining, rules, executable_price):
    """Conservative user policy, including metadata minimums; never round exposure up."""
    qty = floor_quantity(D(remaining), rules.lighter_step)
    return qty if (qty >= rules.lighter_min and executable_price is not None
                   and qty*executable_price >= max(D(10), rules.lighter_notional)) else D(0)


def lighter_ioc_price(side,bid,ask,price_decimals):
    """Native 0.1% IOC price bound, identical for live signing and paper matching."""
    scale=D(10)**price_decimals
    worst=(ask*D('1.001') if side==1 else bid*D('.999'))*scale
    return int(worst.to_integral_value(rounding=ROUND_CEILING if side==1 else ROUND_FLOOR))


def quote_price(side, reference, threshold_pct, rules, arcus_bid, arcus_ask, basis_pct=D(0), *, price_bound=None):
    raw = reference*(1+(basis_pct-D(side)*threshold_pct)/100)
    if price_bound is not None:
        raw=min(raw,price_bound) if side==1 else max(raw,price_bound)
    # Search each tier on the desired side of its strict boundary.
    target=min(raw,arcus_ask) if side==1 else max(raw,arcus_bid)
    tiers=list(rules.tick_tiers)
    if not tiers or tiers[-1][0] is not None:tiers.append((None,rules.tick))
    lower=D(0);candidates=[]
    for upper,tier_tick in tiers:
        tick=common_step(rules.tick,tier_tick)
        if side==1:
            limit=min(target,upper) if upper is not None else target
            candidate=(limit/tick).to_integral_value(rounding=ROUND_FLOOR)*tick
            if candidate>=arcus_ask or (upper is not None and candidate>=upper):candidate-=tick
        else:
            limit=max(target,lower)
            candidate=(limit/tick).to_integral_value(rounding=ROUND_CEILING)*tick
            if candidate<=arcus_bid:candidate+=tick
        if (candidate>0 and candidate>=lower and (upper is None or candidate<upper)
                and candidate%rules.price_tick(candidate)==0
                and (candidate<=raw and candidate<arcus_ask if side==1 else candidate>=raw and candidate>arcus_bid)):
            candidates.append(candidate)
        if upper is None:break
        lower=upper
    return (max(candidates) if side==1 else min(candidates)) if candidates else None


def should_requote(anchor, current, age, config):
    return abs(current/anchor-1)*100 > config.requote_threshold or age >= config.max_order_age
