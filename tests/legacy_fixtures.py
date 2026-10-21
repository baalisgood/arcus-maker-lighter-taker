"""Self-contained order fixture for retained taker-executor regressions."""
from datetime import datetime
from decimal import Decimal as D
from core.adapters.exchanges.models import OrderData,OrderSide,OrderType,OrderStatus


def filled_order(identity,quantity,price,is_buy,fee=None):
    return OrderData(identity,identity,'BTC-USDC-PERP',OrderSide.BUY if is_buy else OrderSide.SELL,
        OrderType.MARKET,quantity,None,quantity,D(0),quantity*price,price,OrderStatus.FILLED,
        datetime.now(),datetime.now(),fee,[],{},{})
