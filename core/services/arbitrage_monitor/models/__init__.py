"""套利监控数据模型"""

from .arbitrage_models import (
    ArbitrageOpportunity,
    PriceSpread,
    FundingRateSpread,
    ArbitrageConfig
)

__all__ = [
    'ArbitrageOpportunity',
    'PriceSpread',
    'FundingRateSpread',
    'ArbitrageConfig',
]

