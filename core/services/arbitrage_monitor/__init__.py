"""
套利监控服务模块

提供多交易所价差和资金费率监控功能。
"""

from .interfaces.arbitrage_monitor_service import IArbitrageMonitorService
from .implementations.arbitrage_monitor_impl import ArbitrageMonitorService
from .models.arbitrage_models import (
    ArbitrageOpportunity,
    PriceSpread,
    FundingRateSpread,
    ArbitrageConfig
)

__all__ = [
    'IArbitrageMonitorService',
    'ArbitrageMonitorService',
    'ArbitrageOpportunity',
    'PriceSpread',
    'FundingRateSpread',
    'ArbitrageConfig',
]

