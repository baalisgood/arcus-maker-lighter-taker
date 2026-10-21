"""
套利决策模块

职责：
- 实现开仓条件判断（价差套利、资金费率套利）
- 实现平仓条件判断
- 持仓状态管理
"""

from .arbitrage_decision import ArbitrageDecisionEngine
from ..models import PositionInfo, FundingRateData

__all__ = [
    'ArbitrageDecisionEngine',
    'PositionInfo',
    'FundingRateData',
]

