"""
差价计算引擎

职责：
- 计算交易所间的价差
- 识别低买高卖机会
- 提供差价数据
- 多交易所锁定策略
"""

from .spread_calculator import SpreadCalculator, SpreadData
from .exchange_locker import ExchangeLocker

__all__ = ['SpreadCalculator', 'SpreadData', 'ExchangeLocker']

