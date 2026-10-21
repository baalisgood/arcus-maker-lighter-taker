"""
历史数据记录模块

职责：
- 记录套利机会的历史数据
- 时间窗口采样
- 异步批量写入
- 数据查询接口
- 历史数据计算（内存计算结果）
"""

from .spread_history_recorder import SpreadHistoryRecorder
from .spread_history_reader import SpreadHistoryReader
from .chart_generator import ChartGenerator
from .history_calculator import HistoryDataCalculator, ExchangePairResult

__all__ = [
    'SpreadHistoryRecorder',
    'SpreadHistoryReader',
    'ChartGenerator',
    'HistoryDataCalculator',
    'ExchangePairResult',
]

