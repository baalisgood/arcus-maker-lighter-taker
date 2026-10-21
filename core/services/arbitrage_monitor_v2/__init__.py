"""
套利监控系统 V2 - 模块化重构版本

特性：
- 零延迟数据接收和处理
- 模块化架构，职责分离
- 支持Debug模式
- 完善的健康监控
"""

from .core.orchestrator import ArbitrageOrchestrator
from .config.monitor_config import ConfigManager, MonitorConfig
from .config.debug_config import DebugConfig, DebugLevel

__version__ = "2.0.0"

__all__ = [
    'ArbitrageOrchestrator',
    'ConfigManager',
    'MonitorConfig',
    'DebugConfig',
    'DebugLevel',
]

