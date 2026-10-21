"""
Debug配置模块

职责：
- 管理Debug模式配置
- 控制日志级别和输出
- 管理性能监控开关
"""

from dataclasses import dataclass
from enum import Enum
from typing import Set


class DebugLevel(Enum):
    """Debug级别"""
    OFF = 0        # 关闭Debug
    BASIC = 1      # 基础信息（关键事件）
    DETAILED = 2   # 详细信息（数据流追踪）
    VERBOSE = 3    # 完整信息（包括原始WebSocket消息）


@dataclass
class DebugConfig:
    """Debug配置"""
    
    # 全局Debug级别
    level: DebugLevel = DebugLevel.OFF
    
    # 分层Debug控制
    debug_data_layer: bool = False      # 数据层Debug
    debug_analysis_layer: bool = False  # 分析层Debug
    debug_ui_layer: bool = False        # UI层Debug
    
    # 功能开关
    show_ws_messages: bool = False      # 显示WebSocket消息（简化版）
    show_spread_calc: bool = False      # 显示差价计算过程
    show_performance: bool = True       # 显示性能指标
    show_queue_stats: bool = True       # 显示队列统计
    show_health_status: bool = True     # 显示健康状态
    
    # WebSocket消息采样
    ws_message_sample_rate: int = 10    # 每N条消息显示1条（避免刷屏）
    
    # 差价计算采样
    spread_calc_sample_rate: int = 5    # 每N次计算显示1次
    
    # 性能监控
    track_latency: bool = True          # 追踪延迟
    track_throughput: bool = True       # 追踪吞吐量
    
    # 监控的交易对（空集表示监控所有）
    monitored_symbols: Set[str] = None
    
    def __post_init__(self):
        """初始化后处理"""
        if self.monitored_symbols is None:
            self.monitored_symbols = set()
    
    def is_debug_enabled(self) -> bool:
        """是否启用Debug模式"""
        return self.level != DebugLevel.OFF
    
    def should_show_ws_message(self, counter: int) -> bool:
        """
        是否应该显示WebSocket消息
        
        Args:
            counter: 消息计数器
            
        Returns:
            是否显示
        """
        if not self.show_ws_messages:
            return False
        return counter % self.ws_message_sample_rate == 0
    
    def should_show_spread_calc(self, counter: int) -> bool:
        """
        是否应该显示差价计算
        
        Args:
            counter: 计算计数器
            
        Returns:
            是否显示
        """
        if not self.show_spread_calc:
            return False
        return counter % self.spread_calc_sample_rate == 0
    
    def should_monitor_symbol(self, symbol: str) -> bool:
        """
        是否应该监控该交易对
        
        Args:
            symbol: 交易对
            
        Returns:
            是否监控
        """
        if not self.monitored_symbols:
            return True  # 空集表示监控所有
        return symbol in self.monitored_symbols
    
    @classmethod
    def create_basic(cls) -> 'DebugConfig':
        """创建基础Debug配置"""
        return cls(
            level=DebugLevel.BASIC,
            show_performance=True,
            show_queue_stats=True,
            show_health_status=True,
        )
    
    @classmethod
    def create_detailed(cls, symbols: Set[str] = None) -> 'DebugConfig':
        """
        创建详细Debug配置
        
        Args:
            symbols: 要监控的交易对，None表示全部
        """
        return cls(
            level=DebugLevel.DETAILED,
            debug_data_layer=True,
            debug_analysis_layer=True,
            show_ws_messages=True,
            show_spread_calc=True,
            show_performance=True,
            show_queue_stats=True,
            show_health_status=True,
            ws_message_sample_rate=20,  # 降低采样率
            spread_calc_sample_rate=10,
            monitored_symbols=symbols or set(),
        )
    
    @classmethod
    def create_production(cls) -> 'DebugConfig':
        """创建生产环境配置（最小Debug输出）"""
        return cls(
            level=DebugLevel.OFF,
            show_performance=False,
            show_queue_stats=False,
            show_health_status=True,
        )

