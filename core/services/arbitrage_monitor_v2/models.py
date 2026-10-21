"""
套利系统数据模型

职责：
- 定义系统中使用的数据类
- 避免循环导入问题
"""

from dataclasses import dataclass, field
from typing import List, Optional, Set
from datetime import datetime
from decimal import Decimal


@dataclass
class PositionLeg:
    """单次开仓腿记录"""
    leg_id: str
    quantity: Decimal
    buy_exchange: str
    sell_exchange: str
    buy_order_id: Optional[str] = None
    sell_order_id: Optional[str] = None
    open_time: datetime = field(default_factory=datetime.now)
    closed_quantity: Decimal = field(default_factory=lambda: Decimal('0'))
    status: str = "open"  # open / partial / closed / failed
    executed_quantity: Decimal = field(default_factory=lambda: Decimal('0'))
    error_message: Optional[str] = None


@dataclass
class PositionInfo:
    """持仓信息"""
    symbol: str
    exchange_buy: str
    exchange_sell: str
    
    # 开仓信息
    open_mode: str  # "spread" 或 "funding_rate"
    open_condition: str  # "condition1" 或 "condition2"
    open_spread_pct: float  # 开仓时的价差百分比
    open_funding_rate_diff: float  # 开仓时的资金费率差（百分比）
    open_funding_rate_diff_annual: float  # 开仓时的资金费率差年化（百分比/年）
    open_price_buy: Decimal  # 开仓时的买入价格
    open_price_sell: Decimal  # 开仓时的卖出价格
    open_funding_rate_buy: float  # 开仓时的买入交易所资金费率
    open_funding_rate_sell: float  # 开仓时的卖出交易所资金费率
    open_time: datetime  # 开仓时间
    
    # 持仓状态
    quantity: Decimal  # 理论持仓数量（用于计算）
    quantity_buy: Decimal  # 买入交易所实际持仓数量
    quantity_sell: Decimal  # 卖出交易所实际持仓数量
    legs: List[PositionLeg] = field(default_factory=list)
    is_open: bool = True  # 是否持仓中


@dataclass
class ClosePositionContext:
    """平仓决策所需的实时行情上下文"""
    close_price_buy: float              # 平仓时买入价格（用于回补空头）
    close_price_sell: float             # 平仓时卖出价格（用于平掉多头）
    long_leg_return_pct: float          # 多头腿收益百分比
    short_leg_return_pct: float         # 空头腿收益百分比
    total_profit_pct: float             # 综合收益（long + short）
    close_spread_pct: float             # 平仓方向即时价差（卖出价 - 买入价）


@dataclass
class FundingRateData:
    """资金费率数据"""
    exchange_buy: str
    exchange_sell: str
    funding_rate_buy: float  # 买入交易所资金费率
    funding_rate_sell: float  # 卖出交易所资金费率
    funding_rate_diff: float  # 资金费率差（百分比，永远≥0）
    funding_rate_diff_annual: float  # 资金费率差年化（百分比/年）
    
    # 🔥 新增：方向信息（关键！）
    is_favorable_for_position: bool = True  # 当前费率方向是否有利于持仓方向
    # True：sell方做空收取更高费率，buy方做多支付更低费率 ✅
    # False：方向反转，需要支付费率 ❌
    
    @property
    def higher_funding_exchange(self) -> str:
        """返回费率更高的交易所"""
        return self.exchange_sell if self.funding_rate_sell >= self.funding_rate_buy else self.exchange_buy


@dataclass
class RiskStatus:
    """风险状态"""
    account_data_unavailable: Set[str] = field(default_factory=set)
    is_paused: bool = False  # 是否暂停套利
    pause_reason: Optional[str] = None  # 暂停原因
    network_failure: bool = False  # 网络故障
    exchange_maintenance: Set[str] = field(default_factory=set)  # 维护中的交易所
    low_balance_exchanges: Set[str] = field(default_factory=set)  # 余额不足的交易所
    critical_balance_exchanges: Set[str] = field(default_factory=set)  # 余额严重不足的交易所


@dataclass
class PositionValue:
    """持仓价值"""
    symbol: str
    exchange: str
    usdc_value: Decimal  # USDC价值
    quantity: Decimal  # 持仓数量


# ============================================================================
# 分段套利模式数据模型
# ============================================================================

@dataclass
class PositionSegment:
    """分段持仓段"""
    segment_id: int                      # 段序号（1, 2, 3, ...）
    target_quantity: Decimal             # 该段目标数量
    open_quantity: Decimal               # 当前已建持仓数量（可部分填充）
    open_spread_pct: float               # 开仓价差（%）
    open_time: datetime                  # 开仓时间
    open_price_buy: Decimal             # 开仓时买入价格
    open_price_sell: Decimal            # 开仓时卖出价格
    open_funding_rate_buy: float        # 开仓时买入资金费率
    open_funding_rate_sell: float       # 开仓时卖出资金费率
    
    # 订单信息
    buy_order_id: Optional[str] = None
    sell_order_id: Optional[str] = None
    
    # 平仓信息
    is_closed: bool = False             # 是否已平仓
    close_time: Optional[datetime] = None # 平仓时间
    close_spread_pct: Optional[float] = None # 平仓价差（%）
    close_price_buy: Optional[Decimal] = None # 平仓时买入价格
    close_price_sell: Optional[Decimal] = None # 平仓时卖出价格
    
    # 平仓订单信息
    close_buy_order_id: Optional[str] = None
    close_sell_order_id: Optional[str] = None
    entry_fee_buy_per_unit: Optional[Decimal] = None
    entry_fee_sell_per_unit: Optional[Decimal] = None


@dataclass
class SegmentedPosition:
    """分段套利持仓"""
    symbol: str                          # 交易对
    exchange_buy: str                    # 买入交易所
    exchange_sell: str                   # 卖出交易所
    segments: List[PositionSegment]      # 持仓段列表
    total_quantity: Decimal              # 总持仓数量
    avg_open_spread_pct: float           # 平均开仓价差（%）
    create_time: datetime                # 创建时间
    last_update_time: datetime           # 最后更新时间
    is_open: bool = True                # 是否有持仓
    buy_symbol: Optional[str] = None     # 买入腿交易对（可选，默认与symbol一致）
    sell_symbol: Optional[str] = None    # 卖出腿交易对（可选，默认与symbol一致）
    pair_key: str = ""                   # 唯一套利对标识（用于1对多模式）
    
    def get_open_segments(self) -> List[PositionSegment]:
        """获取所有未平仓的段"""
        return [
            seg for seg in self.segments
            if not seg.is_closed and seg.open_quantity > Decimal('0')
        ]
    
    def get_segment_count(self) -> int:
        """获取未平仓段数"""
        return len(self.get_open_segments())
    
    def get_next_segment_id(self) -> int:
        """获取下一段的序号"""
        if not self.segments:
            return 1
        return max(seg.segment_id for seg in self.segments) + 1
    
    def calculate_avg_spread(self) -> float:
        """计算平均开仓价差（加权平均）"""
        open_segments = self.get_open_segments()
        if not open_segments:
            return 0.0
        
        total_quantity = sum(float(seg.open_quantity) for seg in open_segments)
        if total_quantity == 0:
            return 0.0
        
        weighted_sum = sum(
            seg.open_spread_pct * float(seg.open_quantity)
            for seg in open_segments
        )
        
        return weighted_sum / total_quantity

    def get_segment(self, segment_id: int) -> Optional[PositionSegment]:
        """根据段ID获取未关闭的段"""
        return next(
            (seg for seg in self.segments if seg.segment_id == segment_id and not seg.is_closed),
            None
        )
    
    def get_completed_segment_count(
        self,
        tolerance: Decimal = Decimal('0.00000001')
    ) -> int:
        """计算已填满目标数量的段数"""
        completed = 0
        for seg in self.get_open_segments():
            if seg.target_quantity - seg.open_quantity <= tolerance:
                completed += 1
        return completed
    
    def get_active_incomplete_segment(
        self,
        tolerance: Decimal = Decimal('0.00000001')
    ) -> Optional[PositionSegment]:
        """
        获取当前最新的未完成段（目标数量尚未填满）
        """
        candidates = [
            seg for seg in self.get_open_segments()
            if seg.target_quantity - seg.open_quantity > tolerance
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda seg: seg.segment_id)

