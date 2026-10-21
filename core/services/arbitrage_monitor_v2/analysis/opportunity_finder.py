"""
套利机会识别器

职责：
- 识别符合条件的套利机会
- 过滤和排序机会
- 管理机会的持续时间
"""

import asyncio
import time
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass, field

from .spread_calculator import SpreadData
from ..config.monitor_config import MonitorConfig
from ..config.debug_config import DebugConfig
from core.adapters.exchanges.utils.setup_logging import LoggingConfig

logger = LoggingConfig.setup_logger(
    name=__name__,
    log_file='opportunity_finder.log',
    console_formatter=None,
    file_formatter='detailed'
)
logger.propagate = False


@dataclass
class ArbitrageOpportunity:
    """套利机会"""
    symbol: str
    exchange_buy: str
    exchange_sell: str
    price_buy: float
    price_sell: float
    size_buy: float
    size_sell: float
    spread_pct: float
    funding_rate_buy: Optional[float] = None
    funding_rate_sell: Optional[float] = None
    funding_rate_diff: Optional[float] = None
    duration_seconds: float = 0.0
    first_seen: datetime = field(default_factory=datetime.now)
    last_seen: datetime = field(default_factory=datetime.now)
    # 🔥 新增：触发条件信息
    trigger_mode: Optional[str] = None  # "spread" 或 "funding_rate"
    trigger_condition: Optional[str] = None  # 具体触发的条件
    
    def update_duration(self):
        """更新持续时间"""
        self.last_seen = datetime.now()
        self.duration_seconds = (self.last_seen - self.first_seen).total_seconds()
    
    def get_opportunity_key(self) -> str:
        """获取机会的唯一标识"""
        return f"{self.symbol}_{self.exchange_buy}_{self.exchange_sell}"


class OpportunityFinder:
    """套利机会识别器"""
    
    def __init__(
        self,
        monitor_config: MonitorConfig,
        debug_config: DebugConfig,
        scroller=None  # 实时滚动区管理器（可选）
    ):
        """
        初始化机会识别器
        
        Args:
            monitor_config: 监控配置
            debug_config: Debug配置
            scroller: 实时滚动区管理器（用于实时打印）
        """
        self.config = monitor_config
        self.debug = debug_config
        self.scroller = scroller  # 🔥 混合模式：实时滚动输出
        
        # 当前追踪的机会 {key: ArbitrageOpportunity}
        self.opportunities: Dict[str, ArbitrageOpportunity] = {}
        
        # 统计信息
        self.stats = {
            'opportunities_found': 0,
            'opportunities_expired': 0,
        }
        
        # 状态日志节流控制（每个symbol每分钟输出一次）
        self._status_log_times: Dict[str, float] = {}
        self._status_log_interval: float = 60.0
    
    def find_opportunities(
        self,
        spreads: List[SpreadData],
        funding_rates: Optional[Dict[str, Dict[str, float]]] = None
    ) -> List[ArbitrageOpportunity]:
        """
        从价差数据中识别套利机会
        
        Args:
            spreads: 价差数据列表
            funding_rates: 资金费率 {exchange: {symbol: rate}}
            
        Returns:
            套利机会列表
        """
        current_opportunities = []
        current_keys = set()
        symbol_stats: Dict[str, Dict[str, Optional[SpreadData]]] = {}
        
        for spread in spreads:
            stats_entry = symbol_stats.setdefault(spread.symbol, {
                'best_spread': None,
                'best_positive': None,
                'accepted_count': 0,
                'filtered_low_spread': 0
            })
            
            if (stats_entry['best_spread'] is None or
                    spread.spread_pct > stats_entry['best_spread'].spread_pct):
                stats_entry['best_spread'] = spread
            
            # 过滤：价差必须大于阈值
            if spread.spread_pct < self.config.min_spread_pct:
                stats_entry['filtered_low_spread'] += 1
                continue
            
            stats_entry['accepted_count'] += 1
            if (stats_entry['best_positive'] is None or
                    spread.spread_pct > stats_entry['best_positive'].spread_pct):
                stats_entry['best_positive'] = spread
            
            # 创建或更新机会
            key = f"{spread.symbol}_{spread.exchange_buy}_{spread.exchange_sell}"
            current_keys.add(key)
            
            if key in self.opportunities:
                # 更新现有机会
                opp = self.opportunities[key]
                opp.price_buy = float(spread.price_buy)
                opp.price_sell = float(spread.price_sell)
                opp.size_buy = float(spread.size_buy)
                opp.size_sell = float(spread.size_sell)
                opp.spread_pct = spread.spread_pct
                opp.update_duration()
            else:
                # 新发现的机会
                opp = ArbitrageOpportunity(
                    symbol=spread.symbol,
                    exchange_buy=spread.exchange_buy,
                    exchange_sell=spread.exchange_sell,
                    price_buy=float(spread.price_buy),
                    price_sell=float(spread.price_sell),
                    size_buy=float(spread.size_buy),
                    size_sell=float(spread.size_sell),
                    spread_pct=spread.spread_pct,
                )
                self.opportunities[key] = opp
                self.stats['opportunities_found'] += 1
                
                # 🔥 混合模式：实时打印新发现的套利机会
                if self.scroller:
                    try:
                        self.scroller.print_opportunity(
                            symbol=spread.symbol,
                            exchange_buy=spread.exchange_buy,
                            exchange_sell=spread.exchange_sell,
                            price_buy=float(spread.price_buy),
                            price_sell=float(spread.price_sell),
                            spread_pct=spread.spread_pct
                        )
                    except Exception:
                        # 静默处理错误，不影响分析
                        pass
            
            # 🔥 添加资金费率信息（参考v1算法：直接相减，保留正负号）
            # 存储的是8小时费率差（小数形式），显示时转换为年化费率差
            funding_rate_diff = None
            if funding_rates:
                opp.funding_rate_buy = funding_rates.get(spread.exchange_buy, {}).get(spread.symbol)
                opp.funding_rate_sell = funding_rates.get(spread.exchange_sell, {}).get(spread.symbol)
                
                if opp.funding_rate_buy is not None and opp.funding_rate_sell is not None:
                    # 🔥 资金费率差应该永远为正数（绝对值差值）
                    # 例如：正数减去负数 = 正数，大负数减去小负数 = 正数
                    # 存储8小时费率差（小数形式，如0.0001表示0.01%）
                    opp.funding_rate_diff = abs(opp.funding_rate_sell - opp.funding_rate_buy)
                    funding_rate_diff = opp.funding_rate_diff
            
            # 🔥 混合模式：实时打印新发现的套利机会（包含资金费率差）
            if self.scroller:
                try:
                    self.scroller.print_opportunity(
                        symbol=spread.symbol,
                        exchange_buy=spread.exchange_buy,
                        exchange_sell=spread.exchange_sell,
                        price_buy=float(spread.price_buy),
                        price_sell=float(spread.price_sell),
                        spread_pct=spread.spread_pct,
                        funding_rate_diff=funding_rate_diff  # 🔥 传递8小时费率差（小数形式）
                    )
                except Exception:
                    # 静默处理错误，不影响分析
                    pass
            
            current_opportunities.append(opp)
        
        # 清理过期的机会
        expired_keys = set(self.opportunities.keys()) - current_keys
        for key in expired_keys:
            del self.opportunities[key]
            self.stats['opportunities_expired'] += 1
        
        # 按价差排序（从大到小）
        current_opportunities.sort(key=lambda x: x.spread_pct, reverse=True)
        
        # 输出状态日志（限频）
        self._log_symbol_statuses(symbol_stats)
        
        return current_opportunities
    
    def _log_symbol_statuses(self, symbol_stats: Dict[str, Dict[str, Optional[SpreadData]]]) -> None:
        """
        每个symbol限频输出一次状态，帮助定位为何没有触发套利
        """
        if not symbol_stats:
            # 没有任何价差信息时仍输出状态，确保日志文件生成
            self._log_status(
                "global:no_spreads",
                "[套利状态] 当前未收到任何价差信息，无法评估套利机会"
            )
            return
        
        threshold = self.config.min_spread_pct
        for symbol, stats_entry in symbol_stats.items():
            accepted_count = stats_entry['accepted_count']
            if accepted_count > 0 and stats_entry['best_positive'] is not None:
                spread = stats_entry['best_positive']
                message = (
                    f"[套利状态] {symbol}: 满足最小价差（≥{threshold:.4f}%），"
                    f"当前最优 {spread.exchange_buy}买→{spread.exchange_sell}卖 "
                    f"差价 +{spread.spread_pct:.4f}% "
                    f"(买价 {float(spread.price_buy):.4f}, 卖价 {float(spread.price_sell):.4f})"
                )
                self._log_status(f"{symbol}:opportunity_positive", message)
            else:
                best_spread = stats_entry['best_spread']
                if best_spread is None:
                    message = f"[套利状态] {symbol}: 暂无有效价差数据，无法评估套利机会"
                else:
                    message = (
                        f"[套利状态] {symbol}: 最大价差 +{best_spread.spread_pct:.4f}% "
                        f"低于门槛 {threshold:.4f}%（filtered {stats_entry['filtered_low_spread']} 次）"
                    )
                self._log_status(f"{symbol}:opportunity_waiting", message)
    
    def _log_status(self, key: str, message: str) -> None:
        """状态日志限频输出"""
        now = time.time()
        last = self._status_log_times.get(key)
        if last is None or (now - last) >= self._status_log_interval:
            logger.info(message)
            self._status_log_times[key] = now
    
    def get_opportunities_by_symbol(self, symbol: str) -> List[ArbitrageOpportunity]:
        """
        获取指定交易对的机会
        
        Args:
            symbol: 交易对
            
        Returns:
            机会列表
        """
        return [opp for opp in self.opportunities.values() if opp.symbol == symbol]
    
    def get_all_opportunities(self) -> List[ArbitrageOpportunity]:
        """获取所有机会"""
        opps = list(self.opportunities.values())
        opps.sort(key=lambda x: x.spread_pct, reverse=True)
        return opps
    
    def get_top_opportunities(self, limit: int = 10) -> List[ArbitrageOpportunity]:
        """
        获取Top N的机会
        
        Args:
            limit: 数量限制
            
        Returns:
            Top机会列表
        """
        all_opps = self.get_all_opportunities()
        return all_opps[:limit]
    
    def get_stats(self) -> Dict:
        """获取统计信息"""
        return {
            **self.stats,
            'active_opportunities': len(self.opportunities),
        }
    
    def clear(self):
        """清空所有机会"""
        self.opportunities.clear()

