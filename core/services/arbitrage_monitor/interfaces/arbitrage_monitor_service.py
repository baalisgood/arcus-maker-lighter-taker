"""
套利监控服务接口

定义套利监控服务的抽象接口。
"""

from abc import ABC, abstractmethod
from typing import List, Dict
from decimal import Decimal

from ..models.arbitrage_models import ArbitrageOpportunity, ArbitrageConfig


class IArbitrageMonitorService(ABC):
    """套利监控服务接口"""
    
    @abstractmethod
    async def start(self) -> bool:
        """
        启动监控服务
        
        Returns:
            是否启动成功
        """
        pass
    
    @abstractmethod
    async def stop(self) -> None:
        """停止监控服务"""
        pass
    
    @abstractmethod
    def get_opportunities(self) -> List[ArbitrageOpportunity]:
        """
        获取当前所有套利机会
        
        Returns:
            套利机会列表
        """
        pass
    
    @abstractmethod
    def get_current_prices(self, symbol: str) -> Dict[str, Decimal]:
        """
        获取当前价格
        
        Args:
            symbol: 交易对符号
        
        Returns:
            {exchange: price}
        """
        pass
    
    @abstractmethod
    def get_current_funding_rates(self, symbol: str) -> Dict[str, Decimal]:
        """
        获取当前资金费率
        
        Args:
            symbol: 交易对符号
        
        Returns:
            {exchange: funding_rate}
        """
        pass
    
    @abstractmethod
    def get_statistics(self) -> Dict:
        """
        获取统计信息
        
        Returns:
            统计信息字典
        """
        pass
    
    @abstractmethod
    def add_opportunity_callback(self, callback) -> None:
        """
        添加套利机会回调函数
        
        Args:
            callback: 回调函数
        """
        pass

