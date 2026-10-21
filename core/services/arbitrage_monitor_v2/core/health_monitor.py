"""
健康监控模块

职责：
- 监控WebSocket连接状态
- 监控数据更新时间
- 检测异常和超时
"""

import asyncio
from typing import Dict, List
from datetime import datetime, timedelta
from collections import defaultdict


class HealthMonitor:
    """健康监控器"""
    
    def __init__(self, data_timeout_seconds: int = 30):
        """
        初始化健康监控器
        
        Args:
            data_timeout_seconds: 数据超时时间（秒）
        """
        self.data_timeout = data_timeout_seconds
        
        # 数据时间戳 {exchange: {symbol: datetime}}
        self.last_data_time: Dict[str, Dict[str, datetime]] = defaultdict(dict)
        
        # 连接状态 {exchange: status}
        self.connection_status: Dict[str, str] = {}
        
        # 运行状态
        self.running = False
        self.monitor_task: Optional[asyncio.Task] = None
    
    async def start(self, check_interval: int = 10):
        """
        启动健康监控
        
        Args:
            check_interval: 检查间隔（秒）
        """
        if self.running:
            return
        
        self.running = True
        self.monitor_task = asyncio.create_task(self._monitor_loop(check_interval))
        print("✅ 健康监控已启动")
    
    async def stop(self):
        """停止健康监控"""
        self.running = False
        if self.monitor_task:
            self.monitor_task.cancel()
            try:
                await self.monitor_task
            except asyncio.CancelledError:
                pass
        print("🛑 健康监控已停止")
    
    async def _monitor_loop(self, check_interval: int):
        """
        监控循环
        
        Args:
            check_interval: 检查间隔（秒）
        """
        try:
            while self.running:
                # 检查所有交易所的健康状态
                for exchange in list(self.last_data_time.keys()):
                    status = self._check_exchange_health(exchange)
                    self.connection_status[exchange] = status
                
                await asyncio.sleep(check_interval)
                
        except asyncio.CancelledError:
            print("🛑 健康监控循环已取消")
        except Exception as e:
            print(f"❌ 健康监控循环错误: {e}")
    
    def _check_exchange_health(self, exchange: str) -> str:
        """
        检查交易所健康状态
        
        Args:
            exchange: 交易所名称
            
        Returns:
            状态：healthy, degraded, unhealthy
        """
        now = datetime.now()
        symbol_times = self.last_data_time.get(exchange, {})
        
        if not symbol_times:
            return "unknown"
        
        # 计算超时的交易对数量
        stale_count = 0
        total_count = len(symbol_times)
        
        for symbol, last_time in symbol_times.items():
            age = (now - last_time).total_seconds()
            if age > self.data_timeout:
                stale_count += 1
        
        stale_ratio = stale_count / total_count if total_count > 0 else 0
        
        if stale_ratio == 0:
            return "healthy"
        elif stale_ratio < 0.3:
            return "degraded"
        else:
            return "unhealthy"
    
    def update_data_time(self, exchange: str, symbol: str):
        """
        更新数据时间戳
        
        Args:
            exchange: 交易所
            symbol: 交易对
        """
        self.last_data_time[exchange][symbol] = datetime.now()
    
    def get_exchange_status(self, exchange: str) -> str:
        """
        获取交易所状态
        
        Args:
            exchange: 交易所名称
            
        Returns:
            状态字符串
        """
        return self.connection_status.get(exchange, "unknown")
    
    def get_all_status(self) -> Dict[str, str]:
        """获取所有交易所的状态"""
        return self.connection_status.copy()
    
    def is_healthy(self, exchange: str) -> bool:
        """
        检查交易所是否健康
        
        Args:
            exchange: 交易所名称
            
        Returns:
            是否健康
        """
        status = self.get_exchange_status(exchange)
        return status in ["healthy", "degraded"]
    
    def get_stale_symbols(self, exchange: str) -> List[str]:
        """
        获取超时的交易对列表
        
        Args:
            exchange: 交易所名称
            
        Returns:
            超时的交易对列表
        """
        now = datetime.now()
        stale = []
        
        for symbol, last_time in self.last_data_time.get(exchange, {}).items():
            age = (now - last_time).total_seconds()
            if age > self.data_timeout:
                stale.append(symbol)
        
        return stale

