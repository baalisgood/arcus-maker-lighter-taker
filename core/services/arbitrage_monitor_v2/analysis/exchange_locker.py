"""
交易所锁定器

职责：
- 从多个交易所中锁定最优的买卖交易所对
- 使用最高价-最低价锁定策略
- 检测异常情况（最高价和最低价在同一交易所）
"""

import logging
from typing import Dict, Optional, Tuple
from decimal import Decimal

from core.adapters.exchanges.models import OrderBookData

# 🔥 使用统一日志系统
from core.adapters.exchanges.utils.setup_logging import LoggingConfig

logger = LoggingConfig.setup_logger(
    name=__name__,
    log_file='exchange_locker.log',
    console_formatter=None,
    file_formatter='detailed',
    level=logging.INFO
)
logger.propagate = False


class ExchangeLocker:
    """交易所锁定器"""
    
    def __init__(self):
        """初始化交易所锁定器"""
        self._lock_counter = 0
    
    def lock_exchanges(
        self,
        orderbooks: Dict[str, OrderBookData]
    ) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """
        锁定最优的买卖交易所对
        
        策略：
        1. 找出最高卖出价（所有交易所中Ask1价格最高的交易所）
        2. 找出最低买入价（所有交易所中Bid1价格最低的交易所）
        3. 如果最高价和最低价在同一交易所，返回警告
        
        Args:
            orderbooks: {exchange: OrderBookData}
        
        Returns:
            (买入交易所, 卖出交易所, 警告信息)
            如果无法锁定，返回 (None, None, 警告信息)
        """
        if len(orderbooks) < 2:
            return None, None, "至少需要2个交易所才能锁定"
        
        # 验证数据完整性
        valid_orderbooks = {}
        for exchange, orderbook in orderbooks.items():
            if self._validate_orderbook(orderbook):
                valid_orderbooks[exchange] = orderbook
        
        if len(valid_orderbooks) < 2:
            return None, None, "至少需要2个有效交易所才能锁定"
        
        # 找出最高买一价（Bid1最高）→ 在该交易所卖出
        highest_bid_exchange = None
        highest_bid_price = Decimal('0')
        
        for exchange, orderbook in valid_orderbooks.items():
            bid_price = orderbook.best_bid.price
            if bid_price > highest_bid_price:
                highest_bid_price = bid_price
                highest_bid_exchange = exchange
        
        # 找出最低卖一价（Ask1最低）→ 在该交易所买入
        lowest_ask_exchange = None
        lowest_ask_price = Decimal('inf')
        
        for exchange, orderbook in valid_orderbooks.items():
            ask_price = orderbook.best_ask.price
            if ask_price < lowest_ask_price:
                lowest_ask_price = ask_price
                lowest_ask_exchange = exchange
        
        # 异常检测：最高价和最低价在同一交易所
        if highest_bid_exchange == lowest_ask_exchange:
            warning_msg = (
                f"⚠️ 最高买一价和最低卖一价在同一交易所（{highest_bid_exchange}），"
                f"无法进行跨交易所套利"
            )
            logger.warning(f"[交易所锁定] {warning_msg}")
            return None, None, warning_msg
        
        # 锁定成功
        # 买入交易所：Ask1价格最低的交易所
        # 卖出交易所：Bid1价格最高的交易所
        buy_exchange = lowest_ask_exchange
        sell_exchange = highest_bid_exchange
        
        self._lock_counter += 1
        logger.debug(
            f"[交易所锁定] 锁定成功: {buy_exchange}买入 -> {sell_exchange}卖出 "
            f"(买入价: {valid_orderbooks[buy_exchange].best_ask.price}, "
            f"卖出价: {valid_orderbooks[sell_exchange].best_bid.price})"
        )
        
        return buy_exchange, sell_exchange, None
    
    def _validate_orderbook(self, orderbook: OrderBookData) -> bool:
        """
        验证订单簿数据
        
        Args:
            orderbook: 订单簿数据
        
        Returns:
            是否有效
        """
        if not orderbook.best_bid or not orderbook.best_ask:
            return False
        
        if orderbook.best_bid.price <= 0 or orderbook.best_ask.price <= 0:
            return False
        
        if orderbook.best_bid.size <= 0 or orderbook.best_ask.size <= 0:
            return False
        
        # 检查价差合理性（Bid应该小于Ask）
        if orderbook.best_bid.price >= orderbook.best_ask.price:
            return False
        
        return True

