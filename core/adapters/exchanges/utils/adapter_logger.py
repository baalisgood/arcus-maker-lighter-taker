"""
交易所适配器统一日志工具

提供统一的日志格式化接口，确保日志清晰、可读性强
"""

import logging
from typing import Optional, Dict, Any
from datetime import datetime
from collections import defaultdict


class AdapterLogger:
    """
    适配器日志工具类
    
    提供统一的日志格式化方法，确保日志清晰、可读性强
    """
    
    # 日志消息模板
    TEMPLATES = {
        'connection': {
            'success': "[{exchange}] 连接成功",
            'failed': "[{exchange}] 连接失败: {error}",
            'reconnecting': "[{exchange}] 正在重连 (尝试 {attempt}/{max})",
            'disconnected': "[{exchange}] 连接已断开",
        },
        'balance': {
            'success': "[{exchange}] 余额查询成功: {count}个币种",
            'failed': "[{exchange}] 余额查询失败: {error}",
            'cached': "[{exchange}] 使用缓存余额 (TTL: {ttl}s)",
        },
        'position': {
            'success': "[{exchange}] 持仓查询成功: {count}个持仓",
            'failed': "[{exchange}] 持仓查询失败: {error}",
            'cached': "[{exchange}] 使用缓存持仓 (TTL: {ttl}s)",
        },
        'order': {
            'placed': "[{exchange}] 订单已提交: {symbol} {side} {amount} @ {price}",
            'filled': "[{exchange}] 订单已成交: {order_id}",
            'cancelled': "[{exchange}] 订单已取消: {order_id}",
            'failed': "[{exchange}] 订单失败: {error}",
        },
        'websocket': {
            'connected': "[{exchange}] WebSocket已连接",
            'disconnected': "[{exchange}] WebSocket已断开",
            'reconnecting': "[{exchange}] WebSocket重连中...",
            'subscribed': "[{exchange}] 已订阅: {channel}",
            'unsubscribed': "[{exchange}] 已取消订阅: {channel}",
        },
        'heartbeat': {
            'ping': "[{exchange}] 心跳检测: ping",
            'pong': "[{exchange}] 心跳检测: pong",
            'timeout': "[{exchange}] 心跳超时，触发重连",
        },
        'error': {
            'rate_limit': "[{exchange}] API限流 (429)，等待后重试",
            'network': "[{exchange}] 网络错误: {error}",
            'auth': "[{exchange}] 认证失败: {error}",
            'server': "[{exchange}] 服务器错误 ({code}): {error}",
            'unknown': "[{exchange}] 未知错误: {error}",
        },
        'data': {
            'ticker': "[{exchange}] 价格更新: {symbol} = {price}",
            'orderbook': "[{exchange}] 订单簿更新: {symbol} (买盘:{bids}档, 卖盘:{asks}档)",
            'trade': "[{exchange}] 成交: {symbol} {side} {amount} @ {price}",
        },
    }
    
    def __init__(self, logger: logging.Logger, exchange_id: str):
        """
        初始化适配器日志工具
        
        Args:
            logger: Python logger实例
            exchange_id: 交易所ID
        """
        self.logger = logger
        self.exchange_id = exchange_id
        
        # 日志聚合（用于减少重复日志）
        self._log_counts: Dict[str, int] = defaultdict(int)
        self._last_log_time: Dict[str, float] = {}
        self._aggregation_window = 60.0  # 60秒内的重复日志会被聚合
    
    def _should_log(self, key: str, level: int = logging.INFO) -> bool:
        """
        判断是否应该记录日志（用于日志聚合）
        
        Args:
            key: 日志键（用于聚合）
            level: 日志级别
        
        Returns:
            是否应该记录日志
        """
        import time
        
        # DEBUG级别总是记录
        if level == logging.DEBUG:
            return True
        
        # ERROR级别总是记录
        if level == logging.ERROR:
            return True
        
        current_time = time.time()
        last_time = self._last_log_time.get(key, 0)
        
        # 如果超过聚合窗口，重置计数
        if current_time - last_time > self._aggregation_window:
            self._log_counts[key] = 0
        
        # 第一次或超过聚合窗口，总是记录
        if self._log_counts[key] == 0 or current_time - last_time > self._aggregation_window:
            self._last_log_time[key] = current_time
            return True
        
        # 增加计数
        self._log_counts[key] += 1
        
        # 每10次记录一次聚合日志
        if self._log_counts[key] % 10 == 0:
            return True
        
        return False
    
    def _format_message(self, template: str, **kwargs) -> str:
        """格式化日志消息"""
        kwargs.setdefault('exchange', self.exchange_id)
        return template.format(**kwargs)
    
    # ========== 连接相关日志 ==========
    
    def connection_success(self):
        """记录连接成功"""
        msg = self._format_message(self.TEMPLATES['connection']['success'])
        self.logger.info(msg)
    
    def connection_failed(self, error: str):
        """记录连接失败"""
        msg = self._format_message(self.TEMPLATES['connection']['failed'], error=str(error))
        self.logger.error(msg)
    
    def reconnecting(self, attempt: int, max_attempts: int = 10):
        """记录重连"""
        msg = self._format_message(
            self.TEMPLATES['connection']['reconnecting'],
            attempt=attempt,
            max=max_attempts
        )
        self.logger.warning(msg)
    
    def disconnected(self):
        """记录断开连接"""
        msg = self._format_message(self.TEMPLATES['connection']['disconnected'])
        self.logger.info(msg)
    
    # ========== 余额相关日志 ==========
    
    def balance_success(self, count: int):
        """记录余额查询成功"""
        key = f"balance_success_{self.exchange_id}"
        if self._should_log(key, logging.INFO):
            msg = self._format_message(self.TEMPLATES['balance']['success'], count=count)
            self.logger.info(msg)
    
    def balance_failed(self, error: str):
        """记录余额查询失败"""
        msg = self._format_message(self.TEMPLATES['balance']['failed'], error=str(error))
        self.logger.error(msg)
    
    def balance_cached(self, ttl: int):
        """记录使用缓存余额"""
        msg = self._format_message(self.TEMPLATES['balance']['cached'], ttl=ttl)
        self.logger.debug(msg)
    
    # ========== 持仓相关日志 ==========
    
    def position_success(self, count: int):
        """记录持仓查询成功"""
        msg = self._format_message(self.TEMPLATES['position']['success'], count=count)
        self.logger.info(msg)
    
    def position_failed(self, error: str):
        """记录持仓查询失败"""
        msg = self._format_message(self.TEMPLATES['position']['failed'], error=str(error))
        self.logger.error(msg)
    
    def position_cached(self, ttl: int):
        """记录使用缓存持仓"""
        msg = self._format_message(self.TEMPLATES['position']['cached'], ttl=ttl)
        self.logger.debug(msg)
    
    # ========== 订单相关日志 ==========
    
    def order_placed(self, symbol: str, side: str, amount: str, price: str):
        """记录订单提交"""
        msg = self._format_message(
            self.TEMPLATES['order']['placed'],
            symbol=symbol,
            side=side,
            amount=amount,
            price=price
        )
        self.logger.info(msg)
    
    def order_filled(self, order_id: str):
        """记录订单成交"""
        msg = self._format_message(self.TEMPLATES['order']['filled'], order_id=order_id)
        self.logger.info(msg)
    
    def order_cancelled(self, order_id: str):
        """记录订单取消"""
        msg = self._format_message(self.TEMPLATES['order']['cancelled'], order_id=order_id)
        self.logger.info(msg)
    
    def order_failed(self, error: str):
        """记录订单失败"""
        msg = self._format_message(self.TEMPLATES['order']['failed'], error=str(error))
        self.logger.error(msg)
    
    # ========== WebSocket相关日志 ==========
    
    def ws_connected(self):
        """记录WebSocket连接"""
        msg = self._format_message(self.TEMPLATES['websocket']['connected'])
        self.logger.info(msg)
    
    def ws_disconnected(self):
        """记录WebSocket断开"""
        msg = self._format_message(self.TEMPLATES['websocket']['disconnected'])
        self.logger.warning(msg)
    
    def ws_reconnecting(self):
        """记录WebSocket重连"""
        msg = self._format_message(self.TEMPLATES['websocket']['reconnecting'])
        self.logger.warning(msg)
    
    def ws_subscribed(self, channel: str):
        """记录WebSocket订阅"""
        msg = self._format_message(self.TEMPLATES['websocket']['subscribed'], channel=channel)
        self.logger.info(msg)
    
    def ws_unsubscribed(self, channel: str):
        """记录WebSocket取消订阅"""
        msg = self._format_message(self.TEMPLATES['websocket']['unsubscribed'], channel=channel)
        self.logger.debug(msg)
    
    # ========== 心跳相关日志 ==========
    
    def heartbeat_ping(self):
        """记录心跳ping（DEBUG级别）"""
        msg = self._format_message(self.TEMPLATES['heartbeat']['ping'])
        self.logger.debug(msg)
    
    def heartbeat_pong(self):
        """记录心跳pong（DEBUG级别）"""
        msg = self._format_message(self.TEMPLATES['heartbeat']['pong'])
        self.logger.debug(msg)
    
    def heartbeat_timeout(self):
        """记录心跳超时"""
        msg = self._format_message(self.TEMPLATES['heartbeat']['timeout'])
        self.logger.warning(msg)
    
    # ========== 错误相关日志 ==========
    
    def error_rate_limit(self, retry_after: Optional[int] = None):
        """记录API限流错误"""
        key = f"rate_limit_{self.exchange_id}"
        if self._should_log(key, logging.WARNING):
            msg = self._format_message(self.TEMPLATES['error']['rate_limit'])
            if retry_after:
                msg += f" (等待 {retry_after}s)"
            if self._log_counts[key] > 0:
                msg += f" (已重复 {self._log_counts[key]} 次)"
            self.logger.warning(msg)
    
    def error_network(self, error: str):
        """记录网络错误"""
        msg = self._format_message(self.TEMPLATES['error']['network'], error=str(error))
        self.logger.error(msg)
    
    def error_auth(self, error: str):
        """记录认证错误"""
        msg = self._format_message(self.TEMPLATES['error']['auth'], error=str(error))
        self.logger.error(msg)
    
    def error_server(self, code: int, error: str):
        """记录服务器错误"""
        msg = self._format_message(
            self.TEMPLATES['error']['server'],
            code=code,
            error=str(error)
        )
        self.logger.error(msg)
    
    def error_unknown(self, error: str):
        """记录未知错误"""
        msg = self._format_message(self.TEMPLATES['error']['unknown'], error=str(error))
        self.logger.error(msg)
    
    # ========== 数据相关日志 ==========
    
    def data_ticker(self, symbol: str, price: str):
        """记录价格更新（DEBUG级别）"""
        msg = self._format_message(
            self.TEMPLATES['data']['ticker'],
            symbol=symbol,
            price=price
        )
        self.logger.debug(msg)
    
    def data_orderbook(self, symbol: str, bids: int, asks: int):
        """记录订单簿更新（DEBUG级别）"""
        msg = self._format_message(
            self.TEMPLATES['data']['orderbook'],
            symbol=symbol,
            bids=bids,
            asks=asks
        )
        self.logger.debug(msg)
    
    def data_trade(self, symbol: str, side: str, amount: str, price: str):
        """记录成交（INFO级别）"""
        msg = self._format_message(
            self.TEMPLATES['data']['trade'],
            symbol=symbol,
            side=side,
            amount=amount,
            price=price
        )
        self.logger.info(msg)
    
    # ========== 通用日志方法 ==========
    
    def info(self, message: str, **kwargs):
        """记录INFO级别日志"""
        if kwargs:
            message = message.format(**kwargs)
        self.logger.info(f"[{self.exchange_id}] {message}")
    
    def warning(self, message: str, **kwargs):
        """记录WARNING级别日志"""
        if kwargs:
            message = message.format(**kwargs)
        self.logger.warning(f"[{self.exchange_id}] {message}")
    
    def error(self, message: str, **kwargs):
        """记录ERROR级别日志"""
        if kwargs:
            message = message.format(**kwargs)
        self.logger.error(f"[{self.exchange_id}] {message}")
    
    def debug(self, message: str, **kwargs):
        """记录DEBUG级别日志"""
        if kwargs:
            message = message.format(**kwargs)
        self.logger.debug(f"[{self.exchange_id}] {message}")
    
    def reset_aggregation(self):
        """重置日志聚合计数"""
        self._log_counts.clear()
        self._last_log_time.clear()

