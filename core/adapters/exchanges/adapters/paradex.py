"""
Paradex交易所适配器 - 主模块

整合Base、REST和WebSocket三个模块，提供完整的Paradex交易所接口实现
"""

import asyncio
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Optional, Any, Callable

from ..adapter import ExchangeAdapter
from ..interface import ExchangeConfig, ExchangeStatus
from ..models import (
    ExchangeType,
    OrderSide,
    OrderType,
    OrderData,
    PositionData,
    BalanceData,
    TickerData,
    OHLCVData,
    OrderBookData,
    TradeData,
    ExchangeInfo
)

from .paradex_base import ParadexBase
from .paradex_rest import ParadexRest
from .paradex_websocket import ParadexWebSocket


class ParadexAdapter(ExchangeAdapter):
    """
    Paradex交易所适配器
    
    基于模块化设计，支持：
    - 永续合约交易
    - 实时WebSocket数据流
    - JWT Token认证
    - 订单管理
    - 持仓管理
    """
    
    def __init__(self, config: ExchangeConfig, event_bus=None):
        """
        初始化Paradex适配器
        
        Args:
            config: 交易所配置
            event_bus: 事件总线（可选）
        """
        super().__init__(config, event_bus)
        
        # 初始化子模块
        self._base = ParadexBase(config)
        self._rest = ParadexRest(config, self.logger)
        
        # 将 SDK 客户端传递给 WebSocket（用于获取带交易权限的 JWT）
        paradex_client = getattr(self._rest, '_paradex_client', None)
        self._websocket = ParadexWebSocket(config, self.logger, paradex_client=paradex_client)
        
        # 🔥 共享持仓缓存（用于UI实时展示，与Lighter保持一致）
        shared_position_cache: Dict[str, Dict[str, Any]] = {}
        self._position_cache = shared_position_cache
        self._websocket._position_cache = shared_position_cache
        
        # 设置日志器
        self._base.set_logger(self.logger)
        
        # WebSocket回调
        self._ws_callbacks = {
            'ticker': [],
            'orderbook': [],
            'trades': [],
            'user_data': []
        }
        
        if self.logger:
            self.logger.info("Paradex适配器初始化完成")
            
    # ===== 生命周期管理 =====
    
    async def _do_connect(self) -> bool:
        """执行连接逻辑"""
        try:
            if self.logger:
                self.logger.info("开始连接Paradex交易所...")
                
            # 连接REST API
            if not await self._rest.connect():
                if self.logger:
                    self.logger.error("REST API连接失败")
                return False
                
            # 获取支持的交易对
            try:
                exchange_info = await self._rest.get_exchange_info()
                symbol_count = len(exchange_info.symbols)
                
                if self.logger:
                    self.logger.info(f"获取到 {symbol_count} 个Paradex交易对")
                    
            except Exception as e:
                if self.logger:
                    self.logger.warning(f"获取交易对失败: {e}")
                symbol_count = 0
                
            # 连接WebSocket
            if not await self._websocket.connect():
                if self.logger:
                    self.logger.error("WebSocket连接失败")
                return False
                
            if self.logger:
                self.logger.info(f"✅ Paradex交易所连接成功 (支持{symbol_count}个交易对)")
                
            return True
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"连接Paradex失败: {e}")
            return False
            
    async def _do_disconnect(self) -> None:
        """执行断开连接逻辑"""
        try:
            # 断开WebSocket
            await self._websocket.disconnect()
            
            # 断开REST
            await self._rest.disconnect()
            
            # 清理回调
            self._ws_callbacks = {
                'ticker': [],
                'orderbook': [],
                'trades': [],
                'user_data': []
            }
            
            if self.logger:
                self.logger.info("Paradex适配器已断开")
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"断开连接时出错: {e}")
                
    async def _do_authenticate(self) -> bool:
        """执行认证逻辑"""
        try:
            return await self._rest.authenticate()
        except Exception as e:
            if self.logger:
                self.logger.warning(f"Paradex认证失败: {e}")
            return True  # 即使认证失败，也允许使用公共API
            
    async def _do_health_check(self) -> Dict[str, Any]:
        """执行健康检查"""
        health_data = {
            'rest_connected': False,
            'websocket_connected': False,
            'market_count': 0,
            'subscriptions': 0
        }
        
        try:
            # 检查REST API
            markets = await self._rest.get_markets()
            health_data['rest_connected'] = True
            health_data['market_count'] = len(markets)
            
            # 检查WebSocket
            ws_status = self._websocket.get_connection_status()
            health_data['websocket_connected'] = ws_status['connected']
            health_data['subscriptions'] = ws_status['subscriptions']
            
            return health_data
            
        except Exception as e:
            health_data['error'] = str(e)
            return health_data
            
    async def _do_heartbeat(self) -> None:
        """执行心跳检测"""
        # Paradex的WebSocket有自动ping/pong机制
        # REST心跳通过获取市场列表
        await self._rest.get_markets()
        
    # ===== 市场数据接口 =====
    
    async def get_exchange_info(self) -> ExchangeInfo:
        """获取交易所信息"""
        return await self._rest.get_exchange_info()
        
    async def get_ticker(self, symbol: str) -> TickerData:
        """获取单个交易对行情"""
        return await self._rest.get_ticker(symbol)
        
    async def get_tickers(self, symbols: Optional[List[str]] = None) -> List[TickerData]:
        """获取多个交易对行情"""
        return await self._rest.get_tickers(symbols)
        
    async def get_orderbook(self, symbol: str, limit: Optional[int] = None) -> OrderBookData:
        """获取订单簿"""
        return await self._rest.get_orderbook(symbol, limit)
        
    async def get_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        since: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[OHLCVData]:
        """获取K线数据（Paradex暂不实现）"""
        raise NotImplementedError("Paradex暂不支持K线数据")
        
    async def get_trades(
        self,
        symbol: str,
        since: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[TradeData]:
        """获取最近成交记录（通过WebSocket实现）"""
        # Paradex没有REST成交记录接口，需要通过WebSocket
        raise NotImplementedError("请使用WebSocket订阅成交数据")
        
    # ===== 账户和交易接口 =====
    
    async def get_balances(self) -> List[BalanceData]:
        """获取账户余额"""
        return await self._rest.get_balances()
        
    async def get_positions(self, symbols: Optional[List[str]] = None) -> List[PositionData]:
        """获取持仓信息"""
        return await self._rest.get_positions(symbols)
        
    async def create_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        amount: Decimal,
        price: Optional[Decimal] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> OrderData:
        """创建订单"""
        order = await self._rest.create_order(symbol, side, order_type, amount, price, params)
        
        # 触发订单创建事件
        await self._handle_order_update(order)
        
        return order
        
    async def cancel_order(self, order_id: str, symbol: str) -> OrderData:
        """取消订单"""
        order = await self._rest.cancel_order(order_id, symbol)
        
        # 触发订单更新事件
        await self._handle_order_update(order)
        
        return order
        
    async def cancel_all_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        """取消所有订单"""
        orders = await self._rest.cancel_all_orders(symbol)
        
        # 触发订单更新事件
        for order in orders:
            await self._handle_order_update(order)
            
        return orders
        
    async def get_order(self, order_id: str, symbol: str) -> OrderData:
        """获取订单信息"""
        return await self._rest.get_order(order_id, symbol)
        
    async def get_open_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        """获取开放订单"""
        return await self._rest.get_open_orders(symbol)
        
    async def get_order_history(
        self,
        symbol: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[OrderData]:
        """获取历史订单"""
        # Paradex的get_open_orders只返回开放订单
        # 历史订单需要通过其他方式获取
        return await self._rest.get_open_orders(symbol)
        
    # ===== 交易设置接口 =====
    
    async def set_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        """设置杠杆倍数（Paradex账户层面配置，暂不实现）"""
        raise NotImplementedError("Paradex杠杆在账户层面配置")
        
    async def set_margin_mode(self, symbol: str, margin_mode: str) -> Dict[str, Any]:
        """设置保证金模式（Paradex账户层面配置，暂不实现）"""
        raise NotImplementedError("Paradex保证金模式在账户层面配置")
        
    # ===== 实时数据流接口 =====
    
    async def subscribe_ticker(self, symbol: str, callback: Callable) -> None:
        """订阅行情数据流"""
        try:
            wrapped_callback = self._wrap_ticker_callback(callback)
            self._ws_callbacks['ticker'].append((symbol, callback, wrapped_callback))
            
            await self._websocket.subscribe_ticker(symbol, wrapped_callback)
            
            if self.logger:
                self.logger.info(f"已订阅{symbol}行情数据")
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"订阅行情失败 {symbol}: {e}")
            raise
            
    async def subscribe_orderbook(self, symbol: str, callback: Callable) -> None:
        """订阅订单簿数据流"""
        try:
            wrapped_callback = self._wrap_orderbook_callback(callback)
            self._ws_callbacks['orderbook'].append((symbol, callback, wrapped_callback))
            
            await self._websocket.subscribe_orderbook(symbol, wrapped_callback)
            
            if self.logger:
                self.logger.info(f"已订阅{symbol}订单簿数据")
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"订阅订单簿失败 {symbol}: {e}")
            raise
            
    async def subscribe_trades(self, symbol: str, callback: Callable) -> None:
        """订阅成交数据流"""
        try:
            wrapped_callback = self._wrap_trades_callback(callback)
            self._ws_callbacks['trades'].append((symbol, callback, wrapped_callback))
            
            await self._websocket.subscribe_trades(symbol, wrapped_callback)
            
            if self.logger:
                self.logger.info(f"已订阅{symbol}成交数据")
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"订阅成交失败 {symbol}: {e}")
            raise
            
    async def subscribe_user_data(self, callback: Callable) -> None:
        """订阅用户数据流"""
        try:
            wrapped_callback = self._wrap_user_data_callback(callback)
            self._ws_callbacks['user_data'].append(('', callback, wrapped_callback))
            
            await self._websocket.subscribe_user_data(wrapped_callback)
            
            if self.logger:
                self.logger.info("已订阅用户数据")
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"订阅用户数据失败: {e}")
            raise
            
    async def unsubscribe_ticker(self, symbol: Optional[str] = None) -> None:
        """取消行情订阅"""
        try:
            targets = [symbol] if symbol else [s for s, _, _ in self._ws_callbacks['ticker']]
            for target in targets:
                self._ws_callbacks['ticker'] = [
                    (s, cb, wcb) for s, cb, wcb in self._ws_callbacks['ticker']
                    if s != target
                ]
                await self._websocket.unsubscribe_ticker(target)
            if self.logger and targets:
                self.logger.info(f"已取消{len(targets)}个行情订阅: {targets}")
        except Exception as e:
            if self.logger:
                self.logger.error(f"取消行情订阅失败: {e}")
            raise
            
    async def unsubscribe_orderbook(self, symbol: Optional[str] = None) -> None:
        """取消订单簿订阅"""
        try:
            targets = [symbol] if symbol else [s for s, _, _ in self._ws_callbacks['orderbook']]
            for target in targets:
                self._ws_callbacks['orderbook'] = [
                    (s, cb, wcb) for s, cb, wcb in self._ws_callbacks['orderbook']
                    if s != target
                ]
                await self._websocket.unsubscribe_orderbook(target)
            if self.logger and targets:
                self.logger.info(f"已取消{len(targets)}个订单簿订阅: {targets}")
        except Exception as e:
            if self.logger:
                self.logger.error(f"取消订单簿订阅失败: {e}")
            raise
            
    async def unsubscribe_trades(self, symbol: Optional[str] = None) -> None:
        """取消成交订阅"""
        try:
            targets = [symbol] if symbol else [s for s, _, _ in self._ws_callbacks['trades']]
            for target in targets:
                self._ws_callbacks['trades'] = [
                    (s, cb, wcb) for s, cb, wcb in self._ws_callbacks['trades']
                    if s != target
                ]
                await self._websocket.unsubscribe_trades(target)
            if self.logger and targets:
                self.logger.info(f"已取消{len(targets)}个成交订阅: {targets}")
        except Exception as e:
            if self.logger:
                self.logger.error(f"取消成交订阅失败: {e}")
            raise
            
    async def unsubscribe_user_data(self) -> None:
        """取消用户数据订阅"""
        try:
            self._ws_callbacks['user_data'].clear()
            await self._websocket.unsubscribe_user_data()
            if self.logger:
                self.logger.info("已取消用户数据订阅")
        except Exception as e:
            if self.logger:
                self.logger.error(f"取消用户数据订阅失败: {e}")
            raise
            
    async def unsubscribe(self, symbol: Optional[str] = None) -> None:
        """取消订阅"""
        if symbol:
            await asyncio.gather(
                self.unsubscribe_ticker(symbol),
                self.unsubscribe_orderbook(symbol),
                self.unsubscribe_trades(symbol)
            )
            if self.logger:
                self.logger.info(f"已取消{symbol}的所有订阅")
        else:
            await asyncio.gather(
                self.unsubscribe_ticker(),
                self.unsubscribe_orderbook(),
                self.unsubscribe_trades(),
                self.unsubscribe_user_data()
            )
            if self.logger:
                self.logger.info("已取消所有订阅")
                
    # ===== 批量订阅接口 =====
    
    async def batch_subscribe_tickers(
        self,
        symbols: Optional[List[str]] = None,
        callback: Optional[Callable[[str, TickerData], None]] = None
    ) -> None:
        """批量订阅ticker数据"""
        try:
            # 获取交易对列表
            if symbols is None:
                exchange_info = await self.get_exchange_info()
                symbols = exchange_info.symbols
                
            if not symbols:
                if self.logger:
                    self.logger.warning("没有找到要订阅的交易对")
                return
                
            # 包装回调
            if callback:
                wrapped_callback = self._wrap_batch_ticker_callback(callback)
                for symbol in symbols:
                    self._ws_callbacks['ticker'].append((symbol, callback, wrapped_callback))
                    
                # 批量订阅
                await self._websocket.batch_subscribe_tickers(symbols, wrapped_callback)
                
            if self.logger:
                self.logger.info(f"✅ 批量订阅ticker完成: {len(symbols)}个交易对")
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"批量订阅ticker失败: {e}")
            raise
            
    async def batch_subscribe_orderbooks(
        self,
        symbols: Optional[List[str]] = None,
        callback: Optional[Callable] = None
    ) -> None:
        """批量订阅订单簿数据"""
        try:
            # 获取交易对列表
            if symbols is None:
                exchange_info = await self.get_exchange_info()
                symbols = exchange_info.symbols
                
            if not symbols:
                if self.logger:
                    self.logger.warning("没有找到要订阅的交易对")
                return
                
            # 包装回调
            if callback:
                wrapped_callback = self._wrap_orderbook_callback(callback)
                for symbol in symbols:
                    self._ws_callbacks['orderbook'].append((symbol, callback, wrapped_callback))
                    
                # 批量订阅
                await self._websocket.batch_subscribe_orderbooks(symbols, wrapped_callback)
                
            if self.logger:
                self.logger.info(f"✅ 批量订阅orderbook完成: {len(symbols)}个交易对")
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"批量订阅orderbook失败: {e}")
            raise
            
    # ===== 回调函数包装器 =====
    
    def _wrap_ticker_callback(self, original_callback: Callable) -> Callable:
        """包装ticker回调函数"""
        async def wrapped_callback(symbol: str, ticker_data: TickerData):
            try:
                import inspect
                param_count = len(inspect.signature(original_callback).parameters)
                
                if asyncio.iscoroutinefunction(original_callback):
                    if param_count >= 2:
                        await original_callback(symbol, ticker_data)
                    else:
                        await original_callback(ticker_data)
                else:
                    if param_count >= 2:
                        original_callback(symbol, ticker_data)
                    else:
                        original_callback(ticker_data)
                    
                await self._handle_ticker_update(ticker_data)
                
            except Exception as e:
                if self.logger:
                    self.logger.warning(f"Ticker回调执行失败: {e}")
                    
        return wrapped_callback
        
    def _wrap_batch_ticker_callback(self, original_callback: Callable[[str, TickerData], None]) -> Callable:
        """包装批量ticker回调函数"""
        async def wrapped_callback(symbol: str, ticker_data: TickerData):
            try:
                import inspect
                param_count = len(inspect.signature(original_callback).parameters)
                
                if asyncio.iscoroutinefunction(original_callback):
                    if param_count >= 2:
                        await original_callback(symbol, ticker_data)
                    else:
                        await original_callback(ticker_data)
                else:
                    if param_count >= 2:
                        original_callback(symbol, ticker_data)
                    else:
                        original_callback(ticker_data)
                    
                await self._handle_ticker_update(ticker_data)
                
            except Exception as e:
                if self.logger:
                    self.logger.warning(f"Ticker回调执行失败 {symbol}: {e}")
                    
        return wrapped_callback
        
    def _wrap_orderbook_callback(self, original_callback: Callable) -> Callable:
        """包装orderbook回调函数"""
        async def wrapped_callback(symbol: str, orderbook_data: OrderBookData):
            try:
                import inspect
                sig = inspect.signature(original_callback)
                param_count = len(sig.parameters)
                
                if asyncio.iscoroutinefunction(original_callback):
                    if param_count == 2:
                        await original_callback(symbol, orderbook_data)
                    else:
                        await original_callback(orderbook_data)
                else:
                    if param_count == 2:
                        original_callback(symbol, orderbook_data)
                    else:
                        original_callback(orderbook_data)
                        
                await self._handle_orderbook_update(orderbook_data)
                
            except Exception as e:
                if self.logger:
                    self.logger.warning(f"OrderBook回调执行失败: {e}")
                    
        return wrapped_callback
        
    def _wrap_trades_callback(self, original_callback: Callable) -> Callable:
        """包装trades回调函数"""
        async def wrapped_callback(symbol: str, trade_data: TradeData):
            try:
                import inspect
                sig = inspect.signature(original_callback)
                param_count = len(sig.parameters)
                
                if asyncio.iscoroutinefunction(original_callback):
                    if param_count == 2:
                        await original_callback(symbol, trade_data)
                    else:
                        await original_callback(trade_data)
                else:
                    if param_count == 2:
                        original_callback(symbol, trade_data)
                    else:
                        original_callback(trade_data)
                        
                await self._handle_trade_update(trade_data)
                
            except Exception as e:
                if self.logger:
                    self.logger.warning(f"Trade回调执行失败: {e}")
                    
        return wrapped_callback
        
    def _wrap_user_data_callback(self, original_callback: Callable) -> Callable:
        """包装user data回调函数"""
        async def wrapped_callback(symbol: str, user_data: Dict[str, Any]):
            try:
                import inspect
                sig = inspect.signature(original_callback)
                param_count = len(sig.parameters)
                
                if asyncio.iscoroutinefunction(original_callback):
                    if param_count == 2:
                        await original_callback(symbol, user_data)
                    else:
                        await original_callback(user_data)
                else:
                    if param_count == 2:
                        original_callback(symbol, user_data)
                    else:
                        original_callback(user_data)
                        
                await self._handle_user_data_update(user_data)
                
            except Exception as e:
                if self.logger:
                    self.logger.warning(f"UserData回调执行失败: {e}")
                    
        return wrapped_callback
        
    # ===== 便利方法 =====
    
    async def get_supported_symbols(self) -> List[str]:
        """获取支持的交易对列表"""
        try:
            exchange_info = await self.get_exchange_info()
            return exchange_info.symbols
        except Exception as e:
            if self.logger:
                self.logger.error(f"获取交易对失败: {e}")
            return []
            
    def get_connection_status(self) -> Dict[str, Any]:
        """获取连接状态"""
        return {
            'rest_connected': self._rest.session is not None,
            'websocket_status': self._websocket.get_connection_status(),
            'total_subscriptions': sum(len(callbacks) for callbacks in self._ws_callbacks.values())
        }
        
    def map_symbol(self, symbol: str) -> str:
        """映射交易对符号"""
        return self._base.map_symbol(symbol)
        
    def reverse_map_symbol(self, exchange_symbol: str) -> str:
        """反向映射交易对符号"""
        return self._base.reverse_map_symbol(exchange_symbol)

