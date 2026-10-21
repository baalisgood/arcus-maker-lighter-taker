"""
Binance交易所WebSocket模块 - 重构版

包含Binance交易所的WebSocket连接和数据流处理
支持行情数据、订单簿、成交数据和用户数据流
"""

import asyncio
import json
import websockets
import time
import ssl
from datetime import datetime
from typing import Dict, List, Optional, Any, Callable, Set
from decimal import Decimal
from enum import Enum

from .binance_base import BinanceBase
from ..models import (
    TickerData, OrderBookData, TradeData, BalanceData, OrderData,
    OrderBookLevel, OrderSide
)


class BinanceStreamType(Enum):
    """Binance数据流类型"""
    TICKER = "ticker"
    ORDERBOOK = "depth"
    TRADES = "trade"
    KLINE = "kline"
    USER_DATA = "userData"


class BinanceWebSocket(BinanceBase):
    """Binance WebSocket连接和数据流处理"""
    
    def __init__(self, config, logger=None):
        super().__init__(config)
        self.logger = logger
        
        # WebSocket连接（现货 + 永续）- 使用直接流，每个交易对一个连接
        self._spot_websockets = {}       # symbol -> websocket (现货，每个交易对一个连接)
        self._futures_websocket = None   # 期货/永续合约 WebSocket
        self._user_websocket = None
        self._connected = False
        self._futures_connected = False
        self._user_connected = False
        
        # 订阅管理
        self._subscriptions = {}  # stream_name -> callback (现货)
        self._futures_subscriptions = {}  # stream_name -> callback (永续)
        self._user_subscriptions = {}  # event_type -> callback
        self._stream_id_counter = 1
        self._futures_stream_id_counter = 1
        
        # 重连配置
        self.reconnect_interval = 5
        self.max_reconnect_attempts = 10
        self._reconnect_attempts = 0
        self._futures_reconnect_attempts = 0
        
        # 心跳配置
        self.heartbeat_interval = 30
        self._last_heartbeat = 0
        
        # 用户数据流配置
        self.listen_key = None
        self.listen_key_interval = 1800  # 30分钟续期
        self._last_listen_key_update = 0
        
        # 数据缓存
        self._ticker_cache = {}
        self._orderbook_cache = {}
        
        # 事件循环任务
        self._heartbeat_task = None
        self._listen_key_task = None
        
    async def initialize(self) -> bool:
        """初始化WebSocket连接"""
        try:
            if self.logger:
                self.logger.info("🚀 初始化Binance WebSocket连接...")
            
            # 创建心跳任务
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
            
            if self.logger:
                self.logger.info("✅ Binance WebSocket初始化成功")
            return True
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ Binance WebSocket初始化失败: {str(e)}")
            return False
    
    async def close(self):
        """关闭WebSocket连接"""
        try:
            # 取消心跳任务
            if self._heartbeat_task:
                self._heartbeat_task.cancel()
                try:
                    await self._heartbeat_task
                except asyncio.CancelledError:
                    pass
            
            # 取消listen key任务
            if self._listen_key_task:
                self._listen_key_task.cancel()
                try:
                    await self._listen_key_task
                except asyncio.CancelledError:
                    pass
            
            # 关闭WebSocket连接
            if self._websocket:
                await self._websocket.close()
                self._websocket = None
                
            if self._user_websocket:
                await self._user_websocket.close()
                self._user_websocket = None
            
            self._connected = False
            self._user_connected = False
            
            if self.logger:
                self.logger.info("✅ Binance WebSocket连接已关闭")
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 关闭WebSocket连接失败: {str(e)}")
    
    async def _connect_market_stream(self) -> bool:
        """连接现货市场数据流"""
        try:
            if self._websocket and not self._websocket.closed:
                return True
            
            # 使用现货 WebSocket URL
            spot_ws_url = self.DEFAULT_SPOT_WS_URL
            
            if self.logger:
                self.logger.info(f"📡 连接Binance现货数据流: {spot_ws_url}")
            
            # 🔥 创建SSL上下文（禁用证书验证以兼容Python 3.13）
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            
            self._websocket = await websockets.connect(spot_ws_url, ssl=ssl_context)
            self._connected = True
            self._reconnect_attempts = 0
            
            # 启动消息处理任务
            asyncio.create_task(self._handle_market_messages())
            
            if self.logger:
                self.logger.info("✅ Binance现货数据流连接成功")
            
            return True
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 连接现货数据流失败: {str(e)}")
            return False
    
    async def _connect_futures_stream(self) -> bool:
        """连接期货/永续合约市场数据流"""
        try:
            if self._futures_websocket and not self._futures_websocket.closed:
                return True
            
            # 使用期货 WebSocket URL
            futures_ws_url = self.DEFAULT_WS_URL
            
            if self.logger:
                self.logger.info(f"📡 连接Binance期货数据流: {futures_ws_url}")
            
            # 🔥 创建SSL上下文（禁用证书验证以兼容Python 3.13）
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            
            self._futures_websocket = await websockets.connect(futures_ws_url, ssl=ssl_context)
            self._futures_connected = True
            self._futures_reconnect_attempts = 0
            
            # 启动消息处理任务
            asyncio.create_task(self._handle_futures_messages())
            
            if self.logger:
                self.logger.info("✅ Binance期货数据流连接成功")
            
            return True
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 连接期货数据流失败: {str(e)}")
            return False
    
    async def _connect_user_stream(self) -> bool:
        """连接用户数据流"""
        try:
            if not self.config or not getattr(self.config, 'api_key'):
                if self.logger:
                    self.logger.warning("⚠️ 未配置API密钥，跳过用户数据流连接")
                return False
            
            # 获取listen key
            if not await self._get_listen_key():
                return False
            
            # 构建用户数据流URL
            ws_url = f"{self.ws_url}/ws/{self.listen_key}"
            
            if self.logger:
                self.logger.info(f"📡 连接Binance用户数据流: {ws_url}")
            
            self._user_websocket = await websockets.connect(ws_url)
            self._user_connected = True
            
            # 启动消息处理任务
            asyncio.create_task(self._handle_user_messages())
            
            # 启动listen key续期任务
            self._listen_key_task = asyncio.create_task(self._listen_key_loop())
            
            if self.logger:
                self.logger.info("✅ Binance用户数据流连接成功")
            
            return True
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 连接用户数据流失败: {str(e)}")
            return False
    
    async def _get_listen_key(self) -> bool:
        """获取用户数据流listen key"""
        try:
            # 这里需要调用REST API获取listen key
            # 简化实现，实际应该通过REST API获取
            # POST /fapi/v1/listenKey
            
            if self.logger:
                self.logger.warning("⚠️ Listen key获取需要实现REST API调用")
            
            # 临时使用假的listen key进行测试
            self.listen_key = "fake_listen_key_for_testing"
            self._last_listen_key_update = time.time()
            
            return True
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 获取listen key失败: {str(e)}")
            return False
    
    async def _handle_spot_stream_messages(self, stream_name: str, ws):
        """处理现货直接流消息（每个交易对独立连接）
        
        Args:
            stream_name: 流名称，如 'btcusdt@ticker'
            ws: WebSocket连接
        """
        try:
            async for message in ws:
                try:
                    data = json.loads(message)
                    # 直接流返回的数据格式: {'e': '24hrTicker', 's': 'BTCUSDT', 'c': '...', ...}
                    if 'e' in data and 's' in data:
                        # 调用回调
                        if stream_name in self._subscriptions:
                            callback = self._subscriptions[stream_name]
                            # 构建 TickerData
                            ticker = TickerData(
                                symbol=data.get('s', ''),
                                bid=self._safe_decimal(data.get('b')),
                                ask=self._safe_decimal(data.get('a')),
                                last=self._safe_decimal(data.get('c')),
                                open=self._safe_decimal(data.get('o')),
                                high=self._safe_decimal(data.get('h')),
                                low=self._safe_decimal(data.get('l')),
                                close=self._safe_decimal(data.get('c')),
                                volume=self._safe_decimal(data.get('v')),
                                quote_volume=self._safe_decimal(data.get('q')),
                                change=self._safe_decimal(data.get('P')),
                                percentage=self._safe_decimal(data.get('P')),
                                timestamp=datetime.fromtimestamp(data.get('E', 0) / 1000),
                                raw_data=data
                            )
                            await self._safe_callback(callback, ticker)
                except json.JSONDecodeError:
                    if self.logger:
                        self.logger.warning(f"⚠️ 无法解析现货WebSocket消息: {message}")
                except Exception as e:
                    if self.logger:
                        self.logger.error(f"❌ 处理现货消息失败 {stream_name}: {str(e)}")
                        
        except websockets.exceptions.ConnectionClosed:
            if self.logger:
                self.logger.warning(f"⚠️ 现货流 {stream_name} 断开")
            # 可以在这里添加重连逻辑
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 现货流 {stream_name} 处理异常: {str(e)}")
    
    async def _handle_futures_messages(self):
        """处理期货/永续合约市场数据消息"""
        try:
            async for message in self._futures_websocket:
                try:
                    data = json.loads(message)
                    await self._process_market_message(data, is_futures=True)
                except json.JSONDecodeError:
                    if self.logger:
                        self.logger.warning(f"⚠️ 无法解析期货WebSocket消息: {message}")
                except Exception as e:
                    if self.logger:
                        self.logger.error(f"❌ 处理期货消息失败: {str(e)}")
                        
        except websockets.exceptions.ConnectionClosed:
            if self.logger:
                self.logger.warning("⚠️ 期货数据流连接断开，尝试重连")
            self._futures_connected = False
            await self._reconnect_futures_stream()
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 期货消息处理异常: {str(e)}")
    
    async def _handle_user_messages(self):
        """处理用户数据消息"""
        try:
            async for message in self._user_websocket:
                try:
                    data = json.loads(message)
                    await self._process_user_message(data)
                except json.JSONDecodeError:
                    if self.logger:
                        self.logger.warning(f"⚠️ 无法解析用户数据消息: {message}")
                except Exception as e:
                    if self.logger:
                        self.logger.error(f"❌ 处理用户消息失败: {str(e)}")
                        
        except websockets.exceptions.ConnectionClosed:
            if self.logger:
                self.logger.warning("⚠️ 用户数据流连接断开，尝试重连")
            self._user_connected = False
            await self._reconnect_user_stream()
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 用户消息处理异常: {str(e)}")
    
    async def _process_market_message(self, data: Dict[str, Any], is_futures: bool = False):
        """处理市场数据消息
        
        Args:
            data: WebSocket消息数据
            is_futures: 是否为期货/永续合约数据
        """
        try:
            # 处理订阅响应消息
            if 'result' in data or ('id' in data and 'error' not in data):
                if self.logger:
                    market_type = "期货" if is_futures else "现货"
                    self.logger.info(f"📨 收到{market_type}订阅响应: {data}")
                return
            
            # 处理错误消息
            if 'error' in data:
                if self.logger:
                    market_type = "期货" if is_futures else "现货"
                    self.logger.error(f"❌ {market_type}订阅错误: {data['error']}")
                return
            
            # 检查消息类型 - 支持两种格式
            if 'stream' in data and 'data' in data:
                # 格式1: Combined stream格式 {'stream': 'btcusdt@ticker', 'data': {...}}
                stream_name = data['stream']
                message_data = data['data']
            elif 'e' in data and 's' in data:
                # 格式2: 直接ticker格式 {'e': '24hrTicker', 's': 'BTCUSDT', ...}
                # 构造stream_name用于回调查找
                symbol = data.get('s', '').lower()
                stream_name = f"{symbol}@ticker"
                message_data = data
            else:
                if self.logger:
                    self.logger.warning(f"⚠️ 未知消息格式: {list(data.keys())}")
                return
            
            # 根据流类型处理数据
            if '@ticker' in stream_name:
                await self._handle_ticker_message(stream_name, message_data, is_futures)
            elif '@depth' in stream_name:
                await self._handle_orderbook_message(stream_name, message_data, is_futures)
            elif '@trade' in stream_name:
                await self._handle_trade_message(stream_name, message_data, is_futures)
            elif '@kline' in stream_name:
                await self._handle_kline_message(stream_name, message_data, is_futures)
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 处理市场消息失败: {str(e)}")
    
    async def _process_user_message(self, data: Dict[str, Any]):
        """处理用户数据消息"""
        try:
            event_type = data.get('e')
            
            if event_type == 'ACCOUNT_UPDATE':
                await self._handle_balance_update(data)
            elif event_type == 'ORDER_TRADE_UPDATE':
                await self._handle_order_update(data)
            elif event_type == 'ACCOUNT_CONFIG_UPDATE':
                await self._handle_account_config_update(data)
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 处理用户消息失败: {str(e)}")
    
    async def _handle_ticker_message(self, stream_name: str, data: Dict[str, Any], is_futures: bool = False):
        """处理行情数据
        
        Args:
            stream_name: 流名称
            data: ticker数据
            is_futures: 是否为期货/永续合约数据
        """
        try:
            symbol = data.get('s', '').lower()
            if not symbol:
                return
            
            # 转换为标准格式
            symbol = self.map_symbol_from_binance(symbol)
            
            ticker = TickerData(
                symbol=symbol,
                bid=self._safe_decimal(data.get('b')),
                ask=self._safe_decimal(data.get('a')),
                last=self._safe_decimal(data.get('c')),
                open=self._safe_decimal(data.get('o')),
                high=self._safe_decimal(data.get('h')),
                low=self._safe_decimal(data.get('l')),
                close=self._safe_decimal(data.get('c')),
                volume=self._safe_decimal(data.get('v')),
                quote_volume=self._safe_decimal(data.get('q')),
                change=self._safe_decimal(data.get('P')),
                percentage=self._safe_decimal(data.get('P')),
                timestamp=datetime.fromtimestamp(data.get('E', 0) / 1000),
                raw_data=data
            )
            
            # 缓存数据
            self._ticker_cache[symbol] = ticker
            
            # 根据市场类型选择订阅字典
            subscriptions = self._futures_subscriptions if is_futures else self._subscriptions
            
            # 调用回调函数
            if stream_name in subscriptions:
                callback = subscriptions[stream_name]
                await self._safe_callback(callback, ticker)
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 处理行情数据失败: {str(e)}")
    
    async def _handle_orderbook_message(self, stream_name: str, data: Dict[str, Any]):
        """处理订单簿数据"""
        try:
            symbol = data.get('s', '').lower()
            if not symbol:
                return
            
            # 转换为标准格式
            symbol = self.map_symbol_from_binance(symbol)
            
            # 解析买卖盘
            bids = [
                OrderBookLevel(
                    price=self._safe_decimal(bid[0]),
                    size=self._safe_decimal(bid[1])
                )
                for bid in data.get('b', [])
            ]
            
            asks = [
                OrderBookLevel(
                    price=self._safe_decimal(ask[0]),
                    size=self._safe_decimal(ask[1])
                )
                for ask in data.get('a', [])
            ]
            
            orderbook = OrderBookData(
                symbol=symbol,
                bids=bids,
                asks=asks,
                timestamp=datetime.fromtimestamp(data.get('E', 0) / 1000),
                nonce=data.get('u'),
                raw_data=data
            )
            
            # 缓存数据
            self._orderbook_cache[symbol] = orderbook
            
            # 调用回调函数
            if stream_name in self._subscriptions:
                callback = self._subscriptions[stream_name]
                await self._safe_callback(callback, orderbook)
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 处理订单簿数据失败: {str(e)}")
    
    async def _handle_trade_message(self, stream_name: str, data: Dict[str, Any]):
        """处理成交数据"""
        try:
            symbol = data.get('s', '').lower()
            if not symbol:
                return
            
            # 转换为标准格式
            symbol = self.map_symbol_from_binance(symbol)
            
            trade = TradeData(
                id=str(data.get('t', '')),
                symbol=symbol,
                side=OrderSide.BUY if data.get('m') == False else OrderSide.SELL,
                amount=self._safe_decimal(data.get('q')),
                price=self._safe_decimal(data.get('p')),
                cost=self._safe_decimal(float(data.get('p', 0)) * float(data.get('q', 0))),
                fee=None,
                timestamp=datetime.fromtimestamp(data.get('T', 0) / 1000),
                order_id=None,
                raw_data=data
            )
            
            # 调用回调函数
            if stream_name in self._subscriptions:
                callback = self._subscriptions[stream_name]
                await self._safe_callback(callback, trade)
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 处理成交数据失败: {str(e)}")
    
    async def _handle_kline_message(self, stream_name: str, data: Dict[str, Any]):
        """处理K线数据"""
        try:
            # K线数据处理逻辑
            kline_data = data.get('k', {})
            if not kline_data:
                return
            
            # 这里可以根据需要实现K线数据处理
            # 暂时跳过
            pass
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 处理K线数据失败: {str(e)}")
    
    async def _handle_balance_update(self, data: Dict[str, Any]):
        """处理余额更新"""
        try:
            # 处理账户余额更新
            account_data = data.get('a', {})
            balances = account_data.get('B', [])
            
            # 调用用户数据回调
            if 'balance' in self._user_subscriptions:
                callback = self._user_subscriptions['balance']
                await self._safe_callback(callback, {'type': 'balance', 'data': balances})
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 处理余额更新失败: {str(e)}")
    
    async def _handle_order_update(self, data: Dict[str, Any]):
        """处理订单更新"""
        try:
            # 处理订单更新
            order_data = data.get('o', {})
            
            # 调用用户数据回调
            if 'order' in self._user_subscriptions:
                callback = self._user_subscriptions['order']
                await self._safe_callback(callback, {'type': 'order', 'data': order_data})
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 处理订单更新失败: {str(e)}")
    
    async def _handle_account_config_update(self, data: Dict[str, Any]):
        """处理账户配置更新"""
        try:
            # 处理账户配置更新
            config_data = data.get('ac', {})
            
            # 调用用户数据回调
            if 'config' in self._user_subscriptions:
                callback = self._user_subscriptions['config']
                await self._safe_callback(callback, {'type': 'config', 'data': config_data})
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 处理账户配置更新失败: {str(e)}")
    
    async def _safe_callback(self, callback: Callable, data: Any):
        """安全调用回调函数"""
        try:
            if asyncio.iscoroutinefunction(callback):
                await callback(data)
            else:
                callback(data)
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 回调函数执行失败: {str(e)}")
    
    async def _heartbeat_loop(self):
        """心跳循环"""
        try:
            while True:
                await asyncio.sleep(self.heartbeat_interval)
                
                # 检查连接状态
                if self._connected and self._websocket:
                    try:
                        await self._websocket.ping()
                        self._last_heartbeat = time.time()
                    except Exception:
                        self._connected = False
                        await self._reconnect_market_stream()
                
                if self._user_connected and self._user_websocket:
                    try:
                        await self._user_websocket.ping()
                    except Exception:
                        self._user_connected = False
                        await self._reconnect_user_stream()
                        
        except asyncio.CancelledError:
            pass
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 心跳循环异常: {str(e)}")
    
    async def _listen_key_loop(self):
        """Listen key续期循环"""
        try:
            while self._user_connected:
                await asyncio.sleep(self.listen_key_interval)
                
                # 续期listen key
                if time.time() - self._last_listen_key_update > self.listen_key_interval:
                    await self._renew_listen_key()
                    
        except asyncio.CancelledError:
            pass
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ Listen key续期循环异常: {str(e)}")
    
    async def _renew_listen_key(self):
        """续期listen key"""
        try:
            # 这里需要调用REST API续期listen key
            # PUT /fapi/v1/listenKey
            
            if self.logger:
                self.logger.info("🔄 续期listen key")
                
            self._last_listen_key_update = time.time()
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 续期listen key失败: {str(e)}")
    
    async def _reconnect_market_stream(self):
        """重连现货市场数据流"""
        if self._reconnect_attempts >= self.max_reconnect_attempts:
            if self.logger:
                self.logger.error(f"❌ 现货数据流重连次数超限: {self._reconnect_attempts}")
            return
        
        self._reconnect_attempts += 1
        
        if self.logger:
            self.logger.info(f"🔄 重连现货数据流 (尝试 {self._reconnect_attempts}/{self.max_reconnect_attempts})")
        
        await asyncio.sleep(self.reconnect_interval)
        
        # 重新连接
        success = await self._connect_market_stream()
        
        if success and self._subscriptions:
            # 重连成功后，重新订阅所有交易对
            if self.logger:
                self.logger.info(f"🔄 重新订阅 {len(self._subscriptions)} 个现货交易对...")
            
            # 保存原有订阅
            old_subscriptions = dict(self._subscriptions)
            self._subscriptions.clear()
            
            # 重新发送订阅请求
            for stream_name, callback in old_subscriptions.items():
                try:
                    # 从stream_name提取symbol (例如: btcusdt@ticker -> btc/usdt)
                    symbol_lower = stream_name.split('@')[0]
                    # 简单转换为标准格式 (这里需要改进)
                    
                    # 重新注册订阅
                    self._subscriptions[stream_name] = callback
                    
                    # 发送订阅消息
                    subscribe_msg = {
                        "method": "SUBSCRIBE",
                        "params": [stream_name],
                        "id": self._stream_id_counter
                    }
                    self._stream_id_counter += 1
                    
                    if self._websocket:
                        await self._websocket.send(json.dumps(subscribe_msg))
                        if self.logger:
                            self.logger.info(f"📤 重新订阅: {stream_name}")
                except Exception as e:
                    if self.logger:
                        self.logger.error(f"❌ 重新订阅失败 {stream_name}: {e}")
    
    async def _reconnect_user_stream(self):
        """重连用户数据流"""
        if self.logger:
            self.logger.info("🔄 重连用户数据流")
        
        await asyncio.sleep(self.reconnect_interval)
        await self._connect_user_stream()
    
    async def _reconnect_futures_stream(self):
        """重新连接期货数据流"""
        if self._futures_reconnect_attempts >= self.max_reconnect_attempts:
            if self.logger:
                self.logger.error("❌ 期货数据流达到最大重连次数，停止重连")
            return
        
        self._futures_reconnect_attempts += 1
        if self.logger:
            self.logger.info(f"⏳ 尝试重连期货数据流 ({self._futures_reconnect_attempts}/{self.max_reconnect_attempts})")
        
        await asyncio.sleep(self.reconnect_interval)
        await self._connect_futures_stream()
    
    # ==================== 公共接口 ====================
    
    async def subscribe_ticker(self, symbol: str, callback: Callable[[TickerData], None]):
        """订阅行情数据（支持现货和永续合约）
        
        Args:
            symbol: 交易对符号，如 "BTC/USDT" (现货) 或 "HYPE/USDT:USDT" (永续)
            callback: 回调函数
        """
        try:
            # 判断是永续合约还是现货
            is_futures = ':' in symbol
            
            if self.logger:
                market_type = "永续合约" if is_futures else "现货"
                self.logger.info(f"🔍 开始订阅{market_type}: {symbol}")
            
            if is_futures:
                # 永续合约：确保期货WebSocket连接
                if not self._futures_connected:
                    await self._connect_futures_stream()
                
                # 移除保证金币种后缀，构建流名称
                # HYPE/USDT:USDT -> HYPEUSDT -> hypeusdt@ticker
                clean_symbol = symbol.split(':')[0]  # HYPE/USDT
                binance_symbol = clean_symbol.replace('/', '').lower()
                stream_name = f"{binance_symbol}@ticker"
                
                # 注册到期货订阅
                self._futures_subscriptions[stream_name] = callback
                
                # 发送订阅消息到期货WebSocket
                subscribe_msg = {
                    "method": "SUBSCRIBE",
                    "params": [stream_name],
                    "id": self._futures_stream_id_counter
                }
                self._futures_stream_id_counter += 1
                
                if self._futures_websocket:
                    await self._futures_websocket.send(json.dumps(subscribe_msg))
                    if self.logger:
                        self.logger.info(f"📤 已发送期货订阅消息: {subscribe_msg}")
            else:
                # 现货：使用直接流，每个交易对一个独立的 WebSocket 连接
                binance_symbol = self.map_symbol_to_binance(symbol).replace('/', '').lower()
                stream_name = f"{binance_symbol}@ticker"
                
                # 🔥 使用直接流URL（不需要发送SUBSCRIBE消息）
                direct_stream_url = f"wss://stream.binance.com:9443/ws/{stream_name}"
                
                if self.logger:
                    self.logger.info(f"📡 连接现货直接流: {direct_stream_url}")
                
                # 创建SSL上下文
                ssl_context = ssl.create_default_context()
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
                
                # 为这个交易对创建独立的 WebSocket 连接
                ws = await websockets.connect(direct_stream_url, ssl=ssl_context)
                self._spot_websockets[stream_name] = ws
                
                # 注册回调
                self._subscriptions[stream_name] = callback
                
                # 启动消息处理任务
                asyncio.create_task(self._handle_spot_stream_messages(stream_name, ws))
                
                if self.logger:
                    self.logger.info(f"✅ 现货直接流连接成功: {symbol}")
            
            if self.logger:
                market_type = "永续合约" if is_futures else "现货"
                self.logger.info(f"📈 订阅{market_type}行情数据: {symbol}")
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 订阅行情失败 {symbol}: {str(e)}")
            raise
    
    async def subscribe_orderbook(self, symbol: str, callback: Callable[[OrderBookData], None]):
        """订阅订单簿数据"""
        try:
            # 确保连接
            if not self._connected:
                await self._connect_market_stream()
            
            # 构建流名称
            binance_symbol = self.map_symbol_to_binance(symbol).lower()
            stream_name = f"{binance_symbol}@depth@100ms"
            
            # 注册回调
            self._subscriptions[stream_name] = callback
            
            # 发送订阅消息
            subscribe_msg = {
                "method": "SUBSCRIBE",
                "params": [stream_name],
                "id": self._stream_id_counter
            }
            self._stream_id_counter += 1
            
            if self._websocket:
                await self._websocket.send(json.dumps(subscribe_msg))
            
            if self.logger:
                self.logger.info(f"📊 订阅订单簿数据: {symbol}")
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 订阅订单簿失败 {symbol}: {str(e)}")
            raise
    
    async def subscribe_trades(self, symbol: str, callback: Callable[[TradeData], None]):
        """订阅成交数据"""
        try:
            # 确保连接
            if not self._connected:
                await self._connect_market_stream()
            
            # 构建流名称
            binance_symbol = self.map_symbol_to_binance(symbol).lower()
            stream_name = f"{binance_symbol}@trade"
            
            # 注册回调
            self._subscriptions[stream_name] = callback
            
            # 发送订阅消息
            subscribe_msg = {
                "method": "SUBSCRIBE",
                "params": [stream_name],
                "id": self._stream_id_counter
            }
            self._stream_id_counter += 1
            
            if self._websocket:
                await self._websocket.send(json.dumps(subscribe_msg))
            
            if self.logger:
                self.logger.info(f"💱 订阅成交数据: {symbol}")
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 订阅成交失败 {symbol}: {str(e)}")
            raise
    
    async def subscribe_user_data(self, callback: Callable[[Dict[str, Any]], None]):
        """订阅用户数据"""
        try:
            # 确保连接
            if not self._user_connected:
                await self._connect_user_stream()
            
            # 注册回调
            self._user_subscriptions['balance'] = callback
            self._user_subscriptions['order'] = callback
            self._user_subscriptions['config'] = callback
            
            if self.logger:
                self.logger.info("👤 订阅用户数据流")
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 订阅用户数据失败: {str(e)}")
            raise
    
    async def unsubscribe(self, symbol: Optional[str] = None):
        """取消订阅"""
        try:
            if symbol:
                # 取消指定符号的订阅
                binance_symbol = self.map_symbol_to_binance(symbol).lower()
                streams_to_remove = [
                    stream for stream in self._subscriptions.keys()
                    if stream.startswith(binance_symbol)
                ]
                
                for stream in streams_to_remove:
                    del self._subscriptions[stream]
                    
                    # 发送取消订阅消息
                    unsubscribe_msg = {
                        "method": "UNSUBSCRIBE",
                        "params": [stream],
                        "id": self._stream_id_counter
                    }
                    self._stream_id_counter += 1
                    
                    if self._websocket:
                        await self._websocket.send(json.dumps(unsubscribe_msg))
                
                if self.logger:
                    self.logger.info(f"🚫 取消订阅: {symbol}")
            else:
                # 取消所有订阅
                self._subscriptions.clear()
                self._user_subscriptions.clear()
                
                if self.logger:
                    self.logger.info("🚫 取消所有订阅")
                    
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 取消订阅失败: {str(e)}")
    
    def get_cached_ticker(self, symbol: str) -> Optional[TickerData]:
        """获取缓存的行情数据"""
        return self._ticker_cache.get(symbol)
    
    def get_cached_orderbook(self, symbol: str) -> Optional[OrderBookData]:
        """获取缓存的订单簿数据"""
        return self._orderbook_cache.get(symbol)
    
    @property
    def is_connected(self) -> bool:
        """检查市场数据流连接状态"""
        return self._connected and self._websocket and not self._websocket.closed
    
    @property
    def is_user_connected(self) -> bool:
        """检查用户数据流连接状态"""
        return self._user_connected and self._user_websocket and not self._user_websocket.closed 