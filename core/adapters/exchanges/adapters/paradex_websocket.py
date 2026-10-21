"""
Paradex WebSocket API 模块

提供WebSocket实时数据订阅，包括：
- 实时行情（markets_summary）
- 订单簿（bbo, order_book）
- 成交记录（trades）
- 用户数据（account, positions, trx）
"""

import asyncio
import websockets
import json
import logging
import time
from typing import Dict, List, Optional, Any, Callable, Set, Tuple
from decimal import Decimal
from datetime import datetime

from ..models import (
    TickerData,
    OrderBookData,
    OrderBookLevel,
    TradeData,
    OrderData,
    PositionData,
    OrderSide,
    OrderType,
    OrderStatus,
    PositionSide
)

from .paradex_base import ParadexBase
from ..utils.logger_factory import get_exchange_logger

logger = get_exchange_logger("ExchangeAdapter.paradex")


class ParadexWebSocket(ParadexBase):
    """
    Paradex WebSocket 实现
    
    特点：
    - 使用JSON-RPC 2.0协议
    - 支持订阅/取消订阅
    - 自动处理Ping/Pong
    - 支持公共和私有频道
    """
    
    def __init__(self, config=None, logger_param=None, paradex_client=None):
        """
        初始化WebSocket客户端
        
        Args:
            config: 交易所配置
            logger_param: 日志器（从适配器传入）
            paradex_client: Paradex SDK 客户端实例（用于获取 JWT）
        """
        super().__init__(config)
        # 🔥 使用模块级 logger，确保日志写入文件
        # 如果传入了 logger，将其 handlers 添加到模块级 logger
        self.logger = logger
        if logger_param and hasattr(logger_param, 'handlers'):
            for handler in logger_param.handlers:
                if handler not in logger.handlers:
                    logger.addHandler(handler)
        
        # WebSocket连接
        self._ws = None
        self._ws_connected = False
        self._ws_task: Optional[asyncio.Task] = None
        self._connection_monitor_task: Optional[asyncio.Task] = None
        self._monitor_interval = 10  # 秒
        self._silence_timeout_seconds = 45  # 超过该时间无业务消息则触发重连
        
        # 消息ID计数器（用于JSON-RPC）
        self._message_id = 0
        
        # 消息统计（参考 Lighter 适配器）
        self._message_count = 0
        self._connection_start_time = 0
        
        # 🔥 持仓状态缓存（用于检测变化，降低日志频率）
        self._last_position_state: Dict[str, Dict] = {}  # {market: {size, avg_price, ...}}
        
        # Paradex SDK 客户端（用于生成带交易权限的 JWT）
        self._paradex_client = paradex_client
        
        # 认证 - 优先使用 SDK 生成的 JWT，其次才用配置中的只读 JWT
        self.jwt_token = ''
        if self._paradex_client:
            # SDK 会自动生成带交易权限的 JWT
            # JWT Token 存储在 account.jwt_token 路径
            try:
                if hasattr(self._paradex_client, 'account'):
                    account = self._paradex_client.account
                    if hasattr(account, 'jwt_token'):
                        self.jwt_token = account.jwt_token
                        if self.logger and self.jwt_token:
                            self.logger.info("✅ 使用 SDK 自动生成的交易权限 JWT（支持私有频道）")
                    else:
                        if self.logger:
                            self.logger.debug("SDK account 对象未包含 jwt_token 属性")
                else:
                    if self.logger:
                        self.logger.debug("SDK 未包含 account 对象")
            except Exception as e:
                if self.logger:
                    self.logger.debug(f"从 SDK 获取 JWT 时出错: {e}")
        
        # 如果 SDK 未提供 JWT，则尝试使用配置中的只读 JWT
        if not self.jwt_token:
            self.jwt_token = getattr(config, 'extra_params', {}).get('jwt_token', '') if config else ''
            if self.jwt_token and self.logger:
                self.logger.warning("⚠️  使用配置中的只读 JWT（无法订阅私有频道）")
        
        # 订阅管理
        self._subscriptions: Dict[str, List[Callable]] = {}
        self._active_channels: List[str] = []
        
        # 回调函数注册
        self.user_data_callback: Optional[Callable] = None
        self._ticker_callbacks: Dict[str, List[Callable]] = {}
        self._orderbook_callbacks: Dict[str, List[Callable]] = {}
        self._trade_callbacks: Dict[str, List[Callable]] = {}
        
        # 🔥 订单状态与成交回调（用于执行器）
        self._order_fill_callbacks: List[Callable] = []
        self._order_callbacks: List[Callable] = []
        self._orders_subscription_channel: Optional[str] = None
        self.order_fill_ws_enabled: bool = False  # 提供给OrderMonitor判断是否可用
        self._position_callbacks: List[Callable] = []
        
        # 订阅跟踪
        self._ticker_symbols: Set[str] = set()
        self._orderbook_symbols: Set[str] = set()
        self._trade_symbols: Set[str] = set()
        
        # 连接监控
        self._last_message_time: float = 0.0
        self._last_business_message_time: float = 0.0
        self._reconnecting: bool = False
        self._reconnect_attempts: int = 0
        self._should_run: bool = True
        
    async def connect(self) -> bool:
        """
        建立WebSocket连接
        
        Returns:
            bool: 是否成功
        """
        try:
            ws_url = self.get_ws_url()
            
            if self.logger:
                self.logger.info(f"正在连接Paradex WebSocket: {ws_url}")
                
            self._ws = await websockets.connect(
                ws_url,
                ping_interval=None,  # 禁用自动ping，我们自己处理
                ping_timeout=None
            )
            
            self._ws_connected = True
            self._should_run = True
            current_time = time.time()
            self._last_message_time = current_time
            self._last_business_message_time = current_time
            
            # 如果有JWT Token，先认证
            if self.jwt_token:
                await self._authenticate()
                
            # 启动消息处理循环（初始化统计信息）
            self._message_count = 0
            self._connection_start_time = datetime.now().timestamp()
            self._ws_task = asyncio.create_task(self._message_loop())
            self._start_connection_monitor()
            
            if self.logger:
                self.logger.info("Paradex WebSocket连接成功")
                
            return True
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"Paradex WebSocket连接失败: {e}")
            return False
            
    async def disconnect(self, for_reconnect: bool = False) -> None:
        """断开WebSocket连接"""
        self._ws_connected = False
        self.order_fill_ws_enabled = False
        if not for_reconnect:
            self._should_run = False
        await self._stop_connection_monitor()
        
        # 取消任务
        if self._ws_task:
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass
                
        # 关闭连接
        if self._ws:
            await self._ws.close()
            self._ws = None
            
        if self.logger:
            self.logger.info("Paradex WebSocket已断开")
            
    async def _authenticate(self) -> None:
        """
        WebSocket认证
        
        发送认证消息（用于私有频道）
        """
        auth_message = {
            'jsonrpc': '2.0',
            'method': 'auth',
            'params': {
                'bearer': self.jwt_token
            },
            'id': self._get_message_id()
        }
        
        await self._send(auth_message)
        
        if self.logger:
            self.logger.info("WebSocket认证消息已发送")
            
    async def _send(self, message: Dict) -> None:
        """发送WebSocket消息"""
        if self._ws and self._ws_connected:
            await self._ws.send(json.dumps(message))
            
    def _get_message_id(self) -> int:
        """获取下一个消息ID"""
        self._message_id += 1
        return self._message_id
        
    async def _message_loop(self) -> None:
        """消息处理循环"""
        if self.logger:
            self.logger.info("🔄 [Paradex] WebSocket 消息循环已启动")
        try:
            async for message in self._ws:
                current_time = time.time()
                self._last_message_time = current_time
                self._last_business_message_time = current_time
                self._message_count += 1
                
                # 📊 每5000条消息打印一次统计（参考 Lighter 适配器）
                if self._message_count % 5000 == 0:
                    elapsed = datetime.now().timestamp() - self._connection_start_time
                    if self.logger:
                        self.logger.info(
                            f"📊 [Paradex] 已接收 {self._message_count} 条消息 | 连接持续 {elapsed:.0f} 秒"
                        )
                
                try:
                    data = json.loads(message)
                    await self._handle_message(data)
                except json.JSONDecodeError as e:
                    if self.logger:
                        self.logger.error(f"[Paradex] JSON解析失败: {e}")
                except Exception as e:
                    if self.logger:
                        self.logger.error(f"[Paradex] 消息处理失败: {e}")
                        
        except asyncio.CancelledError:
            if self.logger:
                self.logger.info(f"🛑 [Paradex] WebSocket 消息循环已取消（共接收 {self._message_count} 条消息）")
            pass
        except Exception as e:
            if self.logger:
                self.logger.error(f"[Paradex] WebSocket消息循环异常: {e}")
            self._ws_connected = False
            await self._schedule_reconnect("消息循环异常")
        finally:
            if (not self._ws_connected) and self._should_run and not self._reconnecting:
                await self._schedule_reconnect("消息循环结束")
    
    def _start_connection_monitor(self):
        if self._connection_monitor_task and not self._connection_monitor_task.done():
            self._connection_monitor_task.cancel()
        self._connection_monitor_task = asyncio.create_task(self._connection_monitor())
    
    async def _stop_connection_monitor(self):
        if self._connection_monitor_task and not self._connection_monitor_task.done():
            self._connection_monitor_task.cancel()
            try:
                await self._connection_monitor_task
            except asyncio.CancelledError:
                pass
        self._connection_monitor_task = None
    
    async def _connection_monitor(self):
        try:
            while self._ws_connected and self._should_run:
                await asyncio.sleep(self._monitor_interval)
                if not self._ws_connected or not self._should_run:
                    break
                
                reference_time = self._last_business_message_time or self._last_message_time
                if reference_time <= 0:
                    continue
                
                silence_time = time.time() - reference_time
                if silence_time > self._silence_timeout_seconds:
                    if self.logger:
                        self.logger.warning(
                            f"⚠️ [Paradex] WebSocket静默 {silence_time:.1f}s，触发重连"
                        )
                    self._ws_connected = False
                    await self._schedule_reconnect("数据静默")
                    break
        except asyncio.CancelledError:
            pass
    
    async def _schedule_reconnect(self, reason: str) -> None:
        if not self._should_run:
            return
        if self._reconnecting:
            if self.logger:
                self.logger.debug(f"🔄 [Paradex] 已在重连中，忽略触发: {reason}")
            return
        self._reconnecting = True
        await self._stop_connection_monitor()
        asyncio.create_task(self._reconnect(reason))
                
    async def _handle_message(self, data: Dict) -> None:
        """
        处理接收到的消息
        
        Paradex使用JSON-RPC 2.0格式：
        - method: "subscription" 表示订阅数据推送
        - result: 订阅/取消订阅的响应
        """
        # 检查是否是订阅数据
        if data.get('method') == 'subscription':
            params = data.get('params', {})
            channel = params.get('channel', '')
            channel_data = params.get('data', {})
            
            await self._handle_subscription_data(channel, channel_data)
            
        # 检查是否是订阅响应
        elif 'result' in data:
            result = data.get('result', {})
            channel = result.get('channel', '')
            if self.logger:
                self.logger.debug(f"订阅确认: {channel}")
                
        # 检查是否是错误
        elif 'error' in data:
            error = data.get('error', {})
            if self.logger:
                self.logger.error(f"WebSocket错误: {error}")
                
    async def _handle_subscription_data(self, channel: str, data: Dict) -> None:
        """处理订阅数据推送"""
        try:
            # 解析频道类型
            if channel == 'markets_summary' or channel.startswith('markets_summary.'):
                await self._handle_market_summary(data)
            elif channel.startswith('bbo.'):
                await self._handle_bbo(channel, data)
            elif channel.startswith('order_book.'):
                await self._handle_orderbook(channel, data)
            elif channel.startswith('trades.'):
                await self._handle_trades(channel, data)
            elif channel == 'orders' or channel.startswith('orders.'):
                await self._handle_orders(channel, data)
            elif channel == 'account':
                await self._handle_account(data)
            elif channel == 'positions':
                await self._handle_positions(data)
            elif channel.startswith('transaction'):
                await self._handle_transaction(data)
            else:
                if self.logger:
                    self.logger.debug(f"未处理的频道: {channel}")
                    
        except Exception as e:
            if self.logger:
                self.logger.error(f"处理订阅数据失败 {channel}: {e}")
                
    async def _handle_market_summary(self, data: Dict) -> None:
        """处理市场摘要数据（用作ticker）"""
        try:
            raw_symbol = data.get('symbol') or data.get('market') or data.get('ticker')
            
            if not raw_symbol:
                return

            if '-' not in raw_symbol:
                # 忽略非交易对数据（例如资金费率、货币概览等）
                return

            symbol = self.normalize_symbol(raw_symbol)
            
            if '/' not in symbol and ':' not in symbol:
                # 非标准交易对数据（如账户余额更新），跳过
                return
            
            funding_rate = self._safe_decimal(data.get('funding_rate'), default=None)
            future_funding_rate = self._safe_decimal(data.get('future_funding_rate'), default=None)
            mark_price = self._safe_decimal(data.get('mark_price'), default=None)
            index_price = self._safe_decimal(
                data.get('underlying_price') or data.get('index_price'),
                default=None
            )
            open_interest = self._safe_decimal(data.get('open_interest'), default=None)
            
            ticker = TickerData(
                symbol=symbol,
                timestamp=datetime.now(),
                last=self._safe_decimal(data.get('last_traded_price')),
                bid=self._safe_decimal(data.get('bid')),
                ask=self._safe_decimal(data.get('ask')),
                open=self._safe_decimal(data.get('open_price_24h')),
                high=self._safe_decimal(data.get('high_price_24h')),
                low=self._safe_decimal(data.get('low_price_24h')),
                volume=self._safe_decimal(data.get('volume_24h')),
                quote_volume=self._safe_decimal(data.get('quote_volume_24h')),
                change=self._safe_decimal(data.get('price_change_24h')),
                percentage=self._safe_decimal(data.get('price_change_rate_24h')),
                funding_rate=funding_rate,
                predicted_funding_rate=future_funding_rate,
                index_price=index_price,
                mark_price=mark_price,
                open_interest=open_interest,
                raw_data=data
            )
            
            # 调用注册回调
            callbacks = self._ticker_callbacks.get(symbol, [])
            
            # 📊 统计ticker更新次数，每50次打印一次（避免刷屏）
            if not hasattr(self, '_ticker_log_counter'):
                self._ticker_log_counter = {}
            self._ticker_log_counter[symbol] = self._ticker_log_counter.get(symbol, 0) + 1
            
            if self.logger and callbacks:
                if self._ticker_log_counter[symbol] == 1 or self._ticker_log_counter[symbol] % 50 == 0:
                    self.logger.info(f"📊 [Paradex Ticker] {symbol}: 最新价={ticker.last}, 买={ticker.bid}, 卖={ticker.ask} (第{self._ticker_log_counter[symbol]}次)")
            
            for callback in callbacks:
                await self._safe_callback(callback, symbol, ticker)
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"处理market_summary失败: {e}")
                
    async def _handle_bbo(self, channel: str, data: Dict) -> None:
        """处理BBO（最佳买卖价）数据"""
        try:
            symbol = self.normalize_symbol(data.get('market', ''))
            
            # 构建简化的订单簿（只有最佳价格）
            exchange_timestamp = self._timestamp_to_datetime(
                data.get('last_updated_at')
            )
            orderbook = OrderBookData(
                symbol=symbol,
                timestamp=exchange_timestamp or datetime.now(),
                bids=[
                    OrderBookLevel(
                        price=self._safe_decimal(data.get('bid', '0')),
                        size=self._safe_decimal(data.get('bid_size', '0'))
                    )
                ],
                asks=[
                    OrderBookLevel(
                        price=self._safe_decimal(data.get('ask', '0')),
                        size=self._safe_decimal(data.get('ask_size', '0'))
                    )
                ],
                nonce=data.get('sequence'),
                exchange_timestamp=exchange_timestamp,
                raw_data=data
            )
            
            callbacks = self._orderbook_callbacks.get(symbol, [])
            # 📈 降低日志频率，避免刷屏（每10次只记录1次）
            if not hasattr(self, '_orderbook_log_counter'):
                self._orderbook_log_counter = {}
            self._orderbook_log_counter[symbol] = self._orderbook_log_counter.get(symbol, 0) + 1
            if self.logger and callbacks and self._orderbook_log_counter[symbol] % 10 == 1:
                self.logger.debug(f"📈 [OrderBook] {symbol}: 买1={orderbook.bids[0].price if orderbook.bids else 'N/A'}, 卖1={orderbook.asks[0].price if orderbook.asks else 'N/A'}")
            for callback in callbacks:
                await self._safe_callback(callback, symbol, orderbook)
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"处理BBO失败: {e}")
                
    async def _handle_orderbook(self, channel: str, data: Dict) -> None:
        """处理完整订单簿数据"""
        try:
            symbol = self.normalize_symbol(data.get('market', ''))
            
            # 解析买盘和卖盘
            bids = []
            asks = []
            
            # Paradex的订单簿使用增量更新
            for update in data.get('updates', []):
                price = self._safe_decimal(update.get('price', '0'))
                size = self._safe_decimal(update.get('size', '0'))
                side = update.get('side', 'BUY')
                
                if side == 'BUY':
                    bids.append(OrderBookLevel(price=price, size=size))
                else:
                    asks.append(OrderBookLevel(price=price, size=size))
                    
            orderbook = OrderBookData(
                symbol=symbol,
                timestamp=datetime.now(),
                bids=sorted(bids, key=lambda x: x.price, reverse=True),
                asks=sorted(asks, key=lambda x: x.price),
                nonce=None,
                raw_data=data
            )
            
            callbacks = self._orderbook_callbacks.get(symbol, [])
            for callback in callbacks:
                await self._safe_callback(callback, symbol, orderbook)
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"处理orderbook失败: {e}")
                
    async def _handle_trades(self, channel: str, data: Dict) -> None:
        """处理成交数据"""
        try:
            symbol = self.normalize_symbol(data.get('market', ''))
            
            amount = self._safe_decimal(data.get('size'))
            price = self._safe_decimal(data.get('price'))
            cost = price * amount if price is not None else amount
            
            trade = TradeData(
                id=str(data.get('id', '')),
                symbol=symbol,
                side=OrderSide.BUY if data.get('side') == 'BUY' else OrderSide.SELL,
                amount=amount,
                price=price,
                cost=cost,
                fee=data.get('fee'),
                timestamp=self._timestamp_to_datetime(data.get('created_at')),
                order_id=data.get('order_id'),
                raw_data=data
            )
            
            callbacks = self._trade_callbacks.get(symbol, [])
            for callback in callbacks:
                await self._safe_callback(callback, symbol, trade)
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"处理trades失败: {e}")
                
    async def _handle_orders(self, channel: str, data: Dict) -> None:
        """处理订单更新推送"""
        try:
            order = self._parse_order_data(data)
            if not order:
                return
            
            if self.logger:
                self.logger.info(
                    f"📝 [Paradex订单] {channel}: id={order.id}, "
                    f"status={order.status.value}, filled={order.filled}/{order.amount}"
                )
            
            # 触发通用用户数据回调（保持与其他频道一致）
            if self.user_data_callback:
                user_data = {
                    'type': 'order',
                    'data': data
                }
                await self._safe_callback(self.user_data_callback, order.symbol, user_data)
            
            # 仅当订单完全成交时，通知套利执行器
            # 🔥 Paradex 特殊处理：status=filled 但 filled=0 说明订单取消，不是成交
            if order.status == OrderStatus.FILLED:
                filled_amount = self._safe_decimal(getattr(order, 'filled', 0))
                if filled_amount and filled_amount > Decimal('0'):
                    await self._emit_order_fill_callbacks(order)
                else:
                    if self.logger:
                        self.logger.warning(
                            f"⚠️ [Paradex订单] status=FILLED 但 filled=0: id={order.id}, "
                            f"订单可能已取消，跳过成交回调"
                        )
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ [Paradex] 处理订单推送失败: {e}", exc_info=True)
                
    async def _handle_account(self, data: Dict) -> None:
        """处理账户更新"""
        if self.user_data_callback:
            user_data = {
                'type': 'account',
                'data': data
            }
            await self._safe_callback(self.user_data_callback, '', user_data)
            
    async def _handle_positions(self, data: Dict) -> None:
        """处理持仓更新"""
        market = data.get('market', '')
        size = data.get('size', '0')
        avg_price = data.get('average_entry_price', '0')
        unrealized_pnl = data.get('unrealized_pnl', '0')
        
        # 🔥 检查持仓是否真正变化（降低日志频率）
        last_state = self._last_position_state.get(market, {})
        position_changed = (
            last_state.get('size') != size or
            last_state.get('avg_price') != avg_price
        )
        
        # 更新缓存状态
        self._last_position_state[market] = {
            'size': size,
            'avg_price': avg_price,
            'unrealized_pnl': unrealized_pnl
        }
        
        # 🔥 只在持仓变化时打印INFO日志，否则用DEBUG
        if position_changed and self.logger:
            self.logger.info(
                f"📊 [Paradex] 持仓变化: {market}, "
                f"数量={size}, 均价={avg_price}, 未实现盈亏={unrealized_pnl}"
            )
        elif self.logger:
            self.logger.debug(
                f"📊 [Paradex] 持仓更新(无变化): {market}, 未实现盈亏={unrealized_pnl}"
            )
        
        # 触发通用用户数据回调
        if self.user_data_callback:
            user_data = {
                'type': 'position',
                'data': data
            }
            await self._safe_callback(self.user_data_callback, '', user_data)
        
        # 🔥 同步更新共享持仓缓存（供UI读取）
        self._update_position_cache(data)
        
        # 🔥 触发持仓回调（用于执行器）- 只在持仓变化时触发
        if self._position_callbacks and position_changed:
            try:
                position_data = self._parse_position_data(data)
                if position_data:
                    if self.logger:
                        self.logger.debug(
                            f"📣 [Paradex] 触发持仓回调: {position_data.symbol}, "
                            f"数量={position_data.amount}"
                        )
                    for callback in self._position_callbacks:
                        try:
                            if asyncio.iscoroutinefunction(callback):
                                await callback(position_data)
                            else:
                                callback(position_data)
                        except Exception as e:
                            if self.logger:
                                self.logger.error(f"❌ [Paradex] 持仓回调执行失败: {e}", exc_info=True)
            except Exception as e:
                if self.logger:
                    self.logger.error(f"❌ [Paradex] 解析持仓数据失败: {e}", exc_info=True)
    
    def _update_position_cache(self, data: Dict) -> None:
        """将WS持仓推送写入共享缓存，供UI展示"""
        if not hasattr(self, '_position_cache'):
            return
        
        try:
            symbol = self.normalize_symbol(data.get('market', ''))
            if not symbol:
                return

            size = self._safe_decimal(data.get('size', '0'), Decimal('0'))
            side_raw = str(data.get('side', '')).upper()

            # Paradex size 可能不带符号，因此需要根据 side 调整
            if side_raw == 'SHORT' and size > 0:
                size = -size
            elif side_raw == 'LONG' and size < 0:
                size = abs(size)

            entry_price = self._safe_decimal(data.get('average_entry_price', '0'), Decimal('0'))
            unrealized = self._safe_decimal(data.get('unrealized_pnl', '0'), Decimal('0'))
            timestamp = (
                self._timestamp_to_datetime(
                    data.get('updated_at') or data.get('last_updated_at') or data.get('timestamp')
                ) or datetime.utcnow()
            )
            
            if size == Decimal('0'):
                # 持仓归零 -> 清理缓存
                if symbol in self._position_cache:
                    self._position_cache.pop(symbol, None)
                    if self.logger:
                        self.logger.debug(f"📉 [Paradex] 持仓清零，移除缓存: {symbol}")
                return
            
            # 🔥 直接使用size的符号，不依赖side字段（Paradex的size已经是带符号的）
            cache_entry = {
                'symbol': symbol,
                'size': size,
                'side': 'long' if size >= 0 else 'short',
                'entry_price': entry_price,
                'unrealized_pnl': unrealized,
                'timestamp': timestamp,
            }
            self._position_cache[symbol] = cache_entry
        except Exception as exc:
            if self.logger:
                self.logger.debug(f"⚠️ [Paradex] 更新持仓缓存失败: {exc}")
            
    async def _handle_transaction(self, data: Dict) -> None:
        """
        处理交易（链上确认）推送
        
        ⚠️ 注意：transaction 频道返回的是链上交易哈希，不是订单详情！
        数据格式：
        {
            "id": "transaction_id",
            "hash": "0x...",      # 链上交易哈希
            "type": "FILL",       # 交易类型
            "state": "PENDING",   # 确认状态
            "created_at": 123456,
            "completed_at": 123456
        }
        """
        # 触发通用用户数据回调（保持兼容性）
        if self.user_data_callback:
            user_data = {
                'type': 'transaction',  # 修正类型名称
                'data': data
            }
            await self._safe_callback(self.user_data_callback, '', user_data)
        
        # transaction 频道不提供订单详情，因此不触发订单成交回调
        if self.logger:
            self.logger.debug(
                f"[Paradex] 收到链上交易确认: tx_hash={data.get('hash', 'N/A')}, "
                f"type={data.get('type', 'N/A')}, state={data.get('state', 'N/A')}"
            )
            
    async def _emit_order_fill_callbacks(self, order: OrderData) -> None:
        """触发订单成交回调"""
        if not self._order_fill_callbacks:
            return
        for callback in list(self._order_fill_callbacks):
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(order)
                else:
                    callback(order)
            except Exception as exc:
                if self.logger:
                    self.logger.error(f"⚠️ [Paradex] 订单成交回调执行失败: {exc}")
            
    async def _safe_callback(self, callback: Callable, symbol: str, data: Any) -> None:
        """安全调用回调函数"""
        try:
            if asyncio.iscoroutinefunction(callback):
                await callback(symbol, data)
            else:
                callback(symbol, data)
        except Exception as e:
            if self.logger:
                self.logger.error(f"回调执行失败: {e}")
    
    def _parse_order_data(self, data: Dict) -> Optional[OrderData]:
        """
        解析WebSocket推送的订单数据
        
        Args:
            data: WebSocket推送的原始订单数据
            
        Returns:
            OrderData对象，解析失败返回None
        """
        try:
            symbol = self.normalize_symbol(data.get('market', ''))
            side_str = data.get('side', 'BUY')
            type_str = data.get('type', 'LIMIT')
            status_str = data.get('status', 'OPEN')
            
            amount = self._safe_decimal(data.get('size', '0'))
            remaining = self._safe_decimal(data.get('remaining_size', '0'))
            filled = amount - remaining
            
            price_value = data.get('price')
            price = self._safe_decimal(price_value) if price_value is not None else None
            
            avg_price_value = data.get('avg_fill_price')
            average_price = self._safe_decimal(avg_price_value) if avg_price_value is not None else None
            
            # 计算成本
            quote_amount = data.get('quote_amount')
            if quote_amount is not None:
                cost = self._safe_decimal(quote_amount)
            elif average_price:
                cost = filled * average_price
            elif price:
                cost = filled * price
            else:
                cost = Decimal('0')
            
            return OrderData(
                id=str(data.get('id', '')),
                client_id=data.get('client_id'),
                symbol=symbol,
                side=OrderSide.BUY if side_str == 'BUY' else OrderSide.SELL,
                type=self._map_order_type(type_str),
                amount=amount,
                price=price,
                filled=filled,
                remaining=remaining,
                cost=cost,
                average=average_price,
                status=self._parse_order_status(status_str),
                timestamp=self._timestamp_to_datetime(data.get('created_at')),
                updated=self._timestamp_to_datetime(data.get('last_updated_at')),
                fee=data.get('fee'),
                trades=data.get('fills', []) or data.get('trades', []),
                params=data.get('params') or {},
                raw_data=data
            )
        except Exception as e:
            if self.logger:
                self.logger.error(f"解析订单数据失败: {e}", exc_info=True)
            return None
    
    def _parse_position_data(self, data: Dict) -> Optional[PositionData]:
        """
        解析WebSocket推送的持仓数据
        
        根据 Paradex 文档，positions 频道返回的字段：
        - market: 市场符号
        - side: 持仓方向 (LONG/SHORT)
        - size: 持仓数量
        - average_entry_price: 平均开仓价格
        - liquidation_price: 清算价格
        - unrealized_pnl: 未实现盈亏
        - leverage: 杠杆倍数
        
        Args:
            data: WebSocket推送的原始持仓数据
            
        Returns:
            PositionData对象，解析失败返回None
        """
        try:
            symbol = self.normalize_symbol(data.get('market', ''))
            side_str = data.get('side', 'LONG')
            size = self._safe_decimal(data.get('size', '0'))
            
            # 如果持仓为0，返回None
            if size == Decimal('0'):
                return None
            
            from datetime import datetime
            from ..models import MarginMode
            
            entry_price = self._safe_decimal(data.get('average_entry_price', '0'))
            mark_price = self._safe_decimal(data.get('mark_price', '0')) or entry_price  # fallback to entry price
            unrealized_pnl = self._safe_decimal(data.get('unrealized_pnl', '0'))
            
            return PositionData(
                symbol=symbol,
                side=PositionSide.LONG if side_str == 'LONG' else PositionSide.SHORT,
                size=size,
                entry_price=entry_price,
                mark_price=mark_price,
                current_price=mark_price,  # 🔥 使用 mark_price 作为当前价格
                liquidation_price=self._safe_decimal(data.get('liquidation_price', '0')),
                unrealized_pnl=unrealized_pnl,
                realized_pnl=self._safe_decimal(data.get('realized_positional_pnl', '0')),  # 🔥 添加已实现盈亏
                percentage=None,  # 🔥 百分比收益率（可以后续计算）
                leverage=int(self._safe_decimal(data.get('leverage', '1'))),
                margin_mode=MarginMode.CROSS,  # 🔥 Paradex 默认全仓模式
                margin=self._safe_decimal(data.get('cost', '0')),
                timestamp=datetime.now(),  # 🔥 添加时间戳
                raw_data=data
            )
        except Exception as e:
            if self.logger:
                self.logger.error(f"解析持仓数据失败: {e}", exc_info=True)
            return None
    
    def _parse_order_status(self, status_str: str) -> OrderStatus:
        """解析订单状态"""
        mapping = {
            'NEW': OrderStatus.PENDING,
            'OPEN': OrderStatus.OPEN,
            'CLOSED': OrderStatus.FILLED,
            'FILLED': OrderStatus.FILLED,
            'UNTRIGGERED': OrderStatus.PENDING,
            'CANCELLED': OrderStatus.CANCELED,
            'CANCELED': OrderStatus.CANCELED,
        }
        return mapping.get(status_str, OrderStatus.UNKNOWN)
    
    def _map_order_type(self, type_str: str) -> OrderType:
        """映射订单类型"""
        mapping = {
            'LIMIT': OrderType.LIMIT,
            'MARKET': OrderType.MARKET,
            'STOP_LIMIT': OrderType.STOP_LIMIT,
            'STOP_MARKET': OrderType.STOP_MARKET,
            'STOP_LOSS_LIMIT': OrderType.STOP_LOSS_LIMIT,
            'STOP_LOSS_MARKET': OrderType.STOP_LOSS,
            'TAKE_PROFIT': OrderType.TAKE_PROFIT_LIMIT,
            'TAKE_PROFIT_MARKET': OrderType.TAKE_PROFIT,
        }
        return mapping.get(type_str, OrderType.LIMIT)
    
    def _safe_decimal(self, value: Any, default: Any = None) -> Optional[Decimal]:
        """
        安全地转换为Decimal
        
        Args:
            value: 要转换的值
            default: 失败时的默认值（None或Decimal值）
            
        Returns:
            Decimal对象或default值
        """
        if value is None:
            return default if default is not None else Decimal('0')
        try:
            return Decimal(str(value))
        except:
            return default if default is not None else Decimal('0')
    
    def _timestamp_to_datetime(self, timestamp: Any) -> Optional[datetime]:
        """将时间戳转换为datetime对象"""
        if not timestamp:
            return None
        try:
            if isinstance(timestamp, (int, float)):
                return datetime.fromtimestamp(timestamp / 1000.0)
            elif isinstance(timestamp, str):
                return datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        except Exception as e:
            if self.logger:
                self.logger.debug(f"时间戳转换失败: {e}")
        return None
                
    async def _reconnect(self, reason: str) -> None:
        """重连逻辑（带指数退避）"""
        if self.logger:
            self.logger.warning(f"🔄 [Paradex] 触发重连 (原因: {reason})")
        
        try:
            while self._should_run:
                self._reconnect_attempts += 1
                delay = min(2 ** min(self._reconnect_attempts, 6), 60)
                if self.logger:
                    self.logger.info(
                        f"🔧 [Paradex] 重连尝试#{self._reconnect_attempts}，延迟{delay}s"
                    )
                await asyncio.sleep(delay)
                
                try:
                    await self.disconnect(for_reconnect=True)
                except Exception as close_error:
                    if self.logger:
                        self.logger.debug(f"⚠️ [Paradex] 清理旧连接失败: {close_error}")
                
                # 🔥 关键修复：重连前刷新 JWT Token
                # Paradex 的 JWT Token 有过期时间（约30分钟），需要调用 SDK 的 auth() 主动续期
                refreshed_token, refreshed = self._refresh_jwt_via_sdk()
                if refreshed_token:
                    old_token = self.jwt_token[:20] + "..." if self.jwt_token else "None"
                    self.jwt_token = refreshed_token
                    new_token = self.jwt_token[:20] + "..." if self.jwt_token else "None"
                    if self.logger:
                        if refreshed:
                            self.logger.info(
                                f"🔑 [Paradex] 已刷新 JWT Token (旧:{old_token} -> 新:{new_token})"
                            )
                        else:
                            self.logger.info(
                                f"ℹ️ [Paradex] 沿用 SDK 缓存的 JWT Token (旧:{old_token} -> 新:{new_token})"
                            )
                
                try:
                    connected = await self.connect()
                    if not connected:
                        raise RuntimeError("connect() 返回 False")
                    
                    # 重新订阅之前的频道
                    for channel in self._active_channels:
                        await self._subscribe_channel(channel)
                    
                    # 🔥 如果订单频道已订阅且有回调，重新启用订单推送标志
                    if "orders.ALL" in self._active_channels and self._order_fill_callbacks:
                        self.order_fill_ws_enabled = True
                        if self.logger:
                            self.logger.info("✅ [Paradex] 订单成交推送已重新启用")
                    
                    self._reconnect_attempts = 0
                    if self.logger:
                        self.logger.info("✅ [Paradex] WebSocket重连成功")
                    break
                except Exception as e:
                    if self.logger:
                        self.logger.error(f"❌ [Paradex] 重连失败: {e}")
                    continue
            else:
                if self.logger:
                    self.logger.info("⏹️ [Paradex] 已停止重连（should_run=False）")
        finally:
            self._reconnecting = False
    
    def _refresh_jwt_via_sdk(self) -> Tuple[Optional[str], bool]:
        """
        使用 paradex-py SDK 主动刷新 JWT，并返回 (token, 是否真正刷新成功)
        """
        if not self._paradex_client:
            return None, False
        
        account = getattr(self._paradex_client, "account", None)
        if not account:
            if self.logger:
                self.logger.debug("SDK 尚未初始化 account，无法刷新 JWT")
            return None, False
        
        api_client = getattr(self._paradex_client, "api_client", None)
        refreshed = False
        
        if api_client and hasattr(api_client, "auth"):
            try:
                api_client.auth()
                refreshed = True
            except Exception as token_error:
                if self.logger:
                    self.logger.warning(f"⚠️ [Paradex] SDK auth() 刷新 JWT 失败: {token_error}")
        else:
            if self.logger:
                self.logger.debug("SDK api_client 不存在或缺少 auth()，无法主动刷新 JWT")
        
        token = getattr(account, "jwt_token", None)
        if not token:
            if self.logger:
                self.logger.warning("⚠️ [Paradex] SDK account.jwt_token 为空，无法用于重连认证")
            return None, False
        
        return token, refreshed
                
    # ===== 订阅接口 =====
    
    async def _subscribe_channel(self, channel: str) -> None:
        """
        订阅频道
        
        Args:
            channel: 频道名称（如 "bbo.BTC-USD-PERP", "markets_summary"）
        """
        message = {
            'jsonrpc': '2.0',
            'method': 'subscribe',
            'params': {
                'channel': channel
            },
            'id': self._get_message_id()
        }
        
        await self._send(message)
        
        if channel not in self._active_channels:
            self._active_channels.append(channel)
            
        if self.logger:
            self.logger.info(f"已订阅频道: {channel}")
            
    async def _unsubscribe_channel(self, channel: str) -> None:
        """
        取消订阅频道
        
        Args:
            channel: 频道名称
        """
        message = {
            'jsonrpc': '2.0',
            'method': 'unsubscribe',
            'params': {
                'channel': channel
            },
            'id': self._get_message_id()
        }
        
        await self._send(message)
        
        if channel in self._active_channels:
            self._active_channels.remove(channel)
            
        if self.logger:
            self.logger.info(f"已取消订阅: {channel}")
            
    async def subscribe_ticker(self, symbol: str, callback: Callable) -> None:
        """
        订阅ticker数据（使用markets_summary频道）
        
        Args:
            symbol: 交易对符号（标准格式或监控系统的hyphen格式）
            callback: 回调函数
        """
        # 注意：Paradex的markets_summary频道推送所有市场数据
        # 我们订阅一次，然后在回调中过滤
        channel = 'markets_summary'
        
        if channel not in self._active_channels:
            await self._subscribe_channel(channel)

        # 🔥 关键修复：统一标准符号
        standard_symbol, paradex_symbol = self._resolve_standard_symbol(symbol)
        original_symbol = symbol

        # 防止旧缓存遗留错误映射，强制刷新缓存
        if paradex_symbol and standard_symbol:
            self._reverse_symbol_mapping[paradex_symbol] = standard_symbol
            self._symbol_mapping[standard_symbol] = paradex_symbol

        if callback:
            self._ticker_callbacks.setdefault(standard_symbol, []).append(callback)
        
        self._ticker_symbols.add(standard_symbol)

    def _resolve_standard_symbol(self, symbol: str) -> Tuple[str, str]:
        """
        将任意格式的符号转换为标准格式(BTC/USDC:PERP)以及Paradex格式(BTC-USD-PERP)
        """
        if '/' in symbol and ':' in symbol:
            # 已是标准格式
            standard_symbol = symbol
            paradex_symbol = self.convert_to_paradex_symbol(symbol)
            return standard_symbol, paradex_symbol

        # 需要转换或已经是Paradex格式
        if symbol.count('-') >= 2:
            parts = symbol.split('-')
            quote = parts[1].upper()

            if quote == 'USD':
                paradex_symbol = '-'.join(parts[:3])
            elif quote == 'USDC':
                parts[1] = 'USD'
                paradex_symbol = '-'.join(parts[:3])
            else:
                paradex_symbol = self.convert_to_paradex_symbol(symbol)
        else:
            paradex_symbol = self.convert_to_paradex_symbol(symbol)

        # 清除旧缓存，确保 normalize_symbol 重新计算
        if paradex_symbol in self._reverse_symbol_mapping:
            self._reverse_symbol_mapping.pop(paradex_symbol, None)

        standard_symbol = self.normalize_symbol(paradex_symbol)
        return standard_symbol, paradex_symbol
            
    async def batch_subscribe_tickers(self, symbols: List[str], callback: Callable) -> None:
        """
        批量订阅ticker数据
        
        Args:
            symbols: 交易对列表
            callback: 回调函数
        """
        # 设置全局回调
        if callback:
            for symbol in symbols:
                self._ticker_callbacks.setdefault(symbol, []).append(callback)
            
        # 订阅markets_summary频道（包含所有市场）
        await self._subscribe_channel('markets_summary')
        self._ticker_symbols.update(symbols)
        
        if self.logger:
            self.logger.info(f"已批量订阅{len(symbols)}个交易对的ticker数据")
            
    async def subscribe_orderbook(self, symbol: str, callback: Callable) -> None:
        """
        订阅订单簿数据（使用bbo频道）
        
        Args:
            symbol: 交易对符号
            callback: 回调函数
        """
        standard_symbol, paradex_symbol = self._resolve_standard_symbol(symbol)
        channel = f'bbo.{paradex_symbol}'
        
        await self._subscribe_channel(channel)
        if callback:
            self._orderbook_callbacks.setdefault(standard_symbol, []).append(callback)
        self._orderbook_symbols.add(standard_symbol)
        self._symbol_mapping[standard_symbol] = paradex_symbol
        self._reverse_symbol_mapping[paradex_symbol] = standard_symbol
        
    async def batch_subscribe_orderbooks(self, symbols: List[str], callback: Callable) -> None:
        """
        批量订阅订单簿数据
        
        Args:
            symbols: 交易对列表
            callback: 回调函数
        """
        # 逐个订阅
        for symbol in symbols:
            standard_symbol, paradex_symbol = self._resolve_standard_symbol(symbol)
            channel = f'bbo.{paradex_symbol}'
            await self._subscribe_channel(channel)
            if callback:
                self._orderbook_callbacks.setdefault(standard_symbol, []).append(callback)
            self._orderbook_symbols.add(standard_symbol)
            self._symbol_mapping[standard_symbol] = paradex_symbol
            self._reverse_symbol_mapping[paradex_symbol] = standard_symbol
            
        if self.logger:
            self.logger.info(f"已批量订阅{len(symbols)}个交易对的订单簿数据")
            
    async def subscribe_trades(self, symbol: str, callback: Callable) -> None:
        """
        订阅成交数据
        
        Args:
            symbol: 交易对符号
            callback: 回调函数
        """
        paradex_symbol = self.convert_to_paradex_symbol(symbol)
        channel = f'trades.{paradex_symbol}'
        
        await self._subscribe_channel(channel)
        if callback:
            self._trade_callbacks.setdefault(symbol, []).append(callback)
        self._trade_symbols.add(symbol)
        
    async def subscribe_user_data(self, callback: Callable) -> None:
        """
        订阅用户数据（账户、持仓、订单更新）
        
        Args:
            callback: 回调函数
        """
        if not self.jwt_token:
            if self.logger:
                self.logger.warning("未配置JWT Token，无法订阅私有频道")
            return
            
        # 设置全局回调
        if callback:
            self.user_data_callback = callback
            
        # 订阅私有频道
        await self._subscribe_channel('account')
        await self._subscribe_channel('positions')
        await self._subscribe_channel('transaction')
        
        if self.logger:
            self.logger.info("已订阅用户数据频道")
    
    async def _ensure_orders_subscription(self, symbols: Optional[List[str]] = None) -> None:
        """
        确保订单频道已订阅。
        
        Paradex官方提供 `orders.<market_symbol>` 以及 `orders.ALL` 两种订阅方式。
        套利系统默认订阅 ALL，以便同时追踪所有订单。
        """
        if not self.jwt_token:
            raise RuntimeError("Paradex订单频道需要交易权限 JWT，请在配置或SDK中提供。")
        
        if symbols:
            for symbol in symbols:
                _, paradex_symbol = self._resolve_standard_symbol(symbol)
                channel = f"orders.{paradex_symbol}"
                if channel not in self._active_channels:
                    await self._subscribe_channel(channel)
        else:
            channel = "orders.ALL"
            if channel not in self._active_channels:
                await self._subscribe_channel(channel)
            self._orders_subscription_channel = channel
    
    async def subscribe_order_fills(self, callback: Callable) -> None:
        """
        订阅订单成交推送（用于执行器）
        Args:
            callback: 回调函数，接收 OrderData 对象
        """
        if not self.jwt_token:
            raise RuntimeError("Paradex订单推送需要JWT Token，请在配置中提供交易权限凭证。")
        
        await self._ensure_orders_subscription()
        
        if callback and callback not in self._order_fill_callbacks:
            self._order_fill_callbacks.append(callback)
        
        self.order_fill_ws_enabled = True
        if self.logger:
            self.logger.info("✅ [Paradex] 订单成交推送已启用 (orders.ALL)")
    
    async def subscribe_positions(self, callback: Optional[Callable] = None) -> None:
        """
        订阅持仓更新推送
        
        当持仓发生变化时，会触发回调函数。
        
        Args:
            callback: 回调函数（可选）
        """
        if not self.jwt_token:
            if self.logger:
                self.logger.warning("⚠️ [Paradex] 未配置JWT Token，无法订阅持仓推送")
            return
        
        if callback:
            self._position_callbacks.append(callback)
            if self.logger:
                self.logger.info(f"✅ [Paradex] 已注册持仓更新回调（共{len(self._position_callbacks)}个）")
        
        # 订阅 positions 频道
        if 'positions' not in self._active_channels:
            await self._subscribe_channel('positions')
            if self.logger:
                self.logger.info("✅ [Paradex] 已订阅 positions 频道（持仓更新）")
            
    async def unsubscribe_ticker(self, symbol: Optional[str] = None) -> None:
        """取消行情订阅"""
        if symbol:
            self._ticker_symbols.discard(symbol)
            self._ticker_callbacks.pop(symbol, None)
        else:
            self._ticker_symbols.clear()
            self._ticker_callbacks.clear()
        
        if not self._ticker_symbols and 'markets_summary' in self._active_channels:
            await self._unsubscribe_channel('markets_summary')
        
    async def unsubscribe_orderbook(self, symbol: Optional[str] = None) -> None:
        """取消订单簿订阅"""
        targets = [symbol] if symbol else list(self._orderbook_symbols)
        for sym in targets:
            std_symbol, paradex_symbol = self._resolve_standard_symbol(sym)
            if std_symbol not in self._orderbook_symbols:
                continue
            channel = f'bbo.{paradex_symbol}'
            if channel in self._active_channels:
                await self._unsubscribe_channel(channel)
            self._orderbook_symbols.discard(std_symbol)
            self._orderbook_callbacks.pop(std_symbol, None)
        
    async def unsubscribe_trades(self, symbol: Optional[str] = None) -> None:
        """取消成交订阅"""
        targets = [symbol] if symbol else list(self._trade_symbols)
        for sym in targets:
            if sym not in self._trade_symbols:
                continue
            paradex_symbol = self.convert_to_paradex_symbol(sym)
            channel = f'trades.{paradex_symbol}'
            if channel in self._active_channels:
                await self._unsubscribe_channel(channel)
            self._trade_symbols.discard(sym)
            self._trade_callbacks.pop(sym, None)
        
    async def unsubscribe_user_data(self) -> None:
        """取消用户数据订阅"""
        for channel in ['account', 'positions', 'transaction']:
            if channel in self._active_channels:
                await self._unsubscribe_channel(channel)
        self.user_data_callback = None
        
    def get_connection_status(self) -> Dict[str, Any]:
        """
        获取连接状态
        
        Returns:
            Dict: 连接状态信息
        """
        return {
            'connected': self._ws_connected,
            'subscriptions': len(self._active_channels),
            'active_channels': self._active_channels.copy()
        }

