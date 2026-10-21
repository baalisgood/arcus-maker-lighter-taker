"""
Backpack WebSocket模块

包含WebSocket连接管理、数据订阅、消息处理、实时数据解析等功能
应用了EdgeX的重连机制修复
"""

import asyncio
import time
import json
import aiohttp
import logging
from typing import Dict, List, Optional, Any, Callable
from decimal import Decimal
from datetime import datetime

from .backpack_base import BackpackBase
from ..models import (
    TickerData, OrderBookData, TradeData, OrderBookLevel, OrderSide,
    OrderData, OrderStatus, OrderType
)


class BackpackWebSocket(BackpackBase):
    """Backpack WebSocket接口"""

    def __init__(self, config=None, logger=None):
        super().__init__(config)
        # 🔥 如果没有传入logger，创建一个专门的logger
        if logger is None:
            self.logger = logging.getLogger('ExchangeAdapter.backpack')
            self.logger.setLevel(logging.INFO)
        elif hasattr(logger, "logger"):
            self.logger = logger.logger
        else:
            self.logger = logger
        
        # 🔥 关键修复：设置logger的文件handler（参考Lighter和EdgeX）
        self._setup_logger()

        if config and hasattr(config, 'ws_url') and config.ws_url:
            self.ws_url = config.ws_url
        else:
            self.ws_url = self.DEFAULT_WS_URL
        self._ws_connection = None
        self._session = None
        self._ws_subscriptions = []
        self.ticker_callback = None
        self.orderbook_callback = None
        self.trades_callback = None
        self.user_data_callback = None
        self._user_data_callbacks: List[Callable[[Dict[str, Any]], None]] = []

        # 初始化状态变量
        self._ws_connected = False
        self._last_heartbeat = 0
        self._last_message_time: float = 0.0
        self._last_business_message_time: float = 0.0
        self._reconnect_attempts = 0
        self._reconnecting = False
        self._connect_lock: asyncio.Lock = asyncio.Lock()
        self._heartbeat_should_stop = False  # 🔧 修复：心跳停止标志
        
        # 🔥 数据超时检测（参考Lighter实现）
        self._data_timeout_task: Optional[asyncio.Task] = None  # 数据超时检测任务
        self._data_timeout_seconds: float = 120.0  # 数据超时阈值（120秒，因为Backpack服务器120秒后才关闭连接）

        # 🔥 初始化持仓监控相关
        # 持仓缓存: {symbol: {size, entry_price, unrealized_pnl, side, timestamp}}
        self._position_cache = {}
        self._position_callbacks = []  # 持仓更新回调函数列表
        
        # 🔥 初始化订单监控相关
        self._order_callbacks = []  # 订单更新回调函数列表
        self._order_fill_callbacks = []  # 订单成交回调函数列表

        # ============================================================================
        # 🔥 心跳检测参数（基于Backpack官方规范 + aiohttp实现）
        # ============================================================================
        # 📌 Backpack 官方文档 - Keeping the connection alive:
        #
        # "To keep the connection alive, a Ping frame will be sent from the
        #  server every 60s, and a Pong is expected to be received from the
        #  client. If a Pong is not received within 120s, a Close frame will
        #  be sent and the connection will be closed.
        #
        #  If the server is shutting down, a Close frame will be sent and then
        #  a grace period of 30s will be given before the connection is closed.
        #  The client should reconnect after receiving the Close frame. The
        #  client will be reconnected to a server that is not shutting down."
        #
        # 🔑 重要实现细节：
        #   - aiohttp 在底层（C扩展）自动处理 Ping/Pong，应用层看不到
        #   - 我们不应该监控服务器Ping（因为看不到）
        #   - 应该信任 aiohttp 的自动 Ping/Pong 机制
        #   - 只需监控连接状态（closed）和业务消息活跃度
        # ============================================================================
        # 注意：长时间无业务消息是正常现象（如等待价格变化期间无订单成交）
        # 因此不使用业务消息超时作为重连触发条件

        # 🔥 本地订单簿缓存（用于处理增量更新，参考EdgeX实现）
        # {symbol: {bids: {price: size}, asks: {price: size}}}
        self._local_orderbooks: Dict[str, Dict[str, Dict[Decimal, Decimal]]] = {}
        
        # orderbook数据缓存（用于ticker等其他功能）
        self._latest_orderbooks: Dict[str, Dict[str, Any]] = {}
        self._orderbook_cache_timeout = 30  # orderbook缓存超时时间（秒）
        
        # 🔥 新增：markPrice数据缓存（用于资金费率）
        # {symbol: {"funding_rate": Decimal, "mark_price": Decimal, "timestamp": float}}
        self._mark_price_cache: Dict[str, Dict[str, Any]] = {}
        # 🔥 markPrice缓存超时时间：资金费率每小时更新一次，所以缓存时间应该足够长
        # 设置为3600秒（1小时），确保资金费率不会因为时间过期而失效
        self._mark_price_cache_timeout = 3600  # markPrice缓存超时时间（秒）
    
    def _setup_logger(self):
        """设置logger的文件handler（参考Lighter和EdgeX实现）"""
        from logging.handlers import RotatingFileHandler
        from pathlib import Path
        
        if not self.logger:
            return
        
        # 🔥 关键：阻止日志传播到父logger，避免输出到控制台
        self.logger.propagate = False

        logger_handlers = getattr(self.logger, "handlers", [])
        
        # 🔥 移除所有StreamHandler（控制台输出），只保留文件输出
        handlers_to_remove = []
        for handler in logger_handlers:
            if isinstance(handler, logging.StreamHandler) and not isinstance(handler, RotatingFileHandler):
                handlers_to_remove.append(handler)
        for handler in handlers_to_remove:
            self.logger.removeHandler(handler)
        
        # 确保logs目录存在
        Path("logs").mkdir(parents=True, exist_ok=True)
        
        # 检查是否已有文件handler
        has_file_handler = any(
            isinstance(h, RotatingFileHandler) and 'ExchangeAdapter.log' in str(h.baseFilename)
            for h in logger_handlers
        )
        
        if not has_file_handler:
            # 添加文件handler
            file_handler = RotatingFileHandler(
                'logs/ExchangeAdapter.log',
                maxBytes=10*1024*1024,  # 10MB
                backupCount=3,
                encoding='utf-8'
            )
            file_handler.setLevel(logging.INFO)
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
            )
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
            self.logger.setLevel(logging.INFO)  # 确保logger级别至少是INFO
            self.logger.debug("[Backpack] WebSocket: 日志文件handler已配置")

    async def _check_exchange_connectivity(self) -> bool:
        """检查交易所服务器连通性"""
        try:
            # 检查Backpack的REST API是否可达
            api_url = "https://api.backpack.exchange/api/v1/status"  # 尝试status端点
            timeout = aiohttp.ClientTimeout(total=8)

            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(api_url) as response:
                    return response.status in [200, 404]  # 404也说明服务器可达

        except Exception as e:
            if self.logger:
                self.logger.warning(f"🏢 Backpack服务器连通性检查失败: {e}")
            return False

    def _is_connection_usable(self) -> bool:
        """检查WebSocket连接是否可用"""
        return (
            hasattr(self, '_ws_connection') and
            self._ws_connection is not None and
            not self._ws_connection.closed and
            getattr(self, '_ws_connected', False)
        )

    async def _stop_task(self, task_attr: str, label: str, timeout: float = 2.0) -> None:
        """
        通用任务停止器，确保不会残留异步任务（避免重复心跳等问题）
        """
        task = getattr(self, task_attr, None)
        if not task:
            return

        if self.logger:
            self.logger.info(f"🛑 取消Backpack{label}任务...")

        task.cancel()
        try:
            await asyncio.wait_for(task, timeout=timeout)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass
        finally:
            setattr(self, task_attr, None)

        if self.logger:
            self.logger.info(f"✅ Backpack{label}任务已停止")

    async def _stop_heartbeat_task(self) -> None:
        """停止心跳任务，防止重复心跳日志"""
        if hasattr(self, '_heartbeat_should_stop'):
            self._heartbeat_should_stop = True
        await self._stop_task('_heartbeat_task', '心跳')

    async def _stop_data_timeout_task(self) -> None:
        """停止数据超时监控任务"""
        await self._stop_task('_data_timeout_task', '数据超时检测')

    async def _stop_ws_handler_task(self) -> None:
        """停止消息处理任务"""
        await self._stop_task('_ws_handler_task', '消息处理')

    async def _safe_send_message(self, message: str) -> bool:
        """安全发送WebSocket消息"""
        try:
            if not self._is_connection_usable():
                if self.logger:
                    self.logger.warning("⚠️ WebSocket连接不可用，无法发送消息")
                return False

            await self._ws_connection.send_str(message)
            return True
        except Exception as e:
            if self.logger:
                self.logger.warning(f"发送WebSocket消息失败: {e}")
            return False

    async def connect(self) -> bool:
        """建立WebSocket连接"""
        async with self._connect_lock:
            try:
                if self.logger:
                    self.logger.info("🔌 [Backpack] 开始建立WebSocket连接...")

                # 已连接则直接复用，避免重复创建心跳任务
                if self._is_connection_usable():
                    if self.logger:
                        self.logger.info("🔁 [Backpack] 已连接，跳过重复connect请求")
                    return True

                # 确保不存在遗留任务，防止重复心跳或监控循环
                await self._stop_heartbeat_task()
                await self._stop_data_timeout_task()
                await self._stop_ws_handler_task()
                
                # 使用aiohttp建立WebSocket连接
                if not hasattr(self, '_session') or self._session is None or (self._session and self._session.closed):
                    self._session = aiohttp.ClientSession()
                self._ws_connection = await self._session.ws_connect(self.ws_url)

                if self.logger:
                    self.logger.info(f"✅ [Backpack] WebSocket连接已建立: {self.ws_url}")

                # 初始化状态
                self._ws_connected = True
                self._last_heartbeat = time.time()
                self._last_message_time = self._last_heartbeat
                self._last_business_message_time = self._last_heartbeat
                self._reconnect_attempts = 0
                self._reconnecting = False
                self._heartbeat_should_stop = False  # 🔧 修复：重置心跳停止标志

                # 启动消息处理任务
                self._ws_handler_task = asyncio.create_task(
                    self._websocket_message_handler())
                if self.logger:
                    self.logger.info("📨 [Backpack] 消息处理任务已启动")

                # 启动心跳检测
                self._heartbeat_task = asyncio.create_task(
                    self._websocket_heartbeat_loop())
                if self.logger:
                    self.logger.info("💓 [Backpack心跳] 心跳检测已启动")
                
                # 🔥 启动数据超时检测（参考Lighter）
                self._data_timeout_task = asyncio.create_task(
                    self._data_timeout_monitor())
                if self.logger:
                    self.logger.info(f"⏱️  [Backpack超时检测] 数据超时检测已启动 (超时阈值={self._data_timeout_seconds}秒)")

                return True

            except Exception as e:
                if self.logger:
                    self.logger.error(f"❌ [Backpack] 建立WebSocket连接失败: {e}")
                self._ws_connected = False
                return False

    async def disconnect(self) -> None:
        """断开WebSocket连接（应用EdgeX修复）"""
        if self.logger:
            self.logger.info("🔄 开始断开Backpack WebSocket连接...")

        try:
            # 1. 标记为断开状态，停止新的操作
            self._ws_connected = False

            # 🔧 修复：停止心跳检测循环
            if hasattr(self, '_heartbeat_should_stop'):
                self._heartbeat_should_stop = True
            await self._stop_heartbeat_task()
            
            # 🔥 2.5. 取消数据超时检测任务
            await self._stop_data_timeout_task()

            # 3. 取消消息处理任务
            await self._stop_ws_handler_task()

            # 4. 关闭WebSocket连接
            if hasattr(self, '_ws_connection') and self._ws_connection and not self._ws_connection.closed:
                if self.logger:
                    self.logger.info("🛑 关闭Backpack WebSocket连接...")
                try:
                    await asyncio.wait_for(self._ws_connection.close(), timeout=3.0)
                except asyncio.TimeoutError:
                    if self.logger:
                        self.logger.warning("⚠️ WebSocket关闭超时，强制设置为None")
                self._ws_connection = None
                if self.logger:
                    self.logger.info("✅ Backpack WebSocket连接已关闭")

            # 5. 关闭session
            if hasattr(self, '_session') and self._session and not self._session.closed:
                if self.logger:
                    self.logger.info("🛑 关闭Backpack session...")
                try:
                    await asyncio.wait_for(self._session.close(), timeout=3.0)
                except asyncio.TimeoutError:
                    if self.logger:
                        self.logger.warning("⚠️ Session关闭超时")
                if self.logger:
                    self.logger.info("✅ Backpack session已关闭")

            # 6. 清理状态变量
            self._last_heartbeat = 0
            self._last_message_time = 0.0
            self._last_business_message_time = 0.0
            self._reconnect_attempts = 0

            if self.logger:
                self.logger.info("🎉 Backpack WebSocket连接断开完成")

        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 关闭Backpack WebSocket连接时出错: {e}")
                import traceback
                self.logger.error(f"断开连接错误堆栈: {traceback.format_exc()}")

            # 强制清理状态
            self._ws_connected = False
            self._ws_connection = None

    # ============================================================================
    # 🚫 已废弃：客户端主动发送Ping（不符合Backpack官方规范）
    # ============================================================================
    # 根据Backpack官方文档，心跳机制是：
    #   - 服务器每60秒发送Ping → 客户端响应Pong（aiohttp自动处理）
    #   - 客户端不应该主动发送Ping
    # 因此此方法已废弃，保留仅供参考
    # ============================================================================
    # async def _send_ping(self) -> None:
    #     """发送标准WebSocket ping消息（已废弃）"""
    #     try:
    #         if self._ws_connection and self._ws_connected and not self._ws_connection.closed:
    #             await self._ws_connection.ping()
    #             if self.logger:
    #                 self.logger.debug("🏓 发送WebSocket ping")
    #     except Exception as e:
    #         if self.logger:
    #             self.logger.error(f"❌ 发送ping失败: {str(e)}")

    async def _websocket_heartbeat_loop(self):
        """WebSocket混合心跳检测循环 - 主动ping + 被动检测 (参考Hyperliquid)"""

        if self.logger:
            self.logger.info("💓 [Backpack心跳] 心跳检测循环启动 (数据流优先模式)")

        try:
            # 🔧 修复：使用独立的停止标志，不依赖连接状态
            self._heartbeat_should_stop = False

            while not self._heartbeat_should_stop:
                try:
                    # 等待10秒后进行下一次检测
                    await asyncio.wait_for(
                        asyncio.sleep(10),
                        timeout=15
                    )

                    # 检查是否应该停止心跳检测
                    if self._heartbeat_should_stop:
                        if self.logger:
                            self.logger.info("💓 [Backpack心跳] 心跳检测被停止")
                        break

                    current_time = time.time()

                    # 🔥 修复：双重检查连接状态（标志位 + WebSocket实际状态）
                    if not self._ws_connected:
                        if self.logger:
                            self.logger.warning("⚠️ [Backpack心跳] 检测到连接标志为断开，立即触发重连...")
                        await self._trigger_reconnection("连接标志显示断开")
                        continue
                    
                    # 🔥 增加：检查WebSocket实际关闭状态（防止僵尸连接）
                    if self._ws_connection and self._ws_connection.closed:
                        if self.logger:
                            self.logger.warning("🔴 [Backpack心跳] 检测到WebSocket实际已关闭，触发重连")
                        self._ws_connected = False
                        await self._trigger_reconnection("WebSocket实际已关闭")
                        continue

                    # === 📡 核心监控：WebSocket连接状态 ===
                    # ⚠️ 重要：aiohttp在底层自动处理Ping/Pong，应用层看不到
                    # - Backpack服务器每60秒发送Ping
                    # - aiohttp自动响应Pong（在C扩展层）
                    # - 如果120秒不响应，服务器会主动Close连接
                    # - 我们只需要信任aiohttp的自动处理，监控连接状态即可

                    # 💡 业务消息监控（仅用于调试，不触发重连）
                    if self._last_business_message_time > 0:
                        message_silence = current_time - self._last_business_message_time
                    else:
                        message_silence = current_time - self._last_message_time

                    # === ✅ 状态日志（每60秒输出一次） ===
                    if self.logger and int(current_time) % 60 < 10:  # 每分钟打印一次
                        if message_silence > 300:  # 5分钟无业务消息时提示
                            self.logger.info(
                                f"💓 [Backpack心跳] 连接正常（aiohttp自动Ping/Pong），"
                                f"但{message_silence:.1f}s无业务消息（等待订单成交/行情变化）"
                            )
                        else:
                            self.logger.info(
                                f"💓 [Backpack心跳] 连接正常，{message_silence:.1f}s前收到消息"
                            )

                except asyncio.CancelledError:
                    if self.logger:
                        self.logger.info("💓 [Backpack心跳] 心跳检测被取消")
                    break
                except asyncio.TimeoutError:
                    if self.logger:
                        self.logger.warning("⚠️ [Backpack心跳] 心跳检测超时")
                    continue
                except Exception as e:
                    if self.logger:
                        self.logger.error(f"❌ [Backpack心跳] 心跳检测错误: {e}")
                    # 错误后等待较短时间再继续
                    try:
                        await asyncio.wait_for(asyncio.sleep(5), timeout=10)
                    except (asyncio.CancelledError, asyncio.TimeoutError):
                        break

        except asyncio.CancelledError:
            if self.logger:
                self.logger.info("💓 [Backpack心跳] 心跳循环被正常取消")
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ [Backpack心跳] 心跳循环异常退出: {e}")
        finally:
            if self.logger:
                self.logger.info("💓 [Backpack心跳] 心跳检测循环已退出")
            # 清理重连状态
            self._reconnecting = False
    
    async def _data_timeout_monitor(self):
        """
        🔥 数据超时检测任务：监控数据接收情况，超时后主动重连（参考Lighter实现）
        
        工作流程：
        - 每30秒检查一次最后消息时间
        - 如果超过阈值（120秒）没有收到任何消息，主动关闭连接并触发重连
        - 这样可以及时发现静默断开的情况
        """
        try:
            if self.logger:
                self.logger.info(f"⏱️  [Backpack超时检测] 数据超时检测循环启动 (阈值={self._data_timeout_seconds}秒)")
            
            while self._ws_connected:
                try:
                    # 每30秒检查一次
                    await asyncio.sleep(30)
                    
                    # 检查是否应该停止
                    if not self._ws_connected:
                        if self.logger:
                            self.logger.info("⏱️  [Backpack超时检测] 连接已断开，停止监控")
                        break
                    
                    # 🔥 检查WebSocket连接状态（修复僵尸连接问题）
                    if not self._ws_connection or self._ws_connection.closed:
                        if self.logger:
                            self.logger.warning("🔴 [Backpack超时检测] WebSocket已关闭，触发重连")
                        
                        # 标记连接断开
                        self._ws_connected = False
                        
                        # 🔥 触发重连（而不是只退出循环）
                        await self._trigger_reconnection("WebSocket连接已关闭")
                        break
                    
                    # 检查数据超时
                    current_time = time.time()
                    if self._last_business_message_time > 0:
                        silence_time = current_time - self._last_business_message_time
                    elif self._last_heartbeat > 0:
                        silence_time = current_time - self._last_heartbeat
                    else:
                        silence_time = 0
                    
                    if silence_time > self._data_timeout_seconds:
                        if self.logger:
                            self.logger.error(
                                f"❌ [Backpack超时检测] 数据超时！"
                                f"静默时间: {silence_time:.1f}秒 > 阈值: {self._data_timeout_seconds}秒，"
                                f"主动关闭连接并触发重连..."
                            )
                        
                        # 🔥 主动关闭连接，触发重连
                        try:
                            if self._ws_connection and not self._ws_connection.closed:
                                await self._ws_connection.close()
                                if self.logger:
                                    self.logger.info("🔌 [Backpack超时检测] 已主动关闭WebSocket连接")
                        except Exception as e:
                            if self.logger:
                                self.logger.warning(f"⚠️ [Backpack超时检测] 关闭连接时出错: {e}")
                        
                        # 标记连接断开
                        self._ws_connected = False
                        
                        # 重置最后消息时间
                        self._last_heartbeat = 0
                        self._last_message_time = 0.0
                        self._last_business_message_time = 0.0
                        
                        # 触发重连
                        await self._trigger_reconnection("数据超时")
                        break
                    else:
                        # 定期输出状态（每5分钟）
                        if self.logger and int(current_time) % 300 < 30:
                            self.logger.debug(
                                f"⏱️  [Backpack超时检测] 连接正常，"
                                f"距上次消息: {silence_time:.1f}秒"
                            )
                
                except asyncio.CancelledError:
                    if self.logger:
                        self.logger.info("⏱️  [Backpack超时检测] 超时检测被取消")
                    break
                except Exception as e:
                    if self.logger:
                        self.logger.error(f"❌ [Backpack超时检测] 超时检测错误: {e}")
                    # 错误后等待较短时间再继续
                    try:
                        await asyncio.sleep(10)
                    except asyncio.CancelledError:
                        break
        
        except asyncio.CancelledError:
            if self.logger:
                self.logger.info("⏱️  [Backpack超时检测] 超时检测循环被正常取消")
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ [Backpack超时检测] 超时检测循环异常退出: {e}")
        finally:
            if self.logger:
                self.logger.info("⏱️  [Backpack超时检测] 超时检测循环已退出")

    async def _trigger_reconnection(self, reason: str) -> None:
        """触发重连的统一入口"""
        # 检查是否已经在重连中
        if hasattr(self, '_reconnecting') and self._reconnecting:
            if self.logger:
                self.logger.info(f"🔄 [Backpack重连] 已有重连在进行中，跳过'{reason}'重连")
            return

        # 标记重连状态
        self._reconnecting = True

        try:
            if self.logger:
                self.logger.warning(f"🔄 [Backpack重连] 开始执行重连 (原因: {reason})...")

            success = await self._reconnect_websocket()

            # 只有真正执行了重连才记录"重连完成"
            if success and self.logger:
                self.logger.info("✅ [Backpack重连] 重连完成")
            elif not success and self.logger:
                self.logger.warning("⚠️ [Backpack重连] 重连被跳过（网络不可达或其他原因）")
        except asyncio.CancelledError:
            if self.logger:
                self.logger.warning("⚠️ [Backpack重连] 重连被取消")
            raise
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ [Backpack重连] 重连失败: {type(e).__name__}: {e}")
        finally:
            # 清除重连状态标记
            self._reconnecting = False

    async def _reconnect_websocket(self) -> bool:
        """WebSocket自动重连 - 指数退避 + 交易所连通性诊断"""
        base_delay = 2
        max_delay = 300  # 最大延迟5分钟

        # 无限重试，移除次数限制
        self._reconnect_attempts += 1

        # 改进的指数退避：限制最大延迟
        delay = min(
            base_delay * (2 ** min(self._reconnect_attempts - 1, 8)), max_delay)

        if self.logger:
            self.logger.warning(
                f"🔄 [Backpack重连] 重连尝试 #{self._reconnect_attempts}，延迟{delay}秒")

        try:
            # 步骤1: 交易所连通性诊断
            if self.logger:
                self.logger.info("🔧 [Backpack重连] 步骤1: 交易所连通性诊断...")

            # 仅检测交易所自身是否可达，避免依赖第三方服务
            exchange_ok = await self._check_exchange_connectivity()
            if self.logger:
                status = "✅ 可达" if exchange_ok else "⚠️ 不可达"
                self.logger.info(f"🏢 [Backpack重连] 服务器连通性: {status}")
            if not exchange_ok:
                if self.logger:
                    self.logger.warning("❌ [Backpack重连] 交易所不可达，跳过本次重连以避免无效请求")
                return False

            # 步骤2: 彻底清理旧连接
            if self.logger:
                self.logger.info("🔧 [Backpack重连] 步骤2: 彻底清理旧连接...")
            await self._cleanup_old_connections()

            # 步骤3: 等待延迟
            if self.logger:
                self.logger.info(f"⏳ [Backpack重连] 步骤3: 等待{delay}秒后重连...")
            await asyncio.sleep(delay)

            # 步骤4: 重新建立连接
            if self.logger:
                self.logger.info("🔧 [Backpack重连] 步骤4: 重新建立WebSocket连接...")

            # 使用现有的connect方法，它已经包含了完整的连接逻辑
            reconnect_success = await self.connect()

            if reconnect_success:
                # 步骤5: 重新订阅所有频道
                if self.logger:
                    self.logger.info("🔧 [Backpack重连] 步骤5: 重新订阅所有频道...")
                await self._resubscribe_all()

                # 步骤6: 重置状态 - 重连成功，重置计数
                self._reconnect_attempts = 0
                current_time = time.time()
                self._last_heartbeat = current_time
                self._last_message_time = current_time
                self._last_business_message_time = current_time
                # aiohttp自动处理Ping/Pong，无需手动管理

                if self.logger:
                    self.logger.info("🎉 [Backpack重连] WebSocket重连成功！")
                return True  # 重连成功
            else:
                if self.logger:
                    self.logger.error("❌ [Backpack重连] 连接建立失败")
                return False  # 连接失败

        except asyncio.CancelledError:
            if self.logger:
                self.logger.warning("⚠️ [Backpack重连] 重连被取消")
            self._ws_connected = False
            raise
        except Exception as e:
            if self.logger:
                self.logger.error(
                    f"❌ [Backpack重连] 重连失败: {type(e).__name__}: {e}")
                import traceback
                self.logger.error(f"[Backpack重连] 完整错误堆栈: {traceback.format_exc()}")

            # 重连失败，返回 False
            return False

    async def _cleanup_old_connections(self):
        """彻底清理旧的连接和任务（应用EdgeX修复）"""
        try:
            # 0. 停止心跳检测任务
            if hasattr(self, '_heartbeat_should_stop'):
                self._heartbeat_should_stop = True
            await self._stop_heartbeat_task()

            # 1. 停止消息处理任务
            await self._stop_ws_handler_task()
            
            # 🔥 1.5. 停止数据超时检测任务
            await self._stop_data_timeout_task()

            # 2. 关闭WebSocket连接
            if hasattr(self, '_ws_connection') and self._ws_connection and not self._ws_connection.closed:
                try:
                    await asyncio.wait_for(self._ws_connection.close(), timeout=2.0)
                except asyncio.TimeoutError:
                    if self.logger:
                        self.logger.warning("⚠️ [清理调试] WebSocket关闭超时")
                self._ws_connection = None

            # 3. 关闭session
            if hasattr(self, '_session') and self._session and not self._session.closed:
                try:
                    await asyncio.wait_for(self._session.close(), timeout=2.0)
                except asyncio.TimeoutError:
                    if self.logger:
                        self.logger.warning("⚠️ [清理调试] Session关闭超时")

            if self.logger:
                self.logger.info("✅ [清理调试] 旧连接清理完成")

        except Exception as e:
            if self.logger:
                self.logger.warning(f"⚠️ [清理调试] 清理旧连接时出错: {e}")

    async def _resubscribe_all(self):
        """重新订阅所有频道（Backpack版本）"""
        try:
            if self.logger:
                self.logger.info("🔄 [重订阅调试] 开始重新订阅Backpack所有频道")

            # 🔥 步骤1: 重新订阅用户数据流（订单 & 持仓）
            if hasattr(self, 'user_data_callback') and self.user_data_callback:
                if self.logger:
                    self.logger.info("🔄 [重订阅调试] 重新订阅用户数据流（订单更新）...")
                try:
                    # 生成签名
                    timestamp = int(time.time() * 1000)
                    window = 5000
                    sign_string = f"instruction=subscribe&timestamp={timestamp}&window={window}"
                    signature = self._sign_message_for_subscription(
                        sign_string)

                    # 重新订阅
                    subscribe_msg = {
                        "method": "SUBSCRIBE",
                        "params": ["account.orderUpdate"],
                        "signature": [self.config.api_key, signature, str(timestamp), str(window)]
                    }

                    if await self._safe_send_message(json.dumps(subscribe_msg)):
                        if self.logger:
                            self.logger.info("✅ [重订阅调试] 用户数据流重新订阅成功")
                    else:
                        if self.logger:
                            self.logger.error("❌ [重订阅调试] 用户数据流重新订阅失败")
                    # 重新订阅持仓流
                    timestamp_pos = int(time.time() * 1000)
                    sign_string_pos = f"instruction=subscribe&timestamp={timestamp_pos}&window={window}"
                    signature_pos = self._sign_message_for_subscription(sign_string_pos)
                    subscribe_position_msg = {
                        "method": "SUBSCRIBE",
                        "params": ["account.positionUpdate"],
                        "signature": [self.config.api_key, signature_pos, str(timestamp_pos), str(window)]
                    }

                    if await self._safe_send_message(json.dumps(subscribe_position_msg)):
                        if self.logger:
                            self.logger.info("✅ [重订阅调试] 持仓更新流重新订阅成功")
                    else:
                        if self.logger:
                            self.logger.error("❌ [重订阅调试] 持仓更新流重新订阅失败")
                except Exception as e:
                    if self.logger:
                        self.logger.error(f"❌ [重订阅调试] 用户数据流重新订阅出错: {e}")

            # 🔥 步骤2: 重新订阅orderbook和ticker数据
            if hasattr(self, '_subscribed_symbols') and self._subscribed_symbols:
                # 应用黑名单过滤
                original_symbols = list(self._subscribed_symbols)
                filtered_symbols = self.filter_websocket_symbols(
                    original_symbols)

                symbol_count = len(filtered_symbols)
                filtered_count = len(original_symbols) - len(filtered_symbols)

                if self.logger:
                    self.logger.info(f"🔧 [重订阅调试] 待重新订阅的交易对数量: {symbol_count}")
                    if filtered_count > 0:
                        self.logger.info(
                            f"🚫 [重订阅调试] 已过滤黑名单交易对: {filtered_count} 个")
                    self.logger.info(
                        f"🔧 [重订阅调试] 交易对列表: {filtered_symbols[:10]}...")  # 只显示前10个

                orderbook_success = 0
                orderbook_failed = 0
                ticker_success = 0
                ticker_failed = 0

                # 🔥 重新订阅orderbook（depth）
                if self.logger:
                    self.logger.info("🔧 [重订阅调试] 开始重新订阅orderbook数据...")
                
                for i, symbol in enumerate(filtered_symbols):
                    try:
                        subscribe_msg = {
                            "method": "SUBSCRIBE",
                            "params": [f"depth.{symbol}"],
                            "id": i + 1
                        }

                        if await self._safe_send_message(json.dumps(subscribe_msg)):
                            orderbook_success += 1
                            if i < 3:  # 只记录前3个的详细信息
                                if self.logger:
                                    self.logger.info(
                                        f"✅ [重订阅调试] 重新订阅orderbook: {symbol}")
                            await asyncio.sleep(0.1)  # 小延迟
                        else:
                            orderbook_failed += 1
                    except Exception as e:
                        if self.logger:
                            self.logger.error(f"❌ [重订阅调试] 订阅orderbook {symbol}失败: {e}")
                        orderbook_failed += 1

                if self.logger:
                    self.logger.info(f"✅ [重订阅调试] Orderbook重新订阅完成: 成功{orderbook_success}个，失败{orderbook_failed}个")

                # 🔥 重新订阅ticker
                if self.logger:
                    self.logger.info("🔧 [重订阅调试] 开始重新订阅ticker数据...")
                
                for i, symbol in enumerate(filtered_symbols):
                    try:
                        subscribe_msg = {
                            "method": "SUBSCRIBE",
                            "params": [
                                f"ticker.{symbol}",
                                f"markPrice.{symbol}",
                            ],
                            "id": i + 1000  # 使用不同的ID范围
                        }

                        if await self._safe_send_message(json.dumps(subscribe_msg)):
                            ticker_success += 1
                            if i < 3:  # 只记录前3个的详细信息
                                if self.logger:
                                    self.logger.info(
                                        f"✅ [重订阅调试] 重新订阅ticker: {symbol}")
                            await asyncio.sleep(0.1)  # 小延迟
                        else:
                            ticker_failed += 1
                    except Exception as e:
                        if self.logger:
                            self.logger.error(f"❌ [重订阅调试] 订阅ticker {symbol}失败: {e}")
                        ticker_failed += 1
                
                if self.logger:
                    self.logger.info(f"✅ [重订阅调试] Ticker重新订阅完成: 成功{ticker_success}个，失败{ticker_failed}个")

                # 更新订阅列表为过滤后的列表
                self._subscribed_symbols = set(filtered_symbols)

                if self.logger:
                    total_success = orderbook_success + ticker_success
                    total_failed = orderbook_failed + ticker_failed
                    self.logger.info(
                        f"✅ [重订阅调试] Backpack重新订阅完成: {total_success}个成功, {total_failed}个失败 "
                        f"(Orderbook: {orderbook_success}/{len(filtered_symbols)}, "
                        f"Ticker: {ticker_success}/{len(filtered_symbols)})")
            else:
                if self.logger:
                    self.logger.warning("⚠️ [重订阅调试] 没有找到订阅的交易对列表")

        except Exception as e:
            if self.logger:
                self.logger.error(
                    f"❌ [重订阅调试] Backpack重新订阅失败: {type(e).__name__}: {e}")
                import traceback
                self.logger.error(f"[重订阅调试] 完整错误堆栈: {traceback.format_exc()}")
            raise

    async def _websocket_message_handler(self) -> None:
        """处理WebSocket消息（使用aiohttp的消息类型）"""
        try:
            async for msg in self._ws_connection:
                # 🔥 新增：更新心跳/消息时间戳（收到任何消息）
                current_time = time.time()
                self._last_message_time = current_time
                self._last_heartbeat = current_time

                if msg.type == aiohttp.WSMsgType.TEXT:
                    message = msg.data
                    await self._process_websocket_message(message)
                elif msg.type == aiohttp.WSMsgType.PING:
                    # aiohttp在底层自动处理Ping/Pong（C扩展层）
                    # 这里不会执行到，因为aiohttp在应用层之前就处理了
                    # 保留此分支仅供文档说明
                    pass
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    if self.logger:
                        self.logger.error(
                            f"❌ [Backpack] WebSocket错误: {self._ws_connection.exception()}")
                    self._ws_connected = False
                    await self._trigger_reconnection("WebSocket错误")
                    break
                elif msg.type == aiohttp.WSMsgType.CLOSE:
                    if self.logger:
                        self.logger.warning("⚠️ [Backpack] WebSocket连接已关闭")
                    self._ws_connected = False
                    await self._trigger_reconnection("连接被关闭")
                    break
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ [Backpack] WebSocket消息处理失败: {e}")
            self._ws_connected = False
            await self._trigger_reconnection(f"消息处理异常: {str(e)}")

    async def _process_websocket_message(self, message: str) -> None:
        """处理WebSocket消息 - 根据Backpack官方文档修复"""
        try:
            data = json.loads(message)
            if 'stream' in data and 'data' in data:
                self._last_business_message_time = self._last_message_time

            # 记录接收到的消息用于调试（减少日志量）
            if not hasattr(self, '_msg_count'):
                self._msg_count = 0
            self._msg_count += 1

            if self._msg_count <= 5:  # 只记录前5条消息
                if self.logger:
                    self.logger.debug(
                        f"收到WebSocket消息 #{self._msg_count}: {data}")

            # 处理订阅响应（可选，Backpack可能不发送）
            if 'result' in data and 'id' in data:
                if data['result'] is None:
                    if self.logger:
                        self.logger.info(f"订阅确认: ID {data['id']}")
                else:
                    if self.logger:
                        self.logger.warning(f"订阅可能失败: {data}")
                return

            # 处理错误消息
            if 'error' in data:
                error_info = data['error']
                error_code = error_info.get('code', 'unknown')
                error_message = error_info.get('message', 'unknown')

                # 记录详细的错误信息
                if self.logger:
                    self.logger.error(f"WebSocket错误: {error_info}")

                # 如果是Invalid market错误，记录但不中断其他订阅
                if error_code == 4005 and 'Invalid market' in error_message:
                    error_id = data.get('id', 'unknown')
                    if self.logger:
                        self.logger.warning(
                            f"某个符号可能不支持WebSocket: 请求ID {error_id}")

                return

            # 🔧 修复：Backpack实际使用嵌套的stream/data格式！
            # 处理Backpack的stream/data格式消息
            if 'stream' in data and 'data' in data:
                stream_name = data['stream']
                stream_data = data['data']

                # Backpack格式：ticker.SOL_USDC_PERP, depth.SOL_USDC_PERP, trade.SOL_USDC_PERP
                if stream_name.startswith('ticker.'):
                    # 从stream名称提取符号：ticker.SOL_USDC_PERP -> SOL_USDC_PERP
                    symbol = stream_name.split(
                        '.', 1)[1] if '.' in stream_name else stream_name
                    await self._handle_backpack_ticker_update(symbol, stream_data)

                elif stream_name.startswith('bookTicker.'):
                    # bookTicker也包含价格信息
                    symbol = stream_name.split(
                        '.', 1)[1] if '.' in stream_name else stream_name
                    await self._handle_backpack_ticker_update(symbol, stream_data)
                
                elif stream_name.startswith('markPrice.'):
                    # 🔥 新增：处理markPrice流（包含资金费率）
                    symbol = stream_name.split(
                        '.', 1)[1] if '.' in stream_name else stream_name
                    await self._handle_backpack_markprice_update(symbol, stream_data)

                elif stream_name.startswith('depth.'):
                    symbol = stream_name.split(
                        '.', 1)[1] if '.' in stream_name else stream_name
                    await self._handle_backpack_orderbook_update(symbol, stream_data)

                elif stream_name.startswith('trade.'):
                    symbol = stream_name.split(
                        '.', 1)[1] if '.' in stream_name else stream_name
                    await self._handle_backpack_trade_update(symbol, stream_data)

                elif stream_name == 'account.orderUpdate':
                    # 订单更新流 - 传入完整data（包含stream字段）
                    inner_data = data.get('data', data)
                    await self._handle_order_update(inner_data)
                    # 🔥 触发余额刷新回调
                    if hasattr(self, '_user_data_callbacks'):
                        for callback in self._user_data_callbacks:
                            await self._safe_callback(callback, data)

                elif stream_name == 'account.positionUpdate':
                    # 持仓更新流 - 传入完整data（包含stream字段）
                    inner_data = data.get('data', data)
                    await self._handle_position_update(inner_data)
                    # 🔥 触发余额刷新回调
                    if hasattr(self, '_user_data_callbacks'):
                        for callback in self._user_data_callbacks:
                            await self._safe_callback(callback, data)

                elif 'userData' in stream_name or 'account.' in stream_name:
                    # 兼容旧格式和其他账户流 - 传入完整data
                    # 🔥 触发余额刷新回调
                    if hasattr(self, '_user_data_callbacks'):
                        for callback in self._user_data_callbacks:
                            await self._safe_callback(callback, data)

                else:
                    if self.logger:
                        self.logger.debug(f"未知的流类型: {stream_name}")
            else:
                # 对于非标准格式的消息，记录但不报错
                if self._msg_count <= 5:
                    if self.logger:
                        self.logger.debug(f"未知消息格式: {data}")

        except Exception as e:
            if self.logger:
                self.logger.error(f"处理WebSocket消息失败: {e}")
                self.logger.error(f"原始消息: {message}")

    # ⚠️ 第一个定义已删除，真正的实现在第985行

    async def _safe_callback(self, callback: Callable, data: Any) -> None:
        """安全调用回调函数"""
        try:
            if callback:
                if asyncio.iscoroutinefunction(callback):
                    await callback(data)
                else:
                    callback(data)
        except Exception as e:
            if self.logger:
                self.logger.error(
                    f"❌ [Backpack] 回调函数执行失败: {e}", 
                    exc_info=True
                )

    async def _handle_backpack_ticker_update(self, symbol: str, data: Dict[str, Any]) -> None:
        """处理Backpack原生格式的ticker更新"""
        try:
            # 解析交易所时间戳（微秒）
            exchange_timestamp = None
            if 'E' in data:
                try:
                    timestamp_microseconds = int(data['E'])
                    exchange_timestamp = datetime.fromtimestamp(
                        timestamp_microseconds / 1000000)
                except (ValueError, TypeError):
                    pass

            # 使用当前时间作为主时间戳（确保时效性）
            current_time = datetime.now()
            main_timestamp = current_time

            # === 优先从ticker数据中获取bid/ask，然后从orderbook缓存获取 ===
            # 首先尝试从ticker数据中获取bid/ask（某些Backpack数据可能包含）
            bid_price = self._safe_decimal(data.get('b'))  # bid price
            ask_price = self._safe_decimal(data.get('a'))  # ask price
            bid_size = self._safe_decimal(data.get('B'))   # bid size
            ask_size = self._safe_decimal(data.get('A'))   # ask size

            # 如果ticker数据中没有bid/ask，从orderbook缓存获取
            if bid_price is None or ask_price is None:
                cached_bid, cached_ask, cached_bid_size, cached_ask_size = self._get_best_bid_ask_from_cache(
                    symbol)
                bid_price = bid_price or cached_bid
                ask_price = ask_price or cached_ask
                bid_size = bid_size or cached_bid_size
                ask_size = ask_size or cached_ask_size

            # 🔥 从 markPrice 缓存中获取资金费率（如果可用）
            funding_rate = None
            mark_price_data = self._mark_price_cache.get(symbol)
            if mark_price_data:
                # 检查缓存是否过期
                cache_age = time.time() - mark_price_data.get("timestamp", 0)
                if cache_age < self._mark_price_cache_timeout:
                    funding_rate = mark_price_data.get("funding_rate")
            
            # 如果缓存中没有，尝试从ticker数据本身获取（作为备用）
            if funding_rate is None:
                funding_rate = self._safe_decimal(data.get('fundingRate') or data.get('funding_rate'))
            
            # 根据测试结果解析ticker数据（Binance兼容格式）
            ticker = TickerData(
                symbol=symbol,
                bid=bid_price,  # 最佳买价
                ask=ask_price,  # 最佳卖价
                bid_size=bid_size,  # 最佳买单数量
                ask_size=ask_size,  # 最佳卖单数量
                # c = close/last price
                last=self._safe_decimal(data.get('c')),
                open=self._safe_decimal(data.get('o')),     # o = open price
                high=self._safe_decimal(data.get('h')),     # h = high price
                low=self._safe_decimal(data.get('l')),      # l = low price
                close=self._safe_decimal(data.get('c')),    # c = close price
                # v = base asset volume
                volume=self._safe_decimal(data.get('v')),
                quote_volume=self._safe_decimal(
                    data.get('V')),  # V = quote asset volume
                change=None,  # 可以通过 open-close 计算
                percentage=None,  # 可以通过 (close-open)/open*100 计算
                # 🔥 资金费率来自 markPrice 缓存
                funding_rate=funding_rate,
                timestamp=main_timestamp,
                exchange_timestamp=exchange_timestamp,
                raw_data=data
            )

            # 记录成功的ticker更新（限制日志量）
            if not hasattr(self, '_ticker_count'):
                self._ticker_count = {}
            if symbol not in self._ticker_count:
                self._ticker_count[symbol] = 0
                if self.logger:
                    # 首次ticker数据，显示完整信息
                    if bid_price and bid_size:
                        bid_info = f"买价: {bid_price:.2f} (数量: {bid_size:.4f})"
                    elif bid_price:
                        bid_info = f"买价: {bid_price:.2f}"
                    else:
                        bid_info = "买价: N/A"

                    if ask_price and ask_size:
                        ask_info = f"卖价: {ask_price:.2f} (数量: {ask_size:.4f})"
                    elif ask_price:
                        ask_info = f"卖价: {ask_price:.2f}"
                    else:
                        ask_info = "卖价: N/A"

                    # ✅ 改为info级别，确保首次数据可见
                    self.logger.info(
                        f"✅ [Backpack] 首次收到ticker数据: {symbol} -> {ticker.last} | {bid_info} | {ask_info}")
                self._ticker_count[symbol] += 1

            # 调用相应的回调函数
            # 1. 检查批量订阅的回调（需要两个参数：symbol, ticker）
            if hasattr(self, 'ticker_callback') and self.ticker_callback:
                await self._safe_callback_with_symbol(self.ticker_callback, symbol, ticker)

            # 2. 检查单独订阅的回调（只需要一个参数：ticker）
            for sub_type, sub_symbol, callback in getattr(self, '_ws_subscriptions', []):
                if sub_type == 'ticker' and sub_symbol == symbol:
                    await self._safe_callback(callback, ticker)

        except Exception as e:
            if self.logger:
                self.logger.error(f"处理Backpack ticker更新失败: {e}")
                self.logger.error(f"符号: {symbol}, 数据内容: {data}")
    
    async def _handle_backpack_markprice_update(self, symbol: str, data: Dict[str, Any]) -> None:
        """
        处理Backpack markPrice更新（包含资金费率）
        
        数据格式：
        {
          "e": "markPrice",
          "E": 1694687965941000,  // Event time in microseconds
          "s": "SOL_USDC",         // Symbol
          "p": "18.70",            // Mark price
          "f": "1.70",             // 🔥 Estimated funding rate (每小时)
          "i": "19.70",            // Index price
          "n": 1694687965941,      // Next funding timestamp
          "T": 1694687965940999    // Engine timestamp
        }
        
        ⚠️ 重要：Backpack 资金费率每小时结算一次
        - API返回：每小时费率（小数形式）
        - 系统需要：8小时费率（小数形式）
        - 转换公式：8小时费率 = 每小时费率 × 8
        """
        try:
            # 🔥 解析资金费率（每小时 → 8小时）
            raw_funding_rate = self._safe_decimal(data.get('f'))
            # Backpack: 每小时费率 × 8 = 8小时费率（系统内部统一格式）
            funding_rate = raw_funding_rate * 8 if raw_funding_rate is not None else None
            mark_price = self._safe_decimal(data.get('p'))
            index_price = self._safe_decimal(data.get('i'))
            next_funding_time = data.get('n')
            
            # 缓存 markPrice 数据
            current_time = time.time()
            self._mark_price_cache[symbol] = {
                "funding_rate": funding_rate,
                "mark_price": mark_price,
                "index_price": index_price,
                "next_funding_time": next_funding_time,
                "timestamp": current_time
            }
            
            # 记录资金费率数据（适度日志）
            if not hasattr(self, '_markprice_count'):
                self._markprice_count = {}
            
            if symbol not in self._markprice_count:
                self._markprice_count[symbol] = 0
                if self.logger and funding_rate:
                    self.logger.info(
                        f"✅ [Backpack] 首次收到 {symbol} 资金费率: {funding_rate} "
                        f"(标记价格: {mark_price}, 指数价格: {index_price})")
            
            self._markprice_count[symbol] += 1
            
            # 每100次更新记录一次
            if self._markprice_count[symbol] % 100 == 0:
                if self.logger and funding_rate:
                    self.logger.info(
                        f"📊 [Backpack] {symbol} markPrice更新 #{self._markprice_count[symbol]}: "
                        f"资金费率={funding_rate}")
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"处理Backpack markPrice更新失败: {e}")
                self.logger.error(f"符号: {symbol}, 数据内容: {data}")

    async def _handle_backpack_orderbook_update(self, symbol: str, data: Dict[str, Any]) -> None:
        """
        处理Backpack原生格式的订单簿更新
        
        🔥 重要：Backpack推送的是增量更新，需要维护本地订单簿状态
        参考EdgeX的实现模式
        """
        try:
            # 🔥 记录首次收到depth消息（诊断用）
            if not hasattr(self, '_depth_count'):
                self._depth_count = {}
            
            if symbol not in self._depth_count:
                self._depth_count[symbol] = 0
                if self.logger:
                    self.logger.info(
                        f"✅ [Backpack] 首次收到 {symbol} depth消息，开始构建本地订单簿")
            
            self._depth_count[symbol] += 1
            
            # 解析交易所时间戳（微秒）
            exchange_timestamp = None
            if 'E' in data:
                try:
                    timestamp_microseconds = int(data['E'])
                    exchange_timestamp = datetime.fromtimestamp(
                        timestamp_microseconds / 1000000)
                except (ValueError, TypeError):
                    pass

            main_timestamp = exchange_timestamp if exchange_timestamp else datetime.now()
            
            # === 🔥 本地订单簿维护逻辑（参考EdgeX） ===
            
            # 初始化本地订单簿（如果不存在）
            if symbol not in self._local_orderbooks:
                self._local_orderbooks[symbol] = {
                    'bids': {},  # {price: size}
                    'asks': {}   # {price: size}
                }
                if self.logger:
                    self.logger.info(f"📚 [Backpack] 初始化 {symbol} 本地订单簿")
            
            # 应用增量更新到买盘
            bids_raw = data.get('b', [])  # Backpack使用 'b' 表示bids
            for bid in bids_raw:
                if len(bid) >= 2:
                    price = self._safe_decimal(bid[0])
                    size = self._safe_decimal(bid[1])
                    
                    if price:
                        if size == 0:
                            # size=0：删除该价格档位
                            self._local_orderbooks[symbol]['bids'].pop(price, None)
                        elif size > 0:
                            # size>0：更新/新增该价格档位
                            self._local_orderbooks[symbol]['bids'][price] = size
            
            # 应用增量更新到卖盘
            asks_raw = data.get('a', [])  # Backpack使用 'a' 表示asks
            for ask in asks_raw:
                if len(ask) >= 2:
                    price = self._safe_decimal(ask[0])
                    size = self._safe_decimal(ask[1])
                    
                    if price:
                        if size == 0:
                            # size=0：删除该价格档位
                            self._local_orderbooks[symbol]['asks'].pop(price, None)
                        elif size > 0:
                            # size>0：更新/新增该价格档位
                            self._local_orderbooks[symbol]['asks'][price] = size
            
            # === 从本地订单簿构造完整的OrderBookData对象 ===
            local_book = self._local_orderbooks[symbol]
            
            # 转换为OrderBookLevel列表（按价格排序）
            bids = [
                OrderBookLevel(price=price, size=size)
                for price, size in sorted(local_book['bids'].items(), reverse=True)  # 买盘：价格从高到低
            ]

            asks = [
                OrderBookLevel(price=price, size=size)
                for price, size in sorted(local_book['asks'].items())  # 卖盘：价格从低到高
            ]
            
            # 🔥 验证订单簿完整性（必须同时有买盘和卖盘）
            if not bids or not asks:
                # 订单簿不完整，等待更多数据
                if self.logger and self._depth_count[symbol] <= 3:
                    self.logger.warning(
                        f"⚠️ [Backpack] {symbol} 订单簿不完整 (bids={len(bids)}, asks={len(asks)})，"
                        f"等待更多数据... (已收到{self._depth_count[symbol]}条depth消息)"
                    )
                return
            
            # === 缓存最新的orderbook数据供ticker使用 ===
            self._cache_orderbook_data(symbol, bids, asks, main_timestamp)

            # 🔥 记录首次构建成功的订单簿
            if not hasattr(self, '_orderbook_ready'):
                self._orderbook_ready = set()
            
            if symbol not in self._orderbook_ready:
                self._orderbook_ready.add(symbol)
                if self.logger:
                    self.logger.info(
                        f"✅ [Backpack] {symbol} 订单簿构建完成！"
                        f"买盘档位={len(bids)}, 卖盘档位={len(asks)}, "
                        f"最优买价={bids[0].price if bids else None}, "
                        f"最优卖价={asks[0].price if asks else None}"
                    )

            # 构造完整的OrderBookData对象
            orderbook = OrderBookData(
                symbol=symbol,
                bids=bids,
                asks=asks,
                timestamp=main_timestamp,
                nonce=data.get('u'),  # 使用更新ID作为nonce
                exchange_timestamp=exchange_timestamp,
                raw_data=data
            )

            # 调用相应的回调函数
            # 1. 检查批量订阅的回调（需要两个参数：symbol, orderbook）
            if hasattr(self, 'orderbook_callback') and self.orderbook_callback:
                await self._safe_callback_with_symbol(self.orderbook_callback, symbol, orderbook)

            # 2. 检查单独订阅的回调（只需要一个参数：orderbook）
            for sub_type, sub_symbol, callback in getattr(self, '_ws_subscriptions', []):
                if sub_type == 'orderbook' and sub_symbol == symbol:
                    await self._safe_callback(callback, orderbook)

        except Exception as e:
            if self.logger:
                self.logger.error(f"处理Backpack订单簿更新失败: {e}")
                self.logger.error(f"符号: {symbol}, 数据内容: {data}")

    async def _handle_backpack_trade_update(self, symbol: str, data: Dict[str, Any]) -> None:
        """处理Backpack原生格式的交易更新"""
        try:
            # 解析成交数据
            trade = TradeData(
                id=str(data.get('t', '')),  # t = trade ID
                symbol=symbol,
                side=OrderSide.BUY if data.get(
                    'm') is False else OrderSide.SELL,  # m = is maker
                amount=self._safe_decimal(data.get('q')),   # q = quantity
                price=self._safe_decimal(data.get('p')),    # p = price
                cost=self._safe_decimal(
                    data.get('q', 0)) * self._safe_decimal(data.get('p', 0)),
                fee=None,
                timestamp=datetime.fromtimestamp(data.get(
                    'T', 0) / 1000000) if data.get('T') else datetime.now(),  # T = timestamp in microseconds
                order_id=None,
                raw_data=data
            )

            # 调用相应的回调函数
            for sub_type, sub_symbol, callback in getattr(self, '_ws_subscriptions', []):
                if sub_type == 'trades' and sub_symbol == symbol:
                    await self._safe_callback(callback, trade)

        except Exception as e:
            if self.logger:
                self.logger.error(f"处理Backpack交易更新失败: {e}")
                self.logger.error(f"符号: {symbol}, 数据内容: {data}")

    async def _handle_user_data_update(self, data: Dict[str, Any]) -> None:
        """处理用户数据更新（订单更新 + 持仓更新）"""
        try:
            subscriptions = getattr(self, '_ws_subscriptions', [])

            # 根据stream字段区分订单更新和持仓更新
            stream = data.get('stream', '')
            
            # 🔥 关键修复：提取内部的data字段（实际的订单/持仓数据）
            inner_data = data.get('data', data)  # 如果没有data嵌套，则直接使用原数据
            event_type = inner_data.get('e', '')  # 从内部数据获取事件类型

            # 🔥 检查是否是订单更新
            if 'order' in stream.lower() and 'position' not in stream.lower():
                # 订单更新 - 传递内部数据
                await self._handle_order_update(inner_data)
            
            # 检查是否是持仓更新
            elif 'position' in stream.lower() or ('position' in event_type.lower()):
                # 持仓更新 - 传递内部数据
                await self._handle_position_update(inner_data)

            # 调用用户数据回调函数（兼容旧逻辑）
            for sub_type, sub_symbol, callback in subscriptions:
                if sub_type == 'user_data':
                    await self._safe_callback(callback, data)

        except Exception as e:
            if self.logger:
                self.logger.error(f"处理用户数据更新失败: {e}")
                self.logger.error(f"数据内容: {data}")

    async def _handle_order_update(self, data: Dict[str, Any]) -> None:
        """
        处理订单更新（WebSocket推送）
        
        ✅ Backpack订单更新格式（基于官方文档）：
        {
          "e": "orderAccepted",       // Event type: orderAccepted/orderFill/orderCancelled/orderExpired/orderModified
          "E": 1694687692980000,      // Event time in microseconds
          "s": "SOL_USD",             // Symbol
          "c": 123,                   // Client order ID (optional)
          "S": "Bid",                 // Side: "Bid" or "Ask"
          "o": "LIMIT",               // Order type: LIMIT/MARKET
          "f": "GTC",                 // Time in force
          "q": "32123",               // Quantity
          "p": "20",                  // Price
          "X": "Filled",              // Order state: Open/PartiallyFilled/Filled/Cancelled
          "i": "1111343026172067",    // Order ID
          "t": 567,                   // Trade ID (only on orderFill)
          "l": "1.23",                // Fill quantity (only on orderFill)
          "z": "321",                 // Executed quantity
          "L": "20",                  // Fill price (only on orderFill)
          "m": true,                  // Maker (only on orderFill)
          "n": "23",                  // Fee (only on orderFill)
          "N": "USD",                 // Fee symbol (only on orderFill)
          "T": 1694687692989999       // Engine timestamp
        }
        """
        try:
            if self.logger:
                self.logger.debug(f"📝 [Backpack] 收到订单更新: {data}")
            
            # ✅ 数据直接在顶层（不是嵌套在data字段）
            event_type = data.get('e', '')  # orderAccepted/orderFill/orderCancelled等
            
            # 解析订单数据
            symbol = data.get('s', '')
            order_id = str(data.get('i', ''))
            client_order_id = str(data.get('c', '')) if data.get('c') else ''
            
            # ✅ 订单方向：Bid/Ask -> BUY/SELL
            side_str = data.get('S', 'Bid')
            side = OrderSide.BUY if side_str == 'Bid' else OrderSide.SELL
            
            # 订单类型
            type_str = data.get('o', 'LIMIT')
            order_type = OrderType.LIMIT if type_str == 'LIMIT' else OrderType.MARKET
            
            # 数量和价格
            quantity = self._safe_decimal(data.get('q', 0))
            price = self._safe_decimal(data.get('p', 0))
            
            # ✅ 关键字段：订单状态和成交数量
            status_str = data.get('X', 'Open')
            filled_quantity = self._safe_decimal(data.get('z', 0))
            
            # ✅ 映射订单状态（首字母大写）
            status_mapping = {
                'OPEN': OrderStatus.OPEN,
                'PARTIALLYFILLED': OrderStatus.OPEN,  # 部分成交仍然是OPEN状态
                'FILLED': OrderStatus.FILLED,
                'CANCELED': OrderStatus.CANCELED,
                'CANCELLED': OrderStatus.CANCELED,
                'REJECTED': OrderStatus.CANCELED
            }
            status = status_mapping.get(status_str.upper().replace('_', ''), OrderStatus.OPEN)
            
            # 构造OrderData对象
            order = OrderData(
                id=order_id,
                client_id=client_order_id,  # 🔥 修复：使用client_id而不是client_order_id
                symbol=symbol,
                side=side,
                type=order_type,  # 🔥 修复：使用type而不是order_type
                amount=quantity,
                price=price,
                filled=filled_quantity,
                remaining=quantity - filled_quantity,
                cost=Decimal('0'),  # 🔥 修复：添加必需的cost字段
                average=price if filled_quantity > 0 else None,  # 🔥 修复：添加必需的average字段
                status=status,
                timestamp=datetime.now(),
                updated=None,  # 🔥 修复：添加必需的updated字段
                fee=None,  # 🔥 修复：添加必需的fee字段
                trades=[],  # 🔥 修复：添加必需的trades字段
                params={},  # 🔥 修复：添加必需的params字段
                raw_data=data
            )
            
            if self.logger:
                self.logger.info(
                    f"✅ [Backpack订单] {event_type} | id={order_id} | {symbol} "
                    f"{side.value}/{status.value} {filled_quantity}/{quantity}"
                )
            
            # 🔥 触发订单更新回调
            if hasattr(self, '_order_callbacks') and self._order_callbacks:
                if self.logger:
                    self.logger.info(
                        f"🔍 [Backpack] 准备调用 {len(self._order_callbacks)} 个订单回调: order_id={order_id}"
                    )
                for callback in self._order_callbacks:
                    await self._safe_callback(callback, order)
            
            # 🔥 如果订单完全成交，触发订单成交回调
            # 也可以通过event_type == 'orderFill' 判断
            if (status == OrderStatus.FILLED or event_type == 'orderFill') and hasattr(self, '_order_fill_callbacks'):
                for callback in self._order_fill_callbacks:
                    await self._safe_callback(callback, order)
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ [Backpack] 处理订单更新失败: {e}", exc_info=True)
                self.logger.error(f"   数据内容: {data}")
    
    async def _handle_position_update(self, data: Dict[str, Any]) -> None:
        """
        处理持仓更新（WebSocket推送）

        ✅ Backpack持仓更新格式（基于官方文档）：
        {
          "e": "positionOpened",      // Event type: positionOpened/positionAdjusted/positionClosed
          "E": 1694687692980000,      // Event time in microseconds
          "s": "SOL_USDC_PERP",       // Symbol
          "b": 123,                   // Break event price
          "B": 122,                   // Entry price
          "f": 0.5,                   // Initial margin fraction
          "M": 122,                   // Mark price
          "m": 0.01,                  // Maintenance margin fraction
          "q": 5,                     // Net quantity (正数=Long, 负数=Short)
          "Q": 6,                     // Net exposure quantity
          "n": 732,                   // Net exposure notional
          "i": "1111343026172067",    // Position ID
          "p": "-1",                  // PnL realized
          "P": "0",                   // PnL unrealized
          "T": 1694687692989999       // Engine timestamp in microseconds
        }

        🔥 关键：q字段（Net quantity）决定持仓方向：
        - q > 0 → Long（多仓）
        - q < 0 → Short（空仓）
        - q = 0 → 无持仓
        """
        try:
            if self.logger:
                self.logger.info(f"📊 [Backpack] 收到持仓更新: {data}")

            # ✅ 数据直接在顶层（不是嵌套在data字段）
            event_type = data.get('e', '')  # positionOpened/positionAdjusted/positionClosed
            
            # 解析持仓数据
            symbol = data.get('s', '')
            position_id = str(data.get('i', ''))

            # ✅ 关键字段：使用小写 'q' 字段（带符号的持仓数量）
            quantity = self._safe_decimal(data.get('q', 0))
            entry_price = self._safe_decimal(data.get('B', 0))  # Entry price
            mark_price = self._safe_decimal(data.get('M', 0))   # Mark price
            realized_pnl = self._safe_decimal(data.get('p', 0))  # PnL realized (小写p)
            unrealized_pnl = self._safe_decimal(data.get('P', 0))  # PnL unrealized (大写P)

            # ✅ 根据 q 字段的符号判断持仓方向
            if quantity > 0:
                side = 'Long'
            elif quantity < 0:
                side = 'Short'
            else:
                side = 'None'

            # 🔥 缓存持仓信息（供剥头皮模式使用）
            if not hasattr(self, '_position_cache'):
                self._position_cache = {}

            self._position_cache[symbol] = {
                'size': quantity,
                'entry_price': entry_price,
                'mark_price': mark_price,
                'realized_pnl': realized_pnl,
                'unrealized_pnl': unrealized_pnl,
                'side': side,
                'timestamp': datetime.now()
            }

            if self.logger:
                self.logger.info(
                    f"💰 [Backpack持仓] event={event_type}, symbol={symbol}, side={side}, "
                    f"size={quantity}, entry_price={entry_price}, pnl={unrealized_pnl}"
                )

            # 🔥 触发持仓更新回调
            if hasattr(self, '_position_callbacks') and self._position_callbacks:
                for callback in self._position_callbacks:
                    await self._safe_callback(callback, {
                        'symbol': symbol,
                        'position_id': position_id,
                        'side': side,
                        'size': quantity,
                        'entry_price': entry_price,
                        'mark_price': mark_price,
                        'realized_pnl': realized_pnl,
                        'unrealized_pnl': unrealized_pnl,
                        'event_type': event_type,
                        'timestamp': datetime.now()
                    })


        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ [Backpack] 处理持仓更新失败: {e}", exc_info=True)
                self.logger.error(f"   数据内容: {data}")

    async def _safe_callback(self, callback: Callable, data: Any) -> None:
        """安全调用回调函数"""
        try:
            if callback:
                if asyncio.iscoroutinefunction(callback):
                    await callback(data)
                else:
                    callback(data)
        except Exception as e:
            if self.logger:
                self.logger.error(
                    f"❌ [Backpack] 回调函数执行失败: {e}", 
                    exc_info=True
                )

    async def _safe_callback_with_symbol(self, callback: Callable, symbol: str, data: Any) -> None:
        """安全调用需要symbol参数的回调函数"""
        try:
            if callback:
                if asyncio.iscoroutinefunction(callback):
                    await callback(symbol, data)
                else:
                    callback(symbol, data)
        except Exception as e:
            if self.logger:
                self.logger.error(
                    f"❌ [Backpack] 回调函数执行失败: {e}", 
                    exc_info=True
                )

    # === 订阅接口 ===

    async def subscribe_order_updates(self, callback: Callable) -> None:
        """
        订阅订单更新流（异步回调）
        
        注意：account.orderUpdate 流已经在连接时自动订阅
        这个方法只是注册回调，确保订单更新能触发回调
        
        Args:
            callback: 订单更新回调函数，接收参数：OrderData对象
        """
        if not hasattr(self, '_order_callbacks'):
            self._order_callbacks = []
        
        self._order_callbacks.append(callback)
        
        if self.logger:
            self.logger.info(
                f"✅ 订单更新回调已注册\n"
                f"   注意：订单更新流已在连接时自动订阅"
            )
    
    async def subscribe_order_fills(self, callback: Callable) -> None:
        """
        订阅订单成交流（只在订单完全成交时触发）
        
        Args:
            callback: 订单成交回调函数，接收参数：OrderData对象（status=FILLED）
        """
        if not hasattr(self, '_order_fill_callbacks'):
            self._order_fill_callbacks = []
        
        self._order_fill_callbacks.append(callback)
        
        if self.logger:
            self.logger.info(
                f"✅ 订单成交回调已注册\n"
                f"   注意：只在订单完全成交时触发"
            )
    
    async def subscribe_position_updates(self, symbol: str, callback: Callable) -> None:
        """
        订阅持仓更新流（异步回调）

        注意：account.positionUpdate 流已经在 subscribe_user_data 中订阅
        这个方法只是注册回调，确保持仓更新能触发回调

        Args:
            symbol: 交易对
            callback: 持仓更新回调函数，接收参数：
                {
                    'symbol': str,
                    'size': Decimal,  # 带符号，正数=多仓，负数=空仓
                    'entry_price': Decimal,
                    'unrealized_pnl': Decimal,
                    'side': str  # 'Long' or 'Short'
                }
        """
        if not hasattr(self, '_position_callbacks'):
            self._position_callbacks = []

        self._position_callbacks.append(callback)

        if self.logger:
            self.logger.info(
                f"✅ 持仓更新回调已注册: {symbol}\n"
                f"   注意：持仓更新流已在subscribe_user_data中订阅"
            )

        # 🔥 如果缓存中已有数据，立即触发一次回调（同步初始状态）
        if hasattr(self, '_position_cache') and symbol in self._position_cache:
            cached_pos = self._position_cache[symbol]
            try:
                await callback({
                    'symbol': symbol,
                    'size': cached_pos['size'],
                    'entry_price': cached_pos['entry_price'],
                    'unrealized_pnl': cached_pos.get('unrealized_pnl', Decimal('0')),
                    'side': cached_pos.get('side', 'None')
                })
                if self.logger:
                    self.logger.info(
                        f"📊 从缓存立即同步初始持仓: {symbol} "
                        f"数量={cached_pos['size']}, 成本=${cached_pos['entry_price']}"
                    )
            except Exception as e:
                if self.logger:
                    self.logger.error(f"❌ 立即同步缓存持仓失败: {e}")
        else:
            if self.logger:
                self.logger.info(
                    f"📊 持仓缓存暂无数据: {symbol}\n"
                    f"   将在收到WebSocket更新后自动同步"
                )

    async def subscribe_positions(self, callback: Callable) -> None:
        """
        订阅持仓更新（标准接口，与Lighter、EdgeX保持一致）
        
        注意：Backpack会通过subscribe_user_data订阅所有交易对的持仓更新，
        不需要像subscribe_position_updates那样指定单个交易对
        
        Args:
            callback: 持仓更新回调函数，接收参数：
                {
                    'symbol': str,
                    'size': Decimal,  # 带符号，正数=多仓，负数=空仓
                    'entry_price': Decimal,
                    'mark_price': Decimal,
                    'unrealized_pnl': Decimal,
                    'realized_pnl': Decimal,
                    'side': str  # 'Long' or 'Short' or 'None'
                }
        """
        if not hasattr(self, '_position_callbacks'):
            self._position_callbacks = []
        
        self._position_callbacks.append(callback)
        
        if self.logger:
            self.logger.info("✅ 持仓更新回调已注册（标准接口）")
            self.logger.info("   注意：持仓更新流已在subscribe_user_data中订阅")

    async def subscribe_ticker(self, symbol: str, callback: Callable[[TickerData], None]) -> None:
        """订阅行情数据流"""
        try:
            # 检查是否为黑名单交易对
            if self.is_websocket_blacklisted(symbol):
                if self.logger:
                    self.logger.warning(f"🚫 跳过黑名单交易对: {symbol}")
                return

            # 🔥 移除旧的同类型订阅，避免重复回调
            self._ws_subscriptions = [
                (sub_type, sym, cb) for sub_type, sym, cb in self._ws_subscriptions
                if not (sub_type == 'ticker' and sym == symbol)
            ]

            self._ws_subscriptions.append(('ticker', symbol, callback))

            # 修复：单独订阅时也要添加到_subscribed_symbols
            if not hasattr(self, '_subscribed_symbols'):
                self._subscribed_symbols = set()
            self._subscribed_symbols.add(symbol)

            # 🔥 同时订阅 ticker 和 markPrice（markPrice 包含资金费率）
            subscribe_msg = {
                "method": "SUBSCRIBE",
                "params": [
                    f"ticker.{symbol}",      # 价格数据
                    f"markPrice.{symbol}"    # 标记价格 + 资金费率
                ],
                "id": len(self._ws_subscriptions)
            }

            if await self._safe_send_message(json.dumps(subscribe_msg)):
                if self.logger:
                    self.logger.debug(f"✅ 已订阅 {symbol} 的 ticker + markPrice（包含资金费率）")
            else:
                if self.logger:
                    self.logger.warning(f"发送 {symbol} 订阅消息失败")

        except Exception as e:
            if self.logger:
                self.logger.warning(f"订阅ticker失败: {e}")

    async def subscribe_orderbook(self, symbol: str, callback: Callable[[OrderBookData], None]) -> None:
        """订阅订单簿数据流"""
        try:
            # 检查是否为黑名单交易对
            if self.is_websocket_blacklisted(symbol):
                if self.logger:
                    self.logger.warning(f"🚫 跳过黑名单交易对: {symbol}")
                return

            # 🔥 移除旧的同类型订阅，避免重复回调
            self._ws_subscriptions = [
                (sub_type, sym, cb) for sub_type, sym, cb in self._ws_subscriptions
                if not (sub_type == 'orderbook' and sym == symbol)
            ]

            self._ws_subscriptions.append(('orderbook', symbol, callback))

            # 修复：单独订阅时也要添加到_subscribed_symbols
            if not hasattr(self, '_subscribed_symbols'):
                self._subscribed_symbols = set()
            self._subscribed_symbols.add(symbol)

            subscribe_msg = {
                "method": "SUBSCRIBE",
                "params": [f"depth.{symbol}"],
                "id": len(self._ws_subscriptions)
            }

            if await self._safe_send_message(json.dumps(subscribe_msg)):
                if self.logger:
                    self.logger.debug(f"已订阅 {symbol} 的orderbook (单独订阅)")
            else:
                if self.logger:
                    self.logger.warning(f"发送 {symbol} orderbook订阅消息失败")

        except Exception as e:
            if self.logger:
                self.logger.warning(f"订阅orderbook失败: {e}")

    async def subscribe_trades(self, symbol: str, callback: Callable[[TradeData], None]) -> None:
        """订阅成交数据流"""
        try:
            # 检查是否为黑名单交易对
            if self.is_websocket_blacklisted(symbol):
                if self.logger:
                    self.logger.warning(f"🚫 跳过黑名单交易对: {symbol}")
                return

            # 🔥 移除旧的同类型订阅，避免重复回调
            self._ws_subscriptions = [
                (sub_type, sym, cb) for sub_type, sym, cb in self._ws_subscriptions
                if not (sub_type == 'trades' and sym == symbol)
            ]

            self._ws_subscriptions.append(('trades', symbol, callback))

            # 修复：单独订阅时也要添加到_subscribed_symbols
            if not hasattr(self, '_subscribed_symbols'):
                self._subscribed_symbols = set()
            self._subscribed_symbols.add(symbol)

            subscribe_msg = {
                "method": "SUBSCRIBE",
                "params": [f"trade.{symbol}"],
                "id": len(self._ws_subscriptions)
            }

            if await self._safe_send_message(json.dumps(subscribe_msg)):
                if self.logger:
                    self.logger.debug(f"已订阅 {symbol} 的trades (单独订阅)")
            else:
                if self.logger:
                    self.logger.warning(f"发送 {symbol} trades订阅消息失败")

        except Exception as e:
            if self.logger:
                self.logger.warning(f"订阅trades失败: {e}")

    def _sign_message_for_subscription(self, message: str) -> str:
        """
        为WebSocket订阅生成ED25519签名

        Args:
            message: 要签名的消息字符串

        Returns:
            Base64编码的签名
        """
        import base64
        from nacl.signing import SigningKey
        from nacl.encoding import Base64Encoder

        # 从config获取私钥（兼容 private_key 和 api_secret 字段名）
        private_key = None
        if self.config:
            if hasattr(self.config, 'private_key') and self.config.private_key:
                private_key = self.config.private_key
            elif hasattr(self.config, 'api_secret') and self.config.api_secret:
                private_key = self.config.api_secret
        
        if not private_key:
            raise ValueError("API私钥未配置")

        # 解码私钥
        private_key_bytes = base64.b64decode(private_key)
        signing_key = SigningKey(private_key_bytes)

        # 签名
        signed = signing_key.sign(message.encode(), encoder=Base64Encoder)
        return signed.signature.decode()

    async def subscribe_user_data(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """
        订阅用户数据流（订单更新 + 持仓更新）

        参考: 
        - https://docs.backpack.exchange/#tag/Streams/Private/Order-update
        - https://docs.backpack.exchange/#tag/Streams/Private/Position-update
        """
        try:
            # 🔥 移除旧的 user_data 订阅，避免重复回调
            self._ws_subscriptions = [
                (sub_type, symbol, cb) for sub_type, symbol, cb in self._ws_subscriptions
                if sub_type != 'user_data'
            ]

            # 记录回调，后续收到 account.* 推送时会统一触发
            self.user_data_callback = callback
            if not hasattr(self, '_user_data_callbacks'):
                self._user_data_callbacks = []
            if callback and callback not in self._user_data_callbacks:
                self._user_data_callbacks.append(callback)

            # 添加新的订阅
            self._ws_subscriptions.append(('user_data', None, callback))

            # 生成签名
            timestamp = int(time.time() * 1000)
            window = 5000

            # 签名字符串: instruction=subscribe&timestamp=<timestamp>&window=<window>
            sign_string = f"instruction=subscribe&timestamp={timestamp}&window={window}"
            signature = self._sign_message_for_subscription(sign_string)

            # 获取API Key
            if not self.config or not hasattr(self.config, 'api_key') or not self.config.api_key:
                raise ValueError("API Key未配置")

            # 🔥 订阅订单更新流
            subscribe_order_msg = {
                "method": "SUBSCRIBE",
                "params": ["account.orderUpdate"],  # 订单更新
                "signature": [self.config.api_key, signature, str(timestamp), str(window)]
            }

            if self.logger:
                self.logger.info(
                    f"订阅订单更新流: account.orderUpdate "
                    f"(timestamp={timestamp})"
                )

            if await self._safe_send_message(json.dumps(subscribe_order_msg)):
                if self.logger:
                    self.logger.info("✅ 订单更新流订阅请求已发送")
            else:
                if self.logger:
                    self.logger.warning("发送订单更新订阅消息失败")

            # 🔥 订阅持仓更新流（重要！剥头皮模式需要实时持仓）
            # 需要重新生成签名（时间戳可能变化）
            timestamp2 = int(time.time() * 1000)
            sign_string2 = f"instruction=subscribe&timestamp={timestamp2}&window={window}"
            signature2 = self._sign_message_for_subscription(sign_string2)

            subscribe_position_msg = {
                "method": "SUBSCRIBE",
                "params": ["account.positionUpdate"],  # 持仓更新
                "signature": [self.config.api_key, signature2, str(timestamp2), str(window)]
            }

            if self.logger:
                self.logger.info(
                    f"订阅持仓更新流: account.positionUpdate "
                    f"(timestamp={timestamp2})"
                )

            if await self._safe_send_message(json.dumps(subscribe_position_msg)):
                if self.logger:
                    self.logger.info("✅ 持仓更新流订阅请求已发送")
            else:
                if self.logger:
                    self.logger.warning("发送持仓更新订阅消息失败")

        except Exception as e:
            if self.logger:
                self.logger.error(f"订阅用户数据流失败: {e}")
                import traceback
                self.logger.error(traceback.format_exc())

    async def unsubscribe_user_data(self) -> None:
        """取消用户数据流订阅，清理回调"""
        try:
            # 清理本地回调
            if hasattr(self, '_user_data_callbacks'):
                self._user_data_callbacks.clear()
            self.user_data_callback = None

            # 移除缓存的订阅记录
            self._ws_subscriptions = [
                (sub_type, symbol, cb) for sub_type, symbol, cb in self._ws_subscriptions
                if sub_type != 'user_data'
            ]

            # 通知服务器取消订阅
            timestamp = int(time.time() * 1000)
            window = 5000
            sign_string = f"instruction=unsubscribe&timestamp={timestamp}&window={window}"
            signature = self._sign_message_for_subscription(sign_string)

            unsubscribe_msg = {
                "method": "UNSUBSCRIBE",
                "params": ["account.orderUpdate"],
                "signature": [self.config.api_key, signature, str(timestamp), str(window)]
            }
            await self._safe_send_message(json.dumps(unsubscribe_msg))

            timestamp2 = int(time.time() * 1000)
            sign_string2 = f"instruction=unsubscribe&timestamp={timestamp2}&window={window}"
            signature2 = self._sign_message_for_subscription(sign_string2)
            unsubscribe_position_msg = {
                "method": "UNSUBSCRIBE",
                "params": ["account.positionUpdate"],
                "signature": [self.config.api_key, signature2, str(timestamp2), str(window)]
            }
            await self._safe_send_message(json.dumps(unsubscribe_position_msg))

            if self.logger:
                self.logger.info("✅ 已发送Backpack用户数据流取消订阅请求")

        except Exception as e:
            if self.logger:
                self.logger.warning(f"取消Backpack用户数据订阅失败: {e}")

    async def batch_subscribe_tickers(self, symbols: Optional[List[str]] = None, callback: Optional[Callable[[str, TickerData], None]] = None) -> None:
        """批量订阅多个交易对的ticker数据 - 使用完整符号格式"""
        try:
            # 如果未指定symbols，使用所有支持的交易对
            if symbols is None:
                symbols = await self.get_supported_symbols()

            # 过滤掉黑名单中的交易对，并将标准符号映射为Backpack格式
            mapped_symbols = []
            for s in symbols:
                mapped = self._map_symbol(s)
                if mapped:
                    mapped_symbols.append(mapped)
            original_count = len(mapped_symbols)
            symbols = self.filter_websocket_symbols(mapped_symbols)

            if self.logger:
                if original_count > len(symbols):
                    self.logger.info(
                        f"开始批量订阅 {len(symbols)} 个交易对的ticker数据 (已过滤 {original_count - len(symbols)} 个黑名单交易对)")
                else:
                    self.logger.info(
                        f"开始批量订阅 {len(symbols)} 个交易对的ticker数据 (使用完整符号格式)")

            # 记录订阅的符号（用于数据映射）
            self._subscribed_symbols = set(symbols)

            # 逐个发送订阅消息（使用完整符号格式）
            successful_subscriptions = 0
            for i, symbol in enumerate(symbols):
                try:
                    # 🔥 同时订阅 ticker 和 markPrice（markPrice 包含资金费率）
                    subscribe_msg = {
                        "method": "SUBSCRIBE",
                        "params": [
                            f"ticker.{symbol}",      # 价格数据
                            f"markPrice.{symbol}"    # 标记价格 + 资金费率
                        ],
                        "id": i + 1
                    }

                    if await self._safe_send_message(json.dumps(subscribe_msg)):
                        if self.logger:
                            self.logger.debug(f"✅ 已订阅: ticker + markPrice for {symbol}")
                        successful_subscriptions += 1

                        # 小延迟避免过快
                        await asyncio.sleep(0.1)

                except Exception as e:
                    if self.logger:
                        self.logger.error(f"订阅 {symbol} 时出错: {e}")
                    continue

            if self.logger:
                self.logger.info(
                    f"🎯 已发送 {successful_subscriptions}/{len(symbols)} 个订阅消息 (完整符号格式)")
                self.logger.info("🎯 开始监听数据流（Backpack无订阅确认）")

            # 如果提供了回调函数，保存它
            if callback:
                self.ticker_callback = callback

            if self.logger:
                self.logger.info(f"✅ 批量ticker订阅完成")

        except Exception as e:
            if self.logger:
                self.logger.error(f"批量订阅ticker时出错: {e}")

    async def batch_subscribe_orderbooks(self, symbols: Optional[List[str]] = None,
                                         callback: Optional[Callable[[str, OrderBookData], None]] = None) -> None:
        """批量订阅多个交易对的订单簿数据"""
        try:
            # 如果未指定symbols，使用所有支持的交易对
            if symbols is None:
                symbols = await self.get_supported_symbols()

            # 过滤掉黑名单中的交易对，并将标准符号映射为Backpack格式
            mapped_symbols = []
            for s in symbols:
                mapped = self._map_symbol(s)
                if mapped:
                    mapped_symbols.append(mapped)
            original_count = len(mapped_symbols)
            symbols = self.filter_websocket_symbols(mapped_symbols)

            if self.logger:
                if original_count > len(symbols):
                    self.logger.info(
                        f"开始批量订阅 {len(symbols)} 个交易对的订单簿数据 (已过滤 {original_count - len(symbols)} 个黑名单交易对)")
                else:
                    self.logger.info(f"开始批量订阅 {len(symbols)} 个交易对的订单簿数据")

            # 批量订阅订单簿
            for symbol in symbols:
                try:
                    # 订阅orderbook数据
                    subscribe_msg = {
                        "method": "SUBSCRIBE",
                        "params": [f"depth.{symbol}"],
                        "id": len(self._ws_subscriptions) + 1
                    }

                    if await self._safe_send_message(json.dumps(subscribe_msg)):
                        if self.logger:
                            self.logger.debug(f"已订阅 {symbol} 的订单簿")

                    # 小延迟避免过于频繁的请求
                    await asyncio.sleep(0.1)

                except Exception as e:
                    if self.logger:
                        self.logger.error(f"订阅 {symbol} 订单簿时出错: {e}")
                    continue

            # 如果提供了回调函数，保存它
            if callback:
                self.orderbook_callback = callback

            if self.logger:
                self.logger.info(f"批量订单簿订阅完成")

        except Exception as e:
            if self.logger:
                self.logger.error(f"批量订阅订单簿时出错: {e}")

    async def unsubscribe(self, symbol: Optional[str] = None) -> None:
        """取消订阅"""
        try:
            if symbol:
                # 取消特定符号的订阅
                subscriptions_to_remove = []
                for sub_type, sub_symbol, callback in self._ws_subscriptions:
                    if sub_symbol == symbol:
                        subscriptions_to_remove.append(
                            (sub_type, sub_symbol, callback))

                for sub in subscriptions_to_remove:
                    self._ws_subscriptions.remove(sub)
            else:
                # 取消所有订阅
                self._ws_subscriptions.clear()

        except Exception as e:
            if self.logger:
                self.logger.warning(f"取消订阅失败: {e}")

    async def get_supported_symbols(self) -> List[str]:
        """获取支持的交易对列表"""
        if not self._supported_symbols:
            await self._use_default_symbols()
        return self._supported_symbols.copy()

    # === 向后兼容方法 ===

    async def batch_subscribe_all_tickers(self, callback: Optional[Callable[[str, TickerData], None]] = None) -> None:
        """批量订阅所有支持交易对的ticker数据"""
        try:
            # 获取所有支持的交易对
            symbols = await self.get_supported_symbols()
            if self.logger:
                self.logger.info(f"开始批量订阅所有 {len(symbols)} 个交易对的ticker数据")

            # 使用batch_subscribe_tickers方法
            await self.batch_subscribe_tickers(symbols, callback)

            if self.logger:
                self.logger.info(f"✅ 已成功批量订阅所有ticker数据")

        except Exception as e:
            if self.logger:
                self.logger.error(f"批量订阅所有ticker数据失败: {e}")
            raise

    async def unsubscribe_all(self) -> None:
        """取消所有订阅"""
        try:
            # 清空所有订阅
            self._ws_subscriptions.clear()
            self._subscribed_symbols.clear()

            if self.logger:
                self.logger.info("已取消所有Backpack订阅")

        except Exception as e:
            if self.logger:
                self.logger.warning(f"取消所有Backpack订阅失败: {e}")

    async def fetch_supported_symbols(self) -> None:
        """通过API获取支持的交易对 - 🔥 修改：只获取永续合约"""
        try:
            if self.logger:
                self.logger.info("开始获取Backpack支持的交易对列表...")

            # 调用市场API获取所有交易对
            if hasattr(self, '_session') and self._session:
                async with self._session.get(f"{self.base_url}api/v1/markets") as response:
                    if response.status == 200:
                        markets_data = await response.json()

                        supported_symbols = []
                        market_info = {}

                        # 统计数据
                        total_markets = len(markets_data)
                        perpetual_count = 0
                        spot_count = 0

                        for market in markets_data:
                            symbol = market.get("symbol")
                            if symbol:
                                # 🔥 修改：只获取永续合约，排除现货
                                if symbol.endswith('_PERP'):
                                    # 永续合约
                                    normalized_symbol = self._normalize_backpack_symbol(
                                        symbol)
                                    supported_symbols.append(normalized_symbol)
                                    market_info[normalized_symbol] = market
                                    perpetual_count += 1

                                    if self.logger:
                                        self.logger.debug(
                                            f"添加永续合约: {normalized_symbol}")
                                else:
                                    # 现货交易对 - 跳过
                                    spot_count += 1
                                    if self.logger:
                                        self.logger.debug(f"跳过现货交易对: {symbol}")

                        self._supported_symbols = supported_symbols
                        self._market_info = market_info

                        if self.logger:
                            self.logger.info(f"✅ Backpack WebSocket市场数据统计:")
                            self.logger.info(f"  - 总市场数量: {total_markets}")
                            self.logger.info(f"  - 永续合约: {perpetual_count}")
                            self.logger.info(f"  - 现货交易对: {spot_count} (已跳过)")
                            self.logger.info(
                                f"  - 最终可用: {len(supported_symbols)} 个永续合约")

                    else:
                        if self.logger:
                            self.logger.error(f"获取市场数据失败: {response.status}")
                        await self._use_default_symbols()

        except Exception as e:
            if self.logger:
                self.logger.error(f"获取支持的交易对时出错: {e}")
            await self._use_default_symbols()

    def _cache_orderbook_data(self, symbol: str, bids: List[OrderBookLevel], asks: List[OrderBookLevel], timestamp: datetime) -> None:
        """缓存最新的orderbook数据供ticker使用"""
        try:
            # 只保留前5档买卖盘数据，减少内存占用
            best_bids = bids[:5] if bids else []
            best_asks = asks[:5] if asks else []

            self._latest_orderbooks[symbol] = {
                'bids': best_bids,
                'asks': best_asks,
                'timestamp': timestamp,
                'cache_time': time.time()
            }

            # 定期清理过期缓存
            self._cleanup_expired_orderbook_cache()

        except Exception as e:
            if self.logger:
                self.logger.warning(f"缓存orderbook数据失败: {e}")

    def _cleanup_expired_orderbook_cache(self) -> None:
        """清理过期的orderbook缓存"""
        try:
            current_time = time.time()
            expired_symbols = []

            for symbol, cache_data in self._latest_orderbooks.items():
                if current_time - cache_data.get('cache_time', 0) > self._orderbook_cache_timeout:
                    expired_symbols.append(symbol)

            for symbol in expired_symbols:
                del self._latest_orderbooks[symbol]

        except Exception as e:
            if self.logger:
                self.logger.warning(f"清理过期orderbook缓存失败: {e}")

    def _get_best_bid_ask_from_cache(self, symbol: str) -> tuple[Optional[Decimal], Optional[Decimal], Optional[Decimal], Optional[Decimal]]:
        """从缓存的orderbook数据中获取最佳买卖价格和数量

        Returns:
            tuple: (bid_price, ask_price, bid_size, ask_size)
        """
        try:
            if symbol not in self._latest_orderbooks:
                return None, None, None, None

            cache_data = self._latest_orderbooks[symbol]

            # 检查缓存是否过期
            if time.time() - cache_data.get('cache_time', 0) > self._orderbook_cache_timeout:
                return None, None, None, None

            bid_price = bid_size = ask_price = ask_size = None

            # 获取最佳买价和数量
            bids = cache_data.get('bids', [])
            if bids:
                # 找到第一个有效的买单（数量大于0）
                for bid in bids:
                    if bid.size > 0:
                        bid_price = bid.price
                        bid_size = bid.size
                        break

            # 获取最佳卖价和数量
            asks = cache_data.get('asks', [])
            if asks:
                # 找到第一个有效的卖单（数量大于0）
                for ask in asks:
                    if ask.size > 0:
                        ask_price = ask.price
                        ask_size = ask.size
                        break

            return bid_price, ask_price, bid_size, ask_size

        except Exception as e:
            if self.logger:
                self.logger.warning(f"从orderbook缓存获取最佳价格失败: {e}")
            return None, None, None, None
