"""
EdgeX WebSocket模块

包含WebSocket连接管理、数据订阅、消息处理、实时数据解析等功能

重构说明（2025-11-11）：
- 添加官方SDK WebSocketManager支持（可选）
- 保留现有HTTP WebSocket实现（向后兼容）
- 改进自动重连机制（参考文档）
- 确保监控系统接口兼容性
"""

import asyncio
import time
import json
import aiohttp
import logging
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
from decimal import Decimal
from datetime import datetime
from pathlib import Path
from logging.handlers import RotatingFileHandler

# 尝试导入官方SDK
try:
    from edgex_sdk import WebSocketManager
    EDGEX_SDK_AVAILABLE = True
except ImportError:
    EDGEX_SDK_AVAILABLE = False
    WebSocketManager = None

from .edgex_base import EdgeXBase
from ..models import (
    TickerData,
    OrderBookData,
    TradeData,
    OrderBookLevel,
    BalanceData,
    PositionData,
    PositionSide,
    MarginMode,
)


class EdgeXWebSocket(EdgeXBase):
    """EdgeX WebSocket接口"""
    
    # metadata日志示例数量上限，避免在INFO级别输出上百条
    _METADATA_LOG_SAMPLE_LIMIT: int = 15

    def __init__(self, config=None, logger=None):
        super().__init__(config)
        if logger is None:
            self.logger = logging.getLogger("ExchangeAdapter.edgex")
        elif hasattr(logger, "logger"):
            self.logger = logger.logger
        else:
            self.logger = logger
        # 🔥 确保logger有文件handler（与Lighter保持一致）
        self._setup_logger()
        
        # 🔥 EdgeX: 如果有认证信息，使用私有WebSocket
        import os
        # 从配置或环境变量读取认证信息
        self.account_id = None
        self.stark_private_key = None
        
        if config and hasattr(config, 'authentication'):
            # 从配置读取
            auth = config.authentication
            self.account_id = getattr(auth, 'account_id', None)
            self.stark_private_key = getattr(auth, 'stark_private_key', None)
        # 如果配置中没有，尝试从环境变量读取
        if not self.account_id:
            self.account_id = os.getenv('EDGEX_ACCOUNT_ID')
        if not self.stark_private_key:
            self.stark_private_key = os.getenv('EDGEX_STARK_PRIVATE_KEY')
        
        # 🔥 统一记录WebSocket类型
        if self.account_id and self.stark_private_key:
            # 有认证信息：使用私有WebSocket
            if config and hasattr(config, 'private_ws_url') and config.private_ws_url:
                self.ws_url = config.private_ws_url
            else:
                self.ws_url = "wss://pro.edgex.exchange/api/v1/private/ws"  # 私有WebSocket默认URL
            if self.logger:
                self.logger.info(
                    "[EdgeX] WebSocket: 使用私有连接（支持订单/持仓推送） -> %s",
                    self.ws_url
                )
        else:
            # 无认证信息：使用公共WebSocket
            if config and hasattr(config, 'ws_url') and config.ws_url:
                self.ws_url = config.ws_url
            else:
                self.ws_url = self.DEFAULT_WS_URL
            if self.logger:
                self.logger.info("[EdgeX] WebSocket: 使用公共连接（仅市场数据）")
        self._ws_connection = None
        self._ws_subscriptions = []
        self.ticker_callback = None
        self.orderbook_callback = None
        self.trades_callback = None
        self.user_data_callback = None  # 向后兼容：单个用户数据回调
        
        # 🔥 订单和持仓回调
        self._order_callbacks = []  # 订单更新回调函数列表
        self._order_fill_callbacks = []  # 订单成交回调函数列表
        self._position_callbacks = []  # 持仓更新回调函数列表
        self._user_data_callbacks = []  # 通用用户数据回调函数列表（支持多个回调）
        
        # 🔥 本地订单簿缓存（用于处理增量更新）
        self._local_orderbooks: Dict[str, Dict[str, Any]] = {}  # {symbol: {bids: {price: size}, asks: {price: size}}}
        
        # 初始化状态变量
        self._ws_connected = False
        self._last_heartbeat = 0
        self._last_message_time: float = 0.0  # 最近一次接收到任何消息的时间
        self._last_business_message_time: float = 0.0  # 最近一次业务消息（行情/订单等）的时间
        self._reconnect_attempts = 0
        self._reconnecting = False
        # 🔥 重连次数统计（实际重连成功的次数）
        self._reconnect_count = 0  # 重连成功次数
        
        # 🔥 新增：失败计数器，避免立即重连
        self._ping_failure_count = 0
        self._connection_issue_count = 0
        
        # 🔥 心跳日志频率控制（每30秒记录一次）
        self._last_heartbeat_log_time: float = 0  # 上次心跳日志时间
        self._heartbeat_log_interval: float = 30.0  # 心跳日志间隔（秒）
        
        # 🔥 网络流量统计（字节数）
        self._network_bytes_received = 0
        self._network_bytes_sent = 0
        
        # 🔥 余额缓存（私有WS推送）
        self._balance_cache: Dict[str, BalanceData] = {}
        self._balance_cache_timestamp: float = 0.0
        self._last_balance_log_time: float = 0.0
        self._balance_log_interval: float = 30.0
        
        # 🔥 持仓缓存（私有WS推送）
        self._position_cache: Dict[str, PositionData] = {}
        self._position_cache_timestamp: float = 0.0
        self._last_position_log_time: float = 0.0
        self._position_log_interval: float = 30.0
        self._coin_currency_map: Dict[str, str] = {
            "1000": "USDC",
        }
        
        # 🔥 新增：官方SDK WebSocketManager支持（可选）
        self.ws_manager = None
        self._use_sdk = False
        self._ws_stop = asyncio.Event()
        self._ws_disconnected = asyncio.Event()
        self._ws_task = None
        self._event_loop = None
        
        # 🔥 初始化官方SDK WebSocketManager（如果SDK可用且有认证信息）
        # 使用已加载的self.account_id和self.stark_private_key（从config或环境变量）
        if EDGEX_SDK_AVAILABLE and self.account_id and self.stark_private_key:
            try:
                # WebSocketManager需要wss://开头的base_url（不包含路径）
                # 从 wss://pro.edgex.exchange/api/v1/private/ws 提取 wss://pro.edgex.exchange
                ws_base_url = self.ws_url.split('/api/v1/')[0]
                
                self.ws_manager = WebSocketManager(
                    base_url=ws_base_url,
                    account_id=int(self.account_id),
                    stark_pri_key=self.stark_private_key
                )
                self._use_sdk = True
                if self.logger:
                    self.logger.info("[EdgeX] WebSocket: SDK管理器初始化成功")
            except Exception as e:
                if self.logger:
                    self.logger.warning(f"⚠️ [EdgeX] 官方SDK WebSocketManager初始化失败，使用HTTP WebSocket: {e}")
                    import traceback
                    self.logger.warning(f"Traceback: {traceback.format_exc()}")
                self._use_sdk = False
        else:
            if self.logger:
                if not EDGEX_SDK_AVAILABLE:
                    self.logger.info("[EdgeX] WebSocket: SDK未安装，使用HTTP连接")
                elif not self.account_id or not self.stark_private_key:
                    self.logger.info("[EdgeX] WebSocket: 认证信息未配置，使用公共连接")

    async def _check_exchange_connectivity(self) -> bool:
        """检查交易所服务器连通性"""
        try:
            # 检查EdgeX的REST API是否可达 - 使用正确的官方端点
            api_url = "https://pro.edgex.exchange/"  # 正确的EdgeX官方端点
            timeout = aiohttp.ClientTimeout(total=8)
            
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(api_url) as response:
                    # 检查HTTP状态码，2xx和3xx都表示服务器可达
                    return response.status < 500  # 500以下状态码说明服务器可达
                    
        except Exception as e:
            if self.logger:
                self.logger.warning(f"⚠️ [EdgeX] 服务器连通性检查失败: {e}")
            return False

    def _is_connection_usable(self) -> bool:
        """检查WebSocket连接是否可用 - 简化版本，只检查连接对象状态"""
        # 基础检查
        if not (hasattr(self, '_ws_connection') and 
                self._ws_connection is not None and 
                not self._ws_connection.closed and
                getattr(self, '_ws_connected', False)):
            return False
        
        # 简单的异常检查
        try:
            if hasattr(self._ws_connection, 'exception') and self._ws_connection.exception():
                return False
        except Exception:
            pass
        
        return True
    
    def get_network_stats(self) -> Dict[str, int]:
        """获取网络流量统计"""
        return {
            'bytes_received': self._network_bytes_received,
            'bytes_sent': self._network_bytes_sent,
        }
    
    def get_reconnect_stats(self) -> Dict[str, int]:
        """获取重连统计"""
        return {
            'reconnect_count': self._reconnect_count,
        }
    
    async def _safe_send_message(self, message: str) -> bool:
        """安全发送WebSocket消息"""
        try:
            if not self._is_connection_usable():
                if self.logger:
                    self.logger.warning("⚠️ [EdgeX] WebSocket连接不可用，无法发送消息")
                return False
            
            # 🔥 统计发送的字节数
            message_bytes = len(message.encode('utf-8'))
            self._network_bytes_sent += message_bytes
            
            await self._ws_connection.send_str(message)
            return True
        except Exception as e:
            if self.logger:
                self.logger.warning(f"❌ [EdgeX] 发送WebSocket消息失败: {e}")
            return False

    async def _send_websocket_ping(self) -> None:
        """发送WebSocket ping消息保持连接活跃"""
        try:
            if self._is_connection_usable():
                # 使用aiohttp的内置ping方法
                await self._ws_connection.ping()
                # 🔥 ping成功，重置失败计数器
                self._ping_failure_count = 0
                if self.logger:
                    current_time = time.time()
                    if current_time - self._last_heartbeat_log_time >= self._heartbeat_log_interval:
                        self.logger.info("[EdgeX] 心跳: ping")
                        self._last_heartbeat_log_time = current_time
            else:
                # 🔥 ping检查失败，增加失败计数
                self._ping_failure_count += 1
                if self.logger:
                    self.logger.warning(f"⚠️ [EdgeX心跳] 无法发送ping，WebSocket连接不可用 (失败次数: {self._ping_failure_count})")
                # 🔥 优化：1次失败即触发重连（从2次降低，提高响应速度）
                if self._ping_failure_count >= 1:
                    self._ws_connected = False
                    stale_time = time.time() - 180  # 超过60秒阈值（触发重连条件）
                    self._last_heartbeat = stale_time
                    self._last_message_time = stale_time
                    self._last_business_message_time = stale_time
                    if self.logger:
                        self.logger.info(f"🔄 [EdgeX心跳] ping失败，触发重连 (失败次数: {self._ping_failure_count})")
        except Exception as e:
            # 🔥 ping异常，增加失败计数
            self._ping_failure_count += 1
            if self.logger:
                self.logger.error(f"❌ [EdgeX心跳] 发送ping失败: {str(e)} (失败次数: {self._ping_failure_count})")
            # 🔥 优化：1次失败即触发重连（从2次降低，提高响应速度）
            if self._ping_failure_count >= 1:
                self._ws_connected = False
                stale_time = time.time() - 180  # 超过60秒阈值（触发重连条件）
                self._last_heartbeat = stale_time
                self._last_message_time = stale_time
                self._last_business_message_time = stale_time
                if self.logger:
                    self.logger.info(f"🔄 [EdgeX心跳] ping异常，触发重连 (失败次数: {self._ping_failure_count})")

    def _calc_silence_time(self) -> Tuple[float, bool]:
        """
        计算距离最近一次业务/任意消息的静默时间
        
        Returns:
            (seconds, no_reference): no_reference=True 表示尚未收到任何消息
        """
        reference_time = self._last_business_message_time or self._last_message_time
        if reference_time <= 0:
            return 0.0, True
        return max(time.time() - reference_time, 0.0), False

    def _setup_logger(self):
        """设置logger的文件handler（与Lighter保持一致，不输出到终端）"""
        if not self.logger:
            return
        
        # 🔥 关键：阻止日志传播到父logger，避免输出到控制台
        self.logger.propagate = False
        
        # 🔥 移除所有StreamHandler（控制台输出），只保留文件输出
        handlers_to_remove = []
        for handler in self.logger.handlers:
            if isinstance(handler, logging.StreamHandler) and not isinstance(handler, RotatingFileHandler):
                handlers_to_remove.append(handler)
        for handler in handlers_to_remove:
            self.logger.removeHandler(handler)
        
        # 确保logs目录存在
        Path("logs").mkdir(parents=True, exist_ok=True)
        
        # 检查是否已有文件handler
        has_file_handler = any(
            isinstance(h, RotatingFileHandler) and 'ExchangeAdapter.log' in str(h.baseFilename)
            for h in self.logger.handlers
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
            self.logger.debug("[EdgeX] WebSocket: 日志文件handler已配置")

    async def connect(self) -> bool:
        """建立WebSocket连接"""
        try:
            if self.logger:
                self.logger.info(f"🔌 [EdgeX] 正在建立WebSocket连接: {self.ws_url}")
            
            # 🔥 EdgeX双连接模式：公共数据 + 私有数据
            # 1. 公共HTTP WebSocket → metadata, ticker, orderbook
            # 2. 私有SDK WebSocket → 订单/持仓（如果有认证）
            
            # 🔥 首先建立公共HTTP WebSocket（用于市场数据）
            public_ws_url = self.DEFAULT_WS_URL  # wss://quote.edgex.exchange/api/v1/public/ws
            
            if not hasattr(self, '_session') or (hasattr(self, '_session') and self._session.closed):
                self._session = aiohttp.ClientSession()
            
            # 🔥 连接公共WebSocket（用于metadata, ticker, orderbook）
            self._ws_connection = await self._session.ws_connect(public_ws_url)
            
            if self.logger:
                self.logger.info(f"✅ [EdgeX] 公共WebSocket已连接: {public_ws_url}")
            
            # 初始化状态
            self._ws_connected = True
            current_time = time.time()
            self._last_heartbeat = current_time
            self._last_message_time = current_time
            self._last_business_message_time = current_time
            self._reconnect_attempts = 0
            self._reconnecting = False
            
            # 🔥 新增：初始化ping/pong时间戳
            self._last_ping_time = current_time
            self._last_pong_time = current_time
            
            # 🔥 重置失败计数器
            self._ping_failure_count = 0
            self._connection_issue_count = 0
            
            # 启动消息处理任务
            self._ws_handler_task = asyncio.create_task(self._websocket_message_handler())
            
            # 启动心跳检测
            self._heartbeat_task = asyncio.create_task(self._websocket_heartbeat_loop())
            if self.logger:
                self.logger.info("💓 [EdgeX心跳] 公共WebSocket心跳检测已启动")
            
            # 🔥 如果有认证信息，再连接私有SDK WebSocket（用于订单/持仓）
            if self._use_sdk and self.ws_manager:
                if self.logger:
                    self.logger.info("🔐 [EdgeX] 正在连接私有SDK WebSocket（订单/持仓）...")
                
                try:
                    # SDK的connect_private()是同步方法，在线程中运行
                    loop = asyncio.get_running_loop()
                    await loop.run_in_executor(None, self.ws_manager.connect_private)
                    
                    if self.logger:
                        self.logger.info("✅ [EdgeX] 私有SDK WebSocket已连接")
                    
                    # 🔥 设置SDK的消息回调（处理trade-event）
                    self._setup_sdk_message_handlers()
                    
                except Exception as e:
                    if self.logger:
                        self.logger.warning(f"⚠️ [EdgeX] 私有SDK WebSocket连接失败（仅影响订单/持仓推送）: {e}")
            
            return True
            
        except Exception as e:
            if self.logger:
                self.logger.warning(f"❌ [EdgeX] WebSocket连接失败: {e}")
                import traceback
                self.logger.warning(f"Traceback: {traceback.format_exc()}")
            return False

    def _setup_sdk_message_handlers(self) -> None:
        """
        设置EdgeX SDK的消息处理器
        
        EdgeX SDK使用on_message()来注册消息回调
        """
        try:
            if not self.ws_manager:
                return
            
            # 🔥 保存当前事件循环引用（SDK消息处理器在独立线程中运行）
            main_loop = asyncio.get_event_loop()
            
            # 获取私有WebSocket客户端
            private_client = self.ws_manager.get_private_client()
            
            # 定义消息处理器（将SDK消息转换为我们的格式）
            def sdk_message_handler(message):
                """处理SDK接收到的trade-event消息"""
                try:
                    import json
                    
                    # 如果是字符串，解析为dict
                    if isinstance(message, str):
                        message = json.loads(message)
                    
                    # 🔥 简化日志：只打印关键信息，而不是整个JSON
                    if self.logger:
                        event_type = message.get('type', 'unknown')
                        content = message.get('content', {})
                        event_name = content.get('event', 'unknown')
                        version = content.get('version', 'N/A')
                        
                        # 统计数据条目数量
                        data = content.get('data', {})
                        order_count = len(data.get('order', []))
                        position_count = len(data.get('position', []))
                        fill_count = len(data.get('orderFillTransaction', []))
                        
                        self.logger.debug(
                            f"📥 [EdgeX WS] {event_name} | "
                            f"版本:{version} | "
                            f"订单:{order_count} 持仓:{position_count} 成交:{fill_count}"
                        )
                    
                    # 🔥 SDK在独立线程中运行，使用run_coroutine_threadsafe调度到主事件循环
                    asyncio.run_coroutine_threadsafe(
                        self._handle_user_data_update(message),
                        main_loop
                    )
                        
                except Exception as e:
                    if self.logger:
                        self.logger.error(f"❌ [EdgeX SDK] 处理消息失败: {e}")
                        import traceback
                        self.logger.error(f"Traceback: {traceback.format_exc()}")
            
            # 注册trade-event消息处理器
            private_client.on_message("trade-event", sdk_message_handler)
            
            if self.logger:
                self.logger.info("✅ [EdgeX SDK] 已注册trade-event消息处理器")
                
        except Exception as e:
            if self.logger:
                self.logger.warning(f"⚠️ [EdgeX SDK] 设置消息处理器失败: {e}")
                import traceback
                self.logger.warning(f"Traceback: {traceback.format_exc()}")

    async def disconnect(self) -> None:
        """断开WebSocket连接"""
        if self.logger:
            self.logger.info("🔄 [EdgeX] 开始断开WebSocket连接...")
        
        try:
            # 1. 标记为断开状态，停止新的操作
            self._ws_connected = False
            
            # 2. 取消心跳任务
            if hasattr(self, '_heartbeat_task') and self._heartbeat_task and not self._heartbeat_task.done():
                if self.logger:
                    self.logger.info("🛑 [EdgeX心跳] 取消心跳任务...")
                self._heartbeat_task.cancel()
                try:
                    await asyncio.wait_for(self._heartbeat_task, timeout=2.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
                if self.logger:
                    self.logger.info("✅ [EdgeX心跳] 心跳任务已停止")
            
            # 3. 取消消息处理任务
            if hasattr(self, '_ws_handler_task') and self._ws_handler_task and not self._ws_handler_task.done():
                if self.logger:
                    self.logger.info("🛑 [EdgeX] 取消消息处理任务...")
                self._ws_handler_task.cancel()
                try:
                    await asyncio.wait_for(self._ws_handler_task, timeout=2.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
                if self.logger:
                    self.logger.info("✅ [EdgeX] 消息处理任务已停止")
            
            # 4. 关闭WebSocket连接
            if hasattr(self, '_ws_connection') and self._ws_connection and not self._ws_connection.closed:
                if self.logger:
                    self.logger.info("🛑 [EdgeX] 关闭WebSocket连接...")
                try:
                    await asyncio.wait_for(self._ws_connection.close(), timeout=3.0)
                except asyncio.TimeoutError:
                    if self.logger:
                        self.logger.warning("⚠️ [EdgeX] WebSocket关闭超时，强制设置为None")
                self._ws_connection = None
                if self.logger:
                    self.logger.info("✅ [EdgeX] WebSocket连接已关闭")
            
            # 5. 关闭session
            if hasattr(self, '_session') and self._session and not self._session.closed:
                if self.logger:
                    self.logger.info("🛑 [EdgeX] 关闭session...")
                try:
                    await asyncio.wait_for(self._session.close(), timeout=3.0)
                except asyncio.TimeoutError:
                    if self.logger:
                        self.logger.warning("⚠️ [EdgeX] Session关闭超时")
                if self.logger:
                    self.logger.info("✅ [EdgeX] session已关闭")
            
            # 6. 清理状态变量
            self._last_heartbeat = 0
            self._reconnect_attempts = 0
            
            if self.logger:
                self.logger.info("🎉 [EdgeX] WebSocket连接断开完成")
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ [EdgeX] 关闭WebSocket连接时出错: {e}")
                import traceback
                self.logger.error(f"❌ [EdgeX] 断开连接错误堆栈: {traceback.format_exc()}")
            
            # 强制清理状态
            self._ws_connected = False
            self._ws_connection = None

    async def _websocket_heartbeat_loop(self):
        """WebSocket主动心跳检测循环 - 优化EdgeX连接稳定性"""
        # 🔥 优化：双重检测机制（主动ping + 数据接收检测）
        # 数据接收检测：10秒无数据即触发重连（主要检测方式）
        # 主动ping检测：作为辅助检测，确保连接活跃
        heartbeat_interval = 5   # 5秒检测一次 (快速检测，确保能及时检测到10秒超时)
        ping_interval = 20       # 20秒发送一次ping (保持连接活跃，作为辅助检测)
        max_silence = 10         # 10秒无消息则重连 (快速检测超时，主要检测方式)
        
        # 初始化ping相关时间戳
        self._last_ping_time = time.time()
        self._last_pong_time = time.time()
        
        if self.logger:
            self.logger.info("💓 [EdgeX心跳] 优化心跳检测循环启动 (双重检测模式)")
            self.logger.info(f"💓 [EdgeX心跳] 心跳参数: 检测间隔={heartbeat_interval}s, ping间隔={ping_interval}s, 数据超时={max_silence}s, ping失败容忍=1次")
            self.logger.info(f"💓 [EdgeX心跳] 检测策略: 主要依赖数据接收检测（{max_silence}秒超时），主动ping作为辅助检测")
        
        try:
            # 🔥 修复：心跳循环应该持续运行，即使连接断开也要继续检测和重连
            # 不依赖 _ws_connected 状态，而是依赖 _running 或明确的停止信号
            while True:
                try:
                    # 使用更短的等待时间，加快检测响应
                    await asyncio.wait_for(
                        asyncio.sleep(heartbeat_interval), 
                        timeout=heartbeat_interval + 5
                    )
                    
                    # 🔥 修复：即使连接断开，也继续检测，以便触发重连
                    # 不在这里退出循环，而是继续检测重连条件
                    
                    current_time = time.time()
                    
                    # === 🔥 优化：加快ping频率 ===
                    if current_time - self._last_ping_time >= ping_interval:
                        await self._send_websocket_ping()
                        self._last_ping_time = current_time
                        if self.logger:
                            current_time = time.time()
                            if current_time - self._last_heartbeat_log_time >= self._heartbeat_log_interval:
                                self.logger.info("[EdgeX] 心跳: ping")
                                self._last_heartbeat_log_time = current_time
                    
                    # === 💌 优化：双重检测机制（数据接收检测为主，ping检测为辅） ===
                    silence_time, no_reference = self._calc_silence_time()
                    
                    # 🔥 双重检测：优先数据接收检测（10秒超时），ping失败作为辅助
                    silence_exceeded = (not no_reference) and silence_time > max_silence
                    should_reconnect = (
                        silence_exceeded or
                        self._ping_failure_count >= 1
                    )
                    
                    if should_reconnect:
                        reason = []
                        if silence_exceeded:
                            reason.append(f"数据接收超时: {silence_time:.1f}s (阈值: {max_silence}s)")
                        elif no_reference:
                            reason.append("尚未收到业务数据")
                        if self._ping_failure_count >= 1:
                            reason.append(f"ping失败: {self._ping_failure_count}次")
                        if self.logger:
                            self.logger.warning(f"⚠️ [EdgeX心跳] WebSocket准备重连: {', '.join(reason)}")
                        
                        # 检查是否已经在重连中
                        if hasattr(self, '_reconnecting') and self._reconnecting:
                            if self.logger:
                                self.logger.info("🔄 [EdgeX心跳] 已有重连在进行中，跳过此次检测")
                            continue
                        
                        # 标记重连状态
                        self._reconnecting = True
                        
                        try:
                            if self.logger:
                                self.logger.info("🔄 [EdgeX心跳] 开始执行重连...")
                            reconnect_success = await self._reconnect_websocket()
                            if reconnect_success:
                                if self.logger:
                                    self.logger.info("✅ [EdgeX心跳] 重连成功")
                                # 🔥 重连成功后重置计数器
                                self._ping_failure_count = 0
                            else:
                                if self.logger:
                                    self.logger.warning("⚠️ [EdgeX心跳] 重连失败，将在下次检测时重试")
                                # 🔥 重连失败时，不重置计数器，继续检测
                        except asyncio.CancelledError:
                            if self.logger:
                                self.logger.warning("⚠️ [EdgeX心跳] 重连被取消")
                            raise
                        except Exception as e:
                            if self.logger:
                                self.logger.error(f"❌ [EdgeX心跳] 重连失败: {type(e).__name__}: {e}")
                        finally:
                            # 清除重连状态标记
                            self._reconnecting = False
                    else:
                        # 🔥 优化：减少正常状态的日志输出频率
                        if self.logger:
                            if no_reference:
                                self.logger.debug("💓 EdgeX WebSocket心跳: 尚未收到业务数据")
                            else:
                                self.logger.debug(f"💓 EdgeX WebSocket心跳正常: {silence_time:.1f}s前有数据")
                    
                except asyncio.CancelledError:
                    if self.logger:
                        self.logger.info("💓 [EdgeX心跳] 心跳检测被取消")
                    break
                except asyncio.TimeoutError:
                    if self.logger:
                        self.logger.warning("⚠️ [EdgeX心跳] 心跳检测超时")
                    continue
                except Exception as e:
                    if self.logger:
                        self.logger.error(f"❌ [EdgeX心跳] 心跳检测错误: {e}")
                    # 错误后等待较短时间再继续
                    try:
                        await asyncio.wait_for(asyncio.sleep(5), timeout=10)  # 减少错误后的等待时间
                    except (asyncio.CancelledError, asyncio.TimeoutError):
                        break
                        
        except asyncio.CancelledError:
            if self.logger:
                self.logger.info("💓 [EdgeX心跳] 心跳循环被正常取消")
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ [EdgeX心跳] 心跳循环异常退出: {e}")
        finally:
            if self.logger:
                self.logger.info("💓 [EdgeX心跳] 心跳检测循环已退出")
            # 清理重连状态
            self._reconnecting = False
    
    async def _reconnect_websocket(self) -> bool:
        """WebSocket自动重连 - 优化EdgeX重连稳定性（提高响应速度）
        
        Returns:
            bool: 重连是否成功
        """
        # 🔥 优化：更激进的指数退避策略，尽快重连
        base_delay = 1   # 基础延迟改为1秒 (从2秒缩短，更快重连)
        max_delay = 300  # 最大延迟保持5分钟
        
        # 无限重试，移除次数限制
        self._reconnect_attempts += 1
        
        # 🔥 优化：更快的指数退避策略（提高响应速度）
        if self._reconnect_attempts == 1:
            # 第1次：立即重连（0秒延迟）
            delay = 0
        elif self._reconnect_attempts == 2:
            # 第2次：1秒延迟
            delay = base_delay
        elif self._reconnect_attempts <= 5:
            # 第3-5次：2秒、4秒、8秒（快速指数增长）
            delay = base_delay * (2 ** (self._reconnect_attempts - 2))
        else:
            # 后续使用指数退避，但限制最大延迟
            delay = min(base_delay * (2 ** min(self._reconnect_attempts - 2, 8)), max_delay)
        
        if self.logger:
                self.logger.info(f"🔄 [EdgeX重连] 重连尝试 #{self._reconnect_attempts}，延迟{delay}秒")
        
        reconnect_success = False
        
        try:
            # 🔥 移除：不再检查第三方网络（httpbin.org），直接尝试连接
            # WebSocket连接失败本身就能说明网络或服务器问题，不需要额外的网络检查
            
            # 步骤1: 检查交易所服务器连通性（可选，不阻塞重连）
            try:
                exchange_ok = await asyncio.wait_for(
                    self._check_exchange_connectivity(),
                    timeout=2.0  # 2秒超时，不阻塞太久
                )
                if self.logger:
                    status = "✅ 可达" if exchange_ok else "⚠️ 不可达"
                    self.logger.info(f"🏢 [EdgeX重连] 交易所服务器连通性: {status}")
                # 🔥 即使服务器不可达，仍然尝试连接（可能只是临时故障）
            except asyncio.TimeoutError:
                if self.logger:
                    self.logger.debug("🔧 [EdgeX重连] 交易所服务器连通性检查超时，直接尝试连接")
            except Exception as e:
                if self.logger:
                    self.logger.debug(f"🔧 [EdgeX重连] 交易所服务器连通性检查失败: {e}，直接尝试连接")
            
            # 步骤2: 彻底清理旧连接
            if self.logger:
                self.logger.info("🔧 [EdgeX重连] 步骤2: 彻底清理旧连接...")
            await self._cleanup_old_connections()
            
            # 步骤3: 等待延迟
            if self.logger:
                self.logger.info(f"🔧 [EdgeX重连] 步骤3: 等待{delay}s后重连...")
            await asyncio.sleep(delay)
            
            # 步骤4: 重新建立连接
            if self.logger:
                self.logger.info("🔧 [EdgeX重连] 步骤4: 重新建立EdgeX WebSocket连接...")
            
            # 使用现有的connect方法，它已经包含了完整的连接逻辑
            reconnect_success = await self.connect()
            
            if reconnect_success:
                # 步骤5: 重新订阅所有频道
                if self.logger:
                    self.logger.info("🔧 [EdgeX重连] 步骤5: 重新订阅所有频道...")
                await self._resubscribe_all()
                
                # 步骤6: 重置状态 - 重连成功，重置计数
                self._reconnect_attempts = 0
                # 🔥 增加重连次数统计
                self._reconnect_count += 1
                current_time = time.time()
                self._last_heartbeat = current_time
                
                # 🔥 新增：重置ping/pong时间戳
                self._last_ping_time = current_time
                self._last_pong_time = current_time
                
                if self.logger:
                    self.logger.info(f"🎉 [EdgeX重连] WebSocket重连成功！ (总重连次数: {self._reconnect_count})")
                return True  # 🔥 修复：返回True表示重连成功
            else:
                if self.logger:
                    self.logger.warning("⚠️ [EdgeX重连] 连接建立失败")
                return False  # 🔥 修复：返回False表示重连失败
                
        except asyncio.CancelledError:
            if self.logger:
                self.logger.warning("⚠️ [EdgeX重连] EdgeX重连被取消")
            self._ws_connected = False
            raise
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ [EdgeX重连] EdgeX重连失败: {type(e).__name__}: {e}")
                import traceback
                self.logger.error(f"❌ [EdgeX重连] 完整错误堆栈: {traceback.format_exc()}")
            self._ws_connected = False
            return False  # 🔥 修复：返回False表示重连失败，心跳循环会继续重试
            
            # 保持连接状态为True，让心跳检测继续工作
            # 不停止心跳任务，实现真正的无限重试
    
    async def _cleanup_old_connections(self):
        """彻底清理旧的连接和任务"""
        try:
            # 1. 停止消息处理任务
            if hasattr(self, '_ws_handler_task') and self._ws_handler_task and not self._ws_handler_task.done():
                self._ws_handler_task.cancel()
                try:
                    await asyncio.wait_for(self._ws_handler_task, timeout=1.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
                
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
            self._last_message_time = 0.0
            self._last_business_message_time = 0.0
                
        except Exception as e:
            if self.logger:
                self.logger.warning(f"⚠️ [清理调试] 清理旧连接时出错: {e}")
    
    async def _resubscribe_all(self):
        """重新订阅所有频道"""
        try:
            if self.logger:
                self.logger.info("🔄 [重订阅调试] 开始重新订阅EdgeX所有频道")
            
            # 1. 重新订阅metadata (关键！)
            if self.logger:
                self.logger.info("🔧 [重订阅调试] 步骤1: 重新订阅metadata频道...")
            try:
                await self.subscribe_metadata()
                if self.logger:
                    self.logger.info("✅ [重订阅调试] 已重新订阅metadata频道")
            except Exception as e:
                if self.logger:
                    self.logger.error(f"❌ [重订阅调试] metadata订阅失败: {e}")
                raise
            
            # 2. 等待metadata解析完成
            if self.logger:
                self.logger.info("🔧 [重订阅调试] 步骤2: 等待metadata解析完成...")
            await asyncio.sleep(1)
            if self.logger:
                self.logger.info("✅ [重订阅调试] metadata等待完成")
            
            # 3. 检查合约映射状态
            if self.logger:
                mapping_count = len(self._symbol_contract_mappings) if hasattr(self, '_symbol_contract_mappings') else 0
                self.logger.info(f"🔧 [重订阅调试] 步骤3: 当前合约映射数量: {mapping_count}")
                if mapping_count > 0:
                    sample_mappings = dict(list(self._symbol_contract_mappings.items())[:3])
                    self.logger.info(f"🔧 [重订阅调试] 合约映射示例: {sample_mappings}")
            
            # 4. 重新订阅所有ticker
            if self.logger:
                self.logger.info("🔧 [重订阅调试] 步骤4: 重新订阅所有ticker...")
            ticker_count = 0
            failed_count = 0
            
            subscription_count = len(self._ws_subscriptions) if hasattr(self, '_ws_subscriptions') else 0
            if self.logger:
                self.logger.info(f"🔧 [重订阅调试] 待处理订阅数量: {subscription_count}")
            
            for sub_type, symbol, callback in self._ws_subscriptions:
                if sub_type == 'ticker' and symbol:
                    # 获取合约ID
                    contract_id = self._symbol_contract_mappings.get(symbol)
                    if contract_id:
                        try:
                            subscribe_msg = {
                                "type": "subscribe",
                                "channel": f"ticker.{contract_id}"
                            }
                            
                            if await self._safe_send_message(json.dumps(subscribe_msg)):
                                ticker_count += 1
                                if self.logger:
                                    self.logger.debug(f"✅ [重订阅调试] 重新订阅ticker: {symbol} (合约ID: {contract_id})")
                            else:
                                if self.logger:
                                    self.logger.error(f"❌ [重订阅调试] WebSocket连接不可用: {symbol}")
                                failed_count += 1
                            
                            await asyncio.sleep(0.1)  # 小延迟
                        except Exception as e:
                            if self.logger:
                                self.logger.error(f"❌ [重订阅调试] 订阅{symbol}失败: {e}")
                            failed_count += 1
                    else:
                        if self.logger:
                            self.logger.warning(f"⚠️ [重订阅调试] 未找到符号 {symbol} 的合约ID，跳过重新订阅")
                        failed_count += 1
                            
            # 5. 重新订阅其他类型的频道（直接发送消息，避免重复添加到订阅列表）
            if self.logger:
                self.logger.info("🔧 [重订阅调试] 步骤5: 重新订阅其他类型频道...")
            other_count = 0
            for sub_type, symbol, callback in self._ws_subscriptions:
                try:
                    if sub_type == 'orderbook' and symbol:
                        # 直接发送订阅消息，避免重复添加到_ws_subscriptions
                        contract_id = self._symbol_contract_mappings.get(symbol)
                        if contract_id:
                            subscribe_msg = {
                                "type": "subscribe",
                                "channel": f"depth.{contract_id}.15"
                            }
                            if await self._safe_send_message(json.dumps(subscribe_msg)):
                                other_count += 1
                                if self.logger:
                                    self.logger.debug(f"✅ [重订阅调试] 重新订阅orderbook: {symbol}")
                            await asyncio.sleep(0.1)
                    elif sub_type == 'trades' and symbol:
                        # 直接发送订阅消息，避免重复添加到_ws_subscriptions
                        contract_id = self._symbol_contract_mappings.get(symbol)
                        if contract_id:
                            subscribe_msg = {
                                "type": "subscribe",
                                "channel": f"trades.{contract_id}"
                            }
                            if await self._safe_send_message(json.dumps(subscribe_msg)):
                                other_count += 1
                                if self.logger:
                                    self.logger.debug(f"✅ [重订阅调试] 重新订阅trades: {symbol}")
                            await asyncio.sleep(0.1)
                except Exception as e:
                    if self.logger:
                        self.logger.error(f"❌ [重订阅调试] 订阅{sub_type}:{symbol}失败: {e}")
                    
            if self.logger:
                self.logger.info(f"✅ [重订阅调试] EdgeX重连订阅完成: {ticker_count}个ticker + {other_count}个其他 + metadata (失败: {failed_count})")
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ [重订阅调试] EdgeX重新订阅失败: {type(e).__name__}: {e}")
                import traceback
                self.logger.error(f"[重订阅调试] 完整错误堆栈: {traceback.format_exc()}")
            raise

    async def _websocket_message_handler(self) -> None:
        """WebSocket消息处理器"""
        try:
            async for msg in self._ws_connection:
                # 更新消息时间戳
                current_time = time.time()
                self._last_message_time = current_time
                self._last_heartbeat = current_time
                
                if msg.type == aiohttp.WSMsgType.TEXT:
                    await self._process_websocket_message(msg.data)
                elif msg.type == aiohttp.WSMsgType.PONG:
                    # 🔥 新增：处理pong响应
                    if hasattr(self, '_last_pong_time'):
                        self._last_pong_time = time.time()
                    if self.logger:
                        self.logger.debug("🏓 EdgeX收到WebSocket pong响应")
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    if self.logger:
                        self.logger.warning(f"EdgeX WebSocket错误: {self._ws_connection.exception()}")
                    self._ws_connected = False
                    break
                elif msg.type == aiohttp.WSMsgType.CLOSE:
                    if self.logger:
                        self.logger.warning("EdgeX WebSocket连接已关闭")
                    self._ws_connected = False
                    break
        except Exception as e:
            if self.logger:
                self.logger.warning(f"❌ [EdgeX] WebSocket消息处理失败: {e}")
            self._ws_connected = False

    async def _process_websocket_message(self, message: str) -> None:
        """处理WebSocket消息"""
        try:
            # 🔥 统计接收的字节数
            message_bytes = len(message.encode('utf-8'))
            self._network_bytes_received += message_bytes
            
            data = json.loads(message)
            msg_type = data.get('type')
            if msg_type not in ('ping', 'connected', 'subscribed'):
                # 仅在真正的业务消息到达时刷新业务时间戳
                self._last_business_message_time = self._last_message_time

            # 处理连接确认消息
            if data.get('type') == 'connected':
                if self.logger:
                    self.logger.info(f"✅ [EdgeX] WebSocket连接确认: {data.get('sid')}")
                return

            # 处理订阅确认消息
            if data.get('type') == 'subscribed':
                if self.logger:
                    self.logger.debug(f"✅ [EdgeX] 订阅成功: {data.get('channel')}")
                return

            # 处理ping消息
            if data.get('type') == 'ping':
                pong_message = {
                    "type": "pong",
                    "time": data.get("time")
                }
                if await self._safe_send_message(json.dumps(pong_message)):
                    if self.logger:
                        current_time = time.time()
                        if current_time - self._last_heartbeat_log_time >= self._heartbeat_log_interval:
                            self.logger.info("[EdgeX] 心跳: pong")
                            self._last_heartbeat_log_time = current_time
                else:
                    if self.logger:
                        self.logger.warning("[EdgeX] 心跳: 发送pong失败")
                return

            # 处理市场数据消息
            if data.get('type') == 'quote-event':
                channel = data.get('channel', '')
                content = data.get('content', {})
                
                # 🔥 诊断日志：订单簿消息（改为INFO级别，便于调试）
                if channel.startswith('depth.'):
                    if self.logger:
                        self.logger.debug(f"📥 EdgeX收到订单簿消息: channel={channel}, dataType={content.get('dataType', 'unknown')}")
                
                # 🔥 诊断日志：ticker消息（改为INFO级别，便于调试）
                if channel.startswith('ticker.'):
                    if self.logger:
                        self.logger.debug(f"📊 EdgeX收到ticker消息: channel={channel}")
                
                if channel.startswith('ticker.'):
                    await self._handle_ticker_update(channel, content)
                elif channel.startswith('depth.'):
                    await self._handle_orderbook_update(channel, content)
                elif channel.startswith('trades.'):
                    await self._handle_trade_update(channel, content)
                elif channel == 'metadata':
                    await self._handle_metadata_update(content)
                else:
                    if self.logger:
                        self.logger.debug(f"EdgeX未知的频道类型: {channel}")
                return

            # 🔥 处理用户数据消息（订单/持仓更新）
            if data.get('type') == 'trade-event':
                if self.logger:
                    self.logger.info(f"📥 [EdgeX] 收到trade-event消息: {data}")
                
                # 调用用户数据处理函数
                await self._handle_user_data_update(data)
                return

            # 处理错误消息
            if data.get('type') == 'error':
                if self.logger:
                    self.logger.warning(f"EdgeX WebSocket错误: {data.get('content')}")
                return

            # 其他未识别的消息
            if self.logger:
                self.logger.debug(f"EdgeX未知消息格式: {data}")

        except Exception as e:
            if self.logger:
                self.logger.warning(f"处理EdgeX WebSocket消息失败: {e}")
                self.logger.debug(f"原始消息: {message}")

    async def _handle_ticker_update(self, channel: str, content: Dict[str, Any]) -> None:
        """处理行情更新"""
        try:
            # 从频道名称提取contractId
            contract_id = channel.split('.')[-1]  # ticker.10000001 -> 10000001
            symbol = self._contract_mappings.get(contract_id)
            
            if not symbol:
                if self.logger:
                    self.logger.debug(f"未找到合约ID {contract_id} 对应的交易对")
                return

            # 解析EdgeX ticker数据格式
            data_list = content.get('data', [])
            if not data_list:
                return
                
            ticker_data = data_list[0]  # 取第一个数据

            # 解析交易所时间戳
            exchange_timestamp = None
            current_time = datetime.now()  # 获取当前时间作为备用
            
            timestamp_candidates = [
                ('timestamp', 1000),        # 毫秒时间戳
                ('ts', 1000),              # 通用时间戳
                ('eventTime', 1000),       # 事件时间
                ('time', 1000),            # 时间字段
            ]
            
            for field, divisor in timestamp_candidates:
                if field in ticker_data and ticker_data[field]:
                    try:
                        timestamp_value = int(ticker_data[field])
                        # 检测时间戳精度（微秒 vs 毫秒）
                        if timestamp_value > 1e12:  # 微秒时间戳
                            exchange_timestamp = datetime.fromtimestamp(timestamp_value / 1000000)
                        elif timestamp_value > 1e9:  # 毫秒时间戳
                            exchange_timestamp = datetime.fromtimestamp(timestamp_value / 1000)
                        else:  # 秒时间戳
                            exchange_timestamp = datetime.fromtimestamp(timestamp_value)
                        
                        # 验证时间戳合理性（不能是未来时间，不能太旧）
                        time_diff = abs((current_time - exchange_timestamp).total_seconds())
                        if time_diff < 3600:  # 时间差小于1小时，认为是有效的
                            break
                        else:
                            exchange_timestamp = None
                    except (ValueError, TypeError, OSError):
                        continue

            # 使用当前时间作为主时间戳（确保时效性正确）
            # 注意：我们故意使用当前时间而不是交易所时间戳，因为我们关心的是数据的新鲜度
            main_timestamp = current_time
            
            # === 完整解析EdgeX ticker数据的所有字段 ===
            ticker = TickerData(
                symbol=symbol,
                
                # === 基础价格信息 ===
                # 注意：EdgeX的ticker数据中没有bid/ask，需要从orderbook获取
                bid=self._safe_decimal(ticker_data.get('bestBidPrice')),  # EdgeX可能没有此字段
                ask=self._safe_decimal(ticker_data.get('bestAskPrice')),  # EdgeX可能没有此字段
                bid_size=None,  # EdgeX ticker中不提供，需要从orderbook获取
                ask_size=None,  # EdgeX ticker中不提供，需要从orderbook获取
                last=self._safe_decimal(ticker_data.get('lastPrice')),   # EdgeX: lastPrice
                open=self._safe_decimal(ticker_data.get('open')),        # EdgeX: open
                high=self._safe_decimal(ticker_data.get('high')),        # EdgeX: high
                low=self._safe_decimal(ticker_data.get('low')),          # EdgeX: low
                close=self._safe_decimal(ticker_data.get('close')),      # EdgeX: close
                
                # === 成交量信息 ===
                volume=self._safe_decimal(ticker_data.get('size')),      # EdgeX: size (基础资产交易量)
                quote_volume=self._safe_decimal(ticker_data.get('value')), # EdgeX: value (计价资产交易额)
                trades_count=self._safe_int(ticker_data.get('trades')),  # EdgeX: trades (成交笔数)
                
                # === 价格变化信息 ===
                change=self._safe_decimal(ticker_data.get('priceChange')),        # EdgeX: priceChange
                percentage=self._safe_decimal(ticker_data.get('priceChangePercent')), # EdgeX: priceChangePercent
                
                # === 合约特有信息（期货/永续合约） ===
                # EdgeX: fundingRate 是4小时费率，需要×2转换为8小时（与Lighter统一）
                funding_rate=self._safe_decimal(ticker_data.get('fundingRate')) * 2 if ticker_data.get('fundingRate') else None,
                predicted_funding_rate=None,  # EdgeX不提供预测资金费率
                funding_time=ticker_data.get('fundingTime'),     # EdgeX: fundingTime (当前资金费率时间)
                next_funding_time=ticker_data.get('nextFundingTime'), # EdgeX: nextFundingTime (下次资金费率时间)
                funding_interval=None,  # EdgeX不直接提供，可从两次时间戳推算
                
                # === 价格参考信息 ===
                index_price=self._safe_decimal(ticker_data.get('indexPrice')),   # EdgeX: indexPrice (指数价格)
                mark_price=None,  # EdgeX不提供标记价格，使用indexPrice或lastPrice
                oracle_price=self._safe_decimal(ticker_data.get('oraclePrice')), # EdgeX: oraclePrice (预言机价格)
                
                # === 持仓和合约信息 ===
                open_interest=self._safe_decimal(ticker_data.get('openInterest')), # EdgeX: openInterest (未平仓合约数量)
                open_interest_value=None,  # EdgeX不直接提供，需要用openInterest * price计算
                delivery_date=None,  # EdgeX永续合约无交割日期
                
                # === 时间相关信息 ===
                high_time=ticker_data.get('highTime'),          # EdgeX: highTime (最高价时间)
                low_time=ticker_data.get('lowTime'),            # EdgeX: lowTime (最低价时间)
                start_time=ticker_data.get('startTime'),        # EdgeX: startTime (统计开始时间)
                end_time=ticker_data.get('endTime'),            # EdgeX: endTime (统计结束时间)
                
                # === 合约标识信息 ===
                contract_id=ticker_data.get('contractId'),      # EdgeX: contractId (合约ID)
                contract_name=ticker_data.get('contractName'),  # EdgeX: contractName (合约名称)
                base_currency=None,  # 需要从symbol解析，如BTC_USDT -> BTC
                quote_currency=None, # 需要从symbol解析，如BTC_USDT -> USDT
                contract_size=None,  # EdgeX不提供，通常为1
                tick_size=None,      # EdgeX不在ticker中提供
                lot_size=None,       # EdgeX不在ticker中提供
                
                # === 时间戳链条 ===
                timestamp=main_timestamp,
                exchange_timestamp=exchange_timestamp,
                received_timestamp=current_time,  # 数据接收时间
                processed_timestamp=None,         # 将在处理完成后设置
                sent_timestamp=None,              # 将在发送给回调时设置
                
                # === 原始数据保留 ===
                raw_data=ticker_data
            )
            
            # 设置处理完成时间戳
            ticker.processed_timestamp = datetime.now()
            
            # 解析基础货币和计价货币（从symbol中提取）
            if symbol and '_' in symbol:
                parts = symbol.split('_')
                if len(parts) >= 2:
                    ticker.base_currency = parts[0]    # 如BTC_USDT -> BTC
                    ticker.quote_currency = parts[1]   # 如BTC_USDT -> USDT
            
            # 计算未平仓合约价值（如果有足够数据）
            if ticker.open_interest is not None and ticker.last is not None:
                ticker.open_interest_value = ticker.open_interest * ticker.last
            
            # 计算资金费率收取间隔（如果有两个时间戳）
            if ticker.funding_time is not None and ticker.next_funding_time is not None:
                try:
                    funding_time_dt = ticker.funding_time if isinstance(ticker.funding_time, datetime) else datetime.fromtimestamp(int(ticker.funding_time) / 1000)
                    next_funding_time_dt = ticker.next_funding_time if isinstance(ticker.next_funding_time, datetime) else datetime.fromtimestamp(int(ticker.next_funding_time) / 1000)
                    interval_seconds = int((next_funding_time_dt - funding_time_dt).total_seconds())
                    ticker.funding_interval = interval_seconds
                except (ValueError, TypeError):
                    pass
            
            # 如果没有mark_price，使用index_price或last作为替代
            if ticker.mark_price is None:
                if ticker.index_price is not None:
                    ticker.mark_price = ticker.index_price
                elif ticker.last is not None:
                    ticker.mark_price = ticker.last
            
            # 设置发送时间戳
            ticker.sent_timestamp = datetime.now()

            # 调用回调函数
            if self.ticker_callback:
                await self._safe_callback_with_symbol(self.ticker_callback, symbol, ticker)
            
            # 调用特定的订阅回调
            for sub_type, sub_symbol, callback in self._ws_subscriptions:
                if sub_type == 'ticker' and sub_symbol == symbol:
                    # 🔥 修复：批量订阅的回调函数需要两个参数 (symbol, ticker_data)
                    if callback:
                        await self._safe_callback_with_symbol(callback, symbol, ticker)

        except Exception as e:
            if self.logger:
                self.logger.warning(f"处理EdgeX行情更新失败: {e}")
                self.logger.debug(f"频道: {channel}, 内容: {content}")

    async def _handle_orderbook_update(self, channel: str, content: Dict[str, Any]) -> None:
        """
        处理EdgeX订单簿更新（支持快照和增量）
        
        EdgeX订单簿推送机制：
        1. 首次订阅：推送完整快照 (depthType=Snapshot)
        2. 后续更新：推送增量数据 (depthType=CHANGED)
        3. 增量更新规则：
           - size = 0：删除该价格档位
           - size > 0：更新/新增该价格档位
        
        数据格式：
        - bids/asks: [["price", "size"], ["price", "size"], ...]
        """
        # 🔥 诊断日志：确认方法被调用
        if self.logger:
            self.logger.debug(f"🔍 EdgeX订单簿更新: channel={channel}, content_keys={list(content.keys())}")
        
        try:
            # 从频道名称提取contractId
            parts = channel.split('.')
            if len(parts) >= 2:
                contract_id = parts[1]  # depth.10000001.15 -> 10000001
            else:
                return
                
            symbol = self._contract_mappings.get(contract_id)
            
            if not symbol:
                if self.logger:
                    self.logger.debug(f"未找到合约ID {contract_id} 对应的交易对")
                return

            # 解析EdgeX订单簿数据格式
            data_list = content.get('data', [])
            if not data_list:
                if self.logger:
                    self.logger.warning(f"⚠️  {symbol}: content中没有data字段或data为空, content_keys={list(content.keys())}")
                return
                
            orderbook_data = data_list[0]  # 取第一个数据
            
            # 🔥 诊断：打印orderbook_data的完整结构（DEBUG级别，减少日志写入）
            if self.logger:
                self.logger.debug(
                    f"🔍 {symbol}: orderbook_data结构 - "
                    f"keys={list(orderbook_data.keys())}, "
                    f"has_bids={'bids' in orderbook_data}, "
                    f"has_asks={'asks' in orderbook_data}"
                )
            
            # 🔥 关键：检查depthType区分快照和增量
            # 注意：EdgeX可能不总是发送depthType字段，默认当作增量处理
            # EdgeX发送的是全大写 "SNAPSHOT" 或 "CHANGED"
            depth_type = orderbook_data.get('depthType', 'CHANGED').upper()

            # 解析交易所时间戳
            exchange_timestamp = None
            if 'timestamp' in orderbook_data:
                try:
                    timestamp_ms = int(orderbook_data['timestamp'])
                    exchange_timestamp = datetime.fromtimestamp(timestamp_ms / 1000)
                except (ValueError, TypeError):
                    pass
            elif 'ts' in orderbook_data:
                try:
                    timestamp_ms = int(orderbook_data['ts'])
                    exchange_timestamp = datetime.fromtimestamp(timestamp_ms / 1000)
                except (ValueError, TypeError):
                    pass

            # 🔥 根据depthType处理数据
            if depth_type == 'SNAPSHOT':
                # === 快照模式：直接替换本地订单簿 ===
                if self.logger:
                    self.logger.debug(f"📸 {symbol}: 收到订单簿快照")
                
                # 初始化本地订单簿
                self._local_orderbooks[symbol] = {
                    'bids': {},  # {price: size}
                    'asks': {}   # {price: size}
                }
                
                # 解析快照数据（EdgeX格式：支持数组[["price", "size"], ...]或字典[{'price': '...', 'size': '...'}, ...]）
                for bid in orderbook_data.get('bids', []):
                    price = None
                    size = None
                    
                    if isinstance(bid, dict):
                        # 🔥 字典格式：{'price': '...', 'size': '...'}
                        price = self._safe_decimal(bid.get('price'))
                        size = self._safe_decimal(bid.get('size'))
                    elif isinstance(bid, list) and len(bid) >= 2:
                        # 数组格式：["price", "size"]
                        price = self._safe_decimal(bid[0])
                        size = self._safe_decimal(bid[1])
                    
                    if price and size and size > 0:
                        self._local_orderbooks[symbol]['bids'][price] = size
                
                for ask in orderbook_data.get('asks', []):
                    price = None
                    size = None
                    
                    if isinstance(ask, dict):
                        # 🔥 字典格式：{'price': '...', 'size': '...'}
                        price = self._safe_decimal(ask.get('price'))
                        size = self._safe_decimal(ask.get('size'))
                    elif isinstance(ask, list) and len(ask) >= 2:
                        # 数组格式：["price", "size"]
                        price = self._safe_decimal(ask[0])
                        size = self._safe_decimal(ask[1])
                    
                    if price and size and size > 0:
                        self._local_orderbooks[symbol]['asks'][price] = size
                
            elif depth_type == 'CHANGED':
                # === 增量模式：应用增量更新到本地订单簿 ===
                if symbol not in self._local_orderbooks:
                    # 🔥 宽容策略：如果本地没有快照，把首次增量当作部分快照初始化
                    # 这是为了应对EdgeX可能不推送快照的情况
                    if self.logger:
                        self.logger.debug(f"📦 {symbol}: 首次增量更新，初始化本地订单簿")
                    self._local_orderbooks[symbol] = {
                        'bids': {},
                        'asks': {}
                    }
                
                # 🔥 诊断：检查收到的数据（DEBUG级别，减少日志写入）
                bids_raw = orderbook_data.get('bids', [])
                asks_raw = orderbook_data.get('asks', [])
                if self.logger:
                    self.logger.debug(
                        f"🔍 {symbol}: 增量更新数据 - "
                        f"bids数量={len(bids_raw)}, asks数量={len(asks_raw)}"
                    )
                
                # 应用增量更新到买盘（支持数组和字典格式）
                bids_processed = 0
                bids_added = 0
                bids_deleted = 0
                for bid in bids_raw:
                    price = None
                    size = None
                    
                    if isinstance(bid, dict):
                        # 🔥 字典格式：{'price': '...', 'size': '...'}
                        price = self._safe_decimal(bid.get('price'))
                        size = self._safe_decimal(bid.get('size'))
                    elif isinstance(bid, list) and len(bid) >= 2:
                        # 数组格式：["price", "size"]
                        price = self._safe_decimal(bid[0])
                        size = self._safe_decimal(bid[1])
                    else:
                        continue  # 跳过未知格式
                    
                    bids_processed += 1
                    
                    if not price:
                        if self.logger:
                            self.logger.warning(f"⚠️  {symbol}: bids[{bids_processed-1}]价格解析失败: {bid}")
                        continue
                    
                    if size == 0:
                        # size=0：删除该价格档位
                        self._local_orderbooks[symbol]['bids'].pop(price, None)
                        bids_deleted += 1
                    elif size > 0:
                        # size>0：更新/新增该价格档位
                        self._local_orderbooks[symbol]['bids'][price] = size
                        bids_added += 1
                
                if self.logger and bids_processed > 0:
                    self.logger.debug(
                        f"📊 {symbol}: bids处理完成 - "
                        f"处理={bids_processed}档, 新增/更新={bids_added}档, 删除={bids_deleted}档"
                    )
                
                # 应用增量更新到卖盘（支持数组和字典格式）
                asks_processed = 0
                asks_added = 0
                asks_deleted = 0
                for ask in asks_raw:
                    price = None
                    size = None
                    
                    if isinstance(ask, dict):
                        # 🔥 字典格式：{'price': '...', 'size': '...'}
                        price = self._safe_decimal(ask.get('price'))
                        size = self._safe_decimal(ask.get('size'))
                    elif isinstance(ask, list) and len(ask) >= 2:
                        # 数组格式：["price", "size"]
                        price = self._safe_decimal(ask[0])
                        size = self._safe_decimal(ask[1])
                    else:
                        continue  # 跳过未知格式
                    
                    asks_processed += 1
                    
                    if not price:
                        if self.logger:
                            self.logger.warning(f"⚠️  {symbol}: asks[{asks_processed-1}]价格解析失败: {ask}")
                        continue
                    
                    if size == 0:
                        # size=0：删除该价格档位
                        self._local_orderbooks[symbol]['asks'].pop(price, None)
                        asks_deleted += 1
                    elif size > 0:
                        # size>0：更新/新增该价格档位
                        self._local_orderbooks[symbol]['asks'][price] = size
                        asks_added += 1
                
                if self.logger and asks_processed > 0:
                    self.logger.debug(
                        f"📊 {symbol}: asks处理完成 - "
                        f"处理={asks_processed}档, 新增/更新={asks_added}档, 删除={asks_deleted}档"
                    )
                
                # 🔥 诊断：显示处理后的订单簿状态（DEBUG级别，减少日志写入）
                if self.logger:
                    final_bids_count = len(self._local_orderbooks[symbol]['bids'])
                    final_asks_count = len(self._local_orderbooks[symbol]['asks'])
                    self.logger.debug(
                        f"🔄 {symbol}: 增量更新处理完成 - "
                        f"处理后: bids={final_bids_count}档, asks={final_asks_count}档"
                    )
            
            else:
                # 未知类型，按快照处理
                if self.logger:
                    self.logger.warning(f"⚠️  {symbol}: 未知depthType={depth_type}，按快照处理")
                depth_type = 'SNAPSHOT'
            
            # 🔥 从本地订单簿构造完整的OrderBookData对象
            if symbol not in self._local_orderbooks:
                return
            
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
                if self.logger:
                    # 🔥 改为DEBUG级别，因为这是正常情况（增量更新可能只有一侧）
                    self.logger.debug(
                        f"⏸️  {symbol}: 订单簿暂不完整 "
                        f"(bids={len(bids)}, asks={len(asks)}), 等待后续更新"
                    )
                return

            # 创建OrderBookData对象
            main_timestamp = exchange_timestamp if exchange_timestamp else datetime.now()
            
            orderbook = OrderBookData(
                symbol=symbol,
                bids=bids,
                asks=asks,
                timestamp=main_timestamp,
                nonce=orderbook_data.get('endVersion'),
                exchange_timestamp=exchange_timestamp,
                raw_data=orderbook_data
            )

            # 🔥 只有完整的订单簿才触发回调（DEBUG级别，减少日志写入）
            if self.logger:
                self.logger.debug(
                    f"✅ {symbol}: 订单簿完整 (bids={len(bids)}, asks={len(asks)}), "
                    f"bid1={bids[0].price if bids else 'N/A'}, ask1={asks[0].price if asks else 'N/A'}"
                )
            
            if self.orderbook_callback:
                await self._safe_callback_with_symbol(self.orderbook_callback, symbol, orderbook)
            
            # 调用特定的订阅回调
            callback_count = 0
            for sub_type, sub_symbol, callback in self._ws_subscriptions:
                if sub_type == 'orderbook' and sub_symbol == symbol:
                    await self._safe_callback(callback, orderbook)
                    callback_count += 1
            
            if self.logger and callback_count > 0:
                self.logger.debug(f"✅ {symbol}: 已触发{callback_count}个订单簿回调")

        except Exception as e:
            if self.logger:
                self.logger.warning(f"处理EdgeX订单簿更新失败: {e}")
                self.logger.debug(f"频道: {channel}, 内容: {content}")
                import traceback
                self.logger.debug(f"错误堆栈: {traceback.format_exc()}")

    async def _handle_trade_update(self, channel: str, content: Dict[str, Any]) -> None:
        """处理成交更新"""
        try:
            # 从频道名称提取contractId
            contract_id = channel.split('.')[-1]  # trades.10000001 -> 10000001
            symbol = self._contract_mappings.get(contract_id)
            
            if not symbol:
                if self.logger:
                    self.logger.debug(f"未找到合约ID {contract_id} 对应的交易对")
                return

            # 解析成交数据
            data_list = content.get('data', [])
            for trade_data in data_list:
                # 解析交易时间戳
                trade_timestamp = None
                if 'timestamp' in trade_data:
                    try:
                        timestamp_ms = int(trade_data['timestamp'])
                        trade_timestamp = datetime.fromtimestamp(timestamp_ms / 1000)
                    except (ValueError, TypeError):
                        trade_timestamp = datetime.now()
                else:
                    trade_timestamp = datetime.now()
                
                trade = TradeData(
                    id=str(trade_data.get('tradeId', '')),
                    symbol=symbol,
                    side=trade_data.get('side', ''),
                    amount=self._safe_decimal(trade_data.get('size')),
                    price=self._safe_decimal(trade_data.get('price')),
                    cost=self._safe_decimal(trade_data.get('size', 0)) * self._safe_decimal(trade_data.get('price', 0)),
                    fee=None,
                    timestamp=trade_timestamp,
                    order_id=None,
                    raw_data=trade_data
                )

                # 调用回调函数
                await self._safe_callback(self.trades_callback, trade)
                
                # 调用特定的订阅回调
                for sub_type, sub_symbol, callback in self._ws_subscriptions:
                    if sub_type == 'trades' and sub_symbol == symbol:
                        await self._safe_callback(callback, trade)

        except Exception as e:
            if self.logger:
                self.logger.warning(f"处理EdgeX成交更新失败: {e}")
                self.logger.debug(f"频道: {channel}, 内容: {content}")

    async def _handle_metadata_update(self, content: Dict[str, Any]) -> None:
        """处理metadata更新"""
        try:
            if self.logger:
                self.logger.info("收到metadata更新")
            await self._process_metadata_response({'content': content})
        except Exception as e:
            if self.logger:
                self.logger.warning(f"处理metadata更新失败: {e}")

    async def _process_metadata_response(self, data: Dict[str, Any]) -> None:
        """处理metadata响应数据"""
        try:
            if self.logger:
                self.logger.info(f"开始处理metadata响应")
            
            content = data.get("content", {})
            
            # 根据分析结果，合约数据位于: content.data[0].contractList
            metadata_data = content.get("data", [])
            
            if metadata_data and isinstance(metadata_data, list) and len(metadata_data) > 0:
                first_item = metadata_data[0]
                
                # EdgeX实际使用contractList字段
                contracts = first_item.get("contractList", [])
                if not contracts:
                    contracts = first_item.get("contract", [])
                
                if not contracts:
                    if self.logger:
                        self.logger.warning("❌ 未找到任何合约数据")
                    return
                    
                supported_symbols = []
                contract_mappings = {}
                symbol_contract_mappings = {}
                
                total_contracts = len(contracts)
                
                if self.logger:
                    self.logger.info(f"开始处理 {total_contracts} 个合约...")
                
                metadata_samples: List[str] = []
                sample_limit = max(self._METADATA_LOG_SAMPLE_LIMIT, 1)
                
                for contract in contracts:
                    contract_id = contract.get("contractId")
                    symbol = contract.get("contractName") or contract.get("symbol")
                    enable_trade = contract.get("enableTrade", False)
                    enable_display = contract.get("enableDisplay", False)
                    
                    if contract_id and symbol and enable_trade and enable_display:
                        contract_id_str = str(contract_id)
                        # 将symbol转换为标准格式
                        normalized_symbol = self._normalize_contract_symbol(symbol)
                        
                        supported_symbols.append(normalized_symbol)
                        contract_mappings[contract_id_str] = normalized_symbol
                        symbol_contract_mappings[normalized_symbol] = contract_id_str
                        
                        if len(metadata_samples) < sample_limit:
                            metadata_samples.append(f"{symbol}->{normalized_symbol} (ID:{contract_id_str})")
                
                # 更新实例变量
                self._supported_symbols = supported_symbols
                self._contract_mappings = contract_mappings
                self._symbol_contract_mappings = symbol_contract_mappings
                
                if self.logger:
                    total_symbols = len(supported_symbols)
                    if metadata_samples:
                        remaining = max(total_symbols - len(metadata_samples), 0)
                        preview = "，".join(metadata_samples)
                        if remaining > 0:
                            self.logger.info(
                                "✅ 成功解析metadata，获取 %s 个可用交易对（示例: %s，其余 %s 个省略）",
                                total_symbols,
                                preview,
                                remaining
                            )
                        else:
                            self.logger.info(
                                "✅ 成功解析metadata，获取 %s 个可用交易对（全部: %s）",
                                total_symbols,
                                preview
                            )
                    else:
                        self.logger.info(f"✅ 成功解析metadata，获取 {len(supported_symbols)} 个可用交易对")

        except Exception as e:
            if self.logger:
                self.logger.warning(f"处理metadata响应时出错: {e}")

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
                self.logger.warning(f"EdgeX回调函数执行失败: {e}")

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
                self.logger.warning(f"EdgeX回调函数执行失败: {e}")

    # === 订阅接口 ===

    async def subscribe_ticker(self, symbol: str, callback: Callable[[TickerData], None]) -> None:
        """订阅行情数据流"""
        try:
            self._ws_subscriptions.append(('ticker', symbol, callback))
            
            contract_id = self._symbol_contract_mappings.get(symbol)
            if not contract_id:
                if self.logger:
                    self.logger.warning(f"未找到交易对 {symbol} 的合约ID")
                return
            
            subscribe_msg = {
                "type": "subscribe",
                "channel": f"ticker.{contract_id}"
            }
            
            if await self._safe_send_message(json.dumps(subscribe_msg)):
                if self.logger:
                    self.logger.debug(f"已订阅 {symbol} 的ticker")
            else:
                if self.logger:
                    self.logger.warning(f"发送 {symbol} ticker订阅消息失败")
                    
        except Exception as e:
            if self.logger:
                self.logger.warning(f"订阅ticker失败: {e}")

    async def subscribe_orderbook(self, symbol: str, callback: Callable[[OrderBookData], None], depth: int = 15) -> None:
        """订阅订单簿数据流"""
        try:
            self._ws_subscriptions.append(('orderbook', symbol, callback))
            
            #🔥 确保WebSocket连接已建立
            if not self._ws_connection or not self._ws_connected:
                if self.logger:
                    self.logger.info(f"EdgeX WebSocket未连接，正在建立连接...")
                await self.connect()
                # 等待连接建立
                await asyncio.sleep(0.5)
            
            contract_id = self._symbol_contract_mappings.get(symbol)
            if not contract_id:
                if self.logger:
                    self.logger.warning(f"未找到交易对 {symbol} 的合约ID")
                return
            
            subscribe_msg = {
                "type": "subscribe",
                "channel": f"depth.{contract_id}.{depth}"
            }
            
            if await self._safe_send_message(json.dumps(subscribe_msg)):
                if self.logger:
                    self.logger.info(f"✅ EdgeX已订阅 {symbol} 的orderbook")
            else:
                if self.logger:
                    self.logger.warning(f"⚠️  发送 {symbol} orderbook订阅消息失败（连接状态: {self._ws_connected}）")
                    
        except Exception as e:
            if self.logger:
                self.logger.warning(f"订阅orderbook失败: {e}")
                import traceback
                self.logger.error(traceback.format_exc())

    async def subscribe_trades(self, symbol: str, callback: Callable[[TradeData], None]) -> None:
        """订阅成交数据流"""
        try:
            self._ws_subscriptions.append(('trades', symbol, callback))
            
            contract_id = self._symbol_contract_mappings.get(symbol)
            if not contract_id:
                if self.logger:
                    self.logger.warning(f"未找到交易对 {symbol} 的合约ID")
                return
            
            subscribe_msg = {
                "type": "subscribe",
                "channel": f"trades.{contract_id}"
            }
            
            if await self._safe_send_message(json.dumps(subscribe_msg)):
                if self.logger:
                    self.logger.debug(f"已订阅 {symbol} 的trades")
            else:
                if self.logger:
                    self.logger.warning(f"发送 {symbol} trades订阅消息失败")
                    
        except Exception as e:
            if self.logger:
                self.logger.warning(f"订阅trades失败: {e}")

    async def subscribe_metadata(self) -> None:
        """订阅metadata频道获取支持的交易对"""
        try:
            subscribe_msg = {
                "type": "subscribe",
                "channel": "metadata"
            }
            
            if await self._safe_send_message(json.dumps(subscribe_msg)):
                if self.logger:
                    self.logger.debug("已订阅metadata频道")
            else:
                if self.logger:
                    self.logger.warning("发送metadata订阅消息失败")
                    
        except Exception as e:
            if self.logger:
                self.logger.warning(f"订阅metadata失败: {e}")

    async def batch_subscribe_tickers(self, symbols: Optional[List[str]] = None, callback: Optional[Callable[[str, TickerData], None]] = None) -> None:
        """批量订阅多个交易对的ticker数据"""
        try:
            if symbols is None:
                symbols = self._supported_symbols
                
            if self.logger:
                self.logger.info(f"开始批量订阅 {len(symbols)} 个交易对的ticker数据")
            
            # 设置全局回调
            if callback:
                self.ticker_callback = callback
            
            # 🔥 修复：保存订阅信息到重连列表
            for symbol in symbols:
                # 检查是否已经存在相同的订阅
                existing_sub = None
                for i, (sub_type, sub_symbol, sub_callback) in enumerate(self._ws_subscriptions):
                    if sub_type == 'ticker' and sub_symbol == symbol:
                        existing_sub = i
                        break
                
                if existing_sub is not None:
                    # 更新现有订阅的回调
                    self._ws_subscriptions[existing_sub] = ('ticker', symbol, callback)
                else:
                    # 添加新的订阅
                    self._ws_subscriptions.append(('ticker', symbol, callback))
            
            # 批量订阅
            for symbol in symbols:
                try:
                    contract_id = self._symbol_contract_mappings.get(symbol)
                    if not contract_id:
                        if self.logger:
                            self.logger.warning(f"未找到交易对 {symbol} 的合约ID，跳过订阅")
                        continue
                    
                    subscribe_msg = {
                        "type": "subscribe",
                        "channel": f"ticker.{contract_id}"
                    }
                    
                    if await self._safe_send_message(json.dumps(subscribe_msg)):
                        if self.logger:
                            self.logger.debug(f"已订阅 {symbol} (合约ID: {contract_id}) 的ticker")
                    else:
                        if self.logger:
                            self.logger.warning(f"发送 {symbol} ticker订阅消息失败")
                    
                    # 小延迟避免过于频繁的请求
                    await asyncio.sleep(0.1)
                    
                except Exception as e:
                    if self.logger:
                        self.logger.warning(f"订阅 {symbol} ticker时出错: {e}")
                    continue
                    
            if self.logger:
                self.logger.info(f"批量ticker订阅完成")
            
        except Exception as e:
            if self.logger:
                self.logger.warning(f"批量订阅ticker时出错: {e}")

    async def batch_subscribe_orderbooks(self, symbols: Optional[List[str]] = None, depth: int = 15, callback: Optional[Callable[[str, OrderBookData], None]] = None) -> None:
        """批量订阅多个交易对的orderbook数据"""
        try:
            if symbols is None:
                symbols = self._supported_symbols
                
            if self.logger:
                self.logger.info(f"开始批量订阅 {len(symbols)} 个交易对的orderbook数据")
            
            # 设置全局回调
            if callback:
                self.orderbook_callback = callback
            
            # 🔥 修复：保存订阅信息到重连列表
            for symbol in symbols:
                # 检查是否已经存在相同的订阅
                existing_sub = None
                for i, (sub_type, sub_symbol, sub_callback) in enumerate(self._ws_subscriptions):
                    if sub_type == 'orderbook' and sub_symbol == symbol:
                        existing_sub = i
                        break
                
                if existing_sub is not None:
                    # 更新现有订阅的回调
                    self._ws_subscriptions[existing_sub] = ('orderbook', symbol, callback)
                else:
                    # 添加新的订阅
                    self._ws_subscriptions.append(('orderbook', symbol, callback))
            
            # 批量订阅
            for symbol in symbols:
                try:
                    contract_id = self._symbol_contract_mappings.get(symbol)
                    if not contract_id:
                        if self.logger:
                            self.logger.warning(f"未找到交易对 {symbol} 的合约ID，跳过订阅")
                        continue
                    
                    subscribe_msg = {
                        "type": "subscribe",
                        "channel": f"depth.{contract_id}.{depth}"
                    }
                    
                    if await self._safe_send_message(json.dumps(subscribe_msg)):
                        if self.logger:
                            self.logger.debug(f"已订阅 {symbol} (合约ID: {contract_id}) 的orderbook")
                    else:
                        if self.logger:
                            self.logger.warning(f"发送 {symbol} orderbook订阅消息失败")
                    
                    # 小延迟避免过于频繁的请求
                    await asyncio.sleep(0.1)
                    
                except Exception as e:
                    if self.logger:
                        self.logger.warning(f"订阅 {symbol} orderbook时出错: {e}")
                    continue
                    
            if self.logger:
                self.logger.info(f"批量orderbook订阅完成")
            
        except Exception as e:
            if self.logger:
                self.logger.warning(f"批量订阅orderbook时出错: {e}")

    async def unsubscribe(self, symbol: Optional[str] = None) -> None:
        """取消订阅"""
        try:
            if symbol:
                # 取消特定符号的订阅
                subscriptions_to_remove = []
                for sub_type, sub_symbol, callback in self._ws_subscriptions:
                    if sub_symbol == symbol:
                        subscriptions_to_remove.append((sub_type, sub_symbol, callback))
                
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
        return self._supported_symbols.copy()

    async def fetch_supported_symbols(self) -> None:
        """通过WebSocket获取支持的交易对"""
        try:
            if self.logger:
                self.logger.info("开始获取EdgeX支持的交易对列表...")
            
            # 如果还没有连接，先连接
            if not self._ws_connection:
                await self.connect()
            
            # 订阅metadata频道
            await self.subscribe_metadata()
            
            # 等待metadata响应
            timeout = 10
            start_time = time.time()
            
            while time.time() - start_time < timeout:
                if self._supported_symbols:
                    break
                await asyncio.sleep(0.5)
            
            if not self._supported_symbols:
                if self.logger:
                    self.logger.warning("未能获取到支持的交易对")
            else:
                if self.logger:
                    self.logger.info(f"成功获取到 {len(self._supported_symbols)} 个交易对")
                    
        except Exception as e:
            if self.logger:
                self.logger.warning(f"获取支持的交易对时出错: {e}")

    # 向后兼容方法
    async def subscribe_order_book(self, symbol: str, callback, depth: int = 20):
        """订阅订单簿数据 - 向后兼容"""
        await self.subscribe_orderbook(symbol, callback, depth)

    async def subscribe_user_data(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """
        订阅用户数据流
        
        旧实现假设 EdgeX 私有 WS 会自动推送用户数据，但线上验证发现仍需要显式发送
        `{"type":"subscribe","channel":"userData"}`。因此这里在注册回调后补发订阅消息。
        """
        try:
            self._ws_subscriptions.append(('user_data', None, callback))
            self.user_data_callback = callback  # 向后兼容
            
            # 🔥 同时添加到回调列表（支持多个回调）
            if callback not in self._user_data_callbacks:
                self._user_data_callbacks.append(callback)
            
            subscribe_msg = self._build_subscribe_message('user_data', None)
            if await self._safe_send_message(subscribe_msg):
                if self.logger:
                    self.logger.info(
                        "✅ [EdgeX] 用户数据流已订阅\n"
                        "   channel=userData"
                    )
            else:
                if self.logger:
                    self.logger.warning("⚠️ [EdgeX] 用户数据流订阅消息发送失败，等待重连后重试")
                    
        except Exception as e:
            if self.logger:
                self.logger.warning(f"注册用户数据回调失败: {e}")
    
    async def subscribe_order_updates(self, callback: Callable) -> None:
        """
        订阅订单更新流（异步回调）
        
        注意：userData流已经包含订单更新，需要先调用subscribe_user_data
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
                f"   注意：订单更新包含在userData流中，请确保已调用subscribe_user_data"
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
    
    async def subscribe_positions(self, callback: Callable) -> None:
        """
        订阅持仓更新流
        
        Args:
            callback: 持仓更新回调函数，接收参数：持仓数据字典
        """
        if not hasattr(self, '_position_callbacks'):
            self._position_callbacks = []
        
        self._position_callbacks.append(callback)
        
        if self.logger:
            self.logger.info(
                f"✅ 持仓更新回调已注册\n"
                f"   注意：持仓更新包含在userData流中，请确保已调用subscribe_user_data"
            )

    async def unsubscribe_all(self) -> None:
        """取消所有订阅"""
        try:
            if self._ws_connection:
                unsubscribe_message = {
                    "type": "unsubscribe_all"
                }
                await self._ws_connection.send_str(json.dumps(unsubscribe_message))
                self.logger.info("已取消所有EdgeX订阅")
                
                # 清空所有订阅
                self._ws_subscriptions.clear()
                self._ticker_callbacks.clear()
                self._orderbook_callbacks.clear()
                self._trade_callbacks.clear()
                self._user_data_callbacks.clear()
                
        except Exception as e:
            self.logger.warning(f"取消所有EdgeX订阅失败: {e}")

    # === 兼容性方法 - 保持与原始实现的兼容性 ===

    async def _subscribe_websocket(self, sub_type: str, symbol: Optional[str], callback: Callable) -> None:
        """WebSocket订阅通用方法 - 与原始实现保持一致"""
        try:
            # 初始化订阅列表
            if not hasattr(self, '_ws_subscriptions'):
                self._ws_subscriptions = []

            # 添加订阅
            self._ws_subscriptions.append((sub_type, symbol, callback))

            # 如果还没有WebSocket连接，创建一个
            if not self._ws_connection:
                await self._setup_websocket_connection()

            # 发送订阅消息
            if self._ws_connection:
                subscribe_msg = self._build_subscribe_message(sub_type, symbol)
                await self._ws_connection.send_str(subscribe_msg)

        except Exception as e:
            self.logger.warning(f"EdgeX WebSocket订阅失败 {sub_type} {symbol}: {e}")

    async def _setup_websocket_connection(self) -> None:
        """建立WebSocket连接 - 与原始实现保持一致"""
        try:
            # 使用现有的connect方法
            await self.connect()
            
            self.logger.info(f"EdgeX WebSocket连接已建立: {self.ws_url}")

        except Exception as e:
            self.logger.warning(f"建立EdgeX WebSocket连接失败: {e}")

    def _build_subscribe_message(self, sub_type: str, symbol: Optional[str]) -> str:
        """构建订阅消息 - 基于EdgeX实际API格式"""
        # 使用动态映射系统获取合约ID
        contract_id = self._symbol_contract_mappings.get(symbol, "10000001") if symbol else "10000001"
        
        if sub_type == 'ticker':
            # 24小时ticker统计
            return json.dumps({
                "type": "subscribe",
                "channel": f"ticker.{contract_id}"
            })
        elif sub_type == 'orderbook':
            # 实时订单簿深度
            return json.dumps({
                "type": "subscribe", 
                "channel": f"depth.{contract_id}.15"
            })
        elif sub_type == 'trades':
            # 实时交易流
            return json.dumps({
                "type": "subscribe",
                "channel": f"trades.{contract_id}"
            })
        elif sub_type == 'user_data':
            # 用户数据流需要认证
            return json.dumps({
                "type": "subscribe",
                "channel": "userData"
            })
        else:
            return json.dumps({
                "type": "subscribe",
                "channel": f"ticker.{contract_id}"
            })

    async def _handle_order_update(self, data: Dict[str, Any]) -> None:
        """
        处理订单更新
        
        ✅ EdgeX订单数据格式（基于官方文档）：
        消息结构：
        {
          "type": "trade-event",
          "content": {
            "event": "ORDER_UPDATE",
            "version": "1000",
            "data": {
              "order": [
                {
                  "id": "123456",                    // 订单ID (int64 string)
                  "userId": "...",
                  "accountId": "...",
                  "coinId": "...",
                  "contractId": "10000001",          // 合约ID
                  "side": "BUY" | "SELL",            // 方向：BUY/SELL (全大写)
                  "price": "50000.00",               // 订单价格
                  "size": "0.001",                   // 订单数量
                  "clientOrderId": "client_123",     // 客户端订单ID
                  "type": "LIMIT" | "MARKET",        // 订单类型
                  "timeInForce": "GOOD_TIL_CANCEL",
                  "reduceOnly": false,
                  "status": "OPEN" | "FILLED" | "CANCELED",  // 订单状态 (全大写)
                  "cumMatchSize": "0.0005",          // 累计成交数量 ✅ 关键字段
                  "cumMatchValue": "25.00",
                  "cumMatchFee": "0.01",
                  "createdTime": 1638360000000,
                  "updatedTime": 1638360000000
                }
              ]
            }
          }
        }
        
        🔥 关键字段：
        - id (不是orderId)
        - cumMatchSize (不是filledSize)
        - status: PENDING/OPEN/FILLED/CANCELING/CANCELED/UNTRIGGERED
        """
        try:
            orders = data.get('order', [])
            if not orders:
                return
            
            from ..models import OrderData, OrderStatus, OrderSide, OrderType
            
            for order_info in orders:
                try:
                    # ✅ 解析订单基本信息（严格按文档字段名）
                    order_id = str(order_info.get('id', ''))  # ✅ 直接使用'id'
                    client_order_id = order_info.get('clientOrderId', '')
                    contract_id = order_info.get('contractId', '')
                    
                    # 获取交易对符号
                    symbol = await self._get_symbol_from_contract_id(contract_id) if contract_id else 'UNKNOWN'
                    
                    # ✅ 订单方向（EdgeX使用全大写的BUY/SELL）
                    side_str = order_info.get('side', 'BUY')
                    side = OrderSide.BUY if side_str == 'BUY' else OrderSide.SELL
                    
                    # ✅ 订单类型（EdgeX使用全大写）
                    type_str = order_info.get('type', 'LIMIT')
                    order_type = OrderType.LIMIT if type_str == 'LIMIT' else OrderType.MARKET
                    
                    # 价格和数量
                    price = self._safe_decimal(order_info.get('price', 0))
                    quantity = self._safe_decimal(order_info.get('size', 0))
                    
                    # ✅ 关键：使用cumMatchSize字段（不是filledSize）
                    filled_quantity = self._safe_decimal(order_info.get('cumMatchSize', 0))
                    
                    # ✅ 订单状态映射（EdgeX使用全大写）
                    status_str = order_info.get('status', 'OPEN')
                    status_mapping = {
                        'PENDING': OrderStatus.OPEN,
                        'OPEN': OrderStatus.OPEN,
                        'FILLED': OrderStatus.FILLED,
                        'CANCELING': OrderStatus.OPEN,  # 取消中也算OPEN
                        'CANCELED': OrderStatus.CANCELED,
                        'UNTRIGGERED': OrderStatus.OPEN,  # 未触发的条件单
                    }
                    status = status_mapping.get(status_str, OrderStatus.OPEN)
                    
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
                        raw_data=order_info
                    )
                    
                    if self.logger:
                        self.logger.info(
                            f"✅ [EdgeX订单] order_id={order_id}, symbol={symbol}, "
                            f"status={status.value}, filled={filled_quantity}/{quantity}"
                        )
                    
                    # 触发订单更新回调
                    if hasattr(self, '_order_callbacks') and self._order_callbacks:
                        for callback in self._order_callbacks:
                            await self._safe_callback(callback, order)
                    
                    # 如果订单完全成交，触发成交回调
                    if status == OrderStatus.FILLED and hasattr(self, '_order_fill_callbacks'):
                        for callback in self._order_fill_callbacks:
                            await self._safe_callback(callback, order)
                
                except Exception as e:
                    if self.logger:
                        self.logger.error(f"❌ [EdgeX] 解析单个订单失败: {e}")
                        self.logger.error(f"   订单数据: {order_info}")
        
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ [EdgeX] 处理订单更新失败: {e}", exc_info=True)
                # 只打印数据摘要，避免日志过长
                order_list = data.get('order', [])
                order_ids = [o.get('id', 'N/A') for o in order_list[:3]]  # 最多显示3个
                self.logger.error(f"   订单ID: {order_ids}, 总数: {len(order_list)}")
    
    async def _handle_position_update(self, data: Dict[str, Any]) -> None:
        """
        处理持仓更新
        
        ✅ EdgeX持仓数据格式（基于官方文档）：
        消息结构：
        {
          "type": "trade-event",
          "content": {
            "event": "POSITION_UPDATE",
            "version": "1000",
            "data": {
              "position": [
                {
                  "userId": "...",
                  "accountId": "...",
                  "coinId": "...",
                  "contractId": "10000001",           // 合约ID
                  "openSize": "0.001",                // ✅ 当前持仓大小（正=多，负=空）
                  "openValue": "50.00",               // 当前持仓价值
                  "openFee": "0.05",                  // 当前已分配的开仓费用
                  "fundingFee": "0.01",               // 当前已分配的资金费用
                  "longTermCount": 1,                 // 多仓期数
                  "longTermStat": {...},              // 多仓累计统计
                  "shortTermCount": 0,                // 空仓期数
                  "shortTermStat": {...},             // 空仓累计统计
                  "createdTime": 1638360000000,
                  "updatedTime": 1638360000000
                }
              ]
            }
          }
        }
        
        🔥 关键字段：
        - openSize: 当前持仓大小（带符号，正=多仓，负=空仓）
        - openValue: 当前持仓价值
        - 平均入场价格 = openValue / openSize (需要计算)
        
        ⚠️ 注意：EdgeX的Position对象不直接包含unrealizedPnl和entryPrice，
                这些信息在PositionAsset中，需要额外查询或从账户更新中获取
        """
        try:
            raw_positions = self._normalize_entry_list(
                data.get('position'),
                candidate_keys=['positionList', 'positions', 'data', 'list']
            )
            
            if raw_positions is None:
                # data 为 None 表示没有推送
                return
            
            self._update_position_cache(raw_positions)
            
            # 触发旧式回调（若有业务仍依赖）
            if raw_positions and hasattr(self, '_position_callbacks') and self._position_callbacks:
                for pos_info in raw_positions:
                    try:
                        contract_id = pos_info.get('contractId') or pos_info.get('contract_id')
                        symbol = (
                            self._standardize_symbol(pos_info.get('symbol'), contract_id)
                            or (f"CONTRACT_{contract_id}" if contract_id else None)
                        )
                        if not symbol:
                            continue
                        open_size = self._safe_decimal(
                            pos_info.get('openSize') or pos_info.get('size') or 0
                        )
                        
                        # 🔥 修复：使用longTermCount/shortTermCount修正持仓方向
                        long_count = int(pos_info.get('longTermCount', 0))
                        short_count = int(pos_info.get('shortTermCount', 0))
                        if open_size > 0 and long_count == 0 and short_count > 0:
                            # EdgeX推送openSize为正数，但实际是空仓
                            open_size = -open_size
                        elif open_size < 0 and short_count == 0 and long_count > 0:
                            # openSize为负数，但实际是多仓（理论上不应该发生）
                            open_size = abs(open_size)
                        
                        open_value = self._safe_decimal(pos_info.get('openValue') or 0)
                        entry_price = self._safe_decimal(
                            pos_info.get('entryPrice') or pos_info.get('entry_price') or 0
                        )
                        if entry_price == 0 and open_size != 0:
                            entry_price = abs(open_value / open_size)
                        open_fee = self._safe_decimal(pos_info.get('openFee') or 0)
                        funding_fee = self._safe_decimal(pos_info.get('fundingFee') or 0)
                        side = 'Long' if open_size > 0 else ('Short' if open_size < 0 else 'None')
                        
                        # 🔥 增强日志：打印原始持仓数据以诊断方向问题
                        if self.logger and abs(open_size) > 0:
                            self.logger.info(
                                f"📊 [EdgeX持仓回调] symbol={symbol}, "
                                f"openSize(原始)={pos_info.get('openSize')}/{pos_info.get('size')}, "
                                f"longTermCount={long_count}, shortTermCount={short_count}, "
                                f"openSize(修正后)={open_size}, "
                                f"判断方向={side}, "
                                f"openValue={open_value}"
                            )
                        
                        payload = {
                            'symbol': symbol,
                            'size': open_size,
                            'entry_price': entry_price,
                            'open_value': open_value,
                            'open_fee': open_fee,
                            'funding_fee': funding_fee,
                            'side': side,
                        }
                        for callback in self._position_callbacks:
                            await self._safe_callback(callback, payload)
                    except Exception as callback_error:
                        if self.logger:
                            self.logger.debug(f"⚠️ [EdgeX] 持仓回调处理失败: {callback_error}")
        
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ [EdgeX] 处理持仓更新失败: {e}", exc_info=True)
                position_list = self._normalize_entry_list(
                    data.get('position'),
                    candidate_keys=['positionList', 'positions', 'data', 'list']
                ) or []
                contract_ids = [p.get('contractId', 'N/A') for p in position_list[:3]]
                self.logger.error(f"   合约ID: {contract_ids}, 总数: {len(position_list)}")
    
    async def _get_symbol_from_contract_id(self, contract_id: str) -> str:
        """从contract_id获取交易对符号"""
        # TODO: 实现contract_id到symbol的映射
        # 可以从self._markets_cache或调用REST API获取
        return f"CONTRACT_{contract_id}"
    
    async def _handle_user_data_update(self, data: Dict[str, Any]) -> None:
        """
        处理用户数据更新
        
        EdgeX私有WebSocket数据格式：
        {
          "type": "trade-event",
          "content": {
            "event": "ORDER_UPDATE" | "ACCOUNT_UPDATE" | "POSITION_UPDATE",
            "version": "1000",
            "data": {
              "order": [...],  // 订单列表
              "position": [...],  // 持仓列表
              "orderFillTransaction": [...]  // 成交记录
            }
          }
        }
        """
        try:
            # 提取事件类型
            msg_type = data.get('type', '')
            content = data.get('content', {})
            event_type = content.get('event', '')
            event_data = content.get('data', {})
            
            if self.logger:
                self.logger.debug(f"[EdgeX] 收到用户数据更新: type={msg_type}, event={event_type}")
            
            # 根据事件类型分发
            if msg_type == 'trade-event':
                # 🔥 处理订单更新
                if event_type == 'ORDER_UPDATE' or 'order' in event_data:
                    await self._handle_order_update(event_data)
                
                # 🔥 处理持仓更新  
                if 'position' in event_data or event_type == 'POSITION_UPDATE':
                    await self._handle_position_update(event_data)
                
                # 🔥 处理抵押品/账户更新（余额）- 暂时注释掉，改用 REST 定时刷新
                # if 'collateral' in event_data or 'account' in event_data:
                #     self._update_balance_cache(
                #         collateral_entries=event_data.get('collateral'),
                #         account_entries=event_data.get('account')
                #     )
            
            # 调用通用用户数据回调函数（兼容性）
            for callback in self._user_data_callbacks:
                await self._safe_callback(callback, data)

        except Exception as e:
            if self.logger:
                self.logger.error(f"处理EdgeX用户数据更新失败: {e}", exc_info=True)
                # 只打印数据摘要，避免日志过长
                content = data.get('content', {})
                event_name = content.get('event', 'unknown')
                event_data = content.get('data', {})
                summary = {
                    'event': event_name,
                    'orders': len(event_data.get('order', [])),
                    'positions': len(event_data.get('position', [])),
                    'fills': len(event_data.get('orderFillTransaction', []))
                }
                self.logger.error(f"   消息摘要: {summary}")
    
    def get_cached_balances(self) -> List[BalanceData]:
        """返回最新的WS余额快照"""
        return list(self._balance_cache.values())
    
    def _update_balance_cache(
        self,
        collateral_entries: Optional[Any] = None,
        account_entries: Optional[Any] = None
    ) -> None:
        try:
            collateral_entries = self._normalize_entry_list(
                collateral_entries,
                candidate_keys=[
                    'collateralList',
                    'collateralAssetModelList',
                    'list',
                    'data',
                ],
            ) or []
            account_entries = self._normalize_entry_list(
                account_entries,
                candidate_keys=[
                    'accountList',
                    'accounts',
                    'list',
                    'data',
                ],
            ) or []
            
            balance_map: Dict[str, BalanceData] = {}
            collateral_balances = self._parse_collateral_entries(collateral_entries) if collateral_entries else []
            for bal in collateral_balances:
                balance_map[bal.currency] = bal
            
            account_balances = self._parse_account_entries(account_entries) if account_entries else []
            for acc in account_balances:
                if acc.currency in balance_map:
                    base = balance_map[acc.currency]
                    base.free = acc.free
                    base.used = acc.used
                    base.total = acc.total
                    base.usd_value = acc.usd_value
                    if isinstance(base.raw_data, dict):
                        base.raw_data.update(acc.raw_data)
                else:
                    balance_map[acc.currency] = acc
            
            if not balance_map:
                return
            
            self._balance_cache = balance_map
            self._balance_cache_timestamp = time.time()
            
            if (
                self.logger
                and time.time() - self._last_balance_log_time >= self._balance_log_interval
            ):
                summary = ", ".join(
                    f"{bal.currency}={bal.total.normalize()}" for bal in list(balance_map.values())[:5]
                )
                self.logger.info(f"✅ [EdgeX] WS余额更新: {summary}")
                self._last_balance_log_time = time.time()
        except Exception as e:
            if self.logger:
                self.logger.warning(f"⚠️ [EdgeX] 解析WS余额失败: {e}", exc_info=True)
    
    def _parse_collateral_entries(self, entries: List[Dict[str, Any]]) -> List[BalanceData]:
        """
        解析 collateral 和 collateralAssetModelList 数据
        
        关键字段：
        - totalEquity: 账户权益（包含未实现盈亏）
        - availableAmount: 可用余额（减去持仓占用）
        """
        balances: List[BalanceData] = []
        now = datetime.now()
        for item in entries:
            currency = self._resolve_currency_symbol(item)
            
            # 🔥 Step 1: 优先提取 totalEquity（账户权益）
            total_equity = self._extract_decimal(
                item,
                [
                    'totalEquity',          # 账户权益（首选）
                    'equity',
                    'netCollateral',
                    'totalCollateral',
                    'totalCollateralValue',
                ],
                default=None
            )
            
            # 🔥 Step 2: 提取 availableAmount（可用余额）
            available_amount = self._extract_decimal(
                item,
                [
                    'availableAmount',      # 可用余额（首选）
                    'available',
                    'availableBalance',
                    'availableMargin',
                    'usableAmount',
                    'free',
                    'withdrawableAmount',
                ],
                default='0'
            )
            
            # 🔥 Step 3: 如果没有 totalEquity，使用 availableAmount 作为 total
            if total_equity is None or total_equity <= Decimal('0'):
                total_equity = available_amount
            
            # 🔥 Step 4: 计算已用余额（账户权益 - 可用余额）
            used_amount = total_equity - available_amount if total_equity > available_amount else Decimal('0')
            
            balances.append(
                BalanceData(
                    currency=currency,
                    free=available_amount,   # 🔥 可用余额
                    used=used_amount,         # 🔥 已用余额（持仓占用+订单冻结）
                    total=total_equity,       # 🔥 账户权益
                    usd_value=total_equity,   # 🔥 USD价值=账户权益
                    timestamp=now,
                    raw_data={'source': 'ws', **item},
                )
            )
        return balances
    
    def _parse_account_entries(self, entries: List[Dict[str, Any]]) -> List[BalanceData]:
        """
        解析 account 数据
        
        关键字段：
        - totalEquity/equity/netAssetValue: 账户净值
        - netCollateral: 净抵押品
        """
        balances: List[BalanceData] = []
        now = datetime.now()
        for item in entries:
            currency = self._resolve_currency_symbol(item)
            
            # 🔥 提取账户权益（账户净值）
            equity = self._extract_decimal(
                item,
                [
                    'totalEquity',          # 总权益（首选）
                    'equity',
                    'netAssetValue',        # 净资产价值
                    'netCollateral',        # 净抵押品
                    'totalCollateral',
                    'totalCollateralValue',
                    'usdValue',
                    'balance',
                ],
                default=None
            )
            
            if equity is None or equity <= Decimal('0'):
                equity = self._extract_decimal(item, ['amount', 'total'], default='0')
            
            # 🔥 对于 account 数据，totalEquity 就是账户权益
            # 可用余额与已用余额无法从 account 数据中区分，所以都设为 totalEquity
            total = equity
            free = equity
            usd_value = equity
            
            balances.append(
                BalanceData(
                    currency=currency,
                    free=free,
                    used=Decimal('0'),
                    total=total,
                    usd_value=usd_value,
                    timestamp=now,
                    raw_data={'source': 'ws', **item},
                )
            )
        return balances
    
    def _extract_decimal(self, data: Dict[str, Any], keys: List[str], default: Any = '0') -> Optional[Decimal]:
        for key in keys:
            if key in data and data[key] is not None:
                return self._safe_decimal(data[key])
        if default is None:
            return None
        return self._safe_decimal(default)

    def _normalize_entry_list(
        self,
        source: Optional[Any],
        candidate_keys: Optional[List[str]] = None
    ) -> Optional[List[Dict[str, Any]]]:
        if source is None:
            return None
        if isinstance(source, list):
            return source
        if isinstance(source, dict):
            if candidate_keys:
                for key in candidate_keys:
                    nested = source.get(key)
                    if isinstance(nested, list):
                        return nested
            return [source]
        return None

    def _standardize_symbol(
        self,
        symbol: Optional[str],
        contract_id: Optional[Union[str, int]] = None
    ) -> Optional[str]:
        if not symbol and contract_id is not None:
            contract_key = str(contract_id)
            symbol = (
                self._contract_mappings.get(contract_key)
                or self._contract_mappings.get(contract_id)
            )
        if not symbol:
            return None
        
        normalized = str(symbol).upper().replace('/', '-').replace('_', '-')
        if normalized.startswith("CONTRACT-"):
            contract_key = normalized.split("-", 1)[1]
            mapped = self._contract_mappings.get(contract_key)
            if mapped:
                normalized = str(mapped).upper()
        
        if normalized.endswith("-PERP"):
            return normalized
        if normalized.endswith("USDC"):
            base = normalized[:-4]
            return f"{base}-USDC-PERP"
        if normalized.endswith("USDT"):
            base = normalized[:-4]
            return f"{base}-USDC-PERP"
        if normalized.endswith("USD"):
            base = normalized[:-3]
            return f"{base}-USDC-PERP"
        if "-" in normalized:
            parts = normalized.split("-")
            if len(parts) == 2:
                base, quote = parts
                quote = "USDC" if quote in {"USD", "USDT"} else quote
                return f"{base}-{quote}-PERP"
        
        return f"{normalized}-USDC-PERP"
    
    def _resolve_currency_symbol(self, entry: Dict[str, Any]) -> str:
        """
        解析货币符号，特别处理 EdgeX 的 coinId 映射
        
        EdgeX coinId 映射：
        - 1000 = USDC
        """
        currency = (
            entry.get('currency')
            or entry.get('coin')
            or entry.get('coinName')
            or entry.get('asset')
            or entry.get('collateralCoin')
            or entry.get('symbol')
        )
        
        # 🔥 如果没有 currency 字段，尝试从 coinId 映射
        if not currency:
            coin_id = entry.get('coinId') or entry.get('coin_id')
            if coin_id is not None:
                coin_key = str(coin_id)
                
                # 🔥 EdgeX 标准 coinId 映射
                EDGEX_COIN_ID_MAP = {
                    '1000': 'USDC',
                    '1001': 'USDT',
                    '1002': 'ETH',
                    '1003': 'BTC',
                }
                
                # 优先使用标准映射
                currency = EDGEX_COIN_ID_MAP.get(coin_key)
                
                # 如果标准映射没有，尝试实例的映射
                if not currency:
                    currency = self._coin_currency_map.get(coin_key)
                
                # 如果都没有，使用 coinId 原值（但记录警告）
                if not currency:
                    currency = f"COIN_{coin_key}"
                    if self.logger:
                        self.logger.warning(f"⚠️ [EdgeX] 未知的 coinId: {coin_key}，使用 {currency}")
        
        # 默认值
        if not currency:
            currency = 'USDC'
        
        return str(currency).upper()

    def _convert_position_entry(
        self,
        pos_info: Dict[str, Any],
        timestamp: datetime
    ) -> Tuple[Optional[str], Optional[PositionData]]:
        if not isinstance(pos_info, dict):
            return None, None
        
        contract_id = pos_info.get('contractId') or pos_info.get('contract_id')
        symbol = self._standardize_symbol(pos_info.get('symbol'), contract_id)
        if not symbol and contract_id:
            symbol = f"CONTRACT_{contract_id}"
        if not symbol:
            return None, None
        
        size_raw = self._safe_decimal(
            pos_info.get('openSize') or pos_info.get('size') or 0
        )
        if size_raw == 0:
            return symbol, None
        
        # 🔥 EdgeX持仓方向判断逻辑
        # 方法1：如果openSize带符号，直接使用
        # 方法2：如果openSize不带符号，使用longTermCount/shortTermCount判断
        if size_raw > 0:
            side = PositionSide.LONG
        elif size_raw < 0:
            side = PositionSide.SHORT
        else:
            # size_raw == 0，已经在上面return了
            side = PositionSide.LONG  # 默认（不应该走到这里）
        
        # 🔥 修复：如果openSize是正数但longTermCount=0且shortTermCount>0，说明是空仓
        long_count = int(pos_info.get('longTermCount', 0))
        short_count = int(pos_info.get('shortTermCount', 0))
        if size_raw > 0 and long_count == 0 and short_count > 0:
            # 修正：openSize是正数但实际是空仓
            side = PositionSide.SHORT
            size_raw = -size_raw  # 转换为负数
        elif size_raw < 0 and short_count == 0 and long_count > 0:
            # 修正：openSize是负数但实际是多仓（理论上不应该发生）
            side = PositionSide.LONG
            size_raw = abs(size_raw)
        
        size_abs = abs(size_raw)
        
        # 🔥 增强日志：打印原始持仓数据以诊断方向问题
        if self.logger and abs(size_raw) > 0:
            self.logger.info(
                f"💾 [EdgeX缓存] symbol={symbol}, "
                f"openSize(原始)={pos_info.get('openSize')}/{pos_info.get('size')}, "
                f"longTermCount={long_count}, shortTermCount={short_count}, "
                f"size_raw(修正后)={size_raw}, "
                f"判断side={side.value}, "
                f"size_abs={size_abs}"
            )
        
        open_value = self._safe_decimal(pos_info.get('openValue') or 0)
        entry_price = self._safe_decimal(
            pos_info.get('entryPrice') or pos_info.get('entry_price')
        )
        if entry_price == 0 and size_abs != 0 and open_value != 0:
            entry_price = abs(open_value / size_abs)
        
        mark_price = self._safe_decimal(
            pos_info.get('markPrice') or pos_info.get('mark_price') or 0
        )
        current_price = self._safe_decimal(
            pos_info.get('currentPrice')
            or pos_info.get('indexPrice')
            or mark_price
            or entry_price
        )
        
        unrealized_pnl = self._safe_decimal(
            pos_info.get('unRealizedPnl') or pos_info.get('unrealizedPnl') or 0
        )
        realized_pnl = self._safe_decimal(
            pos_info.get('realizedPnl') or pos_info.get('realizedPnL') or 0
        )
        percentage = self._safe_decimal(
            pos_info.get('roe') or pos_info.get('percentage') or 0
        )
        percentage_value = percentage if percentage != 0 else None
        
        leverage = self._safe_int(pos_info.get('leverage')) or 1
        margin_mode_str = str(
            pos_info.get('marginMode') or pos_info.get('margin_mode') or 'cross'
        ).lower()
        margin_mode = (
            MarginMode.ISOLATED if margin_mode_str == 'isolated' else MarginMode.CROSS
        )
        margin_value = self._safe_decimal(
            pos_info.get('margin')
            or pos_info.get('positionMargin')
            or pos_info.get('initialMargin')
            or 0
        )
        liquidation_price = self._safe_decimal(
            pos_info.get('liquidationPrice') or pos_info.get('liquidation_price') or 0
        )
        if liquidation_price == 0:
            liquidation_price = None
        
        position = PositionData(
            symbol=symbol,
            side=side,
            size=size_abs,
            entry_price=entry_price if entry_price != 0 else None,
            mark_price=mark_price if mark_price != 0 else None,
            current_price=current_price if current_price != 0 else None,
            unrealized_pnl=unrealized_pnl,
            realized_pnl=realized_pnl,
            percentage=percentage_value,
            leverage=leverage,
            margin_mode=margin_mode,
            margin=margin_value,
            liquidation_price=liquidation_price,
            timestamp=timestamp,
            raw_data=pos_info,
        )
        position.usd_value = self._safe_decimal(
            pos_info.get('openValue')
            or pos_info.get('positionValue')
            or (entry_price * size_abs if entry_price and size_abs else 0)
        )
        return symbol, position

    def _update_position_cache(self, positions: List[Dict[str, Any]]) -> None:
        updated_symbols: List[str] = []
        removed_symbols: List[str] = []
        now = datetime.now()
        
        for pos_info in positions:
            symbol, position_obj = self._convert_position_entry(pos_info, now)
            if not symbol:
                continue
            
            if position_obj is None:
                if self._position_cache.pop(symbol, None):
                    removed_symbols.append(symbol)
                continue
            
            self._position_cache[symbol] = position_obj
            updated_symbols.append(symbol)
        
        self._position_cache_timestamp = time.time()
        
        should_log = (
            self.logger
            and (updated_symbols or removed_symbols)
            and time.time() - self._last_position_log_time >= self._position_log_interval
        )
        if should_log:
            summary = []
            if updated_symbols:
                summary.append(f"更新: {', '.join(updated_symbols[:5])}")
            if removed_symbols:
                summary.append(f"清除: {', '.join(removed_symbols[:5])}")
            self.logger.info(f"✅ [EdgeX] WS持仓更新 ({' | '.join(summary)})")
            self._last_position_log_time = time.time()

    def get_cached_positions(self) -> List[PositionData]:
        return list(self._position_cache.values())

    # === 属性访问方法 ===

    @property
    def ws_connection(self):
        """WebSocket连接属性"""
        return self._ws_connection

    @property
    def ws_subscriptions(self):
        """WebSocket订阅列表属性"""
        return getattr(self, '_ws_subscriptions', []) 