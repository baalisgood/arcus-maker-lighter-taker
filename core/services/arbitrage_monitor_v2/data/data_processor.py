"""
数据处理层 - 订单簿维护和数据验证

职责：
- 从队列消费数据
- 维护最新的订单簿状态
- 数据验证和清洗
"""

import asyncio
import time
from typing import Dict, Optional, List, Tuple, Set
from datetime import datetime
from collections import defaultdict

from core.adapters.exchanges.models import OrderBookData, TickerData
from core.adapters.exchanges.utils.setup_logging import LoggingConfig
import logging
from ..config.debug_config import DebugConfig
from .latest_store import LatestStore

# 创建独立日志文件，避免输出到终端导致界面抖动
# 高频数据路径，默认降级到 WARNING，避免大行情时日志刷屏造成 I/O 压力
# 高频数据路径，默认使用 WARNING，但过期日志改为 DEBUG（见 _log_stale_orderbook）
logger = LoggingConfig.setup_logger(
    name="core.services.arbitrage_monitor_v2.data.data_processor",
    log_file="data_processor.log",
    console_formatter=None,
    level=logging.WARNING
)


class DataProcessor:
    """
    数据处理器 - 独立任务运行
    
    设计原则：
    1. 从队列批量消费数据
    2. 维护内存中的最新状态
    3. 不阻塞数据接收
    """
    
    def __init__(
        self,
        orderbook_queue: asyncio.Queue,
        ticker_queue: asyncio.Queue,
        debug_config: DebugConfig,
        scroller=None,  # 实时滚动区管理器（可选）
        latest_store: Optional[LatestStore] = None,
        latest_only_mode: bool = False,
        latest_poll_interval_ms: int = 50,
        latest_event: Optional[asyncio.Event] = None,
        time_budget_ms: int = 5,
    ):
        """
        初始化数据处理器
        
        Args:
            orderbook_queue: 订单簿队列
            ticker_queue: Ticker队列
            debug_config: Debug配置
            scroller: 实时滚动区管理器（用于实时打印）
        """
        self.orderbook_queue = orderbook_queue
        self.ticker_queue = ticker_queue
        self.debug = debug_config
        self.scroller = scroller  # 🔥 混合模式：实时滚动输出

        # latest-only 数据通道（方案A）
        self._latest_store = latest_store
        self._latest_only_mode = bool(latest_only_mode and latest_store is not None)
        self._latest_poll_interval = max(1, int(latest_poll_interval_ms)) / 1000.0
        self._latest_seen_ticker: Dict[Tuple[str, str], int] = {}
        self._latest_seen_orderbook: Dict[Tuple[str, str], int] = {}
        self._latest_event: Optional[asyncio.Event] = latest_event
        # 事件驱动分析：行情更新时触发（由 orchestrator 绑定）
        self._analysis_event: Optional[asyncio.Event] = None
        self._analysis_event_min_interval_ms: int = 0
        self._analysis_event_last_ts: float = 0.0
        self._analysis_event_triggered_count: int = 0
        # 仅处理“发生更新的键”，避免全量计算
        self._dirty_keys: Set[Tuple[str, str]] = set()
        self._time_budget_s: float = max(1, int(time_budget_ms)) / 1000.0
        # UI 性能面板用：记录“空转等待”的方式与估算延迟
        # - event: 事件驱动（无固定轮询延迟）
        # - poll : 轮询拉取（延迟=latest_poll_interval_ms）
        # - queue: 传统队列消费（延迟仅为协程让出CPU的sleep）
        self._analysis_latency_mode: str = "event" if self._latest_only_mode and self._latest_event else ("poll" if self._latest_only_mode else "queue")
        self._analysis_latency_ms: float = 0.0 if self._analysis_latency_mode == "event" else (float(latest_poll_interval_ms) if self._analysis_latency_mode == "poll" else 1.0)
        
        # 数据存储 {exchange: {symbol: data}}
        self.orderbooks: Dict[str, Dict[str, OrderBookData]] = defaultdict(dict)
        self.tickers: Dict[str, Dict[str, TickerData]] = defaultdict(dict)
        
        # 数据时间戳 {exchange: {symbol: datetime}}
        self.orderbook_timestamps: Dict[str, Dict[str, datetime]] = defaultdict(dict)
        self.orderbook_exchange_timestamps: Dict[str, Dict[str, datetime]] = defaultdict(dict)
        self.ticker_timestamps: Dict[str, Dict[str, datetime]] = defaultdict(dict)
        self._latency_log_times: Dict[str, Dict[str, datetime]] = defaultdict(dict)  # 最近一次延迟日志时间
        self._latency_log_interval = 60.0  # 默认每60秒打印一次成功样本
        self._stale_orderbook_log_times: Dict[str, Dict[str, float]] = defaultdict(dict)
        self._stale_orderbook_log_interval = 120.0  # 同一交易对的过期警告至少间隔120秒，减少刷屏
        self._stale_orderbook_suppress_count: Dict[str, Dict[str, int]] = defaultdict(dict)
        # 队列峰值监控
        self.orderbook_queue_peak: int = 0
        self.ticker_queue_peak: int = 0
        
        # 统计信息（滑动窗口：只统计过去1小时）
        # 🔥 使用时间戳列表记录每次处理的时间，实现滑动窗口统计
        self.orderbook_processed_timestamps: List[float] = []  # 订单簿处理时间戳列表
        self.ticker_processed_timestamps: List[float] = []      # Ticker处理时间戳列表
        
        # 启动时间（用于判断是否满1小时）
        self.start_time = time.time()
        
        # 其他统计信息
        self.stats = {
            'processing_errors': 0,
        }
        
        # 运行状态
        self.running = False
        self.orderbook_task: Optional[asyncio.Task] = None
        self.ticker_task: Optional[asyncio.Task] = None
    
    async def start(self):
        """启动数据处理任务"""
        if self.running:
            return
        
        self.running = True
        # 拆分为订单簿/行情两个协程，避免互相阻塞
        self.orderbook_task = asyncio.create_task(self._process_orderbook_loop())
        self.ticker_task = asyncio.create_task(self._process_ticker_loop())
        print("✅ 数据处理器已启动")
    
    async def stop(self):
        """停止数据处理任务"""
        self.running = False
        for task in (self.orderbook_task, self.ticker_task):
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        print("🛑 数据处理器已停止")

    def bind_analysis_event(
        self,
        event: asyncio.Event,
        *,
        min_interval_ms: int = 0,
    ) -> None:
        """绑定分析触发事件（由 orchestrator 调用）。"""
        self._analysis_event = event
        self._analysis_event_min_interval_ms = max(0, int(min_interval_ms))

    def _trigger_analysis_event(self) -> None:
        """触发分析事件（非阻塞，允许高频调用）。"""
        if not self._analysis_event:
            return
        now = time.perf_counter()
        min_gap = self._analysis_event_min_interval_ms / 1000.0
        if min_gap > 0 and (now - self._analysis_event_last_ts) < min_gap:
            return
        self._analysis_event_last_ts = now
        try:
            self._analysis_event.set()
            self._analysis_event_triggered_count += 1
        except Exception:
            return
    
    async def _process_orderbook_loop(self):
        """订单簿处理循环（独立协程，减少与Ticker互相阻塞）"""
        try:
            while self.running:
                if self._latest_only_mode and self._latest_store is not None:
                    loop_start = time.perf_counter()
                    processed = 0
                    updates = self._latest_store.collect_orderbook_updates(self._latest_seen_orderbook)
                    # 事件驱动：只有在“确实没有更新”时才 wait，避免 event 被清空后错过已有更新
                    if not updates:
                        if self._latest_event is not None:
                            self._analysis_latency_mode = "event"
                            self._analysis_latency_ms = 0.0
                            await self._latest_event.wait()
                            self._latest_event.clear()
                            continue
                        self._analysis_latency_mode = "poll"
                        self._analysis_latency_ms = self._latest_poll_interval * 1000.0
                        await asyncio.sleep(self._latest_poll_interval)
                        continue
                    for key, seq, item in updates:
                        if (time.perf_counter() - loop_start) >= self._time_budget_s:
                            break
                        try:
                            self._process_orderbook(item)
                            processed += 1
                            self._latest_seen_orderbook[key] = seq
                        except Exception:
                            self.stats['processing_errors'] += 1
                else:
                    processed = self._drain_queue(
                        self.orderbook_queue,
                        self._process_orderbook,
                        time_budget=self._time_budget_s,
                    )
                    if processed == 0:
                        self._analysis_latency_mode = "queue"
                        self._analysis_latency_ms = 1.0
                        await asyncio.sleep(0.001)
        except asyncio.CancelledError:
            if self.scroller and type(self.scroller).__name__ == 'SimplePrinter':
                print("🛑 订单簿处理循环已取消")
        except Exception as e:
            if self.scroller and type(self.scroller).__name__ == 'SimplePrinter':
                print(f"❌ 订单簿处理循环错误: {e}")

    async def _process_ticker_loop(self):
        """Ticker处理循环（独立协程，减少与订单簿互相阻塞）"""
        try:
            while self.running:
                if self._latest_only_mode and self._latest_store is not None:
                    loop_start = time.perf_counter()
                    processed = 0
                    updates = self._latest_store.collect_ticker_updates(self._latest_seen_ticker)
                    if not updates:
                        if self._latest_event is not None:
                            self._analysis_latency_mode = "event"
                            self._analysis_latency_ms = 0.0
                            await self._latest_event.wait()
                            self._latest_event.clear()
                            continue
                        self._analysis_latency_mode = "poll"
                        self._analysis_latency_ms = self._latest_poll_interval * 1000.0
                        await asyncio.sleep(self._latest_poll_interval)
                        continue
                    for key, seq, item in updates:
                        if (time.perf_counter() - loop_start) >= self._time_budget_s:
                            break
                        try:
                            self._process_ticker(item)
                            processed += 1
                            self._latest_seen_ticker[key] = seq
                        except Exception:
                            self.stats['processing_errors'] += 1
                else:
                    processed = self._drain_queue(
                        self.ticker_queue,
                        self._process_ticker,
                        time_budget=self._time_budget_s,
                    )
                    if processed == 0:
                        self._analysis_latency_mode = "queue"
                        self._analysis_latency_ms = 1.0
                        await asyncio.sleep(0.001)
        except asyncio.CancelledError:
            if self.scroller and type(self.scroller).__name__ == 'SimplePrinter':
                print("🛑 Ticker处理循环已取消")
        except Exception as e:
            if self.scroller and type(self.scroller).__name__ == 'SimplePrinter':
                print(f"❌ Ticker处理循环错误: {e}")

    def _drain_queue(self, q: asyncio.Queue, handler, time_budget: float) -> int:
        """在时间片内尽量清空队列，避免固定条数限制带来的延迟。"""
        loop_start = time.perf_counter()
        processed = 0
        # 队列接近满时（≥80%）丢弃最旧，优先保留最新，避免长时间积压
        if q.maxsize:
            high_water = max(int(q.maxsize * 0.8), q.maxsize - 2)
            while q.qsize() > high_water:
                try:
                    _ = q.get_nowait()
                except asyncio.QueueEmpty:
                    break

        while not q.empty():
            if (time.perf_counter() - loop_start) >= time_budget:
                break
            try:
                item = q.get_nowait()
            except asyncio.QueueEmpty:
                break
            try:
                handler(item)
            except Exception as e:
                self.stats['processing_errors'] += 1
                if self.scroller and type(self.scroller).__name__ == 'SimplePrinter':
                    print(f"⚠️ 处理数据错误: {e}")
            finally:
                try:
                    q.task_done()
                except Exception:
                    pass
            processed += 1
        return processed
    
    def _process_orderbook(self, item: Dict):
        """
        处理单个订单簿数据
        
        Args:
            item: 队列中的数据项
        """
        exchange = item['exchange']
        symbol = item['symbol']
        orderbook = item['data']
        exchange_timestamp = item.get('exchange_timestamp')
        raw_received = item.get('received_at') or item.get('timestamp') or datetime.now()
        if isinstance(raw_received, datetime):
            received_timestamp = raw_received
        elif isinstance(raw_received, (int, float)):
            received_timestamp = datetime.fromtimestamp(float(raw_received))
        else:
            received_timestamp = datetime.now()
        
        # 更新订单簿状态
        self.orderbooks[exchange][symbol] = orderbook
        self.orderbook_timestamps[exchange][symbol] = received_timestamp
        self._dirty_keys.add((exchange, symbol))
        if exchange_timestamp:
            self.orderbook_exchange_timestamps[exchange][symbol] = exchange_timestamp
            orderbook.exchange_timestamp = exchange_timestamp
        else:
            orderbook.exchange_timestamp = getattr(orderbook, 'exchange_timestamp', None) or getattr(orderbook, 'timestamp', None)
        orderbook.received_timestamp = received_timestamp
        processed_at = datetime.now()
        orderbook.processed_timestamp = processed_at
        
        # 🔥 记录处理时间戳（用于滑动窗口统计）
        current_time = time.time()
        self.orderbook_processed_timestamps.append(current_time)
        
        # 抽样打印延迟信息，便于确认时戳链路（默认每60秒一次，避免刷屏）
        if (
            self.debug.track_latency
            and exchange_timestamp
            and (processed_at - self._latency_log_times[exchange].get(symbol, datetime.fromtimestamp(0))).total_seconds() >= self._latency_log_interval
        ):
            try:
                exch_to_local = (received_timestamp - exchange_timestamp).total_seconds()
                local_to_process = (processed_at - received_timestamp).total_seconds()
                exchange_ts_str = exchange_timestamp.strftime("%H:%M:%S.%f")[:-3]
                received_ts_str = received_timestamp.strftime("%H:%M:%S.%f")[:-3]
                processed_ts_str = processed_at.strftime("%H:%M:%S.%f")[:-3]
                logger.debug(
                    f"🟢 [数据时延] {exchange} {symbol} | "
                    f"交易所时间={exchange_ts_str} | "
                    f"本地接收={received_ts_str} | "
                    f"处理={processed_ts_str} | "
                    f"交易所→本地: {exch_to_local:.3f}s | "
                    f"本地→处理: {local_to_process:.3f}s"
                )
            except Exception as latency_err:
                logger.debug(f"[数据时延] 计算失败: {latency_err}")
            finally:
                self._latency_log_times[exchange][symbol] = processed_at

        # 实时滚动输出
        if self.scroller:
            if orderbook.best_bid and orderbook.best_ask:
                try:
                    # 🔥 获取对应的 ticker 数据（用于资金费率）
                    ticker = self.tickers.get(exchange, {}).get(symbol)
                    funding_rate = None
                    if ticker and hasattr(ticker, 'funding_rate') and ticker.funding_rate is not None:
                        funding_rate = float(ticker.funding_rate)
                    
                    self.scroller.print_orderbook_update(
                        exchange=exchange,
                        symbol=symbol,
                        bid_price=float(orderbook.best_bid.price),
                        bid_size=float(orderbook.best_bid.size),
                        ask_price=float(orderbook.best_ask.price),
                        ask_size=float(orderbook.best_ask.size),
                        funding_rate=funding_rate  # 🔥 传递资金费率
                    )
                except Exception as e:
                    # 🔥 UI模式下不打印，避免界面闪动
                    if self.scroller and type(self.scroller).__name__ == 'SimplePrinter':
                        print(f"❌ [DataProcessor] SimplePrinter异常: {e}")
                        import traceback
                        traceback.print_exc()
            else:
                # 🔥 UI模式下不打印，避免界面闪动
                if self.scroller and type(self.scroller).__name__ == 'SimplePrinter':
                    print(f"⚠️ [DataProcessor] 订单簿数据不完整，跳过: bid={orderbook.best_bid}, ask={orderbook.best_ask}")
        # 行情更新后触发分析事件
        self._trigger_analysis_event()
    
    def _process_ticker(self, item: Dict):
        """
        处理单个Ticker数据
        
        Args:
            item: 队列中的数据项
        """
        exchange = item['exchange']
        symbol = item['symbol']
        ticker = item['data']
        timestamp = item['timestamp']

        # 🔥 关键修复：Ticker 更新合并策略
        # 资金费率等字段在部分交易所的 ticker 推送中并不总是携带。
        # 如果新 ticker 的 funding 字段为空，则保留旧 ticker 的最后一次有效值，
        # 避免 UI “闪一下就消失”。
        prev = self.tickers.get(exchange, {}).get(symbol)
        if prev is not None and ticker is not None:
            for field_name in (
                "funding_rate",
                "predicted_funding_rate",
                "funding_time",
                "next_funding_time",
                "funding_interval",
            ):
                try:
                    new_val = getattr(ticker, field_name, None)
                    old_val = getattr(prev, field_name, None)
                    if new_val is None and old_val is not None:
                        setattr(ticker, field_name, old_val)
                except Exception:
                    # 静默忽略合并失败，避免影响高频路径
                    pass
        # 更新Ticker状态
        self.tickers[exchange][symbol] = ticker
        self.ticker_timestamps[exchange][symbol] = timestamp
        self._dirty_keys.add((exchange, symbol))
        
        # 记录处理时间戳（用于滑动窗口统计）
        current_time = time.time()
        self.ticker_processed_timestamps.append(current_time)
        # ticker 更新不触发分析事件，避免无关唤醒影响速度

    def consume_dirty_keys(self) -> Set[Tuple[str, str]]:
        """获取并清空本轮更新键（exchange, symbol）。"""
        if not self._dirty_keys:
            return set()
        keys = set(self._dirty_keys)
        self._dirty_keys.clear()
        return keys
    
    def get_orderbook(self, exchange: str, symbol: str, max_age_seconds: float = 2.0) -> Optional[OrderBookData]:
        """
        获取订单簿数据（带时效性检查）
        
        Args:
            exchange: 交易所
            symbol: 交易对
            max_age_seconds: 最大数据年龄（秒），默认2秒
            
        Returns:
            订单簿数据，如果不存在或已过期则返回None
        """
        orderbook = self.orderbooks.get(exchange, {}).get(symbol)
        if not orderbook:
            return None
        
        # 🔥 时效性检查：需要同时满足“交易所时间戳”和“本地接收时间”两种约束
        # 注意：不同交易所的 orderbook.timestamp/exchange_timestamp 可能是：
        # - naive datetime（无 tzinfo，通常为本地时间）
        # - aware datetime（带 tzinfo，例如 UTC）
        # 这里统一转换为“本地 naive datetime”再做差，避免 TypeError:
        #   can't subtract offset-naive and offset-aware datetimes
        now = datetime.now()
        def _to_local_naive(dt: Optional[datetime]) -> Optional[datetime]:
            if not dt:
                return None
            if not isinstance(dt, datetime):
                return None
            if dt.tzinfo is None:
                return dt
            try:
                return dt.astimezone().replace(tzinfo=None)
            except Exception:
                # 兜底：直接去 tzinfo（可能不精确，但避免阻断主流程）
                return dt.replace(tzinfo=None)
        exchange_timestamp = (
            getattr(orderbook, 'exchange_timestamp', None)
            or getattr(orderbook, 'timestamp', None)
        )
        received_timestamp = (
            getattr(orderbook, 'received_timestamp', None)
            or self.orderbook_timestamps.get(exchange, {}).get(symbol)
        )
        
        # 优先验证交易所原始时间戳
        if exchange_timestamp:
            ex_ts = _to_local_naive(exchange_timestamp)
            if ex_ts:
                exchange_age = (now - ex_ts).total_seconds()
            else:
                exchange_age = 0.0
            if exchange_age > max_age_seconds:
                self._log_stale_orderbook(
                    exchange=exchange,
                    symbol=symbol,
                    reason="交易所时间戳过期",
                    age=exchange_age,
                    max_age=max_age_seconds,
                )
                return None
        
        # 其次验证本地接收时间
        if received_timestamp:
            if isinstance(received_timestamp, (int, float)):
                local_age = time.time() - float(received_timestamp)
            else:
                recv_ts = _to_local_naive(received_timestamp if isinstance(received_timestamp, datetime) else None)
                local_age = (now - recv_ts).total_seconds() if recv_ts else 0.0
            if local_age > max_age_seconds:
                self._log_stale_orderbook(
                    exchange=exchange,
                    symbol=symbol,
                    reason="订单簿接收时间过期",
                    age=local_age,
                    max_age=max_age_seconds,
                )
                return None
        else:
            self._log_stale_orderbook(
                exchange=exchange,
                symbol=symbol,
                reason="订单簿缺少接收时间",
                age=-1,
                max_age=max_age_seconds,
            )
            return None
        
        return orderbook

    def _log_stale_orderbook(
        self,
        *,
        exchange: str,
        symbol: str,
        reason: str,
        age: float,
        max_age: float,
    ) -> None:
        """
        控制“数据过期”日志的打印频率，避免持续刷屏。
        """
        now_ts = time.time()
        symbol_key = f"{symbol}:{reason}"
        last_log_ts = self._stale_orderbook_log_times[exchange].get(symbol_key, 0)
        suppress_bucket = self._stale_orderbook_suppress_count[exchange].get(symbol_key, 0)

        if now_ts - last_log_ts < self._stale_orderbook_log_interval:
            # 统计被抑制的次数，便于下次打印时汇报
            self._stale_orderbook_suppress_count[exchange][symbol_key] = suppress_bucket + 1
            return

        suppressed = self._stale_orderbook_suppress_count[exchange].pop(symbol_key, 0)
        self._stale_orderbook_log_times[exchange][symbol_key] = now_ts

        # 改为 DEBUG，避免高频刷屏占用 I/O
        if age >= 0:
            logger.debug(
                f"⚠️ [数据过期] {exchange} {symbol} {reason} "
                f"(年龄: {age:.2f}秒 > 阈值: {max_age:.2f}秒)，拒绝返回"
                + (f" | 抑制重复: {suppressed} 次" if suppressed else "")
            )
        else:
            logger.debug(
                f"❌ [时间戳缺失] {exchange} {symbol} {reason}，拒绝返回"
                + (f" | 抑制重复: {suppressed} 次" if suppressed else "")
            )
    
    def get_ticker(self, exchange: str, symbol: str) -> Optional[TickerData]:
        """
        获取Ticker数据
        
        Args:
            exchange: 交易所
            symbol: 交易对
            
        Returns:
            Ticker数据，如果不存在则返回None
        """
        return self.tickers.get(exchange, {}).get(symbol)
    
    def get_all_orderbooks(self) -> Dict[str, Dict[str, OrderBookData]]:
        """获取所有订单簿数据"""
        return dict(self.orderbooks)
    
    def get_all_tickers(self) -> Dict[str, Dict[str, TickerData]]:
        """获取所有Ticker数据"""
        return dict(self.tickers)
    
    def get_stats(self) -> Dict:
        """获取统计信息（滑动窗口：只统计过去1小时）"""
        current_time = time.time()
        one_hour_ago = current_time - 3600  # 1小时前的时间戳
        
        # 🔥 计算过去1小时的处理量
        # 如果启动时间不足1小时，则统计从启动到现在的所有数据
        cutoff_time = max(one_hour_ago, self.start_time)
        
        # 清理过期的时间戳（超过1小时的数据）
        self.orderbook_processed_timestamps = [
            ts for ts in self.orderbook_processed_timestamps if ts >= cutoff_time
        ]
        self.ticker_processed_timestamps = [
            ts for ts in self.ticker_processed_timestamps if ts >= cutoff_time
        ]
        
        # 统计过去1小时（或从启动到现在）的处理量
        orderbook_processed = len(self.orderbook_processed_timestamps)
        ticker_processed = len(self.ticker_processed_timestamps)
        
        # 队列峰值更新
        ob_qsize = self.orderbook_queue.qsize()
        tk_qsize = self.ticker_queue.qsize()
        if ob_qsize > self.orderbook_queue_peak:
            self.orderbook_queue_peak = ob_qsize
        if tk_qsize > self.ticker_queue_peak:
            self.ticker_queue_peak = tk_qsize
        
        return {
            **self.stats,
            'orderbook_processed': orderbook_processed,
            'ticker_processed': ticker_processed,
            'orderbook_queue_size': ob_qsize,
            'ticker_queue_size': tk_qsize,
            'orderbook_queue_peak': self.orderbook_queue_peak,
            'ticker_queue_peak': self.ticker_queue_peak,
            'orderbook_count': sum(len(obs) for obs in self.orderbooks.values()),
            'ticker_count': sum(len(tks) for tks in self.tickers.values()),
            # UI 性能面板：分析延迟（事件=0ms；轮询=interval；队列=1ms让出CPU）
            'analysis_latency_ms': float(self._analysis_latency_ms),
            'analysis_latency_mode': str(self._analysis_latency_mode),
            # 事件驱动触发次数（用于UI观测）
            'analysis_event_triggered_count': int(self._analysis_event_triggered_count),
        }
    
    def is_data_available(self, exchange: str, symbol: str) -> bool:
        """
        检查数据是否可用
        
        Args:
            exchange: 交易所
            symbol: 交易对
            
        Returns:
            数据是否可用
        """
        has_orderbook = symbol in self.orderbooks.get(exchange, {})
        has_ticker = symbol in self.tickers.get(exchange, {})
        return has_orderbook  # Ticker是可选的

