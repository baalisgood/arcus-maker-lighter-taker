"""
套利监控配置管理模块

职责：
- 加载和管理监控配置（交易对、阈值等）
- 提供配置访问接口
- 支持配置验证
"""

from typing import List, Dict, Any, Optional
from pathlib import Path
import yaml
import os
from dataclasses import dataclass, field
from .multi_leg_pairs_config import MultiLegPairsConfigManager


@dataclass
class MonitorConfig:
    """套利监控配置"""
    
    # 交易所配置
    exchanges: List[str] = field(default_factory=list)

    # 多交易所套利配置路径（可选）
    # - 若为空：使用默认 config/arbitrage/multi_exchange_arbitrage.yaml
    # - 若填写：统一调度器将从该路径加载 many_to_many/one_to_many 组合
    multi_exchange_config_path: Optional[str] = None
    
    # 监控的交易对列表
    symbols: List[str] = field(default_factory=list)
    
    # 价差阈值配置
    min_spread_pct: float = 0.1  # 最小价差百分比
    
    # 资金费率配置
    min_funding_rate_diff: float = 0.01  # 最小资金费率差异
    
    # WebSocket配置
    ws_ping_interval: int = 30  # WebSocket心跳间隔（秒）
    ws_reconnect_delay: int = 5  # 重连延迟（秒）
    ws_max_reconnect_attempts: int = 5  # 最大重连次数
    
    # 数据队列配置
    orderbook_queue_size: int = 1000  # 订单簿队列大小
    ticker_queue_size: int = 500  # Ticker队列大小
    analysis_queue_size: int = 100  # 分析结果队列大小

    # 数据流模式（方案A：latest-only）
    # - 启用后，回调侧不再把每条消息入队，而是按 (exchange,symbol) 覆盖保存最新状态；
    # - 处理侧按固定频率拉取最新快照增量，避免队列积压与 QueueFull。
    latest_only_mode: bool = False
    latest_poll_interval_ms: int = 50  # 处理侧拉取 latest 的间隔（毫秒）
    # 事件驱动分析（行情更新后立即触发分析主循环）
    event_driven_analysis_enabled: bool = True
    event_driven_analysis_min_interval_ms: int = 0  # 触发最小间隔（毫秒）
    
    # 性能配置
    analysis_interval_ms: int = 10  # 分析间隔（毫秒）
    ui_refresh_interval_ms: int = 1000  # UI刷新间隔（毫秒）- 降低频率避免卡顿
    loop_interval_ms: int = 100  # 主循环间隔（毫秒）
    data_processor_time_budget_ms: int = 20  # DataProcessor 每轮处理预算（毫秒）
    # 🔥 UI 最小刷新间隔（秒）：用于保护终端渲染性能；想更“跟手”可调小（例如 0.1）
    ui_min_refresh_interval_seconds: float = 0.5
    ui_max_symbols: int = 30  # 终端实时订单簿表格最多展示的代币数量（超过会截断）
    
    # 健康检查配置
    health_check_interval: int = 10  # 健康检查间隔（秒）
    data_timeout_seconds: int = 30  # 数据超时时间（秒）

    # 价差计算对齐（方案A）：当出现“WS ↔ 轮询(伪BBO)”组合时，以轮询侧更新为锚点
    # - enabled=False：保持旧行为（各取最新快照，每轮都计算）
    # - enabled=True ：只有轮询侧出现新更新才计算一次价差，避免用旧轮询价反复对比 WS 新价
    poll_anchor_alignment_enabled: bool = False
    poll_anchor_alignment_skip_log_interval_seconds: float = 120.0  # “跳过本轮”日志节流
    poll_anchor_alignment_enable_log_interval_seconds: float = 3600.0  # “已启用”日志节流（长周期）
    
    # 日志配置
    log_dir: str = "logs"
    log_file: str = "arbitrage_monitor_v2.log"
    log_max_bytes: int = 10 * 1024 * 1024  # 10MB
    log_backup_count: int = 5
    
    # 历史记录配置
    spread_history_enabled: bool = False  # 历史记录功能开关（默认关闭）
    spread_history_data_dir: str = "data/spread_history"  # 数据存储目录
    spread_history_sample_interval_seconds: int = 60  # 采样间隔（秒），默认1分钟
    spread_history_sample_strategy: str = "max"  # 采样策略：max(最大值), mean(平均值), latest(最新值)
    spread_history_batch_size: int = 10  # 批量写入大小
    spread_history_batch_timeout: float = 60.0  # 批量写入超时（秒）
    spread_history_queue_maxsize: int = 500  # 写入队列最大大小
    # 压缩和归档配置
    spread_history_compress_after_days: int = 10  # 压缩天数（10天后压缩）
    spread_history_archive_after_days: int = 30  # 归档天数（30天后归档）
    spread_history_cleanup_interval_hours: int = 24  # 清理任务执行间隔（小时）
    
    # 调试配置
    debug_cli_mode: bool = False  # 是否启用CLI调试输出（关闭Rich UI）
    debug_cli_interval_seconds: float = 1.0  # CLI打印间隔


class ConfigManager:
    """配置管理器"""
    
    def __init__(self, config_path: Optional[Path] = None):
        """
        初始化配置管理器
        
        Args:
            config_path: 配置文件路径，如果为None则使用默认配置
        """
        self.config_path = config_path
        self.config = MonitorConfig()
        self.display_symbols: List[str] = []
        self.subscription_symbols: List[str] = []
        self.multi_leg_pairs_manager = MultiLegPairsConfigManager()
        
        if config_path and config_path.exists():
            self._load_from_file()
        else:
            # 配置文件不存在时，明确提示（避免误以为加载了 monitor_v2.yaml）
            if config_path and (not config_path.exists()):
                try:
                    cwd = os.getcwd()
                except Exception:
                    cwd = "UNKNOWN"
                print(
                    f"⚠️  监控配置文件不存在，将回退到默认配置（edgex+lighter / 47 symbols）："
                    f" path={config_path} cwd={cwd}"
                )
            self._load_defaults()
    
        # 缓存基础symbol列表
        self.display_symbols = list(self.config.symbols or [])
        self.config.symbols = self.display_symbols
        self.subscription_symbols = list(self.display_symbols)
        
        # 无论使用哪种来源，都尝试附加额外的symbol
        self._apply_extra_symbols()

    def _load_from_file(self):
        """从YAML文件加载配置"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)
            
            # 映射YAML配置到MonitorConfig
            if 'multi_exchange_config_path' in data:
                try:
                    p = data.get('multi_exchange_config_path')
                    p = str(p).strip() if p is not None else ""
                    self.config.multi_exchange_config_path = p or None
                except Exception:
                    self.config.multi_exchange_config_path = None

            if 'exchanges' in data:
                self.config.exchanges = data['exchanges']
            
            if 'symbols' in data:
                self.config.symbols = data['symbols']
            
            if 'thresholds' in data:
                thresholds = data['thresholds']
                self.config.min_spread_pct = thresholds.get('min_spread_pct', 0.1)
                self.config.min_funding_rate_diff = thresholds.get('min_funding_rate_diff', 0.01)
            
            if 'websocket' in data:
                ws = data['websocket']
                self.config.ws_ping_interval = ws.get('ping_interval', 30)
                self.config.ws_reconnect_delay = ws.get('reconnect_delay', 5)
                self.config.ws_max_reconnect_attempts = ws.get('max_reconnect_attempts', 5)
            
            if 'performance' in data:
                perf = data['performance']
                self.config.analysis_interval_ms = perf.get('analysis_interval_ms', 10)
                self.config.ui_refresh_interval_ms = perf.get('ui_refresh_interval_ms', 200)
                try:
                    self.config.loop_interval_ms = int(
                        perf.get('loop_interval_ms', self.config.loop_interval_ms)
                    )
                except Exception:
                    pass
                # 可选：允许把 UI 最小刷新间隔调小（默认 0.5 秒）
                try:
                    self.config.ui_min_refresh_interval_seconds = float(
                        perf.get(
                            'ui_min_refresh_interval_seconds',
                            self.config.ui_min_refresh_interval_seconds,
                        )
                    )
                except Exception:
                    # 配置解析失败不影响启动
                    pass
                self.config.ui_max_symbols = int(perf.get('ui_max_symbols', self.config.ui_max_symbols))
                try:
                    self.config.data_processor_time_budget_ms = int(
                        perf.get(
                            'data_processor_time_budget_ms',
                            self.config.data_processor_time_budget_ms,
                        )
                    )
                except Exception:
                    pass

            # 队列大小（可选）
            if 'queues' in data:
                queues = data['queues']
                self.config.orderbook_queue_size = queues.get('orderbook_queue_size', self.config.orderbook_queue_size)
                self.config.ticker_queue_size = queues.get('ticker_queue_size', self.config.ticker_queue_size)
                self.config.analysis_queue_size = queues.get('analysis_queue_size', self.config.analysis_queue_size)

            # 数据流模式（可选）
            if 'data_flow' in data:
                df = data['data_flow']
                self.config.latest_only_mode = bool(df.get('latest_only_mode', self.config.latest_only_mode))
                self.config.latest_poll_interval_ms = int(df.get('latest_poll_interval_ms', self.config.latest_poll_interval_ms))
                try:
                    self.config.event_driven_analysis_enabled = bool(
                        df.get(
                            'event_driven_analysis_enabled',
                            self.config.event_driven_analysis_enabled,
                        )
                    )
                except Exception:
                    pass
                try:
                    self.config.event_driven_analysis_min_interval_ms = int(
                        df.get(
                            'event_driven_analysis_min_interval_ms',
                            self.config.event_driven_analysis_min_interval_ms,
                        )
                    )
                except Exception:
                    pass
            
            if 'health_check' in data:
                health = data['health_check']
                self.config.health_check_interval = health.get('interval', 10)
                self.config.data_timeout_seconds = health.get('data_timeout', 30)

            # 价差计算对齐（可选）
            if 'spread_alignment' in data:
                sa = data['spread_alignment']
                try:
                    self.config.poll_anchor_alignment_enabled = bool(
                        sa.get(
                            'poll_anchor_alignment_enabled',
                            self.config.poll_anchor_alignment_enabled,
                        )
                    )
                except Exception:
                    pass
                try:
                    self.config.poll_anchor_alignment_skip_log_interval_seconds = float(
                        sa.get(
                            'poll_anchor_alignment_skip_log_interval_seconds',
                            self.config.poll_anchor_alignment_skip_log_interval_seconds,
                        )
                    )
                except Exception:
                    pass
                try:
                    self.config.poll_anchor_alignment_enable_log_interval_seconds = float(
                        sa.get(
                            'poll_anchor_alignment_enable_log_interval_seconds',
                            self.config.poll_anchor_alignment_enable_log_interval_seconds,
                        )
                    )
                except Exception:
                    pass
            
            # 历史记录配置
            if 'spread_history' in data:
                history = data['spread_history']
                self.config.spread_history_enabled = history.get('enabled', False)
                self.config.spread_history_data_dir = history.get('data_dir', 'data/spread_history')
                if 'sampling' in history:
                    sampling = history['sampling']
                    self.config.spread_history_sample_interval_seconds = sampling.get('interval_seconds', 60)
                    self.config.spread_history_sample_strategy = sampling.get('strategy', 'max')
                if 'write' in history:
                    write = history['write']
                    self.config.spread_history_batch_size = write.get('batch_size', 10)
                    self.config.spread_history_batch_timeout = write.get('batch_timeout', 60.0)
                    self.config.spread_history_queue_maxsize = write.get('queue_maxsize', 500)
                if 'storage' in history:
                    storage = history['storage']
                    self.config.spread_history_compress_after_days = storage.get('compress_after_days', 10)
                    self.config.spread_history_archive_after_days = storage.get('archive_after_days', 30)
                    self.config.spread_history_cleanup_interval_hours = storage.get('cleanup_interval_hours', 24)
            
            if 'debug_cli' in data:
                cli_cfg = data['debug_cli']
                self.config.debug_cli_mode = bool(cli_cfg.get('enabled', False))
                self.config.debug_cli_interval_seconds = float(
                    cli_cfg.get(
                        'interval_seconds',
                        self.config.debug_cli_interval_seconds
                    )
                )
            
            print(f"✅ 配置已从文件加载: {self.config_path}")
            
        except Exception as e:
            print(f"⚠️  加载配置文件失败: {e}，使用默认配置")
            self._load_defaults()
    
    def _load_defaults(self):
        """加载默认配置"""
        # 🚀 47个高交易量代币（交易量 >= 1M USD）
        # 从 edgex_lighter_markets.json 筛选出的符合条件的代币
        high_volume_symbols = [
            '1000PEPE-USDC-PERP',
            '1000SHIB-USDC-PERP',
            'AAVE-USDC-PERP',
            'ADA-USDC-PERP',
            'APT-USDC-PERP',
            'ARB-USDC-PERP',
            'ASTER-USDC-PERP',
            'AVAX-USDC-PERP',
            'AVNT-USDC-PERP',
            'BCH-USDC-PERP',
            'BNB-USDC-PERP',
            'BTC-USDC-PERP',
            'CRO-USDC-PERP',
            'DOGE-USDC-PERP',
            'DOT-USDC-PERP',
            'ENA-USDC-PERP',
            'ETH-USDC-PERP',
            'FIL-USDC-PERP',
            'HBAR-USDC-PERP',
            'HYPE-USDC-PERP',
            'ICP-USDC-PERP',
            'IP-USDC-PERP',
            'KAITO-USDC-PERP',
            'LINK-USDC-PERP',
            'LTC-USDC-PERP',
            'MNT-USDC-PERP',
            'NEAR-USDC-PERP',
            'ONDO-USDC-PERP',
            'OP-USDC-PERP',
            'PAXG-USDC-PERP',
            'PUMP-USDC-PERP',
            'RESOLV-USDC-PERP',
            'SEI-USDC-PERP',
            'SOL-USDC-PERP',
            'SUI-USDC-PERP',
            'TAO-USDC-PERP',
            'TON-USDC-PERP',
            'TRUMP-USDC-PERP',
            'TRX-USDC-PERP',
            'UNI-USDC-PERP',
            'VIRTUAL-USDC-PERP',
            'WLD-USDC-PERP',
            'XMR-USDC-PERP',
            'XRP-USDC-PERP',
            'ZEC-USDC-PERP',
            'ZK-USDC-PERP',
            'ZORA-USDC-PERP',
        ]
        
        self.config = MonitorConfig(
            exchanges=['edgex', 'lighter'],
            # 🚀 使用V1的标准化格式（参考 config/arbitrage/monitor.yaml）
            # 这种格式会被SimpleSymbolConverter自动转换成各交易所需要的格式
            # EdgeX: BTC-USDC-PERP -> BTCUSD
            # Lighter: BTC-USDC-PERP -> BTC
            symbols=high_volume_symbols,  # 🔥 订阅47个高交易量代币
        )
        print(f"✅ 使用默认配置（监控 {len(high_volume_symbols)} 个高交易量代币）")
    
    def get_config(self) -> MonitorConfig:
        """获取配置对象"""
        return self.config
    
    def validate(self) -> bool:
        """
        验证配置有效性
        
        Returns:
            配置是否有效
        """
        if not self.config.exchanges:
            print("❌ 配置错误: 交易所列表为空")
            return False
        
        # 🔥 允许 symbols 为空，如果有 extra_symbols 或 multi_leg_pairs
        # 这样可以支持"仅多腿套利"模式
        if not self.config.symbols:
            # 检查是否有额外的交易对来源
            has_extra_symbols = self._has_extra_symbols_or_multi_leg()
            if not has_extra_symbols:
                print("❌ 配置错误: 交易对列表为空（且无 extra_symbols 或 multi_leg_pairs）")
                return False
            else:
                print("💡 [配置管理] symbols 为空，将依赖 extra_symbols 和 multi_leg_pairs")
        
        # 允许通过环境变量或配置控制单所模式
        allow_single_exchange = bool(os.environ.get("SEGMENTED_ALLOW_SINGLE_EXCHANGE", "").strip())
        if not allow_single_exchange:
            runtime_flag_path = Path("config/arbitrage/segment_symbol_filters.yaml")
            if runtime_flag_path.exists():
                try:
                    data = yaml.safe_load(runtime_flag_path.read_text(encoding='utf-8'))
                    allow_single_exchange = bool(data.get("allow_single_exchange"))
                except Exception:
                    allow_single_exchange = False

        if len(self.config.exchanges) < 2 and not allow_single_exchange:
            print("❌ 配置错误: 至少需要2个交易所才能进行套利监控 (可在 segment_symbol_filters.yaml 设置 allow_single_exchange: true 以启用单所模式)")
            return False
        
        return True
    
    def _has_extra_symbols_or_multi_leg(self) -> bool:
        """检查是否有额外的交易对来源（extra_symbols 或 multi_leg_pairs）"""
        # 检查 extra_symbols.yaml
        extra_symbols = self._load_extra_symbols()
        if extra_symbols:
            return True
        
        # 检查 multi_leg_pairs.yaml
        multi_leg_symbols = self._load_multi_leg_symbols()
        if multi_leg_symbols:
            return True
        
        return False
    
    def to_dict(self) -> Dict[str, Any]:
        """将配置转换为字典"""
        return {
            'exchanges': self.config.exchanges,
            'multi_exchange_config_path': self.config.multi_exchange_config_path,
            'symbols': self.config.symbols,
            'thresholds': {
                'min_spread_pct': self.config.min_spread_pct,
                'min_funding_rate_diff': self.config.min_funding_rate_diff,
            },
            'websocket': {
                'ping_interval': self.config.ws_ping_interval,
                'reconnect_delay': self.config.ws_reconnect_delay,
                'max_reconnect_attempts': self.config.ws_max_reconnect_attempts,
            },
            'queues': {
                'orderbook_queue_size': self.config.orderbook_queue_size,
                'ticker_queue_size': self.config.ticker_queue_size,
                'analysis_queue_size': self.config.analysis_queue_size,
            },
            'performance': {
                'analysis_interval_ms': self.config.analysis_interval_ms,
                'ui_refresh_interval_ms': self.config.ui_refresh_interval_ms,
                'loop_interval_ms': self.config.loop_interval_ms,
            },
            'data_flow': {
                'latest_only_mode': self.config.latest_only_mode,
                'latest_poll_interval_ms': self.config.latest_poll_interval_ms,
                'event_driven_analysis_enabled': self.config.event_driven_analysis_enabled,
                'event_driven_analysis_min_interval_ms': self.config.event_driven_analysis_min_interval_ms,
            },
            'health_check': {
                'interval': self.config.health_check_interval,
                'data_timeout': self.config.data_timeout_seconds,
            },
            'spread_alignment': {
                'poll_anchor_alignment_enabled': self.config.poll_anchor_alignment_enabled,
                'poll_anchor_alignment_skip_log_interval_seconds': self.config.poll_anchor_alignment_skip_log_interval_seconds,
                'poll_anchor_alignment_enable_log_interval_seconds': self.config.poll_anchor_alignment_enable_log_interval_seconds,
            },
        }

    def _apply_extra_symbols(self) -> None:
        """加载额外的监控交易对（如RWA / 多腿套利依赖符号）"""
        extra_symbols = self._load_extra_symbols()
        pair_leg_symbols = self._load_multi_leg_symbols()

        combined = extra_symbols + pair_leg_symbols
        if not combined:
            return

        existing = set(self.subscription_symbols or [])
        added = 0
        for symbol in combined:
            normalized = symbol.strip()
            if not normalized:
                continue
            normalized = normalized.upper()
            if normalized in existing:
                continue
            self.subscription_symbols.append(normalized)
            existing.add(normalized)
            added += 1

        if added:
            print(f"🔧 已附加 {added} 个额外监控交易对 (extra_symbols + multi_leg_pairs)")

    def _load_extra_symbols(self) -> List[str]:
        """从 config/arbitrage/extra_symbols.yaml 读取额外symbol"""
        extra_path = Path("config/arbitrage/extra_symbols.yaml")
        if not extra_path.exists():
            return []

        try:
            with extra_path.open('r', encoding='utf-8') as f:
                data = yaml.safe_load(f)
        except Exception as exc:
            print(f"⚠️  加载额外symbol失败: {exc}")
            return []

        symbols: List[str] = []
        if isinstance(data, list):
            symbols.extend(data)
        elif isinstance(data, dict):
            # 支持 {extra_symbols: [...], by_exchange: {lighter: [...]}}
            base_symbols = data.get('extra_symbols')
            if isinstance(base_symbols, list):
                symbols.extend(base_symbols)

            by_exchange = data.get('by_exchange')
            if isinstance(by_exchange, dict):
                for exchange_symbols in by_exchange.values():
                    if isinstance(exchange_symbols, list):
                        symbols.extend(exchange_symbols)
        else:
            return []

        return [s for s in (symbols or []) if isinstance(s, str)]

    def _load_multi_leg_symbols(self) -> List[str]:
        """读取多腿套利所需的交易对符号"""
        try:
            pairs = self.multi_leg_pairs_manager.get_pairs()
        except Exception:
            return []

        symbols: List[str] = []
        for pair in pairs:
            symbols.extend(pair.get_leg_symbols())
        return symbols

    def get_subscription_symbols(self) -> List[str]:
        """获取需要订阅的全部交易对列表（包含基础 + 额外 + 多腿依赖）"""
        return list(self.subscription_symbols)

    def get_display_symbols(self) -> List[str]:
        """获取原始显示用基础交易对（不含额外/多腿）"""
        return list(self.display_symbols)

