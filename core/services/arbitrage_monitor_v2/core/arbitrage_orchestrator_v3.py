"""
套利系统总调度器（完整执行版本 | V3-BASE 模式）

V3-BASE 特点：
- 传统跨所、同名永续合约套利（多交易所/单标的）
- 采用单次下单的网格/决策流程，不做多笔拆分
- 集成风险控制、执行、资金费率等完整业务

职责：
- 集成所有模块（配置、历史数据、分析、决策、风险控制、执行）
- 协调模块间通信
- 管理系统生命周期
- 提供统一的对外接口

版本说明：
- V2 Orchestrator (`orchestrator.py`): 监控版本，只监控和显示套利机会，不执行交易
- V3 Orchestrator (`arbitrage_orchestrator_v3.py` / V3-BASE): 完整执行版本，包含决策、执行、风险控制等完整功能

架构：
- 配置管理模块：统一配置管理
- 历史数据计算模块：内存计算结果
- 数据分析模块：多交易所锁定和价差计算
- 套利决策模块：开仓/平仓条件判断
- 全局风险控制模块：仓位、余额、网络等风险控制
- 套利执行模块：订单执行和状态管理
"""

import asyncio
import logging
import time
from collections import deque
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Optional, Any, Set, Deque
from pathlib import Path
from datetime import datetime

from core.adapters.exchanges.factory import ExchangeFactory
from core.adapters.exchanges.interface import ExchangeInterface
from core.adapters.exchanges.models import OrderBookData
from core.utils.config_loader import ExchangeConfigLoader

# 配置模块
from ..config.unified_config_manager import UnifiedConfigManager
from ..config.debug_config import DebugConfig

# 历史数据模块
from ..history.history_calculator import HistoryDataCalculator
from ..history.spread_history_recorder import SpreadHistoryRecorder  # 🔥 新增：历史数据记录器

# 数据分析模块
from ..analysis.spread_calculator import SpreadCalculator, SpreadData
from ..analysis.exchange_locker import ExchangeLocker
from ..analysis.opportunity_finder import ArbitrageOpportunity  # 🔥 新增：套利机会类

# 套利决策模块
from ..decision.arbitrage_decision import ArbitrageDecisionEngine
from ..models import ClosePositionContext, FundingRateData, PositionInfo

# 风险控制模块
from ..risk_control.global_risk_controller import GlobalRiskController
from ..risk_control.network_state import register_network_handlers

# 执行模块
from ..execution.arbitrage_executor import ArbitrageExecutor, ExecutionRequest

# 数据接收和处理（复用现有模块）
from ..data.data_receiver import DataReceiver
from ..data.data_processor import DataProcessor

# UI显示（可选）
from ..display.ui_manager import UIManager
from ..display.realtime_scroller import RealtimeScroller

# 🔥 使用统一日志系统
from core.adapters.exchanges.utils.setup_logging import LoggingConfig

logger = LoggingConfig.setup_logger(
    name=__name__,
    log_file='arbitrage_orchestrator_v3.log',
    console_formatter=None,
    file_formatter='detailed',
    level=logging.INFO
)

trade_logger = LoggingConfig.setup_logger(
    name=f"{__name__}.trade_journal",
    log_file='trade_journal.log',
    console_formatter=None,
    file_formatter='detailed',
    level=logging.INFO
)

opportunity_logger = LoggingConfig.setup_logger(
    name=f"{__name__}.opportunity_state",
    log_file='opportunity_finder.log',
    console_formatter=None,
    file_formatter='detailed',
    level=logging.INFO
)


V3_BASE_MODE_LABEL = "V3-BASE"
"""
V3-BASE（V3基础模式）
- 传统跨所同币种套利主流程
- 启动日志或其他模块若需要识别运行模式，可引用此常量
"""


class ArbitrageOrchestratorV3:
    """
    套利系统总调度器（完整执行版本）
    
    与V2 Orchestrator的区别：
    - V2: 只监控和显示套利机会，不执行实际交易
    - V3: 完整的套利系统，包含决策、执行、风险控制等完整功能
    
    功能对比：
    - V2: 数据接收 → 价差计算 → 机会识别 → UI显示
    - V3: 数据接收 → 价差计算 → 决策判断 → 风险控制 → 订单执行 → 持仓管理
    """
    
    def __init__(
        self,
        unified_config_path: Optional[Path] = None,
        monitor_config_path: Optional[Path] = None,
        debug_config: Optional[DebugConfig] = None
    ):
        """
        初始化总调度器
        
        Args:
            unified_config_path: 统一配置文件路径（套利决策、执行、风险控制）
            monitor_config_path: 监控配置文件路径（交易对、交易所等）
            debug_config: Debug配置
        """
        # 加载统一配置
        self.unified_config_manager = UnifiedConfigManager(unified_config_path)
        self.unified_config = self.unified_config_manager.get_unified_config()
        
        # 加载监控配置（复用现有配置管理器）
        from ..config.monitor_config import ConfigManager
        self.monitor_config_manager = ConfigManager(monitor_config_path)
        self.monitor_config = self.monitor_config_manager.get_config()
        
        self.debug = debug_config or DebugConfig()
        
        # 验证配置
        if not self.unified_config_manager.validate():
            raise ValueError("统一配置验证失败")
        
        if not self.monitor_config_manager.validate():
            raise ValueError("监控配置验证失败")
        
        # 🔥 读取数据新鲜度配置
        self.data_freshness_seconds = self.unified_config.system_mode.data_freshness_seconds
        logger.info(f"📊 [总调度器] 数据新鲜度阈值: {self.data_freshness_seconds}秒")
        
        # 初始化交易所适配器
        self.exchange_adapters: Dict[str, ExchangeInterface] = {}
        self._init_exchange_adapters()
        
        # 🔥 初始化历史数据记录器（负责写入数据到数据库）
        try:
            self.history_recorder = SpreadHistoryRecorder(
                data_dir="data/spread_history",
                sample_interval_seconds=60,  # 每60秒采样一次
                sample_strategy="latest",    # 使用最新值
                db_retention_hours=48        # 保留48小时数据
            )
            logger.info("✅ [总调度器] 历史数据记录器已初始化")
        except Exception as e:
            logger.warning(f"⚠️  [总调度器] 历史数据记录器初始化失败: {e}，将使用None")
            logger.exception("历史数据记录器初始化异常详情:")
            self.history_recorder = None
        
        # 初始化历史数据计算器（负责从数据库读取和计算）
        try:
            # 🔥 从配置读取稳定性判断参数
            thresholds = self.unified_config.decision.thresholds
            self.history_calculator = HistoryDataCalculator(
                db_path="data/spread_history/spread_history.db",
                update_interval_minutes=1,
                max_history_hours=thresholds.history_hours,
                min_data_points=getattr(thresholds, 'min_data_points_for_stability', 10),
                min_runtime_minutes=3.0,
                # 稳定性判断参数（从配置读取）
                funding_rate_stability_threshold=thresholds.funding_rate_stability_threshold,
                funding_rate_min_threshold=thresholds.funding_rate_min_threshold,
                funding_rate_duration_ratio=thresholds.funding_rate_duration_ratio,
                funding_rate_extreme_ratio=thresholds.funding_rate_extreme_ratio,
                funding_rate_extreme_multiplier=thresholds.funding_rate_extreme_multiplier,
                spread_stability_threshold=thresholds.spread_stability_threshold,
                spread_extreme_ratio=thresholds.spread_extreme_ratio,
                spread_extreme_multiplier=thresholds.spread_extreme_multiplier
            )
            logger.info("✅ [总调度器] 历史数据计算器已初始化（已从配置加载稳定性判断参数）")
        except Exception as e:
            logger.warning(f"⚠️  [总调度器] 历史数据计算器初始化失败: {e}，将使用None")
            logger.exception("历史数据计算器初始化异常详情:")
            self.history_calculator = None
        
        # 初始化数据分析模块
        self.spread_calculator = SpreadCalculator(self.debug)
        self.exchange_locker = ExchangeLocker()
        
        # 持仓限制提醒频率控制
        self._position_limit_log_times: Dict[str, float] = {}
        self._position_limit_log_interval: float = 60.0  # 秒
        self._opportunity_status_log_times: Dict[str, float] = {}
        self._opportunity_status_log_interval: float = 60.0  # 秒
        
        # 初始化套利决策引擎
        # 如果历史数据计算器初始化失败，创建一个占位符
        if self.history_calculator is None:
            logger.warning("⚠️  [总调度器] 历史数据计算器未初始化，决策引擎将无法使用历史数据功能")
            # 创建一个简单的占位符（只提供接口，返回默认值）
            # 🔥 修复：不需要重新导入，直接创建占位符类
            class HistoryCalculatorPlaceholder:
                async def get_natural_spread(self, symbol, exchange1, exchange2):
                    """返回None表示没有天然价差数据"""
                    return None, None
                async def get_avg_positive_spread(self, symbol, exchange1, exchange2):
                    """旧方法，向后兼容"""
                    return 0.0
                async def is_funding_rate_stable(self, symbol, exchange1, exchange2):
                    return False
                async def is_spread_stable(self, symbol, exchange1, exchange2):
                    return False
            
            history_calculator_for_decision = HistoryCalculatorPlaceholder()
        else:
            history_calculator_for_decision = self.history_calculator
        
        self.decision_engine = ArbitrageDecisionEngine(
            decision_config=self.unified_config.decision,
            history_calculator=history_calculator_for_decision,
            exchange_order_modes=self.unified_config.execution.exchange_order_modes,
            exchange_fee_config=self.unified_config.execution.exchange_fee_config,
        )
        
        # 初始化全局风险控制器
        allowed_symbols = {symbol.upper() for symbol in self.monitor_config.symbols} if self.monitor_config.symbols else None
        self.risk_controller = GlobalRiskController(
            risk_config=self.unified_config.risk_control,
            exchange_adapters=self.exchange_adapters,
            symbol_quantity_config=self.unified_config.execution.quantity_config,
            allowed_symbols=allowed_symbols
        )
        
        self._open_position_lock = asyncio.Lock()
        self._startup_open_block_reason = None

        # 设置风险控制回调
        self.risk_controller.on_pause = self._on_risk_pause
        self.risk_controller.on_resume = self._on_risk_resume
        self.risk_controller.on_close_all_positions = self._on_close_all_positions
        register_network_handlers(
            self._handle_network_failure_event,
            self._handle_network_recovered_event,
        )
        
        # 初始化套利执行器
        self.executor = ArbitrageExecutor(
            execution_config=self.unified_config.execution,
            exchange_adapters=self.exchange_adapters,
            monitor_only=self.unified_config.system_mode.monitor_only,  # 🔥 传递监控模式配置
            is_segmented_mode=False  # 🔥 基础模式：保留轮次间隔控制
        )
        
        self.executor.before_open_check = self._check_open_execution_risk

        # 数据接收和处理（复用现有模块）
        self.orderbook_queue = asyncio.Queue(maxsize=self.monitor_config.orderbook_queue_size)
        self.ticker_queue = asyncio.Queue(maxsize=self.monitor_config.ticker_queue_size)
        
        # 🔥 混合模式：创建实时滚动区管理器（用于UI）
        self.scroller = RealtimeScroller(throttle_ms=500)  # 500ms 节流
        
        self.data_receiver = DataReceiver(
            self.orderbook_queue,
            self.ticker_queue,
            self.debug
        )
        
        self.data_processor = DataProcessor(
            self.orderbook_queue,
            self.ticker_queue,
            self.debug,
            scroller=self.scroller  # 🔥 传递滚动区管理器
        )
        
        # UI管理器（可选，用于显示系统状态）
        self.ui_manager = UIManager(
            self.debug,
            scroller=self.scroller  # 🔥 传递滚动区管理器
        )
        
        # UI更新节流
        self.last_ui_update_time: float = 0
        self.ui_update_interval: float = 1.0  # UI数据更新间隔（秒）
        
        # 运行状态
        self.running = False
        self.main_loop_task: Optional[asyncio.Task] = None
        self._manual_close_blocklist: Set[str] = set()
        self._manual_close_notified: Set[str] = set()
        self.ui_update_task: Optional[asyncio.Task] = None
        self.ui_data_update_task: Optional[asyncio.Task] = None
        
        # 执行记录（用于UI显示）：非阻塞队列 + 环形缓冲
        self.max_execution_records: int = 50  # 最多保留50条记录
        self._execution_records_store: Deque[Dict[str, Any]] = deque(maxlen=self.max_execution_records)
        self._execution_record_queue: asyncio.Queue = asyncio.Queue(maxsize=200)
        
        # 🔥 套利机会追踪（用于UI显示）
        self.current_opportunities: Dict[str, ArbitrageOpportunity] = {}  # {opportunity_key: ArbitrageOpportunity}
        
        # 🔥 日志限流：记录每个日志消息的最后打印时间
        self._log_throttle: Dict[str, datetime] = {}  # {log_key: last_log_time}
        self._log_throttle_interval: float = 60.0  # 相同日志60秒内只打印一次
        
        logger.info("✅ [总调度器] 套利系统总调度器 V3 初始化完成")
    
    def _init_exchange_adapters(self):
        """初始化交易所适配器（仅创建，不连接）"""
        logger.info(f"🔧 [总调度器] 开始初始化交易所适配器，配置的交易所: {self.monitor_config.exchanges}")
        factory = ExchangeFactory()
        config_loader = ExchangeConfigLoader()
        
        for exchange_name in self.monitor_config.exchanges:
            logger.info(f"🔧 [总调度器] 正在创建适配器: {exchange_name}")
            try:
                # 尝试加载交易所特定配置文件
                config_path = Path(f"config/exchanges/{exchange_name}_config.yaml")
                exchange_config = None
                
                if config_path.exists():
                    try:
                        import yaml
                        with open(config_path, 'r', encoding='utf-8') as f:
                            config_data = yaml.safe_load(f)
                        
                        if exchange_name in config_data:
                            config_data = config_data[exchange_name]
                        
                        from core.adapters.exchanges.interface import ExchangeConfig
                        from core.adapters.exchanges.models import ExchangeType
                        
                        type_map = {
                            'edgex': ExchangeType.SPOT,
                            'lighter': ExchangeType.SPOT,
                            'hyperliquid': ExchangeType.PERPETUAL,
                            'binance': ExchangeType.PERPETUAL,
                            'backpack': ExchangeType.SPOT,
                            'paradex': ExchangeType.PERPETUAL,
                            'grvt': ExchangeType.PERPETUAL,
                            'arcus': ExchangeType.PERPETUAL,
                        }
                        
                        # 🔥 读取API配置和认证配置
                        api_config = config_data.get('api', {})
                        authentication_config = config_data.get('authentication', {})
                        extra_params = dict(config_data.get('extra_params', {}))

                        # 🔐 统一密钥加载：环境变量 > YAML
                        auth = config_loader.load_auth_config(
                            exchange_name,
                            use_env=True,
                            config_file=str(config_path)
                        )
                        
                        # 🔥 兼容多种配置格式：环境变量优先，其次 YAML
                        api_key = auth.api_key or authentication_config.get('api_key') or config_data.get('api_key', '')
                        api_secret = (
                            auth.api_secret
                            or auth.private_key
                            or authentication_config.get('api_secret')
                            or config_data.get('api_secret', '')
                        )
                        private_key = auth.private_key or authentication_config.get('private_key')
                        wallet_address = (
                            auth.wallet_address
                            or config_data.get('wallet_address')
                            or authentication_config.get('wallet_address')
                        )
                        if wallet_address:
                            extra_params.setdefault('wallet_address', wallet_address)

                        if auth.jwt_token:
                            extra_params['jwt_token'] = auth.jwt_token
                        if auth.l2_address:
                            extra_params['l2_address'] = auth.l2_address
                        if auth.sub_account_id:
                            extra_params['sub_account_id'] = auth.sub_account_id
                        
                        if exchange_name == 'arcus':
                            private_key = auth.api_signing_key
                            extra_params['api_signing_key'] = auth.api_signing_key
                            extra_params['account_index'] = auth.account_index

                        exchange_config = ExchangeConfig(
                            exchange_id=exchange_name,
                            name=config_data.get('name', exchange_name),
                            exchange_type=type_map.get(exchange_name, ExchangeType.SPOT),
                            api_key=api_key,
                            api_secret=api_secret,
                            api_passphrase=config_data.get('api_passphrase') or auth.api_passphrase,
                            private_key=private_key,
                            wallet_address=wallet_address,
                            testnet=config_data.get('testnet', False),
                            base_url=api_config.get('base_url') or config_data.get('base_url'),
                            ws_url=api_config.get('ws_url'),
                            extra_params=extra_params
                        )
                        
                        # 🔥 为EdgeX添加authentication对象（与测试脚本保持一致）
                        if exchange_name == 'edgex' and authentication_config:
                            account_id = authentication_config.get('account_id')
                            stark_private_key = authentication_config.get('stark_private_key')
                            if account_id and stark_private_key:
                                exchange_config.authentication = type('Auth', (), {
                                    'account_id': str(account_id),
                                    'stark_private_key': stark_private_key
                                })()
                                # 添加私有WebSocket URL
                                exchange_config.private_ws_url = api_config.get('private_ws_url')
                                logger.info(f"✅ [{exchange_name}] 已加载认证信息: account_id={str(account_id)[:10]}...")
                        
                        # 🔥 为Backpack添加私有WebSocket URL
                        if exchange_name == 'backpack' and api_config.get('private_ws_url'):
                            exchange_config.private_ws_url = api_config.get('private_ws_url')
                            logger.info(f"✅ [{exchange_name}] 已配置私有WebSocket URL")
                    except Exception as e:
                        logger.warning(f"⚠️  [{exchange_name}] 配置文件解析失败: {e}，使用默认配置")
                        exchange_config = None
                
                # 创建适配器
                adapter = factory.create_adapter(
                    exchange_id=exchange_name,
                    config=exchange_config
                )
                
                if adapter:
                    self.exchange_adapters[exchange_name] = adapter
                    logger.info(f"✅ [总调度器] 交易所适配器已创建: {exchange_name}")
                else:
                    logger.warning(f"⚠️  [总调度器] 无法创建交易所适配器: {exchange_name}")
            except Exception as e:
                logger.error(f"❌ [总调度器] 创建交易所适配器失败 {exchange_name}: {e}", exc_info=True)
    
    async def _init_and_connect_adapters(self):
        """初始化并连接交易所适配器"""
        logger.info("🔌 [总调度器] 正在连接交易所...")
        
        async def connect_adapter(exchange_name: str, adapter: ExchangeInterface):
            """连接单个适配器"""
            try:
                logger.info(f"🔌 [{exchange_name}] 开始连接...")
                await adapter.connect()
                logger.info(f"✅ [{exchange_name}] 连接成功，注册到数据接收层...")
                self.data_receiver.register_adapter(exchange_name, adapter)
                logger.info(f"✅ [{exchange_name}] 已注册到数据接收层")
                return (exchange_name, adapter, None)
            except Exception as e:
                logger.error(f"❌ [{exchange_name}] 连接失败: {e}", exc_info=True)
                # 🔥 即使连接失败，也注册适配器（允许降级运行）
                logger.warning(f"⚠️  [{exchange_name}] 尝试降级注册（可能无法订阅数据）")
                try:
                    self.data_receiver.register_adapter(exchange_name, adapter)
                    logger.info(f"✅ [{exchange_name}] 已降级注册到数据接收层")
                except Exception as reg_error:
                    logger.error(f"❌ [{exchange_name}] 降级注册失败: {reg_error}")
                return (exchange_name, None, e)
        
        # 并行连接所有交易所
        results = await asyncio.gather(
            *[connect_adapter(name, adapter) for name, adapter in self.exchange_adapters.items()],
            return_exceptions=True
        )
        
        # 检查连接结果
        failed_exchanges = []
        for result in results:
            if isinstance(result, Exception):
                failed_exchanges.append(str(result))
            elif result[2] is not None:  # 有错误
                failed_exchanges.append(f"{result[0]}: {result[2]}")
        
        if failed_exchanges:
            logger.warning(f"⚠️  [总调度器] 部分交易所连接失败: {', '.join(failed_exchanges)}")
        
        # 订阅市场数据
        await self._subscribe_market_data()
    
    async def _subscribe_market_data(self):
        """订阅市场数据"""
        logger.info("📡 [总调度器] 正在订阅市场数据...")
        
        try:
            await self.data_receiver.subscribe_all(self.monitor_config.symbols)
            logger.info(f"✅ [总调度器] 已订阅 {len(self.monitor_config.symbols)} 个交易对")
        except Exception as e:
            logger.error(f"❌ [总调度器] 订阅市场数据失败: {e}", exc_info=True)
    
    async def start(self):
        """启动总调度器"""
        if self.running:
            logger.warning("[总调度器] 总调度器已在运行")
            return
        
        try:
            self.running = True
            
            # 🔥 启动历史数据记录器（必须先于计算器启动，确保有数据可读）
            if self.history_recorder:
                await self.history_recorder.start()
                logger.info("✅ [总调度器] 历史数据记录器已启动")
            else:
                logger.warning("⚠️  [总调度器] 历史数据记录器未初始化，跳过启动")
            
            # 启动历史数据计算器
            if self.history_calculator:
                await self.history_calculator.start()
                logger.info("✅ [总调度器] 历史数据计算器已启动")
            else:
                logger.warning("⚠️  [总调度器] 历史数据计算器未初始化，跳过启动")
            
            # 启动数据接收器（先启动，准备接收数据）
            # 注意：data_receiver.start() 可能不存在，检查一下
            if hasattr(self.data_receiver, 'start'):
                await self.data_receiver.start()
                logger.info("✅ [总调度器] 数据接收器已启动")
            
            # 启动数据处理器
            await self.data_processor.start()
            logger.info("✅ [总调度器] 数据处理器已启动")
            
            # 连接交易所并订阅数据
            await self._init_and_connect_adapters()
            logger.info("✅ [总调度器] 交易所连接和订阅完成")

            if not self.unified_config.system_mode.monitor_only:
                allowed, reason = await self.risk_controller.check_startup_account_state()
                self._startup_open_block_reason = None if allowed else reason
                if not allowed:
                    logger.error("[启动账户检查] %s", reason)

            await self.risk_controller.start()
            logger.info("✅ [总调度器] 全局风险控制器已启动")
            
            # 🔥 初始化WebSocket订单追踪（在WebSocket连接建立后）
            if not self.unified_config.system_mode.monitor_only:
                logger.info("📡 [总调度器] 初始化WebSocket订单追踪...")
                await self.executor.initialize_websocket_subscriptions()
                logger.info("✅ [总调度器] WebSocket订单追踪已初始化")
            else:
                logger.info("🔍 [总调度器] 监控模式，跳过WebSocket订单追踪")
            
            # 启动UI（可选）
            self.ui_manager.start(refresh_rate=5)
            # 🔥 设置为V3模式（执行系统），传递监控模式配置
            self.ui_manager.set_v3_mode(
                enabled=True,
                monitor_only=self.unified_config.system_mode.monitor_only
            )
            self.ui_manager.update_config({
                'exchanges': self.monitor_config.exchanges,
                'symbols': self.monitor_config.symbols
            })
            logger.info("✅ [总调度器] UI管理器已启动（V3模式）")
            
            # 启动UI更新循环（使用UIManager的update_loop方法）
            self.ui_update_task = asyncio.create_task(
                self.ui_manager.update_loop(self.monitor_config.ui_refresh_interval_ms)
            )
            logger.info("✅ [总调度器] UI更新任务已启动")
            
            # 启动自定义UI数据更新任务（更新持仓、风险状态等V3特有数据）
            self.ui_data_update_task = asyncio.create_task(self._ui_update_loop())
            logger.info("✅ [总调度器] UI数据更新任务已启动")
            
            # 启动主循环
            self.main_loop_task = asyncio.create_task(self._main_loop())
            logger.info("✅ [总调度器] 主循环已启动")
            
            logger.info("✅ [总调度器] 套利系统总调度器 V3 启动成功")
        
        except Exception as e:
            logger.error(f"❌ [总调度器] 启动失败: {e}", exc_info=True)
            self.running = False
            raise
    
    async def stop(self):
        """停止总调度器"""
        if not self.running:
            return
        
        self.running = False
        
        # 停止主循环
        if self.main_loop_task:
            self.main_loop_task.cancel()
            try:
                await self.main_loop_task
            except asyncio.CancelledError:
                pass
        
        # 停止UI更新任务
        if self.ui_update_task:
            self.ui_update_task.cancel()
            try:
                await self.ui_update_task
            except asyncio.CancelledError:
                pass
        
        if self.ui_data_update_task:
            self.ui_data_update_task.cancel()
            try:
                await self.ui_data_update_task
            except asyncio.CancelledError:
                pass
        
        # 停止UI
        self.ui_manager.stop()
        
        # 停止各个模块
        await self.data_processor.stop()
        if hasattr(self.data_receiver, 'stop'):
            await self.data_receiver.stop()
        await self.risk_controller.stop()
        
        # 🔥 停止历史数据模块（记录器先于计算器停止）
        if self.history_recorder:
            await self.history_recorder.stop()
        if self.history_calculator:
            await self.history_calculator.stop()
        
        logger.info("🛑 [总调度器] 套利系统总调度器 V3 已停止")
    
    async def _main_loop(self):
        """主循环：处理套利决策和执行"""
        while self.running:
            try:
                # 检查风险控制状态
                if self.risk_controller.is_paused():
                    pause_reason = self.risk_controller.get_pause_reason()
                    logger.debug(f"[总调度器] 系统暂停: {pause_reason}")
                    await asyncio.sleep(1)
                    continue
                
                # 检查每日交易次数限制
                allowed, reason = self.risk_controller.check_daily_trade_limit()
                if not allowed:
                    logger.debug(f"[总调度器] 每日交易次数限制: {reason}")
                    await asyncio.sleep(1)
                    continue
                
                # 获取最新数据（从数据处理器）
                # TODO: 实现数据获取逻辑
                
                # 处理每个交易对
                for symbol in self.monitor_config.symbols:
                    await self._process_symbol(symbol)
                
                # 控制循环频率
                await asyncio.sleep(0.1)  # 100ms循环一次
            
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[总调度器] 主循环错误: {e}", exc_info=True)
                await asyncio.sleep(1)
    
    async def _process_symbol(self, symbol: str):
        """
        处理单个交易对的套利逻辑
        
        Args:
            symbol: 交易对
        """
        try:
            # 检查交易对是否被禁用
            if self.risk_controller.is_symbol_disabled(symbol):
                return
            
            # 获取订单簿数据（从数据处理器）
            orderbooks = await self._get_orderbooks_for_symbol(symbol)
            if not orderbooks or len(orderbooks) < 2:
                return
            
            # 检查是否已有持仓
            if self.decision_engine.has_open_position(symbol):
                # 处理平仓逻辑
                await self._handle_close_position(symbol, orderbooks)
            else:
                # 处理开仓逻辑
                await self._handle_open_position(symbol, orderbooks)
        
        except Exception as e:
            logger.error(f"[总调度器] 处理交易对失败 {symbol}: {e}", exc_info=True)
    
    async def _record_all_spreads_to_history(
        self,
        symbol: str,
        orderbooks: Dict[str, OrderBookData]
    ):
        """
        🔥 记录所有方向的价差数据到历史存储
        
        核心原则：历史数据记录与决策逻辑完全分离
        - 记录所有交易所对的所有方向的原始价格数据
        - 不管决策模块的结果如何，都完整记录
        - 为历史数据分析提供全面、真实的数据基础
        """
        try:
            # 计算所有方向的价差
            all_spreads = self.spread_calculator.calculate_spreads(symbol, orderbooks)
            
            if not all_spreads:
                return
            
            # 🔥 记录每个方向的数据
            for spread in all_spreads:
                # 获取资金费率数据
                funding_rate_buy = None
                funding_rate_sell = None
                funding_rate_diff = None
                
                # 尝试获取买入交易所的资金费率
                ticker_buy = self.data_processor.get_ticker(spread.exchange_buy, symbol)
                if ticker_buy and hasattr(ticker_buy, 'funding_rate') and ticker_buy.funding_rate is not None:
                    funding_rate_buy = float(ticker_buy.funding_rate)
                
                # 尝试获取卖出交易所的资金费率
                ticker_sell = self.data_processor.get_ticker(spread.exchange_sell, symbol)
                if ticker_sell and hasattr(ticker_sell, 'funding_rate') and ticker_sell.funding_rate is not None:
                    funding_rate_sell = float(ticker_sell.funding_rate)
                
                # 计算资金费率差（绝对值差值）
                if funding_rate_buy is not None and funding_rate_sell is not None:
                    funding_rate_diff = abs(funding_rate_sell - funding_rate_buy)
                
                # 记录到历史存储
                await self.history_recorder.record_spread({
                    'symbol': symbol,
                    'exchange_buy': spread.exchange_buy,
                    'exchange_sell': spread.exchange_sell,
                    'price_buy': float(spread.price_buy),
                    'price_sell': float(spread.price_sell),
                    'spread_pct': spread.spread_pct,
                    'funding_rate_buy': funding_rate_buy,
                    'funding_rate_sell': funding_rate_sell,
                    'funding_rate_diff': funding_rate_diff,
                    'funding_rate_diff_annual': funding_rate_diff * 1095 * 100 if funding_rate_diff else None,
                    'size_buy': float(spread.size_buy),
                    'size_sell': float(spread.size_sell),
                })
                
        except Exception as e:
            logger.error(f"❌ [历史记录] 记录价差数据失败 {symbol}: {e}", exc_info=True)
    
    def _log_position_limit_warning(self, symbol: str, reason: Optional[str]):
        """持仓限制触发提示，INFO级别且限频"""
        now = time.time()
        last = self._position_limit_log_times.get(symbol, 0.0)
        if now - last < self._position_limit_log_interval:
            return
        self._position_limit_log_times[symbol] = now
        detail = reason or "已达到配置的持仓上限"
        logger.info(
            f"⚠️ [总调度器] {symbol}: 持仓限制触发（{detail}）。当前仅允许平仓，"
            "待仓位降低后才会重新开放新的套利开仓。"
        )

    def _log_opportunity_status(self, symbol: str, message: str) -> None:
        """按symbol限频输出套利状态日志"""
        now = time.time()
        last = self._opportunity_status_log_times.get(symbol, 0.0)
        if now - last < self._opportunity_status_log_interval:
            return
        self._opportunity_status_log_times[symbol] = now
        opportunity_logger.info(message)

    async def _check_open_execution_risk(self, request):
        if self._startup_open_block_reason:
            return False, self._startup_open_block_reason
        def fresh_prices():
            prices = {}
            for exchange in (request.exchange_buy, request.exchange_sell):
                book = self.data_processor.get_orderbook(exchange, request.symbol,
                            max_age_seconds=self.data_freshness_seconds)
                if not book or not book.best_bid or not book.best_ask:
                    raise ValueError("双腿行情不可用或过期")
                prices[exchange] = book.best_ask.price if exchange == request.exchange_buy else book.best_bid.price
            request.price_buy = prices[request.exchange_buy]
            request.price_sell = prices[request.exchange_sell]
            return prices
        return await self.risk_controller.check_position_limits(
            request.symbol, request.exchange_buy, request.quantity,
            exchange_sell=request.exchange_sell, prices=fresh_prices,
            local_positions=list(self.decision_engine.positions.values()),
            price_lookup=self._fresh_risk_price,
            slippage=self.unified_config.execution.order_execution.max_slippage,
        )

    def _fresh_risk_price(self, exchange, symbol):
        book = self.data_processor.get_orderbook(exchange, symbol,
                    max_age_seconds=self.data_freshness_seconds)
        if not book or not book.best_bid or not book.best_ask:
            return None
        return max(book.best_bid.price, book.best_ask.price)

    async def _handle_open_position(self, symbol, orderbooks):
        # A queued symbol rechecks exposure after the previous pair has been recorded.
        async with self._open_position_lock:
            if self._startup_open_block_reason:
                self._log_opportunity_status(symbol, self._startup_open_block_reason)
                return
            await self._handle_open_position_locked(symbol, orderbooks)

    async def _handle_open_position_locked(
        self,
        symbol: str,
        orderbooks: Dict[str, OrderBookData]
    ):
        """
        处理开仓逻辑
        
        Args:
            orderbooks: 只包含新鲜数据（2秒内）的交易所订单簿
                       - 例如：a、b、c三个交易所，如果c数据过期
                       - orderbooks只会包含{a: data_a, b: data_b}
                       - 价差计算只会在a-b之间进行，b-c和a-c自动排除
        """
        try:
            # 🔥 步骤0：记录所有方向的历史数据（与决策逻辑分离）
            # 历史数据记录应该是独立的，记录所有价格数据，不受决策逻辑影响
            if self.history_recorder:
                await self._record_all_spreads_to_history(symbol, orderbooks)
            
            # 步骤1：锁定交易所并计算最优价差（用于决策）
            spread_data = self.spread_calculator.calculate_spreads_multi_exchange(
                symbol, orderbooks
            )
            
            if not spread_data:
                self._log_opportunity_status(
                    symbol,
                    f"[套利状态] {symbol}: 无可用价差或价差未达条件，暂未触发套利"
                )
                return
            
            # 步骤2：获取资金费率数据
            funding_rate_data = await self._get_funding_rate_data(
                symbol,
                spread_data.exchange_buy,
                spread_data.exchange_sell
            )
            
            # 步骤2.5：根据配置读取本次计划的下单数量（代币本位）
            can_trade, order_quantity, quantity_error = self.executor.calculate_order_quantity(symbol)
            if not can_trade:
                logger.warning(f"⚠️ [总调度器] {symbol}: 计算下单数量失败: {quantity_error}")
                self._log_opportunity_status(
                    symbol,
                    f"[套利状态] {symbol}: 计算下单数量失败（{quantity_error}）"
                )
                return

            # 步骤2.6：限制相同代币同时存在多个套利组合
            existing_position = self.decision_engine.get_position(symbol)
            if existing_position and existing_position.is_open:
                logger.info(
                    f"⏸️ [总调度器] {symbol}: 已有开放套利 "
                    f"{existing_position.exchange_buy}->{existing_position.exchange_sell}，"
                    f"跳过新的 {spread_data.exchange_buy}->{spread_data.exchange_sell}"
                )
                self._log_opportunity_status(
                    symbol,
                    f"[套利状态] {symbol}: 已有开放套利 {existing_position.exchange_buy}->{existing_position.exchange_sell}，等待平仓"
                )
                return
            
            # 步骤3：检查仓位限制（使用计划下单数量）
            allowed, reason = await self.risk_controller.check_position_limits(
                symbol,
                spread_data.exchange_buy,
                order_quantity,
                exchange_sell=spread_data.exchange_sell,
                prices=lambda: {name: self._fresh_risk_price(name, symbol)
                                for name in (spread_data.exchange_buy, spread_data.exchange_sell)},
                local_positions=list(self.decision_engine.positions.values()),
                price_lookup=self._fresh_risk_price,
                slippage=self.unified_config.execution.order_execution.max_slippage,
            )
            
            if not allowed:
                self._log_position_limit_warning(symbol, reason)
                detail = reason or "触发仓位限制"
                self._log_opportunity_status(
                    symbol,
                    f"[套利状态] {symbol}: 风控拒绝（{detail}）"
                )
                return
            
            # 步骤4：决策引擎判断是否开仓
            should_open, mode, condition = await self.decision_engine.should_open_position(
                symbol, spread_data, funding_rate_data
            )
            
            if not should_open:
                detail = condition or "条件未满足"
                self._log_opportunity_status(
                    symbol,
                    f"[套利状态] {symbol}: 决策引擎未触发（模式={mode}, 条件={detail}）"
                )
                return

            self._log_trade_event(
                "OPEN_TRIGGER",
                symbol=symbol,
                buy_exchange=spread_data.exchange_buy,
                sell_exchange=spread_data.exchange_sell,
                target_qty=f"{order_quantity:.6f}",
                price_buy=f"{self._to_float(spread_data.price_buy):.4f}",
                price_sell=f"{self._to_float(spread_data.price_sell):.4f}",
                mode=mode,
                condition=condition
            )
            
            # 🔥 步骤4.5：只有决策引擎判定为真正的套利机会，才创建/更新套利机会对象
            opportunity_key = f"{symbol}_{spread_data.exchange_buy}_{spread_data.exchange_sell}"
            
            if opportunity_key in self.current_opportunities:
                # 更新现有机会
                opp = self.current_opportunities[opportunity_key]
                opp.price_buy = spread_data.price_buy
                opp.price_sell = spread_data.price_sell
                opp.size_buy = spread_data.size_buy
                opp.size_sell = spread_data.size_sell
                opp.spread_pct = spread_data.spread_pct
                if funding_rate_data:
                    opp.funding_rate_buy = funding_rate_data.funding_rate_buy
                    opp.funding_rate_sell = funding_rate_data.funding_rate_sell
                    opp.funding_rate_diff = funding_rate_data.funding_rate_diff_annual
                opp.trigger_mode = mode  # 🔥 更新触发模式
                opp.trigger_condition = condition  # 🔥 更新触发条件
                opp.update_duration()
            else:
                # 创建新机会
                opp = ArbitrageOpportunity(
                    symbol=symbol,
                    exchange_buy=spread_data.exchange_buy,
                    exchange_sell=spread_data.exchange_sell,
                    price_buy=spread_data.price_buy,
                    price_sell=spread_data.price_sell,
                    size_buy=spread_data.size_buy,
                    size_sell=spread_data.size_sell,
                    spread_pct=spread_data.spread_pct,
                    funding_rate_buy=funding_rate_data.funding_rate_buy if funding_rate_data else None,
                    funding_rate_sell=funding_rate_data.funding_rate_sell if funding_rate_data else None,
                    funding_rate_diff=funding_rate_data.funding_rate_diff_annual if funding_rate_data else None,
                    trigger_mode=mode,  # 🔥 触发模式
                    trigger_condition=condition  # 🔥 触发条件
                )
                self.current_opportunities[opportunity_key] = opp
                
                # 🔥 打印套利机会到滚动区（如果配置了滚动区）
                if self.scroller:
                    self.scroller.print_opportunity(
                        symbol=symbol,
                        exchange_buy=spread_data.exchange_buy,
                        exchange_sell=spread_data.exchange_sell,
                        price_buy=spread_data.price_buy,
                        price_sell=spread_data.price_sell,
                        spread_pct=spread_data.spread_pct,
                        funding_rate_diff=funding_rate_data.funding_rate_diff if funding_rate_data else None
                    )
            
            price_buy = float(spread_data.price_buy) if spread_data.price_buy else 0.0
            price_sell = float(spread_data.price_sell) if spread_data.price_sell else 0.0
            self._log_opportunity_status(
                symbol,
                (f"[套利状态] {symbol}: ✅ 满足条件，准备执行 "
                 f"{spread_data.exchange_buy}买→{spread_data.exchange_sell}卖 "
                 f"差价 +{spread_data.spread_pct:.4f}%")
            )
            logger.info(
                f"💰 [总调度器] {symbol}: 触发开仓 "
                f"模式={mode} 条件={condition} "
                f"价差={spread_data.spread_pct:.3f}% "
                f"{spread_data.exchange_buy}买@{price_buy:.2f} → "
                f"{spread_data.exchange_sell}卖@{price_sell:.2f}"
            )
            
            # 步骤5：执行开仓
            execution_request = ExecutionRequest(
                symbol=symbol,
                exchange_buy=spread_data.exchange_buy,
                exchange_sell=spread_data.exchange_sell,
                price_buy=spread_data.price_buy,
                price_sell=spread_data.price_sell,
                quantity=order_quantity,
                is_open=True,
            spread_data=spread_data,
            buy_symbol=spread_data.buy_symbol or symbol,
            sell_symbol=spread_data.sell_symbol or symbol
            )
            
            result = await self.executor.execute_arbitrage(execution_request)
            
            buy_snapshot = self._extract_order_snapshot(result.order_buy)
            sell_snapshot = self._extract_order_snapshot(result.order_sell)
            exec_prices = self._get_effective_prices(
                buy_snapshot,
                sell_snapshot,
                self._to_float(spread_data.price_buy),
                self._to_float(spread_data.price_sell),
                float(order_quantity) if order_quantity else 0.0
            )
            actual_spread = self._calculate_actual_spread_pct(
                exec_prices['price_buy'],
                exec_prices['price_sell']
            )
            
            # 记录执行记录（无论成功失败）
            execution_record = {
                'execution_time': datetime.now(),
                'symbol': symbol,
                'is_open': True,
                'exchange_buy': spread_data.exchange_buy,
                'exchange_sell': spread_data.exchange_sell,
                'success': result.success,
                'error_message': result.error_message or '',
                # 🔥 新增：交易关键数据
                'quantity': exec_prices['quantity'],
                'price_buy': exec_prices['price_buy'],
                'price_sell': exec_prices['price_sell'],
                'spread_pct': float(spread_data.spread_pct) if spread_data.spread_pct else 0.0,
                'actual_spread_pct': actual_spread
            }
            self._add_execution_record(execution_record)
            
            if result.success:
                self._log_trade_event(
                    "OPEN_EXECUTED",
                    symbol=symbol,
                    buy_exchange=spread_data.exchange_buy,
                    sell_exchange=spread_data.exchange_sell,
                    qty=f"{exec_prices['quantity']:.6f}",
                    price_buy=f"{exec_prices['price_buy']:.4f}",
                    price_sell=f"{exec_prices['price_sell']:.4f}",
                    actual_spread=f"{actual_spread:.4f}%"
                )
                
                # 记录开仓信息
                leg_quantity = self._to_decimal(order_quantity)
                executed_qty = self._to_decimal(exec_prices['quantity'])
                if executed_qty <= Decimal('0'):
                    executed_qty = leg_quantity
                
                await self.decision_engine.record_open_position(
                    symbol,
                    spread_data,
                    funding_rate_data,
                    mode,
                    condition,
                    leg_quantity
                )
                
                # 记录交易
                self.risk_controller.record_trade()
                
                logger.info(f"✅ [总调度器] {symbol}: 开仓成功")
            else:
                self._log_trade_event(
                    "OPEN_FAILED",
                    symbol=symbol,
                    buy_exchange=spread_data.exchange_buy,
                    sell_exchange=spread_data.exchange_sell,
                    error=result.error_message or "unknown"
                )
                logger.error(f"❌ [总调度器] {symbol}: 开仓失败: {result.error_message}")
                self._log_opportunity_status(
                    symbol,
                    f"[套利状态] {symbol}: 执行阶段失败（{result.error_message or '未知错误'}）"
                )
        
        except Exception as e:
            logger.error(f"[总调度器] 处理开仓失败 {symbol}: {e}", exc_info=True)
    
    async def _handle_close_position(
        self,
        symbol: str,
        orderbooks: Dict[str, OrderBookData]
    ):
        """处理平仓逻辑"""
        try:
            if symbol in self._manual_close_blocklist:
                if symbol not in self._manual_close_notified:
                    logger.warning(
                        f"⏸️ [总调度器] {symbol}: 上次平仓失败，已等待人工处理，当前跳过自动平仓"
                    )
                    self._manual_close_notified.add(symbol)
                return
            # 平仓只判断现有持仓，不能用全市场最优开仓方向替代。
            position = self.decision_engine.get_position(symbol)
            if not position:
                self._manual_close_blocklist.discard(symbol)
                self._manual_close_notified.discard(symbol)
                return
            held_orderbooks = {
                exchange: orderbooks[exchange]
                for exchange in (position.exchange_buy, position.exchange_sell)
                if orderbooks.get(exchange)
            }
            spread_data = next((
                spread for spread in self.spread_calculator.calculate_spreads(symbol, held_orderbooks)
                if spread.exchange_buy == position.exchange_buy
                and spread.exchange_sell == position.exchange_sell
            ), None)
            if not spread_data:
                return

            funding_rate_data = await self._get_funding_rate_data(
                symbol,
                position.exchange_buy,
                position.exchange_sell
            )
            close_context = self._build_close_position_context(position, orderbooks)
            
            # 步骤3：决策引擎判断是否平仓
            should_close, reason = self.decision_engine.should_close_position(
                symbol, spread_data, funding_rate_data, close_context
            )
            
            if not should_close:
                return
            
            price_buy = float(spread_data.price_buy) if spread_data.price_buy else 0.0
            price_sell = float(spread_data.price_sell) if spread_data.price_sell else 0.0
            logger.info(
                f"🛑 [总调度器] {symbol}: 触发平仓 "
                f"原因={reason} "
                f"当前价差={spread_data.spread_pct:.3f}% "
                f"{spread_data.exchange_buy}买@{price_buy:.2f} → "
                f"{spread_data.exchange_sell}卖@{price_sell:.2f}"
            )
            
            can_trade, close_quantity, quantity_error = self.executor.calculate_order_quantity(symbol)
            if not can_trade:
                logger.warning(f"⚠️ [总调度器] {symbol}: 计算平仓数量失败: {quantity_error}")
                return
            
            log_price_buy = (
                close_context.close_price_buy
                if close_context
                else self._to_float(spread_data.price_sell)
            )
            log_price_sell = (
                close_context.close_price_sell
                if close_context
                else self._to_float(spread_data.price_buy)
            )
            log_payload = {
                "symbol": symbol,
                "buy_exchange": position.exchange_sell,
                "sell_exchange": position.exchange_buy,
                "target_qty": f"{self._to_float(close_quantity):.6f}",
                "price_buy": f"{log_price_buy:.4f}",
                "price_sell": f"{log_price_sell:.4f}",
                "reason": reason,
            }
            if close_context:
                log_payload["actual_spread"] = f"{close_context.total_profit_pct:.4f}%"
            self._log_trade_event("CLOSE_TRIGGER", **log_payload)
            
            qty_tolerance = Decimal('1e-8')
            request_price_buy = (
                Decimal(str(close_context.close_price_buy))
                if close_context
                else spread_data.price_sell
            )
            request_price_sell = (
                Decimal(str(close_context.close_price_sell))
                if close_context
                else spread_data.price_buy
            )
            execution_request = ExecutionRequest(
                symbol=symbol,
                exchange_buy=position.exchange_sell,
                exchange_sell=position.exchange_buy,
                price_buy=request_price_buy,
                price_sell=request_price_sell,
                quantity=close_quantity,
                is_open=False,
            spread_data=spread_data,
            buy_symbol=spread_data.sell_symbol or symbol,
            sell_symbol=spread_data.buy_symbol or symbol
            )
            
            result = await self.executor.execute_arbitrage(execution_request)
            
            buy_snapshot = self._extract_order_snapshot(result.order_buy)
            sell_snapshot = self._extract_order_snapshot(result.order_sell)
            fallback_buy_price = (
                close_context.close_price_buy
                if close_context
                else self._to_float(spread_data.price_sell)
            )
            fallback_sell_price = (
                close_context.close_price_sell
                if close_context
                else self._to_float(spread_data.price_buy)
            )
            exec_prices = self._get_effective_prices(
                buy_snapshot,
                sell_snapshot,
                fallback_buy_price,
                fallback_sell_price,
                float(close_quantity)
            )
            actual_spread = self._calculate_actual_spread_pct(
                exec_prices['price_buy'],
                exec_prices['price_sell']
            )
            actual_qty = self._to_decimal(exec_prices['quantity'])
            
            execution_record = {
                'execution_time': datetime.now(),
                'symbol': symbol,
                'is_open': False,
                'exchange_buy': position.exchange_sell,
                'exchange_sell': position.exchange_buy,
                'success': result.success,
                'error_message': result.error_message or '',
                'close_reason': reason,
                'quantity': exec_prices['quantity'],
                'price_buy': exec_prices['price_buy'],
                'price_sell': exec_prices['price_sell'],
                'spread_pct': float(spread_data.spread_pct) if spread_data and spread_data.spread_pct else 0.0,
                'actual_spread_pct': actual_spread
            }
            self._add_execution_record(execution_record)
            
            if result.success and actual_qty > qty_tolerance:
                self._log_trade_event(
                    "CLOSE_EXECUTED",
                    symbol=symbol,
                    buy_exchange=position.exchange_sell,
                    sell_exchange=position.exchange_buy,
                    qty=f"{actual_qty:.6f}",
                    price_buy=f"{exec_prices['price_buy']:.4f}",
                    price_sell=f"{exec_prices['price_sell']:.4f}",
                    reason=reason,
                    actual_spread=f"{actual_spread:.4f}%"
                )
                
                position.quantity = max(Decimal('0'), position.quantity - actual_qty)
                position.quantity_buy = max(Decimal('0'), position.quantity_buy - actual_qty)
                position.quantity_sell = max(Decimal('0'), position.quantity_sell - actual_qty)
                
                if position.quantity <= qty_tolerance:
                    await self.decision_engine.record_close_position(symbol, reason)
                    self._manual_close_blocklist.discard(symbol)
                    self._manual_close_notified.discard(symbol)
                    logger.info(f"✅ [总调度器] {symbol}: 平仓完成")
                else:
                    self._manual_close_blocklist.add(symbol)
                    self._manual_close_notified.discard(symbol)
                    logger.error(
                        f"⏸️ [总调度器] {symbol}: 平仓数量 {actual_qty:.6f} 与配置不一致，剩余 {position.quantity:.6f}，"
                        "已暂停自动平仓等待人工确认"
                    )
            else:
                failure_reason = result.error_message or "unknown"
                self._log_trade_event(
                    "CLOSE_FAILED",
                    symbol=symbol,
                    buy_exchange=position.exchange_sell,
                    sell_exchange=position.exchange_buy,
                    error=failure_reason,
                    reason=reason
                )
                self._manual_close_blocklist.add(symbol)
                self._manual_close_notified.add(symbol)
                logger.error(
                    f"⏸️ [总调度器] {symbol}: 平仓失败（已暂停自动平仓，等待人工处理）: {failure_reason}"
                )
        
        except Exception as e:
            logger.error(f"[总调度器] 处理平仓失败 {symbol}: {e}", exc_info=True)
    
    def _build_close_position_context(
        self,
        position: PositionInfo,
        orderbooks: Dict[str, OrderBookData],
    ) -> Optional[ClosePositionContext]:
        """
        基于实时订单簿构建平仓收益上下文
        """
        try:
            long_orderbook = orderbooks.get(position.exchange_buy)
            short_orderbook = orderbooks.get(position.exchange_sell)
            if not long_orderbook or not short_orderbook:
                return None
            
            best_bid = long_orderbook.best_bid
            best_ask = short_orderbook.best_ask
            if not best_bid or not best_ask:
                return None
            
            close_price_sell = self._to_float(best_bid.price)
            close_price_buy = self._to_float(best_ask.price)
            open_price_buy = self._to_float(position.open_price_buy)
            open_price_sell = self._to_float(position.open_price_sell)
            
            price_values = [
                close_price_sell,
                close_price_buy,
                open_price_buy,
                open_price_sell,
            ]
            if any(price <= 0 for price in price_values):
                return None
            
            long_leg_return_pct = (
                (close_price_sell - open_price_buy) / open_price_buy * 100
            )
            short_leg_return_pct = (
                (open_price_sell - close_price_buy) / open_price_sell * 100
            )
            total_profit_pct = long_leg_return_pct + short_leg_return_pct
            close_spread_pct = (
                (close_price_sell - close_price_buy) / close_price_buy * 100
            )
            
            return ClosePositionContext(
                close_price_buy=close_price_buy,
                close_price_sell=close_price_sell,
                long_leg_return_pct=long_leg_return_pct,
                short_leg_return_pct=short_leg_return_pct,
                total_profit_pct=total_profit_pct,
                close_spread_pct=close_spread_pct,
            )
        except Exception as exc:
            logger.debug(
                f"[总调度器] 构建平仓上下文失败 {position.symbol}: {exc}",
                exc_info=True,
            )
            return None
    
    async def _get_orderbooks_for_symbol(
        self,
        symbol: str
    ) -> Dict[str, OrderBookData]:
        """
        获取交易对的订单簿数据（仅包含新鲜数据）
        
        Args:
            symbol: 交易对
        
        Returns:
            订单簿数据字典 {exchange: OrderBookData}
            - 只包含数据新鲜度在2秒内的交易所
            - 过期数据的交易所被自动排除
        """
        orderbooks = {}
        excluded_exchanges = []  # 记录被排除的交易所
        
        # 🔥 获取订单簿数据，自动过滤过期数据（使用配置的新鲜度阈值）
        for exchange_name in self.monitor_config.exchanges:
            orderbook = self.data_processor.get_orderbook(
                exchange_name, 
                symbol, 
                max_age_seconds=self.data_freshness_seconds  # 🔥 使用配置的新鲜度要求
            )
            if orderbook:
                orderbooks[exchange_name] = orderbook
            else:
                excluded_exchanges.append(exchange_name)
        
        # 🔥 记录数据过滤情况（带限流）
        if excluded_exchanges:
            # 使用日志限流，避免重复打印相同信息
            log_key = f"data_filter_{symbol}_{'_'.join(sorted(excluded_exchanges))}"
            if self._should_log(log_key):
                logger.warning(
                    f"⚠️ [数据过滤] {symbol} 排除了 {len(excluded_exchanges)} 个交易所的过期数据: "
                    f"{', '.join(excluded_exchanges)}"
                )
        
        if orderbooks:
            logger.debug(
                f"✅ [数据有效] {symbol} 有 {len(orderbooks)} 个交易所数据可用: "
                f"{', '.join(orderbooks.keys())}"
            )
        else:
            logger.warning(
                f"⚠️ [数据不足] {symbol} 所有交易所数据均不可用或已过期"
            )
        
        return orderbooks
    
    async def _get_funding_rate_data(
        self,
        symbol: str,
        exchange_buy: str,
        exchange_sell: str
    ) -> Optional[FundingRateData]:
        """
        获取资金费率数据
        
        Args:
            symbol: 交易对
            exchange_buy: 买入交易所
            exchange_sell: 卖出交易所
        
        Returns:
            资金费率数据
        """
        try:
            # 从数据处理器获取ticker数据
            ticker_buy = self.data_processor.get_ticker(exchange_buy, symbol)
            ticker_sell = self.data_processor.get_ticker(exchange_sell, symbol)
            
            if not ticker_buy or not ticker_sell:
                return None
            
            # 提取资金费率
            funding_rate_buy = getattr(ticker_buy, 'funding_rate', None)
            funding_rate_sell = getattr(ticker_sell, 'funding_rate', None)
            
            if funding_rate_buy is None or funding_rate_sell is None:
                return None
            
            # 🔥 计算资金费率差（数学规则：数值更大的 - 数值更小的，永远是正数或0）
            # 无论正负，取绝对值差值
            funding_rate_diff = abs(funding_rate_sell - funding_rate_buy) * 100
            
            # 计算年化资金费率差（假设8小时结算一次，一年365天，每天3次）
            funding_rate_diff_annual = funding_rate_diff * 365 * 3
            
            # 🔥 判断方向是否有利于持仓（关键！）
            # 有利条件：sell方费率 >= buy方费率（sell做空收取更高费率，buy做多支付更低费率）
            is_favorable = funding_rate_sell >= funding_rate_buy
            
            return FundingRateData(
                exchange_buy=exchange_buy,
                exchange_sell=exchange_sell,
                funding_rate_buy=funding_rate_buy,
                funding_rate_sell=funding_rate_sell,
                funding_rate_diff=funding_rate_diff,  # 🔥 永远是正数或0
                funding_rate_diff_annual=funding_rate_diff_annual,
                is_favorable_for_position=is_favorable  # 🔥 方向信息
            )
        
        except Exception as e:
            logger.error(f"[总调度器] 获取资金费率数据失败 {symbol}: {e}", exc_info=True)
            return None

    # ===========================
    # 网络故障/恢复事件
    # ===========================
    def _handle_network_failure_event(self, exchange: str, reason: str) -> None:
        message = f"{exchange}: {reason}"
        logger.warning("🚨 [总调度器] 检测到网络故障 -> %s", message)
        self.risk_controller.mark_network_failure(message)

    def _handle_network_recovered_event(self, exchange: str) -> None:
        logger.info("✅ [总调度器] %s 网络恢复", exchange)
        self.risk_controller.mark_network_recovered()
    
    def _on_risk_pause(self, reason: str):
        """风险控制暂停回调"""
        logger.warning(f"⚠️  [总调度器] 风险控制暂停: {reason}")
    
    def _on_risk_resume(self):
        """风险控制恢复回调"""
        logger.info("✅ [总调度器] 风险控制恢复，继续套利操作")
    
    def _on_close_all_positions(self):
        """平仓所有仓位回调"""
        logger.warning("🚨 [总调度器] 风险控制要求平仓所有仓位")
        # TODO: 实现平仓所有仓位的逻辑
    
    def _check_websocket_connected(self, adapter) -> bool:
        """
        通用方法：检查交易所适配器的WebSocket连接状态（自适应不同交易所）
        
        自动尝试多种常见的连接状态检查方式：
        1. 检查适配器自身的连接状态方法/属性
        2. 检查 _websocket 或 websocket 属性的连接状态
        3. 检查各种常见的连接状态属性名
        
        Args:
            adapter: 交易所适配器实例
            
        Returns:
            bool: WebSocket是否已连接
        """
        if not adapter:
            return False
        
        try:
            # === 方式1: 检查适配器自身的连接状态方法/属性 ===
            # 尝试调用 is_connected() 方法
            if hasattr(adapter, 'is_connected'):
                if callable(adapter.is_connected):
                    try:
                        if adapter.is_connected():
                            return True
                    except:
                        pass
                elif adapter.is_connected:
                    return True
            
            # 尝试调用 get_connection_status() 方法
            if hasattr(adapter, 'get_connection_status'):
                try:
                    status = adapter.get_connection_status()
                    if isinstance(status, dict) and status.get('connected', False):
                        return True
                except:
                    pass
            
            # === 方式2: 检查 WebSocket 对象的连接状态 ===
            ws = None
            
            # 尝试获取 WebSocket 对象（多种可能的属性名）
            for attr_name in ['_websocket', 'websocket', '_ws', 'ws']:
                if hasattr(adapter, attr_name):
                    ws = getattr(adapter, attr_name)
                    if ws:
                        break
            
            if ws:
                # 尝试调用 WebSocket 的连接检查方法
                if hasattr(ws, '_is_connection_usable'):
                    try:
                        if ws._is_connection_usable():
                            return True
                    except:
                        pass
                
                # 尝试检查 WebSocket 的连接状态属性（多种可能的属性名）
                for attr_name in ['_connected', '_ws_connected', 'connected', 'is_connected']:
                    if hasattr(ws, attr_name):
                        attr_value = getattr(ws, attr_name)
                        # 如果是方法，调用它
                        if callable(attr_value):
                            try:
                                if attr_value():
                                    return True
                            except:
                                pass
                        # 如果是属性，检查值
                        elif attr_value:
                            return True
                
                # 检查 WebSocket 对象是否有关闭状态（如果未关闭，可能已连接）
                if hasattr(ws, 'closed'):
                    if not ws.closed:
                        # 进一步检查是否有其他连接标志
                        if hasattr(ws, '_connected') or hasattr(ws, '_ws_connected'):
                            return True
            
            # === 方式3: 检查适配器的状态属性 ===
            # 检查适配器自身的连接状态属性
            for attr_name in ['_connected', '_ws_connected', 'connected']:
                if hasattr(adapter, attr_name):
                    attr_value = getattr(adapter, attr_name)
                    if callable(attr_value):
                        try:
                            if attr_value():
                                return True
                        except:
                            pass
                    elif attr_value:
                        return True
            
            return False
            
        except Exception as e:
            # 静默失败，返回False（避免影响其他交易所的检查）
            logger.debug(f"[持仓同步] 检查WebSocket连接状态时出错: {e}")
            return False
    
    def _get_open_block_reason(self):
        if self._startup_open_block_reason:
            return self._startup_open_block_reason
        if self.executor._unknown_execution_pairs or any(
                getattr(adapter, "_uncertain", None) for adapter in self.exchange_adapters.values()):
            return "订单状态不确定，请人工核对双腿；新开仓保持暂停"
        return None

    async def _ui_update_loop(self):
        """UI更新循环"""
        import time
        
        while self.running:
            try:
                current_time = time.time()
                
                # 节流检查：只在间隔时间到了才更新UI数据
                should_update_data = (current_time - self.last_ui_update_time) >= self.ui_update_interval
                
                # 收集统计信息
                stats = {
                    'exchanges': self.monitor_config.exchanges,
                    'symbols_count': len(self.monitor_config.symbols),
                    **self.data_receiver.get_stats(),
                    **self.data_processor.get_stats(),
                }
                
                # 🔥 持仓信息：优先使用WebSocket实时数据，并与本地持久化数据对比
                if should_update_data:
                    self._ui_positions = await self._get_merged_positions()
                open_positions = getattr(self, "_ui_positions", [])
                
                stats['open_positions'] = open_positions
                stats['open_positions_count'] = len(open_positions)
                
                # 更新UI持仓信息
                self.ui_manager.update_positions(open_positions)
                
                # 添加风险状态
                risk_status = self.risk_controller.get_risk_status()
                stats['risk_status'] = {
                    'is_paused': risk_status.is_paused,
                    'pause_reason': self._get_open_block_reason() or risk_status.pause_reason,
                    'account_data_unavailable': sorted(risk_status.account_data_unavailable),
                    'open_block_reason': self._get_open_block_reason(),
                    'network_failure': risk_status.network_failure,
                    'exchange_maintenance': list(risk_status.exchange_maintenance),
                    'low_balance_exchanges': list(risk_status.low_balance_exchanges),
                    'critical_balance_exchanges': list(risk_status.critical_balance_exchanges),
                }
                
                # 始终更新统计（轻量级）
                self.ui_manager.update_stats(stats)
                
                # 🎯 订单簿数据收集（重量级操作，只在需要时执行）
                if should_update_data:
                    orderbook_data = {}
                    ticker_data = {}
                    
                    # 使用订阅符号（包含基础+额外+多腿依赖），避免UI缺盘口
                    subscription_symbols = getattr(
                        self.monitor_config,
                        "subscription_symbols",
                        None
                    ) or getattr(self.monitor_config, "get_subscription_symbols", lambda: [])() or self.monitor_config.symbols

                    for exchange_name in self.monitor_config.exchanges:
                        orderbook_data[exchange_name] = {}
                        ticker_data[exchange_name] = {}
                        
                        for symbol in subscription_symbols:
                            ob = self.data_processor.get_orderbook(exchange_name, symbol, max_age_seconds=self.data_freshness_seconds)
                            if ob:
                                orderbook_data[exchange_name][symbol] = ob
                            
                            ticker = self.data_processor.get_ticker(exchange_name, symbol)
                            if ticker:
                                ticker_data[exchange_name][symbol] = ticker
                    
                    # 计算价差数据（用于UI显示）
                    symbol_spreads = {}
                    for symbol in subscription_symbols:
                        orderbooks = {}
                        for exchange_name in self.monitor_config.exchanges:
                            # 🔥 显式使用配置的数据新鲜度阈值，与决策引擎保持一致
                            ob = self.data_processor.get_orderbook(
                                exchange_name, 
                                symbol, 
                                max_age_seconds=self.data_freshness_seconds
                            )
                            if ob:
                                orderbooks[exchange_name] = ob
                        
                        if len(orderbooks) >= 2:
                            # 使用多交易所价差计算
                            spread_data = self.spread_calculator.calculate_spreads_multi_exchange(
                                symbol, orderbooks
                            )
                            if spread_data:
                                symbol_spreads[symbol] = [spread_data]
                            else:
                                symbol_spreads[symbol] = []
                        else:
                            symbol_spreads[symbol] = []
                    
                    # 更新订单簿数据（包含 Ticker 数据和价差数据）
                    self.ui_manager.update_orderbook_data(
                        orderbook_data,
                        ticker_data=ticker_data,
                        symbol_spreads=symbol_spreads
                    )
                    self.last_ui_update_time = current_time
                
                # 🔥 清理过期套利机会（使用与数据新鲜度一致的阈值）
                current_time_dt = datetime.now()
                expired_keys = []
                for key, opp in self.current_opportunities.items():
                    time_since_last_seen = (current_time_dt - opp.last_seen).total_seconds()
                    if time_since_last_seen > self.data_freshness_seconds:  # 与数据新鲜度阈值一致
                        expired_keys.append(key)
                
                for key in expired_keys:
                    del self.current_opportunities[key]
                
                # 🔥 更新套利机会到UI（将字典转为列表）
                opportunities_list = list(self.current_opportunities.values())
                self.ui_manager.update_opportunities(opportunities_list)
                
                # 更新执行记录（批量从队列取最新快照）
                self.ui_manager.update_execution_records(
                    self.get_execution_records_snapshot()
                )
                
                # 🔥 更新风险控制状态到UI
                risk_status = self.risk_controller.get_risk_status()
                today = datetime.now().strftime("%Y-%m-%d")
                risk_status_data = {
                    'is_paused': risk_status.is_paused,
                    'pause_reason': self._get_open_block_reason() or risk_status.pause_reason,
                    'account_data_unavailable': sorted(risk_status.account_data_unavailable),
                    'open_block_reason': self._get_open_block_reason(),
                    'network_failure': risk_status.network_failure,
                    'exchange_maintenance': risk_status.exchange_maintenance,
                    'low_balance_exchanges': risk_status.low_balance_exchanges,
                    'critical_balance_exchanges': risk_status.critical_balance_exchanges,
                    'daily_trade_count': self.risk_controller.daily_trade_count.get(today, 0),
                    'daily_trade_limit': self.risk_controller.config.daily_trade_limit.max_daily_trades if self.risk_controller.config.daily_trade_limit.enabled else 0
                }
                self.ui_manager.update_risk_status(risk_status_data)
                
                # 更新账户余额（定期更新，避免频繁查询）
                if should_update_data:
                    await self._update_account_balances_ui()
                
                # 控制更新频率
                await asyncio.sleep(0.2)  # 200ms更新一次UI
            
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[总调度器] UI更新错误: {e}", exc_info=True)
                await asyncio.sleep(1)
    
    def _should_log(self, log_key: str) -> bool:
        """
        判断是否应该打印日志（日志限流）
        
        Args:
            log_key: 日志唯一标识
            
        Returns:
            是否应该打印
        """
        now = datetime.now()
        last_log_time = self._log_throttle.get(log_key)
        
        if last_log_time is None:
            # 首次打印
            self._log_throttle[log_key] = now
            return True
        
        # 检查是否超过限流间隔
        elapsed = (now - last_log_time).total_seconds()
        if elapsed >= self._log_throttle_interval:
            self._log_throttle[log_key] = now
            return True
        
        return False

    @staticmethod
    def _to_float(value) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0
    
    @staticmethod
    def _to_decimal(value) -> Decimal:
        if isinstance(value, Decimal):
            return value
        try:
            return Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError):
            return Decimal('0')

    @classmethod
    def _extract_order_snapshot(cls, order) -> Dict[str, float]:
        if not order:
            return {'price': 0.0, 'quantity': 0.0}
        price = order.average or order.price
        quantity = order.filled or order.amount
        return {
            'price': cls._to_float(price),
            'quantity': cls._to_float(quantity)
        }

    def _log_trade_event(self, event: str, **payload) -> None:
        message = self._format_trade_log(event, payload)
        trade_logger.info(message)

    def _format_trade_log(self, event: str, payload: Dict[str, Any]) -> str:
        event_labels = {
            "OPEN_TRIGGER": "开仓触发",
            "OPEN_EXECUTED": "开仓完成",
            "OPEN_FAILED": "开仓失败",
            "CLOSE_TRIGGER": "平仓触发",
            "CLOSE_EXECUTED": "平仓完成",
            "CLOSE_FAILED": "平仓失败",
        }
        field_order = [
            "symbol",
            "buy_exchange",
            "sell_exchange",
            "price_buy",
            "price_sell",
            "target_qty",
            "qty",
            "actual_spread",
            "mode",
            "condition",
            "reason",
            "error",
        ]
        field_labels = {
            "symbol": "交易对",
            "buy_exchange": "买入交易所",
            "sell_exchange": "卖出交易所",
            "price_buy": "买入价格",
            "price_sell": "卖出价格",
            "target_qty": "计划数量",
            "qty": "成交数量",
            "actual_spread": "实际价差",
            "mode": "模式",
            "condition": "触发条件",
            "reason": "原因",
            "error": "错误信息",
        }
        segments = [f"事件：{event_labels.get(event, event)}"]

        def format_value(key: str, value: Any) -> str:
            if value is None or value == "":
                return ""
            if key in ("price_buy", "price_sell"):
                return f"{self._to_float(value):.2f}"
            if key in ("qty", "target_qty"):
                return f"{self._to_float(value):.6f}"
            if key == "actual_spread":
                if isinstance(value, str) and "%" in value:
                    return value
                return f"{self._to_float(value):+.4f}%"
            return str(value)

        added_keys = set()
        for key in field_order:
            if key in payload:
                formatted = format_value(key, payload[key])
                if formatted:
                    segments.append(f"{field_labels.get(key, key)}：{formatted}")
                added_keys.add(key)

        # Append any additional payload entries not covered above
        for key, value in payload.items():
            if key in added_keys:
                continue
            formatted = format_value(key, value)
            if formatted:
                segments.append(f"{key}：{formatted}")

        line = " │ ".join(segments)
        return f"【交易日志】{line}"

    def _calculate_actual_spread_pct(
        self,
        price_buy: float,
        price_sell: float
    ) -> float:
        if price_buy == 0.0:
            return 0.0
        return (price_sell - price_buy) / price_buy * 100
    
    def _get_effective_prices(
        self,
        buy_snapshot: Dict[str, float],
        sell_snapshot: Dict[str, float],
        fallback_buy: float,
        fallback_sell: float,
        fallback_qty: float
    ) -> Dict[str, float]:
        price_buy = buy_snapshot['price'] or fallback_buy
        price_sell = sell_snapshot['price'] or fallback_sell
        quantity = max(buy_snapshot['quantity'], sell_snapshot['quantity'])
        if quantity == 0.0:
            quantity = fallback_qty
        return {
            'price_buy': price_buy,
            'price_sell': price_sell,
            'quantity': quantity
        }
    
    async def _get_merged_positions(self) -> List[Dict]:
        """
        获取UI展示用的实时持仓（优先使用交易所API/WS数据，失败时回退到内存持仓）
        """
        qty_tolerance = 1e-6
        merged_positions: List[Dict] = []
        
        symbol_converter = None
        if hasattr(self, 'data_receiver') and hasattr(self.data_receiver, 'symbol_converter'):
            symbol_converter = self.data_receiver.symbol_converter
        
        try:
            ws_positions_by_symbol: Dict[str, Dict[str, Dict[str, float]]] = {}
            
            for exchange_name in self.monitor_config.exchanges:
                adapter = self.exchange_adapters.get(exchange_name)
                if not adapter:
                    continue
                
                try:
                    positions = await adapter.get_positions()
                except Exception as exc:
                    self.risk_controller.risk_status.account_data_unavailable.add(exchange_name)
                    logger.warning(f"⚠️ [持仓显示] 获取 {exchange_name} 持仓失败: {exc}")
                    continue
                
                if not positions:
                    continue
                
                for pos in positions:
                    raw_symbol = getattr(pos, 'symbol', None)
                    if not raw_symbol:
                        continue
                    
                    size = getattr(pos, 'size', 0.0) or 0.0
                    try:
                        size = abs(float(size))
                    except (TypeError, ValueError):
                        size = 0.0
                    
                    if size <= qty_tolerance:
                        continue
                    
                    standard_symbol = raw_symbol
                    if symbol_converter:
                        try:
                            standard_symbol = symbol_converter.convert_from_exchange(raw_symbol, exchange_name)
                        except Exception:
                            pass
                    
                    entry_price = getattr(pos, 'entry_price', 0.0) or 0.0
                    try:
                        entry_price = float(entry_price)
                    except (TypeError, ValueError):
                        entry_price = 0.0
                    
                    side_value = getattr(pos, 'side', 'unknown')
                    if hasattr(side_value, 'value'):
                        side_value = side_value.value
                    side_value = str(side_value or 'unknown').lower()
                    
                    ws_positions_by_symbol.setdefault(standard_symbol, {})[exchange_name] = {
                        'size': size,
                        'entry_price': entry_price,
                        'side': side_value,
                        'original_symbol': raw_symbol,
                    }
            
            for symbol, exchanges_data in ws_positions_by_symbol.items():
                valid_exchanges = {
                    ex: data for ex, data in exchanges_data.items()
                    if data.get('size', 0.0) > qty_tolerance
                }
                if not valid_exchanges:
                    continue
                
                exchange_list = list(valid_exchanges.keys())
                
                if len(exchange_list) >= 2:
                    long_ex = next(
                        (ex for ex, data in valid_exchanges.items()
                         if data.get('side') not in ('short', 'sell', 'short_position')),
                        exchange_list[0]
                    )
                    short_ex = next(
                        (ex for ex, data in valid_exchanges.items()
                         if data.get('side') in ('short', 'sell', 'short_position') and ex != long_ex),
                        next(ex for ex in exchange_list if ex != long_ex)
                    )
                    
                    buy_data = valid_exchanges.get(long_ex, {})
                    sell_data = valid_exchanges.get(short_ex, {})
                    
                    merged_positions.append({
                        'symbol': symbol,
                        'exchange_buy': long_ex,
                        'exchange_sell': short_ex,
                        'quantity_buy': buy_data.get('size', 0.0),
                        'quantity_sell': sell_data.get('size', 0.0),
                        'open_price_buy': buy_data.get('entry_price', 0.0),
                        'open_price_sell': sell_data.get('entry_price', 0.0),
                        'open_spread_pct': 0.0,
                        'open_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'open_mode': 'ws',
                        'source': 'ws',
                    })
                else:
                    exchange = exchange_list[0]
                    exchange_data = valid_exchanges[exchange]
                    side = exchange_data.get('side', 'unknown')
                    is_short = side in ('short', 'sell', 'short_position')
                    quantity = exchange_data.get('size', 0.0)
                    
                    merged_positions.append({
                        'symbol': symbol,
                        'exchange_buy': exchange if not is_short else '',
                        'exchange_sell': exchange if is_short else '',
                        'quantity_buy': quantity if not is_short else 0.0,
                        'quantity_sell': quantity if is_short else 0.0,
                        'open_price_buy': exchange_data.get('entry_price', 0.0) if not is_short else 0.0,
                        'open_price_sell': exchange_data.get('entry_price', 0.0) if is_short else 0.0,
                        'open_spread_pct': 0.0,
                        'open_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'open_mode': 'ws',
                        'source': 'ws',
                    })
            
            if merged_positions:
                return merged_positions
            
            return self._build_local_position_snapshot()
        
        except Exception as exc:
            logger.error(f"❌ [持仓显示] 获取持仓失败: {exc}", exc_info=True)
            return self._build_local_position_snapshot()
    
    def _build_local_position_snapshot(self) -> List[Dict]:
        """当无法获取实时持仓时，用内存记录兜底"""
        fallback_positions: List[Dict] = []
        for symbol, position in self.decision_engine.positions.items():
            if not position.is_open:
                continue
            fallback_positions.append({
                'symbol': symbol,
                'exchange_buy': position.exchange_buy,
                'exchange_sell': position.exchange_sell,
                'quantity_buy': float(position.quantity_buy),
                'quantity_sell': float(position.quantity_sell),
                'open_price_buy': float(position.open_price_buy),
                'open_price_sell': float(position.open_price_sell),
                'open_spread_pct': position.open_spread_pct,
                'open_time': position.open_time.strftime('%Y-%m-%d %H:%M:%S'),
                'open_mode': position.open_mode,
                'source': 'local',
            })
        return fallback_positions
    
    def _add_execution_record(self, record: Dict):
        """
        添加执行记录
        
        Args:
            record: 执行记录字典
        """
        # 环形缓冲存储最近N条
        self._execution_records_store.append(record)
        # 非阻塞入队，若满则弹出最旧再入队，避免执行路径等待
        try:
            self._execution_record_queue.put_nowait(record)
        except asyncio.QueueFull:
            try:
                _ = self._execution_record_queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
            try:
                self._execution_record_queue.put_nowait(record)
            except asyncio.QueueFull:
                # 如果仍然满，直接丢弃本条，保证执行线程不阻塞
                pass

    def get_execution_records_snapshot(self) -> List[Dict[str, Any]]:
        """
        UI 拉取执行记录的快照：
        - 清空队列（避免积压），环形缓冲已持久最新N条
        - 返回当前缓冲的浅拷贝列表
        """
        try:
            while True:
                self._execution_record_queue.get_nowait()
        except asyncio.QueueEmpty:
            pass
        records = list(self._execution_records_store)
        # 如果主缓冲为空，尝试从执行器内存摘要兜底（不读磁盘）
        if not records:
            try:
                from core.services.arbitrage_monitor_v2.execution.arbitrage_executor import (
                    get_recent_execution_summaries,
                )

                records = get_recent_execution_summaries(limit=10)
            except Exception:
                records = []
        return records
    
    async def _update_account_balances_ui(self):
        """更新账户余额UI数据"""
        try:
            account_balances = {}
            
            for exchange_name, adapter in self.exchange_adapters.items():
                try:
                    balances = await adapter.get_balances()
                    # 转换为字典格式，方便UI显示
                    balance_list = []
                    for balance in balances:
                        # 🔥 从 raw_data 中提取来源信息（ws/rest）
                        source = 'rest'  # 默认值
                        if balance.raw_data and isinstance(balance.raw_data, dict):
                            source = balance.raw_data.get('source', 'rest')
                        
                        balance_list.append({
                            'currency': balance.currency,
                            'free': float(balance.free) if balance.free else 0.0,
                            'used': float(balance.used) if balance.used else 0.0,
                            'total': float(balance.total) if balance.total else 0.0,
                            'source': source,  # 🔥 添加来源标记
                        })
                    account_balances[exchange_name] = balance_list
                except Exception as e:
                    logger.error(f"[总调度器] 获取{exchange_name}余额失败: {e}", exc_info=True)
                    account_balances[exchange_name] = []
            
            # 更新UI账户余额数据
            self.ui_manager.account_balances = account_balances
            
        except Exception as e:
            logger.error(f"[总调度器] 更新账户余额UI失败: {e}", exc_info=True)
