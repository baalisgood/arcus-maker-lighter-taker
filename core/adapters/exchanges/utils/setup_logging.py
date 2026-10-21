"""
日志系统配置工具

提供统一的日志配置接口，支持多种格式化器
"""

import logging
import os
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler
from typing import Optional, Dict
import threading

from .log_formatter import (
    CompactFormatter,
    DetailedFormatter,
    ColoredFormatter,
    format_order_log,
    format_ws_log,
    format_sync_log,
    simplify_order_id
)


class LoggingConfig:
    """日志配置管理器"""

    # 默认日志级别
    DEFAULT_LEVEL = logging.INFO

    # 日志文件配置
    LOG_DIR = Path("logs")
    MAX_BYTES = 10 * 1024 * 1024  # 10MB
    BACKUP_COUNT = 5

    # ------------------------------------------------------------------
    # 🔥 关键修复：同一个 log_file 复用同一个 RotatingFileHandler
    #
    # 背景：
    # - 项目里很多模块都会对同一个文件（如 logs/arbitrage_executor.log）
    #   调用 setup_logger()，每个 logger 会各自创建一个 RotatingFileHandler。
    # - 当发生轮转时（maxBytes 达到阈值），多个 handler 的 doRollover 不同步，
    #   会导致“部分模块继续写入已被重命名的旧文件(.log.1)，新 .log 看起来缺日志”。
    #
    # 解决：
    # - 以“文件绝对路径”为 key 缓存 handler，确保同一个文件只有一个 handler；
    # - 多个 logger 共享同一个 handler -> 轮转行为一致，写入位置一致。
    # ------------------------------------------------------------------
    _file_handler_lock = threading.Lock()
    _file_handler_cache: Dict[str, RotatingFileHandler] = {}

    # 环境变量控制：默认关闭控制台输出，需显式开启
    ENV_ENABLE_CONSOLE_KEY = "ENABLE_CONSOLE_LOGS"
    # 环境变量控制：可关闭文件输出以降低云端磁盘IO
    ENV_ENABLE_FILE_KEY = "ENABLE_FILE_LOGS"
    # 环境变量控制：是否启用 ExchangeAdapter 汇总日志（ExchangeAdapter.log）
    # - 默认关闭：避免与 ExchangeAdapter.<exchange>.log 重复，减少磁盘写入
    # - 如需恢复：export ENABLE_EXCHANGE_ADAPTER_AGGREGATE_LOGS=true
    ENV_ENABLE_EXCHANGE_ADAPTER_AGGREGATE_KEY = "ENABLE_EXCHANGE_ADAPTER_AGGREGATE_LOGS"

    # 格式化器类型
    FORMATTER_TYPES = {
        'compact': CompactFormatter,      # 简洁格式（终端）
        'detailed': DetailedFormatter,    # 详细格式（文件）
        'colored': ColoredFormatter,      # 彩色格式（终端）
    }

    @classmethod
    def setup_logger(cls,
                     name: str,
                     log_file: Optional[str] = None,
                     console_formatter: str = 'compact',
                     file_formatter: str = 'detailed',
                     level: int = None) -> logging.Logger:
        """
        设置logger

        Args:
            name: Logger名称
            log_file: 日志文件名（相对于logs目录）
            console_formatter: 控制台格式化器类型
            file_formatter: 文件格式化器类型
            level: 日志级别

        Returns:
            配置好的Logger对象
        """
        logger = logging.getLogger(name)
        logger.setLevel(level or cls.DEFAULT_LEVEL)
        logger.propagate = False  # 不传播到父logger

        # 清除现有handlers
        logger.handlers.clear()

        # 添加控制台handler
        if console_formatter and cls._is_console_enabled():
            console_handler = cls._create_console_handler(console_formatter)
            logger.addHandler(console_handler)

        # 添加文件handler
        if log_file and cls._is_file_enabled():
            # 🔕 关闭 ExchangeAdapter 汇总日志（可通过环境变量恢复）
            if log_file == "ExchangeAdapter.log" and not cls._is_exchange_adapter_aggregate_enabled():
                log_file = None

        if log_file and cls._is_file_enabled():
            file_handler = cls._create_file_handler(log_file, file_formatter)
            logger.addHandler(file_handler)

        return logger

    @classmethod
    def _is_console_enabled(cls) -> bool:
        """
        根据环境变量判断是否启用控制台日志。

        默认关闭（减少终端抖动），仅当 ENABLE_CONSOLE_LOGS
        被设置为 true/1/yes/on 时才启用。
        """
        flag = os.getenv(cls.ENV_ENABLE_CONSOLE_KEY)
        if flag is None:
            return False
        normalized = flag.strip().lower()
        return normalized in {"1", "true", "yes", "on"}

    @classmethod
    def _is_file_enabled(cls) -> bool:
        """
        根据环境变量判断是否启用文件日志。

        默认开启，若在云端磁盘IO敏感环境下，可设置 ENABLE_FILE_LOGS=false
        以关闭文件写入，仅保留（可选的）控制台输出。
        """
        flag = os.getenv(cls.ENV_ENABLE_FILE_KEY)
        if flag is None:
            return True
        normalized = flag.strip().lower()
        return normalized in {"1", "true", "yes", "on"}

    @classmethod
    def _is_exchange_adapter_aggregate_enabled(cls) -> bool:
        """
        是否启用 ExchangeAdapter 汇总日志（logs/ExchangeAdapter.log）。

        默认关闭，避免与各交易所独立日志（logs/ExchangeAdapter.<exchange>.log）重复。
        """
        flag = os.getenv(cls.ENV_ENABLE_EXCHANGE_ADAPTER_AGGREGATE_KEY)
        if flag is None:
            return False
        normalized = flag.strip().lower()
        return normalized in {"1", "true", "yes", "on"}

    @classmethod
    def _create_console_handler(cls, formatter_type: str) -> logging.Handler:
        """创建控制台handler"""
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(cls.DEFAULT_LEVEL)

        # 获取格式化器
        formatter_class = cls.FORMATTER_TYPES.get(
            formatter_type, CompactFormatter)
        handler.setFormatter(formatter_class())

        return handler

    @classmethod
    def _create_file_handler(cls, log_file: str, formatter_type: str) -> logging.Handler:
        """创建文件handler"""
        # 确保日志目录存在
        cls.LOG_DIR.mkdir(parents=True, exist_ok=True)

        file_path = (cls.LOG_DIR / log_file)
        cache_key = str(file_path.resolve())

        with cls._file_handler_lock:
            cached = cls._file_handler_cache.get(cache_key)
            if cached is not None:
                # 若 handler 曾被关闭（极少见），则重建
                try:
                    stream = getattr(cached, "stream", None)
                except Exception:
                    stream = None
                if stream is not None:
                    return cached

            handler = RotatingFileHandler(
                file_path,
                maxBytes=cls.MAX_BYTES,
                backupCount=cls.BACKUP_COUNT,
                encoding='utf-8'
            )
            # handler 不做二次过滤：由各 logger 的 level 控制
            handler.setLevel(logging.NOTSET)

            # 文件 formatter 统一（同一 log_file 多模块共享一个 handler，formatter 必须一致）
            formatter_class = cls.FORMATTER_TYPES.get(
                formatter_type, DetailedFormatter)
            handler.setFormatter(formatter_class())

            cls._file_handler_cache[cache_key] = handler
            return handler

    @classmethod
    def setup_exchange_adapter_logging(cls,
                                       use_colored: bool = True,
                                       console_level: int = logging.INFO,
                                       file_level: int = logging.DEBUG):
        """
        为交易所适配器设置优化的日志配置

        Args:
            use_colored: 是否使用彩色格式（终端支持ANSI颜色时）
            console_level: 控制台日志级别
            file_level: 文件日志级别
        """
        # 确定格式化器类型
        console_formatter = 'colored' if use_colored and cls._supports_color() else 'compact'

        # WebSocket模块
        ws_logger = cls.setup_logger(
            'core.adapters.exchanges.adapters.lighter_websocket',
            log_file='ExchangeAdapter.log',
            console_formatter=console_formatter,
            file_formatter='detailed',
            level=console_level
        )

        # REST API模块
        rest_logger = cls.setup_logger(
            'core.adapters.exchanges.adapters.lighter_rest',
            log_file='ExchangeAdapter.log',
            console_formatter=console_formatter,
            file_formatter='detailed',
            level=console_level
        )

        # 价格日志（只输出到文件，不输出到控制台；降到WARNING减少高频刷屏）
        price_logger = cls.setup_logger(
            'core.adapters.exchanges.adapters.lighter_websocket.price',
            log_file='ExchangeAdapter.log',
            console_formatter=None,  # 不输出到控制台
            file_formatter='detailed',
            level=logging.WARNING
        )

        return ws_logger, rest_logger, price_logger

    @classmethod
    def setup_grid_engine_logging(cls,
                                  use_colored: bool = True,
                                  console_level: int = logging.INFO):
        """
        为网格引擎设置优化的日志配置

        Args:
            use_colored: 是否使用彩色格式
            console_level: 控制台日志级别
        """
        console_formatter = 'colored' if use_colored and cls._supports_color() else 'compact'

        # 网格引擎
        engine_logger = cls.setup_logger(
            'core.services.grid.implementations.grid_engine_impl',
            log_file='core.services.grid.implementations.grid_engine_impl.log',
            console_formatter=console_formatter,
            file_formatter='detailed',
            level=console_level
        )

        # 网格协调器（降低为WARNING且默认不打控制台，减少重复调度信息）
        coord_logger = cls.setup_logger(
            'core.services.grid.coordinator.grid_coordinator',
            log_file='core.services.grid.coordinator.grid_coordinator.log',
            console_formatter=None,
            file_formatter='detailed',
            level=logging.WARNING
        )

        # 健康检查（降低为WARNING且默认不打控制台，避免高频心跳刷屏）
        health_logger = cls.setup_logger(
            'core.services.grid.implementations.order_health_checker',
            log_file='core.services.grid.implementations.order_health_checker.log',
            console_formatter=None,
            file_formatter='detailed',
            level=logging.WARNING
        )

        return engine_logger, coord_logger, health_logger

    @staticmethod
    def _supports_color() -> bool:
        """检测终端是否支持ANSI颜色"""
        # 检测是否在支持颜色的终端中
        if not hasattr(sys.stdout, 'isatty'):
            return False

        if not sys.stdout.isatty():
            return False

        # Windows终端需要额外检测
        import platform
        if platform.system() == 'Windows':
            # Windows 10及以上支持ANSI颜色
            try:
                import ctypes
                kernel32 = ctypes.windll.kernel32
                kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)
                return True
            except:
                return False

        return True


# 便捷函数
def setup_optimized_logging(use_colored: bool = True):
    """
    一键设置所有优化的日志配置

    Args:
        use_colored: 是否使用彩色输出
    """
    # 设置交易所适配器日志
    LoggingConfig.setup_exchange_adapter_logging(use_colored=use_colored)

    # 设置网格引擎日志
    LoggingConfig.setup_grid_engine_logging(use_colored=use_colored)

    logging.info("✅ 优化的日志系统已配置")


# 导出工具函数
__all__ = [
    'LoggingConfig',
    'setup_optimized_logging',
    'format_order_log',
    'format_ws_log',
    'format_sync_log',
    'simplify_order_id',
]
