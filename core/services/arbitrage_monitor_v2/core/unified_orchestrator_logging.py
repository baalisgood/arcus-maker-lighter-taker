"""
统一调度器日志对象（被拆分后的多个 mixin 共享）。

注意：仅抽取 logger 定义，避免各模块重复 setup_logger。

职责：提供全局 `logger`（保持拆分前的日志行为与文件输出一致）。
"""

import logging

from core.adapters.exchanges.utils.setup_logging import LoggingConfig

# 调度层默认降到 WARNING，减少大行情下的日志格式化与写入开销
logger = LoggingConfig.setup_logger(
    # 保持与拆分前一致的 logger 名称，避免日志过滤/归类依赖 name 时出现行为差异
    name="core.services.arbitrage_monitor_v2.core.unified_orchestrator",
    log_file='unified_orchestrator.log',
    console_formatter=None,  # 🔥 不输出到终端
    file_formatter='detailed',
    level=logging.WARNING
)
# 🔥 额外确保不传播到父logger，防止终端抖动
logger.propagate = False
