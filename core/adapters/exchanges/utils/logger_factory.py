"""交易所适配器日志工厂

提供一个统一的入口，确保所有交易所适配器都能把日志写入
`logs/ExchangeAdapter.<exchange>.log`，并在可用时复用核心日志系统。
"""

from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

try:
    from core.logging import get_logger as core_get_logger
except Exception:  # pragma: no cover - 初始化阶段容错
    core_get_logger = None


def _resolve_core_logger(name: str) -> logging.Logger | None:
    """尝试从核心日志系统获取 logger（若不可用则返回 None）。"""
    if core_get_logger is None:
        return None
    try:
        base_logger = core_get_logger(name)
    except Exception:
        return None
    return getattr(base_logger, "logger", base_logger)


def _ensure_file_handler(logger: logging.Logger, name: str) -> None:
    """为 logger 添加指向 logs/ExchangeAdapter.<exchange>.log 的文件 handler。"""
    target_suffix = f"{name}.log"
    has_handler = any(
        isinstance(handler, RotatingFileHandler)
        and handler.baseFilename.endswith(target_suffix)
        for handler in logger.handlers
    )
    if has_handler:
        return

    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)

    file_handler = RotatingFileHandler(
        log_dir / target_suffix,
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - "
        "%(funcName)s:%(lineno)d - %(message)s"
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)


def _remove_non_file_handlers(logger: logging.Logger) -> None:
    """移除所有非文件类型的 handler，避免输出到终端。"""
    for handler in list(logger.handlers):
        if not isinstance(handler, RotatingFileHandler):
            logger.removeHandler(handler)
            try:
                handler.close()
            except Exception:
                pass


def get_exchange_logger(name: str) -> logging.Logger:
    """
    获取指定交易所的 logger。

    Args:
        name: 形如 `ExchangeAdapter.lighter` 的 logger 名称
    """
    logger = _resolve_core_logger(name)
    if logger is None:
        logger = logging.getLogger(name)

    logger.setLevel(logging.INFO)
    logger.propagate = False
    _ensure_file_handler(logger, name)
    _remove_non_file_handlers(logger)
    return logger


__all__ = ["get_exchange_logger"]

