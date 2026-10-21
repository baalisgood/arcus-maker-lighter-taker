"""
Reduce-Only Probe Service
-------------------------
独立管理 reduce-only 探测循环，保持与 `UnifiedOrchestrator` 原逻辑一致：
- 定时在整点 05 秒触发探测
- 遍历被 `ReduceOnlyGuard` 标记的组合，发送最小量探测单
- 根据结果更新 guard 状态
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Optional, TYPE_CHECKING

from core.adapters.exchanges.utils.setup_logging import LoggingConfig

if TYPE_CHECKING:
    from .unified_orchestrator import UnifiedOrchestrator


logger = LoggingConfig.setup_logger(
    name=__name__,
    log_file="unified_orchestrator.log",
    console_formatter=None,
    file_formatter="detailed",
    level=logging.INFO,
)
logger.propagate = False


class ReduceOnlyProbeService:
    def __init__(self, orchestrator: "UnifiedOrchestrator") -> None:
        self.orc = orchestrator
        self.task: Optional[asyncio.Task] = None

    def start(self) -> None:
        if not self.task:
            self.task = asyncio.create_task(self._probe_loop())

    async def stop(self) -> None:
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
            self.task = None

    async def _probe_loop(self):
        logger.info("🕛 [reduce-only] 探测任务已启动")
        try:
            while True:
                next_probe_at = self._compute_next_probe_time()
                now = datetime.now(self.orc.reduce_only_guard.timezone)
                wait_seconds = max(0.5, (next_probe_at - now).total_seconds())
                await asyncio.sleep(wait_seconds)
                if not self.orc.running:
                    continue
                try:
                    await self._run_reduce_only_probes()
                except Exception as exc:
                    logger.error("❌ [reduce-only] 探测执行失败: %s", exc, exc_info=True)
        except asyncio.CancelledError:
            logger.info("🛑 [reduce-only] 探测任务已停止")

    def _compute_next_probe_time(self) -> datetime:
        now = datetime.now(self.orc.reduce_only_guard.timezone)
        # 与设计一致：每小时整点 05 秒触发探测
        candidate = now.replace(minute=0, second=5, microsecond=0)
        if now >= candidate:
            candidate += timedelta(hours=1)
        return candidate

    async def _run_reduce_only_probes(self):
        blocked_pairs = self.orc.reduce_only_guard.list_blocked_pairs()
        if not blocked_pairs:
            return
        logger.info(
            "🔍 [reduce-only] 当前共有 %d 个套利对被标记为只平仓，执行探测任务",
            len(blocked_pairs),
        )
        for state in blocked_pairs:
            min_qty = self.orc._get_min_order_size(state.pair_id) or Decimal("0.001")
            restored = False
            for exchange, leg_symbol in state.probe_legs():
                success = await self.orc.executor.probe_reduce_only_leg(
                    exchange=exchange,
                    symbol=leg_symbol,
                    quantity=min_qty,
                    price=Decimal("2000"),
                )
                self.orc.reduce_only_guard.record_probe_result(
                    pair_id=state.pair_id,
                    exchange=exchange,
                    symbol=leg_symbol,
                    success=success,
                )
                if success:
                    logger.info(
                        "✅ [reduce-only] 探测成功: %s %s，套利对 %s 恢复开仓/平仓",
                        exchange.upper(),
                        leg_symbol,
                        state.pair_id,
                    )
                    restored = True
                    break
                else:
                    logger.info(
                        "⏳ [reduce-only] 探测失败: %s %s 仍限制交易，等待下一整点",
                        exchange.upper(),
                        leg_symbol,
                    )
            if not restored:
                logger.info(
                    "ℹ️ [reduce-only] %s 仍处于只平仓模式，下一整点将再次探测",
                    state.pair_id,
                )


__all__ = ["ReduceOnlyProbeService"]

