"""
分段套利系统统一调度器（重构版）

【文件拆分说明（置顶）】
- 本文件为“组装入口”，用于保持原有导入路径与运行入口不变。
- 原始大文件：`core/services/arbitrage_monitor_v2/core/unified_orchestrator.py`（已备份为同目录 `.bak.*`）
- 拆分产生的文件：
  - `unified_orchestrator_logging.py`
  - `unified_orchestrator_init_mixin.py`
  - `unified_orchestrator_market_mixin.py`
  - `unified_orchestrator_config_mixin.py`
  - `unified_orchestrator_gates_mixin.py`
  - `unified_orchestrator_ema_mixin.py`
  - `unified_orchestrator_lock_mixin.py`
  - `unified_orchestrator_quantity_mixin.py`
  - `unified_orchestrator_ui_facade_mixin.py`
  - `unified_orchestrator_execution_utils_mixin.py`
  - `unified_orchestrator_open_mixin.py`
  - `unified_orchestrator_close_mixin.py`
  - `unified_orchestrator_position_audit_mixin.py`
  - `unified_orchestrator_runtime_mixin.py`

⚠️ 重要：本次改动仅做“代码搬家/文件拆分”，不改任何业务逻辑，确保功能完全一致。
"""

from __future__ import annotations

import asyncio

from .unified_orchestrator_logging import logger
from .unified_orchestrator_init_mixin import UnifiedOrchestratorInitMixin
from .unified_orchestrator_market_mixin import UnifiedOrchestratorMarketMixin
from .unified_orchestrator_config_mixin import UnifiedOrchestratorConfigMixin
from .unified_orchestrator_gates_mixin import UnifiedOrchestratorGatesMixin
from .unified_orchestrator_ema_mixin import UnifiedOrchestratorEmaMixin
from .unified_orchestrator_lock_mixin import UnifiedOrchestratorLockMixin
from .unified_orchestrator_quantity_mixin import UnifiedOrchestratorQuantityMixin
from .unified_orchestrator_ui_facade_mixin import UnifiedOrchestratorUiFacadeMixin
from .unified_orchestrator_execution_utils_mixin import UnifiedOrchestratorExecutionUtilsMixin
from .unified_orchestrator_open_mixin import UnifiedOrchestratorOpenMixin
from .unified_orchestrator_close_mixin import UnifiedOrchestratorCloseMixin
from .unified_orchestrator_position_audit_mixin import UnifiedOrchestratorPositionAuditMixin
from .unified_orchestrator_runtime_mixin import UnifiedOrchestratorRuntimeMixin


class UnifiedOrchestrator(
    UnifiedOrchestratorInitMixin,
    UnifiedOrchestratorMarketMixin,
    UnifiedOrchestratorConfigMixin,
    UnifiedOrchestratorGatesMixin,
    UnifiedOrchestratorEmaMixin,
    UnifiedOrchestratorLockMixin,
    UnifiedOrchestratorQuantityMixin,
    UnifiedOrchestratorUiFacadeMixin,
    UnifiedOrchestratorExecutionUtilsMixin,
    UnifiedOrchestratorOpenMixin,
    UnifiedOrchestratorCloseMixin,
    UnifiedOrchestratorPositionAuditMixin,
    UnifiedOrchestratorRuntimeMixin,
):
    """
    统一调度器（支持多交易对独立配置）

    说明：实现已拆分到多个 mixin 文件；此类仅负责组装，保持外部 import 不变。
    """


# 主函数（保留原入口行为）
async def main():
    """主函数"""
    orchestrator = UnifiedOrchestrator()

    try:
        await orchestrator.start()
    except KeyboardInterrupt:
        logger.warning("⚠️  [统一调度] 收到 Ctrl+C：3 秒内再次 Ctrl+C 将直接退出（不平仓）")
        force_exit_no_close = False
        try:
            for remaining in (3, 2, 1):
                logger.warning("⏳ [统一调度] 将在 %ss 后执行紧急平仓并退出...", remaining)
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            force_exit_no_close = True
            logger.warning("🛑 [统一调度] 二次 Ctrl+C：立即退出（不执行紧急平仓）")
    finally:
        # Ctrl+C 退出：优先紧急平仓（仅实盘模式）
        try:
            if (
                not getattr(orchestrator, "monitor_only_mode", True)
                and not locals().get("force_exit_no_close", False)
            ):
                await orchestrator.emergency_close_all_positions(timeout_seconds=60.0)
        except Exception:
            pass
        try:
            await orchestrator.stop()
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    asyncio.run(main())
