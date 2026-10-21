"""
全局风险控制模块

职责：
- 仓位管理
- 账户余额管理
- 网络故障处理
- 交易所维护检测
- 脚本崩溃恢复
- 其他风险控制
"""

from typing import TYPE_CHECKING

from ..models import RiskStatus

if TYPE_CHECKING:
    # 仅用于类型提示，避免在包导入阶段触发循环依赖
    from .global_risk_controller import GlobalRiskController  # noqa: F401


def __getattr__(name: str):
    """
    延迟导入：避免 risk_control 包在导入阶段就加载 global_risk_controller，
    从而与 config.arbitrage_config 形成循环依赖。
    """
    if name == "GlobalRiskController":
        from .global_risk_controller import GlobalRiskController  # 局部导入

        return GlobalRiskController
    raise AttributeError(name)

__all__ = [
    'GlobalRiskController',
    'RiskStatus',
]

