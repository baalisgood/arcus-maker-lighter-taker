#!/usr/bin/env python3
"""
分段套利系统统一启动脚本（重构版）

使用方式：
    python main_unified.py

功能：
    - 使用新的统一决策引擎（总量驱动算法）
    - 支持多交易对独立配置
    - 支持剥头皮模式
    - 支持拆单执行
"""

import asyncio
import argparse
import sys
import os
from pathlib import Path
from typing import Optional

# 🔥 SSL 证书自动修复（放在所有网络库导入之前）
# 强制使用 certifi 提供的根证书，解决 macOS 外部终端 SSL 验证失败的问题
try:
    import certifi
    os.environ["SSL_CERT_FILE"] = certifi.where()
    os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()
    # print(f"🔐 SSL 证书路径已强制指定: {certifi.where()}")
except ImportError:
    print("⚠️  未找到 certifi 库，SSL 证书验证可能会在外部终端失败")

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent))

# 🔥 加载环境变量（必须在导入其他模块之前）
# 关键：禁用 dotenv 的变量插值，避免像 VARIATIONAL_COOKIE 这种包含大量 `$` 的值被错误替换
try:
    from dotenv import load_dotenv  # type: ignore
except Exception:
    load_dotenv = None


def _safe_load_dotenv(path: Path) -> bool:
    if not load_dotenv:
        return False
    try:
        load_dotenv(path, override=False, interpolate=False)  # type: ignore[call-arg]
        return True
    except TypeError:
        load_dotenv(path)
        return True


def _load_runtime_environment():
    env_path = Path(__file__).parent / ".env"
    if env_path.exists() and load_dotenv:
        try:
            loaded = _safe_load_dotenv(env_path)
            if loaded:
                print(f"✅ 已加载环境变量: {env_path}")
        except PermissionError as e:
            print(f"⚠️  无法读取 .env（权限不足），已跳过: {e}")
        except Exception as e:
            print(f"⚠️  加载 .env 失败，已跳过: {e}")
    elif not env_path.exists():
        print(f"⚠️  未找到 .env 文件: {env_path}")
        print("💡 如需配置API密钥，请创建 .env 文件")

from core.services.arbitrage_monitor_v2.core.unified_orchestrator import UnifiedOrchestrator
from core.services.arbitrage_monitor_v2.config.debug_config import DebugConfig


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description="分段套利系统统一启动脚本（重构版）"
    )
    
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/arbitrage/arbitrage_segmented.yaml"),
        help="分段配置文件路径"
    )
    
    parser.add_argument(
        "--monitor-config",
        type=Path,
        default=Path("config/arbitrage/monitor_lighter_arcus.yaml"),
        help="监控配置文件路径"
    )

    parser.add_argument(
        "--multi-exchange-config",
        type=Path,
        default=None,
        help=(
            "多交易所套利配置文件路径（many_to_many/one_to_many）。"
            "若提供，则优先于 monitor-config 内的 multi_exchange_config_path。"
        ),
    )
    
    parser.add_argument(
        "--debug",
        action="store_true",
        help="启用调试模式"
    )
    
    parser.add_argument("--execution-config", type=Path, default=Path("config/arbitrage/arbitrage_lighter_arcus.yaml"))
    return parser.parse_args()


async def main():
    """主入口函数"""
    args = parse_args()
    import yaml
    if yaml.safe_load(args.config.read_text()).get('execution_mode','taker_taker') != 'taker_taker':
        raise ValueError('Maker profile requires run_maker_taker.py; legacy executor cannot own maker symbols')
    _load_runtime_environment()
    
    # 配置Debug选项
    if args.debug:
        debug_config = DebugConfig.create_basic()
        print("🔧 调试模式已启用 (Basic)")
    else:
        debug_config = DebugConfig.create_production()
    orchestrator: Optional[UnifiedOrchestrator] = None
    interrupted = False
    force_exit_no_close = False
    try:
        # 初始化统一调度器
        orchestrator = UnifiedOrchestrator(
            segmented_config_path=args.config,
            monitor_config_path=args.monitor_config,
            multi_exchange_config_path=args.multi_exchange_config,
            execution_config_path=args.execution_config,
            debug_config=debug_config
        )

        # 启动系统
        await orchestrator.start()
        
        # 保持运行
        while True:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        interrupted = True
        print("\n🛑 收到 Ctrl+C：3 秒内再按一次 Ctrl+C 将直接退出（不平仓）")
        try:
            for remaining in (3, 2, 1):
                print(f"⏳ 将在 {remaining}s 后执行紧急平仓并退出...")
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            force_exit_no_close = True
            print("\n🛑 二次 Ctrl+C：立即退出（不执行紧急平仓）")
    except Exception as e:
        print(f"\n❌ 程序异常退出: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # 优雅关闭逻辑（如果 orchestrator 提供了 stop 方法）
        if orchestrator and hasattr(orchestrator, 'stop'):
            print("正在关闭调度器...")
            # Ctrl+C 时：先紧急平仓（实盘模式）再退出
            if (
                interrupted
                and (not force_exit_no_close)
                and hasattr(orchestrator, "emergency_close_all_positions")
            ):
                try:
                    # 仅在实盘模式执行紧急平仓
                    if not getattr(orchestrator, "monitor_only_mode", True):
                        print("🚨 检测到中断信号，正在紧急平仓所有仓位...")
                        await orchestrator.emergency_close_all_positions(timeout_seconds=60.0)  # type: ignore[attr-defined]
                except Exception as e:
                    print(f"⚠️  紧急平仓失败（仍将退出）：{e}")
            try:
                await orchestrator.stop()
            except KeyboardInterrupt:
                # 允许用户强制退出（但此时已明确选择不平仓/或已执行完平仓）
                pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
