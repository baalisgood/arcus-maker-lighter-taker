"""
声音报警（极简版）

设计目标：
- 不引入第三方依赖，不调用系统命令（避免平台差异/权限问题）
- 不阻塞主循环：使用 asyncio task 异步蜂鸣
- 做节流：避免同一类风险在短时间内重复响个不停

实现方式：
- 使用终端蜂鸣符号 '\\a'（是否真的发声取决于终端/系统设置）
"""

from __future__ import annotations

import asyncio
import sys
import time
import platform
import shutil
from dataclasses import dataclass
from typing import Optional, Dict, Any


@dataclass
class SoundAlarmConfig:
    enabled: bool = True
    repeat_times: int = 5
    interval_seconds: float = 0.35
    throttle_seconds: float = 300.0
    # 声音输出方式：
    # - "auto": macOS 优先用 say（语音播报），否则 afplay，最后退回终端蜂鸣
    # - "terminal": 仅使用 '\a'（取决于终端/系统设置，可能无声）
    # - "afplay": 仅在 macOS 使用 afplay 播放系统音效
    # - "say": 仅在 macOS 使用 say 语音播报（更直观）
    method: str = "auto"
    # macOS 默认提示音（系统自带，无需额外文件）
    # 更尖锐的默认提示音（比 Ping 更明显）
    afplay_sound_file: str = "/System/Library/Sounds/Sosumi.aiff"
    # macOS 语音播报配置（say）
    # - say_text: 默认播报内容（也可在 trigger 里用 reason 动态替换）
    # - say_voice: 指定音色（留空=系统默认）
    # - say_rate_wpm: 语速（词/分钟），留空=系统默认
    say_text: str = "风控报警，请检查账户余额。"
    say_voice: str = ""
    say_rate_wpm: int = 0


class SoundAlarm:
    def __init__(self, cfg: SoundAlarmConfig) -> None:
        self._cfg = cfg
        # 触发节流：按 signature 分组，而不是“全局单节流”
        # 否则会出现：先播报“余额不足暂停开仓”，随后“强制平仓”播报被 300s 节流吞掉
        self._last_trigger_ts_by_signature: Dict[str, float] = {}
        self._task: Optional[asyncio.Task] = None
        # 最近一次触发的上下文（用于 say 动态播报）
        self._last_signature: str = ""
        self._last_reason: str = ""
        # 最近一次触发的“本次重复次数”（允许 trigger 级别覆盖全局 repeat_times）
        self._last_repeat_times: int = int(getattr(cfg, "repeat_times", 5) or 5)

    @staticmethod
    def _is_force_close_signature(signature: str, reason: str) -> bool:
        sig = (signature or "").strip().lower()
        rsn = (reason or "").strip()
        return bool(
            ("force_close" in sig)
            or ("force_close_all_positions" in rsn)
            or ("强制平仓" in rsn)
            or ("强平" in rsn)
        )

    @staticmethod
    def from_system_mode(system_mode: Optional[Dict[str, Any]]) -> "SoundAlarm":
        """
        读取 system_mode 里的可选开关（不要求必须配置，缺失则用默认值）。
        支持两种写法：
        - system_mode.sound_alarm: { enabled, repeat_times, interval_seconds, throttle_seconds }
        - system_mode.sound_alarm_enabled / sound_alarm_repeat_times / ...
        """
        sys_mode = system_mode or {}
        nested = sys_mode.get("sound_alarm", {}) if isinstance(sys_mode.get("sound_alarm", {}), dict) else {}

        def _get(key: str, default: Any) -> Any:
            if key in nested:
                return nested.get(key, default)
            flat_key = f"sound_alarm_{key}"
            return sys_mode.get(flat_key, default)

        try:
            cfg = SoundAlarmConfig(
                enabled=bool(_get("enabled", True)),
                repeat_times=max(1, int(_get("repeat_times", 5) or 5)),
                interval_seconds=max(0.05, float(_get("interval_seconds", 0.35) or 0.35)),
                throttle_seconds=max(0.0, float(_get("throttle_seconds", 300.0) or 300.0)),
                method=str(_get("method", "auto") or "auto"),
                afplay_sound_file=str(_get("afplay_sound_file", "/System/Library/Sounds/Sosumi.aiff") or "/System/Library/Sounds/Sosumi.aiff"),
                say_text=str(_get("say_text", "风控报警，请检查账户余额。") or "风控报警，请检查账户余额。"),
                say_voice=str(_get("say_voice", "") or ""),
                say_rate_wpm=int(_get("say_rate_wpm", 0) or 0),
            )
        except Exception:
            cfg = SoundAlarmConfig()
        return SoundAlarm(cfg)

    def trigger(
        self,
        *,
        signature: str,
        reason: Optional[str] = None,
        repeat_times_override: Optional[int] = None,
    ) -> None:
        """
        触发声音报警（异步，不阻塞）。
        - signature: 用于区分触发源，但当前实现是“全局单节流”。
        - reason: 仅用于调试/未来扩展，目前不输出（避免刷屏）。
        """
        if not getattr(self._cfg, "enabled", False):
            return

        now = time.time()
        sig = str(signature or "")
        rsn = str(reason or "")
        is_force_close = self._is_force_close_signature(sig, rsn)

        # 已在响：不重复启动，但允许“强平事件”升级当前播报内容
        if self._task and not self._task.done():
            try:
                current_is_force_close = self._is_force_close_signature(
                    self._last_signature, self._last_reason
                )
            except Exception:
                current_is_force_close = False
            if is_force_close and not current_is_force_close:
                # 升级播报内容为“强制平仓”
                self._last_signature = sig
                self._last_reason = rsn
                try:
                    if repeat_times_override is not None:
                        self._last_repeat_times = max(1, int(repeat_times_override))
                    else:
                        self._last_repeat_times = int(self._cfg.repeat_times)
                except Exception:
                    pass
            return

        # 节流：按 signature 分组，避免不同风险事件互相“吞声音”
        try:
            last_ts = float(self._last_trigger_ts_by_signature.get(sig, 0.0) or 0.0)
        except Exception:
            last_ts = 0.0
        if now - last_ts < float(self._cfg.throttle_seconds):
            return

        self._last_trigger_ts_by_signature[sig] = now
        self._last_signature = sig
        self._last_reason = rsn
        try:
            if repeat_times_override is None:
                self._last_repeat_times = int(self._cfg.repeat_times)
            else:
                self._last_repeat_times = max(1, int(repeat_times_override))
        except Exception:
            self._last_repeat_times = int(getattr(self._cfg, "repeat_times", 5) or 5)
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # 无运行中的 loop：不做同步 sleep，直接打一声（尽量不影响启动流程）
            # 这里不做 say/afplay（避免阻塞），只做终端蜂鸣兜底
            self._terminal_bell()
            return

        self._task = loop.create_task(self._beep_sequence(repeat_times=self._last_repeat_times))

    async def _beep_sequence(self, *, repeat_times: int) -> None:
        times = max(1, int(repeat_times))
        for i in range(times):
            await self._beep_once()
            if i < times - 1:
                await asyncio.sleep(float(self._cfg.interval_seconds))

    async def _beep_once(self) -> None:
        """
        单次提示音：
        - macOS 下优先 say（语音播报）
        - macOS 下优先 afplay（更可靠，不依赖终端 bell 设置）
        - 失败/非 macOS 再退回终端蜂鸣 '\a'
        """
        method = (self._cfg.method or "auto").strip().lower()
        if method not in ("auto", "terminal", "afplay", "say"):
            method = "auto"

        if platform.system().lower() == "darwin":
            # 1) say（语音播报）
            if method in ("auto", "say") and shutil.which("say"):
                try:
                    args = ["say"]
                    voice = (self._cfg.say_voice or "").strip()
                    if voice:
                        args.extend(["-v", voice])
                    rate = int(getattr(self._cfg, "say_rate_wpm", 0) or 0)
                    if rate > 0:
                        args.extend(["-r", str(rate)])

                    # 优先使用“触发原因”生成更具体的播报内容（不同风控播报不同）
                    text = self._build_say_text(
                        signature=self._last_signature,
                        reason=self._last_reason,
                    )
                    proc = await asyncio.create_subprocess_exec(
                        *args,
                        text,
                        stdout=asyncio.subprocess.DEVNULL,
                        stderr=asyncio.subprocess.DEVNULL,
                    )
                    await proc.wait()
                    return
                except Exception:
                    if method == "say":
                        return

            # 2) afplay（系统音效）
        if method in ("auto", "afplay"):
            if platform.system().lower() == "darwin" and shutil.which("afplay"):
                sound_file = (self._cfg.afplay_sound_file or "").strip()
                if sound_file:
                    try:
                        proc = await asyncio.create_subprocess_exec(
                            "afplay",
                            sound_file,
                            stdout=asyncio.subprocess.DEVNULL,
                            stderr=asyncio.subprocess.DEVNULL,
                        )
                        await proc.wait()
                        return
                    except Exception:
                        # 继续降级到终端蜂鸣
                        pass
            if method == "afplay":
                return

        # terminal bell（可能无声，但作为兜底无副作用）
        self._terminal_bell()

    def _build_say_text(self, *, signature: str, reason: str) -> str:
        """
        生成语音播报内容：
        - 尽量短、清晰，避免 10 次播报耗时过长
        - 根据 signature/reason 区分不同风控类型
        """
        sig = (signature or "").strip().lower()
        rsn = (reason or "").strip()

        # 1) 余额类（balance_management）
        if "balance" in sig or "余额" in rsn:
            if "force_close" in sig or "force_close_all_positions" in rsn or "强平" in rsn:
                return "风控报警。余额不足，触发强制平仓。"
            return "风控报警。余额不足，已暂停开仓。"

        # 2) 开仓闸门（open_only_close）
        if "open_only_close" in sig or "禁止开仓" in rsn:
            if "权益不足" in rsn or "余额不足" in rsn:
                return "风控报警。开仓限制：权益不足。"
            if "总持仓价值" in rsn or "持仓价值" in rsn:
                return "风控报警。开仓限制：持仓价值过高。"
            if "价格波动" in rsn:
                return "风控报警。开仓限制：价格波动过大。"
            return "风控报警。开仓限制已触发。"

        # 3) 其他：播报原因的摘要（避免把长字符串读出来）
        default_text = (self._cfg.say_text or "").strip() or "风控报警。"
        return default_text

    @staticmethod
    def _terminal_bell() -> None:
        try:
            sys.stdout.write("\a")
            sys.stdout.flush()
        except Exception:
            return


