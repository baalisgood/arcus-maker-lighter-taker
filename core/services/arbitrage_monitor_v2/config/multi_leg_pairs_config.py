"""
多腿套利配置

支持在分段模式下配置任意交易所/交易对组合，例如：
- 同一交易所不同代币（lighter: PAXG vs XAU）
- 不同交易所不同代币（edgex: PAXG vs lighter: XAU）
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
import logging

try:
    import yaml
except Exception:  # pragma: no cover
    yaml = None


logger = logging.getLogger(__name__)


@dataclass
class LegSettings:
    """单条腿的订阅/执行信息"""

    exchange: str
    symbol: str

    def normalized_exchange(self) -> str:
        return self.exchange.lower().strip()

    def normalized_symbol(self) -> str:
        return self.symbol.upper().strip()


@dataclass
class MultiLegPairConfig:
    """多腿套利组合配置"""

    pair_id: str
    leg_primary: LegSettings
    leg_secondary: LegSettings
    allow_reverse: bool = True
    enabled: bool = True
    description: Optional[str] = None
    min_spread_pct: Optional[float] = None

    def get_leg_symbols(self) -> List[str]:
        """返回两个腿涉及的标准化交易对"""
        return [
            self.leg_primary.normalized_symbol(),
            self.leg_secondary.normalized_symbol(),
        ]


class MultiLegPairsConfigManager:
    """多腿套利配置管理器"""

    def __init__(self, config_path: Optional[Path] = None):
        default_path = Path("config/arbitrage/multi_leg_pairs.yaml")
        self.config_path = Path(config_path) if config_path else default_path
        self._pairs: List[MultiLegPairConfig] = []
        self._load()

    def _load(self) -> None:
        if not self.config_path.exists():
            logger.info("🔍 未找到多腿套利配置: %s", self.config_path)
            return

        if yaml is None:
            logger.warning("⚠️ 未安装 PyYAML，无法读取多腿套利配置: %s", self.config_path)
            return

        try:
            with self.config_path.open("r", encoding="utf-8") as f:
                raw = yaml.safe_load(f) or {}
        except Exception as exc:  # pragma: no cover
            logger.warning("⚠️ 加载多腿套利配置失败 %s: %s", self.config_path, exc)
            return

        pairs_data = raw.get("pairs", raw if isinstance(raw, list) else [])
        if not isinstance(pairs_data, list):
            logger.warning("⚠️ 多腿套利配置格式不正确（期望 list），已忽略")
            return

        for entry in pairs_data:
            try:
                pair = self._parse_pair(entry)
            except Exception as exc:
                logger.warning("⚠️ 解析多腿套利配置失败，已跳过: %s", exc)
                continue
            if pair.enabled:
                self._pairs.append(pair)

        if self._pairs:
            logger.info("✅ 加载多腿套利配置 %d 条 (%s)", len(self._pairs), self.config_path)

    def _parse_pair(self, entry: dict) -> MultiLegPairConfig:
        if not isinstance(entry, dict):
            raise ValueError("pair 配置必须为字典")

        pair_id = entry.get("id") or entry.get("pair_id")
        if not pair_id:
            raise ValueError("pair 配置缺少 id 字段")

        leg_primary = self._parse_leg(entry.get("leg_primary") or entry.get("leg_a"))
        leg_secondary = self._parse_leg(entry.get("leg_secondary") or entry.get("leg_b"))

        return MultiLegPairConfig(
            pair_id=pair_id.upper(),
            leg_primary=leg_primary,
            leg_secondary=leg_secondary,
            allow_reverse=entry.get("allow_reverse", True),
            enabled=entry.get("enabled", True),
            description=entry.get("description"),
            min_spread_pct=entry.get("min_spread_pct"),
        )

    @staticmethod
    def _parse_leg(data: dict) -> LegSettings:
        if not isinstance(data, dict):
            raise ValueError("leg 配置必须为字典")

        exchange = data.get("exchange")
        symbol = data.get("symbol")
        if not exchange or not symbol:
            raise ValueError("leg 配置缺少 exchange 或 symbol")

        return LegSettings(exchange=exchange, symbol=symbol)

    def get_pairs(self) -> List[MultiLegPairConfig]:
        return list(self._pairs)

    def get_required_symbols(self) -> List[str]:
        symbols: List[str] = []
        for pair in self._pairs:
            symbols.extend(pair.get_leg_symbols())
        # 去重但保持顺序
        seen = set()
        unique = []
        for sym in symbols:
            if sym not in seen:
                seen.add(sym)
                unique.append(sym)
        return unique


