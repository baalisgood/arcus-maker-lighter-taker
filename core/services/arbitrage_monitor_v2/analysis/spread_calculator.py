"""
差价计算引擎

职责：
- 计算交易所间的价差
- 识别低买高卖机会
- 提供差价数据
- 🔥 支持多腿套利（同交易所不同代币、跨交易所不同代币）
"""

from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from decimal import Decimal

import logging
import time
from core.adapters.exchanges.models import OrderBookData
from ..config.debug_config import DebugConfig

# 🔥 使用统一日志系统
from core.adapters.exchanges.utils.setup_logging import LoggingConfig

logger = LoggingConfig.setup_logger(
    name=__name__,
    log_file='spread_calculator.log',
    console_formatter=None,
    file_formatter='detailed',
    level=logging.INFO
)
logger.propagate = False


@dataclass
class SpreadData:
    """
    价差数据
    
    包含两个交易所之间的价差信息（包括正差价和负差价）
    - 正差价（spread_pct > 0）：有利可图，可以实现低买高卖
    - 负差价（spread_pct < 0）：亏损，无法实现低买高卖
    - 零差价（spread_pct = 0）：无利可图，买卖价格相同
    """
    symbol: str
    exchange_buy: str   # 买入交易所（在该交易所的Ask1价格买入）
    exchange_sell: str  # 卖出交易所（在该交易所的Bid1价格卖出）
    price_buy: Decimal  # 买入价（exchange_buy的Ask1价格）
    price_sell: Decimal # 卖出价（exchange_sell的Bid1价格）
    size_buy: Decimal   # 买入数量（exchange_buy的Ask1数量）
    size_sell: Decimal  # 卖出数量（exchange_sell的Bid1数量）
    spread_abs: Decimal # 绝对差价（price_sell - price_buy，可能为负）
    spread_pct: float   # 差价百分比（(price_sell - price_buy) / price_buy * 100，正数表示有利可图，负数表示亏损）
    buy_symbol: Optional[str] = None  # 买入交易所对应的具体交易对
    sell_symbol: Optional[str] = None # 卖出交易所对应的具体交易对


class SpreadCalculator:
    """差价计算器"""
    
    def __init__(self, debug_config: DebugConfig):
        """
        初始化差价计算器
        
        Args:
            debug_config: Debug配置
        """
        self.debug = debug_config
        self._calc_counter = 0
        self._warning_log_times: Dict[str, float] = {}
        self._warning_log_interval = 60.0  # 秒级：同一类型的警告最多每分钟打印一次
        self._status_log_times: Dict[str, float] = {}
        self._status_log_interval = 60.0  # 状态日志：默认每个symbol每分钟一次
    
    def calculate_spreads(
        self,
        symbol: str,
        orderbooks: Dict[str, OrderBookData]
    ) -> List[SpreadData]:
        """
        计算所有交易所间的价差（包括正差价和负差价）
        
        ✅ 活跃方法：被以下调度器使用
        - orchestrator.py (V2监控模式)
        - orchestrator_simple.py (简化版调度器)
        - arbitrage_orchestrator_v3.py (V3-BASE基础执行模式)
        
        🆕 新版调度器：unified_orchestrator.py 使用 calculate_spreads_multi_exchange_directions()
        
        功能说明：
        - 遍历所有交易所组合（A-B, A-C, B-C...）
        - 每对计算两个方向（A买B卖 & B买A卖）
        - 返回所有价差，包括正价差和负价差
        - 不筛选，由调用方决定使用哪些价差
        
        与 calculate_spreads_multi_exchange_directions() 的区别：
        - 功能完全相同，都返回所有方向的所有价差
        - 此方法有 Debug 输出，但缺少日志节流
        - 新方法有更完善的错误处理和警告提示
        
        Args:
            symbol: 交易对
            orderbooks: {exchange: orderbook}
            
        Returns:
            价差数据列表（包含所有价差，正差价为正数，负差价为负数）
            每个交易所对都会计算两个方向的价差：
            - 方向1: ex1买 -> ex2卖
            - 方向2: ex2买 -> ex1卖
            
        Example:
            假设有3个交易所（A, B, C），返回6个价差：
            [A买B卖, B买A卖, A买C卖, C买A卖, B买C卖, C买B卖]
        """
        spreads = []
        exchanges = list(orderbooks.keys())
        
        # 遍历所有交易所对
        for i, ex1 in enumerate(exchanges):
            for ex2 in enumerate(exchanges[i+1:], start=i+1):
                ex2_idx, ex2 = ex2
                
                ob1 = orderbooks[ex1]
                ob2 = orderbooks[ex2]
                
                # 验证数据完整性
                if not self._validate_orderbook(ob1) or not self._validate_orderbook(ob2):
                    continue
                
                # 🔥 方向1: ex1买 -> ex2卖
                # 计算价差：(ex2的Bid - ex1的Ask) / ex1的Ask * 100
                # 如果 ex2的Bid > ex1的Ask，价差为正（有利可图）
                # 如果 ex2的Bid <= ex1的Ask，价差为负或0（无利可图或亏损）
                spread_abs_1 = ob2.best_bid.price - ob1.best_ask.price
                spread_pct_1 = float((spread_abs_1 / ob1.best_ask.price) * 100)
                
                spreads.append(SpreadData(
                    symbol=symbol,
                    exchange_buy=ex1,
                    exchange_sell=ex2,
                    price_buy=ob1.best_ask.price,
                    price_sell=ob2.best_bid.price,
                    size_buy=ob1.best_ask.size,
                    size_sell=ob2.best_bid.size,
                    spread_abs=spread_abs_1,
                    spread_pct=spread_pct_1,  # 正数表示有利可图，负数表示亏损
                    buy_symbol=symbol,
                    sell_symbol=symbol
                ))
                
                # 🔥 方向2: ex2买 -> ex1卖
                # 计算价差：(ex1的Bid - ex2的Ask) / ex2的Ask * 100
                # 如果 ex1的Bid > ex2的Ask，价差为正（有利可图）
                # 如果 ex1的Bid <= ex2的Ask，价差为负或0（无利可图或亏损）
                spread_abs_2 = ob1.best_bid.price - ob2.best_ask.price
                spread_pct_2 = float((spread_abs_2 / ob2.best_ask.price) * 100)
                
                spreads.append(SpreadData(
                    symbol=symbol,
                    exchange_buy=ex2,
                    exchange_sell=ex1,
                    price_buy=ob2.best_ask.price,
                    price_sell=ob1.best_bid.price,
                    size_buy=ob2.best_ask.size,
                    size_sell=ob1.best_bid.size,
                    spread_abs=spread_abs_2,
                    spread_pct=spread_pct_2,  # 正数表示有利可图，负数表示亏损
                    buy_symbol=symbol,
                    sell_symbol=symbol
                ))
        
        # Debug输出（采样）
        self._calc_counter += 1
        if self.debug.show_spread_calc and self.debug.should_show_spread_calc(self._calc_counter):
            if spreads:
                for s in spreads:
                    # 🔥 根据价差正负显示不同的标识
                    if s.spread_pct > 0:
                        print(f"💰 {s.symbol} 套利机会: "
                              f"{s.exchange_buy}买@{s.price_buy:.2f} → "
                              f"{s.exchange_sell}卖@{s.price_sell:.2f} | "
                              f"差价=+{s.spread_pct:.3f}%")
                    else:
                        print(f"⚠️ {s.symbol} 负价差: "
                              f"{s.exchange_buy}买@{s.price_buy:.2f} → "
                              f"{s.exchange_sell}卖@{s.price_sell:.2f} | "
                              f"差价={s.spread_pct:.3f}%")

        # 每个symbol输出一次状态日志，帮助定位未触发套利的原因
        self._log_spread_status(symbol, spreads)
        
        return spreads
    
    def _validate_orderbook(self, orderbook: OrderBookData) -> bool:
        """
        验证订单簿数据
        
        Args:
            orderbook: 订单簿数据
            
        Returns:
            是否有效
        """
        if not orderbook.best_bid or not orderbook.best_ask:
            return False
        
        if orderbook.best_bid.price <= 0 or orderbook.best_ask.price <= 0:
            return False
        
        if orderbook.best_bid.size <= 0 or orderbook.best_ask.size <= 0:
            return False
        
        # 检查价差合理性（Bid应该小于Ask）
        if orderbook.best_bid.price >= orderbook.best_ask.price:
            return False
        
        return True
    
    def calculate_single_spread(
        self,
        exchange1: str,
        orderbook1: OrderBookData,
        exchange2: str,
        orderbook2: OrderBookData,
        symbol: str
    ) -> Optional[SpreadData]:
        """
        计算两个指定交易所间的最佳价差
        
        ⚠️ 已废弃：此方法不再被任何代码使用
        
        废弃原因：
        - 只返回一个方向（无法获取平仓视角）
        - 只返回正价差（≤0时返回None）
        - 功能被 calculate_spreads_multi_exchange_directions() 完全覆盖
        
        历史用途：
        - 原计划用于多交易所配置，为每个套利对单独计算价差
        - 实际开发中改用了更强大的 calculate_spreads_multi_exchange_directions()
        
        保留原因：
        - 代码简单，不影响性能
        - 可能有外部测试或工具依赖
        - 作为"两个交易所最佳价差"的简单接口
        
        Args:
            exchange1: 交易所1
            orderbook1: 交易所1的订单簿
            exchange2: 交易所2
            orderbook2: 交易所2的订单簿
            symbol: 交易对
            
        Returns:
            最佳价差数据（只返回正价差，≤0时返回None）
            
        Example:
            spread = calculate_single_spread("lighter", ob1, "edgex", ob2, "BTC-USDC-PERP")
            if spread:
                print(f"最优方向: {spread.exchange_buy}买→{spread.exchange_sell}卖")
        """
        if not self._validate_orderbook(orderbook1) or not self._validate_orderbook(orderbook2):
            return None
        
        # 方向1: ex1买 -> ex2卖
        spread1_abs = orderbook2.best_bid.price - orderbook1.best_ask.price
        spread1_pct = float((spread1_abs / orderbook1.best_ask.price) * 100)
        
        # 方向2: ex2买 -> ex1卖
        spread2_abs = orderbook1.best_bid.price - orderbook2.best_ask.price
        spread2_pct = float((spread2_abs / orderbook2.best_ask.price) * 100)
        
        # 选择更大的价差
        if spread1_pct > spread2_pct and spread1_pct > 0:
            return SpreadData(
                symbol=symbol,
                exchange_buy=exchange1,
                exchange_sell=exchange2,
                price_buy=orderbook1.best_ask.price,
                price_sell=orderbook2.best_bid.price,
                size_buy=orderbook1.best_ask.size,
                size_sell=orderbook2.best_bid.size,
                spread_abs=spread1_abs,
                spread_pct=spread1_pct,
                buy_symbol=symbol,
                sell_symbol=symbol
            )
        elif spread2_pct > 0:
            return SpreadData(
                symbol=symbol,
                exchange_buy=exchange2,
                exchange_sell=exchange1,
                price_buy=orderbook2.best_ask.price,
                price_sell=orderbook1.best_bid.price,
                size_buy=orderbook2.best_ask.size,
                size_sell=orderbook1.best_bid.size,
                spread_abs=spread2_abs,
                spread_pct=spread2_pct,
                buy_symbol=symbol,
                sell_symbol=symbol
            )
        
        return None
    
    def calculate_spreads_multi_exchange_directions(
        self,
        symbol: str,
        orderbooks: Dict[str, OrderBookData]
    ) -> List[SpreadData]:
        """
        计算所有交易所间的价差（改进版，推荐使用）
        
        ✅ 主要使用：unified_orchestrator.py (统一网格套利调度器)
        
        功能说明：
        - 遍历所有交易所组合（A-B, A-C, B-C...）
        - 每对计算两个方向（A买B卖 & B买A卖）
        - 返回所有价差，包括正价差和负价差
        - 不筛选，由调用方决定使用哪些价差
        
        与 calculate_spreads() 的区别：
        - ✅ 功能完全相同，都返回所有方向的所有价差
        - ✅ 增加了日志节流机制（避免刷屏）
        - ✅ 更完善的错误处理和警告提示
        - ✅ 使用内部函数 _append_direction，代码结构更清晰
        - ❌ 没有 Debug 输出（依赖外部日志）
        
        Args:
            symbol: 交易对
            orderbooks: {exchange: orderbook}
            
        Returns:
            价差数据列表（包含所有价差，正差价为正数，负差价为负数）
            每个交易所对都会计算两个方向的价差：
            - 方向1: ex1买 -> ex2卖
            - 方向2: ex2买 -> ex1卖
            
        Example:
            假设有3个交易所（A, B, C），返回6个价差：
            [A买B卖, B买A卖, A买C卖, C买A卖, B买C卖, C买B卖]
        """
        spreads: List[SpreadData] = []
        exchange_items = [
            (exchange, ob) for exchange, ob in orderbooks.items() if ob is not None
        ]

        if len(exchange_items) < 2:
            message = f"[价差计算] {symbol}: 可用交易所不足2个，无法计算价差"
            self._log_warning(f"{symbol}:insufficient_exchanges", message)
            self._log_status(
                f"{symbol}:status:insufficient_exchanges",
                f"[价差状态] {symbol}: {message}"
            )
            return spreads

        def _append_direction(
            exchange_buy: str,
            ob_buy: OrderBookData,
            exchange_sell: str,
            ob_sell: OrderBookData,
        ) -> None:
            buy_level = ob_buy.best_ask
            sell_level = ob_sell.best_bid
            if not buy_level or not sell_level:
                self._log_warning(
                    f"{symbol}:missing_direction:{exchange_buy}->{exchange_sell}",
                    (
                        f"[价差计算] {symbol}: 缺少方向 "
                        f"{exchange_buy}买→{exchange_sell}卖 的最优盘口（Ask/Bid）"
                    ),
                )
                return
            spread_abs = sell_level.price - buy_level.price
            spread_pct = float((spread_abs / buy_level.price) * 100)
            spreads.append(
                SpreadData(
                    symbol=symbol,
                    exchange_buy=exchange_buy,
                    exchange_sell=exchange_sell,
                    price_buy=buy_level.price,
                    price_sell=sell_level.price,
                    size_buy=buy_level.size,
                    size_sell=sell_level.size,
                    spread_abs=spread_abs,
                    spread_pct=spread_pct,
                    buy_symbol=symbol,
                    sell_symbol=symbol,
                )
            )

        for idx in range(len(exchange_items)):
            exchange_a, ob_a = exchange_items[idx]
            if not self._validate_orderbook(ob_a):
                continue
            for jdx in range(idx + 1, len(exchange_items)):
                exchange_b, ob_b = exchange_items[jdx]
                if not self._validate_orderbook(ob_b):
                    continue
                _append_direction(exchange_a, ob_a, exchange_b, ob_b)
                _append_direction(exchange_b, ob_b, exchange_a, ob_a)

        if not spreads:
            message = f"[价差计算] {symbol}: 无法从可用盘口计算任一方向价差"
            self._log_warning(f"{symbol}:no_cross_spread", message)
            self._log_status(
                f"{symbol}:status:no_cross_spread",
                f"[价差状态] {symbol}: {message}"
            )

        return spreads

    def build_closing_spread_from_orderbooks(
        self,
        opening_spread: SpreadData,
        orderbooks: Dict[str, OrderBookData],
    ) -> Optional[SpreadData]:
        """
        为单交易对/多交易所重新计算平仓视角价差（基于当前实际盘口）
        
        ✅ 活跃方法：被 unified_orchestrator.py (统一网格调度器) 使用
        
        使用场景：
        - spread_pipeline.process_symbol() - 单交易对场景
        - spread_pipeline._process_single_trading_pair() - 多交易所配置场景
        
        核心作用：
        - 根据开仓方向，确定平仓方向（反向交易）
        - 从当前订单簿重新获取真实的平仓价格
        - 不使用开仓时的价格，不使用字段互换
        
        为什么需要重新计算：
        1. 市场价格实时变化 - 开仓和平仓之间价格已经不同
        2. 平仓需要反向交易 - 需要不同的盘口面（Ask vs Bid）
        3. 执行价格必须准确 - 影响限价单、滑点保护、盈亏计算
        4. 避免使用过期数据 - 不能用开仓时的价格去平仓
        
        逻辑说明：
        - 开仓: 在 exchange_buy 的 Ask 买，在 exchange_sell 的 Bid 卖
        - 平仓: 在 exchange_sell 的 Ask 买回，在 exchange_buy 的 Bid 卖出
        
        Args:
            opening_spread: 开仓方向的价差数据（用于确定平仓方向）
            orderbooks: 当前订单簿字典 {exchange: OrderBookData}
            
        Returns:
            平仓视角的价差数据（使用当前真实盘口价格），如果无法计算则返回None
            
        Example:
            # 开仓：lighter买(100) → edgex卖(100)
            opening_spread = SpreadData(exchange_buy="lighter", exchange_sell="edgex", ...)
            
            # 平仓：edgex买(当前Ask) → lighter卖(当前Bid)
            closing_spread = build_closing_spread_from_orderbooks(opening_spread, current_orderbooks)
            # 返回：SpreadData(exchange_buy="edgex", exchange_sell="lighter", 
            #                   price_buy=当前edgex的Ask, price_sell=当前lighter的Bid)
        """
        if not opening_spread.exchange_buy or not opening_spread.exchange_sell:
            return None

        def _lookup(exchange: str) -> Optional[OrderBookData]:
            if not exchange:
                return None
            return (
                orderbooks.get(exchange)
                or orderbooks.get(exchange.lower())
                or orderbooks.get(exchange.upper())
            )

        buy_exchange = opening_spread.exchange_sell
        sell_exchange = opening_spread.exchange_buy
        buy_orderbook = _lookup(buy_exchange)
        sell_orderbook = _lookup(sell_exchange)

        if not buy_orderbook or not sell_orderbook:
            return None
        if not self._validate_orderbook(buy_orderbook) or not self._validate_orderbook(sell_orderbook):
            return None

        buy_level = buy_orderbook.best_ask
        sell_level = sell_orderbook.best_bid
        if not buy_level or not sell_level:
            return None

        buy_price = buy_level.price
        sell_price = sell_level.price
        buy_size = buy_level.size
        sell_size = sell_level.size
        spread_abs = sell_price - buy_price
        spread_pct = float((spread_abs / buy_price) * 100)

        return SpreadData(
            symbol=opening_spread.symbol,
            exchange_buy=buy_exchange,
            exchange_sell=sell_exchange,
            price_buy=buy_price,
            price_sell=sell_price,
            size_buy=buy_size,
            size_sell=sell_size,
            spread_abs=spread_abs,
            spread_pct=spread_pct,
            buy_symbol=opening_spread.sell_symbol or opening_spread.symbol,
            sell_symbol=opening_spread.buy_symbol or opening_spread.symbol,
        )

    def calculate_spreads_multi_exchange(
        self,
        symbol: str,
        orderbooks: Dict[str, OrderBookData]
    ) -> Optional[SpreadData]:
        """
        计算多个交易所的价差（返回最优正价差）
        
        ✅ 活跃方法：被旧版调度器使用（兼容性接口）
        
        使用场景：
        - 可能被外部工具或测试代码使用
        - 作为"返回单个最优价差"的便捷接口
        
        核心作用：
        - 内部调用 calculate_spreads_multi_exchange_directions()
        - 从所有方向中筛选出最优的正价差
        - 返回单个最佳价差或 None
        
        与 calculate_spreads_multi_exchange_directions() 的关系：
        - 本方法是筛选版本（只返回最优正价差）
        - directions 版本是完整版本（返回所有方向）
        
        实现逻辑：
        1. 调用 calculate_spreads_multi_exchange_directions() 获取所有方向
        2. 筛选出所有正价差（spread_pct > 0）
        3. 返回价差最大的那个
        4. 如果没有正价差，返回 None
        
        Args:
            symbol: 交易对
            orderbooks: {exchange: orderbook}
        
        Returns:
            最优正价差数据，如果无利可图则返回None
            
        Example:
            best_spread = calculate_spreads_multi_exchange("BTC-USDC-PERP", orderbooks)
            if best_spread:
                print(f"最优套利: {best_spread.exchange_buy}买→{best_spread.exchange_sell}卖")
                print(f"价差: {best_spread.spread_pct:.4f}%")
        """
        spreads = self.calculate_spreads_multi_exchange_directions(symbol, orderbooks)

        if not spreads:
            return None

        best_positive: Optional[SpreadData] = None
        for spread in spreads:
            if spread.spread_pct > 0 and (
                best_positive is None or spread.spread_pct > best_positive.spread_pct
            ):
                best_positive = spread

        if best_positive:
            self._log_status(
                f"{symbol}:status:multi_positive",
                (
                    f"[价差状态] {symbol}: 最优套利 {best_positive.exchange_buy}买→"
                    f"{best_positive.exchange_sell}卖 差价 +{best_positive.spread_pct:.4f}% "
                    f"(买价 {best_positive.price_buy:.4f}, 卖价 {best_positive.price_sell:.4f})"
                ),
            )
            return best_positive

        summary = "；".join(
            f"{spread.exchange_buy}买→{spread.exchange_sell}卖={spread.spread_pct:.4f}%"
            for spread in spreads
        )
        message = (
            f"[价差计算] {symbol}: 锁定交易所的两个方向价差均≤0（{summary}）"
        )
        self._log_warning(f"{symbol}:non_positive_spread", message)
        self._log_status(
            f"{symbol}:status:non_positive_multi",
            f"[价差状态] {symbol}: {message}"
        )
        return None

    def calculate_multi_leg_closing_spread(
        self,
        pair_id: str,
        leg_primary_exchange: str,
        leg_primary_symbol: str,
        leg_secondary_exchange: str,
        leg_secondary_symbol: str,
        orderbooks: Dict[Tuple[str, str], OrderBookData],
        opening_direction: SpreadData,
    ) -> Optional[SpreadData]:
        """
        计算多腿套利的平仓价差（基于当前实际盘口）
        
        ✅ 活跃方法：被 unified_orchestrator.py (统一网格调度器) 使用
        
        使用场景：
        - spread_pipeline.process_multi_leg_pairs() - 多腿套利场景
        
        核心作用：
        - 根据开仓方向，确定平仓方向（反向交易）
        - 从当前订单簿重新获取两条腿的真实平仓价格
        - 支持同交易所不同代币、跨交易所不同代币
        
        多腿套利说明：
        - leg1: 第一条腿（如 PAXG）
        - leg2: 第二条腿（如 XAU）
        - 可以是同交易所不同代币（lighter: PAXG vs XAU）
        - 也可以是跨交易所不同代币（edgex: PAXG vs lighter: XAU）
        
        逻辑说明：
        - 如果开仓: 买 leg1 / 卖 leg2 → 平仓: 卖 leg1 / 买 leg2
        - 如果开仓: 买 leg2 / 卖 leg1 → 平仓: 卖 leg2 / 买 leg1
        
        为什么需要重新计算：
        - 与 build_closing_spread_from_orderbooks() 原因相同
        - 市场价格实时变化，必须使用当前盘口价格
        - 不能用开仓时的价格，不能用字段互换
        
        Args:
            pair_id: 组合ID（如 "LIGHTER_LIGHTER_PAXG_XAU"）
            leg_primary_exchange: 第一腿交易所
            leg_primary_symbol: 第一腿交易对
            leg_secondary_exchange: 第二腿交易所
            leg_secondary_symbol: 第二腿交易对
            orderbooks: {(exchange, symbol): OrderBookData}
            opening_direction: 开仓方向的价差数据
            
        Returns:
            平仓视角的价差数据（使用当前真实盘口价格），如果无法计算则返回None
            
        Example:
            # 开仓：买PAXG，卖XAU
            opening = SpreadData(buy_symbol="PAXG-USDC-PERP", sell_symbol="XAU-USDC-PERP", ...)
            
            # 平仓：卖PAXG，买XAU（使用当前盘口价格）
            closing = calculate_multi_leg_closing_spread(
                pair_id="LIGHTER_LIGHTER_PAXG_XAU",
                leg_primary_exchange="lighter",
                leg_primary_symbol="PAXG-USDC-PERP",
                leg_secondary_exchange="lighter",
                leg_secondary_symbol="XAU-USDC-PERP",
                orderbooks=current_orderbooks,
                opening_direction=opening
            )
        """
        leg1_key = (leg_primary_exchange.lower(), leg_primary_symbol.upper())
        leg2_key = (leg_secondary_exchange.lower(), leg_secondary_symbol.upper())
        ob1 = orderbooks.get(leg1_key)
        ob2 = orderbooks.get(leg2_key)

        if not ob1 or not ob2:
            return None
        if not self._validate_orderbook(ob1) or not self._validate_orderbook(ob2):
            return None

        opening_buys_leg1 = (opening_direction.buy_symbol or "").upper() == leg_primary_symbol.upper()

        if opening_buys_leg1:
            # 开仓：买 leg1，卖 leg2 → 平仓：卖 leg1，买 leg2
            sell_price = ob1.best_bid.price if ob1.best_bid else None
            buy_price = ob2.best_ask.price if ob2.best_ask else None
            sell_size = ob1.best_bid.size if ob1.best_bid else None
            buy_size = ob2.best_ask.size if ob2.best_ask else None
            buy_exchange = leg_secondary_exchange
            sell_exchange = leg_primary_exchange
            buy_symbol = leg_secondary_symbol
            sell_symbol = leg_primary_symbol
        else:
            # 开仓：买 leg2，卖 leg1 → 平仓：卖 leg2，买 leg1
            sell_price = ob2.best_bid.price if ob2.best_bid else None
            buy_price = ob1.best_ask.price if ob1.best_ask else None
            sell_size = ob2.best_bid.size if ob2.best_bid else None
            buy_size = ob1.best_ask.size if ob1.best_ask else None
            buy_exchange = leg_primary_exchange
            sell_exchange = leg_secondary_exchange
            buy_symbol = leg_primary_symbol
            sell_symbol = leg_secondary_symbol

        if buy_price is None or sell_price is None:
            return None

        spread_abs = sell_price - buy_price
        spread_pct = float((spread_abs / buy_price) * 100)

        return SpreadData(
            symbol=pair_id,
            exchange_buy=buy_exchange,
            exchange_sell=sell_exchange,
            price_buy=buy_price,
            price_sell=sell_price,
            size_buy=buy_size,
            size_sell=sell_size,
            spread_abs=spread_abs,
            spread_pct=spread_pct,
            buy_symbol=buy_symbol,
            sell_symbol=sell_symbol,
        )

    def _log_warning(self, key: str, message: str) -> None:
        """
        以分钟粒度限频打印警告，避免日志刷屏

        Args:
            key: 去重用的键（通常包含symbol+原因）
            message: 实际输出的日志内容
        """
        now = time.time()
        last = self._warning_log_times.get(key)
        if last is None or (now - last) >= self._warning_log_interval:
            logger.warning(message)
            self._warning_log_times[key] = now

    def _log_spread_status(self, symbol: str, spreads: List[SpreadData]) -> None:
        """
        打印价差状态，默认每个symbol每分钟一次
        """
        if not spreads:
            self._log_status(
                f"{symbol}:status:no_data",
                f"[价差状态] {symbol}: 无法计算价差（没有有效的订单簿组合）"
            )
            return

        best_overall = None
        best_positive = None
        for spread in spreads:
            if best_overall is None or spread.spread_pct > best_overall.spread_pct:
                best_overall = spread
            if spread.spread_pct > 0 and (best_positive is None or spread.spread_pct > best_positive.spread_pct):
                best_positive = spread

        if best_positive:
            self._log_status(
                f"{symbol}:status:positive",
                (f"[价差状态] {symbol}: 最优套利 {best_positive.exchange_buy}买→"
                 f"{best_positive.exchange_sell}卖 差价 +{best_positive.spread_pct:.4f}% "
                 f"(买价 {best_positive.price_buy:.4f}, 卖价 {best_positive.price_sell:.4f})")
            )
        else:
            self._log_status(
                f"{symbol}:status:non_positive_overview",
                (f"[价差状态] {symbol}: 全部价差≤0，最大差价 "
                 f"{best_overall.exchange_buy}买→{best_overall.exchange_sell}卖 = "
                 f"{best_overall.spread_pct:.4f}%，买价 {best_overall.price_buy:.4f} / "
                 f"卖价 {best_overall.price_sell:.4f}")
            )

    def _log_status(self, key: str, message: str) -> None:
        """
        状态信息按symbol节流打印
        """
        now = time.time()
        last = self._status_log_times.get(key)
        if last is None or (now - last) >= self._status_log_interval:
            logger.info(message)
            self._status_log_times[key] = now
    
    def calculate_multi_leg_spread(
        self,
        pair_id: str,
        leg_primary_exchange: str,
        leg_primary_symbol: str,
        leg_secondary_exchange: str,
        leg_secondary_symbol: str,
        orderbooks: Dict[Tuple[str, str], OrderBookData],
        allow_reverse: bool = True
    ) -> List[SpreadData]:
        """
        计算多腿套利组合的价差（开仓方向）
        
        ✅ 活跃方法：被 unified_orchestrator.py (统一网格调度器) 使用
        
        使用场景：
        - spread_pipeline.process_multi_leg_pairs() - 多腿套利场景
        
        核心作用：
        - 计算两条腿之间的价差（买leg1卖leg2 & 买leg2卖leg1）
        - 返回1-2个方向的价差数据
        - 用于开仓决策（选择最优方向）
        
        多腿套利说明：
        - 同交易所不同代币：如 lighter: PAXG vs XAU
        - 跨交易所不同代币：如 edgex: PAXG vs lighter: XAU
        
        计算逻辑：
        - 方向1: 买leg1，卖leg2 → 价差 = (leg2的Bid - leg1的Ask) / leg1的Ask
        - 方向2: 买leg2，卖leg1 → 价差 = (leg1的Bid - leg2的Ask) / leg2的Ask
        
        与平仓的关系：
        - 本方法用于开仓决策
        - 平仓使用 calculate_multi_leg_closing_spread()（重新获取当前盘口）
        
        Args:
            pair_id: 组合ID（如 "LIGHTER_LIGHTER_PAXG_XAU"）
            leg_primary_exchange: 第一腿交易所
            leg_primary_symbol: 第一腿交易对
            leg_secondary_exchange: 第二腿交易所
            leg_secondary_symbol: 第二腿交易对
            orderbooks: {(exchange, symbol): OrderBookData}
            allow_reverse: 是否允许反向套利（False时只返回方向1）
            
        Returns:
            价差数据列表（allow_reverse=True时返回2个方向，False时返回1个方向）
            
        Example:
            spreads = calculate_multi_leg_spread(
                pair_id="LIGHTER_LIGHTER_PAXG_XAU",
                leg_primary_exchange="lighter",
                leg_primary_symbol="PAXG-USDC-PERP",
                leg_secondary_exchange="lighter",
                leg_secondary_symbol="XAU-USDC-PERP",
                orderbooks=orderbooks,
                allow_reverse=True
            )
            # 返回：
            # [
            #   SpreadData(买PAXG卖XAU, spread_pct=+0.05%),
            #   SpreadData(买XAU卖PAXG, spread_pct=-0.05%)
            # ]
        """
        spreads = []
        
        # 获取两个腿的订单簿
        leg1_key = (leg_primary_exchange.lower(), leg_primary_symbol.upper())
        leg2_key = (leg_secondary_exchange.lower(), leg_secondary_symbol.upper())
        
        ob1 = orderbooks.get(leg1_key)
        ob2 = orderbooks.get(leg2_key)
        
        if not ob1 or not ob2:
            missing = []
            if not ob1:
                missing.append(f"{leg1_key[0]}/{leg1_key[1]}")
            if not ob2:
                missing.append(f"{leg2_key[0]}/{leg2_key[1]}")
            
            self._log_warning(
                f"multi_leg:{pair_id}:missing_orderbook",
                f"⚠️  [多腿套利] {pair_id}: 缺少订单簿数据: {', '.join(missing)}"
            )
            return spreads
        
        # 验证数据完整性
        if not self._validate_orderbook(ob1) or not self._validate_orderbook(ob2):
            self._log_warning(
                f"multi_leg:{pair_id}:invalid_orderbook",
                f"⚠️  [多腿套利] {pair_id}: 订单簿数据不完整"
            )
            return spreads
        
        # 🔥 方向1: 买入leg1，卖出leg2
        # 计算价差：(leg2的Bid - leg1的Ask) / leg1的Ask * 100
        spread_abs_1 = ob2.best_bid.price - ob1.best_ask.price
        spread_pct_1 = float((spread_abs_1 / ob1.best_ask.price) * 100)
        
        spreads.append(SpreadData(
            symbol=pair_id,  # 使用组合ID作为symbol
            exchange_buy=leg_primary_exchange,
            exchange_sell=leg_secondary_exchange,
            price_buy=ob1.best_ask.price,
            price_sell=ob2.best_bid.price,
            size_buy=ob1.best_ask.size,
            size_sell=ob2.best_bid.size,
            spread_abs=spread_abs_1,
            spread_pct=spread_pct_1,
            buy_symbol=leg_primary_symbol,
            sell_symbol=leg_secondary_symbol
        ))
        
        # 🔥 方向2: 买入leg2，卖出leg1（如果允许反向）
        if allow_reverse:
            spread_abs_2 = ob1.best_bid.price - ob2.best_ask.price
            spread_pct_2 = float((spread_abs_2 / ob2.best_ask.price) * 100)
            
            spreads.append(SpreadData(
                symbol=pair_id,
                exchange_buy=leg_secondary_exchange,
                exchange_sell=leg_primary_exchange,
                price_buy=ob2.best_ask.price,
                price_sell=ob1.best_bid.price,
                size_buy=ob2.best_ask.size,
                size_sell=ob1.best_bid.size,
                spread_abs=spread_abs_2,
                spread_pct=spread_pct_2,
                buy_symbol=leg_secondary_symbol,
                sell_symbol=leg_primary_symbol
            ))
        
        # Debug输出
        self._calc_counter += 1
        if self.debug.show_spread_calc and self.debug.should_show_spread_calc(self._calc_counter):
            for s in spreads:
                if s.spread_pct > 0:
                    logger.info(
                        f"💰 [多腿套利] {pair_id}: "
                        f"{s.exchange_buy}买@{s.price_buy:.2f} → "
                        f"{s.exchange_sell}卖@{s.price_sell:.2f} | "
                        f"差价=+{s.spread_pct:.3f}%"
                    )
        
        # 🔥 验证返回的价差数量
        if len(spreads) < 2 and allow_reverse:
            logger.warning(
                f"⚠️ [多腿套利] {pair_id}: allow_reverse=True 但只返回了{len(spreads)}个方向的价差"
            )
        
        return spreads

