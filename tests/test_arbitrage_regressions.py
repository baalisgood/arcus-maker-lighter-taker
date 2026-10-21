"""Offline regression checks: real engines, fake history/execution only."""
import asyncio
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, patch
from types import SimpleNamespace
import pytest
import yaml
from core.services.arbitrage_monitor_v2.analysis.spread_calculator import SpreadData
from core.services.arbitrage_monitor_v2.config.arbitrage_config import DecisionConfig, ExchangeFeeConfig, ExchangeOrderModeConfig
from core.services.arbitrage_monitor_v2.config.symbol_config import SegmentedConfigManager
from core.services.arbitrage_monitor_v2.decision.arbitrage_decision import ArbitrageDecisionEngine
from core.services.arbitrage_monitor_v2.decision.unified_decision_engine import UnifiedDecisionEngine
from core.services.arbitrage_monitor_v2.models import FundingRateData, ClosePositionContext

ROOT = Path(__file__).resolve().parents[1]
FEE = {"default": ExchangeFeeConfig(.0001, .0002)}
def run(coro): return asyncio.run(coro)
def spread(pct=.2, reverse=False):
    return SpreadData("BTC", "b" if reverse else "a", "a" if reverse else "b", Decimal("100"), Decimal(str(100+pct)), Decimal("10"), Decimal("10"), Decimal(str(pct)), pct)
def basic(natural=0):
    history = SimpleNamespace(get_natural_spread=AsyncMock(return_value=(natural, datetime.now())), is_funding_rate_stable=AsyncMock(return_value=True), is_spread_stable=AsyncMock(return_value=True))
    cfg=DecisionConfig(); cfg.thresholds.spread_arbitrage_threshold=.1; cfg.thresholds.spread_persistence_seconds=1
    cfg.thresholds.profit_target_threshold=.1; cfg.thresholds.loss_stop_threshold=.3
    return ArbitrageDecisionEngine(cfg, history, exchange_fee_config=FEE)
def fund(favorable=True, annual=20, diff=.02): return FundingRateData("a", "b", .01, .03, diff, annual, favorable)
def open_basic(e): run(e.record_open_position("BTC",spread(.5),None,"spread","condition1",Decimal("1")))
def segmented(deduct=True):
    manager=SegmentedConfigManager(ROOT / "config/arbitrage/arbitrage_taker_taker.yaml")
    cfg=manager.get_config("BTC"); cfg.grid_config.initial_spread_threshold=.1; cfg.grid_config.grid_step=.1; cfg.grid_config.spread_persistence_seconds=1; cfg.grid_config.scalping_enabled=False
    cfg.quantity_config.base_quantity=Decimal("1"); cfg.quantity_config.quantity_mode="fixed"; cfg.grid_config.split_order_size=1
    return UnifiedDecisionEngine(manager, deduct_fees=deduct, exchange_fee_config=FEE)

@pytest.mark.parametrize("name", ["arbitrage_unified", "arbitrage_segmented", "arbitrage_segmented_kuasuo", "arbitrage_segmented_duotui"])
def test_default_monitor_only(name):
    assert yaml.safe_load((ROOT / f"config/arbitrage/{name}.yaml").read_text())["system_mode"]["monitor_only"] is True

def test_round_trip_four_orders_and_symbol_modes():
    e=basic(); e.exchange_order_modes={"a:BTC":ExchangeOrderModeConfig(order_mode="limit")}
    assert sum(e._calculate_fee_breakdown(spread())) == pytest.approx(.06)
    assert run(e._check_spread_arbitrage_conditions("BTC",spread(.15),None))[0] is False
    assert run(e._check_spread_arbitrage_conditions("BTC",spread(.17),None))[0] is True

def test_natural_spread_added_to_cost():
    e=basic(.05)
    assert run(e._check_spread_arbitrage_conditions("BTC",spread(.21),None))[0] is False
    assert run(e._check_spread_arbitrage_conditions("BTC",spread(.24),None))[0] is True

@pytest.mark.parametrize("favorable,annual,diff,expected", [(True,100,.02,True),(False,10,.02,False),(False,9.9,.02,True),(True,0,0,True)])
def test_funding_direction_and_boundary(favorable,annual,diff,expected):
    e=basic(); e.thresholds.funding_rate_annual_threshold=10
    assert run(e._check_spread_arbitrage_conditions("BTC",spread(.3),fund(favorable,annual,diff)))[0] is expected

def test_profitable_funding_never_starts_unfavorable_timer_and_recovers():
    e=basic(); open_basic(e); p=e.positions["BTC"]
    assert e._check_funding_rate_unfavorable(p,fund(False)) is False
    assert "BTC" in e.funding_rate_unfavorable_duration
    assert e._check_funding_rate_unfavorable(p,fund(True,1000)) is False
    assert "BTC" not in e.funding_rate_unfavorable_duration

def test_take_profit_net_stop_loss_gross():
    e=basic(); open_basic(e)
    c=ClosePositionContext(100,100,.15,0,.15,0)
    assert e.should_close_position("BTC",spread(),None,c)[0] is False
    c.total_profit_pct=.2
    assert e.should_close_position("BTC",spread(),None,c)==(True,"profit_target_reached")
    c.total_profit_pct=-.25
    assert e.should_close_position("BTC",spread(),None,c)[0] is False
    c.total_profit_pct=-.31
    assert e.should_close_position("BTC",spread(),None,c)==(True,"spread_loss_stop_loss")

def test_fallback_profit_convergence_and_wrong_pair():
    e=basic(); open_basic(e)
    assert e._calculate_profit_percentage(e.positions["BTC"],spread(.3))==pytest.approx(.2)
    wrong=spread(.1); wrong.exchange_sell="c"
    assert e.should_close_position("BTC",wrong,None)[0] is False

def test_funding_branch_round_trip_loss_boundary():
    e=basic(); e.thresholds.loss_stop_threshold=.1
    assert run(e._check_funding_rate_arbitrage_conditions("BTC",spread(-.04),fund()))[0] is False

def test_segmented_cost_toggle_raw_record_no_immediate_close_and_convergence():
    e=segmented(); raw=spread(.15)
    assert run(e.should_open("BTC",raw))[0] is False
    free=segmented(False)
    assert run(free.should_open("BTC",raw))[0] is True
    raw=spread(.19)
    assert run(e.should_open("BTC",raw))[0] is True
    run(e.record_open("BTC",Decimal("1"),raw,None))
    assert e.positions["BTC"].avg_open_spread_pct==.19
    assert raw.spread_pct==.19
    assert run(e.should_close("BTC",spread(-.19,True)))[0] is False
    assert run(e.should_close("BTC",spread(0,True)))[0] is True

def test_source_scalping_reverse_signal_profit():
    e=segmented(); run(e.record_open("BTC",Decimal("1"),spread(.3),None))
    cfg=e.config_manager.get_config("BTC"); cfg.grid_config.scalping_profit_threshold=.1
    e.scalping_active["BTC"]=True
    assert run(e.should_close("BTC",spread(.2,True)))[0] is False
    assert run(e.should_close("BTC",spread(-.05,True)))[0] is True


def test_real_orchestrator_injects_shared_modes_and_explicit_zero_fee(tmp_path):
    from core.services.arbitrage_monitor_v2.core.unified_orchestrator import UnifiedOrchestrator
    from core.services.arbitrage_monitor_v2.core.orchestrator_bootstrap import OrchestratorBootstrap
    from core.services.arbitrage_monitor_v2.config.arbitrage_config import ExecutionConfig
    execution = ExecutionConfig(exchange_fee_config={"default": ExchangeFeeConfig(.001, .002), "a": ExchangeFeeConfig(.003,.004)}, exchange_order_modes={"a:BTC":ExchangeOrderModeConfig(order_mode="limit")})
    raw=yaml.safe_load((ROOT / "config/arbitrage/arbitrage_segmented.yaml").read_text())
    raw["decision"]={"deduct_fees":False,"exchange_fee_config":{"default":{"limit_fee_rate":0,"market_fee_rate":0}}}
    cfg_path=tmp_path / "segmented.yaml"; cfg_path.write_text(yaml.safe_dump(raw))
    with patch.object(OrchestratorBootstrap, "init_exchange_adapters"), patch.object(UnifiedOrchestrator,"_load_execution_module_config",return_value=execution) as load:
        orchestrator=UnifiedOrchestrator(segmented_config_path=cfg_path)
    assert load.call_count==1
    assert orchestrator.executor.config is execution
    assert orchestrator.decision_engine.deduct_fees is False
    assert orchestrator.decision_engine.exchange_order_modes==execution.exchange_order_modes
    assert orchestrator.decision_engine.exchange_fee_config["default"].market_fee_rate==0
    assert orchestrator.decision_engine.exchange_fee_config["a"].market_fee_rate==.004


@pytest.mark.parametrize("name", ["arbitrage_unified", "arbitrage_segmented", "arbitrage_segmented_kuasuo", "arbitrage_segmented_duotui"])
def test_monitor_executor_never_calls_fake_adapter_and_explicit_live_routes(name):
    from core.services.arbitrage_monitor_v2.execution.arbitrage_executor import ArbitrageExecutor, ExecutionRequest, ExecutionResult
    from core.services.arbitrage_monitor_v2.config.arbitrage_config import ExecutionConfig
    raw=yaml.safe_load((ROOT / f"config/arbitrage/{name}.yaml").read_text())
    adapters={"a":SimpleNamespace(create_order=AsyncMock()), "b":SimpleNamespace(create_order=AsyncMock())}
    executor=ArbitrageExecutor(ExecutionConfig(),adapters,monitor_only=raw["system_mode"]["monitor_only"],is_segmented_mode=True)
    request=ExecutionRequest("BTC","a","b",Decimal("100"),Decimal("101"),Decimal("1"))
    async def fake_plan(*args):
        await adapters["a"].create_order(); await adapters["b"].create_order()
        from core.adapters.exchanges.models import OrderData, OrderSide, OrderType, OrderStatus
        def fill(identity, side, price):
            return OrderData(identity,identity,"BTC",side,OrderType.MARKET,Decimal(1),None,Decimal(1),Decimal(0),price,price,OrderStatus.FILLED,datetime.now(),datetime.now(),None,[],{}, {})
        return ExecutionResult(success=True,order_buy=fill("a1",OrderSide.BUY,Decimal(100)),order_sell=fill("b1",OrderSide.SELL,Decimal(101)))
    with patch.object(executor,"_run_execution_plan",side_effect=fake_plan):
        assert run(executor.execute_arbitrage(request)).success
        assert sum(a.create_order.await_count for a in adapters.values())==0
        executor.monitor_only=False
        assert run(executor.execute_arbitrage(request)).success
        assert sum(a.create_order.await_count for a in adapters.values())==2


@pytest.mark.parametrize("quantity,success", [(Decimal("1"),True),(Decimal(".4"),True),(Decimal("0"),False)])
def test_v3_actual_close_full_partial_failure_and_repeat(quantity,success):
    from core.services.arbitrage_monitor_v2.core.arbitrage_orchestrator_v3 import ArbitrageOrchestratorV3
    from core.services.arbitrage_monitor_v2.execution.arbitrage_executor import ExecutionResult
    o=ArbitrageOrchestratorV3.__new__(ArbitrageOrchestratorV3)
    o.decision_engine=basic(); open_basic(o.decision_engine)
    o._manual_close_blocklist=set(); o._manual_close_notified=set()
    o.spread_calculator=SimpleNamespace(calculate_spreads=lambda *a:[spread(.1)])
    o._get_funding_rate_data=AsyncMock(return_value=None)
    o._build_close_position_context=lambda *a:ClosePositionContext(100,100,.3,0,.3,0)
    o.executor=SimpleNamespace(calculate_order_quantity=lambda *a:(True,Decimal("1"),""),execute_arbitrage=AsyncMock(return_value=ExecutionResult(success=success,error_message=None if success else "offline rejection")))
    o._get_effective_prices=lambda *a:{"price_buy":100,"price_sell":100,"quantity":float(quantity)}
    o._add_execution_record=lambda *a:None; o._log_trade_event=lambda *a,**k:None
    run(o._handle_close_position("BTC",{}))
    p=o.decision_engine.get_position("BTC")
    if quantity==1:
        assert p is None or not p.is_open
        assert "BTC" not in o._manual_close_blocklist
    else:
        assert p.quantity==1-quantity
        assert p.quantity_buy==1-quantity and p.quantity_sell==1-quantity
        assert p.is_open and "BTC" in o._manual_close_blocklist
        run(o._handle_close_position("BTC",{}))
        assert o.executor.execute_arbitrage.await_count==1


def test_segmented_net_persistence_and_grid_target():
    e=segmented(); cfg=e.config_manager.get_config("BTC"); cfg.grid_config.spread_persistence_seconds=2; cfg.grid_config.strict_persistence_check=False
    with patch("core.services.arbitrage_monitor_v2.decision.unified_decision_engine.time.time", return_value=100):
        assert run(e.should_open("BTC",spread(.15)))[0] is False
        assert run(e.should_open("BTC",spread(.35)))[0] is False
    with patch("core.services.arbitrage_monitor_v2.decision.unified_decision_engine.time.time", return_value=101):
        should_open,quantity=run(e.should_open("BTC",spread(.35)))
        assert should_open and quantity==Decimal("1")
        assert e._calculate_current_grid("BTC",.35-.08)==2
        assert e._calculate_current_grid("BTC",.35)==3


def test_legacy_reverse_modes_and_neutral_funding_timer():
    e=basic(); e.exchange_order_modes={"a":SimpleNamespace(order_mode=None,buy_mode="limit",sell_mode="market")}
    assert sum(e._calculate_fee_breakdown(spread()))==pytest.approx(.07)
    open_basic(e)
    e.funding_rate_unfavorable_duration["BTC"]=datetime.now()-timedelta(hours=2)
    assert e._check_funding_rate_unfavorable(e.positions["BTC"],fund(False,0,0)) is False
    assert "BTC" not in e.funding_rate_unfavorable_duration


@pytest.mark.parametrize("natural,last_update", [(None,None),(0,datetime.now()-timedelta(seconds=201))])
def test_history_missing_and_stale_refused(natural,last_update):
    e=basic(); e.history_calculator.get_natural_spread.return_value=(natural,last_update)
    assert run(e._check_spread_arbitrage_conditions("BTC",spread(.4),None))[0] is False
    assert run(e._check_spread_arbitrage_conditions("BTC",None,None))[0] is False


def test_funding_adverse_duration_recovery_and_close():
    e=basic(); open_basic(e); p=e.positions["BTC"]
    e.funding_rate_unfavorable_duration["BTC"]=datetime.now()-timedelta(minutes=61)
    assert e._check_funding_rate_unfavorable(p,fund(False)) is True
    assert e._check_funding_rate_unfavorable(p,fund(True)) is False
    assert e._check_funding_rate_unfavorable(p,fund(False)) is False


@pytest.mark.parametrize("quotes,expected_best,expected_reason", [
    ({"a":("100.9","101"),"b":("99.9","100")}, ("b","a"), "profit_target_reached"),
    ({"a":("99","99.1"),"b":("101","101.1"),"c":("98","98.1")}, ("c","b"), "spread_loss_stop_loss"),
    ({"a":("100","100.1"),"b":("100","100.1")}, None, "funding_rate_unfavorable_stop_loss"),
    ({"a":("100.2","100.21"),"b":("100.2","100.21")}, None, "profit_target_reached"),
])
def test_v3_real_market_close_held_pair_not_global_best(quotes,expected_best,expected_reason):
    from collections import deque
    from core.adapters.exchanges.models import OrderBookData, OrderBookLevel
    from core.services.arbitrage_monitor_v2.analysis.spread_calculator import SpreadCalculator
    from core.services.arbitrage_monitor_v2.config.debug_config import DebugConfig
    from core.services.arbitrage_monitor_v2.core.arbitrage_orchestrator_v3 import ArbitrageOrchestratorV3
    from core.services.arbitrage_monitor_v2.execution.arbitrage_executor import ExecutionResult
    books={exchange:OrderBookData("BTC",[OrderBookLevel(bid,"10")],[OrderBookLevel(ask,"10")],datetime.now()) for exchange,(bid,ask) in quotes.items()}
    o=ArbitrageOrchestratorV3.__new__(ArbitrageOrchestratorV3)
    o.spread_calculator=SpreadCalculator(DebugConfig.create_production())
    best=o.spread_calculator.calculate_spreads_multi_exchange("BTC",books)
    assert ((best.exchange_buy,best.exchange_sell) if best else None)==expected_best
    o.decision_engine=basic(); open_basic(o.decision_engine)
    o.decision_engine.exchange_fee_config["c"]=ExchangeFeeConfig(.1,.1)
    if expected_reason=="funding_rate_unfavorable_stop_loss":
        o.decision_engine.thresholds.profit_target_threshold=100
        o.decision_engine.thresholds.loss_stop_threshold=100
        o.decision_engine.funding_rate_unfavorable_duration["BTC"]=datetime.now()-timedelta(minutes=61)
    # Supply ticker inputs; exercise the real funding calculation and close context.
    tickers={"a":SimpleNamespace(funding_rate=.0003),"b":SimpleNamespace(funding_rate=.0001)}
    o.data_processor=SimpleNamespace(get_ticker=lambda exchange,symbol:tickers.get(exchange))
    o._manual_close_blocklist=set(); o._manual_close_notified=set()
    o._execution_records_store=deque(maxlen=10); o._execution_record_queue=asyncio.Queue()
    o.executor=SimpleNamespace(calculate_order_quantity=lambda *a:(True,Decimal("1"),""),execute_arbitrage=AsyncMock(return_value=ExecutionResult(success=True)))
    run(o._handle_close_position("BTC",books))
    assert o.executor.execute_arbitrage.await_count==1
    request=o.executor.execute_arbitrage.call_args.args[0]
    assert (request.exchange_buy,request.exchange_sell)==("b","a")
    assert (request.spread_data.exchange_buy,request.spread_data.exchange_sell)==("a","b")
    assert sum(o.decision_engine._calculate_fee_breakdown(request.spread_data))==pytest.approx(.08)
    assert request.price_buy==Decimal(quotes["b"][1])
    assert request.price_sell==Decimal(quotes["a"][0])
    assert o._execution_records_store[-1]["close_reason"]==expected_reason
    assert not o.decision_engine.positions["BTC"].is_open


def test_segmented_net_snapshot_helper_respects_cost_toggle():
    raw=spread(.35)
    assert segmented()._calculate_net_spread(raw)==pytest.approx(.27)
    assert segmented(False)._calculate_net_spread(raw)==pytest.approx(.35)
    assert raw.spread_pct==.35
