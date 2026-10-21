"""Offline account/order trust boundaries and seven-pair open guards. No credentials or network."""
import asyncio
import json
from datetime import datetime, timedelta
from decimal import Decimal as D
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, Mock

import pytest
import yaml

from test_lighter_strict_state import rest, order, SYMBOL
from core.adapters.exchanges.adapters.lighter_websocket import LighterWebSocket
from core.adapters.exchanges.adapters.lighter import LighterAdapter
from core.adapters.exchanges.models import BalanceData, OrderStatus
from core.adapters.exchanges.order_state import OrderStateUnknownError
from core.services.arbitrage_monitor_v2.config.arbitrage_config import RiskControlConfig, QuantityConfig
from core.services.arbitrage_monitor_v2.config.unified_config_manager import UnifiedConfigManager
from core.services.arbitrage_monitor_v2.risk_control.global_risk_controller import GlobalRiskController
from core.services.arbitrage_monitor_v2.execution.arbitrage_executor import ArbitrageExecutor, ExecutionConfig, ExecutionRequest, ExecutionResult
from core.services.arbitrage_monitor_v2.core.arbitrage_orchestrator_v3 import ArbitrageOrchestratorV3


def run(coro): return asyncio.run(coro)


def raw(**changes):
    row = dict(client_order_index=41, order_index=1001, market_index=1, is_ask=False,
               status='filled', initial_base_amount='.0001', filled_base_amount='.0001',
               remaining_base_amount='0', filled_quote_amount='8.5')
    row.update(changes)
    return row


def registered():
    r = rest()
    r._register_order_intent('41', SYMBOL, 'buy', D('.0001'), 1)
    r._uncertain_orders['41'] = {'nonce':11}
    return r


def socket(r, callback=None):
    ws = object.__new__(LighterWebSocket)
    ws._strict_rest = r; ws.auth_enabled = True; ws.signer_client = object(); ws.account_index = 7
    ws._direct_ws = object(); ws._authenticated_account_socket = ws._direct_ws
    ws._ws_orders_log_all_pushes = True
    ws._log_order_push = Mock(); ws._adjust_position_cache_from_order = Mock()
    ws._order_callbacks = [callback] if callback else []; ws._order_fill_callbacks = []
    return ws


def frame(row, **changes):
    message = dict(type='update/account_all_orders', channel='account_all_orders:7', orders={'1':[row]})
    message.update(changes)
    return message


def test_real_authenticated_subscription_confirms_terminal_without_rest(monkeypatch):
    monkeypatch.setattr(asyncio, 'sleep', AsyncMock())
    r = registered(); ws = socket(r)
    del ws._authenticated_account_socket
    ws._direct_ws = NS(send=AsyncMock())
    r.get_open_orders = AsyncMock(side_effect=ValueError('HTTP 405 WAF'))
    r.get_order_history = AsyncMock(side_effect=ValueError('HTTP 405 WAF'))
    async def scenario():
        await ws._send_account_subscriptions('offline-test-auth')
        sent = [json.loads(call.args[0]) for call in ws._direct_ws.send.await_args_list]
        assert len(sent) == 4 and all(msg['auth'] == 'offline-test-auth' for msg in sent)
        assert ws._authenticated_account_socket is ws._direct_ws
        await ws._handle_direct_ws_message(frame(raw()))
        observed = await r.get_order('41', SYMBOL)
        assert observed.status is OrderStatus.FILLED and observed.filled == D('.0001')
    run(scenario())
    assert not r._uncertain_orders
    r.get_open_orders.assert_not_awaited(); r.get_order_history.assert_not_awaited()


@pytest.mark.parametrize('failure', ['empty_auth', 'order_subscriptions_failed', 'replaced_socket'])
def test_failed_or_unauthed_subscription_cannot_confirm_terminal(monkeypatch, failure):
    monkeypatch.setattr(asyncio, 'sleep', AsyncMock())
    r = registered(); ws = socket(r)
    del ws._authenticated_account_socket
    async def send(message):
        if failure == 'order_subscriptions_failed' and json.loads(message)['channel'].startswith(('account_all_orders/', 'account_all/')):
            raise ConnectionError('offline subscription failed')
        if failure == 'replaced_socket':
            ws._direct_ws = NS(send=AsyncMock())
    ws._direct_ws = NS(send=AsyncMock(side_effect=send))
    async def scenario():
        await ws._send_account_subscriptions('' if failure == 'empty_auth' else 'offline-test-auth')
        await ws._handle_direct_ws_message(frame(raw()))
    run(scenario())
    assert r._uncertain_orders and r._lookup_terminal_order('41', SYMBOL) is None


def test_public_market_subscription_never_marks_account_authenticated(monkeypatch):
    monkeypatch.setattr(asyncio, 'sleep', AsyncMock())
    ws = socket(registered()); del ws._authenticated_account_socket
    ws._direct_ws = NS(send=AsyncMock()); ws._subscribed_market_stats = {1}
    run(ws._send_market_stats_subscriptions())
    ws._direct_ws.send.assert_awaited_once()
    assert not hasattr(ws, '_authenticated_account_socket')


def test_ws_terminal_survives_callback_mutation_duplicates_regression_and_rest_405():
    r = registered(); calls = []
    def mutate(o):
        calls.append(o.id); o.filled = D('999'); o.status = OrderStatus.OPEN
        o.raw_data['filled_base_amount'] = '999'
    ws = socket(r, mutate)
    r.get_open_orders = AsyncMock(side_effect=ValueError('HTTP 405 WAF'))
    r.get_order_history = AsyncMock(side_effect=ValueError('HTTP 405 WAF'))
    async def scenario():
        await ws._handle_direct_ws_message(frame(raw()))
        await ws._handle_direct_ws_message(frame(raw()))
        await ws._handle_direct_ws_message(frame(raw(status='open')))
        await ws._handle_direct_ws_message(frame(raw(filled_base_amount='0',filled_quote_amount='0',status='canceled')))
        a = object.__new__(LighterAdapter); a._rest = r; a.strict_order_state = True
        a._normalize_symbol = lambda s:s
        for identity in ('41','1001'):
            observed = await a.get_order(identity, SYMBOL)
            assert observed.filled == D('.0001') and observed.status is OrderStatus.FILLED
            assert observed.raw_data['filled_base_amount'] == '.0001'
            observed.filled = D(88)
    run(scenario())
    assert calls == ['1001'] and not r._uncertain_orders
    r.get_open_orders.assert_not_awaited(); r.get_order_history.assert_not_awaited()
    ws._adjust_position_cache_from_order.assert_not_called()


@pytest.mark.parametrize('change', [dict(client_order_index=42),dict(order_index=None),dict(market_index=2),
    dict(is_ask=True),dict(is_ask='false'),dict(initial_base_amount='.0002'),dict(filled_base_amount='NaN'),
    dict(filled_base_amount='.00011'),dict(filled_base_amount='-.00001'),dict(remaining_base_amount='NaN'),
    dict(status='open'),dict(status='acknowledged'),dict(account_index=8),dict(filled_quote_amount='Infinity')])
def test_malformed_or_unmatched_terminal_never_clears_latch(change):
    r = registered(); ws = socket(r)
    run(ws._handle_direct_ws_message(frame(raw(**change))))
    assert r._uncertain_orders and r._lookup_terminal_order('41',SYMBOL) is None


def test_missing_fill_and_wrong_market_bucket_not_terminal():
    r = registered(); row = raw(); del row['filled_base_amount']
    assert r._accept_terminal_order(row,1) is None
    assert r._accept_terminal_order(raw(),2) is None
    assert r._uncertain_orders


@pytest.mark.parametrize('changes', [dict(channel='account_all_orders:8'),dict(account=8),dict(channel='order_book:7'),
    dict(channel='account_all:7'),dict(channel='account_all_orders:7/other')])
def test_wrong_private_account_channel_rejected(changes):
    r = registered(); ws = socket(r)
    run(ws._handle_direct_ws_message(frame(raw(), **changes)))
    assert r._uncertain_orders and r._lookup_terminal_order('41',SYMBOL) is None


def test_unsubscribed_socket_cannot_provide_terminal_evidence():
    r = registered(); ws = socket(r); ws._authenticated_account_socket = object()
    run(ws._handle_direct_ws_message(frame(raw())))
    assert r._uncertain_orders


@pytest.mark.parametrize('fill', ['0','.00004'])
def test_real_terminal_canceled_preserves_zero_or_partial(fill):
    r = registered()
    terminal = r._accept_terminal_order(raw(status='canceled',filled_base_amount=fill,
        filled_quote_amount=str(D(fill)*85000)),1)
    assert terminal.status is OrderStatus.CANCELED and terminal.filled == D(fill)
    assert not r._uncertain_orders


@pytest.mark.parametrize('fill,remaining', [('0','.0001'), ('.00004','.00006'), ('.0001','.00001')])
def test_contradictory_filled_status_is_not_authoritative(fill, remaining):
    r = registered()
    row = raw(filled_base_amount=fill, remaining_base_amount=remaining,
              filled_quote_amount=str(D(fill)*85000))
    assert r._accept_terminal_order(row, 1) is None and r._uncertain_orders
    with pytest.raises(OrderStateUnknownError):
        r._parse_order(NS(**row), SYMBOL)


def test_ws_before_ack_registers_intent_before_transport_and_does_not_restore_latch():
    r = rest(); ws = socket(r)
    async def send(*args):
        await ws._handle_direct_ws_message(frame(raw()))
        return {'tx_hash':['hash']}
    r._websocket = NS(send_tx_batch=AsyncMock(side_effect=send))
    r._prepare_reduce_only_quantity = Mock(return_value={'quantity':D('.0001'),'skip':False})
    r._build_signed_market_order_tx = AsyncMock(return_value=dict(tx_type=14,tx_info='offline',tx_hash='hash',
        api_key_index=2,nonce=11,context=dict(client_order_id=41,symbol=SYMBOL,side='buy',quantity=D('.0001'),avg_execution_price=D(85000))))
    ack = run(r.place_market_orders_via_ws_batch([dict(symbol=SYMBOL,side='buy',quantity=D('.0001'))]))
    assert ack['orders'][0].status is OrderStatus.PENDING
    assert not r._uncertain_orders and run(r.get_order('41',SYMBOL)).filled == D('.0001')
    r._websocket.send_tx_batch.assert_awaited_once()


def account(r):
    r._markets_cache = {1:{'symbol':SYMBOL}}
    return NS(code=200,accounts=[NS(index=7,available_balance='30',collateral='30',positions=[])])


def test_shared_account_snapshot_concurrent_get_bound_expiry_and_throttle(monkeypatch):
    import core.adapters.exchanges.adapters.lighter_rest as module
    now = [100.0]; monkeypatch.setattr(module.time,'monotonic',lambda:now[0])
    r = rest(); r.account_api = NS(account=AsyncMock(return_value=account(r)))
    async def scenario():
        await asyncio.gather(*(r.get_account_balance() if i%2 else r.get_positions() for i in range(30)))
        assert r.account_api.account.await_count == 1
        now[0] += 2.1
        await r.get_positions(); assert r.account_api.account.await_count == 2
        r._invalidate_account_snapshot(); r.account_api.account.side_effect = ValueError('HTTP 429')
        results = await asyncio.gather(*(r.get_positions() for _ in range(20)),return_exceptions=True)
        assert all(isinstance(result,OrderStateUnknownError) for result in results)
        assert r.account_api.account.await_count == 3 and r._account_snapshot is None
        now[0] += 29
        with pytest.raises(OrderStateUnknownError): await r.get_account_balance()
        assert r.account_api.account.await_count == 3
    run(scenario())


def test_order_rest_fallback_failure_is_bounded(monkeypatch):
    import core.adapters.exchanges.adapters.lighter_rest as module
    now=[100.0]; monkeypatch.setattr(module.time,'monotonic',lambda:now[0])
    r=rest(); r.get_open_orders=AsyncMock(side_effect=OrderStateUnknownError('lighter'))
    async def scenario():
        for _ in range(50):
            with pytest.raises(OrderStateUnknownError): await r.get_order('41',SYMBOL)
        assert r.get_open_orders.await_count == 1
        now[0] += 2.1
        with pytest.raises(OrderStateUnknownError): await r.get_order('41',SYMBOL)
        assert r.get_open_orders.await_count == 2
    run(scenario())


@pytest.mark.parametrize('status', [405, 429])
def test_real_wrapped_order_read_error_obeys_thirty_second_cooldown(monkeypatch, status):
    import core.adapters.exchanges.adapters.lighter_rest as module
    now = [100.0]; monkeypatch.setattr(module.time, 'monotonic', lambda: now[0])
    r = rest(); r.order_api = NS(account_active_orders=AsyncMock(side_effect=ValueError(f'HTTP {status} WAF')))
    async def scenario():
        for value in (100.0, 102.1, 129.9):
            now[0] = value
            with pytest.raises(OrderStateUnknownError): await r.get_order('41', SYMBOL)
        assert r.order_api.account_active_orders.await_count == 1
        now[0] = 130.1
        with pytest.raises(OrderStateUnknownError): await r.get_order('41', SYMBOL)
        assert r.order_api.account_active_orders.await_count == 2
    run(scenario())


def adapters():
    return {name:NS(get_positions=AsyncMock(return_value=[]),get_open_orders=AsyncMock(return_value=[]),
            get_balances=AsyncMock(return_value=[BalanceData('USDC',D(30),D(0),D(30),D(30),datetime.now(),{})]),
            create_order=AsyncMock(),cancel_order=AsyncMock()) for name in ('lighter','arcus')}


def risk(a=None):
    a=a or adapters(); cfg=RiskControlConfig(); pm=cfg.position_management
    pm.max_gross_notional_usd=36; pm.max_leg_notional_usd=18; pm.max_open_symbols=1
    pm.min_leg_notional_usd={'lighter':10,'arcus':5}
    cfg.balance_management.min_balance_close_position=20
    return GlobalRiskController(cfg,a,{SYMBOL:QuantityConfig(single_order_quantity=.0002,max_position_quantity=1,quantity_precision=5)})


def check(controller, symbol=SYMBOL, qty='.0002', prices=None, **kwargs):
    return controller.check_position_limits(symbol,'lighter',D(qty),exchange_sell='arcus',
        prices=prices or {'lighter':D(85000),'arcus':D(85000)},**kwargs)


def test_gross_usd_boundary_includes_two_legs_and_legacy_base_cap_not_mixed():
    c=risk()
    assert run(check(c,qty='.1',prices={'lighter':180,'arcus':180}))[0]
    assert not run(check(c,qty='.1',prices={'lighter':180.01,'arcus':180}))[0]
    assert not run(check(c,qty='.1',prices={'lighter':180,'arcus':180},slippage='.001'))[0]
    c.config.position_management.max_leg_notional_usd=100
    assert not run(check(c,qty='.1',prices={'lighter':180,'arcus':181}))[0]
    for a in c.exchange_adapters.values(): assert a.get_positions.await_count == 4


@pytest.mark.parametrize('price',[None,0,-1,'NaN','Infinity'])
def test_unknown_nonfinite_quote_denies_new_open(price):
    c=risk()
    assert not run(check(c,prices={'lighter':price,'arcus':85000}))[0]


def test_minimum_notional_rechecked_at_runtime():
    c=risk()
    assert not run(check(c,prices={'lighter':49999,'arcus':50000}))[0]
    assert not run(check(c,prices={'lighter':50000,'arcus':24999}))[0]


@pytest.mark.parametrize('failure',['error','missing','stale','malformed'])
def test_account_unavailable_denies_open_and_ui_state_exposes_uncertainty(failure):
    a=adapters(); c=risk(a)
    if failure=='error': a['arcus'].get_positions.side_effect=TimeoutError('offline')
    elif failure=='missing': a['arcus'].get_balances.return_value=[]
    elif failure=='stale': a['arcus'].get_positions.return_value=[NS(symbol=SYMBOL,size=D('.0001'),timestamp=datetime.now()-timedelta(seconds=6))]
    else: a['arcus'].get_positions.return_value=[NS(symbol=SYMBOL,size=D('NaN'),timestamp=datetime.now())]
    assert not run(check(c))[0] and 'arcus' in c.risk_status.account_data_unavailable
    for adapter in a.values(): adapter.create_order.assert_not_awaited(); adapter.cancel_order.assert_not_awaited()


def test_balance_success_does_not_hide_position_failure_and_recovers_after_both_reads():
    a = adapters(); c = risk(a)
    a['arcus'].get_positions.side_effect = TimeoutError('offline positions unavailable')
    async def scenario():
        assert not (await check(c))[0]
        await c._check_all_balances()
        assert c.risk_status.account_data_unavailable == {'arcus'}
        a['arcus'].get_positions.side_effect = None
        await c._check_all_balances()
        assert not c.risk_status.account_data_unavailable
    run(scenario())


@pytest.mark.parametrize('initial_age,second_read_seconds', [(0, 6), (4, 2), (0, 0)])
def test_slow_second_account_cannot_use_expired_first_snapshot(monkeypatch, initial_age, second_read_seconds):
    import core.services.arbitrage_monitor_v2.risk_control.global_risk_controller as module
    now = [datetime.now()]
    class Clock:
        @staticmethod
        def now(): return now[0]
    monkeypatch.setattr(module, 'datetime', Clock)
    a = adapters(); first_timestamp = now[0] - timedelta(seconds=initial_age)
    a['lighter'].get_balances.return_value[0].timestamp = first_timestamp
    async def slow_read():
        now[0] += timedelta(seconds=second_read_seconds)
        return []
    a['arcus'].get_positions.side_effect = slow_read
    async def fresh_balance():
        return [BalanceData('USDC', D(30), D(0), D(30), D(30), now[0], {})]
    a['arcus'].get_balances.side_effect = fresh_balance
    c = risk(a); allowed, reason = run(check(c))
    assert allowed == (initial_age + second_read_seconds <= 5)
    if not allowed: assert 'lighter' in c.risk_status.account_data_unavailable
    for adapter in a.values():
        adapter.get_positions.assert_awaited_once(); adapter.get_balances.assert_awaited_once()
        adapter.create_order.assert_not_awaited(); adapter.cancel_order.assert_not_awaited()


@pytest.mark.parametrize('state',['flat','position','open_order','unavailable','unknown'])
def test_read_only_startup_requires_empty_verified_accounts(state):
    a=adapters(); c=risk(a)
    if state=='position': a['lighter'].get_positions.return_value=[NS(symbol='AAPL-USDC-PERP',size=D('.01'),timestamp=datetime.now())]
    if state=='open_order': a['arcus'].get_open_orders.return_value=[order(OrderStatus.OPEN)]
    if state=='unavailable': a['arcus'].get_positions.side_effect=TimeoutError('offline')
    if state=='unknown': a['lighter']._uncertain={'41'}
    allowed, reason=run(c.check_startup_account_state())
    assert allowed == (state=='flat')
    if not allowed: assert '人工' in reason


@pytest.mark.parametrize('state', ['flat', 'position', 'unavailable'])
def test_real_start_connects_before_read_only_reconciliation(state):
    a = adapters(); connected = {name: False for name in a}; events = []
    for name, adapter in a.items():
        async def connect(name=name):
            connected[name] = True; events.append('connect:' + name)
        async def positions(name=name):
            if not connected[name] or (name == 'lighter' and state == 'unavailable'):
                raise OrderStateUnknownError('lighter')
            if name == 'lighter' and state == 'position':
                return [NS(symbol=SYMBOL, size=D('.0001'), timestamp=datetime.now())]
            return []
        adapter.connect = AsyncMock(side_effect=connect)
        adapter.get_positions = AsyncMock(side_effect=positions)
    o = object.__new__(ArbitrageOrchestratorV3)
    o.running = False; o._startup_open_block_reason = None
    o.history_recorder = None; o.history_calculator = None
    o.unified_config = NS(system_mode=NS(monitor_only=False))
    o.exchange_adapters = a; o.risk_controller = risk(a)
    gate = o.risk_controller.check_startup_account_state
    async def reconcile():
        assert all(connected.values()); events.append('reconcile')
        return await gate()
    o.risk_controller.check_startup_account_state = reconcile
    o.risk_controller.start = AsyncMock()
    o.data_receiver = NS(start=AsyncMock(), register_adapter=Mock(), subscribe_all=AsyncMock())
    o.data_processor = NS(start=AsyncMock())
    o.monitor_config = NS(symbols=[SYMBOL], exchanges=list(a), ui_refresh_interval_ms=1000)
    o.executor = NS(initialize_websocket_subscriptions=AsyncMock())
    o.ui_manager = NS(start=Mock(), set_v3_mode=Mock(), update_config=Mock(), update_loop=AsyncMock())
    o._ui_update_loop = AsyncMock()
    async def main_loop():
        assert 'reconcile' in events; events.append('main_loop')
    o._main_loop = main_loop
    async def scenario():
        await o.start()
        await asyncio.gather(o.ui_update_task, o.ui_data_update_task, o.main_loop_task)
    run(scenario())
    assert (o._startup_open_block_reason is None) == (state == 'flat')
    assert events.index('reconcile') < events.index('main_loop')
    for adapter in a.values():
        adapter.create_order.assert_not_awaited(); adapter.cancel_order.assert_not_awaited()
    for adapter in a.values(): adapter.create_order.assert_not_awaited(); adapter.cancel_order.assert_not_awaited()


def test_cross_symbol_local_projection_counts_even_if_exchange_still_reports_zero():
    c=risk(); local=NS(symbol='AAPL-USDC-PERP',is_open=True,exchange_buy='lighter',exchange_sell='arcus',quantity_buy=D('.036'),quantity_sell=D('.036'))
    assert not run(check(c,local_positions=[local]))[0]
    a=adapters(); a['lighter'].get_positions.return_value=[NS(symbol='AAPL-USDC-PERP',size=D('.036'),timestamp=datetime.now())]
    assert not run(check(risk(a)))[0]


def test_local_projection_and_rest_position_are_not_double_counted_and_unknown_price_rejected():
    c=risk(); pm=c.config.position_management; pm.max_open_symbols=2; pm.min_leg_notional_usd={}
    for a in c.exchange_adapters.values(): a.get_positions.return_value=[NS(symbol='ETH-USDC-PERP',size=D('.001'),timestamp=datetime.now())]
    local=NS(symbol='ETH-USDC-PERP',is_open=True,exchange_buy='lighter',exchange_sell='arcus',quantity_buy=D('.001'),quantity_sell=D('.001'))
    assert run(check(c,local_positions=[local],price_lookup=lambda e,s:1000))[0]  # 34+2 = 36, not 38.
    assert not run(check(c,local_positions=[local],price_lookup=lambda e,s:None))[0]


def test_unknown_execution_lock_blocks_different_symbol_after_terminal_adapter_recovers():
    a=adapters(); e=ArbitrageExecutor(ExecutionConfig(),a,monitor_only=False,is_segmented_mode=True)
    e._unknown_execution_pairs.add((SYMBOL,('arcus','lighter')))
    e._run_execution_plan=AsyncMock()
    request=ExecutionRequest(symbol='ETH-USDC-PERP',exchange_buy='lighter',exchange_sell='arcus',price_buy=D(2700),price_sell=D(2701),quantity=D('.0045'))
    result=run(e.execute_arbitrage(request))
    assert result.failure_code=='ORDER_STATE_UNKNOWN'
    e._run_execution_plan.assert_not_awaited()


def test_queued_orchestrator_open_rechecks_after_first_local_fill():
    o=object.__new__(ArbitrageOrchestratorV3); o._open_position_lock=asyncio.Lock(); o._startup_open_block_reason=None
    o._log_opportunity_status=Mock(); occupied=[]; writes=[]
    async def process(symbol, books):
        if occupied: return
        await asyncio.sleep(0)
        writes.append(symbol); occupied.append(symbol)
    o._handle_open_position_locked=process
    async def scenario(): await asyncio.gather(o._handle_open_position(SYMBOL,{}),o._handle_open_position('ETH-USDC-PERP',{}))
    run(scenario()); assert writes==[SYMBOL]


@pytest.mark.parametrize('change', ['expired', 'above_cap', 'fresh'])
def test_execution_rechecks_quotes_after_account_reads_and_updates_request(change):
    a = adapters(); latest = {'price': D(85000)}
    async def account_read():
        latest['price'] = None if change == 'expired' else D(95000) if change == 'above_cap' else D(86000)
        return []
    a['arcus'].get_positions.side_effect = account_read
    o = object.__new__(ArbitrageOrchestratorV3)
    o._startup_open_block_reason = None; o.data_freshness_seconds = 3
    o.risk_controller = risk(a); o.decision_engine = NS(positions={})
    o.unified_config = NS(execution=NS(order_execution=NS(max_slippage=D('.001'))))
    def get_book(exchange, symbol, **kwargs):
        return None if latest['price'] is None else NS(best_bid=NS(price=latest['price']), best_ask=NS(price=latest['price']))
    o.data_processor = NS(get_orderbook=get_book)
    e = ArbitrageExecutor(ExecutionConfig(), a, monitor_only=False, is_segmented_mode=True)
    e.before_open_check = o._check_open_execution_risk
    e._run_execution_plan = AsyncMock(return_value=ExecutionResult(success=False, error_message='offline plan'))
    request = ExecutionRequest(symbol=SYMBOL, exchange_buy='lighter', exchange_sell='arcus',
                               price_buy=D(85000), price_sell=D(85000), quantity=D('.0002'))
    result = run(e.execute_arbitrage(request))
    if change == 'fresh':
        e._run_execution_plan.assert_awaited_once()
        assert request.price_buy == D(86000) and request.price_sell == D(86000)
    else:
        assert result.failure_code == 'RISK_REJECTED'
        e._run_execution_plan.assert_not_awaited()
    for adapter in a.values():
        adapter.create_order.assert_not_awaited(); adapter.cancel_order.assert_not_awaited()


@pytest.mark.parametrize('base', ['BTC', 'ETH', 'BNB', 'SOL', 'HYPE', 'AAPL', 'NVDA', 'TSLA', 'MSFT'])
def test_arcus_native_quotes_reach_standard_strategy_symbol(base, monkeypatch):
    from core.services.arbitrage_monitor.utils.symbol_converter import SimpleSymbolConverter
    from core.services.arbitrage_monitor_v2.data.data_receiver import DataReceiver
    from core.services.arbitrage_monitor_v2.config.debug_config import DebugConfig
    monkeypatch.chdir(Path(__file__).resolve().parents[1])
    monkeypatch.setattr(SimpleSymbolConverter, '_custom_mappings_loaded', False)
    converter = SimpleSymbolConverter()
    standard = base + '-USDC-PERP'; native = base + '-USD'
    assert converter.convert_to_exchange(standard, 'arcus') == native
    assert converter.convert_from_exchange(native, 'arcus') == standard
    receiver = DataReceiver(asyncio.Queue(), asyncio.Queue(), DebugConfig.create_production())
    book = NS(symbol=native, best_bid=NS(price=D(100)), best_ask=NS(price=D(101)), timestamp=datetime.now())
    receiver._create_orderbook_callback('arcus')(native, book)
    message = receiver.orderbook_queue.get_nowait()
    assert message['symbol'] == standard and message['exchange'] == 'arcus'
    receiver._create_ticker_callback('arcus')(native, NS(symbol=native))
    ticker_message = receiver.ticker_queue.get_nowait()
    assert ticker_message['symbol'] == standard and ticker_message['exchange'] == 'arcus'


def test_profile_four_maker_symbols_and_legacy_taker_config_is_self_contained():
    from core.services.arbitrage_monitor_v2.config.symbol_config import SegmentedConfigManager
    profile=yaml.safe_load(Path('config/arbitrage/arbitrage_lighter_arcus.yaml').read_text())
    monitor=yaml.safe_load(Path('config/arbitrage/monitor_lighter_arcus.yaml').read_text())
    segmented=yaml.safe_load(Path('config/arbitrage/arbitrage_segmented.yaml').read_text())
    assert set(monitor['symbols'])=={s+'-USDC-PERP' for s in ('BTC','ETH','SOL','HYPE')}
    assert segmented['execution_mode']=='maker_taker'
    assert segmented['symbol_configs']['BNB-USDC-PERP']['enabled'] is False
    for symbol in monitor['symbols']:
        grid=segmented['symbol_configs'][symbol]['grid_config']
        assert grid['initial_spread_threshold']==.02 and grid['grid_step']==.01 and grid['max_segments']==2
    pm=profile['risk_control']['position_management']
    assert all(key not in pm for key in ('max_gross_notional_usd','max_leg_notional_usd','max_open_symbols'))
    assert profile['system_mode']['monitor_only'] is True
    assert profile['arbitrage_execution']['order_execution']['slippage_protection']['max_slippage']==.001
    assert profile['arbitrage_execution']['exchange_fees']['arcus']['market_fee_rate']==.000225


@pytest.mark.parametrize('is_open', [True, False])
def test_monitor_executor_never_invokes_account_guard_or_writes(is_open):
    config=UnifiedConfigManager(Path('config/arbitrage/arbitrage_lighter_arcus.yaml')).config
    a=adapters(); e=ArbitrageExecutor(config.execution,a,monitor_only=config.system_mode.monitor_only,is_segmented_mode=False)
    e.before_open_check=AsyncMock(side_effect=AssertionError('monitor must not execute live gate'))
    req=ExecutionRequest(SYMBOL,'lighter','arcus',D(85000),D(85001),D('.0002'),is_open=is_open)
    assert run(e.execute_arbitrage(req)).success
    e.before_open_check.assert_not_awaited()
    for adapter in a.values():
        adapter.create_order.assert_not_awaited(); adapter.cancel_order.assert_not_awaited()


def test_real_yaml_loader_usd_mode_and_legacy_defaults():
    from pathlib import Path
    manager=UnifiedConfigManager(Path('config/arbitrage/arbitrage_lighter_arcus.yaml'))
    pm=manager.config.risk_control.position_management
    assert (pm.max_gross_notional_usd,pm.max_leg_notional_usd,pm.max_open_symbols)==(None,None,0)
    assert pm.min_leg_notional_usd=={}
    legacy=UnifiedConfigManager()
    assert legacy.config.risk_control.position_management.max_gross_notional_usd is None
    assert legacy.config.risk_control.position_management.max_open_symbols==0


def test_account_invalidation_during_get_does_not_cache_pre_fill_snapshot():
    r=rest(); response=account(r)
    async def read(**kwargs):
        r._invalidate_account_snapshot()
        return response
    r.account_api=NS(account=AsyncMock(side_effect=read))
    with pytest.raises(OrderStateUnknownError): run(r.get_positions())
    assert r._account_snapshot is None


def test_legacy_base_quantity_cap_still_aggregates_both_exchange_positions():
    c=risk(); c.config.position_management.max_gross_notional_usd=None
    c.config.position_management.max_total_position=2; c.config.position_management.max_open_symbols=0
    for a in c.exchange_adapters.values(): a.get_positions.return_value=[NS(symbol=SYMBOL,size=D('.2'),timestamp=datetime.now())]
    assert run(c.check_position_limits(SYMBOL,'lighter',D('.5')))[0]
    assert not run(c.check_position_limits(SYMBOL,'lighter',D('.7')))[0]


def test_correlated_rest_missing_or_invalid_fill_never_authorizes_terminal():
    r=registered()
    row=NS(**raw()); row.order_id='1001'; row.type='market'; row.price='85000'; row.timestamp=None
    assert r._parse_order(row,SYMBOL).filled==D('.0001')
    for bad in ('NaN','.00011','-.0001'):
        row.filled_base_amount=bad
        with pytest.raises((OrderStateUnknownError,ArithmeticError)): r._parse_order(row,SYMBOL)
    del row.filled_base_amount
    with pytest.raises((OrderStateUnknownError,ArithmeticError)): r._parse_order(row,SYMBOL)


def test_risk_ui_unavailable_account_does_not_show_normal_status():
    from core.services.arbitrage_monitor_v2.display.ui_components import UIComponents
    from io import StringIO
    from rich.console import Console
    output=StringIO()
    Console(file=output,width=120,color_system=None).print(UIComponents.create_risk_control_panel({'account_data_unavailable':['arcus']}))
    rendered=output.getvalue()
    assert '正常运行' not in rendered and '账户数据不可用' in rendered


def test_terminal_arriving_during_rest_failure_wins_over_unknown_or_stale_open():
    for failed in (True,False):
        r=registered()
        async def read(*args):
            r._accept_terminal_order(raw(),1)
            if failed: raise OrderStateUnknownError('lighter')
            return order(OrderStatus.OPEN)
        r._read_order=AsyncMock(side_effect=read)
        actual=run(r.get_order('41',SYMBOL))
        assert actual.status is OrderStatus.FILLED and actual.filled==D('.0001')
        assert not r._uncertain_orders


def test_missing_balance_fields_cannot_fabricate_a_verified_zero_at_startup():
    a=adapters(); a['arcus'].get_balances.return_value=[NS(currency='USDC',total=None,free=None,used=None,timestamp=datetime.now())]
    assert not run(risk(a).check_startup_account_state())[0]


def test_arcus_concurrent_account_reads_are_coalesced_and_invalidated():
    from test_arcus_adapter import adapter
    a=adapter(); a._read_positions=AsyncMock(return_value=[])
    a._read_balances=AsyncMock(return_value=[])
    async def scenario():
        await asyncio.gather(*(a.get_positions() if i%2 else a.get_balances() for i in range(30)))
        assert a._read_positions.await_count==a._read_balances.await_count==1
        a._invalidate_account_reads()
        await a.get_positions()
        assert a._read_positions.await_count==2
        a._invalidate_account_reads(); a._read_positions.side_effect=TimeoutError('offline')
        results=await asyncio.gather(*(a.get_positions() for _ in range(20)),return_exceptions=True)
        assert all(isinstance(r,OrderStateUnknownError) for r in results)
        assert a._read_positions.await_count==3 and a._account_reads=={}
    run(scenario())


def test_confirmed_close_does_not_invoke_new_open_account_guard():
    from core.services.arbitrage_monitor_v2.execution.arbitrage_executor import ExecutionResult
    e=ArbitrageExecutor(ExecutionConfig(),adapters(),monitor_only=False,is_segmented_mode=True)
    e.before_open_check=AsyncMock(side_effect=TimeoutError('account unavailable'))
    from legacy_fixtures import filled_order
    e._run_execution_plan=AsyncMock(return_value=ExecutionResult(success=True,order_buy=filled_order('L',D('.0002'),D(85000),True),order_sell=filled_order('A',D('.0002'),D(85001),False)))
    req=ExecutionRequest(SYMBOL,'lighter','arcus',D(85000),D(85001),D('.0002'),is_open=False)
    assert run(e.execute_arbitrage(req)).success
    e.before_open_check.assert_not_awaited(); e._run_execution_plan.assert_awaited_once()


def test_four_maker_quantities_respect_common_precision_and_arcus_minimum():
    from core.services.arbitrage_monitor_v2.config.maker_taker_config import load_profile
    from core.services.arbitrage_monitor_v2.execution.maker_taker_quotes import MarketRules
    config=UnifiedConfigManager(Path('config/arbitrage/arbitrage_lighter_arcus.yaml')).config
    segmented=load_profile(Path('config/arbitrage/arbitrage_segmented.yaml'))
    prices={'BTC':D(85000),'ETH':D(2700),'SOL':D(120),'HYPE':D(80)}
    executor=ArbitrageExecutor(config.execution,adapters(),monitor_only=True,is_segmented_mode=True)
    for base,price in prices.items():
        symbol=base+'-USDC-PERP'
        quantity=segmented.get_config(symbol).quantity
        step={'BTC':D('.00001'),'ETH':D('.0001'),'SOL':D('.001'),'HYPE':D('.01')}[base]
        rule=MarketRules(step,step,step,step,D(5),D(10),D(10000),D('.01'))
        assert rule.legal(quantity,price,price)
        assert D(10) <= quantity*price <= D(100)
        assert not rule.legal(quantity,D('4.99')/quantity,price)
        for adapter in executor.exchange_adapters.values():
            adapter.create_order.assert_not_awaited();adapter.cancel_order.assert_not_awaited()


def test_unknown_execution_manual_lock_remains_visible_after_adapter_terminal_read():
    o=object.__new__(ArbitrageOrchestratorV3); o._startup_open_block_reason=None
    o.exchange_adapters=adapters(); o.executor=NS(_unknown_execution_pairs={('BTC',('arcus','lighter'))})
    assert '人工' in o._get_open_block_reason()
    o.executor._unknown_execution_pairs.clear(); assert o._get_open_block_reason() is None


@pytest.mark.parametrize('natural', [-.05, 0, .05])
def test_tuned_profile_real_decision_entry_exit_and_persistence(monkeypatch, natural):
    from core.services.arbitrage_monitor_v2.analysis.spread_calculator import SpreadData
    from core.services.arbitrage_monitor_v2.decision import arbitrage_decision as module
    from core.services.arbitrage_monitor_v2.models import ClosePositionContext, FundingRateData
    config = UnifiedConfigManager(Path('config/arbitrage/arbitrage_lighter_arcus.yaml')).config
    history = NS(get_natural_spread=AsyncMock(return_value=(natural, datetime.now())))
    engine = module.ArbitrageDecisionEngine(config.decision, history,
        config.execution.exchange_order_modes, config.execution.exchange_fee_config)
    clock = [1000.1]
    monkeypatch.setattr(module.time, 'time', lambda: clock[0])
    threshold = .065 + max(natural, 0)
    def spread(pct):
        return SpreadData(SYMBOL, 'lighter', 'arcus', D(100), D(100)+D(str(pct)),
                          D(1), D(1), D(str(pct)), pct)
    funding = FundingRateData('lighter', 'arcus', 0, 0, 0, 0)
    assert sum(engine._calculate_fee_breakdown(spread(threshold))) == pytest.approx(.045)
    # Use epsilon around the threshold, avoiding binary-float equality artifacts.
    assert not run(engine.should_open_position(SYMBOL, spread(threshold-1e-6), funding))[0]
    assert not run(engine.should_open_position(SYMBOL, spread(threshold+1e-6), funding))[0]
    assert not run(engine.should_open_position(SYMBOL, spread(threshold+1e-6), funding))[0]
    clock[0] += 1
    assert run(engine.should_open_position(SYMBOL, spread(threshold+1e-6), funding))[0]
    run(engine.record_open_position(SYMBOL, spread(threshold+1e-6), funding,
                                   'spread', 'condition1', D('.0009')))
    context = ClosePositionContext(100, 100, .065-1e-6, 0, .065-1e-6, 0)
    assert not engine.should_close_position(SYMBOL, spread(threshold), funding, context)[0]
    context.total_profit_pct = .065+1e-6
    assert engine.should_close_position(SYMBOL, spread(threshold), funding, context) == (True, 'profit_target_reached')
    context.total_profit_pct = -.300001
    assert engine.should_close_position(SYMBOL, spread(threshold), funding, context) == (True, 'spread_loss_stop_loss')


@pytest.mark.parametrize('is_open', [True, False])
def test_source_segmented_executor_has_no_round_wait(monkeypatch, is_open):
    from core.services.arbitrage_monitor_v2.execution import arbitrage_executor as module
    config = UnifiedConfigManager(Path('config/arbitrage/arbitrage_lighter_arcus.yaml')).config
    executor = ArbitrageExecutor(config.execution, adapters(), monitor_only=False, is_segmented_mode=True)
    assert executor._round_pause_seconds == 0
    # Isolate the round timer; exchange rate limiting remains intentionally enabled.
    executor._reserve_exchange_slots = Mock(return_value=None)
    from legacy_fixtures import filled_order
    executor._run_execution_plan = AsyncMock(return_value=ExecutionResult(success=True,order_buy=filled_order('L',D('.0009'),D(85000),True),order_sell=filled_order('A',D('.0009'),D(85001),False)))
    executor.before_open_check = AsyncMock(return_value=(True, ''))
    executor._next_round_time = module.time.time() + 60
    sleep = AsyncMock(side_effect=AssertionError('unexpected round wait'))
    monkeypatch.setattr(module.asyncio, 'sleep', sleep)
    request = ExecutionRequest(SYMBOL, 'lighter', 'arcus', D(85000), D(85001), D('.0009'), is_open=is_open)
    assert run(executor.execute_arbitrage(request)).success
    executor._run_execution_plan.assert_awaited_once()
    sleep.assert_not_awaited()
