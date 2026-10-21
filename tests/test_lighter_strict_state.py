"""Offline production Lighter boundaries. No dotenv, credentials, native signer or network."""
import asyncio
import inspect
from datetime import datetime
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import lighter
import pytest
from core.adapters.exchanges.adapters.lighter_rest import LighterRest
from core.adapters.exchanges.adapters.lighter import LighterAdapter
from core.adapters.exchanges.interface import ExchangeConfig
from core.adapters.exchanges.models import ExchangeType, OrderData, OrderSide, OrderType, OrderStatus
from core.adapters.exchanges.order_state import OrderStateUnknownError
from core.services.arbitrage_monitor_v2.execution.arbitrage_executor import ArbitrageExecutor, ExecutionConfig

SYMBOL = 'BTC-USDC-PERP'

def run(coro):
    return asyncio.run(coro)

def order(status=OrderStatus.PENDING, filled='0'):
    return OrderData(id='41', client_id='41', symbol=SYMBOL, side=OrderSide.BUY,
                     type=OrderType.MARKET, amount=D('.0001'), price=D('85000'),
                     filled=D(filled), remaining=D('.0001')-D(filled), cost=D('0'),
                     average=None, status=status, timestamp=datetime.now(), updated=None,
                     fee=None, trades=[], params={}, raw_data={})

def rest():
    # Deliberately bypass initialization: fake signer is the only write transport.
    r = object.__new__(LighterRest)
    r.strict_order_state = True
    r._strict_write_lock = asyncio.Lock()
    r._uncertain_orders = {}
    r.account_index = 7
    r.signer_client = SimpleNamespace(get_api_key_nonce=Mock(return_value=(2, 11)),
                                     sign_create_order=Mock(return_value=(14, 'offline signed payload', 'hash', None)),
                                     send_tx=AsyncMock(return_value=SimpleNamespace(code=200, tx_hash='hash')),
                                     nonce_manager=SimpleNamespace(acknowledge_failure=Mock()))
    r.get_market_index = Mock(return_value=1)
    r._get_auth_token = Mock(return_value='offline token')
    return r


def test_production_bridge_returns_pending_and_registers_mapping():
    pending = order()
    a = SimpleNamespace(place_market_orders_ws_batch=AsyncMock(return_value={'orders':[pending]}))
    e = ArbitrageExecutor(ExecutionConfig(), {'lighter':a}, monitor_only=False, is_segmented_mode=True)
    assert run(e._place_lighter_market_order_ws(a, SYMBOL, D('.0001'), True, False)) is pending
    assert pending.status is OrderStatus.PENDING and pending.filled == 0
    assert e._order_exchange_map['41'] == 'lighter'
    assert a.place_market_orders_ws_batch.await_count == 1


@pytest.mark.parametrize('ack', [None, {}, {'orders':[]}, {'orders':[order(), order()]}, {'orders':[SimpleNamespace(id='41')]}])
def test_bridge_malformed_ack_is_unknown(ack):
    a = SimpleNamespace(place_market_orders_ws_batch=AsyncMock(return_value=ack))
    e = ArbitrageExecutor(ExecutionConfig(), {'lighter':a}, monitor_only=False, is_segmented_mode=True)
    with pytest.raises(OrderStateUnknownError):
        run(e._place_lighter_market_order_ws(a, SYMBOL, D('.0001'), True, False))
    assert a.place_market_orders_ws_batch.await_count == 1


@pytest.mark.parametrize('failure', [TimeoutError(), asyncio.CancelledError(), None, SimpleNamespace(code=200, tx_hash=None), SimpleNamespace(code=500, tx_hash='hash')])
def test_native_rest_unknown_one_send_no_nonce_rollback_or_replay(failure):
    r = rest()
    if isinstance(failure, BaseException):
        r.signer_client.send_tx.side_effect = failure
    else:
        r.signer_client.send_tx.return_value = failure
    async def scenario():
        for _ in range(2):
            with pytest.raises(OrderStateUnknownError):
                await r._strict_submit(r.signer_client.sign_create_order, {'market_index':1}, '41')
    run(scenario())
    assert r.signer_client.send_tx.await_count == 1
    assert r.signer_client.sign_create_order.call_count == 1
    assert r.signer_client.nonce_manager.acknowledge_failure.call_count == 0
    assert r._uncertain_orders['41'] == {'api_key_index':2, 'nonce':11}


@pytest.mark.parametrize('name', ['account_active_orders', 'account_inactive_orders'])
def test_installed_sdk_signature_and_serializer_authorization_all_markets(name):
    r = rest()
    cls_method = getattr(lighter.OrderApi, name)
    captured = []
    # Give the fake precisely the installed SDK signature, then bind and serialize real inputs.
    async def fake(**kwargs):
        bound = inspect.signature(cls_method).bind(None, **kwargs)
        bound.apply_defaults()
        captured.append(bound.arguments)
        serialize = getattr(lighter.OrderApi, '_' + name + '_serialize')
        args = {key: bound.arguments[key] for key in inspect.signature(serialize).parameters if key != 'self'}
        params = serialize(SimpleNamespace(api_client=SimpleNamespace(select_header_accept=lambda _:None, select_header_content_type=lambda _:None, param_serialize=lambda **data:data)), **args)
        assert ('account_index',7) in params['query_params']
        assert not any(key == 'market_id' for key, _ in params['query_params'])
        assert params['header_params']['authorization'] == 'offline token'
        return SimpleNamespace(code=200, orders=[])
    fake.__signature__ = inspect.signature(cls_method).replace(parameters=list(inspect.signature(cls_method).parameters.values())[1:])
    r.order_api = SimpleNamespace(**{name:fake})
    result = run(r.get_open_orders()) if name.endswith('active_orders') and not name.endswith('inactive_orders') else run(r.get_order_history())
    assert result == []
    assert captured[0]['authorization'] == 'offline token'
    assert captured[0]['market_id'] is None
    assert 'auth' not in captured[0]


def test_strict_read_failure_and_missing_order_are_unknown_not_empty_or_filled():
    r = rest()
    async def active(authorization, account_index, market_id=None):
        raise TimeoutError('offline')
    r.order_api = SimpleNamespace(account_active_orders=active)
    with pytest.raises(OrderStateUnknownError): run(r.get_open_orders())
    r.get_open_orders = AsyncMock(return_value=[])
    r.get_order_history = AsyncMock(return_value=[])
    with pytest.raises(OrderStateUnknownError): run(r.get_order('41', SYMBOL))


def test_unknown_cancel_is_not_cleared_by_stale_open_read():
    r = rest()
    r._uncertain_orders['41'] = {'api_key_index':2, 'nonce':11}
    r.get_open_orders = AsyncMock(return_value=[order(OrderStatus.OPEN)])
    r.get_order_history = AsyncMock(return_value=[])
    run(r.get_order('41', SYMBOL))
    assert '41' in r._uncertain_orders
    r.get_open_orders.return_value = [order(OrderStatus.CANCELED)]
    r._order_read_retry[('41', SYMBOL)] = 0  # The next polling interval has elapsed.
    run(r.get_order('41', SYMBOL))
    assert not r._uncertain_orders


def test_strict_monitor_partial_live_cannot_advance_or_write():
    a = SimpleNamespace(strict_order_state=True, get_order=AsyncMock(return_value=order(OrderStatus.OPEN,'.00005')))
    e = ArbitrageExecutor(ExecutionConfig(), {'lighter':a}, monitor_only=False, is_segmented_mode=True)
    with pytest.raises(OrderStateUnknownError):
        run(e.order_monitor.wait_for_order_fill(order(), a, timeout=0, is_market_order=True))
    assert a.get_order.await_count == 1


def test_strict_config_opt_in_default_legacy_unchanged():
    static = {'api_config':{'testnet':False,'auth':{}}}
    stub = SimpleNamespace(logger=None, _load_lighter_config=lambda:static)
    cfg = ExchangeConfig('lighter','Lighter',ExchangeType.PERPETUAL,'','')
    assert not LighterAdapter._convert_config_to_dict(stub, cfg)['strict_order_state']
    cfg.extra_params['strict_order_state'] = True
    assert LighterAdapter._convert_config_to_dict(stub, cfg)['strict_order_state']


@pytest.mark.parametrize('is_buy', [True, False])
@pytest.mark.parametrize('ack', [TimeoutError(), asyncio.CancelledError(), None, {}, {'tx_hash':[]}, {'tx_hash':['hash','extra']}, {'tx_hash':['unmatched']}, {'tx_hash':['hash'], 'error':{'code':500}}])
def test_production_ws_batch_unknown_one_write_both_directions(is_buy, ack):
    r = rest()
    r._websocket = SimpleNamespace(send_tx_batch=AsyncMock())
    if isinstance(ack, BaseException): r._websocket.send_tx_batch.side_effect = ack
    else: r._websocket.send_tx_batch.return_value = ack
    side = 'buy' if is_buy else 'sell'
    r._prepare_reduce_only_quantity = Mock(return_value={'quantity':D('.0001'), 'skip':False})
    r._build_signed_market_order_tx = AsyncMock(return_value={
        'tx_type':14, 'tx_info':'offline signed', 'tx_hash':'hash', 'api_key_index':2, 'nonce':11,
        'context':{'client_order_id':41,'symbol':SYMBOL,'side':side,'quantity':D('.0001'),'avg_execution_price':D('85000')}})
    async def scenario():
        for _ in range(2):
            with pytest.raises(OrderStateUnknownError):
                await r.place_market_orders_via_ws_batch([{'symbol':SYMBOL,'side':side,'quantity':D('.0001')}])
    run(scenario())
    assert r._websocket.send_tx_batch.await_count == 1
    assert r._build_signed_market_order_tx.await_count == 1
    assert r.signer_client.nonce_manager.acknowledge_failure.call_count == 0
    assert r._uncertain_orders['41']['nonce'] == 11


def test_production_ws_ack_preserves_pending_identity_quantity_and_nonce():
    r = rest()
    r._websocket = SimpleNamespace(send_tx_batch=AsyncMock(return_value={'tx_hash':['hash']}))
    r._prepare_reduce_only_quantity = Mock(return_value={'quantity':D('.0001'), 'skip':False})
    r._build_signed_market_order_tx = AsyncMock(return_value={
        'tx_type':14,'tx_info':'offline signed','tx_hash':'hash','api_key_index':2,'nonce':11,
        'context':{'client_order_id':41,'symbol':SYMBOL,'side':'buy','quantity':D('.0001'),'avg_execution_price':D('85000')}})
    ack = run(r.place_market_orders_via_ws_batch([{'symbol':SYMBOL,'side':'buy','quantity':D('.0001')}]))
    o = ack['orders'][0]
    assert o.id == o.client_id == '41'
    assert o.status is OrderStatus.PENDING and o.filled == 0 and o.remaining == D('.0001')
    assert r._uncertain_orders['41'] == {'api_key_index':2,'nonce':11,'state':'acknowledged'}


def test_strict_cancel_does_not_fabricate_canceled_from_ack():
    r = rest()
    pending = order(OrderStatus.OPEN)
    r.get_order = AsyncMock(return_value=pending)
    r.signer_client.sign_cancel_order = Mock(return_value=(15,'offline cancel','hash',None))
    a = object.__new__(LighterAdapter)
    a.strict_order_state = True
    a._rest = r
    a._normalize_symbol = lambda value:value
    actual = run(a.cancel_order('41', SYMBOL))
    assert actual is pending and actual.status is OrderStatus.OPEN
    assert r.signer_client.send_tx.await_count == 1


def test_current_sdk_nonce_method_uses_async_manager_selected_key_no_signer():
    assert not hasattr(lighter.SignerClient, "get_api_key_nonce")
    from lighter.nonce_manager import NonceManager
    r = rest()
    r.api_key_index = 2
    del r.signer_client.get_api_key_nonce
    calls = []
    async def current_nonce(api_key=None):
        inspect.signature(NonceManager.async_next_nonce).bind(None, api_key)
        calls.append(api_key)
        return api_key, 11
    r.signer_client.nonce_manager.async_next_nonce = current_nonce
    response = run(r._strict_submit(r.signer_client.sign_create_order, {"market_index":1}, '41'))
    assert response.tx_hash == 'hash' and calls == [2]
    assert r.signer_client.sign_create_order.call_args.kwargs['api_key_index'] == 2
    assert r.signer_client.sign_create_order.call_args.kwargs['nonce'] == 11


@pytest.mark.parametrize('sign,expected', [(1,'long'),(-1,'short')])
def test_strict_rest_position_uses_sdk_direction_and_normalized_market(sign, expected):
    r = rest()
    r._markets_cache = {1:{'symbol':SYMBOL}}
    r.account_api = SimpleNamespace(account=AsyncMock(return_value=SimpleNamespace(code=200, accounts=[
        SimpleNamespace(index=7, available_balance='30', collateral='30', positions=[SimpleNamespace(market_id=1,symbol='BTC',position='.0001',sign=sign,unrealized_pnl='0')])])))
    result = run(r.get_positions([SYMBOL]))
    assert len(result) == 1 and result[0].symbol == SYMBOL
    assert result[0].side.value == expected and result[0].size == D('.0001')


def test_strict_production_rest_market_native_signature_and_pending_ack():
    r = rest()
    r._calculate_slippage_protection_price = AsyncMock(return_value=D('85000'))
    def fake_sign(**kwargs):
        bound = inspect.signature(lighter.SignerClient.sign_create_order).bind(None, **kwargs)
        assert bound.arguments['order_type'] == lighter.SignerClient.ORDER_TYPE_MARKET
        assert bound.arguments['time_in_force'] == lighter.SignerClient.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL
        assert bound.arguments['reduce_only'] is True
        assert bound.arguments['client_order_index'] == 41
        assert bound.arguments['api_key_index'] == 2 and bound.arguments['nonce'] == 11
        return 14, 'offline signed', 'hash', None
    r.signer_client.sign_create_order.side_effect = fake_sign
    result = run(r._execute_market_order(SYMBOL, 'sell', D('.0001'), None,
                 {'market_index':1,'price_decimals':1,'price_multiplier':10,'size_decimals':5},
                 client_order_id=41, reduce_only=True))
    assert result.id == result.client_id == '41'
    assert result.status is OrderStatus.PENDING and result.filled == 0
    assert result.side is OrderSide.SELL and result.amount == D('.0001')
    assert r.signer_client.send_tx.await_count == 1


def test_partial_read_is_retained_monotonically_before_timeout_and_terminal():
    p = order()
    latest = order(OrderStatus.OPEN, '.00004')
    latest.cost = D('3.4'); latest.average = D('85000'); latest.fee = {'cost':D('.001')}
    latest.raw_data = {'confirmed_partial':True}
    a = SimpleNamespace(strict_order_state=True, get_order=AsyncMock(return_value=latest))
    e = ArbitrageExecutor(ExecutionConfig(), {'lighter':a}, monitor_only=False, is_segmented_mode=True)
    with pytest.raises(OrderStateUnknownError):
        run(e.order_monitor.wait_for_order_fill(p,a,timeout=0,is_market_order=True))
    assert p.filled == D('.00004') and p.remaining == D('.00006')
    assert p.cost == D('3.4') and p.average == D('85000') and p.fee == latest.fee
    a.get_order.return_value = order(OrderStatus.CANCELED)
    assert run(e.order_monitor.wait_for_order_fill(p,a,timeout=0,is_market_order=True)) == D('.00004')
    assert p.cost == D('3.4') and p.raw_data['confirmed_partial']


def test_cancel_ack_open_retains_latch_until_authoritative_terminal():
    r = rest(); p = order(OrderStatus.OPEN)
    r.get_open_orders = AsyncMock(return_value=[p]); r.get_order_history = AsyncMock(return_value=[])
    r.signer_client.sign_cancel_order = Mock(return_value=(15,'offline signed','hash',None))
    a = object.__new__(LighterAdapter); a.strict_order_state = True; a._rest = r; a._normalize_symbol = lambda s:s
    assert run(a.cancel_order('41',SYMBOL)).status is OrderStatus.OPEN
    assert a._uncertain is r._uncertain_orders and a._uncertain['41']['cancel_pending']
    with pytest.raises(OrderStateUnknownError):
        run(r._strict_submit(r.signer_client.sign_create_order,{'market_index':1},'42'))
    assert r.signer_client.send_tx.await_count == 1
    r.get_open_orders.return_value = []; r.get_order_history.return_value = [order(OrderStatus.CANCELED)]
    r._order_read_retry[('41', SYMBOL)] = 0  # The next polling interval has elapsed.
    run(a.get_order('41',SYMBOL))
    assert not a._uncertain


def test_real_adapter_normalized_ws_bridge_returns_original_pending():
    p = order(); p.symbol = 'BTC'
    a = object.__new__(LighterAdapter); a._symbol_mapping = {SYMBOL:'BTC'}
    a._rest = SimpleNamespace(base_slippage=D('.005'), place_market_orders_via_ws_batch=AsyncMock(return_value={'orders':[p]}))
    e = ArbitrageExecutor(ExecutionConfig(),{'lighter':a},monitor_only=False,is_segmented_mode=True)
    assert run(e._place_lighter_market_order_ws(a,SYMBOL,D('.0001'),True,False)) is p
    assert a._rest.place_market_orders_via_ws_batch.call_args.args[0][0]['symbol'] == 'BTC'
    assert p.status is OrderStatus.PENDING and p.filled == 0


@pytest.mark.parametrize('queued',[False,True])
@pytest.mark.parametrize('reverse',[False,True])
def test_existing_lighter_unknown_blocks_cross_symbol_and_reserved_opposite_writes(queued,reverse):
    from core.services.arbitrage_monitor_v2.execution.arbitrage_executor import ExchangeRateLimiter, ExecutionRequest, ExecutionResult
    async def scenario():
        r = rest(); a = object.__new__(LighterAdapter); a._rest = r; a.strict_order_state = True
        other = SimpleNamespace(create_order=AsyncMock())
        e = ArbitrageExecutor(ExecutionConfig(),{'lighter':a,'arcus':other},monitor_only=False,is_segmented_mode=True)
        req = ExecutionRequest('ETH-USDC-PERP','arcus' if reverse else 'lighter','lighter' if reverse else 'arcus',D('85000'),D('85001'),D('.0001'))
        if not queued:
            r._uncertain_orders['41'] = {'nonce':11}
            result = await e.execute_arbitrage(req)
            assert result.failure_code == 'ORDER_STATE_UNKNOWN'
        else:
            limiter = ExchangeRateLimiter('lighter',max_concurrent=1,min_interval_ms=0,cooldown_ms=0)
            e._get_rate_limiter = lambda exchange:limiter
            started = asyncio.Event(); release = asyncio.Event(); plans = []
            async def plan(request,grid):
                plans.append(request); started.set(); await release.wait()
                r._uncertain_orders['41'] = {'nonce':11}
                return ExecutionResult(success=False,failure_code='ORDER_STATE_UNKNOWN')
            e._run_execution_plan = plan
            first = asyncio.create_task(e.execute_arbitrage(ExecutionRequest(SYMBOL,'lighter','arcus',D('85000'),D('85001'),D('.0001'))))
            await started.wait()
            second = asyncio.create_task(e.execute_arbitrage(req))
            await asyncio.sleep(0)
            assert limiter._semaphore._waiters and not second.done()
            release.set(); outcomes = await asyncio.gather(first,second)
            assert all(result.failure_code == 'ORDER_STATE_UNKNOWN' for result in outcomes)
            assert len(plans) == 1
        other.create_order.assert_not_awaited()
        r.signer_client.send_tx.assert_not_awaited()
    run(scenario())


@pytest.mark.parametrize('response',[
    TimeoutError('offline'), None, SimpleNamespace(code=500,accounts=[]),
    SimpleNamespace(code=200,accounts=[]),
    SimpleNamespace(code=200,accounts=[SimpleNamespace(index=8,available_balance='30',collateral='30')]),
    SimpleNamespace(code=200,accounts=[SimpleNamespace(index=7,collateral='30')]),
    SimpleNamespace(code=200,accounts=[SimpleNamespace(index=7,available_balance='NaN',collateral='30')])])
def test_strict_balance_failure_is_unknown(response):
    r = rest(); r.account_api = SimpleNamespace(account=AsyncMock())
    if isinstance(response,BaseException): r.account_api.account.side_effect = response
    else: r.account_api.account.return_value = response
    with pytest.raises(OrderStateUnknownError): run(r.get_account_balance())


@pytest.mark.parametrize('value',['0','30'])
def test_strict_balance_returns_real_zero_or_funded_amount(value):
    r = rest(); r.account_api = SimpleNamespace(account=AsyncMock(return_value=SimpleNamespace(code=200,accounts=[
        SimpleNamespace(index=7,available_balance=value,collateral=value,positions=[])])))
    a = object.__new__(LighterAdapter); a.strict_order_state = True; a._rest = r
    balances = run(a.get_balances())
    assert len(balances) == 1 and balances[0].currency == 'USDC'
    assert balances[0].total == balances[0].free == D(value)


@pytest.mark.parametrize('inflight',[False,True])
def test_automatic_restart_cannot_clear_unknown_or_inflight_write(inflight):
    from unittest.mock import patch
    async def scenario():
        r = rest(); a = object.__new__(LighterAdapter); a.strict_order_state = True; a._rest = r
        if inflight: await r._strict_write_lock.acquire()
        else: r._uncertain_orders['41'] = {'nonce':11}
        try:
            with patch('core.adapters.exchanges.adapters.lighter.LighterRest') as build_rest, patch('core.adapters.exchanges.adapters.lighter.LighterWebSocket') as build_ws:
                a.restart_connections()
                build_rest.assert_not_called(); build_ws.assert_not_called()
                assert a._rest is r
                assert r._strict_write_lock.locked() if inflight else a._uncertain['41']['nonce'] == 11
        finally:
            if inflight: r._strict_write_lock.release()
    run(scenario())


def test_acknowledged_create_partial_read_retains_cross_symbol_latch_but_can_cancel():
    r = rest()
    r._uncertain_orders['41'] = {'api_key_index':2,'nonce':11,'state':'acknowledged'}
    r.get_open_orders = AsyncMock(return_value=[order(OrderStatus.OPEN,'.00004')])
    r.get_order_history = AsyncMock(return_value=[])
    run(r.get_order('41',SYMBOL))
    assert r._uncertain_orders['41']['state'] == 'acknowledged'
    with pytest.raises(OrderStateUnknownError):
        run(r._strict_submit(r.signer_client.sign_create_order,{'market_index':1},'42'))
    r.signer_client.sign_cancel_order = Mock(return_value=(15,'offline signed','hash',None))
    assert run(r.cancel_order(SYMBOL,'41'))
    assert r.signer_client.send_tx.await_count == 1
    assert r._uncertain_orders['41']['cancel_pending']
    with pytest.raises(OrderStateUnknownError): run(r.cancel_order(SYMBOL,'41'))
    assert r.signer_client.send_tx.await_count == 1


@pytest.mark.parametrize('strict,count', [(True,2),(True,1),(False,2)])
def test_strict_multiple_ws_orders_rejected_before_any_sign_nonce_or_send(strict,count):
    r = rest(); r.strict_order_state = strict
    r._websocket = SimpleNamespace(send_tx_batch=AsyncMock(return_value={'tx_hash':['hash'+str(i) for i in range(count)]}))
    r._prepare_reduce_only_quantity = Mock(return_value={'quantity':D('.0001'),'skip':False})
    async def sign(symbol,side,qty,**kwargs):
        ident = r.signer_client.get_api_key_nonce.call_count
        r.signer_client.get_api_key_nonce(-1,-1)
        r.signer_client.sign_create_order()
        return {'tx_type':14,'tx_info':'offline signed','tx_hash':'hash'+str(ident),'api_key_index':2,'nonce':11+ident,
                'context':{'client_order_id':ident+41,'symbol':symbol,'side':side,'quantity':qty,'avg_execution_price':D('85000')}}
    r._build_signed_market_order_tx = AsyncMock(side_effect=sign)
    orders = [{'symbol':SYMBOL,'side':'buy','quantity':D('.0001')} for _ in range(count)]
    if strict and count>1:
        with pytest.raises(ValueError,match='only single-order'):
            run(r.place_market_orders_via_ws_batch(orders))
        r._build_signed_market_order_tx.assert_not_awaited()
        r.signer_client.get_api_key_nonce.assert_not_called()
        r.signer_client.sign_create_order.assert_not_called()
        r._websocket.send_tx_batch.assert_not_awaited()
    else:
        response = run(r.place_market_orders_via_ws_batch(orders))
        assert len(response['orders']) == count
        assert all(o.status is OrderStatus.PENDING and o.filled == 0 for o in response['orders'])
        assert r._build_signed_market_order_tx.await_count == count
        assert r.signer_client.get_api_key_nonce.call_count == count
        assert r._websocket.send_tx_batch.await_count == 1


@pytest.fixture
def lifecycle_clients(monkeypatch):
    """Construct the real owner graph with fake SDK clients, never a native signer."""
    from core.adapters.exchanges.adapters import lighter_rest as module
    from core.adapters.exchanges.adapters.lighter_base import LighterBase
    for name in ('LIGHTER_API_KEY_PRIVATE_KEY', 'LIGHTER_ACCOUNT_INDEX', 'LIGHTER_API_KEY_INDEX'):
        monkeypatch.delenv(name, raising=False)
    signers, apis, events = [], [], []
    def create_signer(self, **kwargs):
        index = len(signers)
        async def close(): events.append(('close', index))
        client = SimpleNamespace(close=AsyncMock(side_effect=close), check_client=Mock(return_value=None))
        signers.append(client); events.append(('create', index))
        return client
    def create_api(**kwargs):
        client = SimpleNamespace(close=AsyncMock()); apis.append(client); return client
    monkeypatch.setattr(LighterBase, '_create_signer_client', create_signer)
    monkeypatch.setattr(LighterRest, '_create_signer_client', create_signer)
    monkeypatch.setattr(module, 'ApiClient', create_api)
    for name in ('AccountApi', 'OrderApi', 'TransactionApi', 'CandlestickApi', 'FundingApi'):
        monkeypatch.setattr(module, name, lambda client: SimpleNamespace())
    monkeypatch.setattr(LighterRest, '_load_markets', AsyncMock())
    config = {'api_key_private_key':'offline fake key', 'account_index':7, 'api_key_index':2, 'auth_enabled':True}
    return config, signers, apis, events


def test_rest_closes_constructed_signers_before_replacement_and_all_clients_once(lifecycle_clients):
    config, signers, apis, events = lifecycle_clients
    async def scenario():
        r = LighterRest(config)
        assert len(signers) == 2  # REST and its inner websocket are independent owners.
        await r.initialize()
        assert events.index(('close', 0)) < events.index(('create', 2))
        assert len(signers) == 4
        await r.initialize()  # Already connected: do not replace clients.
        assert len(signers) == 4 and len(apis) == 1
        await r.disconnect(); await r.disconnect()
        assert r.signer_client is None and r.api_client is None and not r.is_connected()
    run(scenario())
    assert all(client.close.await_count == 1 for client in signers + apis)


@pytest.mark.parametrize('failure', ['check', 'market', 'cancel'])
def test_rest_failed_or_cancelled_initialization_cleans_every_created_client(lifecycle_clients, failure):
    config, signers, apis, _ = lifecycle_clients
    async def scenario():
        r = LighterRest(config)
        if failure == 'check':
            create = r._create_signer_client
            def invalid(**kwargs):
                client = create(**kwargs); client.check_client.return_value = 'offline invalid client'; return client
            r._create_signer_client = invalid
        else:
            r._load_markets = AsyncMock(side_effect=asyncio.CancelledError() if failure == 'cancel' else RuntimeError('offline markets failed'))
        with pytest.raises(asyncio.CancelledError if failure == 'cancel' else Exception):
            await r.initialize()
        await r.close()
        assert not r.is_connected() and r.signer_client is None and r.api_client is None
    run(scenario())
    assert all(client.close.await_count == 1 for client in signers + apis)


def test_websocket_explicit_reconnect_rebuilds_only_its_own_signer(lifecycle_clients, monkeypatch):
    config, signers, _, _ = lifecycle_clients
    monkeypatch.setattr(asyncio, 'sleep', AsyncMock())
    async def scenario():
        r = LighterRest(config); await r.initialize()
        signer = r.signer_client; ws_signer = r._websocket.signer_client
        r._websocket._resubscribe_all = AsyncMock()
        await r._websocket.reconnect()
        assert ws_signer.close.await_count == 1 and signer.close.await_count == 0
        assert r._websocket.signer_client is not ws_signer
        await r.close()
    run(scenario())
    assert all(client.close.await_count == 1 for client in signers)


def lifecycle_adapter(config):
    from core.adapters.exchanges.adapters.lighter_base import LighterBase
    from core.adapters.exchanges.adapters.lighter_websocket import LighterWebSocket
    a = object.__new__(LighterAdapter)
    a._base = LighterBase(config); a._rest = LighterRest(config); a._websocket = LighterWebSocket(config)
    a.logger = Mock(); a._connected = False; a._authenticated = False; a.strict_order_state = True
    a._config_dict = config; a._load_market_info = AsyncMock()
    for name in ('_position_cache', '_order_cache', '_order_cache_by_symbol', '_balance_cache'):
        setattr(a, name, {})
    a._position_callbacks = []; a._order_callbacks = []
    return a


def test_adapter_full_reconnect_and_repeated_disconnect_release_all_four_owners(lifecycle_clients):
    config, signers, apis, _ = lifecycle_clients
    async def scenario():
        a = lifecycle_adapter(config)
        assert len(signers) == 4
        assert await a.connect()
        await a.disconnect(); await a.disconnect()
        assert not a._connected and not a._authenticated
        assert await a.connect()
        assert a._websocket.signer_client is not None
        await a.disconnect()
    run(scenario())
    assert all(client.close.await_count == 1 for client in signers + apis)


def test_close_failure_does_not_skip_other_independent_clients(lifecycle_clients):
    config, signers, apis, _ = lifecycle_clients
    async def scenario():
        a = lifecycle_adapter(config); await a.connect()
        a._websocket.signer_client.close.side_effect = RuntimeError('offline close failed')
        await a.disconnect(); await a.disconnect()
    run(scenario())
    assert all(client.close.await_count == 1 for client in signers + apis)


def test_sync_restart_keeps_task_and_closes_old_owners_before_swap(lifecycle_clients):
    config, signers, apis, events = lifecycle_clients
    async def scenario():
        a = lifecycle_adapter(config); await a.connect()
        old_rest, old_ws = a._rest, a._websocket
        old_signers = [old_rest.signer_client, old_rest._websocket.signer_client, old_ws.signer_client]
        count = len(signers)
        a.restart_connections(); task = a._restart_task; a.restart_connections()
        assert a._restart_task is task and a._rest is old_rest
        await task
        assert all(client.close.await_count == 1 for client in old_signers)
        for client in old_signers:
            index = signers.index(client)
            assert events.index(('close', index)) < events.index(('create', count))
        assert a._rest is not old_rest and a._websocket is not old_ws
        assert not a._connected and not a._authenticated
        assert await a.connect()
        await a.disconnect()
    run(scenario())
    assert all(client.close.await_count == 1 for client in signers + apis)


@pytest.mark.parametrize('changed', ['uncertain', 'locked'])
def test_scheduled_restart_rechecks_strict_state_before_closing(lifecycle_clients, changed):
    config, signers, _, _ = lifecycle_clients
    async def scenario():
        a = lifecycle_adapter(config); old_rest = a._rest
        a.restart_connections()
        if changed == 'uncertain': old_rest._uncertain_orders['41'] = {'nonce':11}
        else: await old_rest._strict_write_lock.acquire()
        await a._restart_task
        assert a._rest is old_rest and all(client.close.await_count == 0 for client in signers)
        if changed == 'locked': old_rest._strict_write_lock.release()
        await a.disconnect()
    run(scenario())


def test_restart_partial_construction_failure_cleans_new_owners_and_stays_disconnected(lifecycle_clients, monkeypatch):
    config, signers, apis, _ = lifecycle_clients
    from core.adapters.exchanges.adapters import lighter as module
    async def scenario():
        a = lifecycle_adapter(config); await a.connect()
        monkeypatch.setattr(module, 'LighterWebSocket', Mock(side_effect=RuntimeError('offline new WS failed')))
        a.restart_connections(); await a._restart_task
        assert not a._connected and not a._authenticated
        a.logger.error.assert_called_once()
        assert not any('资源已重建' in str(call) for call in a.logger.info.call_args_list)
        await a.disconnect()
    run(scenario())
    assert all(client.close.await_count == 1 for client in signers + apis)


@pytest.mark.parametrize('phase', ['scheduled', 'cleanup'])
def test_cancelled_restart_cannot_skip_disconnect_cleanup_or_poison_next_connect(lifecycle_clients, phase):
    config, signers, apis, _ = lifecycle_clients
    async def scenario():
        a = lifecycle_adapter(config); await a.connect()
        if phase == 'cleanup':
            entered = asyncio.Event(); original = a._websocket.disconnect
            async def blocking():
                entered.set()
                try: await asyncio.Event().wait()
                finally: await original()
            a._websocket.disconnect = blocking
        a.restart_connections(); task = a._restart_task
        if phase == 'cleanup': await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError): await task
        if phase == 'cleanup': a._websocket.disconnect = original
        with pytest.raises(asyncio.CancelledError): await a.disconnect()
        assert a._restart_task is None and not a._connected
        assert all(client.close.await_count == 1 for client in signers + apis)
        assert await a.connect()
        await a.disconnect(); await a.disconnect()
    run(scenario())
    assert all(client.close.await_count == 1 for client in signers + apis)


def test_completed_restart_waiter_preserves_new_task_created_by_done_callback(lifecycle_clients):
    config, signers, apis, _ = lifecycle_clients
    async def scenario():
        a = lifecycle_adapter(config)
        a.restart_connections(); first = a._restart_task
        entered = asyncio.Event(); release = asyncio.Event(); second_tasks = []
        def schedule_again(done):
            original = a._websocket.disconnect
            async def blocked():
                entered.set()
                await release.wait()
                await original()
            a._websocket.disconnect = blocked
            a.restart_connections(); second_tasks.append(a._restart_task)
        first.add_done_callback(schedule_again)
        await a._wait_for_restart()
        await entered.wait()
        assert a._restart_task is second_tasks[0] and not a._restart_task.done()
        release.set(); await a._wait_for_restart()
        assert a._restart_task is None
        await a.disconnect()
    run(scenario())
    assert all(client.close.await_count == 1 for client in signers + apis)


def prepare_lifecycle_strict_write(a):
    """Use the actual adapter/REST funnels with only signing and transport faked."""
    a._normalize_symbol = Mock(side_effect=lambda symbol: symbol)
    r = a._rest
    r.signer_client.get_api_key_nonce = Mock(return_value=(2, 11))
    r.signer_client.sign_create_order = Mock(return_value=(14, 'offline signed', 'hash', None))
    r.signer_client.send_tx = AsyncMock(side_effect=TimeoutError('offline lost ack'))
    r._prepare_reduce_only_quantity = Mock(return_value={'quantity':D('.0001'), 'skip':False})
    r._build_signed_market_order_tx = AsyncMock(return_value={
        'tx_type':14, 'tx_info':'offline signed', 'tx_hash':'hash', 'api_key_index':2, 'nonce':11,
        'context':{'client_order_id':41, 'symbol':SYMBOL, 'side':'buy',
                   'quantity':D('.0001'), 'avg_execution_price':D('85000')}})
    r._websocket.send_tx_batch = AsyncMock(side_effect=TimeoutError('offline lost ack'))
    return r


@pytest.mark.parametrize('path', ['adapter_ws', 'queued_create', 'queued_cancel', 'queued_ws'])
def test_restart_cleanup_rejects_old_generation_writes_before_sign_or_send(lifecycle_clients, path):
    config, signers, apis, _ = lifecycle_clients
    config['strict_order_state'] = True
    async def scenario():
        a = lifecycle_adapter(config); await a.connect()
        r = prepare_lifecycle_strict_write(a)
        signer = r.signer_client
        entered = asyncio.Event(); release = asyncio.Event()
        original = a._websocket.disconnect
        async def blocked_cleanup():
            entered.set(); await release.wait(); await original()
        a._websocket.disconnect = blocked_cleanup
        a.restart_connections(); restart = a._restart_task
        await entered.wait()
        assert r._retired and r._strict_write_lock.locked() and not a._connected
        orders = [{'symbol':SYMBOL, 'side':'buy', 'quantity':D('.0001')}]
        if path == 'adapter_ws':
            write = asyncio.create_task(a.place_market_orders_ws_batch(orders))
        elif path == 'queued_ws':
            write = asyncio.create_task(r.place_market_orders_via_ws_batch(orders))
        else:
            write = asyncio.create_task(r._strict_submit(signer.sign_create_order, {'market_index':1},
                                                        '41', is_cancel=path == 'queued_cancel'))
        await asyncio.sleep(0)
        assert not write.done()
        release.set(); await restart
        with pytest.raises(RuntimeError, match='retired'): await write
        signer.get_api_key_nonce.assert_not_called(); signer.sign_create_order.assert_not_called()
        signer.send_tx.assert_not_awaited(); r._build_signed_market_order_tx.assert_not_awaited()
        r._websocket.send_tx_batch.assert_not_awaited()
        assert not r._uncertain_orders and a._rest is not r
        assert not a._connected and not a._authenticated
        assert await a.connect()
        await a.disconnect()
    run(scenario())
    assert all(client.close.await_count == 1 for client in signers + apis)


@pytest.mark.parametrize('path', ['rest', 'adapter_ws'])
def test_restart_during_send_keeps_lost_ack_owner_and_nonce(lifecycle_clients, path):
    config, _, _, _ = lifecycle_clients
    config['strict_order_state'] = True
    async def scenario():
        a = lifecycle_adapter(config); await a.connect()
        r = prepare_lifecycle_strict_write(a)
        entered = asyncio.Event(); release = asyncio.Event()
        async def lost_ack(*args, **kwargs):
            entered.set(); await release.wait(); raise TimeoutError('offline lost ack')
        if path == 'rest':
            r.signer_client.send_tx.side_effect = lost_ack
            write = asyncio.create_task(r._strict_submit(r.signer_client.sign_create_order,
                                                        {'market_index':1}, '41'))
        else:
            r._websocket.send_tx_batch.side_effect = lost_ack
            write = asyncio.create_task(a.place_market_orders_ws_batch(
                [{'symbol':SYMBOL, 'side':'buy', 'quantity':D('.0001')}]))
        await entered.wait()
        a.restart_connections()
        assert getattr(a, '_restart_task', None) is None
        release.set()
        with pytest.raises(OrderStateUnknownError): await write
        a.restart_connections()
        assert a._rest is r and a._uncertain['41']['nonce'] == 11
        assert getattr(a, '_restart_task', None) is None and not r._retired
        transport = r.signer_client.send_tx if path == 'rest' else r._websocket.send_tx_batch
        assert transport.await_count == 1
        await a.disconnect()
    run(scenario())


def test_old_initialize_cannot_reactivate_retired_strict_generation(lifecycle_clients):
    config, signers, apis, _ = lifecycle_clients
    config['strict_order_state'] = True
    async def scenario():
        a = lifecycle_adapter(config); await a.connect()
        r = a._rest
        a.restart_connections(); await a._restart_task
        await r.initialize()
        assert r._retired
        signer = r.signer_client
        signer.get_api_key_nonce = Mock(); signer.sign_create_order = Mock()
        with pytest.raises(RuntimeError, match='retired'):
            await r._strict_submit(signer.sign_create_order, {'market_index':1}, '41')
        signer.get_api_key_nonce.assert_not_called(); signer.sign_create_order.assert_not_called()
        signer.cancel_all_orders = AsyncMock(); signer.sign_cancel_order = Mock(); signer.send_tx = AsyncMock()
        r.get_market_index = Mock(return_value=1)
        r.get_open_orders = AsyncMock(return_value=[order()]); r.get_order = AsyncMock(return_value=order())
        assert await asyncio.wait_for(r.cancel_all_orders(SYMBOL), timeout=1) == []
        signer.cancel_all_orders.assert_not_awaited(); signer.sign_cancel_order.assert_not_called()
        signer.get_api_key_nonce.assert_not_called(); signer.send_tx.assert_not_awaited()
        await r.close(); await a.disconnect()
    run(scenario())
    assert all(client.close.await_count == 1 for client in signers + apis)


@pytest.mark.parametrize('failure', ['cancel', 'error'])
def test_failed_cleanup_leaves_old_generation_retired_and_queued_write_rejected(lifecycle_clients, failure):
    config, signers, apis, _ = lifecycle_clients
    config['strict_order_state'] = True
    async def scenario():
        a = lifecycle_adapter(config); await a.connect()
        r = prepare_lifecycle_strict_write(a); signer = r.signer_client
        entered = asyncio.Event(); release = asyncio.Event(); original = a._websocket.disconnect
        async def blocked_cleanup():
            entered.set()
            try:
                await release.wait()
                if failure == 'error': raise RuntimeError('offline cleanup failed')
            finally: await original()
        a._websocket.disconnect = blocked_cleanup
        a.restart_connections(); restart = a._restart_task
        await entered.wait()
        write = asyncio.create_task(r._strict_submit(signer.sign_create_order, {'market_index':1}, '41'))
        await asyncio.sleep(0)
        if failure == 'cancel':
            restart.cancel()
            with pytest.raises(asyncio.CancelledError): await restart
        else:
            release.set(); await restart
        with pytest.raises(RuntimeError, match='retired'): await write
        assert a._rest is r and r._retired and not a._connected and not a._authenticated
        assert not r._strict_write_lock.locked()
        signer.get_api_key_nonce.assert_not_called(); signer.send_tx.assert_not_awaited()
        a._websocket.disconnect = original
        if failure == 'cancel':
            with pytest.raises(asyncio.CancelledError): await a.disconnect()
        else: await a.disconnect()
        await r.initialize()
        assert r._retired
        with pytest.raises(RuntimeError, match='retired'):
            await r.place_market_orders_via_ws_batch([{'symbol':SYMBOL, 'side':'buy', 'quantity':D('.0001')}])
        await r.close(); await a.disconnect()
    run(scenario())
    assert all(client.close.await_count == 1 for client in signers + apis)


@pytest.mark.parametrize('method', ['set_margin_mode', 'cancel_all_orders'])
def test_native_sdk_queued_old_owner_write_is_rejected_and_fallback_does_not_deadlock(lifecycle_clients, method):
    config, signers, apis, _ = lifecycle_clients
    config['strict_order_state'] = True
    async def scenario():
        a = lifecycle_adapter(config); await a.connect()
        r = a._rest; signer = r.signer_client
        signer.update_leverage = AsyncMock(return_value=(True, 'hash', None))
        signer.cancel_all_orders = AsyncMock(return_value=(True, 'hash', None))
        signer.sign_cancel_order = Mock()
        r._get_market_info = AsyncMock(return_value={'market_index':1})
        r.get_open_orders = AsyncMock(return_value=[order()])
        entered = asyncio.Event(); release = asyncio.Event(); original = a._websocket.disconnect
        async def blocked():
            entered.set(); await release.wait(); await original()
        a._websocket.disconnect = blocked
        a.restart_connections(); restart = a._restart_task
        await entered.wait()
        call = r.set_margin_mode(SYMBOL, 'cross', 2) if method == 'set_margin_mode' else r.cancel_all_orders(SYMBOL)
        write = asyncio.create_task(call)
        await asyncio.sleep(0)
        assert not write.done()
        release.set(); await restart
        result = await asyncio.wait_for(write, timeout=1)
        assert result is False if method == 'set_margin_mode' else result == []
        signer.update_leverage.assert_not_awaited(); signer.cancel_all_orders.assert_not_awaited()
        signer.sign_cancel_order.assert_not_called()
        assert r._retired and not r._strict_write_lock.locked()
        await a.disconnect()
    run(scenario())
    assert all(client.close.await_count == 1 for client in signers + apis)


@pytest.mark.parametrize('method', ['update_leverage', 'cancel_all_orders'])
def test_native_sdk_inflight_write_prevents_owner_swap(lifecycle_clients, method):
    config, _, _, _ = lifecycle_clients
    config['strict_order_state'] = True
    async def scenario():
        a = lifecycle_adapter(config); await a.connect()
        r = a._rest; entered = asyncio.Event(); release = asyncio.Event()
        async def send(**params):
            entered.set(); await release.wait(); return True, 'hash', None
        setattr(r.signer_client, method, AsyncMock(side_effect=send))
        write = asyncio.create_task(r._call_sdk_write(method, market_index=1))
        await entered.wait(); a.restart_connections()
        assert getattr(a, '_restart_task', None) is None and a._rest is r and not r._retired
        release.set(); assert await write == (True, 'hash', None)
        assert not r._strict_write_lock.locked()
        await a.disconnect()
    run(scenario())


def test_native_sdk_margin_mode_preserves_boolean_protocol_and_releases_lock(lifecycle_clients):
    config, _, _, _ = lifecycle_clients
    config['strict_order_state'] = True
    async def scenario():
        r = LighterRest(config); await r.initialize()
        r._get_market_info = AsyncMock(return_value={'market_index':1})
        r.signer_client.update_leverage = AsyncMock(return_value=(True, 'hash', None))
        assert await r.set_margin_mode(SYMBOL, 'cross', 2) is True
        assert r.signer_client.update_leverage.call_args.kwargs == {'market_index':1, 'margin_mode':0, 'leverage':2}
        r.signer_client.update_leverage.return_value = (None, None, 'offline rejected')
        assert await r.set_margin_mode(SYMBOL, 'cross', 2) is False
        r.signer_client.update_leverage.side_effect = asyncio.CancelledError()
        with pytest.raises(asyncio.CancelledError): await r.set_margin_mode(SYMBOL, 'cross', 2)
        assert not r._strict_write_lock.locked()
        await r.close()
    run(scenario())


def test_native_bulk_cancel_fallback_can_reenter_strict_single_cancel_without_deadlock(lifecycle_clients):
    config, _, _, _ = lifecycle_clients
    config['strict_order_state'] = True
    async def scenario():
        r = LighterRest(config); await r.initialize()
        r._get_market_info = AsyncMock(return_value={'market_index':1})
        r.get_market_index = Mock(return_value=1)
        r.get_open_orders = AsyncMock(return_value=[order()])
        r.get_order = AsyncMock(return_value=order())
        signer = r.signer_client
        signer.cancel_all_orders = AsyncMock(return_value=(None, None, 'offline bulk rejected'))
        signer.get_api_key_nonce = Mock(return_value=(2, 11))
        signer.sign_cancel_order = Mock(return_value=(15, 'offline cancel', 'hash', None))
        signer.send_tx = AsyncMock(return_value=SimpleNamespace(code=200, tx_hash='hash'))
        cancelled = await asyncio.wait_for(r.cancel_all_orders(SYMBOL), timeout=1)
        assert len(cancelled) == 1 and cancelled[0].id == '41'
        assert signer.cancel_all_orders.await_count == signer.send_tx.await_count == 1
        assert signer.sign_cancel_order.call_count == 1 and not r._strict_write_lock.locked()
        await r.close()
    run(scenario())


@pytest.mark.parametrize('scale', [1, 1000, 1000000, 1000000000])
@pytest.mark.parametrize('convert', [int, str, D])
def test_book_epoch_units(scale, convert):
    from core.adapters.exchanges.adapters.lighter_websocket import LighterWebSocket
    ws = object.__new__(LighterWebSocket)
    assert ws._parse_timestamp_value(convert(1791700000 * scale)) == datetime.fromtimestamp(1791700000)


@pytest.mark.parametrize('fields,expected', [
    ({'last_updated_at':1791700000123}, 1791700000.123),
    ({'timestamp':1791700000,'last_updated_at':1791700010123}, 1791700000),
    *[({'timestamp':bad,'last_updated_at':'1791700000123'},1791700000.123)
      for bad in (None, 0, -1, '', 'invalid', 'NaN', float('inf'))],
    ({}, None), ({'timestamp':'invalid','last_updated_at':0}, None),
])
def test_book_timestamp_fields_reach_snapshot_increment_and_direct(fields, expected):
    from core.adapters.exchanges.adapters.lighter_websocket import LighterWebSocket
    ws = object.__new__(LighterWebSocket); ws._local_orderbooks = {}
    payload = {'bids':[['100','1']], 'asks':[['101','1']], **fields}
    parsed = datetime.fromtimestamp(expected) if expected is not None else None
    ws._initialize_orderbook(1, payload)
    assert ws._local_orderbooks[1]['timestamp'] == parsed
    direct = ws._parse_order_book(SYMBOL, payload)
    assert direct.exchange_timestamp == parsed
    old = datetime.fromtimestamp(1791600000)
    ws._local_orderbooks[1]['timestamp'] = old
    ws._apply_orderbook_update(1, payload)
    built = ws._build_orderbook_from_local(SYMBOL, 1)
    assert built.exchange_timestamp == (parsed or old)
    assert built.timestamp == (parsed or old)


def test_direct_book_waits_for_snapshot_and_preserves_untouched_depth():
    from core.adapters.exchanges.adapters.lighter_websocket import LighterWebSocket
    ws = object.__new__(LighterWebSocket)
    ws.auth_enabled = False
    ws._local_orderbooks = {}
    ws._get_symbol_from_market_index = Mock(return_value=SYMBOL)
    ws._handle_orderbook_data = Mock()
    snapshot = {'bids':[['100','2'],['99','3']], 'asks':[['101','4'],['102','5']]}
    delta = {'bids':[['100','0'],['98','6']], 'asks':[['101','7']]}
    async def dispatch(kind, book):
        await ws._handle_direct_ws_message({'type':kind+'/order_book', 'channel':'order_book:1', 'order_book':book})
    async def scenario():
        for _ in range(2):
            ws._local_orderbooks.clear()
            ws._handle_orderbook_data.reset_mock()
            ws._apply_orderbook_update(1, delta)
            await dispatch('update', delta)
            await dispatch('subscribed', {'timestamp':1791700000})
            assert not ws._local_orderbooks
            ws._handle_orderbook_data.assert_not_called()
            await dispatch('subscribed', snapshot)
            assert ws._handle_orderbook_data.call_count == 1
            book = ws._handle_orderbook_data.call_args.kwargs['order_book_data']
            assert len(book.bids) == len(book.asks) == 2
            await dispatch('update', delta)
            book = ws._handle_orderbook_data.call_args.kwargs['order_book_data']
            assert [(x.price,x.size) for x in book.bids] == [(D('99'),D('3')),(D('98'),D('6'))]
            assert [(x.price,x.size) for x in book.asks] == [(D('101'),D('7')),(D('102'),D('5'))]
        ws._local_orderbooks.clear()
        await dispatch('subscribed', {'bids':[], 'asks':[['101','1']]})
        assert 1 in ws._local_orderbooks and not ws._local_orderbooks[1]['bids']
    run(scenario())
