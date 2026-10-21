"""Arcus offline contracts. Fixed test key, fake transport; never contacts an exchange."""
import asyncio
import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
import yaml
from nacl.signing import SigningKey
from core.adapters.exchanges.adapters.arcus import ArcusAdapter, ArcusRequestRejectedError
from core.adapters.exchanges.interface import ExchangeConfig
from core.adapters.exchanges.models import ExchangeType, OrderSide, OrderType, OrderStatus
from core.adapters.exchanges.order_state import OrderStateUnknownError
from core.services.arbitrage_monitor_v2.execution.arbitrage_executor import ArbitrageExecutor, ExecutionConfig, ExecutionRequest, ExecutionResult

D=Decimal
SEED='01'*32
ADDRESS='0x'+'12'*20
SYMBOL='BTC-USDC-PERP'
MARKET={'marketDisplayName':'BTC-USD','marketId':1,'status':'ONLINE','tickSize':'.1','stepSize':'.00000001','tickTiers':[{'upToPrice':'100000','tick':'.1'},{'tick':'1'}],'minOrderSize':'.0001','maxOrderSize':'100','minOrderNotional':'5','markPrice':'50000','fundingRate':'.00001','nextFundingRate':'.00002','nextFundingAt':1791043200}

def run(coro): return asyncio.run(coro)
def adapter(index=0, **extra):
    cfg=ExchangeConfig('arcus','Arcus',ExchangeType.PERPETUAL,SigningKey(bytes.fromhex(SEED)).verify_key.encode().hex(),'',wallet_address=ADDRESS,extra_params={'api_signing_key':SEED,'account_index':index,'reconcile_timeout':0,**extra})
    a=ArcusAdapter(cfg); a._set_markets([dict(MARKET)]); return a

def row(order_id='O1', status='OPEN', filled='0', index=0, seq=1, **extra):
    return {'address':ADDRESS,'accountIndex':index,'orderId':order_id,'clientId':'Client-A','marketId':1,'marketDisplayName':'BTC-USD','side':'BUY','type':'LIMIT','price':'50000','originalSize':'1','remainingSize':str(1-D(filled)),'status':status,'avgFillPrice':'50000','sequenceNumber':seq,'updatedAt':1791179000000000,**extra}

def frame(channel,contents,index=0,type='subscribed',address=ADDRESS):
    return {'type':type,'channel':channel,'id':address,'accountIndex':index,'contents':contents}


def test_factory_mapping_and_network_guard():
    from core.adapters.exchanges.factory import ExchangeFactory
    from core.services.arbitrage_monitor.utils.symbol_converter import SimpleSymbolConverter
    a=adapter(); assert isinstance(ExchangeFactory().create_adapter('arcus',a.config),ArcusAdapter)
    converter=SimpleSymbolConverter()
    assert converter.convert_to_exchange(SYMBOL,'arcus')=='BTC-USD'
    assert converter.convert_from_exchange('BTC-USD','arcus')==SYMBOL
    a.config.testnet=True
    a.config.base_url='https://api.arcus.xyz'
    with pytest.raises(ValueError): ArcusAdapter(a.config)


def test_signing_golden_typed_payload_and_no_wallet_key_fallback():
    a=adapter(); payload={'ad':ADDRESS,'ai':0,'c':'Client-A','ct':1791041528000000000,'g':1793806328000000000,'m':1,'op':1,'p':500000,'q':100000,'r':0,'s':0,'t':2,'v':1}
    canonical=b'{"ad":"0x1212121212121212121212121212121212121212","ai":0,"c":"Client-A","ct":1791041528000000000,"g":1793806328000000000,"m":1,"op":1,"p":500000,"q":100000,"r":0,"s":0,"t":2,"v":1}'
    # Independent verifier checks the exact signing bytes and client-id case.
    SigningKey(bytes.fromhex(SEED)).verify_key.verify(canonical,bytes.fromhex(a._sign(payload)))
    assert a._sign(payload)!=a._sign({**payload,'c':'client-a'})
    a.api_key='ff'*32
    with pytest.raises(ValueError): a._sign(payload)


def test_ack_is_not_fill_and_unknown_submit_never_replayed():
    a=adapter(); calls=[]
    async def request(method,path,**kwargs):
        calls.append((method,path,kwargs))
        if method=='POST': return 202,{'orderId':'O1','status':'ACK'}
        return 200,row(status='ACK')
    a._request=request
    with pytest.raises(OrderStateUnknownError): run(a.create_order(SYMBOL,OrderSide.BUY,OrderType.LIMIT,D('.001'),D('50000')))
    assert a._orders['O1'].status==OrderStatus.PENDING
    with pytest.raises(OrderStateUnknownError): run(a.create_order(SYMBOL,OrderSide.BUY,OrderType.LIMIT,D('.001'),D('50000')))
    assert sum(method=='POST' for method,_,_ in calls)==1
    payload=calls[0][2]['body']
    assert payload['timestamp']>10**18
    assert int(payload['goodTilTime'])*1000-payload['timestamp']>=31*24*3600*10**9


@pytest.mark.parametrize('bad_ack',[None,[],{},'not-json'])
def test_2xx_malformed_ack_keeps_write_blocked(bad_ack):
    a=adapter(); a._request=AsyncMock(return_value=(202,bad_ack))
    with pytest.raises(OrderStateUnknownError): run(a.create_order(SYMBOL,OrderSide.BUY,OrderType.LIMIT,D('.001'),D('50000')))
    with pytest.raises(OrderStateUnknownError): run(a.create_order(SYMBOL,OrderSide.BUY,OrderType.LIMIT,D('.001'),D('50000')))
    assert a._request.await_count==1


def test_definitive_rejection_does_not_block_future_new_order():
    a=adapter(); a._request=AsyncMock(side_effect=ArcusRequestRejectedError('Arcus HTTP 400'))
    with pytest.raises(ArcusRequestRejectedError): run(a.create_order(SYMBOL,OrderSide.BUY,OrderType.LIMIT,D('.001'),D('50000')))
    assert not a._uncertain


def test_market_ioc_protective_price_and_rest_reconciled_partial_terminal():
    a=adapter(); calls=[]
    async def request(method,path,**kwargs):
        calls.append((method,path,kwargs))
        if 'l2OrderBook' in path: return 200,{'bids':[['49999.9','1']],'asks':[['50000','1']]}
        if method=='POST': return 202,{'orderId':'O1','status':'ACK'}
        return 200,row(status='CANCELED', filled='.0004', originalSize='.001',remainingSize='.0006',timeInForce='IOC',state='PARTIALLY_FILLED')
    a._request=request
    order=run(a.create_order(SYMBOL,OrderSide.BUY,OrderType.MARKET,D('.001'),params={'reduce_only':True}))
    assert order.status==OrderStatus.CANCELED and order.filled==D('.0004')
    body=next(c[2]['body'] for c in calls if c[0]=='POST')
    assert body['timeInForce']=='IOC' and body['reduceOnly'] is True
    assert D(body['price'])==D('50250')


@pytest.mark.parametrize('amount,price',[(D('.00001'),D('50000')),(D('.001000001'),D('50000')),(D('.001'),D('50000.01')),(D('.001'),D('100000.1'))])
def test_precision_and_tick_tiers_no_writes(amount,price):
    a=adapter(); a._request=AsyncMock()
    with pytest.raises(ValueError): run(a.create_order(SYMBOL,OrderSide.BUY,OrderType.LIMIT,amount,price))
    a._request.assert_not_awaited()


def test_cancel_ack_or_rejection_does_not_prove_original_order_closed():
    a=adapter(); a._merge_order(row())
    a._request=AsyncMock(side_effect=[(202,{'status':'CANCEL_ACKNOWLEDGED'}),(200,row(status='OPEN'))])
    with pytest.raises(OrderStateUnknownError): run(a.cancel_order('O1',SYMBOL))
    assert a._orders['O1'].status==OrderStatus.OPEN
    assert a._request.call_args_list[0].kwargs['body']['kind']=='orderId'


def test_cancel_race_filled_terminal_is_preserved():
    a=adapter(); a._merge_order(row())
    a._request=AsyncMock(side_effect=[(202,{'status':'ACK'}),(200,row(status='FILLED',filled='1',seq=3))])
    order=run(a.cancel_order('O1',SYMBOL))
    assert order.status==OrderStatus.FILLED and order.filled==1 and not a._uncertain


def test_order_fill_snapshot_duplicate_late_and_subaccount_filter():
    async def scenario():
        a=adapter(index=2); ws=a._websocket; events=[]
        await ws.subscribe_orders(events.append)
        await ws.handle_message(frame('orders',{'orders':[row(index=2,filled='.4',seq=3)]},index=2))
        await ws.handle_message(frame('orders',row(index=2,filled='.1',seq=1),index=2,type='channel_data'))
        assert a._orders['O1'].filled==D('.4')
        await ws.handle_message(frame('orders',row('OTHER',index=1,status='FILLED',filled='1'),index=1))
        assert 'OTHER' not in a._orders
        fill={'tradeId':'T1','orderId':'O1','marketDisplayName':'BTC-USD','marketId':1,'side':'BUY','originalSize':'1','size':'.4','price':'50000'}
        await ws.handle_message(frame('userFills',{'fills':[fill]},index=2))
        await ws.handle_message(frame('userFills',fill,index=2,type='channel_data'))
        assert a._orders['O1'].filled==D('.4') and len(a._fill_ids)==1
        await ws.handle_message(frame('orders',row(index=2,status='FILLED',filled='1',seq=4),index=2,type='channel_data'))
        await ws.handle_message(frame('orders',row(index=2,filled='.5',seq=2),index=2,type='channel_data'))
        assert a._orders['O1'].status==OrderStatus.FILLED and a._orders['O1'].filled==1
    run(scenario())


def test_snapshot_book_ticker_reconnect_readiness_empty_positions():
    async def scenario():
        a=adapter(); ws=a._websocket; books=[]; tickers=[]; positions=[]
        await ws.subscribe_orderbook('BTC-USD',lambda *args:books.append(args))
        await ws.subscribe_ticker('BTC-USD',lambda *args:tickers.append(args))
        await ws.subscribe_positions(positions.append)
        snap={'bids':[['49999','1']],'asks':[['50000','2']],'lastSequenceId':10,'timestamp':1791041528000000}
        await ws.handle_message({'type':'subscribed','channel':'l2Orderbook','id':'BTC-USD','contents':snap})
        await ws.handle_message({'type':'channel_data','channel':'l2Orderbook','id':'BTC-USD','contents':{**snap,'lastSequenceId':9}})
        assert len(books)==1 and books[0][0]=='BTC-USD' and books[0][1].symbol==SYMBOL
        await ws.handle_message({'type':'degraded','channel':'l2Orderbook','id':'BTC-USD'})
        await ws.handle_message({'type':'channel_data','channel':'l2Orderbook','id':'BTC-USD','contents':{**snap,'lastSequenceId':11}})
        assert len(books)==1
        await ws.handle_message({'type':'subscribed','channel':'markets','contents':{'markets':{'1':MARKET},'isSnapshot':True}})
        assert tickers[0][1].funding_rate==D('.00008') and tickers[0][1].funding_interval==3600
        await ws.handle_message(frame('positions',{'positions':{},'isSnapshot':True}))
        assert a._positions=={} and positions==[]
    run(scenario())


def executor_pair(a=None,other=None,mode='market'):
    from core.services.arbitrage_monitor_v2.config.arbitrage_config import ExchangeOrderModeConfig
    if a is None:
        a=adapter(); a.create_order=AsyncMock(side_effect=OrderStateUnknownError('arcus'))
    other=other or SimpleNamespace(create_order=AsyncMock(return_value=None))
    cfg=ExecutionConfig(exchange_order_modes={'default':ExchangeOrderModeConfig(order_mode=mode)})
    manager=SimpleNamespace(defer=lambda **kwargs:None)
    e=ArbitrageExecutor(cfg,{'arcus':a,'other':other},monitor_only=False,is_segmented_mode=True,symbol_state_manager=manager)
    request=ExecutionRequest(SYMBOL,'arcus','other',D('50000'),D('50001'),D('.001'))
    return e,request,a,other


@pytest.mark.parametrize('mode',['market','limit','dual_limit'])
def test_executor_unknown_submission_no_retry_no_emergency_compensation(mode):
    e,request,a,other=executor_pair(mode='limit' if mode=='dual_limit' else mode)
    if mode=='dual_limit': e.config.order_execution.enable_dual_limit_mode=True
    with patch.object(e.order_strategy_executor,'_mark_manual_intervention') as manual:
        result=run(e.execute_arbitrage(request))
    assert not result.success and result.failure_code=='ORDER_STATE_UNKNOWN'
    assert a.create_order.await_count==1
    assert other.create_order.await_count==(0 if mode=='limit' else 1)
    manual.assert_called_once()


def test_executor_unknown_cancel_is_not_missing_success():
    a=SimpleNamespace(strict_order_state=True,cancel_order=AsyncMock(side_effect=OrderStateUnknownError('arcus',order_id='O1')))
    e,request,_,_=executor_pair(a=a)
    with pytest.raises(OrderStateUnknownError): run(e._cancel_order(a,'O1',SYMBOL))
    assert a.cancel_order.await_count==1


def test_executor_unknown_fill_stops_aggressive_fallback():
    a=adapter(); order=a._parse_order(row())
    e,request,_,_=executor_pair(a=a)
    e.order_monitor.wait_for_order_fill=AsyncMock(return_value=None)
    with patch.object(e,'_place_limit_order',new=AsyncMock()) as fallback:
        with pytest.raises(OrderStateUnknownError):
            run(e.order_strategy_executor._confirm_market_fill_with_retries(adapter=a,symbol=SYMBOL,quantity=D('1'),is_buy=True,reduce_only=False,request=request,exchange_name='arcus',initial_order=order))
        fallback.assert_not_awaited()


def test_live_profile_loading_and_explicit_monitor_override_zero_writes():
    from core.services.arbitrage_monitor_v2.config.unified_config_manager import UnifiedConfigManager
    cfg=UnifiedConfigManager(Path('config/arbitrage/arbitrage_lighter_arcus.yaml')).get_unified_config()
    assert cfg.system_mode.monitor_only and cfg.decision.enable_funding_rate_arbitrage
    a=SimpleNamespace(create_order=AsyncMock())
    e=ArbitrageExecutor(cfg.execution,{'arcus':a},monitor_only=True,is_segmented_mode=True)
    request=ExecutionRequest(SYMBOL,'arcus','lighter',D('50000'),D('50001'),D('.001'))
    assert run(e.execute_arbitrage(request)).success
    a.create_order.assert_not_awaited()


def test_unknown_pair_repeat_no_other_leg_writes_even_after_auto_resume():
    e,request,a,other=executor_pair()
    assert run(e.execute_arbitrage(request)).failure_code=='ORDER_STATE_UNKNOWN'
    before=(a.create_order.await_count,other.create_order.await_count)
    # The execution latch survives any external UI/risk state auto-resume.
    e.symbol_state_manager=SimpleNamespace(defer=lambda **kwargs:None)
    assert run(e.execute_arbitrage(request)).failure_code=='ORDER_STATE_UNKNOWN'
    assert (a.create_order.await_count,other.create_order.await_count)==before
    reverse=ExecutionRequest(SYMBOL,'other','arcus',D('50000'),D('50001'),D('.001'))
    assert run(e.execute_arbitrage(reverse)).failure_code=='ORDER_STATE_UNKNOWN'
    assert (a.create_order.await_count,other.create_order.await_count)==before


def test_shortfall_cancel_false_never_compensates():
    a=adapter(); initial=a._parse_order(row()); opposite=a._parse_order(row('O2',status='FILLED',filled='1'))
    e,request,_,other=executor_pair(a=a)
    with patch.object(e,'_cancel_order',new=AsyncMock(return_value=False)), patch.object(e.order_strategy_executor,'_place_aggressive_limit_retry_order',new=AsyncMock()) as compensation:
        with pytest.raises(OrderStateUnknownError):
            run(e.order_strategy_executor._handle_single_leg_shortfall(request=request,buy_order=initial,sell_order=opposite,buy_adapter=a,sell_adapter=other,buy_symbol=SYMBOL,sell_symbol=SYMBOL,missing_buy=True,shortfall=D('1'),target_quantity=D('1'),epsilon=D('.00000001')))
        compensation.assert_not_awaited()


def test_late_fill_after_cancel_recomputes_difference_before_compensation():
    a=adapter(); initial=a._parse_order(row()); updated=a._parse_order(row(status='CANCELED',filled='.4',seq=2))
    opposite=a._parse_order(row('O2',status='FILLED',filled='1'))
    a.get_order=AsyncMock(return_value=updated)
    other=SimpleNamespace(get_order=AsyncMock(return_value=opposite))
    e,request,_,_=executor_pair(a=a,other=other)
    compensation=AsyncMock(side_effect=OrderStateUnknownError('arcus'))
    with patch.object(e,'_cancel_order',new=AsyncMock(return_value=True)), patch.object(e.order_strategy_executor,'_place_aggressive_limit_retry_order',new=compensation):
        with pytest.raises(OrderStateUnknownError):
            run(e.order_strategy_executor._handle_single_leg_shortfall(request=request,buy_order=initial,sell_order=opposite,buy_adapter=a,sell_adapter=other,buy_symbol=SYMBOL,sell_symbol=SYMBOL,missing_buy=True,shortfall=D('1'),target_quantity=D('1'),epsilon=D('.00000001')))
    assert compensation.call_args.kwargs['quantity']==D('.6')
    assert a.get_order.await_count==1 and other.get_order.await_count==1


def test_probe_mainnet_trade_rejected_before_network_or_env():
    from tools.arcus_testnet_probe import probe
    args=SimpleNamespace(trade=True,network='mainnet')
    with patch('tools.arcus_testnet_probe.ArcusAdapter') as construct, patch('dotenv.load_dotenv') as env:
        with pytest.raises(ValueError,match='testnet'): run(probe(args))
        construct.assert_not_called(); env.assert_not_called()


@pytest.mark.parametrize('failure',['malformed','timeout','server'])
def test_real_transport_unknown_after_submit_blocks_duplicate(failure):
    a=adapter(); requests=[]
    class Response:
        status=503 if failure=='server' else 202
        async def __aenter__(self):
            if failure=='timeout': raise asyncio.TimeoutError()
            return self
        async def __aexit__(self,*args): pass
        async def json(self): raise json.JSONDecodeError('invalid','!',0)
    def request(*args,**kwargs):
        requests.append((args,kwargs)); return Response()
    a._session=SimpleNamespace(closed=False,request=request)
    with pytest.raises(OrderStateUnknownError): run(a.create_order(SYMBOL,OrderSide.BUY,OrderType.LIMIT,D('.001'),D('50000')))
    with pytest.raises(OrderStateUnknownError): run(a.create_order(SYMBOL,OrderSide.BUY,OrderType.LIMIT,D('.001'),D('50000')))
    assert len(requests)==1 and a._uncertain


def test_resolve_quantity_unknown_propagates_without_nameerror():
    a=adapter(); a.get_order=AsyncMock(side_effect=OrderStateUnknownError('arcus'))
    e,request,_,_=executor_pair(a=a)
    with pytest.raises(OrderStateUnknownError):
        run(e.order_strategy_executor._resolve_filled_quantity(order=a._parse_order(row()),adapter=a,symbol=SYMBOL,reported_filled=None))


def test_fills_no_original_size_preserve_amount_and_never_infer_terminal():
    a=adapter(); ws=a._websocket
    fill={'tradeId':'partial-1','orderId':'O1','marketId':1,'marketDisplayName':'BTC-USD','side':'BUY','size':'.2','price':'50000'}
    async def scenario():
        await ws.handle_message(frame('userFills',{'fills':[fill]}))
        assert 'O1' not in a._orders and 'O1' in a._uncertain
        await ws.handle_message(frame('orders',{'orders':[row(filled='.2')]}))
        o=a._orders['O1']; assert o.amount==1 and o.filled==D('.2') and o.status==OrderStatus.OPEN
        await ws.handle_message(frame('userFills',{'fills':[fill,{**fill,'tradeId':'partial-2','size':'.1'}]},type='channel_data'))
        o=a._orders['O1']; assert o.amount==1 and o.filled==D('.2') and o.status==OrderStatus.OPEN
        assert 'O1' in a._uncertain
        await ws.handle_message(frame('orders',{'orders':[row(seq=0)]},type='channel_data'))
        assert a._orders['O1'].filled==D('.2') and a._orders['O1'].status==OrderStatus.OPEN
    run(scenario())


def test_standard_symbol_idempotent_and_filtered_cancel_real_request():
    a=adapter(); calls=[]
    assert a.standard_symbol(SYMBOL)==SYMBOL and a.standard_symbol('BTC-USD')==SYMBOL
    assert a.native_symbol('BTC-USD')=='BTC-USD' and a.native_symbol(SYMBOL)=='BTC-USD'
    async def request(method,path,**kwargs):
        calls.append((method,path))
        if path=='/v1/openOrders': return 200,{'orders':[row()]}
        if method=='POST': return 200,{'status':'ACK'}
        return 200,row(status='CANCELED')
    a._request=request
    assert len(run(a.get_open_orders(SYMBOL)))==1
    assert len(run(a.cancel_all_orders(SYMBOL)))==1
    assert sum(m=='POST' for m,p in calls)==1


@pytest.mark.parametrize('is_open',[True,False])
@pytest.mark.parametrize('buy_fill,sell_fill',[('.001','.001'),('.0004','.001'),('.001','.0004')])
def test_market_pair_terminal_partial_mismatch_quarantined(is_open,buy_fill,sell_fill):
    a=adapter(); buy=a._parse_order(row(status='FILLED' if buy_fill=='.001' else 'CANCELED',filled=buy_fill,originalSize='.001',remainingSize=str(D('.001')-D(buy_fill))))
    sell=a._parse_order(row('O2',status='FILLED' if sell_fill=='.001' else 'CANCELED',filled=sell_fill,originalSize='.001',remainingSize=str(D('.001')-D(sell_fill))))
    a.create_order=AsyncMock(return_value=buy)
    other=SimpleNamespace(create_order=AsyncMock(return_value=sell))
    e,request,_,_=executor_pair(a=a,other=other); request.is_open=is_open
    e.order_strategy_executor._confirm_market_fill_with_retries=AsyncMock(side_effect=[(buy,D(buy_fill)),(sell,D(sell_fill))])
    result=run(e.execute_arbitrage(request))
    if buy_fill==sell_fill: assert result.success
    else:
        assert not result.success and result.failure_code=='ORDER_STATE_UNKNOWN'
        before=other.create_order.await_count
        assert run(e.execute_arbitrage(request)).failure_code=='ORDER_STATE_UNKNOWN'
        assert other.create_order.await_count==before


@pytest.mark.parametrize('mode',['limit','dual_limit'])
def test_initial_close_limit_reduceonly_and_tick_quantization(mode):
    a=adapter(); recorded=[]
    async def create(**kwargs):
        recorded.append(kwargs)
        raise OrderStateUnknownError('arcus')
    a.create_order=create
    e,request,_,_=executor_pair(a=a,mode='limit')
    request.is_open=False; request.price_buy=D('50000.1')
    if mode=='dual_limit': e.config.order_execution.enable_dual_limit_mode=True
    assert run(e.execute_arbitrage(request)).failure_code=='ORDER_STATE_UNKNOWN'
    assert recorded and recorded[0]['params']['reduce_only'] is True
    assert recorded[0]['price'] % D('.1')==0
    assert recorded[0]['price']==D('50050.1')


@pytest.mark.parametrize('is_buy,base,expected',[(True,'99999.9','100099'),(False,'100050','99950')])
def test_strategy_price_crossing_tick_tier(is_buy,base,expected):
    a=adapter(); a._markets['BTC-USD']['markPrice']='100000'
    a.create_order=AsyncMock(side_effect=OrderStateUnknownError('arcus'))
    e,request,_,_=executor_pair(a=a)
    with pytest.raises(OrderStateUnknownError):
        run(e._place_limit_order(a,SYMBOL,D(base),D('.001'),is_buy=is_buy,request=request))
    price=a.create_order.call_args.kwargs['price']
    assert price==D(expected) and price % a._price_tick(a._market(SYMBOL),price)==0


@pytest.mark.parametrize('reverse',[False,True])
@pytest.mark.parametrize('raises',[False,True])
def test_queued_real_reservation_rechecks_unknown_before_any_leg(reverse,raises):
    from core.services.arbitrage_monitor_v2.execution.arbitrage_executor import ExchangeRateLimiter
    async def scenario():
        e,request,a,other=executor_pair()
        limiter=ExchangeRateLimiter('arcus',max_concurrent=1,min_interval_ms=0,cooldown_ms=0)
        e._get_rate_limiter=lambda exchange: limiter
        started=asyncio.Event(); release=asyncio.Event(); plans=[]
        async def plan(req,grid):
            plans.append(req); started.set(); await release.wait()
            if raises: raise OrderStateUnknownError('arcus')
            return ExecutionResult(success=False,failure_code='ORDER_STATE_UNKNOWN')
        e._run_execution_plan=plan
        first=asyncio.create_task(e.execute_arbitrage(request))
        await started.wait()
        queued=ExecutionRequest(SYMBOL,'other' if reverse else 'arcus','arcus' if reverse else 'other',D('50000'),D('50001'),D('.001'))
        second=asyncio.create_task(e.execute_arbitrage(queued))
        await asyncio.sleep(0)
        assert limiter._semaphore._waiters and not second.done()
        release.set()
        results=await asyncio.gather(first,second)
        assert all(r.failure_code=='ORDER_STATE_UNKNOWN' and not r.success for r in results)
        assert len(plans)==1
        a.create_order.assert_not_awaited(); other.create_order.assert_not_awaited()
    run(scenario())


@pytest.mark.parametrize('is_open',[False,True])
@pytest.mark.parametrize('mode,reverse',[('limit_market',False),('dual_limit',False),('dual_limit',True)])
def test_lighter_arcus_initial_limits_forward_reduceonly_to_real_rest(is_open,mode,reverse):
    from core.adapters.exchanges.adapters.lighter import LighterAdapter
    from core.services.arbitrage_monitor_v2.config.arbitrage_config import ExchangeOrderModeConfig
    lighter=LighterAdapter(ExchangeConfig('lighter','Lighter',ExchangeType.PERPETUAL,'',''))
    lighter._rest.place_order=AsyncMock(side_effect=OrderStateUnknownError('lighter'))
    arcus=adapter(); arcus.create_order=AsyncMock(side_effect=OrderStateUnknownError('arcus'))
    cfg=ExecutionConfig(exchange_order_modes={'default':ExchangeOrderModeConfig(order_mode='limit')})
    if mode=='dual_limit': cfg.order_execution.enable_dual_limit_mode=True
    else: cfg.exchange_order_modes['arcus']=ExchangeOrderModeConfig(order_mode='market')
    e=ArbitrageExecutor(cfg,{'lighter':lighter,'arcus':arcus},monitor_only=False,is_segmented_mode=True)
    request=ExecutionRequest(SYMBOL,'arcus' if reverse else 'lighter','lighter' if reverse else 'arcus',D('50000'),D('50001'),D('.001'),is_open=is_open)
    assert run(e.execute_arbitrage(request)).failure_code=='ORDER_STATE_UNKNOWN'
    assert lighter._rest.place_order.await_count==1
    assert lighter._rest.place_order.call_args.kwargs.get('reduce_only',False)==(not is_open)
    if mode=='dual_limit':
        assert arcus.create_order.await_count==1
        assert arcus.create_order.call_args.kwargs['params'].get('reduce_only',False)==(not is_open)
    else: arcus.create_order.assert_not_awaited()


def test_lighter_spot_limit_close_preserves_no_reduceonly():
    from core.adapters.exchanges.adapters.lighter import LighterAdapter
    lighter=LighterAdapter(ExchangeConfig('lighter','Lighter',ExchangeType.SPOT,'',''))
    lighter._rest.place_order=AsyncMock(side_effect=OrderStateUnknownError('lighter'))
    e,request,_,_=executor_pair(a=adapter())
    e.exchange_adapters['lighter']=lighter
    request.symbol='ETH-USDC-SPOT'; request.is_open=False
    with pytest.raises(OrderStateUnknownError):
        run(e._place_limit_order(lighter,'ETH-USDC-SPOT',D('3000'),D('.01'),is_buy=False,request=request))
    assert lighter._rest.place_order.await_count==1
    assert lighter._rest.place_order.call_args.kwargs.get('reduce_only',False) is False


@pytest.mark.parametrize('reverse',[False,True])
@pytest.mark.parametrize('compensation',['filled','rejected','timeout'])
def test_definitive403_known_lighter_leg_compensation_preserves_exposure(reverse,compensation):
    from core.adapters.exchanges.adapters.lighter import LighterAdapter
    arcus=adapter(); arcus_requests=[]
    async def arcus_request(method,path,**kwargs):
        arcus_requests.append(method)
        if method=='POST': raise ArcusRequestRejectedError('Arcus HTTP 403')
        return 200,{'bids':[['49999.9','1']],'asks':[['50000','1']]}
    arcus._request=arcus_request
    lighter=LighterAdapter(ExchangeConfig('lighter','Lighter',ExchangeType.PERPETUAL,'',''))
    known=arcus._parse_order(row('L1',status='FILLED',filled='.001',originalSize='.001',remainingSize='0',side='BUY' if reverse else 'SELL'))
    close=arcus._parse_order(row('L2',status='FILLED',filled='.001',originalSize='.001',remainingSize='0',side='SELL' if reverse else 'BUY'))
    outcome=close if compensation=='filled' else (ValueError('explicit close reject') if compensation=='rejected' else asyncio.TimeoutError())
    lighter._rest.place_order=AsyncMock(side_effect=[known,outcome])
    from core.services.arbitrage_monitor_v2.config.arbitrage_config import ExchangeOrderModeConfig
    cfg=ExecutionConfig(exchange_order_modes={'default':ExchangeOrderModeConfig(order_mode='market')})
    cfg.order_execution.max_retry_count=1
    e=ArbitrageExecutor(cfg,{'arcus':arcus,'lighter':lighter},monitor_only=False,is_segmented_mode=True)
    # Initial production market path uses REST here; compensation also uses the real adapter REST route.
    async def rest_market(**kw):
        return await lighter.create_order(kw['symbol'],OrderSide.BUY if kw['is_buy'] else OrderSide.SELL,OrderType.MARKET,kw['quantity'],params={'reduce_only':kw['reduce_only']})
    e._place_lighter_market_order_ws=rest_market
    request=ExecutionRequest(SYMBOL,'lighter' if reverse else 'arcus','arcus' if reverse else 'lighter',D('50000'),D('50001'),D('.001'))
    result=run(e.execute_arbitrage(request))
    assert not result.success and result.partial_failure and result.success_quantity==D('.001')
    assert (result.order_buy if reverse else result.order_sell) is known
    assert lighter._rest.place_order.await_count==2
    calls=lighter._rest.place_order.call_args_list
    assert calls[0].args[1]!=calls[1].args[1] and calls[1].args[3]==D('.001')
    assert calls[1].kwargs['reduce_only'] is True
    if compensation=='filled': assert result.failure_code is None and result.emergency_closes
    else:
        assert result.failure_code=='ORDER_STATE_UNKNOWN'
        before=(len(arcus_requests),lighter._rest.place_order.await_count)
        assert run(e.execute_arbitrage(request)).failure_code=='ORDER_STATE_UNKNOWN'
        assert (len(arcus_requests),lighter._rest.place_order.await_count)==before


@pytest.mark.parametrize('slippage,expected',[('.001','50050'),('0','50000')])
def test_known_arcus_leg_lighter_rejection_close_respects_slippage(slippage,expected):
    a=adapter(); submissions=[]; orders={}
    async def request(method,path,**kwargs):
        if 'l2OrderBook' in path: return 200,{'bids':[['49999.9','1']],'asks':[['50000','1']]}
        if method=='POST':
            body=kwargs['body']; submissions.append(body); oid='O'+str(len(submissions))
            orders[oid]={**body,'orderId':oid,'marketDisplayName':'BTC-USD','side':body['orderSide'],'type':body['orderType'],'originalSize':body['quantity'],'remainingSize':'0','status':'FILLED','avgFillPrice':body['price'],'updatedAt':1791179000000000}
            return 202,{'orderId':oid,'status':'ACK'}
        return 200,orders[path.rsplit('/',1)[-1]]
    a._request=request
    other=SimpleNamespace(create_order=AsyncMock(side_effect=ValueError('explicit Lighter rejection')))
    e,req,_,_=executor_pair(a=a,other=other)
    req.exchange_buy='other'; req.exchange_sell='arcus'; req.slippage_tolerance_pct=D(slippage)
    e.config.order_execution.max_slippage=D('.001')
    result=run(e.execute_arbitrage(req))
    assert not result.success and result.partial_failure and result.success_quantity==D('.001')
    assert len(submissions)==2
    close=submissions[1]
    assert close['orderSide']=='BUY' and close['timeInForce']=='IOC' and close['reduceOnly'] is True
    assert D(close['price'])==D(expected)


@pytest.mark.parametrize('reverse',[False,True])
@pytest.mark.parametrize('close_fill',['.0004','0'])
def test_zero_terminal_leg_partial_emergency_quarantines_no_second_compensation(reverse,close_fill):
    from core.adapters.exchanges.adapters.lighter import LighterAdapter
    a=adapter(); zero=a._parse_order(row('A0',status='CANCELED',filled='0',originalSize='.001',remainingSize='.001'))
    a.create_order=AsyncMock(return_value=zero)
    lighter=LighterAdapter(ExchangeConfig('lighter','Lighter',ExchangeType.PERPETUAL,'',''))
    known=a._parse_order(row('L1',status='FILLED',filled='.001',originalSize='.001',remainingSize='0'))
    partial=a._parse_order(row('L2',status='CANCELED',filled=close_fill,originalSize='.001',remainingSize=str(D('.001')-D(close_fill))))
    lighter._rest.place_order=AsyncMock(side_effect=[known,partial])
    e,req,_,_=executor_pair(a=a); e.exchange_adapters={'arcus':a,'lighter':lighter}
    req.exchange_buy='lighter' if reverse else 'arcus'; req.exchange_sell='arcus' if reverse else 'lighter'
    async def rest_market(**kw):
        return await lighter.create_order(kw['symbol'],OrderSide.BUY if kw['is_buy'] else OrderSide.SELL,OrderType.MARKET,kw['quantity'],params={'reduce_only':kw['reduce_only']})
    e._place_lighter_market_order_ws=rest_market
    original_confirm=e.order_strategy_executor._confirm_market_fill_with_retries
    async def confirm(**kw):
        if kw['adapter'] is a: return zero,D('0')
        return await original_confirm(**kw)
    e.order_strategy_executor._confirm_market_fill_with_retries=confirm
    result=run(e.execute_arbitrage(req))
    assert not result.success and result.failure_code=='ORDER_STATE_UNKNOWN' and result.partial_failure
    assert result.success_quantity==D('.001') and (result.order_buy if reverse else result.order_sell) is known
    assert lighter._rest.place_order.await_count==2
    assert lighter._rest.place_order.call_args.kwargs['reduce_only'] is True
    assert sum(D(item['quantity']) for item in result.emergency_closes)==D(close_fill)
    before=(a.create_order.await_count,lighter._rest.place_order.await_count)
    for buy,sell in [('arcus','lighter'),('lighter','arcus')]:
        req.exchange_buy=buy; req.exchange_sell=sell
        assert run(e.execute_arbitrage(req)).failure_code=='ORDER_STATE_UNKNOWN'
    assert (a.create_order.await_count,lighter._rest.place_order.await_count)==before


@pytest.mark.parametrize('close_fill',['.0004','0'])
def test_mixed_known_limit_leg_partial_emergency_preserves_and_latches(close_fill):
    from core.adapters.exchanges.adapters.lighter import LighterAdapter
    from core.services.arbitrage_monitor_v2.config.arbitrage_config import ExchangeOrderModeConfig
    a=adapter(); a.create_order=AsyncMock(side_effect=ValueError('explicit rejected market leg'))
    lighter=LighterAdapter(ExchangeConfig('lighter','Lighter',ExchangeType.PERPETUAL,'',''))
    known=a._parse_order(row('L1',status='FILLED',filled='.001',originalSize='.001',remainingSize='0'))
    partial=a._parse_order(row('L2',status='CANCELED',filled=close_fill,originalSize='.001',remainingSize=str(D('.001')-D(close_fill))))
    lighter._rest.place_order=AsyncMock(side_effect=[known,partial])
    cfg=ExecutionConfig(exchange_order_modes={'lighter':ExchangeOrderModeConfig(order_mode='limit'),'arcus':ExchangeOrderModeConfig(order_mode='market')})
    cfg.order_execution.max_retry_count=1
    e=ArbitrageExecutor(cfg,{'lighter':lighter,'arcus':a},monitor_only=False,is_segmented_mode=True)
    req=ExecutionRequest(SYMBOL,'lighter','arcus',D('50000'),D('50001'),D('.001'))
    result=run(e.execute_arbitrage(req))
    assert result.failure_code=='ORDER_STATE_UNKNOWN' and result.partial_failure and result.order_buy is known
    assert result.success_quantity==D('.001') and lighter._rest.place_order.await_count==2
    before=(a.create_order.await_count,lighter._rest.place_order.await_count)
    assert run(e.execute_arbitrage(req)).failure_code=='ORDER_STATE_UNKNOWN'
    assert (a.create_order.await_count,lighter._rest.place_order.await_count)==before


@pytest.mark.parametrize('reverse',[False,True])
@pytest.mark.parametrize('state',['filled','late_fill','cancel_unknown'])
def test_dual_limit_definitive403_preserves_known_leg_and_manual_quarantine(reverse,state):
    from core.adapters.exchanges.adapters.lighter import LighterAdapter
    from core.services.arbitrage_monitor_v2.config.arbitrage_config import ExchangeOrderModeConfig
    a=adapter(); writes=[]
    async def arcus_request(method,path,**kwargs):
        writes.append(method)
        raise ArcusRequestRejectedError('Arcus HTTP 403')
    a._request=arcus_request
    lighter=LighterAdapter(ExchangeConfig('lighter','Lighter',ExchangeType.PERPETUAL,'',''))
    known=a._parse_order(row('L1',status='FILLED' if state=='filled' else 'OPEN',filled='.001' if state=='filled' else '0',originalSize='.001',remainingSize='0' if state=='filled' else '.001'))
    latest=a._parse_order(row('L1',status='FILLED',filled='.001',originalSize='.001',remainingSize='0',seq=2))
    lighter._rest.place_order=AsyncMock(return_value=known)
    lighter.cancel_order=AsyncMock(side_effect=OrderStateUnknownError('lighter') if state=='cancel_unknown' else None,return_value=latest)
    lighter.get_order=AsyncMock(return_value=latest)
    cfg=ExecutionConfig(exchange_order_modes={'default':ExchangeOrderModeConfig(order_mode='limit')})
    cfg.order_execution.enable_dual_limit_mode=True; cfg.order_execution.max_retry_count=1
    e=ArbitrageExecutor(cfg,{'lighter':lighter,'arcus':a},monitor_only=False,is_segmented_mode=True)
    req=ExecutionRequest(SYMBOL,'arcus' if reverse else 'lighter','lighter' if reverse else 'arcus',D('50000'),D('50001'),D('.001'))
    result=run(e.execute_arbitrage(req))
    assert result.failure_code=='ORDER_STATE_UNKNOWN' and not result.success
    assert (result.order_sell if reverse else result.order_buy) is known
    assert result.success_quantity==(D('0') if state=='cancel_unknown' else D('.001'))
    assert result.partial_failure==(state!='cancel_unknown')
    assert lighter._rest.place_order.await_count==1 and not result.emergency_closes
    assert lighter.cancel_order.await_count==(0 if state=='filled' else 1)
    before=(len(writes),lighter._rest.place_order.await_count,lighter.cancel_order.await_count)
    for buy,sell in [('arcus','lighter'),('lighter','arcus')]:
        req.exchange_buy=buy; req.exchange_sell=sell
        assert run(e.execute_arbitrage(req)).failure_code=='ORDER_STATE_UNKNOWN'
    assert (len(writes),lighter._rest.place_order.await_count,lighter.cancel_order.await_count)==before


@pytest.mark.parametrize('stale_fill',['.0004','0'])
def test_dual_cancel_stale_fill_preserves_cumulative_and_valuation(stale_fill):
    from core.adapters.exchanges.adapters.lighter import LighterAdapter
    from core.services.arbitrage_monitor_v2.config.arbitrage_config import ExchangeOrderModeConfig
    a=adapter(); a._request=AsyncMock(side_effect=ArcusRequestRejectedError('Arcus HTTP 403'))
    known=a._parse_order(row('L1',status='OPEN',filled='.0008',originalSize='.001',remainingSize='.0002',seq=2,avgFillPrice='50001'))
    stale=a._parse_order(row('L1',status='CANCELED',filled=stale_fill,originalSize='.001',remainingSize=str(D('.001')-D(stale_fill)),seq=1,avgFillPrice='49999'))
    lighter=LighterAdapter(ExchangeConfig('lighter','Lighter',ExchangeType.PERPETUAL,'',''))
    lighter._rest.place_order=AsyncMock(return_value=known)
    lighter._rest.cancel_order=AsyncMock(return_value=True)
    lighter._rest.get_order=AsyncMock(return_value=stale)
    lighter._websocket.lookup_cached_order=lambda *args:None
    cfg=ExecutionConfig(exchange_order_modes={'default':ExchangeOrderModeConfig(order_mode='limit')})
    cfg.order_execution.enable_dual_limit_mode=True; cfg.order_execution.max_retry_count=1
    e=ArbitrageExecutor(cfg,{'lighter':lighter,'arcus':a},monitor_only=False,is_segmented_mode=True)
    req=ExecutionRequest(SYMBOL,'lighter','arcus',D('50000'),D('50001'),D('.001'))
    result=run(e.execute_arbitrage(req))
    assert result.failure_code=='ORDER_STATE_UNKNOWN' and result.partial_failure
    assert result.order_buy is known and result.success_quantity==known.filled==D('.0008')
    assert known.remaining==0 and known.average==D('50001') and known.cost==D('.0008')*D('50001')
    assert known.raw_data['sequenceNumber']==2
    assert lighter._rest.cancel_order.await_count==1 and lighter._rest.place_order.await_count==1
    assert not result.emergency_closes
    before=a._request.await_count
    for buy,sell in [('lighter','arcus'),('arcus','lighter')]:
        req.exchange_buy=buy; req.exchange_sell=sell
        assert run(e.execute_arbitrage(req)).failure_code=='ORDER_STATE_UNKNOWN'
    assert a._request.await_count==before and lighter._rest.cancel_order.await_count==1 and lighter._rest.place_order.await_count==1


def test_loaded_live_profile_two_legs_open_close_reduceonly_without_real_transport():
    from core.services.arbitrage_monitor_v2.config.unified_config_manager import UnifiedConfigManager
    cfg=UnifiedConfigManager(Path('config/arbitrage/arbitrage_lighter_arcus.yaml')).get_unified_config()
    from core.services.arbitrage_monitor_v2.config.symbol_config import SegmentedConfigManager
    assert SegmentedConfigManager(Path('config/arbitrage/arbitrage_taker_taker.yaml')).get_config(SYMBOL).quantity_config.base_quantity==D('.005')
    assert cfg.risk_control.position_management.max_single_token_position==10
    assert cfg.risk_control.position_management.max_total_position==30
    calls=[]
    def fake(name):
        orders={}
        async def get_order(order_id,symbol):
            return orders[order_id]
        async def create_order(symbol,side,order_type,amount,price=None,params=None):
            calls.append((name,side,amount,params or {}))
            o=adapter()._parse_order(row(order_id=name+str(len(calls)),status='FILLED',filled='.025',
                originalSize='.025',remainingSize='0',side=side.value.upper()))
            orders[o.id]=o
            return o
        return SimpleNamespace(strict_order_state=True,create_order=create_order,get_order=get_order)
    async def scenario():
        e=ArbitrageExecutor(cfg.execution,{'lighter':fake('lighter'),'arcus':fake('arcus')},
                            monitor_only=False,is_segmented_mode=True)
        # Avoid intentional production cooldown in this offline behavior check.
        e.config.order_execution.round_pause_seconds=0
        for is_open in (True,False):
            req=ExecutionRequest(SYMBOL,'lighter' if is_open else 'arcus','arcus' if is_open else 'lighter',
                                 D('50000'),D('50050'),D('.025'),is_open=is_open)
            assert (await e.execute_arbitrage(req)).success
    run(scenario())
    assert len(calls)==4 and all(amount==D('.025') for _,_,amount,_ in calls)
    assert all(not params.get('reduce_only') for _,_,_,params in calls[:2])
    assert all(params.get('reduce_only') for _,_,_,params in calls[2:])
    initial={name:side for name,side,_,_ in calls[:2]}
    assert all(side!=initial[name] for name,side,_,_ in calls[2:])


@pytest.mark.parametrize('total,paused,critical',[('60',False,False),('50',False,False),('49.99',True,False),('39.99',True,True)])
def test_live_balance_thresholds_use_total_not_free(total,paused,critical):
    from core.adapters.exchanges.models import BalanceData
    from core.services.arbitrage_monitor_v2.config.unified_config_manager import UnifiedConfigManager
    from core.services.arbitrage_monitor_v2.risk_control.global_risk_controller import GlobalRiskController
    cfg=UnifiedConfigManager(Path('config/arbitrage/arbitrage_lighter_arcus.yaml')).get_unified_config()
    assert cfg.risk_control.balance_management.min_balance_warning==50
    assert cfg.risk_control.balance_management.min_balance_close_position==40
    balance=BalanceData('USDC',D('0'),D(total),D(total),D(total),datetime.now(),{})
    adapters={name:SimpleNamespace(get_balances=AsyncMock(return_value=[balance]),get_positions=AsyncMock(return_value=[])) for name in ('lighter','arcus')}
    risk=GlobalRiskController(cfg.risk_control,adapters)
    run(risk._check_all_balances())
    assert risk.risk_status.is_paused is paused
    assert bool(risk.risk_status.critical_balance_exchanges) is critical


def test_lighter_live_extra_params_propagate_through_real_bootstrap_without_dotenv():
    from core.utils.config_loader import AuthConfig
    from core.adapters.exchanges.adapters.lighter import LighterAdapter
    from core.services.arbitrage_monitor_v2.core.orchestrator_bootstrap import OrchestratorBootstrap
    loader=SimpleNamespace(load_auth_config=lambda *args,**kwargs:AuthConfig())
    bootstrap=object.__new__(OrchestratorBootstrap)
    cfg=bootstrap._load_exchange_config('lighter',Path('config/exchanges/lighter_runtime.yaml'),loader)
    assert cfg.extra_params['strict_order_state'] is True
    static=yaml.safe_load(Path('config/exchanges/lighter_runtime.yaml').read_text())
    stub=SimpleNamespace(logger=None,_load_lighter_config=lambda:static)
    assert LighterAdapter._convert_config_to_dict(stub,cfg)['strict_order_state'] is True


def test_executor_reconnect_terminal_replay_measures_latency_once(monkeypatch):
    from unittest.mock import Mock
    from core.services.arbitrage_monitor_v2.config.arbitrage_config import RiskControlConfig
    from core.services.arbitrage_monitor_v2.risk_control.global_risk_controller import GlobalRiskController
    now = [1000.0]
    monkeypatch.setattr('core.services.arbitrage_monitor_v2.execution.arbitrage_executor.time.time', lambda: now[0])
    a = adapter(); e, _, _, _ = executor_pair(a=a)
    cfg = RiskControlConfig(); cfg.order_ws_latency_pause.enabled = True
    cfg.order_ws_latency_pause.threshold_seconds = 3.2; cfg.order_ws_latency_pause.pause_seconds = 1000
    risk = GlobalRiskController(cfg, {})
    reporter = Mock(side_effect=risk.record_order_ws_latency_pause)
    e.set_order_ws_latency_reporter(reporter)
    pending = a._parse_order(row(type='MARKET'))
    e._register_pending_order(pending)
    e._order_fill_events['Client-A'] = asyncio.Event()
    async def scenario():
        await a._websocket.subscribe_orders(e._on_order_update)
        now[0] += .095
        await a._websocket.handle_message(frame('orders', {'recentClosedOrders':[row(status='CANCELED', filled='.4', type='MARKET')]}))
        assert reporter.call_count == 1
        assert reporter.call_args.kwargs['latency_seconds'] == pytest.approx(.095)
        assert not risk.should_block_order_ws_latency_pause()[0]
        assert not e._order_submit_ts
        now[0] += 1206
        await a._websocket.handle_message(frame('orders', {'recentClosedOrders':[row(status='CANCELED', filled='.4', type='MARKET')]}))
        # A later genuine fill still updates the result, even without a waiting event.
        await a._websocket.handle_message(frame('orders', {'recentClosedOrders':[row(status='FILLED', filled='1', type='MARKET', seq=2)]}))
        assert e._order_fill_results['O1'].filled == 1
        assert reporter.call_count == 1 and not risk.should_block_order_ws_latency_pause()[0]
        e._order_ws_latency_info.clear(); e._order_ws_latency_keys.clear()
        replay = a._parse_order(row(status='FILLED', filled='1', type='MARKET')); replay.client_id = None
        await e._on_order_update(replay)
        replay.id = None; replay.client_id = 'Client-A'
        await e._on_order_update(replay)
        assert reporter.call_count == 1
        # Unknown cleanup must preserve the submission time for the first late fill.
        other = a._parse_order(row('O2', type='MARKET', clientId='Client-B'))
        e._register_pending_order(other); e._remove_pending_order(other)
        now[0] += 4
        await e._on_order_update(a._parse_order(row('O2', status='FILLED', filled='1', type='MARKET', clientId='Client-B')))
        assert reporter.call_count == 2 and risk.should_block_order_ws_latency_pause()[0]
    run(scenario())


@pytest.mark.parametrize('status,filled,kind', [('CANCELED','0','MARKET'), ('OPEN','.4','MARKET'), ('FILLED','1','LIMIT')])
def test_executor_latency_ignores_zero_partial_and_limit(monkeypatch, status, filled, kind):
    from unittest.mock import Mock
    now = [1000.0]
    monkeypatch.setattr('core.services.arbitrage_monitor_v2.execution.arbitrage_executor.time.time', lambda: now[0])
    a = adapter(); e, _, _, _ = executor_pair(a=a)
    e._register_pending_order(a._parse_order(row(type=kind)))
    reporter = Mock(); e.set_order_ws_latency_reporter(reporter)
    now[0] += 10
    run(e._on_order_update(a._parse_order(row(status=status, filled=filled, type=kind))))
    reporter.assert_not_called()
    if kind == 'MARKET':
        run(e._on_order_update(a._parse_order(row(status='FILLED', filled='1', type=kind))))
        assert reporter.call_count == 1 and reporter.call_args.kwargs['latency_seconds'] == 10


@pytest.mark.parametrize('status', ['FILLED', 'CANCELED', 'REJECTED', 'EXPIRED'])
def test_strict_rest_terminal_consumes_all_submit_aliases_before_late_ws(monkeypatch, status):
    from unittest.mock import Mock
    now = [1000.0]
    monkeypatch.setattr('core.services.arbitrage_monitor_v2.execution.arbitrage_executor.time.time', lambda: now[0])
    a = adapter(); e, _, _, _ = executor_pair(a=a)
    pending = a._parse_order(row('pending-id', type='MARKET'))
    e._register_pending_order(pending)
    submitted = a._parse_order(row('Client-A', type='MARKET'))
    terminal_fill = '1' if status == 'FILLED' else '.4'
    latest = a._parse_order(row('real-id', status=status, filled=terminal_fill, type='MARKET'))
    e._order_submit_ts['real-id'] = now[0]
    reporter = Mock(); e.set_order_ws_latency_reporter(reporter)
    partial = a._parse_order(row('real-id', status='OPEN', filled='.2', type='MARKET'))
    async def get_order(*args):
        assert e._order_submit_ts
        return partial if a.get_order.await_count == 1 else latest
    a.get_order = AsyncMock(side_effect=get_order)
    async def scenario():
        assert await e.order_monitor._wait_for_order_fill(submitted, a, timeout=1) == D(terminal_fill)
        assert not e._order_submit_ts and a.get_order.await_count == 2
        assert e._lookup_pending_order('pending-id') is pending
        now[0] += 67 * 60
        await e._on_order_update(a._parse_order(row('real-id', status='FILLED', filled='1', type='MARKET')))
        reporter.assert_not_called()
        assert e._order_fill_results['real-id'].filled == 1
    run(scenario())


def test_fill_row_side_never_overrides_order_side():
    a=adapter(); ws=a._websocket
    async def scenario():
        await ws.handle_message(frame('orders',{'orders':[row(side='SELL',seq=1)]}))
        fill={'tradeId':'T-side','orderId':'O1','marketId':1,'marketDisplayName':'BTC-USD','side':'BUY',
              'originalSize':'1','remainingSize':'.6','size':'.4','price':'50000','sequenceNumber':2}
        await ws.handle_message(frame('userFills',{'fills':[fill]}))
        o=a._orders['O1']
        assert o.filled==D('.4') and o.side==OrderSide.SELL and o.raw_data['side']=='SELL'
    run(scenario())


def test_side_change_is_recorded_as_diagnostic_without_changing_validation():
    a=adapter(); ws=a._websocket
    async def scenario():
        await ws.handle_message(frame('orders',{'orders':[row(side='SELL',seq=1)]}))
        await ws.handle_message(frame('orders',{'orders':[row(side='BUY',seq=2)]},type='channel_data'))
        o=a._orders['O1']
        assert o.side==OrderSide.BUY  # still reported as-is; the ledger check decides
        change=o.raw_data['_side_change']
        assert change['previous']=='sell' and change['reported']=='buy' and change['sequence']==2 and change['trade_row'] is False
        await ws.handle_message(frame('orders',{'orders':[row(side='BUY',seq=3)]},type='channel_data'))
        assert '_side_change' not in a._orders['O1'].raw_data
    run(scenario())
