"""Startup terminal semantics and asynchronous private recovery provenance, offline only."""
import asyncio
import json
import time
from dataclasses import replace
from decimal import Decimal as D
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock

import pytest

from test_arcus_adapter import frame
from test_maker_taker_engine import engine, SYMBOL
from test_maker_taker_startup import startup_live, startup_row, subscribe


def events(e):
    return [json.loads(row[0]) for row in e.journal.db.execute("SELECT payload FROM events WHERE kind='foreign_order_ignored'")]


@pytest.mark.parametrize('status',['REJECTED','CANCELED','EXPIRED','MARGIN_CANCELED'])
@pytest.mark.parametrize('filled',['0','.2'])
def test_explicit_terminal_history_does_not_require_zero_remaining(tmp_path,status,filled):
    async def scenario():
        e=engine(tmp_path);live=startup_live(e);await subscribe(live,e)
        await live.arcus._websocket.handle_message(frame('orders',{'recentClosedOrders':[startup_row(status=status,filled=filled)]}))
        assert events(e)[0]['cumulative']==str(D(filled))
        assert D(events(e)[0]['remaining'])==1-D(filled)
        assert await e.recover() and e.state=='ACTIVE'
        assert not e.journal.makers() and not e.journal.obligations() and not e.transport.sends
        await e.stop(timeout=.1)
    asyncio.run(scenario())


@pytest.mark.parametrize('position',[D('.2'),D('-.2')])
def test_ignored_history_never_authorizes_position_adoption_even_at_stop(tmp_path,position):
    async def scenario():
        e=engine(tmp_path);live=startup_live(e);await subscribe(live,e)
        e.transport.positions={SYMBOL:(position,D(0))}
        await live.arcus._websocket.handle_message(frame('orders',{'recentClosedOrders':[startup_row(status='CANCELED',filled='.2')]}))
        assert len(events(e))==1 and not e.halted
        assert not await e.recover() and e.halted
        await e.stop(timeout=.1)
        assert e.journal.inventory(SYMBOL)==(D(0),D(0)) and not e.journal.obligations()
        assert not e.transport.sends and not e.transport.emergencies
    asyncio.run(scenario())


@pytest.mark.parametrize('cumulative',[D('-1'),D('2'),D('NaN'),D('Infinity')])
def test_invalid_cumulative_cannot_obtain_startup_history_exemption(tmp_path,cumulative):
    e=engine(tmp_path);live=startup_live(e)
    observation=live.normalize(live.arcus._merge_order(startup_row(status='CANCELED')),'arcus')
    e.receive(replace(observation,startup_snapshot=True,cumulative=cumulative))
    assert e.halted and not events(e)


def private_chain(e):
    live=startup_live(e);ws=live.arcus._websocket
    ws._socket=NS(send=AsyncMock())
    ws._ws_connected=True;ws.generation=1;ws.last_received_monotonic=time.monotonic()
    ws._ready.add(('orders',None))
    socket=object()
    live.lighter._websocket=NS(_direct_ws=socket,_reader_socket=socket,_reader_connected=True,generation=1,
        _reader_generation=1,_private_ready_socket=socket,_private_ready_generation=1,last_received_monotonic=time.monotonic())
    live.source=NS(stale_seconds=5)
    e.transport.startup_terminal_retirement=live.startup_terminal_retirement
    e.transport.private_validity=live.private_validity
    e.transport.healthy=lambda:all(value[0] for value in live.private_validity().values())
    return live,ws


def fill(**kwargs):
    return {'orderId':'O1','tradeId':'T1','marketId':1,'sequenceNumber':3,'size':'.2','price':'100',**kwargs}


@pytest.mark.parametrize('runtime_type',['channel_data','subscribed'])
def test_delayed_fill_recovery_gates_readiness_and_preserves_frame_origin(tmp_path,runtime_type):
    async def scenario():
        e=engine(tmp_path);live,ws=private_chain(e);started=asyncio.Event();release=asyncio.Event();seen=[]
        def receive(order):
            seen.append((order.raw_data['_startup_snapshot'],live.private_validity()['arcus_private'][0]))
            e.receive(live.normalize(order,'arcus'))
        await ws.subscribe_quantity_observations(receive)
        async def get_order(*args,**kwargs):
            started.set();await release.wait()
            return live.arcus._merge_order(startup_row(status='CANCELED',filled='.2',seq=4))
        live.arcus._get_order_observation=live.arcus.get_order=AsyncMock(side_effect=get_order)
        await ws.handle_message(frame('userFills',{'fills':[fill()]}))
        await asyncio.wait_for(started.wait(),1)
        assert not live.private_validity()['arcus_private'][0] and not e.subscriptions_ready()
        release.set();await asyncio.gather(*tuple(ws._recovery_tasks));await asyncio.sleep(0)
        assert seen==[(True,False)] and live.private_validity()['arcus_private'][0]
        assert '_startup_snapshot' not in live.arcus._orders['O1'].raw_data
        assert e.subscriptions_ready() and len(events(e))==1
        await e.start();assert e.state=='ACTIVE'
        if runtime_type=='subscribed':ws.generation+=1
        await ws.handle_message(frame('userFills',{'fills':[fill(tradeId='T2',sequenceNumber=5)]},type=runtime_type))
        await asyncio.gather(*tuple(ws._recovery_tasks));await asyncio.sleep(0)
        assert e.halted and len(events(e))==1
        assert seen[-1][0]==(runtime_type=='subscribed')
        await e.stop(timeout=.1)
        assert not e.transport.sends
    asyncio.run(scenario())


@pytest.mark.parametrize('failure',['timeout','none','cancel'])
def test_unsuccessful_snapshot_recovery_remains_unready(tmp_path,failure):
    async def scenario():
        e=engine(tmp_path);live,ws=private_chain(e);await subscribe(live,e)
        started=asyncio.Event();release=asyncio.Event()
        async def get_order(*args,**kwargs):
            started.set();await release.wait()
            if failure=='timeout':raise TimeoutError()
            return None
        live.arcus._get_order_observation=live.arcus.get_order=AsyncMock(side_effect=get_order)
        await ws.handle_message(frame('userFills',{'fills':[fill()]}));await asyncio.wait_for(started.wait(),1)
        tasks=tuple(ws._recovery_tasks)
        if failure=='cancel':tasks[0].cancel()
        else:release.set()
        await asyncio.gather(*tasks,return_exceptions=True);await asyncio.sleep(0)
        assert not ws._recovery_tasks and ws._startup_recoveries
        assert live.private_validity()['arcus_private'][1]=='startup_recovery'
        assert not e.subscriptions_ready() and not events(e)
    asyncio.run(scenario())


def test_prior_generation_recovery_cannot_claim_new_snapshot_provenance(tmp_path):
    async def scenario():
        e=engine(tmp_path);live,ws=private_chain(e);await subscribe(live,e)
        started=asyncio.Event();release=asyncio.Event()
        async def get_order(*args,**kwargs):
            started.set();await release.wait()
            return live.arcus._merge_order(startup_row(status='CANCELED',filled='.2',seq=4))
        live.arcus._get_order_observation=live.arcus.get_order=AsyncMock(side_effect=get_order)
        await ws.handle_message(frame('userFills',{'fills':[fill()]}));await asyncio.wait_for(started.wait(),1)
        ws.generation+=1
        release.set();await asyncio.gather(*tuple(ws._recovery_tasks));await asyncio.sleep(0)
        assert e.halted and not events(e)
        assert '_startup_snapshot' not in live.arcus._orders['O1'].raw_data
    asyncio.run(scenario())


@pytest.mark.parametrize('branch',['missing_quantity','mismatched_delta','consistent_delta'])
def test_cached_fill_branches_copy_snapshot_origin_without_caching_it(tmp_path,branch):
    async def scenario():
        e=engine(tmp_path);live,ws=private_chain(e);seen=[]
        baseline=live.arcus._merge_order(startup_row(status='OPEN',seq=1))
        await ws.subscribe_quantity_observations(lambda order:seen.append(order.raw_data['_startup_snapshot']))
        live.arcus._get_order_observation=AsyncMock(side_effect=lambda *args,**kwargs:live.arcus._merge_order(startup_row(status='CANCELED',filled='.2',seq=4)))
        value=fill(originalSize='1',remainingSize='.8')
        if branch=='missing_quantity':value.pop('remainingSize')
        elif branch=='mismatched_delta':value['size']='.1'
        await ws.handle_message(frame('userFills',{'fills':[value]}))
        await asyncio.gather(*tuple(ws._recovery_tasks));await asyncio.sleep(0)
        assert seen and all(seen)
        assert len(seen)==(2 if branch=='mismatched_delta' else 1)
        assert '_startup_snapshot' not in live.arcus._orders['O1'].raw_data
    asyncio.run(scenario())


@pytest.mark.parametrize('status',['CANCELED','MARGIN_CANCELED'])
@pytest.mark.parametrize('matches',[True,False])
def test_real_rest_terminal_quantity_without_average_uses_position_reconciliation(tmp_path,status,matches):
    async def scenario():
        e=engine(tmp_path);live,ws=private_chain(e);await subscribe(live,e)
        response=startup_row(status=status,filled='.2',seq=4);response.pop('avgFillPrice')
        live.arcus._request=AsyncMock(return_value=(200,response))
        e.transport.positions={} if matches else {SYMBOL:(D('.2'),D(0))}
        await ws.handle_message(frame('userFills',{'fills':[fill()]}))
        await asyncio.gather(*tuple(ws._recovery_tasks));await asyncio.sleep(0)
        assert live.arcus._orders['O1'].average is None
        assert live.private_validity()['arcus_private'][0] and not ws._startup_recoveries
        assert events(e)[0]['cumulative']=='0.2'
        if matches:
            await e.start();assert e.state=='ACTIVE'
        else:
            with pytest.raises(RuntimeError,match='Startup reconciliation blocked'):await e.start()
        await e.stop(timeout=.1)
        assert e.journal.inventory(SYMBOL)==(D(0),D(0)) and not e.journal.obligations()
        assert not e.transport.sends and not e.transport.emergencies
        assert live.arcus._request.await_count==1
    asyncio.run(scenario())


@pytest.mark.parametrize('fault',['timeout','wrong_account','wrong_id','low_sequence','missing_sequence'])
def test_startup_rest_failure_cannot_reauthorize_terminal_cache(tmp_path,fault):
    async def scenario():
        e=engine(tmp_path);live,ws=private_chain(e);await subscribe(live,e)
        live.arcus._merge_order(startup_row(status='CANCELED',filled='.2',seq=1))
        response=startup_row(status='CANCELED',filled='.2',seq=4)
        if fault=='wrong_account':response['accountIndex']=1
        elif fault=='wrong_id':response['orderId']='OTHER'
        elif fault=='low_sequence':response['sequenceNumber']=2
        elif fault=='missing_sequence':response.pop('sequenceNumber')
        live.arcus._request=AsyncMock(side_effect=TimeoutError() if fault=='timeout' else None,return_value=(200,response))
        await ws.handle_message(frame('userFills',{'fills':[fill()]}))
        await asyncio.gather(*tuple(ws._recovery_tasks));await asyncio.sleep(0)
        assert e.halted and not events(e) and not e.subscriptions_ready()
        assert live.private_validity()['arcus_private'][1]=='startup_recovery'
        assert '_reported_order' not in live.arcus._orders['O1'].raw_data
        assert '_startup_snapshot' not in live.arcus._orders['O1'].raw_data
    asyncio.run(scenario())


def test_public_get_order_still_rejects_terminal_quantity_without_valuation(tmp_path):
    from core.adapters.exchanges.order_state import OrderStateUnknownError
    async def scenario():
        live=startup_live(engine(tmp_path))
        response=startup_row(status='CANCELED',filled='.2');response.pop('avgFillPrice')
        live.arcus._request=AsyncMock(return_value=(200,response))
        with pytest.raises(OrderStateUnknownError):await live.arcus.get_order('O1',SYMBOL)
    asyncio.run(scenario())


@pytest.mark.parametrize('sequence',[None,'unavailable'])
def test_fill_without_usable_sequence_uses_fresh_raw_proof_without_invented_floor(tmp_path,sequence):
    async def scenario():
        e=engine(tmp_path);live,ws=private_chain(e);await subscribe(live,e)
        response=startup_row(status='REJECTED');response.pop('sequenceNumber')
        live.arcus._request=AsyncMock(return_value=(200,response))
        value=fill(sequenceNumber=sequence)
        await ws.handle_message(frame('userFills',{'fills':[value]}))
        await asyncio.gather(*tuple(ws._recovery_tasks));await asyncio.sleep(0)
        assert live.private_validity()['arcus_private'][0] and len(events(e))==1
        assert await e.recover() and not e.halted
    asyncio.run(scenario())


@pytest.mark.parametrize('runtime',[False,True])
def test_mismatched_cached_fill_waits_for_rest_only_during_startup(tmp_path,runtime):
    async def scenario():
        e=engine(tmp_path);live,ws=private_chain(e);await subscribe(live,e)
        await ws.handle_message(frame('orders',{'recentClosedOrders':[startup_row(status='CANCELED',seq=1)]}))
        baseline=len(events(e));started=asyncio.Event();release=asyncio.Event()
        response=startup_row(status='CANCELED',filled='.2',seq=4)
        async def request(*args,**kwargs):started.set();await release.wait();return 200,response
        live.arcus._request=AsyncMock(side_effect=request)
        if runtime:ws._ready.add(('userFills',None))
        await ws.handle_message(frame('userFills',{'fills':[fill(size='.1',originalSize='1',remainingSize='.8')]},
                                      type='channel_data' if runtime else 'subscribed'))
        await asyncio.wait_for(started.wait(),1)
        assert e.halted==runtime
        if not runtime:assert not live.private_validity()['arcus_private'][0]
        assert len(events(e))==baseline
        release.set();await asyncio.gather(*tuple(ws._recovery_tasks));await asyncio.sleep(0)
        if not runtime:
            assert not e.halted and len(events(e))==baseline+1
            await e.start();assert e.state=='ACTIVE'
        else:assert len(events(e))==baseline and e.halted
        assert '_startup_provisional' not in live.arcus._orders['O1'].raw_data
        await e.stop(timeout=.1)
        assert not e.transport.sends
    asyncio.run(scenario())


@pytest.mark.parametrize('guard',['none','unrelated','cancel','modify','client'])
def test_real_create_order_admission_after_valuation_only_history_retirement(tmp_path,guard):
    from unittest.mock import Mock
    from core.adapters.exchanges.models import OrderSide,OrderType
    from core.adapters.exchanges.order_state import OrderStateUnknownError
    async def scenario():
        e=engine(tmp_path);live,ws=private_chain(e);await subscribe(live,e)
        response=startup_row(status='CANCELED',filled='.2',seq=4);response.pop('avgFillPrice')
        live.arcus._request=AsyncMock(return_value=(200,response))
        await ws.handle_message(frame('userFills',{'fills':[fill()]}))
        await asyncio.gather(*tuple(ws._recovery_tasks));await asyncio.sleep(0)
        assert 'O1' in live.arcus._uncertain
        if guard=='unrelated':live.arcus._uncertain.add('another-intent')
        elif guard=='cancel':live.arcus._cancel_uncertain.add('O1')
        elif guard=='modify':live.arcus._modify_uncertain.add('O1')
        elif guard=='client':live.arcus._uncertain.add('Client-A')
        await e.start();assert e.state=='ACTIVE'
        assert ('O1' in live.arcus._uncertain)==(guard in ('cancel','modify','client'))
        live.arcus._do_authenticate=AsyncMock();live.arcus._nonce=Mock(return_value=1791041528000000000)
        live.arcus._sign=Mock(return_value='offline signature')
        live.arcus._request=AsyncMock(return_value=(202,{'orderId':'NEW'}))
        live.arcus._await_order=AsyncMock(return_value=live.arcus._parse_order(startup_row(order_id='NEW',clientId='NewClient')))
        if guard=='none':
            result=await live.arcus.create_order(SYMBOL,OrderSide.BUY,OrderType.LIMIT,D(1),D(50000),params={'clientId':'NewClient'})
            assert result.id=='NEW';live.arcus._sign.assert_called_once();live.arcus._request.assert_awaited_once()
        else:
            with pytest.raises(OrderStateUnknownError):
                await live.arcus.create_order(SYMBOL,OrderSide.BUY,OrderType.LIMIT,D(1),D(50000),params={'clientId':'NewClient'})
            live.arcus._sign.assert_not_called();live.arcus._request.assert_not_awaited()
            assert live.arcus._uncertain
        await e.stop(timeout=.1)
    asyncio.run(scenario())


def test_retirement_does_not_clear_cache_changed_during_initial_position_read(tmp_path):
    async def scenario():
        e=engine(tmp_path);live,ws=private_chain(e);await subscribe(live,e)
        response=startup_row(status='CANCELED',filled='.2',seq=4);response.pop('avgFillPrice')
        live.arcus._request=AsyncMock(return_value=(200,response))
        await ws.handle_message(frame('userFills',{'fills':[fill()]}))
        await asyncio.gather(*tuple(ws._recovery_tasks));await asyncio.sleep(0)
        async def snapshot():
            live.arcus._merge_order({**response,'sequenceNumber':5})
            return {'authoritative':True,'equity':e.equity,'positions':{}}
        e.transport.snapshot=snapshot
        assert await e.recover()
        assert 'O1' in live.arcus._uncertain
    asyncio.run(scenario())


def test_actual_arcus_reconnect_retries_only_unresolved_snapshot_fill(tmp_path):
    class Socket:
        def __init__(self):self.messages=asyncio.Queue()
        async def send(self,data):
            value=json.loads(data);channel=value['channel']
            self.messages.put_nowait(json.dumps(frame(channel,{'fills':[fill()]} if channel=='userFills' else {'orders':[]})))
        def __aiter__(self):return self
        async def __anext__(self):
            value=await self.messages.get()
            if value is None:raise StopAsyncIteration
            return value
        async def close(self):pass
    async def scenario():
        e=engine(tmp_path);live,ws=private_chain(e)
        ws._ws_connected=False;ws.generation=0;ws._ready.clear()
        live.arcus.config.retry_delay=0;live.arcus.config.enable_auto_reconnect=True
        sockets=[Socket(),Socket()];ws._open_socket=AsyncMock(side_effect=sockets)
        runtime_identity=('runtime-processed','R1');live.arcus._fill_ids.add(runtime_identity)
        response=startup_row(status='CANCELED',filled='.2',seq=4)
        live.arcus._request=AsyncMock(side_effect=[TimeoutError(),(200,response)])
        await subscribe(live,e);await ws.connect()
        async def until(predicate):
            deadline=time.monotonic()+2
            while time.monotonic()<deadline:
                if predicate():return
                await asyncio.sleep(.01)
            raise AssertionError('Expected private recovery lifecycle did not occur')
        try:
            await until(lambda:bool(ws._startup_retry_fills) and not ws._recovery_tasks)
            assert not live.private_validity()['arcus_private'][0]
            sockets[0].messages.put_nowait(None)
            await until(lambda:ws.generation==2 and live.private_validity()['arcus_private'][0])
            assert live.arcus._request.await_count==2 and len(events(e))==1
            assert runtime_identity in live.arcus._fill_ids and ('T1','O1') in live.arcus._fill_ids
            assert not ws._startup_retry_fills
            await e.start();assert e.state=='ACTIVE'
        finally:
            await ws.disconnect();await e.stop(timeout=.1)
    asyncio.run(scenario())


@pytest.mark.parametrize('cumulative',[D('NaN'),D('Infinity'),D('-1'),D('2')])
def test_invalid_provisional_quantity_still_halts(tmp_path,cumulative):
    e=engine(tmp_path);live=startup_live(e)
    obs=live.normalize(live.arcus._merge_order(startup_row(status='CANCELED')),'arcus')
    e.receive(replace(obs,startup_snapshot=True,startup_provisional=True,cumulative=cumulative))
    assert e.halted and not events(e)


def test_old_recovery_completion_cannot_clear_new_generation_retry_identity(tmp_path):
    async def scenario():
        e=engine(tmp_path);live,ws=private_chain(e);await subscribe(live,e)
        started=asyncio.Event();release=asyncio.Event();calls=0
        async def reader(*args,**kwargs):
            nonlocal calls
            calls+=1
            if calls==2:raise TimeoutError()
            started.set();await release.wait()
            return live.arcus._merge_order(startup_row(status='CANCELED',filled='.2',seq=4))
        live.arcus._get_order_observation=AsyncMock(side_effect=reader)
        await ws.handle_message(frame('userFills',{'fills':[fill()]}));await asyncio.wait_for(started.wait(),1)
        old_task=next(iter(ws._recovery_tasks))
        ws.generation+=1;ws._reset_startup_recoveries()
        await ws.handle_message(frame('userFills',{'fills':[fill()]}))
        current_tasks=tuple(task for task in ws._recovery_tasks if task is not old_task)
        await asyncio.gather(*current_tasks);await asyncio.sleep(0)
        assert ws._startup_retry_fills[('T1','O1')]==ws.generation
        release.set();await old_task;await asyncio.sleep(0)
        assert ws._startup_retry_fills[('T1','O1')]==ws.generation
        assert not live.private_validity()['arcus_private'][0]
    asyncio.run(scenario())
