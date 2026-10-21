"""Offline startup chain through real adapters and wire frames; no native signer/network."""
import asyncio
import json
import time
from dataclasses import replace
from decimal import Decimal as D
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock

import pytest

from test_arcus_adapter import adapter, row, frame, MARKET
from test_maker_taker_engine import engine, maker, SYMBOL
from test_maker_taker_foreign_terminal import bridge
from test_lighter_strict_state import lifecycle_clients
from core.adapters.exchanges.adapters.lighter_websocket import LighterWebSocket
from core.services.arbitrage_monitor_v2.execution.maker_taker_engine import Book


def startup_live(e):
    live=bridge();live.arcus=adapter();live.journal=e.journal
    live.arcus._set_markets([{**MARKET,'marketDisplayName':'SOL-USD'}])
    live.arcus.get_open_orders=AsyncMock(return_value=[])
    live.lighter=NS(get_open_orders=AsyncMock(return_value=[]))
    e.transport.cancel_legacy=live.cancel_legacy
    return live


def startup_row(**kwargs):
    return row(marketDisplayName='SOL-USD',**kwargs)


async def subscribe(live,e):
    await live.arcus._websocket.subscribe_quantity_observations(lambda order:e.receive(live.normalize(order,'arcus')))


@pytest.mark.parametrize('status',['FILLED','CANCELED','REJECTED','EXPIRED','MARGIN_CANCELED'])
def test_startup_history_known_symbol_reaches_active_but_runtime_snapshots_halt(tmp_path,status):
    async def scenario():
        e=engine(tmp_path);live=startup_live(e);await subscribe(live,e)
        await live.arcus._websocket.handle_message(frame('orders',{'recentClosedOrders':[startup_row(status=status,filled='1')]}))
        assert '_startup_snapshot' not in live.arcus._orders['O1'].raw_data
        await e.start()
        assert e.state=='ACTIVE' and not e.halted
        events=e.journal.db.execute("SELECT payload FROM events WHERE kind='foreign_order_ignored'").fetchall()
        assert len(events)==1 and json.loads(events[0][0])['remaining']=='0'
        assert not e.transport.sends and not e.journal.makers()
        e.transport.maker=AsyncMock(wraps=e.transport.maker)
        await e.tick()
        assert e.transport.maker.await_count>0 and e.journal.makers(active=True)
        await live.arcus._websocket.handle_message(frame('orders',{'recentClosedOrders':[startup_row(status=status,filled='1',seq=2)]}))
        assert e.halt_state=='UNKNOWN_ORDER'
        with pytest.raises(RuntimeError,match='already attempted'):await e.recover()
        await e.stop(timeout=.1)
    asyncio.run(scenario())


@pytest.mark.parametrize('outcome',['canceled','timeout','partial','wrong_id','still_open'])
def test_startup_active_is_canceled_even_when_rest_omits_identity(tmp_path,outcome):
    async def scenario():
        e=engine(tmp_path);live=startup_live(e);await subscribe(live,e)
        await live.arcus._websocket.handle_message(frame('orders',{'openOrders':[startup_row()]}))
        assert len(e._startup_foreign)==1 and not e.halted
        result=live.arcus._merge_order(startup_row(status='CANCELED',seq=2))
        if outcome=='partial':result=live.arcus._merge_order(startup_row(status='CANCELED',filled='.1',seq=3))
        elif outcome=='wrong_id':result.id='OTHER'
        elif outcome=='still_open':result=live.arcus._merge_order(startup_row(status='OPEN',seq=3))
        live.arcus.cancel_known_order=AsyncMock(side_effect=TimeoutError() if outcome=='timeout' else None,return_value=result)
        assert await e.recover()==(outcome=='canceled')
        live.arcus.cancel_known_order.assert_awaited_once_with('O1',SYMBOL)
        assert e._startup_window is False
        assert bool(e.halted)==(outcome!='canceled') and not e.transport.sends
    asyncio.run(scenario())


@pytest.mark.parametrize('kind',['incremental','partial','client_collision','id_collision'])
def test_startup_exemption_does_not_hide_uncertain_or_conflicting_orders(tmp_path,kind):
    async def scenario():
        e=engine(tmp_path);live=startup_live(e);await subscribe(live,e)
        event=startup_row(status='CANCELED',filled='1')
        if kind=='partial':event=startup_row(status='OPEN',filled='.1')
        elif kind in ('client_collision','id_collision'):
            client=maker(e)
            if kind=='client_collision':event.update(clientId=client,side='SELL')
            else:e.journal.db.execute('UPDATE orders SET exchange_id=? WHERE client=?',('O1',client))
        await live.arcus._websocket.handle_message(frame('orders',{'orders':[event]},type='channel_data' if kind=='incremental' else 'subscribed'))
        if kind=='incremental':
            # An established subscription, not an ACK, can deliver runtime data during recovery.
            live.arcus._websocket._ready.add(('orders',None))
            await live.arcus._websocket.handle_message(frame('orders',{'orders':[event]},type='channel_data'))
        if kind in ('client_collision','id_collision'):
            assert not e.journal.db.execute("SELECT 1 FROM events WHERE kind='foreign_order_ignored'").fetchone()
            obs=e.queues[SYMBOL].get_nowait()
            with pytest.raises(ValueError):await e.observe_maker(obs)
        else:assert e.halt_state=='UNKNOWN_ORDER'
        assert not e._startup_foreign
    asyncio.run(scenario())


@pytest.mark.parametrize('positions',[(D('.0000001'),D(0)),(D(0),D('-.001')),(D('1'),D('-1'))])
def test_startup_positions_must_exactly_match_ledger_without_sending(tmp_path,positions):
    async def scenario():
        e=engine(tmp_path);client=maker(e)
        # Existing zero-fill order history must not authorize inventory adoption.
        e.journal.observe_maker(client,D(0),'canceled')
        e.transport.positions={SYMBOL:positions}
        assert not await e.recover() and e.halted
        assert e.journal.inventory(SYMBOL)==(D(0),D(0)) and not e.transport.sends
    asyncio.run(scenario())


def test_readiness_diagnostics_separate_private_and_each_venue_book(tmp_path):
    e=engine(tmp_path)
    e.transport.private_validity=lambda:{'arcus_private':(True,None,0,1),'lighter_private':(False,'reconnect',0,0)}
    del e.books['lighter',SYMBOL]
    diagnostics=e.readiness_diagnostics()
    assert diagnostics['private_validity']['lighter_private']=={'ready':False,'reason':'reconnect','deadline':0,'generation':0}
    assert diagnostics['books'][SYMBOL]['arcus']['ready']
    assert diagnostics['books'][SYMBOL]['lighter']=={'ready':False,'reason':'missing'}


def test_actual_live_connect_private_only_lighter_wire_ack_and_both_reconnects(tmp_path,lifecycle_clients,monkeypatch):
    config,_,_,_=lifecycle_clients
    class Socket:
        def __init__(self):self.frames=[];self.messages=asyncio.Queue()
        async def send(self,data):
            value=json.loads(data);self.frames.append(value)
            if value.get('channel')=='account_all_orders/7':
                self.messages.put_nowait(json.dumps({'type':'subscribed/account_all_orders','channel':'account_all_orders:7','orders':{}}))
        async def recv(self):
            value=await self.messages.get()
            if value is None:raise ConnectionError('offline peer closed')
            return value
        async def close(self,*args,**kwargs):pass
    sockets=[]
    class Connection:
        async def __aenter__(self):
            socket=Socket();sockets.append(socket);return socket
        async def __aexit__(self,*args):pass
    monkeypatch.setattr('core.adapters.exchanges.official_transport.websocket_connect',lambda *args,**kwargs:Connection())
    async def scenario():
        e=engine(tmp_path);live=bridge();live.arcus=adapter();live.journal=e.journal
        live.source=NS(markets={SYMBOL:({**MARKET,'marketDisplayName':'SOL-USD'}, {})},stale_seconds=5)
        live.arcus._do_authenticate=AsyncMock();live.arcus._websocket.connect=AsyncMock()
        ws=LighterWebSocket(config)
        ws._create_auth_token=AsyncMock(side_effect=['offline-token-1','offline-token-2','offline-token-3'])
        ws._heartbeat_loop=AsyncMock();ws._data_timeout_monitor=AsyncMock()
        async def connect():await ws.connect();return True
        live.lighter=NS(connect=connect,_websocket=ws)
        async def ready(generation):
            deadline=time.monotonic()+4
            while time.monotonic()<deadline:
                if getattr(ws,'generation',0)==generation and live.private_validity()['lighter_private'][0]:return
                await asyncio.sleep(.01)
            raise AssertionError('Private socket did not ACK the expected generation')
        try:
            await live.connect(e.receive);await ready(1)
            assert ws._subscribed_markets==[] and ws._private_subscription_requested
            sockets[0].messages.put_nowait(None)
            await ready(2)
            await ws.reconnect();await ready(3)
            for index,socket in enumerate(sockets):
                requests=[r for r in socket.frames if r.get('channel')=='account_all_orders/7']
                assert requests==[{'type':'subscribe','channel':'account_all_orders/7','auth':f'offline-token-{index+1}'}]
            assert len(sockets)==3 and ws._reader_generation==ws._private_ready_generation==3
        finally:await ws.disconnect()
    asyncio.run(scenario())


def test_rest_partial_fill_must_not_be_overwritten_by_zero_fill_snapshot(tmp_path):
    async def scenario():
        e=engine(tmp_path);live=startup_live(e);await subscribe(live,e)
        await live.arcus._websocket.handle_message(frame('orders',{'openOrders':[startup_row()]}))
        live.arcus.get_open_orders=AsyncMock(return_value=[live.arcus._merge_order(startup_row(filled='.1',seq=2))])
        live.arcus.cancel_known_order=AsyncMock()
        assert not await e.recover() and e.halted
        live.arcus.cancel_known_order.assert_not_awaited()
        assert not e.transport.sends
    asyncio.run(scenario())


@pytest.mark.parametrize('outcome',['canceled','timeout','partial'])
def test_active_snapshot_arriving_during_startup_position_read_cannot_reach_active(tmp_path,outcome):
    async def scenario():
        e=engine(tmp_path);live=startup_live(e);await subscribe(live,e)
        async def snapshot():
            await live.arcus._websocket.handle_message(frame('orders',{'openOrders':[startup_row()]}))
            return {'authoritative':True,'equity':e.equity,'positions':{}}
        e.transport.snapshot=snapshot
        async def cancel(order_id,symbol):
            if outcome=='timeout':raise TimeoutError()
            return live.arcus._merge_order(startup_row(status='CANCELED',filled='.1' if outcome=='partial' else '0',seq=2))
        live.arcus.cancel_known_order=AsyncMock(side_effect=cancel)
        assert not await e.recover() and e.halted and e.state!='ACTIVE'
        assert e.halt_reason=='Startup orders arrived during reconciliation'
        live.arcus.cancel_known_order.assert_awaited_once_with('O1',SYMBOL)
        assert not e.transport.sends
    asyncio.run(scenario())


@pytest.mark.parametrize('venue',['arcus','lighter'])
def test_even_sub_step_startup_drift_from_existing_ledger_halts(tmp_path,venue):
    async def scenario():
        e=engine(tmp_path);client=maker(e,qty='.1')
        await e.observe_maker(replace(bridge().normalize(
            adapter()._merge_order(row(status='FILLED',filled='.1',originalSize='.1',remainingSize='0')),'arcus'),
            symbol=SYMBOL,client=client,account='fake'))
        await e.drain_symbol(SYMBOL)
        expected=e.journal.inventory(SYMBOL);e.transport.sends.clear()
        actual=list(expected);actual[0 if venue=='arcus' else 1]+=D('.0000001')
        e.transport.positions={SYMBOL:actual}
        assert not await e.recover() and e.halt_reason=='Startup positions do not exactly match ledger'
        assert e.journal.inventory(SYMBOL)==expected and not e.transport.sends
    asyncio.run(scenario())


def test_runner_timeout_error_contains_private_and_per_venue_book_diagnostics(tmp_path,monkeypatch,capsys):
    import run_maker_taker
    from pathlib import Path
    from core.services.arbitrage_monitor_v2.execution.maker_taker_paper import PublicMarkets
    data=json.loads(Path('tests/fixtures/maker_taker_replay.json').read_text())
    class NoBooks(PublicMarkets):
        async def metadata(self):
            self.load_metadata(data['arcus_markets'],data['lighter_markets']);return self.rules
        async def start(self,callback):pass
    ticks=iter([0,31])
    monkeypatch.setattr(run_maker_taker,'time',NS(monotonic=lambda:next(ticks)))
    monkeypatch.setattr('core.services.arbitrage_monitor_v2.execution.maker_taker_paper.PublicMarkets',NoBooks)
    assert run_maker_taker.main(['--mode','monitor','--duration-seconds','.1','--state-dir',str(tmp_path/'runner')])==2
    output=capsys.readouterr()
    assert 'Public/private subscriptions did not become ready' in output.err
    diagnostics=json.loads(output.err.split('Public/private subscriptions did not become ready: ',1)[1].splitlines()[0])
    assert diagnostics['private_validity']=={}
    assert set(diagnostics['books'][SYMBOL])=={'arcus','lighter'}
    assert all(not value['ready'] for value in diagnostics['books'][SYMBOL].values())


@pytest.mark.parametrize('venue',['lighter','arcus'])
def test_exchange_ids_are_venue_scoped_but_attempt_clients_remain_known(tmp_path,venue):
    async def scenario():
        e=engine(tmp_path);live=startup_live(e);await subscribe(live,e)
        client=maker(e)
        oid=e.journal.observe_maker(client,D('.1'),'canceled',average=D(100))
        attempt=e.journal.prepare_attempt(oid,D('.1'),venue=venue)
        e.journal.observe_attempt(attempt,D(0),'canceled',exchange_id='O1')
        await live.arcus._websocket.handle_message(frame('orders',{'orders':[startup_row(status='CANCELED',filled='1')]}))
        ignored=e.journal.db.execute("SELECT count(*) FROM events WHERE kind='foreign_order_ignored'").fetchone()[0]
        assert ignored==(1 if venue=='lighter' else 0)
        assert e.queues[SYMBOL].qsize()==(0 if venue=='lighter' else 1)
    asyncio.run(scenario())


@pytest.mark.parametrize('late_fill',['none','startup','runtime'])
def test_legacy_no_client_callback_and_late_fills_never_create_hedges(tmp_path,late_fill):
    async def scenario():
        e=engine(tmp_path);live=startup_live(e);await subscribe(live,e)
        def event(status='OPEN',filled='0',seq=1):
            value=startup_row(status=status,filled=filled,seq=seq);value.pop('clientId');return value
        async def deliver(value):
            await live.arcus._websocket.handle_message(frame('orders',{'orders':[value]},type='channel_data'))
        await live.arcus._websocket.handle_message(frame('orders',{'openOrders':[event()]}))
        async def canceled(*args):
            await deliver(event('CANCELED',seq=2))
            result=live.arcus._orders['O1']
            if late_fill=='startup':await deliver(event('CANCELED',filled='.1',seq=3))
            return result
        live.arcus.cancel_known_order=AsyncMock(side_effect=canceled)
        assert await e.recover()==(late_fill!='startup')
        if late_fill=='runtime':
            await deliver(event('CANCELED',filled='.1',seq=3))
            while not e.queues[SYMBOL].empty():
                obs=e.queues[SYMBOL].get_nowait()
                try:await e.observe_maker(obs)
                finally:e.queues[SYMBOL].task_done()
        if late_fill!='none':
            assert e.halted and e._legacy_blocked
            blocked=e.journal.db.execute("SELECT payload FROM events WHERE kind='legacy_fill_blocked'").fetchone()
            assert json.loads(blocked[0])['qty']=='0.1'
            assert not await e.reconcile()
        else:assert not e.halted and e.state=='ACTIVE'
        assert e.journal.maker('O1')['purpose']=='legacy' and D(e.journal.maker('O1')['filled'])==0
        assert not e.journal.obligations() and e.journal.inventory(SYMBOL)==(D(0),D(0))
        await e.stop(timeout=.1)
        assert not e.transport.sends and not e.transport.emergencies
    asyncio.run(scenario())


@pytest.mark.parametrize('stored_fill',[False,True])
def test_restart_legacy_lookup_or_preexisting_fill_remains_blocked_through_stop(tmp_path,stored_fill):
    async def scenario():
        e=engine(tmp_path);live=startup_live(e)
        order=live.arcus._merge_order(startup_row())
        client=e.journal.legacy_cancel_intent(order)
        filled=live.normalize(live.arcus._merge_order(startup_row(status='CANCELED',filled='.1',seq=2)),'arcus')
        if stored_fill:
            e.journal.observe_maker(client,D('.1'),'canceled',order.id,reference=D(100),average=D(100))
        e.transport.lookup_maker=AsyncMock(return_value=filled)
        expected=e.journal.inventory(SYMBOL)
        assert not await e.recover() and e.halted and e._legacy_blocked
        if not stored_fill:assert not e.journal.obligations() and D(e.journal.maker(client)['filled'])==0
        await e.drain_symbol(SYMBOL)
        await e.stop(timeout=.1)
        assert e.journal.inventory(SYMBOL)==expected
        assert not e.transport.sends and not e.transport.emergencies
    asyncio.run(scenario())


def test_legacy_halt_keeps_normal_strategy_late_fill_hedging(tmp_path):
    from test_maker_taker_engine import observation
    async def scenario():
        e=engine(tmp_path);live=startup_live(e)
        legacy=live.arcus._merge_order(startup_row())
        client=e.journal.legacy_cancel_intent(legacy)
        assert e.block_legacy_fill(e.journal.maker(client),D('.1'),'O1')
        strategy=maker(e)
        await e.observe_maker(observation(e.journal.maker(strategy),'.1','canceled',average=D(100)))
        await e.drain_symbol(SYMBOL)
        assert e.halted and len(e.transport.sends)==1 and not e.journal.obligations()
        assert not e.transport.emergencies
    asyncio.run(scenario())


@pytest.mark.parametrize('stage',['failed_recover','readiness_failure'])
def test_reopened_legacy_fill_cannot_be_adopted_during_failed_startup_cleanup(tmp_path,stage):
    from test_maker_taker_engine import Fake
    from core.services.arbitrage_monitor_v2.execution.maker_taker_engine import MakerTakerEngine
    from core.services.arbitrage_monitor_v2.execution.maker_taker_journal import Journal
    async def scenario():
        old=engine(tmp_path);live=startup_live(old);await subscribe(live,old)
        await live.arcus._websocket.handle_message(frame('orders',{'openOrders':[startup_row()]}))
        live.arcus.cancel_known_order=AsyncMock(return_value=live.arcus._merge_order(startup_row(status='CANCELED',seq=2)))
        assert await old.recover()
        await live.arcus._websocket.handle_message(frame('orders',{'orders':[startup_row(status='CANCELED',filled='.1',seq=3)]},type='channel_data'))
        while not old.queues[SYMBOL].empty():
            obs=old.queues[SYMBOL].get_nowait()
            try:await old.observe_maker(obs)
            finally:old.queues[SYMBOL].task_done()
        assert old._legacy_blocked and old.journal.inventory(SYMBOL)==(D(0),D(0))
        path=old.journal.path;old.journal.close()
        journal=Journal(path,'fake');transport=Fake()
        restarted=MakerTakerEngine(old.profile,journal,transport,old.rules)
        restarted.books=old.books.copy();restarted.generations=old.generations.copy()
        transport.positions={SYMBOL:(D('.1'),D(0))}
        assert not restarted._startup_recovered and not restarted._legacy_blocked
        if stage=='failed_recover':assert not await restarted.recover()
        assert not await restarted.stop(timeout=.2)
        assert journal.inventory(SYMBOL)==(D(0),D(0)) and not journal.obligations()
        assert not transport.sends and not transport.emergencies
        journal.close()
    asyncio.run(scenario())


@pytest.mark.parametrize('first_cancel',['canceled','timeout'])
def test_startup_race_cancellation_drains_new_identity_arriving_while_cancel_awaits(tmp_path,first_cancel):
    async def scenario():
        e=engine(tmp_path);live=startup_live(e);await subscribe(live,e)
        async def snapshot():
            await live.arcus._websocket.handle_message(frame('orders',{'openOrders':[startup_row()]}))
            return {'authoritative':True,'equity':e.equity,'positions':{SYMBOL:(D('.1'),D(0))}}
        async def cancel(order_id,symbol):
            if order_id=='O1':
                await live.arcus._websocket.handle_message(frame('orders',{'openOrders':[
                    startup_row(order_id='O2',clientId='Client-B')]}))
                if first_cancel=='timeout':raise TimeoutError('offline first cancellation timed out')
            return live.arcus._merge_order(startup_row(order_id=order_id,clientId='Client-A' if order_id=='O1' else 'Client-B',
                                                       status='CANCELED',seq=2))
        e.transport.snapshot=snapshot
        live.arcus.cancel_known_order=AsyncMock(side_effect=cancel)
        assert not await e.recover() and e.halted and e.state!='ACTIVE'
        assert [call.args[0] for call in live.arcus.cancel_known_order.await_args_list]==['O1','O2']
        assert not e._startup_foreign and not e.journal.makers(active=True)
        assert {row['exchange_id'] for row in e.journal.makers()}=={'O1','O2'}
        await e.stop(timeout=.1)
        assert e.journal.inventory(SYMBOL)==(D(0),D(0)) and not e.journal.obligations()
        assert not e.transport.sends and not e.transport.emergencies
        assert [call.args[0] for call in live.arcus.cancel_known_order.await_args_list]==['O1','O2']
    asyncio.run(scenario())


def test_operator_accept_flat_reset_lets_startup_pass_after_manual_close(tmp_path):
    # Live HYPE: hedged pair closed manually on both venues; ledger reset to flat must pass startup exactly once.
    async def scenario():
        e=engine(tmp_path);client=maker(e,qty='.1')
        await e.observe_maker(replace(bridge().normalize(
            adapter()._merge_order(row(status='FILLED',filled='.1',originalSize='.1',remainingSize='0')),'arcus'),
            symbol=SYMBOL,client=client,account='fake'))
        await e.drain_symbol(SYMBOL)
        a,l=e.journal.inventory(SYMBOL);assert a and a+l==0 and not e.journal.obligations(SYMBOL)
        e.transport.sends.clear();e.transport.positions={SYMBOL:[D(0),D(0)]}
        e.journal.reconcile_inventory(SYMBOL,D(0),D(0),D(100))
        assert e.journal.inventory(SYMBOL)==(D(0),D(0)) and not e.journal.obligations(SYMBOL)
        assert await e.reconcile(initial=True) is not False and not e.halt_reason
        assert not e.journal.pending_reconcile(SYMBOL) and not e.transport.sends
    asyncio.run(scenario())
