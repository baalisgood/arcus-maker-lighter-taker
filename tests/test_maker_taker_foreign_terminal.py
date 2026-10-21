"""Offline private-order evidence boundaries; no exchange connections or credentials."""
import asyncio
import json
from dataclasses import replace
from decimal import Decimal as D
from unittest.mock import AsyncMock, Mock

import pytest

from test_arcus_adapter import adapter, row, frame
from test_maker_taker_engine import engine, maker, SYMBOL
from core.services.arbitrage_monitor_v2.execution.maker_taker_live import LiveTransport

FOREIGN='BNB-USDC-PERP'


def bridge():
    live=object.__new__(LiveTransport)
    live.identity='fake'
    return live


def foreign_row(**kwargs):
    return row(marketDisplayName='BNB-USD', **kwargs)


def ignored(e):
    return e.journal.db.execute("SELECT payload FROM events WHERE kind='foreign_order_ignored'").fetchall()


@pytest.mark.parametrize('status',['FILLED','CANCELED','REJECTED','EXPIRED','MARGIN_CANCELED'])
def test_private_terminal_zero_is_logged_without_queue_or_halt(tmp_path,status):
    async def scenario():
        e=engine(tmp_path);a=adapter();live=bridge()
        await a._websocket.subscribe_quantity_observations(lambda order:e.receive(live.normalize(order,'arcus')))
        for _ in range(2):
            await a._websocket.handle_message(frame('orders',{'recentClosedOrders':[foreign_row(status=status,filled='1')]}))
        assert not e.halt_reason and e.state=='ACTIVE' and e.sequence==0
        assert all(q.empty() for q in e.queues.values())
        events=ignored(e);assert len(events)==2
        payload=json.loads(events[0][0])
        assert payload['remaining']=='0' and payload['order_id']=='O1'
        assert set(payload)=={'venue','order_id','status','reported_status','cumulative','remaining'}
    asyncio.run(scenario())


@pytest.mark.parametrize('fields',[
    {'status':'CANCELED','remainingSize':'.1'},
    {'status':'FILLED','remainingSize':'.1'},
    {'status':'OPEN','remainingSize':'0'},
    {'status':'FILLED','state':'OPEN','remainingSize':'0'},
    {'status':'FILLED','state':'CANCELED','remainingSize':'0'},
    {'status':'OPEN','state':'FILLED','remainingSize':'0'},
    {'status':'ACK','remainingSize':'0'},
    {'status':'DEFERRED','remainingSize':'0'},
    {'status':'CANCELED','remainingSize':'0','cancelReason':'MODIFY_CANCELED'},
])
def test_uncertain_or_nonzero_foreign_rows_still_halt(tmp_path,fields):
    a=adapter();e=engine(tmp_path)
    order=a._merge_order(foreign_row(**fields))
    e.receive(bridge().normalize(order,'arcus'))
    assert e.halt_state=='UNKNOWN_ORDER' and not ignored(e)


@pytest.mark.parametrize('missing',['status','remainingSize','originalSize'])
def test_fresh_partial_row_cannot_inherit_terminal_proof(tmp_path,missing):
    a=adapter();live=bridge();e=engine(tmp_path)
    a._merge_order(foreign_row(status='CANCELED',filled='1'))
    update=foreign_row(status='CANCELED',filled='1',seq=2);update.pop(missing)
    order=a._merge_order(update)
    e.receive(live.normalize(order,'arcus'))
    assert e.halt_state=='UNKNOWN_ORDER' and not ignored(e)


@pytest.mark.parametrize('original,remaining',[
    ('1',None), (None,'0'), ('NaN','0'), ('Infinity','0'),
    ('1','NaN'), ('1','Infinity'), ('1','-1'), ('-1','0'), ('1','2'), ('bad','0')])
def test_invalid_raw_evidence_never_uses_clamped_remaining(tmp_path,original,remaining):
    a=adapter();e=engine(tmp_path)
    order=a._merge_order(foreign_row(status='FILLED',filled='1'))
    order.raw_data['_reported_order'].update(originalSize=original,remainingSize=remaining)
    assert order.remaining==0
    e.receive(replace(bridge().normalize(order,'arcus'),startup_snapshot=True))
    assert e.halt_state=='UNKNOWN_ORDER' and not ignored(e)


def test_configured_observation_still_queues_and_existing_halt_is_retained(tmp_path):
    a=adapter();e=engine(tmp_path);live=bridge()
    observation=live.normalize(a._merge_order(foreign_row(status='CANCELED',filled='1')),'arcus')
    configured=replace(observation,symbol=SYMBOL)
    configured=replace(configured,client=maker(e))
    e.receive(configured)
    assert e.queues[SYMBOL].get_nowait() is configured and e.sequence==1
    e.halt('EMERGENCY','existing halt')
    e.receive(replace(observation,startup_snapshot=True))
    assert e.halt_state=='EMERGENCY' and e.halt_reason=='existing halt' and len(ignored(e))==1


def test_user_fill_direct_mutation_invalidates_terminal_proof(tmp_path):
    async def scenario():
        a=adapter();e=engine(tmp_path);live=bridge()
        a._merge_order(foreign_row(status='CANCELED',filled='0'))
        a._websocket._recover_order=Mock()
        await a._websocket.subscribe_quantity_observations(lambda order:e.receive(live.normalize(order,'arcus')))
        fill=foreign_row(status='CANCELED',filled='1',seq=2,tradeId='T1',size='.5',price='100')
        a._websocket._ready.add(('userFills',None))
        await a._websocket.handle_message(frame('userFills',{'fills':[fill]},type='channel_data'))
        assert '_reported_order' not in a._orders['O1'].raw_data
        assert e.halt_state=='UNKNOWN_ORDER' and not ignored(e)
        a._websocket._recover_order.assert_called_once()
    asyncio.run(scenario())


def test_terminal_without_raw_proof_still_halts(tmp_path):
    a=adapter();e=engine(tmp_path)
    order=a._parse_order(foreign_row(status='FILLED',filled='1'))
    e.receive(bridge().normalize(order,'arcus'))
    assert e.halt_state=='UNKNOWN_ORDER' and not ignored(e)


def test_foreign_inventory_still_blocks_startup_after_ignored_order(tmp_path):
    async def scenario():
        e=engine(tmp_path);a=adapter()
        e.receive(replace(bridge().normalize(a._merge_order(foreign_row(status='CANCELED',filled='1')),'arcus'),startup_snapshot=True))
        e.transport.positions={FOREIGN:(D('1'),D('0'))}
        assert not await e.reconcile(initial=True)
        assert e.halt_state=='UNKNOWN_ORDER' and e.halt_reason=='Disabled/foreign inventory blocks startup'
    asyncio.run(scenario())


def test_stale_terminal_row_cannot_replace_newer_active_evidence(tmp_path):
    a=adapter();e=engine(tmp_path)
    a._merge_order(foreign_row(status='OPEN',seq=2))
    order=a._merge_order(foreign_row(status='CANCELED',filled='1',seq=1))
    e.receive(bridge().normalize(order,'arcus'))
    assert e.halt_state=='UNKNOWN_ORDER' and not ignored(e)


@pytest.mark.parametrize('status',['deferred','pending','unknown'])
def test_nonterminal_observation_cannot_pass_on_forged_terminal_metadata(tmp_path,status):
    a=adapter();e=engine(tmp_path)
    observation=bridge().normalize(a._merge_order(foreign_row(status='CANCELED',filled='1')),'arcus')
    e.receive(replace(observation,status=status))
    assert e.halt_state=='UNKNOWN_ORDER' and not ignored(e)


@pytest.mark.parametrize('missing',['originalSize','remainingSize',None])
def test_uncertain_new_fill_failed_rest_cannot_reuse_terminal_proof(tmp_path,missing):
    async def scenario():
        a=adapter();e=engine(tmp_path);live=bridge()
        a._merge_order(foreign_row(status='CANCELED',filled='1'))
        a._request=AsyncMock(side_effect=TimeoutError('offline recovery timeout'))
        await a._websocket.subscribe_quantity_observations(lambda order:e.receive(live.normalize(order,'arcus')))
        fill=foreign_row(status='OPEN',filled='.5',seq=2,tradeId='T2',size='.5',price='100')
        if missing:fill.pop(missing)
        await a._websocket.handle_message(frame('userFills',{'fills':[fill]}))
        await asyncio.gather(*tuple(a._websocket._recovery_tasks))
        assert '_reported_order' not in a._orders['O1'].raw_data
        assert 'O1' in a._uncertain and a._request.await_count
        assert e.halt_state=='UNKNOWN_ORDER' and not ignored(e)
    asyncio.run(scenario())


def test_margin_canceled_no_average_is_ignored_only_for_foreign_symbol(tmp_path):
    async def scenario():
        a=adapter();e=engine(tmp_path);live=bridge();observed=[]
        def receive(order):
            observation=live.normalize(order,'arcus');observed.append(observation);e.receive(observation)
        await a._websocket.subscribe_quantity_observations(receive)
        terminal=foreign_row(status='MARGIN_CANCELED',filled='1');terminal.pop('avgFillPrice')
        await a._websocket.handle_message(frame('orders',{'recentClosedOrders':[terminal]}))
        observation=observed[0]
        assert observation.status=='pending' and observation.reported_status=='margin_canceled'
        assert not e.halt_reason and len(ignored(e))==1
        configured=replace(observation,symbol=SYMBOL,client=maker(e))
        e.receive(configured)
        assert e.queues[SYMBOL].get_nowait() is configured
        assert configured.status=='pending' and e.sequence==1
    asyncio.run(scenario())


def test_historical_order_reusing_ledger_client_id_is_foreign(tmp_path):
    # Live: startup snapshot held an old BTC order whose clientId equalled the ledger's HYPE amlt-N maker.
    async def scenario():
        e=engine(tmp_path);a=adapter();live=bridge()
        client=maker(e);e.journal.observe_maker(client,D(0),'open','OURS')
        await a._websocket.subscribe_quantity_observations(lambda order:e.receive(live.normalize(order,'arcus')))
        await a._websocket.handle_message(frame('orders',{'recentClosedOrders':[
            foreign_row(order_id='OLD',clientId=client,status='FILLED',filled='1')]}))
        assert not e.halt_reason and all(q.empty() for q in e.queues.values())
        assert len(ignored(e))==1
        assert e.journal.db.execute("SELECT 1 FROM events WHERE kind='client_id_reused_by_foreign_order'").fetchone()
    asyncio.run(scenario())
