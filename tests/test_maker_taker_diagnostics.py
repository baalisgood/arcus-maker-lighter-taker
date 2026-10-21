"""Received-frame liveness and durable execution diagnostics; offline only."""
import asyncio
import json
import sqlite3
from decimal import Decimal as D
import pytest
import time
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock,Mock

from core.services.arbitrage_monitor_v2.execution.maker_taker_paper import PublicMarkets
from test_maker_taker_engine import SYMBOL


def source():
    now=[100.0];s=PublicMarkets([SYMBOL],clock=lambda:now[0])
    s.connected={'arcus':True,'lighter':True};s.generations={'arcus':1,'lighter':1}
    for venue in ('arcus','lighter'):s._publish(venue,SYMBOL,[(100,10)],[(101,10)])
    return s,now


def test_static_book_is_valid_with_real_inbound_heartbeat_without_depth_refresh():
    s,now=source();book=s.books['lighter',SYMBOL]
    for second in range(1,31):
        now[0]=100+second;s.received('lighter',1)
        assert s.book_validity('lighter',SYMBOL)[0]
    assert s.books['lighter',SYMBOL] is book and book.received==100


def test_silent_connection_times_out_and_old_generation_pong_does_not_revive():
    s,now=source();now[0]=106
    assert s.book_validity('lighter',SYMBOL)==(False,'connection_timeout',105)
    s.received('lighter',0)
    assert not s.book_validity('lighter',SYMBOL)[0]
    s.received('lighter',1)
    assert s.book_validity('lighter',SYMBOL)[0]


def close_payload(j):
    row=j.db.execute("SELECT id,payload FROM events WHERE kind='maker_fill' AND json_extract(payload,'$.purpose')='close'").fetchone()
    return row['id'],json.loads(row['payload'])


def test_close_pending_partial_confirmed_enrichment_preserves_economic_fields_and_intent(tmp_path):
    from test_maker_taker_close_floor import setup_basis,opened,settle
    j,basis=setup_basis(tmp_path);opened(j,1,'1','100','98')
    c=j.prepare_maker(SYMBOL,-1,'close',D(1),D(103),0,D(103),D(103),close_quote={'mode':'floor'})
    oid=j.observe_maker(c,D(1),'filled',average=D(103));event,original=close_payload(j)
    basis.sync();_,p=close_payload(j)
    assert p['close_mode']=='floor' and p['roundtrip_status']=='pending' and p['roundtrip_pnl_usd'] is None
    settle(j,oid,'.4',average='100');basis.sync();_,p=close_payload(j)
    assert p['roundtrip_status']=='partial' and D(p['roundtrip_confirmed_qty'])==D('.4')
    assert D(p['roundtrip_pnl_usd'])==D('.4') and D(p['roundtrip_pnl_pct'])==D(1)
    settle(j,oid,'.6',average='100');basis.sync();_,p=close_payload(j)
    assert p['roundtrip_status']=='confirmed' and D(p['roundtrip_pnl_usd'])==D(1)
    assert {key:p[key] for key in original}==original
    audits=[json.loads(r[0]) for r in j.db.execute("SELECT payload FROM events WHERE kind='close_fill_enriched'")]
    assert len(audits)==2 and sum(a['roundtrip_completed'] for a in audits)==1
    assert all(a['maker_event_id']==event for a in audits)
    from core.services.arbitrage_monitor_v2.execution.maker_taker_close_basis import CloseBasis
    CloseBasis(j,[SYMBOL]);assert j.db.execute("SELECT count(*) FROM events WHERE kind='close_fill_enriched'").fetchone()[0]==2


def test_unknown_actual_cost_and_all_restored_are_not_profit(tmp_path):
    from test_maker_taker_close_floor import setup_basis,opened,settle,closing
    j,basis=setup_basis(tmp_path);opened(j,1,'1','100',None)
    oid=closing(j,'1');settle(j,oid,'1',average='100');basis.sync();_,p=close_payload(j)
    assert p['close_mode'] is None and p['roundtrip_status']=='unknown' and p['roundtrip_pnl_usd'] is None
    j.close()
    other=tmp_path/'other';other.mkdir();j,basis=setup_basis(other);opened(j,1,'1','100','98')
    oid=closing(j,'1');settle(j,oid,'1','arcus');basis.sync();_,p=close_payload(j)
    assert p['roundtrip_status']=='restored' and p['roundtrip_pnl_pct'] is None
    assert D(p['roundtrip_confirmed_qty'])==0 and D(p['roundtrip_restored_qty'])==1


def test_diagnostic_failure_rolls_back_payload_audit_cursor_and_retries_once(tmp_path):
    from test_maker_taker_close_floor import setup_basis,opened,settle,closing
    j,basis=setup_basis(tmp_path);opened(j,1,'1','100','98');oid=closing(j,'1');basis.sync()
    event,p=close_payload(j);cursor=basis.cursor;durable=basis.diagnostics.cursor
    settle(j,oid,'1',average='100')
    j.db.execute(f"CREATE TRIGGER fail_diagnostic BEFORE UPDATE ON events WHEN NEW.id={event} BEGIN SELECT RAISE(ABORT,'diagnostic failure'); END")
    with pytest.raises(sqlite3.DatabaseError):basis.sync()
    assert basis.cursor==cursor and basis.diagnostics.cursor==durable and close_payload(j)[1]==p
    assert not j.db.execute("SELECT 1 FROM events WHERE kind='close_fill_enriched'").fetchone()
    j.db.execute('DROP TRIGGER fail_diagnostic');basis.sync()
    assert D(close_payload(j)[1]['roundtrip_pnl_usd'])==D(999)-D(100)+D(98)-D(100)
    assert j.db.execute("SELECT count(*) FROM events WHERE kind='close_fill_enriched'").fetchone()[0]==1


def test_short_same_socket_timeout_preserves_ema_but_pauses_warmup(tmp_path):
    from test_maker_taker_engine import engine
    e=engine(tmp_path);s,now=source();e.transport.source=s;e.transport.healthy=s.healthy;e.clock=s.clock
    e.books=s.books.copy();e.reset_basis(SYMBOL);e.profile=__import__('dataclasses').replace(e.profile,
        risk=__import__('dataclasses').replace(e.profile.risk,stale_book_seconds=5))
    e.basis_center(SYMBOL);ema=e.basis[SYMBOL]
    for value in range(101,121):
        now[0]=value
        for v in ('arcus','lighter'):s.received(v,1)
        e.basis_center(SYMBOL)
    now[0]=126;assert e.basis_center(SYMBOL) is None and e.basis[SYMBOL] is ema
    now[0]=131
    for v in ('arcus','lighter'):s.received(v,1)
    assert e.basis_center(SYMBOL) is not None and e.basis[SYMBOL] is ema
    assert e.warmup_started[SYMBOL]==106
    for value in range(132,167):
        now[0]=value
        for v in ('arcus','lighter'):s.received(v,1)
        e.basis_center(SYMBOL)
    assert e.quote_ready(SYMBOL)
    now[0]=182
    for v in ('arcus','lighter'):s.received(v,1)
    assert not e.quote_ready(SYMBOL) and e.basis[SYMBOL] is not ema


def test_stats_runtime_union_cross_hour_crash_clip_and_connection_episode_dedup(tmp_path):
    from core.services.arbitrage_monitor_v2.execution.maker_taker_journal import Journal
    from core.services.arbitrage_monitor_v2.history.maker_taker_statistics import statistics
    j=Journal(tmp_path/'stats.sqlite','fake');names=[SYMBOL,'ETH-USDC-PERP','BTC-USDC-PERP','HYPE-USDC-PERP']
    def event(at,symbol,kind,**p):j.db.execute('INSERT INTO events(at,symbol,kind,payload) VALUES(?,?,?,?)',(at,symbol,kind,json.dumps(p)))
    event(3500,'*','runtime_start',boot='R',symbols=names,modeled=True)
    event(3530,'*','runtime_observation',boot='R',quoted_symbols=[SYMBOL],symbols=names)
    for name in names:event(3580,name,'market_stale',boot='R',venue='lighter',reason='connection_timeout',since_wall=3580,episode='one-socket')
    event(3620,'*','runtime_observation',boot='R',quoted_symbols=[],symbols=names)
    for name in names:event(3630,name,'market_recovered',boot='R',venue='lighter',reason='connection_timeout',since_wall=3580,episode='one-socket')
    event(3650,'*','runtime_observation',boot='R',quoted_symbols=[],symbols=names)
    report=statistics(j);sol=[r for r in report if r['symbol']==SYMBOL]
    assert len(sol)==2
    assert [(r['observation_seconds'],r['quoted_seconds']) for r in sol]==[(100,70),(50,20)]
    assert sum(r['stale_seconds'] for r in sol)==50
    assert sum(r['stale_counts']['connection_timeout'] for r in report if r['symbol']=='*')==1
    assert all(len([r for r in report if r['symbol']==name])==2 for name in names)
    assert all(r['avg_hold_seconds'] is None and r['open_edge_pct'] is None for r in sol)
    assert sum(r['observation_seconds'] for r in sol)==150 # Crash never extends to report-now.


def test_stats_actual_tranches_period_counts_gross_and_notional_denominator(tmp_path,monkeypatch):
    from test_maker_taker_close_floor import setup_basis,opened,settle
    from core.services.arbitrage_monitor_v2.history.maker_taker_statistics import statistics
    now=[3600.0];monkeypatch.setattr('time.time',lambda:now[0]);j,basis=setup_basis(tmp_path)
    opened(j,1,'1','100','98');basis.sync();now[0]=3700
    c=j.prepare_maker(SYMBOL,-1,'close',D(1),D(103),0,D(103),D(103),close_quote={'mode':'floor'})
    oid=j.observe_maker(c,D(1),'filled',average=D(103));basis.sync()
    now[0]=3710;settle(j,oid,'.4',average='100');basis.sync()
    now[0]=7300;settle(j,oid,'.6',average='100');basis.sync();assert j.archive_closed()==2
    rows=[r for r in statistics(j) if r['symbol']==SYMBOL]
    assert [r['roundtrips'] for r in rows]==[0,1]
    assert [D(r['roundtrip_gross_pnl_usd']) for r in rows]==[D('.4'),D('.6')]
    assert rows[0]['pnl_per_10k_arcus_denominator']=='203'
    assert D(rows[0]['pnl_per_10k_arcus'])==D('.4')/D(203)*10000
    assert rows[1]['pnl_per_10k_arcus'] is None
    assert rows[0]['close_floor_count']==0 and rows[1]['close_floor_count']==1
    assert all(r['avg_hold_seconds']==100 and r['total_cost'] is None for r in rows)
    assert D(rows[0]['open_edge_pct'])==D(-2)
    assert D(rows[0]['close_edge_pct'])==100*D(3)/103


def test_close_counts_follow_completed_obligation_intent_not_unfilled_quotes_or_fill_age(tmp_path,monkeypatch):
    from test_maker_taker_close_floor import setup_basis,opened,settle
    from core.services.arbitrage_monitor_v2.history.maker_taker_statistics import statistics
    now=[0.0];monkeypatch.setattr('time.time',lambda:now[0]);j,basis=setup_basis(tmp_path)
    opened(j,1,'1','100','98');basis.sync();now[0]=599
    c=j.prepare_maker(SYMBOL,-1,'close',D(1),D(103),0,D(103),D(103),close_quote={'mode':'floor'})
    now[0]=601;oid=j.observe_maker(c,D(1),'filled',average=D(103));basis.sync()
    def report():return next(r for r in statistics(j) if r['symbol']==SYMBOL)
    assert report()['close_floor_count']==0
    settle(j,oid,'.4',average='100');basis.sync();assert report()['close_floor_count']==0
    settle(j,oid,'.6',average='100');basis.sync()
    opened(j,1,'1','100','98')
    j.prepare_maker(SYMBOL,-1,'close',D(1),D(103),0,D(103),D(103),close_quote={'mode':'ema'})
    basis.sync();r=report()
    assert r['close_floor_count']==1 and r['close_timeout_count']==0 and r['roundtrips']==1
    assert D(r['roundtrip_pnl_usd'])==D(r['roundtrip_gross_pnl_usd'])==1
    assert r['stale_events']==sum(r['stale_counts'].values())
    assert r['stale_events_by_reason']==r['stale_counts']


@pytest.mark.parametrize('stage',['write_lock','nonce','rollback_failure'])
def test_lighter_actual_presign_guard_after_each_await_and_nonce_rollback(stage):
    from core.adapters.exchanges.adapters.lighter_rest import LighterRest
    from core.adapters.exchanges.order_state import OrderAdmissionDeferredError,OrderStateUnknownError
    async def scenario():
        rest=object.__new__(LighterRest);rest._strict_write_lock=asyncio.Lock();rest._uncertain_orders={};rest._retired=False
        valid=[True];started=asyncio.Event();release=asyncio.Event()
        async def nonce():started.set();await release.wait();return 0,7
        manager=NS(acknowledge_failure=Mock(side_effect=RuntimeError('offline rollback') if stage=='rollback_failure' else None))
        rest.signer_client=NS(nonce_manager=manager,send_tx=AsyncMock());rest.write_audit=Mock();rest._next_order_nonce=AsyncMock(side_effect=nonce)
        sign=Mock(return_value=(1,'signed','hash',None))
        if stage=='write_lock':await rest._strict_write_lock.acquire()
        task=asyncio.create_task(rest._strict_submit(sign,{'price':100},'7',pre_sign_guard=lambda:valid[0]))
        if stage=='write_lock':
            await asyncio.sleep(0);valid[0]=False;rest._strict_write_lock.release()
        else:
            await started.wait();valid[0]=False;release.set()
        with pytest.raises(OrderStateUnknownError if stage=='rollback_failure' else OrderAdmissionDeferredError):await task
        sign.assert_not_called();rest.write_audit.assert_not_called();rest.signer_client.send_tx.assert_not_called()
        assert not rest._uncertain_orders
        if stage=='write_lock':rest._next_order_nonce.assert_not_called();manager.acknowledge_failure.assert_not_called()
        else:manager.acknowledge_failure.assert_called_once_with(0)
        if stage=='rollback_failure':assert rest._retired
    asyncio.run(scenario())


def test_private_silent_socket_not_healthy_despite_connected_ready_flags():
    from core.services.arbitrage_monitor_v2.execution.maker_taker_live import LiveTransport
    from collections import deque
    socket=object();now=time.monotonic()
    aw=NS(_ws_connected=True,_ready={('orders',None),('userFills',None)},last_received_monotonic=now-6,generation=1)
    lw=NS(_direct_ws=socket,_reader_socket=socket,_reader_connected=True,generation=1,_reader_generation=1,
          _private_ready_socket=socket,_private_ready_generation=1,last_received_monotonic=now)
    t=object.__new__(LiveTransport);t.arcus=NS(_websocket=aw);t.lighter=NS(_websocket=lw);t.writes=deque()
    t.source=NS(healthy=lambda:True,stale_seconds=5)
    assert not t.healthy() and t.private_validity()['arcus_private'][1]=='connection_timeout'
    aw.last_received_monotonic=time.monotonic();assert t.healthy()
    lw.last_received_monotonic=now-6;assert not t.healthy()


def private_engine(tmp_path,monkeypatch):
    from collections import deque
    from dataclasses import replace
    from test_maker_taker_engine import engine
    from core.adapters.exchanges.adapters.arcus_websocket import ArcusWebSocket
    from core.adapters.exchanges.adapters.lighter_websocket import LighterWebSocket
    from core.services.arbitrage_monitor_v2.execution.maker_taker_live import LiveTransport
    e=engine(tmp_path);fake=e.transport;s,now=source();monkeypatch.setattr('time.monotonic',lambda:now[0])
    aw=ArcusWebSocket(NS());aw._socket=object();aw.generation=1;aw._ws_connected=True
    aw._ready={('orders',None),('userFills',None)};aw.last_received_monotonic=100
    socket=object();lw=object.__new__(LighterWebSocket)
    lw._direct_ws=socket;lw._reader_socket=socket;lw._reader_connected=True;lw.generation=1;lw._reader_generation=1
    lw._private_ready_socket=socket;lw._private_ready_generation=1;lw.last_received_monotonic=100
    t=object.__new__(LiveTransport);t.arcus=NS(_websocket=aw);t.lighter=NS(_websocket=lw);t.writes=deque();t.source=s
    t.identity='fake';t.cancel_maker=fake.cancel_maker;t.snapshot=fake.snapshot
    t.configure_liveness(5,10)
    e.transport=t;e.books=s.books.copy();e.clock=s.clock;e.reset_basis(SYMBOL)
    e.profile=replace(e.profile,risk=replace(e.profile.risk,stale_book_seconds=5))
    e.basis_center(SYMBOL)
    return e,s,now,aw,lw,fake


@pytest.mark.parametrize('private_venue',['arcus','lighter'])
def test_private_short_timeout_preserves_ema_and_pauses_incomplete_warmup(tmp_path,monkeypatch,private_venue):
    e,s,now,aw,lw,_=private_engine(tmp_path,monkeypatch);ema=e.basis[SYMBOL]
    silent=aw if private_venue=='arcus' else lw;other=lw if private_venue=='arcus' else aw
    now[0]=106
    for venue in ('arcus','lighter'):s.received(venue,1)
    other.last_received_monotonic=106
    assert not e.data_ready() and e.basis[SYMBOL] is ema
    assert e.basis_center(SYMBOL) is None and e.warmup_started[SYMBOL]==100
    now[0]=107;silent.record_received();other.last_received_monotonic=107
    for venue in ('arcus','lighter'):s.received(venue,1)
    assert e.basis_center(SYMBOL) is not None and e.basis[SYMBOL] is ema
    assert e.warmup_started[SYMBOL]==102 and not e.quote_ready(SYMBOL)


@pytest.mark.parametrize('change',['long_timeout','missed_long_timeout','generation','socket','disconnect'])
def test_private_long_timeout_or_real_connection_change_restarts_warmup(tmp_path,monkeypatch,change):
    e,s,now,aw,lw,_=private_engine(tmp_path,monkeypatch);ema=e.basis[SYMBOL]
    if change=='missed_long_timeout':
        now[0]=110
        for venue in ('arcus','lighter'):s.received(venue,1)
        lw.last_received_monotonic=110
    if change not in ('missed_long_timeout',):
        now[0]=106
        for venue in ('arcus','lighter'):s.received(venue,1)
        lw.last_received_monotonic=106
        if change=='disconnect':aw._ws_connected=False
        assert e.basis_center(SYMBOL) is None
        if change=='disconnect':assert SYMBOL not in e.basis
    now[0]=115 if change in ('long_timeout','missed_long_timeout') else 107
    for venue in ('arcus','lighter'):s.received(venue,1)
    lw.last_received_monotonic=now[0]
    if change=='generation':aw.generation+=1
    if change=='socket':aw._socket=object()
    aw._ws_connected=True;aw.record_received()
    assert e.basis_center(SYMBOL) is not None and e.basis[SYMBOL] is not ema
    assert e.warmup_started[SYMBOL]==now[0] and not e.quote_ready(SYMBOL)


def test_private_short_timeout_still_globally_cancels_and_retains_completed_readiness(tmp_path,monkeypatch):
    from test_maker_taker_engine import maker
    async def scenario():
        e,s,now,aw,lw,fake=private_engine(tmp_path,monkeypatch);ema=e.basis[SYMBOL]
        e.warmup_started[SYMBOL]=40;client=maker(e)
        now[0]=106
        for venue in ('arcus','lighter'):s.received(venue,1)
        lw.last_received_monotonic=106
        await e.watchdog_once()
        assert fake.cancels==[client] and e.state=='PAUSED_DATA' and e.basis[SYMBOL] is ema
        now[0]=107;aw.record_received();lw.last_received_monotonic=107
        for venue in ('arcus','lighter'):s.received(venue,1)
        assert e.quote_ready(SYMBOL) and e.basis[SYMBOL] is ema and e.warmup_started[SYMBOL]==42
    asyncio.run(scenario())


def test_pending_reconcile_work_stays_bounded_with_historical_diagnostic_metadata(tmp_path):
    from test_maker_taker_close_floor import setup_basis
    j,_=setup_basis(tmp_path);work=[]
    for count in (0,3000):
        j.db.executemany('INSERT OR REPLACE INTO meta VALUES(?,?)',[(f'close_quote:{n}','{}') for n in range(count)])
        steps=[0]
        def progress():steps[0]+=1;return 0
        j.db.set_progress_handler(progress,1);assert not j.pending_reconcile();j.db.set_progress_handler(None,0);work.append(steps[0])
    assert max(work)<100 and abs(work[0]-work[1])<10
    j.db.execute("INSERT INTO meta VALUES(?,'pending')",('reconcile:'+SYMBOL,))
    assert j.pending_reconcile() and j.pending_reconcile(SYMBOL)


def test_actual_control_ping_pong_frames_call_receive_hook_without_business_messages():
    from websockets.client import ClientProtocol
    from websockets.uri import parse_uri
    from websockets.protocol import OPEN
    from websockets.http11 import Response
    from websockets.datastructures import Headers
    from websockets.frames import Frame,OP_PING,OP_PONG,OP_TEXT
    from core.adapters.exchanges.official_transport import ReceivedConnection
    async def scenario():
        received=[];protocol=ClientProtocol(parse_uri('wss://api.arcus.xyz/v1/ws'),state=OPEN)
        socket=ReceivedConnection(protocol,on_received=lambda:received.append(time.monotonic()),ping_interval=None)
        socket.connection_made(Mock());socket.response=Response(101,'Switching Protocols',Headers())
        for opcode,payload in ((OP_PING,b'control'),(OP_TEXT,b'{"type":"subscribed"}'),(OP_PONG,b'active-ping')):
            socket.data_received(Frame(opcode,payload).serialize(mask=False))
        assert len(received)==3 and await socket.recv()=='{"type":"subscribed"}'
        assert socket.transport.write.called # The real protocol generated its control PONG.
    asyncio.run(scenario())


@pytest.mark.parametrize('venue,gap',[('arcus',False),('lighter',False),('lighter',True)])
def test_official_sequence_contract_and_public_readonly_endpoint(monkeypatch,venue,gap):
    from pathlib import Path
    async def scenario():
        data=json.loads(Path('tests/fixtures/maker_taker_replay.json').read_text());s=PublicMarkets([SYMBOL]);s.load_metadata(data['arcus_markets'],data['lighter_markets'])
        captured=[];urls=[]
        if venue=='arcus':
            frames=[{'type':'subscribed','channel':'l2Orderbook','id':'SOL-USD','contents':{'lastSequenceId':104,'bids':[[100,1]],'asks':[[101,1]]}},
                    {'type':'channel_data','channel':'l2Orderbook','id':'SOL-USD','contents':{'lastSequenceId':104,'bids':[[50,1]],'asks':[[51,1]]}},
                    {'type':'channel_data','channel':'l2Orderbook','id':'SOL-USD','contents':{'lastSequenceId':207,'bids':[[101,1]],'asks':[[102,1]]}}]
        else:
            frames=[{'type':'subscribed/order_book','channel':'order_book:2','order_book':{'nonce':104,'bids':[{'price':'100','size':'1'}],'asks':[{'price':'101','size':'1'}]}},
                    {'type':'update/order_book','channel':'order_book:2','order_book':{'nonce':207,'begin_nonce':105 if gap else 104,
                     'bids':[{'price':'100','size':'2'}],'asks':[]}}]
        class Socket:
            async def __aenter__(self):return self
            async def __aexit__(self,*args):pass
            async def send(self,payload):pass
            def __aiter__(self):
                async def iterator():
                    for n,frame in enumerate(frames):
                        if n==len(frames)-1:s.stopped=True
                        yield json.dumps(frame)
                return iterator()
        def connect(url,**kwargs):
            urls.append(url);assert kwargs['ping_interval']==2.5 and kwargs['ping_timeout']>=15
            return Socket()
        monkeypatch.setattr('core.services.arbitrage_monitor_v2.execution.maker_taker_paper.websocket_connect',connect)
        s.callback=lambda *args:captured.append(args[-1]);await s._stream(venue)
        if venue=='lighter':assert urls==['wss://mainnet.zklighter.elliot.ai/stream?readonly=true']
        if gap:assert len(captured)==1 and s.invalid[venue,SYMBOL][0]=='sequence_gap'
        else:assert len(captured)==2
    asyncio.run(scenario())


def test_timeout_diagnostic_once_known_estimate_and_unknown_observation_age(tmp_path,monkeypatch):
    from test_maker_taker_close_floor import setup_basis,opened
    from core.services.arbitrage_monitor_v2.config.maker_taker_config import MakerSymbolConfig
    now=[100.0];monkeypatch.setattr('time.time',lambda:now[0]);j,basis=setup_basis(tmp_path)
    opened(j,1,'1','100','98');basis.sync();origin=basis.states[SYMBOL]['lots'][0]['origin']
    cfg=MakerSymbolConfig(SYMBOL,D(1),D(0),D(0),D(0),1)
    references=basis.timeout_estimates(SYMBOL,D(100),D(103))
    for _ in range(3):basis.diagnostics.timeouts(basis.states,{SYMBOL:cfg},700,references)
    rows=j.db.execute("SELECT payload FROM events WHERE kind='close_floor_timeout'").fetchall()
    assert len(rows)==1;p=json.loads(rows[0][0]);assert p['open_at']==100 and D(p['estimated_pnl_pct'])==1
    j.reconcile_inventory(SYMBOL,D(2),D(-2),D(100));basis.sync()
    basis.diagnostics.timeouts(basis.states,{SYMBOL:cfg},1000,{})
    # Reconcile observation is at 100, so this new conservative source has expired too.
    p=json.loads(j.db.execute("SELECT payload FROM events WHERE kind='close_floor_timeout' ORDER BY id DESC").fetchone()[0])
    assert p['open_at'] is None and p['observed_at']==100 and p['age_source']=='observation' and p['hold_seconds'] is None


def test_stale_intervals_and_episode_counts_are_scoped_to_each_boot(tmp_path):
    from core.services.arbitrage_monitor_v2.execution.maker_taker_journal import Journal
    from core.services.arbitrage_monitor_v2.history.maker_taker_statistics import statistics
    j=Journal(tmp_path/'stats.sqlite','fake')
    def event(at,kind,**p):
        j.db.execute('INSERT INTO events(at,symbol,kind,payload) VALUES(?,?,?,?)',
                     (at,SYMBOL if kind=='market_stale' else '*',kind,json.dumps(p)))
    event(3500,'runtime_start',boot='A',symbols=[SYMBOL])
    event(3550,'market_stale',boot='A',venue='lighter',reason='reconnect',since_wall=3550,episode='lighter:1:reconnect')
    event(3650,'runtime_end',boot='A') # Deliberately unrecovered interval at shutdown.
    event(3700,'runtime_start',boot='B',symbols=[SYMBOL])
    event(3750,'market_stale',boot='B',venue='lighter',reason='reconnect',since_wall=3750,episode='lighter:1:reconnect')
    event(3800,'runtime_observation',boot='B',quoted_symbols=[]) # Then crash, no runtime_end.
    rows=statistics(j);sol=[r for r in rows if r['symbol']==SYMBOL]
    assert [r['stale_seconds'] for r in sol]==[50,100]
    assert sum(r['stale_events'] for r in rows if r['symbol']=='*')==2
    assert all(r['stale_events_by_reason']==r['stale_counts'] for r in rows)


def test_archived_fill_obligation_joins_preserve_symbol_attribution(tmp_path):
    from test_maker_taker_close_floor import setup_basis,opened
    j,basis=setup_basis(tmp_path);opened(j,1,'1','100','98');basis.sync()
    assert j.archive_closed()==1 and not j.db.execute('SELECT 1 FROM orders').fetchone()
    fills=j.db.execute("SELECT symbol,payload FROM events WHERE kind IN ('maker_fill','hedge_fill')").fetchall()
    assert len(fills)==2
    for row in fills:
        oid=json.loads(row['payload'])['obligation']
        assert j.obligation(oid)['symbol']==row['symbol']==SYMBOL
        symbol=j.db.execute('SELECT m.symbol FROM all_obligations o JOIN all_orders m ON m.client=o.maker WHERE o.id=?',(oid,)).fetchone()[0]
        assert symbol==SYMBOL


@pytest.mark.parametrize('purpose',['open','close'])
@pytest.mark.parametrize('await_point',['write_lock','authentication'])
@pytest.mark.parametrize('condition',['fresh','timeout','generation','reconnect','sequence_gap','private_invalid'])
def test_actual_arcus_maker_guard_after_waits_for_open_and_close(tmp_path,purpose,await_point,condition):
    from dataclasses import replace
    from collections import deque
    from test_arcus_adapter import adapter,MARKET
    from test_maker_taker_engine import engine
    from test_maker_taker_close_floor import opened
    from core.services.arbitrage_monitor_v2.execution.maker_taker_live import LiveTransport
    async def scenario():
        e=engine(tmp_path);s,now=source();e.clock=s.clock;e.books=s.books.copy()
        e.profile=replace(e.profile,hedge=replace(e.profile.hedge,request_timeout=2),
                          risk=replace(e.profile.risk,stale_book_seconds=5))
        a=adapter();a._set_markets([{**MARKET,'marketDisplayName':'SOL-USD','tickSize':'.01','stepSize':'.001',
            'minOrderSize':'.01','maxOrderSize':'1000','markPrice':'100','tickTiers':[]}])
        a._websocket._socket=object();a._websocket._ws_connected=True;a._websocket.generation=1
        a._websocket._ready={('orders',None),('userFills',None)};a._websocket.last_received_monotonic=time.monotonic()
        socket=object();lw=NS(_direct_ws=socket,_reader_socket=socket,_reader_connected=True,generation=1,
            _reader_generation=1,_private_ready_socket=socket,_private_ready_generation=1,last_received_monotonic=time.monotonic())
        live=object.__new__(LiveTransport);live.arcus=a;live.lighter=NS(_websocket=lw);live.source=s;live.writes=deque();live.identity='fake'
        live.maker_guard=e.maker_admitted
        e.transport=live;live.configure_liveness(5,10);e.reset_basis(SYMBOL);e.basis_center(SYMBOL)
        e.warmup_started[SYMBOL]=40
        if purpose=='close':opened(e.journal,1,'1','100','98')
        entered=asyncio.Event();release=asyncio.Event()
        async def authenticate():
            if await_point=='authentication':entered.set();await release.wait()
        a._do_authenticate=AsyncMock(side_effect=authenticate);a._nonce=Mock(return_value=1791041528000000000)
        a._sign=Mock(return_value='offline-signature');a.write_audit=Mock();a._request=AsyncMock(return_value=(202,{'orderId':'O1'}))
        async def confirmed(order_id,symbol,**kwargs):
            row=e.journal.makers(active=True)[0]
            return a._parse_order(dict(orderId=order_id,clientId=row['client'],marketId=1,marketDisplayName='SOL-USD',
                side='BUY' if row['side']==1 else 'SELL',type='LIMIT',price=row['price'],originalSize=row['qty'],
                remainingSize=row['qty'],status='OPEN',address=a.address,accountIndex=0))
        a._await_order=AsyncMock(side_effect=confirmed)
        if await_point=='write_lock':await a._write_lock.acquire()
        side=1 if purpose=='open' else -1
        task=asyncio.create_task(e.place(SYMBOL,side,purpose,D(1),D('.03') if purpose=='open' else D(0),0 if purpose=='open' else 1))
        if await_point=='write_lock':
            for _ in range(20):
                await asyncio.sleep(0)
                if a._write_lock._waiters:break
            assert a._write_lock._waiters
        else:await entered.wait()
        if condition=='timeout':now[0]=106
        elif condition=='generation':s.generations['lighter']+=1
        elif condition=='reconnect':s.connected['lighter']=False
        elif condition=='sequence_gap':s.invalid['lighter',SYMBOL]=('sequence_gap',now[0])
        elif condition=='private_invalid':lw._reader_connected=False
        if await_point=='write_lock':a._write_lock.release()
        else:release.set()
        client=await task;assert client is not None
        if condition=='fresh':
            a._nonce.assert_called_once();a._sign.assert_called_once();a.write_audit.assert_called_once();a._request.assert_awaited_once()
            assert e.queues[SYMBOL].qsize()==1
            payload=a._request.await_args.kwargs['body']
            assert payload['timeInForce']=='ALO' and payload['reduceOnly']==(purpose=='close')
            assert '_pre_sign_guard' not in payload
        else:
            a._nonce.assert_not_called();a._sign.assert_not_called();a.write_audit.assert_not_called();a._request.assert_not_called()
            assert e.journal.maker(client)['status']=='rejected' and not e.halted
    asyncio.run(scenario())


def alarm_payloads(e):
    return [dict(symbol=row['symbol'],**json.loads(row['payload']))
            for row in e.journal.db.execute("SELECT symbol,payload FROM events WHERE kind='alarm' ORDER BY id")]


def test_market_alarm_episode_ignores_state_and_repeats_with_growing_duration(tmp_path):
    from dataclasses import replace
    from test_maker_taker_engine import engine
    names=[SYMBOL,'BTC-USDC-PERP','ETH-USDC-PERP','HYPE-USDC-PERP']
    e=engine(tmp_path);s=PublicMarkets(names,clock=lambda:now[0]);now=[100.0]
    s.connected={'arcus':True,'lighter':True};s.generations={'arcus':1,'lighter':1}
    for name in names:
        for venue in ('arcus','lighter'):s._publish(venue,name,[(100,10)],[(101,10)])
    e.profile=replace(e.profile,symbols={name:replace(e.profile.get_config(SYMBOL),symbol=name) for name in names})
    e.transport.source=s;e.clock=s.clock;e.wall_clock=lambda:1000+now[0];e.books=s.books.copy()
    now[0]=106;s.received('arcus',1);e.observe_market_health()
    first=alarm_payloads(e);assert len(first)==4
    assert {p['reason'] for p in first}=={'connection_timeout'}
    assert all(p['duration_seconds']==1 and p['since_wall']==1105 and p['diagnostic_source']=='public' for p in first)
    e.state='PAUSED_DATA';e.observe_market_health();assert len(alarm_payloads(e))==4
    now[0]=135.9;s.received('arcus',1);e.observe_market_health();assert len(alarm_payloads(e))==4
    now[0]=136;s.received('arcus',1);e.observe_market_health()
    assert len(alarm_payloads(e))==8 and all(p['duration_seconds']==31 for p in alarm_payloads(e)[4:])
    s.received('lighter',1);e.observe_market_health()
    recovered=[json.loads(row[0]) for row in e.journal.db.execute("SELECT payload FROM events WHERE kind='market_recovered'")]
    assert len(recovered)==4 and all(p['duration_seconds']==31 for p in recovered)
    now[0]=142;s.received('arcus',1);e.observe_market_health()
    assert len(alarm_payloads(e))==12 and not set(p['episode'] for p in first)&set(p['episode'] for p in alarm_payloads(e)[8:])


@pytest.mark.parametrize('reason',['connection_timeout','sequence_gap','generation_change','reconnect'])
def test_market_alarm_uses_classified_public_validity_payload(tmp_path,reason):
    from test_maker_taker_engine import engine
    e=engine(tmp_path);s,now=source();e.transport.source=s;e.clock=s.clock;e.books=s.books.copy()
    now[0]=106;s.received('arcus',1);s.received('lighter',1);s.invalid['lighter',SYMBOL]=(reason,105)
    e.observe_market_health();p=alarm_payloads(e)[0]
    assert p['reason']==reason and p['venue']=='lighter' and p['duration_seconds']==1
    assert p['boot']==e.journal.boot and p['diagnostic_source']=='public'


def test_private_market_alarm_does_not_suppress_other_venue_or_change_halt_cause(tmp_path,monkeypatch):
    e,s,now,aw,lw,_=private_engine(tmp_path,monkeypatch)
    e.halt('EMERGENCY','Known risk first cause');now[0]=106
    for venue in ('arcus','lighter'):s.received(venue,1)
    e.observe_market_health();alarms=alarm_payloads(e)
    market=[p for p in alarms if p.get('diagnostic_source')=='private']
    assert {p['venue'] for p in market}=={'arcus_private','lighter_private'}
    assert all(p['reason']=='connection_timeout' and p['duration_seconds']==1 for p in market)
    assert e.halt_reason==e.reason=='Known risk first cause'
    e.alarm('Ordinary risk');e.alarm('Ordinary risk');e.state='PAUSED_DATA';e.alarm('Ordinary risk')
    assert len([p for p in alarm_payloads(e) if p['reason']=='Ordinary risk'])==2


def test_unclassified_transport_health_alarm_has_no_fabricated_market_cause(tmp_path):
    from test_maker_taker_engine import engine
    e=engine(tmp_path);s,now=source();e.transport.source=s;e.clock=s.clock;e.books=s.books.copy();e.transport.connected=False
    e.observe_market_health();p=alarm_payloads(e)[0]
    assert p['reason']=='Unclassified transport health unavailable'
    assert 'venue' not in p and 'since_wall' not in p and 'episode' not in p
    assert not e.journal.db.execute("SELECT 1 FROM events WHERE kind='market_stale'").fetchone()


@pytest.mark.parametrize('fault',['old','generation','missing','future'])
def test_market_alarm_without_public_source_is_diagnostic_only_and_since_stays_stable(tmp_path,fault):
    from test_maker_taker_engine import engine
    e=engine(tmp_path);now=[e.clock()];e.clock=lambda:now[0]
    if fault=='old':now[0]+=e.profile.risk.stale_book_seconds+1
    elif fault=='generation':e.generations['lighter',SYMBOL]+=1
    elif fault=='missing':e.books.pop(('lighter',SYMBOL))
    else:now[0]=e.books['lighter',SYMBOL].received-1
    before=(e.state,dict(e.basis),dict(e.warmup_started),list(e.transport.cancels))
    e.observe_market_health();p=next(p for p in alarm_payloads(e) if p['venue']=='lighter')
    assert p['diagnostic_source']=='book_fallback'
    if fault=='generation':
        assert p['reason']=='generation_change' and p['age_source']=='first_observed'
        started=p['since_wall']
    else:
        assert p['reason']=='unclassified' and p['age_source']=='first_observed'
        assert p['detail']=={'old':'book_age_exceeded','missing':'book_missing','future':'book_timestamp_future'}[fault]
        assert 'since_wall' not in p and 'duration_seconds' not in p
        assert not e.journal.db.execute("SELECT 1 FROM events WHERE kind='market_stale'").fetchone()
        from core.services.arbitrage_monitor_v2.history.maker_taker_statistics import statistics
        assert all(row['stale_events']==0 and not any(row['stale_counts'].values()) for row in statistics(e.journal))
        started=p['observed_at']
    now[0]+=.5;e.observe_market_health()
    again=next(p for p in alarm_payloads(e) if p['venue']=='lighter')
    assert again['since_wall' if fault=='generation' else 'observed_at']==started
    assert (e.state,dict(e.basis),dict(e.warmup_started),list(e.transport.cancels))==before


def close_events(e,kind):
    return [json.loads(r[0]) for r in e.journal.db.execute('SELECT payload FROM events WHERE kind=? ORDER BY id',(kind,))]


@pytest.mark.parametrize('reason',['unsafe_floor','quote_unavailable','floor_target_changed','anchor_or_age'])
def test_close_cancel_decision_records_actual_reason_and_correlated_result(tmp_path,monkeypatch,reason):
    from dataclasses import replace
    from test_maker_taker_close_floor import close_engine,opened,flush_acks
    from core.services.arbitrage_monitor_v2.execution.maker_taker_engine import Book
    wall=[100.0];monkeypatch.setattr('time.time',lambda:wall[0])
    async def scenario():
        e=close_engine(tmp_path);opened(e.journal,1,'1','100','98')
        first=await e.place(SYMBOL,-1,'close',D(1),D(0),1);await flush_acks(e)
        if reason=='unsafe_floor':
            e.book('lighter',SYMBOL,Book(D(110),D('110.1'),D(10),D(10),e.clock(),1))
        elif reason=='quote_unavailable':
            original=e.maker_quote
            def unavailable(symbol,side,purpose,qty,threshold):
                _,reference,anchor,ab,_=original(symbol,side,purpose,qty,threshold)
                return None,reference,anchor,ab,None
            e.maker_quote=unavailable
        elif reason=='floor_target_changed':wall[0]=700
        else:
            wall[0]=101;e.profile=replace(e.profile,maker=replace(e.profile.maker,max_order_age=.5))
        await e.quote_symbol(SYMBOL)
        decisions=close_events(e,'close_cancel_decision');results=close_events(e,'close_cancel_result')
        assert len(decisions)==len(results)==1
        decision,result=decisions[0],results[0]
        assert decision['reason']==reason and decision['client']==result['client']==first
        assert decision['remaining_qty']=='1' and decision['side']==-1
        assert decision['old_bound'] is not None and decision['old_price'] is not None
        assert decision['lighter_reference'] is not None and decision['arcus_bid']=='100'
        assert decision['quota_limit']==2 and decision['reserved_cycle'] is True
        assert decision['global_queue_count']==0 and decision['global_pending'] is False
        assert result['confirmed'] is True and result['status']=='canceled'
        assert result['observed_monotonic']>=decision['observed_monotonic']
        assert not close_events(e,'close_quote_wait') or reason=='quote_unavailable'
    asyncio.run(scenario())


def test_close_wait_reason_scope_dedup_and_prepare_exit_without_ack(tmp_path,monkeypatch):
    from test_maker_taker_close_floor import close_engine,opened,flush_acks,settle
    wall=[100.0];monkeypatch.setattr('time.time',lambda:wall[0])
    async def scenario():
        e=close_engine(tmp_path);opened(e.journal,1,'1','100','98')
        first=await e.place(SYMBOL,-1,'close',D(1),D(0),1);await flush_acks(e)
        now=[e.clock()];e.clock=lambda:now[0];wall[0]=700
        other='ETH-USDC-PERP';e.queues[other]=asyncio.Queue();e.rules[other]=e.rules[SYMBOL]
        e.books['lighter',other]=e.books['lighter',SYMBOL];e.queues[other].put_nowait(None)
        for _ in range(10):await e.quote_symbol(SYMBOL)
        waits=close_events(e,'close_quote_wait')
        assert len(waits)==1 and waits[0]['reason']=='global_queue' and waits[0]['scope']=='retain_safe_order'
        assert e.journal.maker(first)['status']=='open'
        e.queues[other].get_nowait();e.queues[other].task_done()
        client=e.journal.prepare_maker(other,1,'open',D(1),D(100),0,D(100),D(100))
        oid=e.journal.observe_maker(client,D(1),'filled',average=D(100))
        attempt=e.journal.prepare_attempt(oid,D(1));e.journal.attempt_state(attempt,'unknown')
        now[0]+=.1
        for _ in range(10):await e.quote_symbol(SYMBOL)
        waits=close_events(e,'close_quote_wait')
        assert len(waits)==3 and waits[1]['end_reason']=='reason_changed' and waits[2]['reason']=='global_residual'
        assert waits[1]['duration_seconds']==pytest.approx(.1)
        e.journal.observe_attempt(attempt,D(1),'filled',average=D(100));e.requotes[SYMBOL].extend([now[0]]*2)
        for _ in range(10):await e.quote_symbol(SYMBOL)
        waits=close_events(e,'close_quote_wait')
        assert len(waits)==5 and waits[-1]['reason']=='quota' and waits[-1]['quota_wait_seconds']==1
        now[0]+=1.01;await e.quote_symbol(SYMBOL)
        assert not e.close_waits
        assert close_events(e,'close_quote_wait')[-1]['end_reason']=='recovered'
        await flush_acks(e)
        # Missing-order waits end at durable prepare, before the queued ACK is consumed.
        for row in e.journal.makers(SYMBOL,active=True):await e.cancel_one(row)
        e.requotes[SYMBOL].clear();e.queues[other].put_nowait(None)
        for _ in range(10):await e.quote_symbol(SYMBOL)
        assert close_events(e,'close_quote_wait')[-1]['scope']=='replace_missing_order'
        e.queues[other].get_nowait();e.queues[other].task_done();await e.quote_symbol(SYMBOL)
        final=close_events(e,'close_quote_wait')[-1]
        assert final['transition']=='exit' and final['end_reason']=='prepared' and final['client']
        assert not e.close_waits and e.queues[SYMBOL].qsize()==1
        assert e.journal.maker(final['client'])['status']=='prepared'
    asyncio.run(scenario())


def test_close_wait_unknown_admission_requires_place_none_and_cleans_up_flat_stop(tmp_path):
    from test_maker_taker_close_floor import close_engine,opened,closing,settle
    async def scenario():
        e=close_engine(tmp_path);opened(e.journal,1,'1','100','98')
        e._admit=lambda *args:False
        for _ in range(10):await e.quote_symbol(SYMBOL)
        waits=close_events(e,'close_quote_wait')
        assert len(waits)==1 and waits[0]['reason']=='other_admission_unknown'
        assert not e.journal.makers(SYMBOL,active=True)
        # No positions: do not report preparation/recovery as the exit cause.
        oid=closing(e.journal,'1');settle(e.journal,oid,'1')
        await e.quote_symbol(SYMBOL)
        assert not e.close_waits and close_events(e,'close_quote_wait')[-1]['end_reason']=='flat'
        e.close_quote_wait(SYMBOL,-1,'quota','replace_missing_order')
        await e.stop(timeout=.01)
        assert not e.close_waits and close_events(e,'close_quote_wait')[-1]['end_reason']=='stopping'
    asyncio.run(scenario())


def test_close_cancel_result_does_not_label_unknown_as_confirmed(tmp_path):
    from test_maker_taker_close_floor import close_engine,opened,flush_acks
    from core.services.arbitrage_monitor_v2.execution.maker_taker_engine import Book
    async def scenario():
        e=close_engine(tmp_path);opened(e.journal,1,'1','100','98')
        first=await e.place(SYMBOL,-1,'close',D(1),D(0),1);await flush_acks(e)
        e.book('lighter',SYMBOL,Book(D(110),D('110.1'),D(10),D(10),e.clock(),1))
        async def unknown(row):raise TimeoutError()
        e.transport.cancel_maker=unknown
        await e.quote_symbol(SYMBOL)
        assert e.halted
        assert close_events(e,'close_cancel_decision')[0]['client']==first
        result=close_events(e,'close_cancel_result')[0]
        assert result['confirmed'] is False and result['status']=='open'
    asyncio.run(scenario())


def fail_close_diagnostic(j,kind):
    j.db.execute("CREATE TRIGGER fail_close_diagnostic BEFORE INSERT ON events WHEN NEW.kind='"+kind+"' "
                 "BEGIN SELECT RAISE(FAIL,'private diagnostic payload must not escape'); END")


@pytest.mark.parametrize('kind',['close_cancel_decision','close_cancel_result'])
def test_close_cancel_diagnostic_failure_cannot_block_cancel_or_replacement(tmp_path,capsys,kind):
    from test_maker_taker_close_floor import close_engine,opened,flush_acks
    from core.services.arbitrage_monitor_v2.execution.maker_taker_engine import Book
    async def scenario():
        e=close_engine(tmp_path);opened(e.journal,1,'1','100','98')
        first=await e.place(SYMBOL,-1,'close',D(1),D(0),1);await flush_acks(e)
        fail_close_diagnostic(e.journal,kind)
        e.book('lighter',SYMBOL,Book(D(110),D('110.1'),D(10),D(10),e.clock(),1))
        await e.quote_symbol(SYMBOL)
        assert first in e.transport.cancels and e.journal.maker(first)['status']=='canceled'
        assert len(e.journal.makers(SYMBOL,active=True))==1 and not e.halted
    asyncio.run(scenario())
    warning=capsys.readouterr().err
    assert kind in warning and 'IntegrityError' in warning
    assert 'private diagnostic payload' not in warning and '100' not in warning


@pytest.mark.parametrize('transition',['enter','prepared','stop','halt','pause'])
def test_close_wait_diagnostic_failure_cannot_block_risk_processing(tmp_path,capsys,transition):
    from test_maker_taker_close_floor import close_engine,opened,flush_acks
    async def scenario():
        e=close_engine(tmp_path);opened(e.journal,1,'1','100','98')
        if transition in ('stop','halt','pause'):
            first=await e.place(SYMBOL,-1,'close',D(1),D(0),1);await flush_acks(e)
        if transition!='enter':e.close_quote_wait(SYMBOL,-1,'quota','replace_missing_order')
        fail_close_diagnostic(e.journal,'close_quote_wait')
        if transition=='enter':
            e.requotes[SYMBOL].extend([e.clock()]*2);await e.quote_symbol(SYMBOL)
            assert e.close_waits and not e.halted
        elif transition=='prepared':
            await e.quote_symbol(SYMBOL)
            assert not e.close_waits and e.queues[SYMBOL].qsize()==1
        elif transition=='stop':
            await e.stop(timeout=.1)
            assert not e.close_waits and first in e.transport.cancels
        elif transition=='pause':
            e.transport.connected=False;await e.watchdog_once()
            assert not e.close_waits and first in e.transport.cancels and e.state=='PAUSED_DATA'
        else:
            e.halt('EMERGENCY','fixture risk halt')
            await e.cancel_all()
            assert e.halted and not e.close_waits and first in e.transport.cancels
    asyncio.run(scenario())
    warning=capsys.readouterr().err
    assert 'close_quote_wait' in warning and 'IntegrityError' in warning
    assert 'private diagnostic payload' not in warning


@pytest.mark.parametrize('scope',['replace_missing_order','retain_safe_order'])
@pytest.mark.parametrize('failure',['transport','private','symbol'])
def test_watchdog_closes_wait_before_paused_tick_and_excludes_outage_time(tmp_path,monkeypatch,scope,failure):
    from test_maker_taker_close_floor import close_engine,opened,flush_acks
    wall=[100.0];monkeypatch.setattr('time.time',lambda:wall[0])
    async def scenario():
        e=close_engine(tmp_path);opened(e.journal,1,'1','100','98')
        if scope=='retain_safe_order':
            await e.place(SYMBOL,-1,'close',D(1),D(0),1);await flush_acks(e);wall[0]=700
        now=[e.clock()];e.clock=lambda:now[0];e.requotes[SYMBOL].extend([now[0]]*2)
        await e.quote_symbol(SYMBOL)
        assert e.close_waits[SYMBOL,-1][:2]==('quota',scope)
        now[0]+=.25
        if failure in ('transport','private'):
            e.transport.connected=False
            if failure=='private':e.transport.private_validity=lambda:{'arcus_private':(False,'connection_timeout',now[0],1)}
        else:e.symbol_data_ready=lambda symbol:False
        await e.watchdog_once()
        assert not e.close_waits and e.state=='PAUSED_DATA'
        waits=close_events(e,'close_quote_wait');assert waits[-1]['end_reason']=='quote_not_ready'
        assert waits[-1]['duration_seconds']==pytest.approx(.25)
        now[0]+=120;e.last_reconcile=now[0];await e.tick()
        assert close_events(e,'close_quote_wait')==waits and not e.close_waits
    asyncio.run(scenario())


def test_close_diagnostic_failure_and_unwritable_stderr_still_cancel(tmp_path,monkeypatch):
    from test_maker_taker_close_floor import close_engine,opened,flush_acks
    from core.services.arbitrage_monitor_v2.execution.maker_taker_engine import Book
    class BrokenStderr:
        def write(self,value):raise OSError('stderr unavailable')
    async def scenario():
        e=close_engine(tmp_path);opened(e.journal,1,'1','100','98')
        first=await e.place(SYMBOL,-1,'close',D(1),D(0),1);await flush_acks(e)
        fail_close_diagnostic(e.journal,'close_cancel_decision')
        e.book('lighter',SYMBOL,Book(D(110),D('110.1'),D(10),D(10),e.clock(),1))
        with monkeypatch.context() as patch:
            patch.setattr('sys.stderr',BrokenStderr());await e.quote_symbol(SYMBOL)
        assert first in e.transport.cancels and e.journal.maker(first)['status']=='canceled'
    asyncio.run(scenario())


def test_existing_economic_journal_failure_still_propagates(tmp_path):
    from test_maker_taker_close_floor import close_engine,opened
    async def scenario():
        e=close_engine(tmp_path);opened(e.journal,1,'1','100','98')
        fail_close_diagnostic(e.journal,'maker_prepared')
        with pytest.raises(sqlite3.IntegrityError,match='private diagnostic payload'):
            await e.quote_symbol(SYMBOL)
        assert not e.journal.makers(SYMBOL,active=True)
    asyncio.run(scenario())
