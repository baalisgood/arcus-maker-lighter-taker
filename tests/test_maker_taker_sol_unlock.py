"""SOL residual capacity and partial cost coverage regressions; fake transports only."""
import asyncio
import json
import time
from dataclasses import replace
from decimal import Decimal as D

import pytest

from test_maker_taker_close_floor import setup_basis,opened,settle,close_engine,flush_acks
from test_maker_taker_engine import engine,maker,observation
from core.services.arbitrage_monitor_v2.execution.maker_taker_engine import Book

SYMBOL='SOL-USDC-PERP'
OTHER='ETH-USDC-PERP'


@pytest.mark.parametrize('side',[-1,1])
def test_partial_source_cost_keeps_known_bound_and_late_cost_restores_coverage(tmp_path,side):
    j,basis=setup_basis(tmp_path)
    client=j.prepare_maker(SYMBOL,-side,'open',D(1),D(100),0,D(100),D(100))
    oid=j.observe_maker(client,D(1),'filled',average=D(100))
    settle(j,oid,'.7',average='98' if side==-1 else '102')
    at=time.time()
    expected=D('102.01') if side==-1 else D('97.99')
    assert basis.bound(SYMBOL,side,D(1),D(100),D('.01'),600,at)==(True,expected)
    detail=basis.describe(SYMBOL,side,D(1),D(100),D(103),600,at)
    assert detail['mode']=='cost_unknown' and D(detail['protected_qty'])==D('.7')
    assert D(detail['cost_unknown_qty'])==D('.3') and detail['estimated_pnl_pct'] is None
    settle(j,oid,'.3',average='98' if side==-1 else '102')
    assert basis.describe(SYMBOL,side,D(1),D(100),D(103),600,at)['mode']=='floor'


def test_sol_exact_expired_known_and_unknown_partition(tmp_path,monkeypatch):
    now=[100.0];monkeypatch.setattr('time.time',lambda:now[0]);j,basis=setup_basis(tmp_path)
    opened(j,1,'.5','100','98')
    old_close=j.prepare_maker(SYMBOL,-1,'close',D('.499277'),D(110),0,D(110),D(110))
    old_oid=j.observe_maker(old_close,D('.499277'),'filled',average=D(110))
    settle(j,old_oid,'.499248',average='108');now[0]=800
    client=j.prepare_maker(SYMBOL,1,'open',D('.5'),D(110),0,D(110),D(110))
    oid=j.observe_maker(client,D('.5'),'filled',average=D(110))
    settle(j,oid,'.499248',average='107')
    assert j.inventory(SYMBOL)==(D('.500723'),D('-.5'))
    assert basis.bound(SYMBOL,-1,D('.5'),D(108),D('.01'),600,800)==(True,D('111.011'))
    p=basis.describe(SYMBOL,-1,D('.5'),D(108),D(112),600,800)
    assert {k:D(p[k]) for k in ('protected_qty','cost_unknown_qty','expired_qty')}=={
        'protected_qty':D('.499248'),'cost_unknown_qty':D('.000029'),'expired_qty':D('.000723')}
    assert p['mode']=='cost_unknown' and p['estimated_pnl_pct'] is None


def test_remaining_open_capacity_floors_and_reserves_all_pending(tmp_path):
    e=engine(tmp_path);e.profile=replace(e.profile,symbols={SYMBOL:replace(e.profile.get_config(SYMBOL),quantity=D('.5'),max_segments=2)})
    e.journal._inventory_delta(SYMBOL,D('.500723'),D('-.5'))
    assert e.planned_quantity(SYMBOL,1,'open')==D('.499')
    client=maker(e,qty='.1')
    e.journal.observe_maker(client,D('.025'),'open',average=D(100))
    assert e.planned_quantity(SYMBOL,1,'open')==D('.399')


@pytest.mark.parametrize('tail,expected',[('.000723','.5'),('.2','.2')])
def test_legal_tail_cleanup_or_regular_close_fallback(tmp_path,tail,expected):
    e=engine(tmp_path);e.profile=replace(e.profile,symbols={SYMBOL:replace(e.profile.get_config(SYMBOL),quantity=D('.5'),max_segments=2)})
    qty=D('.5')+D(tail);e.journal._inventory_delta(SYMBOL,qty,-qty)
    # A full pending open reserves the remaining opening capacity.
    maker(e,qty=str(D(1)-qty))
    assert e.planned_quantity(SYMBOL,-1,'close')==D(expected)


def add_other(e):
    e.profile=replace(e.profile,symbols={**e.profile.symbols,OTHER:replace(e.profile.get_config(SYMBOL),symbol=OTHER)})
    e.rules[OTHER]=e.rules[SYMBOL];e.queues[OTHER]=asyncio.Queue();e.symbol_locks[OTHER]=asyncio.Lock();e.quote_locks[OTHER]=asyncio.Lock()
    for venue in ('arcus','lighter'):
        e.book(venue,OTHER,e.books[venue,SYMBOL])
    e.warmup_started[OTHER]=e.clock()-61


def test_known_other_residual_allows_quote_but_unknown_and_ack_remain_global(tmp_path):
    async def scenario():
        e=engine(tmp_path);add_other(e);client=maker(e)
        oid=e.journal.observe_maker(client,D('.2'),'filled',average=D(100))
        other=e.journal.prepare_maker(OTHER,1,'open',D('.1'),D(99),0,D(100),D(101));row=e.journal.maker(other)
        assert e.maker_admitted(row)
        attempt=e.journal.prepare_attempt(oid,D('.2'));e.journal.attempt_state(attempt,'unknown')
        assert not e.maker_admitted(row)
        e.journal.observe_attempt(attempt,D('.2'),'filled',average=D(100))
        e.queues[SYMBOL].put_nowait(None)
        assert not e.maker_admitted(row)
    asyncio.run(scenario())


def test_slow_known_hedge_does_not_change_global_state_or_block_tick(tmp_path):
    async def scenario():
        e=engine(tmp_path);add_other(e);entered=asyncio.Event();release=asyncio.Event()
        e.profile=replace(e.profile,hedge=replace(e.profile.hedge,request_timeout=2))
        client=maker(e);e.journal.observe_maker(client,D('.2'),'filled',average=D(100))
        async def hedge(row,obligation):
            entered.set();await release.wait()
            return observation(row,row['qty'],'filled','lighter',-obligation['side'],average=D(100))
        e.transport.hedge=hedge;e.tasks=[asyncio.create_task(e._worker(s)) for s in e.profile.symbols]
        e.last_reconcile=e.clock();e.wake_hedge(SYMBOL);await entered.wait()
        assert e.state=='ACTIVE'
        await asyncio.wait_for(e.tick(),.2)
        assert e.journal.makers(OTHER,active=True)
        e.state='STOPPING';e.stopping=True;release.set();await e.queues[SYMBOL].join()
        assert e.state=='STOPPING'
        for task in e.tasks:task.cancel()
        await asyncio.gather(*e.tasks,return_exceptions=True)
    asyncio.run(scenario())


def test_gaps_and_unknown_prices_never_dilute_known_weighted_bound(tmp_path):
    j,basis=setup_basis(tmp_path)
    client=j.prepare_maker(SYMBOL,1,'open',D(1),D(100),0,D(100),D(100))
    oid=j.observe_maker(client,D(1),'filled',average=D(100))
    settle(j,oid,'.3',average='98')
    attempt=j.prepare_attempt(oid,D('.2'));j.observe_attempt(attempt,D('.2'),'filled',average=None)
    settle(j,oid,'.5',average='106')
    assert basis.bound(SYMBOL,-1,D(1),D(100),D('.01'),600,time.time())==(True,D('97.01'))
    detail=basis.describe(SYMBOL,-1,D(1),D(100),D(103),600,time.time())
    assert D(detail['protected_qty'])==D('.8') and D(detail['cost_unknown_qty'])==D('.2')
    assert basis.bound(SYMBOL,1,D(1),D(100),D('.01'),0,time.time())==(False,None)
    assert basis.bound(SYMBOL,-1,D('1.001'),D(100),D('.01'),0,time.time())==(False,None)


def test_unknown_mode_and_partial_actual_pnl_survive_replay_without_timeout_count(tmp_path):
    from core.services.arbitrage_monitor_v2.execution.maker_taker_close_basis import CloseBasis
    from core.services.arbitrage_monitor_v2.history.maker_taker_statistics import diagnostic_statistics
    async def scenario():
        e=close_engine(tmp_path);j=e.journal
        opening=j.prepare_maker(SYMBOL,1,'open',D(1),D(100),0,D(100),D(100))
        opening_oid=j.observe_maker(opening,D(1),'filled',average=D(100))
        settle(j,opening_oid,'.7',average='98')
        missing=j.prepare_attempt(opening_oid,D('.3'));j.observe_attempt(missing,D('.3'),'filled',average=None)
        client=await e.place(SYMBOL,-1,'close',D(1),D(0),1);assert client
        quote=json.loads(j.db.execute("SELECT payload FROM events WHERE kind='close_quote'").fetchone()[0])
        assert quote['mode']=='cost_unknown' and quote['protection_active'] and D(quote['protected_qty'])==D('.7')
        await flush_acks(e)
        await e.observe_maker(observation(j.maker(client),1,'filled',average=D(103)))
        oid=j.obligations()[0]['id'];settle(j,oid,'1',average='100');e.close_basis.sync()
        event=json.loads(j.db.execute("SELECT payload FROM events WHERE kind='maker_fill' AND symbol=? ORDER BY id DESC",(SYMBOL,)).fetchone()[0])
        assert event['close_mode']=='cost_unknown' and event['roundtrip_pnl_usd'] is None
        assert D(event['roundtrip_pnl_usd_known'])==D('.7') and D(event['roundtrip_priced_qty'])==D('.7')
        count=j.db.execute("SELECT COUNT(*) FROM events WHERE kind='close_fill_enriched'").fetchone()[0]
        CloseBasis(j,[SYMBOL]).sync()
        assert j.db.execute("SELECT COUNT(*) FROM events WHERE kind='close_fill_enriched'").fetchone()[0]==count
        metrics=[p for (_,s),p in diagnostic_statistics(j,'hour').items() if s==SYMBOL]
        assert sum(p['close_cost_unknown_count'] for p in metrics)==1
        assert sum(p['close_timeout_count'] for p in metrics)==0
        assert metrics[-1]['roundtrip_pnl_usd'] is None
    asyncio.run(scenario())


def test_presign_capacity_excludes_self_but_rechecks_new_inventory_and_capital(tmp_path):
    e=engine(tmp_path);cfg=e.profile.get_config(SYMBOL)
    e.profile=replace(e.profile,symbols={SYMBOL:replace(cfg,quantity=D('.5'),max_segments=2)})
    e.journal._inventory_delta(SYMBOL,D('.500723'),D('-.5'))
    client=maker(e,qty='.499');row=e.journal.maker(client)
    assert e.maker_admitted(row)
    e.journal._inventory_delta(SYMBOL,D('.001'),D('-.001'))
    assert not e.maker_admitted(row)
    e.journal._inventory_delta(SYMBOL,D('-.001'),D('.001'))
    e.equity['lighter']=D(1)
    assert not e.maker_admitted(row)


def test_capital_lock_rechecks_remaining_open_capacity(tmp_path):
    async def scenario():
        e=engine(tmp_path);e.profile=replace(e.profile,symbols={SYMBOL:replace(e.profile.get_config(SYMBOL),max_segments=1)})
        await e.capital_lock.acquire()
        task=asyncio.create_task(e.place(SYMBOL,1,'open',D('.5'),D(0),0));await asyncio.sleep(0)
        e.journal._inventory_delta(SYMBOL,D('.6'),D('-.6'))
        e.capital_lock.release()
        assert await task is None and not e.journal.makers(active=True)
    asyncio.run(scenario())


def test_unpaired_tail_does_not_become_paired_cleanup(tmp_path):
    e=engine(tmp_path);e.profile=replace(e.profile,symbols={SYMBOL:replace(e.profile.get_config(SYMBOL),quantity=D('.5'),max_segments=2)})
    e.journal._inventory_delta(SYMBOL,D('.7'),D('-.5'));maker(e,qty='.3')
    assert e.planned_quantity(SYMBOL,-1,'close')==D('.5')


def test_tick_wakes_one_worker_without_repeated_dust_signals(tmp_path):
    async def scenario():
        e=engine(tmp_path);e.tasks=[object()];e.last_reconcile=e.clock()
        client=maker(e);e.journal.observe_maker(client,D('.000723'),'filled',average=D(100))
        e.state='PAUSED_DATA'
        for _ in range(10):await e.tick()
        assert not e.queues[SYMBOL].qsize()
        e.journal.observe_maker(client,D('.2'),'filled',average=D(100))
        for _ in range(10):await e.tick()
        assert e.queues[SYMBOL].qsize()==1
    asyncio.run(scenario())


def test_late_hedge_cost_tightens_bound_and_cancels_unsafe_existing_close(tmp_path,monkeypatch):
    now=[100.0];monkeypatch.setattr('time.time',lambda:now[0])
    async def scenario():
        e=close_engine(tmp_path);j=e.journal;e.rules[SYMBOL]=replace(e.rules[SYMBOL],tick=D('.000001'))
        opened(j,1,'.5','100','98')
        old=j.prepare_maker(SYMBOL,-1,'close',D('.499277'),D(110),0,D(110),D(110))
        oid=j.observe_maker(old,D('.499277'),'filled',average=D(110));settle(j,oid,'.499248',average='108')
        now[0]=800
        client=j.prepare_maker(SYMBOL,1,'open',D('.5'),D(110),0,D(110),D(110))
        oid=j.observe_maker(client,D('.5'),'filled',average=D(110));settle(j,oid,'.499248',average='107')
        close=await e.place(SYMBOL,-1,'close',D('.5'),D(0),0);assert close
        await flush_acks(e);old_price=D(j.maker(close)['price'])
        settle(j,oid,'.000752',average='1')
        allowed,bound=e.close_basis.bound(SYMBOL,-1,D('.5'),e.reference(SYMBOL,-1),D('.01'),600,now[0])
        assert allowed and bound>old_price
        await e.quote_symbol(SYMBOL)
        assert j.maker(close)['status']=='canceled' and close in e.transport.cancels
    asyncio.run(scenario())


def test_small_tail_reason_is_deduplicated_and_regular_close_still_prepares(tmp_path):
    async def scenario():
        e=engine(tmp_path);e.profile=replace(e.profile,symbols={SYMBOL:replace(e.profile.get_config(SYMBOL),quantity=D('.5'),max_segments=2)})
        opened(e.journal,1,'.500723','100','98');e.requotes[SYMBOL].extend([e.clock()]*2)
        for _ in range(10):await e.quote_symbol(SYMBOL)
        events=e.journal.db.execute("SELECT payload FROM events WHERE kind='close_tail_deferred'").fetchall()
        assert len(events)==1 and json.loads(events[0][0])['reason']=='below_common_step'
        assert not e.journal.attempts(active=True)
        await flush_acks(e);e.requotes[SYMBOL].clear();await e.quote_symbol(SYMBOL)
        closes=[r for r in e.journal.makers(SYMBOL,active=True) if r['purpose']=='close']
        assert len(closes)==1 and D(closes[0]['qty'])==D('.5')
    asyncio.run(scenario())


@pytest.mark.parametrize('side',[-1,1])
@pytest.mark.parametrize('unsafe',[False,True])
@pytest.mark.parametrize('blocker',['residual','queue'])
def test_local_blocker_still_checks_close_safety_and_records_safe_wait(tmp_path,side,unsafe,blocker):
    async def scenario():
        e=close_engine(tmp_path);opened(e.journal,-side,'.8','100','98' if side==-1 else '102')
        close=await e.place(SYMBOL,side,'close',D('.8'),D(0),0);await flush_acks(e)
        opening=maker(e,qty='.4',side=-side)
        if blocker=='residual':e.journal.observe_maker(opening,D('.2'),'open',average=D(110))
        else:e.queues[SYMBOL].put_nowait(None)
        e.profile=replace(e.profile,maker=replace(e.profile.maker,max_order_age=0))
        if unsafe:
            price=D(109) if side==-1 else D(90)
            e.book('lighter',SYMBOL,Book(price,price+D('.1'),D(10),D(10),e.clock(),1))
        e.requotes[SYMBOL].extend([e.clock()]*2)
        before=len(e.journal.makers());await e.quote_symbol(SYMBOL)
        assert len(e.journal.makers())==before
        assert e.journal.maker(opening)['status']==('prepared' if blocker=='queue' else 'open')
        assert e.journal.maker(close)['status']==('canceled' if unsafe else 'open')
        assert (close in e.transport.cancels)==unsafe and opening not in e.transport.cancels
        reason,scope,*_=e.close_waits[SYMBOL,side]
        assert reason==('local_residual' if blocker=='residual' else 'global_queue')
        assert scope==('replace_missing_order' if unsafe else 'retain_safe_order')
    asyncio.run(scenario())


def test_cost_partition_scans_source_once_and_keeps_gaps(tmp_path):
    from core.services.arbitrage_monitor_v2.execution.maker_taker_close_basis import cost_portions
    class CountingFills(list):
        scans=0
        def __iter__(self):
            self.scans+=1
            return super().__iter__()
    fills=CountingFills((D(i)/1000,D(i+1)/1000,D(100)) for i in range(1000))
    assert len(list(cost_portions({'source':{'fills':fills}},D(0),D(1))))==1000
    assert fills.scans==1
    fills=CountingFills([(D('.2'),D('.4'),D(100)),(D('.6'),D('.8'),None)])
    assert list(cost_portions({'source':{'fills':fills}},D('.1'),D('.9')))==[
        (D('.1'),D('.2'),None),(D('.2'),D('.4'),D(20)),(D('.4'),D('.6'),None),
        (D('.6'),D('.8'),None),(D('.8'),D('.9'),None)]


@pytest.mark.parametrize('fills',[
    [(D(0),D('.5'),D(100)),(D('.4'),D(1),D(100))],
    [(D('.5'),D('.4'),D(100))],[(D(0),D(1),D('NaN'))]])
def test_invalid_source_ranges_cannot_become_unknown_cost_fallback(fills):
    from core.services.arbitrage_monitor_v2.execution.maker_taker_close_basis import cost_portions
    with pytest.raises(ValueError):list(cost_portions({'source':{'fills':fills}},D(0),D(1)))


@pytest.mark.parametrize('cross_symbol',[False,True])
def test_presign_revalues_every_pending_open_at_current_bbo(tmp_path,cross_symbol):
    e=engine(tmp_path);add_other(e)
    e.profile=replace(e.profile,risk=replace(e.profile.risk,allocation_per_venue=D(112) if cross_symbol else D(102),leverage=D(1),reserve_ratio=D(0)))
    first=maker(e);target=first
    if cross_symbol:target=e.journal.prepare_maker(OTHER,1,'open',D('.1'),D(99),0,D(100),D(101))
    e.book('lighter',SYMBOL,Book(D('102.9'),D(103),D(10),D(10),e.clock(),1))
    assert e._gross('lighter')==(D('113.1') if cross_symbol else D(103))
    assert not e.maker_admitted(e.journal.maker(target))


def test_pending_mark_preserves_higher_reservation_and_only_remaining_quantity(tmp_path):
    e=engine(tmp_path);client=e.journal.prepare_maker(SYMBOL,1,'open',D(1),D(99),0,D(100),D(110))
    assert e._gross('arcus')==D(110)
    e.journal.observe_maker(client,D('.4'),'open',average=D(100))
    assert e._gross('arcus')==D('.4')*D('100.1')+D('.6')*D(110)


def test_missing_other_pending_market_fails_closed_for_presign(tmp_path):
    e=engine(tmp_path);add_other(e);maker(e)
    target=e.journal.prepare_maker(OTHER,1,'open',D('.1'),D(99),0,D(100),D(101))
    e.books.pop(('lighter',SYMBOL))
    assert e._gross('lighter')==D('Infinity') and not e.maker_admitted(e.journal.maker(target))
