"""FIFO close protection; no real accounts, signing, or network calls."""
from decimal import Decimal as D
from pathlib import Path
import asyncio
from dataclasses import replace

import pytest
import yaml

from core.services.arbitrage_monitor_v2.config.maker_taker_config import load_profile
from core.services.arbitrage_monitor_v2.execution.maker_taker_quotes import MarketRules,quote_price

PROFILE=Path(__file__).resolve().parents[1]/'config/arbitrage/arbitrage_segmented.yaml'
SYMBOL='SOL-USDC-PERP'


def test_close_floor_defaults_and_symbol_override(tmp_path):
    data=yaml.safe_load(PROFILE.read_text())
    data['symbol_configs'][SYMBOL]['grid_config']['min_close_profit']='.05'
    data['symbol_configs'][SYMBOL]['grid_config']['close_floor_timeout']=12
    path=tmp_path/'profile.yaml';path.write_text(yaml.safe_dump(data))
    p=load_profile(path)
    assert p.get_config(SYMBOL).min_close_profit==D('.05')
    assert p.get_config(SYMBOL).close_floor_timeout==12
    btc=p.get_config('BTC-USDC-PERP')
    assert btc.min_close_profit==D('.01') and btc.close_floor_timeout==600


@pytest.mark.parametrize('key,value',[('min_close_profit','NaN'),('min_close_profit',-1),
    ('min_close_profit',100),('close_floor_timeout',-1),('close_floor_timeout','Infinity'),('close_floor_timeout','1e10000')])
def test_close_floor_config_rejects_invalid(tmp_path,key,value):
    data=yaml.safe_load(PROFILE.read_text());data['default_config']['grid_config'][key]=value
    path=tmp_path/'profile.yaml';path.write_text(yaml.safe_dump(data))
    with pytest.raises((ValueError,ArithmeticError)):load_profile(path)


@pytest.mark.parametrize('side,bound',[(1,D('99.985')),(-1,D('100.015'))])
def test_close_bound_survives_tick_and_post_only(side,bound):
    rule=MarketRules(D('.001'),D('.001'),D('.01'),D('.01'),D(5),D(10),D(1000),D('.01'),
                     ((D(100),D('.01')),(None,D('.05'))))
    price=quote_price(side,D(100),D(0),rule,D('99.99'),D('100.01'),price_bound=bound)
    assert price is not None and price%rule.price_tick(price)==0
    assert price<=bound and price<D('100.01') if side==1 else price>=bound and price>D('99.99')


def opened(journal,side,qty,arcus,lighter):
    client=journal.prepare_maker(SYMBOL,side,'open',D(qty),D(999),0,D(999),D(999))
    oid=journal.observe_maker(client,D(qty),'filled',average=None if arcus is None else D(arcus),reference=D(777))
    attempt=journal.prepare_attempt(oid,D(qty))
    journal.observe_attempt(attempt,D(qty),'filled',average=None if lighter is None else D(lighter))
    return oid


@pytest.mark.parametrize('side,a,l,now,expected',[(1,'100','98','99','101.0100'),
                                                (-1,'100','102','101','98.9900')])
def test_fifo_two_leg_actual_entry_oracle(tmp_path,side,a,l,now,expected):
    from core.services.arbitrage_monitor_v2.execution.maker_taker_journal import Journal
    from core.services.arbitrage_monitor_v2.execution.maker_taker_close_basis import CloseBasis
    j=Journal(tmp_path/'test.sqlite','fake');opened(j,side,'1',a,l)
    basis=CloseBasis(j,[SYMBOL])
    allowed,bound=basis.bound(SYMBOL,-side,D(1),D(now),D('.01'),600,0)
    assert allowed and bound==D(expected)


def test_fifo_remaining_after_partial_close_uses_next_batch(tmp_path):
    from core.services.arbitrage_monitor_v2.execution.maker_taker_journal import Journal
    from core.services.arbitrage_monitor_v2.execution.maker_taker_close_basis import CloseBasis
    j=Journal(tmp_path/'test.sqlite','fake')
    opened(j,1,'1','100','98');opened(j,1,'2','110','107')
    close=j.prepare_maker(SYMBOL,-1,'close',D('1.5'),D(999),0,D(999),D(999))
    oid=j.observe_maker(close,D('1.5'),'filled',average=D(120))
    attempt=j.prepare_attempt(oid,D('1.5'));j.observe_attempt(attempt,D('1.5'),'filled',average=D(119))
    basis=CloseBasis(j,[SYMBOL])
    allowed,bound=basis.bound(SYMBOL,-1,D(1),D(108),D('.01'),600,0)
    assert allowed and bound==D('111.011')


def setup_basis(tmp_path):
    from core.services.arbitrage_monitor_v2.execution.maker_taker_journal import Journal
    from core.services.arbitrage_monitor_v2.execution.maker_taker_close_basis import CloseBasis
    j=Journal(tmp_path/'test.sqlite','fake')
    return j,CloseBasis(j,[SYMBOL])


def closing(j,qty,side=-1):
    client=j.prepare_maker(SYMBOL,side,'close',D(qty),D(999),0,D(999),D(999))
    return j.observe_maker(client,D(qty),'filled',average=D(999))


def settle(j,oid,qty,venue='lighter',average='999'):
    client=j.prepare_attempt(oid,D(qty),venue)
    j.observe_attempt(client,D(qty),'filled',average=D(average))
    return client


def test_each_fifo_lot_ages_independently_and_expired_loss_does_not_subsidize_fresh(tmp_path,monkeypatch):
    now=[100.0];monkeypatch.setattr('time.time',lambda:now[0])
    j,basis=setup_basis(tmp_path)
    opened(j,1,'1','100','98');now[0]=500;opened(j,1,'1','110','107')
    # Both lots contribute until the exact first-lot boundary.
    assert basis.bound(SYMBOL,-1,D(2),D(108),D('.01'),600,699.999)==(True,D('110.5105'))
    assert basis.bound(SYMBOL,-1,D(2),D(108),D('.01'),600,700)==(True,D('111.011'))
    assert basis.bound(SYMBOL,-1,D(1),D(108),D('.01'),600,700)==(True,None)
    assert basis.bound(SYMBOL,-1,D(2),D(108),D('.01'),600,1100)==(True,None)
    assert basis.bound(SYMBOL,-1,D(2),D(108),D('.01'),0,100)==(True,None)


@pytest.mark.parametrize('a,l',[(None,'98'),('100',None),(None,None)])
def test_unknown_actual_cost_uses_ema_without_fabricating_reference_cost(tmp_path,monkeypatch,a,l):
    now=[100.0];monkeypatch.setattr('time.time',lambda:now[0]);j,basis=setup_basis(tmp_path)
    opened(j,1,'1',a,l)
    assert basis.bound(SYMBOL,-1,D(1),D(108),D('.01'),600,699.999)==(True,None)
    detail=basis.describe(SYMBOL,-1,D(1),D(108),D(110),600,699.999)
    assert detail['mode']=='cost_unknown' and detail['estimated_pnl_pct'] is None
    assert basis.bound(SYMBOL,-1,D(1),D(108),D('.01'),600,700)==(True,None)
    assert not j.db.execute("SELECT 1 FROM events WHERE kind='close_floor_unattributed'").fetchone()


def test_unknown_age_persists_observation_across_restart_and_flat_flip(tmp_path,monkeypatch):
    from core.services.arbitrage_monitor_v2.execution.maker_taker_close_basis import CloseBasis
    now=[100.0];monkeypatch.setattr('time.time',lambda:now[0]);j,basis=setup_basis(tmp_path)
    opened(j,1,'1','100','98')
    j.db.execute("DELETE FROM events WHERE kind IN ('maker_fill','hedge_fill')")
    now[0]=1000;basis=CloseBasis(j,[SYMBOL])
    assert basis.bound(SYMBOL,-1,D(1),D(108),D('.01'),600,1599.999)==(True,None)
    assert basis.describe(SYMBOL,-1,D(1),D(108),D(110),600,1599.999)['open_at'] is None
    assert j.db.execute("SELECT count(*) FROM events WHERE kind='close_floor_unattributed'").fetchone()[0]==1
    now[0]=1400;basis=CloseBasis(j,[SYMBOL])
    assert basis.bound(SYMBOL,-1,D(1),D(108),D('.01'),600,1600)==(True,None)
    oid=closing(j,'1');settle(j,oid,'1')
    now[0]=2000;opened(j,-1,'1','105','107')
    assert basis.bound(SYMBOL,1,D(1),D(108),D('.01'),600,2001)==(True,D('105.9895'))


def test_partial_close_restore_reinstates_original_fifo_cost_and_age(tmp_path,monkeypatch):
    now=[100.0];monkeypatch.setattr('time.time',lambda:now[0]);j,basis=setup_basis(tmp_path)
    opened(j,1,'1','100','98');now[0]=200;opened(j,1,'1','110','107')
    now[0]=300;oid=closing(j,'1.5')
    settle(j,oid,'.5');settle(j,oid,'1','arcus',average='9999')
    assert j.inventory(SYMBOL)==(D('1.5'),D('-1.5'))
    assert basis.bound(SYMBOL,-1,D(1),D(108),D('.01'),600,400)==(True,D('110.5105'))
    assert basis.bound(SYMBOL,-1,D(1),D(108),D('.01'),600,700)==(True,D('111.011'))


def test_unpaired_add_emergency_unwind_does_not_pollute_old_cost(tmp_path):
    j,basis=setup_basis(tmp_path);opened(j,1,'1','100','98')
    client=j.prepare_maker(SYMBOL,1,'open',D(1),D(500),0,D(500),D(500))
    oid=j.observe_maker(client,D(1),'filled',average=D(500));settle(j,oid,'1','arcus')
    assert basis.bound(SYMBOL,-1,D(1),D(108),D('.01'),600,0)==(True,D('110.01'))


@pytest.mark.parametrize('parts',[['2'],['1','1'],['.2','.4','1.4']])
def test_opposite_open_emergency_restores_same_original_lot_for_split_or_whole_fills(tmp_path,monkeypatch,parts):
    now=[100.0];monkeypatch.setattr('time.time',lambda:now[0]);j,basis=setup_basis(tmp_path)
    opened(j,1,'1','100','98');basis.sync();now[0]=300
    client=j.prepare_maker(SYMBOL,-1,'open',D(2),D(110),0,D(110),D(110))
    oid=j.observe_maker(client,D(2),'filled',average=D(110));basis.sync()
    for part in parts:
        settle(j,oid,part,'arcus');basis.sync()
    assert j.inventory(SYMBOL)==(D(1),D(-1))
    assert basis.bound(SYMBOL,-1,D(1),D(99),D('.01'),600,699.999)==(True,D('101.01'))
    assert basis.bound(SYMBOL,-1,D(1),D(99),D('.01'),600,700)==(True,None)
    assert not j.db.execute("SELECT 1 FROM events WHERE kind='close_floor_unattributed'").fetchone()


def test_opposite_unpaired_open_and_late_hedges_do_not_create_phantom_fifo(tmp_path):
    j,basis=setup_basis(tmp_path)
    clients=[j.prepare_maker(SYMBOL,side,'open',D(1),D(100),0,D(100),D(100)) for side in (1,-1)]
    ids=[j.observe_maker(c,D(1),'filled',average=D(100)) for c in clients]
    settle(j,ids[0],'1',average='98');settle(j,ids[1],'1',average='102')
    basis.sync();assert j.inventory(SYMBOL)==(D(0),D(0))
    assert not basis.states[SYMBOL]['lots'] and not basis.states[SYMBOL]['pending']
    opened(j,-1,'1','105','107')
    assert basis.bound(SYMBOL,1,D(1),D(108),D('.01'),600,0)==(True,D('105.9895'))


def test_late_open_confirmation_updates_closed_undo_before_restore(tmp_path):
    j,basis=setup_basis(tmp_path)
    client=j.prepare_maker(SYMBOL,1,'open',D(1),D(100),0,D(100),D(100))
    opening=j.observe_maker(client,D(1),'filled',average=D(100))
    close=closing(j,'1');settle(j,opening,'1',average='98');settle(j,close,'1','arcus')
    assert basis.bound(SYMBOL,-1,D(1),D(108),D('.01'),600,0)==(True,D('110.01'))


def test_restart_archive_late_maker_fill_shared_allocations_and_duplicate_exactness(tmp_path):
    from core.services.arbitrage_monitor_v2.execution.maker_taker_close_basis import CloseBasis
    j,basis=setup_basis(tmp_path)
    client=j.prepare_maker(SYMBOL,1,'open',D(2),D(100),0,D(100),D(100))
    oid=j.observe_maker(client,D('.5'),'canceled',average=D(100));settle(j,oid,'.5',average='98')
    assert j.archive_closed()==1
    oid=j.observe_maker(client,D(1),'canceled',average=D(105))
    other=j.prepare_maker(SYMBOL,1,'open',D('.5'),D(120),0,D(120),D(120))
    oid2=j.observe_maker(other,D('.5'),'filled',average=D(120))
    attempt=j.prepare_attempt(oid,D(1),obligations=[oid,oid2]);j.observe_attempt(attempt,D(1),'filled',average=D(107))
    j.observe_attempt(attempt,D(1),'filled',average=D(107))
    assert j.archive_closed()==2
    basis=CloseBasis(j,[SYMBOL])
    # FIFO .5@100/98 + .5@110/107; new .5@120/107 is excluded from q=1.
    assert basis.bound(SYMBOL,-1,D(1),D(108),D('.01'),600,0)==(True,D('110.5105'))
    oid=closing(j,'1');settle(j,oid,'1');basis=CloseBasis(j,[SYMBOL])
    assert basis.bound(SYMBOL,-1,D('.5'),D(108),D('.01'),600,0)==(True,D('121.012'))


def test_reconcile_drift_forgets_unprovable_cost_and_uses_its_own_time(tmp_path,monkeypatch):
    now=[100.0];monkeypatch.setattr('time.time',lambda:now[0]);j,basis=setup_basis(tmp_path)
    opened(j,1,'1','100','98');now[0]=500
    j.reconcile_inventory(SYMBOL,D(2),D(-2),D(100))
    assert basis.bound(SYMBOL,-1,D(1),D(108),D('.01'),600,1099.999)==(True,None)
    assert basis.describe(SYMBOL,-1,D(1),D(108),D(110),600,1099.999)['mode']=='cost_unknown'
    assert basis.bound(SYMBOL,-1,D(1),D(108),D('.01'),600,1100)==(True,None)


def test_projection_ignores_uncommitted_events_and_failed_batch_is_replayable(tmp_path):
    j,basis=setup_basis(tmp_path);opened(j,1,'1','100','98');basis.sync();cursor=basis.cursor
    j.db.execute('BEGIN');j.event(SYMBOL,'maker_fill',qty='bad')
    basis.sync();assert basis.cursor==cursor;j.db.execute('ROLLBACK')
    j.event(SYMBOL,'maker_fill',qty='bad')
    with pytest.raises((ValueError,ArithmeticError,KeyError)):basis.sync()
    assert basis.cursor==cursor
    j.db.execute('DELETE FROM events WHERE id>?',(cursor,));basis.sync()
    assert basis.bound(SYMBOL,-1,D(1),D(108),D('.01'),600,0)==(True,D('110.01'))


def test_incremental_queries_seek_only_new_events_and_do_not_scan_archives(tmp_path):
    j,basis=setup_basis(tmp_path);opened(j,1,'1','100','98');basis.sync();queries=[]
    j.db.set_trace_callback(queries.append)
    for _ in range(3):basis.bound(SYMBOL,-1,D(1),D(108),D('.01'),600,0)
    assert all('WHERE id>' in sql for sql in queries if 'FROM events' in sql)
    assert not any('all_orders' in sql or 'all_obligations' in sql or 'archive_' in sql for sql in queries)
    work=[]
    for history in (0,2000):
        for _ in range(history):j.event(SYMBOL,'alarm',reason='historical diagnostic')
        cursor=j.db.execute('SELECT COALESCE(MAX(id),0) FROM events').fetchone()[0];steps=[0]
        def progress():steps[0]+=1;return 0
        j.db.set_progress_handler(progress,1)
        assert not j.db.execute('SELECT id,at,symbol,kind,payload FROM events WHERE id>? ORDER BY id LIMIT 512',(cursor,)).fetchall()
        j.db.set_progress_handler(None,0);work.append(steps[0])
    assert max(work)<100 and abs(work[1]-work[0])<10


async def flush_acks(e):
    while not e.queues[SYMBOL].empty():
        obs=e.queues[SYMBOL].get_nowait()
        if obs is not None:await e.observe_maker(obs)
        e.queues[SYMBOL].task_done()


def close_engine(tmp_path):
    from test_maker_taker_engine import engine,profile
    cfg=profile();cfg=replace(cfg,maker=replace(cfg.maker,max_order_age=10000,requote_threshold=D(99)),
                             symbols={SYMBOL:replace(cfg.get_config(SYMBOL),max_segments=1)})
    return engine(tmp_path,cfg)


def test_engine_close_recomputes_inside_capital_lock(tmp_path):
    from core.services.arbitrage_monitor_v2.execution.maker_taker_engine import Book
    async def scenario():
        e=close_engine(tmp_path);opened(e.journal,1,'1','100','98')
        await e.capital_lock.acquire()
        task=asyncio.create_task(e.place(SYMBOL,-1,'close',D(1),D(0),1));await asyncio.sleep(0)
        e.book('lighter',SYMBOL,Book(D(110),D('110.1'),D(10),D(10),e.clock(),1))
        e.capital_lock.release();client=await task
        assert client and D(e.journal.maker(client)['price'])>=D('112.11')
    asyncio.run(scenario())


def test_expiry_reprices_even_without_anchor_or_max_age_trigger(tmp_path,monkeypatch):
    now=[100.0];monkeypatch.setattr('time.time',lambda:now[0])
    async def scenario():
        e=close_engine(tmp_path);opened(e.journal,1,'1','100','98')
        first=await e.place(SYMBOL,-1,'close',D(1),D(0),1);await flush_acks(e)
        assert D(e.journal.maker(first)['price'])>=D('102.11')
        now[0]=700;await e.quote_symbol(SYMBOL)
        rows=[r for r in e.journal.makers(active=True) if r['purpose']=='close']
        assert e.journal.maker(first)['status']=='canceled' and len(rows)==1
        assert D(rows[0]['price'])<D('102.11')
    asyncio.run(scenario())


def test_unsafe_close_cancel_bypasses_throttle_but_replacement_does_not(tmp_path):
    from core.services.arbitrage_monitor_v2.execution.maker_taker_engine import Book
    async def scenario():
        e=close_engine(tmp_path);opened(e.journal,1,'1','100','98')
        first=await e.place(SYMBOL,-1,'close',D(1),D(0),1);await flush_acks(e)
        e.requotes[SYMBOL].extend([e.clock(),e.clock()])
        e.book('lighter',SYMBOL,Book(D(101),D('101.1'),D(10),D(10),e.clock(),1))
        await e.quote_symbol(SYMBOL);await e.quote_symbol(SYMBOL)
        assert e.journal.maker(first)['status']=='canceled' and first in e.transport.cancels
        assert not [r for r in e.journal.makers(active=True) if r['purpose']=='close']
    asyncio.run(scenario())


@pytest.mark.parametrize('prior_tokens',[0,1,2])
@pytest.mark.parametrize('unsafe',[False,True])
def test_close_replacement_reuses_only_its_reserved_cycle_token(tmp_path,monkeypatch,prior_tokens,unsafe):
    from core.services.arbitrage_monitor_v2.execution.maker_taker_engine import Book
    wall=[100.0];monkeypatch.setattr('time.time',lambda:wall[0])
    async def scenario():
        e=close_engine(tmp_path);opened(e.journal,1,'1','100','98')
        first=await e.place(SYMBOL,-1,'close',D(1),D(0),1);await flush_acks(e)
        now=e.clock();e.clock=lambda:now;e.requotes[SYMBOL].extend([now]*prior_tokens)
        if unsafe:e.book('lighter',SYMBOL,Book(D(110),D('110.1'),D(10),D(10),now,1))
        else:wall[0]=700
        await e.quote_symbol(SYMBOL)
        closes=[r for r in e.journal.makers(active=True) if r['purpose']=='close']
        if prior_tokens<2:
            assert e.journal.maker(first)['status']=='canceled'
            assert len(closes)==1 and closes[0]['client']!=first
            assert D(closes[0]['price'])>D(e.journal.maker(first)['price']) if unsafe else D(closes[0]['price'])<D(e.journal.maker(first)['price'])
        elif unsafe:
            assert e.journal.maker(first)['status']=='canceled' and not closes
        else:assert len(closes)==1 and closes[0]['client']==first
        assert len(e.requotes[SYMBOL])==min(2,prior_tokens+1)
    asyncio.run(scenario())


@pytest.mark.parametrize('cancel_result',['unknown','nonterminal','new_fill'])
def test_close_replacement_permit_cannot_bypass_cancel_or_pending_guard(tmp_path,monkeypatch,cancel_result):
    from test_maker_taker_engine import observation
    wall=[100.0];monkeypatch.setattr('time.time',lambda:wall[0])
    async def scenario():
        e=close_engine(tmp_path);opened(e.journal,1,'1','100','98')
        first=await e.place(SYMBOL,-1,'close',D(1),D(0),1);await flush_acks(e)
        e.requotes[SYMBOL].append(e.clock());wall[0]=700
        async def cancel(row):
            if cancel_result=='unknown':raise TimeoutError()
            return observation(row,'.1' if cancel_result=='new_fill' else '0',
                               'canceled' if cancel_result=='new_fill' else 'open',average=D(101))
        e.transport.cancel_maker=cancel
        await e.quote_symbol(SYMBOL)
        assert not [r for r in e.journal.makers(active=True) if r['purpose']=='close' and r['client']!=first]
        assert e.halted if cancel_result!='new_fill' else e.blocking_residuals(SYMBOL)
    asyncio.run(scenario())


def test_close_replacement_permit_still_checks_current_transport_health(tmp_path,monkeypatch):
    from test_maker_taker_engine import observation
    wall=[100.0];monkeypatch.setattr('time.time',lambda:wall[0])
    async def scenario():
        e=close_engine(tmp_path);opened(e.journal,1,'1','100','98')
        first=await e.place(SYMBOL,-1,'close',D(1),D(0),1);await flush_acks(e)
        e.requotes[SYMBOL].append(e.clock());wall[0]=700
        async def cancel(row):
            e.transport.connected=False
            return observation(row,'0','canceled')
        e.transport.cancel_maker=cancel
        await e.quote_symbol(SYMBOL)
        assert e.journal.maker(first)['status']=='canceled'
        assert not [r for r in e.journal.makers(active=True) if r['purpose']=='close']
        assert len(e.requotes[SYMBOL])==2
    asyncio.run(scenario())


def test_partial_fill_reprices_remaining_fifo_and_terminal_cache_is_pruned(tmp_path):
    from test_maker_taker_engine import observation
    async def scenario():
        e=close_engine(tmp_path);opened(e.journal,1,'.5','100','98');opened(e.journal,1,'.5','110','107')
        first=await e.place(SYMBOL,-1,'close',D(1),D(0),1);await flush_acks(e)
        await e.observe_maker(observation(e.journal.maker(first),'.5',average=D(120)))
        oid=e.journal.obligations()[0]['id'];settle(e.journal,oid,'.5')
        await e.quote_symbol(SYMBOL)
        rows=[r for r in e.journal.makers(active=True) if r['purpose']=='close']
        assert e.journal.maker(first)['status']=='canceled' and first not in e.close_bounds
        assert len(rows)==1 and D(rows[0]['qty'])==D('.5') and D(rows[0]['price'])>=D('103.111')
        await e.observe_maker(observation(rows[0],'.5','filled',average=D(120)))
        assert rows[0]['client'] not in e.close_bounds
    asyncio.run(scenario())


def test_real_arcus_write_lock_guard_prevents_sign_audit_and_post_for_stale_floor(tmp_path):
    from unittest.mock import AsyncMock,Mock
    from test_arcus_adapter import adapter,MARKET
    from core.services.arbitrage_monitor_v2.execution.maker_taker_engine import Book
    from core.services.arbitrage_monitor_v2.execution.maker_taker_live import LiveTransport
    async def scenario():
        e=close_engine(tmp_path);e.profile=replace(e.profile,hedge=replace(e.profile.hedge,request_timeout=2))
        opened(e.journal,1,'1','100','98');a=adapter()
        a._set_markets([{**MARKET,'marketDisplayName':'SOL-USD','tickSize':'.01','stepSize':'.001',
                         'minOrderSize':'.01','maxOrderSize':'1000','markPrice':'100','tickTiers':[]}])
        a._do_authenticate=AsyncMock();a._sign=Mock();a._nonce=Mock();a.write_audit=Mock();a._request=AsyncMock()
        live=LiveTransport.__new__(LiveTransport);live.arcus=a;live.maker_guard=e.maker_admitted
        e.transport.maker=live.maker
        await a._write_lock.acquire()
        task=asyncio.create_task(e.place(SYMBOL,-1,'close',D(1),D(0),1))
        for _ in range(10):
            await asyncio.sleep(0)
            if a._write_lock._waiters:break
        assert a._write_lock._waiters
        e.book('lighter',SYMBOL,Book(D(110),D('110.1'),D(10),D(10),e.clock(),1))
        a._write_lock.release();client=await task
        assert e.journal.maker(client)['status']=='rejected' and not e.halted
        a._nonce.assert_not_called();a._sign.assert_not_called();a.write_audit.assert_not_called();a._request.assert_not_called()
    asyncio.run(scenario())


@pytest.mark.parametrize('blocker',['queue','unknown_residual'])
@pytest.mark.parametrize('unsafe',[False,True])
def test_other_symbol_global_blocker_retains_safe_close_without_spending_token(tmp_path,monkeypatch,blocker,unsafe):
    from core.services.arbitrage_monitor_v2.execution.maker_taker_engine import Book
    wall=[100.0];monkeypatch.setattr('time.time',lambda:wall[0])
    async def scenario():
        e=close_engine(tmp_path);opened(e.journal,1,'1','100','98')
        first=await e.place(SYMBOL,-1,'close',D(1),D(0),1);await flush_acks(e)
        other='ETH-USDC-PERP';e.queues[other]=asyncio.Queue();e.rules[other]=e.rules[SYMBOL]
        e.books['lighter',other]=e.books['lighter',SYMBOL]
        if blocker=='queue':e.queues[other].put_nowait(None)
        else:
            client=e.journal.prepare_maker(other,1,'open',D(1),D(100),0,D(100),D(100))
            oid=e.journal.observe_maker(client,D(1),'filled',average=D(100))
            attempt=e.journal.prepare_attempt(oid,D(1));e.journal.attempt_state(attempt,'unknown')
        if unsafe:e.book('lighter',SYMBOL,Book(D(110),D('110.1'),D(10),D(10),e.clock(),1))
        else:wall[0]=700
        await e.quote_symbol(SYMBOL)
        if unsafe:
            assert e.journal.maker(first)['status']=='canceled' and first in e.transport.cancels
        else:
            assert e.journal.maker(first)['status']=='open' and not e.transport.cancels
            assert not e.requotes[SYMBOL]
        assert not [r for r in e.journal.makers(SYMBOL,active=True) if r['client']!=first]
        if not unsafe:
            if blocker=='queue':e.queues[other].get_nowait();e.queues[other].task_done()
            else:e.journal.observe_attempt(attempt,D(1),'filled',average=D(100))
            await e.quote_symbol(SYMBOL)
            closes=[r for r in e.journal.makers(SYMBOL,active=True) if r['purpose']=='close']
            assert len(closes)==1 and closes[0]['client']!=first
            assert len(e.requotes[SYMBOL])==1
    asyncio.run(scenario())


def test_unsafe_cancel_refreshes_expired_quota_before_same_pass_replacement(tmp_path):
    from core.services.arbitrage_monitor_v2.execution.maker_taker_engine import Book
    from test_maker_taker_engine import observation
    async def scenario():
        e=close_engine(tmp_path);opened(e.journal,1,'1','100','98')
        first=await e.place(SYMBOL,-1,'close',D(1),D(0),1);await flush_acks(e)
        now=[e.clock()];e.clock=lambda:now[0];e.requotes[SYMBOL].extend([now[0]-.9]*2)
        e.book('lighter',SYMBOL,Book(D(110),D('110.1'),D(10),D(10),now[0],1))
        async def cancel(row):
            now[0]+=.2
            return observation(row,'0','canceled')
        e.transport.cancel_maker=cancel
        await e.quote_symbol(SYMBOL)
        closes=[r for r in e.journal.makers(SYMBOL,active=True) if r['purpose']=='close']
        assert e.journal.maker(first)['status']=='canceled'
        assert len(closes)==1 and closes[0]['client']!=first
        assert not e.requotes[SYMBOL]
    asyncio.run(scenario())


@pytest.mark.parametrize('side',[-1,1])
@pytest.mark.parametrize('move,age,replaced',[(D('.001'),0,False),(D('.006'),0,False),
    (D('.006001'),0,True),(D('.001'),29.999,False),(D('.001'),30,True)])
def test_safe_floor_changes_obey_existing_anchor_threshold_and_age(tmp_path,monkeypatch,side,move,age,replaced):
    from core.services.arbitrage_monitor_v2.execution.maker_taker_engine import Book
    wall=[100.0];monkeypatch.setattr('time.time',lambda:wall[0])
    async def scenario():
        e=close_engine(tmp_path);e.profile=replace(e.profile,maker=replace(e.profile.maker,
            requote_threshold=D('.006'),max_order_age=30))
        e.rules[SYMBOL]=replace(e.rules[SYMBOL],tick=D('.001'));e.basis_center=lambda symbol:D(0)
        opened(e.journal,-side,'1','100','98' if side==-1 else '102')
        first=await e.place(SYMBOL,side,'close',D(1),D(0),1);await flush_acks(e)
        old=e.journal.maker(first);reference=e.reference(SYMBOL,side)
        current=reference*(1+D(side)*move/100);delta=current-reference
        book=e.books['lighter',SYMBOL]
        e.book('lighter',SYMBOL,Book(book.bid+delta,book.ask+delta,D(10),D(10),e.clock(),1))
        price,_,_,_,bound=e.maker_quote(SYMBOL,side,'close',D(1),e.profile.get_config(SYMBOL).close_threshold)
        assert price!=D(old['price']) and (D(old['price'])<=bound if side==1 else D(old['price'])>=bound)
        assert abs(e.fair_anchor(SYMBOL,side)/D(old['anchor'])-1)*100==move
        wall[0]=100+age;await e.quote_symbol(SYMBOL)
        closes=[r for r in e.journal.makers(SYMBOL,active=True) if r['purpose']=='close']
        assert len(closes)==1
        assert (closes[0]['client']!=first)==replaced
        assert len(e.requotes[SYMBOL])==int(replaced)
    asyncio.run(scenario())


@pytest.mark.parametrize('side',[-1,1])
def test_unsafe_floor_micro_move_still_cancels_at_full_quota(tmp_path,monkeypatch,side):
    from core.services.arbitrage_monitor_v2.execution.maker_taker_engine import Book
    wall=[100.0];monkeypatch.setattr('time.time',lambda:wall[0])
    async def scenario():
        e=close_engine(tmp_path);e.profile=replace(e.profile,maker=replace(e.profile.maker,
            requote_threshold=D('.006'),max_order_age=30))
        e.rules[SYMBOL]=replace(e.rules[SYMBOL],tick=D('.001'));e.basis_center=lambda symbol:D(0)
        opened(e.journal,-side,'1','100','98' if side==-1 else '102')
        first=await e.place(SYMBOL,side,'close',D(1),D(0),1);await flush_acks(e)
        reference=e.reference(SYMBOL,side);delta=-D(side)*reference*D('.001')/100
        book=e.books['lighter',SYMBOL]
        e.book('lighter',SYMBOL,Book(book.bid+delta,book.ask+delta,D(10),D(10),e.clock(),1))
        now=e.clock();e.clock=lambda:now;e.requotes[SYMBOL].extend([now]*2)
        await e.quote_symbol(SYMBOL)
        assert first in e.transport.cancels and e.journal.maker(first)['status']=='canceled'
        assert not [r for r in e.journal.makers(SYMBOL,active=True) if r['purpose']=='close']
        assert len(e.requotes[SYMBOL])==2
    asyncio.run(scenario())


@pytest.mark.parametrize('blocker',['quota','global_queue'])
def test_expired_floor_release_intent_survives_blocker_until_replacement(tmp_path,monkeypatch,blocker):
    wall=[100.0];monkeypatch.setattr('time.time',lambda:wall[0])
    async def scenario():
        e=close_engine(tmp_path);opened(e.journal,1,'1','100','98')
        first=await e.place(SYMBOL,-1,'close',D(1),D(0),1);await flush_acks(e)
        protected=e.close_bounds[first];assert protected[0] is not None
        now=[e.clock()];e.clock=lambda:now[0]
        other='ETH-USDC-PERP';e.queues[other]=asyncio.Queue()
        if blocker=='quota':e.requotes[SYMBOL].extend([now[0]]*2)
        else:e.queues[other].put_nowait(None)
        wall[0]=700
        for _ in range(3):
            await e.quote_symbol(SYMBOL)
            assert e.journal.maker(first)['status']=='open' and e.close_bounds[first]==protected
        if blocker=='quota':now[0]+=1.01
        else:e.queues[other].get_nowait();e.queues[other].task_done()
        await e.quote_symbol(SYMBOL)
        closes=[r for r in e.journal.makers(SYMBOL,active=True) if r['purpose']=='close']
        assert len(closes)==1 and closes[0]['client']!=first
        assert e.close_bounds[closes[0]['client']][0] is None
    asyncio.run(scenario())
