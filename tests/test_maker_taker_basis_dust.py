"""EMA pricing and durable compatible residual aggregation regression cases."""
import asyncio
from decimal import Decimal as D
from test_maker_taker_engine import engine, maker, SYMBOL, observation


def test_subminimum_fills_wait_then_aggregate_fifo(tmp_path):
    async def scenario():
        e=engine(tmp_path)
        for amount in ('.04','.04'):
            client=maker(e,amount)
            await e.observe_maker(observation(e.journal.maker(client),amount,'filled'))
            await e.drain_symbol(SYMBOL)
            assert not e.transport.sends and not e.halted
        client=maker(e,'.04')
        await e.observe_maker(observation(e.journal.maker(client),'.04','filled'))
        await e.drain_symbol(SYMBOL)
        assert [D(r['qty']) for r in e.transport.sends]==[D('.12')]
        assert not e.journal.obligations()
        assert e.journal.inventory(SYMBOL)==(D('.12'),D('-.12'))
    asyncio.run(scenario())

import json
import sqlite3
import time
from dataclasses import replace
import pytest
from core.services.arbitrage_monitor_v2.execution.maker_taker_engine import Book, DefinitiveFailure
from core.services.arbitrage_monitor_v2.execution.maker_taker_journal import Journal
from core.services.arbitrage_monitor_v2.execution.maker_taker_quotes import hedge_quantity, quote_price
from core.services.arbitrage_monitor_v2.history.maker_taker_statistics import statistics


def fill(e, amount, side=1, purpose='open', reference=None):
    client=maker(e,str(amount),purpose,side)
    e.journal.observe_maker(client,D(str(amount)),'filled',reference=reference)
    return e.journal.obligations()[-1]


@pytest.mark.parametrize('basis',['1','-1'])
@pytest.mark.parametrize('side',[1,-1])
@pytest.mark.parametrize('threshold',['.03','-.01'])
def test_basis_formula_for_open_and_close(tmp_path,basis,side,threshold):
    e=engine(tmp_path);rule=e.rules[SYMBOL]
    raw=D(100)*(1+(D(basis)-side*D(threshold))/100)
    price=quote_price(side,D(100),D(threshold),rule,D(90),D(110),D(basis))
    assert price==raw.quantize(D('.01'))


def test_ema_first_sample_reset_stale_and_generation(tmp_path):
    e=engine(tmp_path); now=[time.monotonic()];e.clock=lambda:now[0]
    e.book('arcus',SYMBOL,Book(D(101),D('101.1'),D(10),D(10),now[0],1))
    first=e.basis_center(SYMBOL)
    assert first>0
    ema=e.basis[SYMBOL];assert e.basis_center(SYMBOL)==first and e.basis[SYMBOL] is ema
    now[0]+=3;assert not e.data_ready() and SYMBOL not in e.basis
    for venue in ('arcus','lighter'):
        e.book(venue,SYMBOL,Book(D(99 if venue=='arcus' else 100),D('99.1' if venue=='arcus' else '100.1'),D(10),D(10),now[0],2))
    assert e.basis_center(SYMBOL)<0 and e.basis[SYMBOL] is not ema
    e.transport.connected=False;assert e.basis_center(SYMBOL) is None


@pytest.mark.parametrize('side,bid,ask,expected',[(1,'99','100','99.99'),(-1,'99.99','101','100')])
def test_crossing_nearest_legal_tick_at_tier(tmp_path,side,bid,ask,expected):
    e=engine(tmp_path);rule=replace(e.rules[SYMBOL],tick_tiers=((D(100),D('.01')),(None,D('.1'))))
    price=quote_price(side,D(200 if side==1 else 50),D(0),rule,D(bid),D(ask))
    assert price==D(expected) and price%rule.price_tick(price)==0


def test_policy_precision_and_metadata_boundaries(tmp_path):
    e=engine(tmp_path);r=e.rules[SYMBOL]
    assert hedge_quantity(D('.099999'),r,D(100))==0
    assert hedge_quantity(D('.100999'),r,D(100))==D('.100')
    assert hedge_quantity(D('.1'),replace(r,lighter_min=D('.101')),D(100))==0
    assert hedge_quantity(D('.1'),replace(r,lighter_notional=D('10.01')),D(100))==0


def test_price_change_releases_dust_without_new_fill(tmp_path):
    async def scenario():
        e=engine(tmp_path);fill(e,'.08');await e.drain_symbol(SYMBOL)
        assert not e.transport.sends and not e.blocking_residuals()
        for venue in ('arcus','lighter'):
            e.book(venue,SYMBOL,Book(D(150),D('150.1'),D(10),D(10),time.monotonic(),1))
        await e.drain_symbol(SYMBOL)
        assert D(e.transport.sends[0]['qty'])==D('.08')
    asyncio.run(scenario())


def test_dust_buckets_do_not_mix_symbol_side_or_purpose(tmp_path):
    async def scenario():
        e=engine(tmp_path)
        for side,purpose in ((1,'open'),(-1,'open'),(1,'close'),(1,'reconcile')):fill(e,'.06',side,purpose)
        other='ETH-USDC-PERP';e.rules[other]=e.rules[SYMBOL]
        c=e.journal.prepare_maker(other,1,'open',D('.06'),D(99),0,D(100),D(101))
        e.journal.observe_maker(c,D('.06'),'filled')
        await e.drain_symbol(SYMBOL)
        assert not e.transport.sends and len(e.residual_buckets())==5
        assert e._gross('lighter')>=D(18)
    asyncio.run(scenario())


def test_partial_aggregate_releases_only_unfilled_fifo_allocations(tmp_path):
    async def scenario():
        e=engine(tmp_path)
        for amount in ('.06','.06'):fill(e,amount,reference=D(100))
        e.transport.results=[('canceled','.07')]
        await e.drain_symbol(SYMBOL)
        assert len(e.transport.sends)==1 and not e.halted
        rows=e.journal.obligations(pending=False)
        assert [D(r['hedged']) for r in rows]==[D('.06'),D('.01')]
        fill(e,'.06');await e.drain_symbol(SYMBOL)
        assert [D(r['qty']) for r in e.transport.sends]==[D('.12'),D('.11')]
        assert not e.journal.obligations()
    asyncio.run(scenario())


def test_aggregate_unknown_preserves_original_identity_after_restart(tmp_path):
    async def scenario():
        e=engine(tmp_path)
        fill(e,'.06');fill(e,'.06');e.transport.results=[TimeoutError()]
        await e.drain_symbol(SYMBOL)
        original=e.journal.attempts()[0]['client'];assert e.halted
        e.journal.close();e=engine(tmp_path);fill(e,'.2')
        await e.drain_symbol(SYMBOL)
        assert e.halted and not e.transport.sends and e.journal.attempts()[0]['client']==original
    asyncio.run(scenario())


def test_new_fill_does_not_reset_old_retry_budget(tmp_path):
    async def scenario():
        e=engine(tmp_path);first=fill(e,'.2')
        for _ in range(4):
            client=e.journal.prepare_attempt(first['id'],D('.2'));e.journal.observe_attempt(client,D(0),'rejected')
        fill(e,'.3');await e.drain_symbol(SYMBOL)
        assert e.halted and not e.transport.sends
        assert not e.transport.emergencies and e.manual_intervention_required
        assert e.journal.inventory(SYMBOL)==(D('.5'),D(0)) and e.journal.obligations()
    asyncio.run(scenario())


def downgrade_to_v1(path):
    db=sqlite3.connect(path)
    for name, in db.execute("SELECT name FROM sqlite_master WHERE type='view'").fetchall():db.execute('DROP VIEW '+name)
    for table in ('pending_obligations','current_inventory','archive_attempt_allocations','archive_attempts','archive_obligations','archive_orders','attempt_allocations'):
        db.execute('DROP TABLE '+table)
    for name, in db.execute("SELECT name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%'").fetchall():db.execute('DROP INDEX '+name)
    db.execute('PRAGMA user_version=1');db.commit();db.close()


def test_v1_substep_emergency_dust_migrates_recovers_and_quotes(tmp_path):
    async def scenario():
        e=engine(tmp_path);row=fill(e,'.0005');e.journal.event(SYMBOL,'alarm',state='EMERGENCY',reason='Sub-lot uncovered residual')
        path=e.journal.path;e.journal.close();downgrade_to_v1(path)
        e=engine(tmp_path);e.transport.positions={SYMBOL:[D('.0005'),D(0)]}
        assert await e.recover() and not e.halted and e.state=='ACTIVE'
        assert e.journal.db.execute('PRAGMA user_version').fetchone()[0]==3
        assert not e.transport.sends and e.journal.obligations()
        await e.quote_symbol(SYMBOL)
        assert e.journal.makers(active=True)
    asyncio.run(scenario())


@pytest.mark.parametrize('mutation',["DELETE FROM meta WHERE key='identity'",'ALTER TABLE attempts ADD COLUMN unknown TEXT',
                                    "UPDATE attempts SET obligation=999", "UPDATE attempts SET filled='NaN'"])
def test_corrupt_v1_refused_without_upgrade(tmp_path,mutation):
    e=engine(tmp_path);row=fill(e,'.2');client=e.journal.prepare_attempt(row['id'],D('.2'))
    path=e.journal.path;e.journal.close();downgrade_to_v1(path)
    db=sqlite3.connect(path);db.execute(mutation);db.commit();before=db.execute('PRAGMA user_version').fetchone()[0];db.close()
    with pytest.raises(ValueError):Journal(path,'fake')
    db=sqlite3.connect(path);assert db.execute('PRAGMA user_version').fetchone()[0]==before==1
    assert not db.execute("SELECT name FROM sqlite_master WHERE name='attempt_allocations'").fetchone();db.close()


def test_v1_attempt_backfill_and_duplicate_watermark(tmp_path):
    e=engine(tmp_path);row=fill(e,'.2');client=e.journal.prepare_attempt(row['id'],D('.2'))
    e.journal.observe_attempt(client,D('.12'),'canceled',average=D(100),fee=D('.03'))
    path=e.journal.path;e.journal.close();downgrade_to_v1(path)
    j=Journal(path,'fake');assert D(j.allocations(client)[0]['filled'])==D('.12')
    j.observe_attempt(client,D('.12'),'canceled',average=D(100),fee=D('.03'))
    assert j.inventory(SYMBOL)==(D('.2'),D('-.12'))


def test_aggregate_fees_slippage_and_actual_volume_conserve(tmp_path):
    e=engine(tmp_path)
    a=fill(e,'.04',reference=D(100));b=fill(e,'.08',reference=D(102))
    c=e.journal.prepare_attempt(a['id'],D('.12'),obligations=[a['id'],b['id']])
    e.journal.observe_attempt(c,D('.12'),'filled',average=D(101),fee=D('.01'))
    e.journal.observe_attempt(c,D('.12'),'filled',average=D(101),fee=D('.01'))
    events=[json.loads(r[0]) for r in e.journal.db.execute("SELECT payload FROM events WHERE kind='hedge_fill'")]
    assert sum(D(r['fee']) for r in events)==D('.01')
    assert sum(D(r['qty']) for r in events)==D('.12')
    for i,qty in enumerate(('.05','.07')):
        e.journal.record_execution('lighter',i,SYMBOL,c,D(qty),D(101),D('.005'),'USDC','TAKER',time.time(),'hedge_fill')
    rows=statistics(e.journal)
    assert sum(D(r['lighter_notional']) for r in rows)==D('12.12')
    assert sum(D(r['actual_fee_known']) for r in rows)==D('.01')
    assert sum(D(r['signed_slippage_known']) for r in rows)==D('.04')


def test_dust_reconcile_once_and_stop_reports_nonflat(tmp_path):
    async def scenario():
        e=engine(tmp_path);e.transport.positions={SYMBOL:[D('.04'),D(0)]}
        assert await e.reconcile() and e.state=='ACTIVE'
        assert await e.reconcile() and len(e.journal.obligations())==1
        assert await e.stop()
        event=json.loads(e.journal.db.execute("SELECT payload FROM events WHERE kind='shutdown'").fetchone()[0])
        assert event['clean'] and not event['positions_flat'] and event['pending_residuals']==1
    asyncio.run(scenario())


def test_positive_partial_tail_does_not_spend_failure_budget(tmp_path):
    async def scenario():
        e=engine(tmp_path);row=fill(e,'.5005')
        for amount,filled in (('.5','.1'),('.4','.1'),('.3','.1'),('.2','.2')):
            client=e.journal.prepare_attempt(row['id'],D(amount));e.journal.observe_attempt(client,D(filled),'canceled')
        await e.drain_symbol(SYMBOL)
        assert not e.halted and not e.transport.sends and e.residual_state(e.residual_buckets()[0])[0]=='deferred_dust'
        fill(e,'.2');await e.drain_symbol(SYMBOL)
        assert not e.halted and len(e.transport.sends)==1
        assert D(e.transport.sends[0]['qty'])==D('.2')
    asyncio.run(scenario())


def test_missing_cumulative_economics_remain_unknown(tmp_path):
    e=engine(tmp_path);row=fill(e,'.3');client=e.journal.prepare_attempt(row['id'],D('.3'))
    e.journal.observe_attempt(client,D('.1'),'open',average=D(100),fee=D('.01'))
    e.journal.observe_attempt(client,D('.2'),'open')
    attempt=e.journal.attempts()[0];assert attempt['average'] is None and attempt['fee'] is None
    e.journal.observe_attempt(client,D('.3'),'filled',average=D(120),fee=D('.03'))
    events=[json.loads(r[0]) for r in e.journal.db.execute("SELECT payload FROM events WHERE kind='hedge_fill'")]
    assert events[-1]['average'] is None and events[-1]['fee'] is None


def test_fifo_fill_and_events_rollback_together_on_crash(tmp_path):
    e=engine(tmp_path);a=fill(e,'.06');b=fill(e,'.06')
    client=e.journal.prepare_attempt(a['id'],D('.12'),obligations=[a['id'],b['id']])
    original=e.journal.event
    def crash(*args,**kwargs):raise RuntimeError('simulated crash before commit')
    e.journal.event=crash
    with pytest.raises(RuntimeError):e.journal.observe_attempt(client,D('.12'),'filled')
    assert all(D(r['hedged'])==0 for r in e.journal.obligations())
    assert all(D(r['filled'])==0 for r in e.journal.allocations(client))
    assert D(e.journal.attempts()[0]['filled'])==0
    e.journal.event=original;e.journal.observe_attempt(client,D('.12'),'filled')
    assert not e.journal.obligations()


def test_signing_rechecks_policy_without_touching_signer(tmp_path):
    from types import SimpleNamespace as NS
    from collections import deque
    from core.services.arbitrage_monitor_v2.execution.maker_taker_live import LiveTransport
    from core.services.arbitrage_monitor_v2.execution.maker_taker_engine import DeferredHedge
    async def scenario():
        e=engine(tmp_path);t=LiveTransport.__new__(LiveTransport)
        t.source=NS(markets={SYMBOL:({}, {})},rules=e.rules,books=e.books,book_validity=lambda *args:(True,None,None))
        t.lighter=NS(_rest=NS(),_normalize_symbol=lambda s:s);t.writes=deque()
        with pytest.raises(DeferredHedge):
            await t.hedge({'qty':'.05','client':'2'},{'symbol':SYMBOL,'side':1,'purpose':'open'})
        assert not t.writes
    asyncio.run(scenario())


def test_price_only_wakeup_does_not_invalidate_account_generation(tmp_path):
    e=engine(tmp_path);fill(e,'.04');e.tasks=[object()];before=e.sequence
    e.book('lighter',SYMBOL,Book(D(100),D('100.1'),D(10),D(10),time.monotonic(),1))
    assert e.sequence==before and e.queues[SYMBOL].qsize()==1


def test_small_live_each_flat_quote_meets_both_minima(tmp_path,capsys):
    import run_maker_taker
    from core.services.arbitrage_monitor_v2.execution.maker_taker_paper import PublicMarkets
    fixture='tests/fixtures/maker_taker_replay.json';data=json.loads(open(fixture).read())
    directory=tmp_path/'small'
    assert run_maker_taker.main(['--mode','replay','--replay',fixture,'--small-live','--state-dir',str(directory)])==0
    capsys.readouterr()
    source=PublicMarkets([r['symbol'] for r in data['initial_books'] if r['venue']=='arcus'])
    source.load_metadata(data['arcus_markets'],data['lighter_markets'])
    db=sqlite3.connect(directory/'replay.sqlite');db.row_factory=sqlite3.Row
    opening=list(db.execute("SELECT * FROM all_orders WHERE purpose='open'"));assert opening
    for row in opening:
        qty=D(row['qty']);rule=source.rules[row['symbol']];mark=D(source.markets[row['symbol']][0]['markPrice'])
        assert rule.legal(qty,D(row['price']),mark)
        assert hedge_quantity(qty,rule,mark*D('.99'))==qty
    db.close()


def test_partial_hedge_dust_still_quotes_only_confirmed_paired_close(tmp_path):
    async def scenario():
        e=engine(tmp_path)
        client=maker(e,'.15')
        e.journal.observe_maker(client,D('.15'),'filled',average=D(100))
        original=e.transport.hedge
        async def hedge(*args):
            return replace(await original(*args),average=D(100))
        e.transport.hedge=hedge
        e.transport.results=[('canceled','.10')]
        await e.drain_symbol(SYMBOL)
        assert e.journal.inventory(SYMBOL)==(D('.15'),D('-.10')) and e.state=='ACTIVE'
        await e.quote_symbol(SYMBOL)
        while not e.queues[SYMBOL].empty():
            item=e.queues[SYMBOL].get_nowait()
            if item is not None:await e.observe_maker(item)
            e.queues[SYMBOL].task_done()
        await e.quote_symbol(SYMBOL)
        closing=[r for r in e.journal.makers(active=True) if r['purpose']=='close']
        assert len(closing)==1 and D(closing[0]['qty'])==D('.10')
        assert D(e.journal.obligations()[0]['qty'])-D(e.journal.obligations()[0]['hedged'])==D('.05')
    asyncio.run(scenario())


@pytest.mark.parametrize('later_invalid',[False,True])
def test_dust_reconcile_finishes_full_snapshot_before_active(tmp_path,later_invalid):
    async def scenario():
        from test_maker_taker_engine import profile
        other='ETH-USDC-PERP';cfg=profile()
        cfg=replace(cfg,symbols={**cfg.symbols,other:replace(cfg.get_config(SYMBOL),symbol=other)})
        e=engine(tmp_path,cfg)
        e.rules[other]=e.rules[SYMBOL]
        for venue in ('arcus','lighter'):
            e.book(venue,other,Book(D(100),D('100.1'),D(10),D(10),time.monotonic(),1))
        e.transport.positions={SYMBOL:[D('.04'),D(0)],other:[D('NaN') if later_invalid else D('.03'),D(0)]}
        async def snapshot():
            return {'authoritative':True,'equity':e.transport.equity,'positions':e.transport.positions,
                    'leverage':{'arcus':2 if later_invalid else 3,'lighter':2}}
        e.transport.snapshot=snapshot
        assert not await e.reconcile() and e.halted and e.state=='UNKNOWN_ORDER'
        assert not e.transport.sends
        assert any(('Invalid account size' if later_invalid else 'leverage exceeds 2') in r['payload']
                   for r in e.journal.db.execute("SELECT payload FROM events WHERE kind='alarm'"))
    asyncio.run(scenario())


def test_multiple_dust_corrections_are_confirmed_in_one_snapshot(tmp_path):
    async def scenario():
        from test_maker_taker_engine import profile
        other='ETH-USDC-PERP';cfg=profile()
        cfg=replace(cfg,symbols={**cfg.symbols,other:replace(cfg.get_config(SYMBOL),symbol=other)})
        e=engine(tmp_path,cfg)
        e.rules[other]=e.rules[SYMBOL]
        for venue in ('arcus','lighter'):
            e.book(venue,other,Book(D(100),D('100.1'),D(10),D(10),time.monotonic(),1))
        e.transport.positions={SYMBOL:[D('.04'),D(0)],other:[D('.03'),D(0)]}
        assert await e.reconcile() and e.state=='ACTIVE' and not e.halted
        assert len(e.journal.obligations())==2 and not e.journal.pending_reconcile()
        assert e.journal.inventory(other)==(D('.03'),D(0))
    asyncio.run(scenario())
