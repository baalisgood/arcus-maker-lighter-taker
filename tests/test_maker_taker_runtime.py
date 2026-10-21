"""Current-state, archival, warmup and per-symbol market isolation regressions."""
import asyncio
from decimal import Decimal as D

import pytest
from test_maker_taker_engine import engine,maker,observation,SYMBOL
from core.services.arbitrage_monitor_v2.execution.maker_taker_engine import Book


def test_quote_requires_continuous_sixty_second_warmup(tmp_path):
    async def scenario():
        e=engine(tmp_path);now=[100.0];e.clock=lambda:now[0]
        e.basis.clear();e.basis_samples.clear();e.warmup_started.clear()
        for venue in ('arcus','lighter'):
            e.book(venue,SYMBOL,Book(D(100),D('100.1'),D(10),D(10),now[0],1))
        await e.quote_symbol(SYMBOL)
        assert not e.journal.makers(active=True)
        for second in range(1,61):
            now[0]=100+second
            for venue in ('arcus','lighter'):
                e.book(venue,SYMBOL,Book(D(100),D('100.1'),D(10),D(10),now[0],1))
            await e.quote_symbol(SYMBOL)
            if second<60:assert not e.journal.makers(active=True)
        assert e.journal.makers(active=True)
    asyncio.run(scenario())


def test_archive_moves_closed_graph_restores_late_fill_without_double_count(tmp_path):
    e=engine(tmp_path);client=maker(e)
    oid=e.journal.observe_maker(client,D('.3'),'canceled')
    attempt=e.journal.prepare_attempt(oid,D('.3'))
    e.journal.observe_attempt(attempt,D('.3'),'filled')
    before=e.journal.inventory(SYMBOL)
    assert e.journal.archive_closed()==1
    assert not e.journal.db.execute('SELECT 1 FROM orders').fetchone()
    assert e.journal.attempt(attempt)['status']=='filled'
    assert e.journal.inventory(SYMBOL)==before
    e.journal.observe_maker(client,D('.4'),'canceled')
    assert e.journal.inventory(SYMBOL)==(D('.4'),D('-.3'))
    assert len(e.journal.obligations())==1
    assert not e.journal.db.execute('SELECT 1 FROM archive_orders').fetchone()
    with pytest.raises(ValueError,match='Invalid hedge'):
        e.journal.observe_attempt(attempt,D('.31'),'filled')


def test_pending_and_dust_components_are_not_archived(tmp_path):
    e=engine(tmp_path);client=maker(e)
    e.journal.observe_maker(client,D('.0001'),'canceled')
    assert e.journal.archive_closed()==0
    assert e.journal.maker(client)['status']=='canceled'
    assert e.journal.obligations()


def test_stale_symbol_cancels_only_it_and_healthy_symbol_can_quote(tmp_path):
    from dataclasses import replace
    from test_maker_taker_engine import profile
    other='ETH-USDC-PERP'
    cfg=profile();cfg=replace(cfg,symbols={**cfg.symbols,other:replace(cfg.symbols[SYMBOL],symbol=other)})
    async def scenario():
        e=engine(tmp_path,cfg);e.rules[other]=e.rules[SYMBOL]
        now=[100.0];e.clock=lambda:now[0]
        for symbol in (SYMBOL,other):e.reset_basis(symbol)
        for second in range(61):
            now[0]=100+second
            for symbol in (SYMBOL,other):
                for venue in ('arcus','lighter'):
                    e.book(venue,symbol,Book(D(100),D('100.1'),D(10),D(10),now[0],1))
        a=e.journal.prepare_maker(SYMBOL,1,'open',D(1),D(99),0,D(100),D(101))
        b=e.journal.prepare_maker(other,1,'open',D(1),D(99),0,D(100),D(101))
        now[0]+=3
        for venue in ('arcus','lighter'):
            e.book(venue,other,Book(D(100),D('100.1'),D(10),D(10),now[0]-1,1))
        await e.watchdog_once()
        assert e.transport.cancels==[a]
        assert e.journal.maker(b)['status']=='prepared' and e.state=='ACTIVE'
        # Retain a continuing fresh B stream while A stays stale.
        for second in range(60):
            now[0]+=1
            for venue in ('arcus','lighter'):
                e.book(venue,other,Book(D(100),D('100.1'),D(10),D(10),now[0],1))
        await e.quote_symbol(other)
        assert len(e.journal.makers(other,active=True))==2
        assert not e.journal.makers(SYMBOL,active=True)
        e.transport.connected=False
        await e.watchdog_once()
        assert b in e.transport.cancels and not e.journal.makers(active=True)
        assert e.state=='PAUSED_DATA' and not e.warmup_started
    asyncio.run(scenario())


def test_archive_connected_fifo_component_and_transaction_rollback(tmp_path):
    e=engine(tmp_path);clients=[maker(e) for _ in range(2)]
    ids=[e.journal.observe_maker(c,D('.06'),'canceled') for c in clients]
    attempt=e.journal.prepare_attempt(ids[0],D('.12'),obligations=ids)
    e.journal.observe_attempt(attempt,D('.12'),'filled')
    inventory=e.journal.inventory(SYMBOL)
    e.journal.db.execute("CREATE TRIGGER archive_crash BEFORE DELETE ON orders BEGIN SELECT RAISE(ABORT,'simulated archive crash'); END")
    with pytest.raises(Exception,match='simulated archive crash'):e.journal.archive_closed()
    assert len(e.journal.makers())==2
    assert not e.journal.db.execute('SELECT 1 FROM archive_orders').fetchone()
    e.journal.db.execute('DROP TRIGGER archive_crash')
    assert e.journal.archive_closed()==2
    assert len(e.journal.allocations(attempt))==2
    assert e.journal.inventory(SYMBOL)==inventory
    e.journal.observe_maker(clients[1],D('.07'),'canceled')
    assert len(e.journal.db.execute('SELECT * FROM orders').fetchall())==2
    assert e.journal.inventory(SYMBOL)==(D('.13'),D('-.12'))
    assert len(e.journal.allocations(attempt))==2


def test_archive_cursor_rotates_past_dust_and_large_components(tmp_path):
    e=engine(tmp_path)
    dust=maker(e);e.journal.observe_maker(dust,D('.01'),'canceled')
    for _ in range(12):
        client=maker(e);oid=e.journal.observe_maker(client,D('.1'),'canceled')
        attempt=e.journal.prepare_attempt(oid,D('.1'));e.journal.observe_attempt(attempt,D('.1'),'filled')
    for _ in range(30):e.journal.archive_closed(max_candidates=2,max_nodes=8)
    assert len(e.journal.db.execute('SELECT * FROM archive_orders').fetchall())==12
    assert len(e.journal.db.execute('SELECT * FROM orders').fetchall())==1
    assert e.journal.obligations()[0]['maker']==dust
    for kwargs in ({'max_candidates':-1},{'max_candidates':0},{'max_candidates':65},{'max_nodes':0},{'max_nodes':257}):
        with pytest.raises(ValueError):e.journal.archive_closed(**kwargs)


def test_v3_reopen_refuses_derived_state_tampering(tmp_path):
    from core.services.arbitrage_monitor_v2.execution.maker_taker_journal import Journal
    e=engine(tmp_path);e.journal.observe_maker(maker(e),D('.1'),'canceled')
    e.journal.db.execute("UPDATE current_inventory SET arcus='999'")
    e.journal.close()
    with pytest.raises(ValueError,match='current state disagrees'):Journal(tmp_path/'state.sqlite','fake')


def test_zero_fill_history_does_not_authorize_adopting_inventory(tmp_path):
    async def scenario():
        e=engine(tmp_path);client=maker(e)
        e.journal.observe_maker(client,D(0),'canceled')
        e.journal.archive_closed()
        assert not e.journal.has_history(SYMBOL)
        e.transport.positions={SYMBOL:[D('.2'),D('-.2')]}
        assert not await e.reconcile(initial=True)
        assert e.halted and 'Unowned pre-existing position' in e.halt_reason and e.halt_error_type=='ValueError'
        assert not e.journal.has_history(SYMBOL) and not e.journal.obligations()
    asyncio.run(scenario())


@pytest.mark.parametrize('table',['archive_orders','archive_attempts'])
def test_v3_reopen_rejects_archived_nonterminal_lifecycle(tmp_path,table):
    from core.services.arbitrage_monitor_v2.execution.maker_taker_journal import Journal
    e=engine(tmp_path);oid=e.journal.observe_maker(maker(e),D('.1'),'canceled')
    attempt=e.journal.prepare_attempt(oid,D('.1'));e.journal.observe_attempt(attempt,D('.1'),'filled')
    assert e.journal.archive_closed()==1
    e.journal.db.execute('UPDATE '+table+" SET status='unknown'")
    e.journal.close()
    with pytest.raises(ValueError,match='not terminal'):Journal(tmp_path/'state.sqlite','fake')


def test_warmup_resets_on_stale_generation_disconnect_and_hedges_continue(tmp_path):
    async def scenario():
        e=engine(tmp_path);now=[100.0];e.clock=lambda:now[0];e.reset_basis(SYMBOL)
        def fresh(generation=1):
            for venue in ('arcus','lighter'):
                e.book(venue,SYMBOL,Book(D(100),D('100.1'),D(10),D(10),now[0],generation))
        fresh();assert not e.quote_ready(SYMBOL)
        client=maker(e);await e.observe_maker(observation(e.journal.maker(client),'.1','canceled'))
        await e.drain_symbol(SYMBOL)
        assert e.transport.sends and not e.quote_ready(SYMBOL)
        for _ in range(60):now[0]+=1;fresh()
        assert e.quote_ready(SYMBOL)
        now[0]+=3;assert not e.quote_ready(SYMBOL) and SYMBOL not in e.warmup_started
        fresh();assert not e.quote_ready(SYMBOL)
        for _ in range(60):now[0]+=1;fresh()
        assert e.quote_ready(SYMBOL)
        fresh(2);assert not e.quote_ready(SYMBOL)
        for _ in range(60):now[0]+=1;fresh(2)
        assert e.quote_ready(SYMBOL)
        e.transport.connected=False;assert not e.data_ready() and not e.warmup_started
        e.transport.connected=True;fresh(2);assert not e.quote_ready(SYMBOL)
    asyncio.run(scenario())


def test_place_rechecks_symbol_freshness_after_waiting_for_capital_lock(tmp_path):
    async def scenario():
        e=engine(tmp_path);now=[e.clock()];e.clock=lambda:now[0]
        assert e.quote_ready(SYMBOL)
        await e.capital_lock.acquire()
        task=asyncio.create_task(e.place(SYMBOL,1,'open',D(1),D('.03'),0))
        await asyncio.sleep(0)
        assert not task.done()
        now[0]+=3;e.capital_lock.release()
        assert await task is None
        assert not e.journal.makers(active=True)
    asyncio.run(scenario())


def test_local_cancel_fill_wakes_hedge_during_quote_warmup(tmp_path):
    async def scenario():
        e=engine(tmp_path);now=[e.clock()];e.clock=lambda:now[0]
        client=maker(e);e.tasks=[object()]
        async def cancel_fill(row):return observation(row,'.1','canceled')
        e.transport.cancel_maker=cancel_fill
        now[0]+=3
        e.book('lighter',SYMBOL,Book(D(100),D('100.1'),D(10),D(10),now[0],1))
        await e.watchdog_once()
        assert not e.journal.makers(active=True)
        assert e.queues[SYMBOL].qsize() and e.journal.obligations()
        await e.drain_symbol(SYMBOL)
        assert e.transport.sends and not e.journal.obligations()
        assert not e.quote_ready(SYMBOL)
        e.tasks=[]
    asyncio.run(scenario())


def test_oversized_component_stays_current_while_other_components_archive(tmp_path):
    e=engine(tmp_path);client=maker(e)
    ids=[]
    for i in range(10):ids.append(e.journal.observe_maker(client,D(i+1)/100,'open'))
    e.journal.observe_maker(client,D('.1'),'canceled')
    attempt=e.journal.prepare_attempt(ids[0],D('.1'),obligations=ids)
    e.journal.observe_attempt(attempt,D('.1'),'filled')
    small=[]
    for _ in range(5):
        c=maker(e);e.journal.observe_maker(c,D(0),'canceled');small.append(c)
    for _ in range(10):e.journal.archive_closed(max_candidates=2,max_nodes=8)
    assert len(e.journal.db.execute('SELECT * FROM archive_orders').fetchall())==5
    assert e.journal.db.execute('SELECT client FROM orders').fetchone()[0]==client
    assert len(e.journal.allocations(attempt))==10
    assert e.journal.inventory(SYMBOL)==(D('.1'),D('-.1'))


def test_hot_queries_do_not_walk_completed_history(tmp_path):
    e=engine(tmp_path);j=e.journal;maker(e)
    queries=(lambda:j.makers(active=True),lambda:j.obligations(),lambda:j.inventory(SYMBOL),lambda:j.attempts(active=True))
    def steps(fn):
        count=[0]
        def progress():count[0]+=1;return 0
        j.db.set_progress_handler(progress,1)
        try:fn()
        finally:j.db.set_progress_handler(None,0)
        return count[0]
    before=[steps(fn) for fn in queries]
    with j.transaction():
        for i in range(1000):
            client='history-'+str(i);attempt='hedge-'+str(i);side=1 if i%2 else -1
            j.db.execute("INSERT INTO orders(client,venue,symbol,side,purpose,qty,price,level,status,filled,created,anchor,reserved) VALUES(?,'arcus',?,?,'open','1','100',0,'filled','1',1,'100','100')",(client,SYMBOL,side))
            oid=j.db.execute("INSERT INTO obligations(maker,qty,hedged,received) VALUES(?,'1','1',1)",(client,)).lastrowid
            j.db.execute("INSERT INTO attempts(client,obligation,venue,qty,filled,status,created) VALUES(?,?,'lighter','1','1','filled',1)",(attempt,oid))
            j.db.execute("INSERT INTO attempt_allocations(client,obligation,qty,filled) VALUES(?,?,'1','1')",(attempt,oid))
        j.db.execute("INSERT INTO current_inventory VALUES(?,'0','0')",(SYMBOL,))
    after=[steps(fn) for fn in queries]
    assert all(a<=b+20 for a,b in zip(after,before)),(before,after)


def legacy_copy(journal,version):
    import os,sqlite3
    from core.services.arbitrage_monitor_v2.execution.maker_taker_journal import SCHEMA_V1,SCHEMA_V2
    path=journal.path;replacement=path.with_suffix('.legacy')
    db=sqlite3.connect(replacement)
    db.executescript(SCHEMA_V1 if version==1 else SCHEMA_V2)
    tables=[r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")]
    for table in tables:
        columns=[r[1] for r in db.execute('PRAGMA table_info('+table+')')]
        rows=journal.db.execute('SELECT '+','.join(columns)+' FROM '+table).fetchall()
        if rows:db.executemany('INSERT INTO '+table+' VALUES('+','.join('?' for _ in columns)+')',[tuple(r) for r in rows])
    db.execute('PRAGMA user_version='+str(version));db.commit();db.close();journal.close();os.replace(replacement,path)
    return path


@pytest.mark.parametrize('version',[1,2])
@pytest.mark.parametrize('mutation',[
    "UPDATE orders SET side=0",
    "UPDATE orders SET venue='foreign'",
    "UPDATE orders SET purpose='foreign'",
    "UPDATE orders SET status='foreign'",
    "UPDATE orders SET status='filled'",
    "UPDATE attempts SET venue='foreign'",
    "UPDATE attempts SET status='foreign'",
    "UPDATE attempts SET status='filled'",
])
def test_legacy_invalid_lifecycle_refused_without_migration(tmp_path,version,mutation):
    import sqlite3
    from core.services.arbitrage_monitor_v2.execution.maker_taker_journal import Journal
    e=engine(tmp_path);oid=e.journal.observe_maker(maker(e),D('.5'),'canceled')
    attempt=e.journal.prepare_attempt(oid,D('.5'));e.journal.observe_attempt(attempt,D('.4'),'canceled')
    path=legacy_copy(e.journal,version)
    with sqlite3.connect(path) as db:db.execute(mutation)
    before=path.read_bytes()
    with pytest.raises(ValueError):Journal(path,'fake')
    assert path.read_bytes()==before
    with sqlite3.connect(path) as db:assert db.execute('PRAGMA user_version').fetchone()[0]==version


@pytest.mark.parametrize('version',[1,2])
def test_legacy_valid_partial_graph_migrates_atomically_and_reopens(tmp_path,version):
    from core.services.arbitrage_monitor_v2.execution.maker_taker_journal import Journal
    e=engine(tmp_path);oid=e.journal.observe_maker(maker(e),D('.5'),'canceled')
    attempt=e.journal.prepare_attempt(oid,D('.5'));e.journal.observe_attempt(attempt,D('.4'),'canceled')
    path=legacy_copy(e.journal,version);j=Journal(path,'fake')
    assert j.inventory(SYMBOL)==(D('.5'),D('-.4'))
    assert j.obligations()[0]['id']==oid and len(j.allocations(attempt))==1
    assert j.db.execute('PRAGMA user_version').fetchone()[0]==3
    j.close();j=Journal(path,'fake');assert j.inventory(SYMBOL)==(D('.5'),D('-.4'));j.close()


def closed_graph(e):
    client=maker(e);oid=e.journal.observe_maker(client,D('.5'),'canceled')
    attempt=e.journal.prepare_attempt(oid,D('.5'));e.journal.observe_attempt(attempt,D('.5'),'filled')
    assert e.journal.archive_closed()==1
    return client,oid,attempt


@pytest.mark.parametrize('sequence',['sequence','obligations'])
def test_reopen_rejects_archived_identity_highwater_regression(tmp_path,sequence):
    from core.services.arbitrage_monitor_v2.execution.maker_taker_journal import Journal
    e=engine(tmp_path);closed_graph(e)
    if sequence=='sequence':e.journal.db.execute('DELETE FROM sequence')
    e.journal.db.execute('UPDATE sqlite_sequence SET seq=0 WHERE name=?',(sequence,))
    path=e.journal.path;e.journal.close()
    with pytest.raises(ValueError,match='high-water'):Journal(path,'fake')


def test_runtime_allocator_refuses_archived_identity_reuse_and_rolls_back(tmp_path):
    e=engine(tmp_path);closed_graph(e);j=e.journal;inventory=j.inventory(SYMBOL)
    j.db.execute('DELETE FROM sequence');j.db.execute("UPDATE sqlite_sequence SET seq=0 WHERE name='sequence'")
    with pytest.raises(ValueError,match='reuse'):maker(e)
    assert not j.db.execute('SELECT 1 FROM orders').fetchone()
    assert j.inventory(SYMBOL)==inventory and j.db.execute('SELECT COUNT(*) FROM sequence').fetchone()[0]==0


def test_runtime_obligation_allocator_refuses_archive_collision_atomically(tmp_path):
    e=engine(tmp_path);closed_graph(e);j=e.journal;inventory=j.inventory(SYMBOL);client=maker(e)
    j.db.execute("UPDATE sqlite_sequence SET seq=0 WHERE name='obligations'")
    with pytest.raises(ValueError,match='reuse'):j.observe_maker(client,D('.1'),'canceled')
    assert D(j.maker(client)['filled'])==0 and j.maker(client)['status']=='prepared'
    assert not j.obligations() and j.inventory(SYMBOL)==inventory


def test_legacy_external_client_does_not_inflate_strategy_sequence(tmp_path):
    from types import SimpleNamespace as NS
    from core.services.arbitrage_monitor_v2.execution.maker_taker_journal import Journal
    e=engine(tmp_path);j=e.journal
    client=j.legacy_cancel_intent(NS(client_id='amlt-999999',id='external',symbol=SYMBOL,side=NS(value='buy'),amount=D(1),price=D(100),filled=D(0)))
    j.observe_maker(client,D(0),'canceled');j.archive_closed();path=j.path;j.close()
    j=Journal(path,'fake');assert j.prepare_maker(SYMBOL,1,'open',D(1),D(99),0,D(100),D(101))=='amlt-1';j.close()


def test_deferred_attempt_history_does_not_grow_retry_lookup_work(tmp_path):
    e=engine(tmp_path);j=e.journal;oid=j.observe_maker(maker(e),D('.1'),'canceled')
    failure=j.prepare_attempt(oid,D('.1'));j.observe_attempt(failure,D(0),'canceled')
    def consumed_candidates():
        original=j.attempts;consumed=[]
        def attempts(*args,**kwargs):
            assert kwargs.get('include_deferred') is False
            rows=original(*args,**kwargs)
            consumed.append(tuple(row['client'] for row in rows))
            return rows
        j.attempts=attempts
        try:state=e.residual_state(e.residual_buckets()[0])
        finally:j.attempts=original
        assert state==('eligible',D('.1'))
        assert consumed and all(set(rows)=={failure} for rows in consumed)
        return consumed
    before=consumed_candidates()
    with j.transaction():
        for i in range(2000):
            client='deferred-'+str(i)
            j.db.execute("INSERT INTO attempts(client,obligation,venue,qty,status,created) VALUES(?,?,'lighter','.1','deferred',1)",(client,oid))
            j.db.execute("INSERT INTO attempt_allocations(client,obligation,qty,deferred) VALUES(?,?,'0.1',1)",(client,oid))
    assert len(j.attempts(oid))==2001 and len(j.attempts(oid,include_deferred=False))==1
    after=consumed_candidates()
    assert after==before # Historical no-send rows never reach the retry classifier.


@pytest.mark.parametrize('arcus_failure',[False,True])
def test_full_finance_collection_preserves_unknown_costs_and_survives_failures(tmp_path,arcus_failure):
    import json
    from types import SimpleNamespace as NS
    from core.services.arbitrage_monitor_v2.history.maker_taker_finance import FinanceCollector
    async def scenario():
        e=engine(tmp_path);j=e.journal;oid=j.observe_maker(maker(e),D('.1'),'canceled')
        client=j.prepare_attempt(oid,D('.1'));j.observe_attempt(client,D('.1'),'filled',exchange_id='123')
        async def trades(**kwargs):return NS(code=200,trades=[],next_cursor=None)
        async def funding(**kwargs):return NS(code=200,position_fundings=[],next_cursor=None)
        async def arcus_history(*args):
            if arcus_failure:raise TimeoutError('offline timeout')
            if False:yield None
        transport=NS(arcus_history=arcus_history,source=NS(market_ids={}),lighter=NS(_rest=NS(account_index=1,
            order_api=NS(trades=trades),account_api=NS(position_funding=funding),_get_auth_token=lambda:'offline')))
        collector=FinanceCollector(j,transport);await collector.collect()
        coverage={(r['venue'],r['kind']):dict(r) for r in j.db.execute('SELECT * FROM coverage')}
        assert not coverage['lighter','fills']['complete'] and 'fee_semantics_unverified' in coverage['lighter','fills']['reason']
        assert not coverage['lighter','funding']['complete']
        assert bool(coverage['arcus','fills']['complete']) is not arcus_failure
        assert bool(coverage['arcus','funding']['complete']) is not arcus_failure
        errors=[json.loads(r[0]) for r in j.db.execute("SELECT payload FROM events WHERE kind='finance_unavailable'")]
        assert len(errors)==(4 if arcus_failure else 2) and {r['finance_kind'] for r in errors}=={'fills','funding'}
        assert collector.last>0
        # An unsupported fee/funding response remains unavailable on repeated collections too.
        await collector.collect()
        assert j.db.execute('SELECT COUNT(*) FROM executions').fetchone()[0]==0
    asyncio.run(scenario())


def test_statistics_process_matches_snapshot_and_does_not_modify_journal(tmp_path):
    from core.services.arbitrage_monitor_v2.history.maker_taker_statistics import statistics_background,statistics_snapshot
    async def scenario():
        e=engine(tmp_path);closed_graph(e);j=e.journal
        expected=statistics_snapshot(j.path,'hour')
        assert await statistics_background(j.path,'hour')==expected
        assert j.inventory(SYMBOL)==(D('.5'),D('-.5'))
    asyncio.run(scenario())


@pytest.mark.parametrize('outcome',['success','nonzero','invalid_json','timeout','cancel','kill'])
def test_statistics_child_protocol_environment_and_cleanup(tmp_path,monkeypatch,outcome):
    import json,os
    from core.services.arbitrage_monitor_v2.history.maker_taker_statistics import statistics_background
    async def scenario():
        finished=asyncio.Event();calls={};counts={'terminate':0,'kill':0,'wait':0}
        slow=outcome in ('timeout','cancel','kill')
        class Stream:
            def __init__(self,stdout):self.stdout=stdout;self.used=False
            async def read(self,*args):
                if self.used:return b''
                self.used=True
                if self.stdout and slow:await finished.wait()
                return (b'not JSON' if outcome=='invalid_json' else b'[]') if self.stdout else b'stderr diagnostic'*1000
        class Process:
            returncode=None if slow else (3 if outcome=='nonzero' else 0)
            stdout=Stream(True);stderr=Stream(False)
            async def wait(self):
                counts['wait']+=1
                if self.returncode is None:await finished.wait()
                return self.returncode
            def terminate(self):
                counts['terminate']+=1
                if outcome!='kill':self.returncode=-15;finished.set()
            def kill(self):counts['kill']+=1;self.returncode=-9;finished.set()
        process=Process()
        async def create(*args,**kwargs):calls.update(args=args,kwargs=kwargs);return process
        monkeypatch.setattr(asyncio,'create_subprocess_exec',create)
        monkeypatch.setenv('ARCUS_API_KEY','test-only-placeholder')
        monkeypatch.setenv('LIGHTER_API_KEY_PRIVATE_KEY','test-only-placeholder')
        if outcome=='kill':
            original=asyncio.wait_for
            async def short_grace(awaitable,timeout):return await original(awaitable,.01 if timeout==2 else timeout)
            monkeypatch.setattr(asyncio,'wait_for',short_grace)
        task=asyncio.create_task(statistics_background(tmp_path/'state.sqlite',timeout=.01 if slow else 1))
        if outcome=='cancel':
            await asyncio.sleep(0);await asyncio.sleep(0);task.cancel()
        expected={'nonzero':RuntimeError,'invalid_json':ValueError,'timeout':TimeoutError,'kill':TimeoutError,'cancel':asyncio.CancelledError}.get(outcome)
        if expected:
            with pytest.raises(expected):await task
        else:assert await task==[]
        options=calls['kwargs']
        assert options['close_fds'] is True and options['env']=={'PATH':os.defpath,'PYTHONDONTWRITEBYTECODE':'1'}
        assert calls['args'][-2:]==(str((tmp_path/'state.sqlite').resolve()),'hour')
        assert all('placeholder' not in arg for arg in calls['args'])
        if slow:
            assert counts['terminate']==1 and counts['wait']>=1 and process.returncode is not None
        assert counts['kill']==(1 if outcome=='kill' else 0)
    asyncio.run(scenario())
