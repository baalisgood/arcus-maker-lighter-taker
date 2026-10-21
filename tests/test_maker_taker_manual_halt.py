"""Severe failures preserve positions and require durable manual intervention."""
import asyncio
import json
import sqlite3
from collections import deque
from dataclasses import replace
from decimal import Decimal as D
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock,Mock

import pytest

from test_maker_taker_engine import engine,maker,observation,SYMBOL
from test_maker_taker_sol_unlock import add_other,OTHER
from core.services.arbitrage_monitor_v2.execution.maker_taker_engine import MakerTakerEngine,DefinitiveFailure,DeferredHedge


def test_retry_exhaustion_cancels_every_symbol_without_unwind_and_survives_restart(tmp_path):
    async def run():
        e=engine(tmp_path);add_other(e);client=maker(e,qty='.2')
        other=e.journal.prepare_maker(OTHER,1,'open',D('.1'),D(100),0,D(100),D(100))
        await e.observe_maker(observation(e.journal.maker(client),'.2','filled',average=D(100)))
        e.transport.results=[('rejected',0)]*4
        await e.drain_symbol(SYMBOL)
        assert len(e.transport.sends)==4 and not e.transport.emergencies
        assert e.journal.inventory(SYMBOL)==(D('.2'),D(0))
        assert e.journal.maker(other)['status']=='canceled' and e.journal.obligations()
        assert e.manual_intervention_required and not e.transport.send_guard()
        snapshot=e.manual_intervention_snapshot
        assert snapshot['reason_code']=='retry_exhausted' and snapshot['failure_symbol']==SYMBOL
        assert D(snapshot['net_exposure_qty'])==D('.2')
        e.alarm('Hedge confirmation exceeded 500ms target',OTHER)
        await e.drain_symbol(SYMBOL);await e.tick();assert not await e.reconcile()
        assert len(e.transport.sends)==4 and snapshot==e.manual_intervention_snapshot
        restarted=MakerTakerEngine(e.profile,e.journal,e.transport,e.rules)
        assert restarted.manual_intervention_required and not await restarted.recover()
        assert not await restarted.stop(timeout=.1)
        assert len(e.transport.sends)==4 and not e.transport.emergencies
    asyncio.run(run())


@pytest.mark.parametrize('outcome',['rejected','partial','open','timeout'])
def test_emergency_failure_latches_without_repeated_send_or_lost_residual(tmp_path,outcome):
    async def run():
        e=engine(tmp_path);client=maker(e,qty='.2')
        await e.observe_maker(observation(e.journal.maker(client),'.2','filled',average=D(100)))
        oid=e.journal.obligations()[0]['id'];obligation=e.journal.obligation(oid)
        async def failed(attempt,obligation):
            if outcome=='rejected':raise DefinitiveFailure('synthetic-secret-marker')
            if outcome=='timeout':raise TimeoutError('synthetic-secret-marker')
            return observation(attempt,'.1', 'canceled' if outcome=='partial' else 'open','arcus',-obligation['side'],average=D(100))
        e.transport.emergency=failed
        await e.emergency(obligation,D('.2'))
        assert e.manual_intervention_required and e.manual_intervention_snapshot['reason_code']=='emergency_failed'
        rows=e.journal.attempts();assert len(rows)==1
        assert e.journal.obligations()
        assert e.journal.inventory(SYMBOL)[0]==(D('.1') if outcome in ('partial','open') else D('.2'))
        await e.emergency(obligation,D('.2'));assert len(e.journal.attempts())==1
        assert 'synthetic-secret-marker' not in '\n'.join(r[0] for r in e.journal.db.execute('SELECT payload FROM events'))
    asyncio.run(run())


def test_manual_snapshot_has_independent_first_cause_and_atomic_persistence(tmp_path):
    e=engine(tmp_path);e.halt('UNKNOWN_ORDER','Earlier cause')
    e.serious_halt('retry_exhausted',SYMBOL,D('.2'))
    first=e.manual_intervention_snapshot.copy()
    e.serious_halt('emergency_failed',SYMBOL,D('.5'))
    assert e.halt_reason=='Earlier cause' and e.manual_intervention_snapshot==first
    assert e.journal.db.execute("SELECT COUNT(*) FROM events WHERE kind='manual_intervention'").fetchone()[0]==1
    assert e.journal.manual_intervention()==first


def test_late_cancel_fill_is_recorded_without_new_hedge(tmp_path):
    async def run():
        e=engine(tmp_path);client=maker(e)
        e.serious_halt('retry_exhausted',SYMBOL,D('.2'))
        async def cancel(row):return observation(row,'.1','canceled',average=D(100))
        e.transport.cancel_maker=cancel
        await e.cancel_all();await e.drain_symbol(SYMBOL)
        assert e.journal.inventory(SYMBOL)==(D('.1'),D(0)) and not e.transport.sends
        assert e.journal.db.execute("SELECT COUNT(*) FROM events WHERE kind='manual_intervention_late_fill'").fetchone()[0]==1
    asyncio.run(run())


@pytest.mark.parametrize('failed_kind',['manual_intervention','halt'])
def test_failed_latch_transaction_rolls_back_but_memory_remains_fail_closed(tmp_path,failed_kind):
    e=engine(tmp_path);original=e.journal.event
    def fail(symbol,kind,**payload):
        if kind==failed_kind:raise RuntimeError('offline commit failure')
        return original(symbol,kind,**payload)
    e.journal.event=fail
    with pytest.raises(RuntimeError):e.serious_halt('retry_exhausted',SYMBOL,D('.2'))
    first=e.manual_intervention_snapshot.copy()
    assert e.halted and e.manual_intervention_required and not e.sends_allowed()
    assert not e.manual_intervention_persisted
    assert e.journal.manual_intervention() is None
    assert not e.journal.db.execute("SELECT 1 FROM events WHERE kind='manual_intervention'").fetchone()
    assert not e.journal.db.execute("SELECT 1 FROM events WHERE kind='halt'").fetchone()
    e.journal.event=original;e.serious_halt('emergency_failed',SYMBOL,D('.5'))
    assert e.journal.manual_intervention()==first and e.manual_intervention_persisted


@pytest.mark.parametrize('already_sent',[False,True])
def test_other_inflight_hedge_is_deferred_or_actual_fill_is_booked_after_latch(tmp_path,already_sent):
    async def run():
        e=engine(tmp_path);add_other(e);entered=asyncio.Event();release=asyncio.Event()
        e.profile=replace(e.profile,hedge=replace(e.profile.hedge,request_timeout=2))
        client=e.journal.prepare_maker(OTHER,1,'open',D('.2'),D(100),0,D(100),D(100))
        e.journal.observe_maker(client,D('.2'),'filled',average=D(100))
        async def hedge(attempt,obligation):
            entered.set();await release.wait()
            if not already_sent and not e.transport.send_guard():raise DeferredHedge()
            return observation({**attempt,'symbol':OTHER},attempt['qty'],'filled','lighter',-1,average=D(100))
        e.transport.hedge=hedge;task=asyncio.create_task(e.drain_symbol(OTHER));await entered.wait()
        e.serious_halt('retry_exhausted',SYMBOL,D('.2'));release.set();await task
        assert e.journal.inventory(OTHER)==(D('.2'),D('-.2') if already_sent else D(0))
        assert len(e.journal.attempts())==1
        if not already_sent:assert e.journal.attempts()[0]['status']=='deferred'
        await e.drain_symbol(OTHER);assert len(e.journal.attempts())==1
    asyncio.run(run())


def test_paper_latency_guard_preserves_depth_and_positions(tmp_path):
    from test_maker_taker_depth_retry import paper
    async def run():
        e=engine(tmp_path);transport=paper(e);transport.send_guard=e.sends_allowed;transport.hedge_latency_ms=20
        transport.source._publish('lighter',SYMBOL,[['100','1']],[['100.1','1']])
        before=dict(transport.positions)
        task=asyncio.create_task(transport.hedge({'client':'7','qty':'.2'},{'symbol':SYMBOL,'side':1}))
        await asyncio.sleep(0);e.serious_halt('retry_exhausted',SYMBOL,D('.2'))
        with pytest.raises(DeferredHedge):await task
        assert dict(transport.positions)==before and not transport.hedge_liquidity
        with pytest.raises(DeferredHedge):await transport.emergency({'qty':'.2'},{'symbol':SYMBOL,'side':1})
    asyncio.run(run())


@pytest.mark.parametrize('kind',['maker','emergency'])
def test_real_arcus_lock_wait_checks_latch_before_nonce_sign_audit_http(tmp_path,kind):
    from test_arcus_adapter import adapter,MARKET
    from core.services.arbitrage_monitor_v2.execution.maker_taker_live import LiveTransport
    async def run():
        e=engine(tmp_path);e.profile=replace(e.profile,hedge=replace(e.profile.hedge,request_timeout=2));a=adapter()
        a._set_markets([{**MARKET,'marketDisplayName':'SOL-USD','tickSize':'.01','stepSize':'.001',
            'minOrderSize':'.01','maxOrderSize':'1000','markPrice':'100','tickTiers':[]}])
        a._do_authenticate=AsyncMock();a.get_orderbook=AsyncMock(return_value=NS(best_bid=NS(price=D(100)),best_ask=NS(price=D('100.1'))))
        a._nonce=Mock();a._sign=Mock();a.write_audit=Mock();a._request=AsyncMock()
        live=LiveTransport.__new__(LiveTransport);live.arcus=a;live.maker_guard=e.maker_admitted;live.send_guard=e.sends_allowed
        await a._write_lock.acquire()
        if kind=='maker':
            e.transport.maker=live.maker;task=asyncio.create_task(e.place(SYMBOL,1,'open',D('.2'),D(0),0))
        else:task=asyncio.create_task(live.emergency({'client':'7','qty':'.2'},{'symbol':SYMBOL,'side':1,'purpose':'open'}))
        for _ in range(10):
            await asyncio.sleep(0)
            if a._write_lock._waiters:break
        assert a._write_lock._waiters
        e.serious_halt('retry_exhausted',SYMBOL,D('.2'));a._write_lock.release()
        if kind=='maker':assert e.journal.maker(await task)['status']=='deferred'
        else:
            with pytest.raises(DeferredHedge):await task
        a._nonce.assert_not_called();a._sign.assert_not_called();a.write_audit.assert_not_called();a._request.assert_not_called()
    asyncio.run(run())


@pytest.mark.parametrize('stage',['write_lock','nonce'])
@pytest.mark.parametrize('inventory_failure',[False,True])
def test_real_lighter_strict_wait_checks_bound_latch_before_sign_audit_http(tmp_path,stage,inventory_failure):
    from core.adapters.exchanges.adapters.lighter_rest import LighterRest
    from core.services.arbitrage_monitor_v2.execution.maker_taker_live import LiveTransport
    async def run():
        e=engine(tmp_path);add_other(e);rest=object.__new__(LighterRest);rest._strict_write_lock=asyncio.Lock();rest._uncertain_orders={};rest._retired=False
        entered=asyncio.Event();release=asyncio.Event()
        async def nonce():entered.set();await release.wait();return 0,7
        manager=NS(acknowledge_failure=Mock());sign=Mock();send=AsyncMock()
        rest.signer_client=NS(sign_create_order=sign,nonce_manager=manager,send_tx=send)
        rest.write_audit=Mock();rest._next_order_nonce=AsyncMock(side_effect=nonce);rest._register_order_intent=Mock()
        live=LiveTransport.__new__(LiveTransport);live.send_guard=e.sends_allowed;live.writes=deque()
        live.lighter=NS(_rest=rest,_normalize_symbol=lambda s:s);live.private_validity=lambda:{'lighter':(True,None,None)}
        live.source=NS(markets={OTHER:({},dict(market_id=2,supported_price_decimals=3,supported_size_decimals=3))},
            rules=e.rules,books={('lighter',OTHER):e.books['lighter',OTHER]},book_validity=lambda *args:(True,None,None))
        if stage=='write_lock':await rest._strict_write_lock.acquire()
        task=asyncio.create_task(live.hedge({'client':'7','qty':'.2'},{'symbol':OTHER,'side':1,'purpose':'open'}))
        if stage=='write_lock':
            for _ in range(10):
                await asyncio.sleep(0)
                if rest._strict_write_lock._waiters:break
            assert rest._strict_write_lock._waiters
        else:await entered.wait()
        if inventory_failure:
            def failed(symbol):raise sqlite3.OperationalError('synthetic-secret-marker')
            e.journal.inventory=failed
            with pytest.raises(RuntimeError,match='snapshot unavailable'):e.serious_halt('retry_exhausted',SYMBOL,D('.2'))
        else:e.serious_halt('retry_exhausted',SYMBOL,D('.2'))
        if stage=='write_lock':rest._strict_write_lock.release()
        else:release.set()
        with pytest.raises(DeferredHedge):await task
        sign.assert_not_called();rest.write_audit.assert_not_called();send.assert_not_called()
        if stage=='write_lock':rest._next_order_nonce.assert_not_called()
        else:manager.acknowledge_failure.assert_called_once_with(0)
    asyncio.run(run())


@pytest.mark.parametrize('cause',['size','cancel','unknown','reconcile','capacity','capital'])
def test_emergency_preconditions_cannot_fail_without_manual_lock(tmp_path,cause):
    async def run():
        e=engine(tmp_path);client=maker(e,qty='.2')
        oid=e.journal.observe_maker(client,D('.2'),'filled',average=D(100));o=e.journal.obligation(oid);remaining=D('.2')
        if cause=='size':remaining=D('.0005')
        elif cause=='cancel':maker(e)
        elif cause=='unknown':
            e.journal.prepare_attempt(oid,D('.2'));e.halt('UNKNOWN_ORDER','Existing uncertain attempt')
        elif cause=='reconcile':o={**o,'purpose':'reconcile'}
        else:
            from test_maker_taker_close_floor import opened
            opened(e.journal,1,'.2','100','98')
            close=maker(e,qty='.2',purpose='close',side=-1)
            oid=e.journal.observe_maker(close,D('.2'),'filled',average=D(100));o=e.journal.obligation(oid)
            if cause=='capacity':remaining=D('.3')
            else:e.equity['arcus']=D(0)
        before=e.journal.inventory(SYMBOL)
        await e.emergency(o,remaining)
        assert e.manual_intervention_required and e.manual_intervention_snapshot['failure_detail']
        assert not e.transport.emergencies and e.journal.inventory(SYMBOL)==before
        if cause=='unknown':assert e.halt_reason=='Existing uncertain attempt' and e.journal.attempts(active=True)
    asyncio.run(run())


def test_restart_new_journal_retains_latch_boot_and_snapshot(tmp_path):
    async def run():
        e=engine(tmp_path);e.serious_halt('retry_exhausted',SYMBOL,D('.2'));snapshot=e.manual_intervention_snapshot
        e.journal.close();e=engine(tmp_path)
        assert e.manual_intervention_required and e.manual_intervention_snapshot==snapshot
        assert e.journal.boot!=snapshot['boot'] and not await e.recover()
        assert not e.transport.sends and not e.transport.emergencies
    asyncio.run(run())


def test_runner_reports_manual_snapshot_at_halt_final_and_blocked_restart(tmp_path,monkeypatch,capsys):
    import run_maker_taker
    from pathlib import Path
    from core.services.arbitrage_monitor_v2.execution.maker_taker_paper import PublicMarkets
    data=json.loads(Path('tests/fixtures/maker_taker_replay.json').read_text())
    class OfflineMarkets(PublicMarkets):
        async def metadata(self):
            self.load_metadata(data['arcus_markets'],data['lighter_markets'])
            self.connected={'arcus':True,'lighter':True};self.generations={'arcus':1,'lighter':1};return self.rules
        async def start(self,callback):
            self.callback=callback
            for frame in data['initial_books']:self._publish(frame['venue'],frame['symbol'],frame['bids'],frame['asks'])
    async def failure(self):
        client=self.journal.prepare_maker(SYMBOL,1,'open',D('.2'),D(100),0,D(100),D(100))
        oid=self.journal.observe_maker(client,D('.2'),'filled',average=D(100))
        attempt=self.journal.prepare_attempt(oid,D('.1'));self.journal.observe_attempt(attempt,D('.1'),'filled',average=D(100))
        self.serious_halt('retry_exhausted',SYMBOL,D('.1'))
        await self.cancel_all()
    monkeypatch.setattr('core.services.arbitrage_monitor_v2.execution.maker_taker_paper.PublicMarkets',OfflineMarkets)
    monkeypatch.setattr(MakerTakerEngine,'tick',failure)
    monkeypatch.setattr('core.services.arbitrage_monitor_v2.history.maker_taker_statistics.statistics_background',AsyncMock(return_value=[]))
    args=['--mode','monitor','--duration-seconds','.1','--state-dir',str(tmp_path/'run')]
    assert run_maker_taker.main(args)==1
    rows=[json.loads(line) for line in capsys.readouterr().out.splitlines() if line.startswith('{')]
    halted=[p for p in rows if p.get('manual_intervention_required')]
    assert len(halted)==2 and halted[-1]['state']=='STOPPING' and not halted[-1]['shutdown_clean']
    first=halted[0]['manual_intervention_snapshot']
    assert all(p['manual_intervention_snapshot']==first for p in halted)
    assert D(first['arcus_qty'])==D('.2') and D(first['lighter_qty'])==D('-.1') and D(first['net_exposure_qty'])==D('.1')
    assert run_maker_taker.main(args)==2
    restarted=[json.loads(line) for line in capsys.readouterr().out.splitlines() if line.startswith('{')]
    assert len(restarted)==2 and all(p['manual_intervention_snapshot']==first for p in restarted)


@pytest.mark.parametrize('failed_event',['manual_intervention','close_quote_wait'])
def test_persistence_or_wait_diagnostic_failure_still_attempts_global_cancel(tmp_path,failed_event):
    async def run():
        e=engine(tmp_path);add_other(e);client=maker(e,qty='.2')
        oid=e.journal.observe_maker(client,D('.2'),'filled',average=D(100))
        other=e.journal.prepare_maker(OTHER,1,'open',D('.1'),D(100),0,D(100),D(100))
        for _ in range(4):
            attempt=e.journal.prepare_attempt(oid,D('.2'));e.journal.observe_attempt(attempt,D(0),'rejected')
        e.close_quote_wait(SYMBOL,-1,'quota','replace_missing_order');original=e.journal.event
        def fail(symbol,kind,**payload):
            if kind==failed_event:raise RuntimeError('synthetic storage failure')
            return original(symbol,kind,**payload)
        e.journal.event=fail
        if failed_event=='manual_intervention':
            with pytest.raises(RuntimeError,match='latch persistence failed'):await e.drain_symbol(SYMBOL)
        else:await e.drain_symbol(SYMBOL)
        assert other in e.transport.cancels and e.journal.maker(other)['status']=='canceled'
        assert e.halted and not e.sends_allowed() and not e.transport.sends and not e.transport.emergencies
        assert e.manual_intervention_persisted==(failed_event!='manual_intervention')
    asyncio.run(run())


@pytest.mark.parametrize('operation',['reconcile','correction'])
def test_manual_latch_during_snapshot_await_prevents_new_correction(tmp_path,operation):
    async def run():
        e=engine(tmp_path);entered=asyncio.Event();release=asyncio.Event()
        e.profile=replace(e.profile,hedge=replace(e.profile.hedge,request_timeout=2))
        if operation=='correction':e.journal.reconcile_inventory(SYMBOL,D('.2'),D(0),D(100))
        async def snapshot():
            entered.set();await release.wait()
            return {'authoritative':True,'equity':e.equity,'positions':{SYMBOL:[D('.2'),D(0)]}}
        e.transport.snapshot=snapshot
        task=asyncio.create_task(e.reconcile() if operation=='reconcile' else e.correction_admitted(e.journal.obligations()[0],D('.2')))
        await entered.wait();before=e.journal.inventory(SYMBOL)
        e.serious_halt('retry_exhausted',SYMBOL,D('.2'));release.set()
        assert not await task and e.journal.inventory(SYMBOL)==before
        assert not e.journal.attempts() and not e.transport.sends
    asyncio.run(run())


def test_latched_unknown_attempt_lookup_records_real_late_fill_without_replay(tmp_path):
    async def run():
        e=engine(tmp_path);client=maker(e,qty='.2')
        oid=e.journal.observe_maker(client,D('.2'),'filled',average=D(100))
        attempt=e.journal.prepare_attempt(oid,D('.2'));row=e.journal.attempt(attempt);e.journal.attempt_state(attempt,'unknown')
        e.transport.lookups[attempt]=observation(row,'.2','filled','lighter',-1,average=D(100))
        e.serious_halt('retry_exhausted',SYMBOL,D('.2'));first=e.manual_intervention_snapshot.copy()
        await e.drain_symbol(SYMBOL)
        assert e.journal.inventory(SYMBOL)==(D('.2'),D('-.2')) and not e.transport.sends
        assert e.manual_intervention_snapshot==first and not await e.stop(timeout=.1)
    asyncio.run(run())


def test_unresolved_legacy_emergency_also_latches_and_keeps_unknown_identity(tmp_path):
    async def run():
        e=engine(tmp_path);client=maker(e,qty='.2')
        oid=e.journal.observe_maker(client,D('.2'),'filled',average=D(100))
        attempt=e.journal.prepare_attempt(oid,D('.2'),'arcus');e.journal.attempt_state(attempt,'unknown')
        await e.drain_symbol(SYMBOL)
        assert e.manual_intervention_required and e.manual_intervention_snapshot['reason_code']=='emergency_residual'
        assert e.journal.attempt(attempt)['status']=='unknown' and e.halt_state=='UNKNOWN_ORDER'
        assert not e.transport.sends and not e.transport.emergencies and e.journal.inventory(SYMBOL)==(D('.2'),D(0))
    asyncio.run(run())


def test_inventory_read_failure_blocks_sends_before_db_and_never_fabricates_zero(tmp_path):
    e=engine(tmp_path);e.close_permits[SYMBOL,-1]={'test':True}
    def failed(symbol):raise sqlite3.OperationalError('synthetic-secret-marker')
    e.journal.inventory=failed
    with pytest.raises(RuntimeError,match='snapshot unavailable'):e.serious_halt('retry_exhausted',SYMBOL,D('.2'))
    assert e.manual_intervention_required and e.halted and e.state=='EMERGENCY' and not e.sends_allowed()
    assert not e.close_permits
    snapshot=e.manual_intervention_snapshot
    assert snapshot['arcus_qty'] is None and snapshot['lighter_qty'] is None and snapshot['net_exposure_qty'] is None
    assert snapshot['inventory_coverage']=='unavailable' and 'synthetic-secret-marker' not in json.dumps(snapshot)


@pytest.mark.parametrize('failure',['rejected','unknown'])
def test_emergency_failure_accounting_error_still_latches_and_cancels_other_symbol(tmp_path,failure):
    async def run():
        e=engine(tmp_path);add_other(e);client=maker(e,qty='.2')
        oid=e.journal.observe_maker(client,D('.2'),'filled',average=D(100));obligation=e.journal.obligation(oid)
        entered=asyncio.Event();release=asyncio.Event()
        async def emergency(*args):
            entered.set();await release.wait()
            if failure=='rejected':raise DefinitiveFailure('synthetic-secret-marker')
            raise TimeoutError('synthetic-secret-marker')
        e.transport.emergency=emergency
        task=asyncio.create_task(e.emergency(obligation,D('.2')));await entered.wait()
        other=e.journal.prepare_maker(OTHER,1,'open',D('.1'),D(100),0,D(100),D(100))
        def fail(*args,**kwargs):raise sqlite3.OperationalError('synthetic-secret-marker')
        if failure=='rejected':e.journal.observe_attempt=fail
        else:e.journal.attempt_state=fail
        release.set()
        with pytest.raises(RuntimeError):await task
        assert e.manual_intervention_required and not e.sends_allowed() and other in e.transport.cancels
        assert e.journal.maker(other)['status']=='canceled' and e.journal.attempts(active=True)
        assert e.manual_intervention_snapshot['unresolved_qty'] is None
        assert 'synthetic-secret-marker' not in '\n'.join(r[0] for r in e.journal.db.execute('SELECT payload FROM events'))
    asyncio.run(run())


def test_first_serious_halt_event_atomically_carries_snapshot_after_generic_halt(tmp_path):
    e=engine(tmp_path);e.halt('UNKNOWN_ORDER','Earlier first cause')
    e.serious_halt('retry_exhausted',SYMBOL,D('.2'));e.serious_halt('emergency_failed',SYMBOL,D('.3'))
    rows=[json.loads(r[0]) for r in e.journal.db.execute("SELECT payload FROM events WHERE kind='halt'")]
    severe=[p for p in rows if p.get('manual_intervention_required')]
    assert len(rows)==2 and len(severe)==1 and e.halt_reason=='Earlier first cause'
    assert all(severe[0][key]==value for key,value in e.manual_intervention_snapshot.items())
    assert severe[0]['failure_symbol']==SYMBOL and severe[0]['reason_code']=='retry_exhausted'


def test_resolved_partial_legacy_emergency_snapshot_uses_latest_remaining(tmp_path):
    async def run():
        e=engine(tmp_path);client=maker(e,qty='.2')
        oid=e.journal.observe_maker(client,D('.2'),'filled',average=D(100))
        attempt=e.journal.prepare_attempt(oid,D('.2'),'arcus');e.journal.attempt_state(attempt,'unknown')
        e.transport.lookups[attempt]=observation(e.journal.attempt(attempt),'.1','open','arcus',-1,average=D(100))
        await e.drain_symbol(SYMBOL)
        assert e.manual_intervention_required and e.journal.inventory(SYMBOL)==(D('.1'),D(0))
        assert D(e.manual_intervention_snapshot['unresolved_qty'])==D('.1')
    asyncio.run(run())


@pytest.mark.parametrize('worker',[False,True])
def test_confirmed_failed_residual_locks_before_failure_diagnostics(tmp_path,worker):
    async def run():
        e=engine(tmp_path);add_other(e);client=maker(e,qty='.2')
        oid=e.journal.observe_maker(client,D('.2'),'filled',average=D(100))
        attempt=e.journal.prepare_attempt(oid,D('.2'),'arcus');e.journal.observe_attempt(attempt,D(0),'rejected')
        other=e.journal.prepare_maker(OTHER,1,'open',D('.1'),D(100),0,D(100),D(100))
        assert e.residual_state(e.residual_buckets(SYMBOL)[0])[0]=='failed'
        def failed(*args):raise sqlite3.OperationalError('synthetic-secret-marker')
        e.zero_fill_failures=failed
        if worker:
            task=asyncio.create_task(e._worker(SYMBOL));e.queues[SYMBOL].put_nowait(None)
            await e.queues[SYMBOL].join();task.cancel();await asyncio.gather(task,return_exceptions=True)
        else:
            with pytest.raises(RuntimeError,match='failure diagnostics unavailable'):await e.drain_symbol(SYMBOL)
        assert e.manual_intervention_required and not e.transport.send_guard() and e.manual_intervention_persisted
        assert other in e.transport.cancels and e.journal.maker(other)['status']=='canceled'
        assert e.manual_intervention_snapshot['unresolved_qty'] is None
        assert e.manual_intervention_snapshot['failure_detail']=='failure_diagnostics_unavailable'
        assert e.journal.inventory(SYMBOL)==(D('.2'),D(0)) and e.journal.obligations()
        assert 'synthetic-secret-marker' not in '\n'.join(r[0] for r in e.journal.db.execute('SELECT payload FROM events'))
    asyncio.run(run())


@pytest.mark.parametrize('failure',['obligation_read','partial_response_accounting'])
def test_legacy_emergency_resolution_error_locks_and_preserves_actual_fill(tmp_path,failure):
    async def run():
        e=engine(tmp_path);add_other(e);client=maker(e,qty='.2')
        oid=e.journal.observe_maker(client,D('.2'),'filled',average=D(100))
        attempt=e.journal.prepare_attempt(oid,D('.2'),'arcus');e.journal.attempt_state(attempt,'unknown')
        other=e.journal.prepare_maker(OTHER,1,'open',D('.1'),D(100),0,D(100),D(100))
        def failed(*args):raise sqlite3.OperationalError('synthetic-secret-marker')
        if failure=='obligation_read':e.journal.obligation=failed
        else:
            e.transport.lookups[attempt]=observation(e.journal.attempt(attempt),'.1','open','arcus',-1,average=D(100))
            e.journal.attempt_state=failed
        with pytest.raises(RuntimeError,match='resolution accounting unavailable'):await e.drain_symbol(SYMBOL)
        assert e.manual_intervention_required and not e.sends_allowed() and e.manual_intervention_persisted
        assert other in e.transport.cancels and e.journal.maker(other)['status']=='canceled'
        assert e.journal.inventory(SYMBOL)==(D('.2') if failure=='obligation_read' else D('.1'),D(0))
        assert e.journal.attempt(attempt)['client']==attempt
    asyncio.run(run())
