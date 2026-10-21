"""Offline state-machine and crash boundaries; all exchange transports are fake."""
import asyncio
import sqlite3
import time
from dataclasses import replace
from decimal import Decimal as D
from types import SimpleNamespace

import pytest

from core.services.arbitrage_monitor_v2.config.maker_taker_config import (
    MakerProfile,MakerConfig,HedgeConfig,MakerRiskConfig,MakerSymbolConfig)
from core.services.arbitrage_monitor_v2.execution.maker_taker_journal import Journal
from core.services.arbitrage_monitor_v2.execution.maker_taker_engine import (
    MakerTakerEngine,OrderObservation,Book,DefinitiveFailure)
from core.services.arbitrage_monitor_v2.execution.maker_taker_quotes import MarketRules

SYMBOL='SOL-USDC-PERP'


def profile():
    return MakerProfile('maker_taker',MakerConfig(),HedgeConfig(request_timeout=.05),
                        MakerRiskConfig(),{SYMBOL:MakerSymbolConfig(SYMBOL,D(1),D('.03'),D('-.01'),D('.01'),5)})


def observation(row,qty,status='open',venue='arcus',side=None,account='fake',**kwargs):
    return OrderObservation(venue,account,row.get('symbol',SYMBOL),row['client'],row.get('exchange_id') or 'EX-'+row['client'],
                            side or row.get('side',1),D(row['qty']),D(qty),status,'fake',time.monotonic(),**kwargs)


class Fake:
    identity='fake'
    def __init__(self):
        self.sends=[]
        self.cancels=[]
        self.emergencies=[]
        self.results=[]
        self.lookups={}
        self.connected=True
        self.positions={}
        self.equity={'arcus':D(5000),'lighter':D(5000)}

    def healthy(self): return self.connected
    async def maker(self,row): return observation(row,0)
    async def hedge(self,row,obligation):
        self.sends.append(dict(row))
        result=self.results.pop(0) if self.results else ('filled',row['qty'])
        if isinstance(result,BaseException): raise result
        if result=='hang': await asyncio.sleep(10)
        return observation(row,result[1],result[0],'lighter',-obligation['side'])
    async def emergency(self,row,obligation):
        self.emergencies.append((row,obligation))
        return observation(row,row['qty'],'filled','arcus',-obligation['side'])
    async def cancel_maker(self,row):
        self.cancels.append(row['client'])
        return observation(row,row['filled'],'canceled')
    async def lookup_maker(self,row):
        if row['client'] not in self.lookups: raise TimeoutError()
        return self.lookups[row['client']]
    async def lookup_attempt(self,row,obligation):
        if row['client'] not in self.lookups: raise TimeoutError()
        return self.lookups[row['client']]
    async def cancel_legacy(self,known): pass
    async def snapshot(self): return {'authoritative':True,'equity':self.equity,'positions':self.positions}


def engine(tmp_path,config=None):
    journal=Journal(tmp_path/'state.sqlite','fake')
    transport=Fake()
    rules={SYMBOL:MarketRules(D('.000001'),D('.001'),D('.01'),D('.055'),D(5),D(10),D(1000),D('.01'))}
    e=MakerTakerEngine(config or profile(),journal,transport,rules)
    e.equity=transport.equity.copy()
    warm=[time.monotonic()-60]
    e.clock=lambda:warm[0]
    for second in range(61):
        for venue in ('arcus','lighter'):
            e.book(venue,SYMBOL,Book(D(100),D('100.1'),D(10),D(10),warm[0],1))
        if second<60:warm[0]+=1
    e.clock=time.monotonic
    e.state='ACTIVE'
    e._startup_recovered=True
    return e


def maker(e,qty='1',purpose='open',side=1):
    return e.journal.prepare_maker(SYMBOL,side,purpose,D(qty),D(99),0,D(100),D(101))


def test_cumulative_duplicates_reordered_restart_exact_hedge(tmp_path):
    async def scenario():
        e=engine(tmp_path); client=maker(e)
        for qty in ('.2','.5','.5','.3'):
            await e.observe_maker(observation(e.journal.maker(client),qty))
            await e.drain_symbol(SYMBOL)
        assert sum(D(r['qty']) for r in e.transport.sends)==D('.5')
        assert e.journal.inventory(SYMBOL)==(D('.5'),D('-.5'))
        await e.cancel_all()
        e.journal.close()
        e2=engine(tmp_path)
        await e2.observe_maker(observation(e2.journal.maker(client),'.5','canceled'))
        await e2.drain_symbol(SYMBOL)
        assert not e2.transport.sends
        assert e2.journal.inventory(SYMBOL)==(D('.5'),D('-.5'))
    asyncio.run(scenario())


def test_partial_terminal_hedge_retries_only_remainder(tmp_path):
    async def scenario():
        e=engine(tmp_path); client=maker(e)
        e.transport.results=[('canceled','.2'),('filled','.3')]
        await e.observe_maker(observation(e.journal.maker(client),'.5'))
        await e.drain_symbol(SYMBOL)
        assert [D(r['qty']) for r in e.transport.sends]==[D('.5'),D('.3')]
        assert not e.journal.obligations()
    asyncio.run(scenario())


def test_timeout_unknown_no_retry_no_emergency(tmp_path):
    async def scenario():
        e=engine(tmp_path); client=maker(e)
        e.transport.results=[TimeoutError()]
        await e.observe_maker(observation(e.journal.maker(client),'.2'))
        await e.drain_symbol(SYMBOL)
        assert len(e.transport.sends)==1 and not e.transport.emergencies
        assert e.state=='UNKNOWN_ORDER' and e.journal.obligations()
        assert e.transport.cancels==[client]
        await e.drain_symbol(SYMBOL)
        assert len(e.transport.sends)==1
    asyncio.run(scenario())


def test_accepted_timeout_lookup_same_identity(tmp_path):
    async def scenario():
        e=engine(tmp_path); client=maker(e)
        async def hedge(row,obligation):
            e.transport.sends.append(row)
            e.transport.lookups[row['client']]=observation(row,row['qty'],'filled','lighter',-obligation['side'])
            raise TimeoutError()
        e.transport.hedge=hedge
        await e.observe_maker(observation(e.journal.maker(client),'.2'))
        await e.drain_symbol(SYMBOL)
        assert len(e.transport.sends)==1 and not e.journal.obligations()
    asyncio.run(scenario())


@pytest.mark.parametrize('purpose,side',[('open',1),('close',-1)])
def test_bounded_retry_then_preserve_positions_for_manual_intervention(tmp_path,purpose,side):
    async def scenario():
        e=engine(tmp_path)
        if purpose=='close':
            opening=maker(e,qty='.2')
            await e.observe_maker(observation(e.journal.maker(opening),'.2','filled'))
            await e.drain_symbol(SYMBOL)
            e.transport.sends.clear()
        client=maker(e,purpose=purpose,side=side)
        e.transport.results=[DefinitiveFailure()]*4
        await e.observe_maker(observation(e.journal.maker(client),'.2'))
        await e.drain_symbol(SYMBOL)
        assert len(e.transport.sends)==4
        assert not e.transport.emergencies
        assert e.halted and e.manual_intervention_required and e.journal.obligations()
        assert e.journal.inventory(SYMBOL)==((D(0),D('-.2')) if purpose=='close' else (D('.2'),D(0)))
    asyncio.run(scenario())


def test_substep_residual_preserved_not_rounded_up(tmp_path):
    async def scenario():
        e=engine(tmp_path); client=maker(e)
        await e.observe_maker(observation(e.journal.maker(client),'.0005'))
        await e.drain_symbol(SYMBOL)
        assert not e.transport.sends and not e.transport.emergencies
        assert D(e.journal.obligations()[0]['qty'])==D('.0005')
        assert not e.halted and client not in e.transport.cancels
    asyncio.run(scenario())


def test_lighter_taker_below_policy_minimum_is_deferred(tmp_path):
    async def scenario():
        e=engine(tmp_path); client=maker(e)
        await e.observe_maker(observation(e.journal.maker(client),'.02'))
        await e.drain_symbol(SYMBOL)
        assert not e.transport.sends and not e.halted
        assert e.journal.obligations()
    asyncio.run(scenario())


def test_prepared_after_crash_not_resent(tmp_path):
    async def scenario():
        e=engine(tmp_path); client=maker(e)
        assert not await e.recover()
        assert e.halted and not e.transport.sends
    asyncio.run(scenario())


def test_schema_and_identity_fail_closed(tmp_path):
    j=Journal(tmp_path/'s.sqlite','first'); j.close()
    with pytest.raises(ValueError): Journal(tmp_path/'s.sqlite','other')
    db=sqlite3.connect(tmp_path/'s.sqlite'); db.execute('PRAGMA user_version=123'); db.close()
    with pytest.raises(ValueError): Journal(tmp_path/'s.sqlite','first')


def test_journal_transaction_rolls_back_watermark_with_obligation(tmp_path):
    e=engine(tmp_path); client=maker(e)
    e.journal.db.execute("CREATE TRIGGER fail_obligation BEFORE INSERT ON obligations BEGIN SELECT RAISE(ABORT,'crash'); END")
    with pytest.raises(sqlite3.DatabaseError):
        e.journal.observe_maker(client,D('.2'),'open')
    assert D(e.journal.maker(client)['filled'])==0 and not e.journal.obligations()


def test_watchdog_cancels_while_hedge_transport_hangs(tmp_path):
    async def scenario():
        e=engine(tmp_path); client=maker(e)
        await e.observe_maker(observation(e.journal.maker(client),'.2'))
        e.transport.results=['hang']
        task=asyncio.create_task(e.drain_symbol(SYMBOL))
        await asyncio.sleep(.005)
        e.transport.connected=False
        await e.watchdog_once()
        assert client in e.transport.cancels and not task.done()
        await task
    asyncio.run(scenario())


def test_watchdog_cancel_rest_fill_wakes_worker_without_any_ws(tmp_path):
    async def scenario():
        e=engine(tmp_path);client=maker(e)
        e.tasks=[asyncio.create_task(e._worker(SYMBOL))]
        async def cancel(row):return observation(row,'.2','canceled')
        e.transport.cancel_maker=cancel;e.transport.connected=False
        await e.watchdog_once()
        await asyncio.wait_for(e.queues[SYMBOL].join(),.2)
        assert len(e.transport.sends)==1 and D(e.transport.sends[0]['qty'])==D('.2')
        assert not e.journal.obligations()
        await e.cancel_all()
        assert len(e.transport.sends)==1
        for task in e.tasks:task.cancel()
        await asyncio.gather(*e.tasks,return_exceptions=True)
    asyncio.run(scenario())


def test_pending_open_hedge_retains_lighter_capital_across_symbols(tmp_path):
    async def scenario():
        e=engine(tmp_path);client=maker(e)
        await e.observe_maker(observation(e.journal.maker(client),1,'filled'))
        assert e.journal.inventory(SYMBOL)==(D(1),D(0))
        assert e._gross('lighter')>=D('100.1')
        other='ETH-USDC-PERP'
        from dataclasses import replace
        e.profile=replace(e.profile,symbols={**e.profile.symbols,other:replace(e.profile.get_config(SYMBOL),symbol=other)})
        e.equity['lighter']=D(55)
        assert not e._admit(other,1,'open',D(1),D('100.1'))
        await e.drain_symbol(SYMBOL)
        assert e._gross('lighter')==D('100.1')
    asyncio.run(scenario())


def test_concurrent_reconciliation_and_replayed_stale_snapshot_never_double_hedge(tmp_path):
    async def scenario():
        e=engine(tmp_path);e.transport.positions={SYMBOL:[D('.2'),D(0)]}
        async def snapshot():
            await asyncio.sleep(.01)
            return {'authoritative':True,'equity':e.transport.equity,'positions':e.transport.positions}
        e.transport.snapshot=snapshot
        result=await asyncio.gather(e.reconcile(),e.reconcile())
        assert result.count(True)==1 and len(e.transport.sends)==1
        assert e.state=='RECONCILING' and e.journal.pending_reconcile(SYMBOL)
        assert not await e.reconcile()
        assert len(e.transport.sends)==1 and e.halted
    asyncio.run(scenario())


def test_reconcile_snapshot_during_fill_generation_change_is_discarded(tmp_path):
    async def scenario():
        e=engine(tmp_path);client=maker(e)
        entered=asyncio.Event();release=asyncio.Event()
        async def snapshot():
            entered.set();await release.wait()
            return {'authoritative':True,'equity':e.transport.equity,'positions':{SYMBOL:[D(0),D(0)]}}
        e.transport.snapshot=snapshot
        task=asyncio.create_task(e.reconcile())
        await entered.wait()
        e.receive(observation(e.journal.maker(client),'.2'))
        release.set()
        assert not await task
        assert not e.transport.sends and not e.journal.obligations()
    asyncio.run(scenario())


def test_blocked_opposite_cancel_does_not_delay_first_hedge(tmp_path):
    async def scenario():
        e=engine(tmp_path);buy=maker(e);sell=maker(e,side=-1)
        gate=asyncio.Event()
        async def cancel(row):
            await gate.wait()
            return observation(row,row['filled'],'canceled')
        e.transport.cancel_maker=cancel
        e.tasks=[asyncio.create_task(e._worker(SYMBOL))]
        e.receive(observation(e.journal.maker(buy),'.2'))
        await asyncio.sleep(.005)
        assert len(e.transport.sends)==1 and e.cancel_tasks
        assert D(e.transport.sends[0]['qty'])==D('.2')
        gate.set()
        await asyncio.gather(*list(e.cancel_tasks),return_exceptions=True)
        for task in e.tasks:task.cancel()
        await asyncio.gather(*e.tasks,return_exceptions=True)
    asyncio.run(scenario())


def test_fill_before_maker_ack_is_not_blocked_by_quote_lock(tmp_path):
    async def scenario():
        e=engine(tmp_path);gate=asyncio.Event();sent=asyncio.Event()
        async def send(row):
            e.receive(observation(row,'.2'))
            sent.set();await gate.wait()
            return observation(row,'.2')
        e.transport.maker=send
        e.tasks=[asyncio.create_task(e._worker(SYMBOL))]
        quote=asyncio.create_task(e.quote_symbol(SYMBOL))
        await sent.wait();await asyncio.sleep(.005)
        assert len(e.transport.sends)==1 and not quote.done()
        gate.set();await quote
        for q in e.queues.values():await q.join()
        for task in e.tasks:task.cancel()
        await asyncio.gather(*e.tasks,return_exceptions=True)
    asyncio.run(scenario())


def test_hedge_confirmation_during_snapshot_changes_generation(tmp_path):
    async def scenario():
        e=engine(tmp_path);client=maker(e)
        await e.observe_maker(observation(e.journal.maker(client),'.2'))
        obligation=e.journal.obligations()[0]
        attempt_client=e.journal.prepare_attempt(obligation['id'],D('.2'))
        attempt=e.journal.attempts()[0]
        entered=asyncio.Event();release=asyncio.Event()
        async def snapshot():
            entered.set();await release.wait()
            return {'authoritative':True,'equity':e.transport.equity,'positions':{SYMBOL:[D('.2'),D(0)]}}
        e.transport.snapshot=snapshot
        task=asyncio.create_task(e.reconcile());await entered.wait()
        await e._attempt_observation(attempt,obligation,observation(attempt,'.2','filled','lighter',-1))
        release.set()
        assert not await task and not e.transport.sends
        assert e.journal.inventory(SYMBOL)==(D('.2'),D('-.2'))
    asyncio.run(scenario())


@pytest.mark.parametrize('venue',['arcus','lighter'])
def test_correction_checks_actual_both_venue_cap_before_send(tmp_path,venue):
    async def scenario():
        e=engine(tmp_path);e.transport.positions={SYMBOL:[D('.1'),D(0)]}
        e.transport.equity[venue]=D(1)
        assert not await e.reconcile()
        assert e.halted and not e.transport.sends and not e.transport.emergencies
        assert e.journal.inventory(SYMBOL)==(D('.1'),D(0))
        assert e.journal.obligations() and e.journal.pending_reconcile(SYMBOL)
        assert any('capital cap' in r['payload'] for r in e.journal.db.execute('SELECT payload FROM events'))
    asyncio.run(scenario())


@pytest.mark.parametrize('fall',[False,True])
def test_correction_partial_retry_refreshes_cap_and_preserves_remainder(tmp_path,fall):
    async def scenario():
        e=engine(tmp_path);e.transport.positions={SYMBOL:[D('.3'),D(0)]}
        async def hedge(row,obligation):
            e.transport.sends.append(dict(row))
            first=len(e.transport.sends)==1
            filled=D('.04') if first else D(row['qty'])
            e.transport.positions[SYMBOL][1]-=filled
            if first and fall:e.transport.equity['lighter']=D(1)
            return observation(row,filled,'canceled' if first else 'filled','lighter',-obligation['side'])
        e.transport.hedge=hedge
        result=await e.reconcile()
        if fall:
            assert not result and e.halted and len(e.transport.sends)==1
            pending=e.journal.obligations()[0]
            assert D(pending['qty'])-D(pending['hedged'])==D('.26')
        else:
            assert result and not e.halted
            assert [D(r['qty']) for r in e.transport.sends]==[D('.3'),D('.26')]
            assert not e.journal.obligations()
            assert await e.reconcile() and e.state=='ACTIVE'
    asyncio.run(scenario())


def test_correction_position_reduction_is_not_double_reserved(tmp_path):
    async def scenario():
        e=engine(tmp_path);e.transport.positions={SYMBOL:[D('.2'),D('-.4')]}
        e.transport.equity={'arcus':D(12),'lighter':D(12)}
        assert await e.reconcile() and not e.halted
        assert len(e.transport.sends)==1 and D(e.transport.sends[0]['qty'])==D('.2')
        assert e.journal.inventory(SYMBOL)==(D('.2'),D('-.2'))
    asyncio.run(scenario())


@pytest.mark.parametrize('equity,allowed',[('1',False),('16',True)])
@pytest.mark.parametrize('entry',['drain','recover'])
def test_persisted_correction_restart_cannot_bypass_cap(tmp_path,equity,allowed,entry):
    async def scenario():
        old=engine(tmp_path)
        old.journal.reconcile_inventory(SYMBOL,D('.3'),D(0),D(100))
        oid=old.journal.obligations()[0]['id']
        attempt=old.journal.prepare_attempt(oid,D('.3'))
        old.journal.observe_attempt(attempt,D('.04'),'canceled')
        old.journal.close()
        e=engine(tmp_path);e.transport.positions={SYMBOL:[D('.3'),D('-.04')]}
        e.transport.equity['lighter']=D(equity)
        async def hedge(row,obligation):
            e.transport.sends.append(dict(row));e.transport.positions[SYMBOL][1]-=D(row['qty'])
            return observation(row,row['qty'],'filled','lighter',-obligation['side'])
        e.transport.hedge=hedge
        if entry=='recover':assert await e.recover()==allowed
        else:await e.drain_symbol(SYMBOL)
        assert bool(e.transport.sends)==allowed
        assert bool(e.journal.obligations())!=allowed
        assert e.halted!=allowed
        if allowed:assert D(e.transport.sends[0]['qty'])==D('.26')
    asyncio.run(scenario())


@pytest.mark.parametrize('reservation',['maker','obligation','unknown','position'])
def test_correction_includes_cross_symbol_pending_reservations(tmp_path,reservation):
    async def scenario():
        other='ETH-USDC-PERP'
        cfg=profile();cfg=replace(cfg,symbols={**cfg.symbols,other:replace(cfg.symbols[SYMBOL],symbol=other)})
        e=engine(tmp_path,cfg);e.rules[other]=e.rules[SYMBOL]
        for venue in ('arcus','lighter'):
            e.book(venue,other,Book(D(100),D('100.1'),D(10),D(10),time.monotonic(),1))
        client=None if reservation=='position' else e.journal.prepare_maker(other,1,'open',D('.1'),D(99),0,D(100),D(101))
        if reservation in ('obligation','unknown'):
            oid=e.journal.observe_maker(client,D('.1'),'filled')
            if reservation=='unknown':
                attempt=e.journal.prepare_attempt(oid,D('.1'));e.journal.attempt_state(attempt,'unknown')
        e.journal.reconcile_inventory(SYMBOL,D('.1'),D(0),D(100))
        e.transport.positions={SYMBOL:[D('.1'),D(0)],other:[D('.1') if reservation in ('obligation','unknown') else D(0),
                                                                       D('-.1') if reservation=='position' else D(0)]}
        e.transport.equity['lighter']=D(8) # One $10 correction fits; two independent $10 reservations do not.
        await e.drain_symbol(SYMBOL)
        assert e.halted and not e.transport.sends and e.journal.obligations(SYMBOL)
    asyncio.run(scenario())
