"""Paper execution depth, confirmed-failure budgets and durable halt diagnostics."""
import asyncio
import json
from decimal import Decimal as D
from pathlib import Path

from test_maker_taker_engine import engine,maker,observation,SYMBOL
from core.services.arbitrage_monitor_v2.execution.maker_taker_paper import PublicMarkets,PaperTransport


def paper(e):
    data=json.loads(Path('tests/fixtures/maker_taker_replay.json').read_text())
    source=PublicMarkets([SYMBOL]);source.load_metadata(data['arcus_markets'],data['lighter_markets'])
    source.connected={'arcus':True,'lighter':True};source.generations={'arcus':1,'lighter':1}
    return PaperTransport(source,e.journal,hedge_latency_ms=0)


def test_paper_hedge_uses_all_eligible_levels_for_sol_1_4(tmp_path):
    async def scenario():
        e=engine(tmp_path);transport=paper(e)
        transport.source._publish('lighter',SYMBOL,[['100','.392'],['99.95','1.008']],[['100.1','10']])
        obs=await transport.hedge({'client':'1','qty':'1.4'},{'symbol':SYMBOL,'side':1})
        assert obs.cumulative==D('1.4') and obs.status=='filled'
        assert obs.average==(D(100)*D('.392')+D('99.95')*D('1.008'))/D('1.4')
    asyncio.run(scenario())


def test_more_than_four_positive_partial_attempts_do_not_exhaust_failure_budget(tmp_path):
    async def scenario():
        e=engine(tmp_path);client=maker(e)
        e.transport.results=[('canceled','.1')]*5+[('filled','.5')]
        await e.observe_maker(observation(e.journal.maker(client),1,'filled'))
        await e.drain_symbol(SYMBOL)
        assert not e.halted and not e.transport.emergencies
        assert len(e.transport.sends)==6 and not e.journal.obligations()
        assert e.journal.inventory(SYMBOL)==(D(1),D(-1))
    asyncio.run(scenario())


def test_halt_root_cause_survives_later_latency_alarm(tmp_path):
    async def scenario():
        e=engine(tmp_path);client=maker(e)
        e.transport.results=[('rejected',0)]*4
        await e.observe_maker(observation(e.journal.maker(client),1,'filled'))
        await e.drain_symbol(SYMBOL)
        assert e.halted and 'zero-fill' in e.halt_reason
        reason=e.halt_reason
        e.alarm('Hedge confirmation exceeded 500ms target',SYMBOL)
        assert e.halt_reason==reason and e.reason==reason
        rows=[json.loads(r[0]) for r in e.journal.db.execute("SELECT payload FROM events WHERE kind='halt'")]
        assert rows and rows[0]['reason']==reason
    asyncio.run(scenario())


import time
from collections import deque
from dataclasses import replace
from decimal import ROUND_CEILING,ROUND_FLOOR
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock,Mock

import pytest
from test_maker_taker_engine import profile
from core.services.arbitrage_monitor_v2.execution.maker_taker_engine import Book,MakerTakerEngine
from core.services.arbitrage_monitor_v2.execution.maker_taker_quotes import lighter_ioc_price


@pytest.mark.parametrize('side',[-1,1])
def test_native_bound_matches_previous_live_encoding(side):
    for decimals in (0,1,3,8):
        for i in range(20):
            bid=D('99.001')+D(i)/1000;ask=bid+D('.01');scale=D(10)**decimals
            previous=(ask*D('1.001') if side==1 else bid*D('.999'))*scale
            previous=int(previous.to_integral_value(rounding=ROUND_CEILING if side==1 else ROUND_FLOOR))
            assert lighter_ioc_price(side,bid,ask,decimals)==previous


@pytest.mark.parametrize('side',[-1,1])
def test_live_native_payload_preserves_price_precision_and_flags(tmp_path,side):
    from core.services.arbitrage_monitor_v2.execution.maker_taker_live import LiveTransport
    async def scenario():
        e=engine(tmp_path);transport=LiveTransport.__new__(LiveTransport)
        book=Book(D('99.991'),D('100.001'),D(10),D(10),time.monotonic(),1)
        market={'market_id':2,'supported_price_decimals':3,'supported_size_decimals':3}
        transport.source=NS(markets={SYMBOL:({},market)},rules=e.rules,books={('lighter',SYMBOL):book},
                            book_validity=lambda *args:(True,None,None))
        rest=NS(signer_client=NS(sign_create_order=object()),_register_order_intent=Mock(),_strict_submit=AsyncMock())
        transport.lighter=NS(_rest=rest,_normalize_symbol=lambda s:s);transport.writes=deque()
        transport._terminal_lighter=AsyncMock(return_value='offline terminal')
        attempt={'client':'7','qty':'.2'};obligation={'symbol':SYMBOL,'side':-side,'purpose':'close'}
        assert await transport.hedge(attempt,obligation)=='offline terminal'
        native=rest._strict_submit.await_args.args[1]
        scale=D(1000);previous=(book.ask*D('1.001') if side==1 else book.bid*D('.999'))*scale
        assert native['price']==int(previous.to_integral_value(rounding=ROUND_CEILING if side==1 else ROUND_FLOOR))
        assert native['base_amount']==200 and native['is_ask']==(side==-1) and native['reduce_only'] is True
        assert native['client_order_index']==7 and native['market_index']==2
    asyncio.run(scenario())


@pytest.mark.parametrize('side',[-1,1])
def test_depth_bound_lot_vwap_snapshot_consumption_and_refresh(tmp_path,side):
    async def scenario():
        e=engine(tmp_path);transport=paper(e);source=transport.source
        prices=(['100.001','100.050','100.102','100.103'] if side==1 else ['100.001','99.950','99.900','99.899'])
        levels=[[p,q] for p,q in zip(prices,['.3925','.4004','.6004','10'])]
        bids=[['99.991','10']] if side==1 else levels
        asks=levels if side==1 else [['100.011','10']]
        source._publish('lighter',SYMBOL,bids,asks)
        snapshot=source.books['lighter',SYMBOL]
        assert snapshot.bids and snapshot.asks and isinstance(snapshot.bids,tuple)
        first=await transport.hedge({'client':'1','qty':'1.5'},{'symbol':SYMBOL,'side':-side})
        assert first.cumulative==D('1.393') and first.status=='canceled'
        cost=D(prices[0])*D('.3925')+D(prices[1])*D('.4004')+D(prices[2])*D('.6001')
        assert first.average==cost/D('1.393')
        second=await transport.hedge({'client':'2','qty':'.1'},{'symbol':SYMBOL,'side':-side})
        assert second.cumulative==0 and second.average is None
        assert source.books['lighter',SYMBOL] is snapshot
        assert snapshot.bids==(tuple((D(p),D(q)) for p,q in bids))
        source._publish('lighter',SYMBOL,bids,asks)
        assert source.books['lighter',SYMBOL] is not snapshot
        third=await transport.hedge({'client':'3','qty':'.1'},{'symbol':SYMBOL,'side':-side})
        assert third.cumulative==D('.1') and third.status=='filled' and third.average==D(prices[0])
        assert transport.positions[SYMBOL][1]==side*D('1.493')
    asyncio.run(scenario())


def test_bbo_only_fixture_and_concurrent_attempts_cannot_reuse_liquidity(tmp_path):
    async def scenario():
        e=engine(tmp_path);transport=paper(e);source=transport.source
        source.books['lighter',SYMBOL]=Book(D(100),D('100.1'),D('.392'),D(10),source.clock(),1)
        source.received('lighter',1)
        observations=await asyncio.gather(*(transport.hedge({'client':str(i),'qty':'1.4'},{'symbol':SYMBOL,'side':1}) for i in range(2)))
        assert sorted(o.cumulative for o in observations)==[D(0),D('.392')]
        assert transport.positions[SYMBOL][1]==D('-.392')
    asyncio.run(scenario())


def test_book_depth_is_immutable_and_validated():
    bids=[[D(100),D(1)],[D(99),D(2)]]
    book=Book(D(100),D(101),D(1),D(3),0,1,bids,[[D(101),D(3)]])
    bids[0][1]=D(1000)
    assert book.bids==((D(100),D(1)),(D(99),D(2)))
    with pytest.raises(ValueError):Book(D(100),D(101),D(1),D(3),0,1,[(D(99),D(1))])
    with pytest.raises(ValueError):Book(D(100),D(101),D(1),D(3),0,1,[(D(100),D(1)),(D(100),D(2))])


@pytest.mark.parametrize('retries',[0,1,3])
def test_zero_fill_failure_limit_is_initial_plus_configured_retries(tmp_path,retries):
    async def scenario():
        cfg=profile();e=engine(tmp_path,replace(cfg,hedge=replace(cfg.hedge,max_retries=retries)))
        client=maker(e);e.transport.results=[('canceled',0)]*(retries+1)+[('filled',1)]
        await e.observe_maker(observation(e.journal.maker(client),1,'filled'));await e.drain_symbol(SYMBOL)
        assert e.halted and len(e.transport.sends)==retries+1 and not e.transport.emergencies
        assert e.manual_intervention_required and e.journal.inventory(SYMBOL)==(D(1),D(0))
        assert 'zero-fill' in e.halt_reason
    asyncio.run(scenario())


def test_positive_partials_do_not_clear_previous_zero_failures(tmp_path):
    async def scenario():
        e=engine(tmp_path);client=maker(e)
        e.transport.results=[('canceled',0),('canceled','.1'),('canceled',0),('canceled','.1'),
                             ('canceled','.1'),('canceled',0),('canceled','.1'),('canceled',0)]
        await e.observe_maker(observation(e.journal.maker(client),1,'filled'));await e.drain_symbol(SYMBOL)
        oid=e.journal.obligations(pending=False)[0]['id']
        assert len(e.zero_fill_failures(oid))==4 and len(e.transport.sends)==8
        assert e.halted and not e.transport.emergencies and e.manual_intervention_required
        assert D(e.manual_intervention_snapshot['unresolved_qty'])==D('.6')
        assert e.journal.inventory(SYMBOL)==(D(1),D('-.4'))
    asyncio.run(scenario())


def test_shared_attempt_uses_total_fill_not_zero_filled_fifo_tail(tmp_path):
    async def scenario():
        e=engine(tmp_path)
        ids=[e.journal.observe_maker(maker(e,'.5'),D('.5'),'filled') for _ in range(2)]
        client=e.journal.prepare_attempt(ids[0],D(1),obligations=ids)
        e.journal.observe_attempt(client,D('.5'),'canceled')
        assert e.journal.allocations(client)[1]['filled']=='0'
        assert not e.zero_fill_failures(ids[0]) and not e.zero_fill_failures(ids[1])
        await e.drain_symbol(SYMBOL)
        assert not e.halted and not e.journal.obligations() and len(e.transport.sends)==1
    asyncio.run(scenario())


def test_zero_failure_budget_survives_restart_and_positive_progress(tmp_path):
    async def scenario():
        e=engine(tmp_path);oid=e.journal.observe_maker(maker(e),D(1),'filled')
        for qty,fill in [('1','0'),('1','.1'),('.9','0')]:
            client=e.journal.prepare_attempt(oid,D(qty));e.journal.observe_attempt(client,D(fill),'canceled')
        e.journal.close();e=engine(tmp_path)
        assert len(e.zero_fill_failures(oid))==2
        e.transport.results=[('canceled',0),('canceled','.1'),('canceled',0)]
        await e.drain_symbol(SYMBOL)
        assert e.halted and len(e.zero_fill_failures(oid))==4 and len(e.transport.sends)==3
    asyncio.run(scenario())


def test_archive_restore_keeps_old_failure_history_without_double_counting(tmp_path):
    async def scenario():
        e=engine(tmp_path);client=maker(e,'2');oid=e.journal.observe_maker(client,D(1),'canceled')
        for _ in range(3):
            attempt=e.journal.prepare_attempt(oid,D(1));e.journal.observe_attempt(attempt,D(0),'canceled')
        attempt=e.journal.prepare_attempt(oid,D(1));e.journal.observe_attempt(attempt,D(1),'filled')
        assert e.journal.archive_closed()==1 and len(e.zero_fill_failures(oid))==3
        before=e.journal.inventory(SYMBOL);new=e.journal.observe_maker(client,D('1.2'),'canceled')
        assert e.journal.inventory(SYMBOL)==(before[0]+D('.2'),before[1])
        assert len(e.zero_fill_failures(oid))==3 and not e.zero_fill_failures(new)
        await e.drain_symbol(SYMBOL)
        assert not e.halted and len(e.transport.sends)==1 and not e.journal.obligations()
    asyncio.run(scenario())


def test_unknown_positive_attempt_still_blocks_retry_and_emergency(tmp_path):
    async def scenario():
        e=engine(tmp_path);oid=e.journal.observe_maker(maker(e),D(1),'filled')
        client=e.journal.prepare_attempt(oid,D(1));e.journal.observe_attempt(client,D('.2'),'open')
        assert e.residual_state(e.residual_buckets()[0])[0]=='uncertain_write'
        await e.drain_symbol(SYMBOL)
        assert e.halted and e.state=='UNKNOWN_ORDER' and not e.transport.sends and not e.transport.emergencies
        assert not e.zero_fill_failures(oid) and 'Unresolved hedge' in e.halt_reason
    asyncio.run(scenario())


def test_prior_arcus_emergency_is_not_mislabeled_zero_failure_exhaustion(tmp_path):
    async def scenario():
        e=engine(tmp_path);oid=e.journal.observe_maker(maker(e),D(1),'filled')
        attempt=e.journal.prepare_attempt(oid,D(1),'arcus');e.journal.observe_attempt(attempt,D(0),'rejected')
        await e.drain_symbol(SYMBOL)
        assert e.halted and 'Prior Arcus emergency' in e.halt_reason and not e.zero_fill_failures(oid)
        assert not e.transport.sends and not e.transport.emergencies
    asyncio.run(scenario())


def test_first_halt_is_durable_before_cancellation_and_emergency_warnings(tmp_path):
    async def scenario():
        e=engine(tmp_path);oid=e.journal.observe_maker(maker(e),D(1),'filled');maker(e)
        async def failed_cancel(row):
            assert e.halt_reason and e.journal.db.execute("SELECT 1 FROM events WHERE kind='halt'").fetchone()
            raise TimeoutError()
        e.transport.cancel_maker=failed_cancel;e.transport.results=[('rejected',0)]*4
        await e.drain_symbol(SYMBOL)
        reason=e.halt_reason
        assert 'zero-fill' in reason and e.cancel_failed and e.halt_state=='EMERGENCY'
        assert e.halt_symbol==SYMBOL and e.reason==reason and 'cancellation unresolved' in e.last_alarm
        e.alarm('Hedge confirmation exceeded 500ms target',SYMBOL)
        assert e.reason==reason and e.halt_reason==reason
        assert e.journal.db.execute("SELECT COUNT(*) FROM events WHERE kind='halt'").fetchone()[0]==1
        boot=e.journal.boot;e.journal.close();e=engine(tmp_path)
        assert e.halted and e.manual_intervention_required and e.halt_reason==reason
        payload=json.loads(e.journal.db.execute("SELECT payload FROM events WHERE kind='halt'").fetchone()[0])
        assert payload['boot']==boot and payload['reason']==reason and payload['state']=='EMERGENCY'
    asyncio.run(scenario())


@pytest.mark.parametrize('raw_failure',[False,True])
def test_runner_prints_first_halt_in_runtime_and_final_json(tmp_path,monkeypatch,capsys,raw_failure):
    import run_maker_taker
    data=json.loads(Path('tests/fixtures/maker_taker_replay.json').read_text())
    class OfflineMarkets(PublicMarkets):
        async def metadata(self):
            self.load_metadata(data['arcus_markets'],data['lighter_markets'])
            self.connected={'arcus':True,'lighter':True};self.generations={'arcus':1,'lighter':1};return self.rules
        async def start(self,callback):
            self.callback=callback
            for frame in data['initial_books']:self._publish(frame['venue'],frame['symbol'],frame['bids'],frame['asks'])
    async def halt_tick(self):
        if raw_failure:
            async def bad_snapshot():raise RuntimeError('SYNTHETIC_PRIVATE_PAYLOAD_DO_NOT_LOG')
            self.transport.snapshot=bad_snapshot
            await self.reconcile()
        else:self.halt('EMERGENCY','Confirmed zero-fill test root',SYMBOL)
        self.alarm('Hedge confirmation exceeded 500ms target',SYMBOL)
    monkeypatch.setattr('core.services.arbitrage_monitor_v2.execution.maker_taker_paper.PublicMarkets',OfflineMarkets)
    monkeypatch.setattr(MakerTakerEngine,'tick',halt_tick)
    monkeypatch.setattr('core.services.arbitrage_monitor_v2.history.maker_taker_statistics.statistics_background',AsyncMock(return_value=[]))
    assert run_maker_taker.main(['--mode','monitor','--duration-seconds','.1','--state-dir',str(tmp_path/'run')])==1
    lines=[json.loads(line) for line in capsys.readouterr().out.splitlines() if line.startswith('{')]
    halted=[row for row in lines if row.get('halt_reason')]
    assert len(halted)==2
    expected='Reconciliation failed' if raw_failure else 'Confirmed zero-fill test root'
    assert all(row['halt_reason']==row['reason']==expected for row in halted)
    assert all(row['halt_state']==('UNKNOWN_ORDER' if raw_failure else 'EMERGENCY') for row in halted)
    assert all(row['halt_error_type']==('RuntimeError' if raw_failure else '') for row in halted)
    assert 'SYNTHETIC_PRIVATE_PAYLOAD_DO_NOT_LOG' not in json.dumps(lines)
    assert halted[-1]['state']=='STOPPING' and '500ms' in halted[0]['last_alarm']
    assert halted[-1]['last_alarm']==('Reconciliation failed' if raw_failure else halted[0]['last_alarm'])


@pytest.mark.parametrize('path',['worker','reconcile','correction'])
def test_halt_paths_never_persist_raw_exception_payload(tmp_path,path):
    marker='Authorization=Bearer SYNTHETIC_PRIVATE_PAYLOAD_DO_NOT_LOG'
    async def scenario():
        e=engine(tmp_path)
        async def fail(*args,**kwargs):raise RuntimeError(marker)
        if path=='worker':
            client=maker(e);e.observe_maker=fail
            worker=asyncio.create_task(e._worker(SYMBOL))
            e.receive(observation(e.journal.maker(client),0))
            await e.queues[SYMBOL].join();worker.cancel()
            await asyncio.gather(worker,return_exceptions=True)
        elif path=='reconcile':
            e.transport.snapshot=fail;assert not await e.reconcile()
        else:
            e.journal.reconcile_inventory(SYMBOL,D('.2'),D(0),D(100))
            e.transport.snapshot=fail
            assert not await e.correction_admitted(e.journal.obligations()[0],D('.2'))
        assert e.halted and e.halt_error_type=='RuntimeError'
        assert marker not in json.dumps({'reason':e.reason,'halt_reason':e.halt_reason,'last_alarm':e.last_alarm})
        payloads=[r[0] for r in e.journal.db.execute('SELECT payload FROM events')]
        assert payloads and all(marker not in payload for payload in payloads)
        first=json.loads(e.journal.db.execute("SELECT payload FROM events WHERE kind='halt'").fetchone()[0])
        assert first['error_type']=='RuntimeError'
    asyncio.run(scenario())


def test_dust_deferred_and_new_fill_do_not_reset_old_zero_failure_count(tmp_path):
    async def scenario():
        e=engine(tmp_path);old=e.journal.observe_maker(maker(e,'.04'),D('.04'),'filled')
        for _ in range(3):
            client=e.journal.prepare_attempt(old,D('.04'));e.journal.observe_attempt(client,D(0),'canceled')
        client=e.journal.prepare_attempt(old,D('.04'));e.journal.observe_attempt(client,D(0),'deferred')
        await e.drain_symbol(SYMBOL)
        assert len(e.zero_fill_failures(old))==3 and not e.halted and not e.transport.sends
        new=e.journal.observe_maker(maker(e,'.08'),D('.08'),'filled')
        e.transport.results=[('canceled',0)]
        await e.drain_symbol(SYMBOL)
        assert e.halted and len(e.transport.sends)==1
        assert len(e.zero_fill_failures(old))==4 and len(e.zero_fill_failures(new))==1
    asyncio.run(scenario())
