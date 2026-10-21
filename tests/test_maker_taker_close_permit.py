"""Prepaid close replacement boundaries, using only fake transport and clocks."""
import asyncio
from dataclasses import replace
from decimal import Decimal as D

import pytest

from test_maker_taker_close_floor import close_engine,opened,flush_acks
from core.services.arbitrage_monitor_v2.execution.maker_taker_engine import Book

SYMBOL='SOL-USDC-PERP'
OTHER='ETH-USDC-PERP'


async def blocked_close(tmp_path,*,side=-1,own_ack=False):
    e=close_engine(tmp_path);opened(e.journal,-side,'1','100','98' if side==-1 else '102')
    first=await e.place(SYMBOL,side,'close',D(1),D(0),1);await flush_acks(e)
    now=[e.clock()];e.clock=lambda:now[0];e.requotes[SYMBOL].append(now[0]-.2)
    e.queues[OTHER]=asyncio.Queue()
    original=e.transport.cancel_maker
    async def cancel(row):
        result=await original(row)
        if own_ack:e.queues[SYMBOL].put_nowait(result)
        else:e.queues[OTHER].put_nowait(None)
        return result
    e.transport.cancel_maker=cancel
    if own_ack:e.profile=replace(e.profile,symbols={SYMBOL:replace(e.profile.get_config(SYMBOL),max_segments=2)})
    e.book('lighter',SYMBOL,Book(D(110) if side==-1 else D(90),D('110.1') if side==-1 else D('90.1'),
                               D(10),D(10),now[0],1))
    await e.quote_symbol(SYMBOL)
    assert e.journal.maker(first)['status']=='canceled'
    assert not [r for r in e.journal.makers(SYMBOL,active=True) if r['purpose']=='close']
    return e,now,first


async def release_ack(e,own_ack=False):
    if own_ack:await flush_acks(e)
    else:e.queues[OTHER].get_nowait();e.queues[OTHER].task_done()


@pytest.mark.parametrize('side',[-1,1])
@pytest.mark.parametrize('own_ack',[False,True])
def test_prepaid_close_replaces_after_ack_queue_clears_without_second_quota(tmp_path,side,own_ack):
    async def scenario():
        e,now,first=await blocked_close(tmp_path,side=side,own_ack=own_ack)
        original=list(e.requotes[SYMBOL]);await release_ack(e,own_ack);now[0]+=.05
        await e.quote_symbol(SYMBOL)
        closes=[r for r in e.journal.makers(SYMBOL,active=True) if r['purpose']=='close']
        assert len(closes)==1 and closes[0]['client']!=first
        assert list(e.requotes[SYMBOL])==original
    asyncio.run(scenario())


@pytest.mark.parametrize('elapsed,valid',[(.999,True),(1,False),(1.001,False)])
def test_permit_uses_original_one_second_expiry_without_refresh(tmp_path,elapsed,valid):
    async def scenario():
        e,now,_=await blocked_close(tmp_path);permit=e.close_permits[SYMBOL,-1].copy()
        now[0]+=elapsed
        assert e.close_permit_valid(SYMBOL,-1,D(1)) is valid
        if valid:assert e.close_permits[SYMBOL,-1]==permit
        else:assert not e.close_permits
    asyncio.run(scenario())


def test_missing_original_token_cannot_borrow_two_other_live_tokens(tmp_path):
    async def scenario():
        e,now,_=await blocked_close(tmp_path);await release_ack(e);now[0]+=.05
        e.requotes[SYMBOL].clear();e.requotes[SYMBOL].extend([now[0]-.02,now[0]-.01])
        await e.quote_symbol(SYMBOL)
        assert not e.close_permits and not e.journal.makers(SYMBOL,active=True)
        assert len(e.requotes[SYMBOL])==2
    asyncio.run(scenario())


@pytest.mark.parametrize('change',['inventory','qty','filled','public_generation','private_recovery','halt','not_ready','reconcile','stop'])
def test_permit_invalidates_on_bound_risk_identity_or_state_change(tmp_path,change):
    async def scenario():
        e,now,first=await blocked_close(tmp_path)
        if change=='inventory':e.journal._inventory_delta(SYMBOL,D('.1'))
        elif change=='qty':e.profile=replace(e.profile,symbols={SYMBOL:replace(e.profile.get_config(SYMBOL),quantity=D('.5'))})
        elif change=='filled':e.journal.observe_maker(first,D('.1'),'canceled',average=D(110))
        elif change=='public_generation':
            book=e.books['lighter',SYMBOL]
            e.book('lighter',SYMBOL,replace(book,generation=2))
        elif change=='private_recovery':e.transport.private_episodes=lambda:((2,123,1),)
        elif change=='halt':e.halt('EMERGENCY','test risk stop')
        elif change=='not_ready':e.transport.connected=False
        elif change=='reconcile':
            await release_ack(e);await e.reconcile()
        else:await e.stop(timeout=.01)
        assert not e.close_permit_valid(SYMBOL,-1,D(1)) and not e.close_permits
    asyncio.run(scenario())


def test_many_preprepare_rejections_do_not_grow_renew_or_refund_permit(tmp_path):
    async def scenario():
        e,now,_=await blocked_close(tmp_path);await release_ack(e)
        original=e.close_permits[SYMBOL,-1].copy();tokens=list(e.requotes[SYMBOL])
        e._admit=lambda *args:False
        for _ in range(100):
            now[0]+=.001;await e.quote_symbol(SYMBOL)
            assert e.close_permits=={(SYMBOL,-1):original}
            assert list(e.requotes[SYMBOL])==tokens
        now[0]=original['token']+1
        assert not e.close_permit_valid(SYMBOL,-1,D(1))
    asyncio.run(scenario())


@pytest.mark.parametrize('outcome',['rejected','unknown','prepare_exception'])
def test_prepare_attempt_consumes_permit_forever_even_if_send_fails(tmp_path,outcome):
    from core.services.arbitrage_monitor_v2.execution.maker_taker_engine import DefinitiveFailure
    async def scenario():
        e,now,_=await blocked_close(tmp_path);await release_ack(e);now[0]+=.05
        if outcome=='prepare_exception':
            def prepare(*args,**kwargs):raise RuntimeError('prepare failed')
            e.journal.prepare_maker=prepare
            with pytest.raises(RuntimeError,match='prepare failed'):await e.quote_symbol(SYMBOL)
        else:
            async def maker(row):
                if outcome=='rejected':raise DefinitiveFailure()
                raise TimeoutError()
            e.transport.maker=maker
            await e.quote_symbol(SYMBOL)
            closes=[r for r in e.journal.makers(SYMBOL) if r['purpose']=='close']
            if outcome=='unknown':assert e.halted and e.halt_state=='UNKNOWN_ORDER'
            else:assert closes[-1]['status']==outcome
        assert not e.close_permits
        assert e.close_prepare_versions[SYMBOL,-1]==2
        assert len(e.requotes[SYMBOL])==2
    asyncio.run(scenario())


def test_permit_is_process_local_after_restart(tmp_path):
    from core.services.arbitrage_monitor_v2.execution.maker_taker_engine import MakerTakerEngine
    async def scenario():
        e,now,_=await blocked_close(tmp_path)
        restarted=MakerTakerEngine(e.profile,e.journal,e.transport,e.rules,clock=lambda:now[0])
        assert e.close_permits and not restarted.close_permits
        assert not restarted.close_permit_valid(SYMBOL,-1,D(1))
    asyncio.run(scenario())


@pytest.mark.parametrize('cancel_result',['unknown','nonterminal','filled','partial'])
def test_unsuccessful_or_new_fill_cancel_never_mints_permit(tmp_path,cancel_result):
    from test_maker_taker_engine import observation
    async def scenario():
        e=close_engine(tmp_path);opened(e.journal,1,'1','100','98')
        first=await e.place(SYMBOL,-1,'close',D(1),D(0),1);await flush_acks(e)
        e.requotes[SYMBOL].append(e.clock())
        async def cancel(row):
            if cancel_result=='unknown':raise TimeoutError()
            return observation(row,'1' if cancel_result=='filled' else '.1' if cancel_result=='partial' else '0',
                               'filled' if cancel_result=='filled' else 'canceled' if cancel_result=='partial' else 'open',average=D(110))
        e.transport.cancel_maker=cancel
        e.book('lighter',SYMBOL,Book(D(110),D('110.1'),D(10),D(10),e.clock(),1))
        await e.quote_symbol(SYMBOL)
        assert not e.close_permits
        assert not [r for r in e.journal.makers(SYMBOL,active=True) if r['client']!=first and r['purpose']=='close']
    asyncio.run(scenario())


def test_unreserved_unsafe_cancel_cannot_create_future_quota_exemption(tmp_path):
    async def scenario():
        e=close_engine(tmp_path);opened(e.journal,1,'1','100','98')
        first=await e.place(SYMBOL,-1,'close',D(1),D(0),1);await flush_acks(e)
        now=[e.clock()];e.clock=lambda:now[0];e.requotes[SYMBOL].extend([now[0]]*2)
        e.book('lighter',SYMBOL,Book(D(110),D('110.1'),D(10),D(10),now[0],1))
        await e.quote_symbol(SYMBOL);now[0]+=.05;await e.quote_symbol(SYMBOL)
        assert first in e.transport.cancels and not e.close_permits
        assert not e.journal.makers(SYMBOL,active=True) and len(e.requotes[SYMBOL])==2
    asyncio.run(scenario())


def test_unexpected_preprepare_exception_does_not_preserve_permit(tmp_path):
    async def scenario():
        e,now,_=await blocked_close(tmp_path);await release_ack(e)
        original=e.place
        async def failed(symbol,side,purpose,*args):
            if purpose=='close':raise ValueError('preprepare failure')
            return await original(symbol,side,purpose,*args)
        e.place=failed
        with pytest.raises(ValueError,match='preprepare failure'):await e.quote_symbol(SYMBOL)
        assert not e.close_permits
    asyncio.run(scenario())


@pytest.mark.parametrize('side',[-1,1])
def test_two_symbols_and_directions_cannot_consume_each_others_permit(tmp_path,side):
    async def scenario():
        e,now,first=await blocked_close(tmp_path,side=side);await release_ack(e)
        cfg=replace(e.profile.get_config(SYMBOL),symbol=OTHER)
        e.profile=replace(e.profile,symbols={**e.profile.symbols,OTHER:cfg})
        e.rules[OTHER]=e.rules[SYMBOL];e.quote_locks[OTHER]=asyncio.Lock();e.symbol_locks[OTHER]=asyncio.Lock()
        e.warmup_started[OTHER]=e.warmup_started[SYMBOL];e.basis_center=lambda symbol:D(0)
        for venue in ('arcus','lighter'):
            e.books[venue,OTHER]=e.books[venue,SYMBOL];e.generations[venue,OTHER]=1
        opening=e.journal.prepare_maker(OTHER,side,'open',D(1),D(100),0,D(100),D(100))
        oid=e.journal.observe_maker(opening,D(1),'filled',average=D(100))
        attempt=e.journal.prepare_attempt(oid,D(1));e.journal.observe_attempt(attempt,D(1),'filled',average=D(102 if side==-1 else 98))
        other_first=await e.place(OTHER,-side,'close',D(1),D(0),1)
        while not e.queues[OTHER].empty():
            await e.observe_maker(e.queues[OTHER].get_nowait());e.queues[OTHER].task_done()
        e.requotes[OTHER].append(now[0]-.2)
        book=e.books['lighter',OTHER];shift=D(20 if side==1 else -20)
        e.books['lighter',OTHER]=replace(book,bid=book.bid+shift,ask=book.ask+shift)
        await e.quote_symbol(OTHER)
        assert set(e.close_permits)=={(SYMBOL,side),(OTHER,-side)}
        assert not e.close_permit_valid(SYMBOL,-side) and not e.close_permit_valid(OTHER,side)
        while not e.queues[OTHER].empty():e.queues[OTHER].get_nowait();e.queues[OTHER].task_done()
        now[0]+=.05;await e.quote_symbol(SYMBOL)
        assert set(e.close_permits)=={(OTHER,-side)}
        await flush_acks(e);await e.quote_symbol(OTHER)
        assert not e.close_permits
        assert len([r for r in e.journal.makers(SYMBOL,active=True) if r['purpose']=='close'])==1
        assert len([r for r in e.journal.makers(OTHER,active=True) if r['purpose']=='close'])==1
        assert e.journal.maker(first)['status']==e.journal.maker(other_first)['status']=='canceled'
    asyncio.run(scenario())


@pytest.mark.parametrize('change',['expired_with_two_tokens','inventory','generation','prepare_consumed'])
def test_prepaid_place_revalidates_identity_after_capital_lock_await(tmp_path,change):
    async def scenario():
        e,now,_=await blocked_close(tmp_path);await release_ack(e);now[0]+=.05
        await e.capital_lock.acquire()
        task=asyncio.create_task(e.place(SYMBOL,-1,'close',D(1),D(0),1));await asyncio.sleep(0)
        assert not task.done() and e.capital_lock._waiters
        if change=='expired_with_two_tokens':
            now[0]+=1;e.requotes[SYMBOL].clear();e.requotes[SYMBOL].extend([now[0]-.02,now[0]-.01])
        elif change=='inventory':e.journal._inventory_delta(SYMBOL,D('.1'))
        elif change=='generation':
            e.generations['lighter',SYMBOL]=2
            e.books['lighter',SYMBOL]=replace(e.books['lighter',SYMBOL],generation=2)
        else:
            e.close_permits.pop((SYMBOL,-1));e.close_prepare_versions[SYMBOL,-1]+=1
        e.capital_lock.release()
        assert await task is None
        assert not e.journal.makers(SYMBOL,active=True) and not e.close_permits
    asyncio.run(scenario())


def test_expired_prepaid_place_can_use_current_normal_quota_after_lock_await(tmp_path):
    async def scenario():
        e,now,_=await blocked_close(tmp_path);await release_ack(e)
        await e.capital_lock.acquire()
        task=asyncio.create_task(e.place(SYMBOL,-1,'close',D(1),D(0),1));await asyncio.sleep(0)
        now[0]+=1.01;e.capital_lock.release()
        assert await task and not e.close_permits
    asyncio.run(scenario())


def test_replacement_prepared_during_cancel_await_prevents_later_permit_mint(tmp_path):
    from core.services.arbitrage_monitor_v2.execution.maker_taker_engine import DefinitiveFailure
    from test_maker_taker_engine import observation
    async def scenario():
        e=close_engine(tmp_path);opened(e.journal,1,'1','100','98')
        first=await e.place(SYMBOL,-1,'close',D(1),D(0),1);await flush_acks(e)
        e.requotes[SYMBOL].append(e.clock())
        async def rejected(row):raise DefinitiveFailure()
        e.transport.maker=rejected
        async def cancel(row):
            result=observation(row,'0','canceled');await e.observe_cancel(row,result)
            assert await e.place(SYMBOL,-1,'close',D(1),D(0),1)
            return result
        e.transport.cancel_maker=cancel
        e.book('lighter',SYMBOL,Book(D(110),D('110.1'),D(10),D(10),e.clock(),1))
        await e.quote_symbol(SYMBOL)
        closes=[r for r in e.journal.makers(SYMBOL) if r['purpose']=='close']
        assert len(closes)==2 and closes[-1]['status']=='rejected' and not e.close_permits
        assert e.close_prepare_versions[SYMBOL,-1]==2 and e.journal.maker(first)['status']=='canceled'
    asyncio.run(scenario())


def test_failed_preprepare_attempt_does_not_delete_different_concurrent_permit(tmp_path):
    async def scenario():
        e,now,_=await blocked_close(tmp_path);await release_ack(e)
        old=e.close_permits[SYMBOL,-1];replacement=dict(old);original=e.place
        async def failed(symbol,side,purpose,*args):
            if purpose=='close':
                e.close_permits[symbol,side]=replacement
                raise ValueError('different invocation failed')
            return await original(symbol,side,purpose,*args)
        e.place=failed
        with pytest.raises(ValueError,match='different invocation failed'):await e.quote_symbol(SYMBOL)
        assert e.close_permits[SYMBOL,-1] is replacement and replacement is not old
    asyncio.run(scenario())


async def partial_dust_close(tmp_path,side):
    from test_maker_taker_engine import observation
    e=close_engine(tmp_path);opened(e.journal,-side,'1','100','98' if side==-1 else '102')
    first=await e.place(SYMBOL,side,'close',D(1),D(0),1);await flush_acks(e)
    await e.observe_maker(observation(e.journal.maker(first),'.0005','open',average=D('102.11') if side==-1 else D('97.99')))
    assert e.journal.inventory(SYMBOL)==(-D(side)*D('.9995'),D(side))
    assert not e.blocking_residuals() and e.residual_state(e.residual_buckets()[0])[0]=='deferred_dust'
    now=[e.clock()];e.clock=lambda:now[0];e.requotes[SYMBOL].append(now[0]-.2)
    e.book('lighter',SYMBOL,Book(D(110) if side==-1 else D(90),D('110.1') if side==-1 else D('90.1'),
                               D(10),D(10),now[0],1))
    return e,now,first


@pytest.mark.parametrize('side',[-1,1])
@pytest.mark.parametrize('queued',[False,True])
def test_observed_partial_dust_uses_same_effective_paid_quantity(tmp_path,side,queued):
    async def scenario():
        e,now,first=await partial_dust_close(tmp_path,side)
        if queued:e.queues[OTHER]=asyncio.Queue();e.queues[OTHER].put_nowait(None)
        await e.quote_symbol(SYMBOL)
        assert e.journal.maker(first)['status']=='canceled'
        if queued:
            assert e.close_permits[SYMBOL,side]['qty']==D('.999')
            e.queues[OTHER].get_nowait();e.queues[OTHER].task_done();now[0]+=.05
            await e.quote_symbol(SYMBOL)
        closes=[r for r in e.journal.makers(SYMBOL,active=True) if r['purpose']=='close']
        assert len(closes)==1 and closes[0]['client']!=first and D(closes[0]['qty'])==D('.999')
        assert len(e.requotes[SYMBOL])==2 and not e.close_permits
    asyncio.run(scenario())


@pytest.mark.parametrize('raw,accepted',[(D('.9995'),True),(D('.9985'),False)])
def test_partial_permit_compares_effective_quantity_inside_capital_lock(tmp_path,raw,accepted):
    async def scenario():
        e,now,first=await partial_dust_close(tmp_path,-1)
        row=e.journal.maker(first);price,_,_,_,bound=e.maker_quote(SYMBOL,-1,'close',D('.9995'),D(0))
        e.requotes[SYMBOL].append(now[0])
        assert await e.close_cancel(row,'unsafe_floor',price,bound,True,now[0])
        assert e.close_permits[SYMBOL,-1]['qty']==D('.999')
        await e.capital_lock.acquire()
        task=asyncio.create_task(e.place(SYMBOL,-1,'close',raw,D(0),1));await asyncio.sleep(0)
        assert not task.done() and e.capital_lock._waiters
        e.capital_lock.release();client=await task
        assert bool(client)==accepted and not e.close_permits
        if accepted:assert D(e.journal.maker(client)['qty'])==D('.999')
        assert len(e.requotes[SYMBOL])==2
    asyncio.run(scenario())


def test_partial_permit_rejects_truly_different_effective_quantity(tmp_path):
    async def scenario():
        e,now,first=await partial_dust_close(tmp_path,-1)
        row=e.journal.maker(first);price,_,_,_,bound=e.maker_quote(SYMBOL,-1,'close',D('.9995'),D(0))
        e.requotes[SYMBOL].append(now[0]);assert await e.close_cancel(row,'unsafe_floor',price,bound,True,now[0])
        assert not e.close_permit_valid(SYMBOL,-1,D('.9985')) and not e.close_permits
    asyncio.run(scenario())
