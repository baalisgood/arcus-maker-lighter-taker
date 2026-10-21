"""Unknown Lighter hedge lookup polls inactive history and records tx evidence; offline only."""
import asyncio, json
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, Mock

from test_maker_taker_engine import engine, SYMBOL
from core.adapters.exchanges.order_state import OrderStateUnknownError
from core.services.arbitrage_monitor_v2.execution.maker_taker_live import LiveTransport


def test_unknown_lighter_lookup_finds_lagging_inactive_history_and_logs_evidence(tmp_path):
    async def scenario():
        e=engine(tmp_path)
        canceled=NS(status=NS(value='canceled'),client_id='77')
        rest=NS(_submitted_orders={'77':1},account_index=1,
                _lookup_terminal_order=Mock(return_value=None),
                get_order=AsyncMock(side_effect=OrderStateUnknownError('lighter',client_id='77')),
                find_order_by_client=AsyncMock(side_effect=[OrderStateUnknownError('lighter',client_id='77'),canceled]),
                transaction_api=NS(tx=AsyncMock(),next_nonce=AsyncMock()))
        rest._uncertain_orders={'77':{}}
        e.journal.db.execute("INSERT INTO writes(client,signed_hash,nonce,key_index,kind,at) VALUES('77',NULL,'5',2,'14',0)")
        live=object.__new__(LiveTransport);live.journal=e.journal;live.lookup_poll_seconds=5
        live.lighter=NS(_rest=rest,_normalize_symbol=lambda s:s)
        live.normalize=lambda order,*a:order
        got=await live.lookup_attempt({'venue':'lighter','client':'77','exchange_id':None,'qty':'1'},
                                      {'symbol':SYMBOL,'side':1})
        assert got is canceled and rest.find_order_by_client.await_count==2
        kinds=[json.loads(p)['error_type'] for (p,) in e.journal.db.execute(
            "SELECT payload FROM events WHERE kind='unknown_transaction_evidence_unavailable'").fetchall()]
        assert kinds==['NoSignedHash','NoSignedHash'] or kinds==['NoSignedHash']
        rest.transaction_api.tx.assert_not_awaited()
        assert rest._uncertain_orders=={}  # terminal history clears the adapter write gate
    asyncio.run(scenario())


def test_clientid_reuse_without_ledger_exchange_id_is_foreign_when_intent_contradicts(tmp_path):
    from test_maker_taker_engine import maker, observation
    e=engine(tmp_path);client=maker(e,qty='1',side=1)
    row=dict(e.journal.maker(client));assert not row['exchange_id']
    before=e.sequence
    e.receive(observation({**row,'exchange_id':'OLD-1'},'1',status='filled',side=-1))
    assert e.sequence==before and e.halt_reason  # foreign non-snapshot fill still halts, but is not adopted
    kinds=[k for (k,) in e.journal.db.execute("SELECT kind FROM events").fetchall()]
    assert 'client_id_reused_by_foreign_order' in kinds and 'observation_mismatch' not in kinds
    e2=engine(tmp_path/'b');c2=maker(e2,qty='1',side=1);r2=dict(e2.journal.maker(c2))
    e2.receive(observation({**r2,'exchange_id':'NEW-1'},'0',status='open'))
    assert e2.sequence==1 and not e2.halt_reason


def test_arcus_zero_size_rejected_push_is_status_only_and_inherits_ledger(tmp_path):
    from test_arcus_adapter import adapter, row
    from test_maker_taker_engine import maker
    e=engine(tmp_path);client=maker(e,qty='1',side=1)
    e.journal.observe_maker(client,__import__('decimal').Decimal(0),'open','EX-Z')
    a=adapter();live=object.__new__(LiveTransport);live.identity='fake'
    order=a._merge_order(row(order_id='EX-Z',status='REJECTED',clientId=client,originalSize='0',remainingSize='-1'))
    obs=live.normalize(order,'arcus')
    assert obs.requested is None and obs.cumulative is None and obs.status=='rejected'
    adopted=e._adopt_ledger_identity(obs)
    assert str(adopted.requested)=='1' and str(adopted.cumulative)=='0'


def test_cancel_result_from_zero_size_terminal_push_adopts_ledger(tmp_path):
    from decimal import Decimal
    from test_arcus_adapter import adapter, row
    from test_maker_taker_engine import maker
    async def scenario():
        e=engine(tmp_path);client=maker(e,qty='1',side=1)
        e.journal.observe_maker(client,Decimal(0),'open','EX-C')
        a=adapter();live=object.__new__(LiveTransport);live.identity='fake'
        order=a._merge_order(row(order_id='EX-C',status='CANCELED',clientId=client,originalSize='0',remainingSize='0',
                                 marketDisplayName='SOL-USD'))
        obs=live.normalize(order,'arcus',symbol='SOL-USDC-PERP')
        await e.observe_cancel(dict(e.journal.maker(client)),obs)
        assert e.journal.maker(client)['status']=='canceled'
    asyncio.run(scenario())


def test_unaudited_lighter_attempt_resolves_as_never_sent(tmp_path):
    async def scenario():
        e=engine(tmp_path)
        rest=NS(_submitted_orders={'88':1},_uncertain_orders={},find_order_by_client=AsyncMock())
        live=object.__new__(LiveTransport);live.journal=e.journal;live.identity='fake'
        live.lighter=NS(_rest=rest,_normalize_symbol=lambda s:s)
        got=await live.lookup_attempt({'venue':'lighter','client':'88','exchange_id':None,'qty':'1'},{'symbol':SYMBOL,'side':1})
        assert got.status=='rejected' and str(got.cumulative)=='0' and got.side==-1 and str(got.requested)=='1'
        rest.find_order_by_client.assert_not_awaited()
        assert e.journal.db.execute("SELECT 1 FROM events WHERE kind='lighter_attempt_never_sent'").fetchone()
    asyncio.run(scenario())


def test_settled_attempt_is_cleared_from_adapter_write_gate(tmp_path):
    e=engine(tmp_path)
    rows=[('settled','filled'),('open','submitted')]
    db=NS(execute=lambda q,a:NS(fetchone=lambda:{'status':dict(rows)[a[0]]} if a[0] in dict(rows) else None))
    rest=NS(_uncertain_orders={'settled':{},'open':{},'other':{}})
    live=object.__new__(LiveTransport);live.lighter=NS(_rest=rest)
    live.journal=NS(db=db,event=lambda *a,**k:None)
    live._clear_settled_uncertain()
    assert set(rest._uncertain_orders)=={'open','other'}


import pytest
@pytest.mark.parametrize('blocker',['stale','nonce_error'])
def test_hedge_blocked_before_signing_is_definitive_not_unknown(tmp_path,blocker):
    from collections import deque
    from test_maker_taker_sol_unlock import add_other,OTHER
    from core.adapters.exchanges.adapters.lighter_rest import LighterRest
    from core.services.arbitrage_monitor_v2.execution.maker_taker_engine import DefinitiveFailure,DeferredHedge
    async def run():
        e=engine(tmp_path);add_other(e)
        rest=object.__new__(LighterRest);rest._strict_write_lock=asyncio.Lock();rest._retired=False
        rest._uncertain_orders={'old':{}} if blocker=='stale' else {}
        send=AsyncMock();sign=Mock()
        rest.signer_client=NS(sign_create_order=sign,nonce_manager=NS(acknowledge_failure=Mock()),send_tx=send)
        rest.write_audit=e.journal.audit_write;rest._register_order_intent=Mock()
        rest._next_order_nonce=AsyncMock(side_effect=ConnectionError('x') if blocker=='nonce_error' else None)
        live=LiveTransport.__new__(LiveTransport);live.send_guard=lambda:True;live.writes=deque();live.journal=e.journal
        live.gate_wait_seconds=.1
        live.lighter=NS(_rest=rest,_normalize_symbol=lambda s:s);live.private_validity=lambda:{'lighter':(True,None,None)}
        live.source=NS(markets={OTHER:({},dict(market_id=2,supported_price_decimals=3,supported_size_decimals=3))},
            rules=e.rules,books={('lighter',OTHER):e.books['lighter',OTHER]},book_validity=lambda *args:(True,None,None))
        if blocker=='stale':
            # Another unresolved write holds the adapter gate: defer (no retry consumed), never send.
            with pytest.raises(DeferredHedge):
                await live.hedge({'client':'7','qty':'.2'},{'symbol':OTHER,'side':1,'purpose':'open'})
            send.assert_not_called()
            ev=e.journal.db.execute("SELECT payload FROM events WHERE kind='lighter_hedge_gate_busy'").fetchone()
            assert json.loads(ev[0])['blocking_client']=='old'
            return
        with pytest.raises(DefinitiveFailure):
            await live.hedge({'client':'7','qty':'.2'},{'symbol':OTHER,'side':1,'purpose':'open'})
        send.assert_not_called()
        ev=e.journal.db.execute("SELECT payload FROM events WHERE kind='lighter_hedge_not_sent'").fetchone()
        p=json.loads(ev[0]);assert p['client']=='7'
        assert p['error_type']=='ConnectionError' and 'message' not in p
    asyncio.run(run())


def test_hedge_waits_for_other_symbols_inflight_hedge_then_sends(tmp_path):
    from collections import deque
    from test_maker_taker_sol_unlock import add_other,OTHER
    async def run():
        e=engine(tmp_path);add_other(e)
        gate={'2726':{}}
        rest=NS(_uncertain_orders=gate)
        async def submit(*a,**k):
            assert not gate;raise RuntimeError('sent')
        rest._strict_submit=submit;rest.signer_client=NS(sign_create_order=None);rest._register_order_intent=Mock()
        live=LiveTransport.__new__(LiveTransport);live.send_guard=lambda:True;live.writes=deque();live.journal=e.journal
        live.lighter=NS(_rest=rest,_normalize_symbol=lambda s:s);live.private_validity=lambda:{'lighter':(True,None,None)}
        live.source=NS(markets={OTHER:({},dict(market_id=2,supported_price_decimals=3,supported_size_decimals=3))},
            rules=e.rules,books={('lighter',OTHER):e.books['lighter',OTHER]},book_validity=lambda *args:(True,None,None))
        async def other_finishes():
            await asyncio.sleep(.2);gate.pop('2726')
        asyncio.create_task(other_finishes())
        e.journal.db.execute("INSERT INTO writes(client,signed_hash,nonce,key_index,kind,at) VALUES('7','h','1',2,'14',0)")
        with pytest.raises(RuntimeError,match='sent'):
            await live.hedge({'client':'7','qty':'.2'},{'symbol':OTHER,'side':1,'purpose':'open'})
        assert not e.journal.db.execute("SELECT 1 FROM events WHERE kind IN ('lighter_hedge_gate_busy','lighter_hedge_not_sent')").fetchone()
    asyncio.run(run())


def test_operator_release_retry_exhausted_resets_retry_budget(tmp_path):
    from decimal import Decimal
    from test_maker_taker_engine import maker
    e=engine(tmp_path);client=maker(e,qty='1',side=1)
    e.journal.observe_maker(client,Decimal(1),'filled','EX-R')
    oid=e.journal.obligations()[0]['id']
    for _ in range(e.profile.hedge.max_retries+1):
        c=e.journal.prepare_attempt(oid,Decimal(1));e.journal.observe_attempt(c,Decimal(0),'canceled')
    assert len(e.zero_fill_failures(oid))>e.profile.hedge.max_retries
    snap={'failure_symbol':SYMBOL,'reason_code':'retry_exhausted'}
    e.journal.latch_manual_intervention(snap)
    e.journal.operator_release_retry_exhausted(snap,[oid])
    assert e.journal.manual_intervention() is None and e.zero_fill_failures(oid)==[]
    assert e.journal.db.execute("SELECT 1 FROM events WHERE kind='operator_release_retry_exhausted'").fetchone()


def test_redrain_later_retries_symbol_under_lock(tmp_path):
    async def run():
        e=engine(tmp_path);calls=[]
        async def drain(symbol):
            assert e.symbol_locks[symbol].locked();calls.append(symbol)
        e.drain_symbol=drain
        e._redrain_later(SYMBOL,delay=0)
        await asyncio.sleep(.05)
        assert calls==[SYMBOL] and not e.halted
    asyncio.run(run())


def test_zero_size_rejection_row_side_is_not_evidence_for_filled_sell(tmp_path):
    # amlt-5937: SELL close filled 0.192, then a zero-value REJECTED row (side BUY default) halted the runner.
    from decimal import Decimal
    from test_arcus_adapter import adapter, row
    from test_maker_taker_engine import maker
    e=engine(tmp_path);client=maker(e,qty='1',side=-1)
    e.journal.observe_maker(client,Decimal(1),'filled','EX-S')
    a=adapter();live=object.__new__(LiveTransport);live.identity='fake'
    order=a._merge_order(row(order_id='EX-S',status='REJECTED',clientId=client,side='BUY',originalSize='0',remainingSize='0'))
    obs=live.normalize(order,'arcus')
    assert obs.side is None and obs.requested is None
    adopted=e._adopt_ledger_identity(obs)
    assert adopted.side==-1
    e._validate(adopted,e.journal.maker(client),'arcus')
