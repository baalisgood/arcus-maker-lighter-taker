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
        live=object.__new__(LiveTransport);live.journal=e.journal;live.lookup_poll_seconds=5
        live.lighter=NS(_rest=rest,_normalize_symbol=lambda s:s)
        live.normalize=lambda order,*a:order
        got=await live.lookup_attempt({'venue':'lighter','client':'77','exchange_id':None,'qty':'1'},
                                      {'symbol':SYMBOL,'side':1})
        assert got is canceled and rest.find_order_by_client.await_count==2
        kinds=[json.loads(p)['error_type'] for (p,) in e.journal.db.execute(
            "SELECT payload FROM events WHERE kind='unknown_transaction_evidence_unavailable'").fetchall()]
        assert kinds==['NoAuditedWrite']
        rest.transaction_api.tx.assert_not_awaited()
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
