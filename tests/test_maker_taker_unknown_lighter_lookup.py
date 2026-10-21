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
