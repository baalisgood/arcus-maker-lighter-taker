"""Operator undo of a never-sent correction anchored on a lagging account snapshot (HYPE 2026-10-22)."""
import asyncio
import json
from decimal import Decimal as D

import pytest

from test_maker_taker_engine import engine, maker, observation, SYMBOL, Fake
from core.services.arbitrage_monitor_v2.execution.maker_taker_journal import Journal
from core.services.arbitrage_monitor_v2.execution.maker_taker_close_basis import CloseBasis
from core.services.arbitrage_monitor_v2.execution.maker_taker_engine import MakerTakerEngine


def incident(tmp_path):
    """Hedged open pair, then a reconciliation anchored on a snapshot showing the Lighter leg flat."""
    async def build():
        e = engine(tmp_path)
        client = maker(e, '.13', side=-1)
        await e.observe_maker(observation(e.journal.maker(client), '.13', 'filled'))
        await e.drain_symbol(SYMBOL)
        assert e.journal.inventory(SYMBOL) == (D('-.13'), D('.13'))
        e.journal.reconcile_inventory(SYMBOL, D('-.13'), D(0), D(100))
        assert e.journal.inventory(SYMBOL) == (D('-.13'), D(0)) and e.journal.pending_reconcile(SYMBOL)
        assert len(e.journal.obligations(SYMBOL)) == 1
        return e
    return asyncio.run(build())


def test_plan_reports_pre_reconciliation_ledger(tmp_path):
    e = incident(tmp_path)
    plan = e.journal.unsent_reconcile_plan(SYMBOL)
    assert plan['ledger_before'] == (D('-.13'), D('.13')) and plan['snapshot'] == (D('-.13'), D(0))


def test_revert_restores_ledger_validates_on_reopen_and_keeps_basis(tmp_path):
    e = incident(tmp_path)
    e.journal.revert_unsent_reconcile(SYMBOL, (D('-.13'), D('.13')))
    assert e.journal.inventory(SYMBOL) == (D('-.13'), D('.13'))
    assert not e.journal.obligations() and not e.journal.pending_reconcile()
    event = json.loads(e.journal.db.execute(
        "SELECT payload FROM events WHERE kind='operator_reconcile_reverted'").fetchone()[0])
    assert event['removed_order']['purpose'] == 'reconcile' and event['removed_adjustment']['lighter']
    e.journal.close()
    journal = Journal(tmp_path / 'state.sqlite', 'fake')  # full history validation
    assert journal.inventory(SYMBOL) == (D('-.13'), D('.13'))
    basis = CloseBasis(journal, (SYMBOL,))
    state = basis.states[SYMBOL]
    assert (state['arcus'], state['lighter']) == (D('-.13'), D('.13'))
    fill = journal.db.execute("SELECT id FROM events WHERE kind='maker_fill'").fetchone()[0]
    # The lot keeps its opening fill origin and Lighter coverage; the reverted reconcile never reset it.
    assert [lot['origin'] for lot in state['lots']] == [fill] and all(lot.get('attributed', True) for lot in state['lots'])
    assert state['lots'][0]['source']['fills']
    assert not journal.db.execute("SELECT 1 FROM events WHERE kind='close_floor_unattributed'").fetchone()

    async def restart():
        transport = Fake(); transport.positions = {SYMBOL: [D('-.13'), D('.13')]}
        restarted = MakerTakerEngine(e.profile, journal, transport, e.rules)
        restarted.equity = transport.equity.copy()
        restarted.books = e.books
        assert await restarted.reconcile(initial=True) and not restarted.halted and not transport.sends
    asyncio.run(restart())


def test_revert_refuses_when_account_differs(tmp_path):
    e = incident(tmp_path)
    with pytest.raises(ValueError):
        e.journal.revert_unsent_reconcile(SYMBOL, (D('-.13'), D(0)))
    assert e.journal.inventory(SYMBOL) == (D('-.13'), D(0)) and e.journal.pending_reconcile(SYMBOL)


def test_revert_refuses_after_correction_attempt(tmp_path):
    e = incident(tmp_path)
    oid = e.journal.obligations(SYMBOL)[0]['id']
    client = e.journal.prepare_attempt(oid, D('.13'))
    e.journal.observe_attempt(client, D(0), 'rejected')
    with pytest.raises(ValueError, match='attempt'):
        e.journal.unsent_reconcile_plan(SYMBOL)


def test_revert_refuses_after_later_fill(tmp_path):
    e = incident(tmp_path)
    with e.journal.transaction():
        e.journal.event(SYMBOL, 'maker_fill', qty='.01')
    with pytest.raises(ValueError, match='changed'):
        e.journal.unsent_reconcile_plan(SYMBOL)


def test_no_pending_reconcile_is_refused(tmp_path):
    e = engine(tmp_path)
    with pytest.raises(ValueError):
        e.journal.unsent_reconcile_plan(SYMBOL)
