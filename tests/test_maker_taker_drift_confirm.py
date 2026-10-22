"""A periodic account snapshot that lags a just-confirmed fill must never be anchored as drift."""
import asyncio
import json
from dataclasses import replace
from decimal import Decimal as D
from pathlib import Path

import pytest
import yaml

from test_maker_taker_engine import engine, maker, observation, profile, SYMBOL
from core.services.arbitrage_monitor_v2.config.maker_taker_config import load_profile


def confirming(tmp_path, seconds):
    cfg = profile()
    return engine(tmp_path, replace(cfg, risk=replace(cfg.risk, drift_confirm_seconds=seconds)))


def kinds(e, kind):
    return [json.loads(p) for (p,) in e.journal.db.execute('SELECT payload FROM events WHERE kind=? ORDER BY id', (kind,))]


async def hedged_open(e, qty='.13'):
    client = maker(e, qty, side=-1)
    await e.observe_maker(observation(e.journal.maker(client), qty, 'filled'))
    await e.drain_symbol(SYMBOL)
    assert e.journal.inventory(SYMBOL) == (D('-' + qty), D(qty))


def test_lagging_lighter_snapshot_right_after_hedge_is_not_corrected(tmp_path):
    async def scenario():
        e = confirming(tmp_path, 5)
        await hedged_open(e)
        sends = len(e.transport.sends)
        # Account REST still shows the Lighter leg as flat although the private fill was already confirmed.
        e.transport.positions = {SYMBOL: [D('-.13'), D(0)]}
        assert await e.reconcile() and not e.halted
        assert len(e.transport.sends) == sends and not e.journal.pending_reconcile(SYMBOL)
        assert e.journal.inventory(SYMBOL) == (D('-.13'), D('.13'))
        assert not kinds(e, 'reconcile_drift') and len(kinds(e, 'reconcile_drift_suspect')) == 1
        assert not e._admit(SYMBOL, -1, 'open', D('.13'), D(100))
        # Re-read scheduled within the confirmation window rather than a full interval.
        assert e.clock() - e.last_reconcile >= e.profile.risk.reconcile_interval - 6
        e.transport.positions = {SYMBOL: [D('-.13'), D('.13')]}
        assert await e.reconcile() and not e.halted
        assert SYMBOL not in e._drift_suspects and len(kinds(e, 'reconcile_drift_cleared')) == 1
        assert not kinds(e, 'reconcile_drift') and len(e.transport.sends) == sends
    asyncio.run(scenario())


def test_persistent_drift_is_corrected_only_after_window(tmp_path):
    async def scenario():
        e = confirming(tmp_path, .05)
        e.transport.positions = {SYMBOL: [D('.2'), D(0)]}
        assert await e.reconcile() and not e.transport.sends
        assert await e.reconcile() and not e.transport.sends  # inside the window
        await asyncio.sleep(.06)
        assert await e.reconcile()
        assert len(e.transport.sends) == 1 and len(kinds(e, 'reconcile_drift')) == 1
        assert e.journal.inventory(SYMBOL) == (D('.2'), D('-.2'))
    asyncio.run(scenario())


def test_changed_difference_or_recent_ledger_change_restarts_window(tmp_path):
    async def scenario():
        e = confirming(tmp_path, .05)
        e.transport.positions = {SYMBOL: [D('.2'), D(0)]}
        assert await e.reconcile() and not e.transport.sends
        await asyncio.sleep(.06)
        e.transport.positions = {SYMBOL: [D('.3'), D(0)]}  # different difference: new suspicion
        assert await e.reconcile() and not e.transport.sends
        await asyncio.sleep(.06)
        e.journal.inventory_changed[SYMBOL] = __import__('time').monotonic()  # a fresh ledger change
        assert await e.reconcile() and not e.transport.sends
        await asyncio.sleep(.06)
        assert await e.reconcile() and len(e.transport.sends) == 1
    asyncio.run(scenario())


def test_pending_correction_lagging_snapshot_waits_instead_of_halting(tmp_path):
    async def scenario():
        e = confirming(tmp_path, 5)
        await hedged_open(e)
        e.journal.db.execute("INSERT OR REPLACE INTO meta VALUES('reconcile:'||?,'awaiting_account_confirmation')", (SYMBOL,))
        e.transport.positions = {SYMBOL: [D('-.13'), D(0)]}
        assert await e.reconcile() and not e.halted and e.journal.pending_reconcile(SYMBOL)
        e.transport.positions = {SYMBOL: [D('-.13'), D('.13')]}
        assert await e.reconcile() and not e.halted and not e.journal.pending_reconcile(SYMBOL)
    asyncio.run(scenario())


def test_startup_mismatch_still_blocks_immediately(tmp_path):
    async def scenario():
        e = confirming(tmp_path, 5)
        await hedged_open(e)
        e.transport.positions = {SYMBOL: [D('-.13'), D(0)]}
        assert not await e.reconcile(initial=True) and e.halted
        assert len(kinds(e, 'startup_position_mismatch')) == 1
    asyncio.run(scenario())


def test_config_default_and_validation(tmp_path):
    root = Path(__file__).resolve().parent.parent
    source = root / 'config/arbitrage/arbitrage_segmented.yaml'
    assert load_profile(source).risk.drift_confirm_seconds == 5
    data = yaml.safe_load(source.read_text())
    data['risk']['drift_confirm_seconds'] = -1
    bad = tmp_path / 'bad.yaml'; bad.write_text(yaml.safe_dump(data))
    with pytest.raises(ValueError):
        load_profile(bad)
