from decimal import Decimal as D
from types import SimpleNamespace

import pytest

from tools.operator_resolve_hedge import infer_resolution
from core.services.arbitrage_monitor_v2.execution.maker_taker_engine import MakerTakerEngine, OrderObservation


def attempt(qty='0.098', filled='0'):
    return {'qty': qty, 'filled': filled}


def test_missing_lighter_hedge_is_zero_fill():
    # Maker sold on Arcus (obligation side -1), hedge buys on Lighter; Lighter never moved.
    assert infer_resolution(attempt(), {'side': -1}, (D('-0.196'), D('0.098')), (D('-0.196'), D('0.098')), D('0.001')) \
        == (D('0'), 'canceled')


def test_filled_lighter_hedge_detected():
    assert infer_resolution(attempt(), {'side': -1}, (D('-0.196'), D('0.098')), (D('-0.196'), D('0.196')), D('0.001')) \
        == (D('0.098'), 'filled')


def test_partial_fill_aligned():
    assert infer_resolution(attempt(), {'side': -1}, (D('-0.196'), D('0.098')), (D('-0.196'), D('0.148')), D('0.001')) \
        == (D('0.050'), 'canceled')


@pytest.mark.parametrize('actual', [(D('-0.294'), D('0.098')),   # Arcus drift
                                    (D('-0.196'), D('0.000')),   # wrong direction
                                    (D('-0.196'), D('0.300'))])  # more than the attempt
def test_unexplained_positions_refused(actual):
    with pytest.raises(ValueError):
        infer_resolution(attempt(), {'side': -1}, (D('-0.196'), D('0.098')), actual, D('0.001'))


class FakeJournal:
    def __init__(self, rows):
        import sqlite3
        self.db = sqlite3.connect(':memory:'); self.db.row_factory = sqlite3.Row
        self.db.execute('CREATE TABLE all_orders(client,symbol,exchange_id,side,qty,filled)')
        self.db.executemany('INSERT INTO all_orders VALUES(?,?,?,?,?,?)', [tuple(r)+(-1,'0.098','0.098')[len(r)-3:] for r in rows])
        self.events = []

    def event(self, *a, **k): self.events.append((a, k))


def obs(**k):
    base = dict(venue='arcus', account='acct', symbol='SOL-USDC-PERP', client='C1', order_id='O1', side=-1,
                requested=D('0.098'), cumulative=D('0.098'), status='filled', source='t', received=0.0)
    base.update(k)
    return OrderObservation(**base)


def engine(rows):
    e = MakerTakerEngine.__new__(MakerTakerEngine)
    e.journal = FakeJournal(rows); e.queues = {'SOL-USDC-PERP': None}
    e.transport = SimpleNamespace(identity='acct')
    return e


def test_push_without_client_or_market_adopts_ledger_identity():
    e = engine([('C1', 'SOL-USDC-PERP', 'O1')])
    adopted = e._adopt_ledger_identity(obs(client='', symbol='MARKET-0'))
    assert adopted.client == 'C1' and adopted.symbol == 'SOL-USDC-PERP'
    row = {'client': 'C1', 'symbol': 'SOL-USDC-PERP', 'side': -1, 'qty': '0.098', 'exchange_id': 'O1'}
    e._validate(adopted, row, 'arcus')


def test_conflicting_identity_still_fails_with_detail():
    e = engine([('C1', 'SOL-USDC-PERP', 'O1')])
    row = {'client': 'C1', 'symbol': 'SOL-USDC-PERP', 'side': -1, 'qty': '0.098', 'exchange_id': 'O1'}
    with pytest.raises(ValueError, match=r"requested: observed=Decimal\('0.196'\)"):
        e._validate(obs(requested=D('0.196'), cumulative=D('0')), row, 'arcus')
    assert e._adopt_ledger_identity(obs(client='OTHER')).client == 'OTHER'


def test_status_only_push_inherits_side_and_size_from_ledger():
    # Live HYPE halt: a late FILLED push for a cache-evicted order had side=BUY/size=0 adapter defaults.
    e = engine([('C1', 'SOL-USDC-PERP', 'O1')])
    adopted = e._adopt_ledger_identity(obs(client='', symbol='MARKET-0', side=None, requested=None, cumulative=None))
    assert (adopted.side, adopted.requested, adopted.cumulative) == (-1, D('0.098'), D('0.098'))
    row = {'client': 'C1', 'symbol': 'SOL-USDC-PERP', 'side': -1, 'qty': '0.098', 'exchange_id': 'O1'}
    e._validate(adopted, row, 'arcus')


def test_status_only_push_without_exchange_id_match_fails_closed():
    e = engine([('C1', 'SOL-USDC-PERP', 'OTHER')])
    adopted = e._adopt_ledger_identity(obs(side=None, requested=None, cumulative=None))
    assert adopted.side is None
    row = {'client': 'C1', 'symbol': 'SOL-USDC-PERP', 'side': -1, 'qty': '0.098', 'exchange_id': 'OTHER'}
    with pytest.raises(ValueError, match='side: observed=None'):
        e._validate(adopted, row, 'arcus')


def test_live_normalize_marks_missing_side_and_size_unknown():
    from core.services.arbitrage_monitor_v2.execution.maker_taker_live import LiveTransport
    from core.adapters.exchanges.adapters.arcus import ArcusAdapter
    parser = ArcusAdapter.__new__(ArcusAdapter); parser._markets = {}
    parser.standard_symbol = lambda s: s or 'MARKET-0'
    t = LiveTransport.__new__(LiveTransport); t.identity = 'acct'
    partial = parser._parse_order({'orderId': 'O1', 'status': 'FILLED', 'avgFillPrice': '81.682'})
    o = t.normalize(partial, 'arcus')
    assert o.side is None and o.requested is None and o.cumulative is None and o.status == 'filled'
    full = parser._parse_order({'orderId': 'O1', 'side': 'SELL', 'originalSize': '0.13', 'remainingSize': '0', 'status': 'FILLED'})
    o = t.normalize(full, 'arcus')
    assert (o.side, o.requested, o.cumulative) == (-1, D('0.13'), D('0.13'))
