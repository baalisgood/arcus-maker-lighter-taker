#!/usr/bin/env python3
"""Read-only: show what Arcus itself reports for orders involved in an observation_mismatch.

For each order id (default: the order ids of the last --last observation_mismatch events) prints
the ledger row, related journal events, GET /v1/order/<id> and the account fills of that order
(/v1/fills). Only side/size/status/id/time fields are printed. Nothing is written or sent.
Safe to run while the runner is stopped or running (journal opened read-only).
"""
import argparse, asyncio, json, sqlite3, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
ORDER_FIELDS = ('orderId', 'clientId', 'marketDisplayName', 'side', 'orderSide', 'type', 'status', 'state', 'cancelReason',
                'reduceOnly', 'price', 'originalSize', 'remainingSize', 'avgFillPrice', 'sequenceNumber', 'createdAt', 'updatedAt')
FILL_FIELDS = ('tradeId', 'orderId', 'clientId', 'marketDisplayName', 'side', 'orderSide', 'isMaker', 'liquidity', 'role',
               'size', 'price', 'originalSize', 'remainingSize', 'sequenceNumber', 'createdAt')


def pick(row, fields):
    return {k: row.get(k) for k in fields if k in row} if isinstance(row, dict) else row


async def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--state-dir', type=Path, default=ROOT / 'run/maker-taker')
    p.add_argument('--env-file', type=Path)
    p.add_argument('--order', action='append', default=[], help='Arcus order id or ledger client, e.g. amlt-9307 (repeatable)')
    p.add_argument('--last', type=int, default=2, help='inspect the last N observation_mismatch events')
    args = p.parse_args(argv)
    from run_maker_taker import load_live_env
    from core.services.arbitrage_monitor_v2.execution.maker_taker_live import live_credentials
    from core.adapters.exchanges.adapters.arcus import ArcusAdapter
    from core.adapters.exchanges.interface import ExchangeConfig
    from core.adapters.exchanges.models import ExchangeType
    env_path = args.env_file or ROOT / '.env'
    load_live_env(env_path if env_path.exists() else None)
    a, _l, ai, _li, _ki, _identity = live_credentials()
    db = sqlite3.connect(f"file:{args.state_dir / 'live.sqlite'}?mode=ro", uri=True); db.row_factory = sqlite3.Row
    orders = list(args.order)
    mismatches = db.execute("SELECT id,at,symbol,payload FROM events WHERE kind='observation_mismatch' ORDER BY id DESC LIMIT ?",
                            (args.last,)).fetchall()
    for m in (mismatches if not args.order else []):
        oid = json.loads(m['payload']).get('order_id')
        if oid and oid not in orders: orders.append(oid)
    arcus = ArcusAdapter(ExchangeConfig('arcus', 'Arcus', ExchangeType.PERPETUAL, a['API_KEY'], '',
                                        wallet_address=a['WALLET_ADDRESS'], private_key=a['API_SIGNING_KEY'],
                                        extra_params={'account_index': ai, 'reconcile_timeout': 1}))
    report = {'mismatch_events': [dict(id=m['id'], at=m['at'], symbol=m['symbol'], **json.loads(m['payload'])) for m in mismatches],
              'orders': {}}
    try:
        await arcus.load_markets()
        await arcus._do_authenticate()
    except Exception as exc:
        report['arcus_setup_error'] = type(exc).__name__
    for oid in orders:
        entry = report['orders'].setdefault(oid, {})
        led = db.execute('SELECT client,symbol,side,purpose,qty,filled,status,exchange_id FROM all_orders WHERE exchange_id=? OR client=?',
                         (oid, oid)).fetchall()
        entry['ledger'] = [dict(r) for r in led]
        exchange_id = (led[0]['exchange_id'] if led and led[0]['exchange_id'] else oid)
        keys = [oid] + [r['client'] for r in led]
        ev = db.execute('SELECT id,at,symbol,kind,payload FROM events WHERE ' + ' OR '.join(['payload LIKE ?'] * len(keys)) +
                        ' ORDER BY id DESC LIMIT 40', tuple('%' + k + '%' for k in keys)).fetchall()
        entry['events'] = [dict(id=r['id'], at=r['at'], kind=r['kind'], payload=r['payload'][:600]) for r in reversed(ev)]
        try:
            _, data = await arcus._request('GET', f'/v1/order/{exchange_id}', query=arcus._account_query())
            entry['arcus_order'] = pick(data, ORDER_FIELDS)
        except Exception as exc:
            entry['arcus_order_error'] = type(exc).__name__
        symbol = led[0]['symbol'] if led else None
        try:
            at = max([r['at'] for r in ev] or [0])
            q = {**arcus._account_query(), 'limit': 1000}
            if symbol: q['market'] = arcus.native_symbol(symbol)
            if at: q.update({'from': int((at - 3600) * 1_000_000), 'to': int((at + 600) * 1_000_000)})
            _, data = await arcus._request('GET', '/v1/fills', query=q)
            fills = data.get('fills', []) if isinstance(data, dict) else []
            if isinstance(fills, dict): fills = list(fills.values())
            entry['arcus_fills'] = [pick(f, FILL_FIELDS) for f in fills if str(f.get('orderId')) == str(exchange_id)]
            entry['arcus_fill_keys'] = sorted({k for f in fills for k in f})[:60]
        except Exception as exc:
            entry['arcus_fills_error'] = type(exc).__name__
    if arcus._session: await arcus._session.close()
    print(json.dumps(report, indent=1, ensure_ascii=False, default=str))
    return 0


if __name__ == '__main__':
    sys.exit(asyncio.run(main()))
