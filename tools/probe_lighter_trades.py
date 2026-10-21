#!/usr/bin/env python3
"""Read-only probe: which Lighter /trades parameter combination is accepted for this account.

Usage: .venv/bin/python tools/probe_lighter_trades.py [--env-file .env] [--journal run/maker-taker/live.sqlite]
Never places/cancels orders; it only reads trades and prints HTTP status/body per variant (no secrets printed).
"""
import argparse, asyncio, os, sqlite3, sys
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
BASE = 'https://mainnet.zklighter.elliot.ai'


async def main():
    p = argparse.ArgumentParser(); p.add_argument('--env-file', type=Path, default=ROOT / '.env')
    p.add_argument('--journal', type=Path, default=ROOT / 'run/maker-taker/live.sqlite'); a = p.parse_args()
    from dotenv import load_dotenv
    load_dotenv(a.env_file, override=False, interpolate=False)
    import lighter
    acct = int(os.environ['LIGHTER_ACCOUNT_INDEX']); key = int(os.environ['LIGHTER_API_KEY_INDEX'])
    pk = os.environ['LIGHTER_API_KEY_PRIVATE_KEY']
    try:
        signer = lighter.SignerClient(url=BASE, account_index=acct, api_private_keys={key: pk})
    except TypeError:
        signer = lighter.SignerClient(url=BASE, private_key=pk, account_index=acct, api_key_index=key)
    token, err = signer.create_auth_token_with_expiry(lighter.SignerClient.DEFAULT_10_MIN_AUTH_EXPIRY)
    if err: print('auth token error:', err); return 1
    db = sqlite3.connect(f'file:{a.journal}?mode=ro', uri=True)
    rows = db.execute("SELECT t.client,t.exchange_id,t.status,t.filled,m.symbol FROM attempts t JOIN obligations o ON o.id=t.obligation "
                      "JOIN orders m ON m.client=o.maker WHERE t.venue='lighter' AND t.exchange_id IS NOT NULL ORDER BY t.created DESC LIMIT 1").fetchall()
    order_index = int(rows[0][1]) if rows else None
    symbol = rows[0][4] if rows else None
    print('latest ledger hedge:', rows[0] if rows else None)
    api = lighter.ApiClient(lighter.Configuration(host=BASE)); oa = lighter.OrderApi(api)
    market = None
    try:
        books = await oa.order_books()
        name = (symbol or '').split('-')[0]
        market = next((b.market_id for b in books.order_books if b.symbol == name), None)
    except Exception as e: print('order_books failed:', type(e).__name__)
    print('account', acct, 'market', market, 'order_index', order_index)
    base = dict(limit=20, authorization=token, account_index=acct)
    variants = {
        'A current (timestamp,asc,order_index)': dict(sort_by='timestamp', sort_dir='asc', order_index=order_index),
        'B A + market_id': dict(sort_by='timestamp', sort_dir='asc', order_index=order_index, market_id=market),
        'C timestamp,desc,order_index': dict(sort_by='timestamp', sort_dir='desc', order_index=order_index),
        'D timestamp,no dir,order_index': dict(sort_by='timestamp', order_index=order_index),
        'E timestamp,desc,market_id only': dict(sort_by='timestamp', sort_dir='desc', market_id=market),
        'F timestamp,desc,account only': dict(sort_by='timestamp', sort_dir='desc'),
        'G trade_id,desc,order_index': dict(sort_by='trade_id', sort_dir='desc', order_index=order_index),
        'H block_height,desc,order_index': dict(sort_by='block_height', sort_dir='desc', order_index=order_index),
    }
    for name, extra in variants.items():
        q = {**base, **{k: v for k, v in extra.items() if v is not None}}
        try:
            r = await oa.trades(**q)
            trades = r.trades or []
            mine = [t for t in trades if order_index in (getattr(t, 'ask_id', None), getattr(t, 'bid_id', None))]
            print(f'OK   {name}: {len(trades)} trades, matching order_index={len(mine)}',
                  f'sample={trades[0].to_dict() if trades else None}'[:400])
        except Exception as e:
            print(f'FAIL {name}: {type(e).__name__} status={getattr(e, "status", None)} body={str(getattr(e, "body", ""))[:200]}')
    await api.close(); await signer.close()
    return 0

if __name__ == '__main__':
    raise SystemExit(asyncio.run(main()))
