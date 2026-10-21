#!/usr/bin/env python3
"""Read-only terminal dashboard for the maker/taker runner.

Usage (in a second tmux window, while the runner is running or stopped):
    .venv/bin/python tools/monitor.py                 # live ledger
    .venv/bin/python tools/monitor.py --journal run/maker-taker/paper.sqlite

Opens the SQLite journal in read-only mode; it never connects to an exchange and never writes.
"""
import argparse
import json
import sqlite3
import sys
import time
from datetime import datetime
from decimal import Decimal as D
from pathlib import Path

from rich.console import Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
TERMINAL = ('filled', 'canceled', 'rejected', 'expired', 'deferred')
QUIET = ('maker_prepared', 'requote', 'close_quote', 'quote_', 'stale', 'observation_identity_adopted', 'attempt_prepared')


def connect(path):
    db = sqlite3.connect(f'file:{path}?mode=ro', uri=True, timeout=1)
    db.row_factory = sqlite3.Row
    return db


def q(db, sql, args=()):
    try:
        return [dict(r) for r in db.execute(sql, args)]
    except sqlite3.Error:
        return []


def fmt(x, places=4):
    if x in (None, ''):
        return '-'
    try:
        v = D(str(x))
        return f'{v:.{places}f}'.rstrip('0').rstrip('.') if v else '0'
    except Exception:
        return str(x)


def ago(ts):
    if not ts:
        return '-'
    s = time.time() - ts
    return f'{s:.0f}s' if s < 120 else (f'{s/60:.0f}m' if s < 7200 else f'{s/3600:.1f}h')


def build(path, stats_cache):
    db = connect(path)
    try:
        since = time.time() - 86400
        inv = {r['symbol']: r for r in q(db, 'SELECT * FROM current_inventory')}
        makers = q(db, "SELECT symbol,side,purpose,qty,price,filled,status FROM orders WHERE status NOT IN "
                       f"{TERMINAL} AND venue='arcus' ORDER BY symbol,side")
        pending = q(db, 'SELECT m.symbol,o.qty,o.hedged,o.unwound FROM pending_obligations p JOIN obligations o ON o.id=p.id '
                        'JOIN orders m ON m.client=o.maker')
        attempts = q(db, f'SELECT m.symbol,t.client,t.qty,t.filled,t.status FROM attempts t JOIN obligations o ON o.id=t.obligation '
                         f'JOIN orders m ON m.client=o.maker WHERE t.status NOT IN {TERMINAL}')
        fills = q(db, "SELECT symbol,kind,payload,at FROM events WHERE kind IN ('maker_fill','hedge_fill') AND at>=? ORDER BY id", (since,))
        events = q(db, 'SELECT at,symbol,kind,payload FROM events ORDER BY id DESC LIMIT 400')
        halt = q(db, "SELECT at,symbol,payload FROM events WHERE kind='halt' ORDER BY id DESC LIMIT 1")
        manual = q(db, "SELECT value FROM meta WHERE key='manual_intervention'")
    finally:
        db.close()

    symbols = sorted(set(inv) | {m['symbol'] for m in makers} | {f['symbol'] for f in fills if f['symbol'] != '*'})
    agg = {s: {'maker_n': 0, 'maker_usd': D(0), 'hedge_n': 0, 'lat': [], 'last': None} for s in symbols}
    for f in fills:
        a = agg.get(f['symbol'])
        if a is None:
            continue
        p = json.loads(f['payload'])
        a['last'] = f['at']
        if f['kind'] == 'maker_fill':
            a['maker_n'] += 1
            try:
                a['maker_usd'] += abs(D(str(p.get('qty'))) * D(str(p.get('average') or p.get('reference') or 0)))
            except Exception:
                pass
        else:
            a['hedge_n'] += 1
            if p.get('latency_ms') is not None:
                a['lat'].append(float(p['latency_ms']))

    t = Table(title='持仓 / 挂单 / 对冲（账本，24h）', expand=True)
    for c in ('币种', 'Arcus 仓位', 'Lighter 仓位', '净敞口', '挂单 (买/卖)', '待对冲', '未决对冲', '24h 挂单成交', '24h 对冲', '对冲延迟 p50', '最近成交'):
        t.add_column(c, justify='right' if c != '币种' else 'left')
    for s in symbols:
        r = inv.get(s, {'arcus': '0', 'lighter': '0'})
        a_pos, l_pos = D(r['arcus']), D(r['lighter'])
        net = a_pos + l_pos
        mk = [m for m in makers if m['symbol'] == s]
        quote = ' / '.join(
            ','.join(f"{fmt(m['qty'])}@{fmt(m['price'], 3)}" for m in mk if m['side'] == side) or '-' for side in (1, -1))
        due = sum((D(o['qty']) - D(o['hedged']) - D(o['unwound']) for o in pending if o['symbol'] == s), D(0))
        unk = [x for x in attempts if x['symbol'] == s]
        g = agg[s]
        lat = sorted(g['lat'])
        p50 = f"{lat[len(lat)//2]:.0f}ms" if lat else '-'
        t.add_row(s.replace('-USDC-PERP', ''), fmt(a_pos), fmt(l_pos),
                  Text(fmt(net), style='bold red' if net else 'green'), quote,
                  Text(fmt(due), style='yellow' if due else ''),
                  Text(','.join(f"{x['status']}:{fmt(x['qty'])}" for x in unk) or '-', style='bold red' if unk else ''),
                  f"{g['maker_n']} / {fmt(g['maker_usd'], 2)}U", str(g['hedge_n']), p50, ago(g['last']))

    st = Table(title='今日统计（每 60 秒刷新）', expand=True)
    for c in ('币种', '挂单在线率', '成交额 U', '回合', '回合盈亏 U', '每万 U 盈亏', '重挂次数'):
        st.add_column(c, justify='right' if c != '币种' else 'left')
    for row in stats_cache.get('rows', []):
        if row.get('symbol') not in symbols:
            continue
        st.add_row(row['symbol'].replace('-USDC-PERP', ''),
                   f"{row['quote_uptime_pct']:.1f}%" if row.get('quote_uptime_pct') is not None else '-',
                   fmt(row.get('combined_venue_notional'), 2), str(row.get('roundtrips', 0)),
                   fmt(row.get('roundtrip_pnl_usd'), 4), fmt(row.get('pnl_per_10k_arcus'), 3), str(row.get('requote_attempts', 0)))

    ev = Table(title='最近事件', expand=True, show_lines=False)
    ev.add_column('时间', width=8); ev.add_column('币种', width=6); ev.add_column('事件', width=26); ev.add_column('详情', overflow='fold')
    shown = 0
    for e in events:
        if any(e['kind'].startswith(k) for k in QUIET):
            continue
        style = 'bold red' if e['kind'] in ('halt', 'manual_intervention', 'observation_mismatch', 'lighter_send_rejected',
                                            'lighter_send_error', 'unknown_transaction_evidence') else ''
        ev.add_row(datetime.fromtimestamp(e['at']).strftime('%H:%M:%S'), e['symbol'].replace('-USDC-PERP', ''),
                   Text(e['kind'], style=style), e['payload'][:160])
        shown += 1
        if shown >= 12:
            break

    head = Text(f"账本 {path}   刷新 {datetime.now().strftime('%H:%M:%S')}   ")
    if manual:
        head.append('需要人工介入！ ', style='bold white on red')
    if halt:
        h = halt[0]
        head.append(f"最近停机 {ago(h['at'])} 前 [{h['symbol']}]: {json.loads(h['payload']).get('reason', '')}", style='red')
    return Group(Panel(head), t, st, ev)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--journal', type=Path, default=ROOT / 'run/maker-taker/live.sqlite')
    p.add_argument('--interval', type=float, default=2.0)
    args = p.parse_args()
    if not args.journal.exists():
        print(f'账本不存在: {args.journal}'); return 1
    from core.services.arbitrage_monitor_v2.history.maker_taker_statistics import statistics_snapshot
    cache = {'at': 0, 'rows': []}
    with Live(build(args.journal, cache), refresh_per_second=2, screen=True) as live:
        while True:
            if time.time() - cache['at'] > 60:
                try:
                    cache['rows'] = statistics_snapshot(args.journal, 'day'); cache['at'] = time.time()
                except Exception:
                    cache['at'] = time.time()
            try:
                live.update(build(args.journal, cache))
            except sqlite3.Error:
                pass
            time.sleep(args.interval)


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        pass
