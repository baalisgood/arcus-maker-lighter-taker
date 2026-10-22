#!/usr/bin/env python3
"""Operator reconciliation for Lighter hedge attempts whose terminal state cannot be read.

Default is a read-only dry run. With --confirm, an unresolved Lighter attempt is closed in the
journal ONLY when live account positions prove its fill amount unambiguously:

* the live runner is stopped (the same journal/account locks are acquired here);
* exactly one unresolved Lighter attempt exists for the symbol and no unresolved Arcus attempt;
* the attempt is older than --min-age-seconds (IOC hedges cannot rest that long);
* Lighter shows no open order for that market;
* the Arcus position equals the ledger exactly, and the Lighter position differs from the ledger by
  0..remaining of the attempt in the hedge direction, aligned to the size step.

--revert-unsent-reconcile undoes a pending reconciliation correction that was anchored on a bad (lagging)
account snapshot: only when the correction never sent anything, nothing changed since, and the live account
equals the ledger as it was just before that reconciliation. The journal is backed up first.

The inferred cumulative fill is recorded via the normal journal path (status canceled/filled) together
with an `operator_hedge_resolution` event. Any remaining uncovered quantity is hedged again by the
normal engine recovery on the next start. Nothing is ever sent to an exchange by this tool.
"""
import argparse
import asyncio
import json
import os
import sqlite3
import sys
import time
from contextlib import ExitStack
from decimal import Decimal as D
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

TERMINAL = {'filled', 'canceled', 'rejected', 'expired', 'deferred'}


def infer_resolution(attempt, obligation, ledger, actual, step):
    """Return (cumulative, status) or raise ValueError with a human-readable reason.

    ledger/actual: (arcus_signed, lighter_signed) positions for the symbol.
    """
    step = D(str(step))
    tol = step / 2
    qty, filled = D(attempt['qty']), D(attempt['filled'])
    remaining = qty - filled
    hedge_side = -int(obligation['side'])
    d_arcus = D(actual[0]) - D(ledger[0])
    d_lighter = D(actual[1]) - D(ledger[1])
    if abs(d_arcus) > tol:
        raise ValueError(f'Arcus position {actual[0]} != ledger {ledger[0]} (diff {d_arcus}); resolve Arcus side first')
    extra = d_lighter * hedge_side
    if extra < -tol or extra > remaining + tol:
        raise ValueError(f'Lighter position {actual[1]} vs ledger {ledger[1]} (diff {d_lighter}) is outside '
                         f'0..{remaining} in hedge direction {hedge_side}; not explained by this attempt')
    units = (extra / step).to_integral_value()
    if abs(extra - units * step) > tol / 1000:
        raise ValueError(f'Lighter position difference {d_lighter} is not aligned to size step {step}')
    extra = min(max(units * step, D(0)), remaining)
    cumulative = filled + extra
    return cumulative, ('filled' if cumulative == qty else 'canceled')


def backup_journal(journal, state_dir):
    path = Path(state_dir) / time.strftime('live.sqlite.bak-%Y%m%d-%H%M%S')
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
    os.close(fd)
    target = sqlite3.connect(str(path))
    try:
        journal.db.backup(target)
    finally:
        target.close()
    return str(path)


def attempt_age_seconds(journal, client):
    for at, payload in journal.db.execute("SELECT at,payload FROM events WHERE kind='attempt_prepared' AND payload LIKE ? "
                                          "ORDER BY id DESC", ('%' + str(client) + '%',)):
        try:
            if str(json.loads(payload).get('client')) == str(client):
                return time.time() - at
        except ValueError:
            continue
    return None


async def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--config', type=Path, default=ROOT / 'config/arbitrage/arbitrage_segmented.yaml')
    p.add_argument('--state-dir', type=Path, default=ROOT / 'run/maker-taker')
    p.add_argument('--env-file', type=Path)
    p.add_argument('--min-age-seconds', type=float, default=120)
    p.add_argument('--confirm', action='store_true', help='write the inferred resolution into live.sqlite')
    p.add_argument('--accept-flat', action='store_true',
                   help='a hedged ledger pair (net 0) whose positions were closed outside the runner (both venues flat) '
                        'is reset to flat; requires --confirm to write')
    p.add_argument('--release-retry-exhausted', action='store_true',
                   help='release a retry_exhausted manual-intervention latch so the runner re-hedges the residual; '
                        'requires every symbol ledger == actual, no active attempts/makers; requires --confirm to write')
    p.add_argument('--revert-unsent-reconcile', action='store_true',
                   help='undo a never-sent reconciliation correction anchored on a bad snapshot, when the account '
                        'equals the pre-reconciliation ledger; journal is backed up first; requires --confirm to write')
    args = p.parse_args(argv)

    from run_maker_taker import ProcessLock, account_lock_paths, load_live_env
    from core.services.arbitrage_monitor_v2.config.maker_taker_config import load_profile
    from core.services.arbitrage_monitor_v2.execution.maker_taker_journal import Journal
    from core.services.arbitrage_monitor_v2.execution.maker_taker_paper import PublicMarkets
    from core.services.arbitrage_monitor_v2.execution.maker_taker_live import live_credentials, LiveTransport

    profile = load_profile(args.config)
    env_path = args.env_file or ROOT / '.env'
    load_live_env(env_path if env_path.exists() else None)
    credentials = live_credentials()
    journal_path = args.state_dir / 'live.sqlite'
    if not journal_path.exists():
        print(json.dumps({'result': 'no_journal', 'journal': str(journal_path)}));return 0
    source = PublicMarkets(profile.symbols)
    await source.metadata()
    with ExitStack() as stack:
        stack.enter_context(ProcessLock(args.state_dir / 'live.lock'))
        for path in account_lock_paths(credentials):
            stack.enter_context(ProcessLock(path))
        journal = Journal(journal_path, credentials[-1])
        transport = LiveTransport(source, journal, credentials)
        report = {'journal': str(journal_path), 'confirm': args.confirm, 'symbols': {}}
        try:
            transport.arcus._set_markets([rows[0] for rows in source.markets.values()])
            await transport.arcus._do_authenticate()
            if not await transport.lighter.connect():
                raise RuntimeError('Lighter connection failed')
            snap = await transport.snapshot()
            report['manual_intervention'] = journal.manual_intervention()
            report['unconfigured_positions'] = {s: [str(x) for x in v] for s, v in snap['positions'].items()
                                                if s not in profile.symbols and any(v)}
            rest = transport.lighter._rest
            for symbol in profile.symbols:
                ledger = journal.inventory(symbol)
                actual = tuple(snap['positions'].get(symbol, (D(0), D(0))))
                entry = {'ledger': [str(x) for x in ledger], 'actual': [str(x) for x in actual],
                         'unresolved_attempts': []}
                report['symbols'][symbol] = entry
                active = journal.attempts(active=True, symbol=symbol)
                for attempt in active:
                    obligation = journal.obligation(attempt['obligation'])
                    write = journal.db.execute('SELECT * FROM writes WHERE client=? ORDER BY id DESC LIMIT 1',
                                               (attempt['client'],)).fetchone()
                    info = {'client': attempt['client'], 'venue': attempt['venue'], 'status': attempt['status'],
                            'qty': attempt['qty'], 'filled': attempt['filled'], 'obligation': obligation['id'],
                            'obligation_side': obligation['side'], 'age_seconds': attempt_age_seconds(journal, attempt['client'])}
                    if write is not None and write['signed_hash']:
                        info['signed_hash'] = write['signed_hash']
                        try:
                            tx = await rest.transaction_api.tx(by='hash', value=write['signed_hash'])
                            info['tx'] = {'status': getattr(tx, 'status', None), 'nonce': getattr(tx, 'nonce', None),
                                          'event_info': str(getattr(tx, 'event_info', ''))[:500]}
                        except Exception as exc:
                            info['tx'] = {'lookup_error': type(exc).__name__, 'detail': str(exc)[:300]}
                    entry['unresolved_attempts'].append(info)
                if journal.pending_reconcile(symbol):
                    try:
                        plan = journal.unsent_reconcile_plan(symbol)
                        entry['pending_reconcile'] = {'reconcile_event': plan['reconcile_event'],
                                                      'snapshot': [str(x) for x in plan['snapshot']],
                                                      'ledger_before': [str(x) for x in plan['ledger_before']],
                                                      'obligation': plan['obligation']}
                        if tuple(actual) != plan['ledger_before']:
                            raise ValueError('Account does not equal the pre-reconciliation ledger; not a bad-snapshot correction')
                        if await rest.get_open_orders(transport.lighter._normalize_symbol(symbol)):
                            raise ValueError('Open Lighter order(s) in this market; cancel them first')
                        if not args.revert_unsent_reconcile:
                            entry['action'] = ('pending correction was anchored on a bad snapshot: rerun with '
                                               '--revert-unsent-reconcile --confirm to restore the prior ledger')
                        elif not args.confirm:
                            entry['action'] = 'dry_run revert_unsent_reconcile (add --confirm to write)'
                        else:
                            entry['backup'] = backup_journal(journal, args.state_dir)
                            journal.revert_unsent_reconcile(symbol, actual)
                            entry['ledger'] = [str(x) for x in journal.inventory(symbol)]
                            entry['action'] = 'written (correction reverted; ledger restored)'
                    except ValueError as exc:
                        entry['action'] = 'refused'; entry['reason'] = str(exc)
                    continue
                if not active:
                    entry['action'] = 'none'
                    if actual != ledger:
                        entry['action'] = 'mismatch'
                        try:
                            if any(actual):
                                raise ValueError('Positions are not flat; only an externally closed (flat) pair can be accepted')
                            if ledger[0] + ledger[1] != 0:
                                raise ValueError('Ledger is not a hedged pair (net != 0); refusing to accept flat')
                            if journal.obligations(symbol) or journal.makers(symbol, active=True):
                                raise ValueError('Pending hedge obligation or active maker in ledger')
                            if not args.accept_flat:
                                entry['action'] = 'mismatch (externally closed pair: rerun with --accept-flat --confirm to reset ledger to flat)'
                            elif not args.confirm:
                                entry['action'] = 'dry_run accept_flat (add --confirm to write)'
                            else:
                                mark = D(str(source.markets[symbol][0]['markPrice']))
                                journal.reconcile_inventory(symbol, D(0), D(0), mark)
                                journal.event(symbol, 'operator_accept_flat', ledger=entry['ledger'], actual=entry['actual'],
                                              evidence='account_positions')
                                entry['action'] = 'written (ledger reset to flat)'
                        except ValueError as exc:
                            entry['action'] = 'refused'; entry['reason'] = str(exc)
                    continue
                try:
                    if len(active) != 1 or active[0]['venue'] != 'lighter':
                        raise ValueError('Only a single unresolved Lighter attempt per symbol can be inferred from positions')
                    attempt = active[0]
                    age = entry['unresolved_attempts'][0]['age_seconds']
                    if age is None or age < args.min_age_seconds:
                        raise ValueError(f'Attempt age {age} below --min-age-seconds {args.min_age_seconds}')
                    native = transport.lighter._normalize_symbol(symbol)
                    open_orders = await rest.get_open_orders(native)
                    if open_orders:
                        raise ValueError(f'{len(open_orders)} open Lighter order(s) in this market; cancel them first')
                    obligation = journal.obligation(attempt['obligation'])
                    cumulative, status = infer_resolution(attempt, obligation, ledger, actual, source.rules[symbol].step)
                    entry['inferred'] = {'cumulative': str(cumulative), 'status': status,
                                         'rehedge_after_start': str(D(attempt['qty']) - cumulative)}
                    if args.confirm:
                        journal.observe_attempt(attempt['client'], cumulative, status)
                        journal.event(symbol, 'operator_hedge_resolution', client=attempt['client'], cumulative=str(cumulative),
                                      status=status, ledger=entry['ledger'], actual=entry['actual'],
                                      evidence='account_positions', tx=entry['unresolved_attempts'][0].get('tx'))
                        entry['action'] = 'written'
                    else:
                        entry['action'] = 'dry_run (rerun with --confirm to write)'
                except ValueError as exc:
                    entry['action'] = 'refused'
                    entry['reason'] = str(exc)
            latch = report.get('manual_intervention')
            if args.release_retry_exhausted:
                release = report['release_retry_exhausted'] = {}
                try:
                    if not latch:
                        raise ValueError('No manual-intervention latch present')
                    if latch.get('reason_code') != 'retry_exhausted':
                        raise ValueError(f"Latch reason_code {latch.get('reason_code')!r} is not retry_exhausted")
                    for symbol, entry in report['symbols'].items():
                        if entry['ledger'] != entry['actual'] and [D(x) for x in entry['ledger']] != [D(x) for x in entry['actual']]:
                            raise ValueError(f'{symbol}: ledger {entry["ledger"]} != actual {entry["actual"]}')
                        if entry['unresolved_attempts']:
                            raise ValueError(f'{symbol}: unresolved attempts present')
                    if journal.makers(active=True):
                        raise ValueError('Active maker orders in ledger')
                    symbol = latch.get('failure_symbol')
                    native = transport.lighter._normalize_symbol(symbol)
                    if await rest.get_open_orders(native):
                        raise ValueError('Open Lighter orders in failure market; cancel them first')
                    oids = [row['id'] for row in journal.obligations(symbol)]
                    release['failure_symbol'] = symbol
                    release['unresolved_qty'] = latch.get('unresolved_qty')
                    release['obligations'] = oids
                    if args.confirm:
                        journal.operator_release_retry_exhausted(latch, oids)
                        release['action'] = 'written (latch released; runner will re-hedge the residual after start)'
                    else:
                        release['action'] = 'dry_run (add --confirm to write)'
                except ValueError as exc:
                    release['action'] = 'refused'; release['reason'] = str(exc)
        finally:
            await transport.close()
            journal.close()
    print(json.dumps(report, indent=2, ensure_ascii=False, default=str))
    refused = any(e.get('action') == 'refused' for e in report['symbols'].values())
    refused = refused or report.get('release_retry_exhausted', {}).get('action') == 'refused'
    return 3 if refused else 0


if __name__ == '__main__':
    raise SystemExit(asyncio.run(main()))
