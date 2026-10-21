#!/usr/bin/env python3
"""Explicit paper/live/replay entry; paper never loads credential environment/files."""
import argparse
import asyncio
import fcntl
import hashlib
import json
import os
from pathlib import Path
import signal
import sqlite3
import stat
import sys
import time
from contextlib import ExitStack


class ProcessLock:
    def __init__(self,path): self.path=Path(path); self.fd=None
    def __enter__(self):
        self.path.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
        self.fd=os.open(self.path,os.O_RDWR|os.O_CREAT|os.O_NOFOLLOW,0o600)
        info=os.fstat(self.fd)
        if not stat.S_ISREG(info.st_mode) or info.st_uid!=os.getuid():
            os.close(self.fd); self.fd=None
            raise ValueError('Unsafe lock path')
        try: fcntl.flock(self.fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except OSError:
            os.close(self.fd); self.fd=None
            raise RuntimeError('Another process owns this journal/account')
        return self
    def __exit__(self,*args):
        if self.fd is not None: os.close(self.fd); self.fd=None


def account_lock_paths(credentials,root=Path('/tmp')):
    a,l,ai,li,ki,identity=credentials
    accounts=(f'arcus:mainnet:{a["WALLET_ADDRESS"].lower()}:{ai}',f'lighter:mainnet:{li}')
    return sorted(root/f'arcus-maker-lighter-taker-{os.getuid()}-{hashlib.sha256(account.encode()).hexdigest()}.lock'
                  for account in accounts)


def parser():
    p=argparse.ArgumentParser(description='Arcus maker / Lighter taker. Default: public-data paper simulation.')
    p.add_argument('--mode',choices=('paper','monitor','live','replay','taker_taker'),default='paper')
    p.add_argument('--config',type=Path,default=Path(__file__).parent/'config/arbitrage/arbitrage_segmented.yaml')
    p.add_argument('--state-dir',type=Path,default=Path(__file__).parent/'run/maker-taker')
    p.add_argument('--duration-seconds',type=float,default=86400)
    p.add_argument('--replay',type=Path,help='Explicit JSON replay fixture; not real public-market evidence')
    p.add_argument('--paper-hedge-latency-ms',type=float,default=300)
    p.add_argument('--small-live',action='store_true',help='one segment; each flat side satisfies both venue minimums and conservative 10 USDC hedge policy')
    p.add_argument('--env-file',type=Path,help='Live/taker_taker only: explicit credential env file; no discovery')
    return p


def load_live_env(path):
    if path is None: return
    from dotenv import load_dotenv
    info=path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid!=os.getuid() or info.st_mode&0o077:
        raise ValueError('Credential env file must be owner-only regular file (chmod 600)')
    load_dotenv(path,override=False,interpolate=False)


async def run(args):
    from dataclasses import replace
    from decimal import Decimal as D,ROUND_CEILING
    from core.services.arbitrage_monitor_v2.config.maker_taker_config import load_profile
    from core.services.arbitrage_monitor_v2.execution.maker_taker_engine import MakerTakerEngine
    from core.services.arbitrage_monitor_v2.execution.maker_taker_journal import Journal
    from core.services.arbitrage_monitor_v2.execution.maker_taker_paper import PublicMarkets,PaperTransport
    from core.services.arbitrage_monitor_v2.history.maker_taker_statistics import statistics_background
    profile=load_profile(args.config)
    if profile.execution_mode!='maker_taker':
        raise ValueError('Use --mode taker_taker with an explicit legacy profile')
    if not D(str(args.duration_seconds)).is_finite() or args.duration_seconds<=0:
        raise ValueError('duration-seconds must be positive finite')
    if not D(str(args.paper_hedge_latency_ms)).is_finite() or args.paper_hedge_latency_ms<300:
        raise ValueError('Paper Standard-account latency assumption must be >=300ms')
    if args.mode!='live' and args.env_file:
        raise ValueError('--env-file is allowed only in explicit live/taker_taker mode')
    source=PublicMarkets(profile.symbols)
    replay=None
    if args.mode=='replay':
        if args.replay is None: raise ValueError('--replay required for replay mode')
        replay=json.loads(args.replay.read_text())
        warmup=replay.get('warmup_seconds',0)
        if type(warmup) is not int or not 0<=warmup<=3600:raise ValueError('Invalid replay warmup duration')
        replay_offset=[-warmup]
        source.clock=lambda:time.monotonic()+replay_offset[0]
        source.load_metadata(replay['arcus_markets'],replay['lighter_markets'])
        source.connected={'arcus':True,'lighter':True};source.generations={'arcus':1,'lighter':1}
    else: await source.metadata()
    if args.small_live:
        updated={}
        for symbol,cfg in profile.symbols.items():
            mark=D(str(source.markets[symbol][0]['markPrice']));rule=source.rules[symbol]
            q=max(rule.arcus_min,rule.lighter_min,max(D(5),rule.arcus_notional)/(mark*D('.99')),
                  max(D(10),rule.lighter_notional)/(mark*D('.99')))
            q=(q/rule.step).to_integral_value(rounding=ROUND_CEILING)*rule.step
            updated[symbol]=replace(cfg,quantity=q*2,max_segments=1)
        profile=replace(profile,symbols=updated)
    credentials=None;identity='paper-mainnet'
    if args.mode=='live':
        env_path=args.env_file or Path(__file__).parent/'.env'
        load_live_env(env_path if env_path.exists() else None)
        from core.services.arbitrage_monitor_v2.execution.maker_taker_live import live_credentials
        credentials=live_credentials();identity=credentials[-1]
    args.state_dir.mkdir(parents=True,exist_ok=True,mode=0o700)
    name='live' if args.mode=='live' else ('replay' if replay else 'paper')
    journal_path=args.state_dir/(name+'.sqlite')
    # Shared by this implementation across checkout and journal paths. Use dedicated accounts with older runners.
    locks=account_lock_paths(credentials) if credentials else []
    with ExitStack() as lock_stack:
        lock_stack.enter_context(ProcessLock(args.state_dir/(name+'.lock')))
        for lock_path in locks:lock_stack.enter_context(ProcessLock(lock_path))
        journal=Journal(journal_path,identity)
        transport=None;engine=None;started=False;finance_task=None;maintenance_task=None;stats_task=None;final_reported=False
        stop_attempted=False;runtime_error=None
        try:
            if credentials:
                from core.services.arbitrage_monitor_v2.execution.maker_taker_live import LiveTransport
                transport=LiveTransport(source,journal,credentials)
            else: transport=PaperTransport(source,journal,hedge_latency_ms=args.paper_hedge_latency_ms)
            engine=MakerTakerEngine(profile,journal,transport,source.rules,clock=lambda:source.clock())
            if credentials: await transport.connect(engine.receive)
            else: transport.receive=engine.receive;transport.engine_book=engine.book
            if replay:
                def deliver(frame): source._publish(frame['venue'],frame['symbol'],frame['bids'],frame['asks'])
                source.callback=transport.on_book
                for frame in replay['initial_books']: deliver(frame)
            else:
                await source.start(transport.on_book if not credentials else engine.book)
                deadline=time.monotonic()+30
                while not engine.subscriptions_ready() and time.monotonic()<deadline: await asyncio.sleep(.05)
                if not engine.subscriptions_ready():
                    diagnostics=json.dumps({'error':'subscriptions_timeout',**engine.readiness_diagnostics()},sort_keys=True)
                    print('Public/private subscriptions did not become ready: '+diagnostics,file=sys.stderr,flush=True)
                    raise RuntimeError('Public/private subscriptions did not become ready')
            if engine.manual_intervention_required:
                print(json.dumps({'state':engine.state,'manual_intervention_required':True,
                    'manual_intervention_persisted':engine.manual_intervention_persisted,'manual_intervention_snapshot':engine.manual_intervention_snapshot}),flush=True)
            await engine.start();started=True
            async def maintain_journal():
                while True:
                    await asyncio.sleep(5)
                    for _ in range(8):
                        journal.archive_closed()
                        await asyncio.sleep(.01)
            maintenance_task=asyncio.create_task(maintain_journal())
            if credentials:
                from core.services.arbitrage_monitor_v2.history.maker_taker_finance import FinanceCollector
                collector=FinanceCollector(journal,transport)
                async def collect_finance():
                    while True:
                        await collector.collect()
                        await asyncio.sleep(60)
                finance_task=asyncio.create_task(collect_finance())
            print(json.dumps({'mode':args.mode,'state':engine.state,'symbols':list(profile.symbols),'journal':str(journal_path),
                'manual_intervention_required':engine.manual_intervention_required,
                'manual_intervention_persisted':engine.manual_intervention_persisted,'manual_intervention_snapshot':engine.manual_intervention_snapshot,
                'venue_leverage_setting':'operator_ui_required' if credentials else 'modeled'}),flush=True)
            if replay:
                for second in range(warmup):
                    replay_offset[0]+=1
                    for frame in replay['initial_books']:deliver(frame)
                    await engine.tick()
                    for q in engine.queues.values():await q.join()
                for frame in replay['frames']:
                    await engine.tick();deliver(frame)
                    for q in engine.queues.values(): await q.join()
                await engine.tick()
            else:
                stopped=asyncio.Event();loop=asyncio.get_running_loop()
                for sig in (signal.SIGINT,signal.SIGTERM): loop.add_signal_handler(sig,stopped.set)
                deadline=time.monotonic()+args.duration_seconds;last_stats=time.monotonic()
                while not stopped.is_set() and time.monotonic()<deadline:
                    if maintenance_task.done():maintenance_task.result()
                    await engine.tick()
                    if engine.halted:
                        print(json.dumps({'state':engine.state,'reason':engine.reason,'halt_reason':engine.halt_reason,
                                          'halt_state':engine.halt_state,'halt_symbol':engine.halt_symbol,'halt_error_type':engine.halt_error_type,
                                          'manual_intervention_required':engine.manual_intervention_required,
                                          'manual_intervention_persisted':engine.manual_intervention_persisted,'manual_intervention_snapshot':engine.manual_intervention_snapshot,
                                          'last_alarm':engine.last_alarm}),flush=True);break
                    if stats_task is not None and stats_task.done():
                        print(json.dumps({'hourly':stats_task.result()},sort_keys=True),flush=True)
                        stats_task=None
                    if time.monotonic()-last_stats>=3600 and stats_task is None:
                        stats_task=asyncio.create_task(statistics_background(journal.path,'hour'))
                        last_stats=time.monotonic()
                    await asyncio.sleep(.05)
            stop_attempted=True;clean=await engine.stop();started=False
            if stats_task is not None:
                stats_task.cancel();await asyncio.gather(stats_task,return_exceptions=True);stats_task=None
            if finance_task is not None:
                finance_task.cancel();await asyncio.gather(finance_task,return_exceptions=True);finance_task=None
                try:await asyncio.wait_for(collector.collect(),5)
                except (TimeoutError,ValueError):journal.event('*','finance_final_collection_incomplete')
            print(json.dumps({'shutdown_clean':clean,'state':engine.state,'reason':engine.reason,'halt_reason':engine.halt_reason,
                'halt_state':engine.halt_state,'halt_symbol':engine.halt_symbol,'halt_error_type':engine.halt_error_type,
                'manual_intervention_required':engine.manual_intervention_required,
                'manual_intervention_persisted':engine.manual_intervention_persisted,'manual_intervention_snapshot':engine.manual_intervention_snapshot,
                                          'last_alarm':engine.last_alarm,
                'pending_residuals':len(journal.obligations()),
                'positions_flat':all(not any(journal.inventory(s)) for s in profile.symbols),
                'hourly':await statistics_background(journal.path,'hour'),
                'daily':await statistics_background(journal.path,'day')},sort_keys=True),flush=True)
            final_reported=True
            return 0 if clean and not engine.halted else 1
        except BaseException as exc:
            runtime_error=exc
            raise
        finally:
            cleanup_errors=[]
            async def cleanup_step(stage,operation):
                try:await operation()
                except (Exception,asyncio.CancelledError) as exc:
                    cleanup_errors.append({'stage':stage,'error_type':type(exc).__name__})
            async def cancel_task(task):
                task.cancel();await asyncio.gather(task,return_exceptions=True)
            for stage,task in (('maintenance',maintenance_task),('finance',finance_task),('statistics',stats_task)):
                if task is not None:await cleanup_step(stage,lambda task=task:cancel_task(task))
            if engine is not None:
                if not stop_attempted:
                    stop_attempted=True
                    await cleanup_step('engine.stop',engine.stop)
                # stop can fail before it reaches its own task cleanup.
                for task in set(engine.tasks)|set(engine.cancel_tasks):
                    await cleanup_step('engine.task',lambda task=task:cancel_task(task))
            if transport is not None and credentials:await cleanup_step('transport.close',transport.close)
            await cleanup_step('source.close',source.close)
            try:journal.close()
            except Exception as exc:cleanup_errors.append({'stage':'journal.close','error_type':type(exc).__name__})
            if engine is not None and (not final_reported or cleanup_errors):
                snapshot=engine.manual_intervention_snapshot
                print(json.dumps({'state':engine.state,'shutdown_clean':False,
                    'reason':engine.halt_reason or (snapshot or {}).get('reason') or engine.reason,
                    'halt_reason':engine.halt_reason,'halt_state':engine.halt_state,'halt_symbol':engine.halt_symbol,
                    'halt_error_type':engine.halt_error_type,'last_alarm':engine.last_alarm,
                    'manual_intervention_required':engine.manual_intervention_required,
                    'manual_intervention_persisted':engine.manual_intervention_persisted,'manual_intervention_snapshot':snapshot,
                    'runtime_error_type':type(runtime_error).__name__ if runtime_error is not None else None,
                    'cleanup_errors':cleanup_errors},sort_keys=True),flush=True)
            if cleanup_errors and runtime_error is None:
                raise RuntimeError('Runtime cleanup incomplete; see final status')


def main(argv=None):
    os.umask(0o077);args=parser().parse_args(argv)
    if args.mode=='taker_taker':
        import yaml
        data=yaml.safe_load(args.config.read_text())
        if data.get('execution_mode','taker_taker')!='taker_taker': raise ValueError('Legacy mode refuses maker-owned profile')
        env_path=args.env_file or Path(__file__).parent/'.env'
        load_live_env(env_path if env_path.exists() else None)
        command=[sys.executable,str(Path(__file__).parent/'main_unified.py'),'--config',str(args.config)]
        if not data.get('system_mode',{}).get('monitor_only',True):
            from core.services.arbitrage_monitor_v2.execution.maker_taker_live import live_credentials
            with ExitStack() as stack:
                for path in account_lock_paths(live_credentials()):
                    lock=stack.enter_context(ProcessLock(path))
                    os.set_inheritable(lock.fd,True)
                os.execv(sys.executable,command)
        os.execv(sys.executable,command)
    try: return asyncio.run(run(args))
    except (ValueError,RuntimeError,KeyError,sqlite3.Error) as exc:
        # Only engine-authored diagnostics are echoed; arbitrary exception text may carry private data.
        text=str(exc)
        safe=text.startswith(('Observation does not match durable intent/account','Returned venue leverage exceeds',
                              'Correction returned venue leverage exceeds','Startup reconciliation blocked:'))
        print(f'Startup/runtime blocked: {type(exc).__name__}'+(': '+text[:800] if safe else ''),file=sys.stderr);return 2


if __name__=='__main__': raise SystemExit(main())
