"""Journal projections: missing exchange fees/funding never become zero cost."""
import json
from collections import defaultdict
from datetime import datetime,timezone
from decimal import Decimal

D=Decimal


def percentile(values,p):
    if not values:return None
    ordered=sorted(values)
    index=(len(ordered)-1)*p
    lo=int(index); hi=min(lo+1,len(ordered)-1)
    return ordered[lo]+(ordered[hi]-ordered[lo])*(index-lo)


def diagnostic_statistics(journal,period):
    """Read materialized actual tranches and bounded runtime observations; never enrich here."""
    width=3600 if period=='hour' else 86400
    metrics={};runs={};stales={};ranges=defaultdict(list);global_episodes=set();completed_obligations=set()
    def bucket(at):
        return datetime.fromtimestamp(at,timezone.utc).strftime('%Y-%m-%dT%H:00Z' if period=='hour' else '%Y-%m-%d')
    def metric(at,symbol):
        key=(bucket(at),symbol)
        if key not in metrics:
            metrics[key]=dict(observation_seconds=0.0,quoted_seconds=0.0,modeled_quote_uptime=False,
                stale_counts={r:0 for r in ('connection_timeout','sequence_gap','generation_change','reconnect')},
                roundtrips=0,close_floor_count=0,close_timeout_count=0,close_cost_unknown_count=0,gross=D(0),confirmed=D(0),priced=D(0),
                restored=D(0),hold_cost=D(0),hold_qty=D(0),open_num=D(0),open_den=D(0),open_qty=D(0),open_priced=D(0),
                close_num=D(0),close_den=D(0),close_qty=D(0),close_priced=D(0),slip_num=D(0),slip_den=D(0),slip_qty=D(0),slip_priced=D(0),
                pending_close_qty=D(0),unpriced_closed_qty=D(0))
        return metrics[key]
    def chunks(start,end):
        while start<end:
            stop=min(end,(int(start//width)+1)*width)
            yield start,stop
            start=stop
    def interval(start,end,symbol,reason=None,quoted=False,modeled=False):
        for a,b in chunks(start,end):
            m=metric(a,symbol)
            if reason is None:
                m['observation_seconds']+=b-a;m['quoted_seconds']+=(b-a if quoted else 0)
                m['modeled_quote_uptime']=m['modeled_quote_uptime'] or modeled
            else:ranges[bucket(a),symbol,reason].append((a,b))
    def stale_interval(p,symbol,end):
        run=runs.get(p.get('boot'));start=max(p['since_wall'],run['start']) if run else p['since_wall']
        interval(start,end,symbol,p['reason'])
        # Venue episodes share one ID across symbols; global durations/counts aren't multiplied.
        interval(start,end,'*',p['reason'])
    for row in journal.db.execute('SELECT at,symbol,kind,payload FROM events ORDER BY id'):
        at=row['at'];symbol=row['symbol'];kind=row['kind'];p=json.loads(row['payload']);m=metric(at,symbol)
        if kind=='runtime_start':
            runs[p['boot']]=dict(start=at,last=at,mono=p.get('observed_monotonic'),quoted=set(),symbols=p['symbols'],modeled=p.get('modeled',False))
            for name in p['symbols']:metric(at,name)
        elif kind in ('runtime_observation','runtime_end'):
            run=runs.get(p['boot'])
            if run:
                end=run['last']+max(0,p['observed_monotonic']-run['mono']) if p.get('observed_monotonic') is not None and run['mono'] is not None else at
                for name in run['symbols']:
                    interval(run['last'],end,name,quoted=name in run['quoted'],modeled=run['modeled'])
                run['last']=at;run['mono']=p.get('observed_monotonic');run['quoted']=set(p.get('quoted_symbols',[]))
        elif kind=='market_stale':
            stales[p.get('boot'),symbol,p['venue']]=p
            m['stale_counts'][p['reason']]+=1
            episode=(p.get('boot'),p['episode'])
            if episode not in global_episodes:
                metric(at,'*')['stale_counts'][p['reason']]+=1;global_episodes.add(episode)
        elif kind=='market_recovered':
            old=stales.pop((p.get('boot'),symbol,p['venue']),None)
            if old:stale_interval(old,symbol,old['since_wall']+p['duration_seconds'] if 'duration_seconds' in p else at)
        elif kind=='open_pair_confirmed':
            q=D(p['qty']);m['open_qty']+=q
            if p['arcus_average'] is not None and p['lighter_average'] is not None:
                a=D(p['arcus_average']);l=D(p['lighter_average']);m['open_num']+=p['side']*q*(l-a)
                m['open_den']+=q*a;m['open_priced']+=q
        elif kind=='close_fill_enriched':
            completed=p['roundtrip_completed'] and p['obligation'] not in completed_obligations
            m['roundtrips']+=int(completed);m['gross']+=D(p['gross_pnl_usd_known'])
            if completed:
                completed_obligations.add(p['obligation'])
                mode=p.get('close_mode')
                if mode is None:
                    maker=journal.db.execute('SELECT payload FROM events WHERE id=?',(p['maker_event_id'],)).fetchone()
                    mode=json.loads(maker[0]).get('close_mode') if maker else None
                if mode in ('floor','timeout','cost_unknown'):m['close_'+mode+'_count']+=1
            m['confirmed']+=D(p['confirmed_qty']);m['priced']+=D(p['priced_qty']);m['restored']+=D(p['restored_qty'])
            m['close_qty']+=D(p['confirmed_qty'])
            for t in p['tranches']:
                q=D(t['qty'])
                if t['hold_qty_seconds'] is not None:m['hold_cost']+=D(t['hold_qty_seconds']);m['hold_qty']+=q
                if t['close_arcus_average'] is not None and t['close_lighter_average'] is not None:
                    a=D(t['close_arcus_average']);l=D(t['close_lighter_average'])
                    # A closing side is opposite its original Arcus lot direction.
                    side=p.get('close_side')
                    if side is not None:
                        m['close_num']+=side*q*(l-a);m['close_den']+=q*a;m['close_priced']+=q
        elif kind=='maker_fill' and p.get('purpose')=='close':
            confirmed=D(p.get('roundtrip_confirmed_qty','0'));restored=D(p.get('roundtrip_restored_qty','0'))
            m['pending_close_qty']+=max(D(0),D(p['qty'])-confirmed-restored)
            m['unpriced_closed_qty']+=confirmed-D(p.get('roundtrip_priced_qty','0'))
        elif kind=='hedge_fill':
            q=D(p['qty']);m['slip_qty']+=q
            if p.get('average') is not None and p.get('reference') is not None:
                reference=D(p['reference']);m['slip_num']+=p['side']*q*(D(p['average'])-reference)
                m['slip_den']+=q*reference;m['slip_priced']+=q
    for (boot,symbol,venue),p in stales.items():
        run=runs.get(p.get('boot'))
        if run:stale_interval(p,symbol,run['last'])
    def union_seconds(values):
        total=0.0;end=None
        for a,b in sorted(values):
            total+=max(0,b-max(a,end if end is not None else a));end=max(b,end if end is not None else b)
        return total
    for (stamp,symbol),m in metrics.items():
        seconds={r:union_seconds(ranges[stamp,symbol,r]) for r in m['stale_counts']}
        m['stale_seconds_by_reason']=seconds
        m['stale_events_by_reason']=dict(m['stale_counts']);m['stale_events']=sum(m['stale_counts'].values())
        m['stale_seconds']=union_seconds([r for reason in seconds for r in ranges[stamp,symbol,reason]])
        m['quote_uptime_pct']=100*m['quoted_seconds']/m['observation_seconds'] if m['observation_seconds'] else None
        m['roundtrip_gross_pnl_usd_known']=str(m['gross'])
        m['roundtrip_gross_pnl_usd']=str(m['gross']) if m['confirmed']==m['priced'] else None
        m['roundtrip_pnl_usd']=m['roundtrip_gross_pnl_usd']
        m['roundtrip_price_coverage']=f'{m["priced"]}/{m["confirmed"]}'
        m['roundtrip_confirmed_qty']=str(m['confirmed']);m['roundtrip_restored_qty']=str(m['restored'])
        m['pending_close_qty']=str(m['pending_close_qty']);m['unpriced_closed_qty']=str(m['unpriced_closed_qty'])
        m['avg_hold_seconds']=float(m['hold_cost']/m['hold_qty']) if m['hold_qty'] else None
        m['hold_time_coverage']=f'{m["hold_qty"]}/{m["confirmed"]}'
        for name in ('open','close','slip'):
            key='hedge_slippage_pct' if name=='slip' else name+'_edge_pct'
            m[key]=str(100*m[name+'_num']/m[name+'_den']) if m[name+'_den'] else None
            m[key+'_coverage']=f'{m[name+"_priced"]}/{m[name+"_qty"]}'
        for key in ('gross','confirmed','priced','restored','hold_cost','hold_qty',
                    'open_num','open_den','open_qty','open_priced','close_num','close_den','close_qty','close_priced',
                    'slip_num','slip_den','slip_qty','slip_priced'):m.pop(key)
    return metrics


def statistics(journal,period='hour'):
    diagnostics=diagnostic_statistics(journal,period)
    groups=defaultdict(list)
    actual=defaultdict(list)
    funding_rows=defaultdict(list)
    event_times=defaultdict(list)
    for row in journal.db.execute('SELECT at,symbol,kind,payload FROM events ORDER BY id'):
        stamp=datetime.fromtimestamp(row['at'],timezone.utc)
        bucket=stamp.strftime('%Y-%m-%dT%H:00Z' if period=='hour' else '%Y-%m-%d')
        groups[bucket,row['symbol']].append((row['kind'],json.loads(row['payload'])))
        if row['kind'] in ('maker_fill','hedge_fill','emergency_fill'):
            event_times[bucket,row['symbol']].append(row['at'])
    for table,target in (('executions',actual),('funding',funding_rows)):
        for row in journal.db.execute('SELECT * FROM '+table):
            stamp=datetime.fromtimestamp(row['at'],timezone.utc)
            bucket=stamp.strftime('%Y-%m-%dT%H:00Z' if period=='hour' else '%Y-%m-%d')
            target[bucket,row['symbol']].append(dict(row))
            groups.setdefault((bucket,row['symbol']),[])
    # Attribute each execution interval FIFO across its attempt allocations, across time buckets too.
    reference_cost={}; consumed=defaultdict(lambda:D(0))
    for row in journal.db.execute('SELECT * FROM executions ORDER BY at,venue,identity'):
        if row['kind'] not in ('hedge_fill','emergency_fill'):continue
        start=consumed[row['client']]; end=start+D(row['qty']); consumed[row['client']]=end
        offset=D(0); covered=D(0); cost=D(0); known=True
        for allocation in journal.allocations(row['client']):
            stop=offset+D(allocation['filled'])
            part=max(D(0),min(end,stop)-max(start,offset))
            if part:
                covered+=part
                if allocation['reference'] is None:known=False
                else:cost+=part*(-allocation['side'])*(D(row['price'])-D(allocation['reference']))
            offset=stop
        reference_cost[row['venue'],row['identity']]=cost if known and covered==D(row['qty']) else None
    output=[]
    for key in diagnostics:groups.setdefault(key,[])
    for (bucket,symbol),events in sorted(groups.items()):
        maker=lighter=emergency=D(0); known_fee=D(0); slippage=D(0)
        fills=fees=valued=slipped=0; latency=[]; funding=None; modeled=False; peak=D(0)
        for kind,p in events:
            modeled=modeled or p.get('modeled',False)
            if kind=='exposure': peak=max(peak,D(p['peak']))
            if kind=='funding':
                if p.get('cost') is not None: funding=(funding or D(0))+D(p['cost'])
            if kind not in ('maker_fill','hedge_fill','emergency_fill'): continue
            fills+=1
            if p.get('average') is not None:
                notional=D(p['qty'])*D(p['average']); valued+=1
                if kind=='maker_fill': maker+=notional
                elif kind=='hedge_fill': lighter+=notional
                else: emergency+=notional
            if p.get('fee') is not None:
                known_fee+=D(p['fee']);fees+=1
            if kind in ('hedge_fill','emergency_fill') and p.get('average') is not None and p.get('reference') is not None:
                slippage+=D(p['qty'])*D(p['side'])*(D(p['average'])-D(p['reference'])); slipped+=1
            if kind=='hedge_fill' and p.get('latency_ms') is not None and p.get('clock')=='process_monotonic':
                latency.append(float(p['latency_ms']))
        if not fills and not events and (bucket,symbol) not in diagnostics: continue
        hedge_count=sum(k in ('hedge_fill','emergency_fill') for k,p in events)
        complete=bool(fills) and fees==fills and valued==fills and slipped==hedge_count and funding is not None and not modeled
        actual_rows=actual[bucket,symbol]
        actual_complete=False
        maker_role_coverage=None
        if actual_rows:
            expected=defaultdict(lambda:D(0)); observed=defaultdict(lambda:D(0))
            for k,p in events:
                if k in ('maker_fill','hedge_fill','emergency_fill'):expected[k]+=D(p['qty'])
            maker=lighter=emergency=known_fee=slippage=D(0);fees=0;reference_complete=True
            for row in actual_rows:
                observed[row['kind']]+=D(row['qty']);volume=D(row['qty'])*D(row['price'])
                if row['kind']=='maker_fill':maker+=volume
                elif row['kind']=='hedge_fill':lighter+=volume
                else:emergency+=volume
                if row['fee'] is not None and row['currency']=='USDC':known_fee+=D(row['fee']);fees+=1
                if row['kind'] in ('hedge_fill','emergency_fill'):
                    cost=reference_cost.get((row['venue'],row['identity']))
                    if cost is None:reference_complete=False
                    else:slippage+=cost
            slipped=hedge_count if reference_complete else -1
            actual_complete=(dict(expected)==dict(observed) and fees==len(actual_rows))
            times=event_times[bucket,symbol]
            funding_coverage=[]
            for venue in ('arcus','lighter'):
                row=journal.db.execute("SELECT * FROM coverage WHERE venue=? AND kind='funding'",(venue,)).fetchone()
                funding_coverage.append(bool(row and row['complete'] and times and row['start']<=min(times) and row['end']>=max(times)))
            funding=sum((D(r['cost']) for r in funding_rows[bucket,symbol]),D(0)) if all(funding_coverage) else None
            fills=len(actual_rows)
            valued=fills if actual_complete else 0
            complete=actual_complete and all(funding_coverage) and slipped==hedge_count and not modeled
            makers=[r for r in actual_rows if r['kind']=='maker_fill']
            maker_role_coverage=(sum(r['role']=='MAKER' for r in makers),len(makers))
        known=known_fee+slippage+(funding or D(0))
        volume=maker+lighter+emergency
        item={'period':bucket,'symbol':symbol,'modeled':modeled,
            'arcus_maker_notional':str(maker),'lighter_notional':str(lighter),'arcus_emergency_notional':str(emergency),
            'combined_venue_notional':str(volume),'actual_fee_known':str(known_fee),'fee_coverage':f'{fees}/{fills}',
            'valuation_coverage':f'{valued}/{fills}','signed_slippage_known':str(slippage),
            'actual_execution_coverage_complete':actual_complete,
            'maker_role_coverage':f'{maker_role_coverage[0]}/{maker_role_coverage[1]}' if maker_role_coverage else None,
            'funding_by_venue_known':{v:str(sum((D(r['cost']) for r in funding_rows[bucket,symbol] if r['venue']==v),D(0)))
                                      for v in ('arcus','lighter')},
            'finance_coverage':{r['venue']+':'+r['kind']:{'complete':bool(r['complete']),'reason':r['reason']}
                                for r in journal.db.execute('SELECT * FROM coverage')},
            'actual_funding':str(funding) if funding is not None else None,'total_cost':str(known) if complete else None,
            'cost_status':'complete' if complete else 'incomplete_or_modeled',
            'cost_per_10000_combined_volume':str(known/volume*10000) if complete and volume else None,
            'cost_per_10000_maker_volume':str(known/maker*10000) if complete and maker else None,
            'hedge_latency_ms_p50':percentile(latency,.5),'hedge_latency_ms_p95':percentile(latency,.95),
            'hedge_latency_samples':len(latency),'peak_uncovered_notional':str(peak),
            'requote_attempts':sum(k=='requote_attempt' for k,p in events),
            'successful_requotes':sum(k=='requote_success' for k,p in events)}
        item.update(diagnostics.get((bucket,symbol),{}))
        denominator=maker+emergency
        gross=item.get('roundtrip_gross_pnl_usd')
        item['pnl_per_10k_arcus_denominator']=str(denominator)
        arcus_events=[(k,p) for k,p in events if k in ('maker_fill','emergency_fill')]
        if actual_rows:
            expected=defaultdict(lambda:D(0));observed=defaultdict(lambda:D(0))
            for k,p in arcus_events:expected[k]+=D(p['qty'])
            for r in actual_rows:
                if r['venue']=='arcus':observed[r['kind']]+=D(r['qty'])
            denominator_complete=dict(expected)==dict(observed)
        else:denominator_complete=all(p.get('average') is not None for _,p in arcus_events)
        item['pnl_per_10k_arcus_denominator_complete']=denominator_complete
        item['pnl_per_10k_arcus']=str(D(gross)/denominator*10000) if (gross is not None and denominator and denominator_complete
            and D(item.get('roundtrip_confirmed_qty','0'))>0) else None
        output.append(item)
    return output


def statistics_snapshot(path,period='hour'):
    """Worker owns its read-only connection and one coherent WAL snapshot."""
    import sqlite3
    from pathlib import Path
    from ..execution.maker_taker_journal import Journal
    db=sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True,isolation_level=None)
    db.row_factory=sqlite3.Row
    reader=object.__new__(Journal);reader.db=db
    try:
        db.execute('BEGIN')
        return statistics(reader,period)
    finally:
        db.close()


async def statistics_background(path,period='hour',*,timeout=300):
    """A fresh interpreter avoids history projection holding the trading process's GIL."""
    import asyncio
    import os
    import sys
    from pathlib import Path
    if period not in ('hour','day'):raise ValueError('Statistics period must be hour or day')
    worker='''import contextlib,json,sys
with contextlib.redirect_stdout(sys.stderr):
    from core.services.arbitrage_monitor_v2.history.maker_taker_statistics import statistics_snapshot
    result=statistics_snapshot(sys.argv[1],sys.argv[2])
print(json.dumps(result))
'''
    process=await asyncio.create_subprocess_exec(os.path.abspath(sys.executable),'-c',worker,str(Path(path).resolve()),period,
        cwd=str(Path(__file__).resolve().parents[4]),env={'PATH':os.defpath,'PYTHONDONTWRITEBYTECODE':'1'},
        stdin=asyncio.subprocess.DEVNULL,stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.PIPE,close_fds=True)

    async def drain_errors():
        captured=bytearray()
        while chunk:=await process.stderr.read(16384):
            captured.extend(chunk[:max(0,4096-len(captured))])
        return captured

    async def output():
        stdout,_=await asyncio.gather(process.stdout.read(),drain_errors())
        await process.wait()
        return stdout

    reader=asyncio.create_task(output())
    try:
        stdout=await asyncio.wait_for(asyncio.shield(reader),timeout)
        if process.returncode:
            raise RuntimeError(f'Statistics worker failed with exit status {process.returncode}')
        result=json.loads(stdout)
        if not isinstance(result,list):raise ValueError('Statistics worker returned an invalid report')
        return result
    finally:
        if process.returncode is None:
            try:process.terminate()
            except ProcessLookupError:pass
            try:await asyncio.wait_for(process.wait(),2)
            except TimeoutError:
                try:process.kill()
                except ProcessLookupError:pass
                await asyncio.wait_for(process.wait(),2)
        await asyncio.gather(reader,return_exceptions=True)
