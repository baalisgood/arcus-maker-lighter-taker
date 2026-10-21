"""Current Arcus FIFO lots and unsettled undo, projected from committed journal events."""
from copy import deepcopy
from collections import deque
from decimal import Decimal
import json
import math
from .maker_taker_diagnostics import CloseDiagnostics

D=Decimal
KINDS={'maker_fill','hedge_fill','emergency_fill','reconcile_drift','close_floor_unattributed'}


def amount(value):
    result=D(str(value))
    if not result.is_finite():raise ValueError('Invalid close basis amount')
    return result


def price(value):
    if value is None:return None
    value=amount(value)
    if value<=0:raise ValueError('Invalid close basis price')
    return value


def size(lot):return lot['hi']-lot['lo']


def section(lot,lo,hi):return {**lot,'lo':lo,'hi':hi}


def lighter_cost(lot,lo,hi):
    covered=D(0);cost=D(0)
    for start,end,average in lot['source']['fills']:
        part=max(D(0),min(end,hi)-max(start,lo))
        if part:
            if average is None:return None
            covered+=part;cost+=part*average
    return cost if covered==hi-lo else None


def cost_portions(lot,lo,hi):
    cursor=lo;previous=D(0)
    for start,end,average in lot['source']['fills']:
        if (not start.is_finite() or not end.is_finite() or start<previous or end<=start
                or (average is not None and (not average.is_finite() or average<=0))):
            raise ValueError('Invalid close basis hedge coverage')
        previous=end
        start=max(lo,start);end=min(hi,end)
        if start>=end:continue
        if cursor<start:yield cursor,start,None
        yield start,end,None if average is None else (end-start)*average
        cursor=end
    if cursor<hi:yield cursor,hi,None


def empty():return {'arcus':D(0),'lighter':D(0),'lots':deque(),'pending':{}}


def take(lots,qty):
    """Remove FIFO Arcus quantities, retaining immutable origin ranges for undo."""
    removed=[]
    while qty and lots:
        lot=lots.popleft();part=min(qty,size(lot))
        removed.append(section(lot,lot['lo'],lot['lo']+part))
        if part<size(lot):lots.appendleft(section(lot,lot['lo']+part,lot['hi']))
        qty-=part
    if qty:raise ValueError('Close basis FIFO does not cover Arcus inventory')
    return removed


def insert(lots,lot):
    # A restoration can net a subsequent opposite fill; preserve only its remaining origin.
    if lots and lots[0]['side']!=lot['side']:
        qty=min(size(lot),sum((size(x) for x in lots),D(0)))
        take(lots,qty);lot=section(lot,lot['lo']+qty,lot['hi'])
    if size(lot):
        lots.append(lot);ordered=sorted(lots,key=lambda x:(x['origin'],x['lo']))
        lots.clear();lots.extend(ordered)


def remove_origin(lots,origin,lo,hi):
    result=deque()
    for lot in lots:
        start=max(lo,lot['lo']);end=min(hi,lot['hi'])
        if lot['origin']!=origin or start>=end:
            result.append(lot);continue
        if lot['lo']<start:result.append(section(lot,lot['lo'],start))
        if end<lot['hi']:result.append(section(lot,end,lot['hi']))
    return result


class CloseBasis:
    def __init__(self,journal,symbols):
        self.journal=journal;self.symbols=tuple(symbols);self.cursor=0;self.states={}
        self.diagnostics=CloseDiagnostics(journal)
        self.sync()

    def _apply(self,states,row):
        if row['kind'] not in KINDS:return
        symbol=row['symbol'];s=states.setdefault(symbol,empty());p=json.loads(row['payload'])
        at=float(row['at'])
        if not math.isfinite(at):raise ValueError('Invalid close basis event time')
        kind=row['kind']
        if kind in ('reconcile_drift','close_floor_unattributed'):
            s=empty();states[symbol]=s
            s['arcus']=amount(p['arcus']);s['lighter']=amount(p['lighter'])
            if s['arcus']:
                s['lots']=deque([dict(origin=row['id'],lo=D(0),hi=abs(s['arcus']),side=1 if s['arcus']>0 else -1,
                                      arcus=None,source={'fills':[]},at=at,attributed=False)])
            return
        qty=amount(p['qty']);side=p['side'];purpose=p['purpose'];oid=p['obligation']
        if qty<=0 or side not in (-1,1):raise ValueError('Invalid close basis fill')
        if kind=='maker_fill':
            before=s['arcus'];s['arcus']+=side*qty
            closed=min(qty,abs(before)) if before*side<0 else D(0)
            removed=take(s['lots'],closed)
            source={'fills':[]}
            pending=dict(total=qty,hedged=D(0),unwound=D(0),origin=row['id'],side=side,purpose=purpose,removed=[],source=source)
            pending.update(maker_event_id=row['id'],maker_at=at,average=price(p.get('average')),client=p.get('client'))
            offset=D(0)
            for lot in removed:
                pending['removed'].append((offset,lot));offset+=size(lot)
            if qty>closed:
                s['lots'].append(dict(origin=row['id'],lo=closed,hi=qty,side=side,
                                      arcus=price(p.get('average')),source=source,at=at))
            s['pending'][oid]=pending
            return [{'kind':'close_pending','payload':{'client':p['client']}}] if purpose=='close' else None
        if kind=='hedge_fill':s['lighter']+=side*qty
        else:s['arcus']+=side*qty
        pending=s['pending'].get(oid)
        if pending is None:
            # Legacy/reconciliation quantities have no provable opening cost.
            return
        if pending['hedged']+pending['unwound']+qty>pending['total']:
            raise ValueError('Close basis fill exceeds its obligation')
        if kind=='hedge_fill':
            lo=pending['hedged'];hi=lo+qty;pending['hedged']=hi
        else:
            # Unwind the unpaired new tail before restoring the offset old FIFO head.
            hi=pending['total']-pending['unwound'];lo=hi-qty;pending['unwound']+=qty
        if kind=='hedge_fill' and purpose=='open':
            # An opening hedge may arrive after those Arcus lots were provisionally closed.
            pending['source']['fills'].append((lo,hi,price(p.get('average'))))
        facts=[]
        if kind=='hedge_fill' and purpose=='open':
            facts.append({'kind':'open_pair_confirmed','payload':dict(source_event_id=row['id'],obligation=oid,
                qty=qty,side=pending['side'],arcus_average=pending['average'],lighter_average=price(p.get('average')),
                reference=p.get('reference'))})
        if purpose=='close':
            tranches=[];priced=gross=notional=D(0)
            if kind=='hedge_fill':
                for offset,lot in pending['removed']:
                    start=max(lo,offset);end=min(hi,offset+size(lot))
                    if start>=end:continue
                    origin_lo=lot['lo']+start-offset;origin_hi=origin_lo+end-start;part=end-start
                    for origin_lo,origin_hi,cost in cost_portions(lot,origin_lo,origin_hi):
                        part=origin_hi-origin_lo
                        known=cost is not None and lot['arcus'] is not None and pending['average'] is not None and p.get('average') is not None
                        pnl=lot['side']*(part*(pending['average']-lot['arcus']-price(p['average']))+cost) if known else None
                        entry=part*lot['arcus'] if known else None
                        if known:priced+=part;gross+=pnl;notional+=entry
                        tranches.append(dict(origin=lot['origin'],origin_lo=origin_lo,origin_hi=origin_hi,qty=part,
                            open_at=lot['at'] if lot.get('attributed',True) else None,
                            hold_qty_seconds=part*D(str(max(0,pending['maker_at']-lot['at']))) if lot.get('attributed',True) else None,
                            gross_pnl_usd=pnl,entry_notional=entry,open_arcus_average=lot['arcus'],
                            open_lighter_average=cost/part if cost is not None else None,
                            close_arcus_average=pending['average'],close_lighter_average=price(p.get('average'))))
            facts.append({'kind':'close_fill_enriched','payload':dict(maker_event_id=pending['maker_event_id'],
                hedge_event_id=row['id'],obligation=oid,confirmed_qty=qty if kind=='hedge_fill' else D(0),
                priced_qty=priced,restored_qty=qty if kind=='emergency_fill' else D(0),
                gross_pnl_usd_known=gross,entry_notional_known=notional,tranches=tranches,
                close_side=pending['side'],
                fully_resolved=pending['hedged']+pending['unwound']==pending['total'],
                roundtrip_completed=pending['hedged']+pending['unwound']==pending['total'] and pending['hedged']>0)})
        if kind=='emergency_fill':
            if purpose=='open':
                s['lots']=remove_origin(s['lots'],pending['origin'],lo,hi)
            for offset,lot in pending['removed']:
                start=max(lo,offset);end=min(hi,offset+size(lot))
                if start<end:insert(s['lots'],section(lot,lot['lo']+start-offset,lot['lo']+end-offset))
        # Completed close portions no longer need their original cost/age undo.
        remaining=[]
        for offset,lot in pending['removed']:
            start=max(offset,pending['hedged']);end=min(offset+size(lot),pending['total']-pending['unwound'])
            if start<end:remaining.append((start,section(lot,lot['lo']+start-offset,lot['lo']+end-offset)))
        pending['removed']=remaining
        if pending['hedged']+pending['unwound']==pending['total']:del s['pending'][oid]
        return facts

    def sync(self):
        # The main connection must never expose an uncommitted fill to the projection.
        if self.journal.db.in_transaction:return
        cursor=self.cursor;projected=None
        while True:
            rows=self.journal.db.execute('SELECT id,at,symbol,kind,payload FROM events WHERE id>? ORDER BY id LIMIT 512',
                                         (cursor,)).fetchall()
            if not rows:break
            diagnostic=[]
            for row in rows:
                facts=None
                if row['kind'] in KINDS:
                    if projected is None:projected=deepcopy(self.states)
                    facts=self._apply(projected,row)
                diagnostic.append((row,facts))
            cursor=rows[-1]['id']
            self.diagnostics.write(diagnostic,cursor)
        if projected is not None:
            self._prune(projected);self.states=projected
        self.cursor=cursor
        for symbol in self.symbols:
            actual=self.journal.inventory(symbol);s=self.states.get(symbol,empty())
            coherent=(projected is None or (sum((size(lot)*lot['side'] for lot in s['lots']),D(0))==s['arcus']
                       and all(lot['side']*s['arcus']>0 for lot in s['lots'])))
            if actual==(s['arcus'],s['lighter']) and coherent:continue
            if not any(actual):
                self.states[symbol]=empty();continue
            # A missing source cannot borrow today's price or another cycle's age.
            with self.journal.transaction():
                self.journal.event(symbol,'close_floor_unattributed',arcus=actual[0],lighter=actual[1])
            self.sync();return

    def _prune(self,states):
        # Retain fill prices only for quantities still in inventory or an unsettled undo.
        sources={}
        for s in states.values():
            lots=list(s['lots'])
            for pending in s['pending'].values():
                source=pending['source'];sources[id(source)]=[source,pending['total']]
                lots.extend(lot for _,lot in pending['removed'])
            for lot in lots:
                source=lot['source'];entry=sources.setdefault(id(source),[source,lot['lo']])
                entry[1]=min(entry[1],lot['lo'])
        for source,start in sources.values():
            source['fills']=[(max(lo,start),hi,average) for lo,hi,average in source['fills'] if hi>start]

    def portions(self,symbol,side,qty,timeout,now):
        """Partition selected FIFO at actual hedge coverage boundaries, including gaps."""
        self.sync();s=self.states.get(symbol,empty())
        remaining=qty;selected=[]
        for lot in s['lots']:
            part=min(remaining,size(lot));remaining-=part
            if part:
                if lot['side']!=-side:return None
                lo=lot['lo'];hi=lo+part
                for start,end,cost in cost_portions(lot,lo,hi):
                    selected.append((lot,end-start,cost,timeout>0 and now-lot['at']<timeout))
            if not remaining:break
        return None if remaining or qty<=0 else selected

    def bound(self,symbol,side,qty,current_lighter,profit,timeout,now):
        """Unknown/expired portions never dilute the fresh actual-cost bound."""
        portions=self.portions(symbol,side,qty,timeout,now)
        if portions is None:return False,None
        selected=[(q,lot['arcus'],cost) for lot,q,cost,fresh in portions
                  if fresh and lot['arcus'] is not None and cost is not None]
        total=sum((q for q,_,_ in selected),D(0))
        if not total:return True,None
        a=sum((q*a for q,a,_ in selected),D(0))/total
        l=sum((cost for _,_,cost in selected),D(0))/total
        return True,a+current_lighter-l-D(side)*profit*a/100

    def describe(self,symbol,side,qty,current_lighter,close_price,timeout,now):
        portions=self.portions(symbol,side,qty,timeout,now)
        entry=gross=protected=unknown=expired=priced=D(0);known=portions is not None;age_known=True;open_at=None
        for lot,part,lcost,fresh in portions or ():
            if lot.get('attributed',True):open_at=min(open_at,lot['at']) if open_at is not None else lot['at']
            else:age_known=False
            actual=lot['arcus'] is not None and lcost is not None
            if not fresh:expired+=part
            elif actual:protected+=part
            else:unknown+=part
            if not actual:known=False
            else:
                priced+=part;entry+=part*lot['arcus']
                gross+=lot['side']*(part*(close_price-lot['arcus']-current_lighter)+lcost)
        return dict(mode='cost_unknown' if unknown else 'floor' if protected else 'ema',
                    protected_qty=str(protected),cost_unknown_qty=str(unknown),expired_qty=str(expired),
                    priced_qty=str(priced),price_coverage=f'{priced}/{qty}',
                    estimated_pnl_pct=str(100*gross/entry) if known and entry else None,
                    open_at=open_at if age_known else None,
                    hold_seconds=max(0,now-open_at) if age_known and open_at is not None else None)

    def timeout_estimates(self,symbol,current_lighter,close_price):
        totals={}
        for lot in self.states.get(symbol,empty())['lots']:
            value=totals.setdefault(lot['origin'],[D(0),D(0),True]);qty=size(lot)
            cost=lighter_cost(lot,lot['lo'],lot['hi'])
            if lot['arcus'] is None or cost is None:value[2]=False
            else:
                value[0]+=lot['side']*(qty*(close_price-lot['arcus']-current_lighter)+cost)
                value[1]+=qty*lot['arcus']
        return {origin:str(100*gross/entry) if known and entry else None for origin,(gross,entry,known) in totals.items()}
