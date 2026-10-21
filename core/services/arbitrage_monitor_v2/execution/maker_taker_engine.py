"""Arcus maker / Lighter taker state machine. All transports are injected."""
import asyncio
import sys
import time
from collections import defaultdict, deque
from dataclasses import dataclass, replace
from decimal import Decimal

from .maker_taker_journal import TERMINAL
from .maker_taker_close_basis import CloseBasis
from .maker_taker_quotes import floor_quantity, quote_price, should_requote, hedge_quantity

from ..utils.ema_spread_gate import TimeWeightedEma

D = Decimal


class DeferredHedge(Exception):
    """No request signed: price/size currently below conservative policy."""


class DefinitiveFailure(Exception):
    """Transport proves the request was rejected before acceptance."""


@dataclass(frozen=True)
class OrderObservation:
    venue: str
    account: str
    symbol: str
    client: str
    order_id: str
    side: int
    requested: D
    cumulative: D
    status: str
    source: str
    received: float
    sequence: str = ''
    average: D | None = None
    fee: D | None = None
    trade_id: str | None = None
    remaining: D | None = None
    reported_status: str | None = None
    startup_snapshot: bool = False
    startup_provisional: bool = False


@dataclass(frozen=True)
class Book:
    bid: D
    ask: D
    bid_size: D
    ask_size: D
    received: float
    generation: int
    bids: tuple = ()
    asks: tuple = ()

    def __post_init__(self):
        if (any(not x.is_finite() or x <= 0 for x in (self.bid,self.ask,self.bid_size,self.ask_size))
                or self.bid >= self.ask):
            raise ValueError('Invalid book')
        for name,best,size in (('bids',self.bid,self.bid_size),('asks',self.ask,self.ask_size)):
            levels=tuple((D(p),D(q)) for p,q in getattr(self,name))
            if (any(not p.is_finite() or not q.is_finite() or min(p,q)<=0 for p,q in levels)
                    or (levels and levels[0]!=(best,size))
                    or any(a[0]<=b[0] if name=='bids' else a[0]>=b[0] for a,b in zip(levels,levels[1:]))):
                raise ValueError('Invalid book depth')
            object.__setattr__(self,name,levels)


class MakerTakerEngine:
    def __init__(self, profile, journal, transport, rules, *, clock=time.monotonic):
        self.profile, self.journal, self.transport, self.rules = profile, journal, transport, rules
        self.clock = clock
        self.state = 'STARTING'
        self.reason = ''
        self.halt_reason = ''
        self.halt_state = ''
        self.halt_symbol = ''
        self.halt_error_type = ''
        self.last_alarm = ''
        self.books = {}
        self.basis = {}
        self.basis_samples = {}
        self.warmup_started = {}
        self.generations = defaultdict(int)
        self.queues = {s: asyncio.Queue() for s in profile.symbols}
        self.symbol_locks = {s: asyncio.Lock() for s in profile.symbols}
        self.quote_locks = {s: asyncio.Lock() for s in profile.symbols}
        self.capital_lock = asyncio.Lock()
        self.cancel_lock = asyncio.Lock()
        self.reconcile_lock = asyncio.Lock()
        self.equity = {}
        self.tasks = []
        self.cancel_tasks=set()
        self.hedges_inflight=set()
        self.close_tail_reasons={}
        self.requotes = defaultdict(deque)
        self.last_reconcile = 0
        self.sequence = 0
        self.stopping = False
        self._startup_window = True
        self._recovery_started = False
        self._startup_recovered = False
        self._startup_foreign = {}
        self._startup_terminal = {}
        self._legacy_blocked = False
        self.halted = False
        self.cancel_failed = False
        self.peak_exposure = defaultdict(lambda: D(0))
        self.breach_since = {}
        self._boot_at = time.monotonic()
        self._alarm_last = {}
        self._exposure_last = {}
        self.close_basis=CloseBasis(journal,profile.symbols)
        self.close_bounds={}
        self.close_waits={}
        self.close_permits={}
        self.close_prepare_versions=defaultdict(int)
        self.wall_clock=time.time
        self.manual_intervention_snapshot=journal.manual_intervention()
        self.manual_intervention_required=self.manual_intervention_snapshot is not None
        self.manual_intervention_persisted=self.manual_intervention_required
        self.transport.maker_guard=self.maker_admitted
        self.transport.send_guard=self.sends_allowed
        self.basis_pauses={};self.basis_revisions={}
        source=getattr(transport,'source',None)
        if source is not None and hasattr(source,'book_validity'):
            source.stale_seconds=profile.risk.stale_book_seconds
            source.quick_resume_seconds=profile.maker.quick_resume_seconds
        if hasattr(transport,'configure_liveness'):
            transport.configure_liveness(profile.risk.stale_book_seconds,profile.maker.quick_resume_seconds)
        self.market_stale={};self._market_alarm_last={};self._book_diagnostics={};self._runtime_last=0;self._runtime_quoted=None
        self.journal.event('*','runtime_start',boot=journal.boot,symbols=list(profile.symbols),modeled=journal.modeled,
                           observed_monotonic=self.clock())
        self.observe_runtime(force=True)
        if self.manual_intervention_required:
            self.halt('EMERGENCY',self.manual_intervention_snapshot['reason'],self.manual_intervention_snapshot['failure_symbol'],record=False)

    def sends_allowed(self):
        return not self.manual_intervention_required

    def block_serious_sends(self):
        self.manual_intervention_required=True
        self.halted=True;self.state='EMERGENCY';self.close_permits.clear()

    def serious_halt(self,reason_code,symbol,unresolved,*,error_type='',failure_detail=None,accounting_error_type=''):
        self.block_serious_sends()
        reasons={'retry_exhausted':'Lighter confirmed zero-fill failure budget exhausted; manual intervention required',
                 'emergency_failed':'Emergency action incomplete; positions preserved for manual intervention',
                 'emergency_residual':'Prior Arcus emergency residual requires manual intervention',
                 'residual_failure':'Confirmed severe residual failure; classification unavailable; manual intervention required'}
        inventory_error=None
        try:arcus,lighter=self.journal.inventory(symbol)
        except Exception as exc:arcus=lighter=None;inventory_error=exc
        snapshot=dict(reason_code=reason_code,reason=reasons[reason_code],failure_symbol=symbol,
            arcus_qty=str(arcus) if arcus is not None else None,lighter_qty=str(lighter) if lighter is not None else None,
            net_exposure_qty=str(arcus+lighter) if inventory_error is None else None,
            inventory_coverage='confirmed_ledger' if inventory_error is None else 'unavailable',
            unresolved_qty=str(unresolved) if unresolved is not None else None,unresolved_qty_known=unresolved is not None,
            error_type=error_type,
            at=self.wall_clock(),boot=self.journal.boot,policy='cancel_makers_preserve_positions',manual_intervention_required=True)
        if failure_detail is not None:snapshot['failure_detail']=failure_detail
        if inventory_error is not None:snapshot['inventory_error_type']=type(inventory_error).__name__
        if accounting_error_type:snapshot['failure_accounting_error_type']=accounting_error_type
        if self.manual_intervention_snapshot is None:self.manual_intervention_snapshot=snapshot
        try:self.manual_intervention_snapshot=self.journal.latch_manual_intervention(self.manual_intervention_snapshot)
        except Exception as exc:raise RuntimeError('Manual-intervention latch persistence failed; sends remain blocked') from exc
        self.manual_intervention_persisted=True
        self.halt('EMERGENCY',self.manual_intervention_snapshot['reason'],self.manual_intervention_snapshot['failure_symbol'],error_type=error_type,record=False)
        if inventory_error is not None:raise RuntimeError('Manual-intervention inventory snapshot unavailable; sends remain blocked') from inventory_error

    def manual_fill(self,symbol,before,source):
        after=self.journal.inventory(symbol)
        if self.manual_intervention_required and after!=before:
            self.journal.event(symbol,'manual_intervention_late_fill',source=source,arcus_qty=str(after[0]),lighter_qty=str(after[1]),
                net_exposure_qty=str(sum(after)),delta_arcus_qty=str(after[0]-before[0]),delta_lighter_qty=str(after[1]-before[1]))
            self.alarm('Confirmed late fill changed preserved positions; manual reconciliation required',symbol)

    def alarm(self, reason, symbol='*'):
        self.last_alarm = reason
        if not self.halt_reason:self.reason = reason
        key=(symbol,reason,self.state)
        if self.clock()-self._alarm_last.get(key,-1000)<30:
            return
        self._alarm_last[key]=self.clock()
        self.journal.event(symbol, 'alarm', reason=reason, state=self.state)

    def halt(self,state,reason,symbol='*',*,error_type='',record=True):
        self.halted=True
        self.close_permits.clear()
        self.state=state
        if not self.halt_reason:
            self.halt_reason=reason;self.halt_state=state;self.halt_symbol=symbol;self.reason=reason
            self.halt_error_type=error_type
            if record:self.journal.event(symbol,'halt',reason=reason,state=state,error_type=error_type,boot=self.journal.boot)
        self.alarm(reason,symbol)
        for key in list(self.close_waits):self.close_quote_wait(*key,end_reason='halted')

    def receive(self, observation):
        """Socket callback: immutable copy already normalized; no network/hedge awaits."""
        # A clientId match is ours only when the exchange order id agrees (or is not yet known): Arcus history can
        # retain an older order reusing the same amlt-N clientId (e.g. from a previous/reset ledger on this account).
        known=self.journal.db.execute(
            "SELECT 1 FROM all_orders WHERE exchange_id=? OR (client=? AND (exchange_id IS NULL OR exchange_id='' OR ?='')) "
            "UNION ALL SELECT 1 FROM all_attempts WHERE (venue='arcus' AND exchange_id=?) OR "
            "(client=? AND (venue!='arcus' OR exchange_id IS NULL OR exchange_id='' OR ?='')) LIMIT 1",
            (observation.order_id,observation.client,observation.order_id,
             observation.order_id,observation.client,observation.order_id)).fetchone()
        if not known and observation.client:
            reused=self.journal.db.execute('SELECT client,exchange_id FROM all_orders WHERE client=? LIMIT 1',(observation.client,)).fetchone()
            if reused is not None:
                try:self.journal.event(observation.symbol or '*','client_id_reused_by_foreign_order',client=observation.client,
                    ledger_order_id=reused['exchange_id'],observed_order_id=observation.order_id,status=observation.status)
                except Exception:pass
        if not known and any(getattr(observation,k) is None for k in ('side','requested','cumulative')):
            self.halt('UNKNOWN_ORDER','Unknown private order observation',observation.symbol or '*')
            return
        if not known:
            terminal=('filled','canceled','rejected','expired','margin_canceled')
            snapshot=(self._startup_window and observation.startup_snapshot and observation.venue=='arcus'
                      and observation.account==self.transport.identity)
            if (snapshot and observation.startup_provisional and observation.requested.is_finite()
                    and observation.cumulative.is_finite() and 0<=observation.cumulative<=observation.requested):return
            quantity=(observation.remaining is not None and observation.remaining.is_finite()
                      and observation.cumulative.is_finite() and observation.requested.is_finite()
                      and 0<=observation.remaining<=observation.requested
                      and 0<=observation.cumulative<=observation.requested)
            if snapshot and quantity and observation.reported_status in terminal:
                self.journal.event(observation.symbol,'foreign_order_ignored',venue=observation.venue,
                    order_id=observation.order_id,status=observation.status,
                    reported_status=observation.reported_status,cumulative=str(observation.cumulative),
                    remaining=str(observation.remaining))
                self._startup_terminal[observation.order_id]=observation
                return
            if (snapshot and quantity and observation.symbol in self.queues and observation.order_id
                    and observation.status=='open' and observation.reported_status in ('open','placed','partially_filled')
                    and observation.remaining>0 and observation.cumulative==0):
                self._startup_foreign[observation.order_id]=observation
                self.sequence+=1
                return
            self.halt('UNKNOWN_ORDER','Unknown private order observation',observation.symbol)
            return
        observation=self._adopt_ledger_identity(observation)
        if observation.symbol not in self.queues:
            self.halt('UNKNOWN_ORDER','Unconfigured private order observation',observation.symbol)
            return
        self.sequence += 1
        self.queues[observation.symbol].put_nowait(observation)

    def wake_hedge(self,symbol):
        """Every committed exposure path wakes the same worker without awaiting network I/O."""
        self.sequence+=1
        if self.sends_allowed() and self.tasks and not self.queues[symbol].qsize():
            self.queues[symbol].put_nowait(None)

    def book(self, venue, symbol, book):
        if symbol not in self.profile.symbols:
            return
        key = (venue,symbol)
        if book.generation < self.generations[key]:
            return
        if book.generation > self.generations[key]:
            self.generations[key] = book.generation
            self.reset_basis(symbol)
        self.books[key] = book
        self.basis_center(symbol)
        if self.journal.obligations(symbol) and self.tasks and self.queues[symbol].empty():
            self.queues[symbol].put_nowait(None)

    def basis_center(self,symbol):
        now=self.clock()
        pair=[self.books.get((v,symbol)) for v in ('arcus','lighter')]
        if not self.symbol_data_ready(symbol):
            self.pause_basis(symbol)
            return None
        revisions=self.basis_revision()
        if symbol in self.basis_revisions and self.basis_revisions[symbol]!=revisions:self.reset_basis(symbol)
        self.basis_revisions[symbol]=revisions
        paused=self.basis_pauses.pop(symbol,None)
        if paused is not None:
            if now-paused>=self.profile.maker.quick_resume_seconds:self.reset_basis(symbol)
            elif symbol in self.warmup_started:self.warmup_started[symbol]+=now-paused
        sample=tuple((b.received,b.generation,b.bid,b.ask) for b in pair)
        if sample!=self.basis_samples.get(symbol):
            ema=self.basis.setdefault(symbol,TimeWeightedEma(tau_seconds=self.profile.maker.basis_ema_seconds))
            ema.update(float((pair[0].bid+pair[0].ask)/(pair[1].bid+pair[1].ask)*100-100),now=now)
            self.basis_samples[symbol]=sample
            self.warmup_started.setdefault(symbol,now)
        return D(str(self.basis[symbol].value))

    def fair_anchor(self,symbol,side):
        center=self.basis_center(symbol)
        reference=self.reference(symbol,side)
        return None if center is None or reference is None else reference*(1+center/100)


    def reset_basis(self,symbol):
        for key in list(self.close_permits):
            if key[0]==symbol:self.close_permits.pop(key)
        self.basis.pop(symbol,None)
        self.basis_samples.pop(symbol,None)
        self.warmup_started.pop(symbol,None)
        self.basis_pauses.pop(symbol,None)

    def pause_basis(self,symbol):
        for key in list(self.close_permits):
            if key[0]==symbol:self.close_permits.pop(key)
        source=getattr(self.transport,'source',None)
        revisions=self.basis_revision()
        if symbol in self.basis_revisions and self.basis_revisions[symbol]!=revisions:
            self.reset_basis(symbol);return
        if source is not None and hasattr(source,'book_validity'):
            invalid=[source.book_validity(v,symbol,self.books.get((v,symbol))) for v in ('arcus','lighter')]
            if hasattr(self.transport,'private_validity'):invalid.extend(self.transport.private_validity().values())
            invalid=[r for r in invalid if not r[0]]
            if invalid and all(r[1]=='connection_timeout' for r in invalid):
                since=min(r[2] for r in invalid)
                if self.clock()-since<self.profile.maker.quick_resume_seconds:
                    self.basis_pauses.setdefault(symbol,since);return
        self.reset_basis(symbol)

    def basis_revision(self):
        source=getattr(self.transport,'source',None)
        public=tuple(getattr(source,'recovery_revision',{}).get(v,0) for v in ('arcus','lighter'))
        private=self.transport.private_episodes() if hasattr(self.transport,'private_episodes') else ()
        return public+private

    def symbol_data_ready(self,symbol):
        now=self.clock()
        if not self.transport.healthy():return False
        source=getattr(self.transport,'source',None)
        expected=getattr(source,'generations',{})
        if source is not None and hasattr(source,'book_validity'):
            return all((v,symbol) in self.books and source.book_validity(v,symbol,self.books[v,symbol])[0] for v in ('arcus','lighter'))
        return all((v,symbol) in self.books and 0<=now-self.books[v,symbol].received<=self.profile.risk.stale_book_seconds
                   and self.books[v,symbol].generation==expected.get(v,self.generations[v,symbol])
                   for v in ('arcus','lighter'))

    def data_ready(self):
        if not self.transport.healthy():
            for symbol in self.profile.symbols:self.pause_basis(symbol)
            return False
        for symbol in self.profile.symbols:self.basis_center(symbol)
        return any(self.symbol_data_ready(symbol) for symbol in self.profile.symbols)

    def subscriptions_ready(self):
        return all(self.symbol_data_ready(symbol) for symbol in self.profile.symbols)

    def readiness_diagnostics(self):
        source=getattr(self.transport,'source',None)
        books={}
        for symbol in self.profile.symbols:
            books[symbol]={}
            for venue in ('arcus','lighter'):
                book=self.books.get((venue,symbol))
                if source is not None and hasattr(source,'book_validity'):
                    validity=source.book_validity(venue,symbol,book)
                    ready,reason=validity[:2]
                else:
                    ready=bool(book and 0<=self.clock()-book.received<=self.profile.risk.stale_book_seconds
                               and book.generation==self.generations[venue,symbol])
                    reason=None if ready else ('missing' if book is None else 'stale_or_generation')
                books[symbol][venue]={'ready':ready,'reason':reason}
        private={name:{'ready':value[0],'reason':value[1],'deadline':value[2],'generation':value[3]} for name,value in
                 (self.transport.private_validity().items() if hasattr(self.transport,'private_validity') else ())}
        return {'private_validity':private,'books':books}

    def quote_ready(self,symbol):
        if self.basis_center(symbol) is None:return False
        return self.clock()-self.warmup_started[symbol]>=self.profile.maker.warmup_seconds

    def reference(self, symbol, side):
        book = self.books.get(('lighter',symbol))
        return None if book is None else (book.bid if side == 1 else book.ask)

    def _adopt_ledger_identity(self, observation):
        """A push for a known order may omit clientId/market name; the durable ledger row is the identity source.

        Only fields that are empty/unresolvable are filled from the ledger row matched by exchange order id.
        Conflicting non-empty values are left untouched so _validate still fails closed."""
        if observation.venue!='arcus' or not observation.order_id:return observation
        row=self.journal.db.execute('SELECT client,symbol,exchange_id,side,qty,filled FROM all_orders WHERE exchange_id=? LIMIT 1',
                                    (str(observation.order_id),)).fetchone()
        if row is None and observation.client:
            row=self.journal.db.execute('SELECT client,symbol,exchange_id,side,qty,filled FROM all_orders WHERE client=? LIMIT 1',
                                        (str(observation.client),)).fetchone()
        if row is None:return observation
        changes={}
        if not observation.client and row['exchange_id'] and str(row['exchange_id'])==str(observation.order_id):
            changes['client']=row['client']
        if observation.symbol not in self.queues and row['symbol'] in self.queues:
            changes['symbol']=row['symbol']
        if row['exchange_id'] and str(row['exchange_id'])==str(observation.order_id):
            # Status-only pushes (no side/size) inherit intent from the ledger; FILLED means the full ledger size.
            if observation.side is None:changes['side']=row['side']
            if observation.requested is None:changes['requested']=D(row['qty'])
            if observation.cumulative is None:
                changes['cumulative']=D(row['qty']) if observation.status=='filled' else D(row['filled'])
        if not changes:return observation
        try:
            self.journal.event(row['symbol'],'observation_identity_adopted',order_id=observation.order_id,
                               observed_client=observation.client,observed_symbol=observation.symbol,**{'adopted_'+k:str(v) for k,v in changes.items()})
        except Exception:pass
        return replace(observation,**changes)

    def _validate(self, observation, row, venue):
        client=observation.client
        if not client and observation.order_id and row.get('exchange_id') and str(observation.order_id)==str(row['exchange_id']):
            client=row['client']
        checks=(('venue',observation.venue,venue),('account',observation.account,self.transport.identity),
                ('symbol',observation.symbol,row['symbol']),('side',observation.side,row['side']),
                ('requested',observation.requested,D(row['qty'])),('client',client,row['client']))
        mismatches=[f'{name}: observed={got!r} ledger={want!r}' for name,got,want in checks if got!=want]
        if observation.status=='filled' and observation.cumulative!=observation.requested:
            mismatches.append(f'filled_cumulative: observed={observation.cumulative} requested={observation.requested}')
        if observation.status not in TERMINAL|{'open','pending','unknown'}:
            mismatches.append(f'status: observed={observation.status!r}')
        if mismatches:
            detail='; '.join(mismatches)
            try:
                self.journal.event(row.get('symbol') or observation.symbol or '*','observation_mismatch',venue=venue,
                                   ledger_client=row.get('client'),order_id=observation.order_id,detail=detail)
            except Exception:pass
            raise ValueError('Observation does not match durable intent/account ('+detail+')')

    @staticmethod
    def _leverage_detail(snapshot):
        detail=snapshot.get('leverage_detail') or {}
        summary={venue:str(value) for venue,value in (snapshot.get('leverage') or {}).items()}
        return '(max='+repr(summary)+', by_symbol='+repr({k:str(v) for k,v in detail.items()})+')'

    def block_legacy_fill(self,row,cumulative,order_id):
        if row['purpose']!='legacy' or cumulative==0:
            return False
        reason='Legacy cancellation fill requires operator reconciliation'
        first=not self._legacy_blocked
        self._legacy_blocked=True
        self.halt('UNKNOWN_ORDER',reason,row['symbol'])
        if first:
            self.journal.event(row['symbol'],'legacy_fill_blocked',order_id=str(order_id),
                               qty=str(cumulative),reason=reason)
        return True

    async def observe_maker(self, observation):
        by_id=self.journal.db.execute('SELECT client FROM all_orders WHERE exchange_id=? LIMIT 1',(str(observation.order_id),)).fetchone() if observation.order_id else None
        row = self.journal.maker(by_id['client'] if by_id else (observation.client or observation.order_id))
        self._validate(observation,row,'arcus')
        if self.block_legacy_fill(row,observation.cumulative,observation.order_id):return
        before=self.journal.inventory(row['symbol'])
        obligation=self.journal.observe_maker(row['client'],observation.cumulative,observation.status,observation.order_id,
            reference=self.reference(row['symbol'],row['side']), average=observation.average,
            fee=observation.fee,trade_id=observation.trade_id,received=observation.received)
        self.manual_fill(row['symbol'],before,'maker')
        if observation.status in TERMINAL:self.close_bounds.pop(row['client'],None)
        self.close_basis.sync();self.observe_runtime()
        if obligation is not None:self.wake_hedge(row['symbol'])
        if obligation is not None and row['purpose'] == 'open':
            # A first fill chooses net inventory direction; even a late opposite fill is still journaled/hedged.
            for other in self.journal.makers(row['symbol'],active=True):
                if other['purpose']=='open' and other['side'] != row['side']:
                    task=asyncio.create_task(self.cancel_one(other))
                    self.cancel_tasks.add(task)
                    task.add_done_callback(self.cancel_tasks.discard)

    async def _worker(self, symbol):
        while True:
            obs = await self.queues[symbol].get()
            try:
                async with self.symbol_locks[symbol]:
                    if obs is not None:
                        await self.observe_maker(obs)
                    await self.drain_symbol(symbol)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                try:self.journal.event(symbol,'worker_error',**worker_error_detail(exc,obs))
                except Exception:pass
                self.halt('UNKNOWN_ORDER','Fill processing failed',symbol,error_type=type(exc).__name__)
                await self.cancel_all()
            finally:
                self.queues[symbol].task_done()

    async def _attempt_observation(self, attempt, obligation, observation):
        expected = {**attempt, 'symbol':obligation['symbol'], 'side':-obligation['side']}
        self._validate(observation,expected,attempt['venue'])
        before=self.journal.inventory(obligation['symbol'])
        self.journal.observe_attempt(attempt['client'],observation.cumulative,observation.status,
                                     observation.order_id,observation.average,observation.fee)
        self.manual_fill(obligation['symbol'],before,'attempt')
        self.close_basis.sync();self.observe_runtime()
        self.sequence+=1
        if observation.status not in TERMINAL:
            self.journal.attempt_state(attempt['client'],'unknown')
            return False
        return True

    async def resolve_attempt(self, attempt, obligation):
        try:
            obs = await asyncio.wait_for(self.transport.lookup_attempt(attempt,obligation),
                                         self.profile.hedge.request_timeout)
            return await self._attempt_observation(attempt,obligation,obs)
        except Exception:
            self.journal.attempt_state(attempt['client'],'unknown')
            return False

    def residual_buckets(self,symbol=None):
        buckets={}
        for row in self.journal.obligations(symbol):
            buckets.setdefault((row['symbol'],row['side'],row['purpose']),[]).append(row)
        return list(buckets.values())

    def residual_state(self,rows):
        attempts={a['client']:a for r in rows for a in self.journal.attempts(r['id'],include_deferred=False)}
        if any(a['status'] not in TERMINAL for a in attempts.values()):return 'uncertain_write',D(0)
        if any(a['venue']=='arcus' for a in attempts.values()):return 'failed',D(0)
        if any(len(self.zero_fill_failures(row['id']))>self.profile.hedge.max_retries for row in rows):
            return 'failed',D(0)
        remaining=sum((D(r['qty'])-D(r['hedged'])-D(r['unwound']) for r in rows),D(0))
        first=rows[0];book=self.books.get(('lighter',first['symbol']))
        price=(book.bid if first['side']==1 else book.ask) if book else None
        qty=hedge_quantity(remaining,self.rules[first['symbol']],price)
        return ('eligible' if qty else 'deferred_dust'),qty

    def zero_fill_failures(self,obligation):
        return [a for a in self.journal.attempts(obligation,include_deferred=False)
                if a['venue']=='lighter' and a['status'] in TERMINAL and D(a['filled'])==0]

    def blocking_residuals(self,symbol=None):
        return [rows for rows in self.residual_buckets(symbol) if self.residual_state(rows)[0]!='deferred_dust']

    def quote_residuals(self,symbol):
        # A recovered/uncertain write blocks all symbols; a known live hedge blocks its own.
        return (self.blocking_residuals(symbol) or
                [rows for rows in self.residual_buckets() if self.residual_state(rows)[0]=='failed'] or
                [a for a in self.journal.attempts(active=True) if a['client'] not in self.hedges_inflight])

    def quote_block_reason(self,symbol):
        if any(q.qsize() for q in self.queues.values()):return 'global_queue'
        if self.quote_residuals(symbol):
            if (any(a['client'] not in self.hedges_inflight for a in self.journal.attempts(active=True))
                    or any(self.residual_state(rows)[0]=='failed' for rows in self.residual_buckets())):return 'global_residual'
            return 'local_residual'
        return None

    def unresolved(self):
        return (self.journal.makers(active=True) or self.blocking_residuals() or
                self.journal.attempts(active=True))

    async def drain_symbol(self, symbol):
        for obligation in self.journal.obligations(symbol):
            if obligation['purpose']=='legacy':
                row=self.journal.maker(obligation['maker'])
                if self.block_legacy_fill(row,D(row['filled']),row['exchange_id']):return
        # Resolve original identities before classifying amount; dust never masks an uncertain write.
        for attempt in self.journal.attempts(active=True,symbol=symbol):
            try:
                obligation=self.journal.obligation(attempt['obligation'])
                resolved=await self.resolve_attempt(attempt,obligation)
            except Exception as exc:
                if attempt['venue']!='arcus':raise
                self.block_serious_sends()
                try:
                    self.serious_halt('emergency_residual',symbol,None,failure_detail='resolution_accounting_unavailable',
                        accounting_error_type=type(exc).__name__)
                    raise RuntimeError('Emergency resolution accounting unavailable; sends remain blocked') from exc
                finally:await self.cancel_all()
            if not resolved:
                if attempt['venue']=='arcus':self.block_serious_sends()
                try:
                    self.halt('UNKNOWN_ORDER','Unresolved hedge; no retry or Arcus unwind until authoritative terminal state '
                              '(stop runner, then: .venv/bin/python tools/operator_resolve_hedge.py)',symbol)
                    if attempt['venue']=='arcus':
                        accounting_error=None
                        try:
                            current=self.journal.obligation(attempt['obligation'])
                            remaining=D(current['qty'])-D(current['hedged'])-D(current['unwound'])
                        except Exception as exc:remaining=None;accounting_error=exc
                        self.serious_halt('emergency_residual',symbol,remaining,failure_detail='acceptance_unknown',
                            accounting_error_type=type(accounting_error).__name__ if accounting_error is not None else '')
                        if accounting_error is not None:raise RuntimeError('Emergency residual accounting unavailable; sends remain blocked') from accounting_error
                finally:await self.cancel_all()
                return
        if not self.sends_allowed():return
        while True:
            if not self.sends_allowed():return
            buckets=self.residual_buckets(symbol)
            if not buckets:break
            sent=False
            for rows in buckets:
                state,qty=self.residual_state(rows)
                if state=='deferred_dust':continue
                obligation=rows[0]
                if state=='failed':
                    self.block_serious_sends()
                    try:
                        diagnostics_error=None
                        try:
                            exhausted=[row for row in rows if len(self.zero_fill_failures(row['id']))>self.profile.hedge.max_retries]
                            unresolved=sum((D(row['qty'])-D(row['hedged'])-D(row['unwound']) for row in rows),D(0))
                            reason_code='retry_exhausted' if exhausted else 'emergency_residual'
                        except Exception as exc:reason_code='residual_failure';unresolved=None;diagnostics_error=exc
                        self.serious_halt(reason_code,symbol,unresolved,
                            failure_detail='failure_diagnostics_unavailable' if diagnostics_error is not None else None,
                            accounting_error_type=type(diagnostics_error).__name__ if diagnostics_error is not None else '')
                        if diagnostics_error is not None:raise RuntimeError('Severe failure diagnostics unavailable; sends remain blocked') from diagnostics_error
                    finally:await self.cancel_all()
                    return
                book=self.books.get(('lighter',symbol))
                source=getattr(self.transport,'source',None)
                if source is not None and hasattr(source,'book_validity'):
                    if not source.book_validity('lighter',symbol,book)[0]:return
                elif book is None or not 0<=self.clock()-book.received<=self.profile.risk.stale_book_seconds:return
                if obligation['purpose']=='reconcile':
                    # Corrections retain their exact capital admission accounting.
                    for row in rows:
                        if not await self.correction_admitted(row,D(row['qty'])-D(row['hedged'])-D(row['unwound'])):return
                if not self.sends_allowed():return
                client=self.journal.prepare_attempt(obligation['id'],qty,obligations=[r['id'] for r in rows])
                attempt=self.journal.attempt(client)
                self.journal.attempt_state(client,'submitted');self.hedges_inflight.add(client)
                try:
                    obs=await asyncio.wait_for(self.transport.hedge(attempt,obligation),self.profile.hedge.request_timeout)
                    terminal=await self._attempt_observation(attempt,obligation,obs)
                except DeferredHedge:
                    self.journal.observe_attempt(client,D(0),'deferred')
                    # Durable no-send intent: excluded from retry count, retained for audit.
                    return
                except DefinitiveFailure:
                    self.journal.observe_attempt(client,D(0),'rejected');terminal=True
                except Exception:
                    self.journal.attempt_state(client,'unknown')
                    self.hedges_inflight.discard(client)
                    terminal=await self.resolve_attempt(attempt,obligation)
                finally:
                    self.hedges_inflight.discard(client)
                if not terminal:
                    self.halt('UNKNOWN_ORDER','Hedge UNKNOWN: cancel makers, retain obligation and original identity',symbol)
                    await self.cancel_all();return
                elapsed=(self.clock()-obligation['received'])*1000
                if elapsed>self.profile.hedge.max_hedge_latency_ms:
                    self.alarm(f'Hedge confirmation exceeded {self.profile.hedge.max_hedge_latency_ms}ms target',symbol)
                sent=True
                break
            if not sent:break

    async def emergency(self, obligation, remaining):
        if not self.sends_allowed():return
        symbol = obligation['symbol']
        async def blocked(detail):
            try:self.serious_halt('emergency_failed',symbol,remaining,failure_detail=detail)
            finally:await self.cancel_all()
        if self.cancel_failed or self.journal.makers(active=True):
            await blocked('maker_cancellation_unresolved')
            return
        if self.journal.attempts(active=True):
            await blocked('prior_attempt_unknown')
            return
        if obligation['purpose'] == 'reconcile':
            await blocked('reconciliation_cannot_restore')
            return
        rule = self.rules[symbol]
        if obligation['purpose']=='close':
            maker=self.journal.maker(obligation['maker'])
            if remaining>D(maker['preclose']) or remaining>D(maker['filled']):
                await blocked('restore_exceeds_original_capacity')
                return
            cap=min(self.equity.get('arcus',D(0)),self.profile.risk.allocation_per_venue)*self.profile.risk.leverage
            # Pre-close reservation is already held; a depleted equity snapshot still forbids restoring beyond cap.
            if self._gross('arcus')*(1+self.profile.risk.reserve_ratio)>cap:
                await blocked('restore_exceeds_capital_cap')
                return
        # Never round up uncovered exposure; small Arcus dust may itself be below venue minSize.
        if remaining % rule.arcus_step or remaining < rule.arcus_min:
            await blocked('below_arcus_size_or_step')
            return
        client = self.journal.prepare_attempt(obligation['id'],remaining,'arcus')
        attempt = self.journal.attempt(client)
        self.journal.attempt_state(client,'submitted')
        kind = 'emergency_restore' if obligation['purpose']=='close' else 'emergency_unwind'
        self.journal.event(symbol,kind,qty=remaining,reduce_only=obligation['purpose']=='open')
        accounting_error=None
        try:
            obs = await asyncio.wait_for(self.transport.emergency(attempt,obligation),self.profile.hedge.request_timeout)
            if obs.status not in TERMINAL or obs.cumulative<D(attempt['qty']):self.block_serious_sends()
            terminal=await self._attempt_observation(attempt,obligation,obs)
            current=self.journal.obligation(obligation['id'])
            remaining=D(current['qty'])-D(current['hedged'])-D(current['unwound'])
            if terminal and not remaining:return
            error_type=''
            detail='terminal_residual' if terminal else 'acceptance_unknown'
        except DeferredHedge:
            try:self.journal.observe_attempt(client,D(0),'deferred')
            except Exception as exc:
                self.block_serious_sends();accounting_error=exc
                error_type=type(exc).__name__;detail='unsent_accounting_unavailable'
            else:return
        except DefinitiveFailure:
            self.block_serious_sends()
            error_type='DefinitiveFailure';detail='order_rejected'
            try:self.journal.observe_attempt(client,D(0),'rejected')
            except Exception as exc:accounting_error=exc
        except Exception as exc:
            self.block_serious_sends()
            error_type=type(exc).__name__;detail='acceptance_unknown'
            try:self.journal.attempt_state(client,'unknown')
            except Exception as failed:accounting_error=failed
        self.block_serious_sends()
        try:
            if accounting_error is None:
                try:
                    current=self.journal.obligation(obligation['id'])
                    remaining=D(current['qty'])-D(current['hedged'])-D(current['unwound'])
                except Exception as exc:accounting_error=exc
            if accounting_error is not None:remaining=None
            self.serious_halt('emergency_failed',symbol,remaining,error_type=error_type,failure_detail=detail,
                accounting_error_type=type(accounting_error).__name__ if accounting_error is not None else '')
            if accounting_error is not None:
                raise RuntimeError('Emergency failure accounting unavailable; sends remain blocked') from accounting_error
        finally:await self.cancel_all()

    async def cancel_one(self, row):
        try:
            obs = await asyncio.wait_for(self.transport.cancel_maker(row),self.profile.hedge.request_timeout)
            await self.observe_cancel(row,obs)
            return True
        except Exception as exc:
            self.cancel_failed = True
            self.halt(self.state,f'CRITICAL cancellation unresolved: {row["client"]}: {type(exc).__name__}',row['symbol'])
            return False

    async def observe_cancel(self, row, observation):
        self._validate(observation,row,'arcus')
        if self.block_legacy_fill(row,observation.cumulative,observation.order_id):
            raise ValueError('Legacy fill cannot be adopted')
        before=self.journal.inventory(row['symbol'])
        obligation=self.journal.observe_maker(row['client'],observation.cumulative,observation.status,observation.order_id,
                                  reference=self.reference(row['symbol'],row['side']),average=observation.average,
                                  fee=observation.fee,received=observation.received)
        self.manual_fill(row['symbol'],before,'cancel')
        if obligation is not None:
            self.wake_hedge(row['symbol'])
        if observation.status not in TERMINAL:
            raise ValueError('Cancel ACK is not terminal')
        self.close_bounds.pop(row['client'],None)
        self.close_basis.sync();self.observe_runtime()

    async def cancel_all(self):
        async with self.cancel_lock:
            results = await asyncio.gather(*(self.cancel_one(row) for row in self.journal.makers(active=True)))
            return all(results)

    async def cancel_symbol(self,symbol):
        async with self.cancel_lock:
            results=await asyncio.gather(*(self.cancel_one(row) for row in self.journal.makers(symbol,active=True)))
        if not all(results):await self.cancel_all()
        return all(results)

    def _gross(self, venue):
        total = D(0)
        index = 0 if venue=='arcus' else 1
        for symbol in self.profile.symbols:
            prices = [b.ask for (v,s),b in self.books.items() if s==symbol]
            if not prices:
                if any(self.journal.inventory(symbol)):return D('Infinity')
                continue
            total += abs(self.journal.inventory(symbol)[index])*max(prices)
        for row in self.journal.makers(active=True):
            if row['purpose']=='open':
                books=[self.books.get((v,row['symbol'])) for v in ('arcus','lighter')]
                if any(book is None for book in books):return D('Infinity')
                price=max(D(row['reserved']),*(book.ask for book in books))
                total += (D(row['qty'])-D(row['filled']))*price
        for o in self.journal.obligations():
            if (o['purpose']=='close' and venue=='arcus') or (o['purpose'] in ('open','reconcile') and venue=='lighter'):
                row=self.journal.maker(o['maker'])
                prices=[b.ask for (v,s),b in self.books.items() if s==o['symbol']]
                price=max([D(row['reserved']),*prices])
                remaining=D(o['qty'])-D(o['hedged'])-D(o['unwound'])
                if o['purpose']=='reconcile':
                    current=self.journal.inventory(o['symbol'])[1]
                    remaining=max(D(0),abs(current-o['side']*remaining)-abs(current))
                total+=remaining*price
        return total

    async def correction_admitted(self,obligation,remaining):
        """Revalidate actual capital before EVERY correction send, including restart and partial retry."""
        if not self.sends_allowed():return False
        symbol=obligation['symbol']
        generation=self.sequence
        failure_reason='Reconciliation correction blocked; uncovered obligation preserved'
        try:
            snapshot=await asyncio.wait_for(self.transport.snapshot(),self.profile.hedge.request_timeout)
            if not self.sends_allowed():return False
            if snapshot.get('authoritative') is not True or generation!=self.sequence or any(q.qsize() for q in self.queues.values()):
                failure_reason='Correction account snapshot unavailable or changed during read';raise ValueError(failure_reason)
            if not self.subscriptions_ready():failure_reason='Correction requires fresh public/private data';raise ValueError(failure_reason)
            actual={}
            for name,values in snapshot['positions'].items():
                if len(values)!=2:failure_reason='Correction position snapshot malformed';raise ValueError(failure_reason)
                quantities=tuple(D(str(q)) for q in values)
                if any(not q.is_finite() for q in quantities):failure_reason='Correction position size invalid';raise ValueError(failure_reason)
                if name not in self.profile.symbols and any(quantities):failure_reason='Foreign correction inventory';raise ValueError(failure_reason)
                actual[name]=quantities
            expected=self.journal.inventory(symbol)
            if any(abs(a-e)>self.rules[symbol].step for a,e in zip(actual.get(symbol,(D(0),D(0))),expected)):
                failure_reason='Correction snapshot does not confirm latest known fills; no replay';raise ValueError(failure_reason)
            equity={venue:D(str(snapshot['equity'][venue])) for venue in ('arcus','lighter')}
            if any(not value.is_finite() or value<=0 for value in equity.values()):
                failure_reason='Correction authoritative equity invalid';raise ValueError(failure_reason)
            if any(snapshot.get('leverage',{}).get(venue,1)>2 for venue in ('arcus','lighter')):
                failure_reason='Correction returned venue leverage exceeds 2 '+self._leverage_detail(snapshot);raise ValueError(failure_reason)
            async with self.capital_lock:
                if not self.sends_allowed():return False
                if generation!=self.sequence:failure_reason='Correction account generation changed';raise ValueError(failure_reason)
                self.equity.update(equity)
                gross={'arcus':D(0),'lighter':D(0)}
                prices={name:max(self.books[venue,name].ask for venue in ('arcus','lighter')) for name in self.profile.symbols}
                for name in self.profile.symbols:
                    arcus,lighter=actual.get(name,(D(0),D(0)))
                    if name==symbol:lighter-=obligation['side']*remaining
                    gross['arcus']+=abs(arcus)*prices[name]
                    gross['lighter']+=abs(lighter)*prices[name]
                for row in self.journal.makers(active=True):
                    if row['purpose']=='open':
                        reserve=(D(row['qty'])-D(row['filled']))*max(D(row['reserved']),prices[row['symbol']])
                        for venue in gross:gross[venue]+=reserve
                for other in self.journal.obligations():
                    if other['id']==obligation['id']:continue # Already represented by the projected position above.
                    row=self.journal.maker(other['maker'])
                    reserve=(D(other['qty'])-D(other['hedged'])-D(other['unwound']))*max(D(row['reserved']),prices[other['symbol']])
                    if other['purpose']=='close':gross['arcus']+=reserve
                    elif other['purpose'] in ('open','reconcile'):gross['lighter']+=reserve
                for venue,value in gross.items():
                    cap=min(equity[venue],self.profile.risk.allocation_per_venue)*self.profile.risk.leverage
                    if value*(1+self.profile.risk.reserve_ratio)>cap:
                        failure_reason=f'Correction projected {venue} gross plus reservations exceeds capital cap';raise ValueError(failure_reason)
            return True
        except Exception as exc:
            self.halt('EMERGENCY',failure_reason,symbol,error_type=type(exc).__name__)
            await self.cancel_all()
            return False

    def _admit(self, symbol, side, purpose, qty, price):
        if self.journal.pending_reconcile() and any(rows[0]['purpose']=='reconcile' for rows in self.blocking_residuals()):return False
        cfg = self.profile.get_config(symbol)
        arcus,lighter = self.journal.inventory(symbol)
        makers = self.journal.makers(symbol,active=True)
        pending_open = sum((D(r['qty'])-D(r['filled']) for r in makers if r['purpose']=='open'),D(0))
        pending_close = sum((D(r['qty'])-D(r['filled']) for r in makers if r['purpose']=='close'),D(0))
        pending_risk=sum(((D(r['qty'])-D(r['filled']))*max(price,D(r['reserved'])) for r in makers),D(0))
        if abs(arcus+lighter)*price+pending_risk+qty*price>self.profile.risk.max_unhedged_notional:
            return False
        if purpose=='open':
            if abs(arcus)+pending_open+qty > cfg.quantity*cfg.max_segments:
                return False
            if arcus and (1 if arcus>0 else -1) != side:
                return False
        else:
            reducible = min(abs(arcus),abs(lighter)) if arcus*lighter < 0 else D(0)
            if arcus*side >= 0 or pending_close+qty > reducible:
                return False
        for venue in ('arcus','lighter'):
            equity = self.equity.get(venue,D(0))
            cap = min(equity,self.profile.risk.allocation_per_venue)*self.profile.risk.leverage
            addition = qty*price if purpose=='open' else D(0)
            if equity <= 0 or (self._gross(venue)+addition)*(1+self.profile.risk.reserve_ratio) > cap:
                return False
        return True

    def planned_quantity(self,symbol,side,purpose,*,exclude=None):
        """Use the same floored capacity for quote planning, replacement and final admission."""
        cfg=self.profile.get_config(symbol);rule=self.rules[symbol]
        arcus,lighter=self.journal.inventory(symbol)
        active=[r for r in self.journal.makers(symbol,active=True) if r['client']!=exclude]
        pending=sum((D(r['qty'])-D(r['filled']) for r in active if r['purpose']=='open'),D(0))
        capacity=max(D(0),cfg.quantity*cfg.max_segments-abs(arcus)-pending)
        opening=floor_quantity(min(cfg.quantity/2 if not arcus and cfg.max_segments==1 else cfg.quantity,capacity),rule.step)
        if purpose=='open':return opening
        paired=min(abs(arcus),abs(lighter)) if arcus*lighter<0 and arcus*side<0 else D(0)
        reserved=sum((D(r['qty'])-D(r['filled']) for r in active if r['purpose']=='close'),D(0))
        available=max(D(0),paired-reserved)
        reference=self.reference(symbol,side)
        def legal(qty):
            price,*_=self.maker_quote(symbol,side,'close',qty,cfg.close_threshold)
            return price is not None and rule.legal(qty,price,reference)
        # A legal paired layer tail can clear capacity; tiny tails must not block a normal close.
        paired_tail=max(D(0),paired-(abs(arcus)//cfg.quantity)*cfg.quantity)
        tail=floor_quantity(min(abs(arcus)%cfg.quantity,paired_tail,available),rule.step)
        opening_price,*_=self.maker_quote(symbol,-side,'open',opening,cfg.open_threshold)
        can_open=opening_price is not None and rule.legal(opening,opening_price,self.reference(symbol,-side))
        if not can_open and tail and legal(tail):return tail
        return floor_quantity(min(available,cfg.quantity),rule.step)

    async def place(self, symbol, side, purpose, qty, threshold, level):
        if not self.sends_allowed() or self.state != 'ACTIVE' or self.halted or self.stopping or not self.quote_ready(symbol):
            return None
        prepaid=self.close_permits.get((symbol,side)) if purpose=='close' else None
        rule = self.rules[symbol]
        qty = floor_quantity(qty,rule.step)
        price,reference,anchor,ab,bound=self.maker_quote(symbol,side,purpose,qty,threshold)
        if price is None or not rule.legal(qty,price,reference):
            return None
        async with self.capital_lock:
            if prepaid is not None:
                valid=self.close_permit_valid(symbol,side,qty) and self.close_permits.get((symbol,side)) is prepaid
                now=self.clock();used=sum(at>now-1 for at in self.requotes[symbol])
                if not valid and used>=self.profile.maker.max_requotes_per_sec:return None
            if (self.state!='ACTIVE' or self.halted or self.stopping or not self.quote_ready(symbol)
                    or self.quote_residuals(symbol) or any(q.qsize() for q in self.queues.values())):
                return None
            if purpose=='close':
                price,reference,anchor,ab,bound=self.maker_quote(symbol,side,purpose,qty,threshold)
                if price is None or not rule.legal(qty,price,reference):return None
            current,_=self.journal.inventory(symbol)
            if purpose=='open' and qty>self.planned_quantity(symbol,side,'open'):return None
            if purpose=='open' and int(abs(current)/self.profile.get_config(symbol).quantity)!=level:
                return None
            # Include UNKNOWN orders and pending cancellation in self-cross checks.
            if any(r['side'] != side and (price >= D(r['price']) if side==1 else price <= D(r['price']))
                   for r in self.journal.makers(symbol,active=True)):
                return None
            reserve_price = max(price,ab.ask,self.books['lighter',symbol].ask)
            if not self._admit(symbol,side,purpose,qty,reserve_price):
                return None
            diagnostic=None
            if purpose=='close':
                cfg=self.profile.get_config(symbol)
                diagnostic=self.close_basis.describe(symbol,side,qty,reference,price,cfg.close_floor_timeout,self.wall_clock())
                diagnostic.update(raw_bound=bound,ema_price=quote_price(side,reference,threshold,rule,ab.bid,ab.ask,self.basis_center(symbol)))
                diagnostic['protection_active']=D(diagnostic['protected_qty'])>0
                diagnostic['bound_binding']=bound is not None and (bound<diagnostic['ema_price'] if side==1 else bound>diagnostic['ema_price'])
            if purpose=='close':
                self.close_permits.pop((symbol,side),None)
                self.close_prepare_versions[symbol,side]+=1
            client = self.journal.prepare_maker(symbol,side,purpose,qty,price,level,anchor,reserve_price,close_quote=diagnostic)
            if purpose=='close':
                self.close_bounds[client]=(bound,qty)
                self.close_quote_wait(symbol,side,end_reason='prepared',client=client)
        row = self.journal.maker(client)
        try:
            obs = await asyncio.wait_for(self.transport.maker(row),self.profile.hedge.request_timeout)
            self.receive(obs)
        except DeferredHedge:
            self.journal.observe_maker(client,D(0),'deferred')
            self.close_bounds.pop(client,None)
        except DefinitiveFailure:
            self.journal.observe_maker(client,D(0),'rejected')
            self.close_bounds.pop(client,None)
        except Exception:
            self.journal.observe_maker(client,D(0),'unknown')
            self.halt('UNKNOWN_ORDER','Maker submission UNKNOWN; persisted identity must be reconciled',symbol)
            await self.cancel_all()
        return client

    def maker_quote(self,symbol,side,purpose,qty,threshold):
        ab=self.books['arcus',symbol];reference=self.reference(symbol,side);basis=self.basis_center(symbol)
        if basis is None:return None,reference,None,ab,None
        bound=None
        if purpose=='close':
            cfg=self.profile.get_config(symbol)
            allowed,bound=self.close_basis.bound(symbol,side,qty,reference,cfg.min_close_profit,
                                                 cfg.close_floor_timeout,self.wall_clock())
            if not allowed:return None,reference,None,ab,None
        price=quote_price(side,reference,threshold,self.rules[symbol],ab.bid,ab.ask,basis,price_bound=bound)
        return price,reference,reference*(1+basis/100),ab,bound

    def maker_admitted(self,row):
        """Recheck every normal maker after transport awaits, retaining the full close guard."""
        if not self.sends_allowed():return False
        if row['purpose']=='close':return self.close_admitted(row)
        symbol=row['symbol']
        if (row['purpose']!='open' or self.state!='ACTIVE' or self.halted or self.stopping
                or not self.quote_ready(symbol) or self.quote_residuals(symbol)
                or any(q.qsize() for q in self.queues.values())):return False
        current,lighter=self.journal.inventory(symbol)
        if current*row['side']<0 or D(row['qty'])>self.planned_quantity(symbol,row['side'],'open',exclude=row['client']):return False
        price=max(D(row['reserved']),*(self.books[v,symbol].ask for v in ('arcus','lighter')))
        pending=sum(((D(r['qty'])-D(r['filled']))*max(price,D(r['reserved']))
                     for r in self.journal.makers(symbol,active=True)),D(0))
        if abs(current+lighter)*price+pending>self.profile.risk.max_unhedged_notional:return False
        for venue in ('arcus','lighter'):
            cap=min(self.equity.get(venue,D(0)),self.profile.risk.allocation_per_venue)*self.profile.risk.leverage
            if cap<=0 or self._gross(venue)*(1+self.profile.risk.reserve_ratio)>cap:return False
        return True

    def close_admitted(self,row):
        """Synchronous final check after Arcus's write-lock/auth awaits, before signing."""
        if not self.sends_allowed():return False
        symbol=row['symbol'];side=row['side'];qty=D(row['qty']);price=D(row['price'])
        if (self.state!='ACTIVE' or self.halted or self.stopping or not self.quote_ready(symbol)
                or self.quote_residuals(symbol) or any(q.qsize() for q in self.queues.values())):return False
        arcus,lighter=self.journal.inventory(symbol)
        paired=min(abs(arcus),abs(lighter)) if arcus*lighter<0 else D(0)
        reserved=sum((D(r['qty'])-D(r['filled']) for r in self.journal.makers(symbol,active=True)
                      if r['purpose']=='close' and r['client']!=row['client']),D(0))
        if arcus*side>=0 or qty+reserved>paired:return False
        cfg=self.profile.get_config(symbol);reference=self.reference(symbol,side);ab=self.books['arcus',symbol]
        allowed,bound=self.close_basis.bound(symbol,side,qty,reference,cfg.min_close_profit,
                                             cfg.close_floor_timeout,self.wall_clock())
        return (allowed and (bound is None or (price<=bound if side==1 else price>=bound))
                and price%self.rules[symbol].price_tick(price)==0
                and (price<ab.ask if side==1 else price>ab.bid)
                and self.rules[symbol].legal(qty,price,reference))

    def close_diagnostic(self,symbol,kind,**payload):
        try:self.journal.event(symbol,kind,**payload)
        except Exception as exc:
            try:print(f'Close diagnostic write failed: {kind} ({type(exc).__name__})',file=sys.stderr)
            except Exception:pass

    def close_quote_wait(self,symbol,side,reason=None,scope=None,*,end_reason='recovered',client=None):
        key=(symbol,side);old=self.close_waits.get(key);now=self.clock()
        if old and old[:2]==(reason,scope):return
        if old:
            self.close_waits.pop(key)
            self.close_diagnostic(symbol,'close_quote_wait',side=side,reason=old[0],scope=old[1],
                               transition='exit',duration_seconds=max(0,now-old[2]),end_reason='reason_changed' if reason else end_reason,
                               client=client,observed_monotonic=now)
        if reason:
            self.close_waits[key]=(reason,scope,now)
            q=self.requotes[symbol];limit=self.profile.maker.max_requotes_per_sec
            live=[at for at in q if at>now-1];used=len(live)
            self.close_diagnostic(symbol,'close_quote_wait',side=side,reason=reason,scope=scope,
                               transition='enter',observed_monotonic=now,quota_used=used,quota_limit=limit,
                               quota_wait_seconds=max(0,live[0]+1-now) if used>=limit else 0)

    def close_permit_identity(self,symbol):
        source=getattr(self.transport,'source',None)
        return (tuple((self.generations[v,symbol],getattr(source,'generations',{}).get(v),
                       self.books[v,symbol].generation) for v in ('arcus','lighter')),self.basis_revision())

    def close_permit_valid(self,symbol,side,qty=None):
        key=(symbol,side);permit=self.close_permits.get(key)
        if permit is None:return False
        old=self.journal.maker(permit['old_client']);inventory=self.journal.inventory(symbol)
        planned=self.planned_quantity(symbol,side,'close')
        valid=(self.state=='ACTIVE' and not self.halted and not self.stopping and self.quote_ready(symbol)
               and permit['token'] in self.requotes[symbol] and permit['token']<=self.clock()<permit['token']+1
               and inventory==permit['inventory']
               and (qty is None or floor_quantity(qty,self.rules[symbol].step)==permit['qty']) and planned==permit['qty']
               and self.close_permit_identity(symbol)==permit['identity']
               and self.close_prepare_versions[key]==permit['prepare_version']
               and old['status'] in ('canceled','expired','rejected')
               and D(old['filled'])==permit['filled'] and D(old['qty'])==permit['original_qty']
               and not any(r['side']==side and r['purpose']=='close' for r in self.journal.makers(symbol,active=True)))
        if not valid:self.close_permits.pop(key,None)
        return valid

    async def close_cancel(self,row,reason,target,bound,reserved_cycle,cycle_token=None):
        symbol=row['symbol'];side=row['side'];now=self.clock();q=self.requotes[symbol]
        live=[at for at in q if at>now-1];used=len(live);limit=self.profile.maker.max_requotes_per_sec
        ab=self.books['arcus',symbol]
        self.close_diagnostic(symbol,'close_cancel_decision',client=row['client'],side=side,
                           remaining_qty=D(row['qty'])-D(row['filled']),reason=reason,
                           old_price=row['price'],old_bound=self.close_bounds.get(row['client'],(None,None))[0],
                           target_price=target,raw_bound=bound,lighter_reference=self.reference(symbol,side),
                           arcus_bid=ab.bid,arcus_ask=ab.ask,quota_used=used,quota_limit=limit,
                           quota_wait_seconds=max(0,live[0]+1-now) if used>=limit else 0,
                           reserved_cycle=reserved_cycle,global_queue_count=sum(q.qsize() for q in self.queues.values()),
                           global_pending=self.quote_block_reason(symbol)=='global_residual',
                           local_pending=self.quote_block_reason(symbol)=='local_residual',observed_monotonic=now)
        key=(symbol,side);inventory=self.journal.inventory(symbol);identity=self.close_permit_identity(symbol)
        version=self.close_prepare_versions[key]
        result=await self.cancel_one(row)
        if result and reserved_cycle and cycle_token is not None and key not in self.close_permits:
            self.close_permits[key]=dict(old_client=row['client'],token=cycle_token,
                qty=floor_quantity(min(D(row['qty'])-D(row['filled']),self.profile.get_config(symbol).quantity),self.rules[symbol].step),
                inventory=inventory,identity=identity,prepare_version=version,filled=D(row['filled']),original_qty=D(row['qty']))
            self.close_permit_valid(symbol,side)
        self.close_diagnostic(symbol,'close_cancel_result',client=row['client'],side=side,confirmed=result,
                           status=self.journal.maker(row['client'])['status'],observed_monotonic=self.clock())
        return result

    async def quote_symbol(self, symbol):
        async with self.quote_locks[symbol]:
            for key in list(self.close_permits):
                if key[0]==symbol:self.close_permit_valid(*key)
            if not any(self.journal.inventory(symbol)):
                self.close_tail_reasons.pop(symbol,None)
                for key in list(self.close_waits):
                    if key[0]==symbol:self.close_quote_wait(*key,end_reason='flat')
            if not self.quote_ready(symbol):
                for key in list(self.close_waits):
                    if key[0]==symbol:self.close_quote_wait(*key,end_reason='quote_not_ready')
                if self.journal.makers(symbol,active=True):await self.cancel_symbol(symbol)
                return
            local_blocked=bool(self.queues[symbol].qsize() or self.blocking_residuals(symbol))
            cfg = self.profile.get_config(symbol)
            active=self.journal.makers(active=True)
            active_ids={row['client'] for row in active if row['purpose']=='close'}
            self.close_bounds={client:token for client,token in self.close_bounds.items() if client in active_ids}
            now=self.clock();q=self.requotes[symbol]
            while q and q[0]<=now-1:q.popleft()
            waiting_safe=set()
            for row in self.journal.makers(symbol,active=True):
                if local_blocked and row['purpose']!='close':continue
                reference = self.fair_anchor(symbol,row['side'])
                close_changed=False
                if row['purpose']=='close':
                    remaining=D(row['qty'])-D(row['filled'])
                    price,_,_,_,bound=self.maker_quote(symbol,row['side'],'close',remaining,cfg.close_threshold)
                    token=(bound,remaining)
                    unsafe=(bound is not None and (D(row['price'])>bound if row['side']==1 else D(row['price'])<bound))
                    if price is None or unsafe:
                        reserved_cycle=len(q)<self.profile.maker.max_requotes_per_sec
                        if reserved_cycle:q.append(now)
                        await self.close_cancel(row,'unsafe_floor' if unsafe else 'quote_unavailable',price,bound,reserved_cycle,
                                                now if reserved_cycle else None)
                        self.close_bounds.pop(row['client'],None)
                        continue
                    previous_bound=self.close_bounds.get(row['client'],(None,None))[0]
                    close_changed=(previous_bound is not None and bound is None and price!=D(row['price']))
                    if not close_changed:self.close_bounds[row['client']]=token
                    if local_blocked:
                        self.close_quote_wait(symbol,row['side'],self.quote_block_reason(symbol),'retain_safe_order')
                        waiting_safe.add(row['side']);continue
                if close_changed or (reference and should_requote(D(row['anchor']),reference,time.time()-row['created'],self.profile.maker)):
                    if row['purpose']=='close':
                        reason=self.quote_block_reason(symbol)
                        if reason:
                            self.close_quote_wait(symbol,row['side'],reason,'retain_safe_order');waiting_safe.add(row['side']);continue
                    now = self.clock()
                    q = self.requotes[symbol]
                    while q and q[0] <= now-1:
                        q.popleft()
                    if len(q) >= self.profile.maker.max_requotes_per_sec:
                        if row['purpose']=='close':
                            self.close_quote_wait(symbol,row['side'],'quota','retain_safe_order');waiting_safe.add(row['side'])
                        continue
                    if row['purpose']=='close':self.close_quote_wait(symbol,row['side'])
                    q.append(now)
                    self.journal.event(symbol,'requote_attempt',client=row['client'])
                    canceled=(await self.close_cancel(row,'floor_target_changed' if close_changed else 'anchor_or_age',price,bound,True,now)
                              if row['purpose']=='close' else await self.cancel_one(row))
                    if canceled:
                        self.close_bounds.pop(row['client'],None)
                        self.journal.event(symbol,'requote_success',client=row['client'])
            if self.halted or self.state!='ACTIVE':return
            if local_blocked:
                inventory,lighter=self.journal.inventory(symbol)
                if inventory*lighter<0 and not any(r['purpose']=='close' for r in self.journal.makers(symbol,active=True)):
                    self.close_quote_wait(symbol,-1 if inventory>0 else 1,self.quote_block_reason(symbol),'replace_missing_order')
                return
            if self.quote_residuals(symbol):
                return
            inventory,lighter = self.journal.inventory(symbol)
            level = int(abs(inventory)/cfg.quantity)
            existing = {(r['side'],r['purpose']) for r in self.journal.makers(symbol,active=True)}
            if inventory:
                side = -1 if inventory>0 else 1
                for key in list(self.close_waits):
                    if key[0]==symbol and key[1]!=side:self.close_quote_wait(*key,end_reason='inventory_changed')
                if (side,'close') not in existing:
                    qty=self.planned_quantity(symbol,side,'close')
                    tail=abs(inventory)%cfg.quantity;effective=floor_quantity(tail,self.rules[symbol].step)
                    reason=None
                    if tail:
                        paired=min(abs(inventory),abs(lighter)) if inventory*lighter<0 else D(0)
                        if tail>max(D(0),paired-(abs(inventory)//cfg.quantity)*cfg.quantity):reason='unpaired_tail'
                        elif effective==0:reason='below_common_step'
                        else:
                            price,reference,*_=self.maker_quote(symbol,side,'close',effective,cfg.close_threshold)
                            if price is not None and not self.rules[symbol].legal(effective,price,reference):reason='below_maker_minimum_or_notional'
                    token=(tail,reason,qty) if reason else None
                    if token!=self.close_tail_reasons.get(symbol):
                        self.close_tail_reasons[symbol]=token
                        if reason:self.close_diagnostic(symbol,'close_tail_deferred',tail_qty=tail,reason=reason,fallback_qty=qty)
                    now=self.clock()
                    while q and q[0]<=now-1:q.popleft()
                    # A confirmed cancellation may already have paid within this same rolling second.
                    prepaid=self.close_permit_valid(symbol,side,qty)
                    if qty>0 and (prepaid or len(q)<self.profile.maker.max_requotes_per_sec):
                        attempt_permit=self.close_permits.get((symbol,side))
                        try:client=await self.place(symbol,side,'close',qty,cfg.close_threshold,level)
                        except BaseException:
                            if attempt_permit is not None and self.close_permits.get((symbol,side)) is attempt_permit:
                                self.close_permits.pop((symbol,side),None)
                            raise
                        if client is None:
                            reason=self.quote_block_reason(symbol) or 'other_admission_unknown'
                            self.close_quote_wait(symbol,side,reason,'replace_missing_order')
                    elif qty>0:self.close_quote_wait(symbol,side,'quota','replace_missing_order')
                    else:self.close_quote_wait(symbol,side,end_reason=(
                        'no_legal_common_quantity' if inventory*lighter<0 else 'no_paired_inventory'))
                elif side not in waiting_safe:self.close_quote_wait(symbol,side)

            sides = (1,-1) if not inventory else ((1,) if inventory>0 else (-1,))
            for side in sides:
                if (side,'open') not in existing:
                    qty=self.planned_quantity(symbol,side,'open')
                    await self.place(symbol,side,'open',qty,cfg.open_threshold+cfg.grid_step*level,level)

    async def reconcile(self, initial=False):
        if self._legacy_blocked or (not initial and not self._startup_recovered):return False
        if self.reconcile_lock.locked():return False
        async with self.reconcile_lock:
            return await self._reconcile(initial)

    async def _reconcile(self, initial=False):
        if any(lock.locked() for lock in self.symbol_locks.values()) or any(q.qsize() for q in self.queues.values()):
            return False
        if not self.sends_allowed():
            await self.cancel_all()
            for symbol in self.profile.symbols:
                async with self.symbol_locks[symbol]:await self.drain_symbol(symbol)
            return False
        self.close_permits.clear()
        self.state = 'RECONCILING'
        generation = self.sequence
        failure_reason='Reconciliation failed'
        retire=(self.transport.startup_terminal_retirement(tuple(self._startup_terminal.values()))
                if initial and hasattr(self.transport,'startup_terminal_retirement') else None)
        try:
            snapshot = await asyncio.wait_for(self.transport.snapshot(),self.profile.hedge.request_timeout)
            if not self.sends_allowed():return False
            if initial and self._startup_foreign:
                failure_reason='Startup orders arrived during reconciliation'
                self.halt('UNKNOWN_ORDER',failure_reason)
                await self._cancel_startup_foreign()
                raise ValueError(failure_reason)
            if generation != self.sequence or any(q.qsize() for q in self.queues.values()):
                return False
            if snapshot.get('authoritative') is not True:
                failure_reason='Incomplete account snapshot';raise ValueError(failure_reason)
            for venue in ('arcus','lighter'):
                equity = D(str(snapshot['equity'][venue]))
                if not equity.is_finite() or equity <= 0:
                    failure_reason='Nonpositive/invalid authoritative equity';raise ValueError(failure_reason)
                self.equity[venue] = equity
            for symbol,positions in snapshot['positions'].items():
                if symbol not in self.profile.symbols and any(D(str(q)) for q in positions):
                    failure_reason='Disabled/foreign inventory blocks startup';raise ValueError(failure_reason)
            for symbol in self.profile.symbols:
                actual = tuple(D(str(q)) for q in snapshot['positions'].get(symbol,(0,0)))
                if any(not q.is_finite() for q in actual):
                    failure_reason='Invalid account size';raise ValueError(failure_reason)
                expected = self.journal.inventory(symbol)
                drift=any(abs(a-e)>self.rules[symbol].step for a,e in zip(actual,expected))
                if self.journal.pending_reconcile(symbol):
                    if drift:failure_reason='Prior correction not confirmed by account snapshot; no repeated correction';raise ValueError(failure_reason)
                    self.journal.confirm_reconcile(symbol)
                if initial and not self.journal.has_history(symbol) and any(actual):
                    failure_reason='Unowned pre-existing position; dedicated account required';raise ValueError(failure_reason)
                if initial and actual!=expected:
                    failure_reason='Startup positions do not exactly match ledger';raise ValueError(failure_reason)
                if drift:
                    if self.journal.obligations(symbol):
                        failure_reason='Snapshot drift during unresolved hedge';raise ValueError(failure_reason)
                    if not await self.cancel_all():
                        return False
                    if generation != self.sequence or self.journal.obligations(symbol):
                        return False
                    ref = self.reference(symbol,1)
                    if ref is None:
                        failure_reason='Cannot value reconciliation correction';raise ValueError(failure_reason)
                    async with self.symbol_locks[symbol]:
                        if not self.sends_allowed() or generation!=self.sequence or self.queues[symbol].qsize():return False
                        self.journal.reconcile_inventory(symbol,*actual,ref)
                        self.sequence+=1
                        await self.drain_symbol(symbol)
                    if self.halted:return False
                    # No-send dust retains exactly the authoritative snapshot already read.
                    if not self.blocking_residuals(symbol) and self.journal.inventory(symbol)==actual:
                        self.journal.confirm_reconcile(symbol)
                        generation=self.sequence
                    else:
                        self.state='RECONCILING'
                        return True
            if any(snapshot.get('leverage',{}).get(v,1)>2 for v in ('arcus','lighter')):
                failure_reason='Returned venue leverage exceeds 2 '+self._leverage_detail(snapshot);raise ValueError(failure_reason)
            self.last_reconcile = self.clock()
            if not self.halted:
                if retire is not None:retire()
                self.state = 'ACTIVE' if self.data_ready() else 'PAUSED_DATA'
            return True
        except Exception as exc:
            self.halt('UNKNOWN_ORDER',failure_reason,error_type=type(exc).__name__)
            await self.cancel_all()
            return False

    async def _cancel_startup_foreign(self):
        first_error=None
        for _ in range(10):
            targets=tuple(self._startup_foreign.values())
            self._startup_foreign.clear()
            try:
                if targets:await self.transport.cancel_legacy(self.journal.known_exchange_ids(),targets)
                else:await self.transport.cancel_legacy(self.journal.known_exchange_ids())
            except Exception as exc:
                if first_error is None:first_error=exc
            if not self._startup_foreign:
                if first_error is not None:raise first_error
                return
        if first_error is not None:raise first_error
        raise RuntimeError('Startup foreign orders did not settle')

    async def recover(self):
        if self._recovery_started:
            raise RuntimeError('Startup recovery already attempted')
        self._recovery_started=True
        self._startup_recovered=False
        try:
            result=await self._recover()
            self._startup_recovered=bool(result and not self.halted)
            return result
        finally:
            self._startup_window=False

    async def _recover(self):
        for row in self.journal.db.execute("SELECT * FROM all_orders WHERE purpose='legacy'"):
            row=dict(row)
            if self.block_legacy_fill(row,D(row['filled']),row['exchange_id']):return False
        self.close_permits.clear()
        self.state = 'RECONCILING'
        for row in self.journal.makers(active=True):
            # PREPARED after a crash is potentially sent; NEVER resubmit it.
            try:
                obs = await asyncio.wait_for(self.transport.lookup_maker(row),self.profile.hedge.request_timeout)
                self._validate(obs,row,'arcus')
                if self.block_legacy_fill(row,obs.cumulative,obs.order_id):return False
                before=self.journal.inventory(row['symbol'])
                self.journal.observe_maker(row['client'],obs.cumulative,obs.status,obs.order_id,
                                          average=obs.average,fee=obs.fee,received=obs.received)
                self.manual_fill(row['symbol'],before,'recovery')
            except Exception:
                self.journal.observe_maker(row['client'],D(row['filled']),'unknown')
                self.halt(self.state,'Restart could not resolve prepared/sent maker identity',row['symbol'])
        await self.cancel_all()
        for symbol in self.profile.symbols:
            await self.drain_symbol(symbol)
        # Transport cancels every legacy account order; unknown legacy fills block adoption.
        try:
            await self._cancel_startup_foreign()
        except Exception as exc:
            self.halt('UNKNOWN_ORDER','Startup legacy orders could not be canceled',error_type=type(exc).__name__)
            return False
        # Subscribe-before-snapshot may buffer duplicates/fills during recovery. Consume before readiness.
        for _ in range(10):
            if not any(q.qsize() for q in self.queues.values()): break
            for symbol,q in self.queues.items():
                while not q.empty():
                    obs=q.get_nowait()
                    try:
                        if obs is not None:await self.observe_maker(obs)
                    finally: q.task_done()
                await self.drain_symbol(symbol)
        if self.halted or self.unresolved():
            self.state = 'UNKNOWN_ORDER'
            return False
        return await self.reconcile(initial=True)

    async def watchdog_once(self):
        self.observe_runtime()
        self.observe_market_health()
        if not self.transport.healthy():
            self.data_ready()
            self.state = 'PAUSED_DATA' if not self.halted else self.state
            for key in list(self.close_waits):self.close_quote_wait(*key,end_reason='quote_not_ready')
            await self.cancel_all()
            return
        for symbol in self.profile.symbols:
            if not self.symbol_data_ready(symbol):
                self.pause_basis(symbol)
                for key in list(self.close_waits):
                    if key[0]==symbol:self.close_quote_wait(*key,end_reason='quote_not_ready')
                await self.cancel_symbol(symbol)
        if not self.data_ready() and not self.halted:self.state='PAUSED_DATA'
        for venue in ('arcus','lighter'):
            if venue in self.equity:
                cap=min(self.equity[venue],self.profile.risk.allocation_per_venue)*self.profile.risk.leverage
                if self._gross(venue)*(1+self.profile.risk.reserve_ratio)>cap:
                    self.halt('EMERGENCY','Gross exposure/equity cap exceeded; makers canceled, account reconciliation required')
                    await self.cancel_all()
                    return
        for symbol in self.profile.symbols:
            self.basis_center(symbol)
            arcus,lighter = self.journal.inventory(symbol)
            prices=[self.books[v,symbol].ask for v in ('arcus','lighter') if (v,symbol) in self.books]
            if not prices:continue
            price = max(prices)
            exposure = abs(arcus+lighter)*price
            self.peak_exposure[symbol] = max(self.peak_exposure[symbol],exposure)
            if self.clock()-self._exposure_last.get(symbol,-1000)>=1:
                self.journal.event(symbol,'exposure',notional=exposure,peak=self.peak_exposure[symbol])
                self._exposure_last[symbol]=self.clock()
            if exposure > self.profile.risk.max_unhedged_notional:
                self.breach_since.setdefault(symbol,self.clock())
                self.alarm('Uncovered exposure above threshold',symbol)
                await self.cancel_all()
            elif symbol in self.breach_since:
                self.journal.event(symbol,'exposure_breach_end',duration=self.clock()-self.breach_since.pop(symbol))
        if self.state=='PAUSED_DATA' and not self.halted:
            await self.reconcile()

    def observe_runtime(self,*,force=False):
        quoted=tuple(sorted({r['symbol'] for r in self.journal.makers(active=True)
                              if r['status']=='open' and D(r['qty'])>D(r['filled'])}))
        now=self.clock()
        if force or quoted!=self._runtime_quoted or now-self._runtime_last>=1:
            self.journal.event('*','runtime_observation',boot=self.journal.boot,symbols=list(self.profile.symbols),
                               quoted_symbols=list(quoted),modeled=self.journal.modeled,observed_monotonic=now)
            self._runtime_last=now;self._runtime_quoted=quoted

    def observe_market_health(self):
        source=getattr(self.transport,'source',None)
        public_source=source is not None and hasattr(source,'book_validity')
        now=self.clock();wall=self.wall_clock()
        classified_invalid=False
        for symbol in self.profile.symbols:
            checks=[]
            for venue in ('arcus','lighter'):
                book=self.books.get((venue,symbol))
                generation=getattr(source,'generations',{}).get(venue,self.generations[venue,symbol])
                age_source=None
                if public_source:
                    result=source.book_validity(venue,symbol,book)
                elif book is None:
                    result=(False,'book_missing',now)
                elif book.generation!=generation:
                    result=(False,'generation_change',now);age_source='first_observed'
                elif now-book.received>self.profile.risk.stale_book_seconds:
                    result=(False,'book_age_exceeded',now)
                elif now<book.received:result=(False,'book_timestamp_future',now)
                else:result=(True,None,None)
                checks.append((venue,*result,generation,'public' if public_source else 'book_fallback',age_source))
            if hasattr(self.transport,'private_validity'):
                checks.extend((venue,*result,'private',None) for venue,result in self.transport.private_validity().items())
            for venue,valid,reason,since,generation,diagnostic_source,age_source in checks:
                key=(venue,symbol);old=self.market_stale.get(key)
                classified=reason in ('connection_timeout','sequence_gap','generation_change','reconnect')
                if valid or classified:self._book_diagnostics.pop(key,None)
                episode=f'{venue}:{generation}:{reason}'+(f':{since}' if reason=='connection_timeout' else '')
                if old and (valid or not classified or episode!=old['episode']):
                    self.journal.event(symbol,'market_recovered',**old,duration_seconds=max(0,now-old['since_mono']))
                    self._market_alarm_last.pop((old['boot'],symbol,venue,old['episode']),None)
                    self.market_stale.pop(key,None);old=None
                if not valid:
                    if not classified:
                        detail=reason if diagnostic_source=='book_fallback' else 'validity_unclassified'
                        observed=self._book_diagnostics.get(key)
                        if observed is None or observed['detail']!=detail:
                            observed=dict(detail=detail,at=wall,mono=now,last=-1000)
                            self._book_diagnostics[key]=observed
                        self.last_alarm='Unclassified market readiness unavailable'
                        if not self.halt_reason:self.reason=self.last_alarm
                        if now-observed['last']>=30:
                            payload=dict(reason='unclassified',detail=detail,diagnostic_source=diagnostic_source,
                                venue=venue,boot=self.journal.boot,state=self.state,age_source='first_observed',
                                observed_at=observed['at'],observed_duration_seconds=max(0,now-observed['mono']))
                            book=self.books.get((venue,symbol))
                            if diagnostic_source=='book_fallback' and book is not None:payload['book_age_seconds']=now-book.received
                            self.journal.event(symbol,'alarm',**payload);observed['last']=now
                        continue
                    classified_invalid=True
                    if old is None:
                        old=dict(venue=venue,reason=reason,since_mono=since,since_wall=wall-max(0,now-since),
                                 episode=episode,boot=self.journal.boot,diagnostic_source=diagnostic_source)
                        if age_source:old['age_source']=age_source
                        self.market_stale[key]=old
                        self.journal.event(symbol,'market_stale',**old)
                    self.last_alarm=reason
                    if not self.halt_reason:self.reason=reason
                    alarm_key=(old['boot'],symbol,venue,old['episode'])
                    if now-self._market_alarm_last.get(alarm_key,-1000)>=30:
                        self.journal.event(symbol,'alarm',**old,state=self.state,duration_seconds=max(0,now-old['since_mono']))
                        self._market_alarm_last[alarm_key]=now
        if not self.transport.healthy() and not classified_invalid:
            self.alarm('Unclassified transport health unavailable')
        if not public_source:return
        if now-self._exposure_last.get('close_timeouts',-1000)>=1:
            self.close_basis.sync()
            estimates={}
            for symbol,cfg in self.profile.symbols.items():
                arcus,_=self.journal.inventory(symbol)
                if arcus and self.symbol_data_ready(symbol):
                    side=-1 if arcus>0 else 1;reference=self.reference(symbol,side);ab=self.books['arcus',symbol]
                    center=self.basis_center(symbol)
                    if center is not None:
                        price=quote_price(side,reference,cfg.close_threshold,self.rules[symbol],ab.bid,ab.ask,center)
                        if price is not None:estimates.update(self.close_basis.timeout_estimates(symbol,reference,price))
            self.close_basis.diagnostics.timeouts(self.close_basis.states,self.profile.symbols,wall,estimates)
            self._exposure_last['close_timeouts']=now

    async def _watchdog(self):
        while not self.stopping:
            await self.watchdog_once()
            await asyncio.sleep(.05)

    async def start(self):
        if set(self.rules) != set(self.profile.symbols):
            raise ValueError('Missing venue metadata')
        if not await self.recover():
            raise RuntimeError('Startup reconciliation blocked: '+(self.halt_reason or self.reason or 'see journal alarms'))
        self.tasks = [asyncio.create_task(self._worker(s)) for s in self.profile.symbols]
        self.tasks.append(asyncio.create_task(self._watchdog()))

    async def tick(self):
        if not self.sends_allowed():return
        for symbol in self.profile.symbols:
            if (not self.symbol_locks[symbol].locked() and not self.queues[symbol].qsize()
                    and self.blocking_residuals(symbol)):
                self.wake_hedge(symbol)
        if self.clock()-self.last_reconcile >= self.profile.risk.reconcile_interval:
            await self.reconcile()
        if self.state=='ACTIVE' and not self.halted:
            await asyncio.gather(*(self.quote_symbol(s) for s in self.profile.symbols))

    async def stop(self, timeout=15):
        self.stopping = True
        self.close_permits.clear()
        for key in list(self.close_waits):self.close_quote_wait(*key,end_reason='stopping')
        self.state = 'STOPPING'
        async def finish():
            await self.cancel_all()
            if self.cancel_tasks:await asyncio.gather(*list(self.cancel_tasks),return_exceptions=True)
            if not self.tasks:
                for q in self.queues.values():
                    while not q.empty():
                        obs=q.get_nowait()
                        try:
                            if obs is not None:await self.observe_maker(obs)
                        finally:q.task_done()
            for q in self.queues.values():
                await q.join()
            for symbol in self.profile.symbols:
                async with self.symbol_locks[symbol]:
                    await self.drain_symbol(symbol)
            await self.reconcile()
        try:
            await asyncio.wait_for(finish(),timeout)
        except Exception:
            self.alarm('Shutdown incomplete; journal retained for reconciliation')
        for task in self.tasks:
            task.cancel()
        for task in self.cancel_tasks:task.cancel()
        await asyncio.gather(*list(self.cancel_tasks),return_exceptions=True)
        await asyncio.gather(*self.tasks,return_exceptions=True)
        clean = self.sends_allowed() and self._startup_recovered and not self._legacy_blocked and not self.unresolved() and not self.journal.pending_reconcile() and not self.cancel_failed
        self.state='STOPPING'
        self.close_basis.sync();self.observe_runtime(force=True)
        self.journal.event('*','runtime_end',boot=self.journal.boot,symbols=list(self.profile.symbols),modeled=self.journal.modeled,
                           observed_monotonic=self.clock())
        self.journal.event('*','shutdown',clean=clean,pending_residuals=len(self.journal.obligations()),
                           positions_flat=all(not any(self.journal.inventory(s)) for s in self.profile.symbols),
                           manual_intervention_required=self.manual_intervention_required,
                           manual_intervention_persisted=self.manual_intervention_persisted,
                           manual_intervention_snapshot=self.manual_intervention_snapshot)
        return clean


def worker_error_detail(exc,obs):
    """Code location of a worker failure without raw exception payloads (which may carry credentials).
    Only ValueErrors raised by this project's maker/taker modules (fixed, ledger-derived text) keep their message."""
    import os,traceback
    frames=[(os.path.basename(f.filename),f.lineno,f.name) for f in traceback.extract_tb(exc.__traceback__)]
    detail={'error_type':type(exc).__name__,'frames':[f'{n}:{l}:{fn}' for n,l,fn in frames[-8:]]}
    if isinstance(exc,ValueError) and frames and frames[-1][0].startswith('maker_taker_'):
        detail['message']=str(exc)[:500]
    if obs is not None:
        detail['observation']={k:str(getattr(obs,k,None)) for k in ('client','order_id','symbol','status','cumulative','requested')}
    return detail
