"""Live bridge, imported only by explicit live mode after profile and journal validation."""
import asyncio
import hashlib
import os
import time
from collections import deque
from types import SimpleNamespace
from decimal import Decimal

from core.adapters.exchanges.interface import ExchangeConfig
from core.adapters.exchanges.models import ExchangeType,OrderSide,OrderType,PositionSide
from core.adapters.exchanges.adapters.arcus import ArcusAdapter,ArcusRequestRejectedError
from core.adapters.exchanges.adapters.lighter import LighterAdapter
from core.adapters.exchanges.order_state import OrderStateUnknownError,OrderAdmissionDeferredError,OrderSendRejectedError
from .maker_taker_engine import OrderObservation,DefinitiveFailure,DeferredHedge
from .maker_taker_journal import TERMINAL

from .maker_taker_quotes import hedge_quantity,lighter_ioc_price

D=Decimal


def live_credentials():
    """No implicit dotenv/ancestor config search. Only the user-selected live environment is read."""
    if os.getenv('LIGHTER_VERIFY_SSL','true').lower()!='true':
        raise ValueError('Live maker mode requires verified TLS')
    # Never read config/exchanges/*_config.yaml or any credential file as a fallback.
    a={key:os.environ.get('ARCUS_'+key,'') for key in ('API_KEY','API_SIGNING_KEY','WALLET_ADDRESS','ACCOUNT_INDEX')}
    l={key:os.environ.get('LIGHTER_'+key,'') for key in ('API_KEY_PRIVATE_KEY','ACCOUNT_INDEX','API_KEY_INDEX')}
    if not all(a[k] for k in ('API_KEY','API_SIGNING_KEY','WALLET_ADDRESS')) or not all(l.values()):
        raise ValueError('Explicit Arcus API key/seed/address and Lighter key/account/key index are required')
    ai=int(a['ACCOUNT_INDEX'] or 0); li=int(l['ACCOUNT_INDEX']); ki=int(l['API_KEY_INDEX'])
    if not 0<=ai<=9 or li<0 or not 0<=ki<=254:
        raise ValueError('Invalid account/key index')
    identity='live-mainnet-'+hashlib.sha256(f"{a['WALLET_ADDRESS'].lower()}:{ai}|{li}".encode()).hexdigest()
    return a,l,ai,li,ki,identity


class LiveTransport:
    # Unknown Lighter hedges: poll active+inactive history this long before declaring UNKNOWN.
    lookup_poll_seconds=12
    lookup_timeout=25
    def __init__(self,source,journal,credentials):
        a,l,ai,li,ki,self.identity=credentials
        self.source,self.journal=source,journal
        self.arcus=ArcusAdapter(ExchangeConfig('arcus','Arcus',ExchangeType.PERPETUAL,a['API_KEY'],'',
            wallet_address=a['WALLET_ADDRESS'],private_key=a['API_SIGNING_KEY'],
            extra_params={'account_index':ai,'reconcile_timeout':1}))
        config=ExchangeConfig('lighter','Lighter',ExchangeType.PERPETUAL,'','',
            base_url='https://mainnet.zklighter.elliot.ai',ws_url='wss://mainnet.zklighter.elliot.ai/stream',
            extra_params={'strict_order_state':True,'official_transport_only':True,'api_key_private_key':l['API_KEY_PRIVATE_KEY'],
                          'account_index':li,'api_key_index':ki})
        self.lighter=LighterAdapter(config)
        for part in (self.lighter._base,self.lighter._rest,self.lighter._websocket):
            if (part.base_url!='https://mainnet.zklighter.elliot.ai' or part.ws_url!='wss://mainnet.zklighter.elliot.ai/stream'
                    or part.account_index!=li or part.api_key_index!=ki):
                raise ValueError('Final Lighter signer identity/endpoint differs from journal lock identity')
        self.arcus.write_audit=journal.audit_write
        self.lighter._rest.write_audit=journal.audit_write
        self.writes=deque()
        self.private_generation=None

    def healthy(self):
        while self.writes and self.writes[0]<=time.monotonic()-60: self.writes.popleft()
        return self.source.healthy() and all(r[0] for r in self.private_validity().values()) and len(self.writes)<40

    def configure_liveness(self,stale,quick):
        for ws in (self.arcus._websocket,self.lighter._websocket):
            ws.liveness_seconds=stale;ws.quick_resume_seconds=quick

    def private_validity(self):
        aw=self.arcus._websocket;lw=self.lighter._websocket;now=time.monotonic()
        pending=any(g==getattr(aw,'generation',0) for g in getattr(aw,'_startup_recoveries',{}).values())
        ready={'arcus_private':aw._ws_connected and ('orders',None) in aw._ready and ('userFills',None) in aw._ready and not pending,
            'lighter_private':lw._direct_ws is not None and getattr(lw,'_private_ready_socket',None) is lw._direct_ws
                and getattr(lw,'_reader_connected',False) and getattr(lw,'_reader_socket',None) is lw._direct_ws
                and getattr(lw,'_reader_generation',None)==getattr(lw,'generation',None)
                and getattr(lw,'_private_ready_generation',None)==getattr(lw,'generation',None)}
        result={}
        for name,ws in (('arcus_private',aw),('lighter_private',lw)):
            last=getattr(ws,'last_received_monotonic',0);stale=getattr(self.source,'stale_seconds',5)
            valid=bool(ready[name] and last and 0<=now-last<=stale)
            reason=('startup_recovery' if name=='arcus_private' and pending else
                    ('connection_timeout' if ready[name] else 'reconnect'))
            result[name]=(valid,None if valid else reason,
                          last+stale if ready[name] else now,getattr(ws,'generation',0))
        return result

    def private_episodes(self):
        aw=self.arcus._websocket;lw=self.lighter._websocket
        return ((getattr(aw,'generation',0),id(getattr(aw,'_socket',None)),getattr(aw,'liveness_revision',0)),
                (getattr(lw,'generation',0),id(getattr(lw,'_reader_socket',None)),getattr(lw,'liveness_revision',0)))

    def normalize(self,order,venue,symbol=None,client=None):
        raw=order.raw_data or {}
        fee=None
        # Only actual reported fees in the quote currency count; unknown is not zero.
        if isinstance(order.fee,dict) and str(order.fee.get('currency','')).upper() in ('USDC','USD'):
            fee=D(str(order.fee['cost']))
        status=order.status.value
        # Arcus's old valuation guard can mark a known-quantity terminal row PENDING.
        raw_status=raw.get('status',raw.get('state','')).lower()
        if venue=='arcus' and raw_status in ('filled','canceled','rejected','expired'):
            status=raw_status
        remaining=reported_status=None
        evidence=raw.get('_reported_order',{}) if venue=='arcus' else {}
        terminal={'FILLED':'filled','CANCELED':'canceled','MARGIN_CANCELED':'canceled',
                  'REJECTED':'rejected','EXPIRED':'expired'}
        state=evidence.get('state') or evidence.get('status')
        if evidence.get('status') in terminal:
            state=evidence['status']
        consistent=(not evidence.get('status') or not evidence.get('state')
                    or terminal.get(evidence['status'],evidence['status'])
                    == terminal.get(evidence['state'],evidence['state']))
        if state in (*terminal,'OPEN','PLACED','PARTIALLY_FILLED') and consistent and evidence.get('cancelReason')!='MODIFY_CANCELED':
            try:
                original=D(str(evidence.get('originalSize')))
                reported_remaining=D(str(evidence.get('remainingSize')))
                if (original.is_finite() and reported_remaining.is_finite()
                        and 0<=reported_remaining<=original):
                    remaining=reported_remaining
                    reported_status=state.lower()
            except (ValueError, ArithmeticError):
                pass
        side=1 if order.side==OrderSide.BUY else -1
        requested=D(order.amount);cumulative=D(order.filled)
        if venue=='arcus' and raw:
            # A partial push for an order no longer cached omits side/size; the adapter's BUY/0 defaults are not evidence.
            if not any(k in raw for k in ('side','orderSide')):side=None
            if not any(k in raw for k in ('originalSize','quantity')):requested=cumulative=None
            # Seen live: REJECTED pushes with originalSize 0 (cumulative derived as 0.13). A zero/negative size is not
            # quantity evidence; the row then becomes status-only and inherits size/filled from the ledger.
            elif requested is not None and requested<=0:requested=cumulative=None
        return OrderObservation(venue,self.identity,symbol or order.symbol,str(client or order.client_id or ''),str(order.id),
            side,requested,cumulative,status,'validated_adapter',time.monotonic(),
            sequence=str(raw.get('sequenceNumber','')),average=order.average,fee=fee,
            remaining=remaining,reported_status=reported_status,startup_snapshot=raw.get('_startup_snapshot') is True,
            startup_provisional=raw.get('_startup_provisional') is True)

    def startup_terminal_retirement(self,observations):
        frozen=tuple((obs,self.arcus._orders.get(obs.order_id)) for obs in observations)
        generation=self.arcus._websocket.generation
        def retire():
            ws=self.arcus._websocket
            if ws.generation!=generation or any(g==generation for g in ws._startup_recoveries.values()):return
            for obs,order in frozen:
                if (order is None or self.arcus._orders.get(obs.order_id) is not order
                        or not order.raw_data.get('_valuation_unknown') or order.status.value!='pending'
                        or obs.order_id not in self.arcus._uncertain
                        or obs.order_id in self.arcus._cancel_uncertain or obs.order_id in self.arcus._modify_uncertain
                        or (obs.client and obs.client in self.arcus._uncertain)):
                    continue
                known=self.journal.db.execute(
                    'SELECT 1 FROM all_orders WHERE client=? OR exchange_id=? UNION ALL '
                    "SELECT 1 FROM all_attempts WHERE client=? OR (venue='arcus' AND exchange_id=?) LIMIT 1",
                    (obs.client,obs.order_id,obs.client,obs.order_id)).fetchone()
                current=self.normalize(order,'arcus')
                fields=('venue','account','symbol','client','order_id','side','requested','cumulative',
                        'status','reported_status','remaining','sequence')
                if not known and all(getattr(current,key)==getattr(obs,key) for key in fields):
                    self.arcus._uncertain.discard(obs.order_id)
        return retire

    async def connect(self,receive):
        # Public metadata was loaded without a signer. Use the same authoritative market snapshot.
        self.arcus._set_markets([rows[0] for rows in self.source.markets.values()])
        await self.arcus._do_authenticate()
        await self.arcus._websocket.connect()
        def quantity(order):
            if self.journal.db.execute("SELECT 1 FROM all_attempts WHERE client=? AND venue='arcus' LIMIT 1",(str(order.client_id),)).fetchone():
                return # Emergency lifecycle is reconciled by its original durable attempt, never a maker fill.
            # Unknown/foreign rows are delivered too: engine fails closed instead of silently ignoring.
            receive(self.normalize(order,'arcus'))
        await self.arcus._websocket.subscribe_quantity_observations(quantity)
        if not await self.lighter.connect():
            raise RuntimeError('Lighter connection failed')
        await self.lighter._websocket.subscribe_orders(None)

    async def maker(self,row):
        deferred=False
        try:
            params={'timeInForce':'POST_ONLY','clientId':row['client'],'reduceOnly':row['purpose']=='close'}
            guard=getattr(self,'maker_guard',None)
            def admitted():
                nonlocal deferred
                if not getattr(self,'send_guard',lambda:True)():deferred=True;return False
                return guard is not None and guard(row)
            params['_pre_sign_guard']=admitted
            order=await self.arcus.create_order(row['symbol'],OrderSide.BUY if row['side']==1 else OrderSide.SELL,
                OrderType.LIMIT,D(row['qty']),D(row['price']),params=params)
        except ArcusRequestRejectedError as exc:
            if deferred:raise DeferredHedge('Manual intervention changed before signing; nothing sent') from exc
            raise DefinitiveFailure('Arcus definitively rejected maker') from exc
        return self.normalize(order,'arcus',row['symbol'],row['client'])

    async def lookup_maker(self,row):
        try:
            order=await (self.arcus.get_order(row['exchange_id'],row['symbol']) if row['exchange_id'] else
                         self.arcus.get_order_by_client(row['client'],row['symbol']))
        except OrderStateUnknownError:
            cached=self.arcus._orders.get(row['exchange_id'])
            # Known cumulative quantity is still observable if valuation is missing.
            if cached is None or cached.filled<=D(row['filled']): raise
            order=cached
        return self.normalize(order,'arcus',row['symbol'],row['client'])

    async def cancel_maker(self,row):
        if not row['exchange_id']:
            observed=await self.lookup_maker(row)
            row={**row,'exchange_id':observed.order_id}
        order=await self.arcus.cancel_known_order(row['exchange_id'],row['symbol'])
        return self.normalize(order,'arcus',row['symbol'],row['client'])

    async def _terminal_lighter(self,attempt,obligation,seconds=3,history=False):
        rest=self.lighter._rest
        symbol=self.lighter._normalize_symbol(obligation['symbol'])
        deadline=time.monotonic()+seconds
        next_rest=0
        while True:
            order=rest._lookup_terminal_order(attempt['client'],symbol)
            if order is not None:
                rest._uncertain_orders.pop(str(attempt['client']),None)
                return self.normalize(order,'lighter',obligation['symbol'],attempt['client'])
            if time.monotonic()>=next_rest:
                try:
                    order=await rest.get_order(attempt.get('exchange_id') or attempt['client'],symbol)
                    if order.status.value in ('filled','canceled','rejected','expired'):
                        return self.normalize(order,'lighter',obligation['symbol'],attempt['client'])
                except Exception:
                    pass
                if history:
                    # IOC orders leave the active book at once; inactive history can lag by seconds.
                    try:
                        order=await rest.find_order_by_client(attempt['client'],symbol)
                        if order.status.value in ('filled','canceled','rejected','expired'):
                            rest._uncertain_orders.pop(str(attempt['client']),None)
                            return self.normalize(order,'lighter',obligation['symbol'],attempt['client'])
                    except Exception:
                        pass
                next_rest=time.monotonic()+2
            if time.monotonic()>=deadline: raise OrderStateUnknownError('lighter',client_id=attempt['client'])
            await asyncio.sleep(.02)

    async def hedge(self,attempt,obligation):
        if not getattr(self,'send_guard',lambda:True)():raise DeferredHedge('Manual intervention forbids new hedge')
        import lighter
        rest=self.lighter._rest
        symbol=self.lighter._normalize_symbol(obligation['symbol'])
        a,l=self.source.markets[obligation['symbol']]
        book=self.source.books.get(('lighter',obligation['symbol']))
        if not self.source.book_validity('lighter',obligation['symbol'],book)[0]:
            raise DefinitiveFailure('No fresh public hedge reference before signing')
        while self.writes and self.writes[0]<=time.monotonic()-60: self.writes.popleft()
        if len(self.writes)>=45: raise DefinitiveFailure('Conservative Lighter write-rate budget exhausted')
        side=-obligation['side']; qty=D(attempt['qty'])
        if hedge_quantity(qty,self.source.rules[obligation['symbol']],book.ask if side==1 else book.bid)!=qty:
            raise DeferredHedge('Hedge below conservative policy before signing')
        size_units=qty/(D(10)**-l['supported_size_decimals'])
        if size_units<=0 or size_units!=size_units.to_integral_value():
            raise DefinitiveFailure('Hedge precision mismatch')
        price=lighter_ioc_price(side,book.bid,book.ask,l['supported_price_decimals'])
        native={'market_index':int(l['market_id']),'client_order_index':int(attempt['client']),
                'base_amount':int(size_units),'price':price,'is_ask':side==-1,
                'order_type':lighter.SignerClient.ORDER_TYPE_MARKET,
                'time_in_force':lighter.SignerClient.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,
                'order_expiry':lighter.SignerClient.DEFAULT_IOC_EXPIRY,
                'reduce_only':obligation['purpose']=='close'}
        rest._register_order_intent(attempt['client'],symbol,'buy' if side==1 else 'sell',qty,int(l['market_id']))
        def admitted():
            current=self.source.books.get(('lighter',obligation['symbol']))
            return (getattr(self,'send_guard',lambda:True)() and all(r[0] for r in self.private_validity().values())
                and self.source.book_validity('lighter',obligation['symbol'],current)[0]
                and current is not None and current.generation==book.generation
                and lighter_ioc_price(side,current.bid,current.ask,l['supported_price_decimals'])==native['price']
                and hedge_quantity(qty,self.source.rules[obligation['symbol']],current.ask if side==1 else current.bid)==qty)
        self._clear_settled_uncertain(obligation['symbol'])
        stamp=time.monotonic();self.writes.append(stamp)
        try:await rest._strict_submit(rest.signer_client.sign_create_order,native,attempt['client'],pre_sign_guard=admitted)
        except OrderAdmissionDeferredError as exc:
            self.writes.remove(stamp)
            raise DeferredHedge('Hedge admission changed before signing; nothing sent') from exc
        except OrderSendRejectedError as exc:
            self.journal.event(obligation['symbol'],'lighter_send_rejected',client=attempt['client'],**{
                k:v for k,v in exc.detail.items() if k!='client'})
            raise DefinitiveFailure('Lighter API explicitly rejected hedge: '+str(exc.detail.get('body') or exc.detail.get('message'))[:200]) from exc
        except OrderStateUnknownError as exc:
            if not self._audited(attempt['client']):
                self._not_sent(attempt,obligation,exc)
                raise DefinitiveFailure('Lighter hedge not sent (blocked before signing)') from exc
            detail=getattr(rest,'last_send_error',None)
            if detail:
                self.journal.event(obligation['symbol'],'lighter_send_error',client=attempt['client'],**{
                    k:v for k,v in detail.items() if k!='client'})
            raise
        except Exception as exc:
            # Nonce fetch, signing or audit failures happen before the audited send; nothing reached Lighter.
            if not self._audited(attempt['client']):
                self._not_sent(attempt,obligation,exc)
                raise DefinitiveFailure('Lighter hedge not sent (failed before signing)') from exc
            raise
        return await self._terminal_lighter(attempt,obligation)

    def _audited(self,client):
        return self.journal.db.execute('SELECT 1 FROM writes WHERE client=? LIMIT 1',(str(client),)).fetchone() is not None

    def _not_sent(self,attempt,obligation,exc):
        try:
            from .maker_taker_engine import worker_error_detail
            detail=worker_error_detail(exc,None)
        except Exception:
            detail={'error_type':type(exc).__name__}
        detail.pop('observation',None)
        self.journal.event(obligation['symbol'],'lighter_hedge_not_sent',client=attempt['client'],
                           blocking_client=getattr(exc,'client_id',None),**{k:v for k,v in detail.items() if k!='client'})
        self._clear_settled_uncertain(obligation['symbol'])

    def _clear_settled_uncertain(self,symbol='*'):
        """Adapter write gate entries whose attempt the journal already holds terminal (e.g. resolved from
        inactive history) would block every later hedge before signing."""
        rest=self.lighter._rest
        for client in list(getattr(rest,'_uncertain_orders',{})):
            row=self.journal.db.execute('SELECT status FROM all_attempts WHERE client=? AND venue=?',
                                        (str(client),'lighter')).fetchone()
            if row is not None and row['status'] in TERMINAL:
                rest._uncertain_orders.pop(client,None)
                self.journal.event(symbol,'lighter_uncertain_cleared',client=str(client),status=row['status'])

    async def lookup_attempt(self,attempt,obligation):
        if attempt['venue']=='arcus':
            row={**attempt,'symbol':obligation['symbol'],'side':-obligation['side']}
            order=await (self.arcus.get_order(attempt['exchange_id'],obligation['symbol']) if attempt['exchange_id'] else
                         self.arcus.get_order_by_client(attempt['client'],obligation['symbol']))
            return self.normalize(order,'arcus',obligation['symbol'],attempt['client'])
        rest=self.lighter._rest
        symbol=self.lighter._normalize_symbol(obligation['symbol'])
        if attempt['client'] not in getattr(rest,'_submitted_orders',{}):
            rest._register_order_intent(attempt['client'],symbol,'sell' if obligation['side']==1 else 'buy',D(attempt['qty']))
        if not self._audited(attempt['client']):
            # The audit row is committed before send_tx under the write lock; without it nothing was sent.
            self.journal.event(obligation['symbol'],'lighter_attempt_never_sent',client=attempt['client'])
            return OrderObservation('lighter',self.identity,obligation['symbol'],str(attempt['client']),'',
                                    -obligation['side'],D(attempt['qty']),D(0),'rejected','unsent_no_audit',time.monotonic())
        await self._transaction_evidence(attempt,obligation)
        try:
            return await self._terminal_lighter(attempt,obligation,seconds=self.lookup_poll_seconds,history=True)
        except OrderStateUnknownError:
            await self._transaction_evidence(attempt,obligation)
            order=await rest.find_order_by_client(attempt['client'],symbol)
            if order.status.value in ('filled','canceled','rejected','expired'):
                rest._uncertain_orders.pop(str(attempt['client']),None)
            return self.normalize(order,'lighter',obligation['symbol'],attempt['client'])

    async def _transaction_evidence(self,attempt,obligation):
        """Audit-only: records what Lighter says about the signed tx; never infers fills from it."""
        rest=self.lighter._rest
        write=self.journal.db.execute('SELECT * FROM writes WHERE client=? ORDER BY id DESC LIMIT 1',
                                      (attempt['client'],)).fetchone()
        if not write:
            self.journal.event(obligation['symbol'],'unknown_transaction_evidence_unavailable',client=attempt['client'],
                               error_type='NoAuditedWrite',error='no writes row for client')
            return
        if not write['signed_hash']:
            self.journal.event(obligation['symbol'],'unknown_transaction_evidence_unavailable',client=attempt['client'],
                               error_type='NoSignedHash',error='writes row has no signed hash')
            return
        try:
            tx=await asyncio.wait_for(rest.transaction_api.tx(by='hash',value=write['signed_hash']),3)
            nonce=await asyncio.wait_for(rest.transaction_api.next_nonce(account_index=rest.account_index,
                                                                          api_key_index=write['key_index']),3)
            if (tx.hash!=write['signed_hash'] or tx.account_index!=rest.account_index
                    or tx.api_key_index!=write['key_index'] or tx.nonce!=int(write['nonce'])):
                raise ValueError('Transaction identity mismatch')
            self.journal.event(obligation['symbol'],'unknown_transaction_evidence',client=attempt['client'],
                               transaction_status=tx.status,next_nonce=getattr(nonce,'nonce',None),
                               signed_nonce=write['nonce'],key_index=write['key_index'],
                               signed_hash=write['signed_hash'],
                               event_info=str(getattr(tx,'event_info',''))[:500],
                               code=getattr(tx,'code',None),message=str(getattr(tx,'message',''))[:300])
        except Exception as exc:
            self.journal.event(obligation['symbol'],'unknown_transaction_evidence_unavailable',client=attempt['client'],
                               error_type=type(exc).__name__,signed_hash=write['signed_hash'],
                               **({'error':str(exc)[:300]} if isinstance(exc,ValueError) else {}))

    async def emergency(self,attempt,obligation):
        if not getattr(self,'send_guard',lambda:True)():raise DeferredHedge('Manual intervention forbids emergency order')
        side=-obligation['side']
        deferred=False
        def admitted():
            nonlocal deferred
            allowed=getattr(self,'send_guard',lambda:True)()
            deferred=not allowed
            return allowed
        try:
            order=await self.arcus.create_order(obligation['symbol'],OrderSide.BUY if side==1 else OrderSide.SELL,
                OrderType.MARKET,D(attempt['qty']),params={'clientId':attempt['client'],
                    'reduceOnly':obligation['purpose']=='open','slippage':D('.001'),'_pre_sign_guard':admitted})
        except ArcusRequestRejectedError as exc:
            if deferred:raise DeferredHedge('Manual intervention changed before signing; nothing sent') from exc
            raise DefinitiveFailure('Emergency order definitively rejected') from exc
        return self.normalize(order,'arcus',obligation['symbol'],attempt['client'])

    async def snapshot(self):
        self.arcus._invalidate_account_reads()
        self.lighter._rest._invalidate_account_snapshot()
        ap,lp,ab,lb=await asyncio.gather(self.arcus.get_positions(),self.lighter.get_positions(),
                                      self.arcus.get_balances(),self.lighter.get_balances())
        positions={}
        leverage={}
        leverage_detail={}
        for venue,rows in enumerate((ap,lp)):
            for p in rows:
                symbol=p.symbol.split('-')[0].split('/')[0]+'-USDC-PERP'
                values=positions.setdefault(symbol,[D(0),D(0)])
                values[venue]+=D(p.size)*(1 if p.side==PositionSide.LONG else -1)
                if venue==0 and 'leverage' in p.raw_data:
                    leverage['arcus']=max(leverage.get('arcus',0),p.leverage)
                    leverage_detail['arcus:'+symbol]=p.leverage
        def equity(rows):
            values=[D(r.total) for r in rows if r.currency in ('USDC','USD')]
            if len(values)!=1 or not values[0].is_finite(): raise ValueError('Authoritative equity unavailable')
            return values[0]
        return {'authoritative':True,'equity':{'arcus':equity(ab),'lighter':equity(lb)},
                'positions':positions,'leverage':leverage,'leverage_detail':leverage_detail,'venue_leverage_setting':'operator_ui_required'}

    async def cancel_legacy(self,known,targets=()):
        orders={str(o.id):o for o in await self.arcus.get_open_orders()}
        for target in targets:
            if (target.venue!='arcus' or target.account!=self.identity or not target.order_id
                    or not target.cumulative.is_finite() or target.cumulative!=0):
                raise RuntimeError('Foreign startup order has filled or invalid identity')
            # Snapshot identities remain cancellation targets even if REST omits them.
            existing=orders.get(target.order_id)
            if existing is not None and (existing.symbol!=target.symbol or existing.amount!=target.requested
                    or str(existing.client_id or existing.id)!=str(target.client or target.order_id)
                    or (1 if existing.side==OrderSide.BUY else -1)!=target.side):
                raise RuntimeError('Startup REST order conflicts with private snapshot identity')
            orders.setdefault(target.order_id,SimpleNamespace(id=target.order_id,client_id=target.client,
                symbol=target.symbol,side=OrderSide.BUY if target.side==1 else OrderSide.SELL,
                amount=target.requested,filled=target.cumulative,price=None))
        async def cancel(order):
            if (not order.filled.is_finite() or order.filled!=0 or self.journal.db.execute(
                    'SELECT 1 FROM all_orders WHERE client=? OR exchange_id=? UNION ALL '
                    "SELECT 1 FROM all_attempts WHERE client=? OR (venue='arcus' AND exchange_id=?) LIMIT 1",
                    (str(order.client_id or order.id),str(order.id),str(order.client_id or order.id),str(order.id))).fetchone()):
                raise RuntimeError('Legacy order has fills or conflicts with durable identity')
            client=self.journal.legacy_cancel_intent(order)
            result=await self.arcus.cancel_known_order(order.id,order.symbol)
            observed=self.normalize(result,'arcus')
            if (str(result.id)!=str(order.id) or result.symbol!=order.symbol or result.side!=order.side
                    or result.amount!=order.amount or str(result.client_id or result.id)!=str(order.client_id or order.id)
                    or not result.filled.is_finite() or result.filled!=0
                    or observed.status not in ('filled','canceled','rejected','expired')
                    or observed.reported_status not in ('filled','canceled','margin_canceled','rejected','expired')):
                raise RuntimeError('Legacy cancellation lacks conclusive matching terminal state')
            self.journal.observe_maker(client,result.filled,observed.status,result.id,
                                       average=result.average)
            return result
        outcomes=await asyncio.gather(*(cancel(o) for oid,o in orders.items() if oid not in known),return_exceptions=True)
        if any(isinstance(o,BaseException) for o in outcomes):
            raise RuntimeError('Legacy account orders could not be conclusively canceled')
        # There must be no unowned Lighter order; this runner does not cancel arbitrary hedge strategies.
        if await self.lighter.get_open_orders():
            raise RuntimeError('Foreign Lighter orders require a dedicated account')

    async def arcus_history(self,kind,start,end):
        field='fills' if kind=='fills' else 'fundingPayments'
        time_field='createdAt' if kind=='fills' else 'time'
        path='/v1/fills' if kind=='fills' else '/v1/funding'
        lower=int(start*1_000_000);upper_initial=int(end*1_000_000)
        for symbol in self.source.symbols:
            upper=upper_initial;seen=set()
            for _ in range(1000):
                _,data=await self.arcus._request('GET',path,query={**self.arcus._account_query(),
                    'market':self.arcus.native_symbol(symbol),'from':lower,'to':upper,'limit':1000})
                if not isinstance(data,dict) or not isinstance(data.get(field),list):
                    raise ValueError('Arcus finance history malformed')
                rows=data[field]
                minimum=upper
                for row in rows:
                    scoped=('address' not in row or str(row['address']).lower()==self.arcus.address) and (
                        'accountIndex' not in row or row['accountIndex']==self.arcus.account_index)
                    if not scoped or self.arcus.standard_symbol(row['marketDisplayName'])!=symbol:
                        raise ValueError('Arcus finance history account/market mismatch')
                    at=int(row[time_field])
                    if not lower<=at<=upper:raise ValueError('Arcus finance history outside requested time window')
                    minimum=min(minimum,at)
                    identity=(str(row['tradeId']),str(row['orderId'])) if kind=='fills' else (str(row['marketId']),at)
                    if identity in seen:continue
                    seen.add(identity)
                    yield row
                if len(rows)<1000:
                    if data.get('hasMore') or data.get('nextCursor'):raise ValueError('Unexpected Arcus history continuation')
                    break
                if minimum>=upper:raise ValueError('Arcus full same-microsecond page cannot prove complete coverage')
                upper=minimum # inclusive overlap; never skip a timestamp boundary
            else:raise ValueError('Arcus finance history page budget exhausted')

    def arcus_funding_row(self,row):
        symbol=self.arcus.standard_symbol(row['marketDisplayName'])
        if symbol not in self.source.symbols:raise ValueError('Foreign Arcus funding symbol')
        return symbol,-D(str(row['payment'])),str(row['marketId'])+':'+str(row['time']),int(row['time'])/1_000_000

    async def close(self):
        await asyncio.gather(self.arcus._do_disconnect(),self.lighter.disconnect(),return_exceptions=True)
