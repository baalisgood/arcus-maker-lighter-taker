"""Public-only market I/O and conservative deterministic paper execution. No account SDK."""
import asyncio
import json
import time
from decimal import Decimal

import aiohttp
from core.adapters.exchanges.official_transport import websocket_connect,ReceivedConnection

from .maker_taker_engine import Book,OrderObservation,DefinitiveFailure,DeferredHedge
from .maker_taker_quotes import hedge_quantity, MarketRules,floor_quantity,lighter_ioc_price

D=Decimal
ARCUS='https://api.arcus.xyz'
LIGHTER='https://mainnet.zklighter.elliot.ai'


class PublicMarkets:
    """Only fixed unauthenticated GET endpoints and public WebSocket subscriptions exist here."""
    def __init__(self,symbols,*,clock=time.monotonic):
        self.clock=clock
        self.symbols=tuple(symbols)
        self.markets={}
        self.market_ids={}
        self.rules={}
        self.books={}
        self.connected={'arcus':False,'lighter':False}
        self.generations={'arcus':0,'lighter':0}
        self.tasks=[]
        self.callback=lambda *args: None
        self.stopped=False
        self.last_error={}
        self.stale_seconds=5
        self.quick_resume_seconds=10
        self.last_message_at={}
        self.invalid={}
        self.recovery_revision={'arcus':0,'lighter':0}

    def received(self,venue,generation):
        if not self.connected[venue] or generation!=self.generations[venue]:return
        now=self.clock();previous=self.last_message_at.get(venue)
        if previous is not None and now-previous>=self.stale_seconds+self.quick_resume_seconds:
            self.recovery_revision[venue]+=1
        self.last_message_at[venue]=now

    def book_validity(self,venue,symbol,book=None):
        now=self.clock();book=book or self.books.get((venue,symbol))
        invalid=self.invalid.get((venue,symbol))
        if invalid:return False,invalid[0],invalid[1]
        if not self.connected[venue]:return False,'reconnect',self.last_message_at.get(venue,now)
        if book is None or book.generation!=self.generations[venue]:return False,'generation_change',now
        last=self.last_message_at.get(venue)
        if last is None or now-last>self.stale_seconds:
            return False,'connection_timeout',(last+self.stale_seconds if last is not None else now)
        return True,None,None

    async def metadata(self):
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10),trust_env=False) as session:
            async def get(url):
                async with session.get(url,allow_redirects=False) as response:
                    if response.status!=200: raise ValueError('Public metadata unavailable')
                    return await response.json()
            arcus,lighter=await asyncio.gather(get(ARCUS+'/v1/markets'),get(LIGHTER+'/api/v1/orderBooks'))
        ar=arcus['markets']
        if isinstance(ar,dict): ar=list(ar.values())
        lr=lighter['order_books']
        self.load_metadata(ar,lr)
        return self.rules

    def load_metadata(self,arcus,lighter):
        for symbol in self.symbols:
            base=symbol.split('-')[0]
            a=next(r for r in arcus if r['marketDisplayName']==base+'-USD' and r['status']=='ONLINE')
            l=next(r for r in lighter if r['symbol']==base and r['market_type']=='perp' and r['status']=='active')
            precision=l['supported_size_decimals']
            if type(precision) is not int or not 0<=precision<=18:
                raise ValueError('Invalid Lighter size precision')
            tiers=tuple((D(str(t['upToPrice'])) if 'upToPrice' in t else None,D(str(t['tick']))) for t in a.get('tickTiers',[]))
            self.rules[symbol]=MarketRules(D(a['stepSize']),D(10)**-precision,D(a['minOrderSize']),
                D(l['min_base_amount']),D(a['minOrderNotional']),D(l['min_quote_amount']),D(a['maxOrderSize']),D(a['tickSize']),tiers)
            self.markets[symbol]=(dict(a),dict(l))
            self.market_ids[str(l['market_id'])]=symbol

    def healthy(self):
        return all(self.connected.values())

    async def start(self,callback):
        self.callback=callback
        self.tasks=[asyncio.create_task(self._stream(v)) for v in ('arcus','lighter')]

    def _publish(self,venue,symbol,bids,asks):
        bids=sorted(((D(str(p)),D(str(q))) for p,q in bids if D(str(q))>0),reverse=True)
        asks=sorted((D(str(p)),D(str(q))) for p,q in asks if D(str(q))>0)
        if not bids or not asks: raise ValueError('Empty public book')
        book=Book(bids[0][0],asks[0][0],bids[0][1],asks[0][1],self.clock(),self.generations[venue],tuple(bids),tuple(asks))
        self.received(venue,self.generations[venue])
        self.invalid.pop((venue,symbol),None)
        self.books[venue,symbol]=book
        self.callback(venue,symbol,book)

    async def _stream(self,venue):
        url='wss://api.arcus.xyz/v1/ws' if venue=='arcus' else 'wss://mainnet.zklighter.elliot.ai/stream?readonly=true'
        while not self.stopped:
            local={}
            try:
                generation=self.generations[venue]+1
                def connection(*args,_generation=generation,**kwargs):
                    return ReceivedConnection(*args,on_received=lambda:self.received(venue,_generation),**kwargs)
                async with websocket_connect(url,ping_interval=self.stale_seconds/2,ping_timeout=self.stale_seconds+self.quick_resume_seconds,
                                             open_timeout=10,proxy=None,create_connection=connection) as socket:
                    self.generations[venue]+=1
                    self.connected[venue]=True
                    self.last_message_at[venue]=self.clock()
                    for symbol in self.symbols:
                        channel={'type':'subscribe','channel':'l2Orderbook','id':symbol.split('-')[0]+'-USD','nLevels':20} if venue=='arcus' else {
                            'type':'subscribe','channel':'order_book/'+str(self.markets[symbol][1]['market_id'])}
                        await socket.send(json.dumps(channel))
                    async for raw in socket:
                        self.received(venue,generation)
                        msg=json.loads(raw)
                        if msg.get('type')=='ping':
                            await socket.send('{"type":"pong"}')
                            continue
                        if venue=='arcus':
                            if msg.get('type')=='degraded': raise ValueError('Arcus feed degraded')
                            if msg.get('channel')!='l2Orderbook' or msg.get('type') not in ('subscribed','channel_data'): continue
                            symbol=str(msg['id']).removesuffix('-USD')+'-USDC-PERP'
                            if symbol not in self.symbols: continue
                            data=msg['contents']; seq=int(data['lastSequenceId'])
                            if msg['type']=='subscribed': local[symbol]={'sequence':-1}
                            if symbol not in local: continue
                            if seq<=local[symbol]['sequence']: continue
                            local[symbol]['sequence']=seq
                            self._publish(venue,symbol,data['bids'],data['asks'])
                        else:
                            kind=msg.get('type')
                            if kind not in ('subscribed/order_book','update/order_book'): continue
                            market=str(msg.get('channel','')).replace(':','/').split('/')[-1]
                            symbol=self.market_ids.get(market)
                            if symbol is None: continue
                            data=msg['order_book']
                            if kind=='subscribed/order_book': local[symbol]={'bids':{},'asks':{},'nonce':-1}
                            if symbol not in local: continue
                            book=local[symbol]
                            nonce=int(data['nonce'])
                            if nonce<=book['nonce']: continue
                            if kind=='update/order_book' and (data.get('begin_nonce') is None or int(data['begin_nonce'])!=book['nonce']):
                                self.invalid[venue,symbol]=('sequence_gap',self.clock())
                                raise ValueError('Lighter orderbook sequence gap')
                            offset=int(data['offset']) if data.get('offset') is not None else None
                            if offset is not None and book.get('offset') is not None and offset<book['offset']:
                                self.invalid[venue,symbol]=('sequence_gap',self.clock())
                                raise ValueError('Lighter orderbook offset regressed')
                            for side in ('bids','asks'):
                                for row in data.get(side,[]):
                                    price,size=D(str(row['price'])),D(str(row['size']))
                                    if size<0: raise ValueError('Negative book size')
                                    if size: book[side][price]=size
                                    else: book[side].pop(price,None)
                            book['nonce']=nonce
                            if offset is not None:book['offset']=offset
                            self._publish(venue,symbol,book['bids'].items(),book['asks'].items())
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # Whitelisted diagnostics only: never log arbitrary response bodies/headers.
                status=getattr(getattr(exc,'response',None),'status_code',None)
                self.last_error[venue]={'error_type':type(exc).__name__,'http_status':status}
                last=self.last_message_at.get(venue)
                if last is not None and self.clock()-last>=self.stale_seconds:
                    for symbol in self.symbols:self.invalid.setdefault((venue,symbol),('connection_timeout',last+self.stale_seconds))
            finally:
                self.connected[venue]=False
                for key in list(self.books):
                    if key[0]==venue: self.books.pop(key,None)
            if not self.stopped: await asyncio.sleep(1)

    async def close(self):
        self.stopped=True
        for task in self.tasks: task.cancel()
        await asyncio.gather(*self.tasks,return_exceptions=True)


class PaperTransport:
    identity='paper-mainnet'

    def __init__(self,source,journal,*,hedge_latency_ms=300):
        self.source,self.journal=source,journal
        self.hedge_latency_ms=hedge_latency_ms
        self.orders={r['client']:r for r in journal.makers(active=True)}
        self.positions={s:list(journal.inventory(s)) for s in source.symbols}
        self.receive=lambda obs: None
        self.engine_book=lambda *args: None
        self.book_sequence=0
        self.placed_at={}
        self.hedge_liquidity={}

    def healthy(self): return self.source.healthy()

    def _obs(self,row,qty,status,venue='arcus',side=None,average=None):
        return OrderObservation(venue,self.identity,row.get('symbol',''),row['client'],'paper-'+row['client'],
            side or row.get('side',1),D(row['qty']),D(qty),status,'paper_model',time.monotonic(),
            average=average,fee=None)

    def on_book(self,venue,symbol,book):
        self.engine_book(venue,symbol,book)
        self.book_sequence+=1
        if venue!='arcus': return
        remaining={'buy':book.ask_size,'sell':book.bid_size}
        for client,row in list(self.orders.items()):
            if row['symbol']!=symbol or self.placed_at.get(client,self.book_sequence)>=self.book_sequence: continue
            side=row['side']
            # A later opposite BBO must trade STRICTLY THROUGH the resting quote, never initial touch.
            crossed=book.ask<D(row['price']) if side==1 else book.bid>D(row['price'])
            if not crossed: continue
            key='buy' if side==1 else 'sell'
            qty=min(D(row['qty'])-D(row['filled']),remaining[key])
            qty=floor_quantity(qty,self.source.rules[symbol].arcus_step)
            if qty<=0: continue
            remaining[key]-=qty
            row['filled']=str(D(row['filled'])+qty)
            row['status']='filled' if D(row['filled'])==D(row['qty']) else 'open'
            self.positions[symbol][0]+=side*qty
            self.receive(self._obs(row,row['filled'],row['status'],average=D(row['price'])))
            if row['status']=='filled': self.orders.pop(client,None)

    async def maker(self,row):
        if not getattr(self,'send_guard',lambda:True)():raise DeferredHedge('Manual intervention forbids new maker')
        self.orders[row['client']]=dict(row)
        self.placed_at[row['client']]=self.book_sequence
        return self._obs(row,0,'open')

    async def lookup_maker(self,row):
        current=self.orders.get(row['client'],self.journal.maker(row['client']))
        return self._obs(current,current['filled'],current['status'])

    async def cancel_maker(self,row):
        current=self.orders.pop(row['client'],None) or self.journal.maker(row['client'])
        return self._obs(current,current['filled'],'canceled')

    async def hedge(self,attempt,obligation):
        await asyncio.sleep(self.hedge_latency_ms/1000)
        if not getattr(self,'send_guard',lambda:True)():raise DeferredHedge('Manual intervention forbids new hedge')
        symbol=obligation['symbol']; book=self.source.books.get(('lighter',symbol))
        if not self.source.book_validity('lighter',symbol,book)[0]:raise DefinitiveFailure('Paper hedge book unavailable')
        side=-obligation['side']
        if hedge_quantity(D(attempt['qty']),self.source.rules[symbol],book.ask if side==1 else book.bid)!=D(attempt['qty']):
            raise DeferredHedge('Paper hedge below conservative policy')
        decimals=self.source.markets[symbol][1]['supported_price_decimals']
        limit=D(lighter_ioc_price(side,book.bid,book.ask,decimals))/(D(10)**decimals)
        cached=self.hedge_liquidity.get(symbol)
        if cached is None or cached[0] is not book:
            cached=(book,{'bids':[[p,q] for p,q in (book.bids or ((book.bid,book.bid_size),))],
                          'asks':[[p,q] for p,q in (book.asks or ((book.ask,book.ask_size),))]})
            self.hedge_liquidity[symbol]=cached
        levels=cached[1]['asks' if side==1 else 'bids']
        eligible=[level for level in levels if (level[0]<=limit if side==1 else level[0]>=limit)]
        qty=floor_quantity(min(D(attempt['qty']),sum((q for p,q in eligible),D(0))),self.source.rules[symbol].lighter_step)
        left=qty;cost=D(0)
        for level in eligible:
            part=min(left,level[1]);cost+=part*level[0];level[1]-=part;left-=part
            if not left:break
        self.positions[symbol][1]+=side*qty
        row={**attempt,'symbol':symbol}
        return self._obs(row,qty,'filled' if qty==D(attempt['qty']) else 'canceled','lighter',side,
                         cost/qty if qty else None)

    async def lookup_attempt(self,attempt,obligation):
        raise ValueError('Paper crash between simulation and commit cannot be replayed as exchange evidence')

    async def emergency(self,attempt,obligation):
        if not getattr(self,'send_guard',lambda:True)():raise DeferredHedge('Manual intervention forbids emergency order')
        symbol=obligation['symbol']; book=self.source.books.get(('arcus',symbol))
        if book is None: raise DefinitiveFailure('Paper emergency book unavailable')
        side=-obligation['side']; qty=D(attempt['qty'])
        self.positions[symbol][0]+=side*qty
        return self._obs({**attempt,'symbol':symbol},qty,'filled','arcus',side,book.ask if side==1 else book.bid)

    async def snapshot(self):
        return {'authoritative':True,'equity':{'arcus':D(5000),'lighter':D(5000)},
                'positions':self.positions,'modeled':True}

    async def cancel_legacy(self,known,targets=()): pass
