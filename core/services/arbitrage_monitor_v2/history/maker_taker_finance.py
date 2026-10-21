"""Account-scoped execution/funding backfill. Inputs never authorize orders."""
import asyncio
import sqlite3
import time
from decimal import Decimal

D=Decimal


class SemanticUnavailable(ValueError):
    def __init__(self,reason):
        self.reason=reason
        super().__init__(reason)


class FinanceCollector:
    def __init__(self,journal,transport):
        self.journal,self.transport=journal,transport
        self.last=0

    async def own_orders(self,venue):
        result={}
        # A separate WAL reader stays coherent while maintenance moves rows between storage sides.
        db=sqlite3.connect(self.journal.path.resolve().as_uri()+'?mode=ro',uri=True,isolation_level=None)
        db.row_factory=sqlite3.Row
        try:
            db.execute('BEGIN')
            for prefix in ('','archive_'):
                if venue=='arcus':
                    cursor=db.execute('SELECT * FROM '+prefix+'orders')
                    while rows:=cursor.fetchmany(128):
                        for row in rows:
                            if row['purpose']!='legacy':result[row['client']]=(row['symbol'],'maker_fill',dict(row))
                        await asyncio.sleep(0)
                cursor=db.execute('SELECT t.*,m.symbol FROM '+prefix+'attempts t '
                    'JOIN '+prefix+'obligations o ON o.id=t.obligation JOIN '+prefix+'orders m ON m.client=o.maker WHERE t.venue=?',(venue,))
                while rows:=cursor.fetchmany(128):
                    for row in rows:
                        result[row['client']]=(row['symbol'],'hedge_fill' if venue=='lighter' else 'emergency_fill',dict(row))
                    await asyncio.sleep(0)
            return result
        finally:db.close()

    async def collect(self):
        end=float(int(time.time())) # Funding APIs use closed whole-second windows; do not overstate coverage.
        first=self.journal.db.execute('SELECT MIN(created) FROM (SELECT MIN(created) AS created FROM orders UNION ALL SELECT MIN(created) FROM archive_orders)').fetchone()[0]
        start=float(int(first if first is not None else end))
        for venue in ('arcus','lighter'):
            for kind in ('fills','funding'):
                try:
                    if kind=='fills':await getattr(self,venue+'_fills')(start,end)
                    else:await getattr(self,venue+'_funding')(start,end)
                    self.journal.coverage(venue,kind,start,end,True)
                except Exception as exc:
                    self.journal.coverage(venue,kind,start,end,False,exc.reason if isinstance(exc,SemanticUnavailable) else type(exc).__name__)
                    self.journal.event('*','finance_unavailable',venue=venue,finance_kind=kind,error_type=type(exc).__name__)
        self.last=end

    async def arcus_fills(self,start,end):
        own=await self.own_orders('arcus')
        orders={}
        for index,(client,(_,_,row)) in enumerate(own.items()):
            if row['exchange_id']:orders[str(row['exchange_id'])]=client
            if index%128==0:await asyncio.sleep(0)
        async for row in self.transport.arcus_history('fills',start,end):
            client=orders.get(str(row['orderId']))
            if client is None:continue
            symbol,kind,_=own[client]
            if row.get('role') not in ('MAKER','TAKER'):raise ValueError('Missing Arcus actual liquidity role')
            if kind=='maker_fill' and row['role']!='MAKER':
                raise ValueError('PostOnly order unexpectedly reported as taker')
            self.journal.record_execution('arcus',str(row['tradeId'])+':'+str(row['orderId']),symbol,client,row['size'],row['price'],
                row.get('fee'),'USDC',row['role'],int(row['createdAt'])/1_000_000,kind)

    async def lighter_fills(self,start,end):
        rest=self.transport.lighter._rest
        own=await self.own_orders('lighter')
        for index,(client,(symbol,kind,attempt)) in enumerate(own.items()):
            if index%128==0:await asyncio.sleep(0)
            if not attempt['exchange_id']:continue
            already=self.journal.db.execute("SELECT qty FROM executions WHERE venue='lighter' AND client=?",(client,)).fetchall()
            if sum((D(r['qty']) for r in already),D(0))==D(attempt['filled']) and attempt['status'] in ('filled','canceled','expired','rejected'):
                continue
            cursor=None;seen=set()
            market=None
            try:market=int(self.transport.source.markets[symbol][1]['market_id'])
            except Exception:pass
            variant=0
            for _ in range(100):
                query={'sort_by':'timestamp','sort_dir':'asc','limit':100,'authorization':rest._get_auth_token(),
                       'account_index':rest.account_index,'order_index':int(attempt['exchange_id'])}
                if variant and market is not None:query['market_id']=market
                if cursor:query['cursor']=cursor
                try:response=await rest.order_api.trades(**query)
                except Exception as exc:
                    # Some deployments reject order_index without market_id (HTTP 400). Retry once with market scope.
                    if variant==0 and market is not None and cursor is None:
                        variant=1;continue
                    self.journal.event(symbol,'lighter_trades_request_failed',client=client,variant=variant,
                        error_type=type(exc).__name__,status=getattr(exc,'status',None),body=str(getattr(exc,'body',exc))[:300])
                    raise
                if getattr(response,'code',None)!=200 or not isinstance(getattr(response,'trades',None),list):
                    raise ValueError('Lighter fills unavailable')
                for row in response.trades:
                    is_ask=row.ask_account_id==rest.account_index
                    is_bid=row.bid_account_id==rest.account_index
                    if is_ask==is_bid:raise ValueError('Lighter trade account mismatch/self trade')
                    observed_client=str(row.ask_client_id if is_ask else row.bid_client_id)
                    order=str(row.ask_id if is_ask else row.bid_id)
                    if observed_client!=client or order!=str(attempt['exchange_id']):raise ValueError('Lighter fill intent mismatch')
                    raw_fields={name:getattr(row,name,None) for name in ('trade_id','market_id','ask_id','bid_id','ask_account_id',
                        'bid_account_id','ask_client_id','bid_client_id','is_maker_ask','size','price','usd_amount','timestamp',
                        'maker_fee','taker_fee','integrator_maker_fee','integrator_taker_fee')}
                    self.journal.raw_finance('lighter','fill',str(row.trade_id)+':'+order,raw_fields)
                    # API integer fee -> monetary semantics are undocumented; raw is retained, never guessed.
                    fee=None
                    self.journal.record_execution('lighter',str(row.trade_id),symbol,client,row.size,row.price,
                        fee,'USDC','MAKER' if row.is_maker_ask==is_ask else 'TAKER',row.timestamp/1000,kind)
                cursor=getattr(response,'next_cursor',None)
                if not cursor:break
                if cursor in seen:raise ValueError('Lighter trade pagination repeated cursor')
                seen.add(cursor)
            else:raise ValueError('Lighter trade history truncated')
        self.journal.coverage('lighter','fills_transport',start,end,True)
        raise SemanticUnavailable('fee_semantics_unverified;integrator_fee_semantics_unverified')

    async def lighter_funding(self,start,end):
        rest=self.transport.lighter._rest
        cursor=None;seen=set();foreign=set()
        for _ in range(100):
            response=await rest.account_api.position_funding(account_index=rest.account_index,limit=100,
                authorization=rest._get_auth_token(),cursor=cursor)
            if getattr(response,'code',None)!=200 or not isinstance(getattr(response,'position_fundings',None),list):
                raise ValueError('Lighter funding history unavailable')
            for row in response.position_fundings:
                symbol=self.transport.source.market_ids.get(str(row.market_id))
                if symbol is None:
                    # Shared accounts (e.g. another runner on other markets) are reported, never attributed here.
                    foreign.add(str(row.market_id));continue
                fields={name:getattr(row,name,None) for name in ('timestamp','market_id','funding_id','change','discount','rate',
                                                                               'position_size','position_side')}
                fields['account_index']=rest.account_index
                self.journal.raw_finance('lighter','funding',str(row.market_id)+':'+str(row.funding_id),fields)
            cursor=getattr(response,'next_cursor',None)
            if not cursor:break
            if cursor in seen:raise ValueError('Lighter funding pagination repeated cursor')
            seen.add(cursor)
        else:raise ValueError('Lighter funding history truncated')
        if foreign and foreign!=getattr(self,'_foreign_funding_reported',set()):
            self._foreign_funding_reported=set(foreign)
            self.journal.event('*','lighter_foreign_funding_markets',market_ids=sorted(foreign))
        self.journal.coverage('lighter','funding_transport',start,end,True)
        raise SemanticUnavailable('change_sign_unit_unverified;discount_semantics_unverified;timestamp_unit_unverified')

    async def arcus_funding(self,start,end):
        async for row in self.transport.arcus_history('funding',start,end):
            symbol,cost,identity,at=self.transport.arcus_funding_row(row)
            self.journal.record_funding('arcus',identity,symbol,cost,'USDC',at)
