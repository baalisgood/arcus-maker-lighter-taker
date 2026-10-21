import asyncio
import json
import time
from decimal import Decimal as D
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock

import pytest

from test_maker_taker_engine import engine,maker,observation,SYMBOL
from core.services.arbitrage_monitor_v2.history.maker_taker_finance import FinanceCollector,SemanticUnavailable
from core.services.arbitrage_monitor_v2.history.maker_taker_statistics import statistics
from core.services.arbitrage_monitor_v2.execution.maker_taker_live import LiveTransport


def test_arcus_actual_fees_funding_and_duplicates(tmp_path):
    async def scenario():
        e=engine(tmp_path);client=maker(e,qty='.2')
        await e.observe_maker(observation(e.journal.maker(client),'.2','filled',average=D(100)))
        await e.drain_symbol(SYMBOL)
        class Transport:
            async def arcus_history(self,kind,start,end):
                if kind=='fills':
                    yield {'orderId':'EX-'+client,'tradeId':'trade1','size':'.2','price':'100','fee':'0','role':'MAKER',
                           'createdAt':int(time.time()*1e6)}
                else:
                    yield {'marketId':3,'marketDisplayName':'SOL-USD','time':int(time.time())*1_000_000,'payment':'.03'}
            def arcus_funding_row(self,row):return SYMBOL,-D(row['payment']),str(row['time']),row['time']/1e6
        collector=FinanceCollector(e.journal,Transport())
        for _ in range(2):
            await collector.arcus_fills(time.time()-10,time.time())
            await collector.arcus_funding(time.time()-10,time.time())
        assert e.journal.db.execute('SELECT COUNT(*) FROM executions').fetchone()[0]==1
        assert e.journal.db.execute('SELECT COUNT(*) FROM funding').fetchone()[0]==1
        assert D(e.journal.db.execute('SELECT cost FROM funding').fetchone()[0])==D('-.03')
        with pytest.raises(ValueError):e.journal.record_funding('arcus',str(int(time.time())*1_000_000),SYMBOL,'999','USDC',int(time.time()))
    asyncio.run(scenario())


def test_lighter_funding_fetches_cursor_raw_without_guessing_time_or_money(tmp_path):
    async def scenario():
        e=engine(tmp_path)
        row=NS(timestamp=1700000000123,market_id=2,funding_id=9,change='-0.05',discount='.01',rate='.0001',position_size='1',position_side='long')
        api=NS(position_funding=AsyncMock(side_effect=[NS(code=200,position_fundings=[row],next_cursor='next'),
                                                      NS(code=200,position_fundings=[],next_cursor=None)]))
        rest=NS(account_index=0,account_api=api,_get_auth_token=lambda:'offline-token')
        transport=NS(lighter=NS(_rest=rest),source=NS(market_ids={'2':SYMBOL}))
        collector=FinanceCollector(e.journal,transport)
        with pytest.raises(SemanticUnavailable,match='change_sign_unit_unverified'):
            await collector.lighter_funding(100,200)
        assert api.position_funding.await_count==2
        assert all('start_timestamp' not in call.kwargs and call.kwargs['account_index']==0 for call in api.position_funding.await_args_list)
        raw=e.journal.db.execute('SELECT payload FROM raw_finance').fetchone()[0]
        assert json.loads(raw)['timestamp']==1700000000123
        assert e.journal.db.execute('SELECT COUNT(*) FROM funding').fetchone()[0]==0
        assert e.journal.db.execute("SELECT complete FROM coverage WHERE kind='funding_transport'").fetchone()[0]==1
    asyncio.run(scenario())


def test_lighter_actual_trade_volume_but_unknown_integer_fee_preserved(tmp_path):
    async def scenario():
        e=engine(tmp_path);client=maker(e,qty='.2')
        await e.observe_maker(observation(e.journal.maker(client),'.2','filled'))
        await e.drain_symbol(SYMBOL)
        attempt=e.journal.attempts()[0]
        row=NS(trade_id=7,market_id=2,size='.2',price='100',usd_amount='20',ask_id=int(1),bid_id=2,
               ask_account_id=0,bid_account_id=99,ask_client_id=int(attempt['client']),bid_client_id=123,
               is_maker_ask=False,timestamp=int(time.time()*1000),taker_fee=25,maker_fee=0,
               integrator_maker_fee=0,integrator_taker_fee=0)
        e.journal.db.execute('UPDATE attempts SET exchange_id=\'1\'')
        rest=NS(account_index=0,_get_auth_token=lambda:'offline-token',order_api=NS(
            trades=AsyncMock(return_value=NS(code=200,trades=[row],next_cursor=None))))
        collector=FinanceCollector(e.journal,NS(lighter=NS(_rest=rest)))
        with pytest.raises(SemanticUnavailable,match='fee_semantics_unverified'):
            await collector.lighter_fills(time.time()-10,time.time())
        actual=e.journal.db.execute('SELECT * FROM executions').fetchone()
        assert actual['fee'] is None and D(actual['qty'])*D(actual['price'])==20
        raw=json.loads(e.journal.db.execute('SELECT payload FROM raw_finance').fetchone()[0])
        assert raw['taker_fee']==25 and raw['ask_account_id']==0
    asyncio.run(scenario())


def test_arcus_page_overlap_and_same_timestamp_saturation(tmp_path):
    async def scenario():
        bridge=object.__new__(LiveTransport)
        bridge.source=NS(symbols=[SYMBOL])
        rows=[{'marketDisplayName':'SOL-USD','marketId':3,'createdAt':2000000-i,'tradeId':i,'orderId':'O'} for i in range(1000)]
        api=NS(address='0xabc',account_index=0,native_symbol=lambda s:'SOL-USD',standard_symbol=lambda s:SYMBOL,
               _account_query=lambda:{'address':'0xabc','accountIndex':0},
               _request=AsyncMock(side_effect=[(200,{'fills':rows}),(200,{'fills':[rows[-1]]})]))
        bridge.arcus=api
        result=[r async for r in bridge.arcus_history('fills',1,3)]
        assert len(result)==1000
        calls=api._request.await_args_list
        assert calls[1].kwargs['query']['to']==rows[-1]['createdAt']
        api._request=AsyncMock(return_value=(200,{'fills':[dict(rows[0],createdAt=3000000,tradeId=i) for i in range(1000)]}))
        with pytest.raises(ValueError,match='same-microsecond'):
            result=[r async for r in bridge.arcus_history('fills',1,3)]
    asyncio.run(scenario())


def test_actual_money_requires_both_funding_coverage_and_unknown_lighter_is_not_zero(tmp_path):
    e=engine(tmp_path)
    e.journal.event(SYMBOL,'maker_fill',qty='1',average='100',fee=None,modeled=False)
    e.journal.record_execution('arcus','fill:order',SYMBOL,'C',1,100,0,'USDC','MAKER',time.time(),'maker_fill')
    e.journal.coverage('arcus','funding',time.time()-10,time.time()+10,True)
    e.journal.coverage('lighter','funding',time.time()-10,time.time()+10,False,'change_sign_unit_unverified')
    report=next(r for r in statistics(e.journal,'day') if r['symbol']==SYMBOL)
    assert report['arcus_maker_notional']=='100' and report['actual_fee_known']=='0'
    assert report['maker_role_coverage']=='1/1'
    assert report['actual_funding'] is None and report['total_cost'] is None
    assert report['finance_coverage']['lighter:funding']['reason']=='change_sign_unit_unverified'
