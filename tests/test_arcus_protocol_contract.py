"""Protocol regressions with offline transport; never loads dotenv or contacts exchanges."""
import asyncio
from decimal import Decimal as D
from unittest.mock import AsyncMock
import pytest
from test_arcus_adapter import adapter, row, frame, SYMBOL
from core.adapters.exchanges.models import OrderSide, OrderType, OrderStatus
from core.adapters.exchanges.order_state import OrderStateUnknownError


def run(coro): return asyncio.run(coro)


@pytest.mark.parametrize('cancelled',[False,True])
def test_concurrent_cross_symbol_writes_and_cancelled_submit_one_post(cancelled):
    async def scenario():
        a=adapter(); started=asyncio.Event(); release=asyncio.Event(); writes=[]
        a._do_authenticate=AsyncMock(return_value=True)
        async def request(method,path,**kwargs):
            writes.append((method,path));started.set();await release.wait()
            if cancelled: raise asyncio.CancelledError()
            raise OrderStateUnknownError('arcus')
        a._request=request
        first=asyncio.create_task(a.create_order(SYMBOL,OrderSide.BUY,OrderType.LIMIT,D('.001'),D('50000'),{'clientId':'First-Case'}))
        await started.wait()
        second=asyncio.create_task(a.create_order('ETH-USDC-PERP',OrderSide.SELL,OrderType.LIMIT,D('.001'),D('50000')))
        await asyncio.sleep(0);release.set()
        result=await asyncio.gather(first,second,return_exceptions=True)
        assert all(isinstance(x,OrderStateUnknownError) for x in result)
        assert writes==[('POST','/v1/placeOrder')] and 'First-Case' in a._uncertain
    run(scenario())


@pytest.mark.parametrize('method',['get_open_orders','get_order_history','get_positions','get_balances'])
@pytest.mark.parametrize('data',[{},None,{'orders':[{}]}, {'address':'0x'+'ff'*20,'orders':[]}])
def test_authoritative_malformed_reads_never_become_empty(method,data):
    a=adapter();a._request=AsyncMock(return_value=(200,data))
    with pytest.raises((OrderStateUnknownError,ValueError,TypeError)):
        run(getattr(a,method)())


def test_atomic_order_collection_scope_and_optional_zero_index():
    a=adapter();good=row();good.pop('address');good.pop('accountIndex')
    a._request=AsyncMock(return_value=(200,{'orders':[good],'total':1}))
    assert len(run(a.get_open_orders()))==1
    a._orders.clear()
    a._request.return_value=(200,{'orders':[good,row(order_id='bad',address='0x'+'ff'*20)],'total':2})
    with pytest.raises(OrderStateUnknownError):run(a.get_open_orders())
    assert not a._orders
    a._request.return_value=(200,{'orders':[],'total':1})
    with pytest.raises(OrderStateUnknownError):run(a.get_open_orders())


def test_bad_get_order_cannot_return_cached_open_but_terminal_can_reconcile():
    a=adapter();a._merge_order(row());a._request=AsyncMock(return_value=(200,{}))
    with pytest.raises(OrderStateUnknownError):run(a.get_order('O1',SYMBOL))
    a._merge_order(row(status='FILLED',filled='1',seq=2))
    assert run(a.get_order('O1',SYMBOL)).status is OrderStatus.FILLED


def test_old_sequence_never_rewrites_raw_quantity_or_average():
    a=adapter();base=a._merge_order(row(filled='.6',seq=20))
    old=a._merge_order(row(filled='.1',seq=19,originalSize='2',avgFillPrice='100'))
    assert old is base and old.amount==1 and old.filled==D('.6') and old.average==D('50000')
    assert old.raw_data['sequenceNumber']==20
    live=a._merge_order(row(status='CANCELED',filled='.8',seq=21))
    assert live.filled==D('.8') and live.cost==D('40000')


def test_ioc_partial_without_tif_and_modify_cancel_followup():
    a=adapter();o=a._merge_order(row(status='CANCELED',state='PARTIALLY_FILLED',filled='.6'))
    assert o.status is OrderStatus.CANCELED and o.filled==D('.6')
    a=adapter();a._merge_order(row());a._uncertain.add('O1')
    a._merge_order(row(status='CANCELED',cancelReason='MODIFY_CANCELED',seq=2))
    assert a._orders['O1'].status not in (OrderStatus.CANCELED,OrderStatus.FILLED) and 'O1' in a._uncertain
    a._merge_order(row(status='OPEN',seq=3))
    assert a._orders['O1'].status is OrderStatus.OPEN and 'O1' in a._uncertain


def test_uncached_server_cancel_signature_case_and_unknown_no_replay():
    a=adapter();payloads=[];posts=[];a._sign=lambda p:payloads.append(p) or 'offline'
    async def request(method,path,**kwargs):
        if method=='POST':posts.append(kwargs['body']);raise asyncio.CancelledError()
        return 200,row(order_id='Server-X')
    a._request=request
    for _ in range(2):
        with pytest.raises(OrderStateUnknownError):run(a.cancel_order('Server-X',SYMBOL))
    assert len(posts)==1 and posts[0]['kind']=='orderId' and posts[0]['orderId']=='Server-X'
    assert payloads[0]['id']=='Server-X' and 'c' not in payloads[0]


@pytest.mark.parametrize('gap',[False,True])
def test_late_fill_cumulative_baseline_delta_and_gap_requests_read(gap):
    a=adapter();a._merge_order(row(filled='.6',seq=10,avgFillPrice='100'))
    a._request=AsyncMock(side_effect=OrderStateUnknownError('arcus'))
    fill={'tradeId':'late','orderId':'O1','marketId':1,'side':'BUY','size':'.2' if not gap else '.1','price':'200',
          'originalSize':'1','remainingSize':'.2','sequenceNumber':11}
    async def scenario():
        await a._websocket.handle_message(frame('userFills',{'fills':[fill]}))
        await a._websocket.handle_message(frame('userFills',{'fills':[fill]},type='channel_data'))
    run(scenario());o=a._orders['O1']
    assert o.filled==D('.8') and o.remaining==D('.2')
    if gap:
        assert o.average is None and o.cost==D('60') and o.status is OrderStatus.PENDING and 'O1' in a._uncertain
        assert a._request.await_count==1
    else:
        assert o.average==125 and o.cost==100 and not a._uncertain


def test_streaming_new_fill_without_cumulative_average_never_fabricates_terminal_cost():
    a=adapter();a._merge_order(row(filled='.6',seq=10,avgFillPrice='100'))
    live=row(status='CANCELED',filled='.8',seq=11);live.pop('avgFillPrice')
    o=a._merge_order(live)
    assert o.filled==D('.8') and o.average is None and o.cost==60
    assert o.status is OrderStatus.PENDING and 'O1' in a._uncertain
    o=a._merge_order(row(status='CANCELED',filled='.8',seq=12,avgFillPrice='125'))
    assert o.cost==100 and o.status is OrderStatus.CANCELED and not a._uncertain


def test_concurrent_cancel_unknown_only_first_post_and_terminal_reconcile():
    async def scenario():
        a=adapter();a._merge_order(row());a._sign=lambda _: 'offline';started=asyncio.Event();release=asyncio.Event();posts=[]
        async def request(method,path,**kwargs):
            if method=='POST':
                posts.append(path);started.set();await release.wait();raise asyncio.CancelledError()
            return 200,row()
        a._request=request
        first=asyncio.create_task(a.cancel_order('O1',SYMBOL));await started.wait()
        second=asyncio.create_task(a.cancel_order('O1',SYMBOL));await asyncio.sleep(0);release.set()
        results=await asyncio.gather(first,second,return_exceptions=True)
        assert all(isinstance(x,OrderStateUnknownError) for x in results) and len(posts)==1
        a._request=AsyncMock(return_value=(200,row(status='CANCELED',seq=2)))
        assert (await a.get_order('O1',SYMBOL)).status is OrderStatus.CANCELED
        assert not a._uncertain
    run(scenario())


def test_waiting_writer_cancellation_has_no_new_uncertain_identity():
    async def scenario():
        a=adapter();await a._write_lock.acquire()
        task=asyncio.create_task(a.create_order(SYMBOL,OrderSide.BUY,OrderType.LIMIT,D('.001'),D('50000')))
        await asyncio.sleep(0);task.cancel()
        with pytest.raises(asyncio.CancelledError):await task
        assert not a._uncertain
        a._write_lock.release()
    run(scenario())


def test_sequence_optional_newer_rest_preserves_ws_sequence_and_ignores_late_old_ws():
    a=adapter();a._merge_order(row(filled='.6',seq=10,updatedAt=100,avgFillPrice='100'))
    latest=row(status='CANCELED',filled='.8',updatedAt=101,avgFillPrice='125');latest.pop('sequenceNumber')
    a._request=AsyncMock(return_value=(200,latest))
    o=run(a.get_order('O1',SYMBOL))
    assert o.status is OrderStatus.CANCELED and o.filled==D('.8') and o.cost==100
    assert o.raw_data['sequenceNumber']==10 and o.raw_data['updatedAt']==101
    a._merge_order(row(filled='.6',seq=11,updatedAt=100,avgFillPrice='100'))
    assert a._orders['O1'] is o and o.raw_data['sequenceNumber']==10


def test_valuation_unknown_persists_through_equal_quantity_terminal_until_explicit_average():
    a=adapter();a._merge_order(row(filled='.6',seq=10,avgFillPrice='100'))
    first=row(status='CANCELED',filled='.8',seq=11);first.pop('avgFillPrice')
    a._merge_order(first)
    second={**first,'sequenceNumber':12};o=a._merge_order(second)
    assert o.filled==D('.8') and o.average is None and o.cost==60
    assert o.status is OrderStatus.PENDING and 'O1' in a._uncertain
    assert 'avgFillPrice' not in o.raw_data and o.raw_data['_valuation_unknown']
    a._merge_order(row(status='CANCELED',filled='.8',seq=13,avgFillPrice='125'))
    assert a._orders['O1'].cost==100 and not a._uncertain


def test_rest_missing_order_index_means_zero_but_ws_scoped_parent_can_supply_index_three():
    a=adapter(index=3);record=row(index=3);record.pop('accountIndex')
    a._request=AsyncMock(return_value=(200,{'orders':[record]}))
    with pytest.raises(OrderStateUnknownError):run(a.get_open_orders())
    assert not a._orders
    run(a._websocket.handle_message(frame('orders',{'orders':[record]},index=3)))
    assert a._orders['O1'].raw_data['accountIndex']==3


@pytest.mark.parametrize('missing',['address','accountIndex'])
def test_account_required_scope_missing_is_not_fabricated(missing):
    a=adapter();data={'address':a.address,'accountIndex':0,'equity':'30','freeCollateral':'30'};data.pop(missing)
    a._request=AsyncMock(return_value=(200,data))
    with pytest.raises(OrderStateUnknownError):run(a.get_balances())


def test_account_other_unknown_blocks_cancel_without_sign_or_post():
    a=adapter();a._merge_order(row());a._uncertain.add('Other');a._sign=AsyncMock();a._request=AsyncMock()
    with pytest.raises(OrderStateUnknownError):run(a.cancel_order('O1',SYMBOL))
    a._sign.assert_not_called();a._request.assert_not_awaited()


@pytest.mark.parametrize('preexisting',[False,True])
def test_modify_followup_only_clears_own_transition_reason_and_allows_one_normal_cancel(preexisting):
    a=adapter();a._merge_order(row())
    if preexisting:a._uncertain.add('O1')
    a._merge_order(row(status='CANCELED',cancelReason='MODIFY_CANCELED',seq=2))
    a._merge_order(row(status='OPEN',seq=3));writes=[];a._sign=lambda _: 'offline'
    async def request(method,path,**kwargs):
        if method=='POST':writes.append(path);return 200,{'status':'ACK'}
        return 200,row(status='CANCELED',seq=4) if not preexisting else row(status='OPEN',seq=3)
    a._request=request
    if preexisting:
        with pytest.raises(OrderStateUnknownError):run(a.cancel_order('O1',SYMBOL))
        assert not writes and 'O1' in a._uncertain
    else:
        assert not a._uncertain
        assert run(a.cancel_order('O1',SYMBOL)).status is OrderStatus.CANCELED
        assert writes==['/v1/cancelOrder']


@pytest.mark.parametrize('method',['get_open_orders','get_order_history'])
def test_full_default_page_rejected_as_incomplete_even_when_total_equals_page_length(method):
    a=adapter();a._request=AsyncMock(return_value=(200,{'orders':[row(order_id=str(i)) for i in range(1000)],'total':1000}))
    with pytest.raises(OrderStateUnknownError):run(getattr(a,method)())
    assert not a._orders


def position(a):
    return {'address':a.address,'accountIndex':0,'marketId':1,'marketDisplayName':'BTC-USD','side':'LONG','size':'.001',
            'averageEntryPrice':'50000','leverage':1,'marginMode':'CROSS','borrowedCapital':'0','marginUsed':'50',
            'positionValueNotional':'50','unrealizedPnl':'0','markPx':'50000'}


@pytest.mark.parametrize('missing',['address','accountIndex','averageEntryPrice','marginUsed','unrealizedPnl','markPx'])
def test_position_required_fields_missing_cannot_become_zero_financials(missing):
    a=adapter();p=position(a);p.pop(missing);a._request=AsyncMock(return_value=(200,{'positions':{'1':p}}))
    with pytest.raises(OrderStateUnknownError):run(a.get_positions())


def test_valid_required_position_financials_and_valid_empty_collection():
    a=adapter();a._request=AsyncMock(return_value=(200,{'positions':{'1':position(a)}}))
    p=run(a.get_positions())[0];assert p.size==D('.001') and p.entry_price==50000 and p.margin==50
    a._request.return_value=(200,{'positions':{}})
    a._invalidate_account_reads()  # Observe an account update instead of the still-fresh cached snapshot.
    assert run(a.get_positions())==[]


@pytest.mark.parametrize('via_get',[False,True])
@pytest.mark.parametrize('repair_filled',['.8','1'])
def test_gap_lower_quantity_average_never_repairs_larger_confirmed_quantity(via_get,repair_filled):
    a=adapter();a._merge_order(row(filled='.6',seq=10,avgFillPrice='100'))
    gap=row(status='CANCELED',filled='.8',seq=11);gap.pop('avgFillPrice');a._merge_order(gap)
    lower=row(status='CANCELED',filled='.6',seq=12,avgFillPrice='100')
    if via_get:
        a._request=AsyncMock(return_value=(200,lower))
        with pytest.raises(OrderStateUnknownError):run(a.get_order('O1',SYMBOL))
    else:a._merge_order(lower)
    assert a._orders['O1'].filled==D('.8') and a._orders['O1'].average is None
    assert a._orders['O1'].cost==60 and a._orders['O1'].status is OrderStatus.PENDING
    assert a._orders['O1'].raw_data['_valuation_unknown'] and 'O1' in a._uncertain
    equal=row(status='CANCELED',filled='.8',seq=13);equal.pop('avgFillPrice');o=a._merge_order(equal)
    assert o.average is None and o.cost==60 and o.status is OrderStatus.PENDING and 'O1' in a._uncertain
    assert 'avgFillPrice' not in o.raw_data
    o=a._merge_order(row(status='CANCELED',filled=repair_filled,seq=14,avgFillPrice='125'))
    assert o.filled==D(repair_filled) and o.average==125 and o.cost==D(repair_filled)*125
    assert o.status is OrderStatus.CANCELED and not a._uncertain


def test_complete_valuation_lower_quantity_update_preserves_previous_average_and_cost():
    a=adapter();a._merge_order(row(filled='.8',seq=10,avgFillPrice='125'))
    o=a._merge_order(row(status='CANCELED',filled='.6',seq=11,avgFillPrice='100'))
    assert o.filled==D('.8') and o.average==125 and o.cost==100
    assert o.raw_data['avgFillPrice']=='125' and not o.raw_data['_valuation_unknown']
