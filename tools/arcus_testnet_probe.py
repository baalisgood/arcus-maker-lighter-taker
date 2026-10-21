#!/usr/bin/env python3
"""Arcus public probe by default; --trade is an explicit testnet-only opt-in."""
import argparse
import asyncio
import json
import os
import sys
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))

from core.adapters.exchanges.adapters.arcus import ArcusAdapter
from core.adapters.exchanges.interface import ExchangeConfig
from core.adapters.exchanges.models import ExchangeType, OrderSide, OrderType


def parse_args():
    parser=argparse.ArgumentParser(description='Arcus公开行情探针（默认测试网，只读）')
    parser.add_argument('--network',choices=['testnet','mainnet'],default='testnet')
    parser.add_argument('--symbol',default='BTC-USDC-PERP')
    parser.add_argument('--trade',action='store_true',help='明确授权测试网模拟余额最小限价下单并撤单；绝不使用主网凭证')
    parser.add_argument('--output',type=Path,help='可选JSON证据输出文件，不包含凭证')
    args=parser.parse_args()
    if args.trade and args.network!='testnet': parser.error('--trade仅允许testnet')
    return args


async def probe(args):
    if args.trade and args.network != 'testnet':
        raise ValueError('--trade仅允许testnet')
    extras={}; key=''; address=''
    if args.trade:
        from dotenv import load_dotenv
        load_dotenv(Path(__file__).resolve().parents[1] / '.env', override=False)
        key=os.environ.get('ARCUS_TESTNET_API_KEY','')
        extras['api_signing_key']=os.environ.get('ARCUS_TESTNET_API_SIGNING_KEY','')
        extras['account_index']=int(os.environ.get('ARCUS_TESTNET_ACCOUNT_INDEX','0'))
        address=os.environ.get('ARCUS_TESTNET_WALLET_ADDRESS','')
        if not all((key,extras['api_signing_key'],address)):
            raise ValueError('测试网交易探针需要ARCUS_TESTNET_前缀的独立凭证；不读取主网ARCUS_*')
    config=ExchangeConfig('arcus','Arcus',ExchangeType.PERPETUAL,key,'',wallet_address=address,testnet=args.network=='testnet',extra_params=extras)
    a=ArcusAdapter(config); result={'network':args.network,'symbol':args.symbol,'trade_enabled':args.trade}
    try:
        if not await a.connect(): raise RuntimeError('Arcus public connection failed')
        book=await a.get_orderbook(args.symbol,limit=5)
        ticker=await a.get_ticker(args.symbol)
        result.update(markets=len(a._markets),bid_levels=len(book.bids),ask_levels=len(book.asks),funding_rate_8h=str(ticker.funding_rate))
        event=asyncio.Event()
        async def on_book(native,update):
            result['ws']={'native_symbol':native,'standard_symbol':update.symbol,'bids':len(update.bids),'asks':len(update.asks)}
            event.set()
        await a.subscribe_orderbook(args.symbol,on_book)
        await asyncio.wait_for(event.wait(),timeout=15)
        if args.trade:
            balances=await a.get_balances()
            if not balances or balances[0].free<=0: raise ValueError('测试网无模拟余额；不会注册钱包、充值或下单')
            await a._websocket.subscribe_user_data(lambda *args:None)
            m=a._market(args.symbol)
            if not book.best_bid: raise ValueError('测试网盘口为空')
            price=book.best_bid.price*Decimal('.99'); tick=a._price_tick(m,price)
            price=(price/tick).to_integral_value(rounding=ROUND_DOWN)*tick
            step=Decimal(m['stepSize']); minimum=max(Decimal(m['minOrderSize']),Decimal(m['minOrderNotional'])/price)
            amount=(minimum/step).to_integral_value(rounding=ROUND_UP)*step
            if amount*price>balances[0].free: raise ValueError('测试网余额不足以运行最小订单探针')
            order=await a.create_order(args.symbol,OrderSide.BUY,OrderType.LIMIT,amount,price)
            closed=await a.cancel_order(order.id,args.symbol)
            result['testnet_order']={'status':closed.status.value,'filled':str(closed.filled),'requested_size':str(amount)}
        return result
    finally:
        await a.disconnect()


def main():
    args=parse_args(); result=asyncio.run(probe(args)); payload=json.dumps(result,indent=2)
    if args.output: args.output.write_text(payload+'\n')
    print(payload)

if __name__=='__main__': main()
