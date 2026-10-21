import asyncio
import json
import os
from pathlib import Path
import time
from dataclasses import replace
from decimal import Decimal as D
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock,Mock

import pytest
import yaml

from test_maker_taker_engine import engine,maker,observation,SYMBOL,profile
from core.services.arbitrage_monitor_v2.config.maker_taker_config import load_profile
from core.services.arbitrage_monitor_v2.execution.maker_taker_quotes import quote_price,common_step,should_requote
from core.services.arbitrage_monitor_v2.execution.maker_taker_engine import Book
from core.services.arbitrage_monitor_v2.execution.maker_taker_paper import PublicMarkets,PaperTransport
from core.services.arbitrage_monitor_v2.history.maker_taker_statistics import statistics
from core.adapters.exchanges.adapters.lighter_rest import LighterRest

PROFILE=Path('config/arbitrage/arbitrage_segmented.yaml')
FIXTURE=Path('tests/fixtures/maker_taker_replay.json')


def test_config_disabled_modes_percent_and_invalid_values(tmp_path):
    p=load_profile(PROFILE)
    assert set(p.symbols)=={s+'-USDC-PERP' for s in ('BTC','ETH','SOL','HYPE')}
    with pytest.raises(KeyError): p.get_config('BNB-USDC-PERP')
    from core.services.arbitrage_monitor_v2.config.symbol_config import SegmentedConfigManager
    with pytest.raises(KeyError): SegmentedConfigManager(PROFILE).get_config('BNB-USDC-PERP')
    for section,key,value in [('maker','post_only',False),('risk','leverage',3),('hedge','max_retries',4),
                              ('risk','allocation_per_venue','.nan'),('maker','max_order_age',-1),
                              ('maker','exchange','lighter')]:
        data=yaml.safe_load(PROFILE.read_text());data[section][key]=value
        path=tmp_path/'bad.yaml';path.write_text(yaml.safe_dump(data))
        with pytest.raises((ValueError,ArithmeticError)): load_profile(path)


def test_quote_rounding_percent_tier_crossing_requote_equality(tmp_path):
    e=engine(tmp_path);rule=e.rules[SYMBOL];cfg=e.profile.maker
    assert common_step(D('.00000001'),D('.00001'))==D('.00001')
    assert quote_price(1,D(100),D('.02'),rule,D(99),D(101))==D('99.98')
    assert quote_price(-1,D(100),D('.02'),rule,D(99),D(101))==D('100.02')
    assert quote_price(1,D(100),D('-1'),rule,D(99),D(101))==D('100.99')
    assert not should_requote(D(100),D('100.005'),29,cfg)
    assert should_requote(D(100),D('100.00501'),29,cfg)
    assert should_requote(D(100),D(100),30,cfg)


def test_shared_capital_reservation_and_segment_cap(tmp_path):
    async def scenario():
        e=engine(tmp_path)
        e.equity={'arcus':D(60),'lighter':D(60)}
        clients=await asyncio.gather(e.place(SYMBOL,1,'open',D(1),D('.03'),0),
                                     e.place(SYMBOL,-1,'open',D(1),D('.03'),0))
        assert sum(x is not None for x in clients)==1
        assert len(e.journal.makers(active=True))==1
        e.equity={'arcus':D(5000),'lighter':D(5000)}
        assert not e._admit(SYMBOL,1,'open',D(10),D(100))
    asyncio.run(scenario())


def test_close_reservation_does_not_free_pending_capital(tmp_path):
    async def scenario():
        e=engine(tmp_path);client=maker(e)
        original=e.transport.hedge
        async def hedge(*args):
            return replace(await original(*args),average=D(100))
        e.transport.hedge=hedge
        await e.observe_maker(observation(e.journal.maker(client),1,'filled',average=D(100)))
        await e.drain_symbol(SYMBOL)
        assert e._admit(SYMBOL,-1,'close',D(1),D(100))
        assert not e._admit(SYMBOL,-1,'close',D('1.001'),D(100))
        close=await e.place(SYMBOL,-1,'close',D(1),D('-.01'),0)
        assert close is not None
        assert not e._admit(SYMBOL,-1,'close',D('.01'),D(100))
    asyncio.run(scenario())


def test_stale_generation_disconnect_and_cancel_reject(tmp_path):
    async def scenario():
        e=engine(tmp_path);client=maker(e)
        e.book('arcus',SYMBOL,Book(D(99),D(101),D(10),D(10),time.monotonic()-3,2))
        e.book('arcus',SYMBOL,Book(D(99),D(101),D(10),D(10),time.monotonic(),1))
        assert not e.data_ready()
        e.transport.cancel_maker=AsyncMock(side_effect=TimeoutError())
        await e.watchdog_once()
        assert e.cancel_failed and e.halted and e.journal.makers(active=True)
        assert await e.place(SYMBOL,1,'open',D(1),D('.03'),0) is None
    asyncio.run(scenario())


def test_reconcile_actual_difference_corrects_once_and_no_read_zero_fallback(tmp_path):
    async def scenario():
        e=engine(tmp_path)
        e.transport.positions={SYMBOL:[D('.2'),D(0)]}
        assert await e.reconcile()
        assert len(e.transport.sends)==1
        e.transport.positions={SYMBOL:[D('.2'),D('-.2')]}
        assert await e.reconcile()
        assert len(e.transport.sends)==1
        e.transport.snapshot=AsyncMock(side_effect=TimeoutError())
        assert not await e.reconcile() and e.halted
    asyncio.run(scenario())


@pytest.mark.parametrize('positions,total,expected',[
    ([NS(position='1',unrealized_pnl='-20')],None,'80'),
    ([NS(position='1',unrealized_pnl='-120')],None,'-20'),
    ([],None,'100'),([], '0','0'),
    ([NS(position='1',unrealized_pnl='-20')],'70','70')])
def test_authoritative_equity_losses_zero_tav_and_fallback(positions,total,expected):
    assert LighterRest._authoritative_equity(NS(positions=positions,total_asset_value=total,collateral='100'))==D(expected)


@pytest.mark.parametrize('positions', [None,[NS(position='NaN',unrealized_pnl='1')],
    [NS(position='bad')],[NS(position='1')],[NS(position='1',unrealized_pnl='Infinity')]])
def test_equity_malformed_position_and_pnl_fail_closed(positions):
    with pytest.raises((ValueError,ArithmeticError)):
        LighterRest._authoritative_equity(NS(positions=positions,total_asset_value='100',collateral='100'))


def test_size_precision_never_raises_tiny_fill_to_one():
    r=object.__new__(LighterRest)
    meta={'market_index':1,'price_decimals':3,'size_decimals':2,'price_multiplier':1000}
    with pytest.raises(ValueError): r._convert_market_order_params(meta,D('.009'),D(50),'sell',client_order_id=1)
    params=r._convert_market_order_params(meta,D('.01'),D(50),'sell',client_order_id=1)
    assert params['base_amount']==1


def test_journal_symlink_and_process_account_lock(tmp_path):
    from core.services.arbitrage_monitor_v2.execution.maker_taker_journal import Journal
    from run_maker_taker import ProcessLock
    target=tmp_path/'target';target.write_text('do not touch')
    link=tmp_path/'state';link.symlink_to(target)
    with pytest.raises(ValueError): Journal(link)
    with ProcessLock(tmp_path/'lock'):
        with pytest.raises(RuntimeError):
            with ProcessLock(tmp_path/'lock'): pass


def test_stats_missing_fees_funding_incomplete_emergency_included(tmp_path):
    e=engine(tmp_path)
    e.journal.event(SYMBOL,'maker_fill',qty='1',average='100',fee='0',modeled=False)
    e.journal.event(SYMBOL,'hedge_fill',qty='1',average='99',reference='100',side=-1,fee=None,
                    latency_ms=300,clock='process_monotonic',modeled=False)
    e.journal.event(SYMBOL,'emergency_fill',qty='.1',average='101',reference='100',side=1,fee='.01',modeled=False)
    row=next(r for r in statistics(e.journal,'day') if r['symbol']==SYMBOL)
    assert row['combined_venue_notional']=='209.1' and row['total_cost'] is None
    assert row['actual_funding'] is None and row['fee_coverage']=='2/3'
    assert row['hedge_latency_ms_p95']==300 and row['hedge_latency_samples']==1


def test_paper_entry_replay_forbids_credentials_accounts_network(tmp_path,monkeypatch,capsys):
    import run_maker_taker
    import socket
    from core.adapters.exchanges.adapters.lighter import LighterAdapter
    from core.adapters.exchanges.adapters.arcus import ArcusAdapter
    def forbidden(*a,**k): raise AssertionError('paper attempted live capability')
    monkeypatch.setattr(LighterAdapter,'__init__',forbidden)
    monkeypatch.setattr(ArcusAdapter,'__init__',forbidden)
    monkeypatch.setattr(socket,'create_connection',forbidden)
    monkeypatch.setattr(run_maker_taker,'load_live_env',forbidden)
    orig_getenv=os.getenv
    def env(key,*a):
        if key.startswith(('ARCUS_','LIGHTER_')): forbidden()
        return orig_getenv(key,*a)
    monkeypatch.setattr(os,'getenv',env)
    code=run_maker_taker.main(['--mode','replay','--replay',str(FIXTURE),'--state-dir',str(tmp_path/'run')])
    out=capsys.readouterr().out
    assert code==0 and '"mode": "replay"' in out and '"shutdown_clean": true' in out
    assert '"hedge_latency_samples": 1' in out and '"cost_status": "incomplete_or_modeled"' in out


@pytest.mark.parametrize('venue',['arcus','lighter'])
def test_public_websocket_protocol_snapshot_delta_dedup_and_generation(monkeypatch,venue):
    async def scenario():
        fixture=json.loads(FIXTURE.read_text())
        source=PublicMarkets([SYMBOL]);source.load_metadata(fixture['arcus_markets'],fixture['lighter_markets'])
        captured=[]
        if venue=='arcus':
            frames=[{'type':'subscribed','channel':'l2Orderbook','id':'SOL-USD','contents':
                {'lastSequenceId':1,'bids':[['100','1']],'asks':[['101','2']]}},
                {'type':'channel_data','channel':'l2Orderbook','id':'SOL-USD','contents':
                {'lastSequenceId':1,'bids':[['1','1']],'asks':[['2','1']]}},
                {'type':'channel_data','channel':'l2Orderbook','id':'SOL-USD','contents':
                {'lastSequenceId':2,'bids':[['100.5','3']],'asks':[['101.5','4']]}}]
        else:
            frames=[{'type':'subscribed/order_book','channel':'order_book:2','order_book':
                {'nonce':1,'bids':[{'price':'100','size':'1'}],'asks':[{'price':'101','size':'2'}]}},
                {'type':'update/order_book','channel':'order_book:2','order_book':
                {'nonce':2,'begin_nonce':1,'bids':[{'price':'100','size':'0'},{'price':'99.5','size':'3'}],
                 'asks':[{'price':'101','size':'0'},{'price':'100.5','size':'4'}]}}]
        class Socket:
            async def __aenter__(self):return self
            async def __aexit__(self,*a):pass
            async def send(self,msg):
                frame=json.loads(msg)
                assert set(frame)<= {'type','channel','id','nLevels'}
            def __aiter__(self):
                async def iterator():
                    for f in frames:yield json.dumps(f)
                    source.stopped=True
                return iterator()
        monkeypatch.setattr('core.services.arbitrage_monitor_v2.execution.maker_taker_paper.websocket_connect',lambda *a,**k:Socket())
        source.callback=lambda v,s,b:captured.append(b)
        await source._stream(venue)
        assert len(captured)==2 and captured[0].generation==1
        assert captured[-1].bid==D('100.5' if venue=='arcus' else '99.5')
        assert not source.healthy()
    asyncio.run(scenario())


def test_known_risk_cancel_unrelated_unknown_and_no_same_cancel_replay():
    from test_arcus_adapter import adapter,row
    from core.adapters.exchanges.order_state import OrderStateUnknownError
    async def scenario():
        a=adapter();a._merge_order(row());a._uncertain.add('unrelated-client')
        calls=[]
        async def request(method,path,**kwargs):
            calls.append(method)
            if method=='POST':return 202,{'status':'ACK'}
            return 200,row(status='CANCELED',seq=2)
        a._request=request
        terminal=await a.cancel_known_order('O1','BTC-USDC-PERP')
        assert terminal.status.value=='canceled' and 'unrelated-client' in a._uncertain
        assert calls.count('POST')==1
        b=adapter();b._merge_order(row());b._uncertain.add('unrelated-client')
        b._request=AsyncMock(side_effect=TimeoutError())
        for _ in range(2):
            with pytest.raises(OrderStateUnknownError):await b.cancel_known_order('O1','BTC-USDC-PERP')
        assert sum(c.args[0]=='POST' for c in b._request.await_args_list)==1
    asyncio.run(scenario())


def test_signed_hash_nonce_committed_before_send_and_audit_failure_never_sends(tmp_path):
    from test_lighter_strict_state import rest
    async def scenario():
        e=engine(tmp_path);client=maker(e)
        obligation=e.journal.observe_maker(client,D('.2'),'open')
        hedge=e.journal.prepare_attempt(obligation,D('.2'))
        r=rest();r.write_audit=e.journal.audit_write
        async def send(**kwargs):
            record=e.journal.db.execute('SELECT * FROM writes WHERE client=?',(hedge,)).fetchone()
            assert record['signed_hash']=='hash' and record['nonce']=='11' and record['key_index']==2
            return NS(code=200,tx_hash='hash')
        r.signer_client.send_tx=AsyncMock(side_effect=send)
        await r._strict_submit(r.signer_client.sign_create_order,{'market_index':1},hedge)
        other=rest();other.write_audit=Mock(side_effect=OSError('fake disk full'))
        with pytest.raises(OSError):await other._strict_submit(other.signer_client.sign_create_order,{'market_index':1},'another')
        other.signer_client.send_tx.assert_not_awaited()
    asyncio.run(scenario())


def test_monitor_runs_complete_paper_strategy_with_no_live_construction(tmp_path,monkeypatch,capsys):
    import run_maker_taker
    from core.adapters.exchanges.adapters.lighter import LighterAdapter
    fixture=json.loads(FIXTURE.read_text())
    class OfflineMarkets(PublicMarkets):
        async def metadata(self):
            self.load_metadata(fixture['arcus_markets'],fixture['lighter_markets'])
            self.connected={'arcus':True,'lighter':True};self.generations={'arcus':1,'lighter':1}
            self.clock=lambda:time.monotonic()-60
            return self.rules
        async def start(self,callback):
            self.callback=callback
            for frame in fixture['initial_books']:self._publish(frame['venue'],frame['symbol'],frame['bids'],frame['asks'])
            async def later():
                await asyncio.sleep(.05)
                offset=[-60]
                self.clock=lambda:time.monotonic()+offset[0]
                for second in range(61):
                    for frame in fixture['initial_books']:self._publish(frame['venue'],frame['symbol'],frame['bids'],frame['asks'])
                    offset[0]+=1 if second<60 else 0
                    await asyncio.sleep(0)
                self.clock=time.monotonic
                await asyncio.sleep(.05)
                for frame in fixture['frames']:self._publish(frame['venue'],frame['symbol'],frame['bids'],frame['asks'])
            self.tasks=[asyncio.create_task(later())]
    def forbidden(*a,**k):raise AssertionError('monitor attempted a live capability')
    monkeypatch.setattr('core.services.arbitrage_monitor_v2.execution.maker_taker_paper.PublicMarkets',OfflineMarkets)
    monkeypatch.setattr(run_maker_taker,'load_live_env',forbidden)
    monkeypatch.setattr(LighterAdapter,'__init__',forbidden)
    code=run_maker_taker.main(['--mode','monitor','--duration-seconds','.6','--state-dir',str(tmp_path/'monitor')])
    output=capsys.readouterr().out
    assert code==0 and '"mode": "monitor"' in output and '"hedge_latency_samples": 1' in output
    assert '"shutdown_clean": true' in output


def test_legacy_public_bootstrap_keeps_env_auth_without_private_yaml(tmp_path,monkeypatch):
    from core.utils.config_loader import ExchangeConfigLoader
    from core.services.arbitrage_monitor_v2.core.orchestrator_bootstrap import OrchestratorBootstrap
    import run_maker_taker
    for venue in ('ARCUS','LIGHTER'):
        for pattern in ExchangeConfigLoader.ENV_KEY_PATTERNS.values():
            monkeypatch.setenv(pattern.format(EXCHANGE=venue),'')
    env=tmp_path/'fixture.env'
    env.write_text('ARCUS_API_KEY=offline-public-key\nARCUS_API_SIGNING_KEY=offline-seed\nARCUS_WALLET_ADDRESS=0x'+'11'*20+
                   '\nARCUS_ACCOUNT_INDEX=0\nLIGHTER_API_KEY_PRIVATE_KEY=offline-private-key\nLIGHTER_ACCOUNT_INDEX=0\nLIGHTER_API_KEY_INDEX=0\n')
    env.chmod(0o600)
    # dotenv preserves already-existing variables; remove only these synthetic empty test values.
    for key in ('ARCUS_API_KEY','ARCUS_API_SIGNING_KEY','ARCUS_WALLET_ADDRESS','ARCUS_ACCOUNT_INDEX',
                'LIGHTER_API_KEY_PRIVATE_KEY','LIGHTER_ACCOUNT_INDEX','LIGHTER_API_KEY_INDEX'):
        monkeypatch.delenv(key)
    run_maker_taker.load_live_env(env)
    factory=NS(create_adapter=Mock(return_value=NS()))
    monkeypatch.setattr('core.adapters.exchanges.factory.ExchangeFactory',lambda:factory)
    bootstrap=object.__new__(OrchestratorBootstrap)
    bootstrap.orchestrator=NS(monitor_config=NS(exchanges=['arcus','lighter']),exchange_adapters={})
    bootstrap.init_exchange_adapters()
    configs={c.kwargs['exchange_id']:c.kwargs['config'] for c in factory.create_adapter.call_args_list}
    assert configs['arcus'].api_key=='offline-public-key' and configs['arcus'].extra_params['api_signing_key']=='offline-seed'
    assert configs['arcus'].extra_params['account_index']==0
    assert configs['lighter'].api_key_private_key=='offline-private-key' and configs['lighter'].account_index==0
    assert configs['lighter'].extra_params['official_transport_only'] is True


def test_private_reader_disconnect_invalidates_before_blocked_cleanup_and_requires_new_ack(tmp_path):
    from collections import deque
    from core.adapters.exchanges.adapters.lighter_websocket import LighterWebSocket
    from core.services.arbitrage_monitor_v2.execution.maker_taker_live import LiveTransport
    async def scenario():
        e=engine(tmp_path);client=maker(e);fake=e.transport
        gate=asyncio.Event()
        async def close(**kwargs):await gate.wait()
        socket=NS(recv=AsyncMock(side_effect=ConnectionError('offline disconnect')),
                  close=AsyncMock(side_effect=close))
        w=object.__new__(LighterWebSocket)
        w._running=True;w.generation=1;w._reader_generation=1;w._reader_connected=True
        w._reader_socket=socket;w._direct_ws=socket;w._private_ready_socket=socket
        w._private_ready_generation=1;w._authenticated_account_socket=socket
        w.last_received_monotonic=time.monotonic()
        w.auth_enabled=True;w.signer_client=object();w.account_index=0
        w._data_timeout_task=None;w._heartbeat_task=None
        live=object.__new__(LiveTransport);live.writes=deque();live.source=NS(healthy=lambda:True)
        live.arcus=NS(_websocket=NS(_ws_connected=True,_ready={('orders',None),('userFills',None)},last_received_monotonic=time.monotonic()))
        live.lighter=NS(_websocket=w);live.cancel_maker=fake.cancel_maker
        e.transport=live
        assert e.data_ready()
        await w._listen_websocket(socket)
        assert not live.healthy() and not e.data_ready() and w._authenticated_account_socket is None
        cleanup=asyncio.create_task(w._cleanup_old_connection());await asyncio.sleep(0)
        assert not live.healthy() and not cleanup.done()
        await e.watchdog_once()
        assert fake.cancels==[client] and e.state=='PAUSED_DATA'
        cleanup.cancel();await asyncio.gather(cleanup,return_exceptions=True)
        fresh=object();w._direct_ws=fresh;w._reader_socket=fresh;w.generation=2
        w._reader_generation=2;w._reader_connected=True;w._authenticated_account_socket=fresh
        assert not live.healthy()
        w._invalidate_private_socket(socket) # Old reader must never invalidate the replacement.
        assert w._reader_connected
        await w._handle_direct_ws_message({'type':'subscribed/account_all_orders','channel':'account_all_orders:1'})
        assert not live.healthy()
        await w._handle_direct_ws_message({'type':'subscribed/account_all_orders','channel':'account_all_orders:0'})
        w.last_received_monotonic=time.monotonic() # This fixture delivers the real current-socket ACK directly.
        assert live.healthy() and e.data_ready()
    asyncio.run(scenario())
