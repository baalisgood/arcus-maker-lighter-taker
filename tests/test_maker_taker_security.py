import asyncio
from contextlib import ExitStack
import sqlite3
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock,Mock

import pytest

from core.services.arbitrage_monitor_v2.execution.maker_taker_journal import Journal
from core.adapters.exchanges.official_transport import websocket_connect,OfficialHttpSession,harden_api_client


@pytest.mark.parametrize('mutation',["DROP TABLE obligations","DELETE FROM meta WHERE key='identity'",
    'ALTER TABLE attempts ADD COLUMN surprise TEXT'])
def test_existing_journal_missing_state_is_rejected_without_repair(tmp_path,mutation):
    path=tmp_path/'state.sqlite';j=Journal(path,'account');j.close()
    db=sqlite3.connect(path);db.execute(mutation);db.commit();db.close()
    with pytest.raises(ValueError):Journal(path,'account')
    db=sqlite3.connect(path)
    if mutation.startswith('DROP'):assert db.execute("SELECT name FROM sqlite_master WHERE name='obligations'").fetchone() is None
    if mutation.startswith('DELETE'):assert db.execute("SELECT value FROM meta WHERE key='identity'").fetchone() is None
    db.close()


def test_zero_byte_or_broken_foreign_key_journal_not_reinitialized(tmp_path):
    empty=tmp_path/'empty.sqlite';empty.touch()
    with pytest.raises(ValueError):Journal(empty,'account')
    path=tmp_path/'broken.sqlite';j=Journal(path,'account');j.close()
    db=sqlite3.connect(path)
    db.execute("INSERT INTO attempts(client,obligation,venue,qty,created) VALUES('X',999,'lighter','1',0)")
    db.commit();db.close()
    with pytest.raises(ValueError,match='references'):Journal(path,'account')


def test_per_venue_account_lock_overlap_and_key_rotation(tmp_path):
    from run_maker_taker import ProcessLock,account_lock_paths
    def credentials(address,lighter,key=0):return ({'WALLET_ADDRESS':address},{},0,lighter,key,'pair')
    first=account_lock_paths(credentials('0x11',1),tmp_path)
    same_arcus=account_lock_paths(credentials('0x11',2),tmp_path)
    same_lighter=account_lock_paths(credentials('0x22',1),tmp_path)
    rotated_key=account_lock_paths(credentials('0x11',1,2),tmp_path)
    separate=account_lock_paths(credentials('0x22',2),tmp_path)
    assert first==rotated_key
    with ExitStack() as stack:
        for path in first:stack.enter_context(ProcessLock(path))
        for paths in (same_arcus,same_lighter):
            common=next(p for p in paths if p in first)
            with pytest.raises(RuntimeError):
                with ProcessLock(common):pass
        for path in separate:stack.enter_context(ProcessLock(path))


def test_websocket_redirect_refused_before_private_subscriptions():
    from websockets.exceptions import InvalidStatus
    from websockets.http11 import Response
    from websockets.datastructures import Headers
    async def scenario():
        connector=websocket_connect('wss://mainnet.zklighter.elliot.ai/stream',proxy=None)
        error=InvalidStatus(Response(307,'Redirect',Headers({'Location':'wss://attacker.invalid/stream'})))
        assert connector.process_redirect(error) is error
        assert connector.uri=='wss://mainnet.zklighter.elliot.ai/stream'
    asyncio.run(scenario())


def test_real_sdk_http_transport_disables_redirects_and_environment_proxy():
    import lighter
    async def scenario():
        client=lighter.ApiClient(configuration=lighter.Configuration(host='https://mainnet.zklighter.elliot.ai'))
        await client.rest_client.pool_manager.close()
        response=NS(status=307,release=Mock())
        inner=NS(_trust_env=True,request=AsyncMock(return_value=response),close=AsyncMock())
        client.rest_client.pool_manager=inner
        harden_api_client(client)
        with pytest.raises(ValueError,match='redirect'):
            await client.call_api('POST','https://mainnet.zklighter.elliot.ai/api/v1/sendTx',
                                  header_params={'Authorization':'offline-token'},body={'tx':'offline-signed'})
        args=inner.request.await_args.kwargs
        assert args['allow_redirects'] is False and args['proxy'] is None and inner._trust_env is False
        assert inner.request.await_count==1
        with pytest.raises(ValueError,match='destination'):
            await client.call_api('POST','https://attacker.invalid/api/v1/sendTx',body={})
        assert inner.request.await_count==1
        await client.close()
    asyncio.run(scenario())


def test_runtime_endpoint_rejected_before_any_signer_factory(monkeypatch):
    from core.adapters.exchanges.adapters.lighter import LighterAdapter
    from core.adapters.exchanges.adapters.lighter_base import LighterBase
    from core.adapters.exchanges.interface import ExchangeConfig
    from core.adapters.exchanges.models import ExchangeType
    factory=Mock(side_effect=AssertionError('signer must not be constructed'))
    monkeypatch.setattr(LighterBase,'_create_signer_client',factory)
    monkeypatch.setattr(LighterAdapter,'_load_lighter_config',lambda self:{'api_config':{
        'api_url':'https://attacker.invalid','ws_mainnet_url':'wss://mainnet.zklighter.elliot.ai/stream'}})
    config=ExchangeConfig('lighter','Lighter',ExchangeType.PERPETUAL,'','',extra_params={
        'official_transport_only':True,'api_key_private_key':'offline','account_index':0,'api_key_index':0})
    with pytest.raises(ValueError,match='official'):LighterAdapter(config)
    factory.assert_not_called()


def test_arcus_post_redirect_keeps_unknown_identity_no_replay():
    from test_arcus_adapter import adapter
    from core.adapters.exchanges.models import OrderSide,OrderType
    from core.adapters.exchanges.order_state import OrderStateUnknownError
    from decimal import Decimal as D
    async def scenario():
        a=adapter();calls=[]
        class Response:
            status=307
            async def __aenter__(self):return self
            async def __aexit__(self,*args):pass
        def request(*args,**kwargs):calls.append(kwargs);return Response()
        a._session=NS(request=request)
        for _ in range(2):
            with pytest.raises(OrderStateUnknownError):
                await a.create_order('BTC-USDC-PERP',OrderSide.BUY,OrderType.LIMIT,D('.001'),D(50000),
                                     params={'clientId':'durable-identity','timeInForce':'POST_ONLY'})
        assert len(calls)==1 and calls[0]['allow_redirects'] is False
        assert 'durable-identity' in a._uncertain
    asyncio.run(scenario())


def test_official_base_and_raw_websocket_reject_unverified_tls_before_auth(monkeypatch):
    from core.adapters.exchanges.adapters.lighter_base import LighterBase
    from core.adapters.exchanges.adapters.lighter_websocket import LighterWebSocket
    config={'official_transport_only':True,'auth_enabled':True,'api_key_private_key':'offline',
            'account_index':0,'api_key_index':0}
    factory=Mock(side_effect=AssertionError('signer must not be constructed'))
    monkeypatch.setattr(LighterBase,'_create_signer_client',factory)
    monkeypatch.setenv('LIGHTER_VERIFY_SSL','false')
    with pytest.raises(ValueError,match='TLS'):LighterBase(config)
    factory.assert_not_called()
    async def scenario():
        ws=object.__new__(LighterWebSocket);ws.config=config;ws._running=True
        ws.base_url='https://mainnet.zklighter.elliot.ai';ws.ws_url='wss://mainnet.zklighter.elliot.ai/stream'
        ws._create_auth_token=AsyncMock(return_value='offline-token')
        with pytest.raises(ValueError,match='TLS'):await ws._run_direct_ws_subscription()
        ws._create_auth_token.assert_not_awaited()
    asyncio.run(scenario())


def test_official_adapter_initialize_failure_cannot_fallback_to_private_websocket():
    from core.adapters.exchanges.adapters.lighter import LighterAdapter
    async def scenario():
        adapter=object.__new__(LighterAdapter);adapter._connected=False
        adapter._wait_for_restart=AsyncMock();adapter.logger=Mock()
        adapter._rest=NS(config={'official_transport_only':True},initialize=AsyncMock(side_effect=ValueError('verified TLS required')))
        adapter._websocket=NS(connect=AsyncMock())
        assert not await adapter.connect()
        adapter._websocket.connect.assert_not_awaited()
    asyncio.run(scenario())
