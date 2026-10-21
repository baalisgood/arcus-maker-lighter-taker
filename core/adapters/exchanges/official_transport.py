"""Pinned official transport boundaries for the maker runner; no credential-bearing redirects."""
from urllib.parse import urlsplit
import os

from websockets.asyncio.client import connect,ClientConnection
from websockets.frames import Frame


class ReceivedConnection(ClientConnection):
    """Trusted receive hook covers data, ping and pong frames without exposing their payload."""
    def __init__(self,*args,on_received,**kwargs):
        self.on_received=on_received
        super().__init__(*args,**kwargs)

    def process_event(self,event):
        super().process_event(event)
        if isinstance(event,Frame):self.on_received()


class websocket_connect(connect):
    def process_redirect(self,exc):
        return exc # Returning an exception forbids every redirect before subscriptions/auth are sent.


def validate_lighter_endpoints(api_url,ws_url):
    if api_url!='https://mainnet.zklighter.elliot.ai' or ws_url!='wss://mainnet.zklighter.elliot.ai/stream':
        raise ValueError('Maker live transport requires exact official mainnet endpoints')
    if os.getenv('LIGHTER_VERIFY_SSL','true').lower()!='true':
        raise ValueError('Official Lighter transport requires verified TLS')


class OfficialHttpSession:
    def __init__(self,session):
        self.session=session
        session._trust_env=False # aiohttp 3.9.1 pinned by this project; also disable SDK-created env proxies.

    def __getattr__(self,name):return getattr(self.session,name)

    async def request(self,*args,**kwargs):
        url=kwargs.get('url',args[1] if len(args)>1 else '')
        parsed=urlsplit(str(url))
        if (parsed.scheme!='https' or parsed.hostname!='mainnet.zklighter.elliot.ai' or parsed.port not in (None,443)
                or parsed.username is not None or parsed.password is not None or parsed.fragment
                or not parsed.path.startswith('/api/v1/')):
            raise ValueError('Lighter request destination is outside official API')
        kwargs.update(allow_redirects=False,proxy=None,proxy_auth=None)
        response=await self.session.request(*args,**kwargs)
        if 300<=response.status<400:
            response.release()
            raise ValueError('Official API redirect refused; write acceptance remains unknown')
        return response


def harden_api_client(client):
    if client is None:return
    rest=client.rest_client
    if not isinstance(rest.pool_manager,OfficialHttpSession):
        rest.pool_manager=OfficialHttpSession(rest.pool_manager)
    rest.proxy=None;rest.proxy_headers=None
    # Auto retry is never appropriate for signed write replay; pinned reads recover through the engine.
    rest.retry_client=None
