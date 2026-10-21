"""Arcus perpetual adapter. Writes are signed once and never automatically replayed."""
import asyncio
import json
import re
import time
import uuid
from copy import deepcopy
from datetime import datetime
from decimal import Decimal, ROUND_DOWN, ROUND_UP

import aiohttp
from nacl.signing import SigningKey

from ..adapter import ExchangeAdapter
from ..models import (BalanceData, ExchangeInfo, ExchangeType, MarginMode,
                      OrderBookData, OrderBookLevel, OrderData, OrderSide,
                      OrderStatus, OrderType, PositionData, PositionSide, TickerData)
from ..order_state import OrderStateUnknownError
from .arcus_websocket import ArcusWebSocket

D = Decimal
TERMINAL = {OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED, OrderStatus.EXPIRED}


class ArcusRequestRejectedError(ValueError):
    """Gateway definitively rejected a request before acceptance."""


class ArcusAdapter(ExchangeAdapter):
    strict_order_state = True
    def __init__(self, config, event_bus=None):
        super().__init__(config, event_bus)
        host = 'api.testnet.arcus.xyz' if config.testnet else 'api.arcus.xyz'
        self.base_url = config.base_url or f'https://{host}'
        self.ws_url = config.ws_url or f'wss://{host}/v1/ws'
        # Prevent accidentally mixing testnet credentials/quotes with mainnet endpoints.
        if self.base_url.rstrip('/') != f'https://{host}' or self.ws_url != f'wss://{host}/v1/ws':
            raise ValueError('Arcus endpoints must match the selected network')
        self.address = (config.wallet_address or config.extra_params.get('wallet_address', '')).lower()
        self.account_index = int(config.extra_params.get('account_index', 0))
        if not 0 <= self.account_index <= 9:
            raise ValueError('Arcus account_index must be 0..9')
        self.api_key = config.api_key
        self._seed = config.extra_params.get('api_signing_key', '') or config.private_key or ''
        self._session = None
        self._markets = {}
        self._orders = {}
        self._clients = {}
        self._positions = {}
        self._positions_snapshot_valid = False
        self._positions_snapshot_at = 0.0
        self._fill_ids = set()
        self._fills = {}
        self._uncertain = set()
        self._write_lock = asyncio.Lock()
        self._modify_uncertain = set()
        self._cancel_uncertain = set()
        self._last_nonce = 0
        self._websocket = ArcusWebSocket(self)

    def native_symbol(self, symbol):
        return symbol[:-10] + '-USD' if symbol.endswith('-USDC-PERP') else symbol

    def standard_symbol(self, symbol):
        return symbol[:-4] + '-USDC-PERP' if symbol.endswith('-USD') else symbol

    def _sign(self, payload):
        if not re.fullmatch(r'[0-9a-fA-F]{64}', self._seed or ''):
            raise ValueError('Arcus requires a dedicated 32-byte Ed25519 API signing seed')
        key = SigningKey(bytes.fromhex(self._seed))
        if key.verify_key.encode().hex() != self.api_key.lower():
            raise ValueError('Arcus API public key does not match signing seed')
        return key.sign(json.dumps(payload, sort_keys=True, separators=(',', ':')).encode()).signature.hex()

    def _nonce(self):
        self._last_nonce = max(time.time_ns(), self._last_nonce + 1)
        return self._last_nonce

    async def _request(self, method, path, *, query=None, body=None, signature=None, nonce=None):
        if self._session is None:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.config.request_timeout))
        if method != "GET":
            self._invalidate_account_reads()
        headers = {}
        if self.api_key:
            headers['X-API-Key'] = self.api_key
        if signature:
            headers.update({'X-Timestamp': str(nonce), 'X-Signature': signature})
        try:
            async with self._session.request(method, self.base_url + path, params=query, json=body, headers=headers, allow_redirects=False) as response:
                if 300 <= response.status < 400:
                    if method!='GET':raise OrderStateUnknownError('arcus',client_id=(body or {}).get('clientId'))
                    raise ArcusRequestRejectedError('Arcus redirects are disabled')
                if response.status >= 500 and method != 'GET':
                    raise OrderStateUnknownError('arcus', client_id=(body or {}).get('clientId'))
                if response.status >= 400:
                    # Neither a 404 nor an unparseable response proves an accepted order never existed.
                    if method != 'GET' and response.status not in (400, 401, 403, 429):
                        raise OrderStateUnknownError('arcus', client_id=(body or {}).get('clientId'))
                    raise ArcusRequestRejectedError(f'Arcus HTTP {response.status}')
                return response.status, await response.json()
        except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError) as exc:
            if method != 'GET':
                raise OrderStateUnknownError('arcus', client_id=(body or {}).get('clientId')) from exc
            raise RuntimeError('Arcus read unavailable') from exc

    async def _do_connect(self):
        await self.load_markets()
        if self.config.enable_websocket:
            await self._websocket.connect()
        return True

    async def _do_disconnect(self):
        await self._websocket.disconnect()
        if self._session:
            await self._session.close()
            self._session = None

    async def _do_authenticate(self):
        if not re.fullmatch(r'0x[0-9a-f]{40}', self.address):
            raise ValueError('Arcus wallet address required for private operations')
        self._sign({'check': 1})
        return True

    async def _do_health_check(self):
        return {'rest': bool(self._markets), 'websocket': self._websocket._ws_connected}

    async def _do_heartbeat(self):
        pass  # websockets performs protocol ping/pong.

    async def load_markets(self):
        _, data = await self._request('GET', '/v1/markets')
        self._set_markets(data.get('markets', data))
        return self._markets

    def _set_markets(self, rows):
        if isinstance(rows, dict):
            rows = rows.values()
        self._markets = {row['marketDisplayName']: row for row in rows}

    async def get_supported_symbols(self):
        if not self._markets:
            await self.load_markets()
        return [self.standard_symbol(s) for s, m in self._markets.items() if m.get('status') == 'ONLINE']

    async def get_exchange_info(self):
        raise NotImplementedError('Arcus ExchangeInfo export is not used by the arbitrage workflow')

    def _market(self, symbol):
        market = self._markets[self.native_symbol(symbol)]
        if market.get('status') != 'ONLINE':
            raise ValueError('Arcus market is offline')
        return market

    def _price_tick(self, market, price):
        for band in market.get('tickTiers', []):
            if 'upToPrice' not in band or price < D(band['upToPrice']):
                return D(band['tick'])
        return D(market['tickSize'])

    def _validate_order(self, market, amount, price, reduce_only):
        step = D(market['stepSize']); tick = self._price_tick(market, price)
        if not amount.is_finite() or not price.is_finite() or amount <= 0 or price <= 0:
            raise ValueError('Arcus price and size must be positive finite decimals')
        if amount % step or price % tick or price % D(market['tickSize']):
            raise ValueError('Arcus price/size does not match market tick/step')
        if not D(market['minOrderSize']) <= amount <= D(market['maxOrderSize']):
            raise ValueError('Arcus order size outside market limits')
        if not reduce_only and amount * price < D(market['minOrderNotional']):
            raise ValueError('Arcus order notional below minimum')

    async def create_order(self, symbol, side, order_type, amount, price=None, params=None):
        async with self._write_lock:
            return await self._create_order(symbol, side, order_type, amount, price, params)

    async def _create_order(self, symbol, side, order_type, amount, price=None, params=None):
        params = params or {}
        if self._uncertain:
            raise OrderStateUnknownError('arcus')
        await self._do_authenticate()
        if not self._markets:
            await self.load_markets()
        market = self._market(symbol); amount = D(str(amount))
        side = OrderSide(side); order_type = OrderType(order_type)
        if order_type not in (OrderType.LIMIT, OrderType.MARKET):
            raise ValueError('Arcus arbitrage supports LIMIT and MARKET only')
        reduce_only = bool(params.get('reduceOnly', params.get('reduce_only', False)))
        if order_type == OrderType.MARKET:
            book = await self.get_orderbook(symbol)
            best = book.best_ask if side == OrderSide.BUY else book.best_bid
            if not best:
                raise ValueError('Arcus book is empty')
            slippage = D(str(params.get('slippage', params.get('slippage_tolerance_pct', '.005'))))
            if not 0 <= slippage <= D('.1'):
                raise ValueError('Arcus market slippage must be 0..10%')
            price = best.price * (1 + slippage if side == OrderSide.BUY else 1 - slippage)
            tick = self._price_tick(market, price)
            price = (price / tick).to_integral_value(rounding=ROUND_UP if side == OrderSide.BUY else ROUND_DOWN) * tick
            mark = D(market.get('markPrice', '0'))
            if mark <= 0 or abs(price / mark - 1) > D('.1'):
                raise ValueError('Arcus protective market price exceeds mark bound')
            tif = 'IOC'
        else:
            if price is None:
                raise ValueError('Arcus limit order requires price')
            price = D(str(price)); tif = params.get('timeInForce', 'GTT').upper()
            tif = {'GTC': 'GTT', 'POST_ONLY': 'ALO'}.get(tif, tif)
        self._validate_order(market, amount, price, reduce_only)
        guard=params.get('_pre_sign_guard')
        if guard is not None:
            try:admitted=guard()
            except Exception as exc:raise ArcusRequestRejectedError('Arcus close admission unavailable before signing') from exc
            if admitted is not True:raise ArcusRequestRejectedError('Arcus close admission changed before signing')
        nonce = self._nonce(); expiry = nonce // 1000 + 32 * 24 * 3600 * 1000000
        client = params.get('clientId') or uuid.uuid4().hex
        if not re.fullmatch(r'[A-Za-z0-9_-]{1,36}', client):
            raise ValueError('Arcus clientId must contain 1..36 ASCII identifier characters')
        payload = {'ad': self.address, 'ai': self.account_index, 'c': client, 'ct': nonce,
                   'g': expiry * 1000, 'm': int(market['marketId']), 'op': 1,
                   'p': int(price / D(market['tickSize'])), 'q': int(amount / D(market['stepSize'])),
                   'r': int(reduce_only), 's': int(side == OrderSide.SELL),
                   't': {'GTT': 0, 'FOK': 1, 'IOC': 2, 'ALO': 3}[tif], 'v': 1}
        body = {'address': self.address, 'accountIndex': self.account_index, 'marketId': payload['m'],
                'orderSide': side.value.upper(), 'orderType': order_type.value.upper(),
                'quantity': format(amount, 'f'), 'price': format(price, 'f'), 'timeInForce': tif,
                'goodTilTime': str(expiry), 'clientId': client, 'timestamp': nonce, 'reduceOnly': reduce_only}
        signature = self._sign(payload)
        audit = getattr(self, 'write_audit', None)
        if audit is not None:
            audit(client,nonce=nonce,tx_type='arcus_create')
        self._uncertain.add(client)
        try:
            _, ack = await self._request('POST', '/v1/placeOrder', query={'address': self.address}, body=body, signature=signature, nonce=nonce)
            if not isinstance(ack, dict) or not ack.get('orderId'):
                raise OrderStateUnknownError('arcus', client_id=client)
            order_id = ack.get('orderId')
            if order_id:
                self._clients[client] = order_id
                # ACK is not a lifecycle event. Keep only identity until WS/REST reconciles.
                self._orders.setdefault(order_id, self._parse_order({**body, 'orderId': order_id, 'status': 'ACK', 'marketDisplayName': self.native_symbol(symbol)}))
            order = await self._await_order(order_id or client, symbol, terminal=(order_type == OrderType.MARKET))
            self._uncertain.discard(client)
            return order
        except OrderStateUnknownError:
            raise
        except ArcusRequestRejectedError:
            self._uncertain.discard(client)  # Explicit gateway rejection, not a transport timeout.
            raise
        except BaseException as exc:
            raise OrderStateUnknownError('arcus', order_id=locals().get('order_id'), client_id=client) from exc

    def _account_query(self):
        if not self.address:
            raise ValueError('Arcus account address required')
        return {'address': self.address, 'accountIndex': self.account_index}

    def _scope_matches(self, row):
        return isinstance(row, dict) and str(row.get('address', self.address)).lower() == self.address and row.get('accountIndex', 0) == self.account_index

    def _parse_order(self, row):
        amount = D(str(row.get('originalSize', row.get('quantity', '0'))))
        state = row.get('state', row.get('status', 'UNKNOWN'))
        if row.get('status') in ('FILLED', 'CANCELED', 'REJECTED', 'EXPIRED'):
            state = row['status']
        if row.get('cancelReason') == 'MODIFY_CANCELED':
            state = 'MODIFY_PENDING'
        status = {'OPEN': OrderStatus.OPEN, 'PLACED': OrderStatus.OPEN, 'PARTIALLY_FILLED': OrderStatus.OPEN,
                  'FILLED': OrderStatus.FILLED, 'CANCELED': OrderStatus.CANCELED,
                  'MARGIN_CANCELED': OrderStatus.CANCELED, 'EXPIRED': OrderStatus.EXPIRED, 'REJECTED': OrderStatus.REJECTED}.get(state, OrderStatus.PENDING)
        filled = D(str(row.get('filledSize', amount - D(str(row.get('remainingSize', amount))))))
        if status == OrderStatus.FILLED:
            filled = amount
        if state == 'PARTIALLY_FILLED' and row.get('timeInForce') == 'IOC':
            status = OrderStatus.CANCELED
        average = D(str(row.get('avgFillPrice') or row.get('averageFillPrice') or '0'))
        return OrderData(str(row.get('orderId', '')), row.get('clientId'),
                         self.standard_symbol(row.get('marketDisplayName') or self._symbol_for_market(row.get('marketId'))),
                         OrderSide(str(row.get('side', row.get('orderSide', 'BUY'))).lower()),
                         OrderType(str(row.get('type', row.get('orderType', 'LIMIT'))).lower()),
                         amount, D(str(row.get('price', '0'))), filled, max(D(0), amount-filled),
                         filled*average, average or None, status, datetime.now(), datetime.now(),
                         None, [], {}, dict(row))

    def _symbol_for_market(self, market_id):
        return next((s for s,m in self._markets.items() if str(m['marketId']) == str(market_id)), '')

    def _merge_order(self, row):
        if not self._scope_matches(row) or not row.get('orderId'):
            return None
        order_id = str(row['orderId']); previous = self._orders.get(order_id)
        if previous:
            old_seq = previous.raw_data.get('sequenceNumber'); new_seq = row.get('sequenceNumber')
            old_time = previous.raw_data.get('updatedAt'); new_time = row.get('updatedAt')
            if old_seq is not None and new_seq is not None and int(new_seq) < int(old_seq):
                return previous
            if old_time is not None and new_time is not None and int(new_time) < int(old_time):
                return previous
        merged = {**(previous.raw_data if previous else {}), **row}
        # Event-local evidence must survive quantity clamping without inheriting older proof.
        merged['_reported_order'] = {key: row.get(key) for key in
                                     ('status', 'state', 'cancelReason', 'originalSize', 'remainingSize')}
        if 'originalSize' in row and 'remainingSize' in row:
            merged['filledSize'] = str(D(str(row['originalSize'])) - D(str(row['remainingSize'])))
        if 'status' in row or 'state' in row:
            merged['state'] = row.get('state', row.get('status'))
            # cancelReason is event-local, a following OPEN ends the modify transition.
            merged['cancelReason'] = row.get('cancelReason')
        order = self._parse_order(merged)
        incoming_filled = order.filled
        if previous:
            if order.filled < previous.filled:
                order.filled = previous.filled
                order.average = previous.average
                order.cost = previous.cost
            order.remaining = max(D(0), order.amount-order.filled)
            if previous.status in TERMINAL and order.status not in TERMINAL and row.get('cancelReason') != 'MODIFY_CANCELED':
                order.status = previous.status
        cumulative_average = row.get('avgFillPrice', row.get('averageFillPrice'))
        valid_average = (cumulative_average is not None and D(str(cumulative_average)).is_finite()
                         and D(str(cumulative_average)) > 0 and incoming_filled >= order.filled)
        valuation_unknown = bool(previous and previous.raw_data.get('_valuation_unknown'))
        valuation_unknown = (valuation_unknown or order.filled > (previous.filled if previous else D(0))) and not valid_average
        order.raw_data['_valuation_unknown'] = valuation_unknown
        order.raw_data['filledSize'] = str(order.filled)
        order.raw_data['remainingSize'] = str(order.remaining)
        if valuation_unknown:
            order.average = None
            order.cost = previous.cost if previous else D(0)
            order.status = OrderStatus.PENDING
            order.raw_data.pop('avgFillPrice', None)
            order.raw_data.pop('averageFillPrice', None)
            self._uncertain.add(order_id)
        elif order.average is not None:
            order.raw_data['avgFillPrice'] = str(order.average)
        self._orders[order_id] = order
        if order.client_id:
            self._clients[order.client_id] = order_id
        if row.get('cancelReason') == 'MODIFY_CANCELED':
            if order_id not in self._uncertain and order.client_id not in self._uncertain:
                self._modify_uncertain.add(order_id)
            self._uncertain.add(order_id)
        elif order_id in self._modify_uncertain and order.status == OrderStatus.OPEN and not valuation_unknown:
            self._modify_uncertain.discard(order_id)
            self._uncertain.discard(order_id)
        elif order.status in TERMINAL:
            self._invalidate_account_reads()
            self._modify_uncertain.discard(order_id)
            self._uncertain.discard(order_id); self._uncertain.discard(order.client_id)
        return order

    def _validated_rows(self, data, field):
        if not isinstance(data, dict) or not self._scope_matches({'address':self.address,'accountIndex':self.account_index,**data}) or field not in data:
            raise OrderStateUnknownError('arcus')
        rows = data[field]
        if isinstance(rows, dict): rows = list(rows.values())
        if not isinstance(rows, list) or any(not self._scope_matches(r) for r in rows):
            raise OrderStateUnknownError('arcus')
        if (field == 'orders' and len(rows) >= 1000) or data.get('nextCursor') or data.get('hasMore') or int(data.get('total', len(rows))) > len(rows):
            raise OrderStateUnknownError('arcus')
        return rows

    def _validated_order_row(self, row):
        if (not self._scope_matches(row) or not row.get('orderId')
                or not all(k in row for k in ('marketDisplayName', 'marketId', 'side', 'status', 'price', 'originalSize', 'remainingSize', 'updatedAt'))):
            raise OrderStateUnknownError('arcus')
        amount = D(str(row['originalSize'])); remaining = D(str(row['remainingSize']))
        if not amount.is_finite() or not remaining.is_finite() or not 0 <= remaining <= amount:
            raise OrderStateUnknownError('arcus')
        parsed = self._parse_order(row)
        if not parsed.price.is_finite() or (parsed.average is not None and not parsed.average.is_finite()):
            raise OrderStateUnknownError('arcus')
        return row

    async def _get_order_observation(self, order_id, symbol, minimum_sequence=None):
        resolved = self._clients.get(order_id, order_id)
        _, data = await self._request('GET', f'/v1/order/{resolved}', query=self._account_query())
        row = self._validated_order_row(data)
        if str(row['orderId']) != str(resolved):
            raise OrderStateUnknownError('arcus', order_id=resolved)
        if minimum_sequence is not None:
            try:
                if int(row['sequenceNumber']) < minimum_sequence:
                    raise OrderStateUnknownError('arcus', order_id=resolved)
            except (KeyError, TypeError, ValueError) as exc:
                raise OrderStateUnknownError('arcus', order_id=resolved) from exc
        return self._merge_order(row)

    async def get_order(self, order_id, symbol):
        resolved = self._clients.get(order_id, order_id)
        try:
            await self._get_order_observation(order_id, symbol)
        except Exception as exc:
            cached = self._orders.get(resolved)
            if not cached or cached.status not in TERMINAL:
                raise OrderStateUnknownError('arcus', order_id=resolved) from exc
        order = self._orders.get(resolved)
        if not order or order.status in (OrderStatus.PENDING, OrderStatus.UNKNOWN):
            raise OrderStateUnknownError('arcus', order_id=resolved)
        return order

    async def get_order_by_client(self, client_id, symbol):
        """Client IDs are not system IDs and never authorize a submission replay."""
        resolved = self._clients.get(str(client_id))
        if resolved:
            return await self.get_order(resolved, symbol)
        # Both calls require complete, account-scoped responses. Truncated history fails closed.
        for reader in (self.get_open_orders, self.get_order_history):
            for order in await reader(symbol):
                if order.client_id == str(client_id):
                    return await self.get_order(order.id, symbol)
        raise OrderStateUnknownError('arcus', client_id=str(client_id))

    async def _await_order(self, order_id, symbol, terminal):
        deadline = asyncio.get_running_loop().time() + float(self.config.extra_params.get('reconcile_timeout', 3))
        while True:
            resolved = self._clients.get(order_id, order_id)
            cached = self._orders.get(resolved)
            if cached and (cached.status in TERMINAL or not terminal and cached.status == OrderStatus.OPEN):
                return cached
            try:
                order = await self.get_order(order_id, symbol)
                if order.status in TERMINAL or not terminal and order.status == OrderStatus.OPEN:
                    return order
            except OrderStateUnknownError:
                pass
            if asyncio.get_running_loop().time() >= deadline:
                self._modify_uncertain.discard(resolved)
                self._uncertain.add(order_id)
                raise OrderStateUnknownError('arcus', order_id=resolved)
            await asyncio.sleep(.1)

    async def cancel_order(self, order_id, symbol):
        async with self._write_lock:
            return await self._cancel_order(order_id, symbol)

    async def cancel_known_order(self, order_id, symbol):
        """Risk cancellation may proceed despite unrelated uncertainty; same cancel is never replayed."""
        async with self._write_lock:
            return await self._cancel_order(order_id, symbol, risk_reducing=True)

    async def _cancel_order(self, order_id, symbol, risk_reducing=False):
        market = self._market(symbol); resolved = self._clients.get(order_id, order_id)
        cached = self._orders.get(resolved)
        if cached and cached.status in TERMINAL:
            return cached
        cancel_pending = resolved in self._cancel_uncertain
        known_risk_cancel = risk_reducing and cached is not None and cached.id == resolved and not cancel_pending
        if (resolved in self._uncertain or order_id in self._uncertain) and not known_risk_cancel:
            return await self._await_order(resolved, symbol, terminal=True)
        if self._uncertain and not known_risk_cancel:
            raise OrderStateUnknownError('arcus', order_id=resolved)
        nonce = self._nonce()
        payload = {'ad': self.address, 'ai': self.account_index, 'ct': nonce, 'm': int(market['marketId']), 'op': 2, 'v': 1}
        body = {**self._account_query(), 'marketId': payload['m'], 'timestamp': nonce}
        field = 'c' if order_id in self._uncertain and order_id not in self._clients else 'id'
        payload[field] = resolved; body['orderId' if field == 'id' else 'clientId'] = resolved
        body['kind'] = 'orderId' if field == 'id' else 'clientId'
        self._uncertain.add(resolved)
        self._cancel_uncertain.add(resolved)
        try:
            audit = getattr(self, 'write_audit', None)
            if audit is not None:
                audit(cached.client_id or resolved,nonce=nonce,tx_type='arcus_cancel',is_cancel=True)
            await self._request('POST', '/v1/cancelOrder', query={'address': self.address}, body=body, signature=self._sign(payload), nonce=nonce)
            terminal = await self._await_order(resolved, symbol, terminal=True)
            self._cancel_uncertain.discard(resolved)
            return terminal
        except BaseException as exc:
            raise OrderStateUnknownError('arcus', order_id=resolved) from exc

    async def get_open_orders(self, symbol=None):
        _, data = await self._request('GET', '/v1/openOrders', query=self._account_query())
        rows = self._validated_rows(data, 'orders')
        for row in rows: self._validated_order_row(row)
        orders = [self._merge_order(r) for r in rows]
        return [o for o in orders if o and (not symbol or o.symbol == self.standard_symbol(symbol))]

    async def get_order_history(self, symbol=None, since=None, limit=None):
        if since is not None or limit is not None:
            raise NotImplementedError('Arcus order history pagination/filtering is not supported')
        _, data = await self._request('GET', '/v1/orders', query=self._account_query())
        rows = self._validated_rows(data, 'orders')
        for row in rows: self._validated_order_row(row)
        orders = [self._merge_order(r) for r in rows]
        return [o for o in orders if o and (not symbol or o.symbol == self.standard_symbol(symbol))]

    async def cancel_all_orders(self, symbol=None):
        outcomes = await asyncio.gather(*(self.cancel_known_order(o.id, o.symbol)
                                         for o in await self.get_open_orders(symbol)), return_exceptions=True)
        if any(isinstance(o, BaseException) for o in outcomes):
            raise OrderStateUnknownError('arcus')
        return outcomes

    def _parse_position(self, row):
        size = D(str(row.get('size', '0'))); mark = D(str(row.get('markPx', '0')))
        return PositionData(self.standard_symbol(row.get('marketDisplayName') or self._symbol_for_market(row.get('marketId'))),
                            PositionSide.SHORT if row.get('side') == 'SHORT' else PositionSide.LONG,
                            abs(size), D(str(row.get('averageEntryPrice', '0'))), mark, mark,
                            D(str(row.get('unrealizedPnl', '0'))), D(0), None, int(row.get('leverage', 1)),
                            MarginMode(str(row.get('marginMode', 'CROSS')).lower()), D(str(row.get('marginUsed', '0'))),
                            None, datetime.now(), dict(row))

    def _invalidate_account_reads(self):
        self._positions_snapshot_valid = False
        self._account_reads = {}
        self._account_generation = getattr(self, "_account_generation", 0) + 1

    async def _cached_account_read(self, name, reader):
        if not hasattr(self, "_account_read_lock"):
            self._account_read_lock = asyncio.Lock()
            self._account_reads = {}
        async with self._account_read_lock:
            now = time.monotonic()
            cached = self._account_reads.get(name)
            if cached and now - cached[0] < 2:
                return deepcopy(cached[1])
            if now < getattr(self, "_account_retry_at", 0):
                raise OrderStateUnknownError("arcus")
            try:
                for _ in range(3):
                    generation = getattr(self, "_account_generation", 0)
                    result = await reader()
                    if generation == getattr(self, "_account_generation", 0):
                        break
                else:
                    raise ValueError("Account changed during read")
                if name == "positions":
                    # Publish only after the account generation was validated.
                    self._positions = {p.symbol: p for p in result}
                    self._positions_snapshot_at = time.monotonic()
                    self._positions_snapshot_valid = True
                self._account_reads[name] = (time.monotonic(), deepcopy(result))
                self._account_retry_at = 0
                return result
            except Exception as exc:
                self._account_reads = {}
                throttled = any(word in str(exc).lower() for word in ("429", "403", "405", "waf"))
                self._account_retry_at = time.monotonic() + (30 if throttled else 2)
                raise OrderStateUnknownError("arcus") from exc

    async def get_positions(self, symbols=None):
        try:
            positions = await self._cached_account_read("positions", self._read_positions)
        except BaseException:
            self._positions_snapshot_valid = False
            raise
        requested = {self.standard_symbol(s) for s in symbols} if symbols else None
        return [p for p in positions if requested is None or p.symbol in requested]

    async def _read_positions(self, symbols=None):
        _, data = await self._request('GET', '/v1/positions', query=self._account_query())
        rows = self._validated_rows(data, 'positions')
        for row in rows:
            if not all(k in row for k in ('address', 'accountIndex', 'marketId', 'marketDisplayName', 'side', 'size', 'averageEntryPrice', 'leverage', 'marginMode', 'borrowedCapital', 'marginUsed', 'positionValueNotional', 'unrealizedPnl', 'markPx')):
                raise OrderStateUnknownError('arcus')
            for field in ('size', 'averageEntryPrice', 'borrowedCapital', 'marginUsed', 'positionValueNotional', 'unrealizedPnl', 'markPx'):
                if not D(str(row[field])).is_finite():
                    raise OrderStateUnknownError('arcus')
            size = D(str(row['size']))
            if not size.is_finite() or row['side'] not in ('LONG', 'SHORT'):
                raise OrderStateUnknownError('arcus')
        positions = [self._parse_position(r) for r in rows]
        return [p for p in positions if p.size and (not symbols or p.symbol in symbols)]

    async def get_balances(self):
        return await self._cached_account_read("balances", self._read_balances)

    async def _read_balances(self):
        try:
            _, data = await self._request('GET', '/v1/account', query=self._account_query())
        except ValueError as exc:
            if str(exc) == 'Arcus HTTP 404':
                return []  # Official empty/unfunded account response.
            raise
        if not self._scope_matches(data) or not all(k in data for k in ('address', 'accountIndex', 'equity', 'freeCollateral')):
            raise OrderStateUnknownError('arcus')
        total=D(str(data['equity'])); free=D(str(data['freeCollateral']))
        if not total.is_finite() or not free.is_finite():
            raise OrderStateUnknownError('arcus')
        return [BalanceData('USDC', free, total-free, total, total, datetime.now(), data)]

    def _parse_book(self, symbol, data):
        return OrderBookData(self.standard_symbol(symbol),
                             [OrderBookLevel(D(p),D(q)) for p,q in data.get('bids',[]) if D(q)>0],
                             [OrderBookLevel(D(p),D(q)) for p,q in data.get('asks',[]) if D(q)>0],
                             datetime.fromtimestamp(int(data.get('timestamp',time.time_ns()//1000))/1000000),
                             data.get('lastSequenceId'), raw_data=data)

    async def get_orderbook(self, symbol, limit=None):
        native=self.native_symbol(symbol)
        _, data=await self._request('GET',f'/v1/l2OrderBook/{native}',query={'nLevels':limit or 20})
        return self._parse_book(native,data)

    def _parse_ticker(self, market):
        return TickerData(self.standard_symbol(market['marketDisplayName']),datetime.now(),
                          last=D(str(market.get('lastTradePrice','0'))),mark_price=D(str(market.get('markPrice','0'))),
                          funding_rate=D(str(market.get('fundingRate','0')))*8,
                          predicted_funding_rate=D(str(market.get('nextFundingRate','0')))*8,
                          next_funding_time=datetime.fromtimestamp(market['nextFundingAt']) if market.get('nextFundingAt') else None,
                          funding_interval=3600,raw_data=market)

    async def get_ticker(self,symbol):
        await self.load_markets(); return self._parse_ticker(self._market(symbol))

    async def get_tickers(self,symbols=None):
        await self.load_markets()
        return [self._parse_ticker(m) for s,m in self._markets.items() if not symbols or self.standard_symbol(s) in symbols]

    async def subscribe_orderbook(self,symbol,callback):
        await self._websocket.subscribe_orderbook(self.native_symbol(symbol),callback)

    async def subscribe_ticker(self,symbol,callback):
        await self._websocket.subscribe_ticker(self.native_symbol(symbol),callback)

    async def subscribe_user_data(self,callback):
        await self._websocket.subscribe_user_data(callback)

    async def unsubscribe(self,symbol=None):
        await self._websocket.unsubscribe(self.native_symbol(symbol) if symbol else None)

    async def get_ohlcv(self,*args,**kwargs): raise NotImplementedError('Arcus OHLCV is outside this arbitrage integration')
    async def get_trades(self,*args,**kwargs): raise NotImplementedError('Arcus trade history is outside this arbitrage integration')
    async def subscribe_trades(self,*args,**kwargs): raise NotImplementedError('Arcus trades subscription is outside this arbitrage integration')
    async def set_leverage(self,*args,**kwargs): raise NotImplementedError('Configure Arcus leverage in the exchange UI')
    async def set_margin_mode(self,*args,**kwargs): raise NotImplementedError('Configure Arcus margin mode in the exchange UI')
