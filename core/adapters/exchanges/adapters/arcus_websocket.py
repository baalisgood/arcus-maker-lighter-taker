"""Arcus multiplexed feeds. Reconnect re-subscribes for fresh full snapshots."""
import asyncio
import inspect
import json
import time
from copy import deepcopy

import websockets
from ..official_transport import websocket_connect,ReceivedConnection

from ..order_state import OrderStateUnknownError
from ..models import OrderStatus


class ArcusWebSocket:
    def __init__(self, adapter):
        self.adapter = adapter
        self._ws_connected = False
        self._socket = None
        self._task = None
        self._stopping = False
        self._subscriptions = {}
        self._callbacks = {}
        self._book_sequences = {}
        self.order_fill_ws_enabled = True
        self._ready = set()
        self.generation = 0
        self.last_received_monotonic = 0
        self._quantity_callbacks = []
        self._recovery_tasks = set()
        self._startup_recoveries = {}
        self._startup_retry_fills = {}
        self.liveness_seconds=None

    def _reset_startup_recoveries(self):
        for identity in self._startup_retry_fills:
            self.adapter._fill_ids.discard(identity)
        self._startup_retry_fills.clear()
        self._startup_recoveries.clear()

    def record_received(self):
        now=time.monotonic();last=self.last_received_monotonic
        if (self.liveness_seconds is not None and last
                and now-last>=self.liveness_seconds+getattr(self,'quick_resume_seconds',10)):
            self.liveness_revision=getattr(self,'liveness_revision',0)+1
        self.last_received_monotonic=now

    async def _open_socket(self):
        options=dict(ping_interval=20,ping_timeout=20,open_timeout=self.adapter.config.connect_timeout,proxy=None)
        if self.liveness_seconds is not None:
            generation=self.generation+1
            def received():
                if self._ws_connected and self.generation==generation:self.record_received()
            options.update(ping_interval=self.liveness_seconds/2,
                ping_timeout=self.liveness_seconds+getattr(self,'quick_resume_seconds',10),
                create_connection=lambda *args,**kwargs:ReceivedConnection(*args,on_received=received,**kwargs))
        return await websocket_connect(self.adapter.ws_url,**options)

    async def connect(self):
        if self._task:
            return
        self._stopping = False
        self._socket = await self._open_socket()
        self._ws_connected = True
        self.generation += 1
        self._reset_startup_recoveries()
        self.last_received_monotonic=time.monotonic()
        for frame in self._subscriptions.values():
            await self._socket.send(json.dumps(frame))
        self._task = asyncio.create_task(self._run())

    async def disconnect(self):
        self._stopping = True
        if self._task:
            self._task.cancel()
            try: await self._task
            except asyncio.CancelledError: pass
            self._task = None
        if self._socket:
            await self._socket.close()
        self._ws_connected = False
        self._ready.clear()
        for task in self._recovery_tasks:
            task.cancel()
        await asyncio.gather(*self._recovery_tasks, return_exceptions=True)

    async def _run(self):
        while not self._stopping:
            try:
                async for raw in self._socket:
                    self.record_received()
                    await self.handle_message(json.loads(raw))
            except asyncio.CancelledError:
                raise
            except Exception:
                self.adapter.logger.warning('Arcus WebSocket disconnected; waiting for fresh snapshots')
            self._ws_connected = False
            self._ready.clear()
            self._book_sequences.clear()
            if self._stopping or not self.adapter.config.enable_auto_reconnect:
                return
            await asyncio.sleep(self.adapter.config.retry_delay)
            try:
                self._socket = await self._open_socket()
                self._ws_connected = True
                self.generation += 1
                self._reset_startup_recoveries()
                self.last_received_monotonic=time.monotonic()
                for frame in self._subscriptions.values():
                    await self._socket.send(json.dumps(frame))
            except Exception:
                continue

    async def _subscribe(self, channel, callback, symbol=None):
        key = (channel, symbol)
        callbacks = self._callbacks.setdefault(key, [])
        if callback not in callbacks:
            callbacks.append(callback)
        if key in self._subscriptions:
            return
        frame = {'type':'subscribe','channel':channel}
        if channel in ('orders','userFills','positions','account'):
            if not self.adapter.address:
                raise ValueError('Arcus wallet address required for account subscriptions')
            frame.update(id=self.adapter.address, accountIndex=self.adapter.account_index)
        elif symbol:
            frame.update(id=symbol, nLevels=20)
        self._subscriptions[key] = frame
        if self._ws_connected:
            await self._socket.send(json.dumps(frame))

    async def subscribe_orderbook(self, symbol, callback):
        await self._subscribe('l2Orderbook', callback, symbol)

    async def subscribe_ticker(self, symbol, callback):
        self._callbacks.setdefault(('ticker',symbol), []).append(callback)
        await self._subscribe('markets', lambda *args: None)

    async def subscribe_orders(self, callback):
        await self._subscribe('orders', callback)
        # Merge userFills into the same order cache even when executor subscribes only orders.
        await self._subscribe('userFills', lambda *args: None)

    async def subscribe_order_fills(self, callback):
        await self._subscribe('userFills', callback)
        await self._subscribe('orders', lambda *args: None)

    async def subscribe_quantity_observations(self, callback):
        self._quantity_callbacks.append(callback)
        await self.subscribe_orders(lambda *args: None)

    def _quantity(self, order, startup_snapshot=False, startup_provisional=False):
        # Dedicated synchronous callbacks only copy/enqueue; valuation uncertainty does not hide size.
        for callback in self._quantity_callbacks:
            delivered=deepcopy(order)
            delivered.raw_data['_startup_snapshot']=startup_snapshot
            delivered.raw_data['_startup_provisional']=startup_provisional
            callback(delivered)

    def _recover_order(self, order_id, symbol, startup_snapshot=False, sequence=None, fill_identity=None):
        generation=self.generation
        try:minimum_sequence=int(sequence) if sequence is not None else None
        except (TypeError,ValueError,OverflowError):minimum_sequence=None
        async def recover():
            recovered=True
            try:
                if startup_snapshot:
                    order=await self.adapter._get_order_observation(order_id,symbol,minimum_sequence=minimum_sequence)
                else:
                    order=await self.adapter.get_order(order_id,symbol)
            except Exception:
                recovered=False
                order = self.adapter._orders.get(order_id)
            fresh=recovered and order is not None and bool(order.raw_data.get('_reported_order'))
            if order is not None:
                self._quantity(order,startup_snapshot=startup_snapshot and fresh and generation==self.generation)
            return fresh and generation==self.generation
        task = asyncio.create_task(recover())
        self._recovery_tasks.add(task)
        if startup_snapshot:
            self._startup_recoveries[task]=generation
            if fill_identity is not None:self._startup_retry_fills[fill_identity]=generation
        def completed(task):
            self._recovery_tasks.discard(task)
            # Failed/canceled snapshot recovery remains unresolved for startup readiness.
            if not task.cancelled() and task.exception() is None and task.result():
                self._startup_recoveries.pop(task,None)
                if (generation==self.generation and fill_identity is not None
                        and self._startup_retry_fills.get(fill_identity)==generation):
                    self._startup_retry_fills.pop(fill_identity,None)
        task.add_done_callback(completed)

    async def subscribe_positions(self, callback):
        await self._subscribe('positions', callback)

    async def subscribe_user_data(self, callback):
        await self._subscribe('account', callback)
        await self.subscribe_orders(lambda *args: None)
        await self.subscribe_positions(lambda *args: None)

    def lookup_cached_order(self, order_id):
        return self.adapter._orders.get(self.adapter._clients.get(str(order_id),str(order_id)))

    async def _emit(self, key, *args):
        for callback in self._callbacks.get(key,[]):
            result = callback(*args)
            if inspect.isawaitable(result):
                await result

    def _scoped_row(self, row, message):
        # An envelope authorizes the scope only after BOTH address and index match.
        return {'address': message['id'], 'accountIndex':message['accountIndex'], **row}

    async def handle_message(self, message):
        channel = message.get('channel'); symbol = message.get('id'); data = message.get('contents',{})
        private = channel in ('orders','userFills','positions','account')
        if private and (str(symbol).lower() != self.adapter.address or message.get('accountIndex') != self.adapter.account_index):
            return
        self.record_received()
        key = (channel,None if private or channel=='markets' else symbol)
        if message.get('type') == 'degraded':
            self._ready.discard(key)
            return
        if not isinstance(data,dict) or message.get('type') not in ('subscribed','channel_data'):
            return
        if message.get('type') == 'subscribed':
            self._ready.add(key)
        elif key not in self._ready:
            return
        if channel == 'l2Orderbook':
            sequence = int(data.get('lastSequenceId',0))
            if sequence <= self._book_sequences.get(symbol,-1):
                return
            self._book_sequences[symbol] = sequence
            await self._emit(key,symbol,self.adapter._parse_book(symbol,data))
        elif channel == 'markets':
            self.adapter._set_markets(data.get('markets',{}))
            for native, market in self.adapter._markets.items():
                await self._emit(('ticker',native),native,self.adapter._parse_ticker(market))
        elif channel == 'orders':
            rows=[]
            for field in ('orders','openOrders','recentClosedOrders'):
                values=data.get(field,[])
                rows.extend(values.values() if isinstance(values,dict) else values)
            if 'orderId' in data:
                rows.append(data)
            for row in rows:
                order=self.adapter._merge_order(self._scoped_row(row,message))
                if order:
                    self._quantity(order,startup_snapshot=message.get('type')=='subscribed')
                    await self._emit(('orders',None),order)
        elif channel == 'userFills':
            startup_snapshot=message.get('type')=='subscribed'
            rows=data.get('fills',[data] if 'orderId' in data else [])
            if isinstance(rows,dict): rows=rows.values()
            for row in rows:
                scoped=self._scoped_row(row,message)
                if not self.adapter._scope_matches(scoped): continue
                identity=(row.get('tradeId'),row.get('orderId'))
                if not identity[0] or identity in self.adapter._fill_ids: continue
                self.adapter._fill_ids.add(identity)
                order_id=str(row['orderId']); fills=self.adapter._fills.setdefault(order_id,{})
                fills[identity]=scoped
                from decimal import Decimal
                cached=self.adapter._orders.get(order_id)
                if cached is None:
                    self.adapter._uncertain.add(order_id)
                    self._recover_order(order_id, self.adapter._symbol_for_market(row.get('marketId')),startup_snapshot=startup_snapshot,sequence=row.get('sequenceNumber'),fill_identity=identity)
                    continue
                if int(row.get('sequenceNumber',0)) <= int(cached.raw_data.get('sequenceNumber',0)):
                    continue
                # A newer fill invalidates older proof even if recovery later returns the cache.
                cached.raw_data.pop('_reported_order',None)
                if 'originalSize' not in row or 'remainingSize' not in row:
                    self.adapter._uncertain.add(order_id)
                    self._recover_order(order_id, self.adapter._symbol_for_market(row.get('marketId')),startup_snapshot=startup_snapshot,sequence=row.get('sequenceNumber'),fill_identity=identity)
                    continue
                cumulative=Decimal(str(row['originalSize']))-Decimal(str(row['remainingSize']))
                delta=cumulative-cached.filled
                size=Decimal(str(row['size']))
                if delta != size or delta < 0 or (cached.filled and cached.average is None):
                    self.adapter._uncertain.add(order_id)
                    self._recover_order(order_id, self.adapter._symbol_for_market(row.get('marketId')),startup_snapshot=startup_snapshot,sequence=row.get('sequenceNumber'),fill_identity=identity)
                    if cumulative > cached.filled:
                        cached.filled=cumulative
                        cached.remaining=max(Decimal(0),cached.amount-cumulative)
                        cached.average=None
                        cached.status=OrderStatus.PENDING
                        cached.raw_data['_valuation_unknown']=True
                        cached.raw_data['filledSize']=str(cumulative)
                        cached.raw_data['remainingSize']=str(cached.remaining)
                        cached.raw_data.pop('avgFillPrice',None)
                        cached.raw_data.pop('averageFillPrice',None)
                        self._quantity(cached,startup_snapshot=startup_snapshot,startup_provisional=startup_snapshot)
                    continue
                cost=cached.cost+size*Decimal(str(row['price']))
                # Only a delta consistent with the confirmed baseline yields cumulative valuation.
                order=self.adapter._merge_order({**scoped,'filledSize':str(cumulative),
                    'avgFillPrice':str(cost/cumulative) if cumulative else '0'})
                if order:
                    self._quantity(order,startup_snapshot=startup_snapshot)
                    await self._emit(('orders',None),order)
                    await self._emit(('userFills',None),order)
        elif channel == 'positions':
            if data.get('isSnapshot'):
                self.adapter._positions.clear()
            rows=data.get('positions',{})
            for row in (rows.values() if isinstance(rows,dict) else rows):
                scoped=self._scoped_row(row,message)
                if not self.adapter._scope_matches(scoped): continue
                position=self.adapter._parse_position(scoped)
                previous=self.adapter._positions.get(position.symbol)
                if previous and int(row.get('sequenceNumber',0)) < int(previous.raw_data.get('sequenceNumber',0)): continue
                self.adapter._positions[position.symbol]=position
                await self._emit(key,position)
        elif channel == 'account':
            await self._emit(key,data)

    async def unsubscribe(self,symbol=None):
        for key,frame in list(self._subscriptions.items()):
            if symbol is None or key[1]==symbol:
                if self._ws_connected:
                    await self._socket.send(json.dumps({**frame,'type':'unsubscribe'}))
                self._subscriptions.pop(key,None); self._callbacks.pop(key,None); self._ready.discard(key)
