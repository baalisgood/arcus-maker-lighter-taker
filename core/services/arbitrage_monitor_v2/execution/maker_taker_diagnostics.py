"""Durable diagnostic enrichment only; original economic event fields are never rewritten."""
import json
from decimal import Decimal

D=Decimal


class CloseDiagnostics:
    KEY='diagnostics:event_cursor'

    def __init__(self,journal):
        self.journal=journal
        row=journal.db.execute('SELECT value FROM meta WHERE key=?',(self.KEY,)).fetchone()
        self.cursor=int(row[0]) if row else 0
        self.expired=set()

    def event(self,at,symbol,kind,payload):
        self.journal.db.execute('INSERT INTO events(at,symbol,kind,payload) VALUES(?,?,?,?)',
            (at,symbol,kind,json.dumps(payload,default=str,sort_keys=True)))

    def write(self,rows,cursor):
        if cursor<=self.cursor:return
        with self.journal.transaction():
            for row,facts in rows:
                if row['id']<=self.cursor:continue
                for fact in facts or ():
                    kind=fact['kind'];p=fact['payload']
                    if kind=='close_pending':
                        current=json.loads(row['payload'])
                        intent=self.journal.db.execute('SELECT value FROM meta WHERE key=?',('close_quote:'+p['client'],)).fetchone()
                        mode=json.loads(intent[0])['mode'] if intent else None
                        current.update(close_mode=('timeout' if mode=='ema' else mode) if mode else None,
                            roundtrip_status='pending',roundtrip_pnl_usd=None,roundtrip_pnl_pct=None,
                            roundtrip_confirmed_qty='0',roundtrip_priced_qty='0',roundtrip_restored_qty='0',
                            roundtrip_pnl_usd_known='0',roundtrip_entry_notional_known='0')
                        self._update(row['id'],current)
                    elif kind=='close_fill_enriched':
                        old=self.journal.db.execute('SELECT payload FROM events WHERE id=?',(p['maker_event_id'],)).fetchone()
                        if old is None:raise ValueError('Missing close maker event for diagnostic enrichment')
                        current=json.loads(old[0])
                        confirmed=D(current.get('roundtrip_confirmed_qty','0'))+D(p['confirmed_qty'])
                        priced=D(current.get('roundtrip_priced_qty','0'))+D(p['priced_qty'])
                        restored=D(current.get('roundtrip_restored_qty','0'))+D(p['restored_qty'])
                        gross=D(current.get('roundtrip_pnl_usd_known','0'))+D(p['gross_pnl_usd_known'])
                        notional=D(current.get('roundtrip_entry_notional_known','0'))+D(p['entry_notional_known'])
                        known=confirmed>0 and confirmed==priced
                        current.update(roundtrip_confirmed_qty=str(confirmed),roundtrip_priced_qty=str(priced),
                            roundtrip_restored_qty=str(restored),roundtrip_pnl_usd_known=str(gross),
                            roundtrip_entry_notional_known=str(notional),
                            roundtrip_pnl_usd=str(gross) if known else None,
                            roundtrip_pnl_pct=str(100*gross/notional) if known and notional else None,
                            roundtrip_status=('restored' if not confirmed else ('confirmed' if known else 'unknown'))
                                if p['fully_resolved'] else ('partial' if confirmed else 'pending'))
                        p={**p,'close_mode':current.get('close_mode')}
                        self._update(p['maker_event_id'],current)
                        self.event(row['at'],row['symbol'],kind,p)
                    else:self.event(row['at'],row['symbol'],kind,p)
            self.journal.db.execute('INSERT OR REPLACE INTO meta VALUES(?,?)',(self.KEY,str(cursor)))
        self.cursor=cursor

    def _update(self,event_id,payload):
        self.journal.db.execute('UPDATE events SET payload=? WHERE id=?',
            (json.dumps(payload,default=str,sort_keys=True),event_id))

    def timeouts(self,states,configs,now,references):
        remaining={}
        for symbol,s in states.items():
            if symbol not in configs:continue
            for lot in s['lots']:
                key=lot['origin'];row=remaining.setdefault(key,[symbol,lot['at'],D(0),lot.get('attributed',True)])
                row[2]+=lot['hi']-lot['lo']
        self.expired.intersection_update(remaining)
        for origin,(symbol,at,qty,attributed) in remaining.items():
            if origin in self.expired or now-at<configs[symbol].close_floor_timeout:continue
            key='close_floor_timeout:'+str(origin)
            with self.journal.transaction():
                if self.journal.db.execute('SELECT 1 FROM meta WHERE key=?',(key,)).fetchone() is None:
                    self.event(now,symbol,'close_floor_timeout',dict(origin=origin,remaining_qty=qty,
                        open_at=at if attributed else None,observed_at=None if attributed else at,
                        age_source='opening_fill' if attributed else 'observation',
                        hold_seconds=max(0,now-at) if attributed else None,
                        waiting_seconds=max(0,now-at),estimated_pnl_pct=references.get(origin)))
                    self.journal.db.execute('INSERT INTO meta VALUES(?,?)',(key,str(now)))
            self.expired.add(origin)
