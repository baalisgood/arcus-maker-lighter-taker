"""Durable maker fills and hedge obligations. Decimal amounts never use SQLite floats."""
import json
import sqlite3
import time
import os
import stat
import uuid
from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path

D = Decimal
TERMINAL = {'filled', 'canceled', 'rejected', 'expired', 'deferred'}

SCHEMA_V1 = '''
          CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS sequence(id INTEGER PRIMARY KEY AUTOINCREMENT);
          CREATE TABLE IF NOT EXISTS orders(
            client TEXT PRIMARY KEY,venue TEXT NOT NULL,symbol TEXT NOT NULL,side INTEGER NOT NULL,
            purpose TEXT NOT NULL,qty TEXT NOT NULL,price TEXT NOT NULL,level INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'prepared',exchange_id TEXT,filled TEXT NOT NULL DEFAULT '0',
            created REAL NOT NULL,anchor TEXT NOT NULL,reserved TEXT NOT NULL,preclose TEXT NOT NULL DEFAULT '0',
            cost TEXT,fee TEXT);
          CREATE TABLE IF NOT EXISTS obligations(
            id INTEGER PRIMARY KEY AUTOINCREMENT,maker TEXT NOT NULL REFERENCES orders(client),
            qty TEXT NOT NULL,hedged TEXT NOT NULL DEFAULT '0',unwound TEXT NOT NULL DEFAULT '0',
            received REAL NOT NULL,reference TEXT,average TEXT,fee TEXT,trade_id TEXT,boot TEXT);
          CREATE TABLE IF NOT EXISTS attempts(
            client TEXT PRIMARY KEY,obligation INTEGER NOT NULL REFERENCES obligations(id),
            venue TEXT NOT NULL,qty TEXT NOT NULL,filled TEXT NOT NULL DEFAULT '0',
            status TEXT NOT NULL DEFAULT 'prepared',exchange_id TEXT,average TEXT,fee TEXT,
            created REAL NOT NULL,UNIQUE(obligation,client));
          CREATE TABLE IF NOT EXISTS events(
            id INTEGER PRIMARY KEY AUTOINCREMENT,at REAL NOT NULL,symbol TEXT NOT NULL,
            kind TEXT NOT NULL,payload TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS adjustments(
            id INTEGER PRIMARY KEY AUTOINCREMENT,symbol TEXT NOT NULL,arcus TEXT NOT NULL,lighter TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS writes(
            id INTEGER PRIMARY KEY AUTOINCREMENT,client TEXT NOT NULL,signed_hash TEXT,
            nonce TEXT NOT NULL,key_index INTEGER,kind TEXT NOT NULL,at REAL NOT NULL);
          CREATE TABLE IF NOT EXISTS executions(
            venue TEXT NOT NULL,identity TEXT NOT NULL,symbol TEXT NOT NULL,client TEXT NOT NULL,
            qty TEXT NOT NULL,price TEXT NOT NULL,fee TEXT,currency TEXT,role TEXT,at REAL NOT NULL,
            kind TEXT NOT NULL,PRIMARY KEY(venue,identity));
          CREATE TABLE IF NOT EXISTS funding(
            venue TEXT NOT NULL,identity TEXT NOT NULL,symbol TEXT NOT NULL,cost TEXT NOT NULL,
            currency TEXT NOT NULL,at REAL NOT NULL,PRIMARY KEY(venue,identity));
          CREATE TABLE IF NOT EXISTS coverage(
            venue TEXT NOT NULL,kind TEXT NOT NULL,start REAL NOT NULL,end REAL NOT NULL,
            complete INTEGER NOT NULL,reason TEXT NOT NULL,PRIMARY KEY(venue,kind));
          CREATE TABLE IF NOT EXISTS raw_finance(
            venue TEXT NOT NULL,kind TEXT NOT NULL,identity TEXT NOT NULL,payload TEXT NOT NULL,
            PRIMARY KEY(venue,kind,identity));
'''


ALLOCATIONS = """
          CREATE TABLE IF NOT EXISTS attempt_allocations(
            client TEXT NOT NULL REFERENCES attempts(client),
            obligation INTEGER NOT NULL REFERENCES obligations(id),
            qty TEXT NOT NULL,filled TEXT NOT NULL DEFAULT '0',
            PRIMARY KEY(client,obligation));
"""
SCHEMA_V2 = SCHEMA_V1 + ALLOCATIONS
ALLOCATION_STATE = 'ALTER TABLE attempt_allocations ADD COLUMN deferred INTEGER NOT NULL DEFAULT 0 CHECK(deferred IN (0,1));'
ALLOCATIONS_V3 = ALLOCATIONS.replace('            PRIMARY KEY(client,obligation)', '            deferred INTEGER NOT NULL DEFAULT 0 CHECK(deferred IN (0,1)),\n            PRIMARY KEY(client,obligation)')
ARCHIVE = SCHEMA_V2[SCHEMA_V2.index('          CREATE TABLE IF NOT EXISTS orders('):SCHEMA_V2.index('          CREATE TABLE IF NOT EXISTS events(')] + ALLOCATIONS_V3
for name in ('attempt_allocations','obligations','attempts','orders'):
    ARCHIVE = ARCHIVE.replace(name, 'archive_'+name)
DERIVED = """
CREATE TABLE current_inventory(symbol TEXT PRIMARY KEY,arcus TEXT NOT NULL,lighter TEXT NOT NULL);
CREATE TABLE pending_obligations(id INTEGER PRIMARY KEY REFERENCES obligations(id));
CREATE INDEX orders_status_symbol ON orders(status,symbol);
CREATE INDEX orders_exchange_id ON orders(exchange_id);
CREATE INDEX obligations_maker ON obligations(maker);
CREATE INDEX retry_allocations ON attempt_allocations(obligation,client) WHERE deferred=0;
CREATE INDEX archive_retry_allocations ON archive_attempt_allocations(obligation,client) WHERE deferred=0;
CREATE INDEX allocations_obligation_client ON attempt_allocations(obligation,client);
CREATE INDEX attempts_obligation ON attempts(obligation);
CREATE INDEX attempts_venue ON attempts(venue);
CREATE INDEX archive_attempts_obligation ON archive_attempts(obligation);
CREATE INDEX archive_attempts_venue ON archive_attempts(venue);
CREATE INDEX attempts_exchange_id ON attempts(exchange_id);
CREATE INDEX executions_venue_client ON executions(venue,client);
CREATE INDEX active_orders_symbol ON orders(symbol,created,client) WHERE status NOT IN ('filled','canceled','rejected','expired','deferred');
CREATE INDEX active_attempts ON attempts(created,client) WHERE status NOT IN ('filled','canceled','rejected','expired','deferred');
CREATE INDEX zero_fill_archive_candidates ON orders(created,client) WHERE status IN ('filled','canceled','rejected','expired','deferred') AND filled='0';
CREATE INDEX orders_created ON orders(created);
CREATE INDEX archive_orders_created ON archive_orders(created);
CREATE INDEX archive_orders_exchange_id ON archive_orders(exchange_id);
CREATE INDEX archive_obligations_maker ON archive_obligations(maker);
CREATE INDEX archive_allocations_obligation_client ON archive_attempt_allocations(obligation,client);
CREATE INDEX archive_attempts_exchange_id ON archive_attempts(exchange_id);
"""
VIEWS = ''.join('CREATE VIEW all_'+name+' AS SELECT * FROM '+name+' UNION ALL SELECT * FROM archive_'+name+';'
                for name in ('orders','obligations','attempts','attempt_allocations'))
SCHEMA = SCHEMA_V2 + ALLOCATION_STATE + ARCHIVE + DERIVED + VIEWS
ACTIVE = "status NOT IN ('filled','canceled','rejected','expired','deferred')"
LIFECYCLE = TERMINAL | {'prepared','submitted','pending','open','unknown'}


class Journal:
    VERSION = 3

    def __init__(self, path, identity='paper'):
        self.path = Path(path)
        existed=self.path.exists()
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        parent = self.path.parent.lstat()
        if stat.S_ISLNK(parent.st_mode) or parent.st_uid != os.getuid() or parent.st_mode & 0o022:
            raise ValueError('Journal directory must be owner-controlled and not writable by others')
        for candidate in (self.path, Path(str(self.path)+'-wal'),Path(str(self.path)+'-shm')):
            if candidate.is_symlink():
                raise ValueError('Journal symlinks are forbidden')
            if candidate.exists():
                info = candidate.stat()
                if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid():
                    raise ValueError('Journal must be an owner-controlled regular file')
                os.chmod(candidate,0o600)
        fd = os.open(self.path, os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW|(0 if existed else os.O_EXCL),0o600)
        os.close(fd)
        self._archive_cursor = 0
        self._archive_zero_cursor = (0.0,'')
        self.boot = uuid.uuid4().hex
        self.modeled = identity.startswith('paper')
        self.db = sqlite3.connect(str(path), isolation_level=None)
        self.db.row_factory = sqlite3.Row
        if existed:
            try:self._validate_existing(identity)
            except BaseException:
                self.db.close()
                raise
            self.db.execute('PRAGMA foreign_keys=ON')
            self.db.execute('PRAGMA journal_mode=WAL')
            self.db.execute('PRAGMA synchronous=FULL')
            return
        self.db.execute('PRAGMA foreign_keys=ON')
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        for suffix in ('','-wal','-shm'):
            candidate = Path(str(self.path)+suffix)
            if candidate.exists():
                os.chmod(candidate,0o600)
        version = self.db.execute('PRAGMA user_version').fetchone()[0]
        existing = self.db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        if version not in (0, self.VERSION) or (version == 0 and existing):
            self.db.close()
            raise ValueError('Unknown journal schema; preserve journal and resolve before starting')
        self.db.executescript(SCHEMA)
        self.db.execute(f'PRAGMA user_version={self.VERSION}')
        with self.transaction():
            row = self.db.execute("SELECT value FROM meta WHERE key='identity'").fetchone()
            if row and row[0] != identity:
                raise ValueError('Journal account/mode identity mismatch')
            self.db.execute("INSERT OR IGNORE INTO meta VALUES('identity',?)", (identity,))

    def _validate_existing(self,identity):
        version = self.db.execute('PRAGMA user_version').fetchone()[0]
        if version not in (1,2,self.VERSION):
            raise ValueError('Unknown journal schema version; journal preserved')
        if [r[0] for r in self.db.execute('PRAGMA quick_check')]!=['ok']:
            raise ValueError('Journal integrity check failed')
        expected=sqlite3.connect(':memory:')
        try:
            expected.executescript({1:SCHEMA_V1,2:SCHEMA_V2,3:SCHEMA}[version])
            query="SELECT name,type,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY name"
            if [tuple(r) for r in self.db.execute(query)]!=expected.execute(query).fetchall():
                raise ValueError('Journal schema is incomplete or changed; no automatic repair')
        finally:expected.close()
        if self.db.execute('PRAGMA foreign_key_check').fetchall():
            raise ValueError('Journal has broken obligation/order references')
        row=self.db.execute("SELECT value FROM meta WHERE key='identity'").fetchone()
        if row is None or row['value']!=identity:
            raise ValueError('Missing/mismatched journal account identity; no automatic repair')
        try:
            inventory,pending=self._validate_amounts(version)
        except (ArithmeticError,KeyError,TypeError) as exc:
            raise ValueError('Invalid journal amounts; journal preserved') from exc
        if version==3:
            actual={r['symbol']:(D(r['arcus']),D(r['lighter'])) for r in self.db.execute('SELECT * FROM current_inventory')}
            if actual!=inventory or {r[0] for r in self.db.execute('SELECT id FROM pending_obligations')}!=pending:
                raise ValueError('Journal current state disagrees with history; no automatic repair')
        else:
            with self.transaction():
                if version==1:
                    self.db.execute(ALLOCATIONS.strip())
                    self.db.execute('INSERT INTO attempt_allocations(client,obligation,qty,filled) SELECT client,obligation,qty,filled FROM attempts')
                self.db.execute(ALLOCATION_STATE)
                self.db.execute("UPDATE attempt_allocations SET deferred=1 WHERE client IN (SELECT client FROM attempts WHERE status='deferred')")
                for statement in (ARCHIVE+DERIVED+VIEWS).split(';'):
                    if statement.strip():self.db.execute(statement)
                self.db.executemany('INSERT INTO current_inventory VALUES(?,?,?)',
                                    ((symbol,str(a),str(l)) for symbol,(a,l) in inventory.items()))
                self.db.executemany('INSERT INTO pending_obligations VALUES(?)',((oid,) for oid in pending))
                self.db.execute(f'PRAGMA user_version={self.VERSION}')

    def _validate_amounts(self,version):
        prefix='all_' if version==3 else ''
        orders={r['client']:dict(r) for r in self.db.execute('SELECT * FROM '+prefix+'orders')}
        obligations={r['id']:dict(r) for r in self.db.execute('SELECT * FROM '+prefix+'obligations')}
        attempts={r['client']:dict(r) for r in self.db.execute('SELECT * FROM '+prefix+'attempts')}
        if version==3:
            for table in ('archive_orders','archive_attempts'):
                if self.db.execute('SELECT 1 FROM '+table+' WHERE '+ACTIVE+' LIMIT 1').fetchone():
                    raise ValueError('Archived order/attempt is not terminal')
            for table,key in (('orders','client'),('obligations','id'),('attempts','client')):
                if self.db.execute('SELECT 1 FROM '+table+' c JOIN archive_'+table+' a USING('+key+') LIMIT 1').fetchone():
                    raise ValueError('Duplicate current/archive identity')
        self._validate_sequences(orders,obligations,attempts)
        totals={}; fills={}; maker_fills={}; inventory={}; pending=set()
        for row in orders.values():
            if (row['side'] not in (-1,1) or row['purpose'] not in {'open','close','legacy','reconcile'} or
                    row['venue']!=('reconcile' if row['purpose']=='reconcile' else 'arcus') or row['status'] not in LIFECYCLE):
                raise ValueError('Invalid journal maker side/venue/purpose/lifecycle')
            qty,filled=D(row['qty']),D(row['filled'])
            if not qty.is_finite() or not filled.is_finite() or not 0<=filled<=qty:
                raise ValueError('Invalid journal maker amounts')
            if row['status']=='filled' and filled!=qty:
                raise ValueError('Filled maker watermark differs from quantity')
            for key in ('price','anchor','reserved','preclose','cost','fee'):
                if row[key] is not None and not D(row[key]).is_finite():raise ValueError('Invalid journal maker economics')
        allocations=(self.db.execute('SELECT * FROM '+prefix+'attempt_allocations') if version!=1 else
                     ({'client':r['client'],'obligation':r['obligation'],'qty':r['qty'],'filled':r['filled']} for r in attempts.values()))
        for a in allocations:
            t=attempts[a['client']];o=obligations[a['obligation']];m=orders[o['maker']]
            if version==3 and a['deferred']!=int(t['status']=='deferred'):
                raise ValueError('Allocation deferred state disagrees with attempt lifecycle')
            qty,filled=D(a['qty']),D(a['filled'])
            if not qty.is_finite() or not filled.is_finite() or not 0<=filled<=qty or qty<=0:
                raise ValueError('Invalid journal allocation amounts')
            total=totals.setdefault(a['client'],[D(0),D(0),set(),a['obligation']])
            total[0]+=qty;total[1]+=filled;total[2].add((m['symbol'],m['side'],m['purpose']))
            total[3]=min(total[3],a['obligation'])
            fills[a['obligation'],t['venue']]=fills.get((a['obligation'],t['venue']),D(0))+filled
        for row in attempts.values():
            if row['venue'] not in {'arcus','lighter'} or row['status'] not in LIFECYCLE:
                raise ValueError('Invalid journal attempt venue/lifecycle')
            qty,filled=D(row['qty']),D(row['filled']);total=totals.get(row['client'])
            if (not qty.is_finite() or not filled.is_finite() or not 0<=filled<=qty or qty<=0 or
                    total is None or total[:2]!=[qty,filled] or len(total[2])!=1 or total[3]!=row['obligation']):
                raise ValueError('Invalid journal attempt allocations')
            if (row['status']=='filled' and filled!=qty) or (row['status']=='deferred' and filled):
                raise ValueError('Terminal attempt watermark differs from lifecycle')
            for key in ('average','fee'):
                if row[key] is not None and not D(row[key]).is_finite():raise ValueError('Invalid journal attempt economics')
        for row in obligations.values():
            qty,hedged,unwound=(D(row[k]) for k in ('qty','hedged','unwound'))
            if any(not v.is_finite() or v<0 for v in (qty,hedged,unwound)) or hedged+unwound>qty:
                raise ValueError('Invalid journal obligation amounts')
            if fills.get((row['id'],'lighter'),D(0))!=hedged or fills.get((row['id'],'arcus'),D(0))!=unwound:
                raise ValueError('Journal allocation fills disagree with obligation watermark')
            for key in ('reference','average','fee'):
                if row[key] is not None and not D(row[key]).is_finite():raise ValueError('Invalid journal obligation economics')
            m=orders[row['maker']];symbol=m['symbol'];side=m['side']
            a,l=inventory.get(symbol,(D(0),D(0)))
            inventory[symbol]=(a+side*(qty-unwound),l-side*hedged)
            maker_fills[row['maker']]=maker_fills.get(row['maker'],D(0))+qty
            if qty>hedged+unwound:
                if version==3 and self.db.execute('SELECT 1 FROM archive_obligations WHERE id=?',(row['id'],)).fetchone():
                    raise ValueError('Archived obligation remains pending')
                pending.add(row['id'])
        for row in orders.values():
            if row['purpose']!='legacy' and maker_fills.get(row['client'],D(0))!=D(row['filled']):
                raise ValueError('Maker watermark disagrees with obligations')
        for row in self.db.execute('SELECT * FROM adjustments'):
            a,l=inventory.get(row['symbol'],(D(0),D(0)));da,dl=D(row['arcus']),D(row['lighter'])
            if not da.is_finite() or not dl.is_finite():raise ValueError('Invalid inventory adjustment')
            inventory[row['symbol']]=(a+da,l+dl)
        return inventory,pending


    def _validate_sequences(self,orders,obligations,attempts):
        used=0
        for collection in (orders,attempts):
            for row in collection.values():
                if row.get('purpose')=='legacy':continue
                client=row['client'];number=client[5:] if client.startswith('amlt-') else client
                if number.isdecimal():used=max(used,int(number))
        current=self.db.execute('SELECT MAX(id) FROM sequence').fetchone()[0] or 0
        for name,required in (('sequence',max(used,current)),('obligations',max(obligations,default=0))):
            rows=self.db.execute('SELECT seq FROM sqlite_sequence WHERE name=?',(name,)).fetchall()
            if (len(rows)>1 or (rows and (type(rows[0][0]) is not int or rows[0][0]<required)) or (not rows and required)):
                raise ValueError('Journal identity sequence high-water mark regressed')


    @contextmanager
    def transaction(self):
        self.db.execute('BEGIN IMMEDIATE')
        try:
            yield
            self.db.execute('COMMIT')
        except BaseException:
            self.db.execute('ROLLBACK')
            raise

    def close(self):
        self.db.close()

    def manual_intervention(self):
        row=self.db.execute("SELECT value FROM meta WHERE key='manual_intervention'").fetchone()
        return json.loads(row[0]) if row else None

    def retry_reset_clients(self,obligation):
        row=self.db.execute('SELECT value FROM meta WHERE key=?',('retry_reset:'+str(obligation),)).fetchone()
        return set(json.loads(row[0])) if row else set()

    def operator_release_retry_exhausted(self,snapshot,obligation_ids):
        """Explicit operator release of a retry_exhausted latch: prior zero-fill attempts stop counting."""
        with self.transaction():
            for oid in obligation_ids:
                spent=sorted(self.retry_reset_clients(oid)|{a['client'] for a in self.attempts(oid,include_deferred=False)})
                self.db.execute('INSERT OR REPLACE INTO meta VALUES(?,?)',('retry_reset:'+str(oid),json.dumps(spent)))
            self.db.execute("DELETE FROM meta WHERE key='manual_intervention'")
            self.event(snapshot.get('failure_symbol') or '*','operator_release_retry_exhausted',
                       obligations=list(obligation_ids),released_snapshot=snapshot)

    def latch_manual_intervention(self,snapshot):
        with self.transaction():
            existing=self.manual_intervention()
            if existing is not None:return existing
            self.db.execute('INSERT INTO meta VALUES(?,?)',('manual_intervention',json.dumps(snapshot,sort_keys=True)))
            self.event(snapshot['failure_symbol'],'manual_intervention',**snapshot)
            self.event(snapshot['failure_symbol'],'halt',state='EMERGENCY',**snapshot)
        return snapshot

    def audit_write(self, client, *, signed_hash=None, nonce, api_key_index=None, tx_type=None, is_cancel=False):
        # Only whitelisted identities, hash and nonce; no signatures, keys, tokens or signed payloads.
        with self.transaction():
            known = self.db.execute('SELECT client FROM orders WHERE client=? UNION SELECT client FROM attempts WHERE client=?',
                                    (client,client)).fetchone()
            if known is None:
                raise ValueError('Write lacks durable intent')
            self.db.execute('INSERT INTO writes(client,signed_hash,nonce,key_index,kind,at) VALUES(?,?,?,?,?,?)',
                            (client,signed_hash,str(nonce),api_key_index,'cancel' if is_cancel else str(tx_type),time.time()))

    def record_execution(self,venue,identity,symbol,client,qty,price,fee,currency,role,at,kind):
        qty,price=D(str(qty)),D(str(price))
        fee=D(str(fee)) if fee is not None else None
        if not all(v.is_finite() for v in (qty,price)) or min(qty,price)<=0 or (fee is not None and not fee.is_finite()):
            raise ValueError('Invalid actual execution economics')
        old=self.db.execute('SELECT * FROM executions WHERE venue=? AND identity=?',(venue,str(identity))).fetchone()
        if old and (old['client']!=str(client) or old['symbol']!=symbol or D(old['qty'])!=qty
                    or D(old['price'])!=price or old['kind']!=kind or old['role']!=role):
            raise ValueError('Execution identity has conflicting authoritative payload')
        self.db.execute('INSERT OR IGNORE INTO executions VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                        (venue,str(identity),symbol,str(client),str(qty),str(price),str(fee) if fee is not None else None,
                         currency,role,float(at),kind))
        if fee is not None:
            if old and old['fee'] is not None and D(old['fee'])!=fee:
                raise ValueError('Execution fee revision requires reconciliation')
            self.db.execute('UPDATE executions SET fee=?,currency=? WHERE venue=? AND identity=?',
                            (str(fee),currency,venue,str(identity)))

    def record_funding(self,venue,identity,symbol,cost,currency,at):
        cost=D(str(cost))
        if not cost.is_finite() or currency!='USDC':
            raise ValueError('Unsupported actual funding amount/currency')
        old=self.db.execute('SELECT * FROM funding WHERE venue=? AND identity=?',(venue,str(identity))).fetchone()
        if old and (D(old['cost'])!=cost or old['symbol']!=symbol or old['currency']!=currency or old['at']!=float(at)):
            raise ValueError('Funding identity has conflicting authoritative payload; no double count')
        self.db.execute('INSERT OR IGNORE INTO funding VALUES(?,?,?,?,?,?)',
                        (venue,str(identity),symbol,str(cost),currency,float(at)))

    def coverage(self,venue,kind,start,end,complete,reason=''):
        self.db.execute('INSERT OR REPLACE INTO coverage VALUES(?,?,?,?,?,?)',
                        (venue,kind,float(start),float(end),int(complete),reason))

    def raw_finance(self,venue,kind,identity,fields):
        payload=json.dumps(fields,sort_keys=True,default=str)
        old=self.db.execute('SELECT payload FROM raw_finance WHERE venue=? AND kind=? AND identity=?',
                            (venue,kind,str(identity))).fetchone()
        if old and old['payload']!=payload:
            previous=json.loads(old['payload'])
            if any(previous.get(k) is not None and previous[k]!=v for k,v in fields.items()):
                raise ValueError('Raw finance identity has conflicting authoritative payload')
        self.db.execute('INSERT OR REPLACE INTO raw_finance VALUES(?,?,?,?)',(venue,kind,str(identity),payload))

    def _id(self, venue):
        value = self.db.execute('INSERT INTO sequence DEFAULT VALUES').lastrowid
        if value >= 2 ** 48:
            raise ValueError('Lighter client order ID space exhausted')
        client=str(value) if venue == 'lighter' else f'amlt-{value}'
        if self.db.execute('SELECT 1 FROM all_orders WHERE client IN (?,?) UNION ALL SELECT 1 FROM all_attempts WHERE client IN (?,?) LIMIT 1',
                           (str(value),f'amlt-{value}',str(value),f'amlt-{value}')).fetchone():
            raise ValueError('Journal identity would reuse current/archive history')
        return client

    def event(self, symbol, kind, **payload):
        self.db.execute('INSERT INTO events(at,symbol,kind,payload) VALUES(?,?,?,?)',
                        (time.time(), symbol, kind, json.dumps(payload, default=str, sort_keys=True)))

    def prepare_maker(self, symbol, side, purpose, qty, price, level, anchor, reserved, *, close_quote=None):
        with self.transaction():
            client = self._id('arcus')
            self.db.execute('INSERT INTO orders(client,venue,symbol,side,purpose,qty,price,level,created,anchor,reserved) '
                            'VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                            (client, 'arcus', symbol, side, purpose, str(qty), str(price), level,
                             time.time(), str(anchor), str(reserved)))
            if purpose=='close':
                arcus,lighter=self.inventory(symbol)
                paired=min(abs(arcus),abs(lighter)) if arcus*lighter<0 else D(0)
                self.db.execute('UPDATE orders SET preclose=? WHERE client=?',(str(paired),client))
            self.event(symbol, 'maker_prepared', client=client, purpose=purpose, qty=qty, price=price)
            if close_quote is not None:
                payload={**close_quote,'client':client,'side':side,'qty':qty,'final_price':price}
                self.event(symbol,'close_quote',**payload)
                self.db.execute('INSERT INTO meta VALUES(?,?)',('close_quote:'+client,json.dumps({'mode':payload['mode']})))
        return client

    def maker(self, client):
        row = self.db.execute('SELECT * FROM all_orders WHERE client=? OR exchange_id=?', (str(client), str(client))).fetchone()
        if row is None:
            raise ValueError('Unknown maker identity')
        return dict(row)

    def makers(self, symbol=None, active=False):
        query='SELECT * FROM '+('orders INDEXED BY active_orders_symbol' if active else 'all_orders')
        clauses=[];args=[]
        if active:clauses.append(ACTIVE)
        if symbol is not None:clauses.append('symbol=?');args.append(symbol)
        if clauses:query+=' WHERE '+' AND '.join(clauses)
        return [dict(r) for r in self.db.execute(query+' ORDER BY created,client',args)]

    def has_history(self,symbol):
        return self.db.execute('SELECT 1 FROM all_obligations o JOIN all_orders m ON m.client=o.maker WHERE m.symbol=? LIMIT 1',(symbol,)).fetchone() is not None

    def known_exchange_ids(self):
        return {r[0] for r in self.db.execute('SELECT exchange_id FROM all_orders WHERE exchange_id IS NOT NULL')}

    def _inventory_delta(self,symbol,arcus=D(0),lighter=D(0)):
        a,l=self.inventory(symbol)
        self.db.execute('INSERT OR REPLACE INTO current_inventory VALUES(?,?,?)',(symbol,str(a+arcus),str(l+lighter)))

    def _check_obligation_identity(self,oid):
        if self.db.execute('SELECT 1 FROM archive_obligations WHERE id=?',(oid,)).fetchone():
            raise ValueError('Obligation identity would reuse archived history')

    def _pending(self,oid):
        row=self.obligation(oid)
        if D(row['qty'])>D(row['hedged'])+D(row['unwound']):
            self.db.execute('INSERT OR IGNORE INTO pending_obligations VALUES(?)',(oid,))
        else:self.db.execute('DELETE FROM pending_obligations WHERE id=?',(oid,))

    def observe_maker(self, client, cumulative, status, exchange_id=None, *, reference=None,
                      average=None, fee=None, trade_id=None, received=None):
        """Commit watermark and new exposure in ONE transaction, even with unknown valuation."""
        cumulative = D(str(cumulative))
        with self.transaction():
            row = self.maker(client)
            self._restore(row['client'])
            if not cumulative.is_finite() or not 0 <= cumulative <= D(row['qty']):
                raise ValueError('Invalid maker cumulative amount')
            old = D(row['filled'])
            if cumulative < old:
                return None
            if row['exchange_id'] and exchange_id and row['exchange_id'] != str(exchange_id):
                raise ValueError('Maker identity changed')
            # A late nonterminal snapshot cannot reopen a terminal order/reservation.
            state = row['status'] if row['status'] in TERMINAL else status
            cost=cumulative*D(str(average)) if average is not None else None
            old_cost=D(row['cost']) if row['cost'] is not None else (D(0) if old==0 else None)
            delta_average=(cost-old_cost)/(cumulative-old) if cumulative>old and cost is not None and old_cost is not None else None
            old_fee=D(row['fee']) if row['fee'] is not None else (D(0) if old==0 else None)
            delta_fee=D(str(fee))-old_fee if fee is not None and old_fee is not None else None
            stored_cost=str(cost) if cost is not None else (row['cost'] if cumulative==old else None)
            stored_fee=str(fee) if fee is not None else (row['fee'] if cumulative==old else None)
            self.db.execute('UPDATE orders SET filled=?,status=?,exchange_id=COALESCE(?,exchange_id),cost=?,fee=? WHERE client=?',
                            (str(cumulative), state, str(exchange_id) if exchange_id else None,
                             stored_cost,stored_fee,row['client']))
            if cumulative == old:
                return None
            qty = cumulative-old
            oid = self.db.execute('INSERT INTO obligations(maker,qty,received,reference,average,fee,trade_id,boot) VALUES(?,?,?,?,?,?,?,?)',
                                 (row['client'], str(qty), time.monotonic() if received is None else received,
                                  str(reference) if reference is not None else None,
                                  str(delta_average) if delta_average is not None else None,
                                  str(delta_fee) if delta_fee is not None else None, trade_id,self.boot)).lastrowid
            self._check_obligation_identity(oid)
            self._inventory_delta(row['symbol'],row['side']*qty)
            self._pending(oid)
            self.event(row['symbol'], 'maker_fill', obligation=oid, client=row['client'], qty=qty,
                       side=row['side'], purpose=row['purpose'], average=delta_average, fee=delta_fee,
                       reference=reference, modeled=self.modeled)
            return oid

    def obligations(self, symbol=None, pending=True):
        source='pending_obligations p CROSS JOIN obligations o ON o.id=p.id' if pending else 'all_obligations o'
        query='SELECT o.*,m.symbol,m.side,m.purpose FROM '+source+' JOIN '+('orders' if pending else 'all_orders')+' m ON m.client=o.maker'
        args=()
        if symbol is not None:query+=' WHERE m.symbol=?';args=(symbol,)
        return [dict(r) for r in self.db.execute(query+' ORDER BY o.id',args)]

    def obligation(self,oid):
        row=self.db.execute('SELECT o.*,m.symbol,m.side,m.purpose FROM all_obligations o JOIN all_orders m ON m.client=o.maker WHERE o.id=?',(oid,)).fetchone()
        if row is None:raise ValueError('Unknown obligation identity')
        return dict(row)

    def allocations(self, client):
        return [dict(r) for r in self.db.execute(
            'SELECT a.*,o.reference,o.received,o.boot,m.side,m.symbol,m.purpose FROM all_attempt_allocations a '
            'JOIN all_obligations o ON o.id=a.obligation JOIN all_orders m ON m.client=o.maker '
            'WHERE a.client=? ORDER BY a.obligation',(client,))]

    def attempt(self,client):
        row=self.db.execute('SELECT * FROM all_attempts WHERE client=?',(str(client),)).fetchone()
        if row is None:raise ValueError('Unknown hedge identity')
        return dict(row)

    def attempts(self, obligation=None, *, active=False, symbol=None, include_deferred=True):
        table='attempts' if active else 'all_attempts'
        query='SELECT t.* FROM '+table+' t';clauses=[];args=[]
        if symbol is not None:
            query+=' CROSS JOIN obligations o ON o.id=t.obligation CROSS JOIN orders m ON m.client=o.maker'
            clauses.append('m.symbol=?');args.append(symbol)
        if active:clauses.append('t.'+ACTIVE)
        if obligation is not None:
            clauses.append('t.client IN (SELECT client FROM all_attempt_allocations WHERE obligation=?'+('' if include_deferred else ' AND deferred=0')+')');args.append(obligation)
        if clauses:query+=' WHERE '+' AND '.join(clauses)
        return [dict(r) for r in self.db.execute(query+' ORDER BY t.created,t.client',args)]

    def prepare_attempt(self, obligation, qty, venue='lighter', *, obligations=None):
        qty = D(str(qty))
        with self.transaction():
            rows = [self.obligation(oid) for oid in sorted(set(obligations if obligations is not None else [obligation]))]
            if not rows or rows[0]['id']!=obligation:
                raise ValueError('Missing first allocation')
            if len({(r['symbol'],r['side'],r['purpose']) for r in rows})!=1:
                raise ValueError('Incompatible hedge allocations')
            if venue!='lighter' and len(rows)!=1:
                raise ValueError('Emergency attempts retain exact original obligation')
            if any(a['status'] not in TERMINAL for r in rows for a in self.attempts(r['id'],include_deferred=False)):
                raise ValueError('Previous attempt has unresolved acceptance/fills')
            remaining=sum((D(r['qty'])-D(r['hedged'])-D(r['unwound']) for r in rows),D(0))
            if not qty.is_finite() or not 0 < qty <= remaining:
                raise ValueError('Attempt exceeds uncovered obligation')
            client = self._id(venue)
            self.db.execute('INSERT INTO attempts(client,obligation,venue,qty,created) VALUES(?,?,?,?,?)',
                            (client, obligation, venue, str(qty), time.monotonic()))
            left=qty
            for row in rows:
                allocated=min(left,D(row['qty'])-D(row['hedged'])-D(row['unwound']))
                if allocated:
                    self.db.execute('INSERT INTO attempt_allocations(client,obligation,qty) VALUES(?,?,?)',
                                    (client,row['id'],str(allocated)))
                    left-=allocated
                if not left:break
            self.event(rows[0]['symbol'], 'attempt_prepared', client=client, obligation=obligation, venue=venue, qty=qty)
        return client

    def attempt_state(self, client, state):
        if state not in LIFECYCLE:raise ValueError('Invalid attempt lifecycle')
        with self.transaction():
            self.db.execute('UPDATE attempts SET status=? WHERE client=? AND '+ACTIVE,(state,client))
            row=self.db.execute('SELECT status,filled FROM attempts WHERE client=?',(client,)).fetchone()
            if row is not None:
                if row['status']=='deferred' and D(row['filled']):raise ValueError('Filled attempt cannot be deferred')
                self.db.execute('UPDATE attempt_allocations SET deferred=? WHERE client=?',(int(row['status']=='deferred'),client))

    def observe_attempt(self, client, cumulative, status, exchange_id=None, average=None, fee=None):
        cumulative = D(str(cumulative))
        with self.transaction():
            row = self.attempt(client)
            if row is None:
                raise ValueError('Unknown hedge identity')
            if not cumulative.is_finite() or not 0 <= cumulative <= D(row['qty']):
                raise ValueError('Invalid hedge cumulative amount')
            if row['exchange_id'] and exchange_id and row['exchange_id'] != str(exchange_id):
                raise ValueError('Hedge identity changed')
            old = D(row['filled'])
            if cumulative < old:
                return
            state = row['status'] if row['status'] in TERMINAL else status
            if row['status'] in TERMINAL and cumulative != old:
                raise ValueError('Terminal hedge amount changed; reconciliation required')
            obligation_owner=self.obligation(row['obligation'])
            self._restore(obligation_owner['maker'])
            delta = cumulative-old
            old_cost=old*D(row['average']) if row['average'] is not None else (D(0) if old==0 else None)
            new_cost=cumulative*D(str(average)) if average is not None else None
            delta_average=(new_cost-old_cost)/delta if delta and new_cost is not None and old_cost is not None else None
            old_fee=D(row['fee']) if row['fee'] is not None else (D(0) if old==0 else None)
            delta_fee=D(str(fee))-old_fee if fee is not None and old_fee is not None else None
            target = 'hedged' if row['venue'] == 'lighter' else 'unwound'
            left=delta
            fee_left=delta_fee
            for allocation in self.allocations(client):
                part=min(left,D(allocation['qty'])-D(allocation['filled']))
                if not part:continue
                obligation=self.obligation(allocation['obligation'])
                total=D(obligation[target])+part
                if total+D(obligation['unwound' if target=='hedged' else 'hedged'])>D(obligation['qty']):
                    raise ValueError('Overhedge detected')
                self.db.execute('UPDATE obligations SET '+target+'=? WHERE id=?',(str(total),obligation['id']))
                self._pending(obligation['id'])
                self._inventory_delta(obligation['symbol'], -obligation['side']*part if target=='unwound' else D(0),
                                      -obligation['side']*part if target=='hedged' else D(0))
                self.db.execute('UPDATE attempt_allocations SET filled=? WHERE client=? AND obligation=?',
                                (str(D(allocation['filled'])+part),client,obligation['id']))
                part_fee=(fee_left if part==left else delta_fee*part/delta) if delta_fee is not None else None
                if fee_left is not None:fee_left-=part_fee
                self.event(obligation['symbol'], 'hedge_fill' if target=='hedged' else 'emergency_fill',
                           obligation=obligation['id'], client=client, qty=part, side=-obligation['side'],
                           purpose=obligation['purpose'], average=delta_average,
                           fee=part_fee,
                           reference=obligation['reference'],
                           latency_ms=(time.monotonic()-obligation['received'])*1000 if obligation['boot']==self.boot else None,
                           clock='process_monotonic' if obligation['boot']==self.boot else 'restart_unavailable', modeled=self.modeled)
                left-=part
            if left:raise ValueError('Attempt fill exceeds allocations')
            self.db.execute('UPDATE attempts SET filled=?,status=?,exchange_id=COALESCE(?,exchange_id),average=?,fee=? WHERE client=?',
                            (str(cumulative), state, str(exchange_id) if exchange_id else None,
                             str(average) if average is not None else (row['average'] if cumulative==old else None),
                             str(fee) if fee is not None else (row['fee'] if cumulative==old else None), client))
            self.db.execute('UPDATE attempt_allocations SET deferred=? WHERE client=?',(int(state=='deferred'),client))

    def inventory(self, symbol):
        row=self.db.execute('SELECT arcus,lighter FROM current_inventory WHERE symbol=?',(symbol,)).fetchone()
        return (D(row[0]),D(row[1])) if row else (D(0),D(0))

    def reconcile_inventory(self, symbol, arcus, lighter, reference):
        """Anchor an authoritative idle snapshot and journal its correction before any send."""
        with self.transaction():
            expected = self.inventory(symbol)
            net = arcus+lighter
            client = self._id('arcus')
            # Synthetic fill contributes net to Arcus; adjustments below remove that fiction.
            side = 1 if net > 0 else -1
            qty = abs(net)
            self.db.execute('INSERT INTO adjustments(symbol,arcus,lighter) VALUES(?,?,?)',
                            (symbol, str(arcus-expected[0]-net), str(lighter-expected[1])))
            if qty:
                self.db.execute('INSERT INTO orders(client,venue,symbol,side,purpose,qty,price,level,status,filled,created,anchor,reserved) '
                                'VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',
                                (client,'reconcile',symbol,side,'reconcile',str(qty),str(reference),0,'filled',str(qty),
                                 time.time(),str(reference),'0'))
                oid=self.db.execute('INSERT INTO obligations(maker,qty,received,reference) VALUES(?,?,?,?)',
                                    (client,str(qty),time.monotonic(),str(reference))).lastrowid
                self._check_obligation_identity(oid)
                self._pending(oid)
            self._inventory_delta(symbol,arcus-expected[0],lighter-expected[1])
            self.event(symbol,'reconcile_drift',arcus=arcus,lighter=lighter,correction=net)
            self.db.execute('INSERT OR REPLACE INTO meta VALUES(?,?)',('reconcile:'+symbol,'awaiting_account_confirmation'))

    def pending_reconcile(self,symbol=None):
        if symbol is not None:
            return self.db.execute('SELECT value FROM meta WHERE key=?',('reconcile:'+symbol,)).fetchone() is not None
        return self.db.execute("SELECT value FROM meta WHERE key>='reconcile:' AND key<'reconcile;' LIMIT 1").fetchone() is not None

    def confirm_reconcile(self,symbol):
        self.db.execute('DELETE FROM meta WHERE key=?',('reconcile:'+symbol,))

    def unresolved(self):
        return (self.makers(active=True) or self.obligations() or
                self.attempts(active=True))

    def legacy_cancel_intent(self,order):
        """Authorize only cancellation of an observed legacy identity; never adopt its inventory."""
        client=str(order.client_id or order.id)
        with self.transaction():
            self.db.execute('INSERT OR IGNORE INTO orders(client,venue,symbol,side,purpose,qty,price,level,status,exchange_id,filled,created,anchor,reserved) '
                            'VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                            (client,'arcus',order.symbol,1 if order.side.value=='buy' else -1,'legacy',str(order.amount),
                             str(order.price or 0),0,'open',str(order.id),str(order.filled),time.time(),'0','0'))
            self.event(order.symbol,'legacy_cancel_intent',client=client,order_id=str(order.id))
        return client

    def _component(self,maker,*,archived=False,max_nodes=256):
        """Bound both rows fetched and graph work before deciding whether a component can move."""
        self._component_work=0
        prefix='archive_' if archived else ''
        nodes={'orders':{},'obligations':{},'attempts':{},'attempt_allocations':{}}
        todo=[('orders',maker)]
        while todo:
            table,key=todo.pop()
            if key in nodes[table]:continue
            column='id' if table=='obligations' else 'client'
            row=self.db.execute('SELECT * FROM '+prefix+table+' WHERE '+column+'=?',(key,)).fetchone()
            if row is None:raise ValueError('Broken component identity')
            self._component_work+=1
            nodes[table][key]=dict(row)
            budget=max_nodes-sum(len(v) for v in nodes.values())
            if budget<0:return None
            if table=='orders':
                children=self.db.execute('SELECT id FROM '+prefix+'obligations WHERE maker=? LIMIT ?',(key,max_nodes+1)).fetchall()
                self._component_work+=len(children)
                if len(children)>max_nodes:return None
                todo.extend(('obligations',r[0]) for r in children)
            elif table=='obligations':
                todo.append(('orders',row['maker']))
                children=self.db.execute('SELECT * FROM '+prefix+'attempt_allocations WHERE obligation=? LIMIT ?',(key,max_nodes+1)).fetchall()
                self._component_work+=len(children)
                if len(children)>max_nodes:return None
                for allocation in children:
                    nodes['attempt_allocations'][allocation['client'],allocation['obligation']]=dict(allocation)
                    todo.append(('attempts',allocation['client']))
            else:
                children=self.db.execute('SELECT * FROM '+prefix+'attempt_allocations WHERE client=? LIMIT ?',(key,max_nodes+1)).fetchall()
                self._component_work+=len(children)
                if len(children)>max_nodes:return None
                for allocation in children:
                    nodes['attempt_allocations'][allocation['client'],allocation['obligation']]=dict(allocation)
                    todo.append(('obligations',allocation['obligation']))
        return nodes if sum(len(v) for v in nodes.values())<=max_nodes else None

    def _move_component(self,nodes,*,restore=False):
        source,target=('archive_','') if restore else ('','archive_')
        for table in ('orders','obligations','attempts','attempt_allocations'):
            rows=list(nodes[table].values())
            if rows:
                columns=tuple(rows[0])
                self.db.executemany('INSERT INTO '+target+table+'('+','.join(columns)+') VALUES('+','.join('?' for _ in columns)+')',
                                    (tuple(r[c] for c in columns) for r in rows))
        for table in ('attempt_allocations','attempts','obligations','orders'):
            if table=='attempt_allocations':
                self.db.executemany('DELETE FROM '+source+table+' WHERE client=? AND obligation=?',nodes[table])
            else:
                column='id' if table=='obligations' else 'client'
                self.db.executemany('DELETE FROM '+source+table+' WHERE '+column+'=?',((key,) for key in nodes[table]))

    def _restore(self,client):
        if self.db.execute('SELECT 1 FROM orders WHERE client=?',(client,)).fetchone():return
        # Only bounded components are ever archived, so late updates have bounded restoration work too.
        nodes=self._component(client,archived=True)
        if nodes is None:raise ValueError('Archived component exceeds restoration limit')
        self._move_component(nodes,restore=True)

    def archive_closed(self,*,max_candidates=32,max_nodes=256):
        """One low-frequency bounded batch. Cursor rotates past pending/dust/large components."""
        if type(max_candidates) is not int or not 1<=max_candidates<=64 or type(max_nodes) is not int or not 1<=max_nodes<=256:
            raise ValueError('Archive limits must be positive bounded integers (candidates<=64, nodes<=256)')
        moved=0;work_left=max_nodes*4
        with self.transaction():
            count=max(1,max_candidates//2)
            zero=self.db.execute("SELECT rowid,client,created FROM orders WHERE status IN ('filled','canceled','rejected','expired','deferred') AND filled='0' "
                'AND (created,client)>(?,?) ORDER BY created,client LIMIT ?',(*self._archive_zero_cursor,count)).fetchall()
            if not zero:self._archive_zero_cursor=(0.0,'')
            regular=self.db.execute('SELECT rowid,client,created FROM orders WHERE rowid>? ORDER BY rowid LIMIT ?',
                                    (self._archive_cursor,max_candidates-len(zero))).fetchall()
            if not regular:self._archive_cursor=0
            candidates=[(r,True) for r in zero]+[(r,False) for r in regular]
            for candidate,priority in candidates:
                if work_left<4:break
                if priority:self._archive_zero_cursor=(candidate['created'],candidate['client'])
                else:self._archive_cursor=candidate['rowid']
                row=self.db.execute('SELECT * FROM orders WHERE client=?',(candidate['client'],)).fetchone()
                work_left-=1
                if row is None or row['status'] not in TERMINAL:continue
                nodes=self._component(row['client'],max_nodes=min(max_nodes,max(1,work_left//4)))
                work_left-=self._component_work
                if nodes is None:continue
                if any(r['status'] not in TERMINAL for t in ('orders','attempts') for r in nodes[t].values()):continue
                if any(D(r['qty'])!=D(r['hedged'])+D(r['unwound']) for r in nodes['obligations'].values()):continue
                self._move_component(nodes)
                moved+=len(nodes['orders'])
        return moved
