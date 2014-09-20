__author__ = 'bwillard'

from cassandra.cluster import Cluster
try:
    import ujson as json
except:
    import json

import types
import pytz
from decimal import Decimal
from datetime import datetime, date

from .tools import memoize


default_dumps = json.dumps
default_loads = json.loads

import logging
logger = logging.getLogger(__name__)


ROOT = '||ROOT||'  # pipe is important for byte sort


_batch_template = '''
    BEGIN BATCH
    %s
    APPLY BATCH;
'''

_insert_template = '''
    insert into datastore (collection_id, item_id, key, value)
    values(%s, %s, %s, %s);
'''

_select_template = '''
    select 
        * 
    from 
        datastore 
    where 
        %s
'''

_delete_template = '''
    delete
    from
        datastore
    where
        collection_id = %s
        and item_id = %s
'''


def create_keys(key):

    if key.endswith('/'):
        key = key[:-1]

    if key.startswith('/'):
        key = key[1:]

    size = key.count('/')
    assert size > 0, 'Key requires min depth 1'

    parts = key.split('/')

    if len(parts) == 2:
        return '/'.join(parts), ROOT
    else:
        return '/'.join(parts[:-1]), parts[-1]


@memoize
def connect(nodes):
    return Client(nodes=nodes)


tz_eastern = pytz.timezone('America/New_York')
tz_utc = pytz.timezone('UTC')
date_fmt = '%Y-%m-%dT%H:%M:%SZ'


def to_utc(d):
    if not d.tzinfo:
        d = tz_eastern.localize(d)
    return d.astimezone(tz_utc)


numeric_types = set([types.IntType, types.FloatType, types.LongType])

def clean(obj):
    if isinstance(obj, types.DictType):
        d = {}
        for k, v in obj.items():
            d[k] = clean(v)
        return d
    elif isinstance(obj, types.ListType) or isinstance(obj, types.TupleType) or isinstance(obj, set):
        return [clean(i) for i in obj]
    elif isinstance(obj, basestring):
        return obj
    elif type(obj) in numeric_types:
        return obj
    elif isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, datetime):
        return to_utc(obj).strftime(date_fmt)
    elif isinstance(obj, date):
        return to_utc(obj).strftime(date_fmt)
    elif isinstance(obj, object):
        return unicode(str(obj))


def split(obj, parent=None):
    if isinstance(obj, types.DictType):
        for k, v in obj.items():
            for k2, v2 in split(v, '/'.join(filter(None, [parent, k]))):
                yield k2, clean(v2)
    elif isinstance(obj, datetime):
        yield parent, clean(obj)
    else:
        yield parent, clean(obj)


def combine(items):
    item = {}
    for k, v in items:
        if '/' in k:
            parts = filter(None, k.split('/'))
            sitem = item
            for p in parts[:-1]:
                sitem = sitem.setdefault(p, {})
            sitem[parts[-1]] = v
        else:
            item[k] = v
    return item


class Client(object):

    def __init__(self, nodes=None, keyspace='endless'):
        self.keyspace = keyspace
        self.cluster = Cluster(nodes) if nodes else Cluster()
        self.session = self.cluster.connect(self.keyspace)

    def delete(self, key, async=False):
        collection_id, item_id = create_keys(key)

        logger.debug('Executing delete: %s', _delete_template)

        # print 'Executing', stmt
        if async:
            future = self.session.execute_async(_delete_template, (collection_id, item_id))
            return future.result()
        else:
            return self.session.execute(_delete_template, (collection_id, item_id))

    def put(self, key, data, dumps=default_dumps, overwrite=False, async=False):
        
        batch = []
        args = []

        collection_id, item_id = create_keys(key)

        if overwrite:
            batch.append(_delete_template)
            args += [collection_id, item_id]

        for k, v in split(data):
            batch.append(_insert_template)
            args += [collection_id, item_id, k, dumps(v)]

        stmt = _batch_template % '\n'.join(batch)

        logger.debug('Executing put: %s', stmt)

        # print 'Executing', stmt
        if async:
            future = self.session.execute_async(stmt, args)
            return future.result()
        else:
            self.session.execute(stmt, args)


    def scan(self, parent_key, loads=default_loads):

        stmt, args = build_scan_stmt(parent_key)

        logger.debug('Executing scan: %s', stmt)

        # print 'Executing', stmt
        cur = self.session.execute(stmt, args)

        parts = [(r.key, loads(r.value)) for r in cur]

        return combine(parts)

    def scan_async(self, parent_key, loads=default_loads):

        stmt, args = build_scan_stmt(parent_key)

        logger.debug('Executing scan async: %s', stmt)

        # print 'Executing', stmt
        future = self.session.execute_async(stmt, args)

        return AsyncScanFormatter(future, loads)

    def deep_scan(self, parent_key, loads=default_loads, gt=None, gte=None, lt=None, lte=None, limit=None, keys_per_item=250):

        stmt, args = build_deep_scan_stmt(parent_key, gt, gte, lt, lte, limit, keys_per_item)

        logger.debug('Executing deep scan: %s', stmt)
        cur = self.session.execute(stmt, args)

        for r in construct(cur, limit, loads):
            yield r

    def deep_scan_async(self, parent_key, loads=default_loads, gt=None, gte=None, lt=None, lte=None, limit=None, keys_per_item=250):

        stmt, args = build_deep_scan_stmt(parent_key, gt, gte, lt, lte, limit, keys_per_item)

        logger.debug('Executing deep scan async: %s', stmt)
        future = self.session.execute_async(stmt, args)

        return AsyncDeepScanFormatter(future, limit, loads)

    def get(self, key, loads=default_loads):
        return self.scan(key, loads)

    def get_async(self, key, loads=default_loads):
        return self.scan_async(key, loads)


def build_scan_stmt(parent_key):

    collection_id, item_id = create_keys(parent_key)

    clauses = []
    args = []

    clauses.append("collection_id = %s")
    args.append(collection_id)

    clauses.append("item_id = %s")
    args.append(item_id)

    clause = ' and '.join(clauses)

    stmt = _select_template % clause

    return stmt, args


def build_deep_scan_stmt(parent_key, gt=None, gte=None, lt=None, lte=None, limit=None, keys_per_item=250):
    collection_id = '/'.join(create_keys(parent_key))

    clauses = []
    args = []

    clauses.append("collection_id = %s")
    args.append(collection_id)

    lt = lt or ROOT  # Default for data model correctness

    if lte:
        clauses.append("item_id <= %s")
        args.append(lte)
    else:
        clauses.append("item_id < %s")
        args.append(lt)  # defaults to ROOT which is correct

    if gte:
        clauses.append("item_id >= %s")
        args.append(gte)
    elif gt:
        clauses.append("item_id > %s")
        args.append(gt)

    clause = ' and '.join(clauses)

    stmt = _select_template % clause

    if limit:
        stmt += ' \nlimit %s\n' % (limit * keys_per_item)

    return stmt, args


def construct(cur, limit, loads):
    parts = None
    cur_item = None
    count = 0
    for r in cur:
        if cur_item != r.item_id:
            if parts:
                yield cur_item, combine(parts)
                if limit:
                    count += 1
                    if count >= limit:
                        return # break is not enough

            cur_item = r.item_id
            parts = []
        parts.append((r.key, loads(r.value)))

    # Check remainder
    if parts:
        yield cur_item, combine(parts)


class AsyncScanFormatter(object):
    def __init__(self, future, loads):
        self.future = future
        self.loads = loads

    def result(self):
        cur = self.future.result()
        loads = self.loads

        parts = [(r.key, loads(r.value)) for r in cur]

        return combine(parts)


class AsyncDeepScanFormatter(object):
    def __init__(self, future, limit, loads):
        self.future = future
        self.limit = limit
        self.loads = loads

    def result(self):
        cur = self.future.result()
        limit = self.limit
        loads = self.loads

        for r in construct(cur, limit, loads):
            yield r

