__author__ = 'bwillard'

import logging
logger = logging.getLogger(__name__)

import types
from decimal import Decimal

from flask import Flask, url_for, request, make_response, g, current_app

try:
    import ujson as json
except:
    import json

from functools import wraps

from .tools import crossdomain
from . import endless


app = Flask(__name__)


def get_endless_client():
    if not hasattr(current_app, 'endless_db'):
        print 'Creating new connection'
        current_app.endless_db = endless.Client(app.config['CASSANDRA_NODES'])
    return current_app.endless_db


# @app.teardown_appcontext
# def close_db(error):
#     if hasattr(g, 'endless_db'):
#         g.endless_db.close()



def error_trap(fn):
    @wraps(fn)
    def _error_trap(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            logger.exception(e)
            return make_json({'error': '%s:%s' % (type(e), str(e))}, status=500)
    return _error_trap


def _clean(obj):
    if isinstance(obj, types.DictType):
        for k, v in obj.items():
            obj[k] = _clean(v)
        return obj
    elif isinstance(obj, types.ListType):
        return [_clean(i) for i in obj]
    elif isinstance(obj, Decimal):
        return float(obj)
    else:
        return obj


def make_json(obj, status=200):
    response = json.dumps(_clean(obj))
    status = status
    headers = {
        'Content-type': 'application/json'
    }
    return make_response((response, status, headers))


@app.route('/', methods=['GET'])
@crossdomain(origin='*')
@error_trap
def home():
    return make_json([{'msg': 'Welcome to Endless!'}])


@app.route('/<collection>/<key>', methods=['GET', 'POST', 'DELETE'])
@app.route('/<collection>/<key>/<path:sub_key>', methods=['GET', 'POST', 'DELETE'])
@app.route('/<collection>/<key>/', methods=['GET', 'POST', 'DELETE'])
@crossdomain(origin='*')
@error_trap
def get_table_key(collection, key, sub_key=None):

    logger.info('Parts %s %s %s', collection, key, sub_key)

    parts = [collection, key, sub_key] if sub_key else [collection, key]

    full_key = '/'.join(parts)

    logger.debug('Full Key: %s', full_key)

    with app.app_context():
        client = get_endless_client()

        if request.method == 'GET':
            if full_key.endswith('/'):
                logger.info('Deep Scan: %s', full_key)

                results = {}

                params = request.args

                query_args = {}
                page_size = int(params.get('page_size', 50))

                query_args['limit'] = page_size

                page_args = {}

                for arg in ['gt', 'gte', 'lt', 'lte']:
                    if arg in params:
                        query_args[arg] = params[arg]
                        page_args[arg] = params[arg]

                logger.debug('Deep Scan: %s %s', full_key, query_args)

                future = client.deep_scan_async(full_key, **query_args)

                for k, v in future.result():
                    results[k] = v

                resp = {'data': results}

                if len(results) == page_size:
                    page_args['page_size'] = page_size
                    page_args.pop('gte', None)
                    page_args['gt'] = k
                    resp['next'] = url_for('get_table_key', collection=collection, key=key, sub_key=sub_key, _external=True, **page_args)

                return make_json(resp)
            else:
                logger.info('Get: %s', full_key)
                future = client.get_async(full_key)
                return make_json({'data': future.result()})

        elif request.method == 'POST':

            item = request.json

            # deprecate this
            if not item:
                item = request.form.to_dict()

                if 'd' in item:
                    item = json.loads(item['d'])
                else:
                    json_str = ''
                    for k, v in item.items():
                        json_str += k + '=' + v if v else k
                    item = json.loads(json_str)

            client.put(full_key, item, async=True)

            return make_json({'data': item})

        elif request.method == 'DELETE':
            client.delete(full_key, async=True)
            return make_json({full_key: 'deleted'})


if __name__ == '__main__':

    logging.basicConfig(level=logging.DEBUG)

    port = 5000

    app.config.from_pyfile('endless/default_config.py', silent=False)

    app.run(debug=True, port=port)
