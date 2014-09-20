import types
from decimal import Decimal

from flask import Flask, url_for, request, make_response, g, current_app
from datetime import timedelta
from functools import update_wrapper

try:
    import ujson as json
except:
    import json

import logging
logger = logging.getLogger(__name__)

from functools import wraps
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


def crossdomain(origin=None, methods=None, headers=None,
                max_age=21600, attach_to_all=True,
                automatic_options=True):

    headers = headers or "Origin, X-Requested-With, Content-Type, Accept"

    if methods is not None:
        methods = ', '.join(sorted(x.upper() for x in methods))
    if headers is not None and not isinstance(headers, basestring):
        headers = ', '.join(x.upper() for x in headers)
    if not isinstance(origin, basestring):
        origin = ', '.join(origin)
    if isinstance(max_age, timedelta):
        max_age = max_age.total_seconds()

    def get_methods():
        if methods is not None:
            return methods

        options_resp = current_app.make_default_options_response()
        return options_resp.headers['allow']

    def decorator(f):
        def wrapped_function(*args, **kwargs):
            if automatic_options and request.method == 'OPTIONS':
                resp = current_app.make_default_options_response()
            else:
                resp = make_response(f(*args, **kwargs))
            if not attach_to_all and request.method != 'OPTIONS':
                return resp

            h = resp.headers

            h['Access-Control-Allow-Origin'] = origin
            h['Access-Control-Allow-Methods'] = get_methods()
            h['Access-Control-Max-Age'] = str(max_age)
            h['Access-Control-Allow-Headers'] = headers
            return resp

        f.provide_automatic_options = False
        f.required_methods = ['OPTIONS']
        return update_wrapper(wrapped_function, f)
    return decorator


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

                print 'Deep Scan', full_key, query_args

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

    app.config.from_pyfile('endless_config.py', silent=False)

    app.run(debug=True, port=port)
