__author__ = 'bwillard'


from functools import wraps, update_wrapper
from flask import Flask, url_for, request, make_response, g, current_app
from datetime import timedelta


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


def _make_key_str(name, *args, **kwargs):
    key_str = name

    if args:
        key_str += '::' + '::'.join([str(a) for a in args])
    if kwargs:
        key_str += '::' + '::'.join(['%s=%s' % (str(k), str(v)) for k, v in sorted(kwargs.items(), key=lambda x: x[0])])

    return key_str


def memoize(fn):
    memoize.results = {}
    @wraps(fn)
    def _memoize(*args, **kwargs):
        key = _make_key_str( '%s.%s' % (fn.__module__, fn.__name__), *args, **kwargs)

        if key in memoize.results:
            return memoize.results[key]
        else:
            val = fn(*args, **kwargs)
            memoize.results[key] = val
            return val
    return update_wrapper(_memoize, fn)