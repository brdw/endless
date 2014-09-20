__author__ = 'bwillard'

from . import endless
import logging

logger = logging.getLogger(__name__)

key_template = 'remote/dicts/%s/%s'


class EndlessDict(object):

    def __init__(self, nodes, name, **kwargs):
        self.store = endless.connect(nodes)
        self.name = name

    def __setitem__(self, key, val):

        key = key_template % (self.name, key)

        logger.debug('Set: %s', key)
        item = self.store.put(key, val)

        return True  # refactor client to return something meaningful

    def put_async(self, key, val):

        key = key_template % (self.name, key)

        logger.debug('Set: %s', key)
        future = self.store.put(key, val, async=True)

        return future

    def __getitem__(self, key):
        key = key_template % (self.name, key)

        return self.store.get(key)

    def __delitem__(self, key):

        key = key_template % (self.name, key)

        self.store.delete(key)

    def keys(self):
        logger.warn("This is stupid and potentially infinite")

        key = 'remote/dicts/%s' % self.name

        cur = self.store.deep_scan(key)

        for k, v in cur:
            yield k

    def values(self):
        logger.warn("This is stupid and potentially infinite")

        key = 'remote/dicts/%s' % self.name

        cur = self.store.deep_scan(key)

        for k, v in cur:
            yield v

    def get(self, key, default=None):
        return self.__getitem__(key) or default

    def items(self):
        logger.warn("This is stupid and potentially infinite")

        key = 'remote/dicts/%s' % self.name

        cur = self.store.deep_scan(key)

        for k, v in cur:
            yield k, v
