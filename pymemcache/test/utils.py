"""
Useful testing utilities.

This module is considered public API.

"""

import time


class MockMemcacheClient(object):
    """
    A (partial) in-memory mock for Clients.

    """

    def __init__(self,
                 server=None,
                 serializer=None,
                 deserializer=None,
                 connect_timeout=None,
                 timeout=None,
                 no_delay=False,
                 ignore_exc=False):

        self._contents = {}

        self.serializer = serializer
        self.deserializer = deserializer

        # Unused, but present for interface compatibility
        self.server = server
        self.connect_timeout = connect_timeout
        self.timeout = timeout
        self.no_delay = no_delay
        self.ignore_exc = ignore_exc

    def get(self, key):
        if key not in self._contents:
            return None

        expire, value, was_serialized = self._contents[key]
        if expire and expire < _time.time():
            del self._contents[key]
            return None

        if self.deserializer:
            return self.deserializer(key, value, 2 if was_serialized else 1)
        return value

    def get_many(self, keys):
        out = {}
        for key in keys:
            value = self.get(key)
            if value is not None:
                out[key] = value
        return out

    def set(self, key, value, expire=0, noreply=True):
        was_serialized = False
        if self.serializer:
            value = self.serializer(key, value)

        if expire:
            expire += time.time()

        self._contents[key] = expire, value, was_serialized

    def set_many(self, values, expire=None, noreply=True):
        for key, value in values.iteritems():
            self.set(key, value, expire, noreply)

    def incr(self, key, value, noreply=False):
        current = self.get(key)
        self.set(key, current + value, noreply=noreply)
        return current + value

    def decr(self, key, value, noreply=False):
        current = self.get(key)
        self.set(key, current - value, noreply=noreply)
        return current - value

    def add(self, key, value, expire=None, noreply=True):
        current = self.get(key)
        if current is None:
            self.set(key, value, expire, noreply)
