"""
Useful testing utilities.

This module is considered public API.

"""

import time

import six

from pymemcache.exceptions import MemcacheIllegalInputError
from pymemcache.serde import LegacyWrappingSerde
from pymemcache.client.base import check_key_helper


class MockMemcacheClient(object):
    """
    A (partial) in-memory mock for Clients.

    """

    def __init__(self,
                 server=None,
                 serde=None,
                 serializer=None,
                 deserializer=None,
                 connect_timeout=None,
                 timeout=None,
                 no_delay=False,
                 ignore_exc=False,
                 default_noreply=True,
                 allow_unicode_keys=False,
                 encoding='ascii'):

        self._contents = {}

        self.serde = serde or LegacyWrappingSerde(serializer, deserializer)
        self.allow_unicode_keys = allow_unicode_keys

        # Unused, but present for interface compatibility
        self.server = server
        self.connect_timeout = connect_timeout
        self.timeout = timeout
        self.no_delay = no_delay
        self.ignore_exc = ignore_exc
        self.encoding = encoding

    def check_key(self, key):
        """Checks key and add key_prefix."""
        return check_key_helper(key, allow_unicode_keys=self.allow_unicode_keys)

    def clear(self):
        """Method used to clear/reset mock cache"""
        self._contents.clear()

    def get(self, key, default=None):
        key = self.check_key(key)

        if key not in self._contents:
            return default

        expire, value, flags = self._contents[key]
        if expire and expire < time.time():
            del self._contents[key]
            return default

        return self.serde.deserialize(key, value, flags)

    def get_many(self, keys):
        out = {}
        for key in keys:
            value = self.get(key)
            if value is not None:
                out[key] = value
        return out

    get_multi = get_many

    def set(self, key, value, expire=0, noreply=True, flags=0):
        key = self.check_key(key)
        if (isinstance(value, six.string_types) and
                not isinstance(value, six.binary_type)):
            try:
                value = value.encode(self.encoding)
            except (UnicodeEncodeError, UnicodeDecodeError):
                raise MemcacheIllegalInputError

        value, flags = self.serde.serialize(key, value)

        if expire:
            expire += time.time()

        self._contents[key] = expire, value, flags
        return True

    def set_many(self, values, expire=None, noreply=True):
        for key, value in six.iteritems(values):
            self.set(key, value, expire, noreply)
        return []

    set_multi = set_many

    def incr(self, key, value, noreply=False):
        current = self.get(key)
        present = current is not None
        if present:
            self.set(key, current + value, noreply=noreply)
        return None if noreply or not present else current + value

    def decr(self, key, value, noreply=False):
        current = self.get(key)
        if current is None:
            return

        self.set(key, current - value, noreply=noreply)
        return current - value

    def add(self, key, value, expire=None, noreply=True):
        current = self.get(key)
        present = current is not None
        if not present:
            self.set(key, value, expire, noreply)
        return not present

    def delete(self, key, noreply=True):
        current = self._contents.pop(key, None)
        present = current is not None
        return noreply or present

    def delete_many(self, keys, noreply=True):
        for key in keys:
            self.delete(key, noreply)
        return True

    def prepend(self, key, value, expire=0, noreply=None, flags=None):
        current = self.get(key)
        if current is not None:
            if (isinstance(value, six.string_types) and
                    not isinstance(value, six.binary_type)):
                try:
                    value = value.encode(self.encoding)
                except (UnicodeEncodeError, UnicodeDecodeError):
                    raise MemcacheIllegalInputError
            self.set(key, value + current)
        return True

    def append(self, key, value, expire=0, noreply=None, flags=None):
        current = self.get(key)
        if current is not None:
            if (isinstance(value, six.string_types) and
                    not isinstance(value, six.binary_type)):
                try:
                    value = value.encode(self.encoding)
                except (UnicodeEncodeError, UnicodeDecodeError):
                    raise MemcacheIllegalInputError
            self.set(key, current + value)
        return True

    delete_multi = delete_many

    def stats(self):
        # I make no claim that these values make any sense, but the format
        # of the output is the same as for pymemcache.client.Client.stats()
        return {
            "version": "MockMemcacheClient",
            "rusage_user": 1.0,
            "rusage_system": 1.0,
            "hash_is_expanding": False,
            "slab_reassign_running": False,
            "inter": "in-memory",
            "evictions": False,
            "growth_factor": 1.0,
            "stat_key_prefix": "",
            "umask": 0o644,
            "detail_enabled": False,
            "cas_enabled": False,
            "auth_enabled_sasl": False,
            "maxconns_fast": False,
            "slab_reassign": False,
            "slab_automove": False,
        }
