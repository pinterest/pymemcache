from unittest import TestCase

from pymemcache.serde import (python_memcache_serializer,
                              python_memcache_deserializer)


class TestSerde(TestCase):

    def check(self, value):
        serialized, flags = python_memcache_serializer(b'key', value)
        deserialized = python_memcache_deserializer(b'key', serialized, flags)
        assert deserialized == value

    def test_str(self):
        self.check('value')

    def test_int(self):
        self.check(1)

    def test_long(self):
        self.check(123123123123123123123)

    def test_pickleable(self):
        self.check({'a': 'dict'})
