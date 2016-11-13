from unittest import TestCase

from pymemcache.serde import (python_memcache_serializer,
                              python_memcache_deserializer)
import pytest
import six


@pytest.mark.unit()
class TestSerde(TestCase):

    def check(self, value):
        serialized, flags = python_memcache_serializer(b'key', value)

        # pymemcache stores values as byte strings, so we immediately the value
        # if needed so deserialized works as it would with a real server
        if not isinstance(serialized, six.binary_type):
            serialized = six.text_type(serialized).encode('ascii')

        deserialized = python_memcache_deserializer(b'key', serialized, flags)
        assert deserialized == value

    def test_bytes(self):
        self.check(b'value')

    def test_unicode(self):
        self.check(u'value')

    def test_int(self):
        self.check(1)

    def test_long(self):
        self.check(123123123123123123123)

    def test_pickleable(self):
        self.check({'a': 'dict'})
