# -*- coding: utf-8 -*-
from unittest import TestCase

from pymemcache.serde import (python_memcache_serializer,
                              python_memcache_deserializer, FLAG_BYTES,
                              FLAG_PICKLE, FLAG_INTEGER, FLAG_LONG, FLAG_TEXT)
import pytest
import six


class CustomInt(int):
    """
    Custom integer type for testing.

    Entirely useless, but used to show that built in types get serialized and
    deserialized back as the same type of object.
    """
    pass


@pytest.mark.unit()
class TestSerde(TestCase):

    def check(self, value, expected_flags):
        serialized, flags = python_memcache_serializer(b'key', value)
        assert flags == expected_flags

        # pymemcache stores values as byte strings, so we immediately the value
        # if needed so deserialized works as it would with a real server
        if not isinstance(serialized, six.binary_type):
            serialized = six.text_type(serialized).encode('ascii')

        deserialized = python_memcache_deserializer(b'key', serialized, flags)
        assert deserialized == value

    def test_bytes(self):
        self.check(b'value', FLAG_BYTES)
        self.check(b'\xc2\xa3 $ \xe2\x82\xac', FLAG_BYTES)  # £ $ €

    def test_unicode(self):
        self.check(u'value', FLAG_TEXT)
        self.check(u'£ $ €', FLAG_TEXT)

    def test_int(self):
        self.check(1, FLAG_INTEGER)

    def test_long(self):
        # long only exists with Python 2, so we're just testing for another
        # integer with Python 3
        if six.PY2:
            expected_flags = FLAG_LONG
        else:
            expected_flags = FLAG_INTEGER
        self.check(123123123123123123123, expected_flags)

    def test_pickleable(self):
        self.check({'a': 'dict'}, FLAG_PICKLE)

    def test_subtype(self):
        # Subclass of a native type will be restored as the same type
        self.check(CustomInt(123123), FLAG_PICKLE)
