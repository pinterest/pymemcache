# -*- coding: utf-8 -*-
from unittest import TestCase

from pymemcache import serde
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

    def check(self, value, expected_flags, pickle_version=None):
        s = serde.Serde(pickle_version=pickle_version)

        serialized, flags = s.from_python(b'key', value)
        assert flags == expected_flags

        # pymemcache stores values as byte strings, so we immediately the value
        # if needed so deserialized works as it would with a real server
        if not isinstance(serialized, six.binary_type):
            serialized = six.text_type(serialized).encode('ascii')

        deserialized = s.to_python(b'key', serialized, flags)
        assert deserialized == value

    def test_bytes(self):
        self.check(b'value', serde.FLAG_BYTES)
        self.check(b'\xc2\xa3 $ \xe2\x82\xac', serde.FLAG_BYTES)  # £ $ €

    def test_unicode(self):
        self.check(u'value', serde.FLAG_TEXT)
        self.check(u'£ $ €', serde.FLAG_TEXT)

    def test_int(self):
        self.check(1, serde.FLAG_INTEGER)

    def test_long(self):
        # long only exists with Python 2, so we're just testing for another
        # integer with Python 3
        if six.PY2:
            expected_flags = serde.FLAG_LONG
        else:
            expected_flags = serde.FLAG_INTEGER
        self.check(123123123123123123123, expected_flags)

    def test_pickleable(self):
        self.check({'a': 'dict'}, serde.FLAG_PICKLE)

    def test_subtype(self):
        # Subclass of a native type will be restored as the same type
        self.check(CustomInt(123123), serde.FLAG_PICKLE)
