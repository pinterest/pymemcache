# -*- coding: utf-8 -*-
from unittest import TestCase

import pytest
import six
from six.moves import cPickle as pickle

from pymemcache import codecs


class CustomInt(int):
    """
    Custom integer type for testing.

    Entirely useless, but used to show that built in types get serialized and
    deserialized back as the same type of object.
    """
    pass


@pytest.mark.unit()
class TestSerde(TestCase):
    Serde = codecs.Serde

    def check(self, value, expected_flags, pickle_version=None):
        s = self.Serde(pickle_version=pickle_version)

        serialized, flags = s.serialize(b'key', value)
        assert flags == expected_flags

        # pymemcache stores values as byte strings, so we immediately the value
        # if needed so deserialized works as it would with a real server
        if not isinstance(serialized, six.binary_type):
            serialized = six.text_type(serialized).encode('ascii')

        deserialized = s.deserialize(b'key', serialized, flags)
        assert deserialized == value

    def test_bytes(self):
        self.check(b'value', self.Serde.FLAG_BYTES)
        self.check(b'\xc2\xa3 $ \xe2\x82\xac', self.Serde.FLAG_BYTES)  # £ $ €

    def test_unicode(self):
        self.check(u'value', self.Serde.FLAG_TEXT)
        self.check(u'£ $ €', self.Serde.FLAG_TEXT)

    def test_int(self):
        self.check(1, self.Serde.FLAG_INTEGER)

    def test_long(self):
        # long only exists with Python 2, so we're just testing for another
        # integer with Python 3
        if six.PY2:
            expected_flags = self.Serde.FLAG_LONG
        else:
            expected_flags = self.Serde.FLAG_INTEGER
        self.check(123123123123123123123, expected_flags)

    def test_pickleable(self):
        self.check({'a': 'dict'}, self.Serde.FLAG_PICKLE)

    def test_subtype(self):
        # Subclass of a native type will be restored as the same type
        self.check(CustomInt(123123), self.Serde.FLAG_PICKLE)

    def test_pickle_version(self):
        for pickle_version in range(-1, pickle.HIGHEST_PROTOCOL):
            self.check(
                dict(whoa='nelly', humans=u'amazing', answer=42),
                self.Serde.FLAG_PICKLE,
                pickle_version=pickle_version,
            )
