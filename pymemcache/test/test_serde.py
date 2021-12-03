from unittest import TestCase

from pymemcache.serde import (
    pickle_serde,
    PickleSerde,
    FLAG_BYTES,
    FLAG_PICKLE,
    FLAG_INTEGER,
    FLAG_TEXT,
)
import pytest
import pickle


class CustomInt(int):
    """
    Custom integer type for testing.

    Entirely useless, but used to show that built in types get serialized and
    deserialized back as the same type of object.
    """

    pass


@pytest.mark.unit()
class TestSerde(TestCase):
    serde = pickle_serde

    def check(self, value, expected_flags):
        serialized, flags = self.serde.serialize(b"key", value)
        assert flags == expected_flags

        # pymemcache stores values as byte strings, so we immediately the value
        # if needed so deserialized works as it would with a real server
        if not isinstance(serialized, bytes):
            serialized = str(serialized).encode("ascii")

        deserialized = self.serde.deserialize(b"key", serialized, flags)
        assert deserialized == value

    def test_bytes(self):
        self.check(b"value", FLAG_BYTES)
        self.check(b"\xc2\xa3 $ \xe2\x82\xac", FLAG_BYTES)  # £ $ €

    def test_unicode(self):
        self.check("value", FLAG_TEXT)
        self.check("£ $ €", FLAG_TEXT)

    def test_int(self):
        self.check(1, FLAG_INTEGER)

    def test_pickleable(self):
        self.check({"a": "dict"}, FLAG_PICKLE)

    def test_subtype(self):
        # Subclass of a native type will be restored as the same type
        self.check(CustomInt(123123), FLAG_PICKLE)


@pytest.mark.unit()
class TestSerdePickleVersion0(TestCase):
    serde = PickleSerde(pickle_version=0)


@pytest.mark.unit()
class TestSerdePickleVersion1(TestCase):
    serde = PickleSerde(pickle_version=1)


@pytest.mark.unit()
class TestSerdePickleVersion2(TestCase):
    serde = PickleSerde(pickle_version=2)


@pytest.mark.unit()
class TestSerdePickleVersionHighest(TestCase):
    serde = PickleSerde(pickle_version=pickle.HIGHEST_PROTOCOL)
