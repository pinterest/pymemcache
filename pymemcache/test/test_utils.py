import six
import pytest

from pymemcache.test.utils import MockMemcacheClient


def test_get_set():
    client = MockMemcacheClient()
    assert client.get(b"hello") is None

    client.set(b"hello", 12)
    assert client.get(b"hello") == 12


def test_get_many_set_many():
    client = MockMemcacheClient()
    client.set(b"h", 1)

    result = client.get_many([b"h", b"e", b"l", b"o"])
    assert result == {b"h": 1}

    # Convert keys into bytes
    d = dict((k.encode('ascii'), v)
             for k, v in six.iteritems(dict(h=1, e=2, l=3)))
    client.set_many(d)
    assert client.get_many([b"h", b"e", b"l", b"o"]) == d


def test_add():
    client = MockMemcacheClient()

    client.add(b"k", 2)
    assert client.get(b"k") == 2

    client.add(b"k", 25)
    assert client.get(b"k") == 2


def test_delete():
    client = MockMemcacheClient()

    client.add(b"k", 2)
    assert client.get(b"k") == 2

    client.delete(b"k")
    assert client.get(b"k") is None


def test_incr_decr():
    client = MockMemcacheClient()

    client.add(b"k", 2)

    client.incr(b"k", 4)
    assert client.get(b"k") == 6

    client.decr(b"k", 2)
    assert client.get(b"k") == 4
