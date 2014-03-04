from nose import tools
import six

from pymemcache.test.utils import MockMemcacheClient


def test_get_set():
    client = MockMemcacheClient()
    tools.assert_equal(client.get(b"hello"), None)

    client.set(b"hello", 12)
    tools.assert_equal(client.get(b"hello"), 12)


def test_get_many_set_many():
    client = MockMemcacheClient()
    client.set(b"h", 1)

    tools.assert_equal(client.get_many([b"h", b"e", b"l", b"o"]),
                       {b"h": 1})

    # Convert keys into bytes
    d = dict((k.encode('ascii'), v)
             for k, v in six.iteritems(dict(h=1, e=2, l=3)))
    client.set_many(d)
    tools.assert_equal(client.get_many([b"h", b"e", b"l", b"o"]),
                       d)


def test_add():
    client = MockMemcacheClient()

    client.add(b"k", 2)
    tools.assert_equal(client.get(b"k"), 2)

    client.add(b"k", 25)
    tools.assert_equal(client.get(b"k"), 2)


def test_delete():
    client = MockMemcacheClient()

    client.add(b"k", 2)
    tools.assert_equal(client.get(b"k"), 2)

    client.delete(b"k")
    tools.assert_equal(client.get(b"k"), None)


def test_incr_decr():
    client = MockMemcacheClient()

    client.add(b"k", 2)

    client.incr(b"k", 4)
    tools.assert_equal(client.get(b"k"), 6)

    client.decr(b"k", 2)
    tools.assert_equal(client.get(b"k"), 4)
