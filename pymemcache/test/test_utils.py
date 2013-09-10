from nose import tools

from pymemcache.test.utils import MockMemcacheClient


def test_get_set():
    client = MockMemcacheClient()
    tools.assert_equal(client.get("hello"), None)

    client.set("hello", 12)
    tools.assert_equal(client.get("hello"), 12)


def test_get_many_set_many():
    client = MockMemcacheClient()
    client.set("h", 1)

    tools.assert_equal(client.get_many("hello"), {"h" : 1})

    client.set_many(dict(h=1, e=2, l=3))
    tools.assert_equal(client.get_many("hello"), dict(h=1, e=2, l=3))


def test_add():
    client = MockMemcacheClient()

    client.add("k", 2)
    tools.assert_equal(client.get("k"), 2)

    client.add("k", 25)
    tools.assert_equal(client.get("k"), 2)


def test_delete():
    client = MockMemcacheClient()

    client.add("k", 2)
    tools.assert_equal(client.get("k"), 2)

    client.delete("k")
    tools.assert_equal(client.get("k"), None)


def test_incr_decr():
    client = MockMemcacheClient()

    client.add("k", 2)

    client.incr("k", 4)
    tools.assert_equal(client.get("k"), 6)

    client.decr("k", 2)
    tools.assert_equal(client.get("k"), 4)
