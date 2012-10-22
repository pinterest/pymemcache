pymemcache
==========

NOTE: this is still BETA, use with caution!

A comprehensive, fast, pure-Python memcached client.

pymemcache supports the following features:

* Complete implementation of the memcached text protocol.
* Configurable timeouts for socket connect and send/recv calls.
* Access to the "noreply" flag, which can significantly increase speed.
* Flexible, simple approach to serialization and deserialization.
* The (optional) ability to treat network and memcached errors as cache misses.

Installing pymemcache
=====================

You can install pymemcache manually, with Nose tests, by doing the following:

    git clone https://github.com/pinterest/pymemcache.git
    cd pymemcache
    python setup.py nosetests
    sudo python setup.py install

You can also use pip:

    sudo pip install https://github.com/pinterest/pymemcache.git

Comparison with Other Libraries
===============================

pylibmc
-------

The pylibmc library is a wrapper around libmemcached, implemented in C. It is
fast, implements consistent hashing, the full memcached protocol and timeouts.
It does not provide access to the "noreply" flag, and it doesn't provide a
built-in API for serialization and deserialization. It also isn't pure Python,
so using it with libraries like gevent is out of the question.

Python-memcache
---------------

The python-memcache library implements the entire memcached text protocol, has
a single timeout for all socket calls and has a flexible approach to
serialization and deserialization. It is also written entirely in Python, so
it works well with libraries like gevent. However, it is tied to using thread
locals, doesn't implement "noreply", can't treat errors as cache misses and is
slower than both pylibmc and pymemcache. It is also tied to a specific method
for handling clusters of memcached servers.

memcache_client
---------------

The team at mixpanel put together a pure Python memcached client as well. It
has more fine grained support for socket timeouts, only connects to a single
host. However, it doesn't support most of the memcached API (just get, set,
delete and stats), doesn't support "noreply", has no serialization or
deserialization support and can't treat errors as cache misses.

External Links
==============
