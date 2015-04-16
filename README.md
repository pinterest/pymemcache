pymemcache
==========

[![Build
Status](https://travis-ci.org/pinterest/pymemcache.png)](https://travis-ci.org/pinterest/pymemcache)

A comprehensive, fast, pure-Python memcached client.

pymemcache supports the following features:

* Complete implementation of the memcached text protocol.
* Configurable timeouts for socket connect and send/recv calls.
* Access to the "noreply" flag, which can significantly increase the speed of writes.
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

Usage
=====

See the module documentation in pymemcache.client for details.

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

The memcached text protocol reference page:
  https://github.com/memcached/memcached/blob/master/doc/protocol.txt

The python-memcached library (another pure-Python library):
  http://www.tummy.com/Community/software/python-memcached/

Mixpanel's Blog post about their memcached client for Python:
  http://code.mixpanel.com/2012/07/16/we-went-down-so-we-wrote-a-better-pure-python-memcache-client/

Mixpanel's pure Python memcached client:
  https://github.com/mixpanel/memcache_client

Credits
=======

* [Charles Gordon](http://github.com/cgordon)
* [Dave Dash](http://github.com/davedash)
* [Dan Crosta](http://github.com/dcrosta)
* [Julian Berman](http://github.com/Julian)
* [Mark Shirley](http://github.com/maspwr)
* [Tim Bart](http://github.com/pims)
* [Thomas Orozco](http://github.com/krallin)
* [Marc Abramowitz](http://github.com/msabramo)
* [Marc-Andre Courtois](http://github.com/mcourtois)
* [Julien Danjou](http://github.com/jd)
* [INADA Naoki](http://github.com/methane)
* [James Socol](http://github.com/jsocol)
* [Joshua Harlow](http://github.com/harlowja)
