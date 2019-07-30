Getting started!
================
A comprehensive, fast, pure-Python memcached client library.

Basic Usage
------------

.. code-block:: python

    from pymemcache.client.base import Client

    client = Client(('localhost', 11211))
    client.set('some_key', 'some_value')
    result = client.get('some_key')

Using UNIX domain sockets
-------------------------
You can also connect to a local memcached server over a UNIX domain socket by
passing the socket's path to the client's ``server`` parameter:

.. code-block:: python

    from pymemcache.client.base import Client

    client = Client('/var/run/memcached/memcached.sock')

Using a memcached cluster
-------------------------
This will use a consistent hashing algorithm to choose which server to
set/get the values from. It will also automatically rebalance depending
on if a server goes down.

.. code-block:: python

    from pymemcache.client.hash import HashClient

    client = HashClient([
        ('127.0.0.1', 11211),
        ('127.0.0.1', 11212)
    ])
    client.set('some_key', 'some value')
    result = client.get('some_key')

Serialization
--------------

.. code-block:: python

     import json
     from pymemcache.client.base import Client

     class JsonSerializer(object):
         def serialize(self, key, value):
             if isinstance(value, str):
                 return value, 1
             return json.dumps(value), 2

         def deserialize(self, key, value, flags):
            if flags == 1:
                return value
            if flags == 2:
                return json.loads(value)
            raise Exception("Unknown serialization format")

     client = Client(('localhost', 11211), serializer=JsonSerializer())
     client.set('key', {'a':'b', 'c':'d'})
     result = client.get('key')

pymemcache provides a default
`pickle <https://docs.python.org/3/library/pickle.html>`_-based serializer:

.. code-block:: python

    from pymemcache.client.base import Client
    from pymemcache import serde

    class Foo(object):
      pass

    client = Client(('localhost', 11211),
        serializer=serde.python_memcache_serializer)
    client.set('key', Foo())
    result client.get('key')

The serializer uses the highest pickle protocol available. In order to make
sure multiple versions of Python can read the protocol version, you can specify
the version by explicitly instantiating :class:`pymemcache.serde.PythonMemcacheSerializer`:

.. code-block:: python

    client = Client(
        ('localhost', 11211),
        serializer=serde.PythonMemcacheSerializer(pickle_version=2)
    )


Deserialization with Python 3
-----------------------------

Values passed to the `deserialize()` method will be bytestrings. It is
therefore necessary to encode and decode them correctly. Here's a version of
the `JsonSerializer` from above which is more careful with encodings:

.. code-block:: python

     class JsonSerializer(object):
         def serialize(self, key, value):
             if isinstance(value, str):
                 return value.encode('utf-8'), 1
             return json.dumps(value).encode('utf-8'), 2

         def deserialize(self, key, value, flags):
            if flags == 1:
                return value.decode('utf-8')
            if flags == 2:
                return json.loads(value.decode('utf-8'))
            raise Exception("Unknown serialization format")


Key Constraints
---------------
This client implements the ASCII protocol of memcached. This means keys should not
contain any of the following illegal characters:

   Keys cannot have spaces, new lines, carriage returns, or null characters.
   We suggest that if you have unicode characters, or long keys, you use an
   effective hashing mechanism before calling this client.

At Pinterest, we have found that murmur3 hash is a great candidate for this.
Alternatively you can set `allow_unicode_keys` to support unicode keys, but
beware of what unicode encoding you use to make sure multiple clients can find
the same key.

Best Practices
---------------

 - Always set the ``connect_timeout`` and ``timeout`` arguments in the
   :py:class:`pymemcache.client.base.Client` constructor to avoid blocking
   your process when memcached is slow. You might also want to enable the
   ``no_delay`` option, which sets the TCP_NODELAY flag on the connection's
   socket.
 - Use the ``noreply`` flag for a significant performance boost. The ``noreply``
   flag is enabled by default for "set", "add", "replace", "append", "prepend",
   and "delete". It is disabled by default for "cas", "incr" and "decr". It
   obviously doesn't apply to any get calls.
 - Use :func:`pymemcache.client.base.Client.get_many` and
   :func:`pymemcache.client.base.Client.gets_many` whenever possible, as they
   result in fewer round trip times for fetching multiple keys.
 - Use the ``ignore_exc`` flag to treat memcache/network errors as cache misses
   on calls to the get* methods. This prevents failures in memcache, or network
   errors, from killing your web requests. Do not use this flag if you need to
   know about errors from memcache, and make sure you have some other way to
   detect memcache server failures.
 - Unless you have a known reason to do otherwise, use the provided serializer
   in `pymemcache.serde.python_memcache_serializer` for any serialization or
   deserialization

.. WARNING::

    ``noreply`` will not read errors returned from the memcached server.

    If a function with ``noreply=True`` causes an error on the server, it will
    still succeed and your next call which reads a response from memcached may
    fail unexpectedly.

    ``pymemcached`` will try to catch and stop you from sending malformed
    inputs to memcached, but if you are having unexplained errors, setting
    ``noreply=False`` may help you troubleshoot the issue.
