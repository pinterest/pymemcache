Getting started!
================
A comprehensive, fast, pure-Python memcached client library.

Basic Usage
------------

.. code-block:: python

    from pymemcache.client.base import Client

    client = Client('localhost')
    client.set('some_key', 'some_value')
    result = client.get('some_key')

The server to connect to can be specified in a number of ways.

If using TCP connections over IPv4 or IPv6, the ``server`` parameter can be
passed a ``host`` string, a ``host:port`` string, or a ``(host, port)``
2-tuple. The host part may be a domain name, an IPv4 address, or an IPv6
address. The port may be omitted, in which case it will default to ``11211``.

.. code-block:: python

    ipv4_client = Client('127.0.0.1')
    ipv4_client_with_port = Client('127.0.0.1:11211')
    ipv4_client_using_tuple = Client(('127.0.0.1', 11211))

    ipv6_client = Client('[::1]')
    ipv6_client_with_port = Client('[::1]:11211')
    ipv6_client_using_tuple = Client(('::1', 11211))

    domain_client = Client('localhost')
    domain_client_with_port = Client('localhost:11211')
    domain_client_using_tuple = Client(('localhost', 11211))

Note that IPv6 may be used in preference to IPv4 when passing a domain name as
the host if an IPv6 address can be resolved for that domain.

You can also connect to a local memcached server over a UNIX domain socket by
passing the socket's path to the client's ``server`` parameter. An optional
``unix:`` prefix may be used for compatibility in code that uses other client
libraries that require it.

.. code-block:: python

    client = Client('/run/memcached/memcached.sock')
    client_with_prefix = Client('unix:/run/memcached/memcached.sock')

Using a client pool
-------------------
:class:`pymemcache.client.base.PooledClient` is a thread-safe client pool
that provides the same API as :class:`pymemcache.client.base.Client`. It's
useful in for cases when you want to maintain a pool of already-connected
clients for improved performance.

.. code-block:: python

    from pymemcache.client.base import PooledClient

    client = PooledClient('127.0.0.1', max_pool_size=4)

Using a memcached cluster
-------------------------
This will use a consistent hashing algorithm to choose which server to
set/get the values from. It will also automatically rebalance depending
on if a server goes down.

.. code-block:: python

    from pymemcache.client.hash import HashClient

    client = HashClient([
        '127.0.0.1:11211',
        '127.0.0.1:11212',
    ])
    client.set('some_key', 'some value')
    result = client.get('some_key')

Using the built-in retrying mechanism
-------------------------------------
The library comes with retry mechanisms that can be used to wrap all kind of
pymemcache clients. The wrapper allow you to define the exceptions that you want
to handle with retries, which exceptions to exclude, how many attempts to make
and how long to wait between attemots.

The ``RetryingClient`` wraps around any of the other included clients and will
have the same methods. For this example we're just using the base ``Client``.

.. code-block:: python

    from pymemcache.client.base import Client
    from pymemcache.client.retrying import RetryingClient
    from pymemcache.exceptions import MemcacheUnexpectedCloseError

    base_client = Client(("localhost", 11211))
    client = RetryingClient(
        base_client,
        attempts=3,
        retry_delay=0.01,
        retry_for=[MemcacheUnexpectedCloseError]
    )
    client.set('some_key', 'some value')
    result = client.get('some_key')

The above client will attempt each call three times with a wait of 10ms between
each attempt, as long as the exception is a ``MemcacheUnexpectedCloseError``.

Using TLS
---------
**Memcached** `supports <https://github.com/memcached/memcached/wiki/TLS>`_
authentication and encryption via TLS since version **1.5.13**.

A Memcached server running with TLS enabled will only accept TLS connections.

To enable TLS in pymemcache, pass a valid TLS context to the client's
``tls_context`` parameter:

.. code-block:: python

    import ssl
    from pymemcache.client.base import Client

    context = ssl.create_default_context(
        cafile="my-ca-root.crt",
    )

    client = Client('localhost', tls_context=context)
    client.set('some_key', 'some_value')
    result = client.get('some_key')


Serialization
--------------

.. code-block:: python

     import json
     from pymemcache.client.base import Client

     class JsonSerde(object):
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

     client = Client('localhost', serde=JsonSerde())
     client.set('key', {'a':'b', 'c':'d'})
     result = client.get('key')

pymemcache provides a default
`pickle <https://docs.python.org/3/library/pickle.html>`_-based serializer:

.. code-block:: python

    from pymemcache.client.base import Client
    from pymemcache import serde

    class Foo(object):
      pass

    client = Client('localhost', serde=serde.pickle_serde)
    client.set('key', Foo())
    result = client.get('key')

The serializer uses the highest pickle protocol available. In order to make
sure multiple versions of Python can read the protocol version, you can specify
the version by explicitly instantiating :class:`pymemcache.serde.PickleSerde`:

.. code-block:: python

    client = Client('localhost', serde=serde.PickleSerde(pickle_version=2))


Deserialization with Python 3
-----------------------------

Values passed to the `serde.deserialize()` method will be bytestrings. It is
therefore necessary to encode and decode them correctly. Here's a version of
the `JsonSerde` from above which is more careful with encodings:

.. code-block:: python

     class JsonSerde(object):
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


Interacting with pymemcache
---------------------------

For testing purpose pymemcache can be used in an interactive mode by using
the python interpreter or again ipython and tools like tox.

One main advantage of using `tox` to interact with `pymemcache` is that it
comes with it's own virtual environments. It will automatically install
pymemcache and fetch all the needed requirements at run. See the example below:

.. code-block:: shell

   $ podman run --publish 11211:11211 -it --rm --name memcached memcached
   $ tox -e venv -- python
   >>> from pymemcache.client.base import Client
   >>> client = Client('127.0.0.1')
   >>> client.set('some_key', 'some_value')
   True
   >>> client.get('some_key')
   b'some_value'
   >>> print(client.get.__doc__)
        The memcached "get" command, but only for one key, as a convenience.
        Args:
          key: str, see class docs for details.
          default: value that will be returned if the key was not found.
        Returns:
          The value for the key, or default if the key wasn't found.

You can instantiate all the classes and clients offered by pymemcache.

Your client will remain open until you decide to close it or until you decide
to quit your interpreter. It can allow you to see what's happen if your server
is abruptly closed. Below is an by example.

Starting your server:

.. code-block:: shell

   $ podman run --publish 11211:11211 -it --name memcached memcached

Starting your client and set some keys:

.. code-block:: shell

   $ tox -e venv -- python
   >>> from pymemcache.client.base import Client
   >>> client = Client('127.0.0.1')
   >>> client.set('some_key', 'some_value')
   True

Restarting the server:

.. code-block:: shell

   $ podman restart memcached

The previous client is still opened, now try to retrieve some keys:

.. code-block:: shell

   >>> print(client.get('some_key'))
   Traceback (most recent call last):
     File "<stdin>", line 1, in <module>
     File "/home/user/pymemcache/pymemcache/client/base.py", line 535, in get
       return self._fetch_cmd(b'get', [key], False).get(key, default)
     File "/home/user/pymemcache/pymemcache/client/base.py", line 910, in _fetch_cmd
       buf, line = _readline(self.sock, buf)
     File "/home/user/pymemcache/pymemcache/client/base.py", line 1305, in _readline
       raise MemcacheUnexpectedCloseError()
   pymemcache.exceptions.MemcacheUnexpectedCloseError

We can see that the connection has been closed.

You can also pass a command directly from CLI parameters and get output
directly:

.. code-block:: shell

   $ tox -e venv -- python -c "from pymemcache.client.base import Client; client = Client('127.0.01'); print(client.get('some_key'))"
   b'some_value'

This kind of usage is useful for debug sessions or to dig manually into your
server.

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
   in `pymemcache.serde.pickle_serde` for any de/serialization of objects.

.. WARNING::

    ``noreply`` will not read errors returned from the memcached server.

    If a function with ``noreply=True`` causes an error on the server, it will
    still succeed and your next call which reads a response from memcached may
    fail unexpectedly.

    ``pymemcached`` will try to catch and stop you from sending malformed
    inputs to memcached, but if you are having unexplained errors, setting
    ``noreply=False`` may help you troubleshoot the issue.
