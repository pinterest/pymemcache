Changelog
=========

New in version 3.5.0
--------------------
* Sockets are now closed on ``MemcacheUnexpectedCloseError``.
* Added support for TCP keepalive for client sockets on Linux platforms.
* Added retrying mechanisms by wrapping clients.

New in version 3.4.4
--------------------
* Idle connections will be removed from the pool after ``pool_idle_timeout``.

New in version 3.4.3
--------------------
* Fix ``HashClient.{get,set}_many()`` with UNIX sockets.

New in version 3.4.2
--------------------
* Remove trailing space for commands that don't take arguments, such as
  ``stats``. This was a violation of the memcached protocol.

New in version 3.4.1
--------------------
* CAS operations will now raise ``MemcacheIllegalInputError`` when ``None`` is
  given as the ``cas`` value.

New in version 3.4.0
--------------------
* Added IPv6 support for TCP socket connections. Note that IPv6 may be used in
  preference to IPv4 when passing a domain name as the host if an IPv6 address
  can be resolved for that domain.
* ``HashClient`` now supports UNIX sockets.

New in version 3.3.0
--------------------
* ``HashClient`` can now be imported from the top-level ``pymemcache`` package
  (e.g. ``pymemcache.HashClient``).
* ``HashClient.get_many()`` now longer stores ``False`` for missing keys from
  unavailable clients. Instead, the result won't contain the key at all.
* Added missing ``HashClient.close()`` and ``HashClient.quit()``.

New in version 3.2.0
--------------------
* ``PooledClient`` and ``HashClient`` now support custom ``Client`` classes

New in version 3.1.1
--------------------
* Improve ``MockMemcacheClient`` to behave even more like ``Client``

New in version 3.1.0
--------------------
* Add TLS support for TCP sockets.
* Fix corner case when dead hashed server comes back alive.

New in version 3.0.1
--------------------
* Make MockMemcacheClient more consistent with the real client.
* Pass ``encoding`` from HashClient to its pooled clients when ``use_pooling``
  is enabled.

New in version 3.0.0
--------------------
* The serialization API has been reworked. Instead of consuming a serializer
  and deserializer as separate arguments, client objects now expect an argument
  ``serde`` to be an object which implements ``serialize`` and ``deserialize``
  as methods. (``serialize`` and ``deserialize`` are still supported but
  considered deprecated.)
* Validate integer inputs for ``expire``, ``delay``, ``incr``, ``decr``, and
  ``memlimit`` -- non-integer values now raise ``MemcacheIllegalInputError``
* Validate inputs for ``cas`` -- values which are not integers or strings of
  0-9 now raise ``MemcacheIllegalInputError``
* Add ``prepend`` and ``append`` support to ``MockMemcacheClient``.
* Add the ``touch`` method to ``HashClient``.
* Added official support for Python 3.8.

New in version 2.2.2
--------------------
* Fix ``long_description`` string in Python packaging.

New in version 2.2.1
--------------------
* Fix ``flags`` when setting multiple differently-typed values at once.

New in version 2.2.0
--------------------
* Drop official support for Python 3.4.
* Use ``setup.cfg`` metadata instead ``setup.py`` config to generate package.
* Add ``default_noreply`` parameter to ``HashClient``.
* Add ``encoding`` parameter to ``Client`` constructors (defaults to ``ascii``).
* Add ``flags`` parameter to write operation methods.
* Handle unicode key values in ``MockMemcacheClient`` correctly.
* Improve ASCII encoding failure exception.

New in version 2.1.1
--------------------
* Fix ``setup.py`` dependency on six already being installed.

New in version 2.1.0
--------------------
* Public classes and exceptions can now be imported from the top-level
  ``pymemcache`` package (e.g. ``pymemcache.Client``).
  `#197 <https://github.com/pinterest/pymemcache/pull/197>`_
* Add UNIX domain socket support and document server connection options.
  `#206 <https://github.com/pinterest/pymemcache/pull/206>`_
* Add support for the ``cache_memlimit`` command.
  `#211 <https://github.com/pinterest/pymemcache/pull/211>`_
* Commands key are now always sent in their original order.
  `#209 <https://github.com/pinterest/pymemcache/pull/209>`_
  
New in version 2.0.0
--------------------
* Change set_many and set_multi api return value. `#179 <https://github.com/pinterest/pymemcache/pull/179>`_
* Fix support for newbytes from python-future. `#187 <https://github.com/pinterest/pymemcache/pull/187>`_
* Add support for Python 3.7, and drop support for Python 3.3
* Properly batch Client.set_many() call. `#182 <https://github.com/pinterest/pymemcache/pull/182>`_
* Improve _check_key() and _store_cmd() performance. `#183 <https://github.com/pinterest/pymemcache/pull/183>`_
* Properly batch Client.delete_many() call. `#184 <https://github.com/pinterest/pymemcache/pull/184>`_
* Add option to explicitly set pickle version used by serde. `#190 <https://github.com/pinterest/pymemcache/pull/190>`_

New in version 1.4.4
--------------------
* pypy3 to travis test matrix
* full benchmarks in test
* fix flake8 issues
* Have mockmemcacheclient support non-ascii strings
* Switch from using pickle format 0 to the highest available version. See `#156 <https://github.com/pinterest/pymemcache/pull/156>`_

  *Warning*: different versions of python have different highest pickle versions: https://docs.python.org/3/library/pickle.html


New in version 1.4.3
--------------------
* Documentation improvements
* Fixed cachedump stats command, see `#103 <https://github.com/pinterest/pymemcache/issues/103>`_
* Honor default_value in HashClient

New in version 1.4.2
--------------------
* Drop support for python 2.6, see `#109 <https://github.com/pinterest/pymemcache/issues/139>`_

New in version 1.4.1
--------------------
* Python 3 serializations fixes `#131 <https://github.com/pinterest/pymemcache/pull/131>`_
* Drop support for pypy3
* Comment cleanup
* Add gets_many to hash_client
* Better checking for illegal chars in key

New in version 1.4.0
--------------------
* Unicode keys support. It is now possible to pass the flag ``allow_unicode_keys`` when creating the clients, thanks @jogo!
* Fixed a bug where PooledClient wasn't following ``default_noreply`` arg set on init, thanks @kols!
* Improved documentation

New in version 1.3.8
--------------------
* use cpickle instead of pickle when possible (python2)

New in version 1.3.7
--------------------
* default parameter on get(key, default=0)
* fixed docs to autogenerate themselves with sphinx
* fix linter to work with python3
* improve error message on illegal Input for the key
* refactor stat parsing
* fix MockMemcacheClient
* fix unicode char in middle of key bug

New in version 1.3.6
--------------------
* Fix flake8 and cleanup tox building
* Fix security vulnerability by sanitizing key input

New in version 1.3.5
--------------------
* Bug fix for HashClient when retries is set to zero.
* Adding the VERSION command to the clients.

New in version 1.3.4
--------------------
* Bug fix for the HashClient that corrects behavior when there are no working servers.

New in version 1.3.3
--------------------
* Adding caching to the Travis build.
* A bug fix for pluggable hashing in HashClient.
* Adding a default_noreply argument to the Client ctor.

New in version 1.3.2
--------------------
* Making the location of Memcache Exceptions backwards compatible.

New in version 1.3.0
--------------------
* Python 3 Support
* Introduced HashClient that uses consistent hasing for allocating keys across many memcached nodes. It also can detect servers going down and rebalance keys across the available nodes.
* Retry sock.recv() when it raises EINTR

New in version 1.2.9
--------------------
* Introduced PooledClient a thread-safe pool of clients
