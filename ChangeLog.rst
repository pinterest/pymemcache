Change Log
==========
New in version 1.4.2
--------------------
* Drop support for python 2.6, see [ticket](https://github.com/pinterest/pymemcache/issues/139)

New in version 1.4.1
--------------------
* Python 3 serializations [fixes](https://github.com/pinterest/pymemcache/pull/131)
* Drop support for pypy3
* Comment cleanup
* Add gets_many to hash_client
* Better checking for illegal chars in key

New in version 1.4.0
--------------------
* Unicode keys support. It is now possible to pass the flag `allow_unicode_keys` when creating the clients, thanks @jogo!
* Fixed a bug where PooledClient wasn't following `default_noreply` arg set on init, thanks @kols!
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

