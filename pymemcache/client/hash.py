import socket
import zlib
from pymemcache.client.base import Client, PooledClient
from clandestined import RendezvousHash as RH


class RendezvousHash(RH):
    def get_node(self, key):
        return self.find_node(key)


class HashClient(object):
    """
    A client for communicating with a cluster of memcached servers
    """
    def __init__(
        self,
        servers,
        hasher=None,
        serializer=None,
        connect_timeout=None,
        timeout=None,
        no_delay=False,
        ignore_exc=False,
        socket_module=socket,
        key_prefix=b'',
        max_pool_size=None,
        lock_generator=None,
        use_pooling=False,
    ):
        """
        Args:
          servers: list(tuple(hostname, port))
          serializer: optional class with ``serialize`` and ``deserialize``
                      functions.
          hasher: optional class three functions ``get_node``, ``add_node``, and
                  ``remove_node``

                  defaults to crc32 hash.
          use_pooling: use py:class:`.PooledClient` as the default underlying
                       class. ``max_pool_size`` and ``lock_generator`` can
                       be used with this. default: False

        Further arguments are interpreted as for :py:class:`.Client`
        constructor.
        """
        self.clients = {}

        if hasher is None:
            self.hasher = RendezvousHash()

        for server, port in servers:
            key = '%s:%s' % (server, port)
            kwargs = {
                'connect_timeout': connect_timeout,
                'timeout': timeout,
                'no_delay': no_delay,
                'ignore_exc': ignore_exc,
                'socket_module': socket_module,
                'key_prefix': key_prefix,
            }

            if serializer is not None:
                kwargs['serializer'] = serializer.serialize
                kwargs['deserializer'] = serializer.deserialize

            if use_pooling is True:
                kwargs.update({
                    'max_pool_size': max_pool_size,
                    'lock_generator': lock_generator
                })

                client = PooledClient(
                    (server, port),
                    **kwargs
                )
            else:
                client = Client((server, port))

            self.clients[key] = client
            self.hasher.add_node(key)

    def _get_client(self, key):
        server = self.hasher.get_node(key)
        print('got server %s' % server)
        client = self.clients[server]
        return client

    def set(self, key, *args, **kwargs):
        client = self._get_client(key)
        return client.set(key, *args, **kwargs)

    def get(self, key, *args, **kwargs):
        client = self._get_client(key)
        return client.get(key, *args, **kwargs)
