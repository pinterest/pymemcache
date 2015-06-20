import socket
import zlib
import time
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
        socket_module=socket,
        key_prefix=b'',
        max_pool_size=None,
        lock_generator=None,
        retry_attempts=2,
        retry_timeout=1,
        dead_timeout=60,
        use_pooling=False,
    ):
        """
        Args:
          servers: list(tuple(hostname, port))
          serializer: optional class with ``serialize`` and ``deserialize``
                      functions.
          hasher: optional class three functions ``get_node``, ``add_node``, and
                  ``remove_node``

                  defaults to Rendezvous (HRW) hash.
          use_pooling: use py:class:`.PooledClient` as the default underlying
                       class. ``max_pool_size`` and ``lock_generator`` can
                       be used with this. default: False

          retry_attempts: Amount of times a client should be tried before it
                          is marked dead and removed from the pool.
          retry_timeout: Time in seconds that should pass between retry
                         attempts.
          dead_timeout: Time in seconds before attempting to add a node back in
                        the pool.

        Further arguments are interpreted as for :py:class:`.Client`
        constructor.
        """
        self.clients = {}
        self.retry_attempts = retry_attempts
        self.retry_timeout = retry_timeout
        self.dead_timeout = dead_timeout
        self.use_pooling = use_pooling
        self._failed_clients = {}

        if hasher is None:
            self.hasher = RendezvousHash()

        self.default_kwargs = {
            'connect_timeout': connect_timeout,
            'timeout': timeout,
            'no_delay': no_delay,
            'socket_module': socket_module,
            'key_prefix': key_prefix,
        }

        if serializer is not None:
            self.default.kwargs.update({
                'serializer': serializer.serialize,
                'deserializer': serializer.deserialize,
            })

        if use_pooling is True:
            self.default_kwargs.update({
                'max_pool_size': max_pool_size,
                'lock_generator': lock_generator
            })

        for server, port in servers:
            self._add_server(server, port)

    def _add_server(self, server, port):
        key = '%s:%s' % (server, port)

        if self.use_pooling:
            client = PooledClient(
                (server, port),
                **self.default_kwargs
            )
        else:
            client = Client((server, port))

        self.clients[key] = client
        self.hasher.add_node(key)

    def _remove_server(self, server, port):
        key = '%s:%s' % (server, port)
        self.hasher.remove_node(key)

    def _get_client(self, key):
        server = self.hasher.get_node(key)
        print('got server %s' % server)
        client = self.clients[server]
        return client

    def _run_cmd(self, cmd, key, *args, **kwargs):
        try:
            can_run = True
            client = self._get_client(key)
            func = getattr(client, cmd)

            if client.server in self._failed_clients:
                # This server is currently failing, lets check if it is in retry
                # or marked as dead
                failed_metadata = self._failed_clients[client.server]

                # we haven't tried our max amount yet, if it has been enough
                # time lets just retry using it
                if failed_metadata['attempts'] < self.retry_attempts:
                    failed_time = failed_metadata['failed_time']
                    if time.time() - failed_time > self.retry_timeout:
                        print(failed_metadata)
                        print('retrying')
                        result = func(key, *args, **kwargs)
                        # we were successful, lets remove it from the failed
                        # clients
                        self._failed_clients.pop(client.server)
                        return result
                    return
                else:
                    # We've reached our max retry attempts, we need to mark
                    # the sever as dead
                    print('marking as dead')
                    self._remove_server(*client.server)

            result = func(key, *args, **kwargs)
            return result

        # Connecting to the server fail, we should enter
        # retry mode
        except socket.error:
            # This client has never failed, lets mark it for failure
            if (
                    client.server not in self._failed_clients and
                    self.retry_attempts > 0
            ):
                self._failed_clients[client.server] = {
                    'failed_time': time.time(),
                    'attempts': 0,
                    'is_dead': False
                }
            # We aren't allowing any retries, we should mark the server as
            # dead immediately
            elif (
                client.server not in self._failed_clients and
                self.retry_attempts < 0
            ):
                self._failed_clients[client.server] = {
                    'failed_time': time.time(),
                    'attempts': 0,
                    'is_dead': True
                }
                print("marking node as dead %s" % client.server)
                self._remove_server(*client.server)
            # This client has failed previously, we need to update the metadata
            # to reflect that we have attempted it again
            else:
                print('incrementing')
                failed_metadata = self._failed_clients[client.server]
                failed_metadata['attempts'] += 1
                failed_metadata['failed_time'] = time.time()
                self._failed_clients[client.server] = failed_metadata

    def set(self, key, *args, **kwargs):
        return self._run_cmd('set', key, *args, **kwargs)

    def get(self, key, *args, **kwargs):
        return self._run_cmd('get', key, *args, **kwargs)
