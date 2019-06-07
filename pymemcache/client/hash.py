import socket
import time
import logging
import six

from pymemcache.client.base import Client, PooledClient, _check_key
from pymemcache.client.rendezvous import RendezvousHash
from pymemcache.exceptions import MemcacheError

logger = logging.getLogger(__name__)


class HashClient(object):
    """
    A client for communicating with a cluster of memcached servers
    """

    def __init__(
        self,
        servers,
        hasher=RendezvousHash,
        serializer=None,
        deserializer=None,
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
        ignore_exc=False,
        allow_unicode_keys=False,
        default_noreply=True,
        encoding='ascii'
    ):
        """
        Constructor.

        Args:
          servers: list(tuple(hostname, port))
          hasher: optional class three functions ``get_node``, ``add_node``,
                  and ``remove_node``
                  defaults to Rendezvous (HRW) hash.

          use_pooling: use py:class:`.PooledClient` as the default underlying
                       class. ``max_pool_size`` and ``lock_generator`` can
                       be used with this. default: False

          retry_attempts: Amount of times a client should be tried before it
                          is marked dead and removed from the pool.
          retry_timeout (float): Time in seconds that should pass between retry
                                 attempts.
          dead_timeout (float): Time in seconds before attempting to add a node
                                back in the pool.
          encoding: optional str, controls data encoding (defaults to 'ascii').

        Further arguments are interpreted as for :py:class:`.Client`
        constructor.
        """
        self.clients = {}
        self.retry_attempts = retry_attempts
        self.retry_timeout = retry_timeout
        self.dead_timeout = dead_timeout
        self.use_pooling = use_pooling
        self.key_prefix = key_prefix
        self.ignore_exc = ignore_exc
        self.allow_unicode_keys = allow_unicode_keys
        self._failed_clients = {}
        self._dead_clients = {}
        self._last_dead_check_time = time.time()

        self.hasher = hasher()

        self.default_kwargs = {
            'connect_timeout': connect_timeout,
            'timeout': timeout,
            'no_delay': no_delay,
            'socket_module': socket_module,
            'key_prefix': key_prefix,
            'serializer': serializer,
            'deserializer': deserializer,
            'allow_unicode_keys': allow_unicode_keys,
            'default_noreply': default_noreply,
        }

        if use_pooling is True:
            self.default_kwargs.update({
                'max_pool_size': max_pool_size,
                'lock_generator': lock_generator
            })

        for server, port in servers:
            self.add_server(server, port)
        self.encoding = encoding

    def add_server(self, server, port):
        key = '%s:%s' % (server, port)

        if self.use_pooling:
            client = PooledClient(
                (server, port),
                **self.default_kwargs
            )
        else:
            client = Client((server, port), **self.default_kwargs)

        self.clients[key] = client
        self.hasher.add_node(key)

    def remove_server(self, server, port):
        dead_time = time.time()
        self._failed_clients.pop((server, port))
        self._dead_clients[(server, port)] = dead_time
        key = '%s:%s' % (server, port)
        self.hasher.remove_node(key)

    def _get_client(self, key):
        _check_key(key, self.allow_unicode_keys, self.key_prefix)
        if len(self._dead_clients) > 0:
            current_time = time.time()
            ldc = self._last_dead_check_time
            # we have dead clients and we have reached the
            # timeout retry
            if current_time - ldc > self.dead_timeout:
                for server, dead_time in self._dead_clients.items():
                    if current_time - dead_time > self.dead_timeout:
                        logger.debug(
                            'bringing server back into rotation %s',
                            server
                        )
                        self.add_server(*server)
                        self._last_dead_check_time = current_time

        server = self.hasher.get_node(key)
        # We've ran out of servers to try
        if server is None:
            if self.ignore_exc is True:
                return
            raise MemcacheError('All servers seem to be down right now')

        client = self.clients[server]
        return client

    def _safely_run_func(self, client, func, default_val, *args, **kwargs):
        try:
            if client.server in self._failed_clients:
                # This server is currently failing, lets check if it is in
                # retry or marked as dead
                failed_metadata = self._failed_clients[client.server]

                # we haven't tried our max amount yet, if it has been enough
                # time lets just retry using it
                if failed_metadata['attempts'] < self.retry_attempts:
                    failed_time = failed_metadata['failed_time']
                    if time.time() - failed_time > self.retry_timeout:
                        logger.debug(
                            'retrying failed server: %s', client.server
                        )
                        result = func(*args, **kwargs)
                        # we were successful, lets remove it from the failed
                        # clients
                        self._failed_clients.pop(client.server)
                        return result
                    return default_val
                else:
                    # We've reached our max retry attempts, we need to mark
                    # the sever as dead
                    logger.debug('marking server as dead: %s', client.server)
                    self.remove_server(*client.server)

            result = func(*args, **kwargs)
            return result

        # Connecting to the server fail, we should enter
        # retry mode
        except socket.error:
            self._mark_failed_server(client.server)

            # if we haven't enabled ignore_exc, don't move on gracefully, just
            # raise the exception
            if not self.ignore_exc:
                raise

            return default_val
        except Exception:
            # any exceptions that aren't socket.error we need to handle
            # gracefully as well
            if not self.ignore_exc:
                raise

            return default_val

    def _safely_run_set_many(self, client, values, *args, **kwargs):
        failed = []
        succeeded = []
        try:
            if client.server in self._failed_clients:
                # This server is currently failing, lets check if it is in
                # retry or marked as dead
                failed_metadata = self._failed_clients[client.server]

                # we haven't tried our max amount yet, if it has been enough
                # time lets just retry using it
                if failed_metadata['attempts'] < self.retry_attempts:
                    failed_time = failed_metadata['failed_time']
                    if time.time() - failed_time > self.retry_timeout:
                        logger.debug(
                            'retrying failed server: %s', client.server
                        )
                        succeeded, failed, err = self._set_many(
                            client, values, *args, **kwargs)
                        if err is not None:
                            raise err
                        # we were successful, lets remove it from the failed
                        # clients
                        self._failed_clients.pop(client.server)
                        return failed
                    return values.keys()
                else:
                    # We've reached our max retry attempts, we need to mark
                    # the sever as dead
                    logger.debug('marking server as dead: %s', client.server)
                    self.remove_server(*client.server)

            succeeded, failed, err = self._set_many(
                client, values, *args, **kwargs
            )
            if err is not None:
                raise err

            return failed

        # Connecting to the server fail, we should enter
        # retry mode
        except socket.error:
            self._mark_failed_server(client.server)

            # if we haven't enabled ignore_exc, don't move on gracefully, just
            # raise the exception
            if not self.ignore_exc:
                raise

            return list(set(values.keys()) - set(succeeded))
        except Exception:
            # any exceptions that aren't socket.error we need to handle
            # gracefully as well
            if not self.ignore_exc:
                raise

            return list(set(values.keys()) - set(succeeded))

    def _mark_failed_server(self, server):
        # This client has never failed, lets mark it for failure
        if (
                server not in self._failed_clients and
                self.retry_attempts > 0
        ):
            self._failed_clients[server] = {
                'failed_time': time.time(),
                'attempts': 0,
            }
        # We aren't allowing any retries, we should mark the server as
        # dead immediately
        elif (
            server not in self._failed_clients and
            self.retry_attempts <= 0
        ):
            self._failed_clients[server] = {
                'failed_time': time.time(),
                'attempts': 0,
            }
            logger.debug("marking server as dead %s", server)
            self.remove_server(*server)
        # This client has failed previously, we need to update the metadata
        # to reflect that we have attempted it again
        else:
            failed_metadata = self._failed_clients[server]
            failed_metadata['attempts'] += 1
            failed_metadata['failed_time'] = time.time()
            self._failed_clients[server] = failed_metadata

    def _run_cmd(self, cmd, key, default_val, *args, **kwargs):
        client = self._get_client(key)

        if client is None:
            return default_val

        func = getattr(client, cmd)
        args = list(args)
        args.insert(0, key)
        return self._safely_run_func(
            client, func, default_val, *args, **kwargs
        )

    def _set_many(self, client, values, *args, **kwargs):
        failed = []
        succeeded = []

        try:
            for key, value in six.iteritems(values):
                result = client.set(key, value, *args, **kwargs)
                if result:
                    succeeded.append(key)
                else:
                    failed.append(key)
        except Exception as e:
            return succeeded, failed, e

        return succeeded, failed, None

    def set(self, key, *args, **kwargs):
        return self._run_cmd('set', key, False, *args, **kwargs)

    def get(self, key, *args, **kwargs):
        return self._run_cmd('get', key, None, *args, **kwargs)

    def incr(self, key, *args, **kwargs):
        return self._run_cmd('incr', key, False, *args, **kwargs)

    def decr(self, key, *args, **kwargs):
        return self._run_cmd('decr', key, False, *args, **kwargs)

    def set_many(self, values, *args, **kwargs):
        client_batches = {}
        failed = []

        for key, value in six.iteritems(values):
            client = self._get_client(key)

            if client is None:
                failed.append(key)
                continue

            if client.server not in client_batches:
                client_batches[client.server] = {}

            client_batches[client.server][key] = value

        for server, values in client_batches.items():
            client = self.clients['%s:%s' % server]

            failed += self._safely_run_set_many(
                client, values, *args, **kwargs
            )

        return failed

    set_multi = set_many

    def get_many(self, keys, gets=False, *args, **kwargs):
        client_batches = {}
        end = {}

        for key in keys:
            client = self._get_client(key)

            if client is None:
                end[key] = False
                continue

            if client.server not in client_batches:
                client_batches[client.server] = []

            client_batches[client.server].append(key)

        for server, keys in client_batches.items():
            client = self.clients['%s:%s' % server]
            new_args = list(args)
            new_args.insert(0, keys)

            if gets:
                get_func = client.gets_many
            else:
                get_func = client.get_many

            result = self._safely_run_func(
                client,
                get_func, {}, *new_args, **kwargs
            )
            end.update(result)

        return end

    get_multi = get_many

    def gets(self, key, *args, **kwargs):
        return self._run_cmd('gets', key, None, *args, **kwargs)

    def gets_many(self, keys, *args, **kwargs):
        return self.get_many(keys, gets=True, *args, **kwargs)

    gets_multi = gets_many

    def add(self, key, *args, **kwargs):
        return self._run_cmd('add', key, False, *args, **kwargs)

    def prepend(self, key, *args, **kwargs):
        return self._run_cmd('prepend', key, False, *args, **kwargs)

    def append(self, key, *args, **kwargs):
        return self._run_cmd('append', key, False, *args, **kwargs)

    def delete(self, key, *args, **kwargs):
        return self._run_cmd('delete', key, False, *args, **kwargs)

    def delete_many(self, keys, *args, **kwargs):
        for key in keys:
            self._run_cmd('delete', key, False, *args, **kwargs)
        return True

    delete_multi = delete_many

    def cas(self, key, *args, **kwargs):
        return self._run_cmd('cas', key, False, *args, **kwargs)

    def replace(self, key, *args, **kwargs):
        return self._run_cmd('replace', key, False, *args, **kwargs)

    def flush_all(self):
        for _, client in self.clients.items():
            self._safely_run_func(client, client.flush_all, False)
