import operator
import socket

from pymemcache.client import Client
from pymemcache.client.hash import HashClient
from pymemcache.client.rendezvous import RendezvousHash


class AWSElastiCacheHashClient(HashClient):
    """
    This class is a subclass of HashClient and represents a client for interacting with an AWS ElastiCache cluster
    using a hash-based algorithm for key distribution.

    Supports version 1.4.14 or higher
    """

    def __init__(
            self,
            cfg_node,
            hasher=RendezvousHash,
            serde=None,
            serializer=None,
            deserializer=None,
            connect_timeout=None,
            timeout=None,
            no_delay=False,
            socket_module=socket,
            socket_keepalive=None,
            key_prefix=b"",
            max_pool_size=None,
            pool_idle_timeout=0,
            lock_generator=None,
            retry_attempts=2,
            retry_timeout=1,
            dead_timeout=60,
            use_pooling=False,
            ignore_exc=False,
            allow_unicode_keys=False,
            default_noreply=True,
            encoding="ascii",
            tls_context=None,
    ):
        servers = self._retrieve_cluster_config(f'{cfg_node}', connect_timeout=connect_timeout)

        super().__init__(
            servers=servers,
            hasher=hasher,
            serde=serde,
            serializer=serializer,
            deserializer=deserializer,
            connect_timeout=connect_timeout,
            timeout=timeout,
            no_delay=no_delay,
            socket_module=socket_module,
            socket_keepalive=socket_keepalive,
            key_prefix=key_prefix,
            max_pool_size=max_pool_size,
            pool_idle_timeout=pool_idle_timeout,
            lock_generator=lock_generator,
            retry_attempts=retry_attempts,
            retry_timeout=retry_timeout,
            dead_timeout=dead_timeout,
            use_pooling=use_pooling,
            ignore_exc=ignore_exc,
            allow_unicode_keys=allow_unicode_keys,
            default_noreply=default_noreply,
            encoding=encoding,
            tls_context=tls_context,
        )

    @staticmethod
    def _retrieve_cluster_config(cfg_node: str, connect_timeout: int) -> list[tuple[str, int]]:
        addr, port = cfg_node.split(':')
        client = Client(addr, port, connect_timeout=connect_timeout)

        try:
            *_, config_line = client.raw_command(
                b'config get cluster',
                end_tokens=b'\n\r\nEND\r\n',
                # https://docs.aws.amazon.com/AmazonElastiCache/latest/mem-ug/AutoDiscovery.AddingToYourClientLibrary.html
            ).splitlines()
        finally:
            client.disconnect_all()

        servers = [
            (hostname, port)
            # using hostname and port assuming that client use non-vpc installation (more compatible)
            # see https://docs.aws.amazon.com/AmazonElastiCache/latest/mem-ug/ClientConfig.DNS.html
            for hostname, _, port in map(
                operator.methodcaller('split', '|'),
                config_line.decode().split(' '),
            )
        ]

        return servers

