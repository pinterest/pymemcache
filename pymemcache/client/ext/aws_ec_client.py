import logging
import operator
import socket
import re
import ssl
import time
from typing import Any, Callable, Optional

from pymemcache import MemcacheUnknownCommandError
from pymemcache.client import Client
from pymemcache.client.base import KeepaliveOpts, normalize_server_spec
from pymemcache.client.hash import HashClient
from pymemcache.client.rendezvous import RendezvousHash


logger = logging.getLogger(__name__)

_RE_AWS_ENDPOINT = re.compile(
    r"^(?:(?:[\w\d-]{0,61}[\w\d]\.)+[\w]{1,6}|\[(?:[\d]{1,3}\.){3}[\d]{1,3}\])\:\d{1,5}$"
)


class AWSElastiCacheHashClient(HashClient):
    """
    This class is a subclass of HashClient and represents a client for interacting with an AWS ElastiCache cluster
    using a hash-based algorithm for key distribution.

    *Connection *

    Supports version 1.4.14 or higher

    Example:
        >>> client = AWSElastiCacheServerlessClient('cluster.abcxyz.cfg.use1.cache.amazonaws.com')
    """

    def __init__(
        self,
        cfg_node: str,
        hasher: Callable[[], Any] = RendezvousHash,
        serde: Any = None,
        serializer: Optional[Callable[..., Any]] = None,
        deserializer: Optional[Callable[..., Any]] = None,
        connect_timeout: Optional[float] = None,
        timeout: Optional[float] = None,
        no_delay: bool = False,
        socket_module: Any = socket,
        socket_keepalive: Optional[KeepaliveOpts] = None,
        key_prefix: bytes = b"",
        max_pool_size: Optional[int] = None,
        pool_idle_timeout: int = 0,
        lock_generator: Optional[Callable[[], Any]] = None,
        retry_attempts: int = 2,
        retry_timeout: int = 1,
        dead_timeout: int = 60,
        use_pooling: bool = False,
        ignore_exc: bool = False,
        allow_unicode_keys: bool = False,
        default_noreply: bool = True,
        encoding: str = "ascii",
        tls_context: Optional[ssl.SSLContext] = None,
        use_vpc: bool = True,
    ) -> None:
        """
        Constructor.

        Args:
          cfg_node: formatted string containing endpoint and port of the
            ElastiCache cluster endpoint. Ex.:
            `test-cluster.2os1zk.cfg.use1.cache.amazonaws.com:11211`
          serde: optional serializer object, see notes in the class docs.
          serializer: deprecated serialization function
          deserializer: deprecated deserialization function
          connect_timeout: optional float, seconds to wait for a connection to
            the memcached server. Defaults to "forever" (uses the underlying
            default socket timeout, which can be very long).
          timeout: optional float, seconds to wait for send or recv calls on
            the socket connected to memcached. Defaults to "forever" (uses the
            underlying default socket timeout, which can be very long).
          no_delay: optional bool, set the TCP_NODELAY flag, which may help
            with performance in some cases. Defaults to False.
          ignore_exc: optional bool, True to cause the "get", "gets",
            "get_many" and "gets_many" calls to treat any errors as cache
            misses. Defaults to False.
          socket_module: socket module to use, e.g. gevent.socket. Defaults to
            the standard library's socket module.
          socket_keepalive: Activate the socket keepalive feature by passing
            a KeepaliveOpts structure in this parameter. Disabled by default
            (None). This feature is only supported on Linux platforms.
          key_prefix: Prefix of key. You can use this as namespace. Defaults
            to b''.
          default_noreply: bool, the default value for 'noreply' as passed to
            store commands (except from cas, incr, and decr, which default to
            False).
          allow_unicode_keys: bool, support unicode (utf8) keys
          encoding: optional str, controls data encoding (defaults to 'ascii').
          use_vpc: optional bool, if set False (defaults to True), the client
            will use FQDN to connect to nodes instead of IP addresses. See
            AWS Docs for extra info
            https://docs.aws.amazon.com/AmazonElastiCache/latest/mem-ug/ClientConfig.DNS.html

        Notes:
          The constructor does not make a connection to memcached. The first
          call to a method on the object will do that.
        """
        if not (_RE_AWS_ENDPOINT.fullmatch(cfg_node) and isinstance(cfg_node, str)):
            raise ValueError("Invalid AWS ElastiCache endpoint value '%s'" % cfg_node)

        self._cfg_node = cfg_node
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
            "connect_timeout": connect_timeout,
            "timeout": timeout,
            "no_delay": no_delay,
            "socket_module": socket_module,
            "socket_keepalive": socket_keepalive,
            "key_prefix": key_prefix,
            "serde": serde,
            "serializer": serializer,
            "deserializer": deserializer,
            "allow_unicode_keys": allow_unicode_keys,
            "default_noreply": default_noreply,
            "encoding": encoding,
            "tls_context": tls_context,
        }

        if use_pooling is True:
            self.default_kwargs.update(
                {
                    "max_pool_size": max_pool_size,
                    "pool_idle_timeout": pool_idle_timeout,
                    "lock_generator": lock_generator,
                }
            )

        # server config returns as `[fqdn, ip, port]` if it's VPC installation you need to use ip
        self._use_vpc = int(use_vpc)

        self.reconfigure_nodes()

        self.encoding = encoding
        self.tls_context = tls_context

    def reconfigure_nodes(self):
        """
        Reconfigures the nodes in the server cluster based on the provided configuration node.

        May useful on error handling during cluster scale down or scale up
        """
        old_clients = self.clients.copy()
        self.clients.clear()

        for server in self._get_nodes_list():
            self.add_server(normalize_server_spec(server))

        for client in old_clients.values():
            client.close()

    def _get_nodes_list(self) -> list[tuple[str, int]]:
        """
        Get the list of nodes from the cluster configuration.

        Returns:
            A list of tuples containing the address and port of each node in the cluster.
            Each tuple has the format (address: str, port: int).
        """
        addr, port = self._cfg_node.rsplit(":", maxsplit=1)
        client = Client((addr, int(port)), **self.default_kwargs)

        # https://docs.aws.amazon.com/AmazonElastiCache/latest/mem-ug/AutoDiscovery.AddingToYourClientLibrary.html
        try:
            *_, config_line = client.raw_command(
                b"config get cluster",
                end_tokens=b"\n\r\nEND\r\n",
            ).splitlines()
        except MemcacheUnknownCommandError:
            logger.exception(
                "Can't retrieve cluster configuration from '%s:%s' "
                "Seems like it is ElastiCache Serverless or even isn't ElastiCache at all.",
                client.server,
            )
        finally:
            client.close()

        servers = [
            (server[self._use_vpc], server[2])
            for server in map(
                operator.methodcaller("split", "|"),
                config_line.decode().split(" "),
            )
        ]

        logger.debug("Got the next nodes from cluster config: %s", servers)

        return servers
