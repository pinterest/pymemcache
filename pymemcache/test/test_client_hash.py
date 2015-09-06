from pymemcache.client.hash import HashClient
from pymemcache.client.base import Client, PooledClient
from pymemcache.exceptions import MemcacheUnknownError
from pymemcache import pool

from .test_client import ClientTestMixin, MockSocket
import unittest
import pytest
import mock


class TestHashClient(ClientTestMixin, unittest.TestCase):

    def make_client_pool(self, hostname, mock_socket_values, serializer=None):
        mock_client = Client(hostname, serializer=serializer)
        mock_client.sock = MockSocket(mock_socket_values)
        client = PooledClient(hostname, serializer=serializer)
        client.client_pool = pool.ObjectPool(lambda: mock_client)
        return mock_client

    def make_client(self, *mock_socket_values, **kwargs):
        current_port = 11012
        client = HashClient([], **kwargs)
        ip = '127.0.0.1'

        for vals in mock_socket_values:
            s = '%s:%s' % (ip, current_port)
            c = self.make_client_pool(
                (ip, current_port),
                vals
            )
            client.clients[s] = c
            client.hasher.add_node(s)
            current_port += 1

        return client

    def test_setup_client_without_pooling(self):
        with mock.patch('pymemcache.client.hash.Client') as internal_client:
            client = HashClient([], timeout=999, key_prefix='foo_bar_baz')
            client.add_server('127.0.0.1', '11211')

        assert internal_client.call_args[0][0] == ('127.0.0.1', '11211')
        kwargs = internal_client.call_args[1]
        assert kwargs['timeout'] == 999
        assert kwargs['key_prefix'] == 'foo_bar_baz'

    def test_get_many_all_found(self):
        client = self.make_client(*[
            [b'STORED\r\n', b'VALUE key3 0 6\r\nvalue2\r\nEND\r\n', ],
            [b'STORED\r\n', b'VALUE key1 0 6\r\nvalue1\r\nEND\r\n', ],
        ])

        def get_clients(key):
            if key == b'key3':
                return client.clients['127.0.0.1:11012']
            else:
                return client.clients['127.0.0.1:11013']

        client._get_client = get_clients

        result = client.set(b'key1', b'value1', noreply=False)
        result = client.set(b'key3', b'value2', noreply=False)
        result = client.get_many([b'key1', b'key3'])
        assert result == {b'key1': b'value1', b'key3': b'value2'}

    def test_get_many_some_found(self):
        client = self.make_client(*[
            [b'END\r\n', ],
            [b'STORED\r\n', b'VALUE key1 0 6\r\nvalue1\r\nEND\r\n', ],
        ])

        def get_clients(key):
            if key == b'key3':
                return client.clients['127.0.0.1:11012']
            else:
                return client.clients['127.0.0.1:11013']

        client._get_client = get_clients
        result = client.set(b'key1', b'value1', noreply=False)
        result = client.get_many([b'key1', b'key3'])

        assert result == {b'key1': b'value1'}

    def test_get_many_bad_server_data(self):
        client = self.make_client(*[
            [b'STORED\r\n', b'VAXLUE key3 0 6\r\nvalue2\r\nEND\r\n', ],
            [b'STORED\r\n', b'VAXLUE key1 0 6\r\nvalue1\r\nEND\r\n', ],
        ])

        def get_clients(key):
            if key == b'key3':
                return client.clients['127.0.0.1:11012']
            else:
                return client.clients['127.0.0.1:11013']

        client._get_client = get_clients

        with pytest.raises(MemcacheUnknownError):
            client.set(b'key1', b'value1', noreply=False)
            client.set(b'key3', b'value2', noreply=False)
            client.get_many([b'key1', b'key3'])

    def test_get_many_bad_server_data_ignore(self):
        client = self.make_client(*[
            [b'STORED\r\n', b'VAXLUE key3 0 6\r\nvalue2\r\nEND\r\n', ],
            [b'STORED\r\n', b'VAXLUE key1 0 6\r\nvalue1\r\nEND\r\n', ],
        ], ignore_exc=True)

        def get_clients(key):
            if key == b'key3':
                return client.clients['127.0.0.1:11012']
            else:
                return client.clients['127.0.0.1:11013']

        client._get_client = get_clients

        client.set(b'key1', b'value1', noreply=False)
        client.set(b'key3', b'value2', noreply=False)
        result = client.get_many([b'key1', b'key3'])
        assert result == {}

    def test_no_servers_left(self):
        from pymemcache.client.hash import HashClient
        client = HashClient(
            [], use_pooling=True,
            ignore_exc=True,
            timeout=1, connect_timeout=1
        )

        hashed_client = client._get_client('foo')
        assert hashed_client is None

    def test_no_servers_left_raise_exception(self):
        from pymemcache.client.hash import HashClient
        client = HashClient(
            [], use_pooling=True,
            ignore_exc=False,
            timeout=1, connect_timeout=1
        )

        with pytest.raises(Exception) as e:
            client._get_client('foo')

        assert str(e.value) == 'All servers seem to be down right now'

    def test_no_servers_left_with_commands(self):
        from pymemcache.client.hash import HashClient
        client = HashClient(
            [], use_pooling=True,
            ignore_exc=True,
            timeout=1, connect_timeout=1
        )

        result = client.get('foo')
        assert result is False

    def test_no_servers_left_with_set_many(self):
        from pymemcache.client.hash import HashClient
        client = HashClient(
            [], use_pooling=True,
            ignore_exc=True,
            timeout=1, connect_timeout=1
        )

        result = client.set_many({'foo': 'bar'})
        assert result is False

    def test_no_servers_left_with_get_many(self):
        from pymemcache.client.hash import HashClient
        client = HashClient(
            [], use_pooling=True,
            ignore_exc=True,
            timeout=1, connect_timeout=1
        )

        result = client.get_many(['foo', 'bar'])
        assert result == {'foo': False, 'bar': False}

    # TODO: Test failover logic
