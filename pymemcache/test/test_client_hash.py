from pymemcache.client.hash import HashClient
from pymemcache.client.base import Client, PooledClient
from pymemcache.exceptions import MemcacheError, MemcacheUnknownError
from pymemcache import pool

from .test_client import ClientTestMixin, MockSocket
import unittest
import pytest
import mock
import socket


class TestHashClient(ClientTestMixin, unittest.TestCase):

    def make_client_pool(self, hostname, mock_socket_values,
                         serializer=None, **kwargs):
        mock_client = Client(hostname, serializer=serializer, **kwargs)
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
                vals,
                **kwargs
            )
            client.clients[s] = c
            client.hasher.add_node(s)
            current_port += 1

        return client

    def test_setup_client_without_pooling(self):
        client_class = 'pymemcache.client.hash.HashClient.client_class'
        with mock.patch(client_class) as internal_client:
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

    def test_gets_many(self):
        client = self.make_client(*[
            [b'STORED\r\n', b'VALUE key3 0 6 1\r\nvalue2\r\nEND\r\n', ],
            [b'STORED\r\n', b'VALUE key1 0 6 1\r\nvalue1\r\nEND\r\n', ],
        ])

        def get_clients(key):
            if key == b'key3':
                return client.clients['127.0.0.1:11012']
            else:
                return client.clients['127.0.0.1:11013']

        client._get_client = get_clients

        assert client.set(b'key1', b'value1', noreply=False) is True
        assert client.set(b'key3', b'value2', noreply=False) is True
        result = client.gets_many([b'key1', b'key3'])
        assert (result ==
                {b'key1': (b'value1', b'1'), b'key3': (b'value2', b'1')})

    def test_touch_not_found(self):
        client = self.make_client([b'NOT_FOUND\r\n'])
        result = client.touch(b'key', noreply=False)
        assert result is False

    def test_touch_no_expiry_found(self):
        client = self.make_client([b'TOUCHED\r\n'])
        result = client.touch(b'key', noreply=False)
        assert result is True

    def test_touch_with_expiry_found(self):
        client = self.make_client([b'TOUCHED\r\n'])
        result = client.touch(b'key', 1, noreply=False)
        assert result is True

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

        with pytest.raises(MemcacheError) as e:
            client._get_client('foo')

        assert str(e.value) == 'All servers seem to be down right now'

    def test_unavailable_servers_zero_retry_raise_exception(self):
        from pymemcache.client.hash import HashClient
        client = HashClient(
            [('example.com', 11211)], use_pooling=True,
            ignore_exc=False,
            retry_attempts=0, timeout=1, connect_timeout=1
        )

        with pytest.raises(socket.error):
            client.get('foo')

    def test_no_servers_left_with_commands_return_default_value(self):
        from pymemcache.client.hash import HashClient
        client = HashClient(
            [], use_pooling=True,
            ignore_exc=True,
            timeout=1, connect_timeout=1
        )

        result = client.get('foo')
        assert result is None
        result = client.set('foo', 'bar')
        assert result is False

    def test_no_servers_left_with_set_many(self):
        from pymemcache.client.hash import HashClient
        client = HashClient(
            [], use_pooling=True,
            ignore_exc=True,
            timeout=1, connect_timeout=1
        )

        result = client.set_many({'foo': 'bar'})
        assert result == ['foo']

    def test_no_servers_left_with_get_many(self):
        from pymemcache.client.hash import HashClient
        client = HashClient(
            [], use_pooling=True,
            ignore_exc=True,
            timeout=1, connect_timeout=1
        )

        result = client.get_many(['foo', 'bar'])
        assert result == {'foo': False, 'bar': False}

    def test_ignore_exec_set_many(self):
        values = {
            'key1': 'value1',
            'key2': 'value2',
            'key3': 'value3'
        }

        with pytest.raises(MemcacheUnknownError):
            client = self.make_client(*[
                [b'STORED\r\n', b'UNKNOWN\r\n', b'STORED\r\n'],
                [b'STORED\r\n', b'UNKNOWN\r\n', b'STORED\r\n'],
            ])
            client.set_many(values, noreply=False)

        client = self.make_client(*[
            [b'STORED\r\n', b'UNKNOWN\r\n', b'STORED\r\n'],
        ], ignore_exc=True)
        result = client.set_many(values, noreply=False)

        assert len(result) == 2

    def test_noreply_set_many(self):
        values = {
            'key1': 'value1',
            'key2': 'value2',
            'key3': 'value3'
        }

        client = self.make_client(*[
            [b'STORED\r\n', b'NOT_STORED\r\n', b'STORED\r\n'],
        ])
        result = client.set_many(values, noreply=False)
        assert len(result) == 1

        client = self.make_client(*[
            [b'STORED\r\n', b'NOT_STORED\r\n', b'STORED\r\n'],
        ])
        result = client.set_many(values, noreply=True)
        assert result == []

    def test_server_encoding_pooled(self):
        """
        test passed encoding from hash client to pooled clients
        """
        encoding = 'utf8'
        from pymemcache.client.hash import HashClient
        hash_client = HashClient(
            [('example.com', 11211)], use_pooling=True,
            encoding=encoding
        )

        for client in hash_client.clients.values():
            assert client.encoding == encoding

    def test_server_encoding_client(self):
        """
        test passed encoding from hash client to clients
        """
        encoding = 'utf8'
        from pymemcache.client.hash import HashClient
        hash_client = HashClient(
            [('example.com', 11211)], encoding=encoding
        )

        for client in hash_client.clients.values():
            assert client.encoding == encoding

    @mock.patch("pymemcache.client.hash.HashClient.client_class")
    def test_dead_server_comes_back(self, client_patch):
        client = HashClient([], dead_timeout=0, retry_attempts=0)
        client.add_server("127.0.0.1", 11211)

        test_client = client_patch.return_value
        test_client.server = ("127.0.0.1", 11211)

        test_client.get.side_effect = socket.timeout()
        with pytest.raises(socket.timeout):
            client.get(b"key", noreply=False)
        # Client gets removed because of socket timeout
        assert ("127.0.0.1", 11211) in client._dead_clients

        test_client.get.side_effect = lambda *_: "Some value"
        # Client should be retried and brought back
        assert client.get(b"key") == "Some value"
        assert ("127.0.0.1", 11211) not in client._dead_clients

    @mock.patch("pymemcache.client.hash.HashClient.client_class")
    def test_failed_is_retried(self, client_patch):
        client = HashClient([], retry_attempts=1, retry_timeout=0)
        client.add_server("127.0.0.1", 11211)

        assert client_patch.call_count == 1

        test_client = client_patch.return_value
        test_client.server = ("127.0.0.1", 11211)

        test_client.get.side_effect = socket.timeout()
        with pytest.raises(socket.timeout):
            client.get(b"key", noreply=False)

        test_client.get.side_effect = lambda *_: "Some value"
        assert client.get(b"key") == "Some value"

        assert client_patch.call_count == 1

    def test_custom_client(self):
        class MyClient(Client):
            pass

        client = HashClient([])
        client.client_class = MyClient
        client.add_server('host', 11211)
        assert isinstance(client.clients['host:11211'], MyClient)

    # TODO: Test failover logic
