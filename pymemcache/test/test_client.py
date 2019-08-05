# Copyright 2012 Pinterest.com
# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from builtins import bytes as newbytes

import collections
import errno
import functools
import json
import os
import mock
import socket
import unittest

import pytest

from pymemcache.client.base import PooledClient, Client
from pymemcache.exceptions import (
    MemcacheClientError,
    MemcacheServerError,
    MemcacheUnknownCommandError,
    MemcacheUnknownError,
    MemcacheIllegalInputError
)

from pymemcache import pool
from pymemcache.test.utils import MockMemcacheClient


class MockSocket(object):
    def __init__(self, recv_bufs, connect_failure=None, close_failure=None):
        self.recv_bufs = collections.deque(recv_bufs)
        self.send_bufs = []
        self.closed = False
        self.timeouts = []
        self.connect_failure = connect_failure
        self.close_failure = close_failure
        self.connections = []
        self.socket_options = []

    @property
    def family(self):
        return socket.AF_INET

    def sendall(self, value):
        self.send_bufs.append(value)

    def close(self):
        if isinstance(self.close_failure, Exception):
            raise self.close_failure
        self.closed = True

    def recv(self, size):
        value = self.recv_bufs.popleft()
        if isinstance(value, Exception):
            raise value
        return value

    def settimeout(self, timeout):
        self.timeouts.append(timeout)

    def connect(self, server):
        if isinstance(self.connect_failure, Exception):
            raise self.connect_failure
        self.connections.append(server)

    def setsockopt(self, level, option, value):
        self.socket_options.append((level, option, value))


class MockUnixSocketServer(object):
    def __init__(self, socket_path):
        if os.path.exists(socket_path):
            os.remove(socket_path)
        self.socket_path = socket_path
        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)

    def __enter__(self):
        self.socket.bind(self.socket_path)
        self.socket.listen(1)
        return self.socket

    def __exit__(self, *args):
        self.socket.close()
        os.remove(self.socket_path)


class MockSocketModule(object):
    def __init__(self, connect_failure=None, close_failure=None):
        self.connect_failure = connect_failure
        self.close_failure = close_failure
        self.sockets = []

    def socket(self, family, type):
        socket = MockSocket(
            [],
            connect_failure=self.connect_failure,
            close_failure=self.close_failure)
        self.sockets.append(socket)
        return socket

    def __getattr__(self, name):
        return getattr(socket, name)


class CustomizedClient(Client):

    def _extract_value(self, expect_cas, line, buf, remapped_keys,
                       prefixed_keys):
        return b'key', b'value', b'END\r\n'


@pytest.mark.unit()
class ClientTestMixin(object):
    def make_client(self, mock_socket_values, **kwargs):
        client = Client(None, **kwargs)
        # mock out client._connect() rather than hard-settting client.sock to
        # ensure methods are checking whether self.sock is None before
        # attempting to use it
        sock = MockSocket(list(mock_socket_values))
        client._connect = mock.Mock(side_effect=functools.partial(
            setattr, client, "sock", sock))
        return client

    def make_customized_client(self, mock_socket_values, **kwargs):
        client = CustomizedClient(None, **kwargs)
        # mock out client._connect() rather than hard-settting client.sock to
        # ensure methods are checking whether self.sock is None before
        # attempting to use it
        sock = MockSocket(list(mock_socket_values))
        client._connect = mock.Mock(side_effect=functools.partial(
            setattr, client, "sock", sock))
        return client

    def test_set_success(self):
        client = self.make_client([b'STORED\r\n'])
        result = client.set(b'key', b'value', noreply=False)
        assert result is True

        # unit test for encoding passed in __init__()
        client = self.make_client([b'STORED\r\n'], encoding='utf-8')
        result = client.set(b'key', b'value', noreply=False)
        assert result is True

        # unit test for set operation with parameter flags
        client = self.make_client([b'STORED\r\n'], encoding='utf-8')
        result = client.set(b'key', b'value', noreply=False, flags=0x00000030)
        assert result is True

    def test_set_future(self):
        client = self.make_client([b'STORED\r\n'])
        result = client.set(newbytes(b'key'), newbytes(b'value'), noreply=False)
        assert result is True

        # unit test for encoding passed in __init__()
        client = self.make_client([b'STORED\r\n'], encoding='utf-8')
        result = client.set(newbytes(b'key'), newbytes(b'value'), noreply=False)
        assert result is True

        # unit test for set operation with parameter flags
        client = self.make_client([b'STORED\r\n'], encoding='utf-8')
        result = client.set(newbytes(b'key'), newbytes(b'value'), noreply=False,
                            flags=0x00000030)
        assert result is True

    def test_set_unicode_key(self):
        client = self.make_client([b''])

        def _set():
            client.set(u'\u0FFF', b'value', noreply=False)

        with pytest.raises(MemcacheIllegalInputError):
            _set()

    def test_set_unicode_key_ok(self):
        client = self.make_client([b'STORED\r\n'], allow_unicode_keys=True)

        result = client.set(u'\u0FFF', b'value', noreply=False)
        assert result is True

    def test_set_unicode_key_ok_snowman(self):
        client = self.make_client([b'STORED\r\n'], allow_unicode_keys=True)

        result = client.set('my☃', b'value', noreply=False)
        assert result is True

    def test_set_unicode_char_in_middle_of_key(self):
        client = self.make_client([b'STORED\r\n'])

        def _set():
            client.set('helloworld_\xb1901520_%c3', b'value', noreply=False)

        with pytest.raises(MemcacheIllegalInputError):
            _set()

    def test_set_unicode_char_in_middle_of_key_snowman(self):
        client = self.make_client([b'STORED\r\n'])

        def _set():
            client.set('my☃', b'value', noreply=False)

        with pytest.raises(MemcacheIllegalInputError):
            _set()

    def test_set_unicode_value(self):
        client = self.make_client([b''])

        def _set():
            client.set(b'key', u'\u0FFF', noreply=False)

        with pytest.raises(MemcacheIllegalInputError):
            _set()

    def test_set_unicode_char_in_middle_of_key_ok(self):
        client = self.make_client([b'STORED\r\n'], allow_unicode_keys=True)

        result = client.set('helloworld_\xb1901520_%c3', b'value',
                            noreply=False)
        assert result is True

    def test_set_noreply(self):
        client = self.make_client([])
        result = client.set(b'key', b'value', noreply=True)
        assert result is True

        # unit test for encoding passed in __init__()
        client = self.make_client([], encoding='utf-8')
        result = client.set(b'key', b'value', noreply=True)
        assert result is True

    def test_set_many_success(self):
        client = self.make_client([b'STORED\r\n'])
        result = client.set_many({b'key': b'value'}, noreply=False)
        assert result == []

        # unit test for encoding passed in __init__()
        client = self.make_client([b'STORED\r\n'], encoding='utf-8')
        result = client.set_many({b'key': b'value'}, noreply=False)
        assert result == []

    def test_set_multi_success(self):
        # Should just map to set_many
        client = self.make_client([b'STORED\r\n'])
        result = client.set_multi({b'key': b'value'}, noreply=False)
        assert result == []

        # unit test for encoding passed in __init__()
        client = self.make_client([b'STORED\r\n'], encoding='utf-8')
        result = client.set_multi({b'key': b'value'}, noreply=False)
        assert result == []

    def test_add_stored(self):
        client = self.make_client([b'STORED\r', b'\n'])
        result = client.add(b'key', b'value', noreply=False)
        assert result is True

        # unit test for encoding passed in __init__()
        client = self.make_client([b'STORED\r', b'\n'], encoding='utf-8')
        result = client.add(b'key', b'value', noreply=False)
        assert result is True

    def test_add_not_stored(self):
        client = self.make_client([b'STORED\r', b'\n',
                                   b'NOT_', b'STOR', b'ED', b'\r\n'])
        client.add(b'key', b'value', noreply=False)
        result = client.add(b'key', b'value', noreply=False)
        assert result is False

        # unit test for encoding passed in __init__()
        client = self.make_client([b'STORED\r', b'\n',
                                   b'NOT_', b'STOR', b'ED', b'\r\n'],
                                  encoding='utf-8')
        client.add(b'key', b'value', noreply=False)
        result = client.add(b'key', b'value', noreply=False)
        assert result is False

    def test_get_not_found(self):
        client = self.make_client([b'END\r\n'])
        result = client.get(b'key')
        assert result is None

        # Unit test for customized client (override _extract_value)
        client = self.make_customized_client([b'END\r\n'])
        result = client.get(b'key')
        assert result is None

    def test_get_not_found_default(self):
        client = self.make_client([b'END\r\n'])
        result = client.get(b'key', default='foobar')
        assert result == 'foobar'

        # Unit test for customized client (override _extract_value)
        client = self.make_customized_client([b'END\r\n'])
        result = client.get(b'key', default='foobar')
        assert result == 'foobar'

    def test_get_found(self):
        client = self.make_client([
            b'STORED\r\n',
            b'VALUE key 0 5\r\nvalue\r\nEND\r\n',
        ])
        client.set(b'key', b'value', noreply=False)
        result = client.get(b'key')
        assert result == b'value'

        # Unit test for customized client (override _extract_value)
        client = self.make_customized_client([
            b'STORED\r\n',
            b'VALUE key 0 5\r\nvalue\r\nEND\r\n',
        ])
        client.set(b'key', b'value', noreply=False)
        result = client.get(b'key')
        assert result == b'value'

    def test_get_many_none_found(self):
        client = self.make_client([b'END\r\n'])
        result = client.get_many([b'key1', b'key2'])
        assert result == {}

    def test_get_multi_none_found(self):
        client = self.make_client([b'END\r\n'])
        result = client.get_multi([b'key1', b'key2'])
        assert result == {}

    def test_get_many_some_found(self):
        client = self.make_client([
            b'STORED\r\n',
            b'VALUE key1 0 6\r\nvalue1\r\nEND\r\n',
        ])
        client.set(b'key1', b'value1', noreply=False)
        result = client.get_many([b'key1', b'key2'])
        assert result == {b'key1': b'value1'}

    def test_get_many_all_found(self):
        client = self.make_client([
            b'STORED\r\n',
            b'STORED\r\n',
            b'VALUE key1 0 6\r\nvalue1\r\n',
            b'VALUE key2 0 6\r\nvalue2\r\nEND\r\n',
        ])
        client.set(b'key1', b'value1', noreply=False)
        client.set(b'key2', b'value2', noreply=False)
        result = client.get_many([b'key1', b'key2'])
        assert result == {b'key1': b'value1', b'key2': b'value2'}

    def test_get_unicode_key(self):
        client = self.make_client([b''])

        def _get():
            client.get(u'\u0FFF')

        with pytest.raises(MemcacheIllegalInputError):
            _get()

    def test_delete_not_found(self):
        client = self.make_client([b'NOT_FOUND\r\n'])
        result = client.delete(b'key', noreply=False)
        assert result is False

    def test_delete_found(self):
        client = self.make_client([b'STORED\r', b'\n', b'DELETED\r\n'])
        client.add(b'key', b'value', noreply=False)
        result = client.delete(b'key', noreply=False)
        assert result is True

    def test_delete_noreply(self):
        client = self.make_client([])
        result = client.delete(b'key', noreply=True)
        assert result is True

    def test_delete_many_no_keys(self):
        client = self.make_client([])
        result = client.delete_many([], noreply=False)
        assert result is True

    def test_delete_many_none_found(self):
        client = self.make_client([b'NOT_FOUND\r\n'])
        result = client.delete_many([b'key'], noreply=False)
        assert result is True

    def test_delete_many_found(self):
        client = self.make_client([b'STORED\r', b'\n', b'DELETED\r\n'])
        client.add(b'key', b'value', noreply=False)
        result = client.delete_many([b'key'], noreply=False)
        assert result is True

    def test_delete_many_some_found(self):
        client = self.make_client([
            b'STORED\r\n',
            b'DELETED\r\n',
            b'NOT_FOUND\r\n'
        ])
        client.add(b'key', b'value', noreply=False)
        result = client.delete_many([b'key', b'key2'], noreply=False)
        assert result is True

    def test_delete_multi_some_found(self):
        client = self.make_client([
            b'STORED\r\n',
            b'DELETED\r\n',
            b'NOT_FOUND\r\n'
        ])
        client.add(b'key', b'value', noreply=False)
        result = client.delete_multi([b'key', b'key2'], noreply=False)
        assert result is True

    def test_incr_not_found(self):
        client = self.make_client([b'NOT_FOUND\r\n'])
        result = client.incr(b'key', 1, noreply=False)
        assert result is None

    def test_incr_found(self):
        client = self.make_client([b'STORED\r\n', b'1\r\n'])
        client.set(b'key', 0, noreply=False)
        result = client.incr(b'key', 1, noreply=False)
        assert result == 1

    def test_incr_noreply(self):
        client = self.make_client([b'STORED\r\n'])
        client.set(b'key', 0, noreply=False)

        client = self.make_client([])
        result = client.incr(b'key', 1, noreply=True)
        assert result is None

    def test_decr_not_found(self):
        client = self.make_client([b'NOT_FOUND\r\n'])
        result = client.decr(b'key', 1, noreply=False)
        assert result is None

    def test_decr_found(self):
        client = self.make_client([b'STORED\r\n', b'1\r\n'])
        client.set(b'key', 2, noreply=False)
        result = client.decr(b'key', 1, noreply=False)
        assert result == 1


class TestClient(ClientTestMixin, unittest.TestCase):

    Client = Client

    def test_append_stored(self):
        client = self.make_client([b'STORED\r\n'])
        result = client.append(b'key', b'value', noreply=False)
        assert result is True

        # unit test for encoding passed in __init__()
        client = self.make_client([b'STORED\r\n'], encoding='utf-8')
        result = client.append(b'key', b'value', noreply=False)
        assert result is True

    def test_prepend_stored(self):
        client = self.make_client([b'STORED\r\n'])
        result = client.prepend(b'key', b'value', noreply=False)
        assert result is True

        # unit test for encoding passed in __init__()
        client = self.make_client([b'STORED\r\n'], encoding='utf-8')
        result = client.prepend(b'key', b'value', noreply=False)
        assert result is True

    def test_cas_stored(self):
        client = self.make_client([b'STORED\r\n'])
        result = client.cas(b'key', b'value', b'cas', noreply=False)
        assert result is True

        # unit test for encoding passed in __init__()
        client = self.make_client([b'STORED\r\n'], encoding='utf-8')
        result = client.cas(b'key', b'value', b'cas', noreply=False)
        assert result is True

    def test_cas_exists(self):
        client = self.make_client([b'EXISTS\r\n'])
        result = client.cas(b'key', b'value', b'cas', noreply=False)
        assert result is False

        # unit test for encoding passed in __init__()
        client = self.make_client([b'EXISTS\r\n'], encoding='utf-8')
        result = client.cas(b'key', b'value', b'cas', noreply=False)
        assert result is False

    def test_cas_not_found(self):
        client = self.make_client([b'NOT_FOUND\r\n'])
        result = client.cas(b'key', b'value', b'cas', noreply=False)
        assert result is None

        # unit test for encoding passed in __init__()
        client = self.make_client([b'NOT_FOUND\r\n'], encoding='utf-8')
        result = client.cas(b'key', b'value', b'cas', noreply=False)
        assert result is None

    def test_cr_nl_boundaries(self):
        client = self.make_client([b'VALUE key1 0 6\r',
                                   b'\nvalue1\r\n'
                                   b'VALUE key2 0 6\r\n',
                                   b'value2\r\n'
                                   b'END\r\n'])
        result = client.get_many([b'key1', b'key2'])
        assert result == {b'key1': b'value1', b'key2': b'value2'}

        client = self.make_client([b'VALUE key1 0 6\r\n',
                                   b'value1\r',
                                   b'\nVALUE key2 0 6\r\n',
                                   b'value2\r\n',
                                   b'END\r\n'])
        result = client.get_many([b'key1', b'key2'])
        assert result == {b'key1': b'value1', b'key2': b'value2'}

        client = self.make_client([b'VALUE key1 0 6\r\n',
                                   b'value1\r\n',
                                   b'VALUE key2 0 6\r',
                                   b'\nvalue2\r\n',
                                   b'END\r\n'])
        result = client.get_many([b'key1', b'key2'])
        assert result == {b'key1': b'value1', b'key2': b'value2'}

        client = self.make_client([b'VALUE key1 0 6\r\n',
                                   b'value1\r\n',
                                   b'VALUE key2 0 6\r\n',
                                   b'value2\r',
                                   b'\nEND\r\n'])
        result = client.get_many([b'key1', b'key2'])
        assert result == {b'key1': b'value1', b'key2': b'value2'}

        client = self.make_client([b'VALUE key1 0 6\r\n',
                                   b'value1\r\n',
                                   b'VALUE key2 0 6\r\n',
                                   b'value2\r\n',
                                   b'END\r',
                                   b'\n'])
        result = client.get_many([b'key1', b'key2'])
        assert result == {b'key1': b'value1', b'key2': b'value2'}

        client = self.make_client([b'VALUE key1 0 6\r',
                                   b'\nvalue1\r',
                                   b'\nVALUE key2 0 6\r',
                                   b'\nvalue2\r',
                                   b'\nEND\r',
                                   b'\n'])
        result = client.get_many([b'key1', b'key2'])
        assert result == {b'key1': b'value1', b'key2': b'value2'}

    def test_delete_exception(self):
        client = self.make_client([Exception('fail')])

        def _delete():
            client.delete(b'key', noreply=False)

        with pytest.raises(Exception):
            _delete()

        assert client.sock is None

    def test_flush_all(self):
        client = self.make_client([b'OK\r\n'])
        result = client.flush_all(noreply=False)
        assert result is True

    def test_incr_exception(self):
        client = self.make_client([Exception('fail')])

        def _incr():
            client.incr(b'key', 1)

        with pytest.raises(Exception):
            _incr()

        assert client.sock is None

    def test_get_error(self):
        client = self.make_client([b'ERROR\r\n'])

        def _get():
            client.get(b'key')

        with pytest.raises(MemcacheUnknownCommandError):
            _get()

    def test_get_recv_chunks(self):
        client = self.make_client([b'VALUE key', b' 0 5\r', b'\nvalue',
                                   b'\r\n', b'END', b'\r', b'\n'])
        result = client.get(b'key')
        assert result == b'value'

    def test_get_unknown_error(self):
        client = self.make_client([b'foobarbaz\r\n'])

        def _get():
            client.get(b'key')

        with pytest.raises(MemcacheUnknownError):
            _get()

    def test_gets_not_found(self):
        client = self.make_client([b'END\r\n'])
        result = client.gets(b'key')
        assert result == (None, None)

    def test_gets_not_found_defaults(self):
        client = self.make_client([b'END\r\n'])
        result = client.gets(b'key', default='foo', cas_default='bar')
        assert result == ('foo', 'bar')

    def test_gets_found(self):
        client = self.make_client([b'VALUE key 0 5 10\r\nvalue\r\nEND\r\n'])
        result = client.gets(b'key')
        assert result == (b'value', b'10')

    def test_gets_many_none_found(self):
        client = self.make_client([b'END\r\n'])
        result = client.gets_many([b'key1', b'key2'])
        assert result == {}

    def test_gets_many_some_found(self):
        client = self.make_client([b'VALUE key1 0 6 11\r\nvalue1\r\nEND\r\n'])
        result = client.gets_many([b'key1', b'key2'])
        assert result == {b'key1': (b'value1', b'11')}

    def test_touch_not_found(self):
        client = self.make_client([b'NOT_FOUND\r\n'])
        result = client.touch(b'key', noreply=False)
        assert result is False

    def test_touch_found(self):
        client = self.make_client([b'TOUCHED\r\n'])
        result = client.touch(b'key', noreply=False)
        assert result is True

    def test_quit(self):
        client = self.make_client([])
        result = client.quit()
        assert result is None
        assert client.sock is None

    def test_replace_stored(self):
        client = self.make_client([b'STORED\r\n'])
        result = client.replace(b'key', b'value', noreply=False)
        assert result is True

        # unit test for encoding passed in __init__()
        client = self.make_client([b'STORED\r\n'], encoding='utf-8')
        result = client.replace(b'key', b'value', noreply=False)
        assert result is True

    def test_replace_not_stored(self):
        client = self.make_client([b'NOT_STORED\r\n'])
        result = client.replace(b'key', b'value', noreply=False)
        assert result is False

        # unit test for encoding passed in __init__
        client = self.make_client([b'NOT_STORED\r\n'], encoding='utf-8')
        result = client.replace(b'key', b'value', noreply=False)
        assert result is False

    def test_serialization(self):
        def _ser(key, value):
            return json.dumps(value), 0

        client = self.make_client([b'STORED\r\n'], serializer=_ser)
        client.set('key', {'c': 'd'})
        assert client.sock.send_bufs == [
            b'set key 0 0 10 noreply\r\n{"c": "d"}\r\n'
        ]

    def test_serialization_flags(self):
        def _ser(key, value):
            return value, 1 if isinstance(value, int) else 0

        client = self.make_client(
            [b'STORED\r\n', b'STORED\r\n'], serializer=_ser)
        client.set_many(
            collections.OrderedDict([(b'a', b's'), (b'b', 0)]), noreply=False)
        assert client.sock.send_bufs == [
            b'set a 0 0 1\r\ns\r\nset b 1 0 1\r\n0\r\n'
        ]

    def test_serialization_overridden_flags(self):
        def _ser(key, value):
            return value, 1 if isinstance(value, int) else 0

        client = self.make_client(
            [b'STORED\r\n', b'STORED\r\n'], serializer=_ser)
        client.set_many(
            collections.OrderedDict([(b'a', b's'), (b'b', 0)]),
            noreply=False, flags=5)
        assert client.sock.send_bufs == [
            b'set a 5 0 1\r\ns\r\nset b 5 0 1\r\n0\r\n'
        ]

    def test_explicit_flags(self):
        client = self.make_client([b'STORED\r\n', b'STORED\r\n'])
        client.set_many(
            collections.OrderedDict([(b'a', b's'), (b'b', 0)]),
            noreply=False, flags=5)
        assert client.sock.send_bufs == [
            b'set a 5 0 1\r\ns\r\nset b 5 0 1\r\n0\r\n'
        ]

    def test_set_socket_handling(self):
        client = self.make_client([b'STORED\r\n'])
        result = client.set(b'key', b'value', noreply=False)
        assert result is True
        assert client.sock.closed is False
        assert len(client.sock.send_bufs) == 1

    def test_set_error(self):
        client = self.make_client([b'ERROR\r\n'])

        def _set():
            client.set(b'key', b'value', noreply=False)

        with pytest.raises(MemcacheUnknownCommandError):
            _set()

    def test_set_exception(self):
        client = self.make_client([Exception('fail')])

        def _set():
            client.set(b'key', b'value', noreply=False)
        with pytest.raises(Exception):
            _set()

        assert client.sock is None

    def test_set_client_error(self):
        client = self.make_client([b'CLIENT_ERROR some message\r\n'])

        def _set():
            client.set('key', 'value', noreply=False)

        with pytest.raises(MemcacheClientError):
            _set()

    def test_set_server_error(self):
        client = self.make_client([b'SERVER_ERROR some message\r\n'])

        def _set():
            client.set(b'key', b'value', noreply=False)

        with pytest.raises(MemcacheServerError):
            _set()

    def test_set_unknown_error(self):
        client = self.make_client([b'foobarbaz\r\n'])

        def _set():
            client.set(b'key', b'value', noreply=False)

        with pytest.raises(MemcacheUnknownError):
            _set()

    def test_set_key_with_space(self):
        client = self.make_client([b''])

        def _set():
            client.set(b'key has space', b'value', noreply=False)

        with pytest.raises(MemcacheIllegalInputError):
            _set()

    def test_set_key_with_newline(self):
        client = self.make_client([b''])

        def _set():
            client.set(b'key\n', b'value', noreply=False)

        with pytest.raises(MemcacheIllegalInputError):
            _set()

    def test_set_key_with_carriage_return(self):
        client = self.make_client([b''])

        def _set():
            client.set(b'key\r', b'value', noreply=False)

        with pytest.raises(MemcacheIllegalInputError):
            _set()

    def test_set_key_with_null_character(self):
        client = self.make_client([b''])

        def _set():
            client.set(b'key\00', b'value', noreply=False)

        with pytest.raises(MemcacheIllegalInputError):
            _set()

    def test_set_many_socket_handling(self):
        client = self.make_client([b'STORED\r\n'])
        result = client.set_many({b'key': b'value'}, noreply=False)
        assert result == []
        assert client.sock.closed is False
        assert len(client.sock.send_bufs) == 1

        # unit test for encoding passed in __init__()
        client = self.make_client([b'STORED\r\n'], encoding='utf-8')
        result = client.set_many({b'key': b'value'}, noreply=False)
        assert result == []
        assert client.sock.closed is False
        assert len(client.sock.send_bufs) == 1

    def test_set_many_exception(self):
        client = self.make_client([b'STORED\r\n', Exception('fail')])

        def _set():
            client.set_many({b'key': b'value', b'other': b'value'},
                            noreply=False)

        with pytest.raises(Exception):
            _set()

        assert client.sock is None

        # unit test for encoding passed in __init__()
        client = self.make_client([b'STORED\r\n', Exception('fail')],
                                  encoding='utf-8')

        def _set():
            client.set_many({b'key': b'value', b'other': b'value'},
                            noreply=False)

        with pytest.raises(Exception):
            _set()

        assert client.sock is None

    def test_stats(self):
        client = self.make_client([b'STAT fake_stats 1\r\n', b'END\r\n'])
        result = client.stats()
        assert client.sock.send_bufs == [
            b'stats \r\n'
        ]
        assert result == {b'fake_stats': 1}

    def test_stats_with_args(self):
        client = self.make_client([b'STAT fake_stats 1\r\n', b'END\r\n'])
        result = client.stats('some_arg')
        assert client.sock.send_bufs == [
            b'stats some_arg\r\n'
        ]
        assert result == {b'fake_stats': 1}

    def test_stats_conversions(self):
        client = self.make_client([
            # Most stats are converted to int
            b'STAT cmd_get 2519\r\n',
            b'STAT cmd_set 3099\r\n',
            b'STAT evictions 939\r\n',

            # Unless they can't be, they remain str
            b'STAT libevent 2.0.19-stable\r\n',

            # Some named stats are explicitly converted
            b'STAT hash_is_expanding 0\r\n',
            b'STAT rusage_user 0.609165\r\n',
            b'STAT rusage_system 0.852791\r\n',
            b'STAT slab_reassign_running 1\r\n',
            b'STAT version 1.4.14\r\n',
            b'STAT umask 777\r\n',
            b'STAT auth_enabled_sasl yes\r\n',
            b'END\r\n',
        ])
        result = client.stats()
        assert client.sock.send_bufs == [
            b'stats \r\n'
        ]
        expected = {
            b'cmd_get': 2519,
            b'cmd_set': 3099,
            b'evictions': 939,
            b'libevent': b'2.0.19-stable',
            b'hash_is_expanding': False,
            b'rusage_user': 0.609165,
            b'rusage_system': 0.852791,
            b'slab_reassign_running': True,
            b'version': b'1.4.14',
            b'umask': 0o777,
            b'auth_enabled_sasl': True,
        }
        assert result == expected

    def test_stats_cachedump(self):
        client = self.make_client([b'ITEM bob [7 b; 0 s]\r\n', b'END\r\n'])
        result = client.stats('cachedump', '1', '1')
        assert client.sock.send_bufs == [
            b'stats cachedump 1 1\r\n'
        ]
        assert result == {b'bob': b'[7 b; 0 s]'}

    def test_cache_memlimit(self):
        client = self.make_client([b'OK\r\n'])
        result = client.cache_memlimit(8)
        assert client.sock.send_bufs == [
            b'cache_memlimit 8\r\n'
        ]
        assert result is True

    def test_python_dict_set_is_supported(self):
        client = self.make_client([b'STORED\r\n'])
        client[b'key'] = b'value'

    def test_python_dict_get_is_supported(self):
        client = self.make_client([b'VALUE key 0 5\r\nvalue\r\nEND\r\n'])
        assert client[b'key'] == b'value'

    def test_python_dict_get_not_found_is_supported(self):
        client = self.make_client([b'END\r\n'])

        def _get():
            client[b'key']

        with pytest.raises(KeyError):
            _get()

    def test_python_dict_del_is_supported(self):
        client = self.make_client([b'DELETED\r\n'])
        del client[b'key']

    def test_too_long_key(self):
        client = self.make_client([b'END\r\n'])

        with pytest.raises(MemcacheClientError):
            client.get(b'x' * 251)

    def test_too_long_unicode_key(self):
        client = self.make_client([b'STORED\r\n'], allow_unicode_keys=True)

        with pytest.raises(MemcacheClientError):
            client.get('my☃'*150)

        with pytest.raises(MemcacheClientError):
            client.get(u'\u0FFF'*150)

    def test_key_contains_space(self):
        client = self.make_client([b'END\r\n'])
        with pytest.raises(MemcacheClientError):
            client.get(b'abc xyz')

    def test_key_contains_nonascii(self):
        client = self.make_client([b'END\r\n'])

        with pytest.raises(MemcacheClientError):
            client.get(u'\u3053\u3093\u306b\u3061\u306f')

    def _default_noreply_false(self, cmd, args, response):
        client = self.make_client(response, default_noreply=False)
        result = getattr(client, cmd)(*args)
        assert result is False

    def _default_noreply_true(self, cmd, args, response):
        client = self.make_client(response, default_noreply=True)
        result = getattr(client, cmd)(*args)
        assert result is True

    def _default_noreply_true_and_empty_list(self, cmd, args, response):
        client = self.make_client(response, default_noreply=True)
        result = getattr(client, cmd)(*args)
        assert result == []

    def test_default_noreply_set(self):
        with pytest.raises(MemcacheUnknownError):
            self._default_noreply_false(
                'set', (b'key', b'value'), [b'UNKNOWN\r\n'])
        self._default_noreply_false(
            'set', (b'key', b'value'), [b'NOT_STORED\r\n'])
        self._default_noreply_true(
            'set', (b'key', b'value'), [b'NOT_STORED\r\n'])

    def test_default_noreply_set_many(self):
        with pytest.raises(MemcacheUnknownError):
            client = self.make_client([b'UNKNOWN\r\n'], default_noreply=False)
            result = client.set_many({b'key': b'value'})
            assert result == [b'key']
        self._default_noreply_true_and_empty_list(
            'set_many', ({b'key': b'value'},), [b'NOT_STORED\r\n'])

    def test_default_noreply_add(self):
        self._default_noreply_false(
            'add', (b'key', b'value'), [b'NOT_STORED\r\n'])
        self._default_noreply_true(
            'add', (b'key', b'value'), [b'NOT_STORED\r\n'])

    def test_default_noreply_replace(self):
        self._default_noreply_false(
            'replace', (b'key', b'value'), [b'NOT_STORED\r\n'])
        self._default_noreply_true(
            'replace', (b'key', b'value'), [b'NOT_STORED\r\n'])

    def test_default_noreply_append(self):
        self._default_noreply_false(
            'append', (b'key', b'value'), [b'NOT_STORED\r\n'])
        self._default_noreply_true(
            'append', (b'key', b'value'), [b'NOT_STORED\r\n'])

    def test_default_noreply_prepend(self):
        self._default_noreply_false(
            'prepend', (b'key', b'value'), [b'NOT_STORED\r\n'])
        self._default_noreply_true(
            'prepend', (b'key', b'value'), [b'NOT_STORED\r\n'])

    def test_default_noreply_touch(self):
        self._default_noreply_false('touch', (b'key',), [b'NOT_FOUND\r\n'])
        self._default_noreply_true('touch', (b'key',), [b'NOT_FOUND\r\n'])

    def test_default_noreply_flush_all(self):
        self._default_noreply_false('flush_all', (),
                                    [b'__FAKE_RESPONSE__\r\n'])
        self._default_noreply_true('flush_all', (), [b'__FAKE_RESPONSE__\r\n'])

    def test_version_success(self):
        client = self.make_client([b'VERSION 1.2.3\r\n'],
                                  default_noreply=False)
        result = client.version()
        assert result == b'1.2.3'

    def test_version_exception(self):
        client = self.make_client([b'INVALID DATA\r\n'], default_noreply=False)
        with pytest.raises(MemcacheUnknownError):
            client.version()


@pytest.mark.unit()
class TestClientSocketConnect(unittest.TestCase):
    def test_socket_connect(self):
        server = ("example.com", 11211)

        client = Client(server, socket_module=MockSocketModule())
        client._connect()
        assert client.sock.connections == [server]

        timeout = 2
        connect_timeout = 3
        client = Client(
            server, connect_timeout=connect_timeout, timeout=timeout,
            socket_module=MockSocketModule())
        client._connect()
        assert client.sock.timeouts == [connect_timeout, timeout]

        client = Client(server, socket_module=MockSocketModule())
        client._connect()
        assert client.sock.socket_options == []

        client = Client(
            server, socket_module=MockSocketModule(), no_delay=True)
        client._connect()
        assert client.sock.socket_options == [(socket.IPPROTO_TCP,
                                               socket.TCP_NODELAY, 1)]

    def test_socket_connect_unix(self):
        server = '/tmp/pymemcache.{pid}'.format(pid=os.getpid())

        with MockUnixSocketServer(server):
            client = Client(server)
            client._connect()
            assert client.sock.family == socket.AF_UNIX

    def test_socket_connect_closes_on_failure(self):
        server = ("example.com", 11211)

        socket_module = MockSocketModule(connect_failure=OSError())
        client = Client(server, socket_module=socket_module)
        with pytest.raises(OSError):
            client._connect()
        assert len(socket_module.sockets) == 1
        assert socket_module.sockets[0].connections == []
        assert socket_module.sockets[0].closed

    def test_socket_close(self):
        server = ("example.com", 11211)

        client = Client(server, socket_module=MockSocketModule())
        client._connect()
        assert client.sock is not None

        client.close()
        assert client.sock is None

    def test_socket_close_exception(self):
        server = ("example.com", 11211)

        socket_module = MockSocketModule(close_failure=OSError())
        client = Client(server, socket_module=socket_module)
        client._connect()
        assert client.sock is not None

        client.close()
        assert client.sock is None


class TestPooledClient(ClientTestMixin, unittest.TestCase):
    def make_client(self, mock_socket_values, **kwargs):
        mock_client = Client(None, **kwargs)
        mock_client.sock = MockSocket(list(mock_socket_values))
        client = PooledClient(None, **kwargs)
        client.client_pool = pool.ObjectPool(lambda: mock_client)
        return client

    def _default_noreply_false(self, cmd, args, response):
        client = self.make_client(response, default_noreply=False)
        result = getattr(client, cmd)(*args)
        assert result is False

    def _default_noreply_true(self, cmd, args, response):
        client = self.make_client(response, default_noreply=True)
        result = getattr(client, cmd)(*args)
        assert result is True

    def _default_noreply_true_and_empty_list(self, cmd, args, response):
        client = self.make_client(response, default_noreply=True)
        result = getattr(client, cmd)(*args)
        assert result == []

    def test_default_noreply_set(self):
        with pytest.raises(MemcacheUnknownError):
            self._default_noreply_false(
                'set', (b'key', b'value'), [b'UNKNOWN\r\n'])
        self._default_noreply_false(
            'set', (b'key', b'value'), [b'NOT_STORED\r\n'])
        self._default_noreply_true(
            'set', (b'key', b'value'), [b'NOT_STORED\r\n'])

    def test_default_noreply_set_many(self):
        with pytest.raises(MemcacheUnknownError):
            client = self.make_client([b'UNKNOWN\r\n'], default_noreply=False)
            client.set_many({b'key': b'value'})
        self._default_noreply_true_and_empty_list(
            'set_many', ({b'key': b'value'},), [b'NOT_STORED\r\n'])

    def test_default_noreply_add(self):
        self._default_noreply_false(
            'add', (b'key', b'value'), [b'NOT_STORED\r\n'])
        self._default_noreply_true(
            'add', (b'key', b'value'), [b'NOT_STORED\r\n'])

    def test_default_noreply_replace(self):
        self._default_noreply_false(
            'replace', (b'key', b'value'), [b'NOT_STORED\r\n'])
        self._default_noreply_true(
            'replace', (b'key', b'value'), [b'NOT_STORED\r\n'])

    def test_default_noreply_append(self):
        self._default_noreply_false(
            'append', (b'key', b'value'), [b'NOT_STORED\r\n'])
        self._default_noreply_true(
            'append', (b'key', b'value'), [b'NOT_STORED\r\n'])

    def test_default_noreply_prepend(self):
        self._default_noreply_false(
            'prepend', (b'key', b'value'), [b'NOT_STORED\r\n'])
        self._default_noreply_true(
            'prepend', (b'key', b'value'), [b'NOT_STORED\r\n'])

    def test_default_noreply_touch(self):
        self._default_noreply_false('touch', (b'key',), [b'NOT_FOUND\r\n'])
        self._default_noreply_true('touch', (b'key',), [b'NOT_FOUND\r\n'])

    def test_default_noreply_flush_all(self):
        self._default_noreply_false('flush_all', (),
                                    [b'__FAKE_RESPONSE__\r\n'])
        self._default_noreply_true('flush_all', (), [b'__FAKE_RESPONSE__\r\n'])


class TestMockClient(ClientTestMixin, unittest.TestCase):
    def make_client(self, mock_socket_values, **kwargs):
        client = MockMemcacheClient(None, **kwargs)
        client.sock = MockSocket(list(mock_socket_values))
        return client

    def test_get_found(self):
        client = self.make_client([
            b'STORED\r\n',
            b'VALUE key 0 5\r\nvalue\r\nEND\r\n',
        ])
        result = client.set(b'key', b'value', noreply=False)
        result = client.get(b'key')
        assert result == b'value'

    def test_deserialization(self):
        def _serializer(key, value):
            if isinstance(value, dict):
                return json.dumps(value).encode('UTF-8'), 1
            return value, 0

        def _deserializer(key, value, flags):
            if flags == 1:
                return json.loads(value.decode('UTF-8'))
            return value

        client = self.make_client([
            b'STORED\r\n',
            b'VALUE key1 0 5\r\nhello\r\nEND\r\n',
            b'STORED\r\n',
            b'VALUE key2 0 18\r\n{"hello": "world"}\r\nEND\r\n',
        ], serializer=_serializer, deserializer=_deserializer)

        result = client.set(b'key1', b'hello', noreply=False)
        result = client.get(b'key1')
        assert result == b'hello'

        result = client.set(b'key2', dict(hello='world'), noreply=False)
        result = client.get(b'key2')
        assert result == dict(hello='world')


class TestPrefixedClient(ClientTestMixin, unittest.TestCase):
    def make_client(self, mock_socket_values, **kwargs):
        client = Client(None, key_prefix=b'xyz:', **kwargs)
        client.sock = MockSocket(list(mock_socket_values))
        return client

    def test_get_found(self):
        client = self.make_client([
            b'STORED\r\n',
            b'VALUE xyz:key 0 5\r\nvalue\r\nEND\r\n',
        ])
        result = client.set(b'key', b'value', noreply=False)
        result = client.get(b'key')
        assert result == b'value'

    def test_get_many_some_found(self):
        client = self.make_client([
            b'STORED\r\n',
            b'VALUE xyz:key1 0 6\r\nvalue1\r\nEND\r\n',
        ])
        result = client.set(b'key1', b'value1', noreply=False)
        result = client.get_many([b'key1', b'key2'])
        assert result == {b'key1': b'value1'}

    def test_get_many_all_found(self):
        client = self.make_client([
            b'STORED\r\n',
            b'STORED\r\n',
            b'VALUE xyz:key1 0 6\r\nvalue1\r\n',
            b'VALUE xyz:key2 0 6\r\nvalue2\r\nEND\r\n',
        ])
        result = client.set(b'key1', b'value1', noreply=False)
        result = client.set(b'key2', b'value2', noreply=False)
        result = client.get_many([b'key1', b'key2'])
        assert result == {b'key1': b'value1', b'key2': b'value2'}

    def test_python_dict_get_is_supported(self):
        client = self.make_client([b'VALUE xyz:key 0 5\r\nvalue\r\nEND\r\n'])
        assert client[b'key'] == b'value'


class TestPrefixedPooledClient(TestPrefixedClient):
    def make_client(self, mock_socket_values, **kwargs):
        mock_client = Client(None, key_prefix=b'xyz:', **kwargs)
        mock_client.sock = MockSocket(list(mock_socket_values))
        client = PooledClient(None, key_prefix=b'xyz:', **kwargs)
        client.client_pool = pool.ObjectPool(lambda: mock_client)
        return client


@pytest.mark.unit()
class TestRetryOnEINTR(unittest.TestCase):
    def make_client(self, values):
        client = Client(None)
        client.sock = MockSocket(list(values))
        return client

    def test_recv(self):
        client = self.make_client([
            b'VALUE ',
            socket.error(errno.EINTR, "Interrupted system call"),
            b'key1 0 6\r\nval',
            socket.error(errno.EINTR, "Interrupted system call"),
            b'ue1\r\nEND\r\n',
        ])
        assert client[b'key1'] == b'value1'
