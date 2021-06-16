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
import re
import socket
import unittest

import pytest

from pymemcache.client.base import Client
from pymemcache.exceptions import (
    MemcacheClientError,
    MemcacheServerError,
    MemcacheUnknownCommandError,
    MemcacheUnknownError,
    MemcacheIllegalInputError
)

from pymemcache.test.utils import (
    MockMemcacheClient,
    MockSocket,
    MockSocketModule
)


@pytest.mark.unit()
class TestClientRetrying(unittest.TestCase):
    def test_socket_connect_ipv4(self):
        server = ('127.0.0.1', 11211)

        client = RetryingClient(
            Client(server, socket_module=MockSocketModule()))
        
        client._connect()
        assert client.sock.connections == [server]
        assert client.sock.family == socket.AF_INET

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

    def test_socket_connect_closes_on_failure(self):
        server = ("example.com", 11211)

        socket_module = MockSocketModule(connect_failure=OSError())
        client = RetryingClient(
            Client(server, socket_module=socket_module)
        with pytest.raises(OSError):
            client._connect()
        assert len(socket_module.sockets) == 1
        assert socket_module.sockets[0].connections == []
        assert socket_module.sockets[0].closed

    def test_socket_close(self):
        server = ("example.com", 11211)

        client = RetryingClient(
            Client(server, socket_module=MockSocketModule()))
        client._connect()
        assert client.sock is not None

        client.close()
        assert client.sock is None

    def test_socket_close_exception(self):
        server = ("example.com", 11211)

        socket_module = MockSocketModule(close_failure=OSError())
        client = RetryingClient(
            Client(server, socket_module=socket_module)
        client._connect()
        assert client.sock is not None

        client.close()
        assert client.sock is None
