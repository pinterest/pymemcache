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

from collections import defaultdict
import json
import pytest
import six

from pymemcache.client.base import Client
from pymemcache.exceptions import (
    MemcacheIllegalInputError,
    MemcacheClientError
)
from pymemcache.serde import (
    PickleSerde,
    pickle_serde
)


def get_set_helper(client, key, value, key2, value2):
    result = client.get(key)
    assert result is None

    client.set(key, value, noreply=False)
    result = client.get(key)
    assert result == value

    client.set(key2, value2, noreply=True)
    result = client.get(key2)
    assert result == value2

    result = client.get_many([key, key2])
    assert result == {key: value, key2: value2}

    result = client.get_many([])
    assert result == {}


@pytest.mark.integration()
def test_get_set(client_class, host, port, socket_module):
    client = client_class((host, port), socket_module=socket_module)
    client.flush_all()

    key = b'key'
    value = b'value'
    key2 = b'key2'
    value2 = b'value2'
    get_set_helper(client, key, value, key2, value2)


@pytest.mark.integration()
def test_get_set_newbytes(client_class, host, port, socket_module):
    client = client_class((host, port), socket_module=socket_module)
    client.flush_all()

    key = newbytes(b'key3')
    value = b'value3'
    key2 = newbytes(b'key4')
    value2 = b'value4'
    get_set_helper(client, key, value, key2, value2)


@pytest.mark.integration()
def test_get_set_unicode_key(client_class, host, port, socket_module):
    client = client_class((host, port), socket_module=socket_module,
                          allow_unicode_keys=True)
    client.flush_all()

    key = u"こんにちは"
    value = b'hello'
    key2 = 'my☃'
    value2 = b'value2'
    get_set_helper(client, key, value, key2, value2)


@pytest.mark.integration()
def test_add_replace(client_class, host, port, socket_module):
    client = client_class((host, port), socket_module=socket_module)
    client.flush_all()

    result = client.add(b'key', b'value', noreply=False)
    assert result is True
    result = client.get(b'key')
    assert result == b'value'

    result = client.add(b'key', b'value2', noreply=False)
    assert result is False
    result = client.get(b'key')
    assert result == b'value'

    result = client.replace(b'key1', b'value1', noreply=False)
    assert result is False
    result = client.get(b'key1')
    assert result is None

    result = client.replace(b'key', b'value2', noreply=False)
    assert result is True
    result = client.get(b'key')
    assert result == b'value2'


@pytest.mark.integration()
def test_append_prepend(client_class, host, port, socket_module):
    client = client_class((host, port), socket_module=socket_module)
    client.flush_all()

    result = client.append(b'key', b'value', noreply=False)
    assert result is False
    result = client.get(b'key')
    assert result is None

    result = client.set(b'key', b'value', noreply=False)
    assert result is True
    result = client.append(b'key', b'after', noreply=False)
    assert result is True
    result = client.get(b'key')
    assert result == b'valueafter'

    result = client.prepend(b'key1', b'value', noreply=False)
    assert result is False
    result = client.get(b'key1')
    assert result is None

    result = client.prepend(b'key', b'before', noreply=False)
    assert result is True
    result = client.get(b'key')
    assert result == b'beforevalueafter'


@pytest.mark.integration()
def test_cas(client_class, host, port, socket_module):
    client = client_class((host, port), socket_module=socket_module)
    client.flush_all()
    result = client.cas(b'key', b'value', b'1', noreply=False)
    assert result is None

    result = client.set(b'key', b'value', noreply=False)
    assert result is True

    # binary, string, and raw int all match -- should all be encoded as b'1'
    result = client.cas(b'key', b'value', b'1', noreply=False)
    assert result is False
    result = client.cas(b'key', b'value', '1', noreply=False)
    assert result is False
    result = client.cas(b'key', b'value', 1, noreply=False)
    assert result is False

    result, cas = client.gets(b'key')
    assert result == b'value'

    result = client.cas(b'key', b'value1', cas, noreply=False)
    assert result is True

    result = client.cas(b'key', b'value2', cas, noreply=False)
    assert result is False


@pytest.mark.integration()
def test_gets(client_class, host, port, socket_module):
    client = client_class((host, port), socket_module=socket_module)
    client.flush_all()

    result = client.gets(b'key')
    assert result == (None, None)

    result = client.set(b'key', b'value', noreply=False)
    assert result is True
    result = client.gets(b'key')
    assert result[0] == b'value'


@pytest.mark.integration()
def test_delete(client_class, host, port, socket_module):
    client = client_class((host, port), socket_module=socket_module)
    client.flush_all()

    result = client.delete(b'key', noreply=False)
    assert result is False

    result = client.get(b'key')
    assert result is None
    result = client.set(b'key', b'value', noreply=False)
    assert result is True
    result = client.delete(b'key', noreply=False)
    assert result is True
    result = client.get(b'key')
    assert result is None


@pytest.mark.integration()
def test_incr_decr(client_class, host, port, socket_module):
    client = Client((host, port), socket_module=socket_module)
    client.flush_all()

    result = client.incr(b'key', 1, noreply=False)
    assert result is None

    result = client.set(b'key', b'0', noreply=False)
    assert result is True
    result = client.incr(b'key', 1, noreply=False)
    assert result == 1

    def _bad_int():
        client.incr(b'key', b'foobar')

    with pytest.raises(MemcacheClientError):
        _bad_int()

    result = client.decr(b'key1', 1, noreply=False)
    assert result is None

    result = client.decr(b'key', 1, noreply=False)
    assert result == 0
    result = client.get(b'key')
    assert result == b'0'


@pytest.mark.integration()
def test_touch(client_class, host, port, socket_module):
    client = client_class((host, port), socket_module=socket_module)
    client.flush_all()

    result = client.touch(b'key', noreply=False)
    assert result is False

    result = client.set(b'key', b'0', 1, noreply=False)
    assert result is True

    result = client.touch(b'key', noreply=False)
    assert result is True

    result = client.touch(b'key', 1, noreply=False)
    assert result is True


@pytest.mark.integration()
def test_misc(client_class, host, port, socket_module):
    client = Client((host, port), socket_module=socket_module)
    client.flush_all()


@pytest.mark.integration()
def test_serialization_deserialization(host, port, socket_module):
    class JsonSerde(object):
        def serialize(self, key, value):
            return json.dumps(value).encode('ascii'), 1

        def deserialize(self, key, value, flags):
            if flags == 1:
                return json.loads(value.decode('ascii'))
            return value

    client = Client((host, port), serde=JsonSerde(),
                    socket_module=socket_module)
    client.flush_all()

    value = {'a': 'b', 'c': ['d']}
    client.set(b'key', value)
    result = client.get(b'key')
    assert result == value


def serde_serialization_helper(client_class, host, port,
                               socket_module, serde):
    def check(value):
        client.set(b'key', value, noreply=False)
        result = client.get(b'key')
        assert result == value
        assert type(result) is type(value)

    client = client_class((host, port), serde=serde,
                          socket_module=socket_module)
    client.flush_all()

    check(b'byte string')
    check(u'unicode string')
    check('olé')
    check(u'olé')
    check(1)
    check(123123123123123123123)
    check({'a': 'pickle'})
    check([u'one pickle', u'two pickle'])
    testdict = defaultdict(int)
    testdict[u'one pickle']
    testdict[b'two pickle']
    check(testdict)


@pytest.mark.integration()
def test_serde_serialization(client_class, host, port, socket_module):
    serde_serialization_helper(client_class, host, port,
                               socket_module, pickle_serde)


@pytest.mark.integration()
def test_serde_serialization0(client_class, host, port, socket_module):
    serde_serialization_helper(
        client_class, host, port,
        socket_module,
        PickleSerde(pickle_version=0))


@pytest.mark.integration()
def test_serde_serialization2(client_class, host, port, socket_module):
    serde_serialization_helper(
        client_class, host, port,
        socket_module,
        PickleSerde(pickle_version=2))


@pytest.mark.integration()
def test_errors(client_class, host, port, socket_module):
    client = client_class((host, port), socket_module=socket_module)
    client.flush_all()

    def _key_with_ws():
        client.set(b'key with spaces', b'value', noreply=False)

    with pytest.raises(MemcacheIllegalInputError):
        _key_with_ws()

    def _key_with_illegal_carriage_return():
        client.set(b'\r\nflush_all', b'value', noreply=False)

    with pytest.raises(MemcacheIllegalInputError):
        _key_with_illegal_carriage_return()

    def _key_too_long():
        client.set(b'x' * 1024, b'value', noreply=False)

    with pytest.raises(MemcacheClientError):
        _key_too_long()

    def _unicode_key_in_set():
        client.set(six.u('\u0FFF'), b'value', noreply=False)

    with pytest.raises(MemcacheClientError):
        _unicode_key_in_set()

    def _unicode_key_in_get():
        client.get(six.u('\u0FFF'))

    with pytest.raises(MemcacheClientError):
        _unicode_key_in_get()

    def _unicode_value_in_set():
        client.set(b'key', six.u('\u0FFF'), noreply=False)

    with pytest.raises(MemcacheClientError):
        _unicode_value_in_set()


@pytest.mark.integration()
def test_tls(client_class, tls_host, tls_port, socket_module, tls_context):
    client = client_class(
        (tls_host, tls_port),
        socket_module=socket_module,
        tls_context=tls_context
    )
    client.flush_all()

    key = b'key'
    value = b'value'
    key2 = b'key2'
    value2 = b'value2'
    get_set_helper(client, key, value, key2, value2)
