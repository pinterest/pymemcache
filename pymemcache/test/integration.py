# Copyright 2012 Pinterest.com
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

import argparse
import json
import socket

from pymemcache.client import (Client, MemcacheClientError,
                               MemcacheUnknownCommandError)
from pymemcache.client import MemcacheIllegalInputError
from nose import tools


def get_set_test(host, port, socket_module):
    client = Client((host, port), socket_module=socket_module)
    client.flush_all()

    result = client.get('key')
    tools.assert_equal(result, None)

    client.set('key', 'value', noreply=False)
    result = client.get('key')
    tools.assert_equal(result, 'value')

    client.set('key2', 'value2', noreply=True)
    result = client.get('key2')
    tools.assert_equal(result, 'value2')

    result = client.get_many(['key', 'key2'])
    tools.assert_equal(result, {'key': 'value', 'key2': 'value2'})

    result = client.get_many([])
    tools.assert_equal(result, {})


def add_replace_test(host, port, socket_module):
    client = Client((host, port), socket_module=socket_module)
    client.flush_all()

    result = client.add('key', 'value', noreply=False)
    tools.assert_equal(result, True)
    result = client.get('key')
    tools.assert_equal(result, 'value')

    result = client.add('key', 'value2', noreply=False)
    tools.assert_equal(result, False)
    result = client.get('key')
    tools.assert_equal(result, 'value')

    result = client.replace('key1', 'value1', noreply=False)
    tools.assert_equal(result, False)
    result = client.get('key1')
    tools.assert_equal(result, None)

    result = client.replace('key', 'value2', noreply=False)
    tools.assert_equal(result, True)
    result = client.get('key')
    tools.assert_equal(result, 'value2')


def append_prepend_test(host, port, socket_module):
    client = Client((host, port), socket_module=socket_module)
    client.flush_all()

    result = client.append('key', 'value', noreply=False)
    tools.assert_equal(result, False)
    result = client.get('key')
    tools.assert_equal(result, None)

    result = client.set('key', 'value', noreply=False)
    tools.assert_equal(result, True)
    result = client.append('key', 'after', noreply=False)
    tools.assert_equal(result, True)
    result = client.get('key')
    tools.assert_equal(result, 'valueafter')

    result = client.prepend('key1', 'value', noreply=False)
    tools.assert_equal(result, False)
    result = client.get('key1')
    tools.assert_equal(result, None)

    result = client.prepend('key', 'before', noreply=False)
    tools.assert_equal(result, True)
    result = client.get('key')
    tools.assert_equal(result, 'beforevalueafter')


def cas_test(host, port, socket_module):
    client = Client((host, port), socket_module=socket_module)
    client.flush_all()

    result = client.cas('key', 'value', '1', noreply=False)
    tools.assert_equal(result, None)

    result = client.set('key', 'value', noreply=False)
    tools.assert_equal(result, True)

    result = client.cas('key', 'value', '1', noreply=False)
    tools.assert_equal(result, False)

    result, cas = client.gets('key')
    tools.assert_equal(result, 'value')

    result = client.cas('key', 'value1', cas, noreply=False)
    tools.assert_equal(result, True)

    result = client.cas('key', 'value2', cas, noreply=False)
    tools.assert_equal(result, False)


def gets_test(host, port, socket_module):
    client = Client((host, port), socket_module=socket_module)
    client.flush_all()

    result = client.gets('key')
    tools.assert_equal(result, (None, None))

    result = client.set('key', 'value', noreply=False)
    tools.assert_equal(result, True)
    result = client.gets('key')
    tools.assert_equal(result[0], 'value')


def delete_test(host, port, socket_module):
    client = Client((host, port), socket_module=socket_module)
    client.flush_all()

    result = client.delete('key', noreply=False)
    tools.assert_equal(result, False)

    result = client.get('key')
    tools.assert_equal(result, None)
    result = client.set('key', 'value', noreply=False)
    tools.assert_equal(result, True)
    result = client.delete('key', noreply=False)
    tools.assert_equal(result, True)
    result = client.get('key')
    tools.assert_equal(result, None)


def incr_decr_test(host, port, socket_module):
    client = Client((host, port), socket_module=socket_module)
    client.flush_all()

    result = client.incr('key', 1, noreply=False)
    tools.assert_equal(result, None)

    result = client.set('key', '0', noreply=False)
    tools.assert_equal(result, True)
    result = client.incr('key', 1, noreply=False)
    tools.assert_equal(result, 1)

    def _bad_int():
        client.incr('key', 'foobar')

    tools.assert_raises(MemcacheClientError, _bad_int)

    result = client.decr('key1', 1, noreply=False)
    tools.assert_equal(result, None)

    result = client.decr('key', 1, noreply=False)
    tools.assert_equal(result, 0)
    result = client.get('key')
    tools.assert_equal(result, '0')


def misc_test(host, port, socket_module):
    client = Client((host, port), socket_module=socket_module)
    client.flush_all()


def test_serialization_deserialization(host, port, socket_module):
    def _ser(key, value):
        return json.dumps(value), 1

    def _des(key, value, flags):
        if flags == 1:
            return json.loads(value)
        return value

    client = Client((host, port), serializer=_ser, deserializer=_des,
                    socket_module=socket_module)
    client.flush_all()

    value = {'a': 'b', 'c': ['d']}
    client.set('key', value)
    result = client.get('key')
    tools.assert_equal(result, value)


def test_errors(host, port, socket_module):
    client = Client((host, port), socket_module=socket_module)
    client.flush_all()

    def _key_with_ws():
        client.set('key with spaces', 'value', noreply=False)

    tools.assert_raises(MemcacheIllegalInputError, _key_with_ws)

    def _key_too_long():
        client.set('x' * 1024, 'value', noreply=False)

    tools.assert_raises(MemcacheClientError, _key_too_long)

    def _unicode_key_in_set():
        client.set(u'\u0FFF', 'value', noreply=False)

    tools.assert_raises(MemcacheClientError, _unicode_key_in_set)

    def _unicode_key_in_get():
        client.get(u'\u0FFF')

    tools.assert_raises(MemcacheClientError, _unicode_key_in_get)

    def _unicode_value_in_set():
        client.set('key', u'\u0FFF', noreply=False)

    tools.assert_raises(MemcacheClientError, _unicode_value_in_set)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--server',
                        metavar='HOST',
                        required=True)
    parser.add_argument('-p', '--port',
                        metavar='PORT',
                        type=int,
                        required=True)

    args = parser.parse_args()

    socket_modules = [socket]
    try:
        from gevent import socket as gevent_socket
    except ImportError:
        print "Skipping gevent (not installed)"
    else:
        socket_modules.append(gevent_socket)

    for socket_module in socket_modules:
        print "Testing with socket module:", socket_module.__name__

        print "Testing get and set..."
        get_set_test(args.server, args.port, socket_module)
        print "Testing add and replace..."
        add_replace_test(args.server, args.port, socket_module)
        print "Testing append and prepend..."
        append_prepend_test(args.server, args.port, socket_module)
        print "Testing cas..."
        cas_test(args.server, args.port, socket_module)
        print "Testing gets..."
        gets_test(args.server, args.port, socket_module)
        print "Testing delete..."
        delete_test(args.server, args.port, socket_module)
        print "Testing incr and decr..."
        incr_decr_test(args.server, args.port, socket_module)
        print "Testing flush_all..."
        misc_test(args.server, args.port, socket_module)
        print "Testing serialization and deserialization..."
        test_serialization_deserialization(args.server, args.port,
                                           socket_module)
        print "Testing error cases..."
        test_errors(args.server, args.port, socket_module)


if __name__ == '__main__':
    main()
