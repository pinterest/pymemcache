import argparse
import json

from pymemcache.client import Client, MemcacheClientError
from nose import tools


def get_set_test(host, port):
    client = Client((host, port))
    client.flush_all()

    result = client.get('key')
    tools.assert_equal(result, None)

    client.set('key', 'value')
    result = client.get('key')
    tools.assert_equal(result, 'value')

    client.set('key2', 'value2', noreply=True)
    result = client.get('key2')
    tools.assert_equal(result, 'value2')

    result = client.get_many(['key', 'key2'])
    tools.assert_equal(result, {'key': 'value', 'key2': 'value2'})


def add_replace_test(host, port):
    client = Client((host, port))
    client.flush_all()

    result = client.add('key', 'value')
    tools.assert_equal(result, 'STORED')
    result = client.get('key')
    tools.assert_equal(result, 'value')

    result = client.add('key', 'value2')
    tools.assert_equal(result, 'NOT_STORED')
    result = client.get('key')
    tools.assert_equal(result, 'value')

    result = client.replace('key1', 'value1')
    tools.assert_equal(result, 'NOT_STORED')
    result = client.get('key1')
    tools.assert_equal(result, None)

    result = client.replace('key', 'value2')
    tools.assert_equal(result, 'STORED')
    result = client.get('key')
    tools.assert_equal(result, 'value2')


def append_prepend_test(host, port):
    client = Client((host, port))
    client.flush_all()

    result = client.append('key', 'value')
    tools.assert_equal(result, 'NOT_STORED')
    result = client.get('key')
    tools.assert_equal(result, None)

    result = client.set('key', 'value')
    tools.assert_equal(result, 'STORED')
    result = client.append('key', 'after')
    tools.assert_equal(result, 'STORED')
    result = client.get('key')
    tools.assert_equal(result, 'valueafter')

    result = client.prepend('key1', 'value')
    tools.assert_equal(result, 'NOT_STORED')
    result = client.get('key1')
    tools.assert_equal(result, None)

    result = client.prepend('key', 'before')
    tools.assert_equal(result, 'STORED')
    result = client.get('key')
    tools.assert_equal(result, 'beforevalueafter')


def cas_test(host, port):
    client = Client((host, port))
    client.flush_all()

    result = client.cas('key', 'value', '1')
    tools.assert_equal(result, 'NOT_FOUND')

    result = client.set('key', 'value')
    tools.assert_equal(result, 'STORED')

    result = client.cas('key', 'value', '1')
    tools.assert_equal(result, 'EXISTS')

    result, cas = client.gets('key')
    tools.assert_equal(result, 'value')

    result = client.cas('key', 'value1', cas)
    tools.assert_equal(result, 'STORED')

    result = client.cas('key', 'value2', cas)
    tools.assert_equal(result, 'EXISTS')


def gets_test(host, port):
    client = Client((host, port))
    client.flush_all()

    result = client.gets('key')
    tools.assert_equal(result, (None, None))

    result = client.set('key', 'value')
    tools.assert_equal(result, 'STORED')
    result = client.gets('key')
    tools.assert_equal(result[0], 'value')


def delete_test(host, port):
    client = Client((host, port))
    client.flush_all()

    result = client.delete('key')
    tools.assert_equal(result, 'NOT_FOUND')

    result = client.get('key')
    tools.assert_equal(result, None)
    result = client.set('key', 'value')
    tools.assert_equal(result, 'STORED')
    result = client.delete('key')
    tools.assert_equal(result, 'DELETED')
    result = client.get('key')
    tools.assert_equal(result, None)


def incr_decr_test(host, port):
    client = Client((host, port))
    client.flush_all()

    result = client.incr('key', 1)
    tools.assert_equal(result, 'NOT_FOUND')

    result = client.set('key', '0')
    tools.assert_equal(result, 'STORED')
    result = client.incr('key', 1)
    tools.assert_equal(result, 1)

    def _bad_int():
        client.incr('key', 'foobar')

    tools.assert_raises(MemcacheClientError, _bad_int)

    result = client.decr('key1', 1)
    tools.assert_equal(result, 'NOT_FOUND')

    result = client.decr('key', 1)
    tools.assert_equal(result, 0)
    result = client.get('key')
    tools.assert_equal(result, '0')


def misc_test(host, port):
    client = Client((host, port))
    client.flush_all()


def test_serialization_deserialization(host, port):
    def _ser(value):
        return json.dumps(value), 1

    def _des(value, flags):
        if flags == 1:
            return json.loads(value)
        return value

    client = Client((host, port), serializer=_ser, deserializer=_des)
    client.flush_all()

    value = {'a': 'b', 'c': ['d']}
    client.set('key', value)
    result = client.get('key')
    tools.assert_equal(result, value)


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

    get_set_test(args.server, args.port)
    add_replace_test(args.server, args.port)
    append_prepend_test(args.server, args.port)
    cas_test(args.server, args.port)
    gets_test(args.server, args.port)
    delete_test(args.server, args.port)
    incr_decr_test(args.server, args.port)
    misc_test(args.server, args.port)
    test_serialization_deserialization(args.server, args.port)

if __name__ == '__main__':
    main()
