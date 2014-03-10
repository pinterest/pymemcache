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
import time


def test_client(name, client, size, count):
    client.flush_all()

    value = 'X' * size

    start = time.time()

    for i in range(count):
        client.set(str(i), value)

    for i in range(count):
        client.get(str(i))

    duration = time.time() - start
    print("{0}: {1}".format(name, duration))


def test_pylibmc(host, port, size, count):
    try:
        import pylibmc
    except Exception:
        print("Could not import pylibmc, skipping test...")
        return

    client = pylibmc.Client(['{0}:{1}'.format(host, port)])
    client.behaviors = {"tcp_nodelay": True}
    test_client('pylibmc', client, size, count)


def test_memcache(host, port, size, count):
    try:
        import memcache
    except Exception:
        print("Could not import pymemcache.client, skipping test...")
        return

    client = memcache.Client(['{0}:{1}'.format(host, port)])
    test_client('memcache', client, size, count)


def test_pymemcache(host, port, size, count):
    try:
        import pymemcache.client
    except Exception:
        print("Could not import pymemcache.client, skipping test...")
        return

    client = pymemcache.client.Client((host, port))
    test_client('pymemcache', client, size, count)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--server',
                        metavar='HOST',
                        required=True)
    parser.add_argument('-p', '--port',
                        metavar='PORT',
                        type=int,
                        required=True)
    parser.add_argument('-z', '--size',
                        metavar='SIZE',
                        default=1024,
                        type=int)
    parser.add_argument('-c', '--count',
                        metavar='COUNT',
                        default=10000,
                        type=int)

    args = parser.parse_args()

    test_pylibmc(args.server, args.port, args.size, args.count)
    test_memcache(args.server, args.port, args.size, args.count)
    test_pymemcache(args.server, args.port, args.size, args.count)


if __name__ == '__main__':
    main()
