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

import time
import pytest

try:
    import pylibmc
    HAS_PYLIBMC = True
except Exception:
    HAS_PYLIBMC = False

try:
    import memcache
    HAS_MEMCACHE = True
except Exception:
    HAS_MEMCACHE = False


try:
    import pymemcache.client
    HAS_PYMEMCACHE = True
except Exception:
    HAS_PYMEMCACHE = False


def run_client_test(name, client, size, count):
    client.flush_all()

    value = 'X' * size

    start = time.time()

    for i in range(count):
        client.set(str(i), value)

    for i in range(count):
        client.get(str(i))

    duration = time.time() - start
    print("{0}: {1}".format(name, duration))


@pytest.mark.benchmark()
@pytest.mark.skipif(not HAS_PYLIBMC,
                    reason="requires pylibmc")
def test_pylibmc(host, port, size, count):
    client = pylibmc.Client(['{0}:{1}'.format(host, port)])
    client.behaviors = {"tcp_nodelay": True}
    run_client_test('pylibmc', client, size, count)


@pytest.mark.benchmark()
@pytest.mark.skipif(not HAS_MEMCACHE,
                    reason="requires python-memcached")
def test_memcache(host, port, size, count):
    client = memcache.Client(['{0}:{1}'.format(host, port)])
    run_client_test('memcache', client, size, count)


@pytest.mark.benchmark()
@pytest.mark.skipif(not HAS_PYMEMCACHE,
                    reason="requires pymemcache")
def test_pymemcache(host, port, size, count):
    client = pymemcache.client.Client((host, port))
    run_client_test('pymemcache', client, size, count)
