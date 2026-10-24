# Copyright 2026 Stalin
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

import asyncio
from types import SimpleNamespace
from unittest import mock

import pytest

from pymemcache.pool import ObjectPool
from pymemcache.client.base import PooledClient


@pytest.mark.unit()
@pytest.mark.parametrize("destroy_on_fail", [False, True])
@pytest.mark.parametrize(
    "exception_type",
    [ValueError, KeyboardInterrupt, SystemExit, GeneratorExit, asyncio.CancelledError],
)
def test_get_and_release_cleans_up_on_exception(destroy_on_fail, exception_type):
    """Exceptional exits must not permanently consume a slot in the pool."""
    create = mock.Mock(side_effect=lambda: SimpleNamespace())
    remove = mock.Mock()
    pool = ObjectPool(create, after_remove=remove, max_size=1)
    error = exception_type()

    with pytest.raises(exception_type) as exc_info:
        with pool.get_and_release(destroy_on_fail=destroy_on_fail) as obj:
            raise error

    assert exc_info.value is error
    assert pool.used == ()
    if destroy_on_fail:
        assert pool.free == ()
        remove.assert_called_once_with(obj)
    else:
        assert pool.free == (obj,)
        remove.assert_not_called()

    with pool.get_and_release() as next_obj:
        if destroy_on_fail:
            assert next_obj is not obj
        else:
            assert next_obj is obj

    assert pool.used == ()
    assert create.call_count == (2 if destroy_on_fail else 1)


@pytest.mark.unit()
@pytest.mark.parametrize("destroy_on_fail", [False, True])
def test_get_and_release_reuses_object_on_success(destroy_on_fail):
    """Normal exits return the object to the pool for either failure policy."""
    remove = mock.Mock()
    pool = ObjectPool(SimpleNamespace, after_remove=remove, max_size=1)

    with pool.get_and_release(destroy_on_fail=destroy_on_fail) as obj:
        assert pool.used == (obj,)

    assert pool.used == ()
    assert pool.free == (obj,)
    remove.assert_not_called()


@pytest.mark.unit()
@pytest.mark.parametrize("ignore_exc", [False, True])
@pytest.mark.parametrize("exception_type", [KeyboardInterrupt, asyncio.CancelledError])
def test_pooled_client_discards_interrupted_connection(ignore_exc, exception_type):
    """An interrupted request must close its client and allow another request."""
    client = PooledClient("localhost", max_pool_size=1, ignore_exc=ignore_exc)
    interrupted = mock.Mock()
    interrupted.get.side_effect = exception_type()
    healthy = mock.Mock()
    healthy.get.return_value = b"value"

    with mock.patch.object(client, "client_class", side_effect=[interrupted, healthy]):
        with pytest.raises(exception_type):
            client.get(b"key")

        interrupted.close.assert_called_once_with()
        assert client.client_pool.used == ()
        assert client.client_pool.free == ()
        assert client.get(b"key") == b"value"

    healthy.close.assert_not_called()
