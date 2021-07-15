# Copyright 2021 Pinterest.com
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
__version__ = '3.5.0'

from pymemcache.client.base import Client  # noqa
from pymemcache.client.base import PooledClient  # noqa
from pymemcache.client.hash import HashClient  # noqa
from pymemcache.client.base import KeepaliveOpts  # noqa

from pymemcache.exceptions import MemcacheError  # noqa
from pymemcache.exceptions import MemcacheClientError  # noqa
from pymemcache.exceptions import MemcacheUnknownCommandError  # noqa
from pymemcache.exceptions import MemcacheIllegalInputError  # noqa
from pymemcache.exceptions import MemcacheServerError  # noqa
from pymemcache.exceptions import MemcacheUnknownError  # noqa
from pymemcache.exceptions import MemcacheUnexpectedCloseError  # noqa
