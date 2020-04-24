__version__ = '3.1.1'

from pymemcache.client.base import Client  # noqa
from pymemcache.client.base import PooledClient  # noqa

from pymemcache.exceptions import MemcacheError  # noqa
from pymemcache.exceptions import MemcacheClientError  # noqa
from pymemcache.exceptions import MemcacheUnknownCommandError  # noqa
from pymemcache.exceptions import MemcacheIllegalInputError  # noqa
from pymemcache.exceptions import MemcacheServerError  # noqa
from pymemcache.exceptions import MemcacheUnknownError  # noqa
from pymemcache.exceptions import MemcacheUnexpectedCloseError  # noqa
