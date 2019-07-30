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

import logging
from io import BytesIO
import six
from six.moves import cPickle as pickle

try:
    long_type = long  # noqa
except NameError:
    long_type = None


FLAG_BYTES = 0
FLAG_PICKLE = 1 << 0
FLAG_INTEGER = 1 << 1
FLAG_LONG = 1 << 2
FLAG_COMPRESSED = 1 << 3  # unused, to main compatibility with python-memcached
FLAG_TEXT = 1 << 4

# Pickle protocol version (highest available to runtime)
# Warning with `0`: If somewhere in your value lies a slotted object,
# ie defines `__slots__`, even if you do not include it in your pickleable
# state via `__getstate__`, python will complain with something like:
#   TypeError: a class that defines __slots__ without defining __getstate__
#   cannot be pickled
DEFAULT_PICKLE_VERSION = pickle.HIGHEST_PROTOCOL


class PythonMemcacheSerializer(object):
    """
    An object which implements the serialization/deserialization protocol for
    :py:class:`pymemcache.client.base.Client` and its descendants using pickle.

    Serialization and deserialization are implemented as methods of this class.
    To implement a custom serialization/deserialization method for pymemcache,
    you should implement the same interface as the one provided by this object
    -- :py:meth:`pymemcache.serde.PythonMemcacheSerializer.serialize` and
    :py:meth:`pymemcache.serde.PythonMemcacheSerializer.deserialize`. Then,
    pass your custom object to the pymemcache client object in place of
    `PythonMemcacheSerializer`.

    For more details on the serialization protocol, see the class documentation
    for :py:class:`pymemcache.client.base.Client`
    """

    def __init__(self, pickle_version=DEFAULT_PICKLE_VERSION):
        self.pickle_version = pickle_version

    def serialize(self, key, value):
        flags = 0
        value_type = type(value)

        # Check against exact types so that subclasses of native types will be
        # restored as their native type
        if value_type is bytes:
            pass

        elif value_type is six.text_type:
            flags |= FLAG_TEXT
            value = value.encode('utf8')

        elif value_type is int:
            flags |= FLAG_INTEGER
            value = "%d" % value

        elif six.PY2 and value_type is long_type:
            flags |= FLAG_LONG
            value = "%d" % value

        else:
            flags |= FLAG_PICKLE
            output = BytesIO()
            pickler = pickle.Pickler(output, self.pickle_version)
            pickler.dump(value)
            value = output.getvalue()

        return value, flags

    def deserialize(self, key, value, flags):
        if flags == 0:
            return value

        elif flags & FLAG_TEXT:
            return value.decode('utf8')

        elif flags & FLAG_INTEGER:
            return int(value)

        elif flags & FLAG_LONG:
            if six.PY3:
                return int(value)
            else:
                return long_type(value)

        elif flags & FLAG_PICKLE:
            try:
                buf = BytesIO(value)
                unpickler = pickle.Unpickler(buf)
                return unpickler.load()
            except Exception:
                logging.info('Pickle error', exc_info=True)
                return None

        return value


# default instance of the class, to be used in client construction, etc
python_memcache_serializer = PythonMemcacheSerializer()
