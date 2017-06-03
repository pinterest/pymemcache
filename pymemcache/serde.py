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

log = logging.getLogger(__name__)

try:
    long_type = long  # noqa
except NameError:
    long_type = None

FLAG_BYTES = 0
FLAG_PICKLE = 1 << 0
FLAG_INTEGER = 1 << 1
FLAG_LONG = 1 << 2
FLAG_COMPRESSED = 1 << 3  # unused, to main compatability with python-memcached
FLAG_TEXT = 1 << 4


class Serde(object):
    """
    Serialization handler.

    Meant to be compatible with `python-memcached`.
    """

    pickle_version = 0

    def __init__(self, pickle_version=0):
        """
        Init

        :param int pickle_version: Pickle version to use (from python).
          Use `-1` to use the highest supported at runtime.
          Deserialization is not affected by this parameter.

          A forewarning with `0` (the default): If somewhere in your value lies
          a slotted object, ie defines `__slots__`, even if you do not include
          it in your pickleable state via `__getstate__`, python will raise:
            ```
            TypeError: a class that defines __slots__ without defining
            __getstate__ cannot be pickled
            ```
        """
        if pickle_version is not None:
            self.pickle_version = pickle_version

    def from_python(self, key, value):
        """
        Serialize a python object.

        :param str|unicode key: Key
        :param str|unicode value: Value
        :return tuple[str, str]: tuple(value, flags)
        """
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

    def to_python(self, key, value, flags):
        """
        Deserialize a value into a python object.

        :param str|unicode key: Key
        :param str|unicode value: Value
        :param int flags: Bitflag containing flags used to specify how to
        deserialize this object.
        :return object: Deserialized python object.
        """
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
            except Exception as exc:
                # This includes exc as a string for troubleshooting as well as providing
                # a trace.
                log.exception('Could not depickle value: %s')
                return None

        return value


# Backwards compatibility
_serde = Serde()
python_memcache_serializer = _serde.from_python
python_memcache_deserializer = _serde.to_python

