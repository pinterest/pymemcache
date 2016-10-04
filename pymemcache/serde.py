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
from six.moves import cPickle as pickle

try:
    long_type = long  # noqa
except NameError:
    long_type = None


FLAG_PICKLE = 1 << 0
FLAG_INTEGER = 1 << 1
FLAG_LONG = 1 << 2


def python_memcache_serializer(key, value):
    flags = 0

    if isinstance(value, str):
        pass
    elif isinstance(value, int):
        flags |= FLAG_INTEGER
        value = "%d" % value
    elif long_type is not None and isinstance(value, long_type):
        flags |= FLAG_LONG
        value = "%d" % value
    else:
        flags |= FLAG_PICKLE
        output = BytesIO()
        pickler = pickle.Pickler(output, 0)
        pickler.dump(value)
        value = output.getvalue()

    return value, flags


def python_memcache_deserializer(key, value, flags):
    if flags == 0:
        return value

    if flags & FLAG_INTEGER:
        return int(value)

    if flags & FLAG_LONG:
        return long_type(value)

    if flags & FLAG_PICKLE:
        try:
            buf = BytesIO(value)
            unpickler = pickle.Unpickler(buf)
            return unpickler.load()
        except Exception:
            logging.info('Pickle error', exc_info=True)
            return None

    return value
