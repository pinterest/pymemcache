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
import errno
import socket
import six

from pymemcache import pool

from pymemcache.exceptions import (
    MemcacheClientError,
    MemcacheUnknownCommandError,
    MemcacheIllegalInputError,
    MemcacheServerError,
    MemcacheUnknownError,
    MemcacheUnexpectedCloseError
)


RECV_SIZE = 4096
VALID_STORE_RESULTS = {
    b'set':     (b'STORED', b'NOT_STORED'),
    b'add':     (b'STORED', b'NOT_STORED'),
    b'replace': (b'STORED', b'NOT_STORED'),
    b'append':  (b'STORED', b'NOT_STORED'),
    b'prepend': (b'STORED', b'NOT_STORED'),
    b'cas':     (b'STORED', b'EXISTS', b'NOT_FOUND'),
}
VALID_STRING_TYPES = (six.text_type, six.string_types)


# Some of the values returned by the "stats" command
# need mapping into native Python types
def _parse_bool_int(value):
    return int(value) != 0


def _parse_bool_string_is_yes(value):
    return value == b'yes'


def _parse_float(value):
    return float(value.replace(b':', b'.'))


def _parse_hex(value):
    return int(value, 8)


STAT_TYPES = {
    # General stats
    b'version': six.binary_type,
    b'rusage_user': _parse_float,
    b'rusage_system': _parse_float,
    b'hash_is_expanding': _parse_bool_int,
    b'slab_reassign_running': _parse_bool_int,

    # Settings stats
    b'inter': six.binary_type,
    b'growth_factor': float,
    b'stat_key_prefix': six.binary_type,
    b'umask': _parse_hex,
    b'detail_enabled': _parse_bool_int,
    b'cas_enabled': _parse_bool_int,
    b'auth_enabled_sasl': _parse_bool_string_is_yes,
    b'maxconns_fast': _parse_bool_int,
    b'slab_reassign': _parse_bool_int,
    b'slab_automove': _parse_bool_int,
}

# Common helper functions.


def _check_key(key, allow_unicode_keys, key_prefix=b''):
    """Checks key and add key_prefix."""
    if allow_unicode_keys:
        if isinstance(key, six.text_type):
            key = key.encode('utf8')
    elif isinstance(key, VALID_STRING_TYPES):
        try:
            if isinstance(key, bytes):
                key = key.decode().encode('ascii')
            else:
                key = key.encode('ascii')
        except (UnicodeEncodeError, UnicodeDecodeError):
            raise MemcacheIllegalInputError("Non-ASCII key: %r" % key)

    key = key_prefix + key
    parts = key.split()

    if len(key) > 250:
        raise MemcacheIllegalInputError("Key is too long: %r" % key)
    # second statement catches leading or trailing whitespace
    elif len(parts) > 1 or parts[0] != key:
        raise MemcacheIllegalInputError("Key contains whitespace: %r" % key)
    elif b'\00' in key:
        raise MemcacheIllegalInputError("Key contains null: %r" % key)

    return key


class Client(object):
    """
    A client for a single memcached server.

    *Server Connection*

     The ``server`` parameter controls how the client connects to the memcached
     server. You can either use a (host, port) tuple for a TCP connection or a
     string containing the path to a UNIX domain socket.

     The ``connect_timeout`` and ``timeout`` parameters can be used to set
     socket timeout values. By default, timeouts are disabled.

     When the ``no_delay`` flag is set, the ``TCP_NODELAY`` socket option will
     also be set. This only applies to TCP-based connections.

     Lastly, the ``socket_module`` allows you to specify an alternate socket
     implementation (such as `gevent.socket`_).

     .. _gevent.socket: http://www.gevent.org/api/gevent.socket.html

    *Keys and Values*

     Keys must have a __str__() method which should return a str with no more
     than 250 ASCII characters and no whitespace or control characters. Unicode
     strings must be encoded (as UTF-8, for example) unless they consist only
     of ASCII characters that are neither whitespace nor control characters.

     Values must have a __str__() method to convert themselves to a byte
     string. Unicode objects can be a problem since str() on a Unicode object
     will attempt to encode it as ASCII (which will fail if the value contains
     code points larger than U+127). You can fix this with a serializer or by
     just calling encode on the string (using UTF-8, for instance).

     If you intend to use anything but str as a value, it is a good idea to use
     a serializer and deserializer. The pymemcache.serde library has some
     already implemented serializers, including one that is compatible with
     the python-memcache library.

    *Serialization and Deserialization*

     The constructor takes two optional functions, one for "serialization" of
     values, and one for "deserialization". The serialization function takes
     two arguments, a key and a value, and returns a tuple of two elements, the
     serialized value, and an integer in the range 0-65535 (the "flags"). The
     deserialization function takes three parameters, a key, value and flags
     and returns the deserialized value.

     Here is an example using JSON for non-str values:

     .. code-block:: python

         def serialize_json(key, value):
             if type(value) == str:
                 return value, 1
             return json.dumps(value), 2

         def deserialize_json(key, value, flags):
             if flags == 1:
                 return value

             if flags == 2:
                 return json.loads(value)

             raise Exception("Unknown flags for value: {1}".format(flags))

    .. note::

     Most write operations allow the caller to provide a ``flags`` value to
     support advanced interaction with the server. This will **override** the
     "flags" value returned by the serializer and should therefore only be
     used when you have a complete understanding of how the value should be
     serialized, stored, and deserialized.

    *Error Handling*

     All of the methods in this class that talk to memcached can throw one of
     the following exceptions:

      * :class:`pymemcache.exceptions.MemcacheUnknownCommandError`
      * :class:`pymemcache.exceptions.MemcacheClientError`
      * :class:`pymemcache.exceptions.MemcacheServerError`
      * :class:`pymemcache.exceptions.MemcacheUnknownError`
      * :class:`pymemcache.exceptions.MemcacheUnexpectedCloseError`
      * :class:`pymemcache.exceptions.MemcacheIllegalInputError`
      * :class:`socket.timeout`
      * :class:`socket.error`

     Instances of this class maintain a persistent connection to memcached
     which is terminated when any of these exceptions are raised. The next
     call to a method on the object will result in a new connection being made
     to memcached.
    """

    def __init__(self,
                 server,
                 serializer=None,
                 deserializer=None,
                 connect_timeout=None,
                 timeout=None,
                 no_delay=False,
                 ignore_exc=False,
                 socket_module=socket,
                 key_prefix=b'',
                 default_noreply=True,
                 allow_unicode_keys=False,
                 encoding='ascii'):
        """
        Constructor.

        Args:
          server: tuple(hostname, port) or string containing a UNIX socket path.
          serializer: optional function, see notes in the class docs.
          deserializer: optional function, see notes in the class docs.
          connect_timeout: optional float, seconds to wait for a connection to
            the memcached server. Defaults to "forever" (uses the underlying
            default socket timeout, which can be very long).
          timeout: optional float, seconds to wait for send or recv calls on
            the socket connected to memcached. Defaults to "forever" (uses the
            underlying default socket timeout, which can be very long).
          no_delay: optional bool, set the TCP_NODELAY flag, which may help
            with performance in some cases. Defaults to False.
          ignore_exc: optional bool, True to cause the "get", "gets",
            "get_many" and "gets_many" calls to treat any errors as cache
            misses. Defaults to False.
          socket_module: socket module to use, e.g. gevent.socket. Defaults to
            the standard library's socket module.
          key_prefix: Prefix of key. You can use this as namespace. Defaults
            to b''.
          default_noreply: bool, the default value for 'noreply' as passed to
            store commands (except from cas, incr, and decr, which default to
            False).
          allow_unicode_keys: bool, support unicode (utf8) keys
          encoding: optional str, controls data encoding (defaults to 'ascii').

        Notes:
          The constructor does not make a connection to memcached. The first
          call to a method on the object will do that.
        """
        self.server = server
        self.serializer = serializer
        self.deserializer = deserializer
        self.connect_timeout = connect_timeout
        self.timeout = timeout
        self.no_delay = no_delay
        self.ignore_exc = ignore_exc
        self.socket_module = socket_module
        self.sock = None
        if isinstance(key_prefix, six.text_type):
            key_prefix = key_prefix.encode('ascii')
        if not isinstance(key_prefix, bytes):
            raise TypeError("key_prefix should be bytes.")
        self.key_prefix = key_prefix
        self.default_noreply = default_noreply
        self.allow_unicode_keys = allow_unicode_keys
        self.encoding = encoding

    def check_key(self, key):
        """Checks key and add key_prefix."""
        return _check_key(key, allow_unicode_keys=self.allow_unicode_keys,
                          key_prefix=self.key_prefix)

    def _connect(self):
        self.close()

        if isinstance(self.server, (list, tuple)):
            sock = self.socket_module.socket(self.socket_module.AF_INET,
                                             self.socket_module.SOCK_STREAM)
        else:
            sock = self.socket_module.socket(self.socket_module.AF_UNIX,
                                             self.socket_module.SOCK_STREAM)
        try:
            sock.settimeout(self.connect_timeout)
            sock.connect(self.server)
            sock.settimeout(self.timeout)
            if self.no_delay and sock.family == self.socket_module.AF_INET:
                sock.setsockopt(self.socket_module.IPPROTO_TCP,
                                self.socket_module.TCP_NODELAY, 1)
        except Exception:
            sock.close()
            raise

        self.sock = sock

    def close(self):
        """Close the connection to memcached, if it is open. The next call to a
        method that requires a connection will re-open it."""
        if self.sock is not None:
            try:
                self.sock.close()
            except Exception:
                pass
            finally:
                self.sock = None

    def set(self, key, value, expire=0, noreply=None, flags=None):
        """
        The memcached "set" command.

        Args:
          key: str, see class docs for details.
          value: str, see class docs for details.
          expire: optional int, number of seconds until the item is expired
                  from the cache, or zero for no expiry (the default).
          noreply: optional bool, True to not wait for the reply (defaults to
                   self.default_noreply).
          flags: optional int, arbitrary bit field used for server-specific
                flags

        Returns:
          If no exception is raised, always returns True. If an exception is
          raised, the set may or may not have occurred. If noreply is True,
          then a successful return does not guarantee a successful set.
        """
        if noreply is None:
            noreply = self.default_noreply
        return self._store_cmd(b'set', {key: value}, expire, noreply,
                               flags=flags)[key]

    def set_many(self, values, expire=0, noreply=None, flags=None):
        """
        A convenience function for setting multiple values.

        Args:
          values: dict(str, str), a dict of keys and values, see class docs
                  for details.
          expire: optional int, number of seconds until the item is expired
                  from the cache, or zero for no expiry (the default).
          noreply: optional bool, True to not wait for the reply (defaults to
                   self.default_noreply).
          flags: optional int, arbitrary bit field used for server-specific
                 flags

        Returns:
          Returns a list of keys that failed to be inserted.
          If noreply is True, always returns empty list.
        """
        if noreply is None:
            noreply = self.default_noreply
        result = self._store_cmd(b'set', values, expire, noreply, flags=flags)
        return [k for k, v in six.iteritems(result) if not v]

    set_multi = set_many

    def add(self, key, value, expire=0, noreply=None, flags=None):
        """
        The memcached "add" command.

        Args:
          key: str, see class docs for details.
          value: str, see class docs for details.
          expire: optional int, number of seconds until the item is expired
                  from the cache, or zero for no expiry (the default).
          noreply: optional bool, True to not wait for the reply (defaults to
                   self.default_noreply).
          flags: optional int, arbitrary bit field used for server-specific
                  flags

        Returns:
          If noreply is True, the return value is always True. Otherwise the
          return value is True if the value was stored, and False if it was
          not (because the key already existed).
        """
        if noreply is None:
            noreply = self.default_noreply
        return self._store_cmd(b'add', {key: value}, expire, noreply,
                               flags=flags)[key]

    def replace(self, key, value, expire=0, noreply=None, flags=None):
        """
        The memcached "replace" command.

        Args:
          key: str, see class docs for details.
          value: str, see class docs for details.
          expire: optional int, number of seconds until the item is expired
                  from the cache, or zero for no expiry (the default).
          noreply: optional bool, True to not wait for the reply (defaults to
                   self.default_noreply).
          flags: optional int, arbitrary bit field used for server-specific
                flags

        Returns:
          If noreply is True, always returns True. Otherwise returns True if
          the value was stored and False if it wasn't (because the key didn't
          already exist).
        """
        if noreply is None:
            noreply = self.default_noreply
        return self._store_cmd(b'replace', {key: value}, expire, noreply,
                               flags=flags)[key]

    def append(self, key, value, expire=0, noreply=None, flags=None):
        """
        The memcached "append" command.

        Args:
          key: str, see class docs for details.
          value: str, see class docs for details.
          expire: optional int, number of seconds until the item is expired
                  from the cache, or zero for no expiry (the default).
          noreply: optional bool, True to not wait for the reply (defaults to
                   self.default_noreply).
          flags: optional int, arbitrary bit field used for server-specific
                flags

        Returns:
          True.
        """
        if noreply is None:
            noreply = self.default_noreply
        return self._store_cmd(b'append', {key: value}, expire, noreply,
                               flags=flags)[key]

    def prepend(self, key, value, expire=0, noreply=None, flags=None):
        """
        The memcached "prepend" command.

        Args:
          key: str, see class docs for details.
          value: str, see class docs for details.
          expire: optional int, number of seconds until the item is expired
                  from the cache, or zero for no expiry (the default).
          noreply: optional bool, True to not wait for the reply (defaults to
                   self.default_noreply).
          flags: optional int, arbitrary bit field used for server-specific
                flags

        Returns:
          True.
        """
        if noreply is None:
            noreply = self.default_noreply
        return self._store_cmd(b'prepend', {key: value}, expire, noreply,
                               flags=flags)[key]

    def cas(self, key, value, cas, expire=0, noreply=False, flags=None):
        """
        The memcached "cas" command.

        Args:
          key: str, see class docs for details.
          value: str, see class docs for details.
          cas: int or str that only contains the characters '0'-'9'.
          expire: optional int, number of seconds until the item is expired
                  from the cache, or zero for no expiry (the default).
          noreply: optional bool, False to wait for the reply (the default).
          flags: optional int, arbitrary bit field used for server-specific
                flags

        Returns:
          If noreply is True, always returns True. Otherwise returns None if
          the key didn't exist, False if it existed but had a different cas
          value and True if it existed and was changed.
        """
        return self._store_cmd(b'cas', {key: value}, expire, noreply,
                               flags=flags, cas=cas)[key]

    def get(self, key, default=None):
        """
        The memcached "get" command, but only for one key, as a convenience.

        Args:
          key: str, see class docs for details.
          default: value that will be returned if the key was not found.

        Returns:
          The value for the key, or default if the key wasn't found.
        """
        return self._fetch_cmd(b'get', [key], False).get(key, default)

    def get_many(self, keys):
        """
        The memcached "get" command.

        Args:
          keys: list(str), see class docs for details.

        Returns:
          A dict in which the keys are elements of the "keys" argument list
          and the values are values from the cache. The dict may contain all,
          some or none of the given keys.
        """
        if not keys:
            return {}

        return self._fetch_cmd(b'get', keys, False)

    get_multi = get_many

    def gets(self, key, default=None, cas_default=None):
        """
        The memcached "gets" command for one key, as a convenience.

        Args:
          key: str, see class docs for details.
          default: value that will be returned if the key was not found.
          cas_default: same behaviour as default argument.

        Returns:
          A tuple of (value, cas)
          or (default, cas_defaults) if the key was not found.
        """
        defaults = (default, cas_default)
        return self._fetch_cmd(b'gets', [key], True).get(key, defaults)

    def gets_many(self, keys):
        """
        The memcached "gets" command.

        Args:
          keys: list(str), see class docs for details.

        Returns:
          A dict in which the keys are elements of the "keys" argument list and
          the values are tuples of (value, cas) from the cache. The dict may
          contain all, some or none of the given keys.
        """
        if not keys:
            return {}

        return self._fetch_cmd(b'gets', keys, True)

    def delete(self, key, noreply=None):
        """
        The memcached "delete" command.

        Args:
          key: str, see class docs for details.
          noreply: optional bool, True to not wait for the reply (defaults to
                   self.default_noreply).

        Returns:
          If noreply is True, always returns True. Otherwise returns True if
          the key was deleted, and False if it wasn't found.
        """
        if noreply is None:
            noreply = self.default_noreply
        cmd = b'delete ' + self.check_key(key)
        if noreply:
            cmd += b' noreply'
        cmd += b'\r\n'
        results = self._misc_cmd([cmd], b'delete', noreply)
        if noreply:
            return True
        return results[0] == b'DELETED'

    def delete_many(self, keys, noreply=None):
        """
        A convenience function to delete multiple keys.

        Args:
          keys: list(str), the list of keys to delete.
          noreply: optional bool, True to not wait for the reply (defaults to
                   self.default_noreply).

        Returns:
          True. If an exception is raised then all, some or none of the keys
          may have been deleted. Otherwise all the keys have been sent to
          memcache for deletion and if noreply is False, they have been
          acknowledged by memcache.
        """
        if not keys:
            return True

        if noreply is None:
            noreply = self.default_noreply

        cmds = []
        for key in keys:
            cmds.append(
                b'delete ' + self.check_key(key) +
                (b' noreply' if noreply else b'') +
                b'\r\n')
        self._misc_cmd(cmds, b'delete', noreply)
        return True

    delete_multi = delete_many

    def incr(self, key, value, noreply=False):
        """
        The memcached "incr" command.

        Args:
          key: str, see class docs for details.
          value: int, the amount by which to increment the value.
          noreply: optional bool, False to wait for the reply (the default).

        Returns:
          If noreply is True, always returns None. Otherwise returns the new
          value of the key, or None if the key wasn't found.
        """
        key = self.check_key(key)
        cmd = b'incr ' + key + b' ' + six.text_type(value).encode(self.encoding)
        if noreply:
            cmd += b' noreply'
        cmd += b'\r\n'
        results = self._misc_cmd([cmd], b'incr', noreply)
        if noreply:
            return None
        if results[0] == b'NOT_FOUND':
            return None
        return int(results[0])

    def decr(self, key, value, noreply=False):
        """
        The memcached "decr" command.

        Args:
          key: str, see class docs for details.
          value: int, the amount by which to increment the value.
          noreply: optional bool, False to wait for the reply (the default).

        Returns:
          If noreply is True, always returns None. Otherwise returns the new
          value of the key, or None if the key wasn't found.
        """
        key = self.check_key(key)
        cmd = b'decr ' + key + b' ' + six.text_type(value).encode(self.encoding)
        if noreply:
            cmd += b' noreply'
        cmd += b'\r\n'
        results = self._misc_cmd([cmd], b'decr', noreply)
        if noreply:
            return None
        if results[0] == b'NOT_FOUND':
            return None
        return int(results[0])

    def touch(self, key, expire=0, noreply=None):
        """
        The memcached "touch" command.

        Args:
          key: str, see class docs for details.
          expire: optional int, number of seconds until the item is expired
                  from the cache, or zero for no expiry (the default).
          noreply: optional bool, True to not wait for the reply (defaults to
                   self.default_noreply).

        Returns:
          True if the expiration time was updated, False if the key wasn't
          found.
        """
        if noreply is None:
            noreply = self.default_noreply
        key = self.check_key(key)
        cmd = (
            b'touch ' + key + b' ' + six.text_type(expire).encode(self.encoding)
        )
        if noreply:
            cmd += b' noreply'
        cmd += b'\r\n'
        results = self._misc_cmd([cmd], b'touch', noreply)
        if noreply:
            return True
        return results[0] == b'TOUCHED'

    def stats(self, *args):
        """
        The memcached "stats" command.

        The returned keys depend on what the "stats" command returns.
        A best effort is made to convert values to appropriate Python
        types, defaulting to strings when a conversion cannot be made.

        Args:
          *arg: extra string arguments to the "stats" command. See the
                memcached protocol documentation for more information.

        Returns:
          A dict of the returned stats.
        """
        result = self._fetch_cmd(b'stats', args, False)

        for key, value in six.iteritems(result):
            converter = STAT_TYPES.get(key, int)
            try:
                result[key] = converter(value)
            except Exception:
                pass

        return result

    def cache_memlimit(self, memlimit):
        """
        The memcached "cache_memlimit" command.

        Args:
          memlimit: int, the number of megabytes to set as the new cache memory
                    limit.

        Returns:
          If no exception is raised, always returns True.
        """

        self._fetch_cmd(b'cache_memlimit', [str(int(memlimit))], False)
        return True

    def version(self):
        """
        The memcached "version" command.

        Returns:
            A string of the memcached version.
        """
        cmd = b"version\r\n"
        results = self._misc_cmd([cmd], b'version', False)
        before, _, after = results[0].partition(b' ')

        if before != b'VERSION':
            raise MemcacheUnknownError(
                "Received unexpected response: %s" % results[0])
        return after

    def flush_all(self, delay=0, noreply=None):
        """
        The memcached "flush_all" command.

        Args:
          delay: optional int, the number of seconds to wait before flushing,
                 or zero to flush immediately (the default).
          noreply: optional bool, True to not wait for the reply (defaults to
                   self.default_noreply).

        Returns:
          True.
        """
        if noreply is None:
            noreply = self.default_noreply
        cmd = b'flush_all ' + six.text_type(delay).encode(self.encoding)
        if noreply:
            cmd += b' noreply'
        cmd += b'\r\n'
        results = self._misc_cmd([cmd], b'flush_all', noreply)
        if noreply:
            return True
        return results[0] == b'OK'

    def quit(self):
        """
        The memcached "quit" command.

        This will close the connection with memcached. Calling any other
        method on this object will re-open the connection, so this object can
        be re-used after quit.
        """
        cmd = b"quit\r\n"
        self._misc_cmd([cmd], b'quit', True)
        self.close()

    def _raise_errors(self, line, name):
        if line.startswith(b'ERROR'):
            raise MemcacheUnknownCommandError(name)

        if line.startswith(b'CLIENT_ERROR'):
            error = line[line.find(b' ') + 1:]
            raise MemcacheClientError(error)

        if line.startswith(b'SERVER_ERROR'):
            error = line[line.find(b' ') + 1:]
            raise MemcacheServerError(error)

    def _extract_value(self, expect_cas, line, buf, remapped_keys,
                       prefixed_keys):
        """
        This function is abstracted from _fetch_cmd to support different ways
        of value extraction. In order to use this feature, _extract_value needs
        to be overriden in the subclass.
        """
        if expect_cas:
            _, key, flags, size, cas = line.split()
        else:
            try:
                _, key, flags, size = line.split()
            except Exception as e:
                raise ValueError("Unable to parse line %s: %s" % (line, e))

        buf, value = _readvalue(self.sock, buf, int(size))
        key = remapped_keys[key]
        if self.deserializer:
            value = self.deserializer(key, value, int(flags))

        if expect_cas:
            return key, (value, cas), buf
        else:
            return key, value, buf

    def _fetch_cmd(self, name, keys, expect_cas):
        prefixed_keys = [self.check_key(k) for k in keys]
        remapped_keys = dict(zip(prefixed_keys, keys))

        # It is important for all keys to be listed in their original order.
        cmd = name + b' ' + b' '.join(prefixed_keys) + b'\r\n'

        try:
            if self.sock is None:
                self._connect()

            self.sock.sendall(cmd)

            buf = b''
            result = {}
            while True:
                buf, line = _readline(self.sock, buf)
                self._raise_errors(line, name)
                if line == b'END' or line == b'OK':
                    return result
                elif line.startswith(b'VALUE'):
                    key, value, buf = self._extract_value(expect_cas, line, buf,
                                                          remapped_keys,
                                                          prefixed_keys)
                    result[key] = value
                elif name == b'stats' and line.startswith(b'STAT'):
                    key_value = line.split()
                    result[key_value[1]] = key_value[2]
                elif name == b'stats' and line.startswith(b'ITEM'):
                    # For 'stats cachedump' commands
                    key_value = line.split()
                    result[key_value[1]] = b' '.join(key_value[2:])
                else:
                    raise MemcacheUnknownError(line[:32])
        except Exception:
            self.close()
            if self.ignore_exc:
                return {}
            raise

    def _store_cmd(self, name, values, expire, noreply, flags=None, cas=None):
        cmds = []
        keys = []

        extra = b''
        if cas is not None:
            extra += b' ' + cas
        if noreply:
            extra += b' noreply'
        expire = six.text_type(expire).encode(self.encoding)

        for key, data in six.iteritems(values):
            # must be able to reliably map responses back to the original order
            keys.append(key)

            key = self.check_key(key)
            if self.serializer:
                data, data_flags = self.serializer(key, data)
            else:
                data_flags = 0

            # If 'flags' was explicitly provided, it overrides the value
            # returned by the serializer.
            if flags is not None:
                data_flags = flags

            if not isinstance(data, six.binary_type):
                try:
                    data = six.text_type(data).encode(self.encoding)
                except UnicodeEncodeError as e:
                    raise MemcacheIllegalInputError(
                            "Data values must be binary-safe: %s" % e)

            cmds.append(name + b' ' + key + b' ' +
                        six.text_type(data_flags).encode(self.encoding) +
                        b' ' + expire +
                        b' ' + six.text_type(len(data)).encode(self.encoding) +
                        extra + b'\r\n' + data + b'\r\n')

        if self.sock is None:
            self._connect()

        try:
            self.sock.sendall(b''.join(cmds))
            if noreply:
                return {k: True for k in keys}

            results = {}
            buf = b''
            for key in keys:
                buf, line = _readline(self.sock, buf)
                self._raise_errors(line, name)

                if line in VALID_STORE_RESULTS[name]:
                    if line == b'STORED':
                        results[key] = True
                    if line == b'NOT_STORED':
                        results[key] = False
                    if line == b'NOT_FOUND':
                        results[key] = None
                    if line == b'EXISTS':
                        results[key] = False
                else:
                    raise MemcacheUnknownError(line[:32])
            return results
        except Exception:
            self.close()
            raise

    def _misc_cmd(self, cmds, cmd_name, noreply):
        if self.sock is None:
            self._connect()

        try:
            self.sock.sendall(b''.join(cmds))

            if noreply:
                return []

            results = []
            buf = b''
            for cmd in cmds:
                buf, line = _readline(self.sock, buf)
                self._raise_errors(line, cmd_name)
                results.append(line)
            return results

        except Exception:
            self.close()
            raise

    def __setitem__(self, key, value):
        self.set(key, value, noreply=True)

    def __getitem__(self, key):
        value = self.get(key)
        if value is None:
            raise KeyError
        return value

    def __delitem__(self, key):
        self.delete(key, noreply=True)


class PooledClient(object):
    """A thread-safe pool of clients (with the same client api).

    Args:
      max_pool_size: maximum pool size to use (going above this amount
                     triggers a runtime error), by default this is 2147483648L
                     when not provided (or none).
      lock_generator: a callback/type that takes no arguments that will
                      be called to create a lock or sempahore that can
                      protect the pool from concurrent access (for example a
                      eventlet lock or semaphore could be used instead)

    Further arguments are interpreted as for :py:class:`.Client` constructor.
    """

    def __init__(self,
                 server,
                 serializer=None,
                 deserializer=None,
                 connect_timeout=None,
                 timeout=None,
                 no_delay=False,
                 ignore_exc=False,
                 socket_module=socket,
                 key_prefix=b'',
                 max_pool_size=None,
                 lock_generator=None,
                 default_noreply=True,
                 allow_unicode_keys=False,
                 encoding='ascii'):
        self.server = server
        self.serializer = serializer
        self.deserializer = deserializer
        self.connect_timeout = connect_timeout
        self.timeout = timeout
        self.no_delay = no_delay
        self.ignore_exc = ignore_exc
        self.socket_module = socket_module
        self.default_noreply = default_noreply
        self.allow_unicode_keys = allow_unicode_keys
        if isinstance(key_prefix, six.text_type):
            key_prefix = key_prefix.encode('ascii')
        if not isinstance(key_prefix, bytes):
            raise TypeError("key_prefix should be bytes.")
        self.key_prefix = key_prefix
        self.client_pool = pool.ObjectPool(
            self._create_client,
            after_remove=lambda client: client.close(),
            max_size=max_pool_size,
            lock_generator=lock_generator)
        self.encoding = encoding

    def check_key(self, key):
        """Checks key and add key_prefix."""
        return _check_key(key, allow_unicode_keys=self.allow_unicode_keys,
                          key_prefix=self.key_prefix)

    def _create_client(self):
        client = Client(self.server,
                        serializer=self.serializer,
                        deserializer=self.deserializer,
                        connect_timeout=self.connect_timeout,
                        timeout=self.timeout,
                        no_delay=self.no_delay,
                        # We need to know when it fails *always* so that we
                        # can remove/destroy it from the pool...
                        ignore_exc=False,
                        socket_module=self.socket_module,
                        key_prefix=self.key_prefix,
                        default_noreply=self.default_noreply,
                        allow_unicode_keys=self.allow_unicode_keys)
        return client

    def close(self):
        self.client_pool.clear()

    def set(self, key, value, expire=0, noreply=None, flags=None):
        with self.client_pool.get_and_release(destroy_on_fail=True) as client:
            return client.set(key, value, expire=expire, noreply=noreply,
                              flags=flags)

    def set_many(self, values, expire=0, noreply=None, flags=None):
        with self.client_pool.get_and_release(destroy_on_fail=True) as client:
            failed = client.set_many(values, expire=expire, noreply=noreply,
                                     flags=flags)
            return failed

    set_multi = set_many

    def replace(self, key, value, expire=0, noreply=None, flags=None):
        with self.client_pool.get_and_release(destroy_on_fail=True) as client:
            return client.replace(key, value, expire=expire, noreply=noreply,
                                  flags=flags)

    def append(self, key, value, expire=0, noreply=None, flags=None):
        with self.client_pool.get_and_release(destroy_on_fail=True) as client:
            return client.append(key, value, expire=expire, noreply=noreply,
                                 flags=flags)

    def prepend(self, key, value, expire=0, noreply=None, flags=None):
        with self.client_pool.get_and_release(destroy_on_fail=True) as client:
            return client.prepend(key, value, expire=expire, noreply=noreply,
                                  flags=flags)

    def cas(self, key, value, cas, expire=0, noreply=False, flags=None):
        with self.client_pool.get_and_release(destroy_on_fail=True) as client:
            return client.cas(key, value, cas,
                              expire=expire, noreply=noreply, flags=flags)

    def get(self, key, default=None):
        with self.client_pool.get_and_release(destroy_on_fail=True) as client:
            try:
                return client.get(key, default)
            except Exception:
                if self.ignore_exc:
                    return None
                else:
                    raise

    def get_many(self, keys):
        with self.client_pool.get_and_release(destroy_on_fail=True) as client:
            try:
                return client.get_many(keys)
            except Exception:
                if self.ignore_exc:
                    return {}
                else:
                    raise

    get_multi = get_many

    def gets(self, key):
        with self.client_pool.get_and_release(destroy_on_fail=True) as client:
            try:
                return client.gets(key)
            except Exception:
                if self.ignore_exc:
                    return (None, None)
                else:
                    raise

    def gets_many(self, keys):
        with self.client_pool.get_and_release(destroy_on_fail=True) as client:
            try:
                return client.gets_many(keys)
            except Exception:
                if self.ignore_exc:
                    return {}
                else:
                    raise

    def delete(self, key, noreply=None):
        with self.client_pool.get_and_release(destroy_on_fail=True) as client:
            return client.delete(key, noreply=noreply)

    def delete_many(self, keys, noreply=None):
        with self.client_pool.get_and_release(destroy_on_fail=True) as client:
            return client.delete_many(keys, noreply=noreply)

    delete_multi = delete_many

    def add(self, key, value, expire=0, noreply=None, flags=None):
        with self.client_pool.get_and_release(destroy_on_fail=True) as client:
            return client.add(key, value, expire=expire, noreply=noreply,
                              flags=flags)

    def incr(self, key, value, noreply=False):
        with self.client_pool.get_and_release(destroy_on_fail=True) as client:
            return client.incr(key, value, noreply=noreply)

    def decr(self, key, value, noreply=False):
        with self.client_pool.get_and_release(destroy_on_fail=True) as client:
            return client.decr(key, value, noreply=noreply)

    def touch(self, key, expire=0, noreply=None):
        with self.client_pool.get_and_release(destroy_on_fail=True) as client:
            return client.touch(key, expire=expire, noreply=noreply)

    def stats(self, *args):
        with self.client_pool.get_and_release(destroy_on_fail=True) as client:
            try:
                return client.stats(*args)
            except Exception:
                if self.ignore_exc:
                    return {}
                else:
                    raise

    def version(self):
        with self.client_pool.get_and_release(destroy_on_fail=True) as client:
            return client.version()

    def flush_all(self, delay=0, noreply=None):
        with self.client_pool.get_and_release(destroy_on_fail=True) as client:
            return client.flush_all(delay=delay, noreply=noreply)

    def quit(self):
        with self.client_pool.get_and_release(destroy_on_fail=True) as client:
            try:
                client.quit()
            finally:
                self.client_pool.destroy(client)

    def __setitem__(self, key, value):
        self.set(key, value, noreply=True)

    def __getitem__(self, key):
        value = self.get(key)
        if value is None:
            raise KeyError
        return value

    def __delitem__(self, key):
        self.delete(key, noreply=True)


def _readline(sock, buf):
    """Read line of text from the socket.

    Read a line of text (delimited by "\r\n") from the socket, and
    return that line along with any trailing characters read from the
    socket.

    Args:
        sock: Socket object, should be connected.
        buf: String, zero or more characters, returned from an earlier
            call to _readline or _readvalue (pass an empty string on the
            first call).

    Returns:
      A tuple of (buf, line) where line is the full line read from the
      socket (minus the "\r\n" characters) and buf is any trailing
      characters read after the "\r\n" was found (which may be an empty
      string).

    """
    chunks = []
    last_char = b''

    while True:
        # We're reading in chunks, so "\r\n" could appear in one chunk,
        # or across the boundary of two chunks, so we check for both
        # cases.

        # This case must appear first, since the buffer could have
        # later \r\n characters in it and we want to get the first \r\n.
        if last_char == b'\r' and buf[0:1] == b'\n':
            # Strip the last character from the last chunk.
            chunks[-1] = chunks[-1][:-1]
            return buf[1:], b''.join(chunks)
        elif buf.find(b'\r\n') != -1:
            before, sep, after = buf.partition(b"\r\n")
            chunks.append(before)
            return after, b''.join(chunks)

        if buf:
            chunks.append(buf)
            last_char = buf[-1:]

        buf = _recv(sock, RECV_SIZE)
        if not buf:
            raise MemcacheUnexpectedCloseError()


def _readvalue(sock, buf, size):
    """Read specified amount of bytes from the socket.

    Read size bytes, followed by the "\r\n" characters, from the socket,
    and return those bytes and any trailing bytes read after the "\r\n".

    Args:
        sock: Socket object, should be connected.
        buf: String, zero or more characters, returned from an earlier
            call to _readline or _readvalue (pass an empty string on the
            first call).
        size: Integer, number of bytes to read from the socket.

    Returns:
      A tuple of (buf, value) where value is the bytes read from the
      socket (there will be exactly size bytes) and buf is trailing
      characters read after the "\r\n" following the bytes (but not
      including the \r\n).

    """
    chunks = []
    rlen = size + 2
    while rlen - len(buf) > 0:
        if buf:
            rlen -= len(buf)
            chunks.append(buf)
        buf = _recv(sock, RECV_SIZE)
        if not buf:
            raise MemcacheUnexpectedCloseError()

    # Now we need to remove the \r\n from the end. There are two cases we care
    # about: the \r\n is all in the last buffer, or only the \n is in the last
    # buffer, and we need to remove the \r from the penultimate buffer.

    if rlen == 1:
        # replace the last chunk with the same string minus the last character,
        # which is always '\r' in this case.
        chunks[-1] = chunks[-1][:-1]
    else:
        # Just remove the "\r\n" from the latest chunk
        chunks.append(buf[:rlen - 2])

    return buf[rlen:], b''.join(chunks)


def _recv(sock, size):
    """sock.recv() with retry on EINTR"""
    while True:
        try:
            return sock.recv(size)
        except IOError as e:
            if e.errno != errno.EINTR:
                raise
