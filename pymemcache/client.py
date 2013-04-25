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

"""
A comprehensive, fast, pure-Python memcached client library.

Basic Usage:
------------

 from pymemcache.client import Client

 client = Client(('localhost', 11211))
 client.set('some_key', 'some_value')
 result = client.get('some_key')


Serialization:
--------------

 import json
 from pymemcache.client import Client

 def json_serializer(key, value):
     if type(value) == str:
         return value, 1
     return json.dumps(value), 2

 def json_deserializer(key, value, flags):
     if flags == 1:
         return value
     if flags == 2:
         return json.loads(value)
     raise Exception("Unknown serialization format")

 client = Client(('localhost', 11211), serializer=json_serializer,
                 deserializer=json_deserializer)
 client.set('key', {'a':'b', 'c':'d'})
 result = client.get('key')


Best Practices:
---------------

 - Always set the connect_timeout and timeout arguments in the constructor to
   avoid blocking your process when memcached is slow.
 - Use the "noreply" flag for a significant performance boost. The "noreply"
   flag is enabled by default for "set", "add", "replace", "append", "prepend",
   and "delete". It is disabled by default for "cas", "incr" and "decr". It
   obviously doesn't apply to any get calls.
 - Use get_many and gets_many whenever possible, as they result in less
   round trip times for fetching multiple keys.
 - Use the "ignore_exc" flag to treat memcache/network errors as cache misses
   on calls to the get* methods. This prevents failures in memcache, or network
   errors, from killing your web requests. Do not use this flag if you need to
   know about errors from memcache, and make sure you have some other way to
   detect memcache server failures.
"""

__author__ = "Charles Gordon"


import socket


RECV_SIZE = 4096
VALID_STORE_RESULTS = {
    'set':     ('STORED',),
    'add':     ('STORED', 'NOT_STORED'),
    'replace': ('STORED', 'NOT_STORED'),
    'append':  ('STORED', 'NOT_STORED'),
    'prepend': ('STORED', 'NOT_STORED'),
    'cas':     ('STORED', 'EXISTS', 'NOT_FOUND'),
}


class MemcacheError(Exception):
    "Base exception class"
    pass


class MemcacheClientError(MemcacheError):
    """Raised when memcached fails to parse the arguments to a request, likely
    due to a malformed key and/or value, a bug in this library, or a version
    mismatch with memcached."""
    pass


class MemcacheUnknownCommandError(MemcacheClientError):
    """Raised when memcached fails to parse a request, likely due to a bug in
    this library or a version mismatch with memcached."""
    pass


class MemcacheIllegalInputError(MemcacheClientError):
    """Raised when a key or value is not legal for Memcache (see the class docs
    for Client for more details)."""
    pass


class MemcacheServerError(MemcacheError):
    """Raised when memcached reports a failure while processing a request,
    likely due to a bug or transient issue in memcached."""
    pass


class MemcacheUnknownError(MemcacheError):
    """Raised when this library receives a response from memcached that it
    cannot parse, likely due to a bug in this library or a version mismatch
    with memcached."""
    pass


class MemcacheUnexpectedCloseError(MemcacheServerError):
    "Raised when the connection with memcached closes unexpectedly."
    pass


class Client(object):
    """
    A client for a single memcached server.

    Keys and Values:
    ----------------

     Keys must have a __str__() method which should return a str with no more
     than 250 ASCII characters and no whitespace or control characters. Unicode
     strings must be encoded (as UTF-8, for example) unless they consist only
     of ASCII characters that are neither whitespace nor control characters.

     Values must have a __str__() method to convert themselves to a byte string.
     Unicode objects can be a problem since str() on a Unicode object will
     attempt to encode it as ASCII (which will fail if the value contains
     code points larger than U+127). You can fix this will a serializer or by
     just calling encode on the string (using UTF-8, for instance).

     If you intend to use anything but str as a value, it is a good idea to use
     a serializer and deserializer. The pymemcache.serde library has some
     already implemented serializers, including one that is compatible with
     the python-memcache library.

    Serialization and Deserialization:
    ----------------------------------

     The constructor takes two optional functions, one for "serialization" of
     values, and one for "deserialization". The serialization function takes
     two arguments, a key and a value, and returns a tuple of two elements, the
     serialized value, and an integer in the range 0-65535 (the "flags"). The
     deserialization function takes three parameters, a key, value and flags
     and returns the deserialized value.

     Here is an example using JSON for non-str values:

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

    Error Handling:
    ---------------

     All of the methods in this class that talk to memcached can throw one of
     the following exceptions:

      * MemcacheUnknownCommandError
      * MemcacheClientError
      * MemcacheServerError
      * MemcacheUnknownError
      * MemcacheUnexpectedCloseError
      * MemcacheIllegalInputError
      * socket.timeout
      * socket.error

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
                 ignore_exc=False):
        """
        Constructor.

        Args:
          server: tuple(hostname, port)
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
        self.sock = None
        self.buf = ''

    def _connect(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(self.connect_timeout)
        sock.connect(self.server)
        sock.settimeout(self.timeout)
        if self.no_delay:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.sock = sock

    def close(self):
        """Close the connetion to memcached, if it is open. The next call to a
        method that requires a connection will re-open it."""
        if self.sock is not None:
            try:
                self.sock.close()
            except Exception:
                pass
        self.sock = None
        self.buf = ''

    def set(self, key, value, expire=0, noreply=True):
        """
        The memcached "set" command.

        Args:
          key: str, see class docs for details.
          value: str, see class docs for details.
          expire: optional int, number of seconds until the item is expired
                  from the cache, or zero for no expiry (the default).
          noreply: optional bool, True to not wait for the reply (the default).

        Returns:
          If no exception is raised, always returns True. If an exception is
          raised, the set may or may not have occurred. If noreply is True,
          then a successful return does not guarantee a successful set.
        """
        return self._store_cmd('set', key, expire, noreply, value)

    def set_many(self, values, expire=0, noreply=True):
        """
        A convenience function for setting multiple values.

        Args:
          values: dict(str, str), a dict of keys and values, see class docs
                  for details.
          expire: optional int, number of seconds until the item is expired
                  from the cache, or zero for no expiry (the default).
          noreply: optional bool, True to not wait for the reply (the default).

        Returns:
          If no exception is raised, always returns True. Otherwise all, some
          or none of the keys have been successfully set. If noreply is True
          then a successful return does not guarantee that any keys were
          successfully set (just that the keys were successfully sent).
        """
        if not values:
            return True

        # TODO: make this more performant by sending all the values first, then
        # waiting for all the responses.
        for key, value in values.items():
            self.set(key, value, expire, noreply)

    def add(self, key, value, expire=0, noreply=True):
        """
        The memcached "add" command.

        Args:
          key: str, see class docs for details.
          value: str, see class docs for details.
          expire: optional int, number of seconds until the item is expired
                  from the cache, or zero for no expiry (the default).
          noreply: optional bool, True to not wait for the reply (the default).

        Returns:
          If noreply is True, the return value is always True. Otherwise the
          return value is True if the value was stgored, and False if it was
          not (because the key already existed).
        """
        return self._store_cmd('add', key, expire, noreply, value)

    def replace(self, key, value, expire=0, noreply=True):
        """
        The memcached "replace" command.

        Args:
          key: str, see class docs for details.
          value: str, see class docs for details.
          expire: optional int, number of seconds until the item is expired
                  from the cache, or zero for no expiry (the default).
          noreply: optional bool, True to not wait for the reply (the default).

        Returns:
          If noreply is True, always returns True. Otherwise returns True if
          the value was stored and False if it wasn't (because the key didn't
          already exist).
        """
        return self._store_cmd('replace', key, expire, noreply, value)

    def append(self, key, value, expire=0, noreply=True):
        """
        The memcached "append" command.

        Args:
          key: str, see class docs for details.
          value: str, see class docs for details.
          expire: optional int, number of seconds until the item is expired
                  from the cache, or zero for no expiry (the default).
          noreply: optional bool, True to not wait for the reply (the default).

        Returns:
          True.
        """
        return self._store_cmd('append', key, expire, noreply, value)

    def prepend(self, key, value, expire=0, noreply=True):
        """
        The memcached "prepend" command.

        Args:
          key: str, see class docs for details.
          value: str, see class docs for details.
          expire: optional int, number of seconds until the item is expired
                  from the cache, or zero for no expiry (the default).
          noreply: optional bool, False to wait for the reply (the default).

        Returns:
          True.
        """
        return self._store_cmd('prepend', key, expire, noreply, value)

    def cas(self, key, value, cas, expire=0, noreply=False):
        """
        The memcached "cas" command.

        Args:
          key: str, see class docs for details.
          value: str, see class docs for details.
          cas: int or str that only contains the characters '0'-'9'.
          expire: optional int, number of seconds until the item is expired
                  from the cache, or zero for no expiry (the default).
          noreply: optional bool, False to wait for the reply (the default).

        Returns:
          If noreply is True, always returns True. Otherwise returns None if
          the key didn't exist, False if it existed but had a different cas
          value and True if it existed and was changed.
        """
        return self._store_cmd('cas', key, expire, noreply, value, cas)

    def get(self, key):
        """
        The memcached "get" command, but only for one key, as a convenience.

        Args:
          key: str, see class docs for details.

        Returns:
          The value for the key, or None if the key wasn't found.
        """
        return self._fetch_cmd('get', [key], False).get(key, None)

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

        return self._fetch_cmd('get', keys, False)

    def gets(self, key):
        """
        The memcached "gets" command for one key, as a convenience.

        Args:
          key: str, see class docs for details.

        Returns:
          A tuple of (key, cas), or (None, None) if the key was not found.
        """
        return self._fetch_cmd('gets', [key], True).get(key, (None, None))

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

        return self._fetch_cmd('gets', keys, True)

    def delete(self, key, noreply=True):
        """
        The memcached "delete" command.

        Args:
          key: str, see class docs for details.

        Returns:
          If noreply is True, always returns True. Otherwise returns True if
          the key was deleted, and False if it wasn't found.
        """
        cmd = 'delete {0}{1}\r\n'.format(key, ' noreply' if noreply else '')
        result = self._misc_cmd(cmd, 'delete', noreply)
        if noreply:
            return True
        return result == 'DELETED'

    def delete_many(self, keys, noreply=True):
        """
        A convenience function to delete multiple keys.

        Args:
          keys: list(str), the list of keys to delete.

        Returns:
          True. If an exception is raised then all, some or none of the keys
          may have been deleted. Otherwise all the keys have been sent to
          memcache for deletion and if noreply is False, they have been
          acknowledged by memcache.
        """
        if not keys:
            return True

        # TODO: make this more performant by sending all keys first, then
        # waiting for all values.
        for key in keys:
            self.delete(key, noreply)

        return True

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
        cmd = "incr {0} {1}{2}\r\n".format(
            key,
            str(value),
            ' noreply' if noreply else '')
        result = self._misc_cmd(cmd, 'incr', noreply)
        if noreply:
            return None
        if result == 'NOT_FOUND':
            return None
        return int(result)

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
        cmd = "decr {0} {1}{2}\r\n".format(
            key,
            str(value),
            ' noreply' if noreply else '')
        result = self._misc_cmd(cmd, 'decr', noreply)
        if noreply:
            return None
        if result == 'NOT_FOUND':
            return None
        return int(result)

    def touch(self, key, expire=0, noreply=True):
        """
        The memcached "touch" command.

        Args:
          key: str, see class docs for details.
          expire: optional int, number of seconds until the item is expired
                  from the cache, or zero for no expiry (the default).
          noreply: optional bool, False to wait for the reply (the default).

        Returns:
          True if the expiration time was updated, False if the key wasn't
          found.
        """
        cmd = "touch {0} {1}{2}\r\n".format(
            key,
            expire,
            ' noreply' if noreply else '')
        result = self._misc_cmd(cmd, 'touch', noreply)
        if noreply:
            return True
        return result == 'TOUCHED'

    def stats(self):
        # TODO(charles)
        pass

    def flush_all(self, delay=0, noreply=True):
        """
        The memcached "flush_all" command.

        Args:
          delay: optional int, the number of seconds to wait before flushing,
                 or zero to flush immediately (the default).
          noreply: optional bool, False to wait for the response (the default).

        Returns:
          True.
        """
        cmd = "flush_all {0}{1}\r\n".format(delay, ' noreply' if noreply else '')
        result = self._misc_cmd(cmd, 'flush_all', noreply)
        if noreply:
            return True
        return result == 'OK'

    def quit(self):
        """
        The memcached "quit" command.

        This will close the connection with memcached. Calling any other
        method on this object will re-open the connection, so this object can
        be re-used after quit.
        """
        cmd = "quit\r\n"
        self._misc_cmd(cmd, 'quit', True)
        self.close()

    def _raise_errors(self, line, name):
        if line.startswith('ERROR'):
            raise MemcacheUnknownCommandError(name)

        if line.startswith('CLIENT_ERROR'):
            error = line[line.find(' ') + 1:]
            raise MemcacheClientError(error)

        if line.startswith('SERVER_ERROR'):
            error = line[line.find(' ') + 1:]
            raise MemcacheServerError(error)

    def _fetch_cmd(self, name, keys, expect_cas):
        if not self.sock:
            self._connect()

        try:
            key_strs = []
            for key in keys:
                key = str(key)
                if ' ' in key:
                    raise MemcacheIllegalInputError("Key contains spaces: %s", key)
                key_strs.append(key)
        except UnicodeEncodeError as e:
            raise MemcacheIllegalInputError(str(e))

        cmd = '{0} {1}\r\n'.format(name, ' '.join(key_strs))

        try:
            self.sock.sendall(cmd)

            result = {}
            while True:
                self.buf, line = _readline(self.sock, self.buf)
                self._raise_errors(line, name)

                if line == 'END':
                    return result
                elif line.startswith('VALUE'):
                    if expect_cas:
                        _, key, flags, size, cas = line.split()
                    else:
                        _, key, flags, size = line.split()

                    self.buf, value = _readvalue(self.sock,
                                                 self.buf,
                                                 int(size))

                    if self.deserializer:
                        value = self.deserializer(key, value, int(flags))

                    if expect_cas:
                        result[key] = (value, cas)
                    else:
                        result[key] = value
                else:
                    raise MemcacheUnknownError(line[:32])
        except Exception:
            self.close()
            if self.ignore_exc:
                return {}
            raise

    def _store_cmd(self, name, key, expire, noreply, data, cas=None):
        try:
            key = str(key)
            if ' ' in key:
                raise MemcacheIllegalInputError("Key contains spaces: %s", key)
        except UnicodeEncodeError as e:
            raise MemcacheIllegalInputError(str(e))

        if not self.sock:
            self._connect()

        if self.serializer:
            data, flags = self.serializer(key, data)
        else:
            flags = 0

        try:
            data = str(data)
        except UnicodeEncodeError as e:
            raise MemcacheIllegalInputError(str(e))

        if cas is not None and noreply:
            extra = ' {0} noreply'.format(cas)
        elif cas is not None and not noreply:
            extra = ' {0}'.format(cas)
        elif cas is None and noreply:
            extra = ' noreply'
        else:
            extra = ''

        cmd = '{0} {1} {2} {3} {4}{5}\r\n{6}\r\n'.format(
            name, key, flags, expire, len(data), extra, data)

        try:
            self.sock.sendall(cmd)

            if noreply:
                return True

            self.buf, line = _readline(self.sock, self.buf)
            self._raise_errors(line, name)

            if line in VALID_STORE_RESULTS[name]:
                if line == 'STORED':
                    return True
                if line == 'NOT_STORED':
                    return False
                if line == 'NOT_FOUND':
                    return None
                if line == 'EXISTS':
                    return False
            else:
                raise MemcacheUnknownError(line[:32])
        except Exception:
            self.close()
            raise

    def _misc_cmd(self, cmd, cmd_name, noreply):
        if not self.sock:
            self._connect()

        try:
            self.sock.sendall(cmd)

            if noreply:
                return

            _, line = _readline(self.sock, '')
            self._raise_errors(line, cmd_name)

            return line
        except Exception:
            self.close()
            raise


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
    last_char = ''

    while True:
        # We're reading in chunks, so "\r\n" could appear in one chunk,
        # or across the boundary of two chunks, so we check for both
        # cases.

        # This case must appear first, since the buffer could have
        # later \r\n characters in it and we want to get the first \r\n.
        if last_char == '\r' and buf[0] == '\n':
            # Strip the last character from the last chunk.
            chunks[-1] = chunks[-1][:-1]
            return buf[1:], ''.join(chunks)
        elif buf.find('\r\n') != -1:
            before, sep, after = buf.partition("\r\n")
            chunks.append(before)
            return after, ''.join(chunks)

        if buf:
            chunks.append(buf)
            last_char = buf[-1]

        buf = sock.recv(RECV_SIZE)
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
        buf = sock.recv(RECV_SIZE)
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

    return buf[rlen:], ''.join(chunks)
