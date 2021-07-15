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
