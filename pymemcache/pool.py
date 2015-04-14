# Copyright 2015 Yahoo.com
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

import six

import contextlib
import threading


class ObjectPool(object):
    def __init__(self, obj_creator, before_remove=None, max_size=None):
        self._used_objs = []
        self._free_objs = []
        self._obj_creator = obj_creator
        self._lock = threading.Lock()
        self._before_remove = before_remove
        max_size = max_size or 2 ** 31
        if not isinstance(max_size, six.integer_types) or max_size < 0:
            raise ValueError('"max_size" must be a positive integer')
        self.max_size = max_size

    @property
    def used(self):
        return self._used_objs[:]

    @property
    def free(self):
        return self._free_objs[:]

    @contextlib.contextmanager
    def get_and_release(self):
        obj = self.get()
        try:
            yield obj
        finally:
            try:
                self.release(obj)
            except (ValueError, IndexError):
                pass

    def get(self):
        with self._lock:
            if not self._free_objs:
                curr_count = len(self._used_objs) + len(self._free_objs)
                if curr_count >= self.max_size:
                    raise RuntimeError("Too many objects")
                self._used_objs.append(self._obj_creator())
                return self._used_objs[-1]
            else:
                self._used_objs.append(self._free_objs.pop())
                return self._used_objs[-1]

    def destroy(self, obj):
        with self._lock:
            idx = self._used_objs.index(obj)
            if self._before_remove is not None:
                self._before_remove(obj)
            self._used_objs.pop(idx)

    def release(self, obj):
        with self._lock:
            self._used_objs.remove(obj)
            self._free_objs.append(obj)

    def clear(self):
        with self._lock:
            if self._before_remove is not None:
                while self._used_objs:
                    self._before_remove(self._used_objs.pop())
                while self._free_objs:
                    self._before_remove(self._free_objs.pop())
            else:
                self._free_objs = []
                self._used_objs = []
