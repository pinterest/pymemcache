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

import collections
import contextlib
import sys
import threading
import time


class ObjectPool:
    """A pool of objects that release/creates/destroys as needed."""

    def __init__(
        self,
        obj_creator,
        after_remove=None,
        max_size=None,
        idle_timeout=0,
        lock_generator=None,
    ):
        self._used_objs = collections.deque()
        self._free_objs = collections.deque()
        self._obj_creator = obj_creator
        if lock_generator is None:
            self._lock = threading.Lock()
        else:
            self._lock = lock_generator()
        self._after_remove = after_remove
        max_size = max_size or 2**31
        if not isinstance(max_size, int) or max_size < 0:
            raise ValueError('"max_size" must be a positive integer')
        self.max_size = max_size
        self.idle_timeout = idle_timeout
        self._idle_clock = time.time if idle_timeout else int

    @property
    def used(self):
        return tuple(self._used_objs)

    @property
    def free(self):
        return tuple(self._free_objs)

    @contextlib.contextmanager
    def get_and_release(self, destroy_on_fail=False):
        obj = self.get()
        try:
            yield obj
        except Exception:
            exc_info = sys.exc_info()
            if not destroy_on_fail:
                self.release(obj)
            else:
                self.destroy(obj)
            raise exc_info[1].with_traceback(exc_info[2])
        self.release(obj)

    def get(self):
        with self._lock:
            # Find a free object, removing any that have idled for too long.
            now = self._idle_clock()
            while self._free_objs:
                obj = self._free_objs.popleft()
                if now - obj._last_used <= self.idle_timeout:
                    break

                if self._after_remove is not None:
                    self._after_remove(obj)
            else:
                # No free objects, create a new one.
                curr_count = len(self._used_objs)
                if curr_count >= self.max_size:
                    raise RuntimeError(
                        "Too many objects," " %s >= %s" % (curr_count, self.max_size)
                    )
                obj = self._obj_creator()

            self._used_objs.append(obj)
            obj._last_used = now
            return obj

    def destroy(self, obj, silent=True):
        was_dropped = False
        with self._lock:
            try:
                self._used_objs.remove(obj)
                was_dropped = True
            except ValueError:
                if not silent:
                    raise
        if was_dropped and self._after_remove is not None:
            self._after_remove(obj)

    def release(self, obj, silent=True):
        with self._lock:
            try:
                self._used_objs.remove(obj)
                self._free_objs.append(obj)
                obj._last_used = self._idle_clock()
            except ValueError:
                if not silent:
                    raise

    def clear(self):
        if self._after_remove is not None:
            needs_destroy = []
            with self._lock:
                needs_destroy.extend(self._used_objs)
                needs_destroy.extend(self._free_objs)
                self._free_objs.clear()
                self._used_objs.clear()
            for obj in needs_destroy:
                self._after_remove(obj)
        else:
            with self._lock:
                self._free_objs.clear()
                self._used_objs.clear()
