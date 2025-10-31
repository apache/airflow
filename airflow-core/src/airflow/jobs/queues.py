#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

from asyncio import Lock as AsyncLock, Queue
from collections import OrderedDict, deque, defaultdict
from collections.abc import Iterable
from threading import Lock
from typing import Any


class KeyedHeadQueue:
    """
    A keyed queue where:
      - `popleft()` returns only the *first value* per key (in insertion order of keys).
      - Once a key's first value is popped, that key will never yield in `popleft()` again.
      - Remaining values for consumed keys are preserved.
      - Iteration yields those leftover (key, value) pairs.

    Example:
        q = FirstValueQueue()
        q.append(("task1", "event1"))
        q.append(("task1", "event2"))
        q.append(("task2", "eventA"))

        q.popleft()  # ('task1', 'event1')
        q.popleft()  # ('task2', 'eventA')

        list(q)  # [('task1', 'event2')]
    """

    def __init__(self):
        self.__map = OrderedDict()  # key -> deque of values
        self.__popped_keys = set()  # keys whose first value has been consumed
        self._lock = Lock()

    @property
    def _map(self) -> OrderedDict:
        with self._lock:
            return OrderedDict((key, list(value)) for key, value in self.__map.items())

    @property
    def _popped_keys(self) -> set:
        with self._lock:
            return set(self.__popped_keys)

    def get(self, key, default_value: list | None = None) -> list | None:
        return list(self._map.get(key, default_value or []))

    def extend(self, elements: Iterable[tuple]):
        for element in elements:
            self.append(element)

    def append(self, element: tuple):
        """Append a (key, value) pair unless key already consumed."""
        key = element[0]
        with self._lock:
            if key not in self.__map:
                self.__map[key] = deque()
            self.__map[key].append(element)

    def popleft(self):
        """
        Pop the *first inserted value* for the next key in order.
        Raises IndexError if all first values have been popped.
        """
        # find first key not yet popped
        with self._lock:
            for key, values in self.__map.items():
                if key not in self.__popped_keys:
                    value = values.popleft()
                    self.__popped_keys.add(key)
                    # if deque still has leftovers, keep it for iteration
                    if not values:
                        del self.__map[key]
                    return value

        raise IndexError("pop from empty KeyedHeadQueue")

    def popall(self):
        """
        Pop all values for the first unconsumed key (in insertion order).
        Marks the key as consumed.
        Raises IndexError if no keys remain.
        """
        with self._lock:
            for key in self.__map.keys():
                if key not in self.__popped_keys:
                    values = list(self.__map.pop(key, []))
                    self.__popped_keys.add(key)
                    return key, values

        raise IndexError("pop from empty KeyedHeadQueue")

    def __contains__(self, key: str) -> bool:
        return key in self._map

    def __iter__(self):
        """
        Iterate over leftover (key, value) pairs in a snapshot,
        so concurrent appends during iteration are not visible.
        """
        for key, values in self._map.items():
            for value in values:
                yield key, value

    def __len__(self):
        """
        Count remaining values available.
        """
        with self._lock:
            return sum(len(value) for value in self.__map.values())

    def __bool__(self):
        """
        Count of keys that still have their first value available.
        """
        with self._lock:
            if not sum(1 for key in self.__map if key not in self.__popped_keys) > 0:
                self.__popped_keys.clear()
                return False
            return True


    def keys(self):
        """Keys still waiting for their first value to be popped."""
        with self._lock:
            return [key for key in self.__map.keys() if key not in self.__popped_keys]


class PartitionedQueue(defaultdict):
    """
    Dict-like container where each key maps to an asyncio.Queue.
    Tracks sizes safely for concurrent access.
    Provides put(item) and popleft().
    Uses a total counter to make __bool__ O(1).
    Supports both async and threading locks.
    """

    def __init__(self, maxsize: int = 0):
        super().__init__(lambda: Queue(maxsize=maxsize))
        self.maxsize = maxsize
        self._async_locks: dict[str, AsyncLock] = defaultdict(AsyncLock)
        self._locks: dict[str, Lock] = defaultdict(Lock)
        self._sizes: dict[str, int] = defaultdict(int)  # track sizes per key
        self._total_size: int = 0  # total items across all queues

    def __bool__(self):
        return self._total_size > 0

    async def put(self, item: tuple[Any, Any]) -> None:
        key = item[0]
        queue = self[key]
        async with self._async_locks[key]:
            await queue.put(item)
            with self._locks[key]:
                self._sizes[key] += 1
                self._total_size += 1

    def popleft(self) -> tuple[Any, Any]:
        """Pop an item from the first non-empty queue synchronously (non-blocking) using thread lock."""
        for key, queue in list(self.items()):
            with self._locks[key]:
                if self._sizes[key] > 0:
                    item = queue.get_nowait()  # won't raise if size > 0
                    self._sizes[key] -= 1
                    self._total_size -= 1
                    return item
        raise StopIteration


# def test_serialized_dag():
#     import ast
#     import os
#
#     dir_path = os.path.dirname(__file__)  # directory of the current test file
#     file_path = os.path.join(dir_path, "serialized_dag.json")
#     with open(file_path) as file:
#         from airflow.serialization.serialized_objects import SerializedDAG
#         data = SerializedDAG.from_dict(ast.literal_eval(file.read())).__dict__
#         #data = deserialize_objects(ast.literal_eval(file.read()))
#         print(data)
#         assert data  # or whatever you want to test
