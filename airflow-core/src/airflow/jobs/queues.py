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
from collections import OrderedDict, defaultdict, deque
from collections.abc import Iterable, Iterator
from threading import Lock
from typing import Generic, TypeVar

K = TypeVar("K")
V = TypeVar("V", bound=tuple)


class KeyedHeadQueue(Generic[K, V]):
    """
    A keyed queue that manages values per key in insertion order.

    Features:
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

    def __init__(self) -> None:
        self.__map: OrderedDict[K, deque[V]] = OrderedDict()  # key -> deque of values
        self.__popped_keys: set[K] = set()  # keys whose first value has been consumed
        self._lock = Lock()

    @property
    def _map(self) -> OrderedDict[K, list[V]]:
        with self._lock:
            return OrderedDict((key, list(value)) for key, value in self.__map.items())

    @property
    def _popped_keys(self) -> set[K]:
        with self._lock:
            return set(self.__popped_keys)

    def get(self, key: K, default_value: list[V] | None = None) -> list[V] | None:
        return list(self._map.get(key, default_value or []))

    def extend(self, elements: Iterable[V]) -> None:
        for element in elements:
            self.append(element)

    def append(self, element: V) -> None:
        """Append a (key, value) pair unless key already consumed."""
        key = element[0]
        with self._lock:
            if key not in self.__map:
                self.__map[key] = deque()
            self.__map[key].append(element)

    def popleft(self) -> V:
        """
        Pop the *first inserted value* for the next key in order.

        Raises IndexError if all first values have been popped.
        """
        with self._lock:
            for key, values in self.__map.items():
                if key not in self.__popped_keys:
                    value = values.popleft()
                    self.__popped_keys.add(key)
                    if not values:
                        del self.__map[key]
                    return value
        raise IndexError("pop from empty KeyedHeadQueue")

    def popall(self) -> tuple[K, list[V]]:
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

    def __contains__(self, key: K) -> bool:
        return key in self._map

    def __iter__(self) -> Iterator[tuple[K, V]]:
        """Iterate over leftover (key, value) pairs in a snapshot, so concurrent appends during iteration are not visible."""
        for key, values in self._map.items():
            for value in values:
                yield key, value

    def __len__(self) -> int:
        """Count remaining values available."""
        with self._lock:
            return sum(len(value) for value in self.__map.values())

    def __bool__(self) -> bool:
        """Count of keys that still have their first value available."""
        with self._lock:
            if not sum(1 for key in self.__map if key not in self.__popped_keys) > 0:
                self.__popped_keys.clear()
                return False
            return True

    def keys(self) -> list[K]:
        """Keys still waiting for their first value to be popped."""
        with self._lock:
            return [key for key in self.__map.keys() if key not in self.__popped_keys]


class PartitionedQueue(Generic[K, V], defaultdict[K, Queue[tuple[K, V]]]):
    """
    Dict-like container where each key maps to an asyncio.Queue.

    Tracks sizes safely for concurrent access.
    Provides put(item) and popleft().
    Uses a total counter to make __bool__ O(1).
    Supports both async and threading locks.
    """

    def __init__(self, maxsize: int = 0) -> None:
        super().__init__(lambda: Queue(maxsize=maxsize))
        self.maxsize = maxsize
        self._async_locks: dict[K, AsyncLock] = defaultdict(AsyncLock)
        self._locks: dict[K, Lock] = defaultdict(Lock)
        self._sizes: dict[K, int] = defaultdict(int)  # track sizes per key
        self._total_size: int = 0  # total items across all queues

    def __bool__(self) -> bool:
        return self._total_size > 0

    async def put(self, item: V) -> None:
        key = item[0]
        queue = self[key]
        async with self._async_locks[key]:
            await queue.put(item)
            with self._locks[key]:
                self._sizes[key] += 1
                self._total_size += 1

    def popleft(self) -> tuple[K, V]:
        """Pop an item from the first non-empty queue synchronously (non-blocking) using thread lock."""
        for key, queue in list(self.items()):
            with self._locks[key]:
                if self._sizes[key] > 0:
                    item = queue.get_nowait()  # won't raise if size > 0
                    self._sizes[key] -= 1
                    self._total_size -= 1
                    return item
        raise StopIteration
