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

import collections
import itertools
from collections.abc import AsyncIterator, Iterator, Sequence
from typing import TYPE_CHECKING, Any, Literal, TypeVar, overload

import attrs
import structlog

from airflow.sdk.configuration import conf

if TYPE_CHECKING:
    from airflow.sdk.definitions.xcom_arg import PlainXComArg
    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance

T = TypeVar("T")

# This is used to wrap values from the API so the structure is compatible with
# ``XCom.deserialize_value``. We don't want to wrap the API values in a nested
# {"value": value} dict since it wastes bandwidth.
_XComWrapper = collections.namedtuple("_XComWrapper", "value")

log = structlog.get_logger(logger_name=__name__)

#: Default for ``[core] xcom_sequence_chunk_size``: items a ``LazyXComSequence`` fetches per request
#: when read by index or iterated. One slice request serves this many consecutive reads, and at
#: most this many values are held at a time.
XCOM_SEQUENCE_CHUNK_SIZE = 32


def _configured_chunk_size() -> int:
    return conf.getint("core", "xcom_sequence_chunk_size", fallback=XCOM_SEQUENCE_CHUNK_SIZE)


@attrs.define
class LazyXComIterator(Iterator[T]):
    seq: LazyXComSequence[T]
    index: int = 0
    dir: Literal[1, -1] = 1

    def __next__(self) -> T:
        if self.index < 0:
            # When iterating backwards, avoid extra HTTP request
            raise StopIteration()
        try:
            val = self.seq[self.index]
        except IndexError:
            raise StopIteration from None
        self.index += self.dir
        return val

    def __iter__(self) -> Iterator[T]:
        return self


@attrs.define
class AsyncLazyXComIterator(AsyncIterator[T]):
    """
    Async twin of :class:`LazyXComIterator`: the same item reads, sent through ``asend``.

    An iterated task consumes a mapped task's results as its input on the event loop; reading
    them synchronously there would block the loop thread on the supervisor channel while the
    sub-tasks' own ``asend`` calls are in flight (see ``AsyncAwareExecutor.imap_unordered``).
    """

    seq: LazyXComSequence[T]
    index: int = 0

    def __aiter__(self) -> AsyncIterator[T]:
        return self

    async def __anext__(self) -> T:
        try:
            val = await self.seq.aget(self.index)
        except IndexError:
            raise StopAsyncIteration from None
        self.index += 1
        return val


@attrs.define
class LazyXComSequence(Sequence[T]):
    """
    A mapped upstream's return values, read from the API server on demand.

    Reads come in chunks: an index outside the slice held fetches ``chunk_size`` items starting at
    that index with one ``GetXComSequenceSlice`` request, so iterating, or an iterated task reading
    this by index, costs one request per chunk instead of one per item, while no more than one
    chunk is held in memory. ``chunk_size`` defaults to ``[core] xcom_sequence_chunk_size``. A
    negative index counts from the end through the cached length.
    """

    _len: int | None = attrs.field(init=False, default=None)
    _xcom_arg: PlainXComArg = attrs.field(alias="xcom_arg")
    _ti: RuntimeTaskInstance = attrs.field(alias="ti")
    chunk_size: int = attrs.field(kw_only=True, factory=_configured_chunk_size)
    _chunk_start: int = attrs.field(init=False, default=0)
    _chunk: list[T] = attrs.field(init=False, factory=list)

    def __repr__(self) -> str:
        if self._len is not None:
            counter = "item" if (length := len(self)) == 1 else "items"
            return f"LazyXComSequence([{length} {counter}])"
        return "LazyXComSequence(<unevaluated length>)"

    def __str__(self) -> str:
        return repr(self)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Sequence):
            return NotImplemented
        z = itertools.zip_longest(iter(self), iter(other), fillvalue=object())
        return all(x == y for x, y in z)

    def __hash__(self):
        return hash((*[item for item in iter(self)],))

    def __iter__(self) -> Iterator[T]:
        return LazyXComIterator(seq=self)

    def __aiter__(self) -> AsyncIterator[T]:
        return AsyncLazyXComIterator(seq=self)

    async def aget(self, index: int) -> T:
        """Async counterpart of ``self[index]``: the same chunked reads, sent with ``asend``."""
        from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

        if index < 0:
            index = _normalize_index(index, await self.alen())
        if not self._holds(index):
            msg = await SUPERVISOR_COMMS.asend(self._slice_request(index, index + self.chunk_size))
            self._hold(index, self._slice_values(msg))
        return self._chunk[index - self._chunk_start]

    def _holds(self, index: int) -> bool:
        return self._chunk_start <= index < self._chunk_start + len(self._chunk)

    def _hold(self, index: int, values: list[T]) -> None:
        """Keep the slice fetched for ``index``; an empty slice means ``index`` is past the end."""
        if not values:
            raise IndexError(index)
        self._chunk_start, self._chunk = index, values

    def _slice_request(self, start: int | None, stop: int | None, step: int | None = None) -> Any:
        from airflow.sdk.execution_time.comms import GetXComSequenceSlice

        source = (xcom_arg := self._xcom_arg).operator
        return GetXComSequenceSlice(
            key=xcom_arg.key,
            dag_id=source.dag_id,
            task_id=source.task_id,
            run_id=self._ti.run_id,
            start=start,
            stop=stop,
            step=step,
        )

    @staticmethod
    def _slice_values(msg: Any) -> list[T]:
        from airflow.sdk.execution_time.comms import XComSequenceSliceResult
        from airflow.sdk.execution_time.xcom import XCom

        if not isinstance(msg, XComSequenceSliceResult):
            raise TypeError(f"Got unexpected response to GetXComSequenceSlice: {msg!r}")
        return [XCom.deserialize_value(_XComWrapper(value)) for value in msg.root]

    def __len__(self) -> int:
        if self._len is None:
            from airflow.sdk.execution_time.comms import ErrorResponse, GetXComCount, XComCountResponse
            from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

            task = self._xcom_arg.operator

            msg = SUPERVISOR_COMMS.send(
                GetXComCount(
                    key=self._xcom_arg.key,
                    dag_id=task.dag_id,
                    run_id=self._ti.run_id,
                    task_id=task.task_id,
                ),
            )
            if isinstance(msg, ErrorResponse):
                raise RuntimeError(msg)
            if not isinstance(msg, XComCountResponse):
                raise TypeError(f"Got unexpected response to GetXComCount: {msg!r}")
            self._len = msg.len
        return self._len

    async def alen(self) -> int:
        """Async twin of ``len(self)``: the same ``GetXComCount`` request, sent with ``asend``."""
        if self._len is None:
            from airflow.sdk.execution_time.comms import ErrorResponse, GetXComCount, XComCountResponse
            from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

            task = self._xcom_arg.operator

            msg = await SUPERVISOR_COMMS.asend(
                GetXComCount(
                    key=self._xcom_arg.key,
                    dag_id=task.dag_id,
                    run_id=self._ti.run_id,
                    task_id=task.task_id,
                ),
            )
            if isinstance(msg, ErrorResponse):
                raise RuntimeError(msg)
            if not isinstance(msg, XComCountResponse):
                raise TypeError(f"Got unexpected response to GetXComCount: {msg!r}")
            self._len = msg.len
        return self._len

    @overload
    def __getitem__(self, key: int) -> T: ...

    @overload
    def __getitem__(self, key: slice) -> Sequence[T]: ...

    def __getitem__(self, key: int | slice) -> T | Sequence[T]:
        from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

        if isinstance(key, slice):
            return self._slice_values(SUPERVISOR_COMMS.send(self._slice_request(*_coerce_slice(key))))

        if not isinstance(key, int):
            if (index := getattr(key, "__index__", None)) is not None:
                key = index()
            raise TypeError(f"Sequence indices must be integers or slices not {type(key).__name__}")

        index = _normalize_index(key, len(self)) if key < 0 else key
        if not self._holds(index):
            msg = SUPERVISOR_COMMS.send(self._slice_request(index, index + self.chunk_size))
            self._hold(index, self._slice_values(msg))
        return self._chunk[index - self._chunk_start]


def _normalize_index(index: int, length: int) -> int:
    """Map a negative index onto its position from the start, or raise ``IndexError`` past the front."""
    if index < 0:
        index += length
    if index < 0:
        raise IndexError(index)
    return index


def _coerce_slice_index(value: Any) -> int | None:
    """
    Check slice attribute's type and convert it to int.

    See CPython documentation on this:
    https://docs.python.org/3/reference/datamodel.html#object.__index__
    """
    if value is None or isinstance(value, int):
        return value
    if (index := getattr(value, "__index__", None)) is not None:
        return index()
    raise TypeError("slice indices must be integers or None or have an __index__ method")


def _coerce_slice(key: slice) -> tuple[int | None, int | None, int | None]:
    """
    Check slice content and convert it for SQL.

    See CPython documentation on this:
    https://docs.python.org/3/reference/datamodel.html#slice-objects
    """
    if (step := _coerce_slice_index(key.step)) == 0:
        raise ValueError("slice step cannot be zero")
    return _coerce_slice_index(key.start), _coerce_slice_index(key.stop), step
