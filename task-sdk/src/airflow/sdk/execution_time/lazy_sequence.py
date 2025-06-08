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
from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Any, Literal, TypeVar, overload

import attrs
import structlog

if TYPE_CHECKING:
    from airflow.sdk.definitions.xcom_arg import PlainXComArg
    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance

T = TypeVar("T")

# This is used to wrap values from the API so the structure is compatible with
# ``XCom.deserialize_value``. We don't want to wrap the API values in a nested
# {"value": value} dict since it wastes bandwidth.
_XComWrapper = collections.namedtuple("_XComWrapper", "value")

log = structlog.get_logger(logger_name=__name__)


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
class LazyXComSequence(Sequence[T]):
    _len: int | None = attrs.field(init=False, default=None)
    _xcom_arg: PlainXComArg = attrs.field(alias="xcom_arg")
    _ti: RuntimeTaskInstance = attrs.field(alias="ti")

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

    def __iter__(self) -> Iterator[T]:
        return LazyXComIterator(seq=self)

    def __len__(self) -> int:
        if self._len is None:
            from airflow.sdk.execution_time.comms import ErrorResponse, GetXComCount, XComCountResponse
            from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

            task = self._xcom_arg.operator

            SUPERVISOR_COMMS.send_request(
                log=log,
                msg=GetXComCount(
                    key=self._xcom_arg.key,
                    dag_id=task.dag_id,
                    run_id=self._ti.run_id,
                    task_id=task.task_id,
                ),
            )
            msg = SUPERVISOR_COMMS.get_message()
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
        from airflow.sdk.execution_time.comms import (
            ErrorResponse,
            GetXComSequenceItem,
            GetXComSequenceSlice,
            XComSequenceIndexResult,
            XComSequenceSliceResult,
        )
        from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS
        from airflow.sdk.execution_time.xcom import XCom

        if isinstance(key, slice):
            start, stop, step = _coerce_slice(key)
            with SUPERVISOR_COMMS.lock:
                source = (xcom_arg := self._xcom_arg).operator
                SUPERVISOR_COMMS.send_request(
                    log=log,
                    msg=GetXComSequenceSlice(
                        key=xcom_arg.key,
                        dag_id=source.dag_id,
                        task_id=source.task_id,
                        run_id=self._ti.run_id,
                        start=start,
                        stop=stop,
                        step=step,
                    ),
                )
                msg = SUPERVISOR_COMMS.get_message()
                if not isinstance(msg, XComSequenceSliceResult):
                    raise TypeError(f"Got unexpected response to GetXComSequenceSlice: {msg!r}")
            return [XCom.deserialize_value(_XComWrapper(value)) for value in msg.root]

        if not isinstance(key, int):
            if (index := getattr(key, "__index__", None)) is not None:
                key = index()
            raise TypeError(f"Sequence indices must be integers or slices not {type(key).__name__}")

        with SUPERVISOR_COMMS.lock:
            source = (xcom_arg := self._xcom_arg).operator
            SUPERVISOR_COMMS.send_request(
                log=log,
                msg=GetXComSequenceItem(
                    key=xcom_arg.key,
                    dag_id=source.dag_id,
                    task_id=source.task_id,
                    run_id=self._ti.run_id,
                    offset=key,
                ),
            )
            msg = SUPERVISOR_COMMS.get_message()
        if isinstance(msg, ErrorResponse):
            raise IndexError(key)
        if not isinstance(msg, XComSequenceIndexResult):
            raise TypeError(f"Got unexpected response to GetXComSequenceItem: {msg!r}")
        return XCom.deserialize_value(_XComWrapper(msg.root))


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
