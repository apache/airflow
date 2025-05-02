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

import itertools
from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Any, Literal, TypeVar, overload

import attrs
import structlog

if TYPE_CHECKING:
    from airflow.sdk.definitions.xcom_arg import PlainXComArg
    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance

T = TypeVar("T")

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
                raise TypeError(f"Got unexpected response to GetXComCount: {msg}")
            self._len = msg.len
        return self._len

    @overload
    def __getitem__(self, key: int) -> T: ...

    @overload
    def __getitem__(self, key: slice) -> Sequence[T]: ...

    def __getitem__(self, key: int | slice) -> T | Sequence[T]:
        if not isinstance(key, (int, slice)):
            raise TypeError(f"Sequence indices must be integers or slices, not {type(key).__name__}")

        if isinstance(key, slice):
            raise TypeError("slice is not implemented yet")
        # TODO...
        # This implements the slicing syntax. We want to optimize negative slicing (e.g. seq[-10:]) by not
        # doing an additional COUNT query (via HEAD http request) if possible. We can do this unless the
        # start and stop have different signs (i.e. one is positive and another negative).
        # start, stop, reverse = _coerce_slice(key)
        # if start >= 0:
        #     if stop is None:
        #         stmt = self._select_asc.offset(start)
        #     elif stop >= 0:
        #         stmt = self._select_asc.slice(start, stop)
        #     else:
        #         stmt = self._select_asc.slice(start, len(self) + stop)
        #     rows = [self._process_row(row) for row in self._session.execute(stmt)]
        #     if reverse:
        #         rows.reverse()
        # else:
        #     if stop is None:
        #         stmt = self._select_desc.limit(-start)
        #     elif stop < 0:
        #         stmt = self._select_desc.slice(-stop, -start)
        #     else:
        #         stmt = self._select_desc.slice(len(self) - stop, -start)
        #     rows = [self._process_row(row) for row in self._session.execute(stmt)]
        #     if not reverse:
        #         rows.reverse()
        # return rows

        from airflow.sdk.bases.xcom import BaseXCom
        from airflow.sdk.execution_time.comms import GetXComSequenceItem, XComResult
        from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

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

        if not isinstance(msg, XComResult):
            raise IndexError(key)
        return BaseXCom.deserialize_value(msg)


def _coerce_index(value: Any) -> int | None:
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


def _coerce_slice(key: slice) -> tuple[int, int | None, bool]:
    """
    Check slice content and convert it for SQL.

    See CPython documentation on this:
    https://docs.python.org/3/reference/datamodel.html#slice-objects
    """
    if key.step is None or key.step == 1:
        reverse = False
    elif key.step == -1:
        reverse = True
    else:
        raise ValueError("non-trivial slice step not supported")
    return _coerce_index(key.start) or 0, _coerce_index(key.stop), reverse
