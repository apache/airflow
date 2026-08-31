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
from collections.abc import Iterable, Iterator, Sequence
from typing import Any, Protocol

import structlog

from airflow.sdk.execution_time.comms import (
    DeleteXCom,
    GetXCom,
    GetXComSequenceSlice,
    SetXCom,
    XComResult,
    XComSequenceSliceResult,
)

# Lightweight wrapper for XCom values
_XComValueWrapper = collections.namedtuple("_XComValueWrapper", "value")

log = structlog.get_logger(logger_name="task")


class TIKeyProtocol(Protocol):
    dag_id: str
    task_id: str
    run_id: str
    map_index: int


class BaseXCom:
    """BaseXcom is an interface now to interact with XCom backends."""

    XCOM_RETURN_KEY = "return_value"

    @classmethod
    def set(
        cls,
        key: str,
        value: Any,
        *,
        dag_id: str,
        task_id: str,
        run_id: str,
        map_index: int = -1,
        dag_result: bool = False,
        _mapped_length: int | None = None,
    ) -> None:
        """
        Store an XCom value.

        :param key: Key to store the XCom.
        :param value: XCom value to store.
        :param dag_id: Dag ID.
        :param task_id: Task ID.
        :param run_id: Dag run ID for the task.
        :param map_index: Optional map index to assign XCom for a mapped task.
            The default is ``-1`` (set for a non-mapped task).
        """
        from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

        value = cls.serialize_value(
            value=value,
            key=key,
            task_id=task_id,
            dag_id=dag_id,
            run_id=run_id,
            map_index=map_index,
        )

        SUPERVISOR_COMMS.send(
            SetXCom(
                key=key,
                value=value,
                dag_id=dag_id,
                task_id=task_id,
                run_id=run_id,
                map_index=map_index,
                dag_result=dag_result,
                mapped_length=_mapped_length,
            ),
        )

    @classmethod
    async def aset(
        cls,
        key: str,
        value: Any,
        *,
        dag_id: str,
        task_id: str,
        run_id: str,
        map_index: int = -1,
        dag_result: bool = False,
        _mapped_length: int | None = None,
    ) -> None:
        """
        Store an XCom value asynchronously.

        :param key: Key to store the XCom.
        :param value: XCom value to store.
        :param dag_id: Dag ID.
        :param task_id: Task ID.
        :param run_id: Dag run ID for the task.
        :param map_index: Optional map index to assign XCom for a mapped task.
            The default is ``-1`` (set for a non-mapped task).
        """
        from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

        value = cls.serialize_value(
            value=value,
            key=key,
            task_id=task_id,
            dag_id=dag_id,
            run_id=run_id,
            map_index=map_index,
        )

        await SUPERVISOR_COMMS.asend(
            SetXCom(
                key=key,
                value=value,
                dag_id=dag_id,
                task_id=task_id,
                run_id=run_id,
                map_index=map_index,
                dag_result=dag_result,
                mapped_length=_mapped_length,
            ),
        )

    @classmethod
    def _set_xcom_in_db(
        cls,
        key: str,
        value: Any,
        *,
        dag_id: str,
        task_id: str,
        run_id: str,
        map_index: int = -1,
    ) -> None:
        """
        Store an XCom value directly in the metadata database.

        :param key: Key to store the XCom.
        :param value: XCom value to store.
        :param dag_id: Dag ID.
        :param task_id: Task ID.
        :param run_id: Dag run ID for the task.
        :param map_index: Optional map index to assign XCom for a mapped task.
            The default is ``-1`` (set for a non-mapped task).
        """
        from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

        SUPERVISOR_COMMS.send(
            SetXCom(
                key=key,
                value=value,
                dag_id=dag_id,
                task_id=task_id,
                run_id=run_id,
                map_index=map_index,
            ),
        )

    @classmethod
    def get_value(
        cls,
        *,
        ti_key: TIKeyProtocol,
        key: str,
    ) -> Any:
        """
        Retrieve an XCom value for a task instance.

        This method returns "full" XCom values (i.e. uses ``deserialize_value``
        from the XCom backend).

        If there are no results, *None* is returned. If multiple XCom entries
        match the criteria, an arbitrary one is returned.

        :param ti_key: The TaskInstanceKey to look up the XCom for.
        :param key: A key for the XCom. If provided, only XCom with matching
            keys will be returned. Pass *None* (default) to remove the filter.
        """
        return cls.get_one(
            key=key,
            task_id=ti_key.task_id,
            dag_id=ti_key.dag_id,
            run_id=ti_key.run_id,
            map_index=ti_key.map_index,
        )

    @classmethod
    async def aget_value(
        cls,
        *,
        ti_key: TIKeyProtocol,
        key: str,
    ) -> Any:
        """
        Retrieve an XCom value for a task instance asynchronously.

        This method returns "full" XCom values (i.e. uses ``deserialize_value``
        from the XCom backend).

        If there are no results, *None* is returned. If multiple XCom entries
        match the criteria, an arbitrary one is returned.

        :param ti_key: The TaskInstanceKey to look up the XCom for.
        :param key: A key for the XCom. If provided, only XCom with matching
            keys will be returned. Pass *None* (default) to remove the filter.
        """
        return await cls.aget_one(
            key=key,
            task_id=ti_key.task_id,
            dag_id=ti_key.dag_id,
            run_id=ti_key.run_id,
            map_index=ti_key.map_index,
        )

    @classmethod
    def _get_xcom_db_ref(
        cls,
        *,
        key: str,
        dag_id: str,
        task_id: str,
        run_id: str,
        map_index: int | None = None,
    ) -> XComResult:
        """
        Retrieve an XCom value, optionally meeting certain criteria.

        This method returns "full" XCom values (i.e. uses ``deserialize_value``
        from the XCom backend).

        If there are no results, *None* is returned. If multiple XCom entries
        match the criteria, an arbitrary one is returned.

        .. seealso:: ``get_value()`` is a convenience function if you already
            have a structured TaskInstance or TaskInstanceKey object available.

        :param run_id: Dag run ID for the task.
        :param dag_id: Only pull XCom from this Dag. Pass *None* (default) to
            remove the filter.
        :param task_id: Only XCom from task with matching ID will be pulled.
            Pass *None* (default) to remove the filter.
        :param map_index: Only XCom from task with matching ID will be pulled.
            Pass *None* (default) to remove the filter.
        :param key: A key for the XCom. If provided, only XCom with matching
            keys will be returned. Pass *None* (default) to remove the filter.
        """
        from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

        msg = SUPERVISOR_COMMS.send(
            GetXCom(
                key=key,
                dag_id=dag_id,
                task_id=task_id,
                run_id=run_id,
                map_index=map_index,
            ),
        )

        if not isinstance(msg, XComResult):
            raise TypeError(f"Expected XComResult, received: {type(msg)} {msg}")

        return msg

    @classmethod
    def get_one(
        cls,
        *,
        key: str,
        dag_id: str,
        task_id: str,
        run_id: str,
        map_index: int | None = None,
        include_prior_dates: bool = False,
    ) -> Any | None:
        """
        Retrieve an XCom value, optionally meeting certain criteria.

        This method returns "full" XCom values (i.e. uses ``deserialize_value``
        from the XCom backend).

        If there are no results, *None* is returned. If multiple XCom entries
        match the criteria, an arbitrary one is returned.

        .. seealso:: ``get_value()`` is a convenience function if you already
            have a structured TaskInstance or TaskInstanceKey object available.

        :param run_id: Dag run ID for the task.
        :param dag_id: Only pull XCom from this Dag. Pass *None* (default) to
            remove the filter.
        :param task_id: Only XCom from task with matching ID will be pulled.
            Pass *None* (default) to remove the filter.
        :param map_index: Only XCom from task with matching ID will be pulled.
            Pass *None* (default) to remove the filter.
        :param key: A key for the XCom. If provided, only XCom with matching
            keys will be returned. Pass *None* (default) to remove the filter.
        :param include_prior_dates: If *False* (default), only XCom from the
            specified Dag run is returned. If *True*, the latest matching XCom is
            returned regardless of the run it belongs to.
        """
        from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

        msg = SUPERVISOR_COMMS.send(
            GetXCom(
                key=key,
                dag_id=dag_id,
                task_id=task_id,
                run_id=run_id,
                map_index=map_index,
                include_prior_dates=include_prior_dates,
            ),
        )

        if not isinstance(msg, XComResult):
            raise TypeError(f"Expected XComResult, received: {type(msg)} {msg}")

        if msg.value is not None:
            return cls.deserialize_value(msg)
        log.debug(
            "No XCom value found; defaulting to None.",
            key=key,
            dag_id=dag_id,
            task_id=task_id,
            run_id=run_id,
            map_index=map_index,
        )
        return None

    @classmethod
    async def aget_one(
        cls,
        *,
        key: str,
        dag_id: str,
        task_id: str,
        run_id: str,
        map_index: int | None = None,
        include_prior_dates: bool = False,
    ) -> Any | None:
        """
        Retrieve an XCom value asynchronously, optionally meeting certain criteria.

        This method returns "full" XCom values (i.e. uses ``deserialize_value``
        from the XCom backend).

        If there are no results, *None* is returned. If multiple XCom entries
        match the criteria, an arbitrary one is returned.

        .. seealso:: ``aget_value()`` is a convenience function if you already
            have a structured TaskInstance or TaskInstanceKey object available.

        :param run_id: Dag run ID for the task.
        :param dag_id: Only pull XCom from this Dag. Pass *None* (default) to
            remove the filter.
        :param task_id: Only XCom from task with matching ID will be pulled.
            Pass *None* (default) to remove the filter.
        :param map_index: Only XCom from task with matching ID will be pulled.
            Pass *None* (default) to remove the filter.
        :param key: A key for the XCom. If provided, only XCom with matching
            keys will be returned. Pass *None* (default) to remove the filter.
        :param include_prior_dates: If *False* (default), only XCom from the
            specified Dag run is returned. If *True*, the latest matching XCom is
            returned regardless of the run it belongs to.
        """
        from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

        msg = await SUPERVISOR_COMMS.asend(
            GetXCom(
                key=key,
                dag_id=dag_id,
                task_id=task_id,
                run_id=run_id,
                map_index=map_index,
                include_prior_dates=include_prior_dates,
            ),
        )

        if not isinstance(msg, XComResult):
            raise TypeError(f"Expected XComResult, received: {type(msg)} {msg}")

        if msg.value is not None:
            return cls.deserialize_value(msg)
        log.debug(
            "No XCom value found; defaulting to None.",
            key=key,
            dag_id=dag_id,
            task_id=task_id,
            run_id=run_id,
            map_index=map_index,
        )
        return None

    @classmethod
    def get_all(
        cls,
        *,
        key: str,
        dag_id: str,
        task_id: str,
        run_id: str,
        include_prior_dates: bool = False,
    ) -> Any:
        """
        Retrieve all XCom values for a task, typically from all map indexes.

        XComSequenceSliceResult can never have *None* in it, it returns an empty list
        if no values were found.

        This is particularly useful for getting all XCom values from all map
        indexes of a mapped task at once.

        :param key: A key for the XCom. Only XComs with this key will be returned.
        :param run_id: Dag run ID for the task.
        :param dag_id: Dag ID to pull XComs from.
        :param task_id: Task ID to pull XComs from.
        :param include_prior_dates: If *False* (default), only XComs from the
            specified Dag run are returned. If *True*, the latest matching XComs are
            returned regardless of the run they belong to.
        :return: List of all XCom values if found.
        """
        from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

        msg = SUPERVISOR_COMMS.send(
            msg=GetXComSequenceSlice(
                key=key,
                dag_id=dag_id,
                task_id=task_id,
                run_id=run_id,
                start=None,
                stop=None,
                step=None,
                include_prior_dates=include_prior_dates,
            ),
        )

        if not isinstance(msg, XComSequenceSliceResult):
            raise TypeError(f"Expected XComSequenceSliceResult, received: {type(msg)} {msg}")

        if not msg.root:
            return None

        return [cls.deserialize_value(_XComValueWrapper(value)) for value in msg.root]

    @classmethod
    async def aget_all(
        cls,
        *,
        key: str,
        dag_id: str,
        task_id: str,
        run_id: str,
        include_prior_dates: bool = False,
    ) -> Any:
        """
        Retrieve all XCom values for a task asynchronously, typically from all map indexes.

        XComSequenceSliceResult can never have *None* in it, it returns an empty list
        if no values were found.

        This is particularly useful for getting all XCom values from all map
        indexes of a mapped task at once.

        :param key: A key for the XCom. Only XComs with this key will be returned.
        :param run_id: Dag run ID for the task.
        :param dag_id: Dag ID to pull XComs from.
        :param task_id: Task ID to pull XComs from.
        :param include_prior_dates: If *False* (default), only XComs from the
            specified Dag run are returned. If *True*, the latest matching XComs are
            returned regardless of the run they belong to.
        :return: List of all XCom values if found.
        """
        from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

        msg = await SUPERVISOR_COMMS.asend(
            msg=GetXComSequenceSlice(
                key=key,
                dag_id=dag_id,
                task_id=task_id,
                run_id=run_id,
                start=None,
                stop=None,
                step=None,
                include_prior_dates=include_prior_dates,
            ),
        )

        if not isinstance(msg, XComSequenceSliceResult):
            raise TypeError(f"Expected XComSequenceSliceResult, received: {type(msg)} {msg}")

        if not msg.root:
            return None

        return [cls.deserialize_value(_XComValueWrapper(value)) for value in msg.root]

    @staticmethod
    def serialize_value(
        value: Any,
        *,
        key: str | None = None,
        task_id: str | None = None,
        dag_id: str | None = None,
        run_id: str | None = None,
        map_index: int | None = None,
    ) -> str:
        """Serialize XCom value to JSON str."""
        from airflow.sdk.serde import serialize

        # return back the value for BaseXCom, custom backends will implement this
        return serialize(value)  # type: ignore[return-value]

    @staticmethod
    def deserialize_value(result) -> Any:
        """Deserialize XCom value from str objects."""
        from airflow.sdk.serde import deserialize

        return deserialize(result.value)

    @classmethod
    def purge(cls, xcom: XComResult, *args) -> None:
        """Purge an XCom entry from underlying storage implementations."""
        pass

    @classmethod
    def delete(
        cls,
        key: str,
        task_id: str,
        dag_id: str,
        run_id: str,
        map_index: int | None = None,
    ) -> None:
        """Delete an Xcom entry, for custom xcom backends, it gets the path associated with the data on the backend and purges it."""
        from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

        xcom_result = cls._get_xcom_db_ref(
            key=key,
            dag_id=dag_id,
            task_id=task_id,
            run_id=run_id,
            map_index=map_index,
        )
        cls.purge(xcom_result)
        SUPERVISOR_COMMS.send(
            DeleteXCom(
                key=key,
                dag_id=dag_id,
                task_id=task_id,
                run_id=run_id,
                map_index=map_index,
            ),
        )


class XComIterable(Sequence):
    """An iterable that lazily fetches XCom values one by one instead of loading all at once."""

    def __init__(
        self,
        task_id: str,
        dag_id: str,
        run_id: str,
        map_index: int | None = None,
        length: int | None = None,
    ):
        self.task_id = task_id
        self.dag_id = dag_id
        self.run_id = run_id
        self.map_index = map_index
        self.length = length or 0
        self.index = self.length

    def __iter__(self) -> Iterator[Any]:
        return _XComIterator(self)

    def __len__(self) -> int:
        return self.length

    def __getitem__(self, key: int | slice) -> Any | Sequence[Any]:
        """Allow direct indexing so this works like a sequence."""
        from airflow.sdk.execution_time.xcom import XCom

        if isinstance(key, slice):
            # TODO: This issues one XCom.get_one call per element — N round-trips for a full slice.
            # XComIterable stores results under distinct keys (return_value_0, return_value_1, …)
            # with the same map_index, so the existing GetXComSequenceSlice endpoint (which ranges
            # over map_index for a single key) cannot be reused.  A new POST endpoint that accepts
            # a list of keys and returns values in a single query is needed; once that lands, replace
            # this loop with a single batched fetch.
            start, stop, step = key.indices(len(self))
            return [self[i] for i in range(start, stop, step)]

        if not (0 <= key < self.length):
            raise IndexError(key)

        return XCom.get_one(
            key=f"{BaseXCom.XCOM_RETURN_KEY}_{key}",
            dag_id=self.dag_id,
            task_id=self.task_id,
            run_id=self.run_id,
            map_index=self.map_index,
        )

    def append(self, value: Any):
        from airflow.sdk.execution_time.xcom import XCom

        try:
            XCom.set(
                key=f"{BaseXCom.XCOM_RETURN_KEY}_{self.index}",
                value=value,
                dag_id=self.dag_id,
                task_id=self.task_id,
                run_id=self.run_id,
                map_index=self.map_index,
            )
        finally:
            self.index += 1
            self.length += 1

    async def aappend(self, value: Any):
        from airflow.sdk.execution_time.xcom import XCom

        try:
            await XCom.aset(
                key=f"{BaseXCom.XCOM_RETURN_KEY}_{self.index}",
                value=value,
                dag_id=self.dag_id,
                task_id=self.task_id,
                run_id=self.run_id,
                map_index=self.map_index,
            )
        finally:
            self.index += 1
            self.length += 1

    def flatten(self) -> XComIterable:
        """Return a FlattenedXComIterable that recursively expands nested iterables (except str/bytes)."""
        return FlattenedXComIterable(
            task_id=self.task_id,
            dag_id=self.dag_id,
            run_id=self.run_id,
            map_index=self.map_index,
            length=self.length,
        )

    def serialize(self) -> dict:
        """Ensure the object is JSON serializable."""
        return {
            "task_id": self.task_id,
            "dag_id": self.dag_id,
            "run_id": self.run_id,
            "map_index": self.map_index,
            "length": self.length,
        }

    @classmethod
    def deserialize(cls, data: dict, version: int):
        """Ensure the object is JSON deserializable."""
        return XComIterable(**data)


class FlattenedXComIterable(XComIterable):
    """An XComIterable whose iterator recursively expands nested iterables (except str/bytes)."""

    def __iter__(self) -> Iterator[Any]:
        for item in super().__iter__():
            yield from self._flatten(item)

    @classmethod
    def _flatten(cls, item: Any) -> Iterator[Any]:
        if isinstance(item, Iterable) and not isinstance(item, (str, bytes)):
            for sub in item:
                yield from cls._flatten(sub)
        else:
            yield item

    @classmethod
    def deserialize(cls, data: dict, version: int):
        return FlattenedXComIterable(**data)


class _XComIterator:
    """Iterator for XComIterable."""

    def __init__(self, iterable: XComIterable):
        self._iterable = iterable
        self._index = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self._index >= len(self._iterable):
            raise StopIteration

        value = self._iterable[self._index]
        self._index += 1
        return value
