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
from collections.abc import AsyncIterator, Iterator, Sequence
from typing import Any, Protocol

import structlog

from airflow.sdk.execution_time.comms import (
    DeleteXCom,
    GetXCom,
    GetXComByKeys,
    GetXComSequenceSlice,
    SetXCom,
    XComResult,
    XComSequenceSliceResult,
)

# Lightweight wrapper for XCom values
_XComValueWrapper = collections.namedtuple("_XComValueWrapper", "value")

# Wraps a raw API value the way ``XCom.deserialize_value`` expects it; see ``LazyXComSequence``.
_XComWrapper = collections.namedtuple("_XComWrapper", "value")

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
        :param key: A key for the XCom. Only XCom with this key will be returned.
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
        :param key: A key for the XCom. Only XCom with this key will be returned.
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
        :param dag_id: Dag ID to pull the XCom from.
        :param task_id: Task ID to pull the XCom from.
        :param map_index: Map index of the task instance to pull the XCom from.
            *None* (default) pulls the XCom of a non-mapped task, which has
            map index ``-1``.
        :param key: A key for the XCom. Only XCom with this key will be returned.
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
    def get_by_keys(
        cls,
        *,
        keys: list[str],
        dag_id: str,
        task_id: str,
        run_id: str,
        map_index: int | None = None,
    ) -> list[Any]:
        """
        Retrieve several XCom values of one task instance by key, in one round trip.

        The values come back in the order of ``keys``, ``None`` for a key that has no XCom. This is
        what :class:`XComIterable` iterates and slices with, since its values live under distinct
        keys (``return_value_<index>``) of the same task instance.

        :param keys: The XCom keys to read.
        :param dag_id: Dag ID to pull the XComs from.
        :param task_id: Task ID to pull the XComs from.
        :param run_id: Dag run ID for the task.
        :param map_index: Map index of the task instance. *None* (default) reads the XComs of a
            non-mapped task, which has map index ``-1``.
        """
        from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

        msg = SUPERVISOR_COMMS.send(
            GetXComByKeys(
                keys=keys,
                dag_id=dag_id,
                task_id=task_id,
                run_id=run_id,
                map_index=-1 if map_index is None else map_index,
            ),
        )
        if not isinstance(msg, XComSequenceSliceResult):
            raise TypeError(f"Expected XComSequenceSliceResult, received: {type(msg)} {msg}")
        return [cls.deserialize_value(_XComWrapper(value)) for value in msg.root]

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
        :param dag_id: Dag ID to pull the XCom from.
        :param task_id: Task ID to pull the XCom from.
        :param map_index: Map index of the task instance to pull the XCom from.
            *None* (default) pulls the XCom of a non-mapped task, which has
            map index ``-1``.
        :param key: A key for the XCom. Only XCom with this key will be returned.
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
        :param dag_id: Dag ID to pull the XCom from.
        :param task_id: Task ID to pull the XCom from.
        :param map_index: Map index of the task instance to pull the XCom from.
            *None* (default) pulls the XCom of a non-mapped task, which has
            map index ``-1``.
        :param key: A key for the XCom. Only XCom with this key will be returned.
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
        SUPERVISOR_COMMS.send(
            DeleteXCom(
                key=key,
                dag_id=dag_id,
                task_id=task_id,
                run_id=run_id,
                map_index=map_index,
            ),
        )
        cls.purge(xcom_result)


def _normalize_index(index: int, length: int) -> int:
    """Map a sequence index, negative ones included, onto a position in ``[0, length)``."""
    if index < 0:
        index += length
    if not (0 <= index < length):
        raise IndexError(index)
    return index


class XComIterable(Sequence):
    """
    An iterable that lazily fetches XCom values one by one instead of loading all at once.

    This is a read-only :class:`collections.abc.Sequence` over the ``return_value_<index>`` XComs an
    iterated task pushed, one per index: the values are written by the producing task's runner as
    each sub-task finishes (see ``IterableOperator.axcom_push``), and the iterable only ever reads
    them. Nothing on this class mutates the underlying XComs.

    Indexing follows the usual sequence rules, negative indices included: ``result[-1]`` is the last
    value. Iterations that were skipped pushed nothing and are left out, as the XComs of skipped
    mapped task instances are: ``length`` counts every input item, ``skipped`` lists the indices
    that were skipped, and positions in the sequence run over the others only. An iteration that
    returned ``None`` pushed nothing either but keeps its position: reading it gives ``None``,
    where ``.expand()`` would not count such a mapped task instance at all.

    A single index is one XCom read; iterating or slicing fetches all the values wanted with one
    ``GetXComByKeys`` request, since the values live under distinct keys that the slice endpoint for
    a mapped task's XComs cannot address.
    """

    def __init__(
        self,
        task_id: str,
        dag_id: str,
        run_id: str,
        map_index: int | None = None,
        length: int | None = None,
        skipped: Sequence[int] = (),
    ):
        self.task_id = task_id
        self.dag_id = dag_id
        self.run_id = run_id
        self.map_index = map_index
        self.length = length or 0
        self.skipped: list[int] = sorted(skipped)

    def _index_of(self, position: int) -> int:
        """Map ``position`` in the sequence to its input index, stepping over the skipped indices."""
        index = _normalize_index(position, len(self))
        for skipped_index in self.skipped:
            if skipped_index > index:
                break
            index += 1
        return index

    def __iter__(self) -> Iterator[Any]:
        """Fetch every value with one request."""
        return iter(self._get_by_keys(range(len(self))))

    def __len__(self) -> int:
        return self.length - len(self.skipped)

    async def alen(self) -> int:
        """Async twin of ``len(self)``, for readers on the event loop that take a length before each read."""
        return len(self)

    def __getitem__(self, key: int | slice) -> Any | Sequence[Any]:
        """Allow direct indexing so this works like a sequence."""
        from airflow.sdk.execution_time.xcom import XCom

        if isinstance(key, slice):
            return self._get_by_keys(range(*key.indices(len(self))))

        return XCom.get_one(
            key=f"{BaseXCom.XCOM_RETURN_KEY}_{self._index_of(key)}",
            dag_id=self.dag_id,
            task_id=self.task_id,
            run_id=self.run_id,
            map_index=self.map_index,
        )

    def _get_by_keys(self, positions: range) -> list[Any]:
        """Read the values at ``positions`` with one ``XCom.get_by_keys`` call; nothing for an empty range."""
        from airflow.sdk.execution_time.xcom import XCom

        if not positions:
            return []
        return XCom.get_by_keys(
            keys=[f"{BaseXCom.XCOM_RETURN_KEY}_{self._index_of(position)}" for position in positions],
            dag_id=self.dag_id,
            task_id=self.task_id,
            run_id=self.run_id,
            map_index=self.map_index,
        )

    async def aget(self, index: int) -> Any:
        """
        Async counterpart of ``self[index]``: fetch one value through ``XCom.aget_one``.

        Use it, or ``async for``, from code running on an event loop that has other SDK calls in
        flight (an iterated task consuming this iterable as its input): a synchronous read there
        would block the loop thread on the supervisor channel and deadlock with them.
        """
        from airflow.sdk.execution_time.xcom import XCom

        return await XCom.aget_one(
            key=f"{BaseXCom.XCOM_RETURN_KEY}_{self._index_of(index)}",
            dag_id=self.dag_id,
            task_id=self.task_id,
            run_id=self.run_id,
            map_index=self.map_index,
        )

    def __aiter__(self) -> AsyncIterator[Any]:
        return _AsyncXComIterator(self)

    def serialize(self) -> dict:
        """Ensure the object is JSON serializable."""
        return {
            "task_id": self.task_id,
            "dag_id": self.dag_id,
            "run_id": self.run_id,
            "map_index": self.map_index,
            "length": self.length,
            "skipped": self.skipped,
        }

    @classmethod
    def deserialize(cls, data: dict, version: int):
        """Ensure the object is JSON deserializable."""
        return cls(**data)


class _AsyncXComIterator:
    """Async iterator for XComIterable, one ``aget`` per position in order."""

    def __init__(self, iterable: XComIterable):
        self._iterable = iterable
        self._index = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._index >= await self._iterable.alen():
            raise StopAsyncIteration

        value = await self._iterable.aget(self._index)
        self._index += 1
        return value
