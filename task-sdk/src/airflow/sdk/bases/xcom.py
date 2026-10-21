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

import asyncio
import collections
import contextvars
import hashlib
from collections.abc import AsyncIterator, Iterator, Sequence
from typing import TYPE_CHECKING, Any, Protocol
from urllib.parse import quote
from uuid import UUID, uuid4

import attrs
import structlog

from airflow.sdk.execution_time.comms import (
    DeleteXCom,
    GetXCom,
    GetXComSequenceSlice,
    SetXCom,
    XComResult,
    XComSequenceSliceResult,
)

if TYPE_CHECKING:
    from pydantic import JsonValue

    from airflow.sdk.types import TaskInstance

# Lightweight wrapper for XCom values
_XComValueWrapper = collections.namedtuple("_XComValueWrapper", "value")

log = structlog.get_logger(logger_name="task")


@attrs.frozen
class XComCoordinate:
    """
    The task instance an XCom is written for.

    This is the type passed to :meth:`BaseXCom.store_value`, used when implementing a custom XCom backend.

    ``dag_id``, ``run_id`` and ``task_id`` do not identify a task instance: every iteration of a task inside a
    loop has the same three, and so does every map index of a mapped task. ``ti_id`` does. It is set when the
    write is made by the task instance itself, or by its trigger while it is deferred, and ``None`` when one
    task instance writes another's XCom through coordinates, since only the server can tell which task
    instance those coordinates resolve to.

    ``region_id`` and ``region_index`` are the stored position of the task instance: ``region_index`` is its
    place within the region ``region_id``, which is the map index of a mapped task and the loop iteration of
    a task directly inside a loop. They are known only together with ``ti_id``, and are ``None`` otherwise and
    for a task instance outside any region.
    """

    dag_id: str
    run_id: str
    task_id: str
    ti_id: UUID | None = None
    region_id: UUID | None = None
    region_index: int | None = None

    def build_path(self, prefix: str, key: str, *, suffix: str = "", max_length: int = 1024) -> str:
        """
        Build ``<prefix>/<dag_id>/<run_id>/<task_id>/<instance>/<key><suffix>`` as the storage path for ``key``.

        ``<instance>`` is ``ti-<ti_id>`` when ``ti_id`` is set, so that a task instance rewriting a key reuses
        its path, and ``write-<uuid>`` otherwise, so that no two writes share a path. A backend that builds
        its paths here needs no case of its own for either.

        ``dag_id``, ``run_id`` and ``task_id`` are shortened, longest first, only when the path would exceed
        ``max_length`` bytes. The key is quoted, or replaced by its SHA-256 digest when it is too long to
        quote, and ``suffix`` follows it directly. The key is chosen by the Dag author and can look like a
        file extension, so pass a ``suffix`` that names the stored format if the backend reads the format
        back from the object name.

        :raises ValueError: if the path cannot be made to fit in ``max_length`` bytes
        """
        instance = f"ti-{self.ti_id}" if self.ti_id is not None else f"write-{uuid4()}"
        name = f"{_build_object_name(key)}{suffix}"
        readable = [self.dag_id, self.run_id, self.task_id]
        head = prefix.removesuffix("/")

        def join() -> str:
            return "/".join(part for part in (head, *readable, instance, name) if part)

        while len(join().encode()) > max_length:
            longest = max(range(len(readable)), key=lambda i: len(readable[i].encode()))
            encoded = readable[longest].encode()
            if len(encoded) <= _MIN_READABLE_BYTES:
                raise ValueError(
                    f"Cannot fit an XCom path for key {key!r} in {max_length} bytes under {prefix!r}"
                )
            readable[longest] = encoded[:-1].decode(errors="ignore")
        return join()


_MAX_QUOTED_KEY_LENGTH = 200
_MIN_READABLE_BYTES = 8


def _build_object_name(key: str) -> str:
    quoted = quote(key, safe="")
    # A name made only of dots would be the directory itself.
    if not quoted.strip("."):
        quoted = quoted.replace(".", "%2E")
    # Keys are up to 512 characters and quoting expands them, while file systems cap a name at 255 bytes.
    if len(quoted) <= _MAX_QUOTED_KEY_LENGTH:
        return quoted
    return hashlib.sha256(key.encode()).hexdigest()


class _OwnTaskInstance(Protocol):
    """The task instance a write can come from: a running task, or the task a trigger is deferred for."""

    id: UUID
    dag_id: str
    run_id: str
    task_id: str
    map_index: int | None
    region_id: UUID | None
    region_index: int | None


# Task instance of the trigger whose code is running, so its XCom writes and SDK requests act as that task
# instance; tasks serving several triggers must not inherit it.
_current_trigger_ti: contextvars.ContextVar[_OwnTaskInstance | None] = contextvars.ContextVar(
    "airflow_current_trigger_ti", default=None
)


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
        coordinate = _find_own_coordinate(dag_id, run_id, task_id, map_index) or XComCoordinate(
            dag_id, run_id, task_id
        )
        msg = cls._build_set_message(
            coordinate, map_index, key, value, dag_result=dag_result, mapped_length=None
        )
        cls._send_set(msg)

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
        coordinate = _find_own_coordinate(dag_id, run_id, task_id, map_index) or XComCoordinate(
            dag_id, run_id, task_id
        )
        msg = await cls._abuild_set_message(
            coordinate, map_index, key, value, dag_result=dag_result, mapped_length=None
        )
        await cls._asend_set(msg)

    @classmethod
    def _push(
        cls,
        ti: TaskInstance,
        key: str,
        value: Any,
        *,
        dag_result: bool = False,
        mapped_length: int | None = None,
    ) -> None:
        msg = cls._build_set_message(
            _own_coordinate(ti),
            _public_map_index(ti),
            key,
            value,
            dag_result=dag_result,
            mapped_length=mapped_length,
        )
        cls._send_set(msg)

    @classmethod
    async def _apush(
        cls,
        ti: TaskInstance,
        key: str,
        value: Any,
        *,
        dag_result: bool = False,
        mapped_length: int | None = None,
    ) -> None:
        msg = await cls._abuild_set_message(
            _own_coordinate(ti),
            _public_map_index(ti),
            key,
            value,
            dag_result=dag_result,
            mapped_length=mapped_length,
        )
        await cls._asend_set(msg)

    @classmethod
    def _store(cls, coordinate: XComCoordinate, map_index: int, key: str, value: Any) -> JsonValue:
        """Store ``value`` through the backend, returning what the XCom row holds."""
        if _is_preferred_over(cls, "store_value", "serialize_value"):
            return cls.store_value(coordinate, key, value)
        return cls.serialize_value(
            value=value,
            key=key,
            task_id=coordinate.task_id,
            dag_id=coordinate.dag_id,
            run_id=coordinate.run_id,
            map_index=map_index,
        )

    @classmethod
    def _build_set_message(
        cls,
        coordinate: XComCoordinate,
        map_index: int,
        key: str,
        value: Any,
        *,
        dag_result: bool,
        mapped_length: int | None,
    ) -> SetXCom:
        stored = cls._store(coordinate, map_index, key, value)
        return cls._make_set_message(
            coordinate, map_index, key, stored, dag_result=dag_result, mapped_length=mapped_length
        )

    @classmethod
    async def _abuild_set_message(
        cls,
        coordinate: XComCoordinate,
        map_index: int,
        key: str,
        value: Any,
        *,
        dag_result: bool,
        mapped_length: int | None,
    ) -> SetXCom:
        # The backend writes to external storage, and the event loop also serves the triggerer's other triggers.
        stored = await asyncio.to_thread(cls._store, coordinate, map_index, key, value)
        return cls._make_set_message(
            coordinate, map_index, key, stored, dag_result=dag_result, mapped_length=mapped_length
        )

    @staticmethod
    def _make_set_message(
        coordinate: XComCoordinate,
        map_index: int,
        key: str,
        stored: JsonValue,
        *,
        dag_result: bool,
        mapped_length: int | None,
    ) -> SetXCom:
        return SetXCom(
            key=key,
            value=stored,
            dag_id=coordinate.dag_id,
            task_id=coordinate.task_id,
            run_id=coordinate.run_id,
            map_index=map_index,
            dag_result=dag_result,
            mapped_length=mapped_length,
        )

    @classmethod
    def _send_set(cls, msg: SetXCom) -> None:
        from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

        SUPERVISOR_COMMS.send(msg)

    @classmethod
    async def _asend_set(cls, msg: SetXCom) -> None:
        from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

        await SUPERVISOR_COMMS.asend(msg)

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
    def get_one(
        cls,
        *,
        key: str,
        dag_id: str,
        task_id: str,
        run_id: str,
        map_index: int | None = None,
        include_prior_dates: bool = False,
        _previous_iteration: bool = False,
        _region_id: UUID | None = None,
        _region_index: int | None = None,
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
                previous_iteration=_previous_iteration,
                region_id=_region_id,
                region_index=_region_index,
            ),
        )

        if not isinstance(msg, XComResult):
            raise TypeError(f"Expected XComResult, received: {type(msg)} {msg}")

        if msg.value is not None:
            return cls._load(msg)
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
        _previous_iteration: bool = False,
        _region_id: UUID | None = None,
        _region_index: int | None = None,
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
                previous_iteration=_previous_iteration,
                region_id=_region_id,
                region_index=_region_index,
            ),
        )

        if not isinstance(msg, XComResult):
            raise TypeError(f"Expected XComResult, received: {type(msg)} {msg}")

        if msg.value is not None:
            return await asyncio.to_thread(cls._load, msg)
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

        return cls._load_many(msg.root)

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

        return await asyncio.to_thread(cls._load_many, msg.root)

    @classmethod
    def store_value(cls, coordinate: XComCoordinate, key: str, value: Any) -> JsonValue:
        """
        Store an XCom value for ``coordinate`` and return what its XCom row in the metadata database holds.

        Override this to keep values outside the metadata database, in any format. The return value must be
        JSON-serializable and must locate what was stored, since :meth:`load_value` and :meth:`purge` only
        receive the row. Every iteration of a task inside a loop is a separate task instance with the same
        ``dag_id``, ``run_id``, ``task_id``, ``map_index`` and ``key``, so the storage location must tell
        task instances apart.

        :meth:`XComCoordinate.build_path` builds such a location. When a task instance, or its trigger,
        writes its own XCom, ``coordinate.ti_id`` is that task instance's id, and a location unique per
        ``(coordinate.ti_id, key)`` is enough. When one task writes another's XCom, ``coordinate.ti_id`` is
        ``None``, because only the server knows which task instance the coordinates resolve to, so the
        location must be unique to each write. A backend that does not use ``build_path`` has to handle both
        cases itself, or call ``super()``.

        The default stores the value in the metadata database, so an override can call ``super()`` for the
        values it does not keep elsewhere.
        """
        return BaseXCom.serialize_value(value)

    @classmethod
    def load_value(cls, stored: JsonValue) -> Any:
        """
        Return the value for an XCom row, loading it from storage if :meth:`store_value` put it there.

        Rows can also hold values the backend did not store, such as those written through the REST API or
        under an earlier backend, so pass those to ``super()``. The default delegates to
        :meth:`deserialize_value`.
        """
        return cls.deserialize_value(_XComValueWrapper(stored))

    @classmethod
    def _load_many(cls, values: list[JsonValue]) -> list[Any]:
        return [cls._load(_XComValueWrapper(value)) for value in values]

    @classmethod
    def _load(cls, result: Any) -> Any:
        # Legacy backends keep receiving the result object deserialize_value has always been given.
        if _is_preferred_over(cls, "load_value", "deserialize_value"):
            return cls.load_value(result.value)
        return cls.deserialize_value(result)

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


class XComIterable(Sequence):
    """
    An iterable that lazily fetches XCom values one by one instead of loading all at once.

    This is a read-only :class:`collections.abc.Sequence` over the ``return_value_<index>`` XComs an
    iterated task pushed, one per index: the values are written by the producing task's runner as
    each indexed task finishes (see ``IndexedTaskLifecycle._push_result``), and the iterable only ever reads
    them. Nothing on this class mutates the underlying XComs.

    Indexing follows the usual sequence rules, negative indices included: ``result[-1]`` is the last
    value. Iterations that were skipped pushed nothing and are left out, as the XComs of skipped
    mapped task instances are: ``length`` counts every input item, ``skipped`` lists the indices
    that were skipped, and positions in the sequence run over the others only. An iteration that
    returned ``None`` pushed nothing either but keeps its position: reading it gives ``None``,
    where ``.expand()`` would not count such a mapped task instance at all.

    Every element is a remote fetch, so random access costs one XCom read per element, and so do
    iterating and slicing: N elements are N requests. Reading several in one request needs an
    Execution API endpoint that takes a list of keys, which does not exist yet.

    Inside a loop, the iterable records the region and pass of the task instance that pushed it
    (``region_id``, ``region_index``), and its element reads name them: a reader outside the loop,
    one in a later pass (``loop.previous``) and one in the same pass all read the items of the pass
    that produced them. Outside any loop both are None and the reads carry no scope.
    """

    def __init__(
        self,
        task_id: str,
        dag_id: str,
        run_id: str,
        map_index: int | None = None,
        length: int | None = None,
        skipped: Sequence[int] = (),
        region_id: UUID | str | None = None,
        region_index: int | None = None,
    ):
        self.task_id = task_id
        self.dag_id = dag_id
        self.run_id = run_id
        self.map_index = map_index
        self.length = length or 0
        self.skipped: list[int] = sorted(skipped)
        # A serialized iterable carries the region id as a string.
        self.region_id = UUID(str(region_id)) if region_id is not None else None
        self.region_index = region_index

    def _index_of(self, position: int) -> int:
        """Map ``position`` in the sequence to its input index, stepping over the skipped indices."""
        # range() maps a negative position from the end and raises IndexError past either end.
        index = range(len(self))[position]
        for skipped_index in self.skipped:
            if skipped_index > index:
                break
            index += 1
        return index

    def __iter__(self) -> Iterator[Any]:
        return _XComIterator(self)

    def __len__(self) -> int:
        return self.length - len(self.skipped)

    async def alen(self) -> int:
        """Async twin of ``len(self)``, for readers on the event loop that take a length before each read."""
        return len(self)

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

        return XCom.get_one(**self._item_query(key))

    async def aget(self, index: int) -> Any:
        """
        Async counterpart of ``self[index]``: fetch one value through ``XCom.aget_one``.

        Use it, or ``async for``, from code running on an event loop that has other SDK calls in
        flight (an iterated task consuming this iterable as its input): a synchronous read there
        would block the loop, and every call in flight on it, for its round trip to the supervisor.
        """
        from airflow.sdk.execution_time.xcom import XCom

        return await XCom.aget_one(**self._item_query(index))

    def __aiter__(self) -> AsyncIterator[Any]:
        return _AsyncXComIterator(self)

    def _item_query(self, position: int) -> dict[str, Any]:
        """
        Build the arguments of the XCom read for the item at ``position``.

        The producer's region and pass are passed only when set, so a read outside a loop calls the
        XCom backend's ``get_one``/``aget_one`` with the same arguments as before.
        """
        query: dict[str, Any] = {
            "key": f"{BaseXCom.XCOM_RETURN_KEY}_{self._index_of(position)}",
            "dag_id": self.dag_id,
            "task_id": self.task_id,
            "run_id": self.run_id,
            "map_index": self.map_index,
        }
        if self.region_id is not None:
            query["_region_id"] = self.region_id
            query["_region_index"] = self.region_index
        return query

    def serialize(self) -> dict:
        """Ensure the object is JSON serializable."""
        data = {
            "task_id": self.task_id,
            "dag_id": self.dag_id,
            "run_id": self.run_id,
            "map_index": self.map_index,
            "length": self.length,
            "skipped": self.skipped,
        }
        if self.region_id is not None:
            data["region_id"] = str(self.region_id)
            data["region_index"] = self.region_index
        return data

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


def _is_preferred_over(xcom: type[BaseXCom], new_hook: str, old_hook: str) -> bool:
    # The override closest to the class wins, so a subclass that overrides only the old hook (e.g. of a
    # backend that implements store_value) keeps its override.
    mro = xcom.__mro__
    return next(i for i, k in enumerate(mro) if new_hook in vars(k)) <= next(
        i for i, k in enumerate(mro) if old_hook in vars(k)
    )


def _is_legacy_xcom_backend(xcom: type[BaseXCom]) -> bool:
    """
    Whether the XCom backend stores task XComs through ``serialize_value`` rather than ``store_value``.

    Such a backend does not receive the task instance id, so it cannot keep values of different task
    instances with the same ``dag_id``, ``run_id``, ``task_id``, ``map_index`` and ``key`` apart.
    """
    return not _is_preferred_over(xcom, "store_value", "serialize_value")


def _public_map_index(ti: _OwnTaskInstance) -> int:
    return ti.map_index if ti.map_index is not None else -1


def _own_coordinate(ti: _OwnTaskInstance) -> XComCoordinate:
    # The top-level region and a missing one both mean the task instance is outside any region.
    in_region = ti.region_id not in (None, UUID(int=0))
    return XComCoordinate(
        ti.dag_id,
        ti.run_id,
        ti.task_id,
        ti.id,
        ti.region_id if in_region else None,
        ti.region_index if in_region else None,
    )


def _find_own_coordinate(dag_id: str, run_id: str, task_id: str, map_index: int) -> XComCoordinate | None:
    """Return the task instance making the write as a coordinate when the coordinates are its own."""
    # The Dag definitions import this module, so importing them at the top level is circular.
    from airflow.sdk.definitions._internal.contextmanager import _CURRENT_CONTEXT

    owner: _OwnTaskInstance
    if (trigger_ti := _current_trigger_ti.get()) is not None:
        owner = trigger_ti
    elif _CURRENT_CONTEXT and (running := _CURRENT_CONTEXT[-1].get("ti")) is not None:
        owner = running
    else:
        return None
    if (owner.dag_id, owner.run_id, owner.task_id, _public_map_index(owner)) == (
        dag_id,
        run_id,
        task_id,
        map_index,
    ):
        return _own_coordinate(owner)
    return None
