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

from collections.abc import Iterable, Iterator, Mapping, Sequence
from functools import singledispatch
from typing import TYPE_CHECKING, Any

import attrs
from sqlalchemy import func, or_, select, tuple_
from sqlalchemy.orm import Session

from airflow.models.referencemixin import ReferenceMixin
from airflow.models.taskinstance import TaskInstance
from airflow.models.taskmap import TaskMap
from airflow.models.xcom import XCOM_RETURN_KEY, XComModel
from airflow.serialization.definitions.mappedoperator import is_mapped
from airflow.serialization.definitions.notset import NOTSET, is_arg_set
from airflow.utils.db import exists_query
from airflow.utils.state import State

__all__ = [
    "SchedulerXComArg",
    "deserialize_xcom_arg",
    "get_task_map_length",
    "prefetch_map_lengths",
]

if TYPE_CHECKING:
    from airflow.serialization.definitions.dag import SerializedDAG
    from airflow.serialization.definitions.mappedoperator import Operator
    from airflow.typing_compat import Self

# Map length of each referenced task, keyed by ``(dag_id, task_id)``. A task whose
# length is not known yet (upstream unfinished) is absent rather than mapped to None.
MapLengths = Mapping[tuple[str, str], int]


class SchedulerXComArg:
    """
    Reference to an XCom value pushed from another operator.

    This is the safe counterpart to :class:`airflow.sdk.XComArg`.
    """

    @classmethod
    def _deserialize(cls, data: dict[str, Any], dag: SerializedDAG) -> Self:
        """
        Deserialize an XComArg.

        The implementation should be the inverse function to ``serialize``,
        implementing given a data dict converted from this XComArg derivative,
        how the original XComArg should be created. DAG serialization relies on
        additional information added in ``serialize_xcom_arg`` to dispatch data
        dicts to the correct ``_deserialize`` information, so this function does
        not need to validate whether the incoming data contains correct keys.
        """
        raise NotImplementedError("This class should not be instantiated directly")

    @classmethod
    def iter_xcom_references(cls, arg: Any) -> Iterator[tuple[Operator, str]]:
        """
        Return XCom references in an arbitrary value.

        Recursively traverse ``arg`` and look for XComArg instances in any
        collection objects, and instances with ``template_fields`` set.
        """
        from airflow.serialization.definitions.baseoperator import SerializedBaseOperator
        from airflow.serialization.definitions.mappedoperator import SerializedMappedOperator

        if isinstance(arg, ReferenceMixin):
            yield from arg.iter_references()
        elif isinstance(arg, (tuple, set, list)):
            for elem in arg:
                yield from cls.iter_xcom_references(elem)
        elif isinstance(arg, dict):
            for elem in arg.values():
                yield from cls.iter_xcom_references(elem)
        elif isinstance(arg, (SerializedMappedOperator, SerializedBaseOperator)):
            for attr in arg.template_fields:
                yield from cls.iter_xcom_references(getattr(arg, attr))

    def iter_references(self) -> Iterator[tuple[Operator, str]]:
        raise NotImplementedError("This class should not be instantiated directly")


@attrs.define
class SchedulerPlainXComArg(SchedulerXComArg):
    operator: Operator
    key: str

    @classmethod
    def _deserialize(cls, data: dict[str, Any], dag: SerializedDAG) -> Self:
        return cls(dag.get_task(data["task_id"]), data["key"])

    def iter_references(self) -> Iterator[tuple[Operator, str]]:
        yield self.operator, self.key


@attrs.define
class SchedulerMapXComArg(SchedulerXComArg):
    arg: SchedulerXComArg
    callables: Sequence[str]

    @classmethod
    def _deserialize(cls, data: dict[str, Any], dag: SerializedDAG) -> Self:
        # We are deliberately NOT deserializing the callables. These are shown
        # in the UI, and displaying a function object is useless.
        return cls(deserialize_xcom_arg(data["arg"], dag), data["callables"])

    def iter_references(self) -> Iterator[tuple[Operator, str]]:
        yield from self.arg.iter_references()


@attrs.define
class SchedulerConcatXComArg(SchedulerXComArg):
    args: Sequence[SchedulerXComArg]

    @classmethod
    def _deserialize(cls, data: dict[str, Any], dag: SerializedDAG) -> Self:
        return cls([deserialize_xcom_arg(arg, dag) for arg in data["args"]])

    def iter_references(self) -> Iterator[tuple[Operator, str]]:
        for arg in self.args:
            yield from arg.iter_references()


@attrs.define
class SchedulerZipXComArg(SchedulerXComArg):
    args: Sequence[SchedulerXComArg]
    fillvalue: Any

    @classmethod
    def _deserialize(cls, data: dict[str, Any], dag: SerializedDAG) -> Self:
        return cls(
            [deserialize_xcom_arg(arg, dag) for arg in data["args"]],
            fillvalue=data.get("fillvalue", NOTSET),
        )

    def iter_references(self) -> Iterator[tuple[Operator, str]]:
        for arg in self.args:
            yield from arg.iter_references()


def prefetch_map_lengths(
    xcom_args: Iterable[SchedulerXComArg], run_id: str, *, session: Session
) -> dict[tuple[str, str], int]:
    """
    Resolve the map length of every task referenced by ``xcom_args`` in bulk.

    Passing the result to :func:`get_task_map_length` as ``lengths`` keeps the number of
    queries constant no matter how many arguments -- and how many tasks nested inside
    ``zip()``/``concat()`` arguments -- have to be resolved.

    Tasks whose length is not known yet are absent from the result, mirroring the
    ``None`` that :func:`get_task_map_length` returns for them.
    """
    operators = {(op.dag_id, op.task_id): op for arg in xcom_args for op, _ in arg.iter_references()}
    if not operators:
        return {}
    mapped = {key for key, op in operators.items() if is_mapped(op)}
    unmapped = operators.keys() - mapped

    lengths: dict[tuple[str, str], int] = {}
    if unmapped:
        rows = session.execute(
            select(TaskMap.dag_id, TaskMap.task_id, TaskMap.length).where(
                TaskMap.run_id == run_id,
                TaskMap.map_index < 0,
                tuple_(TaskMap.dag_id, TaskMap.task_id).in_(sorted(unmapped)),
            )
        )
        lengths.update(((dag_id, task_id), length) for dag_id, task_id, length in rows)
    if mapped:
        unfinished = set(
            session.execute(
                select(TaskInstance.dag_id, TaskInstance.task_id)
                .where(
                    TaskInstance.run_id == run_id,
                    tuple_(TaskInstance.dag_id, TaskInstance.task_id).in_(sorted(mapped)),
                    # Special NULL treatment is needed because 'state' can be NULL.
                    # The "IN" part would produce "NULL NOT IN ..." and eventually
                    # "NULl = NULL", which is a big no-no in SQL.
                    or_(
                        TaskInstance.state.is_(None),
                        TaskInstance.state.in_(s.value for s in State.unfinished if s is not None),
                    ),
                )
                .distinct()
            )
        )
        if finished := mapped - unfinished:
            counts = {
                (dag_id, task_id): count
                for dag_id, task_id, count in session.execute(
                    select(XComModel.dag_id, XComModel.task_id, func.count(XComModel.map_index))
                    .where(
                        XComModel.run_id == run_id,
                        XComModel.map_index >= 0,
                        XComModel.key == XCOM_RETURN_KEY,
                        tuple_(XComModel.dag_id, XComModel.task_id).in_(sorted(finished)),
                    )
                    .group_by(XComModel.dag_id, XComModel.task_id)
                )
            }
            # A finished mapped task that pushed nothing has no row to group, but its
            # length is a known zero rather than an unresolved value.
            lengths.update((key, counts.get(key, 0)) for key in finished)
    return lengths


@singledispatch
def get_task_map_length(
    xcom_arg: SchedulerXComArg, run_id: str, *, lengths: MapLengths | None = None, session: Session
) -> int | None:
    # The base implementation -- specific XComArg subclasses have specialised implementations
    raise NotImplementedError(f"get_task_map_length not implemented for {type(xcom_arg)}")


@get_task_map_length.register
def _(
    xcom_arg: SchedulerPlainXComArg, run_id: str, *, lengths: MapLengths | None = None, session: Session
) -> int | None:
    dag_id = xcom_arg.operator.dag_id
    task_id = xcom_arg.operator.task_id

    if lengths is not None:
        return lengths.get((dag_id, task_id))

    if is_mapped(xcom_arg.operator):
        unfinished_ti_exists = exists_query(
            TaskInstance.dag_id == dag_id,
            TaskInstance.run_id == run_id,
            TaskInstance.task_id == task_id,
            # Special NULL treatment is needed because 'state' can be NULL.
            # The "IN" part would produce "NULL NOT IN ..." and eventually
            # "NULl = NULL", which is a big no-no in SQL.
            or_(
                TaskInstance.state.is_(None),
                TaskInstance.state.in_(s.value for s in State.unfinished if s is not None),
            ),
            session=session,
        )
        if unfinished_ti_exists:
            return None  # Not all of the expanded tis are done yet.
        query = select(func.count(XComModel.map_index)).where(
            XComModel.dag_id == dag_id,
            XComModel.run_id == run_id,
            XComModel.task_id == task_id,
            XComModel.map_index >= 0,
            XComModel.key == XCOM_RETURN_KEY,
        )
    else:
        query = select(TaskMap.length).where(
            TaskMap.dag_id == dag_id,
            TaskMap.run_id == run_id,
            TaskMap.task_id == task_id,
            TaskMap.map_index < 0,
        )
    return session.scalar(query)


@get_task_map_length.register
def _(
    xcom_arg: SchedulerMapXComArg, run_id: str, *, lengths: MapLengths | None = None, session: Session
) -> int | None:
    return get_task_map_length(xcom_arg.arg, run_id, lengths=lengths, session=session)


@get_task_map_length.register
def _(
    xcom_arg: SchedulerZipXComArg, run_id: str, *, lengths: MapLengths | None = None, session: Session
) -> int | None:
    all_lengths = (
        get_task_map_length(arg, run_id, lengths=lengths, session=session) for arg in xcom_arg.args
    )
    ready_lengths = [length for length in all_lengths if length is not None]
    if len(ready_lengths) != len(xcom_arg.args):
        return None  # If any of the referenced XComs is not ready, we are not ready either.
    if is_arg_set(xcom_arg.fillvalue):
        return max(ready_lengths)
    return min(ready_lengths)


@get_task_map_length.register
def _(
    xcom_arg: SchedulerConcatXComArg, run_id: str, *, lengths: MapLengths | None = None, session: Session
) -> int | None:
    all_lengths = (
        get_task_map_length(arg, run_id, lengths=lengths, session=session) for arg in xcom_arg.args
    )
    ready_lengths = [length for length in all_lengths if length is not None]
    if len(ready_lengths) != len(xcom_arg.args):
        return None  # If any of the referenced XComs is not ready, we are not ready either.
    return sum(ready_lengths)


def deserialize_xcom_arg(data: dict[str, Any], dag: SerializedDAG):
    """DAG serialization interface."""
    klass = _XCOM_ARG_TYPES[data.get("type", "")]
    return klass._deserialize(data, dag)


_XCOM_ARG_TYPES: dict[str, type[SchedulerXComArg]] = {
    "": SchedulerPlainXComArg,
    "concat": SchedulerConcatXComArg,
    "map": SchedulerMapXComArg,
    "zip": SchedulerZipXComArg,
}
