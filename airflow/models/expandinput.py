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
import collections.abc
import functools
import operator
from typing import TYPE_CHECKING, Any, Iterable, NamedTuple, Sequence, Sized, Union

from sqlalchemy import func
from sqlalchemy.orm import Session

from airflow.compat.functools import cache
from airflow.utils.context import Context

if TYPE_CHECKING:
    from airflow.models.xcom_arg import XComArg

ExpandInput = Union["DictOfListsExpandInput", "ListOfDictsExpandInput"]

# BaseOperator.expand() can be called on an XComArg, sequence, or dict (not any
# mapping since we need the value to be ordered).
Mappable = Union["XComArg", Sequence, dict]


# For isinstance() check.
@cache
def get_mappable_types() -> tuple[type, ...]:
    from airflow.models.xcom_arg import XComArg

    return (XComArg, list, tuple, dict)


class NotFullyPopulated(RuntimeError):
    """Raise when ``get_map_lengths`` cannot populate all mapping metadata.

    This is generally due to not all upstream tasks have finished when the
    function is called.
    """

    def __init__(self, missing: set[str]) -> None:
        self.missing = missing

    def __str__(self) -> str:
        keys = ", ".join(repr(k) for k in sorted(self.missing))
        return f"Failed to populate all mapping metadata; missing: {keys}"


class DictOfListsExpandInput(NamedTuple):
    """Storage type of a mapped operator's mapped kwargs.

    This is created from ``expand(**kwargs)``.
    """

    value: dict[str, Mappable]

    def get_unresolved_kwargs(self) -> dict[str, Any]:
        """Get the kwargs dict that can be inferred without resolving."""
        return self.value

    def iter_parse_time_resolved_kwargs(self) -> Iterable[tuple[str, Sized]]:
        """Generate kwargs with values available on parse-time."""
        from airflow.models.xcom_arg import XComArg

        return ((k, v) for k, v in self.value.items() if not isinstance(v, XComArg))

    def get_parse_time_mapped_ti_count(self) -> int | None:
        if not self.value:
            return 0
        literal_values = [len(v) for _, v in self.iter_parse_time_resolved_kwargs()]
        if len(literal_values) != len(self.value):
            return None  # None-literal type encountered, so give up.
        return functools.reduce(operator.mul, literal_values, 1)

    def _get_map_lengths(self, run_id: str, *, session: Session) -> dict[str, int]:
        """Return dict of argument name to map length.

        If any arguments are not known right now (upstream task not finished),
        they will not be present in the dict.
        """
        from airflow.models.taskmap import TaskMap
        from airflow.models.xcom import XCom
        from airflow.models.xcom_arg import XComArg

        # Populate literal mapped arguments first.
        map_lengths: dict[str, int] = collections.defaultdict(int)
        map_lengths.update((k, len(v)) for k, v in self.value.items() if not isinstance(v, XComArg))

        try:
            dag_id = next(v.operator.dag_id for v in self.value.values() if isinstance(v, XComArg))
        except StopIteration:  # All mapped arguments are literal. We're done.
            return map_lengths

        # Build a reverse mapping of what arguments each task contributes to.
        mapped_dep_keys: dict[str, set[str]] = collections.defaultdict(set)
        non_mapped_dep_keys: dict[str, set[str]] = collections.defaultdict(set)
        for k, v in self.value.items():
            if not isinstance(v, XComArg):
                continue
            assert v.operator.dag_id == dag_id
            if v.operator.is_mapped:
                mapped_dep_keys[v.operator.task_id].add(k)
            else:
                non_mapped_dep_keys[v.operator.task_id].add(k)
            # TODO: It's not possible now, but in the future we may support
            # depending on one single mapped task instance. When that happens,
            # we need to further analyze the mapped case to contain only tasks
            # we depend on "as a whole", and put those we only depend on
            # individually to the non-mapped lookup.

        # Collect lengths from unmapped upstreams.
        taskmap_query = session.query(TaskMap.task_id, TaskMap.length).filter(
            TaskMap.dag_id == dag_id,
            TaskMap.run_id == run_id,
            TaskMap.task_id.in_(non_mapped_dep_keys),
            TaskMap.map_index < 0,
        )
        for task_id, length in taskmap_query:
            for mapped_arg_name in non_mapped_dep_keys[task_id]:
                map_lengths[mapped_arg_name] += length

        # Collect lengths from mapped upstreams.
        xcom_query = (
            session.query(XCom.task_id, func.count(XCom.map_index))
            .group_by(XCom.task_id)
            .filter(
                XCom.dag_id == dag_id,
                XCom.run_id == run_id,
                XCom.task_id.in_(mapped_dep_keys),
                XCom.map_index >= 0,
            )
        )
        for task_id, length in xcom_query:
            for mapped_arg_name in mapped_dep_keys[task_id]:
                map_lengths[mapped_arg_name] += length

        if len(map_lengths) < len(self.value):
            raise NotFullyPopulated(set(self.value).difference(map_lengths))
        return map_lengths

    def get_total_map_length(self, run_id: str, *, session: Session) -> int:
        if not self.value:
            return 0
        lengths = self._get_map_lengths(run_id, session=session)
        return functools.reduce(operator.mul, (lengths[name] for name in self.value), 1)

    def _expand_mapped_field(self, key: str, value: Any, context: Context, *, session: Session) -> Any:
        from airflow.models.xcom_arg import XComArg

        if isinstance(value, XComArg):
            value = value.resolve(context, session=session)
        map_index = context["ti"].map_index
        if map_index < 0:
            raise RuntimeError("can't resolve task-mapping argument without expanding")
        all_lengths = self._get_map_lengths(context["run_id"], session=session)

        def _find_index_for_this_field(index: int) -> int:
            # Need to use the original user input to retain argument order.
            for mapped_key in reversed(list(self.value)):
                mapped_length = all_lengths[mapped_key]
                if mapped_length < 1:
                    raise RuntimeError(f"cannot expand field mapped to length {mapped_length!r}")
                if mapped_key == key:
                    return index % mapped_length
                index //= mapped_length
            return -1

        found_index = _find_index_for_this_field(map_index)
        if found_index < 0:
            return value
        if isinstance(value, collections.abc.Sequence):
            return value[found_index]
        if not isinstance(value, dict):
            raise TypeError(f"can't map over value of type {type(value)}")
        for i, (k, v) in enumerate(value.items()):
            if i == found_index:
                return k, v
        raise IndexError(f"index {map_index} is over mapped length")

    def resolve(self, context: Context, session: Session) -> dict[str, Any]:
        return {k: self._expand_mapped_field(k, v, context, session=session) for k, v in self.value.items()}


class ListOfDictsExpandInput(NamedTuple):
    """Storage type of a mapped operator's mapped kwargs.

    This is created from ``expand_kwargs(xcom_arg)``.
    """

    value: XComArg

    def get_unresolved_kwargs(self) -> dict[str, Any]:
        """Get the kwargs dict that can be inferred without resolving.

        Since the list-of-dicts case relies entirely on run-time XCom, there's
        no kwargs structure available, so this just returns an empty dict.
        """
        return {}

    def iter_parse_time_resolved_kwargs(self) -> Iterable[tuple[str, Sized]]:
        return ()

    def get_parse_time_mapped_ti_count(self) -> int | None:
        return None

    def get_total_map_length(self, run_id: str, *, session: Session) -> int:
        from airflow.models.taskmap import TaskMap
        from airflow.models.xcom import XCom

        task = self.value.operator
        if task.is_mapped:
            query = session.query(func.count(XCom.map_index)).filter(
                XCom.dag_id == task.dag_id,
                XCom.run_id == run_id,
                XCom.task_id == task.task_id,
                XCom.map_index >= 0,
            )
        else:
            query = session.query(TaskMap.length).filter(
                TaskMap.dag_id == task.dag_id,
                TaskMap.run_id == run_id,
                TaskMap.task_id == task.task_id,
                TaskMap.map_index < 0,
            )
        value = query.scalar()
        if value is None:
            raise NotFullyPopulated({"expand_kwargs() argument"})
        return value

    def resolve(self, context: Context, session: Session) -> dict[str, Any]:
        map_index = context["ti"].map_index
        if map_index < 0:
            raise RuntimeError("can't resolve task-mapping argument without expanding")
        # Validation should be done when the upstream returns.
        return self.value.resolve(context, session)[map_index]


EXPAND_INPUT_EMPTY = DictOfListsExpandInput({})  # Sentinel value.

_EXPAND_INPUT_TYPES = {
    "dict-of-lists": DictOfListsExpandInput,
    "list-of-dicts": ListOfDictsExpandInput,
}


def get_map_type_key(expand_input: ExpandInput) -> str:
    return next(k for k, v in _EXPAND_INPUT_TYPES.items() if v == type(expand_input))


def create_expand_input(kind: str, value: Any) -> ExpandInput:
    return _EXPAND_INPUT_TYPES[kind](value)
