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
"""
Derive the arg bindings of a stub task whose arguments differ per map index.

That is a mapped stub, or an unmapped one inside a mapped task group. The server picks this map
index's element of every argument, mirroring how the Python task runner resolves the same Dag,
so no lang-SDK runtime re-implements the mapping arithmetic. An argument the server cannot finish
(``map()`` callables, or a nesting no single binding can hold) is sent as an ``unresolved`` binding.
"""

from __future__ import annotations

import dataclasses
import json
from collections.abc import Mapping, Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any, TypeVar, cast

from pydantic import ValidationError

from airflow.api_fastapi.execution_api.datamodels.task_arg_binding import (
    LiteralArgBinding,
    TaskArgBinding,
    TupleArgBinding,
    UnresolvedArgBinding,
    XComArgBinding,
    XComSequenceArgBinding,
    get_arg_bindings_adapter,
)
from airflow.models.expandinput import (
    NotFullyPopulated,
    SchedulerDictOfListsExpandInput,
    SchedulerListOfDictsExpandInput,
)
from airflow.models.task_coordinates import TaskCoordinateResolver
from airflow.models.taskinstance import _get_relevant_map_indexes
from airflow.serialization.definitions.mappedoperator import get_mapped_ti_count, is_mapped
from airflow.serialization.definitions.notset import NOTSET, ArgNotSet, is_arg_set
from airflow.serialization.definitions.xcom_arg import (
    SchedulerConcatXComArg,
    SchedulerPlainXComArg,
    SchedulerXComArg,
    SchedulerZipXComArg,
    get_task_map_length,
)
from airflow.serialization.serialized_objects import _XComRef

if TYPE_CHECKING:
    from pydantic import BaseModel
    from sqlalchemy.orm import Session

    from airflow.models.dagbag import DBDagBag
    from airflow.models.dynamic_region import ProducerContext
    from airflow.models.taskinstance import TaskInstance
    from airflow.serialization.definitions.dag import SerializedDAG
    from airflow.serialization.definitions.mappedoperator import Operator
    from airflow.serialization.definitions.taskgroup import SerializedMappedTaskGroup

    _BindingT = TypeVar("_BindingT", bound=BaseModel)


class UndeliverableMappedArgBindingsError(ValueError):
    """The stub task's arguments cannot be delivered at this map index."""


class _NeedsWorker(Exception):
    """The argument is sent as an ``unresolved`` binding for the coordinator to resolve."""


@dataclasses.dataclass(frozen=True)
class _GroupArg:
    group_id: str
    key: str


@dataclasses.dataclass(frozen=True)
class _ArgParam:
    """One stub parameter, read from its serialized ``_arg_bindings`` entry."""

    name: str
    value_schema: dict[str, Any] | None = None
    default: Any | ArgNotSet = NOTSET
    group_arg: _GroupArg | None = None

    @classmethod
    def from_entry(cls, entry: Mapping[str, Any]) -> _ArgParam:
        group_arg = entry.get("group_arg")
        return cls(
            name=entry["name"],
            value_schema=entry.get("value_schema"),
            default=entry.get("default", NOTSET),
            group_arg=None if group_arg is None else _GroupArg(group_arg["group_id"], group_arg["key"]),
        )

    def build(self, binding_cls: type[_BindingT], **fields: Any) -> _BindingT:
        # Only fields passed here count as set, so the response's exclude_unset omits the rest.
        if self.value_schema is not None:
            fields["value_schema"] = self.value_schema
        return binding_cls(name=self.name, **fields)


def derive_mapped_arg_bindings(
    task: Operator,
    ti: TaskInstance,
    entries: list[dict[str, Any]],
    *,
    dag_bag: DBDagBag,
    session: Session,
) -> list[TaskArgBinding]:
    """
    Build the bindings of *ti* at its map index from the serialized ``_arg_bindings`` *entries*.

    :raises UndeliverableMappedArgBindingsError: if an argument has no value at this map index.
    """
    resolver = TaskCoordinateResolver(dag_bag, session)
    map_index = resolver.public_map_index(ti, dag_version_id=ti.dag_version_id)
    if map_index < 0:
        raise UndeliverableMappedArgBindingsError("the task instance has not been expanded to a map index")
    try:
        return _MappedArgBindingDeriver(task, ti, map_index, resolver, session).derive(entries)
    except NotFullyPopulated as e:
        raise UndeliverableMappedArgBindingsError(
            f"upstream map lengths are not yet known for {sorted(e.missing)}"
        )
    except ValidationError as e:
        raise UndeliverableMappedArgBindingsError(f"a binding is not valid: {e}")


class _MappedArgBindingDeriver:
    def __init__(
        self,
        task: Operator,
        ti: TaskInstance,
        map_index: int,
        resolver: TaskCoordinateResolver,
        session: Session,
    ) -> None:
        self.task = task
        self.ti = ti
        self.map_index = map_index
        self.resolver = resolver
        self.session = session

    @property
    def dag(self) -> SerializedDAG:
        if (dag := self.task.dag) is None:
            raise UndeliverableMappedArgBindingsError(f"task {self.task.task_id!r} has no Dag")
        return dag

    @cached_property
    def producer_contexts(self) -> Mapping[str, ProducerContext]:
        return self.resolver.producer_contexts(self.ti, task=self.task)

    @cached_property
    def expanded_ti_count(self) -> int:
        """The count the Python task runner gets as ``expanded_ti_count``, to pick relevant upstream rows."""
        count = self.resolver.get_expanded_ti_count(self.ti, task=self.task)
        if count is None:
            count = get_mapped_ti_count(
                self.task, self.ti.run_id, session=self.session, producer_contexts=self.producer_contexts
            )
        return count

    def derive(self, entries: list[dict[str, Any]]) -> list[TaskArgBinding]:
        bindings: list[TaskArgBinding] = []
        for entry in entries:
            param = _ArgParam.from_entry(entry)
            try:
                bindings.append(self._bind_entry(param, entry))
            except _NeedsWorker:
                bindings.append(param.build(UnresolvedArgBinding, kind="unresolved"))
        return bindings

    def _bind_entry(self, param: _ArgParam, entry: dict[str, Any]) -> TaskArgBinding:
        match entry["kind"]:
            case "param" if param.group_arg is not None:
                return self._bind_group_arg(param, param.group_arg)
            case "param" if is_mapped(self.task):
                return self._bind_mapped_param(param)
            case "param":
                return self._bind_default(param)
            case "xcom" | "sequence":
                # Parse time cannot tell which rows of an upstream output this map index reads.
                if (producer := self.dag.task_dict.get(entry["task_id"])) is None:
                    raise UndeliverableMappedArgBindingsError(
                        f"upstream task {entry['task_id']!r} is missing"
                    )
                return self._bind_upstream(param, producer)
            case _:
                (binding,) = get_arg_bindings_adapter().validate_python([entry])
                return binding

    def _bind_mapped_param(self, param: _ArgParam) -> TaskArgBinding:
        expand_input = self.task._get_specified_expand_input()
        partial_kwargs = self.task.partial_kwargs.get("op_kwargs") or {}
        if isinstance(expand_input, SchedulerDictOfListsExpandInput):
            if param.name in expand_input.value:
                index = self._get_sub_indexes(expand_input)[param.name]
                return self._bind_element(param, expand_input.value[param.name], index)
        elif binding := self._bind_mapping_entry(
            param, expand_input, param.name, required=False, known_keys_only=param.name in partial_kwargs
        ):
            return binding
        if param.name in partial_kwargs:
            return self._bind_whole(param, partial_kwargs[param.name])
        return self._bind_default(param)

    def _bind_group_arg(self, param: _ArgParam, group_arg: _GroupArg) -> TaskArgBinding:
        group = cast("SerializedMappedTaskGroup", self.dag.task_group_dict[group_arg.group_id])
        expand_input = group._expand_input
        if isinstance(expand_input, SchedulerDictOfListsExpandInput):
            index = self._get_sub_indexes(expand_input)[group_arg.key]
            return self._bind_element(param, expand_input.value[group_arg.key], index)
        return cast(
            "TaskArgBinding", self._bind_mapping_entry(param, expand_input, group_arg.key, required=True)
        )

    @staticmethod
    def _bind_default(param: _ArgParam) -> LiteralArgBinding:
        if not is_arg_set(param.default):
            raise UndeliverableMappedArgBindingsError(
                f"parameter {param.name!r} has no value at this map index"
            )
        return param.build(LiteralArgBinding, kind="literal", value=param.default, from_default=True)

    def _get_sub_indexes(self, expand_input: SchedulerDictOfListsExpandInput) -> dict[str, int]:
        # Outside a mapped task group, a single expanded kwarg is the whole expansion, so the
        # map index is already its element index and no upstream length needs loading.
        if len(expand_input.value) == 1 and self.task.get_closest_mapped_task_group() is None:
            return dict.fromkeys(expand_input.value, self.map_index)
        try:
            return expand_input.resolve_expansion_sub_indexes(
                self.map_index, self.ti.run_id, session=self.session, producer_contexts=self.producer_contexts
            )
        except ValueError as e:
            raise UndeliverableMappedArgBindingsError(str(e))

    def _bind_mapping_entry(
        self,
        param: _ArgParam,
        expand_input: SchedulerListOfDictsExpandInput,
        key: str,
        *,
        required: bool,
        known_keys_only: bool = False,
    ) -> TaskArgBinding | None:
        """
        Bind entry *key* of this map index's ``expand_kwargs()`` mapping; None if it has no such entry.

        A mapping entry overrides a ``partial()`` value, as in Python. With *known_keys_only*, an
        upstream output's mapping, whose keys are unknown until it runs, yields None so that the
        ``partial()`` value is used.
        """
        if isinstance(expand_input.value, SchedulerXComArg):
            if known_keys_only:
                return None
            return self._select_mapping_key(
                self._bind_element(param, expand_input.value, self.map_index), key
            )
        try:
            mapping = expand_input.value[self.map_index]
        except IndexError:
            raise UndeliverableMappedArgBindingsError(
                f"expand_kwargs() has no mapping at map index {self.map_index}"
            )
        match mapping:
            case _XComRef() | SchedulerXComArg() if known_keys_only:
                return None
            case _XComRef() | SchedulerXComArg():
                return self._select_mapping_key(self._bind_whole(param, mapping), key)
            case Mapping() if key in mapping:
                return self._bind_whole(param, mapping[key])
            case Mapping() if required:
                raise UndeliverableMappedArgBindingsError(f"expand_kwargs() mapping has no key {key!r}")
            case Mapping():
                return None
            case _:
                raise UndeliverableMappedArgBindingsError("expand_kwargs() expects a list of dicts")

    @staticmethod
    def _select_mapping_key(binding: TaskArgBinding, key: str) -> XComArgBinding:
        if not isinstance(binding, XComArgBinding):
            raise UndeliverableMappedArgBindingsError("expand_kwargs() expects a list of dicts")
        return binding.model_copy(update={"mapping_key": key})

    def _bind_whole(self, param: _ArgParam, value: Any) -> TaskArgBinding:
        """Bind *value* as it is, for an argument this map index does not expand over."""
        if isinstance(value, _XComRef):
            value = value.deref(self.dag)
        match value:
            case SchedulerPlainXComArg(operator=producer):
                return self._bind_upstream(param, producer)
            case SchedulerXComArg():
                raise _NeedsWorker
            case _:
                return self._bind_literal(param, value)

    def _bind_upstream(self, param: _ArgParam, producer: Operator) -> XComArgBinding | XComSequenceArgBinding:
        """Bind the output of *producer*, narrowed to this map index's row when they share a mapped group."""
        if not producer.get_needs_expansion():
            return param.build(XComArgBinding, kind="xcom", task_id=producer.task_id)
        match self._get_relevant_map_indexes(producer):
            case None:
                return param.build(XComSequenceArgBinding, kind="sequence", task_id=producer.task_id)
            case int(map_index):
                return param.build(XComArgBinding, kind="xcom", task_id=producer.task_id, map_index=map_index)
            case _:
                raise _NeedsWorker

    def _bind_element(self, param: _ArgParam, value: Any, index: int) -> TaskArgBinding:
        """Bind element *index* of *value*, for an argument this map index expands over."""
        if isinstance(value, _XComRef):
            value = value.deref(self.dag)
        match value:
            case SchedulerXComArg():
                return self._locate(param, value, index)
            case Mapping():
                items: Sequence[Any] = list(value.items())
            case Sequence() if not isinstance(value, str):
                items = value
            case _:
                items = ()
        if not 0 <= index < len(items):
            raise UndeliverableMappedArgBindingsError(f"the expanded value has no element at index {index}")
        return self._bind_literal(param, items[index])

    def _locate(
        self, param: _ArgParam, arg: SchedulerXComArg, index: int
    ) -> XComArgBinding | TupleArgBinding:
        """Locate element *index* of an upstream output, following ``zip()`` and ``concat()``."""
        match arg:
            # An expanded output is never narrowed to a common mapped ancestor's row first: the SDK
            # forbids expanding inside a mapped task group, and a group expands only over outputs
            # of tasks outside it.
            case SchedulerPlainXComArg(operator=producer) if producer.get_needs_expansion():
                return param.build(XComArgBinding, kind="xcom", task_id=producer.task_id, map_index=index)
            case SchedulerPlainXComArg(operator=producer):
                return param.build(XComArgBinding, kind="xcom", task_id=producer.task_id, element_index=index)
            case SchedulerZipXComArg():
                # The parameter's schema describes the whole tuple, not a part of it.
                part_param = dataclasses.replace(param, value_schema=None)
                parts: list[XComArgBinding | LiteralArgBinding] = []
                for zipped in arg.args:
                    if index < self._get_length(zipped):
                        if not isinstance(part := self._locate(part_param, zipped, index), XComArgBinding):
                            raise _NeedsWorker
                        parts.append(part)
                    elif is_arg_set(arg.fillvalue):
                        parts.append(self._bind_literal(part_param, arg.fillvalue))
                    else:
                        raise UndeliverableMappedArgBindingsError(f"zip() has no element at index {index}")
                return param.build(TupleArgBinding, kind="tuple", parts=parts)
            case SchedulerConcatXComArg():
                offset = index
                for concatenated in arg.args:
                    if offset < (length := self._get_length(concatenated)):
                        return self._locate(param, concatenated, offset)
                    offset -= length
                raise UndeliverableMappedArgBindingsError(f"concat() has no element at index {index}")
            case _:
                raise _NeedsWorker

    def _get_length(self, arg: SchedulerXComArg) -> int:
        length = get_task_map_length(
            arg, self.ti.run_id, producer_contexts=self.producer_contexts, session=self.session
        )
        if length is None:
            raise NotFullyPopulated({repr(arg)})
        return length

    def _get_relevant_map_indexes(self, producer: Operator) -> int | range | None:
        if self.task.get_closest_mapped_task_group() is None:
            return None
        return _get_relevant_map_indexes(
            task=self.task,
            run_id=self.ti.run_id,
            map_index=self.map_index,
            relative=producer,
            ti_count=self.expanded_ti_count,
            session=self.session,
            producer_contexts=self.producer_contexts,
        )

    @staticmethod
    def _bind_literal(param: _ArgParam, value: Any) -> LiteralArgBinding:
        try:
            json.dumps(value, allow_nan=False)
        except (TypeError, ValueError):
            raise UndeliverableMappedArgBindingsError(
                f"a {type(value).__name__} value cannot cross the language boundary"
            )
        return param.build(LiteralArgBinding, kind="literal", value=value)
