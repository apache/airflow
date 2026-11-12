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
so no lang-SDK runtime re-implements the mapping arithmetic. The server sends coordinates, never
values: the supervisor reads each XCom binding, and the runtime applies any ``map()`` callables.
"""

from __future__ import annotations

import dataclasses
import json
from collections.abc import Callable, Mapping, Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any, TypeVar, cast

from pydantic import ValidationError

from airflow.api_fastapi.execution_api.datamodels.task_arg_binding import (
    LiteralArgBinding,
    LiteralValue,
    TaskArgBinding,
    TupleArgBinding,
    XComArgBinding,
    XComRef,
    XComSequenceArgBinding,
    get_arg_bindings_adapter,
)
from airflow.models.dynamic_region import select_current_producer_ids
from airflow.models.expandinput import (
    NotFullyPopulated,
    SchedulerDictOfListsExpandInput,
    SchedulerListOfDictsExpandInput,
)
from airflow.models.task_coordinates import TaskCoordinateResolver
from airflow.models.taskinstance import _get_relevant_map_indexes
from airflow.models.xcom import XCOM_RETURN_KEY, XComModel, xcom_entity
from airflow.serialization.definitions.mappedoperator import get_mapped_ti_count, is_mapped
from airflow.serialization.definitions.notset import NOTSET, ArgNotSet, is_arg_set
from airflow.serialization.definitions.xcom_arg import (
    SchedulerConcatXComArg,
    SchedulerMapXComArg,
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
    expand_keys: Sequence[str] | None = None

    @classmethod
    def from_entry(cls, entry: Mapping[str, Any]) -> _ArgParam:
        group_arg = entry.get("group_arg")
        return cls(
            name=entry["name"],
            value_schema=entry.get("value_schema"),
            default=entry.get("default", NOTSET),
            group_arg=None if group_arg is None else _GroupArg(group_arg["group_id"], group_arg["key"]),
            expand_keys=entry.get("expand_keys"),
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
    try:
        map_index = resolver.public_map_index(ti, dag_version_id=ti.dag_version_id)
        if map_index < 0:
            raise UndeliverableMappedArgBindingsError(
                "the task instance has not been expanded to a map index"
            )
        return _MappedArgBindingDeriver(task, ti, map_index, resolver, session=session).derive(entries)
    except NotFullyPopulated as e:
        raise UndeliverableMappedArgBindingsError(
            f"upstream map lengths are not yet known for {sorted(e.missing)}"
        )
    except ValidationError as e:
        raise UndeliverableMappedArgBindingsError(f"a binding is not valid: {e}")


class _MappedArgBindingDeriver:
    """
    Derive one map index's bindings from the serialized Dag.

    Parse time rejects the shapes some branches raise for, so only a Dag serialized by an older core
    reaches those branches.
    """

    def __init__(
        self,
        task: Operator,
        ti: TaskInstance,
        map_index: int,
        resolver: TaskCoordinateResolver,
        *,
        session: Session,
    ) -> None:
        self.task = task
        self.ti = ti
        self.map_index = map_index
        self.resolver = resolver
        self.session = session
        self._sub_indexes: dict[int, dict[str, int]] = {}
        self._lengths: dict[int, int] = {}

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
        return [self._bind_entry(_ArgParam.from_entry(entry), entry) for entry in entries]

    def _bind_entry(self, param: _ArgParam, entry: dict[str, Any]) -> TaskArgBinding:
        match entry["kind"]:
            case "param" if param.group_arg is not None:
                return self._bind_group_arg(param, param.group_arg)
            case "param" if is_mapped(self.task):
                return self._bind_mapped_param(param)
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
                index = self._get_sub_indexes(expand_input, param.expand_keys)[param.name]
                return self._bind_element(param, expand_input.value[param.name], index)
        # With strict=True a mapping cannot override partial(), so the partial() value binds as it is.
        # A mapping read from an upstream output that also has the key fails the Python task, but
        # no binding can express that check.
        elif not (self.task._disallow_kwargs_override and param.name in partial_kwargs) and (
            binding := self._bind_mapping_entry(
                param, expand_input, param.name, fallback=lambda: self._get_fallback(param, partial_kwargs)
            )
        ):
            return binding
        if param.name in partial_kwargs:
            return self._bind_whole(param, partial_kwargs[param.name])
        return self._bind_default(param)

    def _bind_group_arg(self, param: _ArgParam, group_arg: _GroupArg) -> TaskArgBinding:
        group = cast("SerializedMappedTaskGroup", self.dag.task_group_dict[group_arg.group_id])
        expand_input = group._expand_input
        if isinstance(expand_input, SchedulerDictOfListsExpandInput):
            index = self._get_sub_indexes(expand_input, param.expand_keys)[group_arg.key]
            return self._bind_element(param, expand_input.value[group_arg.key], index)
        if binding := self._bind_mapping_entry(param, expand_input, group_arg.key, fallback=lambda: None):
            return binding
        raise UndeliverableMappedArgBindingsError(f"expand_kwargs() mapping has no key {group_arg.key!r}")

    @staticmethod
    def _bind_default(param: _ArgParam) -> LiteralArgBinding:
        if not is_arg_set(param.default):
            raise UndeliverableMappedArgBindingsError(
                f"parameter {param.name!r} has no value at this map index"
            )
        return param.build(LiteralArgBinding, kind="literal", value=param.default, from_default=True)

    def _get_sub_indexes(
        self, expand_input: SchedulerDictOfListsExpandInput, keys: Sequence[str] | None
    ) -> dict[str, int]:
        # Outside a mapped task group, a single expanded kwarg is the whole expansion, so the
        # map index is already its element index and no upstream length needs loading.
        if len(expand_input.value) == 1 and self.task.get_closest_mapped_task_group() is None:
            return dict.fromkeys(expand_input.value, self.map_index)
        if (sub_indexes := self._sub_indexes.get(id(expand_input))) is None:
            lengths = {key: self._get_value_length(value) for key, value in expand_input.value.items()}
            try:
                sub_indexes = expand_input.resolve_expansion_sub_indexes(
                    self.map_index,
                    # Parse time records the order only when there are several expanded kwargs.
                    keys or list(expand_input.value),
                    lengths,
                )
            except ValueError as e:
                raise UndeliverableMappedArgBindingsError(str(e))
            self._sub_indexes[id(expand_input)] = sub_indexes
        return sub_indexes

    def _get_value_length(self, value: Any) -> int:
        if isinstance(value, _XComRef):
            value = value.deref(self.dag)
        if isinstance(value, SchedulerXComArg):
            return self._get_length(value)
        return len(value)

    def _bind_mapping_entry(
        self,
        param: _ArgParam,
        expand_input: SchedulerListOfDictsExpandInput,
        key: str,
        *,
        fallback: Callable[[], XComRef | LiteralValue | None],
    ) -> TaskArgBinding | None:
        """
        Bind entry *key* of this map index's ``expand_kwargs()`` mapping; None if it has no such entry.

        A mapping read from an upstream output has keys unknown until it runs, so its binding names
        the key in ``kwarg`` and carries the value to use when the key is missing in ``fallback``.
        """
        if isinstance(expand_input.value, SchedulerXComArg):
            binding = self._bind_element(param, expand_input.value, self.map_index)
            return self._select_kwarg(binding, key, fallback())
        try:
            mapping = expand_input.value[self.map_index]
        except IndexError:
            raise UndeliverableMappedArgBindingsError(
                f"expand_kwargs() has no mapping at map index {self.map_index}"
            )
        match mapping:
            case _XComRef() | SchedulerXComArg():
                return self._select_kwarg(self._bind_whole(param, mapping), key, fallback())
            case Mapping() if key in mapping:
                return self._bind_whole(param, mapping[key])
            case Mapping():
                return None
            case _:
                raise UndeliverableMappedArgBindingsError("expand_kwargs() expects a list of dicts")

    @staticmethod
    def _select_kwarg(
        binding: TaskArgBinding, key: str, fallback: XComRef | LiteralValue | None
    ) -> TaskArgBinding:
        # A zip() element is a tuple, which only a map() can turn into a dict.
        if not (
            isinstance(binding, XComArgBinding)
            or (isinstance(binding, TupleArgBinding) and binding.apply_map)
        ):
            raise UndeliverableMappedArgBindingsError("expand_kwargs() expects a list of dicts")
        update: dict[str, Any] = {"kwarg": key}
        if fallback is not None:
            update["fallback"] = fallback
        return binding.model_copy(update=update)

    def _get_fallback(
        self, param: _ArgParam, partial_kwargs: Mapping[str, Any]
    ) -> XComRef | LiteralValue | None:
        """Get the value of a parameter this index's ``expand_kwargs()`` mapping may lack."""
        if param.name not in partial_kwargs:
            return (
                None if not is_arg_set(param.default) else LiteralValue(kind="literal", value=param.default)
            )
        match self._bind_whole(param, partial_kwargs[param.name]):
            case XComArgBinding() as binding:
                return XComRef.model_validate(
                    binding.model_dump(include=set(XComRef.model_fields), exclude_unset=True)
                )
            case LiteralArgBinding() as binding:
                return LiteralValue(kind="literal", value=binding.value)
            case _:
                raise UndeliverableMappedArgBindingsError(
                    f"parameter {param.name!r} cannot fall back to a mapped task's whole output"
                )

    def _bind_whole(self, param: _ArgParam, value: Any) -> TaskArgBinding:
        """Bind *value* as it is, for an argument this map index does not expand over."""
        if isinstance(value, _XComRef):
            value = value.deref(self.dag)
        match value:
            case SchedulerPlainXComArg():
                return self._bind_upstream(param, value.operator)
            case SchedulerXComArg():
                raise UndeliverableMappedArgBindingsError(
                    f"parameter {param.name!r} receives a whole map(), zip() or concat() result"
                )
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
                raise UndeliverableMappedArgBindingsError(
                    f"parameter {param.name!r} reads task {producer.task_id!r}, which is mapped again "
                    "inside the mapped task group they share"
                )

    def _bind_element(self, param: _ArgParam, value: Any, index: int) -> TaskArgBinding:
        """Bind element *index* of *value*, for an argument this map index expands over."""
        if isinstance(value, _XComRef):
            value = value.deref(self.dag)
        match value:
            case SchedulerMapXComArg():
                # map() is elementwise, so it moves no index: pick the element, then apply the callables.
                return self._locate(param, value.arg, index).model_copy(update={"apply_map": True})
            case SchedulerXComArg():
                return self._locate(param, value, index)
            case Sequence() if not isinstance(value, str):
                items: Sequence[Any] = value
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
            case SchedulerZipXComArg():
                parts: list[XComRef | LiteralValue] = []
                for zipped in arg.args:
                    if index < self._get_length(zipped):
                        parts.append(self._locate_ref(zipped, index))
                    elif is_arg_set(arg.fillvalue):
                        parts.append(LiteralValue(kind="literal", value=self._ensure_json(arg.fillvalue)))
                    else:
                        raise UndeliverableMappedArgBindingsError(f"zip() has no element at index {index}")
                return param.build(TupleArgBinding, kind="tuple", parts=parts)
            case SchedulerConcatXComArg():
                concatenated, offset = self._find_concatenated(arg, index)
                return self._locate(param, concatenated, offset)
            case _:
                ref = self._locate_ref(arg, index)
                return param.build(XComArgBinding, **ref.model_dump(exclude_unset=True))

    def _locate_ref(self, arg: SchedulerXComArg, index: int) -> XComRef:
        """Locate element *index* of an upstream output that holds no ``zip()``."""
        match arg:
            # An expanded output is never narrowed to a common mapped ancestor's row first: the SDK
            # forbids expanding inside a mapped task group, and a group expands only over outputs
            # of tasks outside it.
            case SchedulerPlainXComArg() if arg.operator.get_needs_expansion():
                return XComRef(
                    kind="xcom",
                    task_id=arg.operator.task_id,
                    map_index=self._find_row_map_index(arg.operator, index),
                )
            case SchedulerPlainXComArg():
                return XComRef(kind="xcom", task_id=arg.operator.task_id, element_index=index)
            case SchedulerConcatXComArg():
                return self._locate_ref(*self._find_concatenated(arg, index))
            case _:
                raise UndeliverableMappedArgBindingsError(
                    "a map() must be outermost and a zip() cannot be inside another zip()"
                )

    def _find_concatenated(self, arg: SchedulerConcatXComArg, index: int) -> tuple[SchedulerXComArg, int]:
        """Find the concatenated argument owning element *index*, and the element's offset in it."""
        offset = index
        for concatenated in arg.args:
            if offset < (length := self._get_length(concatenated)):
                return concatenated, offset
            offset -= length
        raise UndeliverableMappedArgBindingsError(f"concat() has no element at index {index}")

    def _get_length(self, arg: SchedulerXComArg) -> int:
        """Get the length of *arg* as ``get_task_map_length`` does, loading each upstream length once."""
        if (length := self._lengths.get(id(arg))) is not None:
            return length
        match arg:
            case SchedulerMapXComArg():
                length = self._get_length(arg.arg)
            case SchedulerZipXComArg():
                lengths = [self._get_length(zipped) for zipped in arg.args]
                length = max(lengths) if is_arg_set(arg.fillvalue) else min(lengths)
            case SchedulerConcatXComArg():
                length = sum(self._get_length(concatenated) for concatenated in arg.args)
            case _:
                loaded = get_task_map_length(
                    arg, self.ti.run_id, producer_contexts=self.producer_contexts, session=self.session
                )
                if loaded is None:
                    raise NotFullyPopulated({repr(arg)})
                length = loaded
        self._lengths[id(arg)] = length
        return length

    def _find_row_map_index(self, producer: Operator, offset: int) -> int:
        """
        Find the map index of row *offset* of a mapped task's output.

        A mapped instance that returned nothing has no row, so the Python task runner reads the rows
        that exist in map index order, and a row's offset can be lower than its map index.
        """
        read = XComModel.get_many(
            run_id=self.ti.run_id,
            key=XCOM_RETURN_KEY,
            producer_ids=select_current_producer_ids(
                dag_id=producer.dag_id,
                run_id=self.ti.run_id,
                task_id=producer.task_id,
                is_mapped=is_mapped(producer),
                context=self.producer_contexts.get(producer.task_id),
                session=self.session,
            ),
        )
        entity = xcom_entity(read)
        map_index = self.session.scalar(
            read.order_by(None)
            .where(entity.map_index >= 0)
            .with_only_columns(entity.map_index)
            .order_by(entity.map_index)
            .offset(offset)
            .limit(1)
        )
        if map_index is None:
            raise UndeliverableMappedArgBindingsError(
                f"task {producer.task_id!r} has no output row at index {offset}"
            )
        return map_index

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

    @classmethod
    def _bind_literal(cls, param: _ArgParam, value: Any) -> LiteralArgBinding:
        return param.build(LiteralArgBinding, kind="literal", value=cls._ensure_json(value))

    @staticmethod
    def _ensure_json(value: Any) -> Any:
        try:
            json.dumps(value, allow_nan=False)
        except (TypeError, ValueError):
            raise UndeliverableMappedArgBindingsError(
                f"a {type(value).__name__} value cannot cross the language boundary"
            )
        return value
