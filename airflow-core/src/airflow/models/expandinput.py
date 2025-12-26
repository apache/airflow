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

import functools
import operator
from collections.abc import Iterable, Sized
from typing import TYPE_CHECKING, Any, ClassVar

import attrs

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from typing import TypeAlias

    from sqlalchemy.orm import Session
    from typing_extensions import TypeIs

    from airflow.serialization.definitions.mappedoperator import Operator
    from airflow.serialization.definitions.xcom_arg import SchedulerXComArg

    ExpandArgument: TypeAlias = "SchedulerMappedArgument" | SchedulerXComArg | Sequence | Mapping[str, Any]
    ExpandKwargsArgument: TypeAlias = SchedulerXComArg | Sequence[SchedulerXComArg | Mapping[str, Any]]


__all__ = [
    "NotFullyPopulated",
    "SchedulerMappedArgument",
    "SchedulerDictOfListsExpandInput",
    "SchedulerListOfDictsExpandInput",
]


class NotFullyPopulated(RuntimeError):
    """
    Raise when mapped length cannot be calculated due to incomplete metadata.

    This is generally due to not all upstream tasks have been completed (or in
    parse-time length calculations, when any upstream has runtime dependencies
    on mapped length) when the function is called.
    """

    def __init__(self, missing: set[str]) -> None:
        self.missing = missing

    def __str__(self) -> str:
        keys = ", ".join(repr(k) for k in sorted(self.missing))
        return f"Failed to populate all mapping metadata; missing: {keys}"


def _needs_run_time_resolution(v: ExpandArgument) -> TypeIs[SchedulerMappedArgument | SchedulerXComArg]:
    from airflow.serialization.definitions.xcom_arg import SchedulerXComArg

    return isinstance(v, (SchedulerMappedArgument, SchedulerXComArg))


@attrs.define(kw_only=True)
class SchedulerMappedArgument:
    """
    Stand-in stub for task-group-mapping arguments.

    This corresponds on SDK's ``MappedArgument``, which is created when
    dynamically mapping a task group, and an argument used to dynamic-map is
    passed into a task inside the group.

    This value is not currently used anywhere in the scheduler since nested
    dynamic mapping is not supported (i.e. using this value to further expand
    an operator inside a mapped task group), but this is implemented so the
    value is displayed better in the UI.
    """

    _input: SchedulerExpandInput = attrs.field()
    _key: str

    def iter_references(self) -> Iterable[tuple[Operator, str]]:
        yield from self._input.iter_references()


@attrs.define
class SchedulerDictOfListsExpandInput:
    """
    Serialized storage of a mapped operator's mapped kwargs.

    This corresponds to SDK's ``DictOfListsExpandInput``, which was created by
    calling ``expand(**kwargs)`` on an operator type.
    """

    value: dict[str, ExpandArgument]

    EXPAND_INPUT_TYPE: ClassVar[str] = "dict-of-lists"

    def _iter_parse_time_resolved_kwargs(self) -> Iterable[tuple[str, Sized]]:
        """Generate kwargs with values available on parse-time."""
        return ((k, v) for k, v in self.value.items() if not _needs_run_time_resolution(v))

    def get_parse_time_mapped_ti_count(self) -> int:
        if not self.value:
            return 0
        literal_values = [len(v) for _, v in self._iter_parse_time_resolved_kwargs()]
        if len(literal_values) != len(self.value):
            literal_keys = (k for k, _ in self._iter_parse_time_resolved_kwargs())
            raise NotFullyPopulated(set(self.value).difference(literal_keys))
        return functools.reduce(operator.mul, literal_values, 1)

    def _get_map_lengths(self, run_id: str, *, session: Session) -> dict[str, int]:
        """
        Return dict of argument name to map length.

        If any arguments are not known right now (upstream task not finished),
        they will not be present in the dict.
        """
        from airflow.serialization.definitions.xcom_arg import SchedulerXComArg, get_task_map_length

        # TODO: This initiates one database call for each XComArg. Would it be
        # more efficient to do one single db call and unpack the value here?
        def _get_length(v: ExpandArgument) -> int | None:
            if isinstance(v, SchedulerXComArg):
                return get_task_map_length(v, run_id, session=session)

            # Unfortunately a user-defined TypeGuard cannot apply negative type
            # narrowing. https://github.com/python/typing/discussions/1013
            if TYPE_CHECKING:
                assert isinstance(v, Sized)
            return len(v)

        map_lengths_iterator = ((k, _get_length(v)) for k, v in self.value.items())

        map_lengths = {k: v for k, v in map_lengths_iterator if v is not None}
        if len(map_lengths) < len(self.value):
            raise NotFullyPopulated(set(self.value).difference(map_lengths))
        return map_lengths

    def get_total_map_length(self, run_id: str, *, session: Session) -> int:
        if not self.value:
            return 0
        lengths = self._get_map_lengths(run_id, session=session)
        return functools.reduce(operator.mul, (lengths[name] for name in self.value), 1)

    def iter_references(self) -> Iterable[tuple[Operator, str]]:
        from airflow.models.referencemixin import ReferenceMixin

        for x in self.value.values():
            if isinstance(x, ReferenceMixin):
                yield from x.iter_references()


@attrs.define
class SchedulerListOfDictsExpandInput:
    """
    Serialized storage of a mapped operator's mapped kwargs.

    This corresponds to SDK's ``ListOfDictsExpandInput``, which was created by
    calling ``expand_kwargs(xcom_arg)`` on an operator type.
    """

    value: ExpandKwargsArgument

    EXPAND_INPUT_TYPE: ClassVar[str] = "list-of-dicts"

    def get_parse_time_mapped_ti_count(self) -> int:
        if isinstance(self.value, Sized):
            return len(self.value)
        raise NotFullyPopulated({"expand_kwargs() argument"})

    def get_total_map_length(self, run_id: str, *, session: Session) -> int:
        from airflow.serialization.definitions.xcom_arg import get_task_map_length

        if isinstance(self.value, Sized):
            return len(self.value)
        length = get_task_map_length(self.value, run_id, session=session)
        if length is None:
            raise NotFullyPopulated({"expand_kwargs() argument"})
        return length

    def iter_references(self) -> Iterable[tuple[Operator, str]]:
        from airflow.models.referencemixin import ReferenceMixin

        if isinstance(self.value, ReferenceMixin):
            yield from self.value.iter_references()
        else:
            for x in self.value:
                if isinstance(x, ReferenceMixin):
                    yield from x.iter_references()


_EXPAND_INPUT_TYPES: dict[str, type[SchedulerExpandInput]] = {
    "dict-of-lists": SchedulerDictOfListsExpandInput,
    "list-of-dicts": SchedulerListOfDictsExpandInput,
}

SchedulerExpandInput = SchedulerDictOfListsExpandInput | SchedulerListOfDictsExpandInput


def create_expand_input(kind: str, value: Any) -> SchedulerExpandInput:
    return _EXPAND_INPUT_TYPES[kind](value)
