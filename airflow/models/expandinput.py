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

import collections.abc
import functools
import operator
from collections.abc import Sized
from typing import TYPE_CHECKING, Any, Dict, Iterable, Mapping, NamedTuple, Sequence, Union

import attr

from airflow.utils.mixins import ResolveMixin
from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.models.operator import Operator
    from airflow.models.xcom_arg import XComArg
    from airflow.typing_compat import TypeGuard
    from airflow.utils.context import Context

ExpandInput = Union["DictOfListsExpandInput", "ListOfDictsExpandInput"]

# Each keyword argument to expand() can be an XComArg, sequence, or dict (not
# any mapping since we need the value to be ordered).
OperatorExpandArgument = Union["MappedArgument", "XComArg", Sequence, Dict[str, Any]]

# The single argument of expand_kwargs() can be an XComArg, or a list with each
# element being either an XComArg or a dict.
OperatorExpandKwargsArgument = Union["XComArg", Sequence[Union["XComArg", Mapping[str, Any]]]]


@attr.define(kw_only=True)
class MappedArgument(ResolveMixin):
    """Stand-in stub for task-group-mapping arguments.

    This is very similar to an XComArg, but resolved differently. Declared here
    (instead of in the task group module) to avoid import cycles.
    """

    _input: ExpandInput
    _key: str

    def get_task_map_length(self, run_id: str, *, session: Session) -> int | None:
        # TODO (AIP-42): Implement run-time task map length inspection. This is
        # needed when we implement task mapping inside a mapped task group.
        raise NotImplementedError()

    def iter_references(self) -> Iterable[tuple[Operator, str]]:
        yield from self._input.iter_references()

    @provide_session
    def resolve(self, context: Context, *, session: Session = NEW_SESSION) -> Any:
        data, _ = self._input.resolve(context, session=session)
        return data[self._key]


# To replace tedious isinstance() checks.
def is_mappable(v: Any) -> TypeGuard[OperatorExpandArgument]:
    from airflow.models.xcom_arg import XComArg

    return isinstance(v, (MappedArgument, XComArg, Mapping, Sequence)) and not isinstance(v, str)


# To replace tedious isinstance() checks.
def _is_parse_time_mappable(v: OperatorExpandArgument) -> TypeGuard[Mapping | Sequence]:
    from airflow.models.xcom_arg import XComArg

    return not isinstance(v, (MappedArgument, XComArg))


# To replace tedious isinstance() checks.
def _needs_run_time_resolution(v: OperatorExpandArgument) -> TypeGuard[MappedArgument | XComArg]:
    from airflow.models.xcom_arg import XComArg

    return isinstance(v, (MappedArgument, XComArg))


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

    value: dict[str, OperatorExpandArgument]

    def _iter_parse_time_resolved_kwargs(self) -> Iterable[tuple[str, Sized]]:
        """Generate kwargs with values available on parse-time."""
        return ((k, v) for k, v in self.value.items() if _is_parse_time_mappable(v))

    def get_parse_time_mapped_ti_count(self) -> int:
        if not self.value:
            return 0
        literal_values = [len(v) for _, v in self._iter_parse_time_resolved_kwargs()]
        if len(literal_values) != len(self.value):
            literal_keys = (k for k, _ in self._iter_parse_time_resolved_kwargs())
            raise NotFullyPopulated(set(self.value).difference(literal_keys))
        return functools.reduce(operator.mul, literal_values, 1)

    def _get_map_lengths(self, run_id: str, *, session: Session) -> dict[str, int]:
        """Return dict of argument name to map length.

        If any arguments are not known right now (upstream task not finished),
        they will not be present in the dict.
        """

        # TODO: This initiates one database call for each XComArg. Would it be
        # more efficient to do one single db call and unpack the value here?
        def _get_length(v: OperatorExpandArgument) -> int | None:
            if _needs_run_time_resolution(v):
                return v.get_task_map_length(run_id, session=session)
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

    def _expand_mapped_field(self, key: str, value: Any, context: Context, *, session: Session) -> Any:
        if _needs_run_time_resolution(value):
            value = value.resolve(context, session=session)
        map_index = context["ti"].map_index
        if map_index < 0:
            raise RuntimeError("can't resolve task-mapping argument without expanding")
        all_lengths = self._get_map_lengths(context["run_id"], session=session)

        def _find_index_for_this_field(index: int) -> int:
            # Need to use the original user input to retain argument order.
            for mapped_key in reversed(self.value):
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

    def iter_references(self) -> Iterable[tuple[Operator, str]]:
        from airflow.models.xcom_arg import XComArg

        for x in self.value.values():
            if isinstance(x, XComArg):
                yield from x.iter_references()

    def resolve(self, context: Context, session: Session) -> tuple[Mapping[str, Any], set[int]]:
        data = {k: self._expand_mapped_field(k, v, context, session=session) for k, v in self.value.items()}
        literal_keys = {k for k, _ in self._iter_parse_time_resolved_kwargs()}
        resolved_oids = {id(v) for k, v in data.items() if k not in literal_keys}
        return data, resolved_oids


def _describe_type(value: Any) -> str:
    if value is None:
        return "None"
    return type(value).__name__


class ListOfDictsExpandInput(NamedTuple):
    """Storage type of a mapped operator's mapped kwargs.

    This is created from ``expand_kwargs(xcom_arg)``.
    """

    value: OperatorExpandKwargsArgument

    def get_parse_time_mapped_ti_count(self) -> int:
        if isinstance(self.value, collections.abc.Sized):
            return len(self.value)
        raise NotFullyPopulated({"expand_kwargs() argument"})

    def get_total_map_length(self, run_id: str, *, session: Session) -> int:
        if isinstance(self.value, collections.abc.Sized):
            return len(self.value)
        length = self.value.get_task_map_length(run_id, session=session)
        if length is None:
            raise NotFullyPopulated({"expand_kwargs() argument"})
        return length

    def iter_references(self) -> Iterable[tuple[Operator, str]]:
        from airflow.models.xcom_arg import XComArg

        if isinstance(self.value, XComArg):
            yield from self.value.iter_references()
        else:
            for x in self.value:
                if isinstance(x, XComArg):
                    yield from x.iter_references()

    def resolve(self, context: Context, session: Session) -> tuple[Mapping[str, Any], set[int]]:
        map_index = context["ti"].map_index
        if map_index < 0:
            raise RuntimeError("can't resolve task-mapping argument without expanding")

        mapping: Any
        if isinstance(self.value, collections.abc.Sized):
            mapping = self.value[map_index]
            if not isinstance(mapping, collections.abc.Mapping):
                mapping = mapping.resolve(context, session)
        else:
            mappings = self.value.resolve(context, session)
            if not isinstance(mappings, collections.abc.Sequence):
                raise ValueError(f"expand_kwargs() expects a list[dict], not {_describe_type(mappings)}")
            mapping = mappings[map_index]

        if not isinstance(mapping, collections.abc.Mapping):
            raise ValueError(f"expand_kwargs() expects a list[dict], not list[{_describe_type(mapping)}]")

        for key in mapping:
            if not isinstance(key, str):
                raise ValueError(
                    f"expand_kwargs() input dict keys must all be str, "
                    f"but {key!r} is of type {_describe_type(key)}"
                )
        # filter out parse time resolved values from the resolved_oids
        resolved_oids = {id(v) for k, v in mapping.items() if not _is_parse_time_mappable(v)}

        return mapping, resolved_oids


EXPAND_INPUT_EMPTY = DictOfListsExpandInput({})  # Sentinel value.

_EXPAND_INPUT_TYPES = {
    "dict-of-lists": DictOfListsExpandInput,
    "list-of-dicts": ListOfDictsExpandInput,
}


def get_map_type_key(expand_input: ExpandInput) -> str:
    return next(k for k, v in _EXPAND_INPUT_TYPES.items() if v == type(expand_input))


def create_expand_input(kind: str, value: Any) -> ExpandInput:
    return _EXPAND_INPUT_TYPES[kind](value)
