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

from collections.abc import Iterable, Mapping, Sequence, Sized
from typing import TYPE_CHECKING, Any, ClassVar, Union

import attrs

from airflow.sdk.definitions._internal.mixins import ResolveMixin

if TYPE_CHECKING:
    from typing import TypeGuard

    from airflow.sdk.definitions.xcom_arg import XComArg
    from airflow.sdk.types import Operator

ExpandInput = Union["DictOfListsExpandInput", "ListOfDictsExpandInput"]

# Each keyword argument to expand() can be an XComArg, sequence, or dict (not
# any mapping since we need the value to be ordered).
OperatorExpandArgument = Union["MappedArgument", "XComArg", Sequence, dict[str, Any]]

# The single argument of expand_kwargs() can be an XComArg, or a list with each
# element being either an XComArg or a dict.
OperatorExpandKwargsArgument = Union["XComArg", Sequence[Union["XComArg", Mapping[str, Any]]]]


class _NotFullyPopulated(RuntimeError):
    """
    Raise when an expand input cannot be resolved due to incomplete metadata.

    This generally should not happen. The scheduler should have made sure that
    a not-yet-ready-to-expand task should not be executed. In the off chance
    this gets raised, it will fail the task instance.
    """

    def __init__(self, missing: set[str]) -> None:
        self.missing = missing

    def __str__(self) -> str:
        keys = ", ".join(repr(k) for k in sorted(self.missing))
        return f"Failed to populate all mapping metadata; missing: {keys}"


# To replace tedious isinstance() checks.
def is_mappable(v: Any) -> TypeGuard[OperatorExpandArgument]:
    from airflow.sdk.definitions.xcom_arg import XComArg

    return isinstance(v, (MappedArgument, XComArg, Mapping, Sequence)) and not isinstance(v, str)


# To replace tedious isinstance() checks.
def _is_parse_time_mappable(v: OperatorExpandArgument) -> TypeGuard[Mapping | Sequence]:
    from airflow.sdk.definitions.xcom_arg import XComArg

    return not isinstance(v, (MappedArgument, XComArg))


# To replace tedious isinstance() checks.
def _needs_run_time_resolution(v: OperatorExpandArgument) -> TypeGuard[MappedArgument | XComArg]:
    from airflow.sdk.definitions.xcom_arg import XComArg

    return isinstance(v, (MappedArgument, XComArg))


@attrs.define(kw_only=True)
class MappedArgument(ResolveMixin):
    """
    Stand-in stub for task-group-mapping arguments.

    This is very similar to an XComArg, but resolved differently. Declared here
    (instead of in the task group module) to avoid import cycles.
    """

    _input: ExpandInput = attrs.field()
    _key: str

    @_input.validator
    def _validate_input(self, _, input):
        if isinstance(input, DictOfListsExpandInput):
            for value in input.value.values():
                if isinstance(value, MappedArgument):
                    raise ValueError("Nested Mapped TaskGroups are not yet supported")

    def iter_references(self) -> Iterable[tuple[Operator, str]]:
        yield from self._input.iter_references()

    def resolve(self, context: Mapping[str, Any]) -> Any:
        data, _ = self._input.resolve(context)
        return data[self._key]


@attrs.define()
class DictOfListsExpandInput(ResolveMixin):
    """
    Storage type of a mapped operator's mapped kwargs.

    This is created from ``expand(**kwargs)``.
    """

    value: dict[str, OperatorExpandArgument]

    EXPAND_INPUT_TYPE: ClassVar[str] = "dict-of-lists"

    def _iter_parse_time_resolved_kwargs(self) -> Iterable[tuple[str, Sized]]:
        """Generate kwargs with values available on parse-time."""
        return ((k, v) for k, v in self.value.items() if _is_parse_time_mappable(v))

    def _get_map_lengths(
        self, resolved_vals: dict[str, Sized], upstream_map_indexes: dict[str, int]
    ) -> dict[str, int]:
        """
        Return dict of argument name to map length.

        If any arguments are not known right now (upstream task not finished),
        they will not be present in the dict.
        """

        # TODO: This initiates one API call for each XComArg. Would it be
        # more efficient to do one single call and unpack the value here?
        def _get_length(k: str, v: OperatorExpandArgument) -> int | None:
            from airflow.sdk.definitions.xcom_arg import XComArg, get_task_map_length

            if isinstance(v, XComArg):
                return get_task_map_length(v, resolved_vals[k], upstream_map_indexes)

            # Unfortunately a user-defined TypeGuard cannot apply negative type
            # narrowing. https://github.com/python/typing/discussions/1013
            if TYPE_CHECKING:
                assert isinstance(v, Sized)
            return len(v)

        map_lengths = {
            k: res for k, v in self.value.items() if v is not None if (res := _get_length(k, v)) is not None
        }
        if len(map_lengths) < len(self.value):
            raise _NotFullyPopulated(set(self.value).difference(map_lengths))
        return map_lengths

    def _expand_mapped_field(self, key: str, value: Any, map_index: int, all_lengths: dict[str, int]) -> Any:
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
        if isinstance(value, Sequence):
            return value[found_index]
        if not isinstance(value, dict):
            raise TypeError(f"can't map over value of type {type(value)}")
        for i, (k, v) in enumerate(value.items()):
            if i == found_index:
                return k, v
        raise IndexError(f"index {map_index} is over mapped length")

    def iter_references(self) -> Iterable[tuple[Operator, str]]:
        from airflow.sdk.definitions.xcom_arg import XComArg

        for x in self.value.values():
            if isinstance(x, XComArg):
                yield from x.iter_references()

    def resolve(self, context: Mapping[str, Any]) -> tuple[Mapping[str, Any], set[int]]:
        map_index: int | None = context["ti"].map_index
        if map_index is None or map_index < 0:
            raise RuntimeError("can't resolve task-mapping argument without expanding")

        upstream_map_indexes = getattr(context["ti"], "_upstream_map_indexes", {})

        # TODO: This initiates one API call for each XComArg. Would it be
        # more efficient to do one single call and unpack the value here?

        resolved = {
            k: v.resolve(context) if _needs_run_time_resolution(v) else v for k, v in self.value.items()
        }

        sized_resolved = {k: v for k, v in resolved.items() if isinstance(v, Sized)}

        all_lengths = self._get_map_lengths(sized_resolved, upstream_map_indexes)

        data = {k: self._expand_mapped_field(k, v, map_index, all_lengths) for k, v in resolved.items()}
        literal_keys = {k for k, _ in self._iter_parse_time_resolved_kwargs()}
        resolved_oids = {id(v) for k, v in data.items() if k not in literal_keys}
        return data, resolved_oids


def _describe_type(value: Any) -> str:
    if value is None:
        return "None"
    return type(value).__name__


@attrs.define()
class ListOfDictsExpandInput(ResolveMixin):
    """
    Storage type of a mapped operator's mapped kwargs.

    This is created from ``expand_kwargs(xcom_arg)``.
    """

    value: OperatorExpandKwargsArgument

    EXPAND_INPUT_TYPE: ClassVar[str] = "list-of-dicts"

    def iter_references(self) -> Iterable[tuple[Operator, str]]:
        from airflow.sdk.definitions.xcom_arg import XComArg

        if isinstance(self.value, XComArg):
            yield from self.value.iter_references()
        else:
            for x in self.value:
                if isinstance(x, XComArg):
                    yield from x.iter_references()

    def resolve(self, context: Mapping[str, Any]) -> tuple[Mapping[str, Any], set[int]]:
        map_index = context["ti"].map_index
        if map_index < 0:
            raise RuntimeError("can't resolve task-mapping argument without expanding")

        mapping: Any = None
        if isinstance(self.value, Sized):
            mapping = self.value[map_index]
            if not isinstance(mapping, Mapping):
                mapping = mapping.resolve(context)
        else:
            mappings = self.value.resolve(context)
            if not isinstance(mappings, Sequence):
                raise ValueError(f"expand_kwargs() expects a list[dict], not {_describe_type(mappings)}")
            mapping = mappings[map_index]

        if not isinstance(mapping, Mapping):
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
