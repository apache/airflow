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
from typing import TYPE_CHECKING, Any, Iterable, Mapping, NamedTuple, Sequence, Sized, Union

from airflow.compat.functools import cache
from airflow.utils.context import Context

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

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
        from airflow.models.xcom_arg import XComArg

        # TODO: This initiates one database call for each XComArg. Would it be
        # more efficient to do one single db call and unpack the value here?
        map_lengths_iterator = (
            (k, (v.get_task_map_length(run_id, session=session) if isinstance(v, XComArg) else len(v)))
            for k, v in self.value.items()
        )

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

    def resolve(self, context: Context, session: Session) -> tuple[Mapping[str, Any], set[int]]:
        data = {k: self._expand_mapped_field(k, v, context, session=session) for k, v in self.value.items()}
        literal_keys = {k for k, _ in self.iter_parse_time_resolved_kwargs()}
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
        length = self.value.get_task_map_length(run_id, session=session)
        if length is None:
            raise NotFullyPopulated({"expand_kwargs() argument"})
        return length

    def resolve(self, context: Context, session: Session) -> tuple[Mapping[str, Any], set[int]]:
        map_index = context["ti"].map_index
        if map_index < 0:
            raise RuntimeError("can't resolve task-mapping argument without expanding")
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
        return mapping, {id(v) for v in mapping.values()}


EXPAND_INPUT_EMPTY = DictOfListsExpandInput({})  # Sentinel value.

_EXPAND_INPUT_TYPES = {
    "dict-of-lists": DictOfListsExpandInput,
    "list-of-dicts": ListOfDictsExpandInput,
}


def get_map_type_key(expand_input: ExpandInput) -> str:
    return next(k for k, v in _EXPAND_INPUT_TYPES.items() if v == type(expand_input))


def create_expand_input(kind: str, value: Any) -> ExpandInput:
    return _EXPAND_INPUT_TYPES[kind](value)
