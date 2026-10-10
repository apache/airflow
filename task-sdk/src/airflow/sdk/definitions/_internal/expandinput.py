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

import asyncio
import math
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence, Sized
from typing import TYPE_CHECKING, Any, ClassVar, NamedTuple, Union

import attrs

from airflow.sdk.definitions._internal.mixins import ResolveMixin
from airflow.sdk.definitions.xcom_arg import XComArg

if TYPE_CHECKING:
    from typing import TypeGuard

    from airflow.sdk.types import Operator

# Each keyword argument to expand() can be an XComArg, sequence, or dict (not
# any mapping since we need the value to be ordered).
OperatorExpandArgument = Union["MappedArgument", "XComArg", Sequence, dict[str, Any]]

# The single argument of expand_kwargs() can be an XComArg, or a list with each
# element being either an XComArg or a dict.
OperatorExpandKwargsArgument = Union["XComArg", Sequence[Union["XComArg", Mapping[str, Any]]]]


class Resolved(NamedTuple):
    """
    An expand input resolved for iteration: its item count and an async read by index.

    The ``.iterate()`` counterpart of ``ExpandInput.resolve``, which hands one mapped task
    instance the item at its ``map_index``: every source is pulled once, and ``aget(index)`` then
    picks the item an index maps to the way ``resolve`` does for ``.expand()`` (the cross product
    of ``iterate(**kwargs)``, the mapping at that position for ``iterate_kwargs``). Reads run on
    the task's event loop next to the sub-tasks' own SDK calls, so nothing here may block the loop
    thread on the supervisor channel (see ``AsyncAwareExecutor.imap_unordered``).
    """

    length: int
    aget: Callable[[int], Awaitable[Mapping[str, Any]]]


class Source:
    """
    One resolved expand argument, read by index without a blocking supervisor call on the loop.

    A Mapping is read as its ``(key, value)`` pairs and a scalar as a one-item sequence, matching
    ``DictOfListsExpandInput._expand_mapped_field``. A value with async accessors (an
    ``XComIterable``, a mapped upstream's ``LazyXComSequence``) is read through them. Anything
    else may block in ``__getitem__`` (a ``.map()`` result over a lazy sequence, for instance), so
    it is read in a worker thread.
    """

    @classmethod
    async def from_argument(cls, argument: Any, context: Mapping[str, Any]) -> Source:
        """Build a source from an expand argument: pull it if it is an XComArg, then make it a sequence."""
        if isinstance(argument, XComArg):
            argument = await argument.aresolve(context)
            # What .expand() refuses when the upstream pushes (_push_xcom_if_needed raises for a
            # mapped dependant) is refused here: an IterableOperator is not a MappedOperator, so
            # iter_mapped_dependants never finds it and that check does not fire for it.
            from airflow.sdk.definitions.mappedoperator import is_mappable_value
            from airflow.sdk.exceptions import UnmappableXComTypePushed, XComForMappingNotPushed

            if argument is None:
                raise XComForMappingNotPushed()
            if not is_mappable_value(argument):
                raise UnmappableXComTypePushed(argument)
        if isinstance(argument, Mapping):
            return cls(list(argument.items()))
        if isinstance(argument, (str, bytes)) or not isinstance(argument, Iterable):
            return cls((argument,))
        if not isinstance(argument, Sequence):
            return cls(list(argument))
        return cls(argument)

    def __init__(self, value: Sequence[Any]) -> None:
        self.value = value

    async def alen(self) -> int:
        """Item count: the value's own async count when it has one, else ``len()`` in a worker thread."""
        if hasattr(self.value, "alen"):
            return await self.value.alen()
        return await asyncio.to_thread(len, self.value)

    async def aget(self, index: int) -> Any:
        """Item at ``index``; called once per item, so in-memory containers skip the thread hop."""
        value = self.value
        if isinstance(value, (list, tuple, range)):
            return value[index]
        if hasattr(value, "aget"):
            return await value.aget(index)
        return await asyncio.to_thread(value.__getitem__, index)


def index_for_each_field(map_index: int, lengths: Mapping[str, int]) -> dict[str, int]:
    """
    Split a cross-product position into one index per expand argument.

    The arguments are combined as ``itertools.product`` combines them in the order they were
    given, so the last one varies fastest: position 3 of ``a=[1, 2], b=[10, 20]`` is ``a[1], b[1]``.
    Shared by ``.expand()`` (``_expand_mapped_field`` picks its task instance's ``map_index``) and
    ``.iterate()`` (``aresolve`` reads every position), so both hand a sub-task the same item.
    """
    indices: dict[str, int] = {}
    for key in reversed(list(lengths)):
        length = lengths[key]
        if length < 1:
            raise RuntimeError(f"cannot expand field mapped to length {length!r}")
        indices[key] = map_index % length
        map_index //= length
    return {key: indices[key] for key in lengths}


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


@attrs.define(slots=False)
class ExpandInput(ABC, ResolveMixin):
    EXPAND_INPUT_TYPE: ClassVar[str]

    @property
    @abstractmethod
    def value(self) -> Any:
        """The value of the expand input."""
        ...

    async def aresolve(self, context: Mapping[str, Any]) -> Resolved:
        """
        Resolve every index of the input for an iterated task; see :class:`Resolved`.

        Implementations must not make a blocking supervisor call on the loop thread: XComArg
        sources are pulled with ``XComArg.aresolve`` and read through :class:`Source`.
        """
        raise NotImplementedError()

    def resolve(self, context: Mapping[str, Any]) -> Any:
        raise NotImplementedError()


@attrs.define(slots=False)
class DecoratedExpandInput(ExpandInput):
    """The expand input of a decorated task, whose items arrive as ``op_kwargs``."""

    EXPAND_INPUT_TYPE: ClassVar[str] = "decorated"

    delegate: ExpandInput

    @property
    def value(self) -> Any:
        return self.delegate.value

    def iter_references(self) -> Iterable[tuple[Operator, str]]:
        return self.delegate.iter_references()

    async def aresolve(self, context: Mapping[str, Any]) -> Resolved:
        length, aget = await self.delegate.aresolve(context)

        async def aget_op_kwargs(index: int) -> Mapping[str, Any]:
            return {"op_kwargs": await aget(index)}

        return Resolved(length, aget_op_kwargs)

    def resolve(self, context: Mapping[str, Any]) -> tuple[Mapping[str, Any], set[int]]:
        return self.delegate.resolve(context)


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
class DictOfListsExpandInput(ExpandInput):
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
        # Use the original user input to retain argument order.
        found_index = index_for_each_field(map_index, {k: all_lengths[k] for k in self.value})[key]
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

    async def aresolve(self, context: Mapping[str, Any]) -> Resolved:
        sources = {key: await Source.from_argument(value, context) for key, value in self.value.items()}
        lengths = {key: await source.alen() for key, source in sources.items()}

        async def aget(index: int) -> Mapping[str, Any]:
            positions = index_for_each_field(index, lengths)
            return {key: await source.aget(positions[key]) for key, source in sources.items()}

        return Resolved(math.prod(lengths.values()), aget)

    def resolve(self, context: Mapping[str, Any]) -> tuple[Mapping[str, Any], set[int]]:
        map_index: int | None = context["ti"].map_index
        if map_index is None or map_index < 0:
            raise RuntimeError("can't resolve task-mapping argument without expanding")

        # Get pre-computed upstream_map_indexes if available, otherwise default to empty dict.
        # When empty, individual XComArgs will compute their map_indexes lazily in xcom_arg.py.
        upstream_map_indexes = getattr(context["ti"], "_upstream_map_indexes", None) or {}

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
class ListOfDictsExpandInput(ExpandInput):
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

    async def aresolve(self, context: Mapping[str, Any]) -> Resolved:
        if isinstance(self.value, XComArg):
            source = await Source.from_argument(self.value, context)
        else:
            source = Source(
                [await item.aresolve(context) if isinstance(item, XComArg) else item for item in self.value]
            )

        async def aget(index: int) -> Mapping[str, Any]:
            mapping = await source.aget(index)
            if not isinstance(mapping, Mapping):
                raise ValueError(
                    f"iterate_kwargs() expects a list[dict], not list[{_describe_type(mapping)}]"
                )
            return mapping

        return Resolved(await source.alen(), aget)

    def resolve(self, context: Mapping[str, Any]) -> tuple[Mapping[str, Any], set[int]]:
        map_index = context["ti"].map_index
        if map_index is None or map_index < 0:
            raise RuntimeError("can't resolve task-mapping argument without expanding")

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
