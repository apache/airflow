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
from collections.abc import Iterable, Sized, Mapping, Generator
from typing import TYPE_CHECKING, Any, ClassVar, Union

import attrs
from airflow.utils.log.logging_mixin import LoggingMixin

from airflow.models.taskmap import update_task_map_length

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.models.xcom_arg import SchedulerXComArg
    from airflow.typing_compat import TypeGuard

from airflow.sdk.definitions._internal.expandinput import (
    DictOfListsExpandInput,
    ListOfDictsExpandInput,
    MappedArgument,
    NotFullyPopulated,
    OperatorExpandArgument,
    OperatorExpandKwargsArgument,
    is_mappable,
)

__all__ = [
    "DictOfListsExpandInput",
    "ListOfDictsExpandInput",
    "MappedArgument",
    "NotFullyPopulated",
    "OperatorExpandArgument",
    "OperatorExpandKwargsArgument",
    "is_mappable",
]


def _needs_run_time_resolution(v: OperatorExpandArgument) -> TypeGuard[MappedArgument | SchedulerXComArg]:
    from airflow.models.xcom_arg import SchedulerXComArg

    return isinstance(v, (MappedArgument, SchedulerXComArg))


class ExpandInput(LoggingMixin):
    def resolve(self, context: Mapping[str, Any], session: Session):
        raise NotImplementedError()


@attrs.define
class SchedulerDictOfListsExpandInput(ExpandInput):
    value: dict

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
        from airflow.models.xcom_arg import SchedulerXComArg, get_task_map_length

        # TODO: This initiates one database call for each XComArg. Would it be
        # more efficient to do one single db call and unpack the value here?
        def _get_length(v: OperatorExpandArgument) -> int | None:
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

    def resolve(self, context: Mapping[str, Any], session: Session) -> Generator[
        dict[Any, str | Any] | dict[Any, Any], None, list[Any]]:

        self.log.info("expand_dict: %s", self.value)

        value = self.value.resolve(context, session) if isinstance(self.value, SchedulerXComArg) else self.value

        self.log.info("resolved value: %s", value)

        for key, item in value.items():
            result = item.resolve(context, session)

            self.log.info("resolved value %s: %s", key, result)

            for index, sub_item in enumerate(result):
                yield {key: sub_item}

                update_task_map_length(index, item, context["run_id"], session)

        return []


@attrs.define
class SchedulerListOfDictsExpandInput(ExpandInput):
    value: list

    EXPAND_INPUT_TYPE: ClassVar[str] = "list-of-dicts"

    def get_parse_time_mapped_ti_count(self) -> int:
        if isinstance(self.value, Sized):
            return len(self.value)
        raise NotFullyPopulated({"expand_kwargs() argument"})

    def get_total_map_length(self, run_id: str, *, session: Session) -> int:
        from airflow.models.xcom_arg import get_task_map_length

        if isinstance(self.value, Sized):
            return len(self.value)
        length = get_task_map_length(self.value, run_id, session=session)
        if length is None:
            raise NotFullyPopulated({"expand_kwargs() argument"})
        return length

    def resolve(self, context: Mapping[str, Any], session: Session) -> Generator[
        dict[Any, str | Any] | dict[Any, Any], None, list[Any]]:

        self.log.info("expand_list: %s", self.value)

        value = self.value.resolve(context, session) if isinstance(self.value, SchedulerXComArg) else self.value

        self.log.info("resolved value: %s", value)

        for index, entry in enumerate(value):
            self.log.info("entry: %s", entry)

            for key, item in entry.items():
                if isinstance(item, SchedulerXComArg):
                    entry[key] = item.resolve(context, session)

                self.log.info("resolved entry: %s", entry)

            yield entry

            update_task_map_length(index, self.value, context["run_id"], session)

        return []


_EXPAND_INPUT_TYPES: dict[str, type[SchedulerExpandInput]] = {
    "dict-of-lists": SchedulerDictOfListsExpandInput,
    "list-of-dicts": SchedulerListOfDictsExpandInput,
}

SchedulerExpandInput = Union[SchedulerDictOfListsExpandInput, SchedulerListOfDictsExpandInput]


def create_expand_input(kind: str, value: Any) -> SchedulerExpandInput:
    return _EXPAND_INPUT_TYPES[kind](value)
