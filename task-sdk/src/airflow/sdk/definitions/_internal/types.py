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

from typing import TYPE_CHECKING, Any, TypeVar

if TYPE_CHECKING:
    from typing_extensions import TypeIs

    from airflow.sdk.definitions._internal.node import DAGNode

    T = TypeVar("T")

__all__ = [
    "NOTSET",
    "SET_DURING_EXECUTION",
    "ArgNotSet",
    "SetDuringExecution",
    "is_arg_set",
    "validate_instance_args",
]

try:
    # If core and SDK exist together, use core to avoid identity issues.
    from airflow.serialization.definitions.notset import NOTSET, ArgNotSet
except ModuleNotFoundError:

    class ArgNotSet:  # type: ignore[no-redef]
        """Sentinel type for annotations, useful when None is not viable."""

    NOTSET = ArgNotSet()  # type: ignore[no-redef]


def is_arg_set(value: T | ArgNotSet) -> TypeIs[T]:
    return not isinstance(value, ArgNotSet)


class SetDuringExecution(ArgNotSet):
    """Sentinel type for annotations, useful when a value is dynamic and set during Execution but not parsing."""

    @staticmethod
    def serialize() -> str:
        return "DYNAMIC (set during execution)"


SET_DURING_EXECUTION = SetDuringExecution()
"""Sentinel value for argument default. See ``SetDuringExecution``."""


def validate_instance_args(instance: DAGNode, expected_arg_types: dict[str, Any]) -> None:
    """Validate that the instance has the expected types for the arguments."""
    from airflow.sdk.definitions.taskgroup import TaskGroup

    typ = "task group" if isinstance(instance, TaskGroup) else "task"

    for arg_name, expected_arg_type in expected_arg_types.items():
        instance_arg_value = getattr(instance, arg_name, None)
        if instance_arg_value is not None and not isinstance(instance_arg_value, expected_arg_type):
            raise TypeError(
                f"{arg_name!r} for {typ} {instance.node_id!r} expects {expected_arg_type}, got {type(instance_arg_value)} with value "
                f"{instance_arg_value!r}"
            )
