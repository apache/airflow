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

from typing import TYPE_CHECKING, Any


class ArgNotSet:
    """
    Sentinel type for annotations, useful when None is not viable.

    Use like this::

        def is_arg_passed(arg: Union[ArgNotSet, None] = NOTSET) -> bool:
            if arg is NOTSET:
                return False
            return True


        is_arg_passed()  # False.
        is_arg_passed(None)  # True.
    """

    @staticmethod
    def serialize():
        return "NOTSET"

    @classmethod
    def deserialize(cls):
        return cls


NOTSET = ArgNotSet()
"""Sentinel value for argument default. See ``ArgNotSet``."""


class SetDuringExecution(ArgNotSet):
    """Sentinel type for annotations, useful when a value is dynamic and set during Execution but not parsing."""

    @staticmethod
    def serialize() -> str:
        return "DYNAMIC (set during execution)"


SET_DURING_EXECUTION = SetDuringExecution()
"""Sentinel value for argument default. See ``SetDuringExecution``."""


if TYPE_CHECKING:
    from airflow.sdk.definitions._internal.node import DAGNode


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
