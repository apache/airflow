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

import enum
from typing import TYPE_CHECKING

from airflow.typing_compat import TypedDict

if TYPE_CHECKING:
    from datetime import datetime


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


NOTSET = ArgNotSet()
"""Sentinel value for argument default. See ``ArgNotSet``."""


class AttributeRemoved:
    """
    Sentinel type to signal when attribute removed on serialization.

    :meta private:
    """

    def __init__(self, attribute_name: str):
        self.attribute_name = attribute_name

    def __getattr__(self, item):
        if item == "attribute_name":
            return super().__getattribute__(item)
        raise RuntimeError(
            f"Attribute {self.attribute_name} was removed on "
            f"serialization and must be set again - found when accessing {item}."
        )


"""
Sentinel value for attributes removed on serialization.

:meta private:
"""


class DagRunType(str, enum.Enum):
    """Class with DagRun types."""

    BACKFILL_JOB = "backfill"
    SCHEDULED = "scheduled"
    MANUAL = "manual"
    DATASET_TRIGGERED = "dataset_triggered"

    def __str__(self) -> str:
        return self.value

    def generate_run_id(self, logical_date: datetime) -> str:
        return f"{self}__{logical_date.isoformat()}"

    @staticmethod
    def from_run_id(run_id: str) -> DagRunType:
        """Resolve DagRun type from run_id."""
        for run_type in DagRunType:
            if run_id and run_id.startswith(f"{run_type.value}__"):
                return run_type
        return DagRunType.MANUAL


class EdgeInfoType(TypedDict):
    """Extra metadata that the DAG can store about an edge, usually generated from an EdgeModifier."""

    label: str | None
