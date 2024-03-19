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
"""Table to store information about mapped task instances (AIP-42)."""

from __future__ import annotations

import collections.abc
import enum
from typing import TYPE_CHECKING, Any, Collection

from sqlalchemy import CheckConstraint, Column, ForeignKeyConstraint, Integer, String

from airflow.models.base import COLLATION_ARGS, ID_LEN, TaskInstanceDependencies
from airflow.utils.sqlalchemy import ExtendedJSON

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstance
    from airflow.serialization.pydantic.taskinstance import TaskInstancePydantic


class TaskMapVariant(enum.Enum):
    """Task map variant.

    Possible values are **dict** (for a key-value mapping) and **list** (for an
    ordered value sequence).
    """

    DICT = "dict"
    LIST = "list"


class TaskMap(TaskInstanceDependencies):
    """Model to track dynamic task-mapping information.

    This is currently only populated by an upstream TaskInstance pushing an
    XCom that's pulled by a downstream for mapping purposes.
    """

    __tablename__ = "task_map"

    # Link to upstream TaskInstance creating this dynamic mapping information.
    dag_id = Column(String(ID_LEN, **COLLATION_ARGS), primary_key=True)
    task_id = Column(String(ID_LEN, **COLLATION_ARGS), primary_key=True)
    run_id = Column(String(ID_LEN, **COLLATION_ARGS), primary_key=True)
    map_index = Column(Integer, primary_key=True)

    length = Column(Integer, nullable=False)
    keys = Column(ExtendedJSON, nullable=True)

    __table_args__ = (
        CheckConstraint(length >= 0, name="task_map_length_not_negative"),
        ForeignKeyConstraint(
            [dag_id, task_id, run_id, map_index],
            [
                "task_instance.dag_id",
                "task_instance.task_id",
                "task_instance.run_id",
                "task_instance.map_index",
            ],
            name="task_map_task_instance_fkey",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
    )

    def __init__(
        self,
        dag_id: str,
        task_id: str,
        run_id: str,
        map_index: int,
        length: int,
        keys: list[Any] | None,
    ) -> None:
        self.dag_id = dag_id
        self.task_id = task_id
        self.run_id = run_id
        self.map_index = map_index
        self.length = length
        self.keys = keys

    @classmethod
    def from_task_instance_xcom(cls, ti: TaskInstance | TaskInstancePydantic, value: Collection) -> TaskMap:
        if ti.run_id is None:
            raise ValueError("cannot record task map for unrun task instance")
        return cls(
            dag_id=ti.dag_id,
            task_id=ti.task_id,
            run_id=ti.run_id,
            map_index=ti.map_index,
            length=len(value),
            keys=(list(value) if isinstance(value, collections.abc.Mapping) else None),
        )

    @property
    def variant(self) -> TaskMapVariant:
        if self.keys is None:
            return TaskMapVariant.LIST
        return TaskMapVariant.DICT
