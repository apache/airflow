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

import collections.abc
import enum
from typing import TYPE_CHECKING, Any, Collection, List, Optional

from sqlalchemy import Column, ForeignKeyConstraint, Integer, String

from airflow.models.base import COLLATION_ARGS, ID_LEN, Base
from airflow.utils.sqlalchemy import ExtendedJSON

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstance


class TaskMapVariant(enum.Enum):
    """Task map variant.
    Possible values are **dict** (for a key-value mapping) and **list** (for an
    ordered value sequence).
    """

    DICT = "dict"
    LIST = "list"


class TaskTry(Base):
    """Model to track individual TaskInstance tries.
    This is currently only used for storing appropriate hostnames.
    """

    __tablename__ = "task_try"

    # Link to upstream TaskInstance creating this dynamic mapping information.
    dag_id = Column(String(ID_LEN, **COLLATION_ARGS), primary_key=True)
    task_id = Column(String(ID_LEN, **COLLATION_ARGS), primary_key=True)
    run_id = Column(String(ID_LEN, **COLLATION_ARGS), primary_key=True)
    try_number = Column(Integer, primary_key=True)

    hostname = Column(String(1000), nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(
            [dag_id, task_id, run_id],
            [
                "task_instance.dag_id",
                "task_instance.task_id",
                "task_instance.run_id",
            ],
            name="task_try_task_instance_fkey",
            ondelete="CASCADE",
        ),
    )

    def __init__(
        self,
        dag_id: str,
        task_id: str,
        run_id: str,
        try_number: int,
        hostname: str,
    ) -> None:
        self.dag_id = dag_id
        self.task_id = task_id
        self.run_id = run_id
        self.try_number = try_number
        self.hostname = hostname
