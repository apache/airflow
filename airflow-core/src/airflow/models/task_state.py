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

from datetime import datetime

from sqlalchemy import ForeignKeyConstraint, Index, Integer, PrimaryKeyConstraint, String, Text
from sqlalchemy.dialects.mysql import MEDIUMTEXT
from sqlalchemy.orm import Mapped, mapped_column

from airflow._shared.timezones import timezone
from airflow.models.base import COLLATION_ARGS, Base, StringID
from airflow.utils.sqlalchemy import UtcDateTime


class TaskStateModel(Base):
    """
    Persists key/value state for a task within a single DAG run.

    Scoped to (dag_run_id, task_id, map_index). Retries of the same task share
    the same rows — that is the point. Different DAG runs have different dag_run_id
    values so they get independent namespaces automatically.
    """

    __tablename__ = "task_state"

    dag_run_id: Mapped[int] = mapped_column(Integer, nullable=False, primary_key=True)
    task_id: Mapped[str] = mapped_column(StringID(), nullable=False, primary_key=True)
    map_index: Mapped[int] = mapped_column(Integer, primary_key=True, nullable=False, server_default="-1")
    key: Mapped[str] = mapped_column(String(512, **COLLATION_ARGS), nullable=False, primary_key=True)

    dag_id: Mapped[str] = mapped_column(StringID(), nullable=False)
    run_id: Mapped[str] = mapped_column(StringID(), nullable=False)

    value: Mapped[str] = mapped_column(Text().with_variant(MEDIUMTEXT, "mysql"), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(UtcDateTime, default=timezone.utcnow, nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint("dag_run_id", "task_id", "map_index", "key", name="task_state_pkey"),
        ForeignKeyConstraint(
            ["dag_run_id"],
            ["dag_run.id"],
            name="task_state_dag_run_fkey",
            ondelete="CASCADE",
        ),
        Index("idx_task_state_lookup", "dag_id", "run_id", "task_id", "map_index"),
    )
