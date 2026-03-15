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
from typing import TYPE_CHECKING

from sqlalchemy import Float, Index
from sqlalchemy.orm import Mapped, mapped_column

from airflow.models.base import Base, StringID
from airflow.utils.sqlalchemy import UtcDateTime

if TYPE_CHECKING:
    from sqlalchemy.dialects.postgresql import UUID


class FlowRateMetric(Base):
    """
    Persisted FlowRate metrics for task runs.
    """

    __tablename__ = "flowrate_metric"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    dag_id: Mapped[str] = mapped_column(StringID(), nullable=False)
    run_id: Mapped[str] = mapped_column(StringID(), nullable=False)
    task_id: Mapped[str] = mapped_column(StringID(), nullable=False)

    start_date: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)
    end_date: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)

    cpu_request: Mapped[float | None] = mapped_column(Float, nullable=True)
    memory_request: Mapped[float | None] = mapped_column(Float, nullable=True)
    estimated_cost: Mapped[float | None] = mapped_column(Float, nullable=True)

    __table_args__ = (
        Index("ix_flowrate_metric_dag_run_task", "dag_id", "run_id", "task_id"),
    )

