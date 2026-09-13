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
from typing import Literal

from airflow.api_fastapi.core_api.base import BaseModel
from airflow.utils.state import DagRunState


class TimeScheduleItem(BaseModel):
    """An aggregated bar in the Time Schedule UI."""

    dag_id: str
    dag_run_id: str
    duration_ms: float
    end_date: datetime | None
    is_placeholder: bool
    is_planned: bool
    is_time_scheduled: bool
    label: str
    run_count: int
    start_date: datetime | None
    state: DagRunState | Literal["placeholder", "planned"]


class TimeScheduleBatch(BaseModel):
    """A progressively streamed batch of Time Schedule bars."""

    dag_run_count: int
    items: list[TimeScheduleItem]
