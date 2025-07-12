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

from pydantic import BaseModel

from airflow.utils.state import DagRunState


class CalendarTimeRangeResponse(BaseModel):
    """Represents a summary of DAG runs for a specific calendar time range."""

    date: datetime
    state: Literal[
        DagRunState.QUEUED,
        DagRunState.RUNNING,
        DagRunState.SUCCESS,
        DagRunState.FAILED,
        "planned",
    ]
    count: int


class CalendarTimeRangeCollectionResponse(BaseModel):
    """Response model for calendar time range results."""

    total_entries: int
    dag_runs: list[CalendarTimeRangeResponse]
