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

from pydantic import BaseModel

from airflow.models.backfill import ReprocessBehavior


class BackfillPostBody(BaseModel):
    """Object used for create backfill request."""

    dag_id: str
    from_date: datetime
    to_date: datetime
    run_backwards: bool = False
    dag_run_conf: dict = {}
    reprocess_behavior: ReprocessBehavior = ReprocessBehavior.NONE
    max_active_runs: int = 10


class BackfillResponse(BaseModel):
    """Base serializer for Backfill."""

    id: int
    dag_id: str
    from_date: datetime
    to_date: datetime
    dag_run_conf: dict
    is_paused: bool
    reprocess_behavior: ReprocessBehavior
    max_active_runs: int
    created_at: datetime
    completed_at: datetime | None
    updated_at: datetime


class BackfillCollectionResponse(BaseModel):
    """Backfill Collection serializer for responses."""

    backfills: list[BackfillResponse]
    total_entries: int
