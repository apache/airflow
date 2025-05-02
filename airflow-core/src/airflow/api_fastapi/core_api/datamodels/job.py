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

from airflow.api_fastapi.core_api.base import BaseModel


class JobResponse(BaseModel):
    """Job serializer for responses."""

    id: int
    dag_id: str | None
    state: str | None
    job_type: str | None
    start_date: datetime | None
    end_date: datetime | None
    latest_heartbeat: datetime | None
    executor_class: str | None
    hostname: str | None
    unixname: str | None


class JobCollectionResponse(BaseModel):
    """Job Collection Response."""

    jobs: list[JobResponse]
    total_entries: int
