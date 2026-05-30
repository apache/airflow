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


class TaskCircuitBreakerResponse(BaseModel):
    """Serializer for TaskCircuitBreaker responses."""

    dag_id: str
    task_id: str
    is_open: bool
    opened_at: datetime | None
    opened_reason: str | None
    failure_count: int
    window_start: datetime | None
    reset_after: datetime | None
    max_failures: int
    window_seconds: int


class TaskCircuitBreakerCollectionResponse(BaseModel):
    """Collection of TaskCircuitBreaker responses."""

    circuit_breakers: list[TaskCircuitBreakerResponse]
    total_entries: int
