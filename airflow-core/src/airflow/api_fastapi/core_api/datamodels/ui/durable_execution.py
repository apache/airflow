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

from pydantic import JsonValue

from airflow.api_fastapi.core_api.base import BaseModel
from airflow.utils.state import TaskInstanceState


class DurableTryResponse(BaseModel):
    """What one try did with the external job."""

    try_number: int | None
    decision: Literal["submitted", "resubmitted", "reconnected", "already_succeeded"]
    external_id: JsonValue
    status: TaskInstanceState | None
    submitted_at: datetime | None = None
    saved_seconds: float | None
    previous_external_id: JsonValue = None


class DurableExecutionResponse(BaseModel):
    """Durable execution history of a task instance, read by the logs banner."""

    tries: list[DurableTryResponse]
