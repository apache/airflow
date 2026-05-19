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

from airflow.api_fastapi.core_api.base import BaseModel, StrictBaseModel


class TaskStateResponse(BaseModel):
    """A single task state key/value pair with metadata."""

    key: str
    value: str
    updated_at: datetime
    expires_at: datetime | None


class TaskStateCollectionResponse(BaseModel):
    """All task state entries for a task instance."""

    task_states: list[TaskStateResponse]
    total_entries: int


class TaskStateBody(StrictBaseModel):
    """Request body for setting a task state value."""

    value: str
