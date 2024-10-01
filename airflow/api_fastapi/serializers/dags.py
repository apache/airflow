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

from collections import abc
from datetime import datetime, timedelta
from typing import Any, Iterable

from itsdangerous import URLSafeSerializer
from pendulum.tz.timezone import FixedTimezone, Timezone
from pydantic import (
    BaseModel,
    computed_field,
    field_validator,
)

from airflow.configuration import conf
from airflow.serialization.pydantic.dag import DagTagPydantic


class DAGResponse(BaseModel):
    """DAG serializer for responses."""

    dag_id: str
    dag_display_name: str
    is_paused: bool
    is_active: bool
    last_parsed_time: datetime | None
    last_pickled: datetime | None
    last_expired: datetime | None
    scheduler_lock: datetime | None
    pickle_id: datetime | None
    default_view: str | None
    fileloc: str
    description: str | None
    timetable_summary: str | None
    timetable_description: str | None
    tags: list[DagTagPydantic]
    max_active_tasks: int
    max_active_runs: int | None
    max_consecutive_failed_dag_runs: int
    has_task_concurrency_limits: bool
    has_import_errors: bool
    next_dagrun: datetime | None
    next_dagrun_data_interval_start: datetime | None
    next_dagrun_data_interval_end: datetime | None
    next_dagrun_create_after: datetime | None
    owners: list[str]

    @field_validator("owners", mode="before")
    @classmethod
    def get_owners(cls, v: Any) -> list[str] | None:
        """Convert owners attribute to DAG representation."""
        if not (v is None or isinstance(v, str)):
            return v

        if v is None:
            return []
        elif isinstance(v, str):
            return v.split(",")
        return v

    # Mypy issue https://github.com/python/mypy/issues/1362
    @computed_field  # type: ignore[misc]
    @property
    def file_token(self) -> str:
        """Return file token."""
        serializer = URLSafeSerializer(conf.get_mandatory_value("webserver", "secret_key"))
        return serializer.dumps(self.fileloc)


class DAGPatchBody(BaseModel):
    """Dag Serializer for updatable body."""

    is_paused: bool


class DAGCollectionResponse(BaseModel):
    """DAG Collection serializer for responses."""

    dags: list[DAGResponse]
    total_entries: int


class DAGDetailsResponse(DAGResponse):
    """Specific serializer for DAG Details responses."""

    catchup: bool
    dagrun_timeout: timedelta | None
    dataset_expression: dict | None
    doc_md: str | None
    start_date: datetime | None
    end_date: datetime | None
    is_paused_upon_creation: bool | None
    orientation: str
    params: Any | None
    render_template_as_native_obj: bool
    template_searchpath: str | Iterable[str] | None
    timezone: str | None
    last_loaded: datetime | None

    @field_validator("timezone", mode="before")
    @classmethod
    def get_timezone(cls, tz: Timezone | FixedTimezone) -> str | None:
        """Convert timezone attribute to string representation."""
        if tz is None:
            return None
        return str(tz)

    @field_validator("timetable_summary", mode="before")
    @classmethod
    def get_timetable_summary(cls, tts: str | None) -> str | None:
        """Validate the string representation of timetable_summary."""
        if tts is None or tts == "None":
            return None
        return str(tts)

    @field_validator("params", mode="before")
    @classmethod
    def get_params(cls, params: abc.MutableMapping | None) -> dict | None:
        """Convert params attribute to dict representation."""
        if params is None:
            return None
        return {param_name: param_val.serialize() for param_name, param_val in params.items()}

    # Mypy issue https://github.com/python/mypy/issues/1362
    @computed_field  # type: ignore[misc]
    @property
    def concurrency(self) -> int:
        """Return max_active_tasks as concurrency."""
        return self.max_active_tasks

    # Mypy issue https://github.com/python/mypy/issues/1362
    @computed_field  # type: ignore[misc]
    @property
    def last_parsed(self) -> datetime | None:
        """Return last_loaded as last_parsed."""
        return self.last_loaded
