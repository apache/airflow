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

import inspect
from collections import abc
from datetime import datetime, timedelta
from typing import Any

from itsdangerous import URLSafeSerializer
from pendulum.tz.timezone import FixedTimezone, Timezone
from pydantic import (
    AliasGenerator,
    ConfigDict,
    computed_field,
    field_validator,
)

from airflow.api_fastapi.core_api.base import BaseModel, StrictBaseModel
from airflow.api_fastapi.core_api.datamodels.dag_tags import DagTagResponse
from airflow.api_fastapi.core_api.datamodels.dag_versions import DagVersionResponse
from airflow.configuration import conf
from airflow.models.dag_version import DagVersion

DAG_ALIAS_MAPPING: dict[str, str] = {
    # The keys are the names in the response, the values are the original names in the model
    # This is used to map the names in the response to the names in the model
    # See: https://github.com/apache/airflow/issues/46732
    "next_dagrun_logical_date": "next_dagrun",
    "next_dagrun_run_after": "next_dagrun_create_after",
}


class DAGResponse(BaseModel):
    """DAG serializer for responses."""

    model_config = ConfigDict(
        alias_generator=AliasGenerator(
            validation_alias=lambda field_name: DAG_ALIAS_MAPPING.get(field_name, field_name),
        ),
    )

    dag_id: str
    dag_display_name: str
    is_paused: bool
    is_stale: bool
    last_parsed_time: datetime | None
    last_parse_duration: float | None
    last_expired: datetime | None
    bundle_name: str | None
    bundle_version: str | None
    relative_fileloc: str | None
    fileloc: str
    description: str | None
    timetable_summary: str | None
    timetable_description: str | None
    tags: list[DagTagResponse]
    max_active_tasks: int
    max_active_runs: int | None
    max_consecutive_failed_dag_runs: int
    has_task_concurrency_limits: bool
    has_import_errors: bool
    next_dagrun_logical_date: datetime | None
    next_dagrun_data_interval_start: datetime | None
    next_dagrun_data_interval_end: datetime | None
    next_dagrun_run_after: datetime | None
    owners: list[str]

    @field_validator("owners", mode="before")
    @classmethod
    def get_owners(cls, v: Any) -> list[str] | None:
        """Convert owners attribute to DAG representation."""
        if not (v is None or isinstance(v, str)):
            return v

        if v is None:
            return []
        if isinstance(v, str):
            return [x.strip() for x in v.split(",")]
        return v

    @field_validator("timetable_summary", mode="before")
    @classmethod
    def get_timetable_summary(cls, tts: str | None) -> str | None:
        """Validate the string representation of timetable_summary."""
        if tts is None or tts == "None":
            return None
        return str(tts)

    # Mypy issue https://github.com/python/mypy/issues/1362
    @computed_field  # type: ignore[prop-decorator]
    @property
    def file_token(self) -> str:
        """Return file token."""
        serializer = URLSafeSerializer(conf.get_mandatory_value("api", "secret_key"))
        payload = {
            "bundle_name": self.bundle_name,
            "relative_fileloc": self.relative_fileloc,
        }
        return serializer.dumps(payload)


class DAGPatchBody(StrictBaseModel):
    """Dag Serializer for updatable bodies."""

    is_paused: bool


class DAGCollectionResponse(BaseModel):
    """DAG Collection serializer for responses."""

    dags: list[DAGResponse]
    total_entries: int


class DAGDetailsResponse(DAGResponse):
    """Specific serializer for DAG Details responses."""

    model_config = ConfigDict(
        from_attributes=True,
        alias_generator=AliasGenerator(
            validation_alias=lambda field_name: {
                "dag_run_timeout": "dagrun_timeout",
                "last_parsed": "last_loaded",
                "template_search_path": "template_searchpath",
                **DAG_ALIAS_MAPPING,
            }.get(field_name, field_name),
        ),
    )

    catchup: bool
    dag_run_timeout: timedelta | None
    asset_expression: dict | None
    doc_md: str | None
    start_date: datetime | None
    end_date: datetime | None
    is_paused_upon_creation: bool | None
    params: abc.Mapping | None
    render_template_as_native_obj: bool
    template_search_path: list[str] | None
    timezone: str | None
    last_parsed: datetime | None
    default_args: abc.Mapping | None
    owner_links: dict[str, str] | None = None
    is_favorite: bool = False

    @field_validator("timezone", mode="before")
    @classmethod
    def get_timezone(cls, tz: Timezone | FixedTimezone) -> str | None:
        """Convert timezone attribute to string representation."""
        if tz is None:
            return None
        return str(tz)

    @field_validator("doc_md", mode="before")
    @classmethod
    def get_doc_md(cls, doc_md: str | None) -> str | None:
        """Clean indentation in doc md."""
        if doc_md is None:
            return None
        return inspect.cleandoc(doc_md)

    @field_validator("params", mode="before")
    @classmethod
    def get_params(cls, params: abc.Mapping | None) -> dict | None:
        """Convert params attribute to dict representation."""
        if params is None:
            return None
        return {k: v.dump() for k, v in params.items()}

    # Mypy issue https://github.com/python/mypy/issues/1362
    @computed_field(deprecated=True)  # type: ignore[prop-decorator]
    @property
    def concurrency(self) -> int:
        """
        Return max_active_tasks as concurrency.

        Deprecated: Use max_active_tasks instead.
        """
        return self.max_active_tasks

    # Mypy issue https://github.com/python/mypy/issues/1362
    @computed_field  # type: ignore[prop-decorator]
    @property
    def latest_dag_version(self) -> DagVersionResponse | None:
        """Return the latest DagVersion."""
        latest_dag_version = DagVersion.get_latest_version(self.dag_id, load_dag_model=True)
        if latest_dag_version is None:
            return latest_dag_version
        return DagVersionResponse.model_validate(latest_dag_version)
