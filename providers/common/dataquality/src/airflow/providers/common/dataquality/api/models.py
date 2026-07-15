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

from typing import Any, Generic, TypeVar

from pydantic import BaseModel

T = TypeVar("T")


class RuleResultModel(BaseModel):
    """Mirrors ``airflow.providers.common.dataquality.results.RuleResult.to_dict()``."""

    rule_uid: str
    rule_name: str
    status: str
    observed_value: float | str | None = None
    condition: dict[str, Any] = {}
    dimension: str = "validity"
    severity: str = "error"
    duration_ms: float | None = None
    error_message: str | None = None
    description: str | None = None
    sql: str | None = None


class DQRunModel(BaseModel):
    """Mirrors ``airflow.providers.common.dataquality.results.DQRun.to_dict()`` -- the full run header."""

    dag_id: str
    task_id: str
    run_id: str
    try_number: int = 1
    map_index: int = -1
    run_uid: str
    ruleset_name: str | None = None
    table_ref: str | None = None
    asset_names: list[str] = []
    started_at: str | None = None
    finished_at: str | None = None


class RunContextModel(BaseModel):
    """Mirrors ``ObjectStorageResultsBackend._build_run_context()``."""

    run_uid: str
    dag_id: str
    task_id: str
    run_id: str
    map_index: int = -1
    started_at: str | None = None
    table_ref: str | None = None


class DQSummaryModel(BaseModel):
    """Compact summary fields returned by the Data Quality UI API."""

    errored: int
    failed: int
    passed: int
    score: float | None
    warned: int


class TaskDQRunModel(BaseModel):
    """One run, as returned by ``task_runs`` items and ``run_detail_by_task_instance``."""

    run: DQRunModel
    results: list[RuleResultModel]
    summary: DQSummaryModel


class RuleHistoryRecordModel(RuleResultModel):
    """A rule result plus the run context that produced it, as returned by ``task_rule_history``."""

    run: RunContextModel


class PaginatedResponse(BaseModel, Generic[T]):
    """``{"items": [...], "next_cursor": ...}``, shared by every cursor-paginated route."""

    items: list[T]
    next_cursor: str | None = None
