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

from collections.abc import Iterable
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any

from pydantic import AliasPath, AwareDatetime, Field, NonNegativeInt, model_validator

from airflow._shared.timezones import timezone
from airflow.api_fastapi.core_api.base import BaseModel, StrictBaseModel
from airflow.api_fastapi.core_api.datamodels.dag_versions import DagVersionResponse
from airflow.timetables.base import DataInterval
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunTriggeredByType, DagRunType

if TYPE_CHECKING:
    from airflow.serialization.definitions.dag import SerializedDAG


class DagRunMutableStates(str, Enum):
    """Dag Run states from which the run may be mutated (patched, deleted)."""

    QUEUED = DagRunState.QUEUED
    SUCCESS = DagRunState.SUCCESS
    FAILED = DagRunState.FAILED


class DAGRunPatchBody(StrictBaseModel):
    """Dag Run Serializer for PATCH requests."""

    state: DagRunMutableStates | None = None
    note: str | None = Field(None, max_length=1000)


class BulkDAGRunBody(StrictBaseModel):
    """Request body for bulk operations on Dag Runs."""

    dag_run_id: str
    dag_id: str | None = None
    state: DagRunMutableStates | None = None
    note: str | None = Field(None, max_length=1000)


class PartitionSelectorMixin(StrictBaseModel):
    """Partition filter fields shared by bulk-clear and clearPartitions bodies."""

    partition_key: str | None = Field(
        default=None,
        description="Select runs by exact partition key match. Mutually exclusive with the other partition selectors.",
    )
    partition_date_start: datetime | None = Field(
        default=None,
        description=(
            "Inclusive start of the partition date window. "
            "The value is interpreted in the Dag's timetable timezone. "
            "Mutually exclusive with the other partition selectors."
        ),
    )
    partition_date_end: datetime | None = Field(
        default=None,
        description=(
            "Inclusive end of the partition date window. "
            "The value is interpreted in the Dag's timetable timezone. "
            "Mutually exclusive with the other partition selectors."
        ),
    )

    @property
    def has_partition_selectors(self) -> bool:
        return (
            self.partition_key is not None
            or self.partition_date_start is not None
            or self.partition_date_end is not None
        )

    def _validate_partition_date_window_order(self) -> None:
        if (
            self.partition_date_start is not None
            and self.partition_date_end is not None
            and self.partition_date_start > self.partition_date_end
        ):
            raise ValueError("partition_date_start must be on or before partition_date_end.")

    def _check_exactly_one_selection_mode(
        self, *, extra_selector_active: bool, extra_selector_name: str
    ) -> None:
        has_partition_key = self.partition_key is not None
        has_partition_date_window = (
            self.partition_date_start is not None or self.partition_date_end is not None
        )
        modes_active = sum([extra_selector_active, has_partition_key, has_partition_date_window])
        if modes_active != 1:
            raise ValueError(
                f"Exactly one of {extra_selector_name}, partition_key, or a partition date window "
                "(partition_date_start / partition_date_end) must be provided."
            )
        self._validate_partition_date_window_order()


class BaseDAGRunClear(StrictBaseModel):
    """Shared options for the single-run and bulk Dag Run clear endpoints."""

    dry_run: bool = True
    only_failed: bool = False
    only_new: bool = Field(
        default=False,
        description="Only queue newly added tasks in the latest Dag version without clearing existing tasks.",
    )
    run_on_latest_version: bool | None = Field(
        default=None,
        description="(Experimental) Run on the latest bundle version of the Dag after clearing. "
        "If not specified, falls back to the DAG-level ``rerun_with_latest_version`` parameter, "
        "then the ``[core] rerun_with_latest_version`` config option, "
        "and finally ``False``.",
    )
    note: str | None = Field(default=None, max_length=1000)

    @model_validator(mode="before")
    @classmethod
    def validate_only_new_only_failed_mutually_exclusive(cls, data: Any) -> Any:
        if data.get("only_new") and data.get("only_failed"):
            raise ValueError("only_new and only_failed are mutually exclusive")
        return data


class DAGRunClearBody(BaseDAGRunClear):
    """Dag Run serializer for clear endpoint body."""


class BulkDAGRunClearBody(BaseDAGRunClear, PartitionSelectorMixin):
    """Request body for the bulk clear Dag Runs endpoint."""

    dag_runs: list[BulkDAGRunBody] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_exactly_one_selection_mode(self) -> BulkDAGRunClearBody:
        self._check_exactly_one_selection_mode(
            extra_selector_active=bool(self.dag_runs),
            extra_selector_name="dag_runs (non-empty)",
        )
        return self


class DAGRunResponse(BaseModel):
    """Dag Run serializer for responses."""

    dag_run_id: str = Field(validation_alias="run_id")
    dag_id: str
    logical_date: datetime | None
    queued_at: datetime | None
    start_date: datetime | None
    end_date: datetime | None
    duration: float | None
    data_interval_start: datetime | None
    data_interval_end: datetime | None
    run_after: datetime
    last_scheduling_decision: datetime | None
    run_type: DagRunType
    state: DagRunState
    triggered_by: DagRunTriggeredByType | None
    triggering_user_name: str | None
    conf: dict | None
    note: str | None
    dag_versions: list[DagVersionResponse]
    bundle_version: str | None
    dag_display_name: str = Field(validation_alias=AliasPath("dag_model", "dag_display_name"))
    partition_key: str | None
    partition_date: datetime | None


class DAGRunCollectionResponse(BaseModel):
    """
    Dag Run collection response supporting both offset and cursor pagination.

    A single flat model is used instead of a discriminated union
    (``Annotated[Offset | Cursor, Field(discriminator=...)]``) because
    the OpenAPI ``oneOf`` + ``discriminator`` construct is not handled
    correctly by ``@hey-api/openapi-ts`` / ``@7nohe/openapi-react-query-codegen``:
    return types degrade to ``unknown`` in JSDoc and can produce
    incorrect TypeScript types (see hey-api/openapi-ts#1613, #3270).
    """

    dag_runs: Iterable[DAGRunResponse]
    total_entries: int | None = Field(
        default=None,
        description="Total number of matching items. Populated for offset pagination, "
        "``null`` when using cursor pagination.",
    )
    next_cursor: str | None = Field(
        default=None,
        description="Token pointing to the next page. Populated for cursor pagination, "
        "``null`` when using offset pagination or when there is no next page.",
    )
    previous_cursor: str | None = Field(
        default=None,
        description="Token pointing to the previous page. Populated for cursor pagination, "
        "``null`` when using offset pagination or when on the first page.",
    )


class TriggerDAGRunPostBody(StrictBaseModel):
    """Trigger Dag Run Serializer for POST body."""

    dag_run_id: str | None = None
    data_interval_start: AwareDatetime | None = None
    data_interval_end: AwareDatetime | None = None
    logical_date: AwareDatetime | None
    run_after: datetime | None = Field(default_factory=timezone.utcnow)

    conf: dict | None = Field(default_factory=dict)
    note: str | None = None
    partition_key: str | None = None
    bundle_version: str | None = None

    @model_validator(mode="after")
    def check_data_intervals(self):
        if (self.data_interval_start is None) != (self.data_interval_end is None):
            raise ValueError(
                "Either both data_interval_start and data_interval_end must be provided or both must be None"
            )
        return self

    def validate_context(self, dag: SerializedDAG) -> dict:
        dag.validate_partition_key(self.partition_key)
        coerced_logical_date = timezone.coerce_datetime(self.logical_date)
        run_after = self.run_after or timezone.utcnow()
        data_interval = None
        if coerced_logical_date:
            if self.data_interval_start and self.data_interval_end:
                data_interval = DataInterval(
                    start=timezone.coerce_datetime(self.data_interval_start),
                    end=timezone.coerce_datetime(self.data_interval_end),
                )
            else:
                data_interval = dag.timetable.infer_manual_data_interval(run_after=coerced_logical_date)

        run_id = self.dag_run_id or dag.timetable.generate_run_id(
            run_type=DagRunType.MANUAL,
            run_after=timezone.coerce_datetime(run_after),
            data_interval=data_interval,
        )

        partition_date = dag.timetable.resolve_partition_date(self.partition_key)

        return {
            "run_id": run_id,
            "logical_date": coerced_logical_date,
            "data_interval": data_interval,
            "run_after": run_after,
            "conf": self.conf,
            "note": self.note,
            "partition_key": self.partition_key,
            "partition_date": partition_date,
        }


class DAGRunsBatchBody(StrictBaseModel):
    """List Dag Runs body for batch endpoint."""

    order_by: str | None = None
    page_offset: NonNegativeInt = 0
    page_limit: NonNegativeInt = 100
    dag_ids: list[str] | None = None
    states: list[DagRunState | None] | None = None

    run_after_gte: AwareDatetime | None = None
    run_after_gt: AwareDatetime | None = None
    run_after_lte: AwareDatetime | None = None
    run_after_lt: AwareDatetime | None = None

    logical_date_gte: AwareDatetime | None = None
    logical_date_gt: AwareDatetime | None = None
    logical_date_lte: AwareDatetime | None = None
    logical_date_lt: AwareDatetime | None = None

    start_date_gte: AwareDatetime | None = None
    start_date_gt: AwareDatetime | None = None
    start_date_lte: AwareDatetime | None = None
    start_date_lt: AwareDatetime | None = None

    end_date_gte: AwareDatetime | None = None
    end_date_gt: AwareDatetime | None = None
    end_date_lte: AwareDatetime | None = None
    end_date_lt: AwareDatetime | None = None

    duration_gte: float | None = None
    duration_gt: float | None = None
    duration_lte: float | None = None
    duration_lt: float | None = None

    conf_contains: str | None = None


class ClearPartitionsBody(PartitionSelectorMixin):
    """Request body for the clearPartitions endpoint (column-reset: set partition fields to None)."""

    run_id: str | None = Field(
        default=None,
        description="Select runs by exact run_id. Mutually exclusive with ``partition_key`` and partition date window.",
    )
    clear_task_instances: bool = Field(
        default=False,
        description="Also clear task instances on the matched runs.",
    )
    dry_run: bool = Field(
        default=True,
        description="If True, compute counts without writing any changes.",
    )

    @model_validator(mode="after")
    def validate_exactly_one_selector(self) -> ClearPartitionsBody:
        self._check_exactly_one_selection_mode(
            extra_selector_active=self.run_id is not None,
            extra_selector_name="run_id",
        )
        return self


class ClearPartitionsResponse(BaseModel):
    """Response for the clearPartitions endpoint."""

    dag_runs_cleared: int
    task_instances_cleared: int
    dry_run: bool
