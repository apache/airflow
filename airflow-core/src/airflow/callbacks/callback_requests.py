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

from collections.abc import Mapping
from typing import TYPE_CHECKING, Annotated, Any, Literal, cast

import structlog
from pydantic import BaseModel, Field, model_validator
from sqlalchemy import inspect as sa_inspect
from sqlalchemy.exc import NoInspectionAvailable
from sqlalchemy.orm.attributes import set_committed_value
from sqlalchemy.orm.exc import DetachedInstanceError

from airflow.api_fastapi.execution_api.datamodels import taskinstance as ti_datamodel  # noqa: TC001
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    from airflow.typing_compat import Self

log = structlog.get_logger(logger_name=__name__)


class BaseCallbackRequest(BaseModel):
    """
    Base Class with information about the callback to be executed.

    :param msg: Additional Message that can be used for logging
    """

    filepath: str
    """File Path to use to run the callback"""
    bundle_name: str
    bundle_version: str | None
    msg: str | None = None
    """Additional Message that can be used for logging to determine failure/task heartbeat timeout"""

    @classmethod
    def from_json(cls, data: str | bytes | bytearray) -> Self:
        return cls.model_validate_json(data)

    def to_json(self, **kwargs) -> str:
        return self.model_dump_json(**kwargs)


class TaskCallbackRequest(BaseCallbackRequest):
    """
    Task callback status information.

    A Class with information about the success/failure TI callback to be executed. Currently, only failure
    callbacks when tasks are externally killed or experience heartbeat timeouts are run via DagFileProcessorProcess.
    """

    ti: ti_datamodel.TaskInstance
    """Simplified Task Instance representation"""
    task_callback_type: TaskInstanceState | None = None
    """Whether on success, on failure, on retry"""
    context_from_server: ti_datamodel.TIRunContext | None = None
    """Task execution context from the Server"""
    type: Literal["TaskCallbackRequest"] = "TaskCallbackRequest"

    @property
    def is_failure_callback(self) -> bool:
        """Returns True if the callback is a failure callback."""
        if self.task_callback_type is None:
            return True
        return self.task_callback_type in {
            TaskInstanceState.FAILED,
            TaskInstanceState.UP_FOR_RETRY,
            TaskInstanceState.UPSTREAM_FAILED,
        }


class EmailRequest(BaseCallbackRequest):
    """Email notification request for task failures/retries."""

    ti: ti_datamodel.TaskInstance
    """Simplified Task Instance representation"""
    email_type: Literal["failure", "retry"] = "failure"
    """Whether this is for a failure or retry email"""
    context_from_server: ti_datamodel.TIRunContext
    """Task execution context from the Server"""
    type: Literal["EmailRequest"] = "EmailRequest"


class DagRunContext(BaseModel):
    """Class to pass context info from the server to build a Execution context object."""

    dag_run: ti_datamodel.DagRun | None = None
    last_ti: ti_datamodel.TaskInstance | None = None

    @model_validator(mode="before")
    @classmethod
    def _sanitize_consumed_asset_events(cls, values: Mapping[str, Any]) -> Mapping[str, Any]:
        if (dag_run := values.get("dag_run")) is None:
            return values

        # DagRunContext may receive non-ORM dag_run objects (e.g. datamodels).
        # Only apply this validator to ORM-mapped instances.
        try:
            sa_inspect(dag_run)
        except NoInspectionAvailable:
            return values

        # Relationship access may raise DetachedInstanceError; on that path, reload DagRun
        # from the DB to avoid crashing the scheduler.
        try:
            events = dag_run.consumed_asset_events
            set_committed_value(
                dag_run,
                "consumed_asset_events",
                list(events) if events is not None else [],
            )
        except DetachedInstanceError:
            log.warning(
                "DagRunContext encountered DetachedInstanceError while accessing "
                "consumed_asset_events; reloading DagRun from DB."
            )
            from sqlalchemy import select
            from sqlalchemy.orm import selectinload

            from airflow.models.asset import AssetEvent
            from airflow.models.dagrun import DagRun
            from airflow.utils.session import create_session

            # Defensive guardrail: reload DagRun with eager-loaded relationships on
            # DetachedInstanceError to recover state without adding DB I/O to the hot path.
            with create_session() as session:
                dag_run_reloaded = session.scalar(
                    select(DagRun)
                    .where(DagRun.id == dag_run.id)
                    .options(
                        selectinload(DagRun.consumed_asset_events).selectinload(AssetEvent.asset),
                        selectinload(DagRun.consumed_asset_events).selectinload(AssetEvent.source_aliases),
                    )
                )

                # DagRun exists; reload is expected to succeed.
                dag_run_reloaded = cast("DagRun", dag_run_reloaded)
                reloaded_events = dag_run_reloaded.consumed_asset_events

            # Install DB-backed relationship state on the detached instance.
            set_committed_value(
                dag_run, "consumed_asset_events", list(reloaded_events) if reloaded_events is not None else []
            )

        return values


class DagCallbackRequest(BaseCallbackRequest):
    """A Class with information about the success/failure DAG callback to be executed."""

    dag_id: str
    run_id: str
    context_from_server: DagRunContext | None = None
    is_failure_callback: bool | None = True
    """Flag to determine whether it is a Failure Callback or Success Callback"""
    type: Literal["DagCallbackRequest"] = "DagCallbackRequest"


CallbackRequest = Annotated[
    DagCallbackRequest | TaskCallbackRequest | EmailRequest,
    Field(discriminator="type"),
]

# Backwards compatibility alias
EmailNotificationRequest = EmailRequest
