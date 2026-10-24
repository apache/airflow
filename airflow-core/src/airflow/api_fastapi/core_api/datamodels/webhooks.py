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
from typing import Any, Literal
from uuid import UUID

from pydantic import Field, JsonValue, model_validator

from airflow.api_fastapi.common.region import OmitsMissingRegion, RegionId, RegionIndex
from airflow.api_fastapi.core_api.base import BaseModel
from airflow.models.inbound_wait import WaitStatus
from airflow.models.webhook_cancel_request import CancelRequestStatus
from airflow.serialization.definitions.webhook_match import SerializedWebhookMatch
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.webhooks.connections import WebhookSourceTypeStatus
from airflow.webhooks.status import WebhookConnectionStatus


class WebhookMatchResponse(BaseModel):
    """A condition on deliveries to an inbound webhook, as a Dag's or a task's ``cancel_on`` declares it."""

    webhook: str
    events: list[str] | None = Field(
        default=None, description="The delivery events it accepts, or null for any event."
    )
    match: dict[str, list[str]] = Field(
        default_factory=dict,
        description="The delivery attributes it requires, each with the values of which one must match.",
    )
    correlation_key_conf: str | None = Field(
        default=None,
        description=(
            "The top-level conf key whose value makes a run triggered with a conf about a correlation key, "
            "or null."
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def _read_serialized_match(cls, value: Any) -> Any:
        # The serialized form keeps match as hashable pairs, which do not validate as a dict.
        if isinstance(value, SerializedWebhookMatch):
            return value.to_json()
        return value


class WebhookCancelRunTargetResponse(BaseModel):
    """The whole runs a cancel request fails."""

    correlation_key_confs: list[str] = Field(
        description=(
            "The conf keys by which a run triggered with a conf is about the request's correlation key; "
            "empty if only runs started by a webhook delivery are."
        )
    )


class WebhookCancelTaskTargetResponse(BaseModel):
    """A task that a cancel request ends in its runs, and the state it ends in."""

    task_id: str
    cancel_state: Literal["failed", "skipped"]
    correlation_key_confs: list[str] = Field(
        description=(
            "The conf keys by which a run triggered with a conf is about the request's correlation key; "
            "empty if only runs started by a webhook delivery are."
        )
    )


class WebhookCancelTargetsResponse(BaseModel):
    """What a cancel request cancels in each run about its correlation key."""

    run: WebhookCancelRunTargetResponse | None = Field(
        description="Set if a condition of the Dag itself matched, which fails the whole run."
    )
    tasks: list[WebhookCancelTaskTargetResponse] = Field(
        description="The tasks whose conditions matched, by task id."
    )


class CancelledRunResponse(BaseModel):
    """A run that a cancel request cancelled."""

    run_id: str
    state: DagRunState = Field(
        description="`failed`, or `running` or `queued` if a teardown still has to run first."
    )


class CancelledTaskInstanceResponse(OmitsMissingRegion, BaseModel):
    """A task instance that a cancel request ended."""

    ti_id: UUID | None = Field(default=None, description="The task instance's id.")
    run_id: str
    task_id: str
    map_index: int
    region_id: RegionId = None
    region_index: RegionIndex = None
    state: TaskInstanceState


class SkippedCancelRunResponse(BaseModel):
    """A run about a cancel request's correlation key that the scheduler left as it was."""

    run_id: str
    reason: str = Field(
        description=(
            "Why: `dag_not_found` if its Dag version could not be loaded; `dag_has_team` if its Dag "
            "belongs to a team."
        )
    )


class WebhookCancelResultResponse(BaseModel):
    """What the scheduler did with a cancel request."""

    dag_runs: list[CancelledRunResponse] = Field(
        default_factory=list, description="The runs it failed, if a condition of the Dag itself matched."
    )
    task_instances: list[CancelledTaskInstanceResponse] = Field(
        default_factory=list, description="The task instances it ended, if a task's condition matched."
    )
    skipped_runs: list[SkippedCancelRunResponse] = Field(
        default_factory=list, description="The runs about its correlation key that it left as they were."
    )
    error: str | None = Field(default=None, description="The error that applying it raised, if it failed.")


class WebhookCancelRequestResponse(BaseModel):
    """A cancel request that a delivery recorded, and what the scheduler did with it."""

    id: UUID
    dag_id: str
    targets: WebhookCancelTargetsResponse
    status: CancelRequestStatus = Field(
        description=(
            "`pending` until the scheduler applies it, which it does within a second or two while it "
            "runs; then `applied`, or `failed` if applying it raised an error, which the scheduler logged."
        )
    )
    applied_at: datetime | None = Field(description="When the scheduler applied it, or failed to.")
    result: WebhookCancelResultResponse | None = Field(
        description=(
            "The runs and task instances it cancelled. A run created later from an earlier delivery is "
            "added when the scheduler cancels it as it creates it, which can happen while it is `pending`."
        )
    )


class WebhookDeliveryResponse(BaseModel):
    """A delivery an inbound webhook received, without its payload."""

    id: UUID
    webhook: str
    delivery_id: str | None = Field(description="The sender's id for the delivery, if it sends one.")
    received_at: datetime
    source_type: str
    attributes: dict[str, str | int] | None = Field(
        description="What the webhook's source type parsed from the delivery."
    )
    correlation_key: str | None = Field(description="What the delivery is about, such as a pull request.")
    outcome: str = Field(description="What Airflow did with the delivery, such as ``no_subscribers``.")
    candidates: list[str] | None = Field(description="The webhook topic names built from the delivery.")
    actions: dict[str, JsonValue] | None = Field(
        description="What the delivery did, such as the topic events it recorded."
    )
    team_name: str | None
    cancel_requests: list[WebhookCancelRequestResponse] = Field(
        default_factory=list,
        description=(
            "The cancel requests the delivery recorded, by Dag, each with what the scheduler did with it. "
            "These records are authoritative: `actions` summarizes them, can miss one that a concurrent "
            "redelivery recorded, and still names a request whose record was removed."
        ),
    )


class WebhookDeliveryCollectionResponse(BaseModel):
    """Webhook delivery collection response."""

    deliveries: Iterable[WebhookDeliveryResponse]
    total_entries: int


class WebhookResponse(BaseModel):
    """A webhook, whether it can be served, and the last delivery it received."""

    name: str
    listed: bool = Field(
        description="Whether `[webhooks] names` lists the webhook, which it must to be served."
    )
    connection_status: WebhookConnectionStatus = Field(
        description=(
            "Whether the webhook's `webhook_<name>` connection configures a webhook that can be served: "
            "`ok`; `not_found` if no secrets backend has it; `wrong_type` if its type is not "
            "`inbound_webhook`; `misconfigured` if it cannot be used as it is, and `airflow webhooks check` "
            "says why; `unavailable` if a secrets backend failed."
        )
    )
    source_type: str | None = Field(
        description="The source type the connection names, or null if the connection was not read that far."
    )
    source_type_status: WebhookSourceTypeStatus | None = Field(
        description=(
            "Whether an installed provider serves the source type: `ok`; `unknown` if none does; "
            "`conflict` if several declare it and `[webhooks] source_type_owners` names none of them. "
            "Null when `source_type` is."
        )
    )
    last_received_at: datetime | None = Field(description="When the last recorded delivery was received.")
    last_outcome: str | None = Field(description="What Airflow did with the last recorded delivery.")


class WebhookCollectionResponse(BaseModel):
    """Webhook collection response."""

    webhooks: Iterable[WebhookResponse]
    total_entries: int


class WebhookWaitResponse(BaseModel):
    """A task instance's wait for a delivery to an inbound webhook, without the delivery's payload."""

    id: UUID
    webhook: str
    events: list[str | int] | None = Field(
        description="The delivery events the wait accepts, or null for any event."
    )
    match: dict[str, str | int | list[str | int]] = Field(
        description=(
            "The delivery attributes the wait requires, each a value, or a list of values of which one must "
            "match."
        )
    )
    status: WaitStatus = Field(
        description=(
            "`registered` until a matching delivery arrives; `received` if one arrived before the task "
            "parked: while the task was running, or before the wait was registered, when Airflow kept the "
            "delivery for waits (`early`, or `cancel_requested` if it requested a cancel); the task resumes "
            "with it once it parks; `resumed` once a delivery resumed the task; `timed_out` if none arrived "
            "before the deadline; `ended` if the task registered another wait, if a webhook cancel request "
            "ended its task, or if a matching delivery, or a wait registered on the same events and match, "
            "found the wait's try no longer running or parked. Nothing else ends a wait whose try stopped, "
            "so it keeps `registered` or `received` until then."
        )
    )
    deadline: datetime = Field(description="When the wait times out if no matching delivery has arrived.")
    created_at: datetime
    ended_at: datetime | None
    inbound_delivery_id: UUID | None = Field(
        validation_alias="delivery_id",
        description=(
            "The `id` of the delivery record that the wait received, as the webhook's deliveries list it, "
            "or null if it has received none."
        ),
    )


class WebhookWaitCollectionResponse(BaseModel):
    """Webhook wait collection response."""

    webhook_waits: Iterable[WebhookWaitResponse]
    total_entries: int
