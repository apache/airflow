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
from uuid import UUID

from pydantic import Field, JsonValue

from airflow.api_fastapi.core_api.base import BaseModel
from airflow.webhooks.connections import WebhookSourceTypeStatus
from airflow.webhooks.status import WebhookConnectionStatus


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
