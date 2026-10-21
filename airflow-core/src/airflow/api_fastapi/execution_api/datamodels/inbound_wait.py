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

from datetime import timedelta
from uuid import UUID

from pydantic import JsonValue, model_validator

from airflow.api_fastapi.common.types import UtcDateTime
from airflow.api_fastapi.core_api.base import BaseModel, StrictBaseModel
from airflow.models.inbound_wait import WaitStatus
from airflow.webhooks.registry import MAX_WAIT_TIMEOUT
from airflow.webhooks.waits import validate_wait


class InboundWaitRequest(StrictBaseModel):
    """Schema for registering a task instance's wait for a delivery to an inbound webhook."""

    webhook: str
    """The name of the webhook, as listed in ``[webhooks] names``."""
    events: list[JsonValue] | None = None
    """The delivery events the wait accepts, or None for any event."""
    match: dict[str, JsonValue] | None = None
    """Delivery attributes the wait requires: each a value, or a list of values of which one must match."""
    timeout: timedelta | None = None
    """How long to wait from now, at most 365 days; None uses ``[webhooks] default_wait_timeout``."""

    @model_validator(mode="after")
    def _validate_wait(self) -> InboundWaitRequest:
        validate_wait(self.events, self.match)
        if self.timeout is not None and self.timeout <= timedelta(0):
            raise ValueError("timeout must be positive")
        if self.timeout is not None and self.timeout > MAX_WAIT_TIMEOUT:
            raise ValueError(f"timeout must be {MAX_WAIT_TIMEOUT.days} days or less")
        return self


class InboundWaitResponse(BaseModel):
    """Schema for a registered webhook wait."""

    id: UUID
    status: WaitStatus
    """
    ``registered``, or ``received`` if a delivery that arrived before the wait matches it.

    A task whose wait is ``received`` resumes with that delivery as soon as it parks.
    """
    deadline: UtcDateTime
    """When the wait times out if no matching delivery has arrived."""
