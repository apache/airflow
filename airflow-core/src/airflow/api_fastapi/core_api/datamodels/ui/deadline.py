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
from typing import Any
from uuid import UUID

from pydantic import AliasPath, Field, field_validator

from airflow.api_fastapi.core_api.base import BaseModel


class DeadlineResponse(BaseModel):
    """Deadline serializer for responses."""

    id: UUID
    deadline_time: datetime
    missed: bool
    created_at: datetime
    dag_id: str = Field(validation_alias=AliasPath("dagrun", "dag_id"))
    dag_run_id: str = Field(validation_alias=AliasPath("dagrun", "run_id"))
    alert_id: UUID | None = Field(validation_alias="deadline_alert_id", default=None)
    alert_name: str | None = Field(validation_alias=AliasPath("deadline_alert", "name"), default=None)


class DeadlineCollectionResponse(BaseModel):
    """Deadline Collection serializer for responses."""

    deadlines: Iterable[DeadlineResponse]
    total_entries: int


class DeadlineAlertResponse(BaseModel):
    """DeadlineAlert serializer for responses."""

    id: UUID
    name: str | None = None
    reference_type: str = Field(validation_alias=AliasPath("reference", "reference_type"))
    interval: float | None = Field(
        default=None,
        description=(
            "Interval in seconds between the reference time and the deadline. "
            "Null for a dynamic interval (e.g. a VariableInterval) whose value is "
            "only resolved at scheduler evaluation time."
        ),
    )
    created_at: datetime

    @field_validator("interval", mode="before")
    @classmethod
    def coerce_interval_to_seconds(cls, value: Any) -> float | None:
        """
        Coerce the stored ``interval`` into seconds.

        ``DeadlineAlert.interval`` is a JSON column holding the Airflow-serialized form
        of the SDK interval, not a plain number. A fixed ``timedelta`` serializes to
        ``{"__classname__": "datetime.timedelta", "__data__": <seconds>}`` and a dynamic
        ``VariableInterval`` to ``{"__classname__": ".../VariableInterval", "__data__": {...}}``.
        Without this coercion Pydantic cannot turn that dict into ``float`` and the
        ``/ui/dags/{dag_id}/deadlineAlerts`` endpoint raises a 500, which breaks the
        run-page deadline status badge. Return the seconds for a fixed interval, or
        ``None`` for a dynamic one (resolved later by the scheduler).
        """
        if value is None or isinstance(value, (int, float)):
            return value
        if isinstance(value, dict):
            data = value.get("__data__")
            # Fixed timedelta: __data__ is the total seconds as a number.
            if isinstance(data, (int, float)):
                return float(data)
            # Dynamic interval (e.g. VariableInterval): no fixed seconds to report.
            return None
        return None


class DeadlineAlertCollectionResponse(BaseModel):
    """DeadlineAlert Collection serializer for responses."""

    deadline_alerts: Iterable[DeadlineAlertResponse]
    total_entries: int
