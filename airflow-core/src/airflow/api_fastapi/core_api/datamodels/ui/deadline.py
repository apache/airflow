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

from pydantic import AliasPath, Field, model_validator

from airflow.api_fastapi.core_api.base import BaseModel


class DeadlineResponse(BaseModel):
    """Deadline serializer for responses."""

    id: UUID
    deadline_time: datetime
    missed: bool
    created_at: datetime
    alert_name: str | None = Field(validation_alias=AliasPath("deadline_alert", "name"), default=None)
    alert_description: str | None = Field(
        validation_alias=AliasPath("deadline_alert", "description"), default=None
    )


class DeadlineCollectionResponse(BaseModel):
    """Deadline Collection serializer for responses."""

    deadlines: Iterable[DeadlineResponse]
    total_entries: int


class DeadlineWithDagRunResponse(BaseModel):
    """Deadline serializer for responses that includes DAG and DAG run identifiers."""

    id: UUID
    deadline_time: datetime
    missed: bool
    created_at: datetime
    dag_id: str = Field(validation_alias=AliasPath("dagrun", "dag_id"))
    dag_run_id: str = Field(validation_alias=AliasPath("dagrun", "run_id"))
    alert_name: str | None = Field(validation_alias=AliasPath("deadline_alert", "name"), default=None)
    alert_description: str | None = Field(
        validation_alias=AliasPath("deadline_alert", "description"), default=None
    )


class DeadlineWithDagRunCollectionResponse(BaseModel):
    """Deadline Collection serializer for responses that includes DAG and DAG run identifiers."""

    deadlines: Iterable[DeadlineWithDagRunResponse]
    total_entries: int


class DeadlineAlertResponse(BaseModel):
    """DeadlineAlert serializer for responses."""

    id: UUID
    name: str | None = None
    description: str | None = None
    reference_type: str
    interval: float
    created_at: datetime

    @model_validator(mode="before")
    @classmethod
    def extract_reference_type(cls, data):
        """Extract reference_type from the JSON reference column when validating from an ORM object."""
        if not isinstance(data, dict) and hasattr(data, "reference"):
            return {
                "id": data.id,
                "name": data.name,
                "description": data.description,
                "reference_type": data.reference.get("reference_type", "") if data.reference else "",
                "interval": data.interval,
                "created_at": data.created_at,
            }
        return data


class DeadlineAlertCollectionResponse(BaseModel):
    """DeadlineAlert Collection serializer for responses."""

    deadline_alerts: Iterable[DeadlineAlertResponse]
    total_entries: int
