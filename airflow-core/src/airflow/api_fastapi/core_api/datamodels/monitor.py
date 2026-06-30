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

from airflow.api_fastapi.core_api.base import BaseModel


class BaseInfoResponse(BaseModel):
    """Base info serializer for responses."""

    status: str | None


class SchedulerInfoResponse(BaseInfoResponse):
    """Scheduler info serializer for responses."""

    latest_scheduler_heartbeat: str | None


class TriggererInfoResponse(BaseInfoResponse):
    """Triggerer info serializer for responses."""

    latest_triggerer_heartbeat: str | None


class DagProcessorInfoResponse(BaseInfoResponse):
    """DagProcessor info serializer for responses."""

    latest_dag_processor_heartbeat: str | None

class SchedulerInstanceInfoResponse(BaseInfoResponse):
    """Scheduler instance info serializer for responses."""

    hostname: str | None
    latest_scheduler_heartbeat: str | None


class TriggererInstanceInfoResponse(BaseInfoResponse):
    """Triggerer instance info serializer for responses."""

    hostname: str | None
    latest_triggerer_heartbeat: str | None
    team_name: str | None


class DagProcessorInstanceInfoResponse(BaseInfoResponse):
    """Dag processor instance info serializer for responses."""

    hostname: str | None
    latest_dag_processor_heartbeat: str | None

class SchedulersInfoResponse(BaseModel):
    """Schedulers info serializer for responses."""
    status: str | None
    instances: list[SchedulerInstanceInfoResponse] | None = None

class TriggerersInfoResponse(BaseModel):
    """Triggerers info serializer for responses."""
    status: str | None
    instances: list[TriggererInstanceInfoResponse] | None = None

class DagProcessorsInfoResponse(BaseModel):
    """Dag processors info serializer for responses."""
    status: str | None
    instances: list[DagProcessorInstanceInfoResponse] | None = None

class HealthInfoResponse(BaseModel):
    """Health serializer for responses."""

    metadatabase: BaseInfoResponse
    scheduler: SchedulerInfoResponse
    triggerer: TriggererInfoResponse
    dag_processor: DagProcessorInfoResponse | None = None
    schedulers: SchedulersInfoResponse | None = None
    triggerers: TriggerersInfoResponse | None = None
    dag_processors: DagProcessorsInfoResponse | None = None
