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

from pydantic import BaseModel


class BaseInfoSchema(BaseModel):
    """Base status field for metadatabase and scheduler."""

    status: str | None


class SchedulerInfoSchema(BaseInfoSchema):
    """Schema for Scheduler info."""

    latest_scheduler_heartbeat: str | None


class TriggererInfoSchema(BaseInfoSchema):
    """Schema for Triggerer info."""

    latest_triggerer_heartbeat: str | None


class DagProcessorInfoSchema(BaseInfoSchema):
    """Schema for DagProcessor info."""

    latest_dag_processor_heartbeat: str | None


class HealthInfoSchema(BaseModel):
    """Schema for the Health endpoint."""

    metadatabase: BaseInfoSchema
    scheduler: SchedulerInfoSchema
    triggerer: TriggererInfoSchema
    dag_processor: DagProcessorInfoSchema
