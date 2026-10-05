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

from enum import Enum

from pydantic import Field

from airflow.api_fastapi.core_api.base import StrictBaseModel
from airflow.jobs.job import JobState


class TerminalJobState(str, Enum):
    """States a Job can finish in."""

    SUCCESS = JobState.SUCCESS.value
    FAILED = JobState.FAILED.value


class JobRegisterBody(StrictBaseModel):
    """Request body a Dag processor sends to register the Job of its session."""

    hostname: str = Field(min_length=1, max_length=500)
    unixname: str | None = Field(default=None, max_length=1000)
    bundle_names: list[str] | None = Field(
        default=None, description="Bundles the processor parses; defaults to every bundle its token grants."
    )


class JobRegisterResponse(StrictBaseModel):
    """Identifier of the newly registered Job."""

    job_id: int


class JobHeartbeatResponse(StrictBaseModel):
    """Current state of the Job; ``restarting`` asks the processor to stop."""

    state: JobState


class JobCompleteBody(StrictBaseModel):
    """Final state of the Job."""

    state: TerminalJobState
