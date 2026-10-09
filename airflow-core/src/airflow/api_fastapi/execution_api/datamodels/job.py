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
from uuid import UUID

from pydantic import Field, field_validator

from airflow.api_fastapi.core_api.base import StrictBaseModel
from airflow.jobs.job import JobState


class TerminalJobState(str, Enum):
    """States a Job can finish in."""

    SUCCESS = JobState.SUCCESS.value
    FAILED = JobState.FAILED.value


class JobRegisterBody(StrictBaseModel):
    """Request body a Dag processor sends to register the Job of its session."""

    registration_id: UUID = Field(
        description=(
            "Chosen by the processor once per process start. Registering again with the same id while its Job "
            "is open returns that Job with a fresh token, which recovers a lost response and renews the "
            "token. Once the Job completes or is replaced the id is refused, so a restart chooses a new one."
        )
    )
    hostname: str = Field(min_length=1, max_length=500)
    unixname: str | None = Field(default=None, max_length=1000)
    bundle_names: list[str] | None = Field(
        default=None,
        min_length=1,
        description="Bundles the processor parses; defaults to every bundle its token grants.",
    )


class JobRegisterResponse(StrictBaseModel):
    """The registered Job and its management credential."""

    job_id: int
    token: str = Field(description="A ``dag_processor`` token, valid until it expires or the Job ends.")
    expires_in: int = Field(gt=0, description="Seconds until the token expires.")


class JobHeartbeatResponse(StrictBaseModel):
    """Current state of the Job; ``restarting`` asks the processor to stop."""

    state: JobState


class JobCompleteBody(StrictBaseModel):
    """Final state of the Job."""

    state: TerminalJobState


class DagParseTokenBody(StrictBaseModel):
    """Exchange a Job credential for access on behalf of one file-parsing attempt."""

    attempt_id: UUID
    bundle_name: str = Field(min_length=1, max_length=250)
    relative_fileloc: str = Field(min_length=1, max_length=2000)

    @field_validator("relative_fileloc")
    @classmethod
    def validate_relative_fileloc(cls, value: str) -> str:
        if "\\" in value or "\x00" in value or any(part in ("", ".", "..") for part in value.split("/")):
            raise ValueError("The file location must be a normalized bundle-relative path")
        return value


class DagParseTokenResponse(StrictBaseModel):
    """Short-lived parsing credential; cannot register, heartbeat, or complete Jobs."""

    token: str
    expires_in: int = Field(gt=0, description="Seconds until the token expires.")
