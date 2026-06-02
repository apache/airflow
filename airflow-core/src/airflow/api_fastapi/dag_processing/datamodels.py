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
"""Request/response models for the DAG Processing API (AIP-92)."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class ParsingResultBody(BaseModel):
    """One file's parse output, mirroring ``DagFileProcessorManager.persist_parsing_result``."""

    bundle_name: str
    bundle_version: str | None = None
    version_data: dict | None = None
    relative_fileloc: str | None = None
    run_duration: float | None = None
    serialized_dags: list[dict] = []
    import_errors: dict[str, str] | None = None
    warnings: list[dict] | None = None


class ReconcileBody(BaseModel):
    """The full set of source paths currently observed in a bundle, for stale reconciliation."""

    observed_filelocs: list[str]


class BundleStateResponse(BaseModel):
    """A bundle's persisted refresh state (``found=False`` when it has no record)."""

    found: bool
    last_refreshed: datetime | None = None
    version: str | None = None


class BundleStateUpdateBody(BaseModel):
    """Post-refresh state for a bundle. ``version=None`` leaves the stored version unchanged."""

    last_refreshed: datetime
    version: str | None = None


class StaleDagEntry(BaseModel):
    """A parsed file's identity and last parse-finish time, for the stale-dag sweep."""

    bundle_name: str
    relative_fileloc: str
    last_finish_time: datetime


class StaleDagsBody(BaseModel):
    """Inputs for the time-based stale-dag sweep."""

    stale_dag_threshold: int
    last_parsed: list[StaleDagEntry]


class PriorityClaimBody(BaseModel):
    """Bundle names to claim priority parse requests for."""

    bundle_names: list[str]


class CallbackClaimBody(BaseModel):
    """Bundle names and per-loop limit for claiming callbacks."""

    bundle_names: list[str]
    limit: int


class JobRegisterBody(BaseModel):
    """Job type to register for the processor's liveness record."""

    job_type: str


class JobCompleteBody(BaseModel):
    """Terminal state to record when the processor stops."""

    state: str
