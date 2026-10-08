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

from typing import Annotated, Literal
from uuid import UUID

from pydantic import ConfigDict, Field

from airflow.api_fastapi.core_api.base import BaseModel

TaskTokenScope = Literal["execution", "workload", "callback"]
DagProcessorSessionScope = Literal["dag_processor_session"]
DagProcessorScope = Literal["dag_processor"]
DagParseScope = Literal["dag_parse"]
TokenScope = Literal[TaskTokenScope, DagProcessorSessionScope, DagProcessorScope, DagParseScope]


class _JWTClaims(BaseModel):
    """
    Validated JWT claims for an Execution API principal.

    JWTValidator checks standard JWT claims before constructing claims for HTTP requests.
    Extra claims are allowed for compatibility with existing task tokens.
    """

    model_config = ConfigDict(extra="allow")


class TIClaims(_JWTClaims):
    """Claims for task execution, workload exchange, or callback execution."""

    scope: TaskTokenScope = "execution"
    # Trusted in-process callers construct task identities without a JWT or expiry.
    exp: float | None = None


DagBundleGrant = Annotated[frozenset[Annotated[str, Field(min_length=1)]], Field(min_length=1)]


class DagProcessorSessionClaims(_JWTClaims):
    """Provisioned bundle grants used only to register a Job or renew its credential."""

    scope: DagProcessorSessionScope = "dag_processor_session"
    exp: float
    dag_bundles: DagBundleGrant


class DagProcessorClaims(_JWTClaims):
    """Claims for managing one registered Job and exchanging its parsing credentials."""

    scope: DagProcessorScope = "dag_processor"
    exp: float
    dag_bundles: DagBundleGrant
    job_id: int


class DagParseClaims(_JWTClaims):
    """Claims for one file-parsing attempt within a registered Job's bundle."""

    scope: DagParseScope = "dag_parse"
    exp: float
    dag_bundles: DagBundleGrant = Field(max_length=1)
    job_id: int
    session_id: UUID
    relative_fileloc: str = Field(min_length=1, max_length=2000)


ExecutionClaims = Annotated[
    TIClaims | DagProcessorSessionClaims | DagProcessorClaims | DagParseClaims, Field(discriminator="scope")
]


class ExecutionToken(BaseModel):
    """Authenticated task, callback, processor session, or parsing-attempt identity."""

    id: UUID
    claims: ExecutionClaims


class TIToken(ExecutionToken):
    """Authenticated task, workload, or callback identity."""

    claims: TIClaims


class DagProcessorSessionToken(ExecutionToken):
    """Authenticated provisioned Dag processor session."""

    claims: DagProcessorSessionClaims


class DagProcessorToken(ExecutionToken):
    """Authenticated Dag processor Job."""

    claims: DagProcessorClaims
