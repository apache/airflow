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

from pydantic import ConfigDict, Field, model_validator

from airflow.api_fastapi.core_api.base import BaseModel
from airflow.typing_compat import Self

TokenScope = Literal[
    "execution", "workload", "callback", "dag_processor_session", "dag_processor", "dag_parse"
]


class ExecutionClaims(BaseModel):
    """
    Validated JWT claims for an Execution API principal.

    JWTValidator validates exp/iat/nbf/aud before these claims are constructed.
    Extra claims are allowed for compatibility with existing task tokens.
    """

    model_config = ConfigDict(extra="allow")

    scope: TokenScope = "execution"
    exp: float | None = None
    dag_bundles: frozenset[Annotated[str, Field(min_length=1)]] | None = None
    """Dag bundles a Dag processor token may act for."""
    job_id: int | None = None
    """Job a ``dag_processor`` token was issued for when that Job registered."""
    session_id: UUID | None = None
    """Processor session that owns a parsing attempt, whose own identity is the token subject."""
    relative_fileloc: str | None = Field(default=None, min_length=1, max_length=2000)
    """Bundle-relative file being parsed; an archive is one file under the current processor model."""

    @model_validator(mode="after")
    def validate_dag_processor_claims(self) -> Self:
        if self.scope in ("dag_processor_session", "dag_processor", "dag_parse") and not self.dag_bundles:
            raise ValueError(f"A {self.scope} token must grant at least one Dag bundle")
        if self.scope in ("dag_processor", "dag_parse") and self.job_id is None:
            raise ValueError(f"A {self.scope} token must name the Job it was issued for")
        if self.scope == "dag_parse" and (
            len(self.dag_bundles or ()) != 1 or self.session_id is None or self.relative_fileloc is None
        ):
            raise ValueError("A dag_parse token must name one bundle, its file, and its processor session")
        return self


class ExecutionToken(BaseModel):
    """Authenticated task, callback, processor session, or parsing-attempt identity."""

    id: UUID
    claims: ExecutionClaims


# Preserve imports for task-specific consumers while shared endpoints use the general principal.
TIClaims = ExecutionClaims
TIToken = ExecutionToken
