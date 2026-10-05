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

TokenScope = Literal["execution", "workload", "callback", "dag_processor_session", "dag_processor"]


class TIClaims(BaseModel):
    """
    Validated JWT claims for a task identity token.

    Only fields used by the Execution API (sub, scope, exp, dag_bundles, job_id) are explicitly typed.
    JWTValidator already validates exp/iat/nbf/aud/etc. Extra claims are allowed.
    """

    model_config = ConfigDict(extra="allow")

    scope: TokenScope = "execution"
    exp: float | None = None
    dag_bundles: frozenset[Annotated[str, Field(min_length=1)]] | None = None
    """Dag bundles a Dag processor token may act for."""
    job_id: int | None = None
    """Job a ``dag_processor`` token was issued for when that Job registered."""

    @model_validator(mode="after")
    def validate_dag_processor_claims(self) -> Self:
        if self.scope in ("dag_processor_session", "dag_processor") and not self.dag_bundles:
            raise ValueError(f"A {self.scope} token must grant at least one Dag bundle")
        if self.scope == "dag_processor" and self.job_id is None:
            raise ValueError("A dag_processor token must name the Job it was issued for")
        return self


class TIToken(BaseModel):
    """Task Identity Token."""

    id: UUID
    claims: TIClaims
