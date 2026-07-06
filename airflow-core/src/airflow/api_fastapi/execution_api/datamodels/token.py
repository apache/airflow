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

from typing import Literal
from uuid import UUID

from pydantic import ConfigDict

from airflow.api_fastapi.core_api.base import BaseModel

TokenScope = Literal["execution", "workload"]


class TIClaims(BaseModel):
    """
    Validated JWT claims for a task identity token.

    Only fields used by the Execution API (sub, scope) are explicitly typed.
    JWTValidator already validates exp/iat/nbf/aud/etc. Extra claims are allowed.
    """

    model_config = ConfigDict(extra="allow")

    scope: TokenScope = "execution"


class TIToken(BaseModel):
    """Task Identity Token."""

    id: UUID
    claims: TIClaims
