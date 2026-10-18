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

from pydantic import Field

from airflow.api_fastapi.auth.cli_authorization_code import (
    CODE_VERIFIER_PATTERN,
    MAX_LOOPBACK_PORT,
    MIN_LOOPBACK_PORT,
)
from airflow.api_fastapi.core_api.base import BaseModel


class CliTokenBody(BaseModel):
    """Request body for exchanging a code approved in the UI for a CLI token."""

    code: str = Field(description="One-time code the UI sent to airflowctl's loopback callback.")
    code_verifier: str = Field(
        pattern=CODE_VERIFIER_PATTERN,
        description="PKCE verifier whose S256 challenge the code was issued for.",
    )
    port: int = Field(
        ge=MIN_LOOPBACK_PORT,
        le=MAX_LOOPBACK_PORT,
        description="Loopback port the code was issued for.",
    )


class CliTokenResponse(BaseModel):
    """CLI token issued in exchange for a code approved in the UI."""

    access_token: str
    expires_in_seconds: int
