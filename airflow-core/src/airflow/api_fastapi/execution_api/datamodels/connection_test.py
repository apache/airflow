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

from airflow.api_fastapi.core_api.base import BaseModel, StrictBaseModel
from airflow.models.connection_test import ConnectionTestState


class ConnectionTestResultBody(StrictBaseModel):
    """Payload sent by workers to report connection test results."""

    state: ConnectionTestState
    result_message: str | None = None


class ConnectionTestConnectionResponse(BaseModel):
    """Connection data returned to workers from a test request."""

    conn_id: str
    conn_type: str
    host: str | None = None
    login: str | None = None
    password: str | None = None
    schema_: str | None = Field(None, alias="schema")
    port: int | None = None
    extra: str | None = None
