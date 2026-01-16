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

"""Datamodels for worker-side connection test execution."""

from __future__ import annotations

from typing import Literal

from airflow.api_fastapi.core_api.base import BaseModel, StrictBaseModel


class ConnectionTestWorkload(BaseModel):
    """Workload data sent to worker for connection test execution."""

    request_id: str
    encrypted_connection_uri: str
    conn_type: str
    timeout: int


class ConnectionTestPendingResponse(BaseModel):
    """Response containing pending connection test requests for workers."""

    requests: list[ConnectionTestWorkload]


class ConnectionTestRunningPayload(StrictBaseModel):
    """Payload for marking a connection test as running."""

    state: Literal["running"]
    hostname: str


class ConnectionTestResultPayload(StrictBaseModel):
    """Payload for reporting connection test result."""

    state: Literal["success", "failed"]
    result_status: bool
    result_message: str
