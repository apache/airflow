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
"""Connection test workload schema for executor communication."""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Literal

from pydantic import Field

from airflow.executors.workloads.base import BaseWorkloadSchema
from airflow.models.connection_test import ConnectionTestKey, ConnectionTestState

if TYPE_CHECKING:
    from airflow.api_fastapi.auth.tokens import JWTGenerator


class TestConnection(BaseWorkloadSchema):
    """Execute a connection test on a worker."""

    connection_test_id: uuid.UUID
    connection_id: str
    timeout: int
    queue: str | None = None

    type: Literal["TestConnection"] = Field(init=False, default="TestConnection")

    @property
    def key(self) -> ConnectionTestKey:
        """Return the connection-test key (str UUID) for this workload."""
        return ConnectionTestKey(id=str(self.connection_test_id))

    @property
    def display_name(self) -> str:
        """Return a human-readable name for logging and process titles."""
        return f"connection-test {self.connection_id}"

    @property
    def success_state(self) -> ConnectionTestState:
        return ConnectionTestState.SUCCESS

    @property
    def failure_state(self) -> ConnectionTestState:
        return ConnectionTestState.FAILED

    @property
    def running_state(self) -> ConnectionTestState:
        return ConnectionTestState.RUNNING

    @classmethod
    def make(
        cls,
        *,
        connection_test_id: uuid.UUID,
        connection_id: str,
        timeout: int,
        queue: str | None = None,
        generator: JWTGenerator | None = None,
    ) -> TestConnection:
        return cls(
            connection_test_id=connection_test_id,
            connection_id=connection_id,
            timeout=timeout,
            queue=queue,
            token=cls.generate_token(str(connection_test_id), generator),
        )
