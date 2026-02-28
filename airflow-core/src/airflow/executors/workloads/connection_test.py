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

if TYPE_CHECKING:
    from airflow.api_fastapi.auth.tokens import JWTGenerator


class TestConnection(BaseWorkloadSchema):
    """Execute a connection test on a worker."""

    connection_test_id: uuid.UUID
    connection_id: str
    timeout: int = 60

    type: Literal["TestConnection"] = Field(init=False, default="TestConnection")

    @classmethod
    def make(
        cls,
        *,
        connection_test_id: uuid.UUID,
        connection_id: str,
        timeout: int = 60,
        generator: JWTGenerator | None = None,
    ) -> TestConnection:
        return cls(
            connection_test_id=connection_test_id,
            connection_id=connection_id,
            timeout=timeout,
            token=cls.generate_token(str(connection_test_id), generator),
        )
