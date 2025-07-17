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

import enum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from airflow.sdk.execution_time.comms import ErrorResponse


class AirflowRuntimeError(Exception):
    """Generic Airflow error raised by runtime functions."""

    def __init__(self, error: ErrorResponse):
        self.error = error
        super().__init__(f"{error.error.value}: {error.detail}")


class ErrorType(enum.Enum):
    """Error types used in the API client."""

    CONNECTION_NOT_FOUND = "CONNECTION_NOT_FOUND"
    VARIABLE_NOT_FOUND = "VARIABLE_NOT_FOUND"
    XCOM_NOT_FOUND = "XCOM_NOT_FOUND"
    ASSET_NOT_FOUND = "ASSET_NOT_FOUND"
    DAGRUN_ALREADY_EXISTS = "DAGRUN_ALREADY_EXISTS"
    GENERIC_ERROR = "GENERIC_ERROR"
    API_SERVER_ERROR = "API_SERVER_ERROR"


class XComForMappingNotPushed(TypeError):
    """Raise when a mapped downstream's dependency fails to push XCom for task mapping."""

    def __str__(self) -> str:
        return "did not push XCom for task mapping"


class UnmappableXComTypePushed(TypeError):
    """Raise when an unmappable type is pushed as a mapped downstream's dependency."""

    def __init__(self, value: Any, *values: Any) -> None:
        super().__init__(value, *values)

    def __str__(self) -> str:
        typename = type(self.args[0]).__qualname__
        for arg in self.args[1:]:
            typename = f"{typename}[{type(arg).__qualname__}]"
        return f"unmappable return type {typename!r}"
