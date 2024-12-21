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
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from airflow.sdk.execution_time.comms import ErrorResponse


class AirflowRuntimeError(Exception):
    def __init__(self, error: ErrorResponse):
        self.error = error
        super().__init__(f"{error.error.value}: {error.detail}")


class ErrorType(enum.Enum):
    CONNECTION_NOT_FOUND = "CONNECTION_NOT_FOUND"
    VARIABLE_NOT_FOUND = "VARIABLE_NOT_FOUND"
    XCOM_NOT_FOUND = "XCOM_NOT_FOUND"
    GENERIC_ERROR = "GENERIC_ERROR"
