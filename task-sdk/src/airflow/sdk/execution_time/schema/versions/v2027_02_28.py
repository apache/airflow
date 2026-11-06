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

"""
Supervisor schema version 2027-02-28.

New bodies in this version:

- ``HITLDetailResponseResult``: the supervisor's reply when a running task reads its own
  Human-in-the-loop response.
"""

from __future__ import annotations

from cadwyn import (
    ResponseInfo,
    VersionChange,
    convert_response_to_previous_version_for,
    enum,
    schema,
)

from airflow.sdk.api.datamodels._generated import TIRunContext
from airflow.sdk.exceptions import ErrorType
from airflow.sdk.execution_time.comms import ErrorResponse


class AddExecutionTimeoutToSupervisorTIRunContext(VersionChange):
    """
    Add the ``execution_timeout`` field carrying the task's timeout, which the supervisor enforces.

    The supervisor-schema mirror of the execution API's ``AddExecutionTimeoutToTIRunContext``,
    named apart so the two migrations are not confused.
    """

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        schema(TIRunContext).field("execution_timeout").didnt_exist,
    )


class AddXComBackendErrorType(VersionChange):
    """Report a failure of the configured XCom backend under its own error type."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (enum(ErrorType).didnt_have("XCOM_BACKEND_ERROR"),)

    @convert_response_to_previous_version_for(ErrorResponse)  # type: ignore[arg-type]
    def report_as_a_generic_error(response: ResponseInfo = None) -> None:  # type: ignore[assignment]
        """Report the failure as a generic one to a runtime whose version has no type for it."""
        if response.body.get("error") == ErrorType.XCOM_BACKEND_ERROR.value:
            response.body["error"] = ErrorType.GENERIC_ERROR.value
