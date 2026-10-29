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

The first version after 2026-10-30, which shipped with Airflow 3.4.0 and no longer takes
changes. The rules of the previous version file apply here too: a brand-new message body
needs no instructions, only a change to a body that already existed does.
"""

from __future__ import annotations

from cadwyn import ResponseInfo, VersionChange, convert_response_to_previous_version_for, enum

from airflow.sdk.exceptions import ErrorType
from airflow.sdk.execution_time.comms import ErrorResponse


class AddXComBackendErrorType(VersionChange):
    """Report a failure of the configured XCom backend under its own error type."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (enum(ErrorType).didnt_have("XCOM_BACKEND_ERROR"),)

    @convert_response_to_previous_version_for(ErrorResponse)  # type: ignore[arg-type]
    def report_as_a_generic_error(response: ResponseInfo = None) -> None:  # type: ignore[assignment]
        """Report the failure as a generic one to a runtime whose version has no type for it."""
        if response.body.get("error") == ErrorType.XCOM_BACKEND_ERROR.value:
            response.body["error"] = ErrorType.GENERIC_ERROR.value
