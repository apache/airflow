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
"""Supervisor schema version 2027-02-28."""

from __future__ import annotations

from cadwyn import RequestInfo, VersionChange, convert_request_to_next_version_for, schema

from airflow.sdk.api.datamodels._generated import VariableResponse
from airflow.sdk.execution_time.comms import PutVariable


class MakeSupervisorVariableValueNonNullable(VersionChange):
    """
    Make the Variable value non-nullable in ``VariableResult`` and ``PutVariable``.

    The supervisor-schema mirror of the execution API's ``MakeVariableValueNonNullable``,
    named apart so the two migrations are not confused.
    """

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        # VariableResult (and TIRunContext.variables) inherit the field from the generated
        # VariableResponse, and Cadwyn only changes a field on the class that declares it.
        schema(VariableResponse).field("value").had(type=str | None),
        schema(PutVariable).field("value").had(type=str | None),
    )

    @convert_request_to_next_version_for(PutVariable)  # type: ignore[arg-type]
    def store_null_value_as_none_string(request: RequestInfo) -> None:
        """Keep the old behavior of storing ``str(None)`` when an older runtime sets null."""
        if request.body.get("value") is None:
            request.body["value"] = "None"
