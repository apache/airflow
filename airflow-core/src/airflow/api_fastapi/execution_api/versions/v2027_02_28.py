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

from cadwyn import RequestInfo, VersionChange, convert_request_to_next_version_for, endpoint, schema

from airflow.api_fastapi.execution_api.datamodels.variable import VariablePostBody, VariableResponse


class AddDagProcessorJobEndpoints(VersionChange):
    """Add Dag processor lifecycle and parse-token endpoints."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        endpoint("/jobs", ["POST"]).didnt_exist,
        endpoint("/jobs/{job_id}/heartbeat", ["POST"]).didnt_exist,
        endpoint("/jobs/{job_id}/complete", ["POST"]).didnt_exist,
        endpoint("/jobs/{job_id}/parse-token", ["POST"]).didnt_exist,
    )


class MakeVariableValueNonNullable(VersionChange):
    """
    Make the Variable value non-nullable in reads and writes.

    A Variable read never returned null; a missing Variable is a 404. Setting a Variable to null
    stored the string ``"None"``. Older clients may still send null, which keeps doing that.
    """

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        schema(VariableResponse).field("val").had(type=str | None),
        schema(VariablePostBody).field("value").had(type=str | None),
    )

    @convert_request_to_next_version_for(VariablePostBody)  # type: ignore[arg-type]
    def store_null_value_as_none_string(request: RequestInfo) -> None:
        """Keep the old behavior of storing ``str(None)`` when an older client sets null."""
        if request.body.get("val") is None:
            request.body["val"] = "None"
