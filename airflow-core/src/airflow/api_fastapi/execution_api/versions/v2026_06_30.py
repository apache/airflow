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

from cadwyn import ResponseInfo, VersionChange, convert_response_to_previous_version_for, endpoint, schema

from airflow.api_fastapi.execution_api.datamodels.taskinstance import TIRunContext


class AddVariableKeysEndpoint(VersionChange):
    """Add GET /variables/keys endpoint for listing variable keys with optional prefix filter."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (endpoint("/variables/keys", ["GET"]).didnt_exist,)


class AddQueuedDttmField(VersionChange):
    """Add ``queued_dttm`` field to TIRunContext."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (schema(TIRunContext).field("queued_dttm").didnt_exist,)

    @convert_response_to_previous_version_for(TIRunContext)  # type: ignore[arg-type]
    def remove_queued_dttm_field(response: ResponseInfo) -> None:  # type: ignore[misc]
        """Remove queued_dttm field for older API versions."""
        response.body.pop("queued_dttm", None)
