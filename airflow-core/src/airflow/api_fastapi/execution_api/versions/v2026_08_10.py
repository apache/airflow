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

from cadwyn import ResponseInfo, VersionChange, convert_response_to_previous_version_for


class UseProblemDetailsForTaskRunStateConflict(VersionChange):
    """Use RFC 9457 problem details for task run state conflict responses."""

    description = __doc__
    instructions_to_migrate_to_previous_version = ()

    @convert_response_to_previous_version_for(  # type: ignore[arg-type]
        "/task-instances/{task_instance_id}/run",
        ["PATCH"],
        migrate_http_errors=True,
    )
    def restore_legacy_task_run_state_conflict_error(response: ResponseInfo) -> None:  # type: ignore[misc]
        """Restore the legacy FastAPI HTTPException error shape for older clients."""
        if response.status_code != 409 or not isinstance(response.body, dict):
            return
        if response.body.get("reason") != "invalid_state" or response.body.get("previous_state") is None:
            return

        response.body = {
            "detail": {
                "reason": response.body["reason"],
                "message": response.body.get("detail"),
                "previous_state": response.body["previous_state"],
            }
        }
        response.headers["content-type"] = "application/json"
