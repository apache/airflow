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

from cadwyn import RequestInfo, VersionChange, convert_request_to_next_version_for, schema
from fastapi import HTTPException
from pydantic import JsonValue

from airflow.api_fastapi.compat import HTTP_422_UNPROCESSABLE_CONTENT
from airflow.api_fastapi.execution_api.datamodels.taskinstance import TIDeferredStatePayload


class RemoveStringTriggerKwargs(VersionChange):
    """Stop accepting `trigger_kwargs` as a string in TIDeferredStatePayload."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        schema(TIDeferredStatePayload).field("trigger_kwargs").had(type=dict[str, JsonValue] | str),
    )

    # Keyed by path: cadwyn 8 can't match a body-schema converter to one member of the TIStateUpdate union.
    @convert_request_to_next_version_for("/task-instances/{task_instance_id}/state", ["PATCH"])  # type: ignore[arg-type]
    def reject_string_trigger_kwargs(request: RequestInfo) -> None:
        """Reject a string from older clients; the server can't tell whether it is encrypted."""
        if isinstance(request.body, dict) and isinstance(request.body.get("trigger_kwargs"), str):
            raise HTTPException(
                status_code=HTTP_422_UNPROCESSABLE_CONTENT,
                detail={
                    "reason": "invalid_trigger_kwargs",
                    "message": "trigger_kwargs must be a serialized dict, not a string",
                },
            )
