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

from typing import Any

from cadwyn import ResponseInfo, VersionChange, convert_response_to_previous_version_for, endpoint, schema
from pydantic import ConfigDict, TypeAdapter, ValidationError

from airflow.api_fastapi.execution_api.datamodels.taskinstance import TIRunContext

_CONF_ADAPTER = TypeAdapter(dict[str, Any] | None, config=ConfigDict(hide_input_in_errors=True))


class AddDagProcessorJobEndpoints(VersionChange):
    """Add Dag processor lifecycle and parse-token endpoints."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        endpoint("/jobs", ["POST"]).didnt_exist,
        endpoint("/jobs/{job_id}/heartbeat", ["POST"]).didnt_exist,
        endpoint("/jobs/{job_id}/complete", ["POST"]).didnt_exist,
        endpoint("/jobs/{job_id}/parse-token", ["POST"]).didnt_exist,
    )


class SerializeTaskStartDagRunConf(VersionChange):
    """Keep task-start configuration serialized until the executing task needs it."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        schema(TIRunContext).field("serialized_dag_run_conf").didnt_exist,
    )

    @convert_response_to_previous_version_for(TIRunContext)  # type: ignore[arg-type]
    def restore_conf_for_older_clients(response: ResponseInfo) -> None:
        serialized = response.body.pop("serialized_dag_run_conf", None)
        if serialized is not None:
            try:
                response.body["dag_run"]["conf"] = _CONF_ADAPTER.validate_json(serialized)
            except ValidationError:
                raise ValueError("Serialized Dag run configuration must be a JSON object or null") from None
