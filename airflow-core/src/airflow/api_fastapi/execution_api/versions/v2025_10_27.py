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

from airflow.api_fastapi.execution_api.datamodels.taskinstance import TIRunContext


class MakeDagRunConfNullable(VersionChange):
    """Make DagRun.conf field nullable to match database schema."""

    description = __doc__

    instructions_to_migrate_to_previous_version = ()

    @convert_response_to_previous_version_for(TIRunContext)  # type: ignore[arg-type]
    def ensure_conf_is_dict_in_dag_run(response: ResponseInfo) -> None:  # type: ignore[misc]
        """Ensure conf is always a dict (never None) in previous versions."""
        if "dag_run" in response.body and isinstance(response.body["dag_run"], dict):
            if response.body["dag_run"].get("conf") is None:
                response.body["dag_run"]["conf"] = {}
