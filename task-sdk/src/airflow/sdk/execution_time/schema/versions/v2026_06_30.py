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

from cadwyn import ResponseInfo, VersionChange, convert_response_to_previous_version_for, schema

from airflow.sdk.execution_time.comms import SucceedTask, TaskState


class AddLanguageSDKLineageField(VersionChange):
    """Add optional `lineage` field on `SucceedTask` and `TaskState` for lang-SDK runtimes."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        schema(SucceedTask).field("lineage").didnt_exist,
        schema(TaskState).field("lineage").didnt_exist,
    )

    @convert_response_to_previous_version_for(SucceedTask)  # type: ignore[arg-type]
    def drop_lineage_from_succeed_task(response: ResponseInfo) -> None:  # type: ignore[misc]
        response.body.pop("lineage", None)

    @convert_response_to_previous_version_for(TaskState)  # type: ignore[arg-type]
    def drop_lineage_from_task_state(response: ResponseInfo) -> None:  # type: ignore[misc]
        response.body.pop("lineage", None)
