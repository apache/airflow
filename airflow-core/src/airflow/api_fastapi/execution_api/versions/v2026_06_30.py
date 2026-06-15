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

from cadwyn import VersionChange, endpoint, schema

from airflow.api_fastapi.execution_api.datamodels.taskinstance import (
    TaskInstance,
    TIAwaitingInputStatePayload,
)


class AddVariableKeysEndpoint(VersionChange):
    """Add GET /variables/keys endpoint for listing variable keys with optional prefix filter."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (endpoint("/variables/keys", ["GET"]).didnt_exist,)


class AddConnectionTestEndpoint(VersionChange):
    """Add connection-tests endpoints for the async connection-test workflow."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        endpoint("/connection-tests/{connection_test_id}", ["PATCH"]).didnt_exist,
        endpoint("/connection-tests/{connection_test_id}/connection", ["GET"]).didnt_exist,
    )


class AddTaskInstanceQueueField(VersionChange):
    """Add the `queue` field to the TaskInstance model."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (schema(TaskInstance).field("queue").didnt_exist,)


class AddAwaitingInputStatePayload(VersionChange):
    """Add the awaiting_input task instance state transition payload (Human-in-the-loop, no trigger)."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        schema(TIAwaitingInputStatePayload).field("state").didnt_exist,
        schema(TIAwaitingInputStatePayload).field("timeout").didnt_exist,
        schema(TIAwaitingInputStatePayload).field("next_method").didnt_exist,
        schema(TIAwaitingInputStatePayload).field("next_kwargs").didnt_exist,
        schema(TIAwaitingInputStatePayload).field("rendered_map_index").didnt_exist,
    )
