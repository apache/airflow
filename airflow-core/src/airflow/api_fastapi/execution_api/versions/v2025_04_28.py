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

from cadwyn import VersionChange, schema

from airflow.api_fastapi.execution_api.datamodels.taskinstance import (
    TIDeferredStatePayload,
    TIRetryStatePayload,
    TISuccessStatePayload,
    TITerminalStatePayload,
)


class AddRenderedMapIndexField(VersionChange):
    """Add the `rendered_map_index` field to payload models."""

    description = __doc__

    instructions_to_migrate_to_previous_version = (
        schema(TITerminalStatePayload).field("rendered_map_index").didnt_exist,
        schema(TISuccessStatePayload).field("rendered_map_index").didnt_exist,
        schema(TIDeferredStatePayload).field("rendered_map_index").didnt_exist,
        schema(TIRetryStatePayload).field("rendered_map_index").didnt_exist,
    )
