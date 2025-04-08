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

from enum import StrEnum
from typing import Any
from uuid import UUID

from airflow.api_fastapi.core_api.base import BaseModel


class RuntimeBindingsTokenType(StrEnum):
    """Type of a token used to fetch details (e.g. XCom, Connection, Variable) from the execution API.
    For synchronous tasks and triggers from sensor tasks this should be ``TASK_INSTANCE`` type.
    For asset triggers this should be ``ASSET_TRIGGER`` type (because there is no parent Task Instance to
    use). This is used in a custom claim `sub_type` in the token.
    """

    TASK_INSTANCE = "task_instance"
    ASSET_TRIGGER = "asset_trigger"


# TODO: This is a placeholder for Task Identity Token schema.
class RuntimeBindingsToken(BaseModel):
    """Task or Asset Trigger Identity Token for fetching runtime bindings (XCom, Connection, Variable)
    id: str should be set to TaskInstance UUID for tasks and triggers with parent Task Instances. For asset
            triggers this should be set to the asset id.
    type: RuntimeBindingsTokenType
    """

    id: str

    type: RuntimeBindingsTokenType = RuntimeBindingsTokenType.TASK_INSTANCE

    claims: dict[str, Any]
