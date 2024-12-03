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

from typing import Literal

from airflow.api_fastapi.core_api.base import BaseModel


class EdgeResponse(BaseModel):
    """Edge serializer for responses."""

    is_setup_teardown: bool | None = None
    label: str | None = None
    source_id: str
    target_id: str


class NodeResponse(BaseModel):
    """Node serializer for responses."""

    children: list[NodeResponse] | None = None
    id: str | None
    is_mapped: bool | None = None
    label: str | None = None
    tooltip: str | None = None
    setup_teardown_type: Literal["setup", "teardown"] | None = None
    type: Literal["join", "sensor", "task", "task_group"]


class StructureDataResponse(BaseModel):
    """Structure Data serializer for responses."""

    edges: list[EdgeResponse]
    nodes: list[NodeResponse]
    arrange: Literal["BT", "LR", "RL", "TB"]
