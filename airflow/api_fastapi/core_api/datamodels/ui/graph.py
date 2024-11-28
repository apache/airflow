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


class NodeValueResponse(BaseModel):
    """Graph Node Value responses."""

    isMapped: bool | None = None
    label: str | None = None
    labelStyle: str | None = None
    style: str | None = None
    tooltip: str | None = None
    rx: int
    ry: int
    clusterLabelPos: str | None = None
    setupTeardownType: Literal["setup", "teardown"] | None = None


class NodeResponse(BaseModel):
    """Node serializer for responses."""

    children: list[NodeResponse] | None = None
    id: str | None
    value: NodeValueResponse


class GraphDataResponse(BaseModel):
    """Graph Data serializer for responses."""

    edges: list[EdgeResponse]
    nodes: NodeResponse
    arrange: Literal["BT", "LR", "RL", "TB"]
