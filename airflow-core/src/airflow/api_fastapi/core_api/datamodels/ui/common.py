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

from typing import Generic, Literal, TypeVar

from airflow.api_fastapi.core_api.base import BaseModel


class BaseEdgeResponse(BaseModel):
    """Base Edge serializer for responses."""

    source_id: str
    target_id: str


class BaseNodeResponse(BaseModel):
    """Base Node serializer for responses."""

    id: str
    label: str
    type: Literal[
        "join",
        "task",
        "asset-condition",
        "asset",
        "asset-alias",
        "asset-name-ref",
        "asset-uri-ref",
        "dag",
        "sensor",
        "trigger",
    ]


E = TypeVar("E", bound=BaseEdgeResponse)
N = TypeVar("N", bound=BaseNodeResponse)


class BaseGraphResponse(BaseModel, Generic[E, N]):
    """Base Graph serializer for responses."""

    edges: list[E]
    nodes: list[N]
