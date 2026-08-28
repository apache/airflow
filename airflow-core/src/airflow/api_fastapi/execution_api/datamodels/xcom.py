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

from pydantic import Field, JsonValue, RootModel

from airflow.api_fastapi.core_api.base import BaseModel

# Each item expands to 3 SQL bind parameters (task_id, key, map_index); this bounds a
# single batch request to at most 3000 bind parameters, safely under Postgres/MySQL/
# modern-SQLite limits regardless of how many kwargs a caller's .expand() call has.
MAX_XCOM_BATCH_ITEMS = 1000


class XComResponse(BaseModel):
    """XCom schema for responses with fields that are needed for Runtime."""

    key: str
    value: JsonValue
    """The returned XCom value in a JSON-compatible format."""


class XComSequenceIndexResponse(RootModel):
    """XCom schema with minimal structure for index-based access."""

    root: JsonValue


class XComSequenceSliceResponse(RootModel):
    """XCom schema with minimal structure for slice-based access."""

    root: list[JsonValue]


class XComBatchItemRequest(BaseModel):
    """One XCom lookup within a batch request."""

    task_id: str
    key: str
    map_index: int = -1


class XComBatchRequestBody(BaseModel):
    """Body for a batch XCom lookup, scoped to a single dag_id/run_id."""

    items: list[XComBatchItemRequest] = Field(max_length=MAX_XCOM_BATCH_ITEMS)


class XComBatchItemResponse(BaseModel):
    """One XCom lookup result within a batch response."""

    task_id: str
    key: str
    map_index: int
    found: bool
    value: JsonValue = None
    """The returned XCom value in a JSON-compatible format. Meaningless when ``found`` is False."""


class XComBatchResponse(BaseModel):
    """Batch XCom lookup response, ordered the same as the request's items."""

    items: list[XComBatchItemResponse]
