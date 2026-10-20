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

"""
Hand-written asset state store datamodels.

The 3.2.x core API does not expose the asset state store endpoints, so these models cannot be produced
by ``datamodel-codegen``. Once core provides the API, move them back into ``generated.py``.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field

from airflowctl.api.datamodels.generated import JsonValue  # noqa: TC001 - pydantic needs it at runtime


class AssetStateStoreWriterKind(str, Enum):
    """
    Identifies what kind of writer last updated an asset state store entry.

    ``TASK`` — written by a task via the execution API.
    ``WATCHER`` — written by a ``BaseEventTrigger`` (no task instance).
    ``API`` — written directly through the Core API (e.g. manual admin write).
    """

    TASK = "task"
    WATCHER = "watcher"
    API = "api"


class AssetStateStoreBody(BaseModel):
    """Request body for setting an asset state store value."""

    model_config = ConfigDict(
        extra="forbid",
    )
    value: JsonValue


class AssetStateStoreLastUpdatedBy(BaseModel):
    """Writer info for the last write to an asset state store entry."""

    kind: AssetStateStoreWriterKind
    dag_id: Annotated[str | None, Field(title="Dag Id")] = None
    run_id: Annotated[str | None, Field(title="Run Id")] = None
    task_id: Annotated[str | None, Field(title="Task Id")] = None
    map_index: Annotated[int | None, Field(title="Map Index")] = None


class AssetStateStoreResponse(BaseModel):
    """A single asset state store key/value pair with metadata."""

    key: Annotated[str, Field(title="Key")]
    value: JsonValue
    updated_at: Annotated[datetime, Field(title="Updated At")]
    last_updated_by: AssetStateStoreLastUpdatedBy | None = None


class AssetStateStoreCollectionResponse(BaseModel):
    """All asset state store entries for an asset."""

    asset_state_store: Annotated[list[AssetStateStoreResponse], Field(title="Asset State Store")]
    total_entries: Annotated[int, Field(title="Total Entries")]
