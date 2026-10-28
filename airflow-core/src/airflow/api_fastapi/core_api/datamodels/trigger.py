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

from datetime import datetime
from typing import Annotated

from pydantic import BeforeValidator, ConfigDict, Field

from airflow.api_fastapi.common.db.triggers import TriggerKind, TriggerStatus
from airflow.api_fastapi.core_api.base import BaseModel


def _remove_kwargs(_: object) -> str:
    """
    Return empty trigger kwargs for API responses.

    Trigger ``kwargs`` may contain sensitive values (for example credentials a deferred
    operator hands to its trigger -- an API key, a token), so they are never exposed through
    the REST API. The field is kept in the response schema for backwards compatibility -- so
    existing API consumers do not break on a missing property -- but it is always returned
    empty, as ``"{}"`` (the stringified empty dict, matching the string format the field has
    always used). The triggerer still decrypts and uses the real kwargs at runtime; only the
    API representation is emptied.
    """
    return "{}"


class TriggerResponse(BaseModel):
    """Trigger serializer for responses."""

    model_config = ConfigDict(populate_by_name=True)

    id: int
    classpath: str
    # Deprecated: always emptied (see ``_remove_kwargs``). Marked deprecated in the schema so
    # consumers are nudged off it. ``deprecated`` only warns on direct attribute access, not
    # during model serialization, so our own response rendering stays warning-free.
    kwargs: Annotated[str, BeforeValidator(_remove_kwargs), Field(deprecated=True)]
    created_date: datetime
    queue: str | None
    triggerer_id: int | None


class TriggerTaskInstanceReferenceResponse(BaseModel):
    """The task instance a trigger runs for."""

    dag_id: str
    dag_run_id: str
    task_id: str
    map_index: int
    try_number: int
    state: str | None


class TriggerCallbackReferenceResponse(BaseModel):
    """The callback a trigger runs."""

    id: str
    dag_id: str | None
    dag_run_id: str | None
    state: str | None


class TriggerAssetWatcherReferenceResponse(BaseModel):
    """An asset watcher a trigger serves."""

    asset_id: int
    asset_name: str
    asset_uri: str
    watcher_name: str


class TriggerDetailResponse(BaseModel):
    """A trigger, who it runs for, and how it is doing; never its kwargs."""

    id: int
    kind: TriggerKind = Field(
        description="What the trigger runs for: a deferred `task`, a `callback` or an `asset_watcher`."
    )
    classpath: str
    created_date: datetime
    queue: str | None
    team_name: str | None
    status: TriggerStatus = Field(
        description="`pending`: not picked up by a triggerer yet. `running`: on a live triggerer. "
        "`orphaned`: its triggerer stopped sending heartbeats; another triggerer will take it."
    )
    triggerer_hostname: str | None = Field(
        description="Host of the triggerer the trigger is assigned to; whether it is alive is the `orphaned` "
        "status."
    )
    dag_id: str | None = Field(description="The Dag of the task instance or callback the trigger runs for.")
    task_instance: TriggerTaskInstanceReferenceResponse | None
    callback: TriggerCallbackReferenceResponse | None
    asset_watchers: list[TriggerAssetWatcherReferenceResponse] = Field(
        description="The watchers the trigger serves, on the assets the caller can read."
    )


class TriggerCollectionResponse(BaseModel):
    """Trigger collection response."""

    triggers: list[TriggerDetailResponse]
    total_entries: int
