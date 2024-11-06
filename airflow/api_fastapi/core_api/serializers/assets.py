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

from pydantic import BaseModel, ConfigDict, Field


class DagScheduleAssetReference(BaseModel):
    """Serializable version of the DagScheduleAssetReference ORM SqlAlchemyModel."""

    asset_id: int
    dag_id: str
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class TaskOutletAssetReference(BaseModel):
    """Serializable version of the TaskOutletAssetReference ORM SqlAlchemyModel."""

    asset_id: int
    dag_id: str
    task_id: str
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class AssetResponse(BaseModel):
    """Asset serializer for responses."""

    id: int
    uri: str
    extra: str | None = Field(default=None)
    created_at: str
    updated_at: str
    consuming_dags: DagScheduleAssetReference | None = None
    producing_tasks: TaskOutletAssetReference | None = None


class AssetCollectionResponse(BaseModel):
    """Asset collection response."""

    assets: list[AssetResponse]
    total_entries: int
