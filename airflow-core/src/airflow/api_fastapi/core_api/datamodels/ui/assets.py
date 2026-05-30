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

from pydantic import Field

from airflow.api_fastapi.core_api.base import BaseModel
from airflow.api_fastapi.core_api.datamodels.common import AssetExpression


class NextRunAssetEventResponse(BaseModel):
    """An asset, and the time of its latest event, that a DAG's next run is waiting on."""

    id: int
    uri: str
    name: str
    # Serialized as ``lastUpdate`` for the UI; ``None`` until the asset has a qualifying event.
    last_update: datetime | None = Field(default=None, alias="lastUpdate")


class NextRunAssetsResponse(BaseModel):
    """Assets feeding a DAG's next run, with the scheduling expression that combines them."""

    asset_expression: AssetExpression | None = None
    events: list[NextRunAssetEventResponse]
    pending_partition_count: int | None = None
