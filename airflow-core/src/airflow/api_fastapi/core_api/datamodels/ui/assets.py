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
from airflow.api_fastapi.core_api.datamodels.common import MaybeAssetExpression


class NextRunAssetEventResponse(BaseModel):
    """One asset event in the ``next_run_assets`` payload."""

    id: int
    name: str | None
    uri: str
    last_update: datetime | None = None
    received_count: int = 0
    required_count: int = 1
    received_keys: list[str] = Field(default_factory=list)
    required_keys: list[str] = Field(default_factory=list)
    is_rollup: bool = False
    mapper_error: bool = False
    """True when the rollup mapper raised; the asset is not-yet-satisfied and
    counts / keys are placeholders. The scheduler holds the Dag run for this
    asset — UIs should surface this state rather than treating it as "waiting"."""
    asset_inactive: bool = False
    """True when the upstream asset has been deactivated (orphaned — no Dag
    declares it any more). The scheduler freezes partition evaluation for any
    APDR depending on this asset; the UI should surface this state rather
    than treating it as "waiting"."""


class NextRunAssetsResponse(BaseModel):
    """Response for the ``next_run_assets`` endpoint."""

    asset_expression: MaybeAssetExpression = None
    events: list[NextRunAssetEventResponse]
    pending_partition_count: int | None = None
