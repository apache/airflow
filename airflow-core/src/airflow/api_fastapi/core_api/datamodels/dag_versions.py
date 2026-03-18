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

from collections.abc import Iterable
from datetime import datetime
from uuid import UUID

from pydantic import AliasPath, Field

from airflow.api_fastapi.core_api.base import BaseModel


class DagVersionResponse(BaseModel):
    """Dag Version serializer for responses."""

    id: UUID
    version_number: int
    dag_id: str
    bundle_name: str | None
    bundle_version: str | None
    created_at: datetime
    dag_display_name: str = Field(validation_alias=AliasPath("dag_model", "dag_display_name"))

    bundle_url: str | None = Field(validation_alias="bundle_url")


class DAGVersionCollectionResponse(BaseModel):
    """DAG Version Collection serializer for responses."""

    dag_versions: Iterable[DagVersionResponse]
    total_entries: int
