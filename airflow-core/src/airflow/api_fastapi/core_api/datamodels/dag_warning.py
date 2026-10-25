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

from pydantic import AliasPath, Field, model_validator

from airflow.api_fastapi.core_api.base import BaseModel
from airflow.models.dagwarning import DagWarningTypeValue


class DAGWarningResponse(BaseModel):
    """Dag Warning serializer for responses."""

    dag_id: str
    warning_type: DagWarningTypeValue
    message: str
    timestamp: datetime
    dag_display_name: str | None = Field(
        default=None, validation_alias=AliasPath("dag_model", "dag_display_name")
    )


@model_validator(mode="after")
def default_dag_display_name(self) -> DAGWarningResponse:
    if not self.dag_display_name:
        self.dag_display_name = self.dag_id
    return self


class DAGWarningCollectionResponse(BaseModel):
    """Dag warning collection serializer for responses."""

    dag_warnings: Iterable[DAGWarningResponse]
    total_entries: int
