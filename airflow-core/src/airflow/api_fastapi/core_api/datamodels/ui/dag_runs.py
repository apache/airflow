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

from pydantic import computed_field

from airflow.api_fastapi.core_api.base import BaseModel
from airflow.utils.state import DagRunState


class DAGRunLightResponse(BaseModel):
    """DAG Run serializer for responses."""

    id: int
    dag_id: str
    run_id: str
    logical_date: datetime | None
    run_after: datetime
    start_date: datetime | None
    end_date: datetime | None
    state: DagRunState

    @computed_field
    def duration(self) -> float | None:
        if self.end_date and self.start_date:
            return (self.end_date - self.start_date).total_seconds()
        return None
