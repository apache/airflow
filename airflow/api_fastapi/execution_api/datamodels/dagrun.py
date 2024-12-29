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

# This model is not used in the API, but it is included in generated OpenAPI schema
# for use in the client SDKs.
from __future__ import annotations

from airflow.api_fastapi.common.types import UtcDateTime
from airflow.api_fastapi.core_api.base import BaseModel


class DagRun(BaseModel):
    """Schema for TaskInstance model with minimal required fields needed for OL for now."""

    id: int
    dag_id: str
    run_id: str
    logical_date: UtcDateTime
    data_interval_start: UtcDateTime
    data_interval_end: UtcDateTime
    clear_number: int
