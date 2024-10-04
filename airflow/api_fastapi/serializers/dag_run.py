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

from pydantic import BaseModel


class DAGRunResponse(BaseModel):
    """DAG Run serializer for responses."""

    dag_run_id: str | None
    dag_id: str
    logical_date: str | None
    start_date: str | None
    end_date: str | None
    data_interval_start: str | None
    data_interval_end: str | None
    last_scheduling_decision: str | None
    run_type: str  # Enum
    state: str  # Enum
    external_trigger: bool
    conf: dict
    notes: str | None
