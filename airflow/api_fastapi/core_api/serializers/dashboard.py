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


class DAGRunTypes(BaseModel):
    """DAG Run Types for responses."""

    backfill: int
    scheduled: int
    manual: int
    asset_triggered: int


class DAGRunStates(BaseModel):
    """DAG Run States for responses."""

    queued: int
    running: int
    success: int
    failed: int


class TaskInstanceState(BaseModel):
    """TaskInstance serializer for responses."""

    no_status: int
    removed: int
    scheduled: int
    queued: int
    running: int
    success: int
    restarting: int
    failed: int
    up_for_retry: int
    up_for_reschedule: int
    upstream_failed: int
    skipped: int
    deferred: int


class HistoricalMetricDataResponse(BaseModel):
    """Historical Metric Data serializer for responses."""

    dag_run_types: DAGRunTypes
    dag_run_states: DAGRunStates
    task_instance_states: TaskInstanceState
