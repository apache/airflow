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

from sqlalchemy import case, inspect, select
from sqlalchemy.orm.attributes import set_committed_value

from airflow.api_fastapi.core_api.datamodels.event_logs import EventLogResponse
from airflow.models import Log
from airflow.models.task_coordinates import public_map_index_expression
from airflow.models.taskinstance import TaskInstance


def event_log_public_map_index():
    """Resolve an attributed execution's public index without guessing once its row has been purged."""
    attributed = (
        select(public_map_index_expression(TaskInstance))
        .where(TaskInstance.id == Log.task_instance_id)
        .correlate(Log)
        .scalar_subquery()
    )
    return case((Log.task_instance_id.is_(None), Log.map_index), else_=attributed).label("public_map_index")


def event_log_to_response(event_log: Log, *, public_map_index: int | None) -> EventLogResponse:
    # owner_display_name is stored when the action is logged (the API layer populates it; other Log
    # creation paths leave it unset). Resolve it once, at log time, and only fall back to the raw
    # owner here so the value stays stable no matter who views the entry later. set_committed_value
    # keeps the ORM object clean so this response-only fallback is never written back on commit.
    set_committed_value(event_log, "owner_display_name", event_log.owner_display_name or event_log.owner)

    # Null relationships that weren't eager-loaded so model validation cannot trigger a lazy load
    # (N+1) while resolving dag_display_name / task_display_name.
    unloaded: set[str] = inspect(event_log).unloaded
    for relationship_name in ("dag_model", "task_instance"):
        if relationship_name in unloaded:
            set_committed_value(event_log, relationship_name, None)

    response = EventLogResponse.model_validate(event_log)
    response.map_index = public_map_index
    return response
