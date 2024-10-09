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

from typing import TYPE_CHECKING

from flask import g
from sqlalchemy import func, select

from airflow.api_connexion import security
from airflow.api_connexion.schemas.dag_stats_schema import (
    dag_stats_collection_schema,
)
from airflow.auth.managers.base_auth_manager import ResourceSetAccess
from airflow.auth.managers.models.resource_details import DagAccessEntity
from airflow.models.dag import DagRun
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.state import DagRunState
from airflow.www.extensions.init_auth_manager import get_auth_manager

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.api_connexion.types import APIResponse


@security.requires_access_dag("GET", DagAccessEntity.RUN)
@provide_session
def get_dag_stats(*, dag_ids: str, session: Session = NEW_SESSION) -> APIResponse:
    """Get Dag statistics."""
    allowed_dag_ids = get_auth_manager().get_accessible_dag_ids(method="GET", user=g.user)
    dags_list = set(dag_ids.split(","))

    if allowed_dag_ids != ResourceSetAccess.ALL:
        dags_list = dags_list.intersection(allowed_dag_ids)

    query = (
        select(DagRun.dag_id, DagRun.state, func.count(DagRun.state))
        .group_by(DagRun.dag_id, DagRun.state)
        .where(DagRun.dag_id.in_(dags_list))
    )
    dag_state_stats = session.execute(query)

    dag_state_data = {(dag_id, state): count for dag_id, state, count in dag_state_stats}
    dag_stats = {
        dag_id: [{"state": state, "count": dag_state_data.get((dag_id, state), 0)} for state in DagRunState]
        for dag_id in dags_list
    }

    dags = [{"dag_id": stat, "stats": dag_stats[stat]} for stat in dag_stats]
    return dag_stats_collection_schema.dump({"dags": dags, "total_entries": len(dag_stats)})
