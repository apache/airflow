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

from sqlalchemy import Select
from sqlalchemy.orm import contains_eager, joinedload

from airflow.api_fastapi.common.db.dags import eager_load_teams
from airflow.models.dag_version import DagVersion
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance


def eager_load_task_instance_for_validation(query: Select) -> Select:
    """
    Add JOINs and eager-loading options for TaskInstanceResponse.

    Adds ``join(dag_run)`` and ``outerjoin(dag_version)`` to the query and
    configures ``contains_eager`` so SQLAlchemy reuses those joins for
    populating the related objects (dag_run, dag_model, dag_version, bundle).
    This keeps the join logic centralised, avoids duplicate JOINs that would
    otherwise occur when combining explicit joins with ``joinedload``, and
    ensures ORDER BY / WHERE clauses on DagRun columns resolve correctly.

    :param query: The SELECT statement to augment.
    """
    return (
        query.join(TaskInstance.dag_run)
        .outerjoin(TaskInstance.dag_version)
        .options(
            contains_eager(TaskInstance.dag_run).options(
                joinedload(DagRun.dag_model).options(*eager_load_teams()),
            ),
            contains_eager(TaskInstance.dag_version).options(joinedload(DagVersion.bundle)),
            joinedload(TaskInstance.task_instance_note),
            joinedload(TaskInstance.rendered_task_instance_fields),
        )
    )
