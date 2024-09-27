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

from sqlalchemy import func, select

from airflow.models.dag import DagModel
from airflow.models.dagrun import DagRun

latest_dag_run_per_dag_id_cte = (
    select(DagRun.dag_id, func.max(DagRun.start_date).label("start_date"))
    .where()
    .group_by(DagRun.dag_id)
    .cte()
)


dags_select_with_latest_dag_run = (
    select(DagModel)
    .join(
        latest_dag_run_per_dag_id_cte,
        DagModel.dag_id == latest_dag_run_per_dag_id_cte.c.dag_id,
        isouter=True,
    )
    .join(
        DagRun,
        DagRun.start_date == latest_dag_run_per_dag_id_cte.c.start_date
        and DagRun.dag_id == latest_dag_run_per_dag_id_cte.c.dag_id,
        isouter=True,
    )
)
