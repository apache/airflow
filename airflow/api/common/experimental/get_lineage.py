#
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
"""Lineage APIs."""
from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING, Any

from airflow.api.common.experimental import check_and_get_dag, check_and_get_dagrun
from airflow.lineage import PIPELINE_INLETS, PIPELINE_OUTLETS
from airflow.models.xcom import XCom
from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    import datetime

    from sqlalchemy.orm import Session


@provide_session
def get_lineage(
    dag_id: str, execution_date: datetime.datetime, *, session: Session = NEW_SESSION
) -> dict[str, dict[str, Any]]:
    """Get lineage information for dag specified."""
    dag = check_and_get_dag(dag_id)
    dagrun = check_and_get_dagrun(dag, execution_date)

    inlets = XCom.get_many(dag_ids=dag_id, run_id=dagrun.run_id, key=PIPELINE_INLETS, session=session)
    outlets = XCom.get_many(dag_ids=dag_id, run_id=dagrun.run_id, key=PIPELINE_OUTLETS, session=session)

    lineage: dict[str, dict[str, Any]] = defaultdict(dict)
    for meta in inlets:
        lineage[meta.task_id]["inlets"] = meta.value
    for meta in outlets:
        lineage[meta.task_id]["outlets"] = meta.value

    return {"task_ids": dict(lineage)}
