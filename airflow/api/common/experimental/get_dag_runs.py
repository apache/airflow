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
"""DAG runs APIs."""
from typing import Any, Dict, List, Optional

from flask import url_for

from airflow.api.common.experimental import check_and_get_dag
from airflow.models import DagRun


def get_dag_runs(dag_id: str, state: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Returns a list of Dag Runs for a specific DAG ID.

    :param dag_id: String identifier of a DAG
    :param state: queued|running|success...
    :return: List of DAG runs of a DAG with requested state,
        or all runs if the state is not specified
    """
    check_and_get_dag(dag_id=dag_id)

    dag_runs = []
    state = state.lower() if state else None
    for run in DagRun.find(dag_id=dag_id, state=state):
        dag_runs.append(
            {
                'id': run.id,
                'run_id': run.run_id,
                'state': run.state,
                'dag_id': run.dag_id,
                'execution_date': run.execution_date.isoformat(),
                'start_date': ((run.start_date or '') and run.start_date.isoformat()),
                'dag_run_url': url_for('Airflow.graph', dag_id=run.dag_id, execution_date=run.execution_date),
            }
        )

    return dag_runs
