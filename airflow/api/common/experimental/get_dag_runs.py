# -*- coding: utf-8 -*-
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
from typing import Optional, List, Dict, Any

from flask import url_for

from airflow.api.common.experimental import check_and_get_dag
from airflow.models import DagRun


def get_dag_runs(dag_id, state=None, run_url_route='Airflow.graph', **kwargs):
    """
    Returns a list of Dag Runs for a specific DAG ID.

    :param dag_id: String identifier of a DAG
    :param dagrun_id: String identifier of a run_id
    :param execution_date_gte: datetime for which to return all objects greater than or equal to
    :param state: queued|running|success...
    :return: List of DAG runs of a DAG with requested state,
        or all runs if the state is not specified
    """
    check_and_get_dag(dag_id=dag_id)

    dag_runs = list()
    state = state.lower() if state else None
    for run in DagRun.find(
        dag_id=dag_id,
        state=state,
        run_id=kwargs.get('dagrun_id'),
        execution_date_gte=kwargs.get('execution_date_gte')
    ):
        dag_runs.append({
            'id': run.id,
            'run_id': run.run_id,
            'state': run.state,
            'dag_id': run.dag_id,
            'execution_date': run.execution_date.isoformat(),
            'start_date': ((run.start_date or '') and
                           run.start_date.isoformat()),
            'dag_run_url': url_for(run_url_route, dag_id=run.dag_id,
                                   execution_date=run.execution_date)
        })

    return dag_runs
