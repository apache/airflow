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
from flask import url_for

from airflow.exceptions import AirflowException
from airflow.models import DagBag, DagRun


def get_dag_runs(dag_id, state=None, run_url_route='Airflow.graph'):
    """
    Returns a list of Dag Runs for a specific DAG ID.
    :param dag_id: String identifier of a DAG
    :param state: queued|running|success...
    :return: List of DAG runs of a DAG with requested state,
    or all runs if the state is not specified
    """
    dagbag = DagBag()

    # Check DAG exists.
    if dag_id not in dagbag.dags:
        error_message = "Dag id {} not found".format(dag_id)
        raise AirflowException(error_message)

    dag_runs = list()
    state = state.lower() if state else None
    for run in DagRun.find(dag_id=dag_id, state=state):
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
