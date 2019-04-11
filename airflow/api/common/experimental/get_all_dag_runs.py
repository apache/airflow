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

from airflow.models import DagRun
from airflow.utils import timezone


def get_all_dag_runs(dag_id=None, state=None, state_not_equal=None, execution_date_before=None,
                     execution_date_after=None):
    """
    Returns a list of Dag Runs for a specific DAG ID.
    :param dag_id: String identifier of a DAG
    :param state: queued|running|success...
    :param execution_date_before: a query string parameter to find all runs before provided date,
    should be in format "YYYY-mm-DDTHH:MM:SS", for example: "2016-11-16T11:34:15".'
    :param execution_date_after: a query string parameter to find all runs after provided date,
    should be in format "YYYY-mm-DDTHH:MM:SS", for example: "2016-11-16T11:34:15".'
    :return: List of DAG runs of a DAG with requested state,
    or all runs if the state is not specified
    """

    dag_runs = list()
    state = state.lower() if state else None
    state_not_equal = state_not_equal.lower() if state_not_equal else None
    execution_date_before = timezone.parse(execution_date_before) if execution_date_before else None
    execution_date_after = timezone.parse(execution_date_after) if execution_date_after else None
    for run in DagRun.find(dag_id=dag_id, state=state, state_not_equal=state_not_equal,
                           execution_date_before=execution_date_before,
                           execution_date_after=execution_date_after):
        dag_runs.append({
            'id': run.id,
            'run_id': run.run_id,
            'state': run.state,
            'dag_id': run.dag_id,
            'execution_date': run.execution_date.isoformat(),
            'start_date': ((run.start_date or '') and run.start_date.isoformat()),
            'end_date': ((run.end_date or '') and run.end_date.isoformat()),
            'dag_run_url': url_for('Airflow.graph', dag_id=run.dag_id,
                                   execution_date=run.execution_date)
        })

    return dag_runs
