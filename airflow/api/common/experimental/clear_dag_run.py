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
"""Clear DAG run API."""
from typing import Optional, List, Dict, Any

from flask import url_for

from airflow.api.common.experimental import check_and_get_dag
from airflow.exceptions import AirflowBadRequest
from airflow.models import DagRun, clear_task_instances
from airflow.utils.db import provide_session
from airflow.utils.state import State


@provide_session
def clear_dag_run(dag_id, dagrun_run_id, session=None):
    """
    clears the targetted dagrun and returns the DagRun object
    that was just cleared.

    :param dag_id: String identifier of a DAG
    :param dagrun_run_id: run_id of the dag run
    :param session: sqalchemy Session object to use for DB updates
    :return: DagRun object for the dag just cleared
    """
    dag = check_and_get_dag(dag_id=dag_id)

    runs = DagRun.find(dag_id=dag_id, run_id=dagrun_run_id)
    if not runs:
        raise AirflowBadRequest('No matching dagrun with run_id {0}'.format(dagrun_run_id))
    if len(runs) > 1:
        raise AirflowBadRequest('Multiple dagruns found with run_id {0}'.format(dagrun_run_id))

    dag_run = runs[0]

    task_instances = dag_run.get_task_instances()
    if not task_instances:
        raise AirflowBadRequest('dagrun with run_id {0} has no existing task instances to clear.'\
            .format(dagrun_run_id))

    clear_task_instances(task_instances, session, dag=dag)

    session.commit()

    return dag_run
