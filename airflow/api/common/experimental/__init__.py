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
"""Experimental APIs."""
from datetime import datetime
from typing import Optional

from airflow.exceptions import DagNotFound, DagRunNotFound, TaskNotFound
from airflow.models import DagBag, DagModel, DagRun


def check_and_get_dag(dag_id: str, task_id: Optional[str] = None) -> DagModel:
    """Checks that DAG exists and in case it is specified that Task exist"""
    dag_model = DagModel.get_current(dag_id)
    if dag_model is None:
        raise DagNotFound("Dag id {} not found in DagModel".format(dag_id))

    def read_store_serialized_dags():
        from airflow.configuration import conf
        return conf.getboolean('core', 'store_serialized_dags')
    dagbag = DagBag(
        dag_folder=dag_model.fileloc,
        store_serialized_dags=read_store_serialized_dags()
    )
    dag = dagbag.get_dag(dag_id)  # prefetch dag if it is stored serialized
    if dag_id not in dagbag.dags:
        error_message = "Dag id {} not found".format(dag_id)
        raise DagNotFound(error_message)
    if task_id and not dag.has_task(task_id):
        error_message = 'Task {} not found in dag {}'.format(task_id, dag_id)
        raise TaskNotFound(error_message)
    return dag


def check_and_get_dagrun(dag: DagModel, execution_date: datetime) -> DagRun:
    """Get DagRun object and check that it exists"""
    dagrun = dag.get_dagrun(execution_date=execution_date)
    if not dagrun:
        error_message = ('Dag Run for date {} not found in dag {}'
                         .format(execution_date, dag.dag_id))
        raise DagRunNotFound(error_message)
    return dagrun
