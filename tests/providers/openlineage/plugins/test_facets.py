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

from airflow.providers.openlineage.plugins.facets import AirflowRunFacet


def test_airflow_run_facet():
    dag = {"dag_id": "123"}
    dag_run = {"dag_run_id": "456"}
    task = {"task_id": "789"}
    task_instance = {"task_instance_id": "000"}
    task_uuid = "XXX"

    airflow_run_facet = AirflowRunFacet(
        dag=dag, dagRun=dag_run, task=task, taskInstance=task_instance, taskUuid=task_uuid
    )

    assert airflow_run_facet.dag == dag
    assert airflow_run_facet.dagRun == dag_run
    assert airflow_run_facet.task == task
    assert airflow_run_facet.taskInstance == task_instance
    assert airflow_run_facet.taskUuid == task_uuid
