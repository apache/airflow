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

import pickle

import attrs
import pytest

from airflow.providers.openlineage.plugins.facets import (
    AirflowDagRunFacet,
    AirflowJobFacet,
    AirflowRunFacet,
    AirflowStateRunFacet,
)


def test_airflow_run_facet():
    dag = {"dag_id": "123"}
    dag_run = {"dag_run_id": "456"}
    task = {"task_id": "789"}
    task_instance = {"task_instance_id": "000"}
    task_uuid = "XXX"

    airflow_run_facet = AirflowRunFacet(
        dag=dag,
        dagRun=dag_run,
        task=task,
        taskInstance=task_instance,
        taskUuid=task_uuid,
    )

    assert airflow_run_facet.dag == dag
    assert airflow_run_facet.dagRun == dag_run
    assert airflow_run_facet.task == task
    assert airflow_run_facet.taskInstance == task_instance
    assert airflow_run_facet.taskUuid == task_uuid


def test_airflow_dag_run_facet():
    dag = {"dag_id": "123"}
    dag_run = {"dag_run_id": "456"}

    airflow_dag_run_facet = AirflowDagRunFacet(
        dag=dag,
        dagRun=dag_run,
    )

    assert airflow_dag_run_facet.dag == dag
    assert airflow_dag_run_facet.dagRun == dag_run


@pytest.mark.parametrize(
    "instance",
    [
        pytest.param(
            AirflowJobFacet(
                taskTree={"task_0": {"section_1.task_3": {}}},
                taskGroups={
                    "section_1": {
                        "parent_group": None,
                        "tooltip": "",
                        "ui_color": "CornflowerBlue",
                        "ui_fgcolor": "#000",
                        "ui_label": "section_1",
                    }
                },
                tasks={
                    "task_0": {
                        "operator": "airflow.providers.standard.core.operators.bash.BashOperator",
                        "task_group": None,
                        "emits_ol_events": True,
                        "ui_color": "#f0ede4",
                        "ui_fgcolor": "#000",
                        "ui_label": "task_0",
                        "is_setup": False,
                        "is_teardown": False,
                    }
                },
            ),
            id="AirflowJobFacet",
        ),
        pytest.param(
            AirflowStateRunFacet(dagRunState="SUCCESS", tasksState={"task_0": "SKIPPED"}),
            id="AirflowStateRunFacet",
        ),
        pytest.param(
            AirflowDagRunFacet(
                dag={
                    "timetable": {"delta": 86400.0},
                    "owner": "airflow",
                    "start_date": "2024-06-01T00:00:00+00:00",
                },
                dagRun={"conf": {}, "dag_id": "dag_id"},
            ),
            id="AirflowDagRunFacet",
        ),
    ],
)
def test_facets_are_pickled_correctly(instance):
    cls = instance.__class__
    instance = pickle.loads(pickle.dumps(instance))
    for field in attrs.fields(cls):
        getattr(instance, field.name)
