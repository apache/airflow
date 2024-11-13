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

import os
import unittest
from datetime import datetime

import pytest

from airflow.models.dag import DAG
from airflow.models.dagbag import DagBag
from airflow.models.expandinput import EXPAND_INPUT_EMPTY
from airflow.models.serialized_dag import SerializedDagModel
from airflow.operators.empty import EmptyOperator

from tests_common.test_utils.db import clear_db_dags, clear_db_runs, clear_db_serialized_dags

pytestmark = pytest.mark.db_test


class TestTaskEndpoint:
    dag_id = "test_dag"
    mapped_dag_id = "test_mapped_task"
    unscheduled_dag_id = "test_unscheduled_dag"
    task_id = "op1"
    task_id2 = "op2"
    task_id3 = "op3"
    mapped_task_id = "mapped_task"
    unscheduled_task_id1 = "unscheduled_task_1"
    unscheduled_task_id2 = "unscheduled_task_2"
    task1_start_date = datetime(2020, 6, 15)
    task2_start_date = datetime(2020, 6, 16)

    def create_dags(self, test_client):
        with DAG(self.dag_id, schedule=None, start_date=self.task1_start_date, doc_md="details") as dag:
            task1 = EmptyOperator(task_id=self.task_id, params={"foo": "bar"})
            task2 = EmptyOperator(task_id=self.task_id2, start_date=self.task2_start_date)

        with DAG(self.mapped_dag_id, schedule=None, start_date=self.task1_start_date) as mapped_dag:
            EmptyOperator(task_id=self.task_id3)
            # Use the private _expand() method to avoid the empty kwargs check.
            # We don't care about how the operator runs here, only its presence.
            EmptyOperator.partial(task_id=self.mapped_task_id)._expand(EXPAND_INPUT_EMPTY, strict=False)

        with DAG(self.unscheduled_dag_id, start_date=None, schedule=None) as unscheduled_dag:
            task4 = EmptyOperator(task_id=self.unscheduled_task_id1, params={"is_unscheduled": True})
            task5 = EmptyOperator(task_id=self.unscheduled_task_id2, params={"is_unscheduled": True})

        task1 >> task2
        task4 >> task5
        dag_bag = DagBag(os.devnull, include_examples=False)
        dag_bag.dags = {
            dag.dag_id: dag,
            mapped_dag.dag_id: mapped_dag,
            unscheduled_dag.dag_id: unscheduled_dag,
        }
        test_client.app.state.dag_bag = dag_bag

    @staticmethod
    def clear_db():
        clear_db_runs()
        clear_db_dags()
        clear_db_serialized_dags()

    @pytest.fixture(autouse=True)
    def setup(self, test_client) -> None:
        self.clear_db()
        self.create_dags(test_client)

    def teardown_method(self) -> None:
        self.clear_db()


class TestGetTask(TestTaskEndpoint):
    def test_should_respond_200(self, test_client):
        expected = {
            "class_ref": {
                "class_name": "EmptyOperator",
                "module_path": "airflow.operators.empty",
            },
            "depends_on_past": False,
            "downstream_task_ids": [self.task_id2],
            "end_date": None,
            "execution_timeout": None,
            "extra_links": [],
            "operator_name": "EmptyOperator",
            "owner": "airflow",
            "params": {
                "foo": {
                    "__class": "airflow.models.param.Param",
                    "value": "bar",
                    "description": None,
                    "schema": {},
                }
            },
            "pool": "default_pool",
            "pool_slots": 1.0,
            "priority_weight": 1.0,
            "queue": "default",
            "retries": 0.0,
            "retry_delay": {"__type": "TimeDelta", "days": 0, "seconds": 300, "microseconds": 0},
            "retry_exponential_backoff": False,
            "start_date": "2020-06-15T00:00:00Z",
            "task_id": "op1",
            "task_display_name": "op1",
            "template_fields": [],
            "trigger_rule": "all_success",
            "ui_color": "#e8f7e4",
            "ui_fgcolor": "#000",
            "wait_for_downstream": False,
            "weight_rule": "downstream",
            "is_mapped": False,
            "doc_md": None,
        }
        response = test_client.get(
            f"/public/dags/{self.dag_id}/tasks/{self.task_id}",
        )
        assert response.status_code == 200
        assert response.json() == expected

    def test_mapped_task(self, test_client):
        expected = {
            "class_ref": {"class_name": "EmptyOperator", "module_path": "airflow.operators.empty"},
            "depends_on_past": False,
            "downstream_task_ids": [],
            "end_date": None,
            "execution_timeout": None,
            "extra_links": [],
            "is_mapped": True,
            "operator_name": "EmptyOperator",
            "owner": "airflow",
            "params": {},
            "pool": "default_pool",
            "pool_slots": 1.0,
            "priority_weight": 1.0,
            "queue": "default",
            "retries": 0.0,
            "retry_delay": {"__type": "TimeDelta", "days": 0, "microseconds": 0, "seconds": 300},
            "retry_exponential_backoff": False,
            "start_date": "2020-06-15T00:00:00Z",
            "task_id": "mapped_task",
            "task_display_name": "mapped_task",
            "template_fields": [],
            "trigger_rule": "all_success",
            "ui_color": "#e8f7e4",
            "ui_fgcolor": "#000",
            "wait_for_downstream": False,
            "weight_rule": "downstream",
            "doc_md": None,
        }
        response = test_client.get(
            f"/public/dags/{self.mapped_dag_id}/tasks/{self.mapped_task_id}",
        )
        assert response.status_code == 200
        assert response.json() == expected

    def test_unscheduled_task(self, test_client):
        expected = {
            "class_ref": {
                "class_name": "EmptyOperator",
                "module_path": "airflow.operators.empty",
            },
            "depends_on_past": False,
            "downstream_task_ids": [],
            "end_date": None,
            "execution_timeout": None,
            "extra_links": [],
            "operator_name": "EmptyOperator",
            "owner": "airflow",
            "params": {
                "is_unscheduled": {
                    "__class": "airflow.models.param.Param",
                    "value": True,
                    "description": None,
                    "schema": {},
                }
            },
            "pool": "default_pool",
            "pool_slots": 1.0,
            "priority_weight": 1.0,
            "queue": "default",
            "retries": 0.0,
            "retry_delay": {"__type": "TimeDelta", "days": 0, "seconds": 300, "microseconds": 0},
            "retry_exponential_backoff": False,
            "start_date": None,
            "task_id": None,
            "task_display_name": None,
            "template_fields": [],
            "trigger_rule": "all_success",
            "ui_color": "#e8f7e4",
            "ui_fgcolor": "#000",
            "wait_for_downstream": False,
            "weight_rule": "downstream",
            "is_mapped": False,
            "doc_md": None,
        }
        downstream_dict = {
            self.unscheduled_task_id1: self.unscheduled_task_id2,
            self.unscheduled_task_id2: None,
        }
        for task_id, downstream_task_id in downstream_dict.items():
            response = test_client.get(
                f"/public/dags/{self.unscheduled_dag_id}/tasks/{task_id}",
            )
            assert response.status_code == 200
            expected["downstream_task_ids"] = [downstream_task_id] if downstream_task_id else []
            expected["task_id"] = task_id
            expected["task_display_name"] = task_id
            assert response.json() == expected

    def test_should_respond_200_serialized(self, test_client):
        # Get the dag out of the dagbag before we patch it to an empty one
        dag = test_client.app.state.dag_bag.get_dag(self.dag_id)
        dag.sync_to_db()
        SerializedDagModel.write_dag(dag)

        dag_bag = DagBag(os.devnull, include_examples=False, read_dags_from_db=True)
        patcher = unittest.mock.patch.object(test_client.app.state, "dag_bag", dag_bag)
        patcher.start()

        expected = {
            "class_ref": {
                "class_name": "EmptyOperator",
                "module_path": "airflow.operators.empty",
            },
            "depends_on_past": False,
            "downstream_task_ids": [self.task_id2],
            "end_date": None,
            "execution_timeout": None,
            "extra_links": [],
            "operator_name": "EmptyOperator",
            "owner": "airflow",
            "params": {
                "foo": {
                    "__class": "airflow.models.param.Param",
                    "value": "bar",
                    "description": None,
                    "schema": {},
                }
            },
            "pool": "default_pool",
            "pool_slots": 1.0,
            "priority_weight": 1.0,
            "queue": "default",
            "retries": 0.0,
            "retry_delay": {"__type": "TimeDelta", "days": 0, "seconds": 300, "microseconds": 0},
            "retry_exponential_backoff": False,
            "start_date": "2020-06-15T00:00:00Z",
            "task_id": "op1",
            "task_display_name": "op1",
            "template_fields": [],
            "trigger_rule": "all_success",
            "ui_color": "#e8f7e4",
            "ui_fgcolor": "#000",
            "wait_for_downstream": False,
            "weight_rule": "downstream",
            "is_mapped": False,
            "doc_md": None,
        }
        response = test_client.get(
            f"/public/dags/{self.dag_id}/tasks/{self.task_id}",
        )
        assert response.status_code == 200
        assert response.json() == expected
        patcher.stop()

    def test_should_respond_404(self, test_client):
        task_id = "xxxx_not_existing"
        response = test_client.get(
            f"/public/dags/{self.dag_id}/tasks/{task_id}",
        )
        assert response.status_code == 404

    def test_should_respond_404_when_dag_not_found(self, test_client):
        dag_id = "xxxx_not_existing"
        response = test_client.get(
            f"/public/dags/{dag_id}/tasks/{self.task_id}",
        )
        assert response.status_code == 404
