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

from datetime import datetime

import pytest

from airflow.api_fastapi.common.dagbag import dag_bag_from_app
from airflow.models.dagbag import DBDagBag
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG
from airflow.sdk.definitions._internal.expandinput import EXPAND_INPUT_EMPTY

from tests_common.test_utils.asserts import assert_queries_count
from tests_common.test_utils.dag import sync_dag_to_db, sync_dags_to_db
from tests_common.test_utils.db import (
    clear_db_dag_bundles,
    clear_db_dags,
    clear_db_runs,
    clear_db_serialized_dags,
)

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
    api_prefix = "/dags"

    def create_dags(self, test_client):
        with DAG(self.dag_id, schedule=None, start_date=self.task1_start_date, doc_md="details") as dag:
            task1 = EmptyOperator(task_id=self.task_id, params={"foo": "bar"})
            task2 = EmptyOperator(task_id=self.task_id2, start_date=self.task2_start_date)
            task1 >> task2

        with DAG(self.mapped_dag_id, schedule=None, start_date=self.task1_start_date) as mapped_dag:
            EmptyOperator(task_id=self.task_id3)
            # Use the private _expand() method to avoid the empty kwargs check.
            # We don't care about how the operator runs here, only its presence.
            EmptyOperator.partial(task_id=self.mapped_task_id)._expand(EXPAND_INPUT_EMPTY, strict=False)

        with DAG(self.unscheduled_dag_id, start_date=None, schedule=None) as unscheduled_dag:
            task4 = EmptyOperator(task_id=self.unscheduled_task_id1, params={"is_unscheduled": True})
            task5 = EmptyOperator(task_id=self.unscheduled_task_id2, params={"is_unscheduled": True})
            task4 >> task5

        sync_dags_to_db([dag, mapped_dag, unscheduled_dag])
        test_client.app.dependency_overrides[dag_bag_from_app] = DBDagBag

    @staticmethod
    def clear_db():
        clear_db_runs()
        clear_db_dags()
        clear_db_serialized_dags()
        clear_db_dag_bundles()

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
                "module_path": "airflow.providers.standard.operators.empty",
            },
            "depends_on_past": False,
            "downstream_task_ids": [self.task_id2],
            "end_date": None,
            "execution_timeout": None,
            "extra_links": [],
            "operator_name": "EmptyOperator",
            "owner": "airflow",
            "params": {"foo": {"value": "bar", "schema": {}, "description": None, "source": "task"}},
            "pool": "default_pool",
            "pool_slots": 1.0,
            "priority_weight": 1.0,
            "queue": "default",
            "retries": 0.0,
            "retry_delay": {"__type": "TimeDelta", "days": 0, "seconds": 300, "microseconds": 0},
            "retry_exponential_backoff": 0,
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
            f"{self.api_prefix}/{self.dag_id}/tasks/{self.task_id}",
        )
        assert response.status_code == 200
        assert response.json() == expected

    def test_mapped_task(self, test_client):
        expected = {
            "class_ref": {
                "class_name": "EmptyOperator",
                "module_path": "airflow.providers.standard.operators.empty",
            },
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
            "retry_exponential_backoff": 0,
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
            f"{self.api_prefix}/{self.mapped_dag_id}/tasks/{self.mapped_task_id}",
        )
        assert response.status_code == 200
        assert response.json() == expected

    def test_unscheduled_task(self, test_client):
        expected = {
            "class_ref": {
                "class_name": "EmptyOperator",
                "module_path": "airflow.providers.standard.operators.empty",
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
                    "value": True,
                    "schema": {},
                    "description": None,
                    "source": "task",
                }
            },
            "pool": "default_pool",
            "pool_slots": 1.0,
            "priority_weight": 1.0,
            "queue": "default",
            "retries": 0.0,
            "retry_delay": {"__type": "TimeDelta", "days": 0, "seconds": 300, "microseconds": 0},
            "retry_exponential_backoff": 0,
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
                f"{self.api_prefix}/{self.unscheduled_dag_id}/tasks/{task_id}",
            )
            assert response.status_code == 200
            expected["downstream_task_ids"] = [downstream_task_id] if downstream_task_id else []
            expected["task_id"] = task_id
            expected["task_display_name"] = task_id
            assert response.json() == expected

    def test_should_respond_200_serialized(self, test_client, testing_dag_bundle):
        # Get the dag out of the dagbag before we patch it to an empty one

        with DAG(self.dag_id, schedule=None, start_date=self.task1_start_date, doc_md="details") as dag:
            task1 = EmptyOperator(task_id=self.task_id, params={"foo": "bar"})
            task2 = EmptyOperator(task_id=self.task_id2, start_date=self.task2_start_date)
            task1 >> task2

        sync_dag_to_db(dag)

        dag_bag = DBDagBag()
        test_client.app.dependency_overrides[dag_bag_from_app] = lambda: dag_bag

        expected = {
            "class_ref": {
                "class_name": "EmptyOperator",
                "module_path": "airflow.providers.standard.operators.empty",
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
                    "value": "bar",
                    "schema": {},
                    "description": None,
                    "source": "task",
                }
            },
            "pool": "default_pool",
            "pool_slots": 1.0,
            "priority_weight": 1.0,
            "queue": "default",
            "retries": 0.0,
            "retry_delay": {"__type": "TimeDelta", "days": 0, "seconds": 300, "microseconds": 0},
            "retry_exponential_backoff": 0,
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
            f"{self.api_prefix}/{self.dag_id}/tasks/{self.task_id}",
        )
        assert response.status_code == 200
        assert response.json() == expected

    def test_should_respond_404(self, test_client):
        task_id = "xxxx_not_existing"
        response = test_client.get(
            f"{self.api_prefix}/{self.dag_id}/tasks/{task_id}",
        )
        assert response.status_code == 404

    def test_should_respond_404_when_dag_not_found(self, test_client):
        dag_id = "xxxx_not_existing"
        response = test_client.get(
            f"{self.api_prefix}/{dag_id}/tasks/{self.task_id}",
        )
        assert response.status_code == 404

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get(f"{self.api_prefix}/{self.dag_id}/tasks/{self.task_id}")
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get(f"{self.api_prefix}/{self.dag_id}/tasks/{self.task_id}")
        assert response.status_code == 403


class TestGetTasks(TestTaskEndpoint):
    def test_should_respond_200(self, test_client):
        expected = {
            "tasks": [
                {
                    "class_ref": {
                        "class_name": "EmptyOperator",
                        "module_path": "airflow.providers.standard.operators.empty",
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
                            "value": "bar",
                            "schema": {},
                            "description": None,
                            "source": "task",
                        }
                    },
                    "pool": "default_pool",
                    "pool_slots": 1.0,
                    "priority_weight": 1.0,
                    "queue": "default",
                    "retries": 0.0,
                    "retry_delay": {"__type": "TimeDelta", "days": 0, "seconds": 300, "microseconds": 0},
                    "retry_exponential_backoff": 0,
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
                },
                {
                    "class_ref": {
                        "class_name": "EmptyOperator",
                        "module_path": "airflow.providers.standard.operators.empty",
                    },
                    "depends_on_past": False,
                    "downstream_task_ids": [],
                    "end_date": None,
                    "execution_timeout": None,
                    "extra_links": [],
                    "operator_name": "EmptyOperator",
                    "owner": "airflow",
                    "params": {},
                    "pool": "default_pool",
                    "pool_slots": 1.0,
                    "priority_weight": 1.0,
                    "queue": "default",
                    "retries": 0.0,
                    "retry_delay": {"__type": "TimeDelta", "days": 0, "seconds": 300, "microseconds": 0},
                    "retry_exponential_backoff": 0,
                    "start_date": "2020-06-16T00:00:00Z",
                    "task_id": self.task_id2,
                    "task_display_name": self.task_id2,
                    "template_fields": [],
                    "trigger_rule": "all_success",
                    "ui_color": "#e8f7e4",
                    "ui_fgcolor": "#000",
                    "wait_for_downstream": False,
                    "weight_rule": "downstream",
                    "is_mapped": False,
                    "doc_md": None,
                },
            ],
            "total_entries": 2,
        }
        with assert_queries_count(2):
            response = test_client.get(f"{self.api_prefix}/{self.dag_id}/tasks")
        assert response.status_code == 200
        assert response.json() == expected

    def test_get_tasks_mapped(self, test_client):
        expected = {
            "tasks": [
                {
                    "class_ref": {
                        "class_name": "EmptyOperator",
                        "module_path": "airflow.providers.standard.operators.empty",
                    },
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
                    "retry_exponential_backoff": 0,
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
                },
                {
                    "class_ref": {
                        "class_name": "EmptyOperator",
                        "module_path": "airflow.providers.standard.operators.empty",
                    },
                    "depends_on_past": False,
                    "downstream_task_ids": [],
                    "end_date": None,
                    "execution_timeout": None,
                    "extra_links": [],
                    "operator_name": "EmptyOperator",
                    "owner": "airflow",
                    "params": {},
                    "pool": "default_pool",
                    "pool_slots": 1.0,
                    "priority_weight": 1.0,
                    "queue": "default",
                    "retries": 0.0,
                    "retry_delay": {"__type": "TimeDelta", "days": 0, "seconds": 300, "microseconds": 0},
                    "retry_exponential_backoff": 0,
                    "start_date": "2020-06-15T00:00:00Z",
                    "task_id": self.task_id3,
                    "task_display_name": self.task_id3,
                    "template_fields": [],
                    "trigger_rule": "all_success",
                    "ui_color": "#e8f7e4",
                    "ui_fgcolor": "#000",
                    "wait_for_downstream": False,
                    "weight_rule": "downstream",
                    "is_mapped": False,
                    "doc_md": None,
                },
            ],
            "total_entries": 2,
        }

        with assert_queries_count(2):
            response = test_client.get(f"{self.api_prefix}/{self.mapped_dag_id}/tasks")
        assert response.status_code == 200
        assert response.json() == expected

    def test_get_unscheduled_tasks(self, test_client):
        downstream_dict = {
            self.unscheduled_task_id1: self.unscheduled_task_id2,
            self.unscheduled_task_id2: None,
        }
        expected = {
            "tasks": [
                {
                    "class_ref": {
                        "class_name": "EmptyOperator",
                        "module_path": "airflow.providers.standard.operators.empty",
                    },
                    "depends_on_past": False,
                    "downstream_task_ids": [downstream_task_id] if downstream_task_id else [],
                    "end_date": None,
                    "execution_timeout": None,
                    "extra_links": [],
                    "operator_name": "EmptyOperator",
                    "owner": "airflow",
                    "params": {
                        "is_unscheduled": {
                            "value": True,
                            "schema": {},
                            "description": None,
                            "source": "task",
                        }
                    },
                    "pool": "default_pool",
                    "pool_slots": 1.0,
                    "priority_weight": 1.0,
                    "queue": "default",
                    "retries": 0.0,
                    "retry_delay": {"__type": "TimeDelta", "days": 0, "seconds": 300, "microseconds": 0},
                    "retry_exponential_backoff": 0,
                    "start_date": None,
                    "task_id": task_id,
                    "task_display_name": task_id,
                    "template_fields": [],
                    "trigger_rule": "all_success",
                    "ui_color": "#e8f7e4",
                    "ui_fgcolor": "#000",
                    "wait_for_downstream": False,
                    "weight_rule": "downstream",
                    "is_mapped": False,
                    "doc_md": None,
                }
                for (task_id, downstream_task_id) in downstream_dict.items()
            ],
            "total_entries": len(downstream_dict),
        }

        with assert_queries_count(2):
            response = test_client.get(f"{self.api_prefix}/{self.unscheduled_dag_id}/tasks")
        assert response.status_code == 200
        assert response.json() == expected

    def test_should_respond_200_ascending_order_by_start_date(self, test_client):
        with assert_queries_count(2):
            response = test_client.get(
                f"{self.api_prefix}/{self.dag_id}/tasks?order_by=start_date",
            )
        assert response.status_code == 200
        assert self.task1_start_date < self.task2_start_date
        assert response.json()["tasks"][0]["task_id"] == self.task_id
        assert response.json()["tasks"][1]["task_id"] == self.task_id2

    def test_should_respond_200_descending_order_by_start_date(self, test_client):
        with assert_queries_count(2):
            response = test_client.get(
                f"{self.api_prefix}/{self.dag_id}/tasks?order_by=-start_date",
            )
        assert response.status_code == 200
        # - means is descending
        assert self.task1_start_date < self.task2_start_date
        assert response.json()["tasks"][0]["task_id"] == self.task_id2
        assert response.json()["tasks"][1]["task_id"] == self.task_id

    def test_should_raise_400_for_invalid_order_by_name(self, test_client):
        response = test_client.get(
            f"{self.api_prefix}/{self.dag_id}/tasks?order_by=invalid_task_colume_name",
        )
        assert response.status_code == 400
        assert (
            response.json()["detail"] == "'EmptyOperator' object has no attribute 'invalid_task_colume_name'"
        )

    def test_should_respond_404(self, test_client):
        dag_id = "xxxx_not_existing"
        response = test_client.get(f"{self.api_prefix}/{dag_id}/tasks")
        assert response.status_code == 404

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get(f"{self.api_prefix}/{self.dag_id}/tasks")
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get(f"{self.api_prefix}/{self.dag_id}/tasks")
        assert response.status_code == 403
