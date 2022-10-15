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

import datetime as dt

import pytest
from marshmallow import ValidationError

from airflow.api_connexion.schemas.task_instance_schema import (
    clear_task_instance_form,
    set_task_instance_state_form,
    task_instance_schema,
)
from airflow.models import RenderedTaskInstanceFields as RTIF, SlaMiss, TaskInstance as TI
from airflow.operators.empty import EmptyOperator
from airflow.utils.platform import getuser
from airflow.utils.state import State
from airflow.utils.timezone import datetime


class TestTaskInstanceSchema:
    @pytest.fixture(autouse=True)
    def set_attrs(self, session, dag_maker):
        self.default_time = datetime(2020, 1, 1)
        with dag_maker(dag_id="TEST_DAG_ID", session=session):
            self.task = EmptyOperator(task_id="TEST_TASK_ID", start_date=self.default_time)

        self.dr = dag_maker.create_dagrun(execution_date=self.default_time)
        session.flush()

        self.default_ti_init = {
            "run_id": None,
            "state": State.RUNNING,
        }
        self.default_ti_extras = {
            "dag_run": self.dr,
            "start_date": self.default_time + dt.timedelta(days=1),
            "end_date": self.default_time + dt.timedelta(days=2),
            "pid": 100,
            "duration": 10000,
            "pool": "default_pool",
            "queue": "default_queue",
        }

        yield

        session.rollback()

    def test_task_instance_schema_without_sla_and_rendered(self, session):
        ti = TI(task=self.task, **self.default_ti_init)
        for key, value in self.default_ti_extras.items():
            setattr(ti, key, value)
        serialized_ti = task_instance_schema.dump((ti, None, None))
        expected_json = {
            "dag_id": "TEST_DAG_ID",
            "duration": 10000.0,
            "end_date": "2020-01-03T00:00:00+00:00",
            "execution_date": "2020-01-01T00:00:00+00:00",
            "executor_config": "{}",
            "hostname": "",
            "map_index": -1,
            "max_tries": 0,
            "operator": "EmptyOperator",
            "pid": 100,
            "pool": "default_pool",
            "pool_slots": 1,
            "priority_weight": 1,
            "queue": "default_queue",
            "queued_when": None,
            "sla_miss": None,
            "start_date": "2020-01-02T00:00:00+00:00",
            "state": "running",
            "task_id": "TEST_TASK_ID",
            "try_number": 0,
            "unixname": getuser(),
            "dag_run_id": None,
            "rendered_fields": {},
            "trigger": None,
            "triggerer_job": None,
        }
        assert serialized_ti == expected_json

    def test_task_instance_schema_with_sla_and_rendered(self, session):
        sla_miss = SlaMiss(
            task_id="TEST_TASK_ID",
            dag_id="TEST_DAG_ID",
            execution_date=self.default_time,
        )
        session.add(sla_miss)
        session.flush()
        ti = TI(task=self.task, **self.default_ti_init)
        for key, value in self.default_ti_extras.items():
            setattr(ti, key, value)
        self.task.template_fields = ["partitions"]
        setattr(self.task, "partitions", "data/ds=2022-02-17")
        ti.rendered_task_instance_fields = RTIF(ti, render_templates=False)
        serialized_ti = task_instance_schema.dump((ti, sla_miss))
        expected_json = {
            "dag_id": "TEST_DAG_ID",
            "duration": 10000.0,
            "end_date": "2020-01-03T00:00:00+00:00",
            "execution_date": "2020-01-01T00:00:00+00:00",
            "executor_config": "{}",
            "hostname": "",
            "map_index": -1,
            "max_tries": 0,
            "operator": "EmptyOperator",
            "pid": 100,
            "pool": "default_pool",
            "pool_slots": 1,
            "priority_weight": 1,
            "queue": "default_queue",
            "queued_when": None,
            "sla_miss": {
                "dag_id": "TEST_DAG_ID",
                "description": None,
                "email_sent": False,
                "execution_date": "2020-01-01T00:00:00+00:00",
                "notification_sent": False,
                "task_id": "TEST_TASK_ID",
                "timestamp": None,
            },
            "start_date": "2020-01-02T00:00:00+00:00",
            "state": "running",
            "task_id": "TEST_TASK_ID",
            "try_number": 0,
            "unixname": getuser(),
            "dag_run_id": None,
            "rendered_fields": {"partitions": "data/ds=2022-02-17"},
            "trigger": None,
            "triggerer_job": None,
        }
        assert serialized_ti == expected_json


class TestClearTaskInstanceFormSchema:
    @pytest.mark.parametrize(
        "payload",
        [
            (
                {
                    "dry_run": False,
                    "reset_dag_runs": True,
                    "only_failed": True,
                    "only_running": True,
                }
            ),
            (
                {
                    "dry_run": False,
                    "reset_dag_runs": True,
                    "end_date": "2020-01-01T00:00:00+00:00",
                    "start_date": "2020-01-02T00:00:00+00:00",
                }
            ),
            (
                {
                    "dry_run": False,
                    "reset_dag_runs": True,
                    "task_ids": [],
                }
            ),
            (
                {
                    "dry_run": False,
                    "reset_dag_runs": True,
                    "dag_run_id": "scheduled__2022-06-19T00:00:00+00:00",
                    "start_date": "2022-08-03T00:00:00+00:00",
                }
            ),
            (
                {
                    "dry_run": False,
                    "reset_dag_runs": True,
                    "dag_run_id": "scheduled__2022-06-19T00:00:00+00:00",
                    "end_date": "2022-08-03T00:00:00+00:00",
                }
            ),
            (
                {
                    "dry_run": False,
                    "reset_dag_runs": True,
                    "dag_run_id": "scheduled__2022-06-19T00:00:00+00:00",
                    "end_date": "2022-08-04T00:00:00+00:00",
                    "start_date": "2022-08-03T00:00:00+00:00",
                }
            ),
        ],
    )
    def test_validation_error(self, payload):
        with pytest.raises(ValidationError):
            clear_task_instance_form.load(payload)


class TestSetTaskInstanceStateFormSchema:
    current_input = {
        "dry_run": True,
        "task_id": "print_the_context",
        "execution_date": "2020-01-01T00:00:00+00:00",
        "include_upstream": True,
        "include_downstream": True,
        "include_future": True,
        "include_past": True,
        "new_state": "failed",
    }

    def test_success(self):
        result = set_task_instance_state_form.load(self.current_input)
        expected_result = {
            'dry_run': True,
            'execution_date': dt.datetime(2020, 1, 1, 0, 0, tzinfo=dt.timezone(dt.timedelta(0), '+0000')),
            'include_downstream': True,
            'include_future': True,
            'include_past': True,
            'include_upstream': True,
            'new_state': 'failed',
            'task_id': 'print_the_context',
        }
        assert expected_result == result

    @pytest.mark.parametrize(
        "override_data",
        [
            {"task_id": None},
            {"include_future": "foo"},
            {"execution_date": "NOW"},
            {"new_state": "INVALID_STATE"},
            {"execution_date": "2020-01-01T00:00:00+00:00", "dag_run_id": "some-run-id"},
        ],
    )
    def test_validation_error(self, override_data):
        self.current_input.update(override_data)

        with pytest.raises(ValidationError):
            set_task_instance_state_form.load(self.current_input)
