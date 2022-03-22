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
import datetime as dt

import pytest

from airflow.models import TaskInstance
from airflow.models.baseoperator import BaseOperator
from airflow.models.taskmap import TaskMap
from airflow.models.xcom_arg import XComArg
from airflow.security import permissions
from airflow.utils.platform import getuser
from airflow.utils.session import provide_session
from airflow.utils.state import State, TaskInstanceState
from airflow.utils.timezone import datetime
from tests.test_utils.api_connexion_utils import assert_401, create_user, delete_roles, delete_user
from tests.test_utils.db import clear_db_runs, clear_db_sla_miss, clear_rendered_ti_fields
from tests.test_utils.mock_operators import MockOperator

DEFAULT_DATETIME_1 = datetime(2020, 1, 1)
DEFAULT_DATETIME_STR_1 = "2020-01-01T00:00:00+00:00"
DEFAULT_DATETIME_STR_2 = "2020-01-02T00:00:00+00:00"


@pytest.fixture(scope="module")
def configured_app(minimal_app_for_api):
    app = minimal_app_for_api
    create_user(
        app,  # type: ignore
        username="test",
        role_name="Test",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_TASK_INSTANCE),
        ],
    )
    create_user(app, username="test_no_permissions", role_name="TestNoPermissions")  # type: ignore

    yield app

    delete_user(app, username="test")  # type: ignore
    delete_user(app, username="test_no_permissions")  # type: ignore
    delete_roles(app)


class TestMappedTaskInstanceEndpoint:
    @pytest.fixture(autouse=True)
    def setup_attrs(self, configured_app) -> None:
        self.default_time = DEFAULT_DATETIME_1
        self.ti_init = {
            "execution_date": self.default_time,
            "state": State.RUNNING,
        }
        self.ti_extras = {
            "start_date": self.default_time + dt.timedelta(days=1),
            "end_date": self.default_time + dt.timedelta(days=2),
            "pid": 100,
            "duration": 10000,
            "pool": "default_pool",
            "queue": "default_queue",
            "job_id": 0,
        }
        self.app = configured_app
        self.client = self.app.test_client()  # type:ignore
        clear_db_runs()
        clear_db_sla_miss()
        clear_rendered_ti_fields()

    def create_dag_runs_with_mapped_tasks(self, dag_maker, session, dags={}):
        for dag_id in dags:
            with dag_maker(session=session, dag_id=dag_id, start_date=DEFAULT_DATETIME_1):
                task1 = BaseOperator(task_id="op1")
                mapped = MockOperator.partial(task_id='task_2').expand(arg2=XComArg(task1))

            dr = dag_maker.create_dagrun(run_id=f"run_{dag_id}")

            session.add(
                TaskMap(
                    dag_id=dr.dag_id,
                    task_id=task1.task_id,
                    run_id=dr.run_id,
                    map_index=-1,
                    length=dags[dag_id],
                    keys=None,
                )
            )

            if dags[dag_id]:
                # Remove the map_index=-1 TI when we're creating other TIs
                session.query(TaskInstance).filter(
                    TaskInstance.dag_id == mapped.dag_id,
                    TaskInstance.task_id == mapped.task_id,
                    TaskInstance.run_id == dr.run_id,
                ).delete()

            for index in range(dags[dag_id]):
                # Give the existing TIs a state to make sure we don't change them
                ti = TaskInstance(mapped, run_id=dr.run_id, map_index=index, state=TaskInstanceState.SUCCESS)
                setattr(ti, 'start_date', DEFAULT_DATETIME_1)
                session.add(ti)
            session.flush()

            if dags[dag_id]:
                mapped.expand_mapped_task(dr.run_id, session=session)

    @pytest.fixture
    def one_task_with_mapped_tis(self, dag_maker, session):
        self.create_dag_runs_with_mapped_tasks(
            dag_maker,
            session,
            dags={
                'mapped_tis': 3,
            },
        )

    @pytest.fixture
    def one_task_with_single_mapped_ti(self, dag_maker, session):
        self.create_dag_runs_with_mapped_tasks(
            dag_maker,
            session,
            dags={
                'mapped_tis': 1,
            },
        )

    @pytest.fixture
    def one_task_with_many_mapped_tis(self, dag_maker, session):
        self.create_dag_runs_with_mapped_tasks(
            dag_maker,
            session,
            dags={
                'mapped_tis': 110,
            },
        )

    @pytest.fixture
    def one_task_with_zero_mapped_tis(self, dag_maker, session):
        self.create_dag_runs_with_mapped_tasks(
            dag_maker,
            session,
            dags={
                'mapped_tis': 0,
            },
        )


class TestGetMappedTaskInstance(TestMappedTaskInstanceEndpoint):
    @provide_session
    def test_mapped_task_instances(self, one_task_with_mapped_tis, session):
        response = self.client.get(
            "/api/v1/dags/mapped_tis/dagRuns/run_mapped_tis/taskInstances/task_2/0",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 200
        assert response.json == {
            "dag_id": "mapped_tis",
            "dag_run_id": "run_mapped_tis",
            "duration": None,
            "end_date": None,
            "execution_date": "2020-01-01T00:00:00+00:00",
            "executor_config": "{}",
            "hostname": "",
            "map_index": 0,
            "max_tries": 0,
            "operator": "MockOperator",
            "pid": None,
            "pool": "default_pool",
            "pool_slots": 1,
            "priority_weight": 1,
            "queue": "default",
            "queued_when": None,
            "rendered_fields": {},
            "sla_miss": None,
            "start_date": '2020-01-01T00:00:00+00:00',
            "state": 'success',
            "task_id": "task_2",
            "try_number": 0,
            "unixname": getuser(),
        }

    def test_should_raises_401_unauthenticated(self):
        response = self.client.get(
            "/api/v1/dags/mapped_tis/dagRuns/run_mapped_tis/taskInstances/task_2/1",
        )
        assert_401(response)

    def test_should_raise_403_forbidden(self):
        response = self.client.get(
            "api/v1/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context",
            environ_overrides={'REMOTE_USER': "test_no_permissions"},
        )
        assert response.status_code == 403

    def test_without_map_index_returns_custom_404(self, one_task_with_mapped_tis):
        response = self.client.get(
            "/api/v1/dags/mapped_tis/dagRuns/run_mapped_tis/taskInstances/task_2",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 404
        assert response.json == {
            'detail': 'Task instance is mapped, add the map_index value to the URL',
            'status': 404,
            'title': 'Task instance not found',
            'type': 'http://apache-airflow-docs.s3-website.eu-central-1.amazonaws.com/docs/'
            'apache-airflow/latest/stable-rest-api-ref.html#section/Errors/NotFound',
        }

    def test_one_mapped_task_works(self, one_task_with_single_mapped_ti):
        response = self.client.get(
            "/api/v1/dags/mapped_tis/dagRuns/run_mapped_tis/taskInstances/task_2/0",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 200
        response = self.client.get(
            "/api/v1/dags/mapped_tis/dagRuns/run_mapped_tis/taskInstances/task_2/1",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 404
        response = self.client.get(
            "/api/v1/dags/mapped_tis/dagRuns/run_mapped_tis/taskInstances/task_2",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 404
        assert response.json == {
            'detail': 'Task instance is mapped, add the map_index value to the URL',
            'status': 404,
            'title': 'Task instance not found',
            'type': 'http://apache-airflow-docs.s3-website.eu-central-1.amazonaws.com/docs/'
            'apache-airflow/latest/stable-rest-api-ref.html#section/Errors/NotFound',
        }


class TestGetMappedTaskInstances(TestMappedTaskInstanceEndpoint):
    @provide_session
    def test_mapped_task_instances(self, one_task_with_many_mapped_tis, session):
        response = self.client.get(
            "/api/v1/dags/mapped_tis/dagRuns/run_mapped_tis/taskInstances/task_2/listMapped",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 200
        assert response.json["total_entries"] == 110
        assert len(response.json["task_instances"]) == 100

    @provide_session
    def test_mapped_task_instances_offset_limit(self, one_task_with_many_mapped_tis, session):
        response = self.client.get(
            "/api/v1/dags/mapped_tis/dagRuns/run_mapped_tis/taskInstances/task_2/listMapped"
            "?offset=4&limit=10",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 200
        assert response.json["total_entries"] == 110
        assert len(response.json["task_instances"]) == 10
        assert list(range(4, 14)) == [ti['map_index'] for ti in response.json["task_instances"]]

    @provide_session
    def test_mapped_task_instances_with_date(self, one_task_with_mapped_tis, session):
        response = self.client.get(
            "/api/v1/dags/mapped_tis/dagRuns/run_mapped_tis/taskInstances/task_2/listMapped"
            f"?start_date_gte={DEFAULT_DATETIME_STR_1}",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 200
        assert response.json["total_entries"] == 3
        assert len(response.json["task_instances"]) == 3

        response = self.client.get(
            "/api/v1/dags/mapped_tis/dagRuns/run_mapped_tis/taskInstances/task_2/listMapped"
            f"?start_date_gte={DEFAULT_DATETIME_STR_2}",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 200
        assert response.json["total_entries"] == 0

    @provide_session
    def test_mapped_task_instances_with_state(self, one_task_with_mapped_tis, session):
        response = self.client.get(
            "/api/v1/dags/mapped_tis/dagRuns/run_mapped_tis/taskInstances/task_2/listMapped?state=success",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 200
        assert response.json["total_entries"] == 3
        assert len(response.json["task_instances"]) == 3

        response = self.client.get(
            "/api/v1/dags/mapped_tis/dagRuns/run_mapped_tis/taskInstances/task_2/listMapped?state=running",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 200
        assert response.json["total_entries"] == 0

    @provide_session
    def test_mapped_task_instances_with_pool(self, one_task_with_mapped_tis, session):
        response = self.client.get(
            "/api/v1/dags/mapped_tis/dagRuns/run_mapped_tis/taskInstances/task_2/listMapped"
            "?pool=default_pool",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 200
        assert response.json["total_entries"] == 3
        assert len(response.json["task_instances"]) == 3

        response = self.client.get(
            "/api/v1/dags/mapped_tis/dagRuns/run_mapped_tis/taskInstances/task_2/listMapped?pool=test_pool",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 200
        assert response.json["total_entries"] == 0

    @provide_session
    def test_mapped_task_instances_with_queue(self, one_task_with_mapped_tis, session):
        response = self.client.get(
            "/api/v1/dags/mapped_tis/dagRuns/run_mapped_tis/taskInstances/task_2/listMapped?queue=default",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 200
        assert response.json["total_entries"] == 3
        assert len(response.json["task_instances"]) == 3

        response = self.client.get(
            "/api/v1/dags/mapped_tis/dagRuns/run_mapped_tis/taskInstances/task_2/listMapped?queue=test_queue",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 200
        assert response.json["total_entries"] == 0

    @provide_session
    def test_mapped_task_instances_with_zero_mapped(self, one_task_with_zero_mapped_tis, session):
        response = self.client.get(
            "/api/v1/dags/mapped_tis/dagRuns/run_mapped_tis/taskInstances/task_2/listMapped",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 200
        assert response.json["total_entries"] == 0
        assert len(response.json["task_instances"]) == 0

    @provide_session
    def test_invalid_task_instance(self, session):
        response = self.client.get(
            "/api/v1/dags/mapped_tis/dagRuns/run_mapped_tis/taskInstances/task_2/listMapped",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 404
