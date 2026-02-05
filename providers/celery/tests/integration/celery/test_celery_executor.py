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
from __future__ import annotations

import contextlib
import json
import logging
import os
import sys
from ast import literal_eval
from datetime import datetime, timedelta
from time import sleep
from unittest import mock

# leave this it is used by the test worker
import celery.contrib.testing.tasks  # noqa: F401
import pytest
import uuid6
from celery import Celery
from celery.backends.base import BaseBackend, BaseKeyValueStoreBackend
from celery.backends.database import DatabaseBackend
from celery.contrib.testing.worker import start_worker
from kombu.asynchronous import set_event_loop
from kubernetes.client import models as k8s
from uuid6 import uuid7

from airflow._shared.timezones import timezone
from airflow.configuration import conf
from airflow.executors import workloads
from airflow.models.dag import DAG
from airflow.models.taskinstance import TaskInstance
from airflow.providers.common.compat.sdk import AirflowException, AirflowTaskTimeout, TaskInstanceKey
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import BaseOperator
from airflow.utils.state import State

from tests_common.test_utils import db
from tests_common.test_utils.taskinstance import create_task_instance
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

logger = logging.getLogger(__name__)


def _prepare_test_bodies():
    return literal_eval(os.environ["CELERY_BROKER_URLS_MAP"]).values()


class FakeCeleryResult:
    @property
    def state(self):
        raise Exception()

    def task_id(self):
        return "task_id"


@contextlib.contextmanager
def _prepare_app(broker_url=None, execute=None):
    from airflow.providers.celery.executors import celery_executor_utils

    broker_url = broker_url or conf.get("celery", "BROKER_URL")
    if AIRFLOW_V_3_0_PLUS:
        execute_name = "execute_workload"
        execute = execute or celery_executor_utils.execute_workload.__wrapped__
    else:
        execute_name = "execute_command"
        execute = execute or celery_executor_utils.execute_command.__wrapped__

    test_config = dict(celery_executor_utils.get_celery_configuration())
    test_config.update({"broker_url": broker_url})
    test_app = Celery(broker_url, config_source=test_config)
    # Register the fake execute function with the test_app using the correct task name
    # This ensures workers using test_app will execute the fake function
    test_execute = test_app.task(name=execute_name)(execute)
    patch_app = mock.patch.object(celery_executor_utils, "app", test_app)

    if AIRFLOW_V_3_0_PLUS:
        celery_executor_utils.execute_workload.__wrapped__ = execute
    else:
        celery_executor_utils.execute_command.__wrapped__ = execute

    patch_execute = mock.patch.object(celery_executor_utils, execute_name, test_execute)
    # Patch factory function so CeleryExecutor instances get the test app
    patch_factory = mock.patch.object(celery_executor_utils, "create_celery_app", return_value=test_app)

    backend = test_app.backend

    if hasattr(backend, "ResultSession"):
        # Pre-create the database tables now, otherwise SQLA vis Celery has a
        # race condition where it one of the subprocesses can die with "Table
        # already exists" error, because SQLA checks for which tables exist,
        # then issues a CREATE TABLE, rather than doing CREATE TABLE IF NOT
        # EXISTS
        session = backend.ResultSession()
        session.close()

    with patch_app, patch_execute, patch_factory:
        try:
            yield test_app
        finally:
            # Clear event loop to tear down each celery instance
            set_event_loop(None)


@pytest.mark.integration("celery")
@pytest.mark.backend("postgres")
class TestCeleryExecutor:
    def setup_method(self) -> None:
        db.clear_db_runs()
        db.clear_db_jobs()

    def teardown_method(self) -> None:
        db.clear_db_runs()
        db.clear_db_jobs()


def setup_dagrun_with_success_and_fail_tasks(dag_maker):
    date = timezone.utcnow()
    start_date = date - timedelta(days=2)

    with dag_maker("test_celery_integration"):
        BaseOperator(task_id="success", start_date=start_date)
        BaseOperator(task_id="fail", start_date=start_date)

    return dag_maker.create_dagrun(logical_date=date)

    @pytest.mark.flaky(reruns=5, reruns_delay=3)
    @pytest.mark.parametrize("broker_url", _prepare_test_bodies())
    @pytest.mark.parametrize(
        "executor_config",
        [
            pytest.param({}, id="no_executor_config"),
            pytest.param(
                {
                    "pod_override": k8s.V1Pod(
                        spec=k8s.V1PodSpec(
                            containers=[
                                k8s.V1Container(
                                    name="base",
                                    resources=k8s.V1ResourceRequirements(
                                        requests={
                                            "cpu": "100m",
                                            "memory": "384Mi",
                                        },
                                        limits={
                                            "cpu": 1,
                                            "memory": "500Mi",
                                        },
                                    ),
                                )
                            ]
                        )
                    )
                },
                id="pod_override_executor_config",
            ),
        ],
    )
    def test_celery_integration(self, broker_url, executor_config):
        from airflow.providers.celery.executors import celery_executor, celery_executor_utils

        if AIRFLOW_V_3_0_PLUS:
            # Airflow 3: execute_workload receives JSON string
            def fake_execute(input: str) -> None:
                """Fake execute_workload that parses JSON and fails for tasks with 'fail' in task_id."""
                import json

                workload_dict = json.loads(input)
                # Check if this is a task that should fail (task_id contains "fail")
                if "ti" in workload_dict and "task_id" in workload_dict["ti"]:
                    if "fail" in workload_dict["ti"]["task_id"]:
                        raise AirflowException("fail")
        else:
            # Airflow 2: execute_command receives command list
            def fake_execute(input: str) -> None:  # Use same parameter name as Airflow 3 version
                if "fail" in input:
                    raise AirflowException("fail")

        with _prepare_app(broker_url, execute=fake_execute) as app:
            executor = celery_executor.CeleryExecutor()
            assert executor.tasks == {}
            executor.start()

            with start_worker(app=app, logfile=sys.stdout, loglevel="info"):
                ti_success = workloads.TaskInstance.model_construct(
                    id=uuid7(),
                    task_id="success",
                    dag_id="id",
                    run_id="abc",
                    try_number=0,
                    priority_weight=1,
                    queue=celery_executor_utils.get_celery_configuration()["task_default_queue"],
                    executor_config=executor_config,
                )
                keys = [
                    TaskInstanceKey("id", "success", "abc", 0, -1),
                    TaskInstanceKey("id", "fail", "abc", 0, -1),
                ]
                dagrun = setup_dagrun_with_success_and_fail_tasks(dag_maker)
                ti_success, ti_fail = dagrun.task_instances
                for w in (
                    workloads.ExecuteTask.make(
                        ti=ti_success,
                    ),
                    workloads.ExecuteTask.make(ti=ti_fail),
                ):
                    executor.queue_workload(w, session=None)

                executor.trigger_tasks(open_slots=10)
                for _ in range(20):
                    num_tasks = len(executor.tasks.keys())
                    if num_tasks == 2:
                        break
                    logger.info(
                        "Waiting 0.1 s for tasks to be processed asynchronously. Processed so far %d",
                        num_tasks,
                    )
                    sleep(0.4)
                assert list(executor.tasks.keys()) == keys
                assert executor.event_buffer[keys[0]][0] == State.QUEUED
                assert executor.event_buffer[keys[1]][0] == State.QUEUED

                executor.end(synchronous=True)

        assert executor.event_buffer[keys[0]][0] == State.SUCCESS
        assert executor.event_buffer[keys[1]][0] == State.FAILED

        assert keys[0] not in executor.tasks
        assert keys[1] not in executor.tasks

        assert executor.queued_tasks == {}

    def test_error_sending_task(self):
        from airflow.providers.celery.executors import celery_executor, celery_executor_utils

        with _prepare_app():
            executor = celery_executor.CeleryExecutor()
            with DAG(dag_id="dag_id"):
                task = BashOperator(task_id="test", bash_command="true", start_date=datetime.now())
            if AIRFLOW_V_3_0_PLUS:
                ti = create_task_instance(task=task, run_id="abc", dag_version_id=uuid6.uuid7())
            else:
                ti = TaskInstance(task=task, run_id="abc")
            workload = workloads.ExecuteTask.model_construct(
                ti=workloads.TaskInstance.model_validate(ti, from_attributes=True),
            )

            key = (task.dag.dag_id, task.task_id, ti.run_id, 0, -1)
            executor.queued_tasks[key] = workload
            executor.task_publish_retries[key] = 1

            # Mock send_task_to_executor to return an error result
            # This simulates a failure when sending the task to Celery
            def mock_send_error(task_tuple):
                key_from_tuple = task_tuple[0]
                return (
                    key_from_tuple,
                    task_tuple[1],  # args
                    celery_executor_utils.ExceptionWithTraceback(
                        RuntimeError("Intentional test failure"),
                        "Celery Task ID: mock\nTraceback: test error",
                    ),
                )

            with mock.patch.object(
                celery_executor_utils, "send_task_to_executor", side_effect=mock_send_error
            ):
                executor.heartbeat()
        assert len(executor.queued_tasks) == 0, "Task should no longer be queued"
        assert executor.event_buffer[key][0] == State.FAILED

    def test_retry_on_error_sending_task(self, caplog):
        """Test that Airflow retries publishing tasks to Celery Broker at least 3 times"""
        from airflow.providers.celery.executors import celery_executor, celery_executor_utils

        with (
            _prepare_app(),
            caplog.at_level(logging.INFO),
            mock.patch.object(
                # Mock `with timeout()` to _instantly_ fail.
                celery_executor_utils.timeout,
                "__enter__",
                side_effect=AirflowTaskTimeout,
            ),
        ):
            executor = celery_executor.CeleryExecutor()
            assert executor.task_publish_retries == {}
            assert executor.task_publish_max_retries == 3, "Assert Default Max Retries is 3"

            with DAG(dag_id="id"):
                task = BashOperator(task_id="test", bash_command="true", start_date=datetime.now())
            if AIRFLOW_V_3_0_PLUS:
                ti = create_task_instance(task=task, run_id="abc", dag_version_id=uuid6.uuid7())
            else:
                ti = TaskInstance(task=task, run_id="abc")
            workload = workloads.ExecuteTask.model_construct(
                ti=workloads.TaskInstance.model_validate(ti, from_attributes=True),
            )

            key = (task.dag.dag_id, task.task_id, ti.run_id, 0, -1)
            executor.queued_tasks[key] = workload

            # Test that when heartbeat is called again, task is published again to Celery Queue
            executor.heartbeat()
            assert dict(executor.task_publish_retries) == {key: 1}
            assert len(executor.queued_tasks) == 1, "Task should remain in queue"
            assert executor.event_buffer == {}
            assert f"[Try 1 of 3] Task Timeout Error for Task: ({key})." in caplog.text

            executor.heartbeat()
            assert dict(executor.task_publish_retries) == {key: 2}
            assert len(executor.queued_tasks) == 1, "Task should remain in queue"
            assert executor.event_buffer == {}
            assert f"[Try 2 of 3] Task Timeout Error for Task: ({key})." in caplog.text

            executor.heartbeat()
            assert dict(executor.task_publish_retries) == {key: 3}
            assert len(executor.queued_tasks) == 1, "Task should remain in queue"
            assert executor.event_buffer == {}
            assert f"[Try 3 of 3] Task Timeout Error for Task: ({key})." in caplog.text

            executor.heartbeat()
            assert dict(executor.task_publish_retries) == {}
            assert len(executor.queued_tasks) == 0, "Task should no longer be in queue"
            assert executor.event_buffer[key][0] == State.FAILED


class ClassWithCustomAttributes:
    """Class for testing purpose: allows to create objects with custom attributes in one single statement."""

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    def __str__(self):
        return f"{ClassWithCustomAttributes.__name__}({str(self.__dict__)})"

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __hash__(self):
        return hash(self.__dict__)

    def __ne__(self, other):
        return not self.__eq__(other)


@pytest.mark.integration("celery")
@pytest.mark.backend("postgres")
class TestBulkStateFetcher:
    bulk_state_fetcher_logger = "airflow.providers.celery.executors.celery_executor_utils.BulkStateFetcher"

    @mock.patch(
        "celery.backends.base.BaseKeyValueStoreBackend.mget",
        return_value=[json.dumps({"status": "SUCCESS", "task_id": "123"})],
    )
    def test_should_support_kv_backend(self, mock_mget, caplog):
        from airflow.providers.celery.executors import celery_executor, celery_executor_utils

        caplog.set_level(logging.DEBUG, logger=self.bulk_state_fetcher_logger)
        with _prepare_app():
            mock_backend = BaseKeyValueStoreBackend(app=celery_executor.app)
            with mock.patch(
                "airflow.providers.celery.executors.celery_executor_utils.Celery.backend", mock_backend
            ):
                caplog.clear()
                fetcher = celery_executor_utils.BulkStateFetcher(1)
                result = fetcher.get_many(
                    [
                        mock.MagicMock(task_id="123"),
                        mock.MagicMock(task_id="456"),
                    ]
                )

        # Assert called - ignore order
        mget_args, _ = mock_mget.call_args
        assert set(mget_args[0]) == {b"celery-task-meta-456", b"celery-task-meta-123"}
        mock_mget.assert_called_once_with(mock.ANY)

        assert result == {"123": ("SUCCESS", None), "456": ("PENDING", None)}
        assert caplog.messages == ["Fetched 2 state(s) for 2 task(s)"]

    @mock.patch("celery.backends.database.DatabaseBackend.ResultSession")
    def test_should_support_db_backend(self, mock_session, caplog):
        from airflow.providers.celery.executors import celery_executor, celery_executor_utils

        caplog.set_level(logging.DEBUG, logger=self.bulk_state_fetcher_logger)
        with _prepare_app():
            mock_backend = DatabaseBackend(app=celery_executor.app, url="sqlite3://")
            with mock.patch(
                "airflow.providers.celery.executors.celery_executor_utils.Celery.backend", mock_backend
            ):
                caplog.clear()
                mock_session = mock_backend.ResultSession.return_value
                mock_session.scalars.return_value.all.return_value = [
                    mock.MagicMock(**{"to_dict.return_value": {"status": "SUCCESS", "task_id": "123"}})
                ]

                fetcher = celery_executor_utils.BulkStateFetcher(1)
                result = fetcher.get_many(
                    [
                        mock.MagicMock(task_id="123"),
                        mock.MagicMock(task_id="456"),
                    ]
                )

        assert result == {"123": ("SUCCESS", None), "456": ("PENDING", None)}
        assert caplog.messages == ["Fetched 2 state(s) for 2 task(s)"]

    @mock.patch("celery.backends.database.DatabaseBackend.ResultSession")
    def test_should_retry_db_backend(self, mock_session, caplog):
        from airflow.providers.celery.executors import celery_executor, celery_executor_utils

        caplog.set_level(logging.DEBUG, logger=self.bulk_state_fetcher_logger)
        from sqlalchemy.exc import DatabaseError

        with _prepare_app():
            mock_backend = DatabaseBackend(app=celery_executor.app, url="sqlite3://")
            with mock.patch(
                "airflow.providers.celery.executors.celery_executor_utils.Celery.backend", mock_backend
            ):
                caplog.clear()
                mock_session = mock_backend.ResultSession.return_value
                mock_retry_db_result = mock_session.scalars.return_value.all
                mock_retry_db_result.return_value = [
                    mock.MagicMock(**{"to_dict.return_value": {"status": "SUCCESS", "task_id": "123"}})
                ]
                mock_retry_db_result.side_effect = [
                    DatabaseError("DatabaseError", "DatabaseError", "DatabaseError"),
                    mock_retry_db_result.return_value,
                ]

                fetcher = celery_executor_utils.BulkStateFetcher(1)
                result = fetcher.get_many(
                    [
                        mock.MagicMock(task_id="123"),
                        mock.MagicMock(task_id="456"),
                    ]
                )
        assert mock_retry_db_result.call_count == 2
        assert result == {"123": ("SUCCESS", None), "456": ("PENDING", None)}
        assert caplog.messages == [
            "Failed operation _query_task_cls_from_db_backend.  Retrying 2 more times.",
            "Fetched 2 state(s) for 2 task(s)",
        ]

    def test_should_support_base_backend(self, caplog):
        from airflow.providers.celery.executors import celery_executor_utils

        caplog.set_level(logging.DEBUG, logger=self.bulk_state_fetcher_logger)
        with _prepare_app():
            mock_backend = mock.MagicMock(autospec=BaseBackend)

            with mock.patch(
                "airflow.providers.celery.executors.celery_executor_utils.Celery.backend", mock_backend
            ):
                caplog.clear()
                fetcher = celery_executor_utils.BulkStateFetcher(1)
                result = fetcher.get_many(
                    [
                        ClassWithCustomAttributes(task_id="123", state="SUCCESS"),
                        ClassWithCustomAttributes(task_id="456", state="PENDING"),
                    ]
                )

        assert result == {"123": ("SUCCESS", None), "456": ("PENDING", None)}
        assert caplog.messages == ["Fetched 2 state(s) for 2 task(s)"]
