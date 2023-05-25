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
from datetime import datetime
from unittest import mock

# leave this it is used by the test worker
import celery.contrib.testing.tasks  # noqa: F401
import pytest
from celery import Celery
from celery.backends.base import BaseBackend, BaseKeyValueStoreBackend
from celery.backends.database import DatabaseBackend
from celery.contrib.testing.worker import start_worker
from kombu.asynchronous import set_event_loop

from airflow.configuration import conf
from airflow.exceptions import AirflowException, AirflowTaskTimeout
from airflow.executors import celery_executor
from airflow.executors.celery_executor import BulkStateFetcher
from airflow.models.dag import DAG
from airflow.models.taskinstance import SimpleTaskInstance, TaskInstance
from airflow.operators.bash import BashOperator
from airflow.utils.state import State
from tests.test_utils import db


def _prepare_test_bodies():
    if "CELERY_BROKER_URLS" in os.environ:
        return os.environ["CELERY_BROKER_URLS"].split(",")
    return [conf.get("celery", "BROKER_URL")]


class FakeCeleryResult:
    @property
    def state(self):
        raise Exception()

    def task_id(self):
        return "task_id"


@contextlib.contextmanager
def _prepare_app(broker_url=None, execute=None):
    broker_url = broker_url or conf.get("celery", "BROKER_URL")
    execute = execute or celery_executor.execute_command.__wrapped__

    test_config = dict(celery_executor.celery_configuration)
    test_config.update({"broker_url": broker_url})
    test_app = Celery(broker_url, config_source=test_config)
    test_execute = test_app.task(execute)
    patch_app = mock.patch("airflow.executors.celery_executor.app", test_app)
    patch_execute = mock.patch("airflow.executors.celery_executor.execute_command", test_execute)

    backend = test_app.backend

    if hasattr(backend, "ResultSession"):
        # Pre-create the database tables now, otherwise SQLA vis Celery has a
        # race condition where it one of the subprocesses can die with "Table
        # already exists" error, because SQLA checks for which tables exist,
        # then issues a CREATE TABLE, rather than doing CREATE TABLE IF NOT
        # EXISTS
        session = backend.ResultSession()
        session.close()

    with patch_app, patch_execute:
        try:
            yield test_app
        finally:
            # Clear event loop to tear down each celery instance
            set_event_loop(None)


@pytest.mark.integration("celery")
@pytest.mark.backend("mysql", "postgres")
class TestCeleryExecutor:
    def setup_method(self) -> None:
        db.clear_db_runs()
        db.clear_db_jobs()

    def teardown_method(self) -> None:
        db.clear_db_runs()
        db.clear_db_jobs()

    @pytest.mark.parametrize("broker_url", _prepare_test_bodies())
    def test_celery_integration(self, broker_url):
        success_command = ["airflow", "tasks", "run", "true", "some_parameter"]
        fail_command = ["airflow", "version"]

        def fake_execute_command(command):
            if command != success_command:
                raise AirflowException("fail")

        with _prepare_app(broker_url, execute=fake_execute_command) as app:
            executor = celery_executor.CeleryExecutor()
            assert executor.tasks == {}
            executor.start()

            with start_worker(app=app, logfile=sys.stdout, loglevel="info"):
                execute_date = datetime.now()

                task_tuples_to_send = [
                    (
                        ("success", "fake_simple_ti", execute_date, 0),
                        success_command,
                        celery_executor.celery_configuration["task_default_queue"],
                        celery_executor.execute_command,
                    ),
                    (
                        ("fail", "fake_simple_ti", execute_date, 0),
                        fail_command,
                        celery_executor.celery_configuration["task_default_queue"],
                        celery_executor.execute_command,
                    ),
                ]

                # "Enqueue" them. We don't have a real SimpleTaskInstance, so directly edit the dict
                for key, command, queue, task in task_tuples_to_send:
                    executor.queued_tasks[key] = (command, 1, queue, None)
                    executor.task_publish_retries[key] = 1

                executor._process_tasks(task_tuples_to_send)

                assert list(executor.tasks.keys()) == [
                    ("success", "fake_simple_ti", execute_date, 0),
                    ("fail", "fake_simple_ti", execute_date, 0),
                ]
                assert (
                    executor.event_buffer[("success", "fake_simple_ti", execute_date, 0)][0] == State.QUEUED
                )
                assert executor.event_buffer[("fail", "fake_simple_ti", execute_date, 0)][0] == State.QUEUED

                executor.end(synchronous=True)

        assert executor.event_buffer[("success", "fake_simple_ti", execute_date, 0)][0] == State.SUCCESS
        assert executor.event_buffer[("fail", "fake_simple_ti", execute_date, 0)][0] == State.FAILED

        assert "success" not in executor.tasks
        assert "fail" not in executor.tasks

        assert executor.queued_tasks == {}

    def test_error_sending_task(self):
        def fake_execute_command():
            pass

        with _prepare_app(execute=fake_execute_command):
            # fake_execute_command takes no arguments while execute_command takes 1,
            # which will cause TypeError when calling task.apply_async()
            executor = celery_executor.CeleryExecutor()
            task = BashOperator(
                task_id="test", bash_command="true", dag=DAG(dag_id="id"), start_date=datetime.now()
            )
            when = datetime.now()
            value_tuple = (
                "command",
                1,
                None,
                SimpleTaskInstance.from_ti(ti=TaskInstance(task=task, run_id=None)),
            )
            key = ("fail", "fake_simple_ti", when, 0)
            executor.queued_tasks[key] = value_tuple
            executor.task_publish_retries[key] = 1
            executor.heartbeat()
        assert 0 == len(executor.queued_tasks), "Task should no longer be queued"
        assert executor.event_buffer[("fail", "fake_simple_ti", when, 0)][0] == State.FAILED

    def test_retry_on_error_sending_task(self, caplog):
        """Test that Airflow retries publishing tasks to Celery Broker at least 3 times"""

        with _prepare_app(), caplog.at_level(logging.INFO), mock.patch.object(
            # Mock `with timeout()` to _instantly_ fail.
            celery_executor.timeout,
            "__enter__",
            side_effect=AirflowTaskTimeout,
        ):
            executor = celery_executor.CeleryExecutor()
            assert executor.task_publish_retries == {}
            assert executor.task_publish_max_retries == 3, "Assert Default Max Retries is 3"

            task = BashOperator(
                task_id="test", bash_command="true", dag=DAG(dag_id="id"), start_date=datetime.now()
            )
            when = datetime.now()
            value_tuple = (
                "command",
                1,
                None,
                SimpleTaskInstance.from_ti(ti=TaskInstance(task=task, run_id=None)),
            )
            key = ("fail", "fake_simple_ti", when, 0)
            executor.queued_tasks[key] = value_tuple

            # Test that when heartbeat is called again, task is published again to Celery Queue
            executor.heartbeat()
            assert dict(executor.task_publish_retries) == {key: 1}
            assert 1 == len(executor.queued_tasks), "Task should remain in queue"
            assert executor.event_buffer == {}
            assert f"[Try 1 of 3] Task Timeout Error for Task: ({key})." in caplog.text

            executor.heartbeat()
            assert dict(executor.task_publish_retries) == {key: 2}
            assert 1 == len(executor.queued_tasks), "Task should remain in queue"
            assert executor.event_buffer == {}
            assert f"[Try 2 of 3] Task Timeout Error for Task: ({key})." in caplog.text

            executor.heartbeat()
            assert dict(executor.task_publish_retries) == {key: 3}
            assert 1 == len(executor.queued_tasks), "Task should remain in queue"
            assert executor.event_buffer == {}
            assert f"[Try 3 of 3] Task Timeout Error for Task: ({key})." in caplog.text

            executor.heartbeat()
            assert dict(executor.task_publish_retries) == {}
            assert 0 == len(executor.queued_tasks), "Task should no longer be in queue"
            assert executor.event_buffer[("fail", "fake_simple_ti", when, 0)][0] == State.FAILED


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

    def __ne__(self, other):
        return not self.__eq__(other)


@pytest.mark.integration("celery")
@pytest.mark.backend("mysql", "postgres")
class TestBulkStateFetcher:
    bulk_state_fetcher_logger = "airflow.executors.celery_executor.BulkStateFetcher"

    @mock.patch(
        "celery.backends.base.BaseKeyValueStoreBackend.mget",
        return_value=[json.dumps({"status": "SUCCESS", "task_id": "123"})],
    )
    def test_should_support_kv_backend(self, mock_mget, caplog):
        caplog.set_level(logging.DEBUG, logger=self.bulk_state_fetcher_logger)
        with _prepare_app():
            mock_backend = BaseKeyValueStoreBackend(app=celery_executor.app)
            with mock.patch("airflow.executors.celery_executor.Celery.backend", mock_backend):
                caplog.clear()
                fetcher = BulkStateFetcher()
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
        caplog.set_level(logging.DEBUG, logger=self.bulk_state_fetcher_logger)
        with _prepare_app():
            mock_backend = DatabaseBackend(app=celery_executor.app, url="sqlite3://")
            with mock.patch("airflow.executors.celery_executor.Celery.backend", mock_backend):
                caplog.clear()
                mock_session = mock_backend.ResultSession.return_value
                mock_session.query.return_value.filter.return_value.all.return_value = [
                    mock.MagicMock(**{"to_dict.return_value": {"status": "SUCCESS", "task_id": "123"}})
                ]

                fetcher = BulkStateFetcher()
                result = fetcher.get_many(
                    [
                        mock.MagicMock(task_id="123"),
                        mock.MagicMock(task_id="456"),
                    ]
                )

        assert result == {"123": ("SUCCESS", None), "456": ("PENDING", None)}
        assert caplog.messages == ["Fetched 2 state(s) for 2 task(s)"]

    def test_should_support_base_backend(self, caplog):
        caplog.set_level(logging.DEBUG, logger=self.bulk_state_fetcher_logger)
        with _prepare_app():
            mock_backend = mock.MagicMock(autospec=BaseBackend)

            with mock.patch("airflow.executors.celery_executor.Celery.backend", mock_backend):
                caplog.clear()
                fetcher = BulkStateFetcher(1)
                result = fetcher.get_many(
                    [
                        ClassWithCustomAttributes(task_id="123", state="SUCCESS"),
                        ClassWithCustomAttributes(task_id="456", state="PENDING"),
                    ]
                )

        assert result == {"123": ("SUCCESS", None), "456": ("PENDING", None)}
        assert caplog.messages == ["Fetched 2 state(s) for 2 task(s)"]
