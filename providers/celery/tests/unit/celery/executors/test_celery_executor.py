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
import logging
import os
import signal
import sys
from datetime import timedelta
from unittest import mock

# leave this it is used by the test worker
import celery.contrib.testing.tasks  # noqa: F401
import pytest
import time_machine
from celery import Celery
from celery.result import AsyncResult
from kombu.asynchronous import set_event_loop

from airflow.configuration import conf
from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.models.dag import DAG
from airflow.models.taskinstance import TaskInstance, TaskInstanceKey
from airflow.providers.celery.executors import celery_executor, celery_executor_utils, default_celery
from airflow.providers.celery.executors.celery_executor import CeleryExecutor
from airflow.utils.state import State

from tests_common.test_utils import db
from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.dag import sync_dag_to_db
from tests_common.test_utils.taskinstance import create_task_instance
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS, AIRFLOW_V_3_1_PLUS, AIRFLOW_V_3_2_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.models.dag_version import DagVersion
if AIRFLOW_V_3_1_PLUS:
    from airflow.sdk import BaseOperator, timezone
else:
    from airflow.models.baseoperator import BaseOperator  # type: ignore[attr-defined,no-redef]
    from airflow.utils import timezone  # type: ignore[attr-defined,no-redef]

pytestmark = pytest.mark.db_test


FAKE_EXCEPTION_MSG = "Fake Exception"


def _prepare_test_bodies():
    if "CELERY_BROKER_URLS" in os.environ:
        return [(url,) for url in os.environ["CELERY_BROKER_URLS"].split(",")]
    return [(conf.get("celery", "BROKER_URL"))]


class FakeCeleryResult:
    @property
    def state(self):
        raise Exception(FAKE_EXCEPTION_MSG)

    def task_id(self):
        return "task_id"


@contextlib.contextmanager
def _prepare_app(broker_url=None, execute=None):
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
    test_execute = test_app.task(execute)
    patch_app = mock.patch.object(celery_executor_utils, "app", test_app)
    patch_execute = mock.patch.object(celery_executor_utils, execute_name, test_execute)

    backend = test_app.backend

    if hasattr(backend, "ResultSession"):
        # Pre-create the database tables now, otherwise SQLA via Celery has a
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


class TestCeleryExecutor:
    def setup_method(self) -> None:
        db.clear_db_runs()
        db.clear_db_jobs()

    def teardown_method(self) -> None:
        db.clear_db_runs()
        db.clear_db_jobs()

    @pytest.mark.skipif(not AIRFLOW_V_3_2_PLUS, reason="Airflow 3.2+ prefers new configuration")
    def test_sentry_integration(self):
        assert CeleryExecutor.sentry_integration == "sentry_sdk.integrations.celery.CeleryIntegration"

    @pytest.mark.skipif(AIRFLOW_V_3_2_PLUS, reason="Test only for Airflow < 3.2")
    def test_supports_sentry(self):
        assert CeleryExecutor.supports_sentry

    def test_cli_commands_vended(self):
        assert CeleryExecutor.get_cli_commands()

    def test_celery_executor_init_with_args_kwargs(self):
        """Test that CeleryExecutor properly passes args and kwargs to BaseExecutor."""
        parallelism = 50
        team_name = "test_team"

        if AIRFLOW_V_3_1_PLUS:
            # team_name was added in Airflow 3.1
            executor = celery_executor.CeleryExecutor(parallelism=parallelism, team_name=team_name)
        else:
            executor = celery_executor.CeleryExecutor(parallelism)

        assert executor.parallelism == parallelism

        if AIRFLOW_V_3_1_PLUS:
            # team_name was added in Airflow 3.1
            assert executor.team_name == team_name
            assert executor.conf.team_name == team_name

    @pytest.mark.backend("mysql", "postgres")
    def test_exception_propagation(self, caplog):
        caplog.set_level(
            logging.ERROR, logger="airflow.providers.celery.executors.celery_executor_utils.BulkStateFetcher"
        )
        with _prepare_app():
            executor = celery_executor.CeleryExecutor()
            executor.tasks = {"key": FakeCeleryResult()}
            executor.bulk_state_fetcher._get_many_using_multiprocessing(executor.tasks.values())
        assert celery_executor_utils.CELERY_FETCH_ERR_MSG_HEADER in caplog.text, caplog.record_tuples
        assert FAKE_EXCEPTION_MSG in caplog.text, caplog.record_tuples

    @mock.patch("airflow.providers.celery.executors.celery_executor.CeleryExecutor.sync")
    @mock.patch("airflow.providers.celery.executors.celery_executor.CeleryExecutor.trigger_tasks")
    @mock.patch("airflow.executors.base_executor.Stats.gauge")
    def test_gauge_executor_metrics(self, mock_stats_gauge, mock_trigger_tasks, mock_sync):
        executor = celery_executor.CeleryExecutor()
        executor.heartbeat()
        calls = [
            mock.call(
                "executor.open_slots", value=mock.ANY, tags={"status": "open", "name": "CeleryExecutor"}
            ),
            mock.call(
                "executor.queued_tasks", value=mock.ANY, tags={"status": "queued", "name": "CeleryExecutor"}
            ),
            mock.call(
                "executor.running_tasks", value=mock.ANY, tags={"status": "running", "name": "CeleryExecutor"}
            ),
        ]
        mock_stats_gauge.assert_has_calls(calls)

    @pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Airflow 3 doesn't have execute_command anymore")
    @pytest.mark.parametrize(
        ("command", "raise_exception"),
        [
            pytest.param(["true"], True, id="wrong-command"),
            pytest.param(["airflow", "tasks"], True, id="incomplete-command"),
            pytest.param(["airflow", "tasks", "run"], False, id="complete-command"),
        ],
    )
    def test_command_validation(self, command, raise_exception):
        """Check that we validate _on the receiving_ side, not just sending side"""
        expected_context = contextlib.nullcontext()
        if raise_exception:
            expected_context = pytest.raises(
                ValueError, match=r'The command must start with \["airflow", "tasks", "run"\]\.'
            )

        with (
            mock.patch(
                "airflow.providers.celery.executors.celery_executor_utils._execute_in_subprocess"
            ) as mock_subproc,
            mock.patch(
                "airflow.providers.celery.executors.celery_executor_utils._execute_in_fork"
            ) as mock_fork,
            mock.patch("celery.app.task.Task.request") as mock_task,
        ):
            mock_task.id = "abcdef-124215-abcdef"
            with expected_context:
                celery_executor_utils.execute_command(command)
            if raise_exception:
                mock_subproc.assert_not_called()
                mock_fork.assert_not_called()
            else:
                # One of these should be called.
                assert mock_subproc.call_args == (
                    (command, "abcdef-124215-abcdef"),
                ) or mock_fork.call_args == ((command, "abcdef-124215-abcdef"),)

    @pytest.mark.backend("mysql", "postgres")
    def test_try_adopt_task_instances_none(self, clean_dags_dagruns_and_dagbundles, testing_dag_bundle):
        start_date = timezone.utcnow() - timedelta(days=2)

        with DAG("test_try_adopt_task_instances_none", schedule=None) as dag:
            task_1 = BaseOperator(task_id="task_1", start_date=start_date)

        if AIRFLOW_V_3_0_PLUS:
            sync_dag_to_db(dag)
            dag_version = DagVersion.get_latest_version(dag.dag_id)
            key1 = create_task_instance(task=task_1, run_id=None, dag_version_id=dag_version.id)
        else:
            key1 = TaskInstance(task=task_1, run_id=None)
        tis = [key1]

        executor = celery_executor.CeleryExecutor()

        assert executor.try_adopt_task_instances(tis) == tis

    @pytest.mark.backend("mysql", "postgres")
    @time_machine.travel("2020-01-01", tick=False)
    def test_try_adopt_task_instances(self, clean_dags_dagruns_and_dagbundles, testing_dag_bundle):
        start_date = timezone.utcnow() - timedelta(days=2)

        with DAG("test_try_adopt_task_instances_none", schedule=None) as dag:
            task_1 = BaseOperator(task_id="task_1", start_date=start_date)
            task_2 = BaseOperator(task_id="task_2", start_date=start_date)

        if AIRFLOW_V_3_0_PLUS:
            sync_dag_to_db(dag)
            dag_version = DagVersion.get_latest_version(dag.dag_id)
            ti1 = create_task_instance(task=task_1, run_id=None, dag_version_id=dag_version.id)
            ti2 = create_task_instance(task=task_2, run_id=None, dag_version_id=dag_version.id)
        else:
            ti1 = TaskInstance(task=task_1, run_id=None)
            ti2 = TaskInstance(task=task_2, run_id=None)
        ti1.external_executor_id = "231"
        ti1.state = State.QUEUED
        ti2.external_executor_id = "232"
        ti2.state = State.QUEUED

        tis = [ti1, ti2]

        executor = celery_executor.CeleryExecutor()
        assert executor.running == set()
        assert executor.tasks == {}

        not_adopted_tis = executor.try_adopt_task_instances(tis)

        key_1 = TaskInstanceKey(dag.dag_id, task_1.task_id, None, 0)
        key_2 = TaskInstanceKey(dag.dag_id, task_2.task_id, None, 0)
        assert executor.running == {key_1, key_2}

        assert executor.tasks == {key_1: AsyncResult("231"), key_2: AsyncResult("232")}
        assert not_adopted_tis == []

    @pytest.fixture
    def mock_celery_revoke(self):
        with _prepare_app() as app:
            app.control.revoke = mock.MagicMock()
            yield app.control.revoke

    @pytest.mark.backend("mysql", "postgres")
    @mock.patch("airflow.providers.celery.executors.celery_executor.CeleryExecutor.fail")
    def test_cleanup_stuck_queued_tasks(
        self, mock_fail, clean_dags_dagruns_and_dagbundles, testing_dag_bundle
    ):
        start_date = timezone.utcnow() - timedelta(days=2)

        with DAG("test_cleanup_stuck_queued_tasks_failed", schedule=None) as dag:
            task = BaseOperator(task_id="task_1", start_date=start_date)

        if AIRFLOW_V_3_0_PLUS:
            sync_dag_to_db(dag)
            dag_version = DagVersion.get_latest_version(task.dag.dag_id)
            ti = create_task_instance(task=task, run_id=None, dag_version_id=dag_version.id)
        else:
            ti = TaskInstance(task=task, run_id=None)
        ti.external_executor_id = "231"
        ti.state = State.QUEUED
        ti.queued_dttm = timezone.utcnow() - timedelta(minutes=30)
        ti.queued_by_job_id = 1
        tis = [ti]
        with _prepare_app() as app:
            app.control.revoke = mock.MagicMock()
            executor = celery_executor.CeleryExecutor()
            executor.job_id = 1
            executor.running = {ti.key}
            executor.tasks = {ti.key: AsyncResult("231")}
            assert executor.has_task(ti)
            with pytest.warns(AirflowProviderDeprecationWarning, match="cleanup_stuck_queued_tasks"):
                executor.cleanup_stuck_queued_tasks(tis=tis)
            executor.sync()
        assert executor.tasks == {}
        app.control.revoke.assert_called_once_with("231")
        mock_fail.assert_called()
        assert not executor.has_task(ti)

    @pytest.mark.backend("mysql", "postgres")
    @mock.patch("airflow.providers.celery.executors.celery_executor.CeleryExecutor.fail")
    def test_revoke_task(self, mock_fail, clean_dags_dagruns_and_dagbundles, testing_dag_bundle):
        start_date = timezone.utcnow() - timedelta(days=2)

        with DAG("test_revoke_task", schedule=None) as dag:
            task = BaseOperator(task_id="task_1", start_date=start_date)

        if AIRFLOW_V_3_0_PLUS:
            sync_dag_to_db(dag)
            dag_version = DagVersion.get_latest_version(task.dag.dag_id)
            ti = create_task_instance(task=task, run_id=None, dag_version_id=dag_version.id)
        else:
            ti = TaskInstance(task=task, run_id=None)
        ti.external_executor_id = "231"
        ti.state = State.QUEUED
        ti.queued_dttm = timezone.utcnow() - timedelta(minutes=30)
        ti.queued_by_job_id = 1
        tis = [ti]
        with _prepare_app() as app:
            app.control.revoke = mock.MagicMock()
            executor = celery_executor.CeleryExecutor()
            executor.job_id = 1
            executor.running = {ti.key}
            executor.tasks = {ti.key: AsyncResult("231")}
            assert executor.has_task(ti)
            for ti in tis:
                executor.revoke_task(ti=ti)
            executor.sync()
        app.control.revoke.assert_called_once_with("231")
        assert executor.tasks == {}
        assert not executor.has_task(ti)
        mock_fail.assert_not_called()

    @conf_vars({("celery", "result_backend_sqlalchemy_engine_options"): '{"pool_recycle": 1800}'})
    @mock.patch("celery.Celery")
    def test_result_backend_sqlalchemy_engine_options(self, mock_celery):
        import importlib

        # reload celery conf to apply the new config
        importlib.reload(default_celery)
        # reload celery_executor_utils to recreate the celery app with new config
        importlib.reload(celery_executor_utils)

        call_args = mock_celery.call_args.kwargs.get("config_source")
        assert "database_engine_options" in call_args
        assert call_args["database_engine_options"] == {"pool_recycle": 1800}


def test_operation_timeout_config():
    assert celery_executor_utils.OPERATION_TIMEOUT == 1


class MockTask:
    """
    A picklable object used to mock tasks sent to Celery. Can't use the mock library
    here because it's not picklable.
    """

    def apply_async(self, *args, **kwargs):
        return 1


def _exit_gracefully(signum, _):
    print(f"{os.getpid()} Exiting gracefully upon receiving signal {signum}")
    sys.exit(signum)


@pytest.fixture
def register_signals():
    """
    Register the same signals as scheduler does to test celery_executor to make sure it does not
    hang.
    """
    orig_sigint = orig_sigterm = orig_sigusr2 = signal.SIG_DFL

    orig_sigint = signal.signal(signal.SIGINT, _exit_gracefully)
    orig_sigterm = signal.signal(signal.SIGTERM, _exit_gracefully)
    orig_sigusr2 = signal.signal(signal.SIGUSR2, _exit_gracefully)

    yield

    # Restore original signal handlers after test
    signal.signal(signal.SIGINT, orig_sigint)
    signal.signal(signal.SIGTERM, orig_sigterm)
    signal.signal(signal.SIGUSR2, orig_sigusr2)


@pytest.mark.execution_timeout(200)
@pytest.mark.quarantined
def test_send_tasks_to_celery_hang(register_signals):
    """
    Test that celery_executor does not hang after many runs.
    """
    executor = celery_executor.CeleryExecutor()

    task = MockTask()
    task_tuples_to_send = [(None, None, None, task) for _ in range(26)]

    for _ in range(250):
        # This loop can hang on Linux if celery_executor does something wrong with
        # multiprocessing.
        results = executor._send_tasks_to_celery(task_tuples_to_send)
        assert results == [(None, None, 1) for _ in task_tuples_to_send]


@conf_vars({("celery", "result_backend"): "rediss://test_user:test_password@localhost:6379/0"})
def test_celery_executor_with_no_recommended_result_backend(caplog):
    import importlib

    from airflow.providers.celery.executors.default_celery import log

    with caplog.at_level(logging.WARNING, logger=log.name):
        # reload celery conf to apply the new config
        importlib.reload(default_celery)
        assert "test_password" not in caplog.text
        assert (
            "You have configured a result_backend using the protocol `rediss`,"
            " it is highly recommended to use an alternative result_backend (i.e. a database)."
        ) in caplog.text


@conf_vars({("celery_broker_transport_options", "sentinel_kwargs"): '{"service_name": "mymaster"}'})
def test_sentinel_kwargs_loaded_from_string():
    import importlib

    # reload celery conf to apply the new config
    importlib.reload(default_celery)
    assert default_celery.DEFAULT_CELERY_CONFIG["broker_transport_options"]["sentinel_kwargs"] == {
        "service_name": "mymaster"
    }


@conf_vars({("celery", "task_acks_late"): "False"})
def test_celery_task_acks_late_loaded_from_string():
    import importlib

    # reload celery conf to apply the new config
    importlib.reload(default_celery)
    assert default_celery.DEFAULT_CELERY_CONFIG["task_acks_late"] is False


@conf_vars({("celery", "extra_celery_config"): '{"worker_max_tasks_per_child": 10}'})
def test_celery_extra_celery_config_loaded_from_string():
    import importlib

    # reload celery conf to apply the new config
    importlib.reload(default_celery)
    assert default_celery.DEFAULT_CELERY_CONFIG["worker_max_tasks_per_child"] == 10


@conf_vars({("celery_result_backend_transport_options", "sentinel_kwargs"): '{"password": "redis_password"}'})
def test_result_backend_sentinel_kwargs_loaded_from_string():
    """Test that sentinel_kwargs for result backend transport options is correctly parsed."""
    import importlib

    # reload celery conf to apply the new config
    importlib.reload(default_celery)
    assert "result_backend_transport_options" in default_celery.DEFAULT_CELERY_CONFIG
    assert default_celery.DEFAULT_CELERY_CONFIG["result_backend_transport_options"]["sentinel_kwargs"] == {
        "password": "redis_password"
    }


@conf_vars({("celery_result_backend_transport_options", "master_name"): "mymaster"})
def test_result_backend_master_name_loaded():
    """Test that master_name for result backend transport options is correctly loaded."""
    import importlib

    # reload celery conf to apply the new config
    importlib.reload(default_celery)
    assert "result_backend_transport_options" in default_celery.DEFAULT_CELERY_CONFIG
    assert (
        default_celery.DEFAULT_CELERY_CONFIG["result_backend_transport_options"]["master_name"] == "mymaster"
    )


@conf_vars(
    {
        ("celery_result_backend_transport_options", "sentinel_kwargs"): '{"password": "redis_password"}',
        ("celery_result_backend_transport_options", "master_name"): "mymaster",
    }
)
def test_result_backend_transport_options_with_multiple_options():
    """Test that multiple result backend transport options are correctly loaded."""
    import importlib

    # reload celery conf to apply the new config
    importlib.reload(default_celery)
    result_backend_opts = default_celery.DEFAULT_CELERY_CONFIG["result_backend_transport_options"]
    assert result_backend_opts["sentinel_kwargs"] == {"password": "redis_password"}
    assert result_backend_opts["master_name"] == "mymaster"


@conf_vars({("celery_result_backend_transport_options", "sentinel_kwargs"): "invalid_json"})
def test_result_backend_sentinel_kwargs_invalid_json():
    """Test that invalid JSON in sentinel_kwargs raises an error."""
    import importlib

    from airflow.providers.common.compat.sdk import AirflowException

    with pytest.raises(
        AirflowException, match="sentinel_kwargs.*should be written in the correct dictionary format"
    ):
        importlib.reload(default_celery)


@conf_vars({("celery_result_backend_transport_options", "sentinel_kwargs"): '"not_a_dict"'})
def test_result_backend_sentinel_kwargs_not_dict():
    """Test that non-dict sentinel_kwargs raises an error."""
    import importlib

    from airflow.providers.common.compat.sdk import AirflowException

    with pytest.raises(
        AirflowException, match="sentinel_kwargs.*should be written in the correct dictionary format"
    ):
        importlib.reload(default_celery)


@conf_vars(
    {
        ("celery", "result_backend"): "sentinel://sentinel1:26379;sentinel://sentinel2:26379",
        ("celery_result_backend_transport_options", "sentinel_kwargs"): '{"password": "redis_pass"}',
        ("celery_result_backend_transport_options", "master_name"): "mymaster",
    }
)
def test_result_backend_sentinel_full_config():
    """Test full Redis Sentinel configuration for result backend."""
    import importlib

    # reload celery conf to apply the new config
    importlib.reload(default_celery)

    assert default_celery.DEFAULT_CELERY_CONFIG["result_backend"] == (
        "sentinel://sentinel1:26379;sentinel://sentinel2:26379"
    )
    result_backend_opts = default_celery.DEFAULT_CELERY_CONFIG["result_backend_transport_options"]
    assert result_backend_opts["sentinel_kwargs"] == {"password": "redis_pass"}
    assert result_backend_opts["master_name"] == "mymaster"
