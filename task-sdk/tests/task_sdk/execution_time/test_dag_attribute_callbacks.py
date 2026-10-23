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

import textwrap
import uuid
from pathlib import Path
from unittest import mock

import pytest
import structlog

from airflow.sdk import DAG, BaseOperator
from airflow.sdk.api.datamodels._generated import (
    BundleInfo,
    DagCallbackContext,
    DagRun,
    EmailCallbackContext,
    TaskCallbackContext,
    TaskInstance,
    TaskInstanceState,
    TIRunContext,
)
from airflow.sdk.execution_time.dag_attribute_callbacks import run_dag_attribute_callbacks

TIMESTAMP = "2026-10-22T12:00:00Z"
DAG_RUN = DagRun.model_validate(
    {
        "dag_id": "test_dag",
        "run_id": "test_run",
        "logical_date": TIMESTAMP,
        "data_interval_start": TIMESTAMP,
        "data_interval_end": TIMESTAMP,
        "run_after": TIMESTAMP,
        "start_date": TIMESTAMP,
        "end_date": None,
        "run_type": "manual",
        "state": "failed",
        "consumed_asset_events": [],
        "partition_key": None,
    }
)
TI = TaskInstance(
    id=uuid.uuid4(),
    task_id="task",
    dag_id="test_dag",
    run_id="test_run",
    try_number=1,
    dag_version_id=uuid.uuid4(),
)
TI_CONTEXT = TIRunContext(dag_run=DAG_RUN, max_tries=1)
BUNDLE_INFO = BundleInfo(name="test-bundle", version=None)
LOG = structlog.get_logger()


@pytest.fixture
def calls():
    return []


@pytest.fixture
def dag(calls):
    def record(name):
        def callback(context):
            calls.append((name, context))

        callback.__name__ = name
        return callback

    def raise_error(context):
        raise ValueError("boom")

    with DAG(
        dag_id="test_dag",
        on_failure_callback=[raise_error, record("dag_failure")],
        on_success_callback=record("dag_success"),
    ) as dag:
        BaseOperator(
            task_id="task",
            on_failure_callback=record("task_failure"),
            on_retry_callback=record("task_retry"),
            email="team@example.com",
            email_on_failure=True,
            email_on_retry=False,
        )
        BaseOperator(task_id="no_callbacks")
    return dag


def _run(context, dag):
    bundle = mock.Mock(path=Path("/bundle"), version=None)
    bundle.name = "test-bundle"
    with (
        mock.patch(
            "airflow.sdk.execution_time.bundles.initialize_ti_bundle", autospec=True, return_value=bundle
        ),
        mock.patch(
            "airflow.sdk.execution_time.dag_attribute_callbacks._load_dag", autospec=True, return_value=dag
        ) as load_dag,
    ):
        errors = run_dag_attribute_callbacks(
            context, dag_rel_path=Path("dags/test_dag.py"), bundle_info=BUNDLE_INFO, log=LOG
        )
    load_dag.assert_called_once_with(
        "test_dag", Path("/bundle/dags/test_dag.py"), Path("/bundle"), "test-bundle"
    )
    return errors


class TestDagCallbacks:
    def test_failure_callbacks_all_run_and_failures_are_returned(self, dag, calls):
        context = DagCallbackContext(
            type="DagCallback", dag_id="test_dag", run_id="test_run", msg="task_failure"
        )

        errors = _run(context, dag)

        assert errors == ["raise_error: ValueError: boom"]
        ((name, template_context),) = calls
        assert name == "dag_failure"
        assert template_context["dag"] is dag
        assert template_context["run_id"] == "test_run"
        assert template_context["reason"] == "task_failure"

    @pytest.mark.parametrize(
        ("dag_run", "expected_tags"),
        [
            pytest.param(None, {"dag_id": "test_dag"}, id="no-team"),
            pytest.param(
                DAG_RUN.model_copy(update={"team_name": "team_a"}),
                {"dag_id": "test_dag", "team_name": "team_a"},
                id="team",
            ),
        ],
    )
    @mock.patch("airflow.sdk.execution_time.dag_attribute_callbacks.stats", autospec=True)
    def test_failing_callbacks_are_counted(self, mock_stats, dag, dag_run, expected_tags):
        """The Dag processor counts them in the same metric."""
        context = DagCallbackContext(
            type="DagCallback", dag_id="test_dag", run_id="test_run", dag_run=dag_run
        )

        _run(context, dag)

        mock_stats.incr.assert_called_once_with("dag.callback_exceptions", 1, tags=expected_tags)

    def test_success_callbacks_run_for_a_successful_run(self, dag, calls):
        context = DagCallbackContext(
            type="DagCallback", dag_id="test_dag", run_id="test_run", is_failure_callback=False
        )

        assert _run(context, dag) == []
        assert [name for name, _ in calls] == ["dag_success"]

    def test_the_last_task_instance_enriches_the_context(self, dag, calls):
        context = DagCallbackContext(
            type="DagCallback",
            dag_id="test_dag",
            run_id="test_run",
            is_failure_callback=False,
            msg="success",
            dag_run=DAG_RUN,
            last_ti=TI,
        )

        assert _run(context, dag) == []
        ((_, template_context),) = calls
        assert template_context["ti"].task_id == "task"
        assert template_context["dag_run"].run_id == "test_run"
        assert template_context["reason"] == "success"

    def test_a_removed_last_task_leaves_a_minimal_context(self, dag, calls):
        context = DagCallbackContext(
            type="DagCallback",
            dag_id="test_dag",
            run_id="test_run",
            is_failure_callback=False,
            dag_run=DAG_RUN,
            last_ti=TI.model_copy(update={"task_id": "removed"}),
        )

        assert _run(context, dag) == []
        ((_, template_context),) = calls
        assert "ti" not in template_context

    def test_a_dag_without_callbacks_succeeds(self, calls):
        with DAG(dag_id="test_dag") as dag:
            BaseOperator(task_id="task")

        assert _run(DagCallbackContext(type="DagCallback", dag_id="test_dag", run_id="test_run"), dag) == []
        assert calls == []


class TestTaskCallbacks:
    @pytest.mark.parametrize(
        ("callback_type", "expected"),
        [
            pytest.param(None, "task_failure", id="unset-is-failure"),
            pytest.param(TaskInstanceState.FAILED, "task_failure", id="failed"),
            pytest.param(TaskInstanceState.UP_FOR_RETRY, "task_retry", id="retry"),
        ],
    )
    def test_runs_the_callbacks_of_the_callback_type(self, dag, calls, callback_type, expected):
        context = TaskCallbackContext(
            type="TaskCallback", ti=TI, task_callback_type=callback_type, ti_context=TI_CONTEXT
        )

        assert _run(context, dag) == []
        ((name, template_context),) = calls
        assert name == expected
        assert template_context["ti"].task_id == "task"
        assert template_context["ti"].max_tries == 1

    def test_runs_without_the_task_run_context(self, dag, calls):
        context = TaskCallbackContext(type="TaskCallback", ti=TI)

        assert _run(context, dag) == []
        ((name, template_context),) = calls
        assert name == "task_failure"
        assert template_context["ti"].task_id == "task"

    def test_a_non_failure_callback_type_runs_nothing(self, dag, calls):
        context = TaskCallbackContext(
            type="TaskCallback", ti=TI, task_callback_type=TaskInstanceState.SUCCESS
        )

        assert _run(context, dag) == []
        assert calls == []

    def test_a_task_without_callbacks_succeeds(self, dag, calls):
        context = TaskCallbackContext(
            type="TaskCallback", ti=TI.model_copy(update={"task_id": "no_callbacks"})
        )

        assert _run(context, dag) == []
        assert calls == []

    def test_a_removed_task_fails(self, dag):
        context = TaskCallbackContext(type="TaskCallback", ti=TI.model_copy(update={"task_id": "removed"}))

        assert _run(context, dag) == ["ValueError: Task 'removed' was not found in Dag 'test_dag'"]


@mock.patch("airflow.sdk.execution_time.task_runner._send_error_email_notification", autospec=True)
class TestTaskEmails:
    def test_sends_a_failure_email(self, send_email, dag):
        context = EmailCallbackContext(
            type="EmailCallback", ti=TI, email_type="failure", msg="it broke", ti_context=TI_CONTEXT
        )

        assert _run(context, dag) == []
        task, runtime_ti, _, error, _ = send_email.call_args.args
        assert task is dag.get_task("task")
        assert runtime_ti.task_id == "task"
        assert str(error) == "it broke"

    @pytest.mark.parametrize(
        ("task_id", "email_type"),
        [
            pytest.param("task", "retry", id="email-on-retry-off"),
            pytest.param("no_callbacks", "failure", id="no-email-configured"),
        ],
    )
    def test_sends_nothing_the_task_does_not_ask_for(self, send_email, dag, task_id, email_type):
        context = EmailCallbackContext(
            type="EmailCallback",
            ti=TI.model_copy(update={"task_id": task_id}),
            email_type=email_type,
            ti_context=TI_CONTEXT,
        )

        assert _run(context, dag) == []
        send_email.assert_not_called()

    def test_a_failed_send_is_returned(self, send_email, dag):
        send_email.side_effect = OSError("SMTP down")
        context = EmailCallbackContext(
            type="EmailCallback", ti=TI, email_type="failure", ti_context=TI_CONTEXT
        )

        assert _run(context, dag) == ["OSError: SMTP down"]


class TestLoadingTheDag:
    @pytest.fixture
    def bundle(self, tmp_path):
        (tmp_path / "dags").mkdir()
        (tmp_path / "dags" / "test_dag.py").write_text(
            textwrap.dedent(
                """
                from airflow.sdk import DAG, BaseOperator

                with DAG(dag_id="test_dag", on_success_callback=lambda context: print("callback ran")):
                    BaseOperator(task_id="task")
                """
            )
        )
        bundle = mock.Mock(path=tmp_path, version=None)
        bundle.name = "test-bundle"
        with mock.patch(
            "airflow.sdk.execution_time.bundles.initialize_ti_bundle", autospec=True, return_value=bundle
        ):
            yield bundle

    def _run(self, dag_id):
        context = DagCallbackContext(
            type="DagCallback", dag_id=dag_id, run_id="test_run", is_failure_callback=False
        )
        return run_dag_attribute_callbacks(
            context, dag_rel_path=Path("dags/test_dag.py"), bundle_info=BUNDLE_INFO, log=LOG
        )

    def test_parses_the_dag_file_in_the_bundle(self, bundle, capsys):
        assert self._run("test_dag") == []
        assert "callback ran" in capsys.readouterr().out

    def test_a_dag_missing_from_the_file_fails(self, bundle):
        (error,) = self._run("other_dag")

        assert error.startswith("ValueError: Dag 'other_dag' was not found in")

    def test_a_lang_sdk_dag_fails(self, bundle):
        importer = mock.Mock(coordinator_classpath="my.Coordinator")
        with mock.patch(
            "airflow.sdk.execution_time.task_runner._find_native_dag_importer",
            autospec=True,
            return_value=importer,
        ):
            (error,) = self._run("test_dag")

        assert "native Lang-SDK Dag parsed by my.Coordinator" in error

    def test_a_bundle_that_cannot_be_prepared_fails(self):
        with mock.patch(
            "airflow.sdk.execution_time.bundles.initialize_ti_bundle",
            autospec=True,
            side_effect=OSError("clone failed"),
        ):
            assert self._run("test_dag") == ["OSError: clone failed"]
