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

import itertools
import warnings
from datetime import datetime, timedelta, timezone
from unittest import mock

import pytest
from snowflake.connector.errors import ProgrammingError

from airflow.providers.common.compat.sdk import TaskDeferred
from airflow.providers.snowflake.operators.snowpark_containers import (
    _DURABLE_UNSET,
    SnowparkContainerJobOperator,
    _warn_and_disable_durable_pre_3_3,
)
from airflow.providers.snowflake.triggers.snowpark_containers import SnowparkContainerJobTrigger
from airflow.providers.snowflake.utils.snowpark_containers import (
    NOT_FOUND_STATUS,
    OBJECT_NOT_EXIST_ERROR_CODE,
)

from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_PLUS

TASK_ID = "test_spcs_job"
COMPUTE_POOL = "test_pool"
CONTAINER_NAME = "main"
SPEC = "spec.yaml"
SPEC_STAGE = "@test_stage"
SPEC_TEXT = "spec:\n  containers:\n  - name: main\n    image: /db/schema/repo/img:latest"
JOB_NAME = "TEST_JOB"
SNOWFLAKE_CONN_ID = "snowflake_default"
MOCK_HOOK_PATH = "airflow.providers.snowflake.operators.snowpark_containers.SnowflakeHook"
SUBMIT_RESPONSE = [f"Started Snowpark Container Services Job '{JOB_NAME}'."]


def _make_operator(**kwargs):
    defaults = {
        "task_id": TASK_ID,
        "compute_pool": COMPUTE_POOL,
        "container_name": CONTAINER_NAME,
        "spec": SPEC,
        "spec_stage": SPEC_STAGE,
    }
    defaults.update(kwargs)
    return SnowparkContainerJobOperator(**defaults)


def _context(task_store=None):
    ctx = {"ti": mock.MagicMock(stats_tags={})}
    if task_store is not None:
        ctx["task_state_store"] = task_store
    return ctx


class TestSnowparkContainerJobOperator:
    def test_invalid_spec_combinations_at_init(self):
        with pytest.raises(ValueError, match=r"Cannot specify both"):
            _make_operator(spec=SPEC, spec_stage=SPEC_STAGE, spec_text=SPEC_TEXT)

    @pytest.mark.parametrize(
        ("kwargs", "match"),
        (
            pytest.param(
                {"spec": None, "spec_stage": None, "spec_text": None},
                "Must provide either",
                id="no_spec_provided",
            ),
            pytest.param(
                {"spec": SPEC, "spec_stage": None, "spec_text": None},
                "Must provide either",
                id="spec_without_stage",
            ),
            pytest.param(
                {"spec": None, "spec_stage": SPEC_STAGE, "spec_text": None},
                "Must provide either",
                id="stage_without_spec",
            ),
        ),
    )
    def test_invalid_spec_combinations_at_execute(self, kwargs, match):
        op = _make_operator(**kwargs)
        with pytest.raises(ValueError, match=match):
            op.execute(context=None)

    @pytest.mark.parametrize(
        ("deferrable", "wait_for_completion", "warns"),
        (
            pytest.param(True, False, True, id="deferrable_no_wait"),
            pytest.param(True, True, False, id="deferrable_wait"),
            pytest.param(False, False, False, id="sync_no_wait"),
        ),
    )
    @mock.patch.object(SnowparkContainerJobOperator, "log")
    def test_warns_when_deferrable_without_wait_for_completion(
        self, mock_log, deferrable, wait_for_completion, warns
    ):
        _make_operator(deferrable=deferrable, wait_for_completion=wait_for_completion)
        assert mock_log.warning.called is warns

    def test_build_sql_with_spec_stage(self):
        op = _make_operator()
        sql = op._build_sql()
        assert sql == (
            f"EXECUTE JOB SERVICE IN COMPUTE POOL {COMPUTE_POOL}"
            " ASYNC = TRUE"
            f" FROM {SPEC_STAGE} SPEC = '{SPEC}'"
        )

    def test_build_sql_with_spec_text(self):
        op = _make_operator(spec=None, spec_stage=None, spec_text=SPEC_TEXT)
        sql = op._build_sql()
        assert sql == (
            f"EXECUTE JOB SERVICE IN COMPUTE POOL {COMPUTE_POOL}"
            " ASYNC = TRUE"
            f" FROM SPECIFICATION $${SPEC_TEXT}$$"
        )

    @pytest.mark.parametrize(
        ("kwargs", "expected"),
        (
            pytest.param({"name": "my_job"}, "NAME = my_job", id="name"),
            pytest.param({"replicas": 5}, "REPLICAS = 5", id="replicas"),
            pytest.param(
                {"query_warehouse": "COMPUTE_WH"}, "QUERY_WAREHOUSE = COMPUTE_WH", id="query_warehouse"
            ),
            pytest.param(
                {"external_access_integrations": ["test_eai"]},
                "EXTERNAL_ACCESS_INTEGRATIONS = (test_eai)",
                id="external_access_integrations_single",
            ),
            pytest.param(
                {"external_access_integrations": ["test_eai", "test_eai_2"]},
                "EXTERNAL_ACCESS_INTEGRATIONS = (test_eai, test_eai_2)",
                id="external_access_integrations_multiple",
            ),
        ),
    )
    def test_build_sql_optional_params(self, kwargs, expected):
        op = _make_operator(**kwargs)
        assert expected in op._build_sql()

    def test_external_access_integrations_in_template_fields(self):
        op = _make_operator(external_access_integrations=["test_eai"])
        assert "external_access_integrations" in op.template_fields
        assert hasattr(op, "external_access_integrations")

    def test_external_id_key(self):
        assert _make_operator().external_id_key == "snowpark_container_job_name"

    @mock.patch(MOCK_HOOK_PATH)
    def test_submit_job_parses_job_name(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.run.return_value = SUBMIT_RESPONSE
        op = _make_operator()
        result = op.submit_job(None)
        assert result == JOB_NAME

    @pytest.mark.parametrize(
        "status",
        ("FAILED", "CANCELLED", "INTERNAL_ERROR"),
    )
    @mock.patch.object(SnowparkContainerJobOperator, "_log_container_output")
    @mock.patch(MOCK_HOOK_PATH)
    def test_poll_raises_on_terminal_failure(self, mock_hook_cls, mock_log, status):
        mock_hook = mock_hook_cls.return_value
        mock_hook.run.return_value = {"status": status}
        op = _make_operator(poll_interval=0)
        with pytest.raises(RuntimeError, match="finished with status"):
            op.poll_until_complete(JOB_NAME, None)

    @mock.patch(MOCK_HOOK_PATH)
    def test_poll_raises_on_unexpected_status(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.run.return_value = {"status": "UNKNOWN"}
        op = _make_operator(poll_interval=0)
        with pytest.raises(RuntimeError, match="unexpected status"):
            op.poll_until_complete(JOB_NAME, None)

    @mock.patch.object(SnowparkContainerJobOperator, "_handle_final_status")
    @mock.patch("time.sleep")
    @mock.patch(MOCK_HOOK_PATH)
    def test_poll_waits_through_pending_then_done(self, mock_hook_cls, mock_sleep, mock_handle):
        mock_hook = mock_hook_cls.return_value
        mock_hook.run.side_effect = [
            {"status": "PENDING"},
            {"status": "RUNNING"},
            {"status": "DONE"},
        ]
        op = _make_operator(poll_interval=5)
        op.poll_until_complete(JOB_NAME, None)
        assert mock_sleep.call_count == 2
        mock_handle.assert_called_once_with(status="DONE")

    @pytest.mark.parametrize(
        ("drop_on_completion", "drops"),
        [(True, True), (False, False)],
    )
    @mock.patch("time.sleep")
    # 0 sets the deadline at 10, 5 admits one poll that observes RUNNING, 10 then trips the timeout.
    @mock.patch("time.monotonic", side_effect=itertools.count(0, 5))
    @mock.patch(MOCK_HOOK_PATH)
    @mock.patch.object(SnowparkContainerJobOperator, "_log_container_output")
    def test_poll_raises_and_logs_on_timeout(
        self, mock_log, mock_hook_cls, mock_monotonic, sleep_mock, drop_on_completion, drops
    ):
        mock_hook = mock_hook_cls.return_value
        mock_hook.run.return_value = {"status": "RUNNING"}
        op = _make_operator(poll_interval=5, timeout=10, drop_on_completion=drop_on_completion)
        op.job_name = JOB_NAME

        with pytest.raises(TimeoutError, match="did not reach a terminal status"):
            op.poll_until_complete(JOB_NAME, None)

        mock_log.assert_called_once_with("RUNNING")
        drop_call = mock.call(f"DROP SERVICE IF EXISTS {JOB_NAME}")
        if drops:
            assert drop_call in mock_hook.run.call_args_list
        else:
            assert drop_call not in mock_hook.run.call_args_list

    @mock.patch(MOCK_HOOK_PATH)
    def test_log_container_output_uses_info_on_done(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.run.return_value = ["container output here"]
        op = _make_operator()
        op.job_name = JOB_NAME
        with mock.patch.object(op.log, "info") as mock_info:
            op._log_container_output("DONE")
        mock_info.assert_called_once_with("Logs for instance_id %d:\n%s", 0, "container output here")

    @mock.patch(MOCK_HOOK_PATH)
    def test_log_container_output_uses_error_on_failure(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.run.return_value = ["container output here"]
        op = _make_operator()
        op.job_name = JOB_NAME
        with mock.patch.object(op.log, "error") as mock_error:
            op._log_container_output("FAILED")
        mock_error.assert_called_once_with("Logs for instance_id %d:\n%s", 0, "container output here")

    @mock.patch(MOCK_HOOK_PATH)
    def test_log_container_output_no_logs(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.run.return_value = [""]
        op = _make_operator()
        op.job_name = JOB_NAME
        with mock.patch.object(op.log, "info") as mock_info:
            op._log_container_output("DONE")
        mock_info.assert_called_once_with("No logs returned for instance_id %d", 0)

    @mock.patch(MOCK_HOOK_PATH)
    def test_log_container_output_multiple_replicas(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.run.return_value = ["logs"]
        op = _make_operator(replicas=3)
        op.job_name = JOB_NAME
        with mock.patch.object(op.log, "info") as mock_info:
            op._log_container_output("DONE")
        assert mock_info.call_count == 3

    @mock.patch(MOCK_HOOK_PATH)
    def test_log_container_output_swallows_fetch_error(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.run.side_effect = Exception("Unable to retrieve logs")
        op = _make_operator()
        op.job_name = JOB_NAME
        with mock.patch.object(op.log, "warning") as mock_warning:
            op._log_container_output("RUNNING")
        mock_warning.assert_called_once_with("Could not retrieve logs for instance_id %d: %s", 0, mock.ANY)

    @mock.patch(MOCK_HOOK_PATH)
    def test_on_kill_no_job_name(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        op = _make_operator()
        op.on_kill()
        mock_hook.run.assert_not_called()

    @mock.patch(MOCK_HOOK_PATH)
    def test_on_kill_drops_service(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        op = _make_operator()
        op.job_name = JOB_NAME
        op.on_kill()
        mock_hook.run.assert_called_once_with(f"DROP SERVICE IF EXISTS {JOB_NAME}")

    @mock.patch(MOCK_HOOK_PATH)
    def test_on_kill_logs_error_on_exception(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.run.side_effect = Exception("drop failed")
        op = _make_operator()
        op.job_name = JOB_NAME
        with mock.patch.object(op.log, "error") as mock_error:
            op.on_kill()
        mock_error.assert_called_once()

    @mock.patch(MOCK_HOOK_PATH)
    def test_submit_job_raises_on_malformed_response(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.run.return_value = ["unexpected response"]
        op = _make_operator()
        with pytest.raises(IndexError):
            op.submit_job(None)

    @mock.patch(MOCK_HOOK_PATH)
    def test_submit_job_raises_when_job_name_empty(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.run.return_value = ["Started Snowpark Container Services Job ''."]
        op = _make_operator()
        with pytest.raises(RuntimeError, match="Job name was not returned"):
            op.submit_job(None)

    @mock.patch(MOCK_HOOK_PATH)
    def test_execute_no_wait(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.run.return_value = SUBMIT_RESPONSE
        op = _make_operator(wait_for_completion=False)
        result = op.execute(context=None)
        assert result == JOB_NAME
        assert mock_hook.run.call_count == 1

    @mock.patch.object(SnowparkContainerJobOperator, "_log_container_output")
    @mock.patch.object(SnowparkContainerJobOperator, "poll_until_complete")
    @mock.patch.object(SnowparkContainerJobOperator, "submit_job", return_value=JOB_NAME)
    @mock.patch(MOCK_HOOK_PATH)
    def test_execute_wait_success(self, mock_hook_cls, mock_submit, mock_poll, mock_log):
        op = _make_operator(durable=False)
        result = op.execute(context=None)
        mock_submit.assert_called_once()
        mock_poll.assert_called_once()
        mock_log.assert_called_once_with("DONE")
        assert result == JOB_NAME

    @mock.patch.object(SnowparkContainerJobOperator, "_log_container_output")
    @mock.patch.object(SnowparkContainerJobOperator, "submit_job", return_value=JOB_NAME)
    @mock.patch(MOCK_HOOK_PATH)
    def test_execute_drops_service_on_completion(self, mock_hook_cls, mock_submit, mock_log):
        mock_hook = mock_hook_cls.return_value
        mock_hook.run.side_effect = [
            {"status": "DONE"},
            None,
        ]
        op = _make_operator(drop_on_completion=True, poll_interval=0, durable=False)
        op.execute(context=None)
        assert mock.call(f"DROP SERVICE IF EXISTS {JOB_NAME}") in mock_hook.run.call_args_list

    @mock.patch.object(SnowparkContainerJobOperator, "_log_container_output")
    @mock.patch.object(SnowparkContainerJobOperator, "submit_job", return_value=JOB_NAME)
    @mock.patch(MOCK_HOOK_PATH)
    def test_execute_skips_drop_when_disabled(self, mock_hook_cls, mock_submit, mock_log):
        mock_hook = mock_hook_cls.return_value
        mock_hook.run.side_effect = [
            {"status": "DONE"},
        ]
        op = _make_operator(drop_on_completion=False, poll_interval=0, durable=False)
        op.execute(context=None)
        drop_call = mock.call(f"DROP SERVICE IF EXISTS {JOB_NAME}")
        assert drop_call not in mock_hook.run.call_args_list

    @mock.patch.object(SnowparkContainerJobOperator, "submit_job", return_value=JOB_NAME)
    def test_execute_defers_when_deferrable(self, mock_submit):
        op = _make_operator(deferrable=True)
        with pytest.raises(TaskDeferred) as exc:
            op.execute(context=None)
        assert isinstance(exc.value.trigger, SnowparkContainerJobTrigger)
        assert exc.value.trigger.job_name == JOB_NAME
        assert exc.value.method_name == "execute_complete"

    @mock.patch.object(SnowparkContainerJobOperator, "submit_job", return_value=JOB_NAME)
    def test_execute_defer_without_execution_timeout(self, mock_submit):
        op = _make_operator(deferrable=True, timeout=100, poll_interval=10)
        with pytest.raises(TaskDeferred) as exc:
            op.execute(context=None)
        assert exc.value.trigger.execution_deadline is None
        assert exc.value.timeout == timedelta(seconds=100 + 10 + 60)

    @mock.patch.object(SnowparkContainerJobOperator, "submit_job", return_value=JOB_NAME)
    def test_execute_defer_uses_execution_timeout_for_deadline_and_buffer(self, mock_submit, time_machine):
        time_machine.move_to(1000, tick=False)
        context = {"ti": mock.Mock(start_date=datetime.fromtimestamp(1000, tz=timezone.utc))}
        op = _make_operator(
            deferrable=True,
            timeout=3600,
            poll_interval=10,
            execution_timeout=timedelta(seconds=120),
        )
        with pytest.raises(TaskDeferred) as exc:
            op.execute(context=context)
        assert exc.value.trigger.end_time == 1000 + 3600
        assert exc.value.trigger.execution_deadline == 1000 + 120
        assert exc.value.timeout == timedelta(seconds=120 + 10 + 60)

    @mock.patch(MOCK_HOOK_PATH)
    @mock.patch.object(SnowparkContainerJobOperator, "_log_container_output")
    def test_execute_complete_success_drops_and_returns(self, mock_log, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        op = _make_operator(drop_on_completion=True)
        result = op.execute_complete(context=None, event={"status": "DONE", "job_name": JOB_NAME})
        assert result == JOB_NAME
        mock_log.assert_called_once_with("DONE")
        mock_hook.run.assert_called_once_with(f"DROP SERVICE IF EXISTS {JOB_NAME}")

    @mock.patch(MOCK_HOOK_PATH)
    @mock.patch.object(SnowparkContainerJobOperator, "_log_container_output")
    def test_execute_complete_failure_raises_without_drop(self, mock_log, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        op = _make_operator()
        with pytest.raises(RuntimeError, match="FAILED"):
            op.execute_complete(context=None, event={"status": "FAILED", "job_name": JOB_NAME})
        mock_log.assert_called_once_with("FAILED")
        mock_hook.run.assert_not_called()

    @pytest.mark.parametrize(
        ("status", "exc", "drops", "logs"),
        [("timeout", TimeoutError, True, True), ("error", RuntimeError, False, False)],
    )
    @mock.patch(MOCK_HOOK_PATH)
    @mock.patch.object(SnowparkContainerJobOperator, "_log_container_output")
    def test_execute_complete_raises_and_drops_only_on_timeout(
        self, mock_log, mock_hook_cls, status, exc, drops, logs
    ):
        mock_hook = mock_hook_cls.return_value
        op = _make_operator()
        with pytest.raises(exc, match="boom"):
            op.execute_complete(
                context=None,
                event={"status": status, "job_name": JOB_NAME, "message": "boom"},
            )
        if drops:
            mock_hook.run.assert_called_once_with(f"DROP SERVICE IF EXISTS {JOB_NAME}")
        else:
            mock_hook.run.assert_not_called()
        if logs:
            mock_log.assert_called_once_with(status)
        else:
            mock_log.assert_not_called()

    @mock.patch(MOCK_HOOK_PATH)
    @mock.patch.object(SnowparkContainerJobOperator, "_log_container_output")
    def test_execute_complete_skips_drop_when_disabled(self, mock_log, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        op = _make_operator(drop_on_completion=False)
        op.execute_complete(context=None, event={"status": "DONE", "job_name": JOB_NAME})
        mock_hook.run.assert_not_called()

    @mock.patch(MOCK_HOOK_PATH)
    @mock.patch.object(SnowparkContainerJobOperator, "_log_container_output")
    def test_execute_complete_timeout_skips_drop_when_disabled(self, mock_log, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        op = _make_operator(drop_on_completion=False)
        with pytest.raises(TimeoutError, match="boom"):
            op.execute_complete(
                context=None,
                event={"status": "timeout", "job_name": JOB_NAME, "message": "boom"},
            )
        mock_log.assert_called_once_with("timeout")
        mock_hook.run.assert_not_called()


@pytest.mark.skipif(
    not AIRFLOW_V_3_3_PLUS, reason="task_state_store (durable execution) requires Airflow 3.3+"
)
class TestSnowparkContainerJobOperatorDurable:
    @mock.patch.object(SnowparkContainerJobOperator, "_handle_final_status")
    @mock.patch.object(SnowparkContainerJobOperator, "submit_job", return_value=JOB_NAME)
    @mock.patch(MOCK_HOOK_PATH)
    def test_job_name_persists_to_task_state_store_on_fresh_submit(
        self, mock_hook_cls, mock_submit_job, mock_handle
    ):
        mock_hook = mock_hook_cls.return_value
        mock_hook.run.return_value = {"status": "DONE"}

        op = _make_operator(poll_interval=0)
        task_store = mock.MagicMock(spec_set=["get", "set"])
        task_store.get.return_value = None

        op.execute(context=_context(task_store=task_store))

        task_store.get.assert_called_once_with(op.external_id_key)
        task_store.set.assert_called_once_with(op.external_id_key, JOB_NAME)
        mock_handle.assert_called_once_with(status="DONE")

    @mock.patch("time.sleep")
    @mock.patch.object(SnowparkContainerJobOperator, "_handle_final_status")
    @mock.patch.object(SnowparkContainerJobOperator, "submit_job")
    @mock.patch(MOCK_HOOK_PATH)
    def test_reconnects_to_running_job_without_resubmitting(
        self, mock_hook_cls, mock_submit_job, mock_handle, mock_sleep
    ):
        mock_hook = mock_hook_cls.return_value
        mock_hook.run.side_effect = [
            {"status": "RUNNING"},
            {"status": "DONE"},
        ]

        op = _make_operator(poll_interval=5)
        task_store = mock.MagicMock(spec_set=["get", "set"])
        task_store.get.return_value = JOB_NAME

        result = op.execute(context=_context(task_store=task_store))

        task_store.get.assert_called_once_with(op.external_id_key)
        task_store.set.assert_not_called()
        mock_submit_job.assert_not_called()
        mock_handle.assert_called_once_with(status="DONE")
        assert result == JOB_NAME

    @mock.patch.object(SnowparkContainerJobOperator, "_handle_final_status")
    @mock.patch.object(SnowparkContainerJobOperator, "poll_until_complete")
    @mock.patch.object(SnowparkContainerJobOperator, "submit_job")
    @mock.patch(MOCK_HOOK_PATH)
    def test_already_succeeded_completes_without_polling(
        self, mock_hook_cls, mock_submit_job, mock_poll_until_complete, mock_handle
    ):
        mock_hook = mock_hook_cls.return_value
        mock_hook.run.side_effect = [{"status": "DONE"}]

        op = _make_operator(poll_interval=5)
        task_store = mock.MagicMock(spec_set=["get", "set"])
        task_store.get.return_value = JOB_NAME

        op.execute(context=_context(task_store=task_store))

        task_store.get.assert_called_once_with(op.external_id_key)
        mock_submit_job.assert_not_called()
        mock_poll_until_complete.assert_not_called()
        mock_handle.assert_called_once_with(status="DONE")

    @mock.patch.object(SnowparkContainerJobOperator, "_handle_final_status")
    @mock.patch.object(SnowparkContainerJobOperator, "poll_until_complete")
    @mock.patch.object(SnowparkContainerJobOperator, "get_job_status")
    @mock.patch.object(SnowparkContainerJobOperator, "submit_job")
    @mock.patch(MOCK_HOOK_PATH)
    def test_resubmits_when_stored_job_in_terminal_error(
        self, mock_hook_cls, mock_submit_job, mock_get_job_status, mock_poll_until_complete, mock_handle
    ):
        mock_get_job_status.return_value = "FAILED"
        mock_submit_job.return_value = f"{JOB_NAME}_2"

        op = _make_operator(poll_interval=0)
        task_store = mock.MagicMock(spec_set=["get", "set"])
        task_store.get.return_value = JOB_NAME

        op.execute(context=_context(task_store=task_store))

        task_store.get.assert_called_once_with(op.external_id_key)
        mock_submit_job.assert_called_once()
        task_store.set.assert_called_once_with(op.external_id_key, f"{JOB_NAME}_2")

    @mock.patch.object(SnowparkContainerJobOperator, "_handle_final_status")
    @mock.patch.object(SnowparkContainerJobOperator, "poll_until_complete")
    @mock.patch.object(SnowparkContainerJobOperator, "_describe_status")
    @mock.patch.object(SnowparkContainerJobOperator, "submit_job")
    @mock.patch(MOCK_HOOK_PATH)
    def test_resubmits_when_stored_job_not_exist(
        self, mock_hook_cls, mock_submit_job, mock_describe_status, mock_poll_until_complete, mock_handle
    ):
        mock_describe_status.side_effect = ProgrammingError(
            msg="test job does not exist", errno=OBJECT_NOT_EXIST_ERROR_CODE
        )
        mock_submit_job.return_value = f"{JOB_NAME}_2"

        op = _make_operator(poll_interval=0)
        task_store = mock.MagicMock(spec_set=["get", "set"])
        task_store.get.return_value = JOB_NAME

        op.execute(context=_context(task_store=task_store))

        task_store.get.assert_called_once_with(op.external_id_key)
        mock_submit_job.assert_called_once()
        task_store.set.assert_called_once_with(op.external_id_key, f"{JOB_NAME}_2")

    @mock.patch.object(SnowparkContainerJobOperator, "_handle_final_status")
    @mock.patch.object(SnowparkContainerJobOperator, "submit_job", return_value=JOB_NAME)
    @mock.patch(MOCK_HOOK_PATH)
    def test_durable_false_never_touches_task_state_store(self, mock_hook_cls, mock_submit_job, mock_handle):
        mock_hook = mock_hook_cls.return_value
        mock_hook.run.return_value = {"status": "DONE"}

        task_store = mock.MagicMock(spec_set=["get", "set"])
        op = _make_operator(durable=False)

        op.execute(context=_context(task_store=task_store))

        task_store.get.assert_not_called()
        task_store.set.assert_not_called()

    @mock.patch(MOCK_HOOK_PATH)
    def test_deferrable_does_not_persist_to_task_state_store(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.run.return_value = SUBMIT_RESPONSE

        op = _make_operator(deferrable=True)
        task_store = mock.MagicMock(spec_set=["get", "set"])

        with pytest.raises(TaskDeferred):
            op.execute(context=_context(task_store=task_store))

        task_store.get.assert_not_called()
        task_store.set.assert_not_called()

    @mock.patch(MOCK_HOOK_PATH)
    def test_no_wait_does_not_persist_to_task_state_store(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.run.return_value = SUBMIT_RESPONSE

        op = _make_operator(wait_for_completion=False)
        task_store = mock.MagicMock(spec_set=["get", "set"])

        result = op.execute(context=_context(task_store=task_store))

        assert result == JOB_NAME
        task_store.get.assert_not_called()
        task_store.set.assert_not_called()

    @mock.patch.object(SnowparkContainerJobOperator, "_describe_status")
    def test_get_job_status_returns_status(self, mock_describe_status):
        mock_describe_status.return_value = "DONE"
        op = _make_operator()
        status = op.get_job_status(context=None, external_id="test job")
        assert status == "DONE"

    @mock.patch.object(SnowparkContainerJobOperator, "_describe_status")
    def test_get_job_status_returns_not_found_when_service_missing(self, mock_describe_status):
        mock_describe_status.side_effect = ProgrammingError(
            msg="test job does not exist", errno=OBJECT_NOT_EXIST_ERROR_CODE
        )
        op = _make_operator()
        status = op.get_job_status(context=None, external_id="test job")
        assert status == NOT_FOUND_STATUS

    @mock.patch.object(SnowparkContainerJobOperator, "_describe_status")
    def test_get_job_status_reraises_other_programming_errors(self, mock_describe_status):
        mock_describe_status.side_effect = ProgrammingError(msg="syntax error", errno=1003)
        op = _make_operator()
        with pytest.raises(ProgrammingError, match="syntax error"):
            op.get_job_status(context=None, external_id="test job")

    def test_is_job_active_and_is_job_succeeded(self):
        op = _make_operator()
        assert op.is_job_active("RUNNING") is True
        assert op.is_job_active("DONE") is False

        assert op.is_job_succeeded("DONE") is True
        assert op.is_job_succeeded("FAILED") is False

    def test_default_args_durable_reaches_operator(self):
        op = _make_operator(default_args={"durable": False})
        assert op.durable is False

    def test_durable_false_direct_kwarg_reaches_operator(self):
        op = _make_operator(durable=False)
        assert op.durable is False


class TestWarnAndDisableDurableAirflowPre3_3:
    def test_no_warning_when_unset(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = _warn_and_disable_durable_pre_3_3(_DURABLE_UNSET)
        assert result is False
        assert caught == []

    @pytest.mark.parametrize("value", [True, False])
    def test_warns_and_disables_when_explicitly_set(self, value):
        with pytest.warns(UserWarning, match="durable.*no effect"):
            result = _warn_and_disable_durable_pre_3_3(value)
        assert result is False
