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

from unittest import mock

import pytest

from airflow.providers.snowflake.operators.snowpark_containers import SnowparkContainerJobOperator

TASK_ID = "test_spcs_job"
COMPUTE_POOL = "test_pool"
CONTAINER_NAME = "main"
SPEC = "spec.yaml"
SPEC_STAGE = "@test_stage"
SPEC_TEXT = "spec:\n  containers:\n  - name: main\n    image: /db/schema/repo/img:latest"
JOB_NAME = "TEST_JOB"
SNOWFLAKE_CONN_ID = "snowflake_default"
MOCK_HOOK_PATH = "airflow.providers.snowflake.operators.snowpark_containers.SnowflakeHook"


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


class TestSnowparkContainerJobOperator:
    @pytest.mark.parametrize(
        ("kwargs", "match"),
        (
            pytest.param(
                {"spec": None, "spec_stage": None, "spec_text": None},
                "Must provide either",
                id="no_spec_provided",
            ),
            pytest.param(
                {"spec": SPEC, "spec_stage": SPEC_STAGE, "spec_text": SPEC_TEXT},
                "Cannot specify both",
                id="both_spec_and_spec_text",
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
    def test_invalid_spec_combinations(self, kwargs, match):
        with pytest.raises(ValueError, match=match):
            _make_operator(**kwargs)

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
        ),
    )
    def test_build_sql_optional_params(self, kwargs, expected):
        op = _make_operator(**kwargs)
        assert expected in op._build_sql()

    @mock.patch(MOCK_HOOK_PATH)
    def test_submit_job_parses_job_name(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.run.return_value = ["Started Snowpark Container Services Job 'TEST_JOB'."]
        op = _make_operator()
        result = op._submit_job()
        assert result == JOB_NAME

    @pytest.mark.parametrize(
        "status",
        ("DONE", "FAILED", "CANCELLED", "INTERNAL_ERROR"),
    )
    @mock.patch(MOCK_HOOK_PATH)
    def test_poll_returns_terminal_status(self, mock_hook_cls, status):
        mock_hook = mock_hook_cls.return_value
        mock_hook.run.return_value = {"status": status}
        op = _make_operator(poll_interval=0)
        op.job_name = JOB_NAME
        assert op._poll_for_status() == status

    @mock.patch(MOCK_HOOK_PATH)
    def test_poll_raises_on_unexpected_status(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.run.return_value = {"status": "UNKNOWN"}
        op = _make_operator(poll_interval=0)
        op.job_name = JOB_NAME
        with pytest.raises(RuntimeError, match="unexpected status"):
            op._poll_for_status()

    @mock.patch("time.sleep")
    @mock.patch(MOCK_HOOK_PATH)
    def test_poll_waits_through_pending_then_done(self, mock_hook_cls, mock_sleep):
        mock_hook = mock_hook_cls.return_value
        mock_hook.run.side_effect = [
            {"status": "PENDING"},
            {"status": "RUNNING"},
            {"status": "DONE"},
        ]
        op = _make_operator(poll_interval=5)
        op.job_name = JOB_NAME
        assert op._poll_for_status() == "DONE"
        assert mock_sleep.call_count == 2

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
    def test_log_container_output_no_logs_skips(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.run.return_value = [""]
        op = _make_operator()
        op.job_name = JOB_NAME
        with mock.patch.object(op.log, "info") as mock_info, mock.patch.object(op.log, "error") as mock_error:
            op._log_container_output("DONE")
        mock_info.assert_not_called()
        mock_error.assert_not_called()

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
            op._submit_job()

    @mock.patch.object(SnowparkContainerJobOperator, "_submit_job", return_value=None)
    def test_execute_raises_when_job_name_not_returned(self, mock_submit):
        op = _make_operator()
        with pytest.raises(RuntimeError, match="Job name was not returned"):
            op.execute(context=None)

    @mock.patch(MOCK_HOOK_PATH)
    def test_execute_no_wait(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.run.return_value = ["Started Snowpark Container Services Job 'TEST_JOB'."]
        op = _make_operator(wait_for_completion=False)
        result = op.execute(context=None)
        assert result == JOB_NAME
        assert mock_hook.run.call_count == 1

    @mock.patch(MOCK_HOOK_PATH)
    @mock.patch.object(SnowparkContainerJobOperator, "_log_container_output")
    @mock.patch.object(SnowparkContainerJobOperator, "_poll_for_status", return_value="DONE")
    @mock.patch.object(SnowparkContainerJobOperator, "_submit_job", return_value=JOB_NAME)
    def test_execute_wait_success(self, mock_submit, mock_poll, mock_log, mock_hook_cls):
        op = _make_operator()
        result = op.execute(context=None)
        mock_submit.assert_called_once()
        mock_poll.assert_called_once()
        mock_log.assert_called_once_with("DONE")
        assert result == JOB_NAME

    @mock.patch(MOCK_HOOK_PATH)
    @mock.patch.object(SnowparkContainerJobOperator, "_log_container_output")
    @mock.patch.object(SnowparkContainerJobOperator, "_poll_for_status", return_value="FAILED")
    @mock.patch.object(SnowparkContainerJobOperator, "_submit_job", return_value=JOB_NAME)
    def test_execute_wait_failure_raises(self, mock_submit, mock_poll, mock_log, mock_hook_cls):
        op = _make_operator()
        with pytest.raises(RuntimeError, match="FAILED"):
            op.execute(context=None)
        mock_log.assert_called_once_with("FAILED")

    @mock.patch.object(SnowparkContainerJobOperator, "_log_container_output")
    @mock.patch.object(SnowparkContainerJobOperator, "_poll_for_status", return_value="DONE")
    @mock.patch.object(SnowparkContainerJobOperator, "_submit_job", return_value=JOB_NAME)
    @mock.patch(MOCK_HOOK_PATH)
    def test_execute_drops_service_on_completion(self, mock_hook_cls, mock_submit, mock_poll, mock_log):
        mock_hook = mock_hook_cls.return_value
        op = _make_operator(drop_on_completion=True)
        op.execute(context=None)
        mock_hook.run.assert_called_once_with(f"DROP SERVICE IF EXISTS {JOB_NAME}")

    @mock.patch.object(SnowparkContainerJobOperator, "_log_container_output")
    @mock.patch.object(SnowparkContainerJobOperator, "_poll_for_status", return_value="DONE")
    @mock.patch.object(SnowparkContainerJobOperator, "_submit_job", return_value=JOB_NAME)
    @mock.patch(MOCK_HOOK_PATH)
    def test_execute_skips_drop_when_disabled(self, mock_hook_cls, mock_submit, mock_poll, mock_log):
        mock_hook = mock_hook_cls.return_value
        op = _make_operator(drop_on_completion=False)
        op.execute(context=None)
        mock_hook.run.assert_not_called()
