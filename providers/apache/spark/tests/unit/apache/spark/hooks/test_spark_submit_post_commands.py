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

import shlex
import subprocess
from unittest.mock import MagicMock, call, patch

import pytest

from airflow.providers.apache.spark.hooks.spark_submit import SparkSubmitHook


def _make_hook(post_submit_commands=None):
    mock_conn = MagicMock()
    mock_conn.host = "local"
    mock_conn.port = None
    mock_conn.extra = "{}"
    mock_conn.conn_type = "spark"
    # extra_dejson.get("spark-binary", ...) must return "spark-submit" to pass validation
    mock_conn.extra_dejson = {"spark-binary": "spark-submit"}
    with patch(
        "airflow.providers.apache.spark.hooks.spark_submit.SparkSubmitHook.get_connection",
        return_value=mock_conn,
    ):
        return SparkSubmitHook(conn_id="spark_default", post_submit_commands=post_submit_commands)


class TestNoPostSubmitCommands:
    def test_default_is_empty_list(self):
        hook = _make_hook()
        assert hook._post_submit_commands == []

    @patch("airflow.providers.apache.spark.hooks.spark_submit.subprocess.run")
    def test_no_subprocess_call_when_empty(self, mock_run):
        hook = _make_hook(post_submit_commands=[])
        hook._run_post_submit_commands()
        mock_run.assert_not_called()


class TestSingleCommand:
    def test_command_stored(self):
        hook = _make_hook(post_submit_commands=["echo hello"])
        assert hook._post_submit_commands == ["echo hello"]

    @patch("airflow.providers.apache.spark.hooks.spark_submit.subprocess.run")
    def test_command_is_split_and_executed(self, mock_run):
        mock_result = MagicMock(spec=subprocess.CompletedProcess)
        mock_result.returncode = 0
        mock_result.stdout = ""
        mock_run.return_value = mock_result

        hook = _make_hook(post_submit_commands=["echo hello"])
        hook._run_post_submit_commands()

        mock_run.assert_called_once_with(
            ["echo", "hello"],
            shell=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
            timeout=30,
        )

    @patch("airflow.providers.apache.spark.hooks.spark_submit.subprocess.run")
    def test_istio_quit_command(self, mock_run):
        mock_result = MagicMock(spec=subprocess.CompletedProcess)
        mock_result.returncode = 0
        mock_result.stdout = ""
        mock_run.return_value = mock_result

        cmd = "curl -X POST localhost:15020/quitquitquit"
        hook = _make_hook(post_submit_commands=[cmd])
        hook._run_post_submit_commands()

        mock_run.assert_called_once_with(
            shlex.split(cmd),
            shell=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
            timeout=30,
        )


class TestMultipleCommands:
    @patch("airflow.providers.apache.spark.hooks.spark_submit.subprocess.run")
    def test_multiple_commands_executed_in_order(self, mock_run):
        mock_result = MagicMock(spec=subprocess.CompletedProcess)
        mock_result.returncode = 0
        mock_result.stdout = ""
        mock_run.return_value = mock_result

        cmds = ["echo one", "echo two", "echo three"]
        hook = _make_hook(post_submit_commands=cmds)
        hook._run_post_submit_commands()

        assert mock_run.call_count == 3
        mock_run.assert_has_calls(
            [
                call(
                    ["echo", "one"],
                    shell=False,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    check=False,
                    timeout=30,
                ),
                call(
                    ["echo", "two"],
                    shell=False,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    check=False,
                    timeout=30,
                ),
                call(
                    ["echo", "three"],
                    shell=False,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    check=False,
                    timeout=30,
                ),
            ]
        )


class TestFailureResilience:
    @patch("airflow.providers.apache.spark.hooks.spark_submit.subprocess.run")
    def test_nonzero_exit_does_not_raise(self, mock_run):
        mock_result = MagicMock(spec=subprocess.CompletedProcess)
        mock_result.returncode = 1
        mock_result.stdout = "error output"
        mock_run.return_value = mock_result

        hook = _make_hook(post_submit_commands=["false"])
        hook._run_post_submit_commands()  # must not raise

    @patch("airflow.providers.apache.spark.hooks.spark_submit.subprocess.run")
    def test_timeout_does_not_raise(self, mock_run):
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="sleep 100", timeout=30)
        hook = _make_hook(post_submit_commands=["sleep 100"])
        hook._run_post_submit_commands()  # must not raise

    @patch("airflow.providers.apache.spark.hooks.spark_submit.subprocess.run")
    def test_oserror_does_not_raise(self, mock_run):
        mock_run.side_effect = OSError("No such file")
        hook = _make_hook(post_submit_commands=["nonexistent_cmd"])
        hook._run_post_submit_commands()  # must not raise

    @patch("airflow.providers.apache.spark.hooks.spark_submit.subprocess.run")
    def test_remaining_commands_run_after_failure(self, mock_run):
        mock_result_ok = MagicMock(spec=subprocess.CompletedProcess)
        mock_result_ok.returncode = 0
        mock_result_ok.stdout = ""
        mock_run.side_effect = [subprocess.TimeoutExpired("cmd1", 30), mock_result_ok]

        hook = _make_hook(post_submit_commands=["cmd1", "cmd2"])
        hook._run_post_submit_commands()
        assert mock_run.call_count == 2


class TestSubmitIntegration:
    @patch.object(SparkSubmitHook, "_run_post_submit_commands")
    @patch.object(SparkSubmitHook, "_start_driver_status_tracking")
    @patch.object(SparkSubmitHook, "_build_spark_submit_command", return_value=["spark-submit", "app.py"])
    @patch("airflow.providers.apache.spark.hooks.spark_submit.subprocess.Popen")
    def test_post_commands_called_after_submit(self, mock_popen, _build, _track, mock_post):
        proc = MagicMock()
        proc.stdout = iter([])
        proc.wait.return_value = 0
        mock_popen.return_value = proc

        hook = _make_hook(post_submit_commands=["echo done"])
        hook.submit("app.py")
        mock_post.assert_called_once()

    @patch.object(SparkSubmitHook, "_run_post_submit_commands")
    @patch.object(SparkSubmitHook, "_build_spark_submit_command", return_value=["spark-submit", "app.py"])
    @patch("airflow.providers.apache.spark.hooks.spark_submit.subprocess.Popen")
    def test_post_commands_called_even_when_submit_fails(self, mock_popen, _build, mock_post):
        """Post-submit commands always run via try/finally, even when submit raises."""
        proc = MagicMock()
        proc.stdout = iter([])
        proc.wait.return_value = 1  # non-zero = failure
        mock_popen.return_value = proc

        hook = _make_hook(post_submit_commands=["echo cleanup"])
        with pytest.raises(Exception, match="Cannot execute"):
            hook.submit("app.py")
        mock_post.assert_called_once()


class TestOnKillIntegration:
    @patch.object(SparkSubmitHook, "_run_post_submit_commands")
    def test_post_commands_called_on_kill(self, mock_post):
        hook = _make_hook(post_submit_commands=["echo cleanup"])
        hook.on_kill()
        mock_post.assert_called_once()


class TestBackwardCompatibility:
    def test_hook_works_without_parameter(self):
        hook = _make_hook()
        assert hook._post_submit_commands == []

    def test_none_becomes_empty_list(self):
        hook = _make_hook(post_submit_commands=None)
        assert hook._post_submit_commands == []
