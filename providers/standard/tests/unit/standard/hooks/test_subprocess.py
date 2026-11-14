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

import signal
from io import BufferedReader
from pathlib import Path
from subprocess import PIPE, STDOUT, Popen
from tempfile import TemporaryDirectory
from unittest import mock
from unittest.mock import MagicMock

import pytest

from airflow.providers.standard.hooks.subprocess import SubprocessHook

OS_ENV_KEY = "SUBPROCESS_ENV_TEST"
OS_ENV_VAL = "this-is-from-os-environ"


class _TaskTimeout(BaseException):
    """Stands in for ``AirflowTaskTimeout``, which is a ``BaseException`` so task code cannot catch it."""


class TestSubprocessHook:
    @pytest.mark.parametrize(
        ("env", "expected"),
        [
            ({"ABC": "123", "AAA": "456"}, {"ABC": "123", "AAA": "456", OS_ENV_KEY: ""}),
            ({}, {OS_ENV_KEY: ""}),
            (None, {OS_ENV_KEY: OS_ENV_VAL}),
        ],
        ids=["with env", "empty env", "no env"],
    )
    def test_env(self, env, expected):
        """
        Test that env variables are exported correctly to the command environment.
        When ``env`` is ``None``, ``os.environ`` should be passed to ``Popen``.
        Otherwise, the variables in ``env`` should be available, and ``os.environ`` should not.
        """
        hook = SubprocessHook()

        def build_cmd(keys, filename):
            """
            Produce bash command to echo env vars into filename.
            Will always echo the special test var named ``OS_ENV_KEY`` into the file to test whether
            ``os.environ`` is passed or not.
            """
            return "\n".join(f"echo {k}=${k}>> {filename}" for k in [*keys, OS_ENV_KEY])

        with TemporaryDirectory() as tmp_dir, mock.patch.dict("os.environ", {OS_ENV_KEY: OS_ENV_VAL}):
            tmp_file = Path(tmp_dir, "test.txt")
            command = build_cmd(env and env.keys() or [], tmp_file.as_posix())
            hook.run_command(command=["bash", "-c", command], env=env)
            actual = dict([x.split("=") for x in tmp_file.read_text().splitlines()])
            assert actual == expected

    @pytest.mark.parametrize(
        ("val", "expected"),
        [
            ("test-val", "test-val"),
            ("test-val\ntest-val\n", ""),
            ("test-val\ntest-val", "test-val"),
            ("", ""),
        ],
    )
    def test_return_value(self, val, expected):
        hook = SubprocessHook()
        result = hook.run_command(command=["bash", "-c", f'echo "{val}"'])
        assert result.output == expected

    @mock.patch.dict("os.environ", clear=True)
    @mock.patch(
        "airflow.providers.standard.hooks.subprocess.TemporaryDirectory",
        return_value=MagicMock(__enter__=MagicMock(return_value="/tmp/airflowtmpcatcat")),
    )
    @mock.patch(
        "airflow.providers.standard.hooks.subprocess.Popen",
        return_value=MagicMock(stdout=MagicMock(readline=MagicMock(side_effect=StopIteration), returncode=0)),
    )
    def test_should_exec_subprocess(self, mock_popen, mock_temporary_directory):
        hook = SubprocessHook()
        hook.run_command(command=["bash", "-c", 'echo "stdout"'])

        mock_temporary_directory.assert_called_once()
        mock_popen.assert_called_once_with(
            ["bash", "-c", 'echo "stdout"'],
            cwd="/tmp/airflowtmpcatcat",
            env={},
            restore_signals=True,
            start_new_session=True,
            stderr=STDOUT,
            stdout=PIPE,
        )

    def test_run_command_closes_stdout_pipe(self):
        hook = SubprocessHook()
        hook.run_command(command=["bash", "-c", 'echo "stdout"'])
        assert hook.sub_process.stdout.closed

    @mock.patch(
        "airflow.providers.standard.hooks.subprocess.Popen",
        return_value=MagicMock(
            spec=Popen,
            stdout=MagicMock(spec=BufferedReader, readline=MagicMock(side_effect=OSError("pipe broken"))),
        ),
    )
    def test_run_command_closes_stdout_pipe_on_read_error(self, mock_popen):
        hook = SubprocessHook()
        with pytest.raises(OSError, match="pipe broken"):
            hook.run_command(command=["bash", "-c", 'echo "stdout"'])
        mock_popen.return_value.stdout.close.assert_called_once()
        mock_popen.return_value.wait.assert_not_called()

    def test_run_command_keeps_stdout_open_for_sigterm_cleanup(self, tmp_path):
        """
        On an ``execution_timeout`` the timeout unwinds out of ``run_command`` and ``on_kill()``
        sends SIGTERM only afterwards, so stdout has to still be open by then: the child is started
        with ``restore_signals=True``, so a TERM handler writing to a closed pipe is killed by
        SIGPIPE instead of finishing its cleanup.
        """
        marker = tmp_path / "cleanup-ran"
        hook = SubprocessHook()

        def raise_task_timeout(signum, frame):
            raise _TaskTimeout()

        previous_handler = signal.signal(signal.SIGALRM, raise_task_timeout)
        try:
            signal.setitimer(signal.ITIMER_REAL, 0.5)
            with pytest.raises(_TaskTimeout):
                hook.run_command(
                    command=[
                        "bash",
                        "-c",
                        f"trap 'echo cleaning up; touch {marker}; exit 1' TERM; "
                        "echo started; while true; do sleep 0.1; done",
                    ]
                )
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, previous_handler)

        hook.send_sigterm()
        return_code = hook.sub_process.wait(timeout=10)
        # run_command deliberately leaves this path's pipe open, so the test owns closing it.
        hook.sub_process.stdout.close()

        assert marker.exists()
        assert return_code == 1

    def test_task_decode(self):
        hook = SubprocessHook()
        command = ["bash", "-c", 'printf "This will cause a coding error \\xb1\\xa6\\x01\n"']
        result = hook.run_command(command=command)
        assert result.exit_code == 0

    @mock.patch("os.getpgid", return_value=123)
    @mock.patch("os.killpg")
    def test_send_sigterm(self, mock_killpg, mock_getpgid):
        hook = SubprocessHook()
        hook.sub_process = MagicMock()
        hook.send_sigterm()
        mock_getpgid.assert_called_once()
        mock_killpg.assert_called_with(123, signal.SIGTERM)
