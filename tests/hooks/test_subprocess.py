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

from pathlib import Path
from subprocess import PIPE, STDOUT
from tempfile import TemporaryDirectory
from unittest import mock
from unittest.mock import MagicMock

import pytest

from airflow.hooks.subprocess import SubprocessHook

OS_ENV_KEY = 'SUBPROCESS_ENV_TEST'
OS_ENV_VAL = 'this-is-from-os-environ'


class TestSubprocessHook:
    @pytest.mark.parametrize(
        "env,expected",
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
            return '\n'.join(f"echo {k}=${k}>> {filename}" for k in [*keys, OS_ENV_KEY])

        with TemporaryDirectory() as tmp_dir, mock.patch.dict('os.environ', {OS_ENV_KEY: OS_ENV_VAL}):
            tmp_file = Path(tmp_dir, 'test.txt')
            command = build_cmd(env and env.keys() or [], tmp_file.as_posix())
            hook.run_command(command=['bash', '-c', command], env=env)
            actual = dict([x.split('=') for x in tmp_file.read_text().splitlines()])
            assert actual == expected

    @pytest.mark.parametrize(
        "val,expected",
        [
            ('test-val', 'test-val'),
            ('test-val\ntest-val\n', ''),
            ('test-val\ntest-val', 'test-val'),
            ('', ''),
        ],
    )
    def test_return_value(self, val, expected):
        hook = SubprocessHook()
        result = hook.run_command(command=['bash', '-c', f'echo "{val}"'])
        assert result.output == expected

    @mock.patch.dict('os.environ', clear=True)
    @mock.patch(
        "airflow.hooks.subprocess.TemporaryDirectory",
        return_value=MagicMock(__enter__=MagicMock(return_value='/tmp/airflowtmpcatcat')),
    )
    @mock.patch(
        "airflow.hooks.subprocess.Popen",
        return_value=MagicMock(stdout=MagicMock(readline=MagicMock(side_effect=StopIteration), returncode=0)),
    )
    def test_should_exec_subprocess(self, mock_popen, mock_temporary_directory):
        hook = SubprocessHook()
        hook.run_command(command=['bash', '-c', 'echo "stdout"'])

        mock_popen.assert_called_once_with(
            ['bash', '-c', 'echo "stdout"'],
            cwd='/tmp/airflowtmpcatcat',
            env={},
            preexec_fn=mock.ANY,
            stderr=STDOUT,
            stdout=PIPE,
        )

    def test_task_decode(self):
        hook = SubprocessHook()
        command = ['bash', '-c', 'printf "This will cause a coding error \\xb1\\xa6\\x01\n"']
        result = hook.run_command(command=command)
        assert result.exit_code == 0
