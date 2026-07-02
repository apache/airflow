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
"""SandboxOperator runs a command in a sandbox and maps exit code to task state."""

from __future__ import annotations

import pytest

pytest.importorskip("airflow.sdk")

from airflow.exceptions import AirflowException
from airflow.providers.sandbox.operators.sandbox import SandboxOperator


def test_returns_stdout_on_success():
    op = SandboxOperator(
        task_id="ok",
        provider="local",
        command='echo "INJECTED=${SECRET:+yes}"; echo hello',
        env={"SECRET": "x"},
        poll_interval=1,
    )
    out = op.execute({})
    assert "hello" in out
    assert "INJECTED=yes" in out


def test_raises_on_nonzero_exit():
    op = SandboxOperator(task_id="bad", provider="local", command="exit 3", poll_interval=1)
    with pytest.raises(AirflowException):
        op.execute({})


def test_argv_command_form():
    op = SandboxOperator(
        task_id="argv", provider="local", command=["sh", "-c", "echo argv-form"], poll_interval=1
    )
    assert "argv-form" in op.execute({})
