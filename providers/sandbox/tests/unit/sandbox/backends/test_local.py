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
"""LocalProvider exercises the full provider contract with no SaaS creds."""

from __future__ import annotations

import time

from airflow.providers.sandbox.backends.base import SandboxSpec, SandboxState
from airflow.providers.sandbox.backends.local import LocalProvider


def _run_to_terminal(provider, handle, exec_ref, timeout=30):
    deadline = time.monotonic() + timeout
    res = provider.poll_status(handle, exec_ref)
    while res.state in (SandboxState.PENDING, SandboxState.RUNNING, SandboxState.UNKNOWN):
        assert time.monotonic() < deadline, "timed out"
        time.sleep(0.05)
        res = provider.poll_status(handle, exec_ref)
    return res


def test_successful_command_lifecycle():
    provider = LocalProvider()
    provider.authenticate()
    handle = provider.create_sandbox(SandboxSpec(name="t-ok", env={"X": "hi"}))
    exec_ref = provider.run(handle, ["sh", "-c", 'echo "$X"; exit 0'], env={"X": "hi"})
    res = _run_to_terminal(provider, handle, exec_ref)
    assert res.state is SandboxState.SUCCEEDED
    assert res.exit_code == 0
    _, logs = provider.fetch_logs(handle, exec_ref)
    assert any("hi" in line for line in logs)
    provider.destroy(handle)
    assert provider.poll_status(handle, exec_ref).state is SandboxState.GONE


def test_failing_command_maps_to_failed():
    provider = LocalProvider()
    provider.authenticate()
    handle = provider.create_sandbox(SandboxSpec(name="t-fail"))
    exec_ref = provider.run(handle, ["sh", "-c", "exit 7"], env={})
    res = _run_to_terminal(provider, handle, exec_ref)
    assert res.state is SandboxState.FAILED
    assert res.exit_code == 7
    provider.destroy(handle)


def test_uses_spec_name_as_handle():
    provider = LocalProvider()
    handle = provider.create_sandbox(SandboxSpec(name="deterministic-name"))
    assert handle == "deterministic-name"
    provider.destroy(handle)
