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
"""IsloProvider wiring test using a fake client (no SDK/creds required).

Verifies the provider calls the islo ``sandboxes`` resource with the right
shape and maps exec results onto SandboxState correctly.
"""

from __future__ import annotations

from types import SimpleNamespace

from airflow.providers.sandbox.backends.base import SandboxSpec, SandboxState
from airflow.providers.sandbox.backends.islo import IsloProvider


class _FakeSandboxes:
    def __init__(self):
        self.created = None
        self.exec_calls = []
        self.deleted = []
        self._result = SimpleNamespace(exec_id="e1", exit_code=None, stdout="", stderr="")

    def create_sandbox(self, **kwargs):
        self.created = kwargs
        return SimpleNamespace(name=kwargs.get("name") or "sbx", id="id-1")

    def exec_in_sandbox(self, name, *, command, env=None, timeout_secs=None, workdir=None):
        self.exec_calls.append((name, command, env, timeout_secs, workdir))
        return SimpleNamespace(exec_id="e1", sandbox_id="id-1", status="running")

    def get_exec_result(self, name, exec_id):
        return self._result

    def delete_sandbox(self, name):
        self.deleted.append(name)


class _FakeClient:
    def __init__(self):
        self.sandboxes = _FakeSandboxes()


def _provider():
    p = IsloProvider()
    fake = _FakeClient()
    p._client_factory = lambda: fake  # type: ignore[attr-defined]
    p.authenticate()
    return p, fake


def test_create_passes_named_spec():
    p, fake = _provider()
    handle = p.create_sandbox(SandboxSpec(name="job-1", image="python:3.12", cpu=2, memory_mb=2048))
    assert handle == "job-1"
    assert fake.sandboxes.created["name"] == "job-1"
    assert fake.sandboxes.created["image"] == "python:3.12"
    assert fake.sandboxes.created["vcpus"] == 2


def test_run_and_poll_state_mapping():
    p, fake = _provider()
    ref = p.run("job-1", ["echo", "hi"], env={"K": "v"}, timeout=30)
    assert ref == "e1"
    # running while exit_code is None
    assert p.poll_status("job-1", ref).state is SandboxState.RUNNING
    fake.sandboxes._result.exit_code = 0
    assert p.poll_status("job-1", ref).state is SandboxState.SUCCEEDED
    fake.sandboxes._result.exit_code = 2
    res = p.poll_status("job-1", ref)
    assert res.state is SandboxState.FAILED and res.exit_code == 2


def test_destroy_calls_delete():
    p, fake = _provider()
    p.destroy("job-1")
    assert fake.sandboxes.deleted == ["job-1"]


def test_capabilities_reflect_named_pausable_no_upload():
    assert IsloProvider.capabilities.supports_reattach is True
    assert IsloProvider.capabilities.supports_pause_resume is True
    assert IsloProvider.capabilities.supports_file_upload is False
