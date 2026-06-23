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
"""System tests for the Sandbox provider.

The ``local`` backend runs as a real system test with no credentials. Each SaaS
backend runs only when its API key is present in the environment, so this doubles
as the continuous live-validation plan required by ACCEPTING_PROVIDERS.rst:

    DAYTONA_API_KEY=... pytest tests/system -k daytona
    E2B_API_KEY=...     pytest tests/system -k e2b
    MODAL_TOKEN_ID=...  pytest tests/system -k modal
    ISLO_API_KEY=...    pytest tests/system -k islo
"""

from __future__ import annotations

import os

import pytest

pytest.importorskip("airflow.sdk")

from airflow.exceptions import AirflowException  # noqa: E402

from airflow.providers.sandbox.operators.sandbox import SandboxOperator  # noqa: E402

# (provider, env var that gates it, default image)
SAAS = [
    ("daytona", "DAYTONA_API_KEY", None),
    ("e2b", "E2B_API_KEY", "base"),
    ("modal", "MODAL_TOKEN_ID", "python:3.12-slim"),
    ("islo", "ISLO_API_KEY", "python:3.12-slim"),
]


def test_local_backend_runs_command_and_injects_env():
    """Runnable with zero credentials — the always-on system check."""
    op = SandboxOperator(
        task_id="sys_local",
        provider="local",
        env={"SECRET": "s3cr3t"},
        command='echo "injected=${SECRET:+yes}"; echo SYS_OK',
        poll_interval=1,
    )
    out = op.execute({})
    assert "injected=yes" in out
    assert "SYS_OK" in out


def test_local_backend_propagates_failure():
    op = SandboxOperator(task_id="sys_fail", provider="local", command="exit 5", poll_interval=1)
    with pytest.raises(AirflowException):
        op.execute({})


@pytest.mark.parametrize("provider,env_key,image", SAAS, ids=[p for p, _, _ in SAAS])
def test_saas_backend_live(provider, env_key, image):
    if not os.environ.get(env_key):
        pytest.skip(f"{provider}: {env_key} not set (live SaaS test skipped)")
    op = SandboxOperator(
        task_id=f"sys_{provider}",
        provider=provider,
        image=image,
        command='echo SYS_OK_${RANDOM}',
        sandbox_timeout=300,
        poll_interval=3,
    )
    assert "SYS_OK" in op.execute({})
