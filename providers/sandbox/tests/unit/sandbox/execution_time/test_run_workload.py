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
"""The in-sandbox runner reconstructs the workload and calls supervise()."""

from __future__ import annotations

import base64
from types import SimpleNamespace

import pytest

pytest.importorskip("airflow.sdk.execution_time.supervisor")

from airflow.providers.sandbox.execution_time import run_workload


def test_load_raw_from_env(monkeypatch):
    monkeypatch.setenv("AIRFLOW_SANDBOX_WORKLOAD", base64.b64encode(b'{"x":1}').decode())
    assert run_workload._load_raw() == '{"x":1}'


def test_main_calls_supervise_with_workload_fields(monkeypatch):
    import airflow.executors.workloads as wl_mod
    import airflow.sdk.execution_time.supervisor as sup_mod

    fake_wl = SimpleNamespace(
        ti="TI", bundle_info="BI", dag_rel_path="dags/d.py", token="jwt-tok", log_path="log.txt"
    )
    monkeypatch.setattr(
        wl_mod.ExecuteTask, "model_validate_json", classmethod(lambda cls, s: fake_wl)
    )
    captured: dict = {}
    monkeypatch.setattr(sup_mod, "supervise", lambda **kw: captured.update(kw) or 0)
    monkeypatch.setenv("AIRFLOW_SANDBOX_WORKLOAD", base64.b64encode(b"{}").decode())
    monkeypatch.setenv("AIRFLOW__CORE__EXECUTION_API_SERVER_URL", "https://api.example/execution/")

    rc = run_workload.main()

    assert rc == 0
    assert captured["ti"] == "TI"
    assert captured["bundle_info"] == "BI"
    assert captured["dag_rel_path"] == "dags/d.py"
    assert captured["token"] == "jwt-tok"
    assert captured["server"] == "https://api.example/execution/"
