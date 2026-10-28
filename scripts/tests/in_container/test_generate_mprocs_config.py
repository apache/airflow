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

import importlib.util
from pathlib import Path

import pytest
import yaml

MODULE_PATH = Path(__file__).parents[2] / "in_container" / "bin" / "generate_mprocs_config.py"


def load_module():
    spec = importlib.util.spec_from_file_location("generate_mprocs_config", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_quit_key_quits_immediately():
    module = load_module()

    config = yaml.safe_load(module.generate_mprocs_config())

    assert config["keymap_procs"] == {"<q>": {"c": "quit"}}


@pytest.mark.parametrize(
    ("airflow_version", "web_process"),
    [pytest.param("", "api_server", id="airflow-3"), pytest.param("2.11.0", "webserver", id="airflow-2")],
)
def test_every_process_restarts_after_it_exits(monkeypatch, airflow_version, web_process):
    monkeypatch.setenv("USE_AIRFLOW_VERSION", airflow_version)
    monkeypatch.setenv("INTEGRATION_CELERY", "true")
    monkeypatch.setenv("CELERY_FLOWER", "true")
    monkeypatch.setenv("STANDALONE_DAG_PROCESSOR", "true")
    monkeypatch.setenv(
        "AIRFLOW__CORE__EXECUTOR", "airflow.providers.edge3.executors.edge_executor.EdgeExecutor"
    )
    module = load_module()

    procs = yaml.safe_load(module.generate_mprocs_config())["procs"]

    assert set(procs) == {
        "scheduler",
        web_process,
        "triggerer",
        "celery_worker",
        "flower",
        "edge_worker",
        "dag_processor",
        "shell",
    }
    assert {name: proc.get("autorestart") for name, proc in procs.items()} == dict.fromkeys(procs, True)
