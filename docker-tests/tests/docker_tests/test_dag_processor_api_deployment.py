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

import pytest
from python_on_whales import DockerClient

from docker_tests.constants import AIRFLOW_ROOT_PATH


@pytest.mark.parametrize("uid", [50000, 54321])
def test_isolated_processor_compose_example(tmp_path, monkeypatch, uid):
    monkeypatch.setenv("AIRFLOW_IMAGE_NAME", "processor-api:test")
    monkeypatch.setenv("DAG_PROCESSOR_API_NETWORK", "test-api-only")
    monkeypatch.setenv("AIRFLOW_UID", str(uid))
    credentials = {
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": "postgresql://must-not-reach-the-processor",
        "AIRFLOW__CORE__FERNET_KEY": "must-not-reach-the-processor",
        "AIRFLOW__API_AUTH__JWT_SECRET": "must-not-reach-the-processor",
        "AIRFLOW__API_AUTH__JWT_PRIVATE_KEY_PATH": "/server/private.key",
    }
    for name, value in credentials.items():
        monkeypatch.setenv(name, value)
    (tmp_path / ".env").write_text("\n".join(f"{name}={value}" for name, value in credentials.items()))
    example = AIRFLOW_ROOT_PATH / "airflow-core/docs/security/dag-processor-api/docker-compose.yaml"
    rendered = DockerClient(compose_files=[example], compose_project_directory=tmp_path).compose.config(
        return_json=True
    )
    assert set(rendered["services"]) == {"dag-processor"}
    processor = rendered["services"]["dag-processor"]
    assert processor["command"] == ["dag-processor"]
    assert processor["user"] == f"{uid}:0"
    assert processor["environment"]["USER"] == "airflow"
    assert not processor.get("depends_on")
    assert not processor.get("env_file")
    assert not credentials.keys() & processor["environment"].keys()
    assert processor["environment"]["CONNECTION_CHECK_MAX_COUNT"] == "0"
    assert (
        processor["environment"]["AIRFLOW__DAG_PROCESSOR__EXECUTION_API_TOKEN_FILE"]
        == "/run/dag-processor/processor.jwt"
    )
    assert processor["environment"]["AIRFLOW_CONFIG"] == "/tmp/airflow.cfg"
    assert processor["healthcheck"]["test"] == ["CMD", "airflow", "dag-processor", "--check-health"]
    assert set(processor["networks"]) == {"processor-api"}
    assert rendered["networks"]["processor-api"] == {"name": "test-api-only", "external": True}
    mounts = {mount["target"]: mount for mount in processor["volumes"]}
    assert set(mounts) == {"/run/dag-processor", "/opt/airflow/dags", "/opt/airflow/logs"}
    assert mounts["/run/dag-processor"]["read_only"]
    assert mounts["/opt/airflow/dags"]["read_only"]
    assert processor["read_only"]
    assert processor["cap_drop"] == ["ALL"]
