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

import pytest

DATA_FILES_DIRECTORY = Path(__file__).resolve().parent


@pytest.fixture(autouse=True)
def initialize_providers_manager():
    from airflow.providers_manager import ProvidersManager

    ProvidersManager().initialize_providers_configuration()


@pytest.fixture
def pod_template() -> Path:
    return (DATA_FILES_DIRECTORY / "pod.yaml").resolve(strict=True)


@pytest.fixture
def basic_pod_template() -> Path:
    return (DATA_FILES_DIRECTORY / "basic_pod.yaml").resolve(strict=True)


@pytest.fixture
def create_connection_without_db(monkeypatch):
    """
    Fixture to create connections for tests without using the database.

    This fixture uses monkeypatch to set the appropriate AIRFLOW_CONN_{conn_id} environment variable.
    """

    def _create_conn(connection, session=None):
        """Create connection using environment variable."""

        env_var_name = f"AIRFLOW_CONN_{connection.conn_id.upper()}"
        monkeypatch.setenv(env_var_name, connection.as_json())

    return _create_conn
