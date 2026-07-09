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

import os
from pathlib import Path

from rich.console import Console

console = Console(width=400, color_system="standard")

AIRFLOW_ROOT_PATH = Path(__file__).resolve().parents[3]
OPENLINEAGE_E2E_TESTS_ROOT = AIRFLOW_ROOT_PATH / "providers-e2e-tests" / "openlineage"

DEFAULT_PYTHON_MAJOR_MINOR_VERSION = "3.10"
MIN_DOCKER_VERSION = "25.0.0"
MIN_DOCKER_COMPOSE_VERSION = "2.20.2"
DEFAULT_DOCKER_IMAGE = f"ghcr.io/apache/airflow/main/prod/python{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}:latest"
DOCKER_IMAGE = os.environ.get("DOCKER_IMAGE") or DEFAULT_DOCKER_IMAGE

API_HOST_PORT = os.environ.get("HOST_PORT", "localhost:8080")
API_BASE_URL = f"http://{API_HOST_PORT}"

DOCKER_COMPOSE_FILE_PATH = OPENLINEAGE_E2E_TESTS_ROOT / "docker-compose.yaml"
LOCAL_DOCKER_COMPOSE_FILE_PATH = OPENLINEAGE_E2E_TESTS_ROOT / "docker-compose-local.yaml"
DAGS_FOLDER = OPENLINEAGE_E2E_TESTS_ROOT / "dags"
LOGS_FOLDER = OPENLINEAGE_E2E_TESTS_ROOT / "logs"
ENV_FILE = OPENLINEAGE_E2E_TESTS_ROOT / ".env"
