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

AIRFLOW_ROOT_PATH = Path(__file__).resolve().parents[3]
TASK_SDK_INTEGRATION_TESTS_ROOT = AIRFLOW_ROOT_PATH / "task-sdk-integration-tests"

DEFAULT_PYTHON_MAJOR_MINOR_VERSION = "3.10"
MIN_DOCKER_VERSION = "25.0.0"
MIN_DOCKER_COMPOSE_VERSION = "2.20.2"
DEFAULT_DOCKER_IMAGE = f"ghcr.io/apache/airflow/main/prod/python{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}:latest"
DOCKER_IMAGE = os.environ.get("DOCKER_IMAGE") or DEFAULT_DOCKER_IMAGE

DOCKER_COMPOSE_HOST_PORT = os.environ.get("HOST_PORT", "localhost:8080")
TASK_SDK_HOST_PORT = os.environ.get("TASK_SDK_HOST_PORT", "localhost:8080")

TASK_SDK_INTEGRATION_DOCKER_COMPOSE_FILE_PATH = TASK_SDK_INTEGRATION_TESTS_ROOT / "docker-compose.yaml"
TASK_SDK_INTEGRATION_LOCAL_DOCKER_COMPOSE_FILE_PATH = (
    TASK_SDK_INTEGRATION_TESTS_ROOT / "docker-compose-local.yaml"
)
TASK_SDK_INTEGRATION_DAGS_FOLDER = TASK_SDK_INTEGRATION_TESTS_ROOT / "dags"
TASK_SDK_INTEGRATION_LOGS_FOLDER = TASK_SDK_INTEGRATION_TESTS_ROOT / "logs"
TASK_SDK_INTEGRATION_ENV_FILE = TASK_SDK_INTEGRATION_TESTS_ROOT / ".env"
