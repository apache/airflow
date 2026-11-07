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

DEFAULT_PYTHON_MAJOR_MINOR_VERSION = "3.10"
DEFAULT_DOCKER_IMAGE = f"ghcr.io/apache/airflow/main/prod/python{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}:latest"
DOCKER_IMAGE = os.environ.get("DOCKER_IMAGE") or DEFAULT_DOCKER_IMAGE

DOCKER_COMPOSE_HOST_PORT = os.environ.get("HOST_PORT", "localhost:8080")

DOCKER_COMPOSE_FILE_PATH = (
    AIRFLOW_ROOT_PATH / "airflow-core" / "docs" / "howto" / "docker-compose" / "docker-compose.yaml"
)

API_USERNAME = "airflow"
API_PASSWORD = "airflow"

LOGIN_COMMAND = f"auth login --username {API_USERNAME} --password {API_PASSWORD}"
LOGIN_OUTPUT = "Login successful! Welcome to airflowctl!"

DAG_RUN_WAIT_TIMEOUT_ENV = "AIRFLOW_CTL_TEST_DAG_RUN_TIMEOUT"
# Budget for a freshly booted compose stack to schedule and finish a Dag run: the
# scheduler still has to warm up, parse the examples and queue the tasks, none of which
# is a failure. Slower machines can raise it through the environment variable.
DAG_RUN_WAIT_TIMEOUT = float(os.environ.get(DAG_RUN_WAIT_TIMEOUT_ENV, "300"))
