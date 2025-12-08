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

DOCKER_COMPOSE_HOST_PORT = os.environ.get("HOST_PORT", "localhost:8080")
DEFAULT_PYTHON_MAJOR_MINOR_VERSION = "3.10"
DEFAULT_DOCKER_IMAGE = f"ghcr.io/apache/airflow/main/prod/python{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}:latest"
DOCKER_IMAGE = os.environ.get("DOCKER_IMAGE") or DEFAULT_DOCKER_IMAGE
os.environ["AIRFLOW_UID"] = str(os.getuid())

DOCKER_COMPOSE_PATH = (
    AIRFLOW_ROOT_PATH / "airflow-core" / "docs" / "howto" / "docker-compose" / "docker-compose.yaml"
)

AIRFLOW_WWW_USER_USERNAME = os.environ.get("_AIRFLOW_WWW_USER_USERNAME", "airflow")
AIRFLOW_WWW_USER_PASSWORD = os.environ.get("_AIRFLOW_WWW_USER_PASSWORD", "airflow")

E2E_DAGS_FOLDER = AIRFLOW_ROOT_PATH / "airflow-e2e-tests" / "tests" / "airflow_e2e_tests" / "dags"

# The logs folder where the Airflow logs will be copied to and uploaded to github artifacts
LOGS_FOLDER = AIRFLOW_ROOT_PATH / "airflow-e2e-tests" / "logs"
TEST_REPORT_FILE = AIRFLOW_ROOT_PATH / "airflow-e2e-tests" / "_e2e_test_report.json"
LOCALSTACK_PATH = AIRFLOW_ROOT_PATH / "airflow-e2e-tests" / "docker" / "localstack.yml"
E2E_TEST_MODE = os.environ.get("E2E_TEST_MODE", "basic")
AWS_INIT_PATH = AIRFLOW_ROOT_PATH / "airflow-e2e-tests" / "scripts" / "init-aws.sh"
