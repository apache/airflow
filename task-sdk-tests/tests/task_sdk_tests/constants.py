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
TASK_SDK_TESTS_ROOT = Path(__file__).resolve().parents[2]

DEFAULT_PYTHON_MAJOR_MINOR_VERSION = "3.10"
DEFAULT_DOCKER_IMAGE = f"ghcr.io/apache/airflow/main/prod/python{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}:latest"
DOCKER_IMAGE = os.environ.get("DOCKER_IMAGE") or DEFAULT_DOCKER_IMAGE

DOCKER_COMPOSE_HOST_PORT = os.environ.get("HOST_PORT", "localhost:8080")
TASK_SDK_HOST_PORT = os.environ.get("TASK_SDK_HOST_PORT", "localhost:8080")
TASK_SDK_API_VERSION = "2025-08-10"

DOCKER_COMPOSE_FILE_PATH = TASK_SDK_TESTS_ROOT / "docker" / "docker-compose.yaml"
