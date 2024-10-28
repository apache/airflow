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

SOURCE_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_PYTHON_MAJOR_MINOR_VERSION = "3.9"
DEFAULT_DOCKER_IMAGE = (
    f"ghcr.io/apache/airflow/main/prod/python{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}:latest"
)
DOCKER_IMAGE = os.environ.get("DOCKER_IMAGE") or DEFAULT_DOCKER_IMAGE
