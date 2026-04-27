#!/usr/bin/env python
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
"""Generate Execution API OpenAPI schema. Prints JSON to stdout. Run with cwd at repo root."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

os.environ["_AIRFLOW__AS_LIBRARY"] = "1"
sys.path.insert(0, str(Path("airflow-core/src").resolve()))

import httpx

from airflow.api_fastapi.execution_api.app import InProcessExecutionAPI

app = InProcessExecutionAPI()
version = app.app.versions.version_values[0]
client = httpx.Client(transport=app.transport)
response = client.get(f"http://localhost/openapi.json?version={version}")
response.raise_for_status()
print(json.dumps(response.json()))
