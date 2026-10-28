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

import pytest

os.environ["_AIRFLOW__AS_LIBRARY"] = "true"


@pytest.fixture(autouse=True)
def clear_otel_env_vars(monkeypatch):
    # These tests run on the CI runner itself, and a runner may export OTEL_* variables for its own
    # OpenTelemetry collector (RunsOn does), which take precedence over the Airflow configuration.
    for name in list(os.environ):
        if name.startswith("OTEL_"):
            monkeypatch.delenv(name)
