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

import subprocess
import sys

import pytest


@pytest.mark.parametrize(
    "create_instance",
    [
        "from airflow.api_fastapi.execution_api.in_process import InProcessExecutionAPI; "
        "api = InProcessExecutionAPI(); ",
        "from airflow.dag_processing.manager import DagFileProcessorManager; "
        "manager = DagFileProcessorManager(max_runs=1); ",
    ],
    ids=["in_process_api", "dag_processor_manager"],
)
def test_construction_defers_execution_api_imports(create_instance):
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; " + create_instance + "modules = {'svcs', 'cadwyn', 'fastapi', 'aiohttp', "
            "'airflow.api_fastapi.execution_api.app'}; "
            "loaded = modules.intersection(sys.modules); "
            "assert not loaded, f'{loaded} imported eagerly'",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_app_reexports_in_process_api():
    from airflow.api_fastapi.execution_api.app import InProcessExecutionAPI as legacy_api
    from airflow.api_fastapi.execution_api.in_process import InProcessExecutionAPI

    assert legacy_api is InProcessExecutionAPI
