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
from datetime import datetime

from airflow.sdk import dag, task

SYSTEM_TEST_MARKER = "docker-sandbox-system-test"


@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "docker", "sandbox"],
)
def example_docker_sandbox_executor():
    @task(
        executor_config={
            "sandbox": {
                "env": {"DOCKER_SANDBOX_SYSTEM_TEST_MARKER": SYSTEM_TEST_MARKER},
            }
        }
    )
    def run_in_sandbox() -> dict[str, str]:
        assert os.environ.get("AIRFLOW_SANDBOX_DRIVER") == "docker-sandbox"
        assert os.environ.get("DOCKER_SANDBOX_SYSTEM_TEST_MARKER") == SYSTEM_TEST_MARKER
        return {"driver": "docker-sandbox", "marker": SYSTEM_TEST_MARKER}

    @task
    def verify_xcom(result: dict[str, str]) -> None:
        assert result == {"driver": "docker-sandbox", "marker": SYSTEM_TEST_MARKER}

    verify_xcom(run_in_sandbox())


example_docker_sandbox_executor_dag = example_docker_sandbox_executor()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example Dag with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(example_docker_sandbox_executor_dag)
