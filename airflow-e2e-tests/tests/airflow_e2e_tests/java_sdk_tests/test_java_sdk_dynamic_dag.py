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
"""E2E test for the native Java Dags that ``java-native-bundle`` generates from a YAML file.

Run with::

    E2E_TEST_MODE=java_sdk uv run --project airflow-e2e-tests pytest \\
        tests/airflow_e2e_tests/java_sdk_tests/test_java_sdk_dynamic_dag.py -xvs

``conftest._setup_java_sdk_integration`` puts the bundle JAR into the Dags folder, with no ``dags.yaml``
next to it yet. The JAR generates one Dag for each entry of the YAML the test writes there, so the test
swaps only the YAML, with no rebuild, and checks that the live Dag IDs follow it and that each live Dag
runs.
"""

from __future__ import annotations

from pathlib import Path

from airflow_e2e_tests.e2e_test_utils.clients import AirflowClient
from airflow_e2e_tests.e2e_test_utils.dynamic_dags import swap_yaml_and_check

# The Dag processor starts a JVM to parse the JAR, so the Dags appear some time after the stack is up.
_PARSE_TIMEOUT = 600
# After that, the Dag processor re-parses every few seconds (see java.yml), and a Dag that left the
# YAML goes stale a few seconds after that.
_RELOAD_TIMEOUT = 180
_JAVA_TASK_TIMEOUT = 600

_TAGS = ["java_dynamic"]


def test_swapping_the_yaml_changes_the_live_dags(airflow_dags_path: Path):
    client = AirflowClient()
    yaml_path = airflow_dags_path / "java-native" / "dags.yaml"

    live = swap_yaml_and_check(
        client,
        yaml_path,
        {
            "java_dynamic_a": "hello from a",
            "java_dynamic_b": "hello from b",
            "java_dynamic_c": "hello from c",
        },
        previous=set(),
        tags=_TAGS,
        timeout=_PARSE_TIMEOUT,
        task_timeout=_JAVA_TASK_TIMEOUT,
    )
    live = swap_yaml_and_check(
        client,
        yaml_path,
        {
            "java_dynamic_a": "hello from a",
            "java_dynamic_b": "hello from b",
            "java_dynamic_c": "hello from c",
            "java_dynamic_d": "hello from d",
            "java_dynamic_e": "hello from e",
        },
        previous=live,
        tags=_TAGS,
        timeout=_RELOAD_TIMEOUT,
        task_timeout=_JAVA_TASK_TIMEOUT,
    )
    # a keeps its Dag ID but gets a new greeting. The greeting is not in the serialized Dag, so its run
    # shows that the worker built the Dag from the new YAML, not that the Dag processor rebuilt it.
    swap_yaml_and_check(
        client,
        yaml_path,
        {"java_dynamic_a": "hello again from a", "java_dynamic_f": "hello from f"},
        previous=live,
        tags=_TAGS,
        timeout=_RELOAD_TIMEOUT,
        task_timeout=_JAVA_TASK_TIMEOUT,
    )
