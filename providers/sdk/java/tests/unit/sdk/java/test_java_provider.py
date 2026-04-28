#
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

import pytest

from airflow.providers.sdk.java.coordinator import JavaCoordinator
from airflow.providers.sdk.java.get_provider_info import get_provider_info

from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_PLUS

if not AIRFLOW_V_3_3_PLUS:
    pytest.skip("Coordinator is only compatible with Airflow >= 3.3.0", allow_module_level=True)


def test_get_provider_info_exposes_java_runtime_components():
    assert get_provider_info() == {
        "package-name": "apache-airflow-providers-sdk-java",
        "name": "SDK: Java",
        "description": "Java SDK support for Apache Airflow runtime coordinators.\n",
        "integrations": [
            {
                "integration-name": "Java",
                "external-doc-url": "https://openjdk.org/",
                "tags": ["software"],
            }
        ],
        "coordinators": [
            "airflow.providers.sdk.java.coordinator.JavaCoordinator",
        ],
    }


def test_java_provider_entrypoints_are_importable():
    assert JavaCoordinator.sdk == "java"
