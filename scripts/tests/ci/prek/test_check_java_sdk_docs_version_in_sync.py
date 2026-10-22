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
from check_java_sdk_docs_version_in_sync import DOKKA_MODULE, JAVA_RST, SDK_README, check_sync


def _contents(rst_version: str, readme_version: str, module_version: str) -> dict[str, str]:
    return {
        JAVA_RST: f"""
This guide covers version ``{rst_version}``
and later.

    airflowJavaSdkVersion={rst_version}

      <airflow.sdk.version>{rst_version}</airflow.sdk.version>
""",
        SDK_README: f"""
**Status:** {readme_version} · Java 11+

    id("org.apache.airflow.sdk") version "{readme_version}"
    annotationProcessor("org.apache.airflow:airflow-sdk-processor:{readme_version}")
    implementation("org.apache.airflow:airflow-sdk:{readme_version}")
""",
        DOKKA_MODULE: f"""
    id("org.apache.airflow.sdk") version "{module_version}"
    annotationProcessor("org.apache.airflow:airflow-sdk-processor:{module_version}")
    implementation("org.apache.airflow:airflow-sdk:{module_version}")
""",
    }


def test_accepts_one_version_everywhere():
    assert check_sync(_contents("1.0.0-beta2", "1.0.0-beta2", "1.0.0-beta2")) == []


@pytest.mark.parametrize(
    "rst, readme, module, stale",
    [
        pytest.param("1.0.0", "1.0.0-beta2", "1.0.0-beta2", SDK_README, id="guide-bumped-alone"),
        pytest.param("1.0.0-beta2", "1.0.0-beta2", "1.0.0-beta1", DOKKA_MODULE, id="dokka-left-behind"),
    ],
)
def test_reports_a_version_that_drifted(rst: str, readme: str, module: str, stale: str):
    errors = check_sync(_contents(rst, readme, module))

    assert errors[0].startswith("Documented Java SDK versions disagree:")
    assert any(line.lstrip().startswith(stale) for line in errors[1:])


def test_reports_a_file_with_no_version():
    contents = _contents("1.0.0-beta2", "1.0.0-beta2", "1.0.0-beta2")
    contents[DOKKA_MODULE] = "Write Apache Airflow Dags and tasks in Java.\n"

    assert check_sync(contents) == [f"{DOKKA_MODULE}: no Java SDK version found"]
