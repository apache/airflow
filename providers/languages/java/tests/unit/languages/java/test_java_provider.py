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

from airflow.providers.languages.java.dag_file_processors import JavaDagFileProcessor
from airflow.providers.languages.java.get_provider_info import get_provider_info
from airflow.providers.languages.java.task_coordinators import JavaTaskCoordinator


def test_get_provider_info_exposes_java_runtime_components():
    assert get_provider_info() == {
        "package-name": "apache-airflow-providers-languages-java",
        "name": "Languages: Java",
        "description": "Java language support for Apache Airflow task coordinators and DAG file processors.\n",
        "integrations": [
            {
                "integration-name": "Java",
                "external-doc-url": "https://openjdk.org/",
                "tags": ["language", "java"],
            }
        ],
        "task-coordinators": [
            "airflow.providers.languages.java.task_coordinators.JavaTaskCoordinator",
        ],
        "dag-file-processors": [
            "airflow.providers.languages.java.dag_file_processors.JavaDagFileProcessor",
        ],
    }


def test_java_provider_entrypoints_are_importable():
    assert JavaTaskCoordinator.language == "java"
    assert JavaDagFileProcessor.language == "java"
