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
# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#   "rich>=13.6.0",
# ]
# ///

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.resolve()))  # make sure common_prek_utils is imported
from common_prek_utils import AIRFLOW_ROOT_PATH, get_all_provider_ids, insert_documentation

START_MARKER = "      # START automatically generated volumes by generate-volumes-for-sources prek hook"
END_MARKER = "      # END automatically generated volumes by generate-volumes-for-sources prek hook"

REMOVE_SOURCES_YAML = AIRFLOW_ROOT_PATH / "scripts" / "ci" / "docker-compose" / "remove-sources.yml"
TESTS_SOURCES_YAML = AIRFLOW_ROOT_PATH / "scripts" / "ci" / "docker-compose" / "tests-sources.yml"

providers_paths = sorted([provider.replace(".", "/") for provider in get_all_provider_ids()])


if __name__ == "__main__":
    remove_sources_lines = [
        f"      - ../../../empty:/opt/airflow/providers/{provider}/src\n" for provider in providers_paths
    ]
    insert_documentation(REMOVE_SOURCES_YAML, remove_sources_lines, START_MARKER, END_MARKER)
    tests_sources_lines = [
        f"      - ../../../providers/{provider_path}/tests:/opt/airflow/providers/{provider_path}/tests\n"
        for provider_path in providers_paths
    ]
    insert_documentation(TESTS_SOURCES_YAML, tests_sources_lines, START_MARKER, END_MARKER)
