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

import sys
from pathlib import Path

from cov_runner import run_tests

sys.path.insert(0, str(Path(__file__).parent.resolve()))

source_files = [
    "airflow/dag_processing",
    "airflow/triggers",
]
"""
Other potential source file packages to scan for coverage.
You can also compare the stats against those on
https://app.codecov.io/github/apache/airflow
(as it combines the coverage from all tests and so may be a bit higher).

    "airflow/auth",
    "airflow/callbacks",
    "airflow/config_templates",
    "airflow/dag_processing",
    "airflow/assets",
    "airflow/decorators",
    "airflow/hooks",
    "airflow/io",
    "airflow/lineage",
    "airflow/listeners",
    "airflow/macros",
    "airflow/notifications",
    "airflow/secrets",
    "airflow/security",
    "airflow/sensors",
    "airflow/task",
    "airflow/template",
    "airflow/timetables",
    "airflow/triggers",
"""

files_not_fully_covered = [
    "airflow/dag_processing/manager.py",
    "airflow/dag_processing/processor.py",
    "airflow/triggers/base.py",
    "airflow/triggers/external_task.py",
    "airflow/triggers/file.py",
    "airflow/triggers/testing.py",
]

other_tests = [
    "tests/dag_processing",
    "tests/jobs",
    "tests/triggers",
]

"""
Other tests to potentially run against the source_file packages:

    "tests/api_internal",
    "tests/auth",
    "tests/callbacks",
    "tests/charts",
    "tests/cluster_policies",
    "tests/config_templates",
    "tests/dag_processing",
    "tests/assets",
    "tests/decorators",
    "tests/hooks",
    "tests/io",
    "tests/jobs",
    "tests/lineage",
    "tests/listeners",
    "tests/macros",
    "tests/notifications",
    "tests/plugins",
    "tests/secrets",
    "tests/security",
    "tests/sensors",
    "tests/task",
    "tests/template",
    "tests/testconfig",
    "tests/timetables",
    "tests/triggers",
"""


if __name__ == "__main__":
    args = ["-qq"] + other_tests
    run_tests(args, source_files, files_not_fully_covered)
