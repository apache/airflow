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
    "airflow-core/src/airflow/dag_processing",
    "airflow-core/src/airflow/triggers",
]
"""
Other potential source file packages to scan for coverage.
You can also compare the stats against those on
https://app.codecov.io/github/apache/airflow
(as it combines the coverage from all tests and so may be a bit higher).

    "airflow-core/src/airflow/callbacks",
    "airflow-core/src/airflow/config_templates",
    "airflow-core/src/airflow/dag_processing",
    "airflow-core/src/airflow/assets",
    "airflow-core/src/airflow/decorators",
    "airflow-core/src/airflow/hooks",
    "airflow-core/src/airflow/io",
    "airflow-core/src/airflow/lineage",
    "airflow-core/src/airflow/listeners",
    "airflow-core/src/airflow/macros",
    "airflow-core/src/airflow/notifications",
    "airflow-core/src/airflow/secrets",
    "airflow-core/src/airflow/security",
    "airflow-core/src/airflow/sensors",
    "airflow-core/src/airflow/task",
    "airflow-core/src/airflow/template",
    "airflow-core/src/airflow/timetables",
    "airflow-core/src/airflow/triggers",
"""

files_not_fully_covered = [
    "airflow-core/src/airflow/dag_processing/manager.py",
    "airflow-core/src/airflow/dag_processing/processor.py",
    "airflow-core/src/airflow/triggers/base.py",
    "airflow-core/src/airflow/triggers/external_task.py",
    "airflow-core/src/airflow/triggers/file.py",
    "airflow-core/src/airflow/triggers/testing.py",
]

other_tests = [
    "airflow-core/tests/dag_processing",
    "airflow-core/tests/jobs",
]

"""
Other tests to potentially run against the source_file packages:

    "airflow-core/tests/auth",
    "airflow-core/tests/callbacks",
    "airflow-core/tests/charts",
    "airflow-core/tests/cluster_policies",
    "airflow-core/tests/config_templates",
    "airflow-core/tests/dag_processing",
    "airflow-core/tests/assets",
    "airflow-core/tests/decorators",
    "airflow-core/tests/hooks",
    "airflow-core/tests/io",
    "airflow-core/tests/jobs",
    "airflow-core/tests/lineage",
    "airflow-core/tests/listeners",
    "airflow-core/tests/macros",
    "airflow-core/tests/notifications",
    "airflow-core/tests/plugins",
    "airflow-core/tests/secrets",
    "airflow-core/tests/security",
    "airflow-core/tests/sensors",
    "airflow-core/tests/task",
    "airflow-core/tests/testconfig",
    "airflow-core/tests/timetables",
"""


if __name__ == "__main__":
    args = ["-qq"] + other_tests
    run_tests(args, source_files, files_not_fully_covered)
