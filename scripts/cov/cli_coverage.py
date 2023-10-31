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

source_files = ["airflow/cli"]

cli_files = ["tests/cli"]

files_not_fully_covered = [
    "airflow/cli/cli_config.py",
    "airflow/cli/cli_parser.py",
    "airflow/cli/commands/celery_command.py",
    "airflow/cli/commands/config_command.py",
    "airflow/cli/commands/connection_command.py",
    "airflow/cli/commands/dag_command.py",
    "airflow/cli/commands/dag_processor_command.py",
    "airflow/cli/commands/db_command.py",
    "airflow/cli/commands/info_command.py",
    "airflow/cli/commands/internal_api_command.py",
    "airflow/cli/commands/jobs_command.py",
    "airflow/cli/commands/kubernetes_command.py",
    "airflow/cli/commands/plugins_command.py",
    "airflow/cli/commands/pool_command.py",
    "airflow/cli/commands/provider_command.py",
    "airflow/cli/commands/scheduler_command.py",
    "airflow/cli/commands/standalone_command.py",
    "airflow/cli/commands/task_command.py",
    "airflow/cli/commands/triggerer_command.py",
    "airflow/cli/commands/variable_command.py",
    "airflow/cli/commands/webserver_command.py",
    "airflow/cli/simple_table.py",
]

if __name__ == "__main__":
    args = ["-qq"] + cli_files
    run_tests(args, source_files, files_not_fully_covered)
