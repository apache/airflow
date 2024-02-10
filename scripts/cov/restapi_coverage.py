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

source_files = ["airflow/api_experimental", "airflow/api_connexion", "airflow/api_internal"]

restapi_files = ["tests/api_experimental", "tests/api_connexion", "tests/api_internal"]

files_not_fully_covered = [
    "airflow/api_connexion/endpoints/forward_to_fab_endpoint.py",
    "airflow/api_connexion/endpoints/task_instance_endpoint.py",
    "airflow/api_connexion/exceptions.py",
    "airflow/api_connexion/schemas/common_schema.py",
    "airflow/api_connexion/security.py",
    "airflow/api_connexion/types.py",
    "airflow/api_internal/endpoints/rpc_api_endpoint.py",
    "airflow/api_internal/internal_api_call.py",
]

if __name__ == "__main__":
    args = ["-qq"] + restapi_files
    run_tests(args, source_files, files_not_fully_covered)
