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
import subprocess
from pathlib import Path

import pytest

SCRIPT_PATH = Path(__file__).resolve().parents[2] / "docker" / "clean-logs.sh"


@pytest.mark.parametrize("nested_empty_directory", [False, True], ids=["empty-root", "empty-subdirectories"])
def test_clean_logs_preserves_root_directory(tmp_path, nested_empty_directory):
    logs = tmp_path / "logs"
    logs.mkdir()
    if nested_empty_directory:
        (logs / "scheduler" / "empty").mkdir(parents=True)

    subprocess.run(
        # Stop after the first cleanup pass without waiting for the next interval.
        ["bash", "-c", 'sleep() { exit 0; }; source "$1"', "bash", str(SCRIPT_PATH)],
        env={
            **os.environ,
            "AIRFLOW_HOME": str(tmp_path),
            "AIRFLOW__LOG_RETENTION_DAYS": "15",
            "AIRFLOW__LOG_RETENTION_MINUTES": "0",
            "AIRFLOW__LOG_CLEANUP_FREQUENCY_MINUTES": "15",
            "AIRFLOW__LOG_MAX_SIZE_BYTES": "0",
            "AIRFLOW__LOG_MAX_SIZE_PERCENT": "0",
        },
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert logs.is_dir()
    assert not list(logs.iterdir())
