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

import subprocess
from unittest import mock

from airflow_breeze.utils import workflow_status


@mock.patch.object(workflow_status.subprocess, "run", autospec=True)
def test_reads_only_completed_runs(mock_run):
    """A run still in progress has no conclusion yet and must not be reported as all passing."""
    mock_run.return_value = subprocess.CompletedProcess(
        args=[], returncode=0, stdout=b'[{"conclusion": "failure", "url": "u", "databaseId": 1}]'
    )

    workflow_status.workflow_status("v3-4-test", "ci-amd.yml")

    cmd = mock_run.call_args.args[0]
    assert cmd[cmd.index("--status") + 1] == "completed"
