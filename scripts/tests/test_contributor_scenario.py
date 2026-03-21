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

"""Test simulating a full human/agent contributor workflow (the 'Exam')."""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest
from ci.prek.breeze_context import BreezieContext


class TestContributorScenario:
    """Verifies the standard workflow (git add -> prek -> pytest) models correctly."""

    def test_end_to_end_host_contribution_workflow(self):
        """Simulates an agent running the standard workflow entirely on the host."""
        with patch.dict(os.environ, {}, clear=True):
            with patch("os.path.exists", return_value=False):
                with patch("os.path.isdir", return_value=False):
                    assert BreezieContext.is_in_breeze() is False

                    # Step 1: Stage changes
                    cmd1 = BreezieContext.get_command("stage-changes", path="airflow/models/dag.py")
                    assert cmd1 == "git add airflow/models/dag.py"

                    # Step 2: Run static checks
                    cmd2 = BreezieContext.get_command("run-static-checks", module="airflow/models/dag.py")
                    assert cmd2 == "prek run airflow/models/dag.py"

                    # Step 3: Run targeted unit tests
                    cmd3 = BreezieContext.get_command("run-unit-tests", test_path="tests/models/test_dag.py")
                    assert cmd3 == "uv run --project airflow pytest tests/models/test_dag.py"

    def test_host_only_guard_in_breeze(self):
        """Simulates an agent attempting to run a host-only command while inside Breeze context."""
        with patch.dict(os.environ, {"BREEZE_HOME": "/opt/airflow"}):
            assert BreezieContext.is_in_breeze() is True

            # Step 1: Attempt to stage changes (should fail because git is host-only)
            with pytest.raises(ValueError, match="host-only"):
                BreezieContext.get_command("stage-changes", path="airflow/models/dag.py")

            # Step 2 & 3: Still work
            cmd2 = BreezieContext.get_command("run-static-checks", module="airflow/models/dag.py")
            assert cmd2 == "prek run airflow/models/dag.py"

            cmd3 = BreezieContext.get_command("run-unit-tests", test_path="tests/models/test_dag.py")
            assert cmd3 == "breeze exec pytest tests/models/test_dag.py"
