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

"""Tests for the agent-skills decision engine."""

from __future__ import annotations

from scripts.ci.agent_skills.decision_engine import get_recommended_command


def test_host_prefers_local_command():
    result = get_recommended_command(
        workflow_id="run_targeted_tests",
        test_path="tests/models/test_example.py",
        context="host",
    )

    assert result["mode"] == "command"
    assert result["command"] == "uv run --project distribution_folder pytest tests/models/test_example.py"
    assert result["reason"] == "Host context prefers local-first execution."


def test_host_falls_back_to_breeze():
    result = get_recommended_command(
        workflow_id="run_targeted_tests",
        test_path="tests/models/test_example.py",
        context="host",
        force_breeze_fallback=True,
    )

    assert result["mode"] == "command"
    assert result["command"] == "breeze exec pytest tests/models/test_example.py -xvs"


def test_inside_breeze_runs_direct_pytest():
    result = get_recommended_command(
        workflow_id="run_targeted_tests",
        test_path="tests/models/test_example.py",
        context="breeze",
    )

    assert result["mode"] == "command"
    assert result["command"] == "pytest tests/models/test_example.py -xvs"
