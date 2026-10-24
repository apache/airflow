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

import pytest
import yaml
from check_ci_gate_needs import CI_WORKFLOW, find_problems


def test_real_workflow_gate_needs_every_job():
    assert find_problems(yaml.safe_load(CI_WORKFLOW.read_text())["jobs"]) == []


@pytest.mark.parametrize(
    ("jobs", "expected"),
    [
        pytest.param(
            {"build": {}, "test": {}, "notify-slack": {}, "ci-passed": {"needs": ["build", "test"]}},
            [],
            id="complete",
        ),
        pytest.param({"build": {}}, ["job 'ci-passed' is missing"], id="no-gate"),
        pytest.param(
            {"build": {}, "test": {}, "ci-passed": {"needs": ["build"]}},
            ["job 'test' is not in ci-passed.needs, so its failure would not stop auto-merge"],
            id="missing-job",
        ),
        pytest.param(
            {"build": {}, "ci-passed": {"needs": "build"}},
            [],
            id="needs-as-string",
        ),
        pytest.param(
            {"build": {}, "ci-passed": {"needs": ["build", "gone"]}},
            ["ci-passed.needs lists 'gone', which is not a job"],
            id="unknown-job",
        ),
        pytest.param(
            {"build": {}, "summarize-warnings": {}, "ci-passed": {"needs": ["build", "summarize-warnings"]}},
            ["ci-passed.needs lists exempt reporting job 'summarize-warnings'"],
            id="exempt-job-listed",
        ),
    ],
)
def test_find_problems(jobs, expected):
    assert find_problems(jobs) == expected
