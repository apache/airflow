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
from check_scheduled_workflows_skip_forks import GUARD, check_file, is_covered

SCHEDULE_ONLY = {"schedule": [{"cron": "0 0 * * *"}]}


@pytest.mark.parametrize(
    ("jobs", "job_name", "expected"),
    [
        pytest.param({"job": {"if": GUARD}}, "job", True, id="own-guard"),
        pytest.param({"job": {}}, "job", False, id="no-guard-no-needs"),
        pytest.param(
            {"parent": {"if": GUARD}, "job": {"needs": "parent"}}, "job", True, id="needs-as-string"
        ),
        pytest.param(
            {"root": {"if": GUARD}, "middle": {"needs": "root"}, "job": {"needs": "middle"}},
            "job",
            True,
            id="chain",
        ),
        pytest.param(
            {"first": {}, "second": {"if": GUARD}, "job": {"needs": ["first", "second"]}},
            "job",
            True,
            id="one-of-several-parents-covered",
        ),
        pytest.param(
            {"first": {}, "second": {}, "job": {"needs": ["first", "second"]}},
            "job",
            False,
            id="no-parent-covered",
        ),
        pytest.param(
            {"parent": {"if": GUARD}, "job": {"needs": "parent", "if": "!cancelled()"}},
            "job",
            False,
            id="status-function-overrides-covered-parent",
        ),
        pytest.param(
            {"parent": {"if": GUARD}, "job": {"needs": "parent", "if": f"!cancelled() && {GUARD}"}},
            "job",
            True,
            id="status-function-with-own-guard",
        ),
    ],
)
def test_is_covered(jobs, job_name, expected):
    assert is_covered(jobs, job_name) is expected


@pytest.mark.parametrize(
    ("workflow", "expected"),
    [
        pytest.param(
            {"on": {"push": None}, "jobs": {"job": {}}},
            [],
            id="not-scheduled",
        ),
        pytest.param(
            {"on": "push", "jobs": {"job": {}}},
            [],
            id="trigger-as-string",
        ),
        pytest.param(
            {"on": SCHEDULE_ONLY, "jobs": {"guarded": {"if": GUARD}, "unguarded": {}}},
            ["unguarded"],
            id="scheduled-with-unguarded-job",
        ),
        # Real workflows load `on:` as the key True, so this case uses True instead of "on".
        pytest.param(
            {True: SCHEDULE_ONLY, "jobs": {"job": {}}},
            ["job"],
            id="bare-on-key",
        ),
    ],
)
def test_check_file(write_workflow_file, workflow, expected):
    assert check_file(write_workflow_file(workflow)) == expected


def test_check_file_skips_exempt_workflow(tmp_path):
    path = tmp_path / "ci-amd.yml"
    path.write_text(yaml.dump({"on": SCHEDULE_ONLY, "jobs": {"job": {}}}))
    assert check_file(path) == []
