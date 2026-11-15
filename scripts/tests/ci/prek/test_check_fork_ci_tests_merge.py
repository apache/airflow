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
from check_fork_ci_tests_merge import MERGE_ACTION, check_job, check_workflow, main

CHECKOUT = {"uses": "actions/checkout@v7"}
MERGE = {"uses": MERGE_ACTION, "with": {"merge-with": "${{ inputs.merge-with }}"}}


@pytest.mark.parametrize(
    ("steps", "valid"),
    [
        pytest.param([CHECKOUT, MERGE], True, id="merge-after-checkout"),
        pytest.param([CHECKOUT], False, id="no-merge"),
        pytest.param([MERGE, CHECKOUT], False, id="merge-before-checkout"),
        pytest.param(
            [CHECKOUT, MERGE, {"uses": "actions/checkout@v7", "with": {"ref": "${{ github.sha }}"}}],
            False,
            id="checkout-again-after-merge",
        ),
        pytest.param(
            [{"uses": "actions/checkout@v7", "with": {"ref": "${{ inputs.checkout-ref }}"}}, MERGE],
            True,
            id="checkout-ref-input",
        ),
        pytest.param(
            [CHECKOUT, MERGE, {"uses": "actions/checkout@v7", "with": {"path": "constraints"}}],
            True,
            id="checkout-into-sub-directory",
        ),
        pytest.param(
            [{"uses": "actions/checkout@v7", "with": {"ref": "constraints-main"}}],
            True,
            id="checkout-of-another-ref",
        ),
        pytest.param([{"run": "echo"}], True, id="no-checkout"),
    ],
)
def test_check_job_steps(steps, valid):
    assert (check_job("tests.yml", "job", {"steps": steps}) == []) is valid


@pytest.mark.parametrize(
    ("job", "valid"),
    [
        pytest.param({"uses": "./.github/workflows/x.yml", "with": {"merge-with": "a b"}}, True, id="passes"),
        pytest.param({"uses": "./.github/workflows/x.yml", "with": {}}, False, id="missing"),
        pytest.param({"uses": "./.github/workflows/x.yml"}, False, id="no-with"),
    ],
)
def test_check_job_calls(job, valid):
    assert (check_job("tests.yml", "job", job) == []) is valid


@pytest.mark.parametrize("job_id", ["gate", "build-info", "notify-slack", "summarize-warnings"])
def test_exempt_jobs_of_the_top_workflows(job_id):
    assert check_job("ci-amd.yml", job_id, {"steps": [CHECKOUT]}) == []
    assert check_job("other.yml", job_id, {"steps": [CHECKOUT]}) != []


@pytest.mark.parametrize(
    ("inputs", "valid"),
    [
        pytest.param({"merge-with": {"type": "string"}}, True, id="takes-it"),
        pytest.param({"other": {"type": "string"}}, False, id="lacks-it"),
    ],
)
def test_reusable_workflows_take_merge_with(inputs, valid):
    workflow = {True: {"workflow_call": {"inputs": inputs}}, "jobs": {}}
    assert (check_workflow("reusable.yml", workflow) == []) is valid


def _write(path, workflow):
    path.write_text(yaml.safe_dump(workflow))


def test_main_follows_called_workflows(tmp_path):
    call = {"uses": "./.github/workflows/called.yml", "with": {"merge-with": "a b"}}
    for top in ("ci-amd.yml", "ci-arm.yml"):
        _write(tmp_path / top, {"on": {"push": {}}, "jobs": {"tests": call}})
    _write(
        tmp_path / "called.yml",
        {"on": {"workflow_call": {"inputs": {"merge-with": {}}}}, "jobs": {"j": {"steps": [CHECKOUT]}}},
    )
    _write(tmp_path / "unrelated.yml", {"on": {"push": {}}, "jobs": {"j": {"steps": [CHECKOUT]}}})

    assert main(tmp_path) == 1

    _write(
        tmp_path / "called.yml",
        {
            "on": {"workflow_call": {"inputs": {"merge-with": {}}}},
            "jobs": {"j": {"steps": [CHECKOUT, MERGE]}},
        },
    )

    assert main(tmp_path) == 0
