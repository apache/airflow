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

import json
import subprocess

import pytest
import yaml

from airflow_breeze.utils.path_utils import AIRFLOW_ROOT_PATH

NODE_HARNESS = """
import { readFileSync } from 'node:fs';
const input = JSON.parse(readFileSync(0, 'utf8'));
process.env.SOURCE_RUN_ID = input.id;
process.env.GITHUB_RUN_ATTEMPT = String(input.attempt);
const outputs = {};
const requests = [];
const summary = {
  addHeading() { return this; }, addLink() { return this; },
  addRaw() { return this; }, async write() {}
};
const github = {
  rest: { actions: {
    async getWorkflowRun(args) { requests.push(args); return { data: input.run }; },
    listWorkflowRunArtifacts() {}
  } },
  async paginate(method, args) { requests.push(args); return input.artifacts; }
};
const core = { setOutput(key, value) { outputs[key] = value; }, summary, notice() {} };
const context = { repo: { owner: 'apache', repo: 'airflow' } };
const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;
try {
  await new AsyncFunction('github', 'context', 'core', input.script)(github, context, core);
  console.log(JSON.stringify({ outputs, requests }));
} catch (error) {
  console.log(JSON.stringify({ error: error.message, requests }));
}
"""


@pytest.fixture
def source_run():
    return {
        "head_repository": {"full_name": "apache/airflow"},
        "head_branch": "main",
        "path": ".github/workflows/ci-amd.yml",
        "event": "schedule",
        "status": "completed",
        "head_sha": "a" * 40,
        "html_url": "https://github.com/apache/airflow/actions/runs/123",
    }


@pytest.fixture
def source_artifacts():
    return [
        {
            "id": 1,
            "name": "dependency-report-inputs-3.14-constraints-source-providers-true",
            "expired": False,
        },
        {"id": 2, "name": "ci-image-save-v3-linux_amd64-3.14-main", "expired": False},
        {"id": 3, "name": "dependency-report-inputs-3.10-constraints-true", "expired": False},
    ]


def run_plan(source_run, source_artifacts, *, attempt=1, source_run_id="123"):
    workflow = yaml.safe_load(
        (AIRFLOW_ROOT_PATH / ".github/workflows/dependency-upgrade-report.yml").read_text()
    )
    script = workflow["jobs"]["plan"]["steps"][0]["with"]["script"]
    result = subprocess.run(
        ["node", "--input-type=module", "--eval", NODE_HARNESS],
        input=json.dumps(
            {
                "script": script,
                "id": source_run_id,
                "attempt": attempt,
                "run": source_run,
                "artifacts": source_artifacts,
            }
        ),
        capture_output=True,
        text=True,
        check=True,
        timeout=20,
    )
    return json.loads(result.stdout)


@pytest.mark.parametrize("attempt", [1, 2])
def test_plan_keeps_original_inputs_when_the_report_is_rerun(source_run, source_artifacts, attempt):
    result = run_plan(source_run, source_artifacts, attempt=attempt)
    outputs = result["outputs"]
    assert outputs["source-run-id"] == "123"
    assert outputs["source-sha"] == source_run["head_sha"]
    assert outputs["has-reports"] == "true"
    assert json.loads(outputs["matrix"])["include"] == [
        {
            "python": "3.14",
            "mode": "constraints-source-providers",
            "use-uv": "true",
            "inputs-id": 1,
            "image-id": 2,
        }
    ]
    assert all(request["run_id"] == 123 for request in result["requests"])


@pytest.mark.parametrize("attempt", [2, 3])
def test_plan_fails_a_rerun_when_original_inputs_are_gone(source_run, attempt):
    result = run_plan(source_run, [], attempt=attempt)
    assert result["error"] == "Saved report inputs are missing or expired"


def test_plan_skips_a_first_run_without_saved_inputs(source_run):
    result = run_plan(source_run, [])
    assert result["outputs"]["has-reports"] == "false"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("head_repository", {"full_name": "fork/airflow"}),
        ("head_branch", "feature"),
        ("event", "pull_request"),
        ("path", ".github/workflows/other.yml"),
        ("status", "in_progress"),
    ],
)
def test_plan_rejects_sources_outside_completed_main_canaries(source_run, source_artifacts, field, value):
    source_run[field] = value
    result = run_plan(source_run, source_artifacts)
    assert result["error"] == "Only completed main AMD canary runs are supported"


@pytest.mark.parametrize("missing", ["image", "expired-image", "expired-input"])
def test_plan_does_not_substitute_newer_artifacts(source_run, source_artifacts, missing):
    if missing == "image":
        source_artifacts.pop(1)
    elif missing == "expired-image":
        source_artifacts[1]["expired"] = True
    else:
        source_artifacts[0]["expired"] = True
    result = run_plan(source_run, source_artifacts)
    assert result["error"] == "Original inputs or CI image unavailable for 3.14:constraints-source-providers"


@pytest.mark.parametrize("source_run_id", ["0", "abc", "123\ninjected", "9007199254740992"])
def test_plan_rejects_invalid_source_ids_before_calling_github(source_run, source_artifacts, source_run_id):
    result = run_plan(source_run, source_artifacts, source_run_id=source_run_id)
    assert result["error"] == "Invalid source run ID"
    assert result["requests"] == []
