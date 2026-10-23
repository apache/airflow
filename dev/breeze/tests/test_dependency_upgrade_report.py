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
import os
import re
import shutil
import subprocess

import pytest
import yaml

from airflow_breeze.utils.path_utils import AIRFLOW_ROOT_PATH

NODE_HARNESS = """
import { readFileSync } from 'node:fs';
const input = JSON.parse(readFileSync(0, 'utf8'));
process.env.SOURCE_RUN_ID = input.id;
process.env.SOURCE_RUN_ATTEMPT = input.sourceAttempt;
process.env.GITHUB_RUN_ATTEMPT = String(input.attempt);
const result = { outputs: {}, requests: [], notices: [], warnings: [], summary: '' };
const summary = {
  addHeading() { return this; }, addLink() { return this; },
  addRaw(text) { result.summary += text; return this; }, async write() {}
};
const github = {
  rest: { actions: {
    async getWorkflowRunAttempt(args) { result.requests.push(args); return { data: input.run }; },
    listWorkflowRunArtifacts() {}
  } },
  async paginate(method, args) { result.requests.push(args); return input.artifacts; }
};
const core = {
  setOutput(key, value) { result.outputs[key] = value; }, summary,
  notice(message) { result.notices.push(message); },
  warning(message) { result.warnings.push(message); }
};
const context = { repo: { owner: 'apache', repo: 'airflow' } };
const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;
try {
  await new AsyncFunction('github', 'context', 'core', input.script)(github, context, core);
} catch (error) {
  result.error = error.message;
}
console.log(JSON.stringify(result));
"""

NO_INPUTS_NOTICE = "No saved report inputs: this run did not request independent diagnostics"

requires_node = pytest.mark.skipif(shutil.which("node") is None, reason="The plan script runs in node")


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
            "name": "dependency-report-inputs-3.14-constraints-source-providers-true-1",
            "expired": False,
        },
        {"id": 2, "name": "ci-image-save-v3-linux_amd64-3.14-main", "expired": False},
    ]


def load_workflow(path):
    return yaml.safe_load((AIRFLOW_ROOT_PATH / ".github" / path).read_text())


def summary_steps():
    steps = load_workflow("workflows/finalize-tests.yml")["jobs"]["dependency-upgrade-summary"]["steps"]
    return {step["name"]: step for step in steps}


def run_plan(source_run, source_artifacts, *, attempt=1, source_run_id="123", source_attempt="1"):
    workflow = load_workflow("workflows/dependency-upgrade-report.yml")
    script = workflow["jobs"]["plan"]["steps"][0]["with"]["script"]
    result = subprocess.run(
        ["node", "--input-type=module", "--eval", NODE_HARNESS],
        input=json.dumps(
            {
                "script": script,
                "id": source_run_id,
                "sourceAttempt": source_attempt,
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


def included_inputs(result):
    return [entry["inputs-id"] for entry in json.loads(result["outputs"]["matrix"])["include"]]


@requires_node
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
    assert result["requests"][0]["attempt_number"] == 1


@requires_node
@pytest.mark.parametrize("attempt", [1, 2, 3])
def test_plan_posts_the_same_notice_on_every_attempt_when_no_inputs_were_saved(source_run, attempt):
    result = run_plan(source_run, [], attempt=attempt)
    assert "error" not in result
    assert result["outputs"]["has-reports"] == "false"
    assert result["notices"] == [NO_INPUTS_NOTICE]


@requires_node
@pytest.mark.parametrize("attempt", [2, 3])
def test_plan_fails_a_rerun_when_original_inputs_are_gone(source_run, source_artifacts, attempt):
    result = run_plan(source_run, source_artifacts[:1], attempt=attempt)
    assert result["error"] == "Saved report inputs are missing or expired"


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
@requires_node
def test_plan_rejects_sources_outside_completed_main_canaries(source_run, source_artifacts, field, value):
    source_run[field] = value
    result = run_plan(source_run, source_artifacts)
    assert result["error"] == "Only completed main AMD canary runs are supported"


@requires_node
@pytest.mark.parametrize(
    ("missing", "reason"),
    [
        ("image", "CI image missing"),
        ("expired-image", "CI image expired"),
        ("expired-input", "inputs expired"),
    ],
)
def test_plan_does_not_substitute_newer_artifacts(source_run, source_artifacts, missing, reason):
    if missing == "image":
        source_artifacts.pop(1)
    elif missing == "expired-image":
        source_artifacts[1]["expired"] = True
    else:
        source_artifacts[0]["expired"] = True
    source_artifacts += [
        {"id": 3, "name": "dependency-report-inputs-3.13-constraints-true-1", "expired": False},
        {"id": 4, "name": "ci-image-save-v3-linux_amd64-3.13-main", "expired": False},
    ]
    result = run_plan(source_run, source_artifacts)
    skipped = f"3.14:constraints-source-providers ({reason})"
    assert result["warnings"] == [
        f"Skipping 3.14:constraints-source-providers: original inputs or CI image unavailable ({reason})"
    ]
    assert f"Skipped: {skipped}" in result["summary"]
    assert included_inputs(result) == [3]


@requires_node
@pytest.mark.parametrize(("source_attempt", "inputs_ids"), [("1", [1]), ("2", [3])])
def test_plan_explains_inputs_only_for_the_first_canary_attempt_that_saved_them(
    source_run, source_artifacts, source_attempt, inputs_ids
):
    source_artifacts += [
        {
            "id": 5,
            "name": "dependency-report-inputs-3.14-constraints-source-providers-true-2",
            "expired": False,
        },
        {"id": 3, "name": "dependency-report-inputs-3.13-constraints-true-2", "expired": False},
        {"id": 4, "name": "ci-image-save-v3-linux_amd64-3.13-main", "expired": False},
    ]
    result = run_plan(source_run, source_artifacts, source_attempt=source_attempt)
    assert result["requests"][0]["attempt_number"] == int(source_attempt)
    assert included_inputs(result) == inputs_ids
    if source_attempt == "2":
        assert "Explained for an earlier attempt: 3.14:constraints-source-providers" in result["summary"]


@requires_node
def test_plan_does_not_queue_a_second_report_from_inputs_already_explained(source_run, source_artifacts):
    source_artifacts.append(
        {
            "id": 5,
            "name": "dependency-report-inputs-3.14-constraints-source-providers-true-2",
            "expired": False,
        }
    )
    result = run_plan(source_run, source_artifacts, source_attempt="2")
    assert result["outputs"]["has-reports"] == "false"
    assert result["notices"] == [
        "The saved report inputs were already explained for an earlier canary attempt"
    ]


def stash_artifact_name(key, ref):
    """The artifact name apache/infrastructure-actions/stash/save gives a stash (its shared/mung.py)."""
    return "-".join(re.sub(r"[^_\-.\w]", "_", part) for part in (key, ref))


@requires_node
def test_plan_finds_the_image_under_the_name_the_canary_stashes_it(source_run, source_artifacts):
    steps = load_workflow("workflows/ci-image-build.yml")["jobs"]["build-ci-images"]["steps"]
    keys = [
        step["with"]["key"]
        for step in steps
        if step.get("uses", "").startswith("apache/infrastructure-actions/stash/save@")
        and step["with"]["key"].startswith("ci-image-save-")
        and "image-stash-ref" not in step["with"]["key"]
    ]
    assert keys == ["ci-image-save-v3-${{ inputs.platform }}-${{ env.PYTHON_MAJOR_MINOR_VERSION }}"]
    key = keys[0].replace("${{ inputs.platform }}", "linux/amd64")
    key = key.replace("${{ env.PYTHON_MAJOR_MINOR_VERSION }}", "3.14")
    source_artifacts[1]["name"] = stash_artifact_name(key, "main")
    assert included_inputs(run_plan(source_run, source_artifacts)) == [1]


def test_each_canary_attempt_triggers_its_own_report():
    workflow = load_workflow("workflows/dependency-upgrade-report.yml")
    assert workflow["concurrency"]["group"].endswith("-${{ github.event.workflow_run.run_attempt }}")
    assert workflow["concurrency"]["cancel-in-progress"] is False
    plan_env = workflow["jobs"]["plan"]["steps"][0]["env"]
    assert plan_env["SOURCE_RUN_ATTEMPT"] == "${{ github.event.workflow_run.run_attempt }}"
    upload = summary_steps()["Upload dependency report inputs"]
    assert upload["with"]["name"].endswith("-${{ github.run_attempt }}")


def test_explain_prepares_the_disk_like_the_image_build_jobs():
    build_steps = load_workflow("workflows/ci-image-build.yml")["jobs"]["build-ci-images"]["steps"]
    steps = load_workflow("workflows/dependency-upgrade-report.yml")["jobs"]["explain"]["steps"]
    names = [step["name"] for step in steps]
    disk_steps = ["Free up disk space", "Make /mnt writeable", "Move docker to /mnt"]
    assert names[1:5] == [*disk_steps, "Install Breeze"]
    assert names.index("Install Breeze") < names.index("Download the original CI image")
    build_runs = {step["name"]: step.get("run") for step in build_steps}
    for name in disk_steps:
        assert steps[names.index(name)]["run"] == build_runs[name]


@pytest.mark.parametrize(
    ("independent_report", "report_args"),
    [
        ("true", ["--no-explain-why", "--save-constraints-to", "files/dependency-report"]),
        ("false", ["--explain-why"]),
    ],
)
def test_canary_saves_report_inputs_through_breeze(tmp_path, independent_report, report_args):
    steps = summary_steps()
    assert not any("gh api" in step.get("run", "") for step in steps.values())
    upload = steps["Upload dependency report inputs"]
    assert "hashFiles('files/dependency-report/constraints-commit.txt')" in upload["if"]
    assert upload["continue-on-error"] is True
    deps = next(step for name, step in steps.items() if name.startswith("Deps: "))
    (tmp_path / "breeze").write_text('#!/usr/bin/env bash\nprintf "%s\\n" "$@"\n')
    (tmp_path / "breeze").chmod(0o755)
    result = subprocess.run(
        ["bash", "--noprofile", "--norc", "-eo", "pipefail", "-c", deps["run"]],
        env={
            **os.environ,
            "PATH": f"{tmp_path}{os.pathsep}{os.environ['PATH']}",
            "INDEPENDENT_REPORT": independent_report,
            "MATRIX_PYTHON_VERSION": "3.14",
            "MATRIX_CONSTRAINTS_MODE": "constraints",
        },
        capture_output=True,
        text=True,
        check=True,
        timeout=20,
    )
    assert result.stdout.splitlines() == [
        "release-management",
        "constraints-version-check",
        "--python",
        "3.14",
        "--airflow-constraints-mode",
        "constraints",
        *report_args,
    ]


@requires_node
@pytest.mark.parametrize("source_run_id", ["0", "abc", "123\ninjected", "9007199254740992"])
def test_plan_rejects_invalid_source_ids_before_calling_github(source_run, source_artifacts, source_run_id):
    result = run_plan(source_run, source_artifacts, source_run_id=source_run_id)
    assert result["error"] == "Invalid source run ID"
    assert result["requests"] == []


@requires_node
@pytest.mark.parametrize("source_attempt", ["0", "abc", "1\ninjected", "12345"])
def test_plan_rejects_invalid_source_attempts_before_calling_github(
    source_run, source_artifacts, source_attempt
):
    result = run_plan(source_run, source_artifacts, source_attempt=source_attempt)
    assert result["error"] == "Invalid source run attempt"
    assert result["requests"] == []
