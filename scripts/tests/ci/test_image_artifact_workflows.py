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
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[3]
RESOLVE_ACTION = ROOT / ".github/actions/resolve_main_image/action.yml"


class TestResolveMainImageAction:
    @pytest.mark.parametrize("kind", ["ci", "prod"])
    def test_legacy_builds_do_not_require_image_resolver(self, kind: str) -> None:
        workflow = yaml.safe_load((ROOT / f".github/workflows/{kind}-image-build.yml").read_text())
        trigger = workflow.get("on", workflow.get(True))
        inputs = trigger["workflow_call"]["inputs"]
        assert inputs["use-selected-image"]["default"] is False
        assert inputs["publish-main-image"]["default"] is False
        resolvers = [
            step
            for step in workflow["jobs"][f"build-{kind}-images"]["steps"]
            if step.get("uses") == "./.github/actions/resolve_main_image"
        ]
        assert resolvers
        for step in resolvers:
            assert "inputs.use-selected-image || inputs.publish-main-image" in step["if"]

    @pytest.mark.parametrize(
        ("disabled", "publish", "docker_script", "returncode", "miss"),
        [
            pytest.param("true", "false", "exit 99", 0, True, id="disabled-skips-registry"),
            pytest.param("false", "false", "exit 1", 0, True, id="registry-failure-builds-normally"),
            pytest.param("true", "true", "exit 1", 1, False, id="publisher-requires-base-identity"),
            pytest.param("false", "false", "echo invalid", 0, True, id="invalid-digest-builds-normally"),
        ],
    )
    def test_resolver_failure_modes(
        self, tmp_path: Path, disabled: str, publish: str, docker_script: str, returncode: int, miss: bool
    ) -> None:
        docker = tmp_path / "docker"
        docker.write_text("#!/bin/bash\n" + docker_script + "\n")
        docker.chmod(0o755)
        action = yaml.safe_load(RESOLVE_ACTION.read_text())
        output = tmp_path / "output"
        result = subprocess.run(
            [
                "bash",
                "--noprofile",
                "--norc",
                "-e",
                "-o",
                "pipefail",
                "-c",
                action["runs"]["steps"][0]["run"],
            ],
            cwd=ROOT,
            env={
                "PATH": f"{tmp_path}:/usr/bin:/bin",
                "RUNNER_TEMP": str(tmp_path),
                "GITHUB_OUTPUT": str(output),
                "GITHUB_RUN_ATTEMPT": "2",
                "IMAGE_KIND": "ci",
                "IMAGE_PYTHON": "3.12",
                "IMAGE_PLATFORM": "linux/amd64",
                "IMAGE_REUSE_DISABLED": disabled,
                "IMAGE_PUBLISH": publish,
                "IMAGE_CONSTRAINTS_FILE": "",
            },
            text=True,
            capture_output=True,
            check=False,
        )
        assert result.returncode == returncode, result.stderr
        values = dict(line.split("=", 1) for line in output.read_text().splitlines())
        assert values["selection-name"] == "selected-ci-3.12-amd64-2"
        assert values["built-image-name"] == "built-ci-3.12-amd64-2"
        assert (values.get("hit") == "false") is miss
        assert "base-image" not in values

    @pytest.mark.parametrize("kind", ["ci", "prod"])
    def test_selected_image_hit_bypasses_build_and_export(self, kind: str) -> None:
        workflow = yaml.safe_load((ROOT / f".github/workflows/{kind}-image-build.yml").read_text())
        steps = workflow["jobs"][f"build-{kind}-images"]["steps"]
        expensive_steps = [
            step
            for step in steps
            if "run" in step
            and (f"breeze {kind}-image build" in step["run"] or f"breeze {kind}-image save" in step["run"])
        ]
        assert len(expensive_steps) == 2
        assert all("steps.main-image.outputs.hit != 'true'" in step["if"] for step in expensive_steps)
        local_upload = next(step for step in steps if step.get("id") == "local-image")
        assert local_upload["with"]["name"] == "${{ steps.main-image.outputs.built-image-name }}"
        selection_step = next(step for step in steps if "select-local" in step.get("run", ""))
        assert steps.index(selection_step) > steps.index(local_upload)

    @pytest.mark.parametrize("exists", ["true", "false"])
    def test_publisher_retains_existing_immutable_artifact(self, tmp_path: Path, exists: str) -> None:
        digest = "sha256:" + "a" * 64
        docker = tmp_path / "docker"
        docker.write_text(f"#!/bin/bash\necho {digest}\n")
        docker.chmod(0o755)
        uv = tmp_path / "uv"
        uv.write_text(
            """#!/bin/bash
command="$*"
while [[ $# -gt 0 ]]; do
  if [[ "$1" == "--output" ]]; then
    shift
    output="$1"
  fi
  shift
done
case "$command" in
  *publication-exists*) echo "{\\"exists\\": ${EXISTING_ARTIFACT}}" > "$output" ;;
  *" fingerprint "*) echo '{"artifact-name": "main-image-ci-3.12-amd64-fingerprint"}' > "$output" ;;
  *" resolve "*) echo '{"hit": false}' > "$output" ;;
esac
"""
        )
        uv.chmod(0o755)
        output = tmp_path / "output"
        action = yaml.safe_load(RESOLVE_ACTION.read_text())
        result = subprocess.run(
            ["bash", "-e", "-o", "pipefail", "-c", action["runs"]["steps"][0]["run"]],
            cwd=ROOT,
            env={
                "PATH": f"{tmp_path}:/usr/bin:/bin",
                "RUNNER_TEMP": str(tmp_path),
                "GITHUB_OUTPUT": str(output),
                "GITHUB_RUN_ATTEMPT": "2",
                "GITHUB_RUN_ID": "123",
                "GITHUB_REPOSITORY": "apache/airflow",
                "IMAGE_KIND": "ci",
                "IMAGE_PYTHON": "3.12",
                "IMAGE_PLATFORM": "linux/amd64",
                "IMAGE_REUSE_DISABLED": "true",
                "IMAGE_PUBLISH": "true",
                "IMAGE_CONSTRAINTS_FILE": "",
                "EXISTING_ARTIFACT": exists,
            },
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stderr
        values = dict(line.split("=", 1) for line in output.read_text().splitlines())
        assert values["publication-exists"] == exists
        assert values["base-image"] == f"debian@{digest}"
        assert values["hit"] == "false"

    @pytest.mark.parametrize("architecture", ["amd", "arm"])
    @pytest.mark.parametrize("kind", ["ci", "prod"])
    def test_shared_images_require_upstream_repository_context(self, architecture: str, kind: str) -> None:
        workflow = yaml.safe_load((ROOT / f".github/workflows/ci-{architecture}.yml").read_text())
        inputs = workflow["jobs"][f"build-{kind}-images"]["with"]
        assert inputs["reuse-main-image"] == (
            "${{ github.repository == 'apache/airflow' && "
            "needs.build-info.outputs.image-reuse-eligible == 'true' }}"
        )
        assert inputs["use-selected-image"] is True

    @pytest.mark.parametrize(
        ("workflow_name", "producer_job", "artifact_name"),
        [
            (
                "generate-constraints",
                "generate-constraints-matrix",
                "constraints-${{ matrix.python-version }}",
            ),
            ("prod-image-build", "build-prod-packages", "prod-packages"),
        ],
    )
    def test_publisher_intermediates_can_be_replaced_on_partial_reruns(
        self, workflow_name: str, producer_job: str, artifact_name: str
    ) -> None:
        workflow = yaml.safe_load((ROOT / f".github/workflows/{workflow_name}.yml").read_text())
        trigger = workflow.get("on", workflow.get(True))
        assert trigger["workflow_call"]["inputs"]["artifact-prefix"]["default"] == ""
        uploads = [
            step
            for step in workflow["jobs"][producer_job]["steps"]
            if step.get("uses", "").startswith("actions/upload-artifact@")
        ]
        upload = next(step for step in uploads if step["with"]["name"].endswith(artifact_name))
        assert upload["with"]["overwrite"] == "${{ inputs.artifact-prefix != '' }}"
        prod = yaml.safe_load((ROOT / ".github/workflows/prod-image-build.yml").read_text())
        downloads = [
            step
            for step in prod["jobs"]["build-prod-images"]["steps"]
            if step.get("uses", "").startswith("actions/download-artifact@")
        ]
        assert any(step["with"]["name"] == upload["with"]["name"] for step in downloads)

    @pytest.mark.parametrize("kind", ["ci", "prod"])
    def test_publisher_image_artifacts_remain_immutable(self, kind: str) -> None:
        workflow = yaml.safe_load((ROOT / f".github/workflows/{kind}-image-build.yml").read_text())
        uploads = [
            step
            for step in workflow["jobs"][f"build-{kind}-images"]["steps"]
            if step.get("uses", "").startswith("actions/upload-artifact@")
        ]
        assert all("overwrite" not in step["with"] for step in uploads)


class TestRestoreSelectedImageAction:
    @pytest.mark.parametrize(
        ("kind", "scope", "returncode", "mount_sources"),
        [
            pytest.param("ci", "main", 0, True, id="main-ci-mounts-checkout"),
            pytest.param("ci", "current-run", 0, False, id="local-ci-keeps-mounts"),
            pytest.param("prod", "main", 0, False, id="prod-keeps-mounts"),
            pytest.param("ci", "main", 1, False, id="failed-download-stops-consumer"),
        ],
    )
    def test_restore_selection(
        self, tmp_path: Path, kind: str, scope: str, returncode: int, mount_sources: bool
    ) -> None:
        uv = tmp_path / "uv"
        uv.write_text(f'#!/bin/bash\nprintf "%s\\n" "$@" > "${{RUNNER_TEMP}}/arguments"\nexit {returncode}\n')
        uv.chmod(0o755)
        (tmp_path / "selected-image.json").write_text(json.dumps({"hit": True, "scope": scope}))
        output = tmp_path / "output"
        environment = tmp_path / "environment"
        output.touch()
        environment.touch()
        action = yaml.safe_load((ROOT / ".github/actions/prepare_breeze_and_image/action.yml").read_text())
        step = next(step for step in action["runs"]["steps"] if step.get("id") == "selected-image")
        result = subprocess.run(
            ["bash", "-e", "-o", "pipefail", "-c", step["run"]],
            cwd=ROOT,
            env={
                "PATH": f"{tmp_path}:/usr/bin:/bin",
                "RUNNER_TEMP": str(tmp_path),
                "GITHUB_OUTPUT": str(output),
                "GITHUB_ENV": str(environment),
                "GITHUB_REPOSITORY": "apache/airflow",
                "GITHUB_RUN_ID": "123",
                "GITHUB_RUN_ATTEMPT": "2",
                "IMAGE_KIND": kind,
                "IMAGE_PYTHON": "3.12",
                "IMAGE_PLATFORM": "linux/amd64",
            },
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == returncode, result.stderr
        assert output.read_text() == ("hit=true\n" if returncode == 0 else "")
        assert environment.read_text() == ("MOUNT_SOURCES=selected\n" if mount_sources else "")
        arguments = (tmp_path / "arguments").read_text().splitlines()
        assert arguments == [
            "run",
            "--project",
            "dev/breeze",
            "python",
            "-m",
            "airflow_breeze.utils.image_artifacts",
            "restore-selection",
            "--kind",
            kind,
            "--python",
            "3.12",
            "--platform",
            "linux/amd64",
            "--repository",
            "apache/airflow",
            "--run-id",
            "123",
            "--run-attempt",
            "2",
            "--output",
            str(tmp_path / "selected-image.json"),
            "--output-directory",
            "/mnt",
            "--require-selection",
        ]


class TestProductionDependencyCache:
    @pytest.mark.parametrize("restored", ["true", "false"])
    @pytest.mark.parametrize("publish", ["true", "false"])
    def test_configure_cache(self, tmp_path: Path, restored: str, publish: str) -> None:
        workflow = yaml.safe_load((ROOT / ".github/workflows/prod-image-build.yml").read_text())
        step = next(
            step
            for step in workflow["jobs"]["build-prod-images"]["steps"]
            if step["name"] == "Configure production dependency cache"
        )
        environment = tmp_path / "environment"
        subprocess.run(
            ["bash", "-e", "-o", "pipefail", "-c", step["run"]],
            cwd=ROOT,
            env={"GITHUB_ENV": str(environment), "CACHE_RESTORED": restored, "PUBLISH_CACHE": publish},
            check=True,
        )
        expected = {"PROD_IMAGE_DEPENDENCY_CACHE": "true"}
        if restored == "true":
            expected["PROD_IMAGE_BUILD_CACHE_FROM"] = "/mnt/prod-dependencies-cache-in"
        if publish == "true":
            expected["PROD_IMAGE_BUILD_CACHE_TO"] = "/mnt/prod-dependencies-cache-out"
        assert dict(line.split("=", 1) for line in environment.read_text().splitlines()) == expected
