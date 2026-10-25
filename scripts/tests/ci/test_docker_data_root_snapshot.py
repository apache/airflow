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

import hashlib
import os
import shlex
import subprocess
from pathlib import Path

import pytest
import yaml

SCRIPT = Path(__file__).resolve().parents[2] / "ci" / "docker_data_root_snapshot.sh"
WORKFLOW = SCRIPT.parents[2] / ".github" / "workflows" / "ci-image-build.yml"
ACTIONS = SCRIPT.parents[2] / ".github" / "actions"
LOADER = ACTIONS / "load_image" / "action.yml"
FINGERPRINT = "28.5.2 overlay2 x86_64 /var/lib/docker"
STOP = ["sudo", "timeout", "60", "systemctl", "stop", "docker.socket", "docker"]
START = ["sudo", "timeout", "60", "systemctl", "start", "docker"]
REMOVE_STORE = ["sudo", "rm", "-rf", "/var/lib/docker/image", "/var/lib/docker/overlay2"]
TREE = hashlib.sha256(b"/ d 755\n/tmp d 1777\n").hexdigest()


@pytest.fixture
def fake_tools(tmp_path):
    """Shadow ``docker``, ``sudo``, ``zstd`` and ``git`` with a logger answering the script's queries."""
    tools = tmp_path / "bin"
    tools.mkdir()
    command = tools / "command"
    command.write_text(
        r"""#!/usr/bin/env bash
name="${0##*/}"
printf '%s\n' "${name} $*" >> "${COMMAND_LOG}"
if [[ "${name}" == "git" && "$*" == "rev-parse HEAD" ]]; then
    echo "${CHECKOUT_SHA:-revision1}"
fi
if [[ "${name}" == "zstd" ]]; then
    if [[ -n "${ZSTD_SLEEP:-}" ]]; then
        exec sleep "${ZSTD_SLEEP}"
    fi
    exit "${ZSTD_EXIT:-0}"
fi
if [[ "${name}" == "sudo" && ("$*" == "tar "* || "$*" == "timeout "*" tar "*) ]]; then
    exit "${TAR_EXIT:-0}"
fi
if [[ "${name}" == "sudo" && "$*" == *"systemctl stop docker.socket docker" && "${STOP_EXIT:-0}" != 0 ]]; then
    exit "${STOP_EXIT}"
fi
if [[ "${name}" == "sudo" && "$*" == *"systemctl start docker" && "${START_FAIL_ONCE:-0}" == 1 ]]; then
    if [[ $(grep -c 'systemctl start docker' "${COMMAND_LOG}") == 1 ]]; then
        exit 1
    fi
fi
if [[ "${name}" == "docker" ]]; then
    case "$*" in
        "info --format "*) echo "${DAEMON_FINGERPRINT}" ;;
        "images --quiet --filter label=org.apache.airflow.image=airflow-ci") printf '%b' "${CI_IMAGES:-}" ;;
        "images --quiet") printf '%b' "${ALL_IMAGES:-}" ;;
        "images --all --quiet") printf '%b' "${ALL_IMAGES:-}"; exit "${IMAGES_EXIT:-0}" ;;
        "ps --all --quiet") printf '%b' "${CONTAINERS:-}"; exit "${CONTAINERS_EXIT:-0}" ;;
        "image inspect --format {{.Id}} "*) echo "${IMAGE_DIGEST:-sha256:abc123}" ;;
        "run "*)
            if [[ "${RUN_EXIT:-0}" != 0 ]]; then
                exit "${RUN_EXIT}"
            fi
            printf '%b' "${TREE_LISTING:-/tmp d 1777\\n/ d 755\\n}"
            ;;
    esac
fi
"""
    )
    command.chmod(0o755)
    for name in ("docker", "sudo", "zstd", "git"):
        (tools / name).symlink_to(command)
    return {
        "PATH": f"{tools}:{os.environ['PATH']}",
        "COMMAND_LOG": str(tmp_path / "commands.log"),
        "DAEMON_FINGERPRINT": FINGERPRINT,
    }


@pytest.fixture
def snapshot(tmp_path):
    snapshot_file = tmp_path / "ci-image-snapshot-linux_amd64-3.10.tar.zst"
    snapshot_file.write_bytes(b"")
    Path(f"{snapshot_file}.meta").write_text(f"{FINGERPRINT} sha256:abc123 revision1 {TREE}\n")
    return snapshot_file


def run_script(env, *args):
    command = ["bash", "--noprofile", "--norc", str(SCRIPT), *args]
    result = subprocess.run(
        command,
        env={**os.environ, **env},
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    if args[0] == "restore":
        assert not Path(args[1]).exists()
        assert not Path(f"{args[1]}.meta").exists()
    return result


def read_commands(env):
    log = Path(env["COMMAND_LOG"])
    return [shlex.split(line) for line in log.read_text().splitlines()] if log.exists() else []


def is_tar(command, mode):
    return command[0] == "sudo" and "tar" in command and mode in command


def steps(path, *keys):
    document = yaml.safe_load(path.read_text())
    for key in keys:
        document = document[key]
    return document


def test_restore_unpacks_snapshot_into_stopped_daemon(fake_tools, snapshot):
    result = run_script(fake_tools, "restore", str(snapshot))

    assert result.returncode == 0, result.stdout + result.stderr
    commands = read_commands(fake_tools)
    stop = commands.index(STOP)
    remove = commands.index(REMOVE_STORE)
    extract = next(i for i, c in enumerate(commands) if is_tar(c, "--extract"))
    start = commands.index(START)
    inspect = commands.index(["docker", "image", "inspect", "--format", "{{.Id}}", "sha256:abc123"])
    run = next(i for i, c in enumerate(commands) if c[:2] == ["docker", "run"])
    assert stop < remove < extract < start < inspect < run
    assert commands[run][:9] == [
        "docker",
        "run",
        "--rm",
        "--pull",
        "never",
        "--network",
        "none",
        "--entrypoint",
        "find",
    ]
    assert commands[run][9:12] == ["sha256:abc123", "/", "-xdev"]
    assert commands[extract][:3] == ["sudo", "timeout", "300"]
    assert "--xattrs-include=*" in commands[extract]
    assert not snapshot.exists()
    assert not Path(f"{snapshot}.meta").exists()


@pytest.mark.parametrize(
    ("env", "remove_meta", "expected_exit"),
    [
        pytest.param({}, True, 2, id="no-snapshot"),
        pytest.param(
            {"DAEMON_FINGERPRINT": "29.0.0 overlay2 x86_64 /var/lib/docker"}, False, 3, id="other-daemon"
        ),
        pytest.param({"ALL_IMAGES": "def456\\n"}, False, 3, id="daemon-holds-images"),
    ],
)
def test_restore_leaves_daemon_alone_when_snapshot_does_not_fit(
    fake_tools, snapshot, env, remove_meta, expected_exit
):
    if remove_meta:
        Path(f"{snapshot}.meta").unlink()

    result = run_script({**fake_tools, **env}, "restore", str(snapshot))

    assert result.returncode == expected_exit
    if "DAEMON_FINGERPRINT" in env:
        assert "::warning::" in result.stdout
    assert not [c for c in read_commands(fake_tools) if "systemctl" in c]


@pytest.mark.parametrize(
    "env",
    [{"RUN_EXIT": "1"}, {"TREE_LISTING": "/ d 755\\n/data d 755\\n"}, {"IMAGE_DIGEST": "sha256:other"}],
    ids=["does-not-run", "other-file-tree", "other-image"],
)
def test_restore_empties_store_again_when_image_does_not_match(fake_tools, snapshot, env):
    result = run_script({**fake_tools, **env}, "restore", str(snapshot))

    assert result.returncode == 4
    commands = read_commands(fake_tools)
    start = commands.index(START)
    after_start = commands[start + 1 :]
    assert REMOVE_STORE in after_start
    assert after_start[-1] == START


def test_create_keeps_only_the_ci_image(fake_tools, tmp_path):
    snapshot_file = tmp_path / "out.tar.zst"
    env = {**fake_tools, "CI_IMAGES": "abc123\\nabc123\\n", "ALL_IMAGES": "abc123\\nother1\\nabc123\\n"}

    result = run_script(env, "create", str(snapshot_file))

    assert result.returncode == 0, result.stdout + result.stderr
    commands = read_commands(env)
    assert ["docker", "rmi", "--force", "other1"] in commands
    assert not [c for c in commands if c[:2] == ["docker", "rmi"] and "abc123" in c]
    assert ["docker", "builder", "prune", "--all", "--force"] in commands
    assert Path(f"{snapshot_file}.meta").read_text() == f"{FINGERPRINT} sha256:abc123 revision1 {TREE}\n"
    fingerprint = next(i for i, c in enumerate(commands) if c[:2] == ["docker", "run"])
    assert fingerprint < commands.index(STOP)
    archive = next(c for c in commands if is_tar(c, "--create"))
    assert archive[-2:] == ["image", "overlay2"]
    assert "--xattrs-include=*" in archive


def test_create_refuses_ambiguous_ci_image(fake_tools, tmp_path):
    env = {**fake_tools, "CI_IMAGES": "abc123\\ndef456\\n"}

    result = run_script(env, "create", str(tmp_path / "out.tar.zst"))

    assert result.returncode == 1
    assert not [c for c in read_commands(env) if "systemctl" in c]


@pytest.mark.parametrize(
    "failure",
    [
        {"ZSTD_EXIT": "1"},
        {"TAR_EXIT": "1"},
        {"START_FAIL_ONCE": "1"},
        {"ZSTD_SLEEP": "10", "SNAPSHOT_RESTORE_TIMEOUT_SECONDS": "1"},
    ],
    ids=["zstd", "tar", "start", "stalled"],
)
def test_restore_recovers_empty_running_daemon_after_materialization_failure(fake_tools, snapshot, failure):
    result = run_script({**fake_tools, **failure}, "restore", str(snapshot))
    assert result.returncode != 0
    commands = read_commands(fake_tools)
    assert commands[-2:] == [
        REMOVE_STORE,
        START,
    ]
    assert not any(command[:2] == ["docker", "run"] for command in commands)


@pytest.mark.parametrize("failure", [{"ZSTD_EXIT": "1"}, {"TAR_EXIT": "1"}])
def test_create_restarts_daemon_after_archive_failure(fake_tools, tmp_path, failure):
    snapshot_file = tmp_path / "snapshot.tar.zst"
    result = run_script({**fake_tools, **failure, "CI_IMAGES": "abc123"}, "create", str(snapshot_file))
    assert result.returncode != 0
    assert read_commands(fake_tools)[-1] == START
    assert Path(f"{snapshot_file}.meta").read_text() == "unavailable\n"


def test_failed_create_retires_earlier_snapshot(fake_tools, tmp_path):
    snapshot_file = tmp_path / "snapshot.tar.zst"
    snapshot_file.write_bytes(b"earlier attempt")
    Path(f"{snapshot_file}.meta").write_text(f"{FINGERPRINT} sha256:abc123 revision1 {TREE}\n")

    result = run_script({**fake_tools, "CI_IMAGES": ""}, "create", str(snapshot_file))

    assert result.returncode == 1
    assert not snapshot_file.exists()
    assert Path(f"{snapshot_file}.meta").read_text() == "unavailable\n"


def test_check_accepts_fitting_snapshot_before_download(fake_tools, snapshot):
    snapshot.unlink()

    result = run_script(fake_tools, "check", str(snapshot))

    assert result.returncode == 0, result.stdout + result.stderr
    assert Path(f"{snapshot}.meta").exists()
    assert not any(command[0] == "sudo" for command in read_commands(fake_tools))


@pytest.mark.parametrize(
    "env",
    [
        {"DAEMON_FINGERPRINT": "29.0.0 overlay2 x86_64 /var/lib/docker"},
        {"ALL_IMAGES": "def456\\n"},
        {"CHECKOUT_SHA": "revision2"},
    ],
    ids=["other-daemon", "daemon-holds-images", "other-revision"],
)
def test_check_rejects_snapshot_that_does_not_fit(fake_tools, snapshot, env):
    result = run_script({**fake_tools, **env}, "check", str(snapshot))

    assert result.returncode == 3
    assert not any(command[0] == "sudo" for command in read_commands(fake_tools))


def test_check_rejects_unavailable_marker(fake_tools, snapshot):
    Path(f"{snapshot}.meta").write_text("unavailable\n")

    result = run_script(fake_tools, "check", str(snapshot))

    assert result.returncode == 3
    assert not any(command[0] == "sudo" for command in read_commands(fake_tools))


@pytest.mark.parametrize("mode", ["create", "restore"])
@pytest.mark.parametrize("daemon", ["28.5.2 btrfs x86_64 /var/lib/docker", "28.5.2 overlay2 x86_64 /other"])
def test_unsupported_storage_does_not_modify_daemon(fake_tools, snapshot, mode, daemon):
    result = run_script({**fake_tools, "DAEMON_FINGERPRINT": daemon}, mode, str(snapshot))
    assert result.returncode == 3
    assert "::warning::" in result.stdout
    commands = read_commands(fake_tools)
    assert not any(command[0] == "sudo" for command in commands)
    assert not any(command[:2] in (["docker", "rmi"], ["docker", "builder"]) for command in commands)
    if mode == "create":
        assert Path(f"{snapshot}.meta").read_text() == "unavailable\n"


def test_existing_container_prevents_restore(fake_tools, snapshot):
    result = run_script({**fake_tools, "CONTAINERS": "container1"}, "restore", str(snapshot))
    assert result.returncode == 3
    assert not any(command[0] == "sudo" for command in read_commands(fake_tools))


@pytest.mark.parametrize("failure", [{"IMAGES_EXIT": "1"}, {"CONTAINERS_EXIT": "1"}])
def test_failed_daemon_inventory_does_not_modify_store(fake_tools, snapshot, failure):
    result = run_script({**fake_tools, **failure}, "restore", str(snapshot))

    assert result.returncode != 0
    assert not any(command[0] == "sudo" for command in read_commands(fake_tools))


def test_malformed_metadata_prevents_restore(fake_tools, snapshot):
    Path(f"{snapshot}.meta").write_text("invalid\n")
    result = run_script(fake_tools, "restore", str(snapshot))
    assert result.returncode == 3
    assert not any(command[0] == "sudo" for command in read_commands(fake_tools))


def test_stale_snapshot_does_not_modify_daemon(fake_tools, snapshot):
    result = run_script({**fake_tools, "CHECKOUT_SHA": "revision2"}, "restore", str(snapshot))
    assert result.returncode == 3
    assert not any(command[0] == "sudo" for command in read_commands(fake_tools))


@pytest.mark.parametrize("mode", ["create", "restore"])
def test_partial_stop_failure_restarts_daemon(fake_tools, snapshot, mode):
    result = run_script({**fake_tools, "STOP_EXIT": "1", "CI_IMAGES": "abc123"}, mode, str(snapshot))
    assert result.returncode != 0
    assert read_commands(fake_tools)[-1] == START
    assert not any(command[:2] == ["sudo", "rm"] for command in read_commands(fake_tools))


def test_snapshot_creation_uses_builder_after_cache_publication():
    workflow = yaml.safe_load(WORKFLOW.read_text())
    steps = workflow["jobs"]["build-ci-images"]["steps"]
    snapshot = next(i for i, step in enumerate(steps) if step.get("id") == "snapshot-export")
    cache_publishers = [
        i
        for i, step in enumerate(steps)
        if "/stash/save@" in step.get("uses", "") or step.get("uses", "").endswith("/stash-save")
    ]
    assert cache_publishers
    assert all(publisher < snapshot for publisher in cache_publishers)


def test_snapshot_artifacts_replace_earlier_attempts():
    uploads = [
        step
        for step in steps(WORKFLOW, "jobs", "build-ci-images", "steps")
        if "/upload-artifact@" in step.get("uses", "") and step["name"].startswith("Upload CI image snapshot")
    ]
    assert len(uploads) == 2
    assert all(upload["with"]["overwrite"] is True for upload in uploads)
    archive, metadata = uploads
    assert archive["if"] == "steps.snapshot-export.outcome == 'success'"
    assert metadata["if"] == "steps.snapshot-export.outcome != 'skipped'"
    assert metadata["with"]["path"].endswith(".tar.zst.meta")


def test_snapshot_artifact_names_match_between_upload_and_restore():
    uploads = [
        step["with"]["name"].replace("env.PYTHON_MAJOR_MINOR_VERSION", "inputs.python")
        for step in steps(WORKFLOW, "jobs", "build-ci-images", "steps")
        if "/upload-artifact@" in step.get("uses", "") and step["name"].startswith("Upload CI image snapshot")
    ]
    loader = {step.get("id"): step for step in steps(LOADER, "runs", "steps")}
    assert uploads == [
        loader["snapshot-download"]["with"]["name"],
        loader["snapshot-meta"]["with"]["pattern"],
    ]
    assert uploads[0].startswith("ci-image-snapshot-v1-")


def test_loader_checks_snapshot_before_downloading_it():
    loader = steps(LOADER, "runs", "steps")
    order = [step.get("id") for step in loader]
    assert order[:4] == ["snapshot-meta", "snapshot-check", "snapshot-download", "snapshot"]
    by_id = {step.get("id"): step for step in loader}
    assert by_id["snapshot-download"]["if"] == "steps.snapshot-check.outputs.fits == 'true'"
    assert " check " in by_id["snapshot-check"]["run"]
    assert "name" not in by_id["snapshot-meta"]["with"]
    assert by_id["snapshot-meta"]["with"]["merge-multiple"] is True
    assert "breeze ci-image load" in by_id["snapshot"]["run"]
    stash = [step for step in loader if "stash" in step.get("uses", "")]
    assert [step["uses"] for step in stash] == ["./.github/actions/stash-restore"]
    assert stash[0]["if"] == "steps.snapshot.outputs.restored != 'true'"
    assert "--snapshot-file" in by_id["snapshot"]["run"]


def test_snapshot_is_created_and_restored_through_breeze():
    export = next(
        step
        for step in steps(WORKFLOW, "jobs", "build-ci-images", "steps")
        if step.get("id") == "snapshot-export"
    )
    restore = next(step for step in steps(LOADER, "runs", "steps") if step.get("id") == "snapshot")
    assert "breeze ci-image save" in export["run"] and "--snapshot-file" in export["run"]
    assert "breeze ci-image load" in restore["run"] and "--snapshot-file" in restore["run"]


def test_snapshot_is_limited_to_github_hosted_runners():
    export = next(
        step
        for step in steps(WORKFLOW, "jobs", "build-ci-images", "steps")
        if step.get("id") == "snapshot-export"
    )
    metadata = next(step for step in steps(LOADER, "runs", "steps") if step.get("id") == "snapshot-meta")
    assert "runner.environment == 'github-hosted'" in export["if"]
    assert "runner.environment == 'github-hosted'" in metadata["if"]


@pytest.mark.parametrize("action", ["prepare_breeze_and_image", "prepare_single_ci_image"])
def test_image_preparing_actions_share_the_loader(action):
    action_steps = steps(ACTIONS / action / "action.yml", "runs", "steps")
    assert [step["uses"] for step in action_steps if "load_image" in step.get("uses", "")] == [
        "./.github/actions/load_image"
    ]
    assert not [step for step in action_steps if "image load" in step.get("run", "")]
    assert not [step for step in action_steps if "stash" in step.get("uses", "")]
