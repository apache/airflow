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

import os
import shlex
import subprocess
from pathlib import Path

import pytest
import yaml

SCRIPT = Path(__file__).resolve().parents[2] / "ci" / "docker_data_root_snapshot.sh"
WORKFLOW = SCRIPT.parents[2] / ".github" / "workflows" / "ci-image-build.yml"
ACTION = SCRIPT.parents[2] / ".github" / "actions" / "prepare_breeze_and_image" / "action.yml"
FINGERPRINT = "28.5.2 overlay2 x86_64 /var/lib/docker"


@pytest.fixture
def fake_tools(tmp_path):
    """Shadow ``docker``, ``sudo`` and ``zstd`` with a logger answering the queries the script makes."""
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
    exit "${ZSTD_EXIT:-0}"
fi
if [[ "${name}" == "sudo" && "$*" == "tar "* ]]; then
    exit "${TAR_EXIT:-0}"
fi
if [[ "${name}" == "sudo" && "$*" == "systemctl stop docker.socket docker" && "${STOP_EXIT:-0}" != 0 ]]; then
    exit "${STOP_EXIT}"
fi
if [[ "${name}" == "sudo" && "$*" == "systemctl start docker" && "${START_FAIL_ONCE:-0}" == 1 ]]; then
    if [[ $(grep -c 'sudo systemctl start docker' "${COMMAND_LOG}") == 1 ]]; then
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
        "run "*) exit "${RUN_EXIT:-0}" ;;
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
    Path(f"{snapshot_file}.meta").write_text(f"{FINGERPRINT} abc123 revision1\n")
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


def test_restore_unpacks_snapshot_into_stopped_daemon(fake_tools, snapshot):
    result = run_script(fake_tools, "restore", str(snapshot))

    assert result.returncode == 0, result.stdout + result.stderr
    commands = read_commands(fake_tools)
    stop = commands.index(["sudo", "systemctl", "stop", "docker.socket", "docker"])
    remove = commands.index(["sudo", "rm", "-rf", "/var/lib/docker/image", "/var/lib/docker/overlay2"])
    extract = next(i for i, c in enumerate(commands) if c[:2] == ["sudo", "tar"] and "--extract" in c)
    start = commands.index(["sudo", "systemctl", "start", "docker"])
    run = commands.index(["docker", "run", "--rm", "--entrypoint", "/bin/bash", "abc123", "-c", "true"])
    assert stop < remove < extract < start < run
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
    assert not [c for c in read_commands(fake_tools) if c[:2] == ["sudo", "systemctl"]]


def test_restore_empties_store_again_when_image_does_not_run(fake_tools, snapshot):
    result = run_script({**fake_tools, "RUN_EXIT": "1"}, "restore", str(snapshot))

    assert result.returncode == 4
    commands = read_commands(fake_tools)
    run = next(i for i, c in enumerate(commands) if c[:2] == ["docker", "run"])
    after_run = commands[run + 1 :]
    assert ["sudo", "rm", "-rf", "/var/lib/docker/image", "/var/lib/docker/overlay2"] in after_run
    assert after_run[-1] == ["sudo", "systemctl", "start", "docker"]


def test_create_keeps_only_the_ci_image(fake_tools, tmp_path):
    snapshot_file = tmp_path / "out.tar.zst"
    env = {**fake_tools, "CI_IMAGES": "abc123\\nabc123\\n", "ALL_IMAGES": "abc123\\nother1\\nabc123\\n"}

    result = run_script(env, "create", str(snapshot_file))

    assert result.returncode == 0, result.stdout + result.stderr
    commands = read_commands(env)
    assert ["docker", "rmi", "--force", "other1"] in commands
    assert not [c for c in commands if c[:2] == ["docker", "rmi"] and "abc123" in c]
    assert ["docker", "builder", "prune", "--all", "--force"] in commands
    assert Path(f"{snapshot_file}.meta").read_text() == f"{FINGERPRINT} abc123 revision1\n"
    archive = next(c for c in commands if c[:2] == ["sudo", "tar"])
    assert archive[-2:] == ["image", "overlay2"]


def test_create_refuses_ambiguous_ci_image(fake_tools, tmp_path):
    env = {**fake_tools, "CI_IMAGES": "abc123\\ndef456\\n"}

    result = run_script(env, "create", str(tmp_path / "out.tar.zst"))

    assert result.returncode == 1
    assert not [c for c in read_commands(env) if c[:2] == ["sudo", "systemctl"]]


@pytest.mark.parametrize("failure", [{"ZSTD_EXIT": "1"}, {"TAR_EXIT": "1"}, {"START_FAIL_ONCE": "1"}])
def test_restore_recovers_empty_running_daemon_after_materialization_failure(fake_tools, snapshot, failure):
    result = run_script({**fake_tools, **failure}, "restore", str(snapshot))
    assert result.returncode != 0
    commands = read_commands(fake_tools)
    assert commands[-2:] == [
        ["sudo", "rm", "-rf", "/var/lib/docker/image", "/var/lib/docker/overlay2"],
        ["sudo", "systemctl", "start", "docker"],
    ]
    assert not any(command[:2] == ["docker", "run"] for command in commands)


@pytest.mark.parametrize("failure", [{"ZSTD_EXIT": "1"}, {"TAR_EXIT": "1"}])
def test_create_restarts_daemon_after_archive_failure(fake_tools, tmp_path, failure):
    result = run_script(
        {**fake_tools, **failure, "CI_IMAGES": "abc123"}, "create", str(tmp_path / "snapshot.tar.zst")
    )
    assert result.returncode != 0
    assert read_commands(fake_tools)[-1] == ["sudo", "systemctl", "start", "docker"]


@pytest.mark.parametrize("mode", ["create", "restore"])
@pytest.mark.parametrize("daemon", ["28.5.2 btrfs x86_64 /var/lib/docker", "28.5.2 overlay2 x86_64 /other"])
def test_unsupported_storage_does_not_modify_daemon(fake_tools, snapshot, mode, daemon):
    result = run_script({**fake_tools, "DAEMON_FINGERPRINT": daemon}, mode, str(snapshot))
    assert result.returncode == 3
    assert "::warning::" in result.stdout
    assert not any(command[0] == "sudo" for command in read_commands(fake_tools))


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
    assert read_commands(fake_tools)[-1] == ["sudo", "systemctl", "start", "docker"]
    assert not any(command[:2] == ["sudo", "rm"] for command in read_commands(fake_tools))


def test_snapshot_creation_uses_builder_after_cache_publication():
    workflow = yaml.safe_load(WORKFLOW.read_text())
    assert "snapshot-ci-images" not in workflow["jobs"]
    steps = workflow["jobs"]["build-ci-images"]["steps"]
    snapshot = next(i for i, step in enumerate(steps) if step.get("id") == "snapshot-export")
    cache_publishers = [i for i, step in enumerate(steps) if "/stash/save@" in step.get("uses", "")]
    assert cache_publishers
    assert all(publisher < snapshot for publisher in cache_publishers)


def test_snapshot_artifact_name_matches_between_upload_and_restore():
    upload = next(
        step
        for step in yaml.safe_load(WORKFLOW.read_text())["jobs"]["build-ci-images"]["steps"]
        if "/upload-artifact@" in step.get("uses", "") and step["name"].startswith("Upload CI image snapshot")
    )
    restore = next(
        step
        for step in yaml.safe_load(ACTION.read_text())["runs"]["steps"]
        if step.get("id") == "restore-snapshot"
    )
    producer = upload["with"]["name"].replace("env.PYTHON_MAJOR_MINOR_VERSION", "inputs.python")
    assert producer.startswith("ci-image-snapshot-v1-")
    assert producer == restore["with"]["name"]
