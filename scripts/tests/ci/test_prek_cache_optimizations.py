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
import shutil
import subprocess
import tarfile
from pathlib import Path

import pytest
import yaml
from ci import prek_cache_markers

ROOT = Path(__file__).resolve().parents[3]
PREK_ACTION = ".github/actions/install-prek/action.yml"


def load_yaml(path):
    return yaml.load((ROOT / path).read_text(), Loader=yaml.BaseLoader)


def find_step(path, *, step_id=None, name=None):
    steps = load_yaml(path)["runs"]["steps"]
    return next(
        step
        for step in steps
        if (step_id is not None and step.get("id") == step_id)
        or (name is not None and step.get("name") == name)
    )


def run_shell(script, env):
    return subprocess.run(
        ["bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c", script],
        cwd=ROOT,
        env={**os.environ, **env},
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )


@pytest.fixture
def sandbox(tmp_path):
    home = tmp_path / "home"
    home.mkdir()
    output = tmp_path / "output"
    output.touch()
    return {"HOME": str(home), "GITHUB_OUTPUT": str(output)}


def read_outputs(env):
    return dict(line.split("=", 1) for line in Path(env["GITHUB_OUTPUT"]).read_text().splitlines())


def run_cache_step(step, env, tmp_path):
    script = step["run"].replace("/tmp/cache-prek.tar.gz", str(tmp_path / "cache-prek.tar.gz"))
    return run_shell(script, env)


@pytest.fixture
def fake_tools(tmp_path):
    tools = tmp_path / "bin"
    tools.mkdir()
    log = tmp_path / "commands.log"
    command = tools / "command"
    command.write_text(
        r"""#!/usr/bin/env bash
name="${0##*/}"
printf '%s\n' "${name} $*" >> "${COMMAND_LOG}"
if [[ -n "${FAIL_MATCH:-}" && "${name} $*" == *"${FAIL_MATCH}"* ]]; then
    exit 42
fi
if [[ "${name}" == "prek" ]]; then
    count_file="${COMMAND_COUNT}"
    count=0
    [[ ! -f "${count_file}" ]] || count=$(<"${count_file}")
    count=$((count + 1))
    printf '%s' "${count}" > "${count_file}"
    if [[ "${WRITE_MARKER_ATTEMPT:-}" == "${count}" ]]; then
        marker="${HOME}/.cache/prek/hooks/python-new/.prek-hook.json"
        mkdir -p "${marker%/*}"
        printf '{"schema_version":1}' > "${marker}"
    fi
    if [[ "${WRITE_LOG_ONLY:-false}" == "true" ]]; then
        mkdir -p "${HOME}/.cache/prek"
        printf 'diagnostic\n' >> "${HOME}/.cache/prek/prek.log"
    fi
    if (( count <= ${FAIL_ATTEMPTS:-0} )); then
        exit 42
    fi
fi
"""
    )
    command.chmod(0o755)
    for name in ("prek", "sleep"):
        (tools / name).symlink_to(command)
    return {
        "PATH": f"{tools}:{os.environ['PATH']}",
        "COMMAND_LOG": str(log),
        "COMMAND_COUNT": str(tmp_path / "command-count"),
    }


def read_commands(env):
    return [shlex.split(line) for line in Path(env["COMMAND_LOG"]).read_text().splitlines()]


def test_cache_key_keeps_only_environment_inputs():
    step = find_step(PREK_ACTION, step_id="cache-key")
    assert "cache-prek-v13-${PLATFORM}" in step["run"]
    assert "python${PYTHON_VERSION}" in step["run"]
    assert "uv${UV_VERSION}" in step["run"]
    assert "prek${PREK_VERSION}" in step["run"]
    assert step["env"]["PREK_CONFIG_HASH"] == "${{ hashFiles('**/.pre-commit-config.yaml') }}"


@pytest.mark.parametrize(
    ("inputs", "expected_save", "expected_reason"),
    [
        ({}, False, "reader"),
        ({"SAVE_CACHE": "true", "STASH_HIT": "false"}, True, "cache-miss"),
        ({"SAVE_CACHE": "true", "TAR_RESTORED": "false"}, True, "extraction-failed"),
        ({"SAVE_CACHE": "true", "CACHE_CHANGED": "true"}, True, "cache-repaired"),
        ({"SAVE_CACHE": "true", "EVENT_NAME": "schedule"}, True, "non-pr-republication"),
        ({"SAVE_CACHE": "true", "EVENT_NAME": "push"}, True, "non-pr-republication"),
        ({"SAVE_CACHE": "true", "EVENT_NAME": "workflow_dispatch"}, True, "non-pr-republication"),
        (
            {"SAVE_CACHE": "true", "CHANGE_DETECTION_UNCERTAIN": "true"},
            True,
            "change-detection-uncertain",
        ),
        ({"SAVE_CACHE": "true"}, False, "unchanged"),
    ],
)
def test_cache_refresh_policy(sandbox, inputs, expected_save, expected_reason):
    env = {
        **sandbox,
        "SAVE_CACHE": "false",
        "STASH_HIT": "true",
        "TAR_RESTORED": "true",
        "CACHE_CHANGED": "false",
        "CHANGE_DETECTION_UNCERTAIN": "false",
        "EVENT_NAME": "pull_request",
        **inputs,
    }
    result = run_shell(find_step(PREK_ACTION, step_id="cache-policy")["run"], env)
    assert result.returncode == 0, result.stderr
    assert read_outputs(env)["save"] == str(expected_save).lower()
    assert read_outputs(env)["reason"] == expected_reason


def test_restored_hooks_are_always_validated(sandbox, fake_tools):
    marker = Path(sandbox["HOME"]) / ".cache/prek/hooks/python-existing/.prek-hook.json"
    marker.parent.mkdir(parents=True)
    marker.write_text('{"schema_version":1}')
    step = find_step(PREK_ACTION, step_id="install-hooks")
    assert "if" not in step
    result = run_shell(step["run"], {**sandbox, **fake_tools, "WRITE_LOG_ONLY": "true"})
    assert result.returncode == 0, result.stderr
    assert [
        "prek",
        "install-hooks",
        "--skip",
        "run-skill-eval",
        "--skip",
        "run-skill-eval-codex",
        "--skip",
        "view-skill-eval",
    ] in read_commands(fake_tools)
    assert read_outputs(sandbox)["cache-changed"] == "false"
    assert read_outputs(sandbox)["change-detection-uncertain"] == "false"


@pytest.mark.parametrize("cache_state", ("missing", "empty", "renamed-markers", "log-only"))
@pytest.mark.parametrize("save_cache", ("true", "false"))
def test_markerless_cache_refresh_policy(sandbox, fake_tools, cache_state, save_cache):
    cache = Path(sandbox["HOME"]) / ".cache/prek"
    if cache_state == "empty":
        cache.mkdir(parents=True)
    elif cache_state == "renamed-markers":
        marker = cache / "environments/python-one/.new-marker.json"
        marker.parent.mkdir(parents=True)
        marker.write_text("{}")
    result = run_shell(
        find_step(PREK_ACTION, step_id="install-hooks")["run"],
        {**sandbox, **fake_tools, "WRITE_LOG_ONLY": str(cache_state == "log-only").lower()},
    )
    assert result.returncode == 0, result.stderr
    outputs = read_outputs(sandbox)
    uncertain = cache_state in ("renamed-markers", "log-only")
    assert outputs["cache-changed"] == "false"
    assert outputs["change-detection-uncertain"] == str(uncertain).lower()

    result = run_shell(
        find_step(PREK_ACTION, step_id="cache-policy")["run"],
        {
            **sandbox,
            "SAVE_CACHE": save_cache,
            "STASH_HIT": "true",
            "TAR_RESTORED": "true",
            "CACHE_CHANGED": outputs["cache-changed"],
            "CHANGE_DETECTION_UNCERTAIN": outputs["change-detection-uncertain"],
            "EVENT_NAME": "pull_request",
        },
    )
    assert result.returncode == 0, result.stderr
    outputs = read_outputs(sandbox)
    assert outputs["save"] == str(uncertain and save_cache == "true").lower()
    assert outputs["reason"] == (
        "reader" if save_cache == "false" else "change-detection-uncertain" if uncertain else "unchanged"
    )


def test_change_detection_spans_failed_then_successful_attempts(sandbox, fake_tools):
    result = run_shell(
        find_step(PREK_ACTION, step_id="install-hooks")["run"],
        {**sandbox, **fake_tools, "FAIL_ATTEMPTS": "1", "WRITE_MARKER_ATTEMPT": "1"},
    )
    assert result.returncode == 0, result.stderr
    assert len([cmd for cmd in read_commands(fake_tools) if cmd[:2] == ["prek", "install-hooks"]]) == 2
    assert read_outputs(sandbox)["cache-changed"] == "true"


def test_unreadable_installation_metadata_is_uncertain(sandbox, fake_tools):
    marker = Path(sandbox["HOME"]) / ".cache/prek/hooks/python-broken/.prek-hook.json"
    marker.mkdir(parents=True)
    result = run_shell(find_step(PREK_ACTION, step_id="install-hooks")["run"], {**sandbox, **fake_tools})
    assert result.returncode == 0, result.stderr
    assert read_outputs(sandbox)["cache-changed"] == "false"
    assert read_outputs(sandbox)["change-detection-uncertain"] == "true"


def test_hook_install_failure_remains_fatal(sandbox, fake_tools):
    assert "if" not in find_step(PREK_ACTION, step_id="cache-policy")
    assert "continue-on-error" not in find_step(PREK_ACTION, step_id="install-hooks")
    result = run_shell(
        find_step(PREK_ACTION, step_id="install-hooks")["run"],
        {**sandbox, **fake_tools, "FAIL_MATCH": "prek install-hooks"},
    )
    assert result.returncode != 0
    assert len([cmd for cmd in read_commands(fake_tools) if cmd[:2] == ["prek", "install-hooks"]]) == 4


def test_marker_snapshot_uses_relative_paths_and_exact_contents(tmp_path):
    cache = tmp_path / "prek"
    hook_marker = cache / "hooks/python-one/.prek-hook.json"
    repo_marker = cache / "repos/repo-one/.prek-repo.json"
    hook_marker.parent.mkdir(parents=True)
    repo_marker.parent.mkdir(parents=True)
    hook_marker.write_bytes(b'{"hook": 1}\n')
    repo_marker.write_bytes(b'{"repo": 1}\n')

    snapshot = prek_cache_markers.snapshot_markers(cache)

    assert set(snapshot) == {"hooks/python-one/.prek-hook.json", "repos/repo-one/.prek-repo.json"}
    assert snapshot == prek_cache_markers.snapshot_markers(cache)


def test_repaired_archive_is_reused_without_another_save(sandbox, fake_tools, tmp_path):
    cache = Path(sandbox["HOME"]) / ".cache/prek"
    (cache / "hooks/python-incomplete").mkdir(parents=True)
    marker = cache / "hooks/python-existing/.prek-hook.json"
    marker.parent.mkdir(parents=True)
    marker.write_text('{"schema_version":1}')
    install_step = find_step(PREK_ACTION, step_id="install-hooks")
    policy_step = find_step(PREK_ACTION, step_id="cache-policy")

    first_install = run_shell(install_step["run"], {**sandbox, **fake_tools, "WRITE_MARKER_ATTEMPT": "1"})
    assert first_install.returncode == 0, first_install.stderr
    first_outputs = read_outputs(sandbox)
    assert first_outputs["cache-changed"] == "true"

    Path(sandbox["GITHUB_OUTPUT"]).write_text("")
    policy_env = {
        **sandbox,
        "SAVE_CACHE": "true",
        "STASH_HIT": "true",
        "TAR_RESTORED": "true",
        "CACHE_CHANGED": "true",
        "CHANGE_DETECTION_UNCERTAIN": "false",
        "EVENT_NAME": "pull_request",
    }
    assert run_shell(policy_step["run"], policy_env).returncode == 0
    assert read_outputs(sandbox) == {"save": "true", "reason": "cache-repaired"}

    Path(sandbox["GITHUB_OUTPUT"]).write_text("")
    archive_step = find_step(PREK_ACTION, step_id="archive-prek")
    assert run_cache_step(archive_step, sandbox, tmp_path).returncode == 0
    shutil.rmtree(cache)
    Path(sandbox["GITHUB_OUTPUT"]).write_text("")
    restore_step = find_step(PREK_ACTION, step_id="restore-prek-tar")
    assert run_cache_step(restore_step, sandbox, tmp_path).returncode == 0

    Path(sandbox["GITHUB_OUTPUT"]).write_text("")
    second_install = run_shell(install_step["run"], {**sandbox, **fake_tools})
    assert second_install.returncode == 0, second_install.stderr
    second_outputs = read_outputs(sandbox)
    assert second_outputs["cache-changed"] == "false"

    Path(sandbox["GITHUB_OUTPUT"]).write_text("")
    second_policy_env = {
        **policy_env,
        "CACHE_CHANGED": "false",
    }
    assert run_shell(policy_step["run"], second_policy_env).returncode == 0
    assert read_outputs(sandbox) == {"save": "false", "reason": "unchanged"}


@pytest.mark.skipif(
    os.environ.get("RUN_PREK_INTEGRATION") != "1",
    reason="Set RUN_PREK_INTEGRATION=1 to exercise real prek 0.5.2",
)
def test_real_prek_repair_and_reuse(tmp_path):
    repository = tmp_path / "repository"
    repository.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=repository, check=True)
    (repository / ".pre-commit-config.yaml").write_text(
        """\
repos:
  - repo: local
    hooks:
      - id: local-python
        name: local python
        entry: python -c 'print("ok")'
        language: python
        pass_filenames: false
"""
    )
    cache = tmp_path / "prek-cache"
    env = {**os.environ, "PREK_HOME": str(cache)}
    command = ["uvx", "--from", "prek==0.5.2", "prek", "install-hooks"]

    def install():
        return subprocess.run(
            command,
            cwd=repository,
            env=env,
            capture_output=True,
            text=True,
            timeout=180,
            check=False,
        )

    first_install = install()
    assert first_install.returncode == 0, first_install.stderr
    marker = next((cache / "hooks").glob("*/.prek-hook.json"))
    marker.unlink()
    before_repair = prek_cache_markers.snapshot_markers(cache)

    repair = install()
    assert repair.returncode == 0, repair.stderr
    repaired = prek_cache_markers.snapshot_markers(cache)
    assert repaired != before_repair

    archive = tmp_path / "prek-cache.tar.gz"
    with tarfile.open(archive, "w:gz") as handle:
        handle.add(cache, arcname="prek-cache")
    shutil.rmtree(cache)
    with tarfile.open(archive, "r:gz") as handle:
        handle.extractall(tmp_path, filter="fully_trusted")
    before_reuse = prek_cache_markers.snapshot_markers(cache)

    reuse = install()
    assert reuse.returncode == 0, reuse.stderr
    assert prek_cache_markers.snapshot_markers(cache) == before_reuse


@pytest.mark.parametrize("payload", ("missing", "corrupt", "wrong-directory"))
def test_unusable_archive_is_a_miss(sandbox, tmp_path, payload):
    archive = tmp_path / "cache-prek.tar.gz"
    if payload == "corrupt":
        archive.write_bytes(b"not a gzip archive")
    elif payload == "wrong-directory":
        other = tmp_path / "other"
        other.mkdir()
        with tarfile.open(archive, "w:gz") as handle:
            handle.add(other, arcname=".cache/other")
    result = run_cache_step(find_step(PREK_ACTION, step_id="restore-prek-tar"), sandbox, tmp_path)
    assert result.returncode == 0, result.stderr
    assert read_outputs(sandbox)["tar-restored"] == "false"


def test_archive_roundtrip_preserves_permissions_links_and_contents(sandbox, tmp_path):
    cache = Path(sandbox["HOME"]) / ".cache/prek"
    cache.mkdir(parents=True)
    executable = cache / "python"
    executable.write_text("payload\n")
    executable.chmod(0o751)
    (cache / "link").symlink_to("python")
    result = run_cache_step(find_step(PREK_ACTION, step_id="archive-prek"), sandbox, tmp_path)
    assert result.returncode == 0, result.stderr
    shutil.rmtree(cache)
    result = run_cache_step(find_step(PREK_ACTION, step_id="restore-prek-tar"), sandbox, tmp_path)
    assert result.returncode == 0, result.stderr
    assert read_outputs(sandbox)["tar-restored"] == "true"
    assert executable.read_text() == "payload\n"
    assert executable.stat().st_mode & 0o777 == 0o751
    assert (cache / "link").is_symlink()
    assert os.readlink(cache / "link") == "python"


def test_missing_cache_cannot_be_saved(sandbox, tmp_path):
    result = run_cache_step(find_step(PREK_ACTION, step_id="archive-prek"), sandbox, tmp_path)
    assert result.returncode != 0
    assert "duration-seconds" not in read_outputs(sandbox)


def test_cache_cleanup_preserves_unrelated_cache(sandbox, tmp_path):
    home = Path(sandbox["HOME"])
    for name in ("prek", "uv"):
        (home / ".cache" / name).mkdir(parents=True)
    archive = tmp_path / "cache-prek.tar.gz"
    archive.write_bytes(b"stale")
    result = run_cache_step(
        find_step(PREK_ACTION, name="Clear local prek cache before restore"), sandbox, tmp_path
    )
    assert result.returncode == 0, result.stderr
    assert not (home / ".cache/prek").exists()
    assert (home / ".cache/uv").is_dir()
    assert not archive.exists()
