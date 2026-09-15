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

import itertools
import os
import re
import shlex
import shutil
import subprocess
import tarfile
from pathlib import Path

import pytest
import yaml
from ci import prek_cache_key

ROOT = Path(__file__).resolve().parents[3]
PREK_ACTION = ".github/actions/install-prek/action.yml"
UNIT_WORKFLOW = ".github/workflows/run-unit-tests.yml"
UI_WORKFLOW = ".github/workflows/ui-e2e-tests.yml"


def load_yaml(path):
    # BaseLoader preserves GitHub's `on` key instead of treating it as YAML 1.1's boolean True.
    return yaml.load((ROOT / path).read_text(), Loader=yaml.BaseLoader)


def find_step(path, *, step_id=None, name=None, job=None):
    document = load_yaml(path)
    steps = document["jobs"][job]["steps"] if job else document["runs"]["steps"]
    return next(
        step
        for step in steps
        if (step_id is not None and step.get("id") == step_id)
        or (name is not None and step.get("name") == name)
    )


def run_shell(script, env):
    return subprocess.run(
        ["bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c", script],
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
    # Remap only the fixed archive location; exercise the real action's Bash otherwise.
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
if [[ "${name}" == "sudo" && "${1:-}" == "find" ]]; then
    printf '%s' "${FIND_RESULT:-}"
fi
if [[ "${name}" == "uname" ]]; then
    printf '%s\n' "${TEST_ARCH:-x86_64}"
fi
"""
    )
    command.chmod(0o755)
    for name in ("sudo", "df", "lsblk", "uname", "prek", "pnpm", "sleep"):
        (tools / name).symlink_to(command)
    return {"PATH": f"{tools}:{os.environ['PATH']}", "COMMAND_LOG": str(log)}


def read_commands(env):
    return [shlex.split(line) for line in Path(env["COMMAND_LOG"]).read_text().splitlines()]


def test_cache_key_is_stable_and_safe():
    first = {"z": "x/y\n", "a": "1"}
    key = prek_cache_key.compute_cache_key(first)
    assert key == prek_cache_key.compute_cache_key(dict(reversed(list(first.items()))))
    assert re.fullmatch(r"cache-prek-v11-[0-9a-f]{64}", key)


@pytest.fixture
def identity_inputs(tmp_path):
    return {
        "PLATFORM": "linux/amd64",
        "UV_VERSION": "1",
        "PREK_VERSION": "2",
        "PREK_CONFIG_HASH": "abc",
        "GITHUB_WORKSPACE": str(tmp_path),
    }


@pytest.mark.parametrize("field", prek_cache_key.REQUIRED_INPUTS)
def test_cache_identity_requires_inputs(identity_inputs, field):
    identity_inputs.pop(field)
    with pytest.raises(ValueError, match=field):
        prek_cache_key.build_cache_identity(identity_inputs)


@pytest.mark.parametrize(
    "field",
    [
        *prek_cache_key.REQUIRED_INPUTS,
        "system",
        "machine",
        "os_id",
        "os_version",
        "python_version",
        "python_abi",
        "python_executable",
        "python_prefix",
        "home",
    ],
)
def test_every_environment_component_invalidates_key(identity_inputs, field):
    identity = prek_cache_key.build_cache_identity(identity_inputs)
    changed = {**identity, field: identity[field] + "-changed"}
    assert prek_cache_key.compute_cache_key(identity) != prek_cache_key.compute_cache_key(changed)


def test_cache_identity_handles_missing_os_release(identity_inputs, monkeypatch):
    def unavailable():
        raise OSError("not available")

    monkeypatch.setattr(prek_cache_key.platform, "freedesktop_os_release", unavailable)
    identity = prek_cache_key.build_cache_identity(identity_inputs)
    assert identity["os_id"] == identity["os_version"] == ""


def test_cache_key_appends_output(identity_inputs, sandbox, monkeypatch):
    for key, value in {**identity_inputs, **sandbox}.items():
        monkeypatch.setenv(key, value)
    Path(sandbox["GITHUB_OUTPUT"]).write_text("existing=value\n")
    prek_cache_key.main()
    assert read_outputs(sandbox)["existing"] == "value"
    assert read_outputs(sandbox)["key"].startswith("cache-prek-v11-")


@pytest.mark.parametrize(
    "save,hit,restored,event",
    list(
        itertools.product(
            ("true", "false"),
            ("true", "false", ""),
            ("true", "false", ""),
            ("pull_request", "schedule", "push"),
        )
    ),
)
def test_cache_refresh_policy(sandbox, save, hit, restored, event):
    env = {**sandbox, "SAVE_CACHE": save, "STASH_HIT": hit, "TAR_RESTORED": restored, "EVENT_NAME": event}
    result = run_shell(find_step(PREK_ACTION, step_id="cache-policy")["run"], env)
    assert result.returncode == 0, result.stderr
    expected = save == "true" and (hit != "true" or restored != "true" or event == "schedule")
    assert read_outputs(env)["save"] == str(expected).lower()


def test_restored_hooks_are_always_validated(sandbox, fake_tools):
    step = find_step(PREK_ACTION, step_id="install-hooks")
    assert "if" not in step
    result = run_shell(step["run"], {**sandbox, **fake_tools})
    assert result.returncode == 0, result.stderr
    assert ["prek", "install-hooks"] in read_commands(fake_tools)


def test_hook_install_failure_remains_fatal(sandbox, fake_tools):
    result = run_shell(
        find_step(PREK_ACTION, step_id="install-hooks")["run"],
        {**sandbox, **fake_tools, "FAIL_MATCH": "prek install-hooks"},
    )
    assert result.returncode != 0
    assert read_commands(fake_tools).count(["prek", "install-hooks"]) == 4


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


@pytest.mark.parametrize(
    "browser,expected",
    [("all", []), ("chromium", ["chromium"]), ("firefox", ["firefox"]), ("webkit", ["webkit"])],
)
def test_browser_install_preserves_requested_engine(fake_tools, browser, expected):
    step = find_step(
        UI_WORKFLOW,
        name="Install selected Playwright browser and dependencies",
        job="test-ui-e2e-tests",
    )
    result = run_shell(step["run"], {**fake_tools, "BROWSER": browser})
    assert result.returncode == 0, result.stderr
    assert read_commands(fake_tools) == [["pnpm", "exec", "playwright", "install", "--with-deps", *expected]]


@pytest.mark.parametrize("browser", ("", "chrome", "chromium firefox", "$(echo chromium)", "--help"))
def test_browser_install_rejects_unknown_values(fake_tools, browser):
    step = find_step(
        UI_WORKFLOW,
        name="Install selected Playwright browser and dependencies",
        job="test-ui-e2e-tests",
    )
    result = run_shell(step["run"], {**fake_tools, "BROWSER": browser})
    assert result.returncode != 0
    assert not Path(fake_tools["COMMAND_LOG"]).exists()


def test_ui_jobs_keep_frozen_installs_and_tests_without_duplicate_stashes():
    steps = load_yaml(".github/workflows/basic-tests.yml")["jobs"]["tests-ui"]["steps"]
    commands = [step.get("run", "") for step in steps]
    for directory in (
        "airflow-core/src/airflow/ui",
        "airflow-core/src/airflow/api_fastapi/auth/managers/simple/ui",
    ):
        assert f"cd {directory} && pnpm install --frozen-lockfile" in commands
        assert f"cd {directory} && pnpm test" in commands
    assert not any("stash/" in step.get("uses", "") for step in steps)
    setup = next(step for step in steps if step.get("uses", "").startswith("actions/setup-node@"))
    assert setup["with"]["cache"] == "pnpm"
    assert len(setup["with"]["cache-dependency-path"].splitlines()) == 2


@pytest.mark.parametrize("path", ("scripts/ci/make_mnt_writeable.sh", "scripts/ci/move_docker_to_mnt.sh"))
@pytest.mark.parametrize("remaining,recursive", [("", False), ("/mnt/.hidden\n", True)])
def test_ownership_walk_only_when_needed(fake_tools, path, remaining, recursive):
    result = run_shell((ROOT / path).read_text(), {**fake_tools, "FIND_RESULT": remaining, "USER": "runner"})
    assert result.returncode == 0, result.stderr
    ownership = [command for command in read_commands(fake_tools) if command[:2] == ["sudo", "chown"]]
    assert len(ownership) == 1
    assert ("-R" in ownership[0]) == recursive


@pytest.mark.parametrize("path", ("scripts/ci/make_mnt_writeable.sh", "scripts/ci/move_docker_to_mnt.sh"))
def test_failed_directory_scan_cannot_take_empty_fast_path(fake_tools, path):
    result = run_shell((ROOT / path).read_text(), {**fake_tools, "FAIL_MATCH": "sudo find", "USER": "runner"})
    assert result.returncode != 0
    assert not any(command[:2] == ["sudo", "chown"] for command in read_commands(fake_tools))


def test_optional_disk_probe_does_not_fail_cleanup(fake_tools):
    result = run_shell(
        (ROOT / "scripts/ci/make_mnt_writeable.sh").read_text(),
        {**fake_tools, "FAIL_MATCH": "sudo blkid", "USER": "runner"},
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    "failure",
    ("systemctl stop", "sudo rm", "sudo mkdir", "sudo mount", "sudo chown", "systemctl start"),
)
def test_docker_relocation_propagates_errors(fake_tools, failure):
    result = run_shell(
        (ROOT / "scripts/ci/move_docker_to_mnt.sh").read_text(), {**fake_tools, "FAIL_MATCH": failure}
    )
    assert result.returncode != 0


def test_docker_relocation_stays_disabled_on_arm(fake_tools):
    result = run_shell(
        (ROOT / "scripts/ci/move_docker_to_mnt.sh").read_text(), {**fake_tools, "TEST_ARCH": "aarch64"}
    )
    assert result.returncode == 0, result.stderr
    assert not any(command[0] == "sudo" for command in read_commands(fake_tools))


def test_parallel_cleanup_keeps_exact_targets_and_concurrency_bound(fake_tools):
    script = (ROOT / "scripts/tools/free_up_disk_space.sh").read_text()
    result = run_shell(script, fake_tools)
    assert result.returncode == 0, result.stderr
    commands = read_commands(fake_tools)
    deletes = [command for command in commands if command[:3] == ["sudo", "rm", "-rf"]]
    expected = {
        "/usr/share/dotnet/",
        "/usr/local/graalvm/",
        "/usr/local/.ghcup/",
        "/usr/local/share/powershell",
        "/usr/local/share/chromium",
        "/usr/local/share/boost",
        "/usr/local/lib/android",
        "/opt/hostedtoolcache",
        "/opt/ghc",
    }
    assert len(deletes) == len(expected)
    assert {command[-1] for command in deletes} == expected
    assert "xargs -0 -r -n 1 -P 4 sudo rm -rf --" in script
    apt_position = commands.index(["sudo", "apt-get", "clean"])
    assert all(commands.index(command) < apt_position for command in deletes)


@pytest.mark.parametrize("failure", ("/usr/local/lib/android", "sudo apt-get clean"))
def test_parallel_cleanup_propagates_errors(fake_tools, failure):
    result = run_shell(
        (ROOT / "scripts/tools/free_up_disk_space.sh").read_text(), {**fake_tools, "FAIL_MATCH": failure}
    )
    assert result.returncode != 0


def test_migrations_stay_in_an_existing_required_test_check():
    jobs = load_yaml(UNIT_WORKFLOW)["jobs"]
    assert set(jobs) == {"tests"}
    tests = jobs["tests"]
    assert "continue-on-error" not in tests
    assert tests["strategy"]["fail-fast"] == "false"
    step = next(step for step in tests["steps"] if step.get("uses", "").endswith("/migration_tests"))
    assert step["with"]["python-version"] == "${{ matrix.python-version }}"
    condition = step["if"]
    assert "inputs.run-migration-tests == 'true'" in condition
    assert "inputs.test-group == 'core'" in condition
    assert "matrix.python-version != '3.14'" in condition
    assert (
        "matrix.test-types.description == fromJSON(inputs.test-types-as-strings-in-json)[0].description"
        in condition
    )
    assert "continue-on-error" not in step


@pytest.mark.parametrize("python_versions", [("3.10",), ("3.14",), ("3.10", "3.12", "3.14")])
@pytest.mark.parametrize("backend_versions", [("12",), ("12", "16")])
@pytest.mark.parametrize(
    "excluded",
    [[], [{"python-version": "3.12", "backend-version": "12"}], [{"backend-version": "16"}]],
)
def test_migration_projection_keeps_each_eligible_environment_once(
    python_versions, backend_versions, excluded
):
    original = [
        {"python-version": py, "backend-version": db, "test-types": shard}
        for py, db, shard in itertools.product(python_versions, backend_versions, ("A", "B"))
    ]
    retained = [
        row
        for row in original
        if row["python-version"] != "3.14"
        and not any(all(row.get(key) == value for key, value in entry.items()) for entry in excluded)
    ]
    expected = {(row["python-version"], row["backend-version"]) for row in retained}
    actual = [(row["python-version"], row["backend-version"]) for row in retained if row["test-types"] == "A"]
    assert set(actual) == expected
    assert len(actual) == len(set(actual))


def test_translation_check_remains_independent_of_ui_selection():
    job = load_yaml(".github/workflows/basic-tests.yml")["jobs"]["check-translation-completness"]
    assert "if" not in job and "needs" not in job
    assert job["name"] == "Check translation completeness"
    assert job["steps"][-1]["run"] == "breeze ui check-translation-completeness || true"


@pytest.mark.parametrize("fail", (False, True))
def test_parallel_compressor_and_failure_propagation(sandbox, tmp_path, fail):
    cache = Path(sandbox["HOME"]) / ".cache/prek"
    cache.mkdir(parents=True)
    (cache / "payload").write_text("data")
    tools = tmp_path / "tools"
    tools.mkdir()
    compressor = tools / "pigz"
    compressor.write_text(
        "#!/usr/bin/env bash\n"
        'printf "%s\n" "$*" > "${COMPRESSOR_ARGS}"\n' + ("exit 42\n" if fail else "exec gzip -1\n")
    )
    compressor.chmod(0o755)
    args = tmp_path / "compressor-args"
    env = {**sandbox, "PATH": f"{tools}:{os.environ['PATH']}", "COMPRESSOR_ARGS": str(args)}
    result = run_cache_step(find_step(PREK_ACTION, step_id="archive-prek"), env, tmp_path)
    assert args.read_text().strip() == "-1 -p 4"
    if fail:
        assert result.returncode != 0
        assert "duration-seconds" not in read_outputs(sandbox)
    else:
        assert result.returncode == 0, result.stderr
        with tarfile.open(tmp_path / "cache-prek.tar.gz", "r:gz") as archive:
            assert ".cache/prek/payload" in archive.getnames()
