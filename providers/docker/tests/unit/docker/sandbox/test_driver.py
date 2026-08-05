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
import stat
from pathlib import Path
from unittest import mock
from uuid import uuid4

import pytest

from airflow.providers.common.sandbox.models import SandboxLaunchRequest, SandboxState
from airflow.providers.docker.sandbox.cli import AsyncSbxCli, SbxSandbox
from airflow.providers.docker.sandbox.driver import DockerSandboxDriver
from airflow.providers.docker.sandbox.exceptions import (
    DockerSandboxConfigurationError,
    DockerSandboxProtocolError,
)
from airflow.providers.docker.sandbox.models import (
    METADATA_FILENAME,
    STATUS_FILENAME,
    DockerSandboxDriverConfig,
    DockerSandboxHandle,
    DockerSandboxMetadata,
    sandbox_name_from_request_id,
)


def make_driver(tmp_path: Path) -> tuple[DockerSandboxDriver, mock.AsyncMock]:
    driver = DockerSandboxDriver(
        DockerSandboxDriverConfig(
            scratch_root=str(tmp_path / "scratch"),
            acceptance_timeout_seconds=1,
            acceptance_poll_interval=0.001,
        )
    )
    cli = mock.AsyncMock(spec=AsyncSbxCli)
    driver._cli = cli
    return driver, cli


def make_request(*, keep: bool = False) -> SandboxLaunchRequest:
    return SandboxLaunchRequest(
        request_id=str(uuid4()),
        command=("python", "-m", "airflow.sdk.execution_time.execute_workload"),
        env={"AIRFLOW_TOKEN": "super-secret"},
        provider_config={"template": "airflow-sandbox:dev", "cpus": 2, "memory": "1g"},
        workdir="/workspace",
        timeout_seconds=300,
        ttl_seconds=600,
        keep=keep,
    )


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value))
    path.chmod(0o600)


def read_json(path: Path) -> object:
    return json.loads(path.read_text())


def write_status(request_dir: Path, request_id: str, state: str, exit_code: int | None) -> None:
    write_json(
        request_dir / STATUS_FILENAME,
        {
            "exit_code": exit_code,
            "message": "task failed" if state == "failed" else None,
            "request_id": request_id,
            "schema_version": 1,
            "state": state,
        },
    )


def prepare_request_dir(driver: DockerSandboxDriver, request_id: str) -> Path:
    driver._ensure_scratch_root()
    request_dir = driver._root / request_id
    request_dir.mkdir(mode=0o700)
    request_dir.chmod(0o700)
    return request_dir


@pytest.mark.asyncio
async def test_launch_uses_secret_free_argv_and_waits_for_supervisor_acceptance(tmp_path: Path) -> None:
    driver, cli = make_driver(tmp_path)
    request = make_request()
    sandbox_name = sandbox_name_from_request_id(request.request_id)
    listed = SbxSandbox("stable-id", sandbox_name, "running")
    cli.find_sandbox.side_effect = [listed, listed]
    observed_spec: dict[str, object] | None = None

    async def accept(args: tuple[str, ...]) -> None:
        nonlocal observed_spec
        spec_path = Path(args[args.index("--spec") + 1])
        value = read_json(spec_path)
        assert isinstance(value, dict)
        observed_spec = value
        write_status(spec_path.parent, request.request_id, "running", None)

    cli.execute_detached.side_effect = accept

    handle = await driver.launch(request)

    expected_dir = tmp_path / "scratch" / request.request_id
    cli.create.assert_awaited_once_with(
        (
            "--quiet",
            "--name",
            sandbox_name,
            "--cpus",
            "2",
            "--memory",
            "1g",
            "--template",
            "airflow-sandbox:dev",
            "shell",
            str(expected_dir),
        )
    )
    execute_argv = cli.execute_detached.await_args.args[0]
    assert "super-secret" not in repr((cli.create.await_args, execute_argv))
    assert observed_spec is not None
    assert observed_spec["env"] == {"AIRFLOW_TOKEN": "super-secret"}
    assert stat.S_IMODE((expected_dir / "launch.json").stat().st_mode) == 0o600
    assert stat.S_IMODE((expected_dir / METADATA_FILENAME).stat().st_mode) == 0o600
    assert DockerSandboxHandle.from_common(handle).sandbox_id == "stable-id"


@pytest.mark.asyncio
async def test_launch_rejects_keep_without_calling_sbx(tmp_path: Path) -> None:
    driver, cli = make_driver(tmp_path)

    with pytest.raises(DockerSandboxConfigurationError, match="no hard TTL"):
        await driver.launch(make_request(keep=True))

    cli.create.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status_state", "exit_code", "sandbox", "expected_state"),
    [
        ("succeeded", 0, None, SandboxState.SUCCEEDED),
        ("failed", 7, None, SandboxState.FAILED),
        ("running", None, None, SandboxState.GONE),
        ("running", None, SbxSandbox("stable-id", "unused", "stopped"), SandboxState.GONE),
    ],
)
async def test_status_file_is_authoritative_and_presence_is_verified(
    tmp_path: Path,
    status_state: str,
    exit_code: int | None,
    sandbox: SbxSandbox | None,
    expected_state: SandboxState,
) -> None:
    driver, cli = make_driver(tmp_path)
    request = make_request()
    sandbox_name = sandbox_name_from_request_id(request.request_id)
    handle = DockerSandboxHandle(request.request_id, sandbox_name, "stable-id").to_common()
    request_dir = prepare_request_dir(driver, request.request_id)
    write_status(request_dir, request.request_id, status_state, exit_code)
    if sandbox is not None:
        sandbox = SbxSandbox(sandbox.sandbox_id, sandbox_name, sandbox.status)
    cli.find_sandbox.return_value = sandbox

    assert (await driver.get_status(handle)).state is expected_state


@pytest.mark.asyncio
async def test_status_rejects_stable_id_reuse(tmp_path: Path) -> None:
    driver, cli = make_driver(tmp_path)
    request = make_request()
    sandbox_name = sandbox_name_from_request_id(request.request_id)
    handle = DockerSandboxHandle(request.request_id, sandbox_name, "original-id").to_common()
    request_dir = prepare_request_dir(driver, request.request_id)
    write_status(request_dir, request.request_id, "running", None)
    cli.find_sandbox.return_value = SbxSandbox("replacement-id", sandbox_name, "running")

    with pytest.raises(DockerSandboxProtocolError, match="stable ID changed"):
        await driver.get_status(handle)


@pytest.mark.asyncio
async def test_recover_requires_metadata_status_and_exact_stable_id(tmp_path: Path) -> None:
    driver, cli = make_driver(tmp_path)
    request = make_request()
    sandbox_name = sandbox_name_from_request_id(request.request_id)
    request_dir = prepare_request_dir(driver, request.request_id)
    metadata = DockerSandboxMetadata(request.request_id, sandbox_name, "stable-id")
    write_json(request_dir / METADATA_FILENAME, metadata.to_json())
    write_status(request_dir, request.request_id, "running", None)
    cli.find_sandbox.return_value = SbxSandbox("stable-id", sandbox_name, "running")

    recovered = await driver.recover(request.request_id)

    assert recovered is not None
    assert recovered.keep is False
    assert DockerSandboxHandle.from_common(recovered.handle).sandbox_id == "stable-id"


@pytest.mark.asyncio
async def test_recovery_identity_mismatch_fences_deterministic_name(tmp_path: Path) -> None:
    driver, cli = make_driver(tmp_path)
    request = make_request()
    sandbox_name = sandbox_name_from_request_id(request.request_id)
    request_dir = prepare_request_dir(driver, request.request_id)
    metadata = DockerSandboxMetadata(request.request_id, sandbox_name, "original-id")
    write_json(request_dir / METADATA_FILENAME, metadata.to_json())
    write_status(request_dir, request.request_id, "running", None)
    replacement = SbxSandbox("replacement-id", sandbox_name, "running")
    cli.find_sandbox.side_effect = [replacement, replacement, None]

    assert await driver.recover(request.request_id) is None
    cli.remove.assert_awaited_once_with(sandbox_name)
    assert not request_dir.exists()


@pytest.mark.asyncio
async def test_recovery_corrupt_metadata_is_fenced_instead_of_guessed(tmp_path: Path) -> None:
    driver, cli = make_driver(tmp_path)
    request = make_request()
    request_dir = prepare_request_dir(driver, request.request_id)
    metadata_path = request_dir / METADATA_FILENAME
    metadata_path.write_text("not json")
    metadata_path.chmod(0o600)
    cli.find_sandbox.return_value = None

    assert await driver.recover(request.request_id) is None
    assert not request_dir.exists()


@pytest.mark.asyncio
async def test_fence_is_idempotent_when_sandbox_and_scratch_are_absent(tmp_path: Path) -> None:
    driver, cli = make_driver(tmp_path)
    request_id = str(uuid4())
    cli.find_sandbox.return_value = None

    await driver.fence(request_id)

    cli.remove.assert_not_awaited()


@pytest.mark.asyncio
async def test_terminate_removes_only_the_exact_stable_identity(tmp_path: Path) -> None:
    driver, cli = make_driver(tmp_path)
    request = make_request()
    sandbox_name = sandbox_name_from_request_id(request.request_id)
    handle = DockerSandboxHandle(request.request_id, sandbox_name, "stable-id").to_common()
    request_dir = prepare_request_dir(driver, request.request_id)
    sandbox = SbxSandbox("stable-id", sandbox_name, "running")
    cli.find_sandbox.side_effect = [sandbox, None]

    await driver.terminate(handle)

    cli.remove.assert_awaited_once_with(sandbox_name)
    assert not request_dir.exists()


@pytest.mark.asyncio
async def test_terminate_refuses_same_name_with_a_replacement_identity(tmp_path: Path) -> None:
    driver, cli = make_driver(tmp_path)
    request = make_request()
    sandbox_name = sandbox_name_from_request_id(request.request_id)
    handle = DockerSandboxHandle(request.request_id, sandbox_name, "original-id").to_common()
    request_dir = prepare_request_dir(driver, request.request_id)
    cli.find_sandbox.return_value = SbxSandbox("replacement-id", sandbox_name, "running")

    with pytest.raises(DockerSandboxProtocolError, match="stable ID does not match"):
        await driver.terminate(handle)

    cli.remove.assert_not_awaited()
    assert request_dir.exists()


@pytest.mark.asyncio
async def test_terminate_is_idempotent_after_resource_disappears(tmp_path: Path) -> None:
    driver, cli = make_driver(tmp_path)
    request = make_request()
    sandbox_name = sandbox_name_from_request_id(request.request_id)
    handle = DockerSandboxHandle(request.request_id, sandbox_name, "stable-id").to_common()
    request_dir = prepare_request_dir(driver, request.request_id)
    cli.find_sandbox.return_value = None

    await driver.terminate(handle)

    cli.remove.assert_not_awaited()
    assert not request_dir.exists()


def test_request_cleanup_unlinks_a_symlink_without_following_it(tmp_path: Path) -> None:
    driver, _ = make_driver(tmp_path)
    driver._ensure_scratch_root()
    request_id = str(uuid4())
    target = tmp_path / "must-survive"
    target.mkdir()
    request_path = driver._root / request_id
    request_path.symlink_to(target, target_is_directory=True)

    driver._remove_request_dir(request_id)

    assert not request_path.exists()
    assert target.is_dir()


@pytest.mark.asyncio
@pytest.mark.parametrize("unsafe_root", ["symlink", "permissions"])
async def test_health_check_rejects_unsafe_scratch_root(tmp_path: Path, unsafe_root: str) -> None:
    root = tmp_path / "scratch"
    if unsafe_root == "symlink":
        target = tmp_path / "target"
        target.mkdir(mode=0o700)
        root.symlink_to(target, target_is_directory=True)
    else:
        root.mkdir(mode=0o755)
        root.chmod(0o755)
    driver = DockerSandboxDriver(DockerSandboxDriverConfig(scratch_root=str(root)))

    with pytest.raises(DockerSandboxConfigurationError):
        await driver.health_check()
