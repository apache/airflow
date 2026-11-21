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

import builtins
import json
import os
import shutil
import subprocess
import time
from types import SimpleNamespace
from unittest import mock

import pytest

pytest.importorskip("boxd")

from boxd import AuthenticationError, BoxdError, NotFoundError
from boxd.errors import MachineFileNotFoundError, MachinePermissionError
from boxd.models import ExecResult

from airflow.providers.common.ai.sandbox.base import (
    EXPIRES_AT_TAG,
    HOLDER_TAG,
    NETWORK_TAG,
    OWNER_TAG,
    SandboxError,
    SandboxSpec,
    SandboxTerminalError,
)
from airflow.providers.common.ai.sandbox.boxd import (
    _COMMAND_WRAPPER,
    _EGRESS_SETTLE,
    BoxdSandboxBackend,
)

_BASE_HOOK = "airflow.providers.common.ai.sandbox.boxd.BaseHook"
_MONOTONIC = "airflow.providers.common.ai.sandbox.boxd.time.monotonic"
_SLEEP = "airflow.providers.common.ai.sandbox.boxd.time.sleep"
_MACHINE_ID = "0b6a1f0e-0000-4000-8000-000000000001"


def _machine(*, egress=("none",), isolated=True, labels=None, status="running", name="airflow-sandbox-x"):
    return SimpleNamespace(
        id=_MACHINE_ID,
        name=name,
        status=status,
        egress_allow=list(egress),
        spec={"version": 1, "isolated": True} if isolated else {"version": 1},
        labels=dict(labels or {}),
    )


def _backend_with_client(**kwargs) -> tuple[BoxdSandboxBackend, mock.MagicMock]:
    backend = BoxdSandboxBackend(**kwargs)
    client = mock.MagicMock(spec=["machines"])
    client.machines = mock.MagicMock(
        spec=["create", "wait_until_ready", "exec", "delete", "get", "update_labels", "fs"]
    )
    client.machines.fs = mock.MagicMock(spec=["write"])
    client.machines.create.return_value = _machine()
    backend._client = client
    return backend, client


def _sent_spec(client) -> dict:
    return client.machines.create.call_args.kwargs["spec"].resolve()


@pytest.fixture
def clock():
    """A monotonic clock that moves only when the backend sleeps or the test says so."""
    now = [0.0]

    def sleep(seconds):
        now[0] += seconds

    with (
        mock.patch(_MONOTONIC, autospec=True, side_effect=lambda: now[0]),
        mock.patch(_SLEEP, autospec=True, side_effect=sleep),
    ):
        yield now


def test_missing_sdk_error_is_actionable():
    real_import = builtins.__import__

    def blocked_import(name, *args, **kwargs):
        if name == "boxd" or name.startswith("boxd."):
            raise ImportError("blocked for test")
        return real_import(name, *args, **kwargs)

    backend = BoxdSandboxBackend()
    with mock.patch.dict("os.environ", {"BOXD_API_KEY": "test-key"}, clear=False):
        with mock.patch("builtins.__import__", side_effect=blocked_import):
            with pytest.raises(SandboxTerminalError, match=r"\[boxd\]"):
                backend._get_client()


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"delete_after": 0}, "delete_after"),
        ({"delete_after": 1.5}, "whole number of seconds"),
        ({"delete_after": True}, "delete_after"),
        ({"ready_timeout": 0}, "ready_timeout"),
        ({"size": " "}, "size"),
        ({"snapshot": ""}, "snapshot"),
        ({"name_prefix": " "}, "name_prefix"),
    ],
)
def test_constructor_rejects_invalid_values(kwargs, message):
    with pytest.raises(ValueError, match=message):
        BoxdSandboxBackend(**kwargs)


class TestCredentials:
    @mock.patch(_BASE_HOOK, autospec=True)
    @mock.patch("boxd.Boxd", autospec=True)
    def test_construction_reads_no_credentials_and_opens_no_client(self, boxd_client, hook):
        with mock.patch.dict("os.environ", {}, clear=True):
            BoxdSandboxBackend()
            BoxdSandboxBackend(boxd_conn_id="my_boxd")

        hook.get_connection.assert_not_called()
        boxd_client.assert_not_called()

    @mock.patch(_BASE_HOOK, autospec=True)
    @mock.patch("boxd.Boxd", autospec=True)
    def test_environment_key_is_passed_explicitly(self, boxd_client, hook):
        with mock.patch.dict("os.environ", {"BOXD_API_KEY": " env-key "}, clear=False):
            backend = BoxdSandboxBackend()
            assert backend._get_client() is backend._get_client()

        boxd_client.assert_called_once_with(api_key="env-key")
        hook.get_connection.assert_not_called()

    @mock.patch(_BASE_HOOK, autospec=True)
    @mock.patch("boxd.Boxd", autospec=True)
    def test_connection_password_is_the_key(self, boxd_client, hook):
        hook.get_connection.return_value = SimpleNamespace(password=" conn-key ")
        with mock.patch.dict("os.environ", {"BOXD_API_KEY": "env-key"}, clear=False):
            BoxdSandboxBackend(boxd_conn_id="my_boxd")._get_client()

        hook.get_connection.assert_called_once_with("my_boxd")
        boxd_client.assert_called_once_with(api_key="conn-key")

    @mock.patch("boxd.Boxd", autospec=True)
    def test_missing_environment_key_fails_before_the_sdk_can_fall_back(self, boxd_client):
        # Without an explicit key the SDK authenticates as the boxd machine it runs on.
        with mock.patch.dict("os.environ", {"BOXD_API_KEY": "  "}, clear=False):
            with pytest.raises(SandboxTerminalError, match="BOXD_API_KEY is not set"):
                BoxdSandboxBackend()._get_client()
        boxd_client.assert_not_called()

    @mock.patch(_BASE_HOOK, autospec=True)
    @mock.patch("boxd.Boxd", autospec=True)
    def test_connection_without_password_is_refused(self, boxd_client, hook):
        hook.get_connection.return_value = SimpleNamespace(password=None)
        with pytest.raises(SandboxTerminalError, match="'my_boxd' has no password"):
            BoxdSandboxBackend(boxd_conn_id="my_boxd")._get_client()
        boxd_client.assert_not_called()

    def test_rejected_key_names_the_api_key(self):
        backend, client = _backend_with_client()
        client.machines.create.side_effect = AuthenticationError(
            "invalid or expired API key", code="unauthenticated", reason="invalid_api_key"
        )
        with pytest.raises(SandboxTerminalError, match="boxd rejected the API key when asked to create"):
            backend.create(spec=SandboxSpec())


class TestCreate:
    def test_default_spec_is_an_isolated_machine_with_no_egress(self, clock):
        backend, client = _backend_with_client()

        assert backend.create(spec=SandboxSpec()) == _MACHINE_ID

        name = client.machines.create.call_args.args[0]
        assert name.startswith("airflow-sandbox-")
        sent = _sent_spec(client)
        assert sent["isolated"] is True
        assert sent["egress"] == ["none"]
        assert "env" not in sent
        assert client.machines.create.call_args.kwargs["delete_after"] == 3600
        client.machines.wait_until_ready.assert_called_once_with(_MACHINE_ID, timeout=120.0)

    def test_no_spec_means_the_isolated_default(self, clock):
        backend, client = _backend_with_client()
        backend.create()
        assert _sent_spec(client)["egress"] == ["none"]

    @pytest.mark.parametrize(
        ("spec", "egress"),
        [
            pytest.param(SandboxSpec(block_network=False), None, id="open"),
            pytest.param(
                SandboxSpec(allow_egress_to=["pypi.org", "*.github.com"]),
                ["pypi.org", "*.github.com"],
                id="hosts",
            ),
            pytest.param(
                SandboxSpec(allow_egress_to_cidrs=["203.0.113.0/24"]), ["203.0.113.0/24"], id="cidrs"
            ),
            pytest.param(
                SandboxSpec(allow_egress_to=["pypi.org"], allow_egress_to_cidrs=["203.0.113.7/32"]),
                ["pypi.org", "203.0.113.7/32"],
                id="both",
            ),
        ],
    )
    def test_network_policy_becomes_the_egress_allowlist(self, clock, spec, egress):
        backend, client = _backend_with_client()
        client.machines.create.return_value = _machine(egress=egress or ())

        backend.create(spec=spec)

        assert _sent_spec(client).get("egress") == egress
        labels = _sent_spec(client)["labels"]
        assert json.loads(labels[NETWORK_TAG])["block_network"] is spec.block_network

    def test_create_waits_for_the_allowlist_only_when_there_is_one(self, clock):
        backend, client = _backend_with_client()
        backend.create(spec=SandboxSpec())
        assert clock[0] == _EGRESS_SETTLE

        clock[0] = 0.0
        client.machines.create.return_value = _machine(egress=())
        backend.create(spec=SandboxSpec(block_network=False))
        assert clock[0] == 0.0

    def test_env_size_snapshot_and_labels_are_sent(self, clock):
        backend, client = _backend_with_client(
            size="4x16",
            snapshot="golden",
            labels={"team": "data", OWNER_TAG: "forged"},
            name_prefix="etl",
            delete_after=600,
        )
        with mock.patch("airflow.providers.common.ai.sandbox.boxd.time.time", return_value=1000.0):
            backend.create(spec=SandboxSpec(env={"MODE": "test"}))

        assert client.machines.create.call_args.args[0].startswith("etl-")
        sent = _sent_spec(client)
        assert sent["env"] == {"MODE": "test"}
        assert sent["size"] == "4x16"
        assert sent["base"] == {"snapshot": "golden"}
        # The reserved labels overwrite the author's; an unset owner is not left forged.
        assert sent["labels"]["team"] == "data"
        assert OWNER_TAG not in sent["labels"]
        assert sent["labels"][EXPIRES_AT_TAG] == "1600"
        assert client.machines.create.call_args.kwargs["delete_after"] == 600

    def test_owner_is_stamped(self, clock):
        backend, client = _backend_with_client()
        backend.create(spec=SandboxSpec(owner="my_dag/run_1"))
        assert _sent_spec(client)["labels"][OWNER_TAG] == "my_dag/run_1"

    def test_no_delete_after_means_no_expiry(self, clock):
        backend, client = _backend_with_client(delete_after=None)
        backend.create(spec=SandboxSpec())
        assert client.machines.create.call_args.kwargs["delete_after"] is None
        assert EXPIRES_AT_TAG not in _sent_spec(client)["labels"]

    def test_a_network_policy_too_long_for_a_label_is_not_recorded(self, clock):
        backend, client = _backend_with_client()
        hosts = [f"host-{i}.example.com" for i in range(20)]
        client.machines.create.return_value = _machine(egress=hosts)

        backend.create(spec=SandboxSpec(allow_egress_to=hosts))

        sent = _sent_spec(client)
        assert sent["egress"] == hosts
        assert NETWORK_TAG not in sent["labels"]

    @pytest.mark.parametrize(
        ("spec", "message"),
        [
            pytest.param(
                SandboxSpec(block_network=False, allow_egress_to=["pypi.org"]),
                "leaves block_network False",
                id="hosts-with-open-network",
            ),
            pytest.param(
                SandboxSpec(block_network=False, allow_egress_to_cidrs=["203.0.113.0/24"]),
                "leaves block_network False",
                id="cidrs-with-open-network",
            ),
            pytest.param(SandboxSpec(env={"PORT": 8080}), r"\['PORT'\]", id="non-string-env"),  # type: ignore[dict-item]
            pytest.param(SandboxSpec(owner="d" * 300), "at most 256", id="long-owner"),
        ],
    )
    def test_refused_specs_provision_nothing(self, spec, message):
        backend, client = _backend_with_client()
        with pytest.raises(SandboxTerminalError, match=message):
            backend.create(spec=spec)
        client.machines.create.assert_not_called()

    def test_failed_create_deletes_by_the_name_it_sent(self, clock):
        backend, client = _backend_with_client()
        client.machines.create.side_effect = BoxdError("deadline exceeded", code="deadline_exceeded")

        with pytest.raises(SandboxTerminalError, match="boxd could not create a sandbox: deadline exceeded"):
            backend.create(spec=SandboxSpec())

        name = client.machines.create.call_args.args[0]
        client.machines.delete.assert_called_once_with(name)

    def test_machine_that_never_starts_is_deleted(self, clock):
        backend, client = _backend_with_client()
        client.machines.wait_until_ready.side_effect = TimeoutError("not ready")

        with pytest.raises(SandboxTerminalError, match="wait for a sandbox to start: TimeoutError"):
            backend.create(spec=SandboxSpec())

        client.machines.delete.assert_called_once_with(client.machines.create.call_args.args[0])

    @pytest.mark.parametrize(
        ("machine", "message"),
        [
            pytest.param(_machine(egress=()), "without the egress allowlist", id="no-allowlist"),
            pytest.param(_machine(isolated=False), "without isolation", id="not-isolated"),
        ],
    )
    def test_machine_without_the_asked_for_policy_is_deleted(self, clock, machine, message):
        backend, client = _backend_with_client()
        client.machines.create.return_value = machine

        with pytest.raises(SandboxTerminalError, match=message):
            backend.create(spec=SandboxSpec())

        client.machines.wait_until_ready.assert_not_called()
        client.machines.delete.assert_called_once_with(client.machines.create.call_args.args[0])

    def test_cleanup_failure_does_not_hide_the_create_error(self, clock):
        backend, client = _backend_with_client()
        client.machines.create.side_effect = BoxdError("boom", code="internal")
        client.machines.delete.side_effect = BoxdError("also down", code="unavailable")

        with pytest.raises(SandboxTerminalError, match="boom"):
            backend.create(spec=SandboxSpec())


class TestRunCommand:
    def test_wrapper_is_sent_as_argv_with_budget_and_deadline(self, clock):
        backend, client = _backend_with_client()
        client.machines.exec.return_value = ExecResult(stdout=b"hi\n", stderr=b"", exit_code=0)

        result = backend.run_command(_MACHINE_ID, "echo hi", timeout=9.2, max_output_bytes=100)

        args, kwargs = client.machines.exec.call_args
        assert args == (
            _MACHINE_ID,
            ["sh", "-c", _COMMAND_WRAPPER, "airflow-sandbox", "echo hi", "101", "10", "5"],
        )
        assert kwargs == {"timeout": 10 + 5 + 30, "encoding": "buffer"}
        assert result.exit_code == 0
        assert result.stdout == "hi\n"
        assert result.applied_timeout == 10.0
        assert not result.timed_out

    def test_streams_over_budget_keep_the_tail_and_are_marked(self, clock):
        backend, client = _backend_with_client()
        client.machines.exec.return_value = ExecResult(stdout=b"0123456789", stderr=b"abcd", exit_code=1)

        result = backend.run_command(_MACHINE_ID, "x", timeout=5, max_output_bytes=4)

        assert (result.stdout, result.stdout_truncated) == ("6789", True)
        assert (result.stderr, result.stderr_truncated) == ("abcd", False)
        assert result.exit_code == 1

    def test_text_output_is_bounded_in_bytes(self, clock):
        backend, client = _backend_with_client()
        client.machines.exec.return_value = ExecResult(stdout="héllo", stderr="", exit_code=0)
        result = backend.run_command(_MACHINE_ID, "x", timeout=5, max_output_bytes=5)
        assert (result.stdout, result.stdout_truncated) == ("éllo", True)

    def test_invalid_utf8_is_replaced_not_fatal(self, clock):
        backend, client = _backend_with_client()
        client.machines.exec.return_value = ExecResult(stdout=b"ok\xff\n", stderr=b"", exit_code=0)
        assert backend.run_command(_MACHINE_ID, "x", timeout=5, max_output_bytes=100).stdout == "ok�\n"

    @pytest.mark.parametrize(
        ("exit_code", "elapsed", "timed_out"),
        [
            pytest.param(124, 10, True, id="sigterm-at-deadline"),
            pytest.param(124, 3, False, id="early-124-is-the-command's-own"),
            pytest.param(137, 15, True, id="sigkill-after-grace"),
            pytest.param(137, 11, False, id="early-137-is-the-command's-own"),
            pytest.param(0, 10, False, id="success"),
        ],
    )
    def test_timeout_is_read_from_timeout_exit_status_and_elapsed(self, clock, exit_code, elapsed, timed_out):
        backend, client = _backend_with_client()

        def run(*args, **kwargs):
            clock[0] += elapsed
            return ExecResult(stdout=b"partial\n", stderr=b"", exit_code=exit_code)

        client.machines.exec.side_effect = run

        result = backend.run_command(_MACHINE_ID, "sleep 60", timeout=10, max_output_bytes=100)

        assert result.timed_out is timed_out
        assert result.stdout == "partial\n"
        assert not result.sandbox_terminated
        client.machines.delete.assert_not_called()

    def test_call_outliving_the_deadline_destroys_the_sandbox(self, clock):
        backend, client = _backend_with_client()

        def hang(*args, **kwargs):
            clock[0] += kwargs["timeout"]
            raise BoxdError(f"exec timed out after {kwargs['timeout']}s")

        client.machines.exec.side_effect = hang

        result = backend.run_command(_MACHINE_ID, "sleep 999", timeout=10, max_output_bytes=100)

        assert result.timed_out
        assert result.sandbox_terminated
        assert result.applied_timeout == 10.0
        client.machines.delete.assert_called_once_with(_MACHINE_ID)

    def test_unconfirmed_deletion_after_the_deadline_fails_the_task(self, clock):
        backend, client = _backend_with_client()

        def hang(*args, **kwargs):
            clock[0] += kwargs["timeout"]
            raise BoxdError("exec timed out")

        client.machines.exec.side_effect = hang
        client.machines.delete.side_effect = BoxdError("unavailable", code="unavailable")

        with pytest.raises(SandboxTerminalError, match="deletion of its sandbox could not be confirmed"):
            backend.run_command(_MACHINE_ID, "sleep 999", timeout=10, max_output_bytes=100)

    def test_gone_machine_is_terminal(self, clock):
        backend, client = _backend_with_client()
        client.machines.exec.side_effect = NotFoundError("VM is destroyed", code="not_found")

        with pytest.raises(SandboxTerminalError, match="VM is destroyed"):
            backend.run_command(_MACHINE_ID, "true", timeout=10, max_output_bytes=100)
        client.machines.delete.assert_not_called()

    @pytest.mark.parametrize("kwargs", [{"timeout": 0}, {"max_output_bytes": 0}])
    def test_invalid_bounds_are_refused(self, kwargs):
        backend, _ = _backend_with_client()
        with pytest.raises(ValueError, match=next(iter(kwargs))):
            backend.run_command(_MACHINE_ID, "true", **{"timeout": 1, "max_output_bytes": 1, **kwargs})


@pytest.mark.skipif(
    shutil.which("bash") is None or shutil.which("timeout") is None, reason="needs bash and GNU timeout"
)
class TestCommandWrapper:
    """Run the wrapper the way the machine does, through ``sh -c``, against the host's coreutils."""

    @staticmethod
    def _run(command, *, budget=1000, timeout=10, tmp_path=None):
        env = {"PATH": os.environ["PATH"]}
        if tmp_path is not None:
            env["TMPDIR"] = str(tmp_path)
        return subprocess.run(
            ["sh", "-c", _COMMAND_WRAPPER, "airflow-sandbox", command, str(budget), str(timeout), "1"],
            capture_output=True,
            timeout=60,
            check=False,
            env=env,
        )

    def test_streams_and_status_are_kept_apart(self, tmp_path):
        done = self._run("echo out; echo err >&2; exit 3", tmp_path=tmp_path)
        assert (done.returncode, done.stdout, done.stderr) == (3, b"out\n", b"err\n")
        # The scratch directory is removed.
        assert list(tmp_path.iterdir()) == []

    def test_command_runs_in_bash(self, tmp_path):
        assert self._run("[[ 1 == 1 ]] && echo yes", tmp_path=tmp_path).stdout == b"yes\n"

    def test_only_the_tail_is_returned(self, tmp_path):
        done = self._run("seq 1 1000", budget=10, tmp_path=tmp_path)
        assert done.stdout == b"\n999\n1000\n"

    def test_deadline_stops_the_command_and_keeps_its_output(self, tmp_path):
        started = time.monotonic()
        done = self._run("echo started; sleep 30; echo never", timeout=1, tmp_path=tmp_path)
        assert time.monotonic() - started < 10
        assert done.returncode == 124
        assert done.stdout == b"started\n"

    def test_background_process_does_not_hold_the_command(self, tmp_path):
        started = time.monotonic()
        done = self._run("sleep 20 & echo bg", tmp_path=tmp_path)
        assert done.stdout == b"bg\n"
        assert time.monotonic() - started < 10

    def test_stdin_is_closed(self, tmp_path):
        assert self._run("cat; echo eof", tmp_path=tmp_path).stdout == b"eof\n"


class TestFiles:
    def test_write_goes_through_the_file_api(self):
        backend, client = _backend_with_client()
        backend.write_file(_MACHINE_ID, "work/out.bin", b"\x00\x01")
        client.machines.fs.write.assert_called_once_with(_MACHINE_ID, "work/out.bin", b"\x00\x01")

    @pytest.mark.parametrize(
        "error",
        [
            MachinePermissionError("/etc/x: Permission denied", reason="permission_denied"),
            MachineFileNotFoundError("/proc/x: No such file or directory", reason="not_found"),
        ],
    )
    def test_write_refused_inside_the_machine_is_recoverable(self, error):
        backend, client = _backend_with_client()
        client.machines.fs.write.side_effect = error
        with pytest.raises(SandboxError) as excinfo:
            backend.write_file(_MACHINE_ID, "/etc/x", b"x")
        assert not isinstance(excinfo.value, SandboxTerminalError)
        assert str(error) in str(excinfo.value)

    def test_write_to_a_gone_machine_is_terminal(self):
        backend, client = _backend_with_client()
        client.machines.fs.write.side_effect = NotFoundError("VM is destroyed", code="not_found")
        with pytest.raises(SandboxTerminalError, match="write a sandbox file"):
            backend.write_file(_MACHINE_ID, "x", b"x")


class TestDestroy:
    def test_destroy_deletes(self):
        backend, client = _backend_with_client()
        backend.destroy(_MACHINE_ID)
        client.machines.delete.assert_called_once_with(_MACHINE_ID)

    def test_destroy_is_idempotent(self):
        backend, client = _backend_with_client()
        client.machines.delete.side_effect = NotFoundError("VM not found", code="not_found")
        backend.destroy(_MACHINE_ID)

    def test_other_failures_are_terminal(self):
        backend, client = _backend_with_client()
        client.machines.delete.side_effect = BoxdError("unavailable", code="unavailable")
        with pytest.raises(SandboxTerminalError, match="delete a sandbox"):
            backend.destroy(_MACHINE_ID)


class TestTags:
    def test_read_tags_returns_the_labels(self):
        backend, client = _backend_with_client()
        client.machines.get.return_value = _machine(labels={"team": "data"})
        assert backend.read_tags(_MACHINE_ID) == {"team": "data"}

    @pytest.mark.parametrize("status", ["destroyed", "failed"])
    def test_ended_machine_is_terminal(self, status):
        backend, client = _backend_with_client()
        client.machines.get.return_value = _machine(status=status)
        with pytest.raises(SandboxTerminalError, match="has ended"):
            backend.read_tags(_MACHINE_ID)

    def test_missing_machine_is_terminal(self):
        backend, client = _backend_with_client()
        client.machines.get.side_effect = NotFoundError("VM not found", code="not_found")
        with pytest.raises(SandboxTerminalError, match="VM not found"):
            backend.read_tags(_MACHINE_ID)

    def test_write_tags_replaces_the_whole_set(self):
        backend, client = _backend_with_client()
        backend.write_tags(_MACHINE_ID, {"a": "1"})
        client.machines.update_labels.assert_called_once_with(_MACHINE_ID, {"a": "1"}, replace_all=True)

    def test_write_tags_refuses_a_value_over_the_label_limit(self):
        backend, client = _backend_with_client()
        with pytest.raises(SandboxTerminalError, match=f"{HOLDER_TAG} label would be 300 characters"):
            backend.write_tags(_MACHINE_ID, {HOLDER_TAG: "h" * 300})
        client.machines.update_labels.assert_not_called()

    def test_attach_and_release_round_trip_through_labels(self):
        backend, client = _backend_with_client()
        store = {
            "team": "data",
            OWNER_TAG: "dag/run",
            EXPIRES_AT_TAG: str(int(time.time()) + 600),
            NETWORK_TAG: json.dumps(
                {"block_network": True, "allow_egress_to": ["pypi.org"], "allow_egress_to_cidrs": []}
            ),
        }
        client.machines.get.side_effect = lambda _id: _machine(labels=store)

        def update(_id, labels, *, replace_all):
            assert replace_all
            store.clear()
            store.update(labels)

        client.machines.update_labels.side_effect = update

        attached = backend.attach(_MACHINE_ID, owner="dag/run", holder="dag/run/agent")

        assert store[HOLDER_TAG] == "dag/run/agent"
        assert 0 < attached.remaining_lifetime <= 600
        assert attached.network.allow_egress_to == ["pypi.org"]
        with pytest.raises(SandboxTerminalError, match="already held"):
            backend.attach(_MACHINE_ID, owner="dag/run", holder="dag/run/other")

        backend.release(_MACHINE_ID, holder="dag/run/agent")

        assert HOLDER_TAG not in store
        assert store["team"] == "data"
