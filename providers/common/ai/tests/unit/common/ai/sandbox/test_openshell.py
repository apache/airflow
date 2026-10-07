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
import copy
import errno
import os
import shlex
import shutil
import signal
import subprocess
import sys
import time
from unittest import mock

import pytest

pytest.importorskip("openshell")

import grpc
from openshell import SandboxClient
from openshell._proto import openshell_pb2, sandbox_pb2

from airflow.providers.common.ai.sandbox.base import (
    SandboxError,
    SandboxFileTooLargeError,
    SandboxSpec,
    SandboxTerminalError,
)
from airflow.providers.common.ai.sandbox.openshell import (
    _EXEC_GRACE,
    _NO_SETSID,
    _NO_SETSID_STATUS,
    _RUN_WRAPPER,
    _STAGING_FAILED,
    _SYSTEM_PATH,
    OpenShellSandboxBackend,
)

_MODULE = "airflow.providers.common.ai.sandbox.openshell"
_MONOTONIC_PATH = f"{_MODULE}.time.monotonic"
_GATEWAY_DIR = "/home/airflow/.config/openshell/gateways/prod"
# The temp file the SDK writes a refreshed OIDC token through, next to the registration.
_TOKEN_TEMP_FILE = f"{_GATEWAY_DIR}/.oidc_token.k2x9w1qz.tmp"


class _RpcError(grpc.RpcError):
    def __init__(self, code: grpc.StatusCode, details: str = "") -> None:
        super().__init__(details)
        self._code = code
        self._details = details

    def code(self) -> grpc.StatusCode:
        return self._code

    def details(self) -> str:
        return self._details


class _Stream:
    """An ExecSandbox response stream: events, optionally ending in an error."""

    def __init__(self, events, error: Exception | None = None) -> None:
        self._events = iter(events)
        self._error = error
        self.cancel = mock.Mock()

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return next(self._events)
        except StopIteration:
            if self._error is not None:
                raise self._error from None
            raise


def _stdout(data: bytes):
    return openshell_pb2.ExecSandboxEvent(stdout=openshell_pb2.ExecSandboxStdout(data=data))


def _stderr(data: bytes):
    return openshell_pb2.ExecSandboxEvent(stderr=openshell_pb2.ExecSandboxStderr(data=data))


def _exit(code: int):
    return openshell_pb2.ExecSandboxEvent(exit=openshell_pb2.ExecSandboxExit(exit_code=code))


def _result(code: int = 0, out: bytes = b"", err: bytes = b"") -> _Stream:
    events = []
    if out:
        events.append(_stdout(out))
    if err:
        events.append(_stderr(err))
    events.append(_exit(code))
    return _Stream(events)


def _sandbox(phase: openshell_pb2.SandboxPhase = openshell_pb2.SANDBOX_PHASE_READY, *conditions):
    response = openshell_pb2.SandboxResponse()
    response.sandbox.status.phase = phase
    for reason, message in conditions:
        response.sandbox.status.conditions.add(type="Ready", status="False", reason=reason, message=message)
    return response


def _config(
    hosts=(),
    *,
    source: sandbox_pb2.PolicySource = sandbox_pb2.POLICY_SOURCE_SANDBOX,
    admitted: bool = True,
    landlock: str = "hard_requirement",
    approval_mode: str | None = None,
    proposals: bool = False,
    extra_rule: tuple[str, int] | None = None,
    allowed_ips=(),
    binaries=("/**",),
    middlewares=(),
):
    config = sandbox_pb2.GetSandboxConfigResponse(
        version=1, policy_source=source, configuration_admitted=admitted
    )
    config.policy.landlock.compatibility = landlock
    for name in middlewares:
        # Reading a missing key of a message map adds it.
        config.policy.network_middlewares[name]
    if hosts or allowed_ips:
        rule = config.policy.network_policies["airflow-egress"]
        rule.name = "airflow-egress"
        for host in hosts:
            rule.endpoints.add(host=host, ports=[443], allowed_ips=list(allowed_ips))
        if not hosts:
            rule.endpoints.add(ports=[443], allowed_ips=list(allowed_ips))
        rule.binaries.extend(sandbox_pb2.NetworkBinary(path=path) for path in binaries)
    if extra_rule is not None:
        rule = config.policy.network_policies["allow_extra"]
        rule.endpoints.add(host=extra_rule[0], port=extra_rule[1])
        rule.binaries.add(path="/usr/local/bin/python3.12")
    if approval_mode is not None:
        config.settings["proposal_approval_mode"].value.string_value = approval_mode
        config.settings["proposal_approval_mode"].scope = sandbox_pb2.SETTING_SCOPE_GLOBAL
    if proposals:
        config.settings["agent_policy_proposals_enabled"].value.bool_value = True
    return config


def _backend(**kwargs) -> tuple[OpenShellSandboxBackend, mock.MagicMock]:
    backend = OpenShellSandboxBackend(gateway="test-gateway", **kwargs)
    client = mock.MagicMock(spec=SandboxClient)
    client._stub = mock.MagicMock(spec=["GetSandbox", "GetSandboxConfig", "ExecSandbox"])
    client._stub.GetSandbox.return_value = _sandbox()
    client._stub.GetSandboxConfig.return_value = _config()
    client._stub.ExecSandbox.return_value = _result()
    backend._client = client
    return backend, client


def _exec_request(client, call: int = -1):
    return client._stub.ExecSandbox.call_args_list[call].args[0]


@pytest.fixture(autouse=True)
def _no_sleep():
    with mock.patch(f"{_MODULE}.time.sleep", autospec=True):
        yield


def test_missing_sdk_error_is_actionable():
    real_import = builtins.__import__

    def blocked_import(name, *args, **kwargs):
        if name.startswith("openshell"):
            raise ImportError("blocked for test")
        return real_import(name, *args, **kwargs)

    backend = OpenShellSandboxBackend()
    with mock.patch("builtins.__import__", side_effect=blocked_import):
        with pytest.raises(SandboxTerminalError, match=r"\[openshell\]"):
            backend.create()


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"gateway": ""}, "gateway"),
        ({"workspace": ""}, "workspace"),
        ({"image": ""}, "image"),
        ({"cpu": ""}, "cpu"),
        ({"memory": ""}, "memory"),
        ({"ready_timeout": 0}, "ready_timeout"),
        ({"request_timeout": float("inf")}, "request_timeout"),
    ],
)
def test_constructor_rejects_invalid_values(kwargs, message):
    with pytest.raises(ValueError, match=message):
        OpenShellSandboxBackend(**kwargs)


class TestClient:
    @mock.patch("openshell.SandboxClient.from_active_cluster", autospec=True)
    def test_construction_reads_no_gateway_registration(self, from_active_cluster):
        OpenShellSandboxBackend(gateway="prod")

        from_active_cluster.assert_not_called()

    def test_the_backend_can_be_deep_copied(self):
        backend = OpenShellSandboxBackend(gateway="prod")

        assert copy.deepcopy(backend)._gateway == "prod"

    @mock.patch("openshell.SandboxClient.from_active_cluster", autospec=True)
    def test_the_cli_gateway_registration_is_loaded_once(self, from_active_cluster):
        backend = OpenShellSandboxBackend(gateway="prod", request_timeout=12)

        assert backend._get_client() is backend._get_client()

        from_active_cluster.assert_called_once_with(cluster="prod", timeout=12)

    @mock.patch("openshell.SandboxClient.from_active_cluster", autospec=True)
    def test_a_missing_registration_is_terminal(self, from_active_cluster):
        from openshell import SandboxError as SdkError

        from_active_cluster.side_effect = SdkError("gateway 'prod' not found")

        with pytest.raises(SandboxTerminalError, match="gateway 'prod' not found"):
            OpenShellSandboxBackend(gateway="prod")._get_client()

    @pytest.mark.parametrize(
        "error",
        [
            pytest.param(
                PermissionError(errno.EACCES, "Permission denied", _TOKEN_TEMP_FILE), id="not-writable"
            ),
            pytest.param(
                OSError(errno.EROFS, "Read-only file system", _TOKEN_TEMP_FILE), id="read-only-mount"
            ),
        ],
    )
    def test_an_oidc_token_that_cannot_be_written_back_is_terminal(self, error):
        backend, client = _backend()
        client._stub.GetSandboxConfig.side_effect = error

        with pytest.raises(SandboxTerminalError, match="registration is read-only.*mTLS is the supported"):
            backend.run_command("box", "true", timeout=5, max_output_bytes=100)

        client._stub.ExecSandbox.assert_not_called()

    @pytest.mark.parametrize(
        "error",
        [
            pytest.param(
                PermissionError(errno.EACCES, "Permission denied", f"{_GATEWAY_DIR}/mtls/tls.key"),
                id="an-unreadable-mtls-key",
            ),
            pytest.param(
                PermissionError(errno.EACCES, "Permission denied", f"{_GATEWAY_DIR}/oidc_token.json"),
                id="the-token-file-itself",
            ),
            pytest.param(OSError(errno.ENOSPC, "No space left on device"), id="no-file-named"),
        ],
    )
    def test_other_os_errors_are_not_reported_as_a_read_only_registration(self, error):
        backend, client = _backend()
        client._stub.GetSandboxConfig.side_effect = error

        with pytest.raises(SandboxTerminalError) as raised:
            backend.run_command("box", "true", timeout=5, max_output_bytes=100)

        assert str(raised.value) == (
            f"OpenShell could not read the network policy of sandbox box ({type(error).__name__}: {error})."
        )


class TestSpecRefusals:
    @pytest.mark.parametrize(
        ("spec", "message"),
        [
            (SandboxSpec(owner="dag/run"), "owner"),
            (SandboxSpec(allow_egress_to_cidrs=["203.0.113.0/24"]), "allow_egress_to_cidrs"),
            (SandboxSpec(block_network=False), "open network"),
            (SandboxSpec(block_network=False, allow_egress_to=["pypi.org"]), "open network"),
            (SandboxSpec(allow_egress_to="pypi.org"), "not one string"),
            (SandboxSpec(env={"FOO": 1}), "strings to strings"),  # type: ignore[dict-item]
        ],
    )
    def test_unenforceable_specs_are_refused_before_the_gateway_is_asked(self, spec, message):
        backend, client = _backend()

        with pytest.raises(SandboxTerminalError, match=message):
            backend.create(spec=spec)

        client.create.assert_not_called()

    @pytest.mark.parametrize(
        "host",
        ["https://pypi.org", "pypi.org:443", "203.0.113.7", "localhost", "*.com", "*", "pypi.org/simple", ""],
    )
    def test_entries_that_are_not_hostnames_are_refused(self, host):
        backend, client = _backend()

        with pytest.raises(SandboxTerminalError, match="bare hostnames"):
            backend.create(spec=SandboxSpec(allow_egress_to=[host]))

        client.create.assert_not_called()

    @pytest.mark.parametrize(
        "key", ["HTTPS_PROXY", "no_proxy", "SSL_CERT_FILE", "REQUESTS_CA_BUNDLE", "OPENSHELL_SANDBOX"]
    )
    def test_env_the_supervisor_would_silently_change_is_refused(self, key):
        backend, client = _backend()

        with pytest.raises(SandboxTerminalError, match=f"sets {key}"):
            backend.create(spec=SandboxSpec(env={key: "value"}))

        client.create.assert_not_called()


class TestCreate:
    def test_default_spec_is_deny_all_under_hard_landlock(self):
        backend, client = _backend(image="python:3.13-slim", cpu="2", memory="4Gi")

        with mock.patch(f"{_MODULE}.time.time", return_value=1_700_000_000.4):
            name = backend.create(spec=SandboxSpec(env={"TOKEN": "value"}))

        kwargs = client.create.call_args.kwargs
        assert kwargs["name"] == name
        assert name.startswith("airflow-")
        assert len(name) <= 19
        assert kwargs["workspace"] == "default"
        assert kwargs["labels"] == {"created-by": "airflow", "airflow-created-at": "1700000000"}
        spec = kwargs["spec"]
        assert dict(spec.environment) == {"TOKEN": "value"}
        assert spec.template.image == "python:3.13-slim"
        assert dict(spec.template.resources)["limits"] == {"cpu": "2", "memory": "4Gi"}
        assert len(spec.policy.network_policies) == 0
        assert spec.policy.landlock.compatibility == "hard_requirement"
        assert spec.policy.filesystem.include_workdir is True
        assert list(spec.policy.filesystem.read_only) == [
            "/bin",
            "/usr",
            "/lib",
            "/proc",
            "/dev/urandom",
            "/etc",
            "/var/log",
        ]
        assert list(spec.policy.filesystem.read_write) == ["/tmp", "/dev/null", "/dev/shm"]
        client.delete.assert_not_called()

    def test_none_spec_still_gets_an_explicit_deny_all_policy(self):
        # Without a policy OpenShell falls back to one baked into the image, which may be open.
        backend, client = _backend()

        backend.create()

        spec = client.create.call_args.kwargs["spec"]
        assert spec.HasField("policy")
        assert len(spec.policy.network_policies) == 0

    def test_hostnames_become_one_rule_on_port_443_for_any_binary(self):
        backend, client = _backend()
        client._stub.GetSandboxConfig.return_value = _config(["files.pythonhosted.org", "pypi.org"])

        backend.create(spec=SandboxSpec(allow_egress_to=["PyPI.org", "files.pythonhosted.org", "pypi.org"]))

        rules = client.create.call_args.kwargs["spec"].policy.network_policies
        assert list(rules) == ["airflow-egress"]
        assert [(endpoint.host, list(endpoint.ports)) for endpoint in rules["airflow-egress"].endpoints] == [
            ("files.pythonhosted.org", [443]),
            ("pypi.org", [443]),
        ]
        assert [binary.path for binary in rules["airflow-egress"].binaries] == ["/**"]
        client.delete.assert_not_called()

    def test_waits_for_the_sandbox_to_become_ready(self):
        backend, client = _backend()
        client._stub.GetSandbox.side_effect = [
            _sandbox(openshell_pb2.SANDBOX_PHASE_PROVISIONING),
            _RpcError(grpc.StatusCode.UNAVAILABLE, "restarting"),
            _sandbox(),
        ]

        backend.create(spec=SandboxSpec())

        assert client._stub.GetSandbox.call_count == 3

    def test_a_sandbox_that_fails_to_start_is_deleted_and_terminal(self):
        backend, client = _backend()
        client._stub.GetSandbox.return_value = _sandbox(
            openshell_pb2.SANDBOX_PHASE_ERROR, ("IdentityResolutionFailed", "no passwd entry")
        )

        with pytest.raises(SandboxTerminalError, match="IdentityResolutionFailed: no passwd entry"):
            backend.create(spec=SandboxSpec())

        name = client.create.call_args.kwargs["name"]
        client.delete.assert_called_once_with(name, workspace="default", allow_missing=True)

    def test_a_sandbox_not_ready_in_time_is_deleted_and_terminal(self):
        backend, client = _backend(ready_timeout=5)
        client._stub.GetSandbox.return_value = _sandbox(openshell_pb2.SANDBOX_PHASE_PROVISIONING)

        with mock.patch(_MONOTONIC_PATH, side_effect=[0.0, 1.0, 6.0]):
            with pytest.raises(SandboxTerminalError, match="not ready: it is SANDBOX_PHASE_PROVISIONING"):
                backend.create(spec=SandboxSpec())

        client.delete.assert_called_once()

    def test_a_create_the_gateway_rejects_is_terminal_and_cleaned_up(self):
        backend, client = _backend()
        client.create.side_effect = _RpcError(grpc.StatusCode.INVALID_ARGUMENT, "name exceeds maximum length")

        with pytest.raises(SandboxTerminalError, match="INVALID_ARGUMENT: name exceeds maximum length"):
            backend.create(spec=SandboxSpec())

        client.delete.assert_called_once()

    @pytest.mark.parametrize(
        ("config", "message"),
        [
            (_config(source=sandbox_pb2.POLICY_SOURCE_GLOBAL), "gateway-wide policy"),
            (_config(["pypi.org", "example.com"]), "admits"),
            (_config(), "admits nothing where"),
            (_config(extra_rule=("www.google.com", 443)), "www.google.com:443"),
            (_config(["pypi.org"], binaries=("/usr/bin/curl",)), "admits"),
            (_config(allowed_ips=["0.0.0.0/1"]), "admits addresses"),
            (_config(["pypi.org"], approval_mode="auto"), "proposal_approval_mode is 'auto'"),
            (_config(["pypi.org"], proposals=True), "agent_policy_proposals_enabled"),
            (_config(["pypi.org"], landlock="best_effort"), "Landlock"),
            (_config(["pypi.org"], admitted=False), "not admitted"),
            (_config(["pypi.org"], middlewares=["inspect"]), "network middlewares"),
            (_RpcError(grpc.StatusCode.PERMISSION_DENIED, "config:read"), "PERMISSION_DENIED"),
        ],
        ids=[
            "global-override",
            "wider-allowlist",
            "missing-rule",
            "auto-approved-rule",
            "narrower-binaries",
            "address-rule",
            "auto-approval",
            "agent-proposals",
            "landlock-best-effort",
            "not-admitted",
            "network-middlewares",
            "unreadable",
        ],
    )
    def test_a_policy_other_than_the_requested_one_destroys_the_sandbox(self, config, message):
        backend, client = _backend()
        if isinstance(config, Exception):
            client._stub.GetSandboxConfig.side_effect = config
        else:
            client._stub.GetSandboxConfig.return_value = config

        with pytest.raises(SandboxTerminalError, match=message):
            backend.create(spec=SandboxSpec(allow_egress_to=["pypi.org"]))

        client.delete.assert_called_once()
        assert backend._egress == {}

    @pytest.mark.parametrize("code", [grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED])
    def test_a_policy_read_rides_out_a_restarting_gateway(self, code):
        backend, client = _backend()
        client._stub.GetSandboxConfig.side_effect = [_RpcError(code, "restarting"), _config()]

        backend.create(spec=SandboxSpec())

        assert client._stub.GetSandboxConfig.call_count == 2
        client.delete.assert_not_called()

    def test_manual_approval_mode_is_accepted(self):
        backend, client = _backend()
        client._stub.GetSandboxConfig.return_value = _config(approval_mode="manual")

        backend.create(spec=SandboxSpec())

        client.delete.assert_not_called()


class TestRunCommand:
    def test_the_command_travels_on_stdin_to_the_guest_wrapper(self):
        backend, client = _backend()
        client._stub.ExecSandbox.return_value = _result(3, b"out\n", b"err\n")

        result = backend.run_command(
            "box", "echo out; echo err >&2; exit 3", timeout=2.5, max_output_bytes=100
        )

        request = _exec_request(client)
        assert request.sandbox == "box"
        assert list(request.command) == ["/bin/sh", "-c", _RUN_WRAPPER, "airflow-exec", "3", "100"]
        assert request.stdin == b"echo out; echo err >&2; exit 3"
        assert request.no_login_shell is True
        assert request.request_id
        assert not request.HasField("execution_timeout")
        assert client._stub.ExecSandbox.call_args.kwargs["timeout"] == 3 + _EXEC_GRACE
        assert (result.exit_code, result.stdout, result.stderr) == (3, "out\n", "err\n")
        assert not result.timed_out
        assert not result.sandbox_terminated

    @pytest.mark.parametrize(
        ("exit_code", "elapsed", "timed_out"),
        [(124, 3.1, True), (124, 0.2, False), (137, 3.1, False), (0, 3.1, False)],
    )
    def test_only_the_wrappers_status_after_the_budget_is_a_timeout(self, exit_code, elapsed, timed_out):
        backend, client = _backend()
        client._stub.ExecSandbox.return_value = _result(exit_code)

        # The policy read before and after the command each take a deadline reading too.
        with mock.patch(_MONOTONIC_PATH, side_effect=[0.0, 100.0, 100.0 + elapsed, 0.0]):
            result = backend.run_command("box", "sleep 300", timeout=3, max_output_bytes=100)

        assert result.timed_out is timed_out

    @pytest.mark.parametrize(
        ("exit_code", "stderr"),
        [
            pytest.param(125, b"docker: invalid reference format\n", id="a-command-exiting-125"),
            pytest.param(1, f"{_STAGING_FAILED}\n".encode(), id="the-staging-line-with-another-status"),
            pytest.param(125, f"{_STAGING_FAILED}\nmore\n".encode(), id="the-staging-line-not-last"),
            pytest.param(127, b"sh: 1: setsid: not found\n", id="a-command-exiting-127"),
            pytest.param(1, f"{_NO_SETSID}\n".encode(), id="the-setsid-line-with-another-status"),
            pytest.param(127, f"{_NO_SETSID}\nmore\n".encode(), id="the-setsid-line-not-last"),
        ],
    )
    def test_a_result_unlike_a_wrapper_failure_is_the_commands_own(self, exit_code, stderr):
        backend, client = _backend()
        client._stub.ExecSandbox.return_value = _result(exit_code, err=stderr)

        result = backend.run_command("box", "cmd", timeout=5, max_output_bytes=100)

        assert (result.exit_code, result.stderr) == (exit_code, stderr.decode())

    @pytest.mark.parametrize(
        ("max_output_bytes", "message"),
        [
            pytest.param(
                100,
                "The command was not run: the sandbox could not write it to /tmp "
                "(cat: write error: No space left on device).",
                id="with-the-cause",
            ),
            pytest.param(
                10,
                "The command was not run: the sandbox could not write it to /tmp.",
                id="under-a-cap-shorter-than-the-staging-line",
            ),
        ],
    )
    def test_the_wrappers_own_staging_failure_is_a_recoverable_error(self, max_output_bytes, message):
        backend, client = _backend()
        client._stub.ExecSandbox.return_value = _result(
            125, err=f"cat: write error: No space left on device\n{_STAGING_FAILED}\n".encode()
        )

        with pytest.raises(SandboxError) as error:
            backend.run_command("box", "make", timeout=5, max_output_bytes=max_output_bytes)

        assert str(error.value) == message
        assert not isinstance(error.value, SandboxTerminalError)

    def test_an_image_without_setsid_is_terminal(self):
        backend, client = _backend()
        client._stub.ExecSandbox.return_value = _result(_NO_SETSID_STATUS, err=f"{_NO_SETSID}\n".encode())

        # A cap shorter than the line, so the line is still recognised past it.
        with pytest.raises(SandboxTerminalError, match="no setsid.*util-linux"):
            backend.run_command("box", "true", timeout=5, max_output_bytes=10)

    def test_stderr_stays_capped_below_the_length_of_the_staging_line(self):
        backend, client = _backend()
        client._stub.ExecSandbox.return_value = _result(1, err=b"0123456789" * 3)

        result = backend.run_command("box", "cmd", timeout=5, max_output_bytes=20)

        assert (result.stderr, result.stderr_truncated) == ("0123456789" * 2, True)

    def test_each_stream_is_capped_and_flagged_on_its_own(self):
        backend, client = _backend()
        # The wrapper sends one byte past the cap for a stream it had to cut.
        client._stub.ExecSandbox.return_value = _Stream(
            [_stdout(b"partial line\nkept 1\n"), _stdout(b"kept 2\n"), _stderr(b"short\n"), _exit(0)]
        )

        result = backend.run_command("box", "cmd", timeout=5, max_output_bytes=20)

        assert result.stdout == "kept 1\nkept 2\n"
        assert result.stdout_truncated
        assert result.stderr == "short\n"
        assert not result.stderr_truncated

    def test_one_line_longer_than_the_budget_still_reaches_the_model(self):
        """Dropping the leading partial line must not empty the window: the toolset prints "(no output)" for it."""
        backend, client = _backend()
        client._stub.ExecSandbox.return_value = _Stream([_stdout(b"x" * 60000 + b"\n"), _exit(0)])

        result = backend.run_command("box", "cat big.json", timeout=5, max_output_bytes=51200)

        assert result.stdout != ""
        assert result.stdout_truncated
        assert len(result.stdout.encode()) <= 51200

    def test_a_long_line_followed_by_a_short_one_keeps_the_window(self):
        backend, client = _backend()
        client._stub.ExecSandbox.return_value = _Stream(
            [_stdout(b"L" * 200_000 + b"\nshort tail\n"), _exit(0)]
        )

        result = backend.run_command("box", "spew", timeout=5, max_output_bytes=51200)

        assert len(result.stdout.encode()) > 51200 // 2
        assert result.stdout.endswith("short tail\n")

    def test_the_whole_second_deadline_the_command_got_is_reported(self):
        backend, client = _backend()

        result = backend.run_command("box", "true", timeout=2.2, max_output_bytes=100)

        assert result.applied_timeout == 3.0

    def test_the_whole_second_deadline_is_reported_for_an_abandoned_command_too(self):
        backend, client = _backend()
        client._stub.ExecSandbox.return_value = _Stream([], _RpcError(grpc.StatusCode.DEADLINE_EXCEEDED))

        result = backend.run_command("box", "sleep 300", timeout=2.2, max_output_bytes=100)

        assert (result.sandbox_terminated, result.applied_timeout) == (True, 3.0)

    def test_output_injected_past_the_wrapper_stays_bounded_in_worker_memory(self):
        backend, client = _backend()
        chunk = b"y\n" * 32768
        client._stub.ExecSandbox.return_value = _Stream([_stdout(chunk) for _ in range(200)] + [_exit(0)])

        result = backend.run_command("box", "yes > /proc/$PPID/fd/1", timeout=5, max_output_bytes=1024)

        assert len(result.stdout.encode()) <= 1024
        assert result.stdout_truncated

    def test_undecodable_output_is_replaced_not_raised(self):
        backend, client = _backend()
        client._stub.ExecSandbox.return_value = _result(0, b"\xff\xfeok\n")

        assert backend.run_command("box", "cmd", timeout=5, max_output_bytes=100).stdout == "��ok\n"

    def test_a_command_over_the_request_limit_is_recoverable_and_not_sent(self):
        backend, client = _backend()

        with pytest.raises(SandboxError, match="write_file") as error:
            backend.run_command("box", "x" * 1_000_001, timeout=5, max_output_bytes=100)

        assert not isinstance(error.value, SandboxTerminalError)
        client._stub.ExecSandbox.assert_not_called()

    def test_a_hung_exec_destroys_the_sandbox(self):
        backend, client = _backend()
        client._stub.ExecSandbox.return_value = _Stream([], _RpcError(grpc.StatusCode.DEADLINE_EXCEEDED))

        result = backend.run_command("box", "sleep 300", timeout=3, max_output_bytes=100)

        assert (result.exit_code, result.timed_out, result.sandbox_terminated) == (-1, True, True)
        client.delete.assert_called_once_with("box", workspace="default", allow_missing=True)

    def test_a_sandbox_not_ready_after_a_gateway_restart_is_waited_for_and_retried(self):
        backend, client = _backend()
        client._stub.ExecSandbox.side_effect = [
            _RpcError(grpc.StatusCode.FAILED_PRECONDITION, "sandbox is not ready"),
            _result(0, b"ok\n"),
        ]
        client._stub.GetSandbox.side_effect = [_sandbox(openshell_pb2.SANDBOX_PHASE_PROVISIONING), _sandbox()]

        result = backend.run_command("box", "echo ok", timeout=5, max_output_bytes=100)

        assert result.stdout == "ok\n"
        assert client._stub.ExecSandbox.call_count == 2
        assert _exec_request(client, 0).request_id != _exec_request(client, 1).request_id

    def test_a_gateway_drop_during_the_not_ready_retry_is_reported_not_leaked(self):
        backend, client = _backend()
        client._stub.ExecSandbox.side_effect = [
            _RpcError(grpc.StatusCode.FAILED_PRECONDITION, "sandbox is not ready"),
            _Stream([], _RpcError(grpc.StatusCode.UNAVAILABLE, "exec relay closed")),
        ]

        with pytest.raises(SandboxError, match="may or may not have run") as error:
            backend.run_command("box", "make install", timeout=5, max_output_bytes=100)

        assert not isinstance(error.value, SandboxTerminalError)
        assert client._stub.ExecSandbox.call_count == 2

    def test_an_exec_the_gateway_drops_is_reported_not_retried(self):
        backend, client = _backend()
        client._stub.ExecSandbox.return_value = _Stream(
            [],
            _RpcError(
                grpc.StatusCode.UNAVAILABLE, "exec relay closed before the command reported an exit status"
            ),
        )
        client._stub.GetSandbox.side_effect = [_RpcError(grpc.StatusCode.UNAVAILABLE), _sandbox()]

        with pytest.raises(SandboxError, match="may or may not have run") as error:
            backend.run_command("box", "make install", timeout=5, max_output_bytes=100)

        assert not isinstance(error.value, SandboxTerminalError)
        client._stub.ExecSandbox.assert_called_once()

    def test_a_dropped_exec_on_a_sandbox_that_does_not_come_back_is_terminal(self):
        backend, client = _backend()
        client._stub.ExecSandbox.return_value = _Stream([_stdout(b"x")])
        client._stub.GetSandbox.return_value = _sandbox(openshell_pb2.SANDBOX_PHASE_ERROR)

        with pytest.raises(SandboxTerminalError, match="SANDBOX_PHASE_ERROR"):
            backend.run_command("box", "cmd", timeout=5, max_output_bytes=100)

    @pytest.mark.parametrize(
        ("code", "terminal"),
        [
            (grpc.StatusCode.NOT_FOUND, True),
            (grpc.StatusCode.UNAUTHENTICATED, True),
            (grpc.StatusCode.PERMISSION_DENIED, True),
            (grpc.StatusCode.OUT_OF_RANGE, False),
            (grpc.StatusCode.RESOURCE_EXHAUSTED, False),
        ],
    )
    def test_gateway_errors_are_classified(self, code, terminal):
        backend, client = _backend()
        client._stub.ExecSandbox.side_effect = _RpcError(code, "detail")

        with pytest.raises(SandboxError, match=f"{code.name}: detail") as error:
            backend.run_command("box", "cmd", timeout=5, max_output_bytes=100)

        assert isinstance(error.value, SandboxTerminalError) is terminal
        assert client._stub.ExecSandbox.call_count == 1

    def test_egress_widened_before_the_command_is_terminal_and_nothing_runs(self):
        backend, client = _backend()
        backend.create(spec=SandboxSpec())
        name = client.create.call_args.kwargs["name"]
        client._stub.GetSandboxConfig.return_value = _config(extra_rule=("www.google.com", 443))

        with pytest.raises(SandboxTerminalError, match="changed after it was created"):
            backend.run_command(name, "curl https://www.google.com", timeout=5, max_output_bytes=100)

        client._stub.ExecSandbox.assert_not_called()
        client.delete.assert_called_once_with(name, workspace="default", allow_missing=True)

    def test_egress_widened_during_the_command_is_terminal(self):
        backend, client = _backend()
        backend.create(spec=SandboxSpec())
        name = client.create.call_args.kwargs["name"]
        client._stub.GetSandboxConfig.side_effect = [
            _config(),
            _config(source=sandbox_pb2.POLICY_SOURCE_GLOBAL),
        ]

        with pytest.raises(SandboxTerminalError, match="gateway-wide policy"):
            backend.run_command(name, "sleep 20", timeout=30, max_output_bytes=100)

        client._stub.ExecSandbox.assert_called_once()

    @mock.patch(_MONOTONIC_PATH, side_effect=[0.0, 59.0, 61.0])
    def test_a_gateway_down_past_the_recovery_window_is_terminal_and_nothing_runs(self, _):
        backend, client = _backend()
        client._stub.GetSandboxConfig.side_effect = [_RpcError(grpc.StatusCode.UNAVAILABLE, "down")] * 3

        with pytest.raises(SandboxTerminalError, match="UNAVAILABLE: down"):
            backend.run_command("box", "true", timeout=5, max_output_bytes=100)

        assert client._stub.GetSandboxConfig.call_count == 2
        client._stub.ExecSandbox.assert_not_called()

    def test_a_handle_created_elsewhere_is_held_to_what_it_admits_when_first_seen(self):
        backend, client = _backend()
        client._stub.GetSandboxConfig.side_effect = [
            _config(["pypi.org"]),
            _config(["pypi.org"]),
            _config(["pypi.org", "evil.example.com"]),
        ]

        backend.run_command("other", "true", timeout=5, max_output_bytes=100)
        with pytest.raises(SandboxTerminalError, match="evil.example.com"):
            backend.run_command("other", "true", timeout=5, max_output_bytes=100)

    def test_a_stream_without_an_exit_status_is_not_a_success(self):
        backend, client = _backend()
        client._stub.ExecSandbox.return_value = _Stream([_stdout(b"half")])

        with pytest.raises(SandboxError, match="without an exit status"):
            backend.run_command("box", "cmd", timeout=5, max_output_bytes=100)


class TestFiles:
    def test_read_returns_raw_bytes_after_the_size_line(self):
        backend, client = _backend()
        payload = bytes(range(256))
        client._stub.ExecSandbox.return_value = _result(0, b"256\n" + payload)

        assert backend.read_file("box", "/tmp/data.bin", max_bytes=1000) == payload

        request = _exec_request(client)
        assert list(request.command[-2:]) == ["/tmp/data.bin", "1001"]
        assert "head -c" in request.command[2]

    def test_an_oversized_read_reports_the_real_size(self):
        backend, client = _backend()
        client._stub.ExecSandbox.return_value = _result(0, b"10000000\n" + b"x" * 11)

        with pytest.raises(SandboxFileTooLargeError) as error:
            backend.read_file("box", "/tmp/big", max_bytes=10)

        assert (error.value.size_bytes, error.value.max_bytes) == (10_000_000, 10)

    def test_a_stream_reporting_no_size_is_still_over_budget(self):
        backend, client = _backend()
        client._stub.ExecSandbox.return_value = _result(0, b"0\n" + b"z" * 11)

        with pytest.raises(SandboxFileTooLargeError) as error:
            backend.read_file("box", "/proc/self/fd/0", max_bytes=10)

        assert error.value.size_bytes == 11

    @pytest.mark.parametrize(
        ("exit_code", "stderr", "message"),
        [
            (66, b"", "does not exist"),
            (67, b"", "is a directory"),
            (1, b"head: cannot open '/dev/zero' for reading: Permission denied\n", "Permission denied"),
        ],
    )
    def test_read_failures_are_recoverable(self, exit_code, stderr, message):
        backend, client = _backend()
        client._stub.ExecSandbox.return_value = _result(exit_code, err=stderr)

        with pytest.raises(SandboxError, match=message) as error:
            backend.read_file("box", "/x", max_bytes=10)

        assert not isinstance(error.value, SandboxTerminalError)

    def test_a_hung_file_operation_is_recoverable_and_keeps_the_sandbox(self):
        backend, client = _backend()
        client._stub.ExecSandbox.return_value = _Stream([], _RpcError(grpc.StatusCode.DEADLINE_EXCEEDED))

        with pytest.raises(SandboxError, match="did not finish") as error:
            backend.read_file("box", "/x", max_bytes=10)

        assert not isinstance(error.value, SandboxTerminalError)
        client.delete.assert_not_called()

    def test_write_streams_content_on_stdin_in_chunks(self):
        backend, client = _backend()
        client._stub.ExecSandbox.side_effect = lambda *args, **kwargs: _result(0)
        content = bytes(range(256)) * 5

        with mock.patch(f"{_MODULE}._WRITE_CHUNK_BYTES", 512):
            backend.write_file("box", "/sandbox/dir/file.bin", content)

        requests = [call.args[0] for call in client._stub.ExecSandbox.call_args_list]
        assert b"".join(request.stdin for request in requests) == content
        assert [len(request.stdin) for request in requests] == [512, 512, 256]
        assert "mkdir -p" in requests[0].command[2]
        assert 'cat >"$1"' in requests[0].command[2]
        assert all('cat >>"$1"' in request.command[2] for request in requests[1:])
        assert all(request.command[-1] == "/sandbox/dir/file.bin" for request in requests)

    def test_writing_empty_content_still_creates_the_file(self):
        backend, client = _backend()

        backend.write_file("box", "/tmp/empty", b"")

        client._stub.ExecSandbox.assert_called_once()
        assert _exec_request(client).stdin == b""

    def test_a_failed_write_is_recoverable(self):
        backend, client = _backend()
        client._stub.ExecSandbox.return_value = _result(1, err=b"cat: /opt/x: Permission denied\n")

        with pytest.raises(SandboxError, match="Permission denied") as error:
            backend.write_file("box", "/opt/x", b"data")

        assert not isinstance(error.value, SandboxTerminalError)

    @pytest.mark.parametrize(
        ("path", "message"),
        [
            pytest.param("/tmp/a\0b", "contains a NUL", id="nul"),
            # 16385 characters, but 32769 bytes of UTF-8.
            pytest.param("/" + "é" * 16384, "is 32769 bytes, over the 32768", id="over-32-kib"),
        ],
    )
    @pytest.mark.parametrize(
        "operation",
        [
            pytest.param(lambda backend, path: backend.read_file("box", path, max_bytes=10), id="read"),
            pytest.param(lambda backend, path: backend.write_file("box", path, b"data"), id="write"),
        ],
    )
    def test_a_path_the_gateway_refuses_is_recoverable_and_not_sent(self, operation, path, message):
        backend, client = _backend()

        with pytest.raises(SandboxError, match=message) as error:
            operation(backend, path)

        assert not isinstance(error.value, SandboxTerminalError)
        client._stub.ExecSandbox.assert_not_called()

    def test_listing_parses_nul_separated_records(self):
        backend, client = _backend()
        client._stub.ExecSandbox.return_value = _result(0, b"d sub\0f a file\0f new\nline\0")

        assert backend.list_directory("box", "/sandbox") == [
            ("sub", True),
            ("a file", False),
            ("new\nline", False),
        ]
        assert "find -- /sandbox" in _exec_request(client).stdin.decode()

    def test_a_listing_cut_at_the_cap_is_refused_rather_than_returned_partial(self):
        backend, client = _backend()
        client._stub.ExecSandbox.return_value = _result(0, b"f x\0" * 300_000)

        with pytest.raises(SandboxError, match="too many entries"):
            backend.list_directory("box", "/sandbox")


class TestDestroy:
    def test_destroy_deletes_without_waiting_and_forgets_the_policy(self):
        backend, client = _backend()
        name = backend.create(spec=SandboxSpec())

        backend.destroy(name)

        client.delete.assert_called_once_with(name, workspace="default", allow_missing=True)
        client.wait_deleted.assert_not_called()
        assert name not in backend._egress

    def test_destroying_a_missing_sandbox_is_not_an_error(self):
        backend, client = _backend()
        client.delete.side_effect = _RpcError(grpc.StatusCode.NOT_FOUND)

        backend.destroy("gone")

    def test_other_destroy_failures_are_raised_for_the_toolset_to_log(self):
        backend, client = _backend()
        client.delete.side_effect = _RpcError(grpc.StatusCode.UNAVAILABLE, "gateway down")

        with pytest.raises(SandboxError, match="gateway down"):
            backend.destroy("box")


_HAS_PROC_AND_SETSID = sys.platform.startswith("linux") and os.path.isdir("/proc") and shutil.which("setsid")


@pytest.mark.skipif(not _HAS_PROC_AND_SETSID, reason="the guest wrapper needs Linux /proc and setsid")
class TestRunWrapper:
    """Run the guest wrapper under the local /bin/sh, the way the sandbox runs it."""

    @pytest.fixture(autouse=True)
    def _no_sleep(self):
        """Keep the real time.sleep: the module's fixture patches it everywhere, not only in the backend."""

    @staticmethod
    def _run(command: str, seconds: int = 5, cap: int = 1000) -> subprocess.CompletedProcess[bytes]:
        return subprocess.run(
            ["/bin/sh", "-c", _RUN_WRAPPER, "airflow-exec", str(seconds), str(cap)],
            input=command.encode(),
            capture_output=True,
            timeout=seconds + 20,
            check=False,
        )

    @staticmethod
    def _pids(marker: str) -> list[int]:
        pids = []
        for entry in os.listdir("/proc"):
            if entry.isdigit():
                try:
                    with open(f"/proc/{entry}/cmdline", "rb") as f:
                        if f.read().replace(b"\0", b" ").strip() == marker.encode():
                            pids.append(int(entry))
                except OSError:
                    continue
        return pids

    def test_status_and_streams_pass_through(self):
        result = self._run("echo out; echo err >&2; exit 7")

        assert (result.returncode, result.stdout, result.stderr) == (7, b"out\n", b"err\n")

    def test_the_timeout_kills_the_command_and_everything_it_started(self):
        started = time.monotonic()
        result = self._run("sleep 301 & (sleep 302; echo x) & sleep 303", seconds=2)

        assert result.returncode == 124
        assert time.monotonic() - started < 10
        time.sleep(0.5)
        assert not any(self._pids(f"sleep {n}") for n in (301, 302, 303))

    @pytest.mark.parametrize(
        "name", [pytest.param("a) b c", id="a-parenthesis"), pytest.param("a\nb", id="a-newline")]
    )
    def test_the_timeout_kills_a_process_with_a_parenthesis_or_newline_in_its_name(self, tmp_path, name):
        # The kernel names a process after the file it executes, and /proc/<pid>/stat shows that
        # name unescaped, in parentheses. Under a subshell, the sleep's parent is not the session leader.
        named = tmp_path / name
        named.symlink_to(shutil.which("sleep"))

        result = self._run(f"({shlex.quote(str(named))} 309; :) & sleep 310", seconds=2)

        time.sleep(0.5)
        survivors = self._pids(f"{named} 309")
        for pid in survivors:
            os.kill(pid, signal.SIGKILL)
        assert result.returncode == 124
        assert not survivors

    def test_a_timer_starved_of_cpu_fires_on_elapsed_time(self):
        # Stopping the wrapper's process group, which the setsid'd command has left, stands in for
        # CPU contention; a timer counting its own one-second sleeps would fire five seconds late.
        stdin, writer = os.pipe()
        os.write(writer, b"sleep 306")
        os.close(writer)
        with subprocess.Popen(
            ["/bin/sh", "-c", _RUN_WRAPPER, "airflow-exec", "6", "1000"],
            stdin=stdin,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        ) as wrapper:
            os.close(stdin)
            for _ in range(100):
                if self._pids("sleep 306"):
                    break
                time.sleep(0.05)
            time.sleep(0.3)
            os.killpg(wrapper.pid, signal.SIGSTOP)
            time.sleep(6)
            os.killpg(wrapper.pid, signal.SIGCONT)
            resumed = time.monotonic()

            assert wrapper.wait(timeout=10) == 124
        assert time.monotonic() - resumed < 3

    def test_a_background_child_does_not_hold_the_call(self):
        started = time.monotonic()
        result = self._run("sleep 304 & echo started", seconds=30)

        survivors = self._pids("sleep 304")
        for pid in survivors:
            os.kill(pid, signal.SIGKILL)
        assert (result.returncode, result.stdout) == (0, b"started\n")
        assert time.monotonic() - started < 5
        assert survivors

    def test_a_failure_to_stage_the_command_has_its_own_status(self):
        # A directory on stdin fails the staging `cat` the way a full /tmp does.
        stdin = os.open("/", os.O_RDONLY)
        try:
            result = subprocess.run(
                ["/bin/sh", "-c", _RUN_WRAPPER, "airflow-exec", "5", "1000"],
                stdin=stdin,
                capture_output=True,
                timeout=20,
                check=False,
            )
        finally:
            os.close(stdin)

        assert result.returncode == 125
        assert result.stderr.endswith(f"{_STAGING_FAILED}\n".encode())

    def test_a_missing_setsid_has_its_own_status(self, tmp_path):
        for tool in ("mktemp", "cat", "rm"):
            (tmp_path / tool).symlink_to(shutil.which(tool))
        wrapper = _RUN_WRAPPER.replace(_SYSTEM_PATH, f"PATH={tmp_path}")
        assert wrapper != _RUN_WRAPPER

        result = subprocess.run(
            ["/bin/sh", "-c", wrapper, "airflow-exec", "5", "1000"],
            input=b"echo ran",
            capture_output=True,
            timeout=20,
            check=False,
        )

        assert (result.returncode, result.stdout, result.stderr) == (
            _NO_SETSID_STATUS,
            b"",
            f"{_NO_SETSID}\n".encode(),
        )

    def test_each_stream_is_cut_to_one_byte_past_the_cap(self):
        result = self._run("head -c 5000 /dev/zero; head -c 10 /dev/zero >&2", cap=100)

        assert (len(result.stdout), len(result.stderr)) == (101, 10)

    def test_a_status_of_124_from_the_command_itself_passes_through_fast(self):
        started = time.monotonic()

        assert self._run("exit 124", seconds=30).returncode == 124
        assert time.monotonic() - started < 5
