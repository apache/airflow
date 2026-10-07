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
"""NVIDIA OpenShell backend for :class:`~airflow.providers.common.ai.toolsets.sandbox.SandboxToolset`."""

from __future__ import annotations

import logging
import math
import shlex
import threading
import time
import uuid
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from airflow.providers.common.ai.sandbox.base import (
    _FILE_OP_OUTPUT_CAP,
    _FILE_OP_TIMEOUT,
    SandboxBackend,
    SandboxError,
    SandboxExecResult,
    SandboxFileTooLargeError,
    SandboxTerminalError,
    _validate_positive_finite,
)

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping, Sequence

    from openshell import SandboxClient

    from airflow.providers.common.ai.sandbox.base import SandboxSpec

log = logging.getLogger(__name__)

_CLIENT_BUILD_LOCK = threading.Lock()

# Wall-clock allowance past the per-command budget before the exec stream is
# treated as hung. The guest wrapper enforces the budget itself; this covers a
# gateway or supervisor that stops delivering events at all.
_EXEC_GRACE = 30.0
# How long a command or policy read waits for a restarting gateway to come back
# before the failure is reported. A restart measured at 6-11 s end to end.
_GATEWAY_RECOVERY = 60.0
_POLL_INTERVAL = 0.25
# The gateway decodes at most 1 MiB per request, and the command or a file chunk
# travels as stdin inside that request, so both stay under it with headroom.
_MAX_STDIN_BYTES = 1_000_000
_WRITE_CHUNK_BYTES = 768 * 1024
# The gateway rejects a sandbox name longer than 19 characters, which
# _new_sandbox_name's ``airflow-sandbox-<12 hex>`` is, so the name is shorter
# here. Labels carry the attribution instead, and the creation time so an
# operator can reap by age: OpenShell has no server-side lifetime.
_NAME_PREFIX = "airflow-"
_CREATED_BY_LABEL = ("created-by", "airflow")
_CREATED_AT_LABEL = "airflow-created-at"
_EGRESS_RULE = "airflow-egress"
_HTTPS_PORT = 443
_ANY_BINARY = "/**"
# OpenShell's restrictive default filesystem policy, plus /dev/shm so Python's
# multiprocessing and anything else using POSIX shared memory works.
_READ_ONLY_PATHS = ("/bin", "/usr", "/lib", "/proc", "/dev/urandom", "/etc", "/var/log")
_READ_WRITE_PATHS = ("/tmp", "/dev/null", "/dev/shm")
# The sandbox supervisor removes the proxy variables from every command it runs
# and overwrites the CA-bundle variables with its own TLS-terminating CA
# (openshell-sandbox process.rs PROXY_ENV_VARS and child_env.rs tls_env_vars in
# 0.1.2), in both cases without an error. A spec naming them would be silently
# changed, so it is refused instead.
_SUPERVISOR_OWNED_ENV = frozenset(
    {
        "ALL_PROXY",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "NO_PROXY",
        "all_proxy",
        "http_proxy",
        "https_proxy",
        "no_proxy",
        "grpc_proxy",
        "NODE_USE_ENV_PROXY",
        "SSL_CERT_FILE",
        "REQUESTS_CA_BUNDLE",
        "CURL_CA_BUNDLE",
        "GIT_SSL_CAINFO",
        "NODE_EXTRA_CA_CERTS",
        "DENO_CERT",
    }
)
_PROPOSAL_APPROVAL_MODE = "proposal_approval_mode"
_AGENT_POLICY_PROPOSALS = "agent_policy_proposals_enabled"
# Wrapper-private exit status for "the budget ran out". The wrapper only uses it
# after its own timer fired, and the caller also checks the elapsed time, so a
# command that exits 124 on its own inside the budget is not a timeout.
_TIMEOUT_STATUS = 124
# Wrapper-private exit status and stderr line for "the command could not be
# staged in /tmp, so it did not run". The status alone is not enough, because a
# command can exit 125 itself, as docker run, env and nohup do on their own errors.
_STAGING_STATUS = 125
_STAGING_FAILED = "airflow-exec: could not stage the command in /tmp"
_STAGING_LINE = f"{_STAGING_FAILED}\n".encode()
_SYSTEM_PATH = "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

# Runs one command for run_command. OpenShell's own exec timeout returns a
# synthetic 124 and leaves the process running, and a backgrounded child that
# inherits stdout holds the call open until the supervisor gives up on it, so
# the wrapper owns both problems:
#
# * The command arrives on stdin, which avoids the gateway's 32 KiB per-argument
#   cap, and runs in its own session via setsid, with stdout and stderr going to
#   files, so a background child holds a file rather than the exec stream.
# * A timer reading the monotonic /proc/uptime signals the wrapper when the
#   budget is spent; counting its own one-second sleeps instead drifts late on a
#   CPU-starved sandbox, past _EXEC_GRACE. The wrapper then SIGKILLs the
#   processes in the command's session, rescanning /proc until a pass kills
#   nothing, at most 50 times, so children forked mid-sweep are usually caught
#   too. A negative-pid kill of the group is blocked by the sandbox's seccomp
#   filter. The sweep shares the CPU with the processes it is killing, so it
#   reads /proc with builtins and strips the name from each stat line with the
#   cheap shortest match, taking the longest only for a name that contains ")":
#   a `cat` per entry let one sweep of 128 busy processes on one CPU run past
#   _EXEC_GRACE.
# * The exit status is the wrapper's own process status, which the supervisor
#   reports out of band, so nothing the command prints can change it; a command
#   that signals the wrapper only ends its own run early. Output is sent last as
#   one byte more than the caller's cap, so an over-cap stream is detected by
#   counting what arrives rather than trusting a trailer.
#
# A command that starts its own session (setsid, a daemonizing server) leaves
# this one and is not killed on timeout, and one that keeps forking faster than
# the sweep can outlast it.
_RUN_WRAPPER = rf"""t=$1 c=$2 o=$PATH
{_SYSTEM_PATH}
d=$(mktemp -d /tmp/.airflow-exec.XXXXXX) && cat >"$d/c" || {{ rm -rf "$d"; echo "{_STAGING_FAILED}" >&2; exit {_STAGING_STATUS}; }}
f=0
trap f=1 ALRM
trap f=2 HUP INT TERM
z=$(command -v setsid) || {{ echo "setsid is not installed in the sandbox image" >&2; rm -rf "$d"; exit 125; }}
PATH=$o "$z" /bin/sh "$d/c" </dev/null >"$d/o" 2>"$d/e" &
p=$!
(
  read u _ </proc/uptime
  e=$((${{u%.*}}${{u#*.}} + t * 100))
  while [ "${{u%.*}}${{u#*.}}" -lt "$e" ]; do
    sleep 1
    [ -d "/proc/$$" ] || exit 0
    read u _ </proc/uptime
  done
  kill -ALRM $$
) </dev/null >/dev/null 2>&1 &
w=$!
wait "$p"
r=$?
[ "$f" = 0 ] && trap '' ALRM
if [ "$f" != 0 ]; then
  n=0
  while [ "$n" -lt 50 ]; do
    k=0
    for x in /proc/[0-9]*/stat; do
      read -r s 2>/dev/null <"$x" || continue
      s=${{s#*) }}
      case $s in *")"*) s=${{s##*) }} ;; esac
      set -- $s
      [ "$4" = "$p" ] || continue
      x=${{x#/proc/}}
      kill -KILL "${{x%/stat}}" 2>/dev/null && k=1
    done
    [ "$k" = 0 ] && break
    n=$((n + 1))
  done
  wait "$p" 2>/dev/null
fi
kill -KILL "$w" 2>/dev/null
tail -c "$((c + 1))" "$d/o"
tail -c "$((c + 1))" "$d/e" >&2
rm -rf "$d"
[ "$f" = 1 ] && exit {_TIMEOUT_STATUS}
[ "$f" = 2 ] && exit 143
exit "$r"
"""
# read_file: the base class's script with the content sent raw instead of as
# base64, so a binary file arrives intact and the transfer is half the size.
_READ_SCRIPT = f"""{_SYSTEM_PATH}
sz=$(stat -Lc %s -- "$1" 2>/dev/null) || exit {SandboxBackend._MISSING_PATH_STATUS}
[ -d "$1" ] && exit {SandboxBackend._IS_DIRECTORY_STATUS}
printf '%s\\n' "$sz"
exec head -c "$2" -- "$1"
"""
_WRITE_FIRST_SCRIPT = f'{_SYSTEM_PATH}\nmkdir -p -- "$(dirname -- "$1")" && cat >"$1"\n'
_WRITE_NEXT_SCRIPT = f'{_SYSTEM_PATH}\ncat >>"$1"\n'


def _new_openshell_name() -> str:
    return f"{_NAME_PREFIX}{uuid.uuid4().hex[:11]}"


def _status_name(error: BaseException) -> str | None:
    code = getattr(error, "code", None)
    if not callable(code):
        return None
    try:
        status = code()
    except Exception:
        return None
    return getattr(status, "name", None)


def _describe_error(error: BaseException) -> str:
    status = _status_name(error)
    if status is None:
        return f"{type(error).__name__}: {error}"
    details = getattr(error, "details", None)
    text = details() if callable(details) else ""
    return f"{status}: {text}" if text else status


@contextmanager
def _translate_openshell_errors(
    operation: str, *, recoverable_statuses: frozenset[str] = frozenset()
) -> Iterator[None]:
    try:
        yield
    except SandboxError:
        raise
    except ImportError as e:
        raise SandboxTerminalError(
            "The OpenShell SDK is not installed. Install "
            '"apache-airflow-providers-common-ai[openshell]" (it needs Python 3.11 or later).'
        ) from e
    except Exception as e:
        message = f"OpenShell could not {operation} ({_describe_error(e)})."
        if _status_name(e) in recoverable_statuses:
            raise SandboxError(message) from e
        raise SandboxTerminalError(message) from e


def _is_hostname(value: object) -> bool:
    """Whether ``value`` is a hostname OpenShell's policy can match, optionally with one leading ``*.``."""
    if not isinstance(value, str) or not value or len(value) > 253:
        return False
    labels = value.removeprefix("*.").split(".")
    # A single label ('localhost', or '*.com' once the wildcard is removed) is
    # refused by the gateway for wildcards and cannot name a public endpoint.
    if len(labels) < 2 or labels[-1].isdigit():
        return False
    return all(
        0 < len(label) <= 63
        and label[0] != "-"
        and label[-1] != "-"
        and all(ch.isascii() and (ch.isalnum() or ch == "-") for ch in label)
        for label in labels
    )


class _BoundedTail:
    """
    The last ``max_bytes`` of a stream, and how many bytes the stream carried in total.

    ``min_window`` keeps that many trailing bytes even under a smaller ``max_bytes``,
    for :meth:`ends_with` only; the text and the truncation flag still follow ``max_bytes``.
    """

    def __init__(self, max_bytes: int, *, min_window: int = 0) -> None:
        self._max_bytes = max_bytes
        self._window = max(max_bytes, min_window)
        self._data = bytearray()
        self.received = 0

    def add(self, chunk: bytes) -> None:
        self.received += len(chunk)
        self._data.extend(chunk)
        if len(self._data) > self._window:
            del self._data[: len(self._data) - self._window]

    @property
    def truncated(self) -> bool:
        return self.received > self._max_bytes

    def ends_with(self, suffix: bytes) -> bool:
        return self._data.endswith(suffix)

    def get_text(self) -> str:
        data = bytes(self._data[max(0, len(self._data) - self._max_bytes) :])
        if self.truncated:
            # Drop the leading partial line so no fragment reads as a whole record, unless that
            # would throw away most of the window: one line longer than the cap has its newline
            # at the very end, which would leave "(no output)" for a command that wrote megabytes.
            newline = data.find(b"\n")
            if newline != -1 and len(data) - (newline + 1) >= self._max_bytes // 2:
                data = data[newline + 1 :]
        return data.decode("utf-8", errors="replace")


class _BoundedHead:
    """The first ``max_bytes`` of a stream; anything past that is dropped."""

    def __init__(self, max_bytes: int) -> None:
        self._max_bytes = max_bytes
        self._data = bytearray()

    def add(self, chunk: bytes) -> None:
        room = self._max_bytes - len(self._data)
        if room > 0:
            self._data.extend(chunk[:room])

    def get_bytes(self) -> bytes:
        return bytes(self._data)


@dataclass(frozen=True)
class _ExecOutcome:
    exit_code: int
    stdout: _BoundedTail | _BoundedHead
    stderr: _BoundedTail
    elapsed: float


class _ExecHung(Exception):
    """The exec stream outlived its deadline without reporting an exit status."""


class _GatewayDropped(Exception):
    """The gateway went away after the exec request was sent; the command's outcome is unknown."""


@dataclass(frozen=True)
class _EgressPolicy:
    """What an OpenShell policy lets a sandbox reach, in a form two policies can be compared by."""

    destinations: frozenset[tuple[str, int, frozenset[str]]]
    problems: tuple[str, ...]

    @classmethod
    def requested(cls, hosts: Sequence[str]) -> _EgressPolicy:
        return cls(
            destinations=frozenset((host, _HTTPS_PORT, frozenset({_ANY_BINARY})) for host in hosts),
            problems=(),
        )

    @classmethod
    def effective(cls, config: Any) -> _EgressPolicy:
        """Read the egress a ``GetSandboxConfigResponse`` says is in force."""
        from openshell._proto import sandbox_pb2

        destinations: set[tuple[str, int, frozenset[str]]] = set()
        problems: list[str] = []
        if config.policy_source != sandbox_pb2.POLICY_SOURCE_SANDBOX:
            problems.append(
                "a gateway-wide policy is in force instead of the sandbox's own "
                f"(policy_source {sandbox_pb2.PolicySource.Name(config.policy_source)})"
            )
        if not config.configuration_admitted:
            problems.append(
                f"the gateway has not admitted the sandbox's configuration ({config.configuration_error or 'no reason given'})"
            )
        if config.policy.landlock.compatibility != "hard_requirement":
            problems.append(
                f"Landlock enforcement is {config.policy.landlock.compatibility or 'unset'!r}, not 'hard_requirement'"
            )
        if config.policy.network_middlewares:
            problems.append(
                f"network middlewares are configured: {sorted(config.policy.network_middlewares)}"
            )
        for rule in config.policy.network_policies.values():
            binaries = frozenset(binary.path for binary in rule.binaries)
            for endpoint in rule.endpoints:
                if endpoint.allowed_ips:
                    problems.append(
                        f"{endpoint.host or 'a rule with no host'} admits addresses {list(endpoint.allowed_ips)}"
                    )
                ports = set(endpoint.ports)
                if endpoint.port:
                    ports.add(endpoint.port)
                destinations.update((endpoint.host.lower(), port, binaries) for port in ports)
        if _PROPOSAL_APPROVAL_MODE in config.settings:
            setting = config.settings[_PROPOSAL_APPROVAL_MODE]
            if setting.value.string_value == "auto":
                problems.append(
                    "proposal_approval_mode is 'auto' "
                    f"({sandbox_pb2.SettingScope.Name(setting.scope)}), so a connection the sandbox "
                    "is denied can become an allow rule without review"
                )
        if _AGENT_POLICY_PROPOSALS in config.settings:
            if config.settings[_AGENT_POLICY_PROPOSALS].value.bool_value:
                problems.append(
                    "agent_policy_proposals_enabled is on, so the agent can propose its own egress"
                )
        return cls(destinations=frozenset(destinations), problems=tuple(problems))

    def describe(self) -> list[str]:
        return sorted(f"{host}:{port}" for host, port, _ in self.destinations)

    def mismatch(self, wanted: _EgressPolicy) -> str | None:
        """Why this policy is not ``wanted``, or ``None`` when it is exactly that."""
        problems = list(self.problems)
        if self.destinations != wanted.destinations:
            problems.append(
                f"it admits {self.describe() or 'nothing'} where {wanted.describe() or 'nothing'} was asked for"
            )
        return "; ".join(problems) or None


class OpenShellSandboxBackend(SandboxBackend):
    """
    Run sandbox tools through an NVIDIA OpenShell gateway.

    .. note::

        Experimental: this can change or be removed in a minor release of this provider.
        See :ref:`howto/stability`.

    `OpenShell <https://github.com/NVIDIA/OpenShell>`__ is a self-hosted gateway
    that runs each sandbox on Docker, Podman or Kubernetes with Landlock and
    seccomp inside the container and a per-sandbox supervisor that mediates
    every outbound connection. Airflow workers only need gRPC access to the
    gateway, with the ``openshell`` extra installed (Python 3.11 or later).

    **Credentials are ambient.** The backend reads the gateway registration the
    ``openshell`` CLI keeps under ``$XDG_CONFIG_HOME/openshell/gateways/<name>/``:
    its endpoint and either mTLS material or an OIDC token. There is no Airflow
    connection type for it; see the backend page for how to provision a worker.

    **Egress is deny-all unless listed, and verified.** A spec's hostnames become
    one policy rule admitting each host on port 443 only, and a default spec
    sends no rule at all, which OpenShell enforces as no egress; name resolution
    is answered by the supervisor without leaving the sandbox. After create the
    backend reads the effective policy back and destroys the sandbox unless it
    is exactly what was asked for, comes from the sandbox rather than a
    gateway-wide override, and cannot be widened by auto-approved proposals.
    The same check runs before and after every command, because a gateway
    administrator, an approved draft or a global policy can widen a running
    sandbox; a change fails the task. What it cannot see is the network the
    gateway runs on: on Kubernetes the NetworkPolicy that keeps a sandbox pod
    behind its supervisor is the cluster's to enforce.

    **Commands run through a guest wrapper.** OpenShell's own exec timeout
    leaves the command running, so the wrapper enforces the budget itself and
    kills the processes in the command's session when it runs out. Output is
    spooled to the sandbox's ``/tmp`` and each stream is capped to
    ``max_output_bytes`` before it leaves the sandbox. ``allow_egress_to_cidrs``,
    an open network and ``SandboxSpec.owner`` are refused. The image needs
    ``setsid`` and GNU coreutils and findutils; ``python:*-slim`` has them.

    :param gateway: Name of the OpenShell CLI gateway registration to use.
        ``None`` uses ``$OPENSHELL_GATEWAY``, then the CLI's active gateway.
    :param workspace: OpenShell workspace the sandboxes are created in.
    :param image: Container image used for each sandbox.
    :param cpu: CPU limit, as a Kubernetes quantity.
    :param memory: Memory limit, as a Kubernetes quantity.
    :param ready_timeout: Seconds to wait for a new sandbox to become ready.
    :param request_timeout: Seconds allowed for each gateway call other than a command.
    """

    name = "openshell"

    def __init__(
        self,
        *,
        gateway: str | None = None,
        workspace: str = "default",
        image: str = "python:3.12-slim",
        cpu: str = "1",
        memory: str = "2Gi",
        ready_timeout: float = 120.0,
        request_timeout: float = 30.0,
    ) -> None:
        if gateway is not None and not gateway:
            raise ValueError("gateway must not be empty; pass None to use the CLI's active gateway.")
        if not workspace:
            raise ValueError("workspace must not be empty.")
        if not image:
            raise ValueError("image must not be empty.")
        if not cpu:
            raise ValueError("cpu must not be empty.")
        if not memory:
            raise ValueError("memory must not be empty.")
        _validate_positive_finite(ready_timeout, "ready_timeout")
        _validate_positive_finite(request_timeout, "request_timeout")
        self._gateway = gateway
        self._workspace = workspace
        self._image = image
        self._resources = {"limits": {"cpu": cpu, "memory": memory}}
        self._ready_timeout = ready_timeout
        self._request_timeout = request_timeout
        self._client: SandboxClient | None = None
        # The egress each sandbox was created with, compared against the effective
        # policy before and after every command.
        self._egress: dict[str, _EgressPolicy] = {}

    def _get_client(self) -> SandboxClient:
        with _CLIENT_BUILD_LOCK:
            if self._client is None:
                with _translate_openshell_errors("load the gateway registration"):
                    from openshell import SandboxClient

                    self._client = SandboxClient.from_active_cluster(
                        cluster=self._gateway, timeout=self._request_timeout
                    )
            return self._client

    def _workspace_scope(self) -> Any:
        from openshell._proto import datamodel_pb2

        return datamodel_pb2.WorkspaceSelector(workspace=self._workspace)

    @staticmethod
    def _check_spec(spec: SandboxSpec | None) -> tuple[list[str], dict[str, str]]:
        """
        Return the hosts and environment to provision, or refuse a spec OpenShell cannot enforce.

        ``None`` gets the same deny-all policy as a default spec: OpenShell has no
        policy-free mode that is not the image's own, which may be permissive.
        """
        if spec is None:
            return [], {}
        if spec.owner is not None:
            # An owner exists so that a later task can attach to the sandbox. OpenShell
            # labels are fixed at create and its annotations change only through an
            # admin-scoped config mutation, so this version does not attach.
            raise SandboxTerminalError(
                "SandboxSpec names an owner, but this backend does not implement attaching, so a "
                "sandbox created here cannot be attached to from another task. Drop owner, or "
                "provision the sandbox on a backend that supports attaching, such as ModalSandboxBackend."
            )
        if spec.allow_egress_to_cidrs:
            raise SandboxTerminalError(
                "SandboxSpec names allow_egress_to_cidrs, which this backend cannot enforce as "
                "specified: OpenShell address rules admit TCP on listed ports only, never UDP or any "
                "port. Use allow_egress_to with hostnames, or a backend with an address-layer allowlist."
            )
        if not spec.block_network:
            raise SandboxTerminalError(
                "SandboxSpec asks for an open network, which OpenShell cannot provide: every egress "
                "rule names a host and its ports. Set block_network=True and list the hosts the "
                "sandbox needs in allow_egress_to."
            )
        hosts: list[str] = []
        if spec.allow_egress_to:
            if isinstance(spec.allow_egress_to, str):
                raise SandboxTerminalError(
                    "SandboxSpec.allow_egress_to must be a sequence of hostnames, not one string: "
                    f"{spec.allow_egress_to!r} would be read a character at a time. Wrap it in a list."
                )
            rejected = [host for host in spec.allow_egress_to if not _is_hostname(host)]
            if rejected:
                raise SandboxTerminalError(
                    "SandboxSpec.allow_egress_to must contain bare hostnames, optionally with a "
                    f"leading '*.' label, and these entries are not: {rejected}. Write 'pypi.org' or "
                    "'*.pythonhosted.org', not a URL, a host:port, an address, or a single label."
                )
            hosts = sorted({host.lower() for host in spec.allow_egress_to})
        env: dict[str, str] = {}
        for key, value in (spec.env or {}).items():
            if not isinstance(key, str) or not isinstance(value, str):
                raise SandboxTerminalError(
                    f"SandboxSpec.env must map strings to strings; {key!r} is set to {type(value).__name__}."
                )
            if key in _SUPERVISOR_OWNED_ENV or key.startswith("OPENSHELL_"):
                raise SandboxTerminalError(
                    f"SandboxSpec.env sets {key}, which the OpenShell supervisor owns: it removes the "
                    "proxy variables and replaces the CA bundle variables with its own TLS-terminating "
                    "CA in every command, and reserves OPENSHELL_*. Remove it from the spec."
                )
            env[key] = value
        return hosts, env

    def _build_spec(self, hosts: Sequence[str], env: Mapping[str, str]) -> Any:
        from google.protobuf import struct_pb2
        from openshell._proto import openshell_pb2, sandbox_pb2

        policy = sandbox_pb2.SandboxPolicy(
            version=1,
            filesystem=sandbox_pb2.FilesystemPolicy(
                include_workdir=True, read_only=list(_READ_ONLY_PATHS), read_write=list(_READ_WRITE_PATHS)
            ),
            # Refuse to start at all rather than run without the filesystem confinement.
            landlock=sandbox_pb2.LandlockPolicy(compatibility="hard_requirement"),
        )
        if hosts:
            policy.network_policies[_EGRESS_RULE].CopyFrom(
                sandbox_pb2.NetworkPolicyRule(
                    name=_EGRESS_RULE,
                    endpoints=[sandbox_pb2.NetworkEndpoint(host=host, ports=[_HTTPS_PORT]) for host in hosts],
                    binaries=[sandbox_pb2.NetworkBinary(path=_ANY_BINARY)],
                )
            )
        spec = openshell_pb2.SandboxSpec(environment=dict(env), policy=policy)
        spec.template.image = self._image
        resources = struct_pb2.Struct()
        resources.update(self._resources)
        spec.template.resources.CopyFrom(resources)
        return spec

    def create(self, *, spec: SandboxSpec | None = None) -> str:
        hosts, env = self._check_spec(spec)
        client = self._get_client()
        with _translate_openshell_errors("create a sandbox"):
            request = self._build_spec(hosts, env)
        name = _new_openshell_name()
        try:
            with _translate_openshell_errors("create a sandbox"):
                client.create(
                    workspace=self._workspace,
                    spec=request,
                    name=name,
                    labels={
                        _CREATED_BY_LABEL[0]: _CREATED_BY_LABEL[1],
                        _CREATED_AT_LABEL: str(int(time.time())),
                    },
                )
            self._wait_until_ready(name, deadline=time.monotonic() + self._ready_timeout)
            wanted = _EgressPolicy.requested(hosts)
            mismatch = self._read_egress(name).mismatch(wanted)
            if mismatch is not None:
                raise SandboxTerminalError(
                    f"OpenShell did not apply the requested network policy to sandbox {name} ({mismatch}), "
                    "so the sandbox was destroyed."
                )
        except BaseException:
            # Bound before the call, so a create that fails half way -- after the
            # gateway accepted it, or at the policy check -- still gets deleted.
            with suppress(Exception):
                self.destroy(name)
            raise
        self._egress[name] = wanted
        return name

    def _get_sandbox(self, name: str) -> Any:
        from openshell._proto import openshell_pb2

        return (
            self._get_client()
            ._stub.GetSandbox(
                openshell_pb2.GetSandboxRequest(workspace_scope=self._workspace_scope(), name=name),
                timeout=self._request_timeout,
            )
            .sandbox
        )

    def _wait_until_ready(self, name: str, *, deadline: float) -> None:
        """
        Poll until the sandbox is ready, riding out a gateway that is restarting.

        Raises :class:`SandboxTerminalError` for a sandbox that is gone, has failed or
        has stopped, or that is not ready by ``deadline``.
        """
        from openshell._proto import openshell_pb2

        last = "it did not report a phase"
        while True:
            try:
                sandbox = self._get_sandbox(name)
            except Exception as e:
                if _status_name(e) not in {"UNAVAILABLE", "DEADLINE_EXCEEDED"}:
                    with _translate_openshell_errors(f"check the status of sandbox {name}"):
                        raise
                last = f"the gateway did not answer ({_describe_error(e)})"
            else:
                phase = sandbox.status.phase
                if phase == openshell_pb2.SANDBOX_PHASE_READY:
                    return
                reasons = "; ".join(
                    f"{condition.reason}: {condition.message}"
                    for condition in sandbox.status.conditions
                    if condition.status != "True" and (condition.reason or condition.message)
                )
                last = f"it is {openshell_pb2.SandboxPhase.Name(phase)}" + (
                    f" ({reasons})" if reasons else ""
                )
                if phase in {
                    openshell_pb2.SANDBOX_PHASE_ERROR,
                    openshell_pb2.SANDBOX_PHASE_STOPPING,
                    openshell_pb2.SANDBOX_PHASE_STOPPED,
                    openshell_pb2.SANDBOX_PHASE_COMPLETED,
                    openshell_pb2.SANDBOX_PHASE_DELETING,
                }:
                    raise SandboxTerminalError(f"OpenShell sandbox {name} cannot run commands: {last}.")
            if time.monotonic() >= deadline:
                raise SandboxTerminalError(f"OpenShell sandbox {name} is not ready: {last}.")
            time.sleep(_POLL_INTERVAL)

    def _read_egress(self, name: str) -> _EgressPolicy:
        """Read the sandbox's effective egress, waiting out a restarting gateway for a bounded time."""
        from openshell._proto import sandbox_pb2

        request = sandbox_pb2.GetSandboxConfigRequest(workspace_scope=self._workspace_scope(), name=name)
        deadline = time.monotonic() + _GATEWAY_RECOVERY
        while True:
            try:
                config = self._get_client()._stub.GetSandboxConfig(request, timeout=self._request_timeout)
            except Exception as e:
                if (
                    _status_name(e) not in {"UNAVAILABLE", "DEADLINE_EXCEEDED"}
                    or time.monotonic() >= deadline
                ):
                    with _translate_openshell_errors(f"read the network policy of sandbox {name}"):
                        raise
                time.sleep(_POLL_INTERVAL)
                continue
            return _EgressPolicy.effective(config)

    def _check_egress(self, name: str) -> None:
        """Destroy the sandbox and fail the task if its egress is no longer what it was created with."""
        effective = self._read_egress(name)
        # A handle this instance did not create is held to what it admits the first
        # time it is seen, on top of the same invariants.
        wanted = self._egress.setdefault(name, _EgressPolicy(effective.destinations, ()))
        mismatch = effective.mismatch(wanted)
        if mismatch is None:
            return
        with suppress(SandboxError):
            self.destroy(name)
        raise SandboxTerminalError(
            f"The network policy of OpenShell sandbox {name} changed after it was created ({mismatch}), "
            "so the sandbox was destroyed. A gateway administrator, an approved policy draft or a "
            "gateway-wide policy can widen a running sandbox; commands already run may have used it."
        )

    def _exec(
        self,
        sandbox: str,
        argv: list[str],
        *,
        stdin: bytes,
        deadline: float,
        stdout: _BoundedTail | _BoundedHead,
        stderr: _BoundedTail,
    ) -> _ExecOutcome:
        """
        Run ``argv`` once, streaming both outputs into the given bounded buffers.

        Streams the gateway's events directly: the SDK's ``exec_stream`` keeps every
        chunk in memory until the command exits. Raises :class:`_ExecHung` past
        ``deadline`` and :class:`_GatewayDropped` when the gateway goes away after the
        request left, where the command may or may not have run.
        """
        from openshell._proto import openshell_pb2

        client = self._get_client()
        request = openshell_pb2.ExecSandboxRequest(
            workspace_scope=self._workspace_scope(),
            sandbox=sandbox,
            command=argv,
            stdin=stdin,
            # A login shell would source profiles that can override SandboxSpec.env.
            no_login_shell=True,
            request_id=str(uuid.uuid4()),
        )
        exit_code: int | None = None
        started = time.monotonic()
        stream = None
        try:
            stream = client._stub.ExecSandbox(request, timeout=deadline)
            for event in stream:
                kind = event.WhichOneof("payload")
                if kind == "stdout":
                    stdout.add(event.stdout.data)
                elif kind == "stderr":
                    stderr.add(event.stderr.data)
                elif kind == "exit":
                    exit_code = event.exit.exit_code
        except Exception as e:
            status = _status_name(e)
            if status == "DEADLINE_EXCEEDED":
                raise _ExecHung from e
            if status == "UNAVAILABLE":
                raise _GatewayDropped(_describe_error(e)) from e
            with _translate_openshell_errors(
                f"run a command in sandbox {sandbox}",
                recoverable_statuses=frozenset({"OUT_OF_RANGE", "RESOURCE_EXHAUSTED"}),
            ):
                raise
        finally:
            if stream is not None:
                with suppress(Exception):
                    stream.cancel()
        if exit_code is None:
            raise _GatewayDropped("the exec stream ended without an exit status")
        return _ExecOutcome(exit_code, stdout, stderr, time.monotonic() - started)

    def _exec_with_recovery(
        self,
        sandbox: str,
        argv: list[str],
        *,
        stdin: bytes,
        deadline: float,
        stdout: _BoundedTail | _BoundedHead,
        stderr: _BoundedTail,
    ) -> _ExecOutcome:
        """
        :meth:`_exec`, with a gateway restart turned into an honest outcome.

        A sandbox that is not ready yet, which is how it looks for a few seconds after a
        gateway restart, has not launched anything, so the command is sent again once it
        is. A gateway that drops an exec in flight has killed the command with every
        other process in the sandbox, and whether it had already done its work is not
        knowable, so that is reported to the model rather than retried or guessed.
        """
        try:
            try:
                return self._exec(sandbox, argv, stdin=stdin, deadline=deadline, stdout=stdout, stderr=stderr)
            except SandboxTerminalError as e:
                if _status_name(e.__cause__ or e) != "FAILED_PRECONDITION":
                    raise
                self._wait_until_ready(sandbox, deadline=time.monotonic() + self._ready_timeout)
                return self._exec(sandbox, argv, stdin=stdin, deadline=deadline, stdout=stdout, stderr=stderr)
        except _GatewayDropped as e:
            self._wait_until_ready(sandbox, deadline=time.monotonic() + _GATEWAY_RECOVERY)
            raise SandboxError(
                f"The OpenShell gateway dropped the command before it reported an exit status ({e}); the "
                "gateway may have restarted. The command may or may not have run, and a gateway restart "
                "also stops processes that earlier commands left running. Files are kept. Check what "
                "state you need and run the command again."
            ) from e

    def run_command(
        self, sandbox: str, command: str, *, timeout: float, max_output_bytes: int
    ) -> SandboxExecResult:
        _validate_positive_finite(timeout, "timeout")
        _validate_positive_finite(max_output_bytes, "max_output_bytes")
        # Whole seconds, at least one: the wrapper takes its budget as an integer.
        seconds = max(1, math.ceil(timeout))
        payload = command.encode("utf-8")
        if len(payload) > _MAX_STDIN_BYTES:
            raise SandboxError(
                f"The command is {len(payload)} bytes, over the {_MAX_STDIN_BYTES} bytes OpenShell carries "
                "in one request. Write the script to a file with write_file and run the file."
            )
        self._check_egress(sandbox)
        stdout = _BoundedTail(int(max_output_bytes))
        stderr = _BoundedTail(int(max_output_bytes), min_window=len(_STAGING_LINE))
        try:
            outcome = self._exec_with_recovery(
                sandbox,
                ["/bin/sh", "-c", _RUN_WRAPPER, "airflow-exec", str(seconds), str(int(max_output_bytes))],
                stdin=payload,
                deadline=seconds + _EXEC_GRACE,
                stdout=stdout,
                stderr=stderr,
            )
        except _ExecHung:
            return self._abandon_command(sandbox, stdout, stderr, seconds=seconds)
        self._check_egress(sandbox)
        if outcome.exit_code == _STAGING_STATUS and stderr.ends_with(_STAGING_LINE):
            cause = stderr.get_text().rpartition(_STAGING_FAILED)[0].strip()
            raise SandboxError(
                "The command was not run: the sandbox could not write it to /tmp"
                + (f" ({cause})." if cause else ".")
            )
        return SandboxExecResult(
            exit_code=outcome.exit_code,
            stdout=stdout.get_text(),
            stderr=stderr.get_text(),
            timed_out=outcome.exit_code == _TIMEOUT_STATUS and outcome.elapsed >= seconds,
            stdout_truncated=stdout.truncated,
            stderr_truncated=stderr.truncated,
            applied_timeout=float(seconds),
        )

    def _abandon_command(
        self, sandbox: str, stdout: _BoundedTail, stderr: _BoundedTail, *, seconds: int
    ) -> SandboxExecResult:
        # The wrapper reports within its budget unless the command disabled it or the
        # gateway stopped relaying; either way the command may still be running, and
        # OpenShell does not stop an exec whose caller went away, so only destroying
        # the sandbox ends it. There is no server-side lifetime to fall back on.
        try:
            self.destroy(sandbox)
        except SandboxError:
            log.warning(
                "Timed out running a command in OpenShell sandbox %s and could not destroy it; it has no "
                "server-side lifetime and will need manual cleanup",
                sandbox,
                exc_info=True,
            )
        return SandboxExecResult(
            exit_code=-1,
            stdout=stdout.get_text(),
            stderr=stderr.get_text(),
            timed_out=True,
            stdout_truncated=stdout.truncated,
            stderr_truncated=stderr.truncated,
            sandbox_terminated=True,
            applied_timeout=float(seconds),
        )

    def _run_file_op(
        self, sandbox: str, script: str, *args: str, stdin: bytes = b"", stdout_cap: int = _FILE_OP_OUTPUT_CAP
    ) -> tuple[_ExecOutcome, bytes]:
        head = _BoundedHead(stdout_cap)
        try:
            outcome = self._exec_with_recovery(
                sandbox,
                ["/bin/sh", "-c", script, "airflow-file", *args],
                stdin=stdin,
                deadline=_FILE_OP_TIMEOUT,
                stdout=head,
                stderr=_BoundedTail(_FILE_OP_OUTPUT_CAP),
            )
        except _ExecHung as e:
            raise SandboxError(
                f"The file operation did not finish within {_FILE_OP_TIMEOUT:g} seconds."
            ) from e
        return outcome, head.get_bytes()

    def read_file(self, sandbox: str, path: str, *, max_bytes: int) -> bytes:
        """Read a file as raw bytes, stopping one byte past ``max_bytes`` inside the guest."""
        _validate_positive_finite(max_bytes, "max_bytes")
        # The size line is at most 20 digits and a newline; the rest is the content.
        outcome, raw = self._run_file_op(
            sandbox, _READ_SCRIPT, path, str(max_bytes + 1), stdout_cap=max_bytes + 1 + 32
        )
        if outcome.exit_code == self._MISSING_PATH_STATUS:
            raise SandboxError(f"{path!r} does not exist in the sandbox, or is not readable.")
        if outcome.exit_code == self._IS_DIRECTORY_STATUS:
            raise SandboxError(f"{path!r} is a directory. Use list_directory to see what is in it.")
        if outcome.exit_code:
            raise SandboxError(outcome.stderr.get_text().strip() or f"Could not read {path!r}.")
        reported, _, data = raw.partition(b"\n")
        if len(data) > max_bytes:
            try:
                size = int(reported.strip())
            except ValueError:
                size = 0
            # A streaming source reports 0; the true size is then unknown but over.
            raise SandboxFileTooLargeError(path, max(size, len(data)), max_bytes)
        return data

    def write_file(self, sandbox: str, path: str, content: bytes) -> None:
        """
        Write ``content`` over stdin, creating parent directories.

        The base class's in-command payload is base64 inside the one request that
        carries the command, so it stops at about 750 KB of content. Raw stdin in
        chunks has no such ceiling. A file larger than one chunk is not written
        atomically: a failure part way leaves the chunks written so far.
        """
        chunks = [
            content[i : i + _WRITE_CHUNK_BYTES] for i in range(0, len(content), _WRITE_CHUNK_BYTES)
        ] or [b""]
        for index, chunk in enumerate(chunks):
            script = _WRITE_FIRST_SCRIPT if index == 0 else _WRITE_NEXT_SCRIPT
            outcome, _ = self._run_file_op(sandbox, script, path, stdin=chunk)
            if outcome.exit_code:
                raise SandboxError(outcome.stderr.get_text().strip() or f"Could not write {path!r}.")

    def list_directory(self, sandbox: str, path: str) -> list[tuple[str, bool]]:
        """List as the base class does, but refuse a listing cut short at the output cap."""
        result = self.run_command(
            sandbox,
            f"find -- {shlex.quote(path)} -maxdepth 1 -mindepth 1 -printf '%y %f\\0'",
            timeout=_FILE_OP_TIMEOUT,
            max_output_bytes=_FILE_OP_OUTPUT_CAP,
        )
        if result.exit_code:
            raise SandboxError(result.stderr.strip() or f"Could not list {path!r}.")
        if result.stdout_truncated:
            raise SandboxError(
                f"{path!r} has too many entries to list; list a subdirectory instead, or use a shell "
                "command such as `ls | head`."
            )
        entries: list[tuple[str, bool]] = []
        for record in result.stdout.split("\0"):
            kind, _, name = record.partition(" ")
            if name:
                entries.append((name, kind == "d"))
        return entries

    def destroy(self, sandbox: str) -> None:
        self._egress.pop(sandbox, None)
        client = self._get_client()
        try:
            # Accepted asynchronously; waiting for the containers to go takes seconds
            # and changes nothing for the caller.
            client.delete(sandbox, workspace=self._workspace, allow_missing=True)
        except Exception as e:
            if _status_name(e) == "NOT_FOUND":
                return
            with _translate_openshell_errors(f"destroy sandbox {sandbox}"):
                raise
