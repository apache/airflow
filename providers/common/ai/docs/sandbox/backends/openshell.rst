 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

.. _sandbox-backend-openshell:

OpenShell (self-hosted remote)
==============================

.. note::

    Experimental: this can change or be removed in a minor release of this provider.
    See :ref:`howto/stability`.

:class:`~airflow.providers.common.ai.sandbox.openshell.OpenShellSandboxBackend`
runs sandboxes through an `NVIDIA OpenShell <https://github.com/NVIDIA/OpenShell>`__
gateway, which runs each one on Docker, Podman or Kubernetes. Inside the
container the workload is confined by Landlock and seccomp; on the Docker driver it
has only a loopback network interface, and a per-sandbox supervisor opens every
outbound connection on its behalf, against a policy the backend writes and reads back. Airflow workers
only need gRPC access to the gateway.

Choose it over :ref:`OpenSandbox <sandbox-backend-opensandbox>` when you want the
sandbox runtime itself to enforce deny-all and the hostname allowlist, rather than
an egress sidecar that has to run in the right mode. It is Apache-2.0 and runs on a
single machine with no account, so it is also the self-hosted backend to try first
on a laptop. For a production gateway on Kubernetes, prefer OpenSandbox or Modal:
this backend has been verified on OpenShell's Docker driver only, which OpenShell
scopes to local development and single-machine gateways.

Install the SDK extra:

.. code-block:: bash

    pip install "apache-airflow-providers-common-ai[openshell]"

**Credentials are ambient; there is no Airflow connection for this backend.**
The gateway's endpoint, mTLS material and OIDC token live in the gateway
registration that the ``openshell`` CLI keeps under
``$XDG_CONFIG_HOME/openshell/gateways/<name>/`` (``~/.config`` by default), and
the OpenShell SDK reads them from there itself: ``metadata.json`` with the
endpoint, and ``mtls/ca.crt``, ``mtls/tls.crt`` and ``mtls/tls.key`` for mTLS,
or the CLI's cached token for an OIDC gateway. The backend only names the
registration. A worker without the CLI needs the same files, provisioned by the
Deployment Manager:

.. code-block:: json

    {"name": "prod", "gateway_endpoint": "https://openshell.example.com:17670", "auth_mode": "mtls"}

.. code-block:: python

    from airflow.providers.common.ai.sandbox import OpenShellSandboxBackend
    from airflow.providers.common.ai.toolsets import SandboxToolset

    SandboxToolset(OpenShellSandboxBackend(gateway="prod"))

An OIDC registration must be writable and per worker. The SDK refreshes an
expiring OIDC token itself and writes the new one back into the registration,
so a registration mounted read-only, from a Kubernetes Secret or ConfigMap for
example, fails the task at the first refresh, and copies of one registration on
several workers share a refresh token that the first refresh can invalidate for
the others. mTLS is the supported setup for workers that mount the registration
read-only.

Whoever holds that credential is inside the trust boundary. On a gateway
without OIDC, an mTLS client is a gateway-wide administrator: it can change the
policy and settings of every sandbox. ``openshell-gateway generate-certs`` issues
a single client identity whose certificate does not expire for practical
purposes, so use your own PKI, with expiry and rotation, for a worker credential.

**Network policy.** A default ``SandboxSpec()`` sends a policy with no egress
rule, which OpenShell enforces as no egress: a connection fails with
``EACCES``, and a name lookup is answered by the supervisor with a synthetic
``198.18.0.0/15`` address rather than failing, so no query leaves the sandbox
although the tool description's "including DNS" wording reads stricter than the
lookup behaves. ``allow_egress_to`` becomes one rule admitting each listed host
on port 443 for any program in the sandbox; ports are part of every rule, so
plain HTTP and other ports stay closed. HTTPS to a listed host is terminated by
the supervisor, which injects its CA through ``SSL_CERT_FILE``,
``REQUESTS_CA_BUNDLE`` and similar variables; a client with its own trust store
(a JVM, a statically linked binary) has to be pointed at it. A leading ``*.``
label is accepted on a name of three labels or more.

The backend verifies the policy rather than trusting the request. After create
it reads the effective policy back and destroys the sandbox unless it admits
exactly the requested hosts, comes from the sandbox rather than a gateway-wide
policy, keeps Landlock at ``hard_requirement``, and has neither
``proposal_approval_mode=auto`` nor agent policy proposals enabled at any scope.
Auto-approval is refused because OpenShell turns a denied connection into a
proposed allow rule and, in that mode, approves it without review. The same
check runs before every command and file operation and after each one that
succeeds, because an administrator, an approved
draft or a gateway-wide policy can widen a running sandbox; a change destroys the
sandbox and fails the task. That detects a widening, it does not prevent one: a
command already running when the policy changes can use it. The check sees
OpenShell's policy, not the network under the gateway: on Kubernetes, a sandbox
pod is kept behind its supervisor by a NetworkPolicy, which only a CNI that
enforces NetworkPolicy upholds. Every denied connection also becomes a draft
proposal in the gateway's approval inbox, even in manual mode, so operators see
which destinations an agent tried.

Refused at create: ``block_network=False`` (every OpenShell rule names a host and
its ports, so an open network cannot be expressed), ``allow_egress_to_cidrs``
(address rules admit TCP on listed ports only), ``SandboxSpec.owner``, and
``SandboxSpec.env`` keys the supervisor owns: it removes ``HTTP_PROXY`` and the
other proxy variables from every command and replaces ``SSL_CERT_FILE``,
``REQUESTS_CA_BUNDLE``, ``CURL_CA_BUNDLE``, ``GIT_SSL_CAINFO``,
``NODE_EXTRA_CA_CERTS`` and ``DENO_CERT`` with its own, without an error, and
reserves ``OPENSHELL_*``. Keys starting with ``_airflow_`` are refused too, because the
command wrapper below names its own shell variables that way. Everything else in
``SandboxSpec.env``, ``PATH`` included, reaches commands as given, since they run
without a login shell.

**Commands.** OpenShell's own exec timeout reports exit 124 and leaves the
command running, so the backend runs each command through a small shell wrapper
in the sandbox. The command arrives on stdin and runs in a session of its own,
with the command and its output spooled to ``/tmp``; one that cannot be written
there, because ``/tmp`` is full for example, is not run and is reported as an
error. When the budget runs out the wrapper kills the processes in that session,
scanning again until a pass finds none to kill, at most 50 times, and returns the
tail of each stream. A process the command left in the background keeps running
after a command that finishes in time, and one that started a session of its own
(``setsid``, a daemonizing server) escapes the kill on timeout, as can a command
that keeps forking faster than the sweep. If the gateway stops relaying the
command for longer than its budget plus 30 seconds, ``sandbox_terminated`` is
reported and the backend asks the gateway to delete the sandbox; if that call
fails too, it logs a warning and the sandbox is left for the reaper described
below. Nothing crosses the stream
until the command ends, so a proxy or load balancer in front of the gateway
needs an idle timeout longer than the longest command budget plus 30 seconds. A
command the gateway could not start, because its relay into the sandbox did not
open, is reported to the model as not run. A gateway restart stops every process
in its sandboxes. If the gateway brings the sandbox back, its files are kept and a
command in flight is reported to the model as having an unknown outcome rather
than retried; if it does not, the task fails. A dropped connection to a gateway
that stays up is reported the same way, but does not stop the command: it runs
until its budget runs out, and the model is told it may still be running.

**Files.** ``write_file`` sends content on stdin in chunks of 768 KiB, since the
gateway limits one command argument to 32 KiB and one request to 1 MiB; a larger
write is not atomic, and it follows a symlink like a shell redirect. ``read_file``
transfers raw bytes, capped inside the guest. Both refuse a named pipe, since
opening one waits for another process and nothing would end the wait in the
sandbox; ``run_command`` with a timeout can still read or write one. The image needs ``setsid`` (from
util-linux), GNU coreutils and findutils, and a ``/bin/sh`` that is dash or bash,
which ``python:*-slim`` and other Debian or Ubuntu based images have; an image
without ``setsid`` fails the task at its first command. BusyBox ``ash``, the
``/bin/sh`` of Alpine images, is not supported: the timeout sweep does not finish
under it, so a command that runs out of time is not killed, and after its budget
plus 30 seconds the backend asks the gateway to delete the sandbox instead. The gateway's own default image has no ``python3``, which is
why the backend defaults to ``python:3.12-slim``.

**Cleanup.** OpenShell has no server-side sandbox lifetime. ``destroy`` deletes
the sandbox, but one whose worker died is kept until someone deletes it, even
after its main process has ended. Sandboxes are named ``airflow-`` plus 11 hex
characters, within OpenShell's 19-character limit, and labeled
``created-by=airflow`` and ``airflow-created-at=<unix seconds>``, so a reaper can
delete by age:

.. code-block:: python

    import time

    from openshell import SandboxClient

    client = SandboxClient.from_active_cluster(cluster="prod")
    cutoff = time.time() - 6 * 3600
    for sandbox in client.list_all(workspace="default", label_selector="created-by=airflow"):
        if int(sandbox.labels.get("airflow-created-at", "0")) < cutoff:
            client.delete(sandbox.name, workspace="default", allow_missing=True)

Constructor parameters:

- ``gateway``: the CLI gateway registration to use. ``None`` (default) uses
  ``$OPENSHELL_GATEWAY``, then the CLI's active gateway.
- ``workspace``: OpenShell workspace. Default ``"default"``.
- ``image``: image used by the gateway. Default ``"python:3.12-slim"``.
- ``cpu`` and ``memory``: resource limits as Kubernetes quantities. Defaults
  ``"1"`` and ``"2Gi"``. ``nproc`` in the sandbox still reports the host's CPUs.
- ``ready_timeout``: seconds to wait for a new sandbox. Default ``120``; a first
  pull of a large image can take longer, so pre-pull it on the gateway host.
- ``request_timeout``: seconds for each gateway call other than a command.
  Default ``30``.

The kernel the sandboxes run on needs Landlock ABI 3 or later (Linux 6.2+) and
seccomp user notification, and the gateway needs Docker 28 or later, Podman 5, or
a Kubernetes cluster with the Agent Sandbox controller. On macOS the gateway runs
against the Linux VM of a Docker runtime such as OrbStack or colima. OpenShell scopes its Docker driver to
local development and single-machine gateways. This backend has been verified with
the Docker driver only; on Kubernetes, egress enforcement also depends on the
cluster's CNI enforcing NetworkPolicy. The gateway sends anonymous
telemetry unless it runs with ``OPENSHELL_TELEMETRY_ENABLED=false``.
