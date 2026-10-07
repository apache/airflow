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

Sandbox backends
================

.. note::

    Experimental: this can change or be removed in a minor release of this provider.
    See :ref:`howto/stability`.

.. _sandbox-backend-modal:

Modal (hosted)
--------------

:class:`~airflow.providers.common.ai.sandbox.modal.ModalSandboxBackend` runs each
sandbox in Modal, provisioned over the API. Of the backends that ship with the
provider, **this is the managed one to use in production**, and with
:ref:`OpenSandbox <sandbox-backend-opensandbox>` and
:ref:`OpenShell <sandbox-backend-openshell>` one of the three that run on
Kubernetes: nothing has to be installed on the worker, model-written code never
executes on the worker host, and Modal reclaims a sandbox at its own lifetime
whether or not the worker survives. It needs the ``modal`` extra and Modal
credentials, from a ``modal`` connection or the worker environment, as under
:ref:`Quick start <sandbox-quick-start>`.

Constructor parameters:

- ``modal_conn_id``: ``modal`` connection the token and, optionally, the Modal
  ``environment`` come from. Default ``"modal_default"``; ``None`` uses the worker's
  credentials without looking for a connection. How a missing or partial connection
  resolves is on the :ref:`Modal connection page <howto/connection:modal>`. A credential
  problem fails the task, except in the toolset's own teardown, which logs it so a
  finished run is not failed. The connection type comes from the Modal provider, which
  the ``modal`` extra installs and which needs Airflow 3.
- ``image``: Registry tag for the sandbox image, or a prepared ``modal.Image``
  carrying pre-installed packages. Default ``"python:3.12-slim"``.
- ``app_name``: Modal app the sandboxes are created under. Default
  ``"airflow-sandbox"``.
- ``create_app_if_missing``: Create that app if it does not exist. Default ``True``.
- ``sandbox_timeout``: Maximum lifetime of a sandbox in seconds. Default ``3600``.
  Modal's own default is 300, which is below a plausible agent run.
- ``idle_timeout``: Seconds of inactivity after which Modal reclaims the sandbox,
  or ``None`` to rely on ``sandbox_timeout`` alone. Default ``None``; see
  :ref:`Configuring a sandbox <sandbox-configuring>`.
- ``workdir``: Working directory for commands, created if the image lacks it.
  Default ``"/workspace"``. It is a starting directory, not a jail: absolute paths
  and ``..`` are passed through, and commands run as root, so the model can read
  and write anywhere in the sandbox filesystem. The sandbox boundary is what
  contains that.
- ``cpu``, ``memory``, ``gpu``, ``region``, ``cloud``: passed through to Modal.
  ``None`` lets Modal choose. An unrecognized ``region`` or ``cloud`` fails the
  task rather than falling back.
- ``tags``: Extra Modal tags on every sandbox, e.g. ``{"dag_id": "my_dag"}``.
  The backend's own ``airflow_`` keys overwrite a tag of the same name.
- ``egress_enforcement``: ``"strict"`` (default) or ``"sni"``. See below.

**A sandbox can be provisioned by one task and used by another.** Modal finds a
sandbox by id from any process, so this backend implements
:class:`~airflow.providers.common.ai.sandbox.AttachableSandboxBackend`: a
``@task`` calls ``create`` and later ``destroy``, and a ``SandboxToolset`` with
``attach_to`` uses the sandbox in between. The ownership rules ride on Modal tags,
``airflow_owner`` from ``SandboxSpec.owner``, ``airflow_holder`` while a run holds
the sandbox, ``airflow_expires_at`` so the attaching side knows the clock and
``airflow_network`` so it knows the policy, and ``Sandbox.list(tags=...)`` finds
them. Reading tags back needs ``modal>=1.5.2``, which is the extra's floor. How
to use it is on :ref:`Configuration <sandbox-attach>`.

**Network policy.** ``block_network=True`` maps exactly onto Modal's own
``block_network``, which drops all outbound traffic including DNS. A spec that
names ``allow_egress_to`` is **refused by default**, because Modal cannot combine
an allowlist with ``block_network`` at all, and its hostname allowlist is enforced
by matching the name in the TLS handshake, which means:

- TLS on port 443 to a listed host connects; any other host is refused at once.
- **Non-TLS traffic is dropped, not refused.** A plain HTTP connection to a listed
  host stalls until the client gives up, around two minutes of TCP retries for one
  address, so with the 60 s default command budget the model reads
  ``[timed out after 60s]`` and concludes its command was slow, never that the
  network stopped it.
- **The destination address is not part of the decision.** A connection opened to
  an unrelated address while presenting a listed name is routed to the listed host
  and answered by it: connecting to ``8.8.8.8:443`` with ``pypi.org`` in the
  handshake returns the certificate and content of ``pypi.org``. The allowlist is a
  name-routed egress proxy, not a filter on where packets may go.
- **DNS resolution stays open for every hostname**, listed or not, against
  authoritative servers outside Modal. A freshly generated label under a domain the
  operator controls resolves and returns its answer, so this is a two-way channel.
- A host that shares a **TLS endpoint** with a listed one can be reached by
  presenting the listed name in the handshake and the other in the request. With
  ``pypi.org`` as the only allowed host, a TLS session opened to
  ``files.pythonhosted.org`` while presenting ``pypi.org`` was allowed through and
  answered. Other tenants of the same CDN returned ``421 Misdirected Request``; it is
  co-tenancy of the same TLS endpoint that matters, which you cannot check from
  outside and which can change without notice.

So the allowlist says which name a TLS session may be routed to, and nothing else.
Pass ``ModalSandboxBackend(egress_enforcement="sni")`` to say you accept that and
have the allowlist applied:

.. code-block:: python

    SandboxToolset(
        ModalSandboxBackend(egress_enforcement="sni"),
        spec=SandboxSpec(block_network=True, allow_egress_to=["pypi.org", "files.pythonhosted.org"]),
    )

Entries must be bare hostnames or one leading ``*.`` label; a URL, a ``host:port``,
an address or a single-label name is refused, because Modal applies the list without
checking it and any of those would silently match nothing.

**An address allowlist is enforced properly, and needs no opt-in.**
``allow_egress_to_cidrs`` maps onto Modal's ``outbound_cidr_allowlist``, which
decides on the destination address for any port and protocol. Measured on
2026-09-22 with ``["1.1.1.1/32"]``: the listed address connected on 443 and on 53,
an unlisted address timed out on both, and ``block_network`` with the list set was
refused at create, as with the hostname list. This is the right mode for one
service at a fixed public address, which is the case the hostname list serves
worst. It cannot serve a package registry behind a CDN, whose addresses rotate
faster than a sandbox lives.

.. code-block:: python

    SandboxToolset(
        ModalSandboxBackend(),
        spec=SandboxSpec(block_network=True, allow_egress_to_cidrs=["203.0.113.0/24", "198.51.100.7/32"]),
    )

Four things to know about it:

- **The address has to be public, and IPv4.** Private ranges are unreachable from a
  Modal sandbox whatever the allowlist says: measured, connections to ``10.20.0.1``,
  ``172.16.0.1``, ``192.168.1.1`` and the cloud metadata address timed out both under
  an open network and with those ranges on the allowlist, so a service on your own private
  network cannot be reached this way; it needs a public address, or a way onto your
  network that Modal provides and this backend does not configure. Modal's allowlist
  also rejects IPv6 ranges outright, and the sandbox has no IPv6 route, so an IPv6
  entry is refused here with the reason.

- **Hostnames still resolve.** Modal's own resolver inside the sandbox answers
  every lookup, so ``pypi.org`` resolves to its addresses and a connection to them
  then times out. The tool description tells the model this so a successful lookup
  is not read as a reachable host. DNS is therefore still a channel out, as it is
  under the hostname list; only ``block_network=True`` with no allowlist closes it.
- **Entries are canonical CIDR.** A bare address is written as ``/32``. A range with
  host bits set, such as ``203.0.113.1/24``, is refused rather than widened to
  ``203.0.113.0/24``, because that is not what was written. A hostname, a
  URL or a ``host:port`` is refused, since Modal would accept it and match nothing.
  ``0.0.0.0/0`` and ``::/0`` are refused too: an allowlist of every address is an
  open network, and ``block_network=False`` is how to ask for one.
- **Combining the two lists weakens the address one.** Modal applies them
  together, and traffic matching either passes. Measured, adding ``pypi.org`` to the
  hostname list beside ``["1.1.1.1/32"]`` made a TCP connection to ``8.8.8.8:443``
  succeed, because port 443 is then routed by handshake name for every address. So
  a combined spec has the address list's guarantee on every port except 443, and
  the hostname list's caveats there. The hostname half keeps its
  ``egress_enforcement="sni"`` opt-in when combined, and the backend logs a warning
  at create naming the weakening.

**What the image needs.** ``write_file`` and ``list_directory`` use Modal's own
filesystem API, served by a helper Modal injects into the sandbox, so they need
nothing from the image. ``read_file`` deliberately does not: Modal's read API takes
no length, so it cannot honor a read budget, and a single call on a file that
streams without end (``/dev/zero``, a FIFO, a procfs entry) would pull it into
worker memory unbounded. That tool runs the base class's shell implementation,
which caps the read inside the guest, and the image needs ``stat``, ``head`` and
``base64``. Any Debian or Ubuntu based image, including ``python:*-slim``, has
them.

**Symlinks.** Because ``write_file`` goes through the native API, writing to a path
that is a symlink replaces the link with a regular file and leaves the original
target untouched, where a shell redirect would follow the link.

.. _sandbox-backend-opensandbox:

OpenSandbox (self-hosted remote)
--------------------------------

:class:`~airflow.providers.common.ai.sandbox.opensandbox.OpenSandboxBackend`
runs sandboxes through an `OpenSandbox <https://open-sandbox.ai/>`__ server.
The server may use Docker or Kubernetes; Airflow workers only use its HTTP API
and do not need access to the container runtime.

Install the SDK extra:

.. code-block:: bash

    pip install "apache-airflow-providers-common-ai[opensandbox]"

Use a generic Airflow connection, resolved lazily on first use:

.. code-block:: python

    from airflow.providers.common.ai.sandbox import OpenSandboxBackend
    from airflow.providers.common.ai.toolsets import SandboxToolset

    SandboxToolset(OpenSandboxBackend(opensandbox_conn_id="opensandbox_default"))

The connection ``host`` is required; ``port`` is optional, ``schema`` defaults to
``http``, and ``password`` carries the API key when required. Extras may set
``request_timeout`` (default 30 seconds) and ``use_server_proxy`` (default
``true``). Set ``opensandbox_conn_id=None`` to let the SDK read
``OPEN_SANDBOX_DOMAIN`` and ``OPEN_SANDBOX_API_KEY``.

``SandboxSpec.env`` is sent at creation. A default spec sends a deny-all
network policy; ``allow_egress_to`` becomes explicit allow rules. With
``block_network=False``, the backend omits network policy entirely so a
deployment without the egress sidecar can still run an intentionally open
sandbox. Deny/allowlist policy requires the sidecar, so the backend reads the
enforced policy back after creation and destroys the sandbox if it does not
match the requested spec. ``allow_egress_to_cidrs`` is refused: OpenSandbox only
enforces CIDR targets in ``dns+nft`` mode, and the Python SDK does not expose
that enforcement mode on policy read-back, so this backend cannot prove the
address-layer restriction is active.

Every sandbox carries ``created-by: airflow`` metadata and an
``airflow-sandbox-*`` name for attribution and cleanup. The server enforces a
sandbox lifetime (default 3600 seconds). If the SDK event stream stalls, the
worker abandons the call after the command budget plus a grace period, destroys
the sandbox, and reports ``sandbox_terminated`` so the toolset provisions a fresh
one. Output is bounded per stream after the SDK yields it; a single newline-free
line is the SDK-level exception, because the SDK assembles that line before the
backend sees it.

Constructor parameters:

- ``image``: image used by the server. Default ``"python:3.12-slim"``.
- ``cpu`` and ``memory``: resource limits. Defaults ``"1"`` and ``"2Gi"``.
- ``sandbox_timeout``: server-side lifetime in seconds. Default ``3600``.
- ``ready_timeout``: provisioning/reconnect timeout. Default ``120``.
- ``use_server_proxy``: override the connection extra for file and command calls.

The runtime remains a deployment choice. The default Docker runtime shares the
host kernel; choose a stronger runtime such as Kata when your threat model needs
a VM boundary.

.. _sandbox-backend-openshell:

OpenShell (self-hosted remote)
------------------------------

:class:`~airflow.providers.common.ai.sandbox.openshell.OpenShellSandboxBackend`
runs sandboxes through an `NVIDIA OpenShell <https://github.com/NVIDIA/OpenShell>`__
gateway, which runs each one on Docker, Podman or Kubernetes. Inside the
container the workload is confined by Landlock and seccomp and has no network
interface of its own; a per-sandbox supervisor opens every outbound connection
on its behalf, against a policy the backend writes and reads back. Airflow workers
only need gRPC access to the gateway.

Install the SDK extra, which needs Python 3.11 or later:

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
check runs before and after every command, because an administrator, an approved
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
reserves ``OPENSHELL_*``. Everything else in ``SandboxSpec.env``, ``PATH``
included, reaches commands as given, since they run without a login shell.

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
command for longer than its budget plus 30 seconds, the sandbox is destroyed and
``sandbox_terminated`` is reported. Nothing crosses the stream until the command
ends, so a proxy or load balancer in front of the gateway needs an idle timeout
longer than the longest command budget plus 30 seconds. A gateway restart stops
every process in its sandboxes and keeps their files; a command in flight is
reported to the model as having an unknown outcome rather than retried.

**Files.** ``write_file`` sends content on stdin in chunks of 768 KiB, since the
gateway limits one command argument to 32 KiB and one request to 1 MiB; a larger
write is not atomic, and it follows a symlink like a shell redirect. ``read_file``
transfers raw bytes, capped inside the guest. The image needs ``setsid`` and GNU
coreutils and findutils, which ``python:*-slim`` and other Debian or Ubuntu based
images have; the gateway's own default image has no ``python3``, which is why the
backend defaults to ``python:3.12-slim``.

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

The gateway host needs Linux with Landlock ABI 3 or later (kernel 6.2+) and
seccomp user notification, and Docker 28 or later, Podman 5, or a Kubernetes
cluster with the Agent Sandbox controller. OpenShell scopes its Docker driver to
local development and single-machine gateways. The gateway sends anonymous
telemetry unless it runs with ``OPENSHELL_TELEMETRY_ENABLED=false``.

.. _sandbox-backend-sbx:

sbx (Docker Sandboxes, local)
-----------------------------

:class:`~airflow.providers.common.ai.sandbox.SbxSandboxBackend` runs each sandbox
in a Docker Sandboxes microVM by driving the ``sbx`` CLI. Each sandbox is a real
microVM with its own kernel.

.. warning::

   **Use this backend for local development, not production.** Docker Sandboxes
   is built for running coding agents against a checkout on your own machine, and
   driving it from an Airflow worker is off-label use. A production worker would
   need the ``sbx`` binary on the host, an authenticated Docker account
   (``sbx login``), a one-time ``sbx policy init``, and on Linux, KVM or nested
   virtualization, which a worker in an unprivileged container cannot provide.

   **Orphans are not reclaimed.** There is no server-side lifetime. If the worker is
   killed outright, the microVM and its workspace directory survive; sandboxes are
   named ``airflow-sandbox-*`` so an operator can find and remove them.

Installing the CLI is a Deployment Manager prerequisite (``brew install
docker/tap/sbx`` or ``winget install Docker.sbx``); the backend needs no Python
dependency. The template image must provide GNU coreutils ``timeout``, ``base64``,
``stat``, ``head``, ``find``, ``mkdir`` and ``dirname``, which any Debian or Ubuntu
based image has.

Constructor parameters:

- ``image``: Container image for the sandbox. Default ``"python:3.12-slim"``.
- ``memory``: Memory limit in binary units. ``sbx`` enforces a 1 GiB minimum.
  Default ``"2g"``.
- ``cpus``: CPUs to allocate. ``None`` (default) uses the ``sbx`` default, which
  is every host CPU.
- ``sbx_path``: Path to the ``sbx`` binary. Default ``"sbx"``.
- ``create_timeout``: Seconds allowed for provisioning; a first-run microVM boot
  plus an image pull can be slow. Default ``600``.
- ``host_network_policy``: What ``sbx policy`` is set to on this host.
  ``"unknown"`` (default) makes ``create`` refuse any spec asking for a network
  guarantee this backend cannot make, and since ``block_network`` defaults to
  ``True`` that includes a bare ``SandboxSpec()``. Set ``"deny-all"`` after running
  ``sbx policy init deny-all``, or ``"allow-all"`` to state that egress is open
  and pass ``SandboxSpec(block_network=False)`` to match.

What differs between the backends
---------------------------------

Swapping the backend is one constructor argument, and tool names, spec and prompt
do not change. Four behaviours do, so read them before assuming the same Dag
behaves identically everywhere:

- **CPU.** ``sbx`` gives a sandbox every host CPU; Modal defaults to a request of
  0.125 of one, so set ``cpu``; OpenSandbox and OpenShell take ``cpu`` as a limit the
  server enforces.
- **Egress allowlists.** ``sbx`` enforces ``allow_egress_to`` at the host policy
  layer; Modal matches TLS handshake names, which is weaker and has to be opted
  into; OpenSandbox enforces it in an egress sidecar, and the backend reads the
  enforced policy back rather than trusting the create request; OpenShell enforces it
  in a per-sandbox supervisor on port 443 only, and reads the effective policy back at
  create and around every command. ``allow_egress_to_cidrs``
  is enforced at the address layer on Modal, refused on ``sbx``, refused by
  OpenSandbox because its SDK cannot prove that the sidecar is running in the
  ``dns+nft`` mode required for CIDR enforcement, and refused by OpenShell, whose
  address rules admit TCP on listed ports only. OpenShell also refuses
  ``block_network=False``.
- **Command timeouts.** A timeout destroys an ``sbx`` sandbox and its files;
  Modal and a server-enforced OpenSandbox timeout preserve the sandbox and files.
  OpenSandbox destroys it only if the command event stream itself stalls past the
  client-side grace period. OpenShell kills the command's session and keeps the
  sandbox, and destroys it only if the gateway stops relaying the command.
- **Symlinks.** ``write_file`` through a symlink follows the link on ``sbx`` and
  OpenShell and replaces it on Modal and OpenSandbox.
- **Attaching.** A Modal sandbox can be provisioned by one task and used by an
  agent in another (:ref:`sandbox-attach`). An ``sbx`` microVM lives on the worker
  that created it and cannot be reached from another task, OpenSandbox has no
  per-sandbox metadata the ownership rules could be kept in, and OpenShell does not
  implement attaching yet, so all three refuse ``SandboxSpec.owner`` and the toolset
  refuses ``attach_to`` for them.

.. _sandbox-byo:

Bringing your own backend
-------------------------

Any vendor that can create a sandbox, run a command in it and destroy it can plug
in. Subclass :class:`~airflow.providers.common.ai.sandbox.SandboxBackend` in your
own package and pass an instance to ``SandboxToolset``.

**Three methods are required**: ``create``, ``run_command`` and ``destroy``. The
file operations ship as defaults implemented over ``run_command``, because
reading, writing, listing and exporting a file are all expressible as shell
commands. The default ``export_file``, behind ``SandboxToolset(exports=...)``,
copies a file in 4 MiB slices, one command each, and needs ``stat``, ``tail``,
``head`` and ``base64`` in the guest. It relies on ``run_command`` returning each
slice's output intact, or setting ``stdout_truncated`` when it could not. Override
it when the vendor can stream a download, as the ``sbx`` and OpenSandbox backends do. Override the others
only when the vendor has a native file API:

.. code-block:: python

    from airflow.providers.common.ai.sandbox import (
        SandboxBackend,
        SandboxExecResult,
        SandboxSpec,
    )


    class AcmeSandboxBackend(SandboxBackend):
        name = "acme"

        def create(self, *, spec: SandboxSpec | None = None) -> str:
            return acme_sdk.create_sandbox().id

        def run_command(self, sandbox, command, *, timeout, max_output_bytes):
            r = acme_sdk.exec(sandbox, command, timeout=timeout)
            return SandboxExecResult(exit_code=r.exit_code, stdout=r.stdout, stderr=r.stderr)

        def destroy(self, sandbox) -> None:
            acme_sdk.delete_sandbox(sandbox)

        # Optional: inherited from SandboxBackend unless the vendor has
        # something better than shelling out.
        def read_file(self, sandbox, path, *, max_bytes) -> bytes:
            return acme_sdk.download(sandbox, path, limit=max_bytes)

Four rules for an implementation:

- Constructors run at Dag-parse time, so resolve credentials lazily, on first use.
- ``destroy`` must be idempotent; destroying an already-gone sandbox is not an error.
- Raise ``SandboxTerminalError`` when retrying cannot help and ``SandboxError``
  when it might. The first fails the task for Airflow to retry; the second
  becomes a bounded prompt back to the model.
- If you cannot enforce something the ``SandboxSpec`` asks for, **raise**. Never
  provision a weaker sandbox than the Dag author asked for.

**If your sandboxes can be found again from another process**, subclass
:class:`~airflow.providers.common.ai.sandbox.AttachableSandboxBackend` instead,
and a ``@task`` can provision a sandbox for an agent task to attach to
(:ref:`sandbox-attach`). It adds two methods, ``read_tags`` and ``write_tags``,
over whatever key-value metadata the vendor keeps on a sandbox, and the ownership
rules are written once on the base class on top of them. Three things the base
class relies on: ``read_tags`` raises ``SandboxTerminalError`` for a sandbox that
does not exist or has ended; ``write_tags`` replaces the whole set, because
releasing a claim is a rewrite without the holder key; and ``create`` stamps
``SandboxSpec.owner`` under ``OWNER_TAG``, the sandbox's end time as Unix seconds
under ``EXPIRES_AT_TAG``, and the network policy under ``NETWORK_TAG`` using
``encode_network_policy(spec)``, all importable from
``airflow.providers.common.ai.sandbox.base``. If the vendor lets a caller of your
backend set metadata too, make your reserved keys overwrite theirs. Stamping the
working directory under ``WORKDIR_TAG`` is optional; without it the attaching
backend asks the sandbox. The toolset shortens ``run_command`` to the remaining
lifetime itself; a backend clamps only if its own file operations need it.
