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

.. _sandbox-backend-modal:

Modal (hosted)
--------------

:class:`~airflow.providers.common.ai.sandbox.modal.ModalSandboxBackend` runs each
sandbox in Modal, provisioned over the API. Of the backends that ship with the
provider, **this is the one to use in production**, and the only one that runs on
Kubernetes: nothing has to be installed on the worker, model-written code never
executes on the worker host, and Modal reclaims a sandbox at its own lifetime
whether or not the worker survives. It needs the ``modal`` extra and ambient
credentials, as under :ref:`Quick start <sandbox-quick-start>`.

Constructor parameters:

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
- ``egress_enforcement``: ``"strict"`` (default) or ``"sni"``. See below.

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

What differs between the two
----------------------------

Swapping the backend is one constructor argument, and tool names, spec and prompt
do not change. Four behaviours do, so read them before assuming the same Dag
behaves identically in both places:

- **CPU.** ``sbx`` gives a sandbox every host CPU; Modal defaults to a fraction of
  one, so set ``cpu``.
- **Egress allowlists.** ``sbx`` enforces ``allow_egress_to`` at the host policy
  layer; Modal matches TLS handshake names, which is weaker and has to be opted
  into. ``allow_egress_to_cidrs`` is enforced at the address layer on Modal and
  refused on ``sbx``, which has no per-sandbox address rule.
- **Command timeouts.** A timeout destroys an ``sbx`` sandbox and its files; a
  Modal sandbox survives with its files intact.
- **Symlinks.** ``write_file`` through a symlink follows the link on ``sbx`` and
  replaces it on Modal.

Bringing your own backend
-------------------------

Any vendor that can create a sandbox, run a command in it and destroy it can plug
in. Subclass :class:`~airflow.providers.common.ai.sandbox.SandboxBackend` in your
own package and pass an instance to ``SandboxToolset``.

**Three methods are required**: ``create``, ``run_command`` and ``destroy``. The
three file operations ship as defaults implemented over ``run_command``, because
reading, writing and listing a file are all expressible as shell commands.
Override them only when the vendor has a native file API:

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
