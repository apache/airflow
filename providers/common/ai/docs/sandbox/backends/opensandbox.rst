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

.. _sandbox-backend-opensandbox:

OpenSandbox (self-hosted remote)
================================

.. note::

    Experimental: this can change or be removed in a minor release of this provider.
    See :ref:`howto/stability`.

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
match the requested spec. ``allow_egress_to_cidrs`` is refused; this backend does
not translate address ranges into OpenSandbox rules.

The read-back also checks the mode the sidecar enforces the policy in, set by
``[egress] mode`` in the server configuration. Only ``dns+nft`` drops connections at
the address layer. In ``dns`` mode, the server's default, the sidecar filters name
resolution and nothing else, so a deny-all sandbox could still connect to any
numeric address, including a DNS-over-HTTPS resolver that would look up any name for
it, and a sandbox with an allowlist could connect to addresses outside the list. The
backend destroys the sandbox and fails the task on any mode but ``dns+nft``, so set
``mode = "dns+nft"`` on a server that runs sandboxes with a deny-all policy or an
allowlist, or pass ``SandboxSpec(block_network=False)`` for a sandbox that may reach
the network.

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
