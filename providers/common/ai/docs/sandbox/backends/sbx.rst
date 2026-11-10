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

.. _sandbox-backend-sbx:

sbx (Docker Sandboxes, local)
=============================

.. note::

    Experimental: this can change or be removed in a minor release of this provider.
    See :ref:`howto/stability`.

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

The ``sbx`` CLI has no way to pass environment variables at creation, so the backend
writes ``SandboxSpec.env`` as ``export`` lines to ``/etc/profile`` inside the
sandbox. Anything in the sandbox can read that file for as long as the sandbox
exists.
