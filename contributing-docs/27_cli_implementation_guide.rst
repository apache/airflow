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

CLI Implementation Guide
==================================

This document describes the direction for implementing new CLI functionality in Apache Airflow.

.. contents:: Table of Contents
   :depth: 2
   :local:


Overview
--------

Airflow ships two CLIs:

- **airflow** (``airflow-core``) — bundled with the core distribution. Hosts both legacy
  remote commands (being rewired internally) and admin/local commands that have no Public
  API equivalent.
- **airflowctl** (``airflow-ctl``) — a standalone CLI distributed separately that talks to a
  running Airflow instance exclusively through the Public (Core) API.

Following AIP-94 (tracked via `GitHub Projects #570 and #571
<https://github.com/orgs/apache/projects/570>`_), CLI work follows two rules:

1. **New commands** that are achievable via the Public API are added to ``airflowctl``
   **only**. Adding the same command to the ``airflow`` CLI as well is discouraged — it
   duplicates maintenance surface without user benefit.
2. **Existing** ``airflow`` **CLI remote commands** stay in place (so users keep running
   ``airflow dags list``, ``airflow pools get``, …) but are rewired internally to call the
   Public API via the ``airflowctl`` HTTP client instead of accessing the metadata
   database directly.

Both rules enforce RBAC, remove direct database exposure for remote operations, and eliminate
duplicate code paths. This builds on AIP-81, which introduced the distinction between *local*
and *remote* commands.

Decision Table
---------------

.. list-table::
   :header-rows: 1
   :widths: 35 35 30

   * - Scenario
     - Where it goes
     - Notes
   * - New command, achievable via Public API
     - ``airflowctl`` only
     - Do not add to the ``airflow`` CLI unless strongly needed in core
   * - New command, not achievable via Public API
     - ``airflow`` CLI (admin/local)
     - Admin/local commands only — see below
   * - Existing ``airflow`` CLI command, achievable via Public API
     - ``airflow`` CLI → delegates to the ``airflowctl`` HTTP client
     - Rewire; no direct DB access, no SQLAlchemy/``session`` usage
   * - Existing ``airflow`` CLI command, not achievable via Public API
     - ``airflow`` CLI, unchanged
     - Stays as a pure ``airflow-core`` implementation

"Not achievable via the Public API" means the operation has no API representation and is
inherently admin/local in nature — database shell, schema migrations, process management,
or deployment configuration that requires direct infrastructure access.

Adding a New Command (Public API achievable)
---------------------------------------------

Add the command to ``airflowctl`` **only**. Do not also add it to the ``airflow`` CLI unless
there is a strong reason it must live in core (e.g., it is tightly coupled to a local process
or a deployment concern with no API representation).

Source location: ``airflow-ctl/src/airflowctl/ctl/commands/``

HTTP client and operations: ``airflow-ctl/src/airflowctl/api/`` (``client.py``, ``operations.py``).

1. Add the command under the appropriate group module in
   ``airflow-ctl/src/airflowctl/ctl/commands/``.
2. Call the Public API through the ``airflowctl`` HTTP client (``airflowctl.api.client``) and
   the operations layer in ``airflowctl.api.operations``. Do not import ``airflow-core``
   models or touch the metadata database.
3. If the required API endpoint does not exist yet, add it first
   (see `Adding API Endpoints <16_adding_api_endpoints.rst>`__).
4. Add tests under ``airflow-ctl/tests/``.
5. Run integration tests with:

   .. code-block:: bash

      breeze testing airflow-ctl-integration-test

Rewiring an Existing ``airflow`` CLI Command
---------------------------------------------

Use this when an existing ``airflow`` CLI remote command still talks to the database
directly and needs to go through the Public API instead. The user-facing command name and
arguments stay the same.

Source location: ``airflow-core/src/airflow/cli/``

1. Replace direct database access with calls through the ``airflowctl`` HTTP client
   (``airflowctl.api.client`` / ``airflowctl.api.operations``). Remove SQLAlchemy model
   imports and ``session``\-based helpers from the command.
2. If the required API endpoint does not exist yet, add it first
   (see `Adding API Endpoints <16_adding_api_endpoints.rst>`__).
3. Update tests under ``airflow-core/tests/cli/`` to mock or exercise the HTTP client
   instead of the database.

Adding an Admin/Local Command (no Public API equivalent)
---------------------------------------------------------

Use this only when the operation cannot reasonably be exposed through the Public API —
typically database shell, schema migrations, process management, or deployment-time
configuration.

Source location: ``airflow-core/src/airflow/cli/``

1. Add the command to an appropriate admin group (e.g., ``db``, ``config``).
2. Add ``(admin only)`` to the ``help`` string so users know the command requires direct
   infrastructure access.
3. Add tests under ``airflow-core/tests/cli/``.

References
-----------

- `AIP-94 Confluence page <https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=382175838>`_
- `Adding API Endpoints <16_adding_api_endpoints.rst>`__
- `Airflow Ctl Tests <testing/airflow_ctl_tests.rst>`__
- `GitHub Project #570 <https://github.com/orgs/apache/projects/570>`_
- `GitHub Project #571 <https://github.com/orgs/apache/projects/571>`_
