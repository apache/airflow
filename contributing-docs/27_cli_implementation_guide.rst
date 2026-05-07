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

CLI Implementation Guide (AIP-94)
==================================

This document describes the direction for implementing new CLI functionality in Apache Airflow
following `AIP-94: Decoupling Remote Commands from Airflow CLI to airflowctl
<https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=382175838>`_.

.. contents:: Table of Contents
   :depth: 2
   :local:


Overview
--------

As of Airflow 3.3 (tracked via `GitHub Projects #570 and #571
<https://github.com/orgs/apache/projects/570>`_), the ``airflow`` CLI commands are being
rearchitected so that **remote commands delegate their implementation to the** ``airflowctl``
**HTTP client** rather than accessing the metadata database directly.

The user-facing ``airflow`` CLI commands remain unchanged — users keep running
``airflow dags list``, ``airflow pools get``, and so on. What changes is the internal
implementation: remote commands call the Public (Core) API through the ``airflowctl`` client
instead of querying the database directly. This enforces RBAC, removes direct database exposure,
and eliminates duplicate code paths.

This builds on AIP-81, which introduced the distinction between *local* and *remote* commands.

Decision Rules
--------------

**Adding a brand-new command**

Prefer adding new commands directly to ``airflowctl`` rather than to the ``airflow`` CLI.
If the operation is achievable via the Public API, implement it in ``airflowctl`` only unless
there is a strong reason it must live in core (e.g., it is tightly coupled to a local process
or a deployment concern that has no API representation). Adding it to the ``airflow`` CLI as
well is discouraged — it creates duplicate maintenance surface without user benefit.

**Modifying an existing command**

For existing ``airflow`` CLI commands, apply one rule:

- **Achievable via the Public API?** → rewire the implementation to use the ``airflowctl``
  HTTP client. The command stays in ``airflow-core/src/airflow/cli/`` but delegates all data
  access to ``airflowctl``. It must not import SQLAlchemy models or call ``session``\-based
  helpers.
- **Not achievable via the Public API** (database shell, migrations, process management,
  deployment configuration) → pure core implementation inside ``airflow-core``. No API call,
  no ``airflowctl`` dependency.

.. list-table::
   :header-rows: 1
   :widths: 35 35 30

   * - Scenario
     - Where it goes
     - Notes
   * - New command, achievable via Public API
     - ``airflowctl`` only
     - Do not add to ``airflow`` CLI unless strongly needed in core
   * - New command, not achievable via Public API
     - ``airflow`` CLI, pure core
     - Admin/local commands only
   * - Existing command, achievable via Public API
     - ``airflow`` CLI → delegates to ``airflowctl`` client
     - Rewire; no direct DB access
   * - Existing command, not achievable via Public API
     - ``airflow`` CLI, pure core
     - No change in approach

Implementing a Command Backed by the Public API
------------------------------------------------

Source location: ``airflow-core/src/airflow/cli/``

1. Add or update the command in the appropriate CLI group.
2. Use the ``airflowctl`` HTTP client (``airflow-ctl/src/airflowctl/``) for all data access.
3. If the required API endpoint does not exist yet, add it first
   (see `Adding API Endpoints <16_adding_api_endpoints.rst>`__).
4. Add tests under ``airflow-core/tests/cli/`` that mock or exercise the HTTP client.
5. Run integration tests with:

   .. code-block:: bash

      breeze testing airflow-ctl-integration-test

Implementing a Pure Core Command
----------------------------------

Source location: ``airflow-core/src/airflow/cli/``

1. Add or update the command in the appropriate admin group (e.g., ``db``, ``config``).
2. Add ``(admin only)`` to the ``help`` string.
3. Add tests under ``airflow-core/tests/cli/``.

Bug Fixes
----------

- Bug fixes for existing ``airflow`` CLI commands should target the ``v3-2-test`` branch
  (backported to ``main`` as needed), as the CLI rearchitecture work is scheduled for 3.3.
- Bug fixes for the ``airflowctl`` client itself target ``main``.

References
-----------

- `AIP-94 Confluence page <https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=382175838>`_
- `Adding API Endpoints <16_adding_api_endpoints.rst>`__
- `Airflow Ctl Tests <testing/airflow_ctl_tests.rst>`__
- `GitHub Project #570 <https://github.com/orgs/apache/projects/570>`_
- `GitHub Project #571 <https://github.com/orgs/apache/projects/571>`_
