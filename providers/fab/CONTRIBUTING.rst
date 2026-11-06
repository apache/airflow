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

Contributing to the FAB provider
================================

This page collects developer notes for the ``apache-airflow-providers-fab``
distribution. It is intentionally kept out of the published provider
documentation under ``docs/`` — it targets contributors editing the provider,
not deployment users.

Upgrading Flask-AppBuilder
--------------------------

The FAB provider is **tightly coupled** to a specific Flask-AppBuilder (FAB)
release: it vendors and subclasses large parts of FAB's security manager in
``src/airflow/providers/fab/auth_manager/security_manager/override.py`` (and
vendors FAB's web assets/templates under ``src/airflow/providers/fab/www/``).
Bumping the pinned ``flask-appbuilder`` version is therefore never just a pin
change — the vendored code must be reconciled against the new release, which
``tests/unit/fab/auth_manager/security_manager/test_fab_alignment.py`` enforces
in CI.

Use the ``upgrade-fab-provider`` agent skill
(``.agents/skills/upgrade-fab-provider/``) to perform a bump end-to-end. It
drives the pin change, the ``uv.lock`` regeneration, the alignment-test
tripwire, the manual review of the vendored ``override.py`` against upstream
FAB, and the generated-README sync — and documents the gotchas that a green
alignment test alone does not catch (a security fix can live inside a method
Airflow vendors).

Past FAB version bumps, for reference (newest first):

* ``5.2.1`` -> ``5.2.2``: https://github.com/apache/airflow/pull/69730
* ``5.2.0`` -> ``5.2.1``: https://github.com/apache/airflow/pull/66841
* ``5.0.1`` -> ``5.2.0``: https://github.com/apache/airflow/pull/62924
* ``5.0.0`` -> ``5.0.1``: https://github.com/apache/airflow/pull/57170
* ``4.6.3`` -> ``5.0.0``: https://github.com/apache/airflow/pull/50960
* ``4.5.3`` -> ``4.6.3``: https://github.com/apache/airflow/pull/50513

When you complete a bump, prepend your PR to this list so the history stays
current.
