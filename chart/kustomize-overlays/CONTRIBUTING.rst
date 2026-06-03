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

Contributing Kustomize Overlays
===============================

This document is the authoritative reference for adding, evolving, and
retiring overlays under ``chart/kustomize-overlays/``.

Why this directory exists
-------------------------

The Airflow Helm chart has historically carried components that are not
Airflow-native. They make the chart heavier than it needs to be and pull
maintenance toward things that already have external owners. Expressing
these components as Kustomize overlays keeps the chart focused on Airflow
itself while still giving users a working starting point for the rest.

The chart never removes a component without a working overlay already in
place. Users always have a migration path before anything disappears.

Criteria for chart vs Kustomize
-------------------------------

A component **belongs in the chart** when all of the following are true:

* It is required to run Airflow (scheduler, API server, dag-processor,
  triggerer, workers).
* Removing/adding it requires changes to Airflow's own configuration.
* It has no external owner.
* It is used by the larger majority of users (>80%)

A component **belongs in Kustomize** when any of the following are true:

* It can be expressed as a standalone Kubernetes resource without modifying
  chart-rendered resources.
* It is environment-specific (authentication schemes, logging backends,
  autoscaling controllers, etc.).
* It has an external owner (KEDA, Elasticsearch, any PostgreSQL distribution, etc.).
* It requires CRDs that the chart does not install.
* It is used by a minority of users, such that the additional complexity and maintenance burden do not pay off

If a component qualifies for Kustomize but no overlay exists yet, it stays in
the chart until the overlay is in place and verified.

Overlay structure
-----------------

Each overlay directory must contain:

* ``kustomization.yaml`` - the Kustomize entry point.
* The Kubernetes resources the overlay produces.
* ``STATUS.yaml`` - a small YAML document declaring the verification state.
* ``README.rst`` - usage instructions and a migration guide from the
  equivalent chart-side configuration.

STATUS file format
------------------

The ``STATUS.yaml`` file is a small YAML document with the following fields.

For a verified overlay:

.. code-block:: yaml

    status: tested
    chart-version: "1.21.0"
    last-verified: "2026-04-25"

For a starting-point overlay without functional CI coverage:

.. code-block:: yaml

    status: not-tested
    reason: "Pending community validation. Use as a starting point only."

For an overlay scheduled for removal:

.. code-block:: yaml

    status: deprecated
    message: "Replaced by <overlay-name>. Will be removed in chart 3.0.0."

Lifecycle
---------

The lifecycle mirrors how providers work, just on a smaller scale. Two
checks gate the ``STATUS`` field, and they are deliberately separate.

The ``build_kustomize_overlays`` prek hook
(``scripts/ci/prek/build_kustomize_overlays.py``) runs on every commit and
applies a generic structural check to every overlay: the build succeeds, the
output parses as valid YAML, every resource has ``apiVersion``, ``kind`` and
``metadata.name``, and there are no duplicate resource keys. This is enough
to catch most authoring mistakes but it does not validate against the CRD
schemas of the controllers the overlay targets, and nothing is ever applied
to a live cluster.

A functional integration test is the separate, stronger check. It applies
the overlay against a real cluster (typically a kind cluster with the chart
already installed and the relevant controller running) and asserts the
runtime behaviour the overlay promises. Until such a test exists for an
overlay, its ``STATUS`` must stay at ``not-tested``.

Lifecycle steps:

* A new overlay is proposed via a PR and lands with ``status: not-tested``.
  The prek hook automatically applies the generic structural check; if the
  overlay needs invariants beyond that (for example a cross-reference
  between resources), they belong in the integration test, not in the prek
  hook.
* A follow-up PR adds a functional integration test for the overlay. Once
  that test passes, ``STATUS`` is flipped to ``tested``.
* An overlay is deprecated by setting ``status: deprecated`` together with a
  ``message`` field pointing to the replacement.
* Deprecated overlays remain for one chart major version before they are
  removed, so users always have a window to migrate.

Adding a new overlay
--------------------

1. Confirm the component meets the Kustomize criteria above.
2. Create ``chart/kustomize-overlays/<name>/`` with the required files.
3. Use placeholders such as ``RELEASE-NAME`` for values the user must fill in,
   and document the substitutions in the overlay's ``README.rst``.
4. Land the PR with ``status: not-tested``.
5. Add a row to the table in ``chart/kustomize-overlays/README.rst``.
6. Follow up with a CI test and flip ``STATUS`` to ``tested``.

Migration guide pattern
-----------------------

Each overlay ``README.rst`` should include a migration guide section with
exactly three parts:

1. **What the chart currently does** - the relevant ``values.yaml`` keys and
   the Kubernetes resources they produce today.
2. **What the overlay provides** - the equivalent resources rendered from the
   overlay.
3. **How to switch** - step-by-step instructions, with the explicit order of
   operations.

The guide must be written against the current chart template. It is not
speculative documentation.
