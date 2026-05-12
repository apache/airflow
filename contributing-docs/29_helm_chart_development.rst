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

Developing the Helm Chart
=========================

Use this document when working on the Airflow Helm chart to decide where a
change belongs (chart, Kustomize overlay, or out entirely) and how to shape
it. It is the contributor-facing guide for chart development; the full
alignment narrative lives on Confluence (`Helm Refurbish
<https://cwiki.apache.org/confluence/display/AIRFLOW/Helm+Refurbish>`__).

.. contents:: Table of Contents
   :depth: 2
   :local:


Decision tree
-------------

Start here when adding or modifying a chart parameter or component.

**1. Is this a new feature or a change to an existing one?**

- New feature → go to **2**.
- Change to an existing parameter → go to **4**.

**2. Does the feature meet all three "belongs in the chart" criteria below?**

- Yes → **chart**.
- No → go to **3**.

**3. Does the feature meet any "belongs in Kustomize" criterion below?**

- Yes → **Kustomize overlay**, subject to the exceptions process. The chart
  does not lose a component until a working overlay exists.
- No → the feature does not fit either bucket cleanly. Bring it to the
  ``dev@`` list for a scope discussion before opening a PR.

**4. Is the parameter under the right parent section?**

- Yes → go to **5**.
- No → relocate it to its owning component. The canonical name changes; the
  value continues to work.

**5. Is the same setting reachable through more than one path?**

- Yes → consolidate to one path.
- No → go to **6**.

**6. Are the defaults sensible at the chart level for the common case?**

- Yes → done.
- No → tighten the default to a least-privilege / common-case baseline. Leave
  finer shaping to overlays.


Decision criteria: chart vs Kustomize
-------------------------------------

The canonical criteria document lives at
``chart/kustomize-overlays/CONTRIBUTING.rst``. The summary below mirrors that
document and is aligned with the chart documentation convention.

**Belongs in the chart (all must be true)**

- Required to run Airflow (scheduler, API server, dag-processor, triggerer,
  workers).
- Removing it requires changes to Airflow's own configuration.
- No external ownership.

**Belongs in Kustomize (any may be true)**

- Can be expressed as a standalone Kubernetes resource without modifying
  chart-rendered resources.
- Environment-specific (authentication schemes, logging backends, autoscaling
  controllers).
- Has an external owner (for example, KEDA, Elasticsearch, any PostgreSQL
  distribution).
- Requires CRDs that the chart does not install.

**Exceptions process**

- If a component qualifies for Kustomize but has no overlay yet, it stays in
  the chart.
- The chart never removes a component without a working overlay already in
  place. This is the invariant that protects users.


Component reference
-------------------

Where each component currently lives. Use this when adding parameters or
templates that touch a specific component.

.. list-table::
   :header-rows: 1
   :widths: 30 25 45

   * - Component
     - Where it lives
     - Notes
   * - Flower
     - Chart
     -
   * - Redis
     - Chart
     - Lives under the celery section — exists to support the Celery executor.
   * - PgBouncer
     - Chart
     - Service only; PgBouncer ingress is not part of the chart.
   * - Jobs
     - Chart
     - Lives under the kubernetes section.
   * - OpenTelemetry
     - Chart
     - OTel is the designated primary telemetry path and is to be supported by the chart.
   * - Ingress
     - Chart
     - Per-component ingress only. Do not add ``registry.secretNames``,
       ``securityContext``, ``ingress.apiServer.host``,
       ``ingress.apiServer.tls.*``, or a top-level ``ingress.enabled`` here —
       those belong with their owning components, not under ingress.
   * - ``allowJobLaunching`` / ``allowPodLaunching``
     - Chart
     - Two separate switches by design. Do not collapse into an auto-detect
       parameter — auto-detect removes flexibility without buying meaningful
       simplification.
   * - ``serviceAccount``
     - Chart
     - Sensible defaults at the chart level; finer shaping via overlays.
   * - Kerberos
     - Kustomize overlay
     - Sidecar-injection pattern.
   * - gitSync
     - Not in chart
     - Replaced by ``GitDagBundle`` (Airflow-native). An overlay exists only
       if community demand persists after the ``GitDagBundle`` migration is
       documented.
   * - Elasticsearch / OpenSearch
     - Kustomize overlay
     - Logging-backend choice is environment-specific.
   * - PostgreSQL (StatefulSet)
     - Kustomize overlay
     - The chart does not ship a database. Production deployments expect
       production-grade Postgres regardless.
   * - StatsD
     - Kustomize overlay
     - OpenTelemetry is the chart-level default.
   * - HPA / KEDA
     - Kustomize overlay
     - Standalone-addition pattern.
   * - Telemetry / monitoring (general)
     - Kustomize overlay
     - Beyond the OpenTelemetry default. Specific monitoring stacks are
       environment choices.


Authoring conventions
---------------------

When adding or changing chart parameters, follow these conventions.

**Configuration via environment variables.** ``airflow.cfg`` is decoupled
from the chart; configuration is expressed through environment variables.
The chart keeps a basic cfg surface — complex per-component configuration
goes through overlays.

**Persistence follows the bundles model.** Log and Dag folders are two
distinct types: ``Log`` and ``Bundles``. One ``DagBundle`` per deployment,
multiple ``DagProcessor`` instances per bundle. Multi-team support fits this
shape rather than retrofitted onto a different model.

**Parameters belong with their owning component.** Place a setting under
the component that consumes it. Redis configuration goes under the celery
section, jobs configuration under kubernetes, and so on. Do not introduce
top-level keys for component-scoped settings.

**No duplicate definition paths.** A given image is defined in one place.
A given setting is reachable from one section. If you find yourself adding
a parameter that overlaps an existing one, consolidate instead of layering.

**Defaults are least-privilege.** Security context is enforced at the
component and container level. Roles and role bindings follow the same
principle. Probes (readiness, liveness) cover the common case in the chart;
edge cases live in overlays.


Quality bar for overlays
------------------------

Overlays are not second-class. Three things travel with every overlay PR:

- **Tests in CI** where the environment allows. Each overlay carries a
  recorded status — tested or not tested — so users know what they are
  picking up before they adopt it.
- **Validation tooling.** Linters and the standard Kustomize validators run
  against the overlays themselves; correctness is not deferred to the user.
- **First-class contributor experience.** Clear documentation, a
  chart-vs-overlay reasoning note, structural checks analogous to the
  project's other ``uv``-driven tooling, and a contribution guide modelled
  on how providers are managed.
