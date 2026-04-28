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

Airflow Helm Chart - Kustomize Overlays
=======================================

This directory contains Kustomize overlays that complement the Airflow Helm
chart for components that are not Airflow-native. They are shipped alongside
the chart as a guide for users, **not** as part of the released chart artifact.

The motivation, criteria, and lifecycle for these overlays are defined in
``CONTRIBUTING.rst`` in this directory.

Available overlays
------------------

+----------+----------------------+----------------------------------------------+
| Overlay  | STATUS               | Purpose                                      |
+==========+======================+==============================================+
| ``keda`` | not-tested (PoC)     | Autoscaling for Celery workers via KEDA.     |
+----------+----------------------+----------------------------------------------+

Each overlay directory contains its own ``README.rst`` with usage details and
a migration guide from the equivalent chart-side configuration.

Using an overlay
----------------

The overlays are designed for the "standalone addition" pattern. They do not
modify resources rendered by the chart. A typical workflow is:

1. Install the Airflow chart as usual.
2. Reference the overlay from your own ``kustomization.yaml`` and apply the
   substitutions described in the overlay's ``README.rst`` (release name,
   namespace, secret references).
3. Apply the rendered manifests with ``kubectl apply -k`` against the same
   namespace as the chart release.

Overlays are versioned with the chart. When pinning, use a tag matching the
chart version (for example ``helm-chart/1.21.0``) rather than ``main``.

Status conventions
------------------

Each overlay carries a ``STATUS.yaml`` file that declares its verification level:

* ``tested`` - the overlay is verified in CI against the current chart version.
* ``not-tested`` - the overlay builds successfully but has no functional CI
  coverage. Treat it as a starting point that you adapt to your environment.
* ``deprecated`` - the overlay is scheduled for removal. The ``STATUS.yaml`` file
  carries a ``message`` field pointing to the replacement.

See ``CONTRIBUTING.rst`` for the full status grammar and lifecycle.
