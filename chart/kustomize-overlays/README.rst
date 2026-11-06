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

.. note::

   **Not distributed with chart releases.**
   This directory lives in the source repository as a reference for users but
   is **not** packaged or published as part of the official Airflow Helm chart
   release artifacts. Consume it directly from the repository at the tag that
   matches your chart version.

This directory contains Kustomize overlays that complement the Airflow Helm
chart for components that are not Airflow-native.

The motivation, criteria, and lifecycle for these overlays are defined in
``CONTRIBUTING.rst`` in this directory.

Available overlays
------------------

+--------------+----------------------+----------------------------------------------+
| Overlay      | STATUS               | Purpose                                      |
+==============+======================+==============================================+
| ``keda``     | not-tested (PoC)     | Autoscaling for Celery workers via KEDA.     |
+--------------+----------------------+----------------------------------------------+
| ``kerberos`` | tested               | In-cluster test KDC + keytab Secret bootstrap|
|              |                      | consumable by the chart's kerberos sidecar.  |
+--------------+----------------------+----------------------------------------------+

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

Status conventions
------------------

Each overlay carries a ``STATUS.yaml`` file that declares its verification level:

* ``tested`` - the overlay is verified in Apache Airflow CI against the current chart version.
* ``not-tested`` - the overlay builds successfully but has no functional CI
  coverage. Treat it as a starting point that you adapt to your environment.
* ``deprecated`` - the overlay is scheduled for removal. The ``STATUS.yaml`` file
  carries a ``message`` field pointing to the replacement.

The ``STATUS.yaml`` may also carry a ``verify:`` block listing the
resources the overlay produces and how to check them. This block is the
contract consumed by ``breeze k8s smoke-test-overlay <name>`` (the
functional smoke test) - the structural prek hook only validates its
schema.

See `CONTRIBUTING.rst <CONTRIBUTING.rst>`_ for the full status grammar,
lifecycle, and the exact local commands for advancing an overlay's
``STATUS`` to ``tested``.
