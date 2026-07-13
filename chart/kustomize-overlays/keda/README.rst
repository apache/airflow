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

KEDA Autoscaler Overlay
=======================

This overlay produces a `KEDA <https://keda.sh/>`__ ``ScaledObject`` plus a
``TriggerAuthentication`` for the chart-rendered Celery workers. It is a
standalone addition: no resource produced by the Helm chart is modified.

It is the Kustomize equivalent of enabling ``workers.celery.keda.enabled``
in ``values.yaml``, and is the recommended migration path for users who
want to keep Celery autoscaling without relying on chart-side templating.

Prerequisites
-------------

* `KEDA <https://keda.sh/docs/latest/deploy/>`__ installed in the cluster.
* The Airflow chart installed with the **CeleryExecutor**.
* The chart's metadata Secret (``<release>-airflow-metadata``) reachable
  from KEDA's namespace - usually the same namespace as the chart release.

Resources produced
------------------

* ``TriggerAuthentication/airflow-keda-postgres-auth`` - reads the metadata
  DB connection string directly from the chart's metadata Secret.
* ``ScaledObject/airflow-worker`` - targets the chart-rendered worker
  Deployment and scales it based on the count of running and queued task
  instances.

Usage
-----

Reference this overlay from your own kustomization and substitute the
release name. A minimal example:

.. code-block:: yaml

    # my-overlay/kustomization.yaml
    apiVersion: kustomize.config.k8s.io/v1beta1
    kind: Kustomization
    namespace: airflow

    resources:
      - github.com/apache/airflow/chart/kustomize-overlays/keda?ref=helm-chart/1.21.0

    replacements:
      - source:
          kind: ConfigMap
          name: airflow-overlay-config
          fieldPath: data.releaseName
        targets:
          - select:
              kind: ScaledObject
              name: airflow-worker
            fieldPaths:
              - spec.scaleTargetRef.name
            options:
              delimiter: "-"
              index: 0
          - select:
              kind: TriggerAuthentication
              name: airflow-keda-postgres-auth
            fieldPaths:
              - spec.secretTargetRef.0.name
            options:
              delimiter: "-"
              index: 0

    configMapGenerator:
      - name: airflow-overlay-config
        literals:
          - releaseName=airflow

Apply with:

.. code-block:: bash

    kubectl apply -k my-overlay/

For a quick test, you can also just ``sed`` the placeholder:

.. code-block:: bash

    kustomize build chart/kustomize-overlays/keda | \
      sed 's/RELEASE-NAME/airflow/g' | \
      kubectl apply -f -

Tuning the trigger query
------------------------

The default query mirrors the chart for a single Celery queue named
``default`` with ``worker_concurrency=16``. If you set different values in
your chart install, edit ``scaledobject.yaml`` accordingly:

* Replace ``16`` with the value of ``config.celery.worker_concurrency``.
* Extend ``queue IN ('default')`` to list every entry from
  ``workers.celery.queue`` (comma-separated in ``values.yaml``, single-quoted
  here).

Pgbouncer
---------

If pgbouncer is enabled and you do not want KEDA polling through it, change
the ``key`` field in ``triggerauthentication.yaml`` from ``connection`` to
``kedaConnection``. The chart writes a direct-to-Postgres connection string
under that key for exactly this purpose.

Persistence
-----------

If your worker is deployed as a ``StatefulSet`` (i.e. you set
``workers.celery.persistence.enabled=true``), change ``kind: Deployment`` to
``kind: StatefulSet`` under ``scaleTargetRef`` in ``scaledobject.yaml``.

Migration guide from the chart
------------------------------

What the chart currently does
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When ``workers.celery.keda.enabled=true``, the chart renders:

* A ``ScaledObject`` named ``<release>-worker`` targeting the worker
  Deployment or StatefulSet.
* The KEDA trigger reads the connection string from the worker pod's
  ``KEDA_DB_CONN`` (or ``AIRFLOW_CONN_AIRFLOW_DB``) env var, which is
  itself sourced from the metadata Secret.
* Tuning is exposed under ``workers.celery.keda.*`` in ``values.yaml``.

What this overlay provides
^^^^^^^^^^^^^^^^^^^^^^^^^^

* The same ``ScaledObject`` as a standalone resource.
* A ``TriggerAuthentication`` that reads the connection string directly
  from the chart's metadata Secret. This avoids the indirection through
  the worker pod env var, and means the overlay does not need to patch any
  chart-rendered resource.

How to switch
^^^^^^^^^^^^^

1. Install or upgrade the chart with ``workers.celery.keda.enabled=false``.
2. Render this overlay with the substitutions described above.
3. Apply the rendered manifests.
4. Confirm KEDA reports ``ScaledObject`` ``Ready`` and the worker scales
   on demand. Useful command:

   .. code-block:: bash

       kubectl describe scaledobject airflow-worker -n <namespace>

If you previously set custom ``pollingInterval``, ``cooldownPeriod``,
``minReplicaCount``, ``maxReplicaCount``, ``advanced``, or ``query`` under
``workers.celery.keda``, copy them into ``scaledobject.yaml`` before
applying.

Status
------

This overlay carries ``status: not-tested``. It builds successfully but has
no functional CI coverage yet. Treat it as a starting point and adapt it
to your environment. Feedback and improvements via pull request are very
welcome under the `helm-chart refurbish umbrella issue
<https://github.com/apache/airflow/issues/64037>`__.
