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

Kerberos Test KDC Overlay
=========================

This overlay stands up a throwaway in-cluster MIT Kerberos KDC, creates
the ``airflow/airflow.<namespace>.svc.cluster.local`` service principal,
and stores its keytab in a Secret named ``<release>-kerberos-keytab``.
It is a standalone addition; no resource produced by the Helm chart is
modified.

It is intended as a proof-of-concept of how a non-Airflow component
(in this case Kerberos infrastructure) can be expressed as a Kustomize
overlay alongside the chart, rather than baked into the chart itself.
The keytab Secret it produces is consumable as-is by the chart's
existing kerberos sidecar (``kerberos.enabled=true``,
``kerberos.keytab=/etc/airflow.keytab``,
``extraSecrets.<release>-kerberos-keytab: {}``).

.. warning::

    The KDC pod uses a fixed admin password and stores its database in
    an ``emptyDir``. Do not connect production workloads to it. Treat it
    as a test fixture only.

Prerequisites
-------------

* The Airflow chart installed in the same namespace (any executor).
* ``kubectl`` access sufficient to apply Deployments, Services,
  ConfigMaps, Secrets, ServiceAccounts/Roles/RoleBindings, and Jobs in
  that namespace.

Usage
-----

Reference this overlay from your own kustomization and substitute the
release name and namespace. A minimal example:

.. code-block:: yaml

    # my-overlay/kustomization.yaml
    apiVersion: kustomize.config.k8s.io/v1beta1
    kind: Kustomization
    namespace: airflow

    resources:
      - github.com/apache/airflow/chart/kustomize-overlays/kerberos?ref=helm-chart/1.22.0

Apply with:

.. code-block:: bash

    kubectl apply -k my-overlay/

For a quick test, you can also just substitute the placeholders inline:

.. code-block:: bash

    kubectl kustomize chart/kustomize-overlays/kerberos | \
      sed -e 's/RELEASE-NAME/airflow/g' -e 's/NAMESPACE/airflow/g' | \
      kubectl apply -n airflow -f -

This is exactly what ``breeze k8s smoke-test-overlay kerberos`` does
during the local and CI smoke test.

Wiring the keytab into the chart's sidecar
------------------------------------------

The chart's kerberos sidecar (``workers.celery.kerberosInitContainer``,
``workers.celery.kerberosSidecar``, ``workers.kubernetes.kerberosInitContainer``,
``workers.kubernetes.kerberosSidecar``) always mounts a keytab Secret whose
name is fixed by the chart to ``<release>-kerberos-keytab`` (rendered by the
``kerberos_keytab_secret`` helper from the release fullname). There is no
chart value that points the sidecar at a differently named Secret.

This overlay produces a Secret with exactly that name, so nothing in
``values.yaml`` needs to reference it. Enable kerberos and leave
``keytabBase64Content`` unset, so the chart does not render its own
(competing) keytab Secret and instead mounts the one this overlay creates:

.. code-block:: yaml

    # values.yaml fragment
    kerberos:
      enabled: true
      ccacheMountPath: /var/kerberos-ccache
      keytabPath: /etc/airflow.keytab
      principal: airflow/airflow.airflow.svc.cluster.local@EXAMPLE.COM

    workers:
      celery:
        kerberosSidecar:
          enabled: true

Migration guide from the chart
------------------------------

What the chart's kerberos support
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When ``kerberos.enabled=true`` and ``kerberos.keytabBase64Content`` is
provided, the chart renders a ``Secret`` carrying the user-supplied
keytab and a ``ConfigMap`` with the user-supplied ``krb5.conf``. The
user is expected to bring their own KDC.

What this overlay provides
^^^^^^^^^^^^^^^^^^^^^^^^^^

* A working test KDC in the same namespace, so a developer can exercise
  the chart's kerberos sidecar end-to-end without standing up an
  external Kerberos service.
* A bootstrap Job that materialises the keytab Secret automatically,
  so no base64-encoded blob ends up in ``values.yaml`` or a developer's
  shell history.

How to switch
^^^^^^^^^^^^^

1. Install or upgrade the chart with ``kerberos.enabled`` set as you
   want, but **without** ``kerberos.keytabBase64Content``.
2. Apply this overlay against the same namespace.
3. Wait for ``Job/<release>-keytab-bootstrap`` to complete.
4. Confirm the Secret exists and reference it from the chart's sidecar
   config as shown above.

Status
------

This overlay is ``tested``. See ``STATUS.yaml`` for the smoke-test
contract (its ``verify:`` block and ``last-verified`` date); the
behavioural assertion lives in ``test_kerberos.py`` under
``chart/tests/overlay_tests/``.

To run the smoke test locally:

.. code-block:: bash

    breeze k8s deploy-cluster --rebuild-base-image
    breeze k8s deploy-airflow
    breeze k8s smoke-test-overlay kerberos --promote-status

.. note::

   ``--rebuild-base-image`` flag is only needed during the first run
