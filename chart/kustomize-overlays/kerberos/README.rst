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

Resources produced
------------------

* ``ConfigMap/<release>-krb5-conf`` - ``krb5.conf`` with the test realm
  ``EXAMPLE.COM`` and the in-cluster KDC service as ``kdc``/``admin_server``.
* ``Deployment/<release>-kerberos-kdc`` - single-replica MIT Kerberos
  KDC + kadmind, image ``gcavalcante8808/krb5-server:latest``.
* ``Service/<release>-kerberos-kdc`` - exposes 88 TCP+UDP and 749 TCP.
* ``ServiceAccount`` + ``Role`` + ``RoleBinding`` named
  ``<release>-kerberos-bootstrap`` - minimum permissions for the
  bootstrap Job (pod exec + secret create/update in the same namespace).
* ``Job/<release>-keytab-bootstrap`` - waits for the KDC to be Ready,
  runs ``kadmin.local`` against it to create the principal and write
  the keytab, then stores the keytab in:
* ``Secret/<release>-kerberos-keytab`` (created by the Job) - holds
  ``airflow.keytab`` under that key.

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

    kustomize build chart/kustomize-overlays/kerberos | \
      sed -e 's/RELEASE-NAME/airflow/g' -e 's/NAMESPACE/airflow/g' | \
      kubectl apply -n airflow -f -

This is exactly what ``breeze k8s smoke-test-overlay kerberos`` does
during the local and CI smoke test.

Wiring the keytab into the chart's sidecar
------------------------------------------

The chart's kerberos sidecar (``workers.kerberosSidecar``,
``workers.celery.kerberosSidecar``) mounts a Secret named in
``kerberos.keytab``. Point that at the Secret produced by this overlay:

.. code-block:: yaml

    # values.yaml fragment
    kerberos:
      enabled: true
      ccacheMountPath: /var/kerberos-ccache
      keytabPath: /etc/airflow.keytab
      principal: airflow/airflow.airflow.svc.cluster.local@EXAMPLE.COM

    workers:
      kerberosSidecar:
        enabled: true

    extraSecrets:
      airflow-kerberos-keytab: {}   # exists from this overlay

Migration guide from the chart
------------------------------

What the chart currently does
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

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

This overlay is ``tested``: the ``verify:`` block in ``STATUS.yaml``
is the smoke-test contract (KDC Deployment Ready, Service exists,
bootstrap Job Complete, keytab Secret exists), and the
``test_kerberos.py`` module under
``chart/tests/overlay_tests/`` adds the
behavioural assertion: a throwaway client pod ``kinit``\ ing against
the in-cluster KDC and confirming the principal in ``klist`` output.
``last-verified`` in ``STATUS.yaml`` records the most recent green
local run; re-run the smoke test with ``--promote-status`` to refresh
it whenever you re-verify against your cluster.

To run the smoke test locally:

.. code-block:: bash

    breeze k8s setup-env
    breeze k8s create-cluster
    breeze k8s configure-cluster
    breeze k8s build-k8s-image --rebuild-base-image  # first time only
    breeze k8s upload-k8s-image                      # load into kind nodes
    breeze k8s deploy-airflow
    breeze k8s smoke-test-overlay kerberos --promote-status

The ``--promote-status`` flag rewrites this overlay's ``STATUS.yaml``
in place on a green run (chart-version from ``chart/Chart.yaml`` plus
today's date). Without it the smoke test still runs and verifies, it
just leaves ``STATUS.yaml`` untouched.

The ``build-k8s-image`` + ``upload-k8s-image`` pair is required locally
because the chart's default image lives on ghcr.io behind CI auth;
without those steps ``deploy-airflow`` will fail with ImagePullBackOff
(HTTP 403). CI itself runs ``breeze k8s run-complete-tests`` which
chains all of the above.

The smoke test runner takes care of this overlay's own images itself:
``gcavalcante8808/krb5-server`` (KDC pod) and ``alpine/k8s`` (bootstrap
Job) are auto-discovered from the rendered manifest and pre-loaded into
every kind node before apply, so the test does not depend on a live
Docker Hub pull at run time. See ``chart/kustomize-overlays/CONTRIBUTING.rst``
"What ``smoke-test-overlay`` does for every overlay" for the generic
machinery.

See ``CONTRIBUTING.rst`` in this directory's parent for the lifecycle
and the difference between the structural ``build_kustomize_overlays``
prek hook (which runs on every commit) and this functional smoke test.
