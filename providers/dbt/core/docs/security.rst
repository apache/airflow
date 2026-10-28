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

Security
========

Artifact upload credentials are injected into the pod
-----------------------------------------------------

When you set ``artifact_dest`` together with ``artifact_conn_id``,
:class:`~airflow.providers.dbt.core.operators.dbt.DbtKubernetesRunOperator`
resolves the object-storage credentials **on the worker** and passes them into
the dbt pod as environment variables:

* ``s3://`` — ``AWS_ACCESS_KEY_ID``, ``AWS_SECRET_ACCESS_KEY`` and, for
  temporary credentials, ``AWS_SESSION_TOKEN``.
* ``gs://`` — ``GOOGLE_APPLICATION_CREDENTIALS_JSON`` (the service-account key
  JSON), which the in-pod script writes to ``/tmp/gcp-key.json``.

This is a deliberate tradeoff. Injecting credentials as env vars is what lets
the pod perform per-file, browsable uploads (``aws s3 sync`` / ``gsutil -m
rsync``) of ``target/`` straight to your bucket. The cost is that the
credentials are present in the pod's environment: anyone who can read the pod
spec or ``exec`` into the pod (for example via ``kubectl``) can read them. Scope
the artifact connection to least-privilege, write-only-to-the-prefix
credentials, and prefer short-lived / temporary credentials where possible.

Hardening
~~~~~~~~~

Because the operator is a
:class:`~airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator`,
you can harden credential handling with the pod operator's native mechanisms
instead of ``artifact_conn_id``:

* Use ``secrets`` to mount a Kubernetes ``Secret`` as env vars or files, so the
  credentials come from the cluster's secret store rather than from a resolved
  Airflow connection.
* Use ``env_from`` to pull credentials from a ``Secret`` or ``ConfigMap``
  already present in the namespace.
* Better still, avoid long-lived keys entirely: attach an IAM identity to the
  pod's service account (IRSA on EKS, Workload Identity on GKE) and leave
  ``artifact_conn_id`` unset so that no static credentials are injected.

If you supply credentials through any of these mechanisms, omit
``artifact_conn_id`` so the operator does not also inject its own.

The git token appears in the clone URL transiently
---------------------------------------------------

When ``git_conn_id`` is set, its password (the git token) is injected as
``GIT_TOKEN`` and embedded in the HTTPS clone URL
(``https://<token>@<host>/<path>``) for the duration of the clone. The in-pod
script ``unset``\ s ``GIT_TOKEN`` immediately after cloning, and the clone runs
with ``--quiet`` so the URL is not echoed to the logs; however, the token is
briefly present in the arguments of the ``git clone`` process inside the pod.
Use a least-privilege, read-only deploy token, and prefer the ``secrets`` /
``env_from`` approaches above if your threat model requires that the token never
appear on a command line.

Reporting security issues
-------------------------

If you believe you have found a security vulnerability in this provider, please
follow your organization's responsible-disclosure process rather than filing a
public issue.
