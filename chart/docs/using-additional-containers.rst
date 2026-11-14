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

Using additional containers
===========================

Sidecar Containers
------------------

If you want to deploy your own sidecar container, you can add it through the ``extraContainers`` parameter.
You can define different containers for the scheduler, api-server, Kubernetes/Celery workers, triggerer, dag processor, flower, PgBouncer, create user job and migrate database job pods.

For example, sidecars that sync Dags from object storage:

.. code-block:: yaml
   :caption: values.yaml

   scheduler:
     extraContainers:
       - name: s3-sync
         image: my-company/s3-sync:latest
         imagePullPolicy: Always

   workers:
     kubernetes:
       extraContainers:
         - name: s3-sync
           image: my-company/s3-sync:latest
           imagePullPolicy: Always

.. note::

   If you use ``workers.kubernetes.extraContainers`` (dedicated for ``KubernetesExecutor``), you are responsible for signaling
   sidecars to exit when the main container finishes so Airflow can continue the worker shutdown process.


Init Containers
---------------

You can also deploy extra init containers through the ``extraInitContainers`` parameter.
You can define different containers for the scheduler, api-server, Celery/Kubernetes workers, triggerer, dag processor, PgBouncer, create user job and migrate database job pods.

For example, ``pgbouncer.extraInitContainers`` can run a user-provided credential refresher for Azure Database
for PostgreSQL authentication with short-lived Microsoft Entra ID tokens. The credentials must be obtained before PgBouncer
starts and refreshed while it runs. On clusters supporting `native Kubernetes sidecars
<https://kubernetes.io/docs/concepts/workloads/pods/sidecar-containers/>`_, use ``restartPolicy: Always``
and a ``startupProbe`` that succeeds only after the initial credentials are ready. This lets the refresher
continue running while PgBouncer starts after successful initialization.

For example, an init container that just says hello:

.. code-block:: yaml
   :caption: values.yaml

   scheduler:
     extraInitContainers:
       - name: hello
         image: debian
         args:
           - echo
           - hello
