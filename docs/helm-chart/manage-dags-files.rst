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

Manage DAGs files
=================

When you create new or modify existing DAG files, it is necessary to implement them into the environment. This section will describe some basic techniques you can use.

Bake DAGs in Docker image
-------------------------

The recommended way to update your DAGs with this chart is to build a new docker image with the latest DAG code (``docker build -t my-company/airflow:8a0da78 . ``), push it to an accessible registry ```docker push my-company/airflow:8a0da78``), then update the Airflow pods with that image:

.. code-block:: bash

    helm upgrade airflow . \
      --set images.airflow.repository=my-company/airflow \
      --set images.airflow.tag=8a0da78

For local development purpose you can also build the image locally and use it via deployment method described by Breeze.

Mounting DAGs using Git-Sync sidecar with Persistence enabled
-------------------------------------------------------------

This option will use a Persistent Volume Claim with an access mode of ``ReadWriteMany``. The scheduler pod will sync DAGs from a git repository onto the PVC every configured number of seconds. The other pods will read the synced DAGs. Not all volume  plugins have support for ``ReadWriteMany`` access mode. Refer `Persistent Volume Access Modes <https://kubernetes.io/docs/concepts/storage/persistent-volumes/#access-modes>`__ for details

.. code-block:: bash

    helm upgrade airflow . \
      --set dags.persistence.enabled=true \
      --set dags.gitSync.enabled=true
      # you can also override the other persistence or gitSync values
      # by setting the  dags.persistence.* and dags.gitSync.* values
      # Please refer to values.yaml for details

Mounting DAGs using Git-Sync sidecar without Persistence
--------------------------------------------------------

This option will use an always running Git-Sync side car on every scheduler, webserver and worker pods. The Git-Sync side car containers will sync DAGs from a git repository every configured number of seconds. If you are using the KubernetesExecutor, Git-sync will run as an init container on your worker pods.

.. code-block:: bash

    helm upgrade airflow . \
      --set dags.persistence.enabled=false \
      --set dags.gitSync.enabled=true
      # you can also override the other gitSync values
      # by setting the  dags.gitSync.* values
      # Refer values.yaml for details

Mounting DAGs from an externally populated PVC
----------------------------------------------

In this approach, Airflow will read the DAGs from a PVC which has ``ReadOnlyMany`` or ``ReadWriteMany`` access mode. You will have to ensure that the PVC is populated/updated with the required DAGs(this won't be handled by the chart). You can pass in the name of the  volume claim to the chart

.. code-block:: bash

    helm upgrade airflow . \
      --set dags.persistence.enabled=true \
      --set dags.persistence.existingClaim=my-volume-claim
      --set dags.gitSync.enabled=false
