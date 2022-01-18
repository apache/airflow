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

When you create new or modify existing DAG files, it is necessary to deploy them into the environment. This section will describe some basic techniques you can use.

Bake DAGs in Docker image
-------------------------

The recommended way to update your DAGs with this chart is to build a new docker image with the latest DAG code:

.. code-block:: bash

    docker build --pull --tag "my-company/airflow:8a0da78" . -f - <<EOF
    FROM apache/airflow

    COPY ./dags/ \${AIRFLOW_HOME}/dags/

    EOF

.. note::

   In airflow images prior to version 2.0.2, there was a bug that required you to use
   a bit longer Dockerfile, to make sure the image remains OpenShift-compatible (i.e dag
   has root group similarly as other files). In 2.0.2 this has been fixed.

.. code-block:: bash

    docker build --pull --tag "my-company/airflow:8a0da78" . -f - <<EOF
    FROM apache/airflow:2.0.2

    USER root

    COPY --chown=airflow:root ./dags/ \${AIRFLOW_HOME}/dags/

    USER airflow

    EOF


Then publish it in the accessible registry:

.. code-block:: bash

    docker push my-company/airflow:8a0da78

Finally, update the Airflow pods with that image:

.. code-block:: bash

    helm upgrade --install airflow apache-airflow/airflow \
      --set images.airflow.repository=my-company/airflow \
      --set images.airflow.tag=8a0da78

If you are deploying an image with a constant tag, you need to make sure that the image is pulled every time.

.. code-block:: bash

    helm upgrade --install airflow apache-airflow/airflow \
      --set images.airflow.repository=my-company/airflow \
      --set images.airflow.tag=8a0da78 \
      --set images.airflow.pullPolicy=Always

Mounting DAGs using Git-Sync sidecar with Persistence enabled
-------------------------------------------------------------

This option will use a Persistent Volume Claim with an access mode of ``ReadWriteMany``.
The scheduler pod will sync DAGs from a git repository onto the PVC every configured number of
seconds. The other pods will read the synced DAGs. Not all volume plugins have support for
``ReadWriteMany`` access mode.
Refer `Persistent Volume Access Modes <https://kubernetes.io/docs/concepts/storage/persistent-volumes/#access-modes>`__
for details.

.. code-block:: bash

    helm upgrade --install airflow apache-airflow/airflow \
      --set dags.persistence.enabled=true \
      --set dags.gitSync.enabled=true
      # you can also override the other persistence or gitSync values
      # by setting the  dags.persistence.* and dags.gitSync.* values
      # Please refer to values.yaml for details

.. code-block:: bash

    helm upgrade --install airflow apache-airflow/airflow \
      --set dags.persistence.enabled=true \
      --set dags.gitSync.enabled=true \
      # you can also override the other persistence or gitSync values
      # by setting the  dags.persistence.* and dags.gitSync.* values
      # Please refer to values.yaml for details

Mounting DAGs using Git-Sync sidecar without Persistence
--------------------------------------------------------

This option will use an always running Git-Sync sidecar on every scheduler, webserver (if ``airflowVersion < 2.0.0``)
and worker pods.
The Git-Sync sidecar containers will sync DAGs from a git repository every configured number of
seconds. If you are using the ``KubernetesExecutor``, Git-sync will run as an init container on your worker pods.

.. code-block:: bash

    helm upgrade --install airflow apache-airflow/airflow \
      --set dags.persistence.enabled=false \
      --set dags.gitSync.enabled=true
      # you can also override the other gitSync values
      # by setting the  dags.gitSync.* values
      # Refer values.yaml for details

When using ``apache-airflow>=2.0.0``, :ref:`DAG Serialization <apache-airflow:dag-serialization>` is enabled by default,
hence Webserver does not need access to DAG files, so ``git-sync`` sidecar is not run on Webserver.

Mounting DAGs from an externally populated PVC
----------------------------------------------

In this approach, Airflow will read the DAGs from a PVC which has ``ReadOnlyMany`` or ``ReadWriteMany`` access mode. You will have to ensure that the PVC is populated/updated with the required DAGs(this won't be handled by the chart). You can pass in the name of the  volume claim to the chart

.. code-block:: bash

    helm upgrade --install airflow apache-airflow/airflow \
      --set dags.persistence.enabled=true \
      --set dags.persistence.existingClaim=my-volume-claim
      --set dags.gitSync.enabled=false

Mounting DAGs from a private Github repo using Git-Sync sidecar
---------------------------------------------------------------
Create a private repo on Github if you have not created one already.

Then create your ssh keys:

.. code-block:: bash

    ssh-keygen -t rsa -b 4096 -C "your_email@example.com"

Add the public key to your private repo (under ``Settings > Deploy keys``).

You have to convert the private ssh key to a base64 string. You can convert the private ssh key file like so:

.. code-block:: bash

    base64 <my-private-ssh-key> -w 0 > temp.txt

Then copy the string from the ``temp.txt`` file. You'll add it to your ``override-values.yaml`` next.

In this example, you will create a yaml file called ``override-values.yaml`` to override values in the
``values.yaml`` file, instead of using ``--set``:

.. code-block:: yaml

    dags:
      gitSync:
        enabled: true
        repo: ssh://git@github.com/<username>/<private-repo-name>.git
        branch: <branch-name>
        subPath: ""
        sshKeySecret: airflow-ssh-secret
    extraSecrets:
      airflow-ssh-secret:
        data: |
          gitSshKey: '<base64-converted-ssh-private-key>'

Don't forget to copy in your private key base64 string.

Finally, from the context of your Airflow Helm chart directory, you can install Airflow:

.. code-block:: bash

    helm upgrade --install airflow apache-airflow/airflow -f override-values.yaml

If you have done everything correctly, Git-Sync will pick up the changes you make to the DAGs
in your private Github repo.

You should take this a step further and set ``dags.gitSync.knownHosts`` so you are not susceptible to man-in-the-middle
attacks. This process is documented in the :ref:`production guide <production-guide:knownhosts>`.
