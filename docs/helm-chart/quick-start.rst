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

Quick start with kind
=====================

This article will show you how to install Airflow using Helm Chart on `Kind <https://kind.sigs.k8s.io/>`__

Install kind, and create a cluster
----------------------------------

We recommend testing with Kubernetes 1.20+, example:

.. code-block:: bash

   kind create cluster --image kindest/node:v1.21.1

Confirm it's up:

.. code-block:: bash

   kubectl cluster-info --context kind-kind

Add Airflow Helm Stable Repo
----------------------------

.. code-block:: bash

   helm repo add apache-airflow https://airflow.apache.org
   helm repo update

Create namespace
----------------

.. code-block:: bash

  export NAMESPACE=example-namespace
  kubectl create namespace $NAMESPACE

Install the chart
-----------------

.. code-block:: bash

  export RELEASE_NAME=example-release
  helm install $RELEASE_NAME apache-airflow/airflow --namespace $NAMESPACE

Use the following code to install the chart with Example DAGs:

.. code-block:: bash

  export NAMESPACE=example-namespace
  helm install $RELEASE_NAME apache-airflow/airflow \
    --namespace $NAMESPACE \
    --set-string "env[0].name=AIRFLOW__CORE__LOAD_EXAMPLES" \
    --set-string "env[0].value=True"

It may take a few minutes. Confirm the pods are up:

.. code-block:: bash

   kubectl get pods --namespace $NAMESPACE
   helm list --namespace $NAMESPACE

Run the following command
to port-forward the Airflow UI to http://localhost:8080/ to confirm
Airflow is working.

.. code-block:: bash

   kubectl port-forward svc/$RELEASE_NAME-webserver 8080:8080 --namespace $NAMESPACE

Extending Airflow Image
-----------------------

The Apache Airflow community, releases Docker Images which are ``reference images`` for Apache Airflow.
However when you try it out you want to add your own DAGs, custom dependencies,
packages or even custom providers.

The best way to achieve it, is to build your own, custom image.

Adding DAGs to your image
.........................

1. Create a project

    .. code-block:: bash

        mkdir my-airflow-project && cd my-airflow-project
        mkdir dags  # put dags here
        cat <<EOM > Dockerfile
        FROM apache/airflow
        COPY . .
        EOM


2. Then build the image:

    .. code-block:: bash

        docker build --pull --tag my-dags:0.0.1 .


3. Load the image into kind:

    .. code-block:: bash

      kind load docker-image my-dags:0.0.1

4. Upgrade Helm deployment:

    .. code-block:: bash

      helm upgrade $RELEASE_NAME apache-airflow/airflow --namespace $NAMESPACE \
          --set images.airflow.repository=my-dags \
          --set images.airflow.tag=0.0.1

Adding ``apt`` packages to your image
.....................................

Example below adds ``vim`` apt package.

1. Create a project

    .. code-block:: bash

        mkdir my-airflow-project && cd my-airflow-project
        cat <<EOM > Dockerfile
        FROM apache/airflow
        USER root
        RUN apt-get update \
          && apt-get install -y --no-install-recommends \
                 vim \
          && apt-get autoremove -yqq --purge \
          && apt-get clean \
          && rm -rf /var/lib/apt/lists/*
        USER airflow
        EOM


2. Then build the image:

    .. code-block:: bash

        docker build --pull --tag my-image:0.0.1 .


3. Load the image into kind:

    .. code-block:: bash

      kind load docker-image my-image:0.0.1

4. Upgrade Helm deployment:

    .. code-block:: bash

      helm upgrade $RELEASE_NAME apache-airflow/airflow --namespace $NAMESPACE \
          --set images.airflow.repository=my-image \
          --set images.airflow.tag=0.0.1

Adding ``PyPI`` packages to your image
......................................

Example below adds ``lxml`` PyPI package.

1. Create a project

    .. code-block:: bash

        mkdir my-airflow-project && cd my-airflow-project
        cat <<EOM > Dockerfile
        FROM apache/airflow
        RUN pip install --no-cache-dir lxml
        EOM


2. Then build the image:

    .. code-block:: bash

        docker build --pull --tag my-image:0.0.1 .


3. Load the image into kind:

    .. code-block:: bash

      kind load docker-image my-image:0.0.1

4. Upgrade Helm deployment:

    .. code-block:: bash

      helm upgrade $RELEASE_NAME apache-airflow/airflow --namespace $NAMESPACE \
          --set images.airflow.repository=my-image \
          --set images.airflow.tag=0.0.1

Further extending and customizing the image
...........................................

See `Building the image <https://airflow.apache.org/docs/docker-stack/build.html>`_ for more
details on how you can extend and customize the Airflow image.
