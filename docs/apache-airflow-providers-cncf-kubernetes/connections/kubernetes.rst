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

.. _howto/connection:kubernetes:

Kubernetes cluster Connection
=============================

The Kubernetes cluster Connection type enables connection to a Kubernetes cluster by :class:`~airflow.providers.cncf.kubernetes.operators.spark_kubernetes.SparkKubernetesOperator` tasks and :class:`~airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperator` tasks.


Authenticating to Kubernetes cluster
------------------------------------

There are different ways to connect to Kubernetes using Airflow.

#. Use kube_config that reside in the default location on the machine(~/.kube/config) - just leave all fields empty
#. Use in_cluster config, if Airflow runs inside Kubernetes cluster take the configuration from the cluster - mark:
   In cluster configuration
#. Use kube_config from different location - insert the path into ``Kube config path``
#. Use kube_config in JSON format from connection configuration - paste  kube_config into ``Kube config (JSON format)``


Default Connection IDs
----------------------

The default connection ID is ``kubernetes_default`` .

Configuring the Connection
--------------------------


In cluster configuration
  Use in cluster configuration.

Kube config path
  Use custom path to kube config.

Kube config (JSON format)
  `Kube config <https://kubernetes.io/docs/tasks/access-application-cluster/configure-access-multiple-clusters/>`_
  that used to connect to Kubernetes client.

Namespace
  Default Kubernetes namespace for the connection.

Cluster context
  When using a kube config, can specify which context to use.

Disable verify SSL
  Can optionally disable SSL certificate verification.  By default SSL is verified.

Disable TCP keepalive
  TCP keepalive is a feature (enabled by default) that tries to keep long-running connections
  alive. Set this parameter to True to disable this feature.

Xcom sidecar image
  Define the ``image`` used by the ``PodDefaults.SIDECAR_CONTAINER`` (defaults to ``"alpine"``) to allow private
  repositories, as well as custom image overrides.

Example storing connection in env var using URI format:

.. code-block:: bash

    AIRFLOW_CONN_KUBERNETES_DEFAULT='kubernetes://?in_cluster=True&kube_config_path=~%2F.kube%2Fconfig&kube_config=kubeconfig+json&namespace=namespace'

And using JSON format:

.. code-block:: bash

    AIRFLOW_CONN_KUBERNETES_DEFAULT='{"conn_type": "kubernetes", "extra": {"in_cluster": true, "kube_config_path": "~/.kube/config", "namespace": "my-namespace"}}'
