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



.. _howto/operator:KubernetesPodOperator:

KubernetesPodOperator
=====================

The :class:`~airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperator` allows
you to create and run Pods on a Kubernetes cluster.

.. contents::
  :depth: 1
  :local:

.. note::
  If you use `Google Kubernetes Engine <https://cloud.google.com/kubernetes-engine/>`__, consider
  using the
  :ref:`GKEStartPodOperator <howto/operator:GKEStartPodOperator>` operator as it
  simplifies the Kubernetes authorization process.

.. note::
  The :doc:`Kubernetes executor <../../../executor/kubernetes>` is **not** required to use this operator.

How does this operator work?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The ``KubernetesPodOperator`` uses the Kubernetes API to launch a pod in a Kubernetes cluster. By supplying an
image URL and a command with optional arguments, the operator uses the Kube Python Client to generate a Kubernetes API
request that dynamically launches those individual pods.
Users can specify a kubeconfig file using the ``config_file`` parameter, otherwise the operator will default
to ``~/.kube/config``.

The ``KubernetesPodOperator`` enables task-level resource configuration and is optimal for custom Python
dependencies that are not available through the public PyPI repository. It also allows users to supply a template
YAML file using the ``pod_template_file`` parameter.
Ultimately, it allows Airflow to act a job orchestrator - no matter the language those jobs are written in.

How to use cluster ConfigMaps, Secrets, and Volumes with Pod?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Kubernetes cluster resources such as ConfigMaps, Secrets, and Volumes can be used with a Pod to be launched.
Utilize the Airflow Kubernetes model classes such as:
:class:`~airflow.kubernetes.secret.Secret`
or
:class:`~airflow.kubernetes.volume.Volume`
or
:class:`~airflow.kubernetes.volume_mount.VolumeMount`
to do this (as well as standard Python dictionaries). These can they be specified in the appropriate parameters
when declaring the Pod task.

.. exampleinclude:: ../../../airflow/providers/cncf/kubernetes/example_dags/example_kubernetes.py
    :language: python
    :start-after: [START howto_operator_k8s_cluster_resources]
    :end-before: [END howto_operator_k8s_cluster_resources]

Difference between ``KubernetesPodOperator`` and Kubernetes object spec
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The ``KubernetesPodOperator`` can be considered a substitute for a Kubernetes object spec definition that is able
to be run in the Airflow scheduler in the DAG context. If using the operator, there is no need to create the
equivalent YAML/JSON object spec for the Pod you would like to run.
The YAML file can still be provided with the ``pod_template_file`` or even the Pod Spec constructed in Python via
the ``full_pod_spec`` parameter which requires a Kubernetes ``V1Pod``.

How to use private images (container registry)?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
By default, the ``KubernetesPodOperator`` will look for images hosted publicly on Dockerhub.
To pull images from a private registry (such as ECR, GCR, Quay, or others), you must create a
Kubernetes Secret that represents the credentials for accessing images from the private registry that is ultimately
specified in the ``image_pull_secrets`` parameter.

Create the Secret using ``kubectl``:

.. code-block:: none

    kubectl create secret docker-registry testquay \
        --docker-server=quay.io \
        --docker-username=<Profile name> \
        --docker-password=<password>

Then use it in your pod like so:

.. exampleinclude:: ../../../airflow/providers/cncf/kubernetes/example_dags/example_kubernetes.py
    :language: python
    :start-after: [START howto_operator_k8s_private_image]
    :end-before: [END howto_operator_k8s_private_image]

How does XCom work?
^^^^^^^^^^^^^^^^^^^
The ``KubernetesPodOperator`` handles XCom values differently than other operators. In order to pass a XCom value
from your Pod you must specify the ``do_xcom_push`` as ``True``. This will create a sidecar container that runs
alongside the Pod. The Pod must write the XCom value into this location at the ``/airflow/xcom/return.json`` path.

See the following example on how this occurs:

.. exampleinclude:: ../../../airflow/providers/cncf/kubernetes/example_dags/example_kubernetes.py
    :language: python
    :start-after: [START howto_operator_k8s_write_xcom]
    :end-before: [END howto_operator_k8s_write_xcom]

Reference
^^^^^^^^^
For further information, look at:

* `Kubernetes Documentation <https://kubernetes.io/docs/home/>`__
* `Pull and Image from a Private Registry <https://kubernetes.io/docs/tasks/configure-pod-container/pull-image-private-registry/>`__
