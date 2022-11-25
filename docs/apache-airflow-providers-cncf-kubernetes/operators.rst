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

.. note::
  If you use `Google Kubernetes Engine <https://cloud.google.com/kubernetes-engine/>`__, consider
  using the
  :ref:`GKEStartPodOperator <howto/operator:GKEStartPodOperator>` operator as it
  simplifies the Kubernetes authorization process.

.. note::
  The :doc:`Kubernetes executor <apache-airflow:executor/kubernetes>` is **not** required to use this operator.

How does this operator work?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The :class:`~airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperator` uses the
Kubernetes API to launch a pod in a Kubernetes cluster. By supplying an
image URL and a command with optional arguments, the operator uses the Kube Python Client to generate a Kubernetes API
request that dynamically launches those individual pods.
Users can specify a kubeconfig file using the ``config_file`` parameter, otherwise the operator will default
to ``~/.kube/config``.

The :class:`~airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperator` enables task-level
resource configuration and is optimal for custom Python
dependencies that are not available through the public PyPI repository. It also allows users to supply a template
YAML file using the ``pod_template_file`` parameter.
Ultimately, it allows Airflow to act a job orchestrator - no matter the language those jobs are written in.

Debugging KubernetesPodOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can print out the Kubernetes manifest for the pod that would be created at runtime by calling
:meth:`~.KubernetesPodOperator.dry_run` on an instance of the operator.

.. code-block:: python

    from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

    k = KubernetesPodOperator(
        name="hello-dry-run",
        image="debian",
        cmds=["bash", "-cx"],
        arguments=["echo", "10"],
        labels={"foo": "bar"},
        task_id="dry_run_demo",
        do_xcom_push=True,
    )

    k.dry_run()

Argument precedence
^^^^^^^^^^^^^^^^^^^

When building the pod object, there may be overlap between KPO params, pod spec, template and airflow connection.
In general, the order of precedence is KPO argument > full pod spec > pod template file > airflow connection.

For ``namespace``, if namespace is not provided via any of these methods, then we'll first try to
get the current namespace (if the task is already running in kubernetes) and failing that we'll use
the ``default`` namespace.

For pod name, if not provided explicitly, we'll use the task_id. A random suffix is added by default so the pod
name is not generally of great consequence.

How to use cluster ConfigMaps, Secrets, and Volumes with Pod?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To add ConfigMaps, Volumes, and other Kubernetes native objects, we recommend that you import the Kubernetes model API
like this:

.. code-block:: python

  from kubernetes.client import models as k8s

With this API object, you can have access to all Kubernetes API objects in the form of python classes.
Using this method will ensure correctness
and type safety. While we have removed almost all Kubernetes convenience classes, we have kept the
:class:`~airflow.kubernetes.secret.Secret` class to simplify the process of generating secret volumes/env variables.

.. exampleinclude:: /../../tests/system/providers/cncf/kubernetes/example_kubernetes.py
    :language: python
    :start-after: [START howto_operator_k8s_cluster_resources]
    :end-before: [END howto_operator_k8s_cluster_resources]

Difference between ``KubernetesPodOperator`` and Kubernetes object spec
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The :class:`~airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperator` can be considered
a substitute for a Kubernetes object spec definition that is able
to be run in the Airflow scheduler in the DAG context. If using the operator, there is no need to create the
equivalent YAML/JSON object spec for the Pod you would like to run.
The YAML file can still be provided with the ``pod_template_file`` or even the Pod Spec constructed in Python via
the ``full_pod_spec`` parameter which requires a Kubernetes ``V1Pod``.

How to use private images (container registry)?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
By default, the :class:`~airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperator` will
look for images hosted publicly on Dockerhub.
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

.. exampleinclude:: /../../tests/system/providers/cncf/kubernetes/example_kubernetes.py
    :language: python
    :start-after: [START howto_operator_k8s_private_image]
    :end-before: [END howto_operator_k8s_private_image]

How does XCom work?
^^^^^^^^^^^^^^^^^^^
The :class:`~airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperator` handles
XCom values differently than other operators. In order to pass a XCom value
from your Pod you must specify the ``do_xcom_push`` as ``True``. This will create a sidecar container that runs
alongside the Pod. The Pod must write the XCom value into this location at the ``/airflow/xcom/return.json`` path.

See the following example on how this occurs:

.. exampleinclude:: /../../tests/system/providers/cncf/kubernetes/example_kubernetes.py
    :language: python
    :start-after: [START howto_operator_k8s_write_xcom]
    :end-before: [END howto_operator_k8s_write_xcom]
.. note::
  XCOMs will be pushed only for tasks marked as ``State.SUCCESS``.

Reference
^^^^^^^^^
For further information, look at:

* `Kubernetes Documentation <https://kubernetes.io/docs/home/>`__
* `Pull an Image from a Private Registry <https://kubernetes.io/docs/tasks/configure-pod-container/pull-image-private-registry/>`__
