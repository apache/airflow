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



.. _howto/operator:kubernetespodoperator:

.. contents:: Table of Contents
    :depth: 2

KubernetesPodOperator
=====================

The :class:`~airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperator` allows
you to create and run Pods on a Kubernetes cluster.

.. note::
  If you use a managed Kubernetes consider using a specialize KPO operator as it simplifies the Kubernetes authorization process :

  - :ref:`GKEStartPodOperator <howto/operator:GKEStartPodOperator>` operator for `Google Kubernetes Engine <https://cloud.google.com/kubernetes-engine/>`__,

  - :ref:`EksPodOperator <howto/operator:EksPodOperator>` operator for `AWS Elastic Kubernetes Engine <https://aws.amazon.com/eks/>`__.

.. note::
  The :doc:`Kubernetes executor <apache-airflow:core-concepts/executor/kubernetes>` is **not** required to use this operator.

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

    from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

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
:class:`~airflow.providers.cncf.kubernetes.secret.Secret` class to simplify the process of generating secret volumes/env variables.

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

Also for this action you can use operator in the deferrable mode:

.. exampleinclude:: /../../tests/system/providers/cncf/kubernetes/example_kubernetes_async.py
    :language: python
    :start-after: [START howto_operator_k8s_private_image_async]
    :end-before: [END howto_operator_k8s_private_image_async]

How does XCom work?
^^^^^^^^^^^^^^^^^^^
The :class:`~airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperator` handles
XCom values differently than other operators. In order to pass a XCom value
from your Pod you must specify the ``do_xcom_push`` as ``True``. This will create a sidecar container that runs
alongside the Pod. The Pod must write the XCom value into this location at the ``/airflow/xcom/return.json`` path.

.. note::
  An invalid json content will fail, example ``echo 'hello' > /airflow/xcom/return.json`` fail and  ``echo '\"hello\"' > /airflow/xcom/return.json`` work


See the following example on how this occurs:

.. exampleinclude:: /../../tests/system/providers/cncf/kubernetes/example_kubernetes.py
    :language: python
    :start-after: [START howto_operator_k8s_write_xcom]
    :end-before: [END howto_operator_k8s_write_xcom]
.. note::
  XCOMs will be pushed only for tasks marked as ``State.SUCCESS``.

Also for this action you can use operator in the deferrable mode:

.. exampleinclude:: /../../tests/system/providers/cncf/kubernetes/example_kubernetes_async.py
    :language: python
    :start-after: [START howto_operator_k8s_write_xcom_async]
    :end-before: [END howto_operator_k8s_write_xcom_async]

Include error message in email alert
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Any content written to ``/dev/termination-log`` will be retrieved by Kubernetes and
included in the exception message if the task fails.

.. code-block:: python

    k = KubernetesPodOperator(
        task_id="test_error_message",
        image="alpine",
        cmds=["/bin/sh"],
        arguments=["-c", "echo hello world; echo Custom error > /dev/termination-log; exit 1;"],
        name="test-error-message",
        email="airflow@example.com",
        email_on_failure=True,
    )


Read more on termination-log `here <https://kubernetes.io/docs/tasks/debug/debug-application/determine-reason-pod-failure/>`__.

Reference
^^^^^^^^^
For further information, look at:

* `Kubernetes Documentation <https://kubernetes.io/docs/home/>`__
* `Pull an Image from a Private Registry <https://kubernetes.io/docs/tasks/configure-pod-container/pull-image-private-registry/>`__

SparkKubernetesOperator
==========================
The :class:`~airflow.providers.cncf.kubernetes.operators.spark_kubernetes.SparkKubernetesOperator` allows
you to create and run spark job on a Kubernetes cluster. It is based on [ spark-on-k8s-operator ](https://github.com/GoogleCloudPlatform/spark-on-k8s-operator)project.

This operator simplify the interface and accept different parameters to configure and run spark application on Kubernetes.
Similar to the KubernetesOperator, we have added the logic to wait for a job after submission,
manage error handling, retrieve logs from the driver pod and the ability to delete a spark job.
It also supports out-of-the-box Kubernetes functionalities such as handling of volumes, config maps, secrets, etc.


How does this operator work?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^


Usage examples
^^^^^^^^^^^^^^
To create sparkKubenetesOpeartor task you need to have a minimum template to pass to the operator and
that includes both Spark configuration as well as kubentents related resource configuration.
It accepts both yaml and json. here is a sample template which you can use:

spark_job_template.yaml

.. dropdown::

    .. code-block:: yaml

        spark:
          apiVersion: sparkoperator.k8s.io/v1beta2
          version: v1beta2
          kind: SparkApplication
          apiGroup: sparkoperator.k8s.io
          metadata:
            namespace: ds
          spec:
            type: Python
            pythonVersion: "3"
            mode: cluster
            sparkVersion: 3.0.0
            successfulRunHistoryLimit: 1
            restartPolicy:
              type: Never
            imagePullPolicy: Always
            hadoopConf: {}
            imagePullSecrets: []
            dynamicAllocation:
              enabled: false
              initialExecutors: 1
              minExecutors: 1
              maxExecutors: 1
            labels: {}
            driver:
              serviceAccount: default
              container_resources:
                gpu:
                  name: null
                  quantity: 0
                cpu:
                  request: null
                  limit: null
                memory:
                  request: null
                  limit: null
            executor:
              instances: 1
              container_resources:
                gpu:
                  name: null
                  quantity: 0
                cpu:
                  request: null
                  limit: null
                memory:
                  request: null
                  limit: null
        kubernetes:
          # example:
          # env_vars:
          # - name: TEST_NAME
          #   value: TEST_VALUE
          env_vars: []

          # example:
          # env_from:
          # - name: test
          #   valueFrom:
          #     secretKeyRef:
          #       name: mongo-secret
          #       key: mongo-password
          env_from: []

          # example:
          # node_selector:
          #   karpenter.sh/provisioner-name: spark
          node_selector: {}

          # example: https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/
          # affinity:
          #   nodeAffinity:
          #     requiredDuringSchedulingIgnoredDuringExecution:
          #       nodeSelectorTerms:
          #       - matchExpressions:
          #         - key: beta.kubernetes.io/instance-type
          #           operator: In
          #           values:
          #           - r5.xlarge
          affinity:
            nodeAffinity: {}
            podAffinity: {}
            podAntiAffinity: {}

          # example: https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/
          # type: list
          # tolerations:
          # - key: "key1"
          #   operator: "Equal"
          #   value: "value1"
          #   effect: "NoSchedule"
          tolerations: []

          # example:
          # config_map_mounts:
          #   snowflake-default: /mnt/tmp
          config_map_mounts: {}

          # example:
          # volume_mounts:
          # - name: config
          #   mountPath: /airflow
          volume_mounts: []

          # https://kubernetes.io/docs/concepts/storage/volumes/
          # example:
          # volumes:
          # - name: config
          #   persistentVolumeClaim:
          #     claimName: airflow
          volumes: []

          # read config map into an env variable
          # example:
          # from_env_config_map:
          # - configmap_1
          # - configmap_2
          from_env_config_map: []

          # load secret into an env variable
          # example:
          # from_env_secret:
          # - secret_1
          # - secret_2
          from_env_secret: []

          in_cluster: true
          conn_id: kubernetes_default
          kube_config_file: null
          cluster_context: null

.. important::

  * There are two high level category in that template file: ``spark`` and ``kubernetes``

    * spark: This section contains the task's Spark configuration. The fields here align directly with the Spark API template.

    * kubernetes: This section contains the task's kubernetes resource configuration. The fields here align directly with the kubernetes API Documentation. We have included an example for each resource in the above template.

  * The base image that should be used is ``gcr.io/spark-operator/spark-py:v3.1.1``

  * Make sure you copy the spark code into the image or mount the code using persistentVolume or use the code stored on S3

Next, create the task using the following:

.. code-block:: python

    SparkKubernetesOperator(
        task_id="spark_task",
        image="gcr.io/spark-operator/spark-py:v3.1.1",  # OR custom image using that
        code_path="local://path/to/spark/code.py",
        application_file="spark_job_template.json",  # OR spark_job_template.json
        dag=dag,
    )

Note: Alternatively application_file can also be a json file. see below example

spark_job_template.json

.. dropdown::

  .. code-block:: yaml

    {
      "spark": {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "version": "v1beta2",
        "kind": "SparkApplication",
        "apiGroup": "sparkoperator.k8s.io",
        "metadata": {
          "namespace": "ds"
        },
        "spec": {
          "type": "Python",
          "pythonVersion": "3",
          "mode": "cluster",
          "sparkVersion": "3.0.0",
          "successfulRunHistoryLimit": 1,
          "restartPolicy": {
            "type": "Never"
          },
          "imagePullPolicy": "Always",
          "hadoopConf": {},
          "imagePullSecrets": [],
          "dynamicAllocation": {
            "enabled": false,
            "initialExecutors": 1,
            "minExecutors": 1,
            "maxExecutors": 1
          },
          "labels": {},
          "driver": {
            "serviceAccount": "default",
            "container_resources": {
              "gpu": {
                "name": null,
                "quantity": 0
              },
              "cpu": {
                "request": null,
                "limit": null
              },
              "memory": {
                "request": null,
                "limit": null
              }
            }
          },
          "executor": {
            "instances": 1,
            "container_resources": {
              "gpu": {
                "name": null,
                "quantity": 0
              },
              "cpu": {
                "request": null,
                "limit": null
              },
              "memory": {
                "request": null,
                "limit": null
              }
            }
          }
        }
      },
      "kubernetes": {
        "env_vars": [],
        "env_from": [],
        "node_selector": {},
        "affinity": {
          "nodeAffinity": {},
          "podAffinity": {},
          "podAntiAffinity": {}
        },
        "tolerations": [],
        "config_map_mounts": {},
        "volume_mounts": [
          {
            "name": "config",
            "mountPath": "/airflow"
          }
        ],
        "volumes": [
          {
            "name": "config",
            "persistentVolumeClaim": {
              "claimName": "hsaljoog-airflow"
            }
          }
        ],
        "from_env_config_map": [],
        "from_env_secret": [],
        "in_cluster": true,
        "conn_id": "kubernetes_default",
        "kube_config_file": null,
        "cluster_context": null
      }
    }



Another Alternative to yaml or json file is to pass ``template_spec`` field directly instead of application_file,
 if you dont want to use a file.
