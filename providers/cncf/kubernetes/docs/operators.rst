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


.. contents:: Table of Contents
    :depth: 2

.. _howto/operator:kubernetespodoperator:

KubernetesPodOperator
=====================

The :class:`~airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator` allows
you to create and run Pods on a Kubernetes cluster.

.. note::
  If you use a managed Kubernetes consider using a specialize KPO operator as it simplifies the Kubernetes authorization process :

  - :ref:`GKEStartPodOperator <howto/operator:GKEStartPodOperator>` operator for `Google Kubernetes Engine <https://cloud.google.com/kubernetes-engine/>`__,

  - :ref:`EksPodOperator <howto/operator:EksPodOperator>` operator for `AWS Elastic Kubernetes Engine <https://aws.amazon.com/eks/>`__.

.. note::
  The :doc:`Kubernetes executor <kubernetes_executor>` is **not** required to use this operator.

How does this operator work?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The :class:`~airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator` uses the
Kubernetes API to launch a pod in a Kubernetes cluster. By supplying an
image URL and a command with optional arguments, the operator uses the Kube Python Client to generate a Kubernetes API
request that dynamically launches those individual pods.
Users can specify a kubeconfig file using the ``config_file`` parameter, otherwise the operator will default
to ``~/.kube/config``.

The :class:`~airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator` enables task-level
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

When KPO defines the pod object, there may be overlap between the :class:`~airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator` arguments.
In general, the order of precedence is KPO field-specific arguments (e.g., ``secrets``, ``cmds``, ``affinity``), more general templates ``full_pod_spec``, ``pod_template_file``, ``pod_template_dict``,  and followed by ``V1Pod``, by default.

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

.. exampleinclude:: /../tests/system/cncf/kubernetes/example_kubernetes.py
    :language: python
    :start-after: [START howto_operator_k8s_cluster_resources]
    :end-before: [END howto_operator_k8s_cluster_resources]

Difference between ``KubernetesPodOperator`` and Kubernetes object spec
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The :class:`~airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator` can be considered
a substitute for a Kubernetes object spec definition that is able
to be run in the Airflow scheduler in the Dag context. If using the operator, there is no need to create the
equivalent YAML/JSON object spec for the Pod you would like to run.
The YAML file can still be provided with the ``pod_template_file`` or even the Pod Spec constructed in Python via
the ``full_pod_spec`` parameter which requires a Kubernetes ``V1Pod``.

How to use private images (container registry)?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
By default, the :class:`~airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator` will
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

.. exampleinclude:: /../tests/system/cncf/kubernetes/example_kubernetes.py
    :language: python
    :start-after: [START howto_operator_k8s_private_image]
    :end-before: [END howto_operator_k8s_private_image]

Also for this action you can use operator in the deferrable mode:

.. exampleinclude:: /../tests/system/cncf/kubernetes/example_kubernetes_async.py
    :language: python
    :start-after: [START howto_operator_k8s_private_image_async]
    :end-before: [END howto_operator_k8s_private_image_async]

Example to fetch and display container log periodically

.. exampleinclude:: /../tests/system/cncf/kubernetes/example_kubernetes_async.py
    :language: python
    :start-after: [START howto_operator_async_log]
    :end-before: [END howto_operator_async_log]


How does XCom work?
^^^^^^^^^^^^^^^^^^^
The :class:`~airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator` handles
XCom values differently than other operators. In order to pass a XCom value
from your Pod you must specify the ``do_xcom_push`` as ``True``. This will create a sidecar container that runs
alongside the Pod. The Pod must write the XCom value into this location at the ``/airflow/xcom/return.json`` path.

.. note::
  An invalid json content will fail, example ``echo 'hello' > /airflow/xcom/return.json`` fail and  ``echo '\"hello\"' > /airflow/xcom/return.json`` work


See the following example on how this occurs:

.. exampleinclude:: /../tests/system/cncf/kubernetes/example_kubernetes.py
    :language: python
    :start-after: [START howto_operator_k8s_write_xcom]
    :end-before: [END howto_operator_k8s_write_xcom]
.. note::
  XCOMs will be pushed only for tasks marked as ``State.SUCCESS``.

Also for this action you can use operator in the deferrable mode:

.. exampleinclude:: /../tests/system/cncf/kubernetes/example_kubernetes_async.py
    :language: python
    :start-after: [START howto_operator_k8s_write_xcom_async]
    :end-before: [END howto_operator_k8s_write_xcom_async]


Run command in KubernetesPodOperator from TaskFlow
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
With the usage of the ``@task.kubernetes_cmd`` decorator, you can run a command returned by a function
in a ``KubernetesPodOperator`` simplifying it's connection to the TaskFlow.

Difference between ``@task.kubernetes`` and ``@task.kubernetes_cmd``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
``@task.kubernetes`` decorator is designed to run a Python function inside a Kubernetes pod using KPO.
It does this by serializing the function into a temporary Python script that is executed inside the container.
This is well-suited for cases where you want to isolate Python code execution and manage complex dependencies,
as described in the :doc:`TaskFlow documentation <apache-airflow:tutorial/taskflow>`.

In contrast, ``@task.kubernetes_cmd`` decorator allows the decorated function to return
a shell command (as a list of strings), which is then passed as cmds or arguments to
``KubernetesPodOperator``.
This enables executing arbitrary commands available inside a Kubernetes pod --
without needing to wrap it in Python code.

A key benefit here is that Python excels at composing and templating these commands.
Shell commands can be dynamically generated using Python's string formatting, templating,
extra function calls and logic. This makes it a flexible tool for orchestrating complex pipelines
where the task is to invoke CLI-based operations in containers without the need to leave
a TaskFlow context.

How does this decorator work?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
See the following examples on how the decorator works:

.. exampleinclude:: /../tests/system/cncf/kubernetes/example_kubernetes_cmd_decorator.py
    :language: python
    :start-after: [START howto_decorator_kubernetes_cmd]
    :end-before: [END howto_decorator_kubernetes_cmd]

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

KubernetesPodOperator callbacks
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :class:`~airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator` supports different
callbacks that can be used to trigger actions during the lifecycle of the pod. In order to use them, you need to
create a subclass of :class:`~airflow.providers.cncf.kubernetes.callbacks.KubernetesPodOperatorCallback` and override
the callbacks methods you want to use. Then you can pass your callback class to the operator using the ``callbacks``
parameter.

The following callbacks are supported:

* on_sync_client_creation: called after creating the sync client
* on_pod_creation: called after creating the pod
* on_pod_starting: called after the pod starts
* on_pod_completion: called when the pod completes
* on_pod_cleanup: called after cleaning/deleting the pod
* on_operator_resuming: when resuming the task from deferred state
* progress_callback: called on each line of containers logs

Currently, the callbacks methods are not called in the async mode, this support will be added in the future.

Example:
~~~~~~~~
.. code-block:: python

    import kubernetes.client as k8s
    import kubernetes_asyncio.client as async_k8s

    from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
    from airflow.providers.cncf.kubernetes.callbacks import KubernetesPodOperatorCallback


    class MyCallback(KubernetesPodOperatorCallback):
        @staticmethod
        def on_pod_creation(*, pod: k8s.V1Pod, client: k8s.CoreV1Api, mode: str, **kwargs) -> None:
            client.create_namespaced_service(
                namespace=pod.metadata.namespace,
                body=k8s.V1Service(
                    metadata=k8s.V1ObjectMeta(
                        name=pod.metadata.name,
                        labels=pod.metadata.labels,
                        owner_references=[
                            k8s.V1OwnerReference(
                                api_version=pod.api_version,
                                kind=pod.kind,
                                name=pod.metadata.name,
                                uid=pod.metadata.uid,
                                controller=True,
                                block_owner_deletion=True,
                            )
                        ],
                    ),
                    spec=k8s.V1ServiceSpec(
                        selector=pod.metadata.labels,
                        ports=[
                            k8s.V1ServicePort(
                                name="http",
                                port=80,
                                target_port=80,
                            )
                        ],
                    ),
                ),
            )


    k = KubernetesPodOperator(
        task_id="test_callback",
        image="alpine",
        cmds=["/bin/sh"],
        arguments=["-c", "echo hello world; echo Custom error > /dev/termination-log; exit 1;"],
        name="test-callback",
        callbacks=MyCallback,
    )

Passing secrets
^^^^^^^^^^^^^^^

Never use environment variables to pass secrets (for example connection authentication information) to
Kubernetes Pod Operator. Such environment variables will be visible to anyone who has access
to see and describe PODs in Kubernetes. Instead, pass your secrets via native Kubernetes ``Secrets`` or
use Connections and Variables from Airflow. For the latter, you need to have ``apache-airflow`` package
installed in your image in the same version as Airflow you run your Kubernetes Pod Operator from).

Reference
^^^^^^^^^
For further information, look at:

* `Kubernetes Documentation <https://kubernetes.io/docs/home/>`__
* `Pull an Image from a Private Registry <https://kubernetes.io/docs/tasks/configure-pod-container/pull-image-private-registry/>`__

SparkKubernetesOperator
==========================
The :class:`~airflow.providers.cncf.kubernetes.operators.spark_kubernetes.SparkKubernetesOperator` allows
you to create and run spark job on a Kubernetes cluster. It is based on `spark-on-k8s-operator <https://github.com/GoogleCloudPlatform/spark-on-k8s-operator>`__ project.

This operator simplifies the interface and accepts different parameters to configure and run spark application on Kubernetes.
Similar to the KubernetesOperator, we have added the logic to wait for a job after submission,
manage error handling, retrieve logs from the driver pod and the ability to delete a spark job.
It also supports out-of-the-box Kubernetes functionalities such as handling of volumes, config maps, secrets, etc.


How does this operator work?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The operator initiates a Spark task by generating a SparkApplication Custom Resource Definition (CRD) within Kubernetes.
This SparkApplication task subsequently generates driver and required executor pods, using the parameters specified by the user.
The operator continuously monitors the task's progress until it either succeeds or fails.
It retrieves logs from the driver pod and displays them in the Airflow UI.


Usage examples
^^^^^^^^^^^^^^
In order to create a SparkKubernetesOperator task, you must provide a basic template that includes Spark configuration and
Kubernetes-related resource configuration. This template, which can be in either YAML or JSON format, serves as a
starting point for the operator. Below is a sample template that you can utilize:

spark_job_template.yaml

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

  * The template file consists of two primary categories: ``spark`` and ``kubernetes``.

    * spark: This segment encompasses the task's Spark configuration, mirroring the structure of the Spark API template.

    * kubernetes: This segment encompasses the task's Kubernetes resource configuration, directly corresponding to the Kubernetes API Documentation. Each resource type includes an example within the template.

  * The designated base image to be utilized is ``apache/spark-py:v3.4.0``.

  * Ensure that the Spark code is either embedded within the image, mounted using a persistentVolume, or accessible from an external location such as an S3 bucket.

Next, create the task using the following:

.. code-block:: python

    SparkKubernetesOperator(
        task_id="spark_task",
        image="apache/spark-py:v3.4.0",  # OR custom image using that
        code_path="local://path/to/spark/code.py",
        application_file="spark_job_template.yaml",  # OR spark_job_template.json
        dag=dag,
    )

Note: Alternatively application_file can also be a json file. see below example

spark_job_template.json

.. code-block:: json

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



An alternative method, apart from using YAML or JSON files, is to directly pass the ``template_spec`` field instead of application_file
if you prefer not to employ a file for configuration.


Reference
^^^^^^^^^
For further information, look at:

* `Kubernetes Documentation <https://kubernetes.io/docs/home/>`__
* `Spark-on-k8s-operator Documentation - User guide <https://www.kubeflow.org/docs/components/spark-operator/user-guide/>`__
* `Spark-on-k8s-operator Documentation - API <https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/blob/master/docs/api-docs.md>`__


.. _howto/operator:kubernetesjoboperator:

KubernetesJobOperator
=====================

The :class:`~airflow.providers.cncf.kubernetes.operators.job.KubernetesJobOperator` allows
you to create and run Jobs on a Kubernetes cluster.

.. note::
  If you use a managed Kubernetes consider using a specialize KJO operator as it simplifies the Kubernetes authorization process :

  - ``GKEStartJobOperator`` operator for `Google Kubernetes Engine <https://cloud.google.com/kubernetes-engine/>`__.

.. note::
  The :doc:`Kubernetes executor <kubernetes_executor>` is **not** required to use this operator.

How does this operator work?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The :class:`~airflow.providers.cncf.kubernetes.operators.job.KubernetesJobOperator` uses the
Kubernetes API to launch a job in a Kubernetes cluster. The operator uses the Kube Python Client to generate a Kubernetes API
request that dynamically launches this Job.
Users can specify a kubeconfig file using the ``config_file`` parameter, otherwise the operator will default
to ``~/.kube/config``. It also allows users to supply a template YAML file using the ``job_template_file`` parameter.

.. exampleinclude:: /../tests/system/cncf/kubernetes/example_kubernetes_job.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_k8s_job]
    :end-before: [END howto_operator_k8s_job]

The :class:`~airflow.providers.cncf.kubernetes.operators.job.KubernetesJobOperator` also supports deferrable mode:

.. exampleinclude:: /../tests/system/cncf/kubernetes/example_kubernetes_job.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_k8s_job_deferrable]
    :end-before: [END howto_operator_k8s_job_deferrable]


Difference between ``KubernetesPodOperator`` and ``KubernetesJobOperator``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The :class:`~airflow.providers.cncf.kubernetes.operators.job.KubernetesJobOperator` is operator for creating Job.
A Job creates one or more Pods and will continue to retry execution of the Pods until a specified number of them successfully terminate.
As Pods successfully complete, the Job tracks the successful completions. When a specified number of successful completions is reached, the Job is complete.
Users can limit how many times a Job retries execution using configuration parameters like ``activeDeadlineSeconds`` and ``backoffLimit``.
Instead of ``template`` parameter for Pod creating this operator uses :class:`~airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator`.
It means that user can use all parameters from :class:`~airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator` in :class:`~airflow.providers.cncf.kubernetes.operators.job.KubernetesJobOperator`.

More information about the Jobs here: `Kubernetes Job Documentation <https://kubernetes.io/docs/concepts/workloads/controllers/job/>`__


.. _howto/operator:KubernetesDeleteJobOperator:

KubernetesDeleteJobOperator
===========================

The :class:`~airflow.providers.cncf.kubernetes.operators.job.KubernetesDeleteJobOperator` allows
you to delete Jobs on a Kubernetes cluster.

.. exampleinclude:: /../tests/system/cncf/kubernetes/example_kubernetes_job.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_delete_k8s_job]
    :end-before: [END howto_operator_delete_k8s_job]


.. _howto/operator:KubernetesPatchJobOperator:

KubernetesPatchJobOperator
==========================

The :class:`~airflow.providers.cncf.kubernetes.operators.job.KubernetesPatchJobOperator` allows
you to update Jobs on a Kubernetes cluster.

.. exampleinclude:: /../tests/system/cncf/kubernetes/example_kubernetes_job.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_update_job]
    :end-before: [END howto_operator_update_job]


.. _howto/operator:KubernetesInstallKueueOperator:

KubernetesInstallKueueOperator
==============================

The :class:`~airflow.providers.cncf.kubernetes.operators.kueue.KubernetesInstallKueueOperator` allows
you to install the Kueue component in a Kubernetes cluster

.. exampleinclude:: /../tests/system/cncf/kubernetes/example_kubernetes_kueue.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_k8s_kueue_install]
    :end-before: [END howto_operator_k8s_kueue_install]

Reference
^^^^^^^^^
For further information, look at:

* `Kubernetes Documentation <https://kubernetes.io/docs/home/>`__
* `Kueue Installation details <https://kueue.sigs.k8s.io/docs/installation/>`__


.. _howto/operator:KubernetesStartKueueJobOperator:


KubernetesStartKueueJobOperator
===============================

The :class:`~airflow.providers.cncf.kubernetes.operators.kueue.KubernetesStartKueueJobOperator` allows
you to start a Kueue job in a Kubernetes cluster

.. exampleinclude:: /../tests/system/cncf/kubernetes/example_kubernetes_kueue.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_k8s_install_kueue]
    :end-before: [END howto_operator_k8s_install_kueue]

For further information, look at:

* `Kubernetes Documentation <https://kubernetes.io/docs/home/>`__
* `Run a Kubernetes job in Kueue <https://kueue.sigs.k8s.io/docs/tasks/run/jobs/>`__
