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


.. _KubernetesExecutor:

Kubernetes Executor
===================

.. note::

    As of Airflow 2.7.0, you need to install the ``cncf.kubernetes`` provider package to use
    this executor. This can done by installing ``apache-airflow-providers-cncf-kubernetes>=7.4.0``
    or by installing Airflow with the ``cncf.kubernetes`` extras:
    ``pip install 'apache-airflow[cncf.kubernetes]'``.


The Kubernetes executor runs each task instance in its own pod on a Kubernetes cluster.

KubernetesExecutor runs as a process in the Airflow Scheduler. The scheduler itself does
not necessarily need to be running on Kubernetes, but does need access to a Kubernetes cluster.

KubernetesExecutor requires a non-sqlite database in the backend.

When a Dag submits a task, the KubernetesExecutor requests a worker pod from the Kubernetes API. The worker pod then runs the task, reports the result, and terminates.

.. image:: img/arch-diag-kubernetes.png


One example of an Airflow deployment running on a distributed set of five nodes in a Kubernetes cluster is shown below.

.. image:: img/arch-diag-kubernetes2.png

Consistent with the regular Airflow architecture, the Workers need access to the Dag files to execute the tasks within those Dags and interact with the Metadata repository. Also, configuration information specific to the Kubernetes Executor, such as the worker namespace and image information, needs to be specified in the Airflow Configuration file.

Additionally, the Kubernetes Executor enables specification of additional features on a per-task basis using the Executor config.

.. @startuml
.. Airflow_Scheduler -> Kubernetes: Request a new pod with command "airflow run..."
.. Kubernetes -> Airflow_Worker: Create Airflow worker with command "airflow run..."
.. Airflow_Worker -> Airflow_DB: Report task passing or failure to DB
.. Airflow_Worker -> Kubernetes: Pod completes with state "Succeeded" and k8s records in ETCD
.. Kubernetes -> Airflow_Scheduler: Airflow scheduler reads "Succeeded" from k8s watcher thread
.. @enduml
.. image:: img/k8s-happy-path.png

Configuration
-------------

.. _concepts:pod_template_file:

pod_template_file
~~~~~~~~~~~~~~~~~

To customize the pod used for k8s executor worker processes, you may create a pod template file. You must provide
the path to the template file in the ``pod_template_file`` option in the ``kubernetes_executor`` section of ``airflow.cfg``.

Airflow has two strict requirements for pod template files: base image and pod name.

Base image
^^^^^^^^^^

A ``pod_template_file`` must have a container named ``base`` at the ``spec.containers[0]`` position, and
its ``image`` must be specified.

You are free to create sidecar containers after this required container, but Airflow assumes that the
airflow worker container exists at the beginning of the container array, and assumes that the
container is named ``base``.

.. note::

    Airflow may override the base container ``image``, e.g. through :ref:`pod_override <concepts:pod_override>`
    configuration; but it must be present in the template file and must not be blank.

Pod name
^^^^^^^^

The pod's ``metadata.name`` must be set in the template file.  This field will *always* be set dynamically at
pod launch to guarantee uniqueness across all pods. But again, it must be included in the template, and cannot
be left blank.


Example pod templates
~~~~~~~~~~~~~~~~~~~~~

With these requirements in mind, here are some examples of basic ``pod_template_file`` YAML files.

.. note::

    The examples below should work when using default Airflow configuration values. However, many custom
    configuration values need to be explicitly passed to the pod via this template too. This includes,
    but is not limited to, sql configuration, required Airflow connections, Dags folder path and
    logging settings. See :doc:`../../configurations-ref` for details.

Storing Dags in the image:

.. literalinclude:: /../src/airflow/providers/cncf/kubernetes/pod_template_file_examples/dags_in_image_template.yaml
    :language: yaml
    :start-after: [START template_with_dags_in_image]
    :end-before: [END template_with_dags_in_image]

Storing Dags in a ``persistentVolume``:

.. literalinclude:: /../src/airflow/providers/cncf/kubernetes/pod_template_file_examples/dags_in_volume_template.yaml
    :language: yaml
    :start-after: [START template_with_dags_in_volume]
    :end-before: [END template_with_dags_in_volume]

Pulling Dags from ``git``:

.. literalinclude:: /../src/airflow/providers/cncf/kubernetes/pod_template_file_examples/git_sync_template.yaml
    :language: yaml
    :start-after:  [START git_sync_template]
    :end-before: [END git_sync_template]

.. _concepts:pod_override:

pod_override
~~~~~~~~~~~~

When using the KubernetesExecutor, Airflow offers the ability to override system defaults on a per-task basis.
To utilize this functionality, create a Kubernetes V1pod object and fill in your desired overrides.

To overwrite the base container of the pod launched by the KubernetesExecutor,
create a V1pod with a single container, and overwrite the fields as follows:

.. exampleinclude:: /../../../../airflow-core/src/airflow/example_dags/example_kubernetes_executor.py
    :language: python
    :start-after: [START task_with_volume]
    :end-before: [END task_with_volume]

Note that the following fields **will all be extended** instead of overwritten. From *spec*: volumes, and init_containers. From *container*: volume mounts, environment variables, ports, and devices.

To add a sidecar container to the launched pod, create a V1pod with an empty first container with the
name ``base`` and a second container containing your desired sidecar.

.. exampleinclude:: /../../../../airflow-core/src/airflow/example_dags/example_kubernetes_executor.py
    :language: python
    :start-after: [START task_with_sidecar]
    :end-before: [END task_with_sidecar]

You can also create custom ``pod_template_file`` on a per-task basis so that you can recycle the same base values between multiple tasks.
This will replace the default ``pod_template_file`` named in the airflow.cfg and then override that template using the ``pod_override``.
That ``pod_template_file`` will also be used to generate the Pod K8s Spec visible in the Airflow UI.

Here is an example of a task with both features:

.. code-block:: python

    import os

    import pendulum

    from airflow import DAG
    from airflow.decorators import task
    from airflow.example_dags.libs.helper import print_stuff
    from airflow.settings import AIRFLOW_HOME

    from kubernetes.client import models as k8s

    with DAG(
        dag_id="example_pod_template_file",
        schedule=None,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        tags=["example3"],
    ) as dag:
        executor_config_template = {
            "pod_template_file": os.path.join(AIRFLOW_HOME, "pod_templates/basic_template.yaml"),
            "pod_override": k8s.V1Pod(metadata=k8s.V1ObjectMeta(labels={"release": "stable"})),
        }

        @task(executor_config=executor_config_template)
        def task_with_template():
            print_stuff()


Managing Dags and logs
~~~~~~~~~~~~~~~~~~~~~~

Use of persistent volumes is optional and depends on your configuration.

- **Dags**:

To get the Dags into the workers, you can:

  - Include Dags in the image.
  - Use ``git-sync`` which, before starting the worker container, will run a ``git pull`` of the Dags repository.
  - Storing Dags on a persistent volume, which can be mounted on all workers.

- **Logs**:

To get task logs out of the workers, you can:

  - Use a persistent volume mounted on both the webserver and workers.

  - Enable remote logging.

.. note::

    If you don't enable logging persistence, and if you have not enabled remote logging, logs will be lost after the worker pods shut down.


Comparison with CeleryExecutor
------------------------------

In contrast to CeleryExecutor, KubernetesExecutor does not require additional components such as Redis,
but does require access to Kubernetes cluster.

Also monitoring the Pods can be done with the built-in Kubernetes monitoring.

With KubernetesExecutor, each task runs in its own pod. The pod is created when the task is queued, and terminates when the task completes.
Historically, in scenarios such as burstable workloads, this presented a resource utilization advantage over CeleryExecutor, where you needed
a fixed number of long-running Celery worker pods, whether or not there were tasks to run.

However, the :doc:`official Apache Airflow Helm chart <helm-chart:index>` can automatically scale celery workers down to zero based on the number of tasks in the queue,
so when using the official chart, this is no longer an advantage.

With Celery workers you will tend to have less task latency because the worker pod is already up and running when the task is queued. On the
other hand, because multiple tasks are running in the same pod, with Celery you may have to be more mindful about resource utilization
in your task design, particularly memory consumption.

One scenario where KubernetesExecutor can be helpful is if you have long-running tasks, because if you deploy while a task is running,
the task will keep running until it completes (or times out, etc). But with CeleryExecutor, provided you have set a grace period, the
task will only keep running up until the grace period has elapsed, at which time the task will be terminated.  Another scenario where
KubernetesExecutor can work well is when your tasks are not very uniform with respect to resource requirements or images.

Finally, note that it does not have to be either-or; with CeleryKubernetesExecutor, it is possible to use both CeleryExecutor and
KubernetesExecutor simultaneously on the same cluster. CeleryKubernetesExecutor will look at a task's ``queue`` to determine
whether to run on Celery or Kubernetes.  By default, tasks are sent to Celery workers, but if you want a task to run using KubernetesExecutor,
you send it to the  ``kubernetes`` queue and it will run in its own pod.  And KubernetesPodOperator can be used
to similar effect, no matter what executor you are using.

Fault Tolerance
---------------

.. tip::

    To troubleshoot issues with KubernetesExecutor, you can use ``airflow kubernetes generate-dag-yaml`` command.
    This command generates the pods as they will be launched in Kubernetes and dumps them into yaml files for you to inspect.


Handling Worker Pod Crashes
~~~~~~~~~~~~~~~~~~~~~~~~~~~

When dealing with distributed systems, we need a system that assumes that any component can crash at any moment for reasons ranging from OOM errors to node upgrades.

In the case where a worker dies before it can report its status to the backend DB, the executor can use a Kubernetes watcher thread to discover the failed pod.

.. @startuml
..
.. Airflow_Scheduler -> Kubernetes: Request a new pod with command "airflow run..."
.. Kubernetes -> Airflow_Worker: Create Airflow worker with command "airflow run..."
.. Airflow_Worker -> Airflow_Worker: Pod fails before task can complete
.. Airflow_Worker -> Kubernetes: Pod completes with state "Failed" and k8s records in ETCD
.. Kubernetes -> Airflow_Scheduler: Airflow scheduler reads "Failed" from k8s watcher thread
.. Airflow_Scheduler -> Airflow_DB: Airflow scheduler records "FAILED" state to DB for task
..
.. @enduml

.. image:: img/k8s-failed-pod.png


A Kubernetes watcher is a thread that can subscribe to every change that occurs in Kubernetes' database. It is alerted when pods start, run, end, and fail.
By monitoring this stream, the KubernetesExecutor can discover that the worker crashed and correctly report the task as failed.


But What About Cases Where the Scheduler Pod Crashes?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In cases of scheduler crashes, the scheduler will recover its state using the watcher's ``resourceVersion``.

When monitoring the Kubernetes cluster's watcher thread, each event has a monotonically rising number called a ``resourceVersion``.
Every time the executor reads a ``resourceVersion``, the executor stores the latest value in the backend database.
Because the resourceVersion is stored, the scheduler can restart and continue reading the watcher stream from where it left off.
Since the tasks are run independently of the executor and report results directly to the database, scheduler failures will not lead to task failures or re-runs.
