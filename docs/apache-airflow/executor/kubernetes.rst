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


.. _executor:KubernetesExecutor:

Kubernetes Executor
===================

The Kubernetes executor runs each task instance in its own pod on a Kubernetes cluster.

Use of persistent volumes is optional and depends on your configuration. There are two types of volumes you may configure:

- **Dags**:

  - By storing dags on a persistent volume, it will be made available to all workers

  - Alternatively, you can include dags in the image, or you can use use ``git-sync``.  With ``git-sync``, before starting the worker container,
    a git pull of the dags repository will be performed and used throughout the lifecycle of the pod.

- **Logs**:

  - You can enable a persistent volume shared between webserver and workers to store task logs.

  - If you don't enable logging persistence, and if you have not enabled remote logging, logs will be lost after the worker pods shut down.

To troubleshoot issue with KubernetesExecutor, you can use ``airflow kubernetes generate-dag-yaml`` command.
This command generates the pods as they will be launched in Kubernetes and dumps them into yaml files for you to inspect.

.. _concepts:pod_template_file:

pod_template_file
#################

As of Airflow 1.10.12, you can use the ``pod_template_file`` option in the ``kubernetes`` section
of the ``airflow.cfg`` file to form the basis of your KubernetesExecutor pods. This process is faster to execute
and easier to modify compared with the legacy configuration approach.

We include multiple examples of working pod operators below, but we would also like to identify a few components
that you must include if you want to provide a custom template file. Aside from these components, every other
element in the template is customizable.

1. Airflow will overwrite the base container ``image`` and the pod's ``metadata.name``.

There are two points where Airflow potentially overwrites the base image: in the ``airflow.cfg``
or the ``pod_override`` (discussed below) setting. This value is overwritten to ensure that users do
not need to update multiple template files every time they upgrade their docker image. The other field
that Airflow overwrites is the ``metadata.name`` field. This field has to be unique across all pods,
so we generate these names dynamically before launch.

It's important to note although Airflow overwrites these fields, they **can not be left blank** in the template.
If these fields are not present in the template, kubernetes can not load the yaml into a Kubernetes ``V1Pod``.

2. A ``pod_template_file`` must have a container named ``base`` at the ``spec.containers[0]`` position

Airflow uses the ``pod_template_file`` by making certain assumptions about the structure of the template.
When airflow creates the worker pod's command, it assumes that the airflow worker container part exists
at the beginning of the container array. It then assumes that the container is named ``base``
when it merges this pod with internal configs. You are more than welcome to create
sidecar containers after this required container.

With these requirements in mind, here are some examples of basic ``pod_template_file`` YAML files.

``pod_template_file`` using the ``dag_in_image`` setting:

.. exampleinclude:: /../../airflow/kubernetes/pod_template_file_examples/dags_in_image_template.yaml
    :language: yaml
    :start-after: [START template_with_dags_in_image]
    :end-before: [END template_with_dags_in_image]

``pod_template_file`` which stores DAGs in a ``persistentVolume``:

.. exampleinclude:: /../../airflow/kubernetes/pod_template_file_examples/dags_in_volume_template.yaml
    :language: yaml
    :start-after: [START template_with_dags_in_volume]
    :end-before: [END template_with_dags_in_volume]

``pod_template_file`` which pulls DAGs from git:

.. exampleinclude:: /../../airflow/kubernetes/pod_template_file_examples/git_sync_template.yaml
    :language: yaml
    :start-after:  [START git_sync_template]
    :end-before: [END git_sync_template]

.. _concepts:pod_override:

pod_override
############

When using the KubernetesExecutor, Airflow offers the ability to override system defaults on a per-task basis.
To utilize this functionality, create a Kubernetes V1pod object and fill in your desired overrides.
Please note that the scheduler will override the ``metadata.name`` of the V1pod before launching it.

To overwrite the base container of the pod launched by the KubernetesExecutor,
create a V1pod with a single container, and overwrite the fields as follows:

.. exampleinclude:: /../../airflow/example_dags/example_kubernetes_executor_config.py
    :language: python
    :start-after: [START task_with_volume]
    :end-before: [END task_with_volume]

Note that the following fields **will all be extended** instead of overwritten. From *spec*: volumes, and init_containers. From *container*: volume mounts, environment variables, ports, and devices.

To add a sidecar container to the launched pod, create a V1pod with an empty first container with the
name ``base`` and a second container containing your desired sidecar.

.. exampleinclude:: /../../airflow/example_dags/example_kubernetes_executor_config.py
    :language: python
    :dedent: 8
    :start-after: [START task_with_sidecar]
    :end-before: [END task_with_sidecar]

You can also create custom ``pod_template_file`` on a per-task basis so that you can recycle the same base values between multiple tasks.
This will replace the default ``pod_template_file`` named in the airflow.cfg and then override that template using the ``pod_override``.

Here is an example of a task with both features:

.. exampleinclude:: /../../airflow/example_dags/example_kubernetes_executor_config.py
    :language: python
    :dedent: 8
    :start-after: [START task_with_template]
    :end-before: [END task_with_template]

KubernetesExecutor Architecture
################################

KubernetesExecutor runs as a process in the Airflow Scheduler.  The scheduler itself does not necessarily need to be running on Kubernetes, but does need access to a Kubernetes cluster.

KubernetesExecutor requires a non-sqlite database in the backend.

When a DAG submits a task, the KubernetesExecutor requests a worker pod from the Kubernetes API. The worker pod then runs the task, reports the result, and terminates.

.. image:: ../img/arch-diag-kubernetes.png


One example of an Airflow deployment running on a distributed set of five nodes in a Kubernetes cluster is shown below.

.. image:: ../img/arch-diag-kubernetes2.png

Consistent with the regular Airflow architecture, the Workers need access to the DAG files to execute the tasks within those DAGs and interact with the Metadata repository. Also, configuration information specific to the Kubernetes Executor, such as the worker namespace and image information, needs to be specified in the Airflow Configuration file.

Additionally, the Kubernetes Executor enables specification of additional features on a per-task basis using the Executor config.

.. @startuml
.. Airflow_Scheduler -> Kubernetes: Request a new pod with command "airflow run..."
.. Kubernetes -> Airflow_Worker: Create Airflow worker with command "airflow run..."
.. Airflow_Worker -> Airflow_DB: Report task passing or failure to DB
.. Airflow_Worker -> Kubernetes: Pod completes with state "Succeeded" and k8s records in ETCD
.. Kubernetes -> Airflow_Scheduler: Airflow scheduler reads "Succeeded" from k8s watcher thread
.. @enduml
.. image:: ../img/k8s-happy-path.png

Comparison with CeleryExecutor
------------------------------

In contrast to CeleryExecutor, KubernetesExecutor does not require additional components such as Redis and Flower, but does require access to Kubernetes cluster.

With KubernetesExecutor, each task runs in its own pod. The pod is created when the task is queued, and terminates when the task completes.
Historically, in some cases this presented a resource utilization advantage over CeleryExecutor, where you needed a fixed number of
long-running celery worker pods, whether or not there were tasks to run.

However, the official Apache Airflow helm chart can automatically scale celery workers down to zero based on the number of tasks in the queue,
so when using the official chart, this is no longer an advantage.

With Celery workers you will tend to have less task latency because the worker pod is already up and running when the task is queued. On the
other hand, because multiple tasks are running in the same pod, with Celery you may have to be more mindful about resource utilization
in your task design, particularly memory consumption.

An positive of KubernetesExecutor is if you have long-running tasks. With KubernetesExecutor, if you do a deployment while a task is running,
the task will keep running until it completes (or times out, etc). But with CeleryExecutor, provided you have set a grace period, the
task will only keep running up until the grace period has elapsed, at which time the task will be terminated.

Finally, note that it does not have to be either-or; with CeleryKubernetesExecutor, it is possible to use both CeleryExecutor and
KubernetesExecutor simultaneously on the same cluster. CeleryKubernetesExecutor will look at a task's ``queue`` to determine
whether to run on Celery or Kubernetes.  By default, tasks are sent to Celery workers, but if you want a task to run using KubernetesExecutor,
you send it to the  ``kubernetes`` queue and it will run in its own pod.

***************
Fault Tolerance
***************

===========================
Handling Worker Pod Crashes
===========================

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

.. image:: ../img/k8s-failed-pod.png


A Kubernetes watcher is a thread that can subscribe to every change that occurs in Kubernetes' database. It is alerted when pods start, run, end, and fail.
By monitoring this stream, the KubernetesExecutor can discover that the worker crashed and correctly report the task as failed.


=====================================================
But What About Cases Where the Scheduler Pod Crashes?
=====================================================

In cases of scheduler crashes, the scheduler will recover its state using the watcher's ``resourceVersion``.

When monitoring the Kubernetes cluster's watcher thread, each event has a monotonically rising number called a ``resourceVersion``.
Every time the executor reads a ``resourceVersion``, the executor stores the latest value in the backend database.
Because the resourceVersion is stored, the scheduler can restart and continue reading the watcher stream from where it left off.
Since the tasks are run independently of the executor and report results directly to the database, scheduler failures will not lead to task failures or re-runs.
