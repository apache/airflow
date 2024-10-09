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



Google Kubernetes Engine Operators
==================================

`Google Kubernetes Engine (GKE) <https://cloud.google.com/kubernetes-engine/>`__ provides a managed environment for
deploying, managing, and scaling your containerized applications using Google infrastructure. The GKE environment
consists of multiple machines (specifically, Compute Engine instances) grouped together to form a cluster.

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: /operators/_partials/prerequisite_tasks.rst

Manage GKE cluster
^^^^^^^^^^^^^^^^^^

A cluster is the foundation of GKE - all workloads run on top of the cluster. It is made up on a cluster master
and worker nodes. The lifecycle of the master is managed by GKE when creating or deleting a cluster.
The worker nodes are represented as Compute Engine VM instances that GKE creates on your behalf when creating a cluster.

.. _howto/operator:GKECreateClusterOperator:

Create GKE cluster
""""""""""""""""""

Here is an example of a cluster definition:

.. exampleinclude:: /../../providers/tests/system/google/cloud/kubernetes_engine/example_kubernetes_engine.py
    :language: python
    :start-after: [START howto_operator_gcp_gke_create_cluster_definition]
    :end-before: [END howto_operator_gcp_gke_create_cluster_definition]

A dict object like this, or a
:class:`~google.cloud.container_v1.types.Cluster`
definition, is required when creating a cluster with
:class:`~airflow.providers.google.cloud.operators.kubernetes_engine.GKECreateClusterOperator`.

.. exampleinclude:: /../../providers/tests/system/google/cloud/kubernetes_engine/example_kubernetes_engine.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gke_create_cluster]
    :end-before: [END howto_operator_gke_create_cluster]

You can use deferrable mode for this action in order to run the operator asynchronously. It will give you a
possibility to free up the worker when it knows it has to wait, and hand off the job of resuming Operator to a Trigger.
As a result, while it is suspended (deferred), it is not taking up a worker slot and your cluster will have a
lot less resources wasted on idle Operators or Sensors:

.. exampleinclude:: /../../providers/tests/system/google/cloud/kubernetes_engine/example_kubernetes_engine_async.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gke_create_cluster_async]
    :end-before: [END howto_operator_gke_create_cluster_async]


.. _howto/operator:GKEStartKueueInsideClusterOperator:

Install Kueue of specific version inside Cluster
""""""""""""""""""""""""""""""""""""""""""""""""

Kueue is a Cloud Native Job scheduler that works with the default Kubernetes scheduler, the Job controller,
and the cluster autoscaler to provide an end-to-end batch system. Kueue implements Job queueing, deciding when
Jobs should wait and when they should start, based on quotas and a hierarchy for sharing resources fairly among teams.
Kueue supports Autopilot clusters, Standard GKE with Node Auto-provisioning and regular autoscaled node pools.
To install and use Kueue on your cluster with the help of
:class:`~airflow.providers.google.cloud.operators.kubernetes_engine.GKEStartKueueInsideClusterOperator`
as shown in this example:

.. exampleinclude:: /../../providers/tests/system/google/cloud/kubernetes_engine/example_kubernetes_engine_kueue.py
    :language: python
    :start-after: [START howto_operator_gke_install_kueue]
    :end-before: [END howto_operator_gke_install_kueue]


.. _howto/operator:GKEDeleteClusterOperator:

Delete GKE cluster
""""""""""""""""""

To delete a cluster, use
:class:`~airflow.providers.google.cloud.operators.kubernetes_engine.GKEDeleteClusterOperator`.
This would also delete all the nodes allocated to the cluster.

.. exampleinclude:: /../../providers/tests/system/google/cloud/kubernetes_engine/example_kubernetes_engine.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gke_delete_cluster]
    :end-before: [END howto_operator_gke_delete_cluster]

You can use deferrable mode for this action in order to run the operator asynchronously. It will give you a
possibility to free up the worker when it knows it has to wait, and hand off the job of resuming Operator to a Trigger.
As a result, while it is suspended (deferred), it is not taking up a worker slot and your cluster will have a
lot less resources wasted on idle Operators or Sensors:

.. exampleinclude:: /../../providers/tests/system/google/cloud/kubernetes_engine/example_kubernetes_engine_async.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gke_delete_cluster_async]
    :end-before: [END howto_operator_gke_delete_cluster_async]

Manage workloads on a GKE cluster
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

GKE works with containerized applications, such as those created on Docker, and deploys them to run on the cluster.
These are called workloads, and when deployed on the cluster they leverage the CPU and memory resources of the cluster
to run effectively.

.. _howto/operator:GKEStartPodOperator:

Run a Pod on a GKE cluster
""""""""""""""""""""""""""

There are two operators available in order to run a pod on a GKE cluster:

* :class:`~airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator`
* :class:`~airflow.providers.google.cloud.operators.kubernetes_engine.GKEStartPodOperator`

``GKEStartPodOperator`` extends ``KubernetesPodOperator`` to provide authorization using Google Cloud credentials.
There is no need to manage the ``kube_config`` file, as it will be generated automatically.
All Kubernetes parameters (except ``config_file``) are also valid for the ``GKEStartPodOperator``.
For more information on ``KubernetesPodOperator``, please look at: :ref:`howto/operator:KubernetesPodOperator` guide.

Using with Private cluster
'''''''''''''''''''''''''''

All clusters have a canonical endpoint. The endpoint is the IP address of the Kubernetes API server that
Airflow use to communicate with your cluster master. The endpoint is displayed in Cloud Console under the **Endpoints** field of the cluster's Details tab, and in the
output of ``gcloud container clusters describe`` in the endpoint field.

Private clusters have two unique endpoint values: ``privateEndpoint``, which is an internal IP address, and
``publicEndpoint``, which is an external one. Running ``GKEStartPodOperator`` against a private cluster
sets the external IP address as the endpoint by default. If you prefer to use the internal IP as the
endpoint, you need to set ``use_internal_ip`` parameter to ``True``.

Using with Autopilot (serverless) cluster
'''''''''''''''''''''''''''''''''''''''''

When running on serverless cluster like GKE Autopilot, the pod startup can sometimes take longer due to cold start.
During the pod startup, the status is checked in regular short intervals and warning messages are emitted if the pod
has not yet started. You can increase this interval length via the ``startup_check_interval_seconds`` parameter, with
recommendation of 60 seconds.

Use of XCom
'''''''''''

We can enable the usage of :ref:`XCom <concepts:xcom>` on the operator. This works by launching a sidecar container
with the pod specified. The sidecar is automatically mounted when the XCom usage is specified and its mount point
is the path ``/airflow/xcom``. To provide values to the XCom, ensure your Pod writes it into a file called
``return.json`` in the sidecar. The contents of this can then be used downstream in your DAG.
Here is an example of it being used:

.. exampleinclude:: /../../providers/tests/system/google/cloud/kubernetes_engine/example_kubernetes_engine.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gke_start_pod_xcom]
    :end-before: [END howto_operator_gke_start_pod_xcom]

And then use it in other operators:

.. exampleinclude:: /../../providers/tests/system/google/cloud/kubernetes_engine/example_kubernetes_engine.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gke_xcom_result]
    :end-before: [END howto_operator_gke_xcom_result]

You can use deferrable mode for this action in order to run the operator asynchronously. It will give you a
possibility to free up the worker when it knows it has to wait, and hand off the job of resuming Operator to a Trigger.
As a result, while it is suspended (deferred), it is not taking up a worker slot and your cluster will have a
lot less resources wasted on idle Operators or Sensors:

.. exampleinclude:: /../../providers/tests/system/google/cloud/kubernetes_engine/example_kubernetes_engine_async.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gke_start_pod_xcom_async]
    :end-before: [END howto_operator_gke_start_pod_xcom_async]


.. _howto/operator:GKEStartJobOperator:

Run a Job on a GKE cluster
""""""""""""""""""""""""""

There are two operators available in order to run a job on a GKE cluster:

* :class:`~airflow.providers.cncf.kubernetes.operators.job.KubernetesJobOperator`
* :class:`~airflow.providers.google.cloud.operators.kubernetes_engine.GKEStartJobOperator`

``GKEStartJobOperator`` extends ``KubernetesJobOperator`` to provide authorization using Google Cloud credentials.
There is no need to manage the ``kube_config`` file, as it will be generated automatically.
All Kubernetes parameters (except ``config_file``) are also valid for the ``GKEStartJobOperator``.

.. exampleinclude:: /../../providers/tests/system/google/cloud/kubernetes_engine/example_kubernetes_engine_job.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gke_start_job]
    :end-before: [END howto_operator_gke_start_job]

``GKEStartJobOperator`` also supports deferrable mode. Note that it makes sense only if the ``wait_until_job_complete``
parameter is set ``True``.

.. exampleinclude:: /../../providers/tests/system/google/cloud/kubernetes_engine/example_kubernetes_engine_job.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gke_start_job_def]
    :end-before: [END howto_operator_gke_start_job_def]

For run Job on a GKE cluster with Kueue enabled use ``GKEStartKueueJobOperator``.

.. exampleinclude:: /../../providers/tests/system/google/cloud/kubernetes_engine/example_kubernetes_engine_kueue.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_kueue_start_job]
    :end-before: [END howto_operator_kueue_start_job]


.. _howto/operator:GKEDeleteJobOperator:

Delete a Job on a GKE cluster
"""""""""""""""""""""""""""""

There are two operators available in order to delete a job on a GKE cluster:

* :class:`~airflow.providers.cncf.kubernetes.operators.job.KubernetesDeleteJobOperator`
* :class:`~airflow.providers.google.cloud.operators.kubernetes_engine.GKEDeleteJobOperator`

``GKEDeleteJobOperator`` extends ``KubernetesDeleteJobOperator`` to provide authorization using Google Cloud credentials.
There is no need to manage the ``kube_config`` file, as it will be generated automatically.
All Kubernetes parameters (except ``config_file``) are also valid for the ``GKEDeleteJobOperator``.

.. exampleinclude:: /../../providers/tests/system/google/cloud/kubernetes_engine/example_kubernetes_engine_job.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gke_delete_job]
    :end-before: [END howto_operator_gke_delete_job]


.. _howto/operator:GKEDescribeJobOperator:

Retrieve information about Job by given name
""""""""""""""""""""""""""""""""""""""""""""

You can use :class:`~airflow.providers.google.cloud.operators.kubernetes_engine.GKEDescribeJobOperator` to retrieve
detailed description of existing Job by providing its name and namespace.

.. exampleinclude:: /../../providers/tests/system/google/cloud/kubernetes_engine/example_kubernetes_engine_job.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gke_describe_job]
    :end-before: [END howto_operator_gke_describe_job]


.. _howto/operator:GKEListJobsOperator:

Retrieve list of Jobs
"""""""""""""""""""""

You can use :class:`~airflow.providers.google.cloud.operators.kubernetes_engine.GKEListJobsOperator` to retrieve
list of existing Jobs. If ``namespace`` parameter is provided, output will include Jobs across given namespace.
If ``namespace`` parameter is not specified, the information across all the namespaces will be outputted.

.. exampleinclude:: /../../providers/tests/system/google/cloud/kubernetes_engine/example_kubernetes_engine_job.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gke_list_jobs]
    :end-before: [END howto_operator_gke_list_jobs]


.. _howto/operator:GKECreateCustomResourceOperator:

Create a resource in a GKE cluster
""""""""""""""""""""""""""""""""""

You can use :class:`~airflow.providers.google.cloud.operators.kubernetes_engine.GKECreateCustomResourceOperator` to
create resource in the specified Google Kubernetes Engine cluster.

.. exampleinclude:: /../../providers/tests/system/google/cloud/kubernetes_engine/example_kubernetes_engine_resource.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gke_create_resource]
    :end-before: [END howto_operator_gke_create_resource]


.. _howto/operator:GKEDeleteCustomResourceOperator:

Delete a resource in a GKE cluster
""""""""""""""""""""""""""""""""""

You can use :class:`~airflow.providers.google.cloud.operators.kubernetes_engine.GKEDeleteCustomResourceOperator` to
delete resource in the specified Google Kubernetes Engine cluster.

.. exampleinclude:: /../../providers/tests/system/google/cloud/kubernetes_engine/example_kubernetes_engine_resource.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gke_delete_resource]
    :end-before: [END howto_operator_gke_delete_resource]


.. _howto/operator:GKESuspendJobOperator:

Suspend a Job on a GKE cluster
""""""""""""""""""""""""""""""

You can use :class:`~airflow.providers.google.cloud.operators.kubernetes_engine.GKESuspendJobOperator` to
suspend Job in the specified Google Kubernetes Engine cluster.

.. exampleinclude:: /../../providers/tests/system/google/cloud/kubernetes_engine/example_kubernetes_engine_job.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gke_suspend_job]
    :end-before: [END howto_operator_gke_suspend_job]


.. _howto/operator:GKEResumeJobOperator:

Resume a Job on a GKE cluster
"""""""""""""""""""""""""""""

You can use :class:`~airflow.providers.google.cloud.operators.kubernetes_engine.GKEResumeJobOperator` to
resume Job in the specified Google Kubernetes Engine cluster.

.. exampleinclude:: /../../providers/tests/system/google/cloud/kubernetes_engine/example_kubernetes_engine_job.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gke_resume_job]
    :end-before: [END howto_operator_gke_resume_job]

Reference
^^^^^^^^^

For further information, look at:

* `GKE API Documentation <https://cloud.google.com/kubernetes-engine/docs/reference/rest>`__
* `Product Documentation <https://cloud.google.com/kubernetes-engine/docs/>`__
* `Kubernetes Documentation <https://kubernetes.io/docs/home/>`__
* `Configuring GKE cluster access for kubectl <https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl>`__
