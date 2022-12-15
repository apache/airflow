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



Kubernetes
----------

Apache Airflow aims to be a very Kubernetes-friendly project, and many users run Airflow
from within a Kubernetes cluster in order to take advantage of the increased stability
and autoscaling options that Kubernetes provides.

Helm Chart for Kubernetes
=========================

We maintain :doc:`official Helm chart <helm-chart:index>` for Airflow that helps you define, install, and upgrade deployment. The Helm Chart uses :doc:`official Docker image and Dockerfile <docker-stack:index>` that is also maintained and released by the community.

Kubernetes Executor
^^^^^^^^^^^^^^^^^^^

The :doc:`Kubernetes Executor </core-concepts/executor/kubernetes>` allows you to run all the Airflow tasks on
Kubernetes as separate Pods.

KubernetesPodOperator
^^^^^^^^^^^^^^^^^^^^^

The :ref:`KubernetesPodOperator <howto/operator:kubernetespodoperator>` allows you to create
Pods on Kubernetes.

Pod Mutation Hook
^^^^^^^^^^^^^^^^^

The Airflow local settings file (``airflow_local_settings.py``) can define a ``pod_mutation_hook`` function
that has the ability to mutate pod objects before sending them to the Kubernetes client
for scheduling. It receives a single argument as a reference to pod objects, and
is expected to alter its attributes.

This could be used, for instance, to add sidecar or init containers
to every worker pod launched by KubernetesExecutor or KubernetesPodOperator.


.. code-block:: python

    from kubernetes.client.models import V1Pod


    def pod_mutation_hook(pod: V1Pod):
        pod.metadata.annotations["airflow.apache.org/launched-by"] = "Tests"
