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


.. _executor:CeleryKubernetesExecutor:

CeleryKubernetes Executor
=========================

The :class:`~airflow.executors.celery_kubernetes_executor.CeleryKubernetesExecutor` allows users
to run simultaneously a ``CeleryExecutor`` and a ``KubernetesExecutor``.
An executor is chosen to run a task based on the task's queue.

``CeleryKubernetesExecutor`` inherits the scalability of the ``CeleryExecutor`` to
handle the high load at the peak time and runtime isolation of the ``KubernetesExecutor``.

.. note::

    Celery workers must be configured to use CeleryExecutor, while ``scheduler`` should  should be
    configured with executor ``CeleryKubernetesExecutor``.

When to use CeleryKubernetesExecutor
####################################

Using ``CeleryKubernetesExecutor`` can require more configuration than other executors
simply because you must configure both ``CeleryExecutor`` and ``KubernetesExecutor``.

We recommend considering the ``CeleryKubernetesExecutor`` when your use case meets:

1. The number of tasks needed to be scheduled at the peak exceeds the scale that your Kubernetes cluster
   can comfortably handle

2. A relative small portion of your tasks requires runtime isolation.

3. You have plenty of small tasks that can be executed on Celery workers
   but you also have resource-hungry tasks that will be better to run in predefined environments.
