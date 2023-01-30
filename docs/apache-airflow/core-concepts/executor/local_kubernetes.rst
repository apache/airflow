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


.. _executor:LocalKubernetesExecutor:

LocalKubernetes Executor
=========================

The :class:`~airflow.executors.local_kubernetes_executor.LocalKubernetesExecutor` allows users
to simultaneously run a ``LocalExecutor`` and a ``KubernetesExecutor``.
An executor is chosen to run a task based on the task's queue.

``LocalKubernetesExecutor`` provides the capability of running tasks with either ``LocalExecutor``,
which runs tasks within the scheduler service, or with ``KubernetesExecutor``, which runs each task
in its own pod on a kubernetes cluster.
