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


.. _LocalKubernetesExecutor:

LocalKubernetes Executor
=========================

.. note::

    As of Airflow 2.7.0, you need to install the ``cncf.kubernetes`` provider package to use
    this executor. This can be done by installing ``apache-airflow-providers-cncf-kubernetes>=7.4.0``
    or by installing Airflow with the ``cncf.kubernetes`` extras:
    ``pip install 'apache-airflow[cncf.kubernetes]'``.

.. note::

    ``LocalKubernetesExecutor`` is no longer supported starting from Airflow 3.0.0. You can use the
    :ref:`Using Multiple Executors Concurrently <using-multiple-executors-concurrently>` feature instead,
    which provides equivalent functionality in a more flexible manner.

The :class:`~airflow.providers.cncf.kubernetes.executors.local_kubernetes_executor.LocalKubernetesExecutor` allows users
to simultaneously run a ``LocalExecutor`` and a ``KubernetesExecutor``.
An executor is chosen to run a task based on the task's queue.

``LocalKubernetesExecutor`` provides the capability of running tasks with either ``LocalExecutor``,
which runs tasks within the scheduler service, or with ``KubernetesExecutor``, which runs each task
in its own pod on a kubernetes cluster.
