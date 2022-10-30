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

Customizing Workers
===================

Both ``CeleryExecutor`` and ``KubernetesExecutor`` workers can be highly customized with the :ref:`workers parameters <parameters:workers>`.
For example, to set resources on workers:

.. code-block:: yaml

  workers:
    resources:
      requests:
        cpu: 1
      limits:
        cpu: 1

See :ref:`workers parameters <parameters:workers>` for a complete list.

One notable exception for ``KubernetesExecutor`` is that the default anti-affinity applied to ``CeleryExecutor`` workers to spread them across nodes
is not applied to ``KubernetesExecutor`` workers, as there is no reason to spread out per-task workers.

Custom ``pod_template_file``
----------------------------

With ``KubernetesExecutor`` or ``CeleryKubernetesExecutor`` you can also provide a complete ``pod_template_file`` to configure Kubernetes workers.
This may be useful if you need different configuration between worker types for ``CeleryKubernetesExecutor``
or if you need to customize something not possible with :ref:`workers parameters <parameters:workers>` alone.

As an example, let's say you want to set ``priorityClassName`` on your workers:

.. note::

  The following example is NOT functional, but meant to be illustrative of how you can provide a custom ``pod_template_file``.
  You're better off starting with the `default pod_template_file`_ instead.

.. _default pod_template_file: https://github.com/apache/airflow/blob/main/chart/files/pod-template-file.kubernetes-helm-yaml

.. code-block:: yaml

  podTemplate: |
    apiVersion: v1
    kind: Pod
    metadata:
      name: dummy-name
      labels:
        tier: airflow
        component: worker
        release: {{ .Release.Name }}
    spec:
      priorityClassName: high-priority
      containers:
        - name: base
