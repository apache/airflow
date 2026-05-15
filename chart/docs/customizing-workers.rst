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
For example, to set resources on workers for CeleryExecutor:

.. code-block:: yaml
   :caption: values.yaml

   workers:
     celery:
       resources:
         requests:
           cpu: 1
         limits:
           cpu: 1

Custom ``pod_template_file``
----------------------------

With ``KubernetesExecutor`` or ``CeleryKubernetesExecutor`` you can also provide a complete ``pod_template_file``
to fully override default Kubernetes workers configuration. This may be useful if you need different configuration between
worker types for ``CeleryKubernetesExecutor`` or if you need to customize something not possible with :ref:`workers parameters <parameters:workers>` alone.

.. note::

   Configuration options between Celery and Kubernetes workers can be overwritten by ``workers.celery`` and ``workers.kubernetes`` sections.

As an example, let's say you want to set ``priorityClassName`` on your workers:

.. note::

   The following example is NOT functional, but meant to be illustrative of how you can provide a custom ``pod_template_file``.
   You're better off starting with the default `pod_template_file`_ instead.

.. _pod_template_file: https://github.com/apache/airflow/blob/main/chart/files/pod-template-file.kubernetes-helm-yaml

.. code-block:: yaml
   :caption: values.yaml

   podTemplate: |
     apiVersion: v1
     kind: Pod
     metadata:
       name: placeholder-name
       labels:
         tier: airflow
         component: worker
         release: {{ .Release.Name }}
     spec:
       priorityClassName: high-priority
       containers:
         - name: base

The same can be achieved with default `pod_template_file`_ by overriding the ``priorityClassName`` option for KubernetesExecutor like:

.. code-block:: yaml
   :caption: values.yaml

   workers:
     kubernetes:
       priorityClassName: high-priority
