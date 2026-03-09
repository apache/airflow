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

Customizing Labels and Annotations for Pods
===========================================

Customizing Pod Labels
----------------------

The Helm Chart allows you to customize labels for your Airflow objects. You can set global labels that apply to all objects and pods defined in the chart, as well as component-specific labels for individual Airflow components.

Global Labels
~~~~~~~~~~~~~

Global labels can be set using the ``labels`` parameter in your values file. These labels will be applied to all Airflow objects and pods defined in the chart:

.. code-block:: yaml
   :caption: values.yaml

   labels:
     environment: production

Component-Specific Labels
~~~~~~~~~~~~~~~~~~~~~~~~~

You can also set specific labels for individual Airflow components, which will be merged with the global labels. Component-specific labels take precedence over global labels, allowing you to override them as needed.

For example, to add specific labels to different components:

.. code-block:: yaml
   :caption: values.yaml

   # Global labels applied to all pods
   labels:
     environment: production

   # Scheduler specific labels
   scheduler:
     labels:
       role: scheduler

   # Worker specific labels
   workers:
     labels:
       role: worker

   # API Server specific labels
   apiServer:
     labels:
       role: ui

Customizing Pod Annotations
---------------------------

Pod annotations can be customized similarly to labels using ``podAnnotations`` and ``airflowPodAnnotations``.

Global Pod Annotations
~~~~~~~~~~~~~~~~~~~~~~

Global pod annotations can be set using ``airflowPodAnnotations``. These are applied to all Airflow component pods (scheduler, api-server/webserver, triggerer, dag-processor and workers):

.. code-block:: yaml
   :caption: values.yaml

   airflowPodAnnotations:
     example.com/team: data-platform

Component-Specific Pod Annotations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Each component also supports its own ``podAnnotations``. Component-specific annotations take precedence over global ones:

.. code-block:: yaml
   :caption: values.yaml

   scheduler:
     podAnnotations:
       example.com/component: scheduler

Templated Pod Annotations
~~~~~~~~~~~~~~~~~~~~~~~~~

Both ``airflowPodAnnotations`` and ``podAnnotations`` support Helm template expressions. This allows annotations to reference release metadata or compute checksums of chart-managed resources, so that pods automatically restart when those resources change.

For example, to restart scheduler pods whenever the chart's extra ConfigMaps change:

.. code-block:: yaml
   :caption: values.yaml

   extraConfigMaps:
     my-listener-config:
       data: |
         listener.py: ...

   scheduler:
     podAnnotations:
       checksum/extra-configmaps: '{{ include (print $.Template.BasePath "/configmaps/extra-configmaps.yaml") . | sha256sum }}'

You can also reference release metadata:

.. code-block:: yaml
   :caption: values.yaml

   airflowPodAnnotations:
     release: '{{ .Release.Name }}'

.. note::

   The ``include``/``sha256sum`` pattern only works for resources managed by this chart
   (e.g., those created via ``extraConfigMaps`` or ``extraSecrets``).
   For ConfigMaps or Secrets created outside the chart, consider using a tool like
   `Stakater Reloader <https://github.com/stakater/Reloader>`__ to trigger pod restarts
   automatically.
