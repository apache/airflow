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

Customizing Labels for Pods
---------------------------

The Helm chart allows you to customize labels for your Airflow objects. You can set global labels that apply to all objects and pods defined in the chart, as well as component-specific labels for individual Airflow components.

Global Labels
~~~~~~~~~~~~~

Global labels can be set using the ``labels`` parameter in your values file. These labels will be applied to all Airflow objects and pods defined in the chart:

.. code-block:: yaml

    labels:
      environment: production

Component-Specific Labels
~~~~~~~~~~~~~~~~~~~~~~~~~

You can also set specific labels for individual Airflow components, which will be merged with the global labels. Component-specific labels take precedence over global labels, allowing you to override them as needed.

For example, to add specific labels to different components:

.. code-block:: yaml

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

    # Webserver specific labels
    webserver:
      labels:
        role: ui
