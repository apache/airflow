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

Extending the Chart
===================

The Airflow Helm Chart can be easily extended by creating a custom chart which will depend on the Airflow chart.
That can be useful in cases where there is a need for custom templates deployment (e.g. maintenance CronJobs),
which are not directly related to the Airflow Helm Chart and should not be added to it in the source repository.
During installation of custom chart, the Airflow chart will also be installed.

You can extend the official Airflow chart by applying the following steps.

Create your custom Helm Chart
-----------------------------

First, you will need to create you own chart directory. You can do it by running the following command:

.. code-block:: bash

   helm create my-custom-chart


This command will create a directory called ``my-custom-chart`` with the following structure:

.. code-block:: none

   my-custom-chart/
   ├── .helmignore
   ├── Chart.yaml
   ├── values.yaml
   ├── charts/
   └── templates/
       └── tests/

Add Airflow Helm Chart as dependency
------------------------------------

Second, you will need to add the Airflow chart as dependency to the custom chart.
This will give you the ability to add your custom templates without the need to modify the Airflow chart itself.
In order to add the Airflow chart as a dependency (often called ``subcharts``) to your chart,
add the following lines to your ``Chart.yaml`` file:

.. jinja:: global_ctx

   .. code-block:: yaml
      :caption: Chart.yaml

      dependencies:
        - name: airflow
          version: {{ package_version }}
          repository: https://airflow.apache.org

.. note::

   Make sure that you have already added the Airflow repo locally by running: ``helm repo add apache-airflow https://airflow.apache.org``.

.. tip::

   You can also use the name of the repo instead of the URL by replacing
   ``https://airflow.apache.org`` with ``"@apache-airflow"``.

Adding the Airflow chart as a dependency means that it will be deployed together with your custom chart.
You can disable the installation of Airflow by adding the ``condition`` field to the ``dependencies`` section
like in the example below:

.. jinja:: global_ctx

   .. code-block:: yaml
      :caption: Chart.yaml

      dependencies:
        - name: airflow
          version: {{ package_version }}
          repository: https://airflow.apache.org
          condition: airflow.enabled

This will check if the value of ``airflow.enabled`` inside your ``values.yaml`` is ``true``.
If it is, the Airflow chart will be deployed together with your custom chart.
Otherwise, only your templates will be deployed.

Download the Airflow Helm Chart
-------------------------------

Third, after you have specified the Airflow chart inside the ``dependencies`` section in ``Chart.yaml`` file,
you can download it by running the following command:

.. code-block:: bash

   helm dependency build

.. note::

   Make sure that you are inside the directory which contains the ``Chart.yaml`` file.

The chart will be downloaded and saved inside the ``charts/`` directory.

Overriding default values
-------------------------

When you add a chart as a subchart to your chart,
you have the ability to override the default values of the subchart in your ``values.yaml``.
This is useful when your chart needs a specific configuration for your custom chart.
E.g. if you want that the Airflow chart be installed with the ``KubernetesExecutor``,
you can do it by adding the following section to your ``values.yaml``:

.. code-block:: yaml
   :caption: values.yaml

   airflow:
     executor: KubernetesExecutor

Deploying extra Kubernetes objects
----------------------------------

Creating a custom chart is the right approach when the extra templates are a project of their own.
For a small number of resources the chart does not model, ``extraObjects`` is a lighter alternative:
every item of that list is rendered as an additional manifest by the Airflow chart itself,
so no umbrella chart, no ``helm dependency build`` and no second release are needed.

Each item is either a mapping holding a full manifest, or a string holding a rendered manifest.
Both forms go through ``tpl``, so the chart values, ``.Release`` and ``.Chart`` are available:

.. code-block:: yaml
   :caption: values.yaml

   extraObjects:
     - apiVersion: networking.k8s.io/v1
       kind: NetworkPolicy
       metadata:
         name: '{{ .Release.Name }}-deny-egress'
       spec:
         podSelector:
           matchLabels:
             release: '{{ .Release.Name }}'
         policyTypes:
           - Egress
     - |
       apiVersion: v1
       kind: ConfigMap
       metadata:
         name: {{ .Release.Name }}-extra-config
       data:
         MY_KEY: "my_value"

The manifests are passed through as they are written, which means the chart adds no labels,
no annotations and no Helm hooks to them, and it applies no validation beyond what the API server does.
Objects created this way share the lifecycle of the release, so they are removed on ``helm uninstall``.

.. note::

   Use the dedicated values where they exist. Secrets and ConfigMaps consumed by Airflow containers
   belong in :ref:`extraSecrets and extraConfigMaps <parameters:Kubernetes>`, which do add the chart
   labels and Helm hooks, and additional containers belong in the ``extra*Containers`` values.
