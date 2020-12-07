

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

Google DataFusion Operators
=======================================

Cloud Data Fusion is a fully managed, cloud-native data integration service that helps
users efficiently build and manage ETL/ELT data pipelines. With a graphical interface
and a broad open source library of preconfigured connectors and transformations, Cloud
Data Fusion shifts an organization’s focus away from code and integration to insights
and action.

.. contents::
  :depth: 1
  :local:

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include::/operators/_partials/prerequisite_tasks.rst


.. _howto/operator:CloudDataFusionRestartInstanceOperator:

Restart DataFusion Instance
^^^^^^^^^^^^^^^^^^^^^^^^^^^

To restart Data Fusion instance use:
:class:`~airflow.providers.google.cloud.operators.datafusion.CloudDataFusionRestartInstanceOperator`.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_datafusion.py
    :language: python
    :dedent: 4
    :start-after: [START howto_cloud_data_fusion_restart_instance_operator]
    :end-before: [END howto_cloud_data_fusion_restart_instance_operator]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datafusion.CloudDataFusionRestartInstanceOperator`
parameters which allows you to dynamically determine values.
The result is saved to :ref:`XCom <concepts:xcom>`, which allows it to be used by other operators.

.. _howto/operator:CloudDataFusionDeleteInstanceOperator:

Delete DataFusion Instance
^^^^^^^^^^^^^^^^^^^^^^^^^^

To delete Data Fusion instance use:
:class:`~airflow.providers.google.cloud.operators.datafusion.CloudDataFusionDeleteInstanceOperator`.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_datafusion.py
    :language: python
    :dedent: 4
    :start-after: [START howto_cloud_data_fusion_delete_instance_operator]
    :end-before: [END howto_cloud_data_fusion_delete_instance_operator]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datafusion.CloudDataFusionDeleteInstanceOperator`
parameters which allows you to dynamically determine values.
The result is saved to :ref:`XCom <concepts:xcom>`, which allows it to be used by other operators.


.. _howto/operator:CloudDataFusionCreateInstanceOperator:

Create DataFusion Instance
^^^^^^^^^^^^^^^^^^^^^^^^^^

To create Data Fusion instance use:
:class:`~airflow.providers.google.cloud.operators.datafusion.CloudDataFusionCreateInstanceOperator`.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_datafusion.py
    :language: python
    :dedent: 4
    :start-after: [START howto_cloud_data_fusion_create_instance_operator]
    :end-before: [END howto_cloud_data_fusion_create_instance_operator]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datafusion.CloudDataFusionCreateInstanceOperator`
parameters which allows you to dynamically determine values.
The result is saved to :ref:`XCom <concepts:xcom>`, which allows it to be used by other operators.


.. _howto/operator:CloudDataFusionUpdateInstanceOperator:

Update DataFusion Instance
^^^^^^^^^^^^^^^^^^^^^^^^^^

To update Data Fusion instance use:
:class:`~airflow.providers.google.cloud.operators.datafusion.CloudDataFusionUpdateInstanceOperator`.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_datafusion.py
    :language: python
    :dedent: 4
    :start-after: [START howto_cloud_data_fusion_update_instance_operator]
    :end-before: [END howto_cloud_data_fusion_update_instance_operator]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datafusion.CloudDataFusionUpdateInstanceOperator`
parameters which allows you to dynamically determine values.
The result is saved to :ref:`XCom <concepts:xcom>`, which allows it to be used by other operators.

.. _howto/operator:CloudDataFusionGetInstanceOperator:

Get DataFusion Instance
^^^^^^^^^^^^^^^^^^^^^^^

To retrieve Data Fusion instance use:
:class:`~airflow.providers.google.cloud.operators.datafusion.CloudDataFusionGetInstanceOperator`.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_datafusion.py
    :language: python
    :dedent: 4
    :start-after: [START howto_cloud_data_fusion_get_instance_operator]
    :end-before: [END howto_cloud_data_fusion_get_instance_operator]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datafusion.CloudDataFusionGetInstanceOperator`
parameters which allows you to dynamically determine values.
The result is saved to :ref:`XCom <concepts:xcom>`, which allows it to be used by other operators.


.. _howto/operator:CloudDataFusionCreatePipelineOperator:

Create a DataFusion pipeline
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To create Data Fusion pipeline use:
:class:`~airflow.providers.google.cloud.operators.datafusion.CloudDataFusionCreatePipelineOperator`.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_datafusion.py
    :language: python
    :dedent: 4
    :start-after: [START howto_cloud_data_fusion_create_pipeline]
    :end-before: [END howto_cloud_data_fusion_create_pipeline]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datafusion.CloudDataFusionCreatePipelineOperator`
parameters which allows you to dynamically determine values.
The result is saved to :ref:`XCom <concepts:xcom>`, which allows it to be used by other operators.

.. _howto/operator:CloudDataFusionStartPipelineOperator:

Start a DataFusion pipeline
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To start Data Fusion pipeline use:
:class:`~airflow.providers.google.cloud.operators.datafusion.CloudDataFusionStartPipelineOperator`.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_datafusion.py
    :language: python
    :dedent: 4
    :start-after: [START howto_cloud_data_fusion_start_pipeline]
    :end-before: [END howto_cloud_data_fusion_start_pipeline]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datafusion.CloudDataFusionStartPipelineOperator`
parameters which allows you to dynamically determine values.
The result is saved to :ref:`XCom <concepts:xcom>`, which allows it to be used by other operators.

.. _howto/operator:CloudDataFusionStopPipelineOperator:

Stop a DataFusion pipeline
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To stop Data Fusion pipeline use:
:class:`~airflow.providers.google.cloud.operators.datafusion.CloudDataFusionStopPipelineOperator`.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_datafusion.py
    :language: python
    :dedent: 4
    :start-after: [START howto_cloud_data_fusion_stop_pipeline]
    :end-before: [END howto_cloud_data_fusion_stop_pipeline]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datafusion.CloudDataFusionStopPipelineOperator`
parameters which allows you to dynamically determine values.
The result is saved to :ref:`XCom <concepts:xcom>`, which allows it to be used by other operators.

.. _howto/operator:CloudDataFusionDeletePipelineOperator:

Delete a DataFusion pipeline
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To delete Data Fusion pipeline use:
:class:`~airflow.providers.google.cloud.operators.datafusion.CloudDataFusionDeletePipelineOperator`.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_datafusion.py
    :language: python
    :dedent: 4
    :start-after: [START howto_cloud_data_fusion_delete_pipeline]
    :end-before: [END howto_cloud_data_fusion_delete_pipeline]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datafusion.CloudDataFusionDeletePipelineOperator`
parameters which allows you to dynamically determine values.
The result is saved to :ref:`XCom <concepts:xcom>`, which allows it to be used by other operators.


.. _howto/operator:CloudDataFusionListPipelinesOperator:

List DataFusion pipelines
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To list Data Fusion pipelines use:
:class:`~airflow.providers.google.cloud.operators.datafusion.CloudDataFusionListPipelinesOperator`.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_datafusion.py
    :language: python
    :dedent: 4
    :start-after: [START howto_cloud_data_fusion_list_pipelines]
    :end-before: [END howto_cloud_data_fusion_list_pipelines]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.datafusion.CloudDataFusionListPipelinesOperator`
parameters which allows you to dynamically determine values.
The result is saved to :ref:`XCom <concepts:xcom>`, which allows it to be used by other operators.
