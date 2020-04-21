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

Google Cloud BigQuery Data Transfer Service Operators
=====================================================

The `BigQuery Data Transfer Service <https://cloud.google.com/bigquery/transfer/>`__
automates data movement from SaaS applications to Google BigQuery on a scheduled, managed basis.
Your analytics team can lay the foundation for a data warehouse without writing a single line of code.
BigQuery Data Transfer Service initially supports Google application sources like Google Ads,
Campaign Manager, Google Ad Manager and YouTube. Through BigQuery Data Transfer Service, users also
gain access to data connectors that allow you to easily transfer data from Teradata and Amazon S3 to BigQuery.


.. contents::
  :depth: 1
  :local:

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: _partials/prerequisite_tasks.rst


.. _howto/operator:BigQueryDTSDocuments:

.. _howto/operator:BigQueryCreateDataTransferOperator:

Creating transfer configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To create DTS transfer configuration you can use
:class:`~airflow.providers.google.cloud.operators.bigquery_dts.BigQueryCreateDataTransferOperator`.

In the case of Airflow, the customer needs to create a transfer config with the automatic scheduling disabled
and then trigger a transfer run using a specialized Airflow operator that will call StartManualTransferRuns API
for example :class:`~airflow.providers.google.cloud.operators.bigquery_dts.BigQueryDataTransferServiceStartTransferRunsOperator`.
:class:`~airflow.providers.google.cloud.operators.bigquery_dts.BigQueryCreateDataTransferOperator` checks if automatic
scheduling option is present in passed configuration. If present then nothing is done, otherwise it's value is
set to ``True``.

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_bigquery_dts.py
    :language: python
    :start-after: [START howto_bigquery_dts_create_args]
    :end-before: [END howto_bigquery_dts_create_args]

You can create the operator with or without project id. If project id is missing
it will be retrieved from the GCP connection used. Basic usage of the operator:

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_bigquery_dts.py
    :language: python
    :dedent: 4
    :start-after: [START howto_bigquery_create_data_transfer]
    :end-before: [END howto_bigquery_create_data_transfer]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.bigquery_dts.BigQueryCreateDataTransferOperator`
parameters which allows you to dynamically determine values. The result is saved to :ref:`XCom <concepts:xcom>`,
which allows it to be used by other operators. Additionally, id of the new config is accessible in
:ref:`XCom <concepts:xcom>` under ``transfer_config_id`` key.


.. _howto/operator:BigQueryDeleteDataTransferConfigOperator:

Deleting transfer configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To delete DTS transfer configuration you can use
:class:`~airflow.providers.google.cloud.operators.bigquery_dts.BigQueryDeleteDataTransferConfigOperator`.

Basic usage of the operator:

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_bigquery_dts.py
    :language: python
    :dedent: 4
    :start-after: [START howto_bigquery_delete_data_transfer]
    :end-before: [END howto_bigquery_delete_data_transfer]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.bigquery_dts.BigQueryCreateDataTransferOperator`
parameters which allows you to dynamically determine values.

.. _howto/operator:BigQueryDataTransferServiceStartTransferRunsOperator:
.. _howto/operator:BigQueryDataTransferServiceTransferRunSensor:

Manually starting transfer runs
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Start manual transfer runs to be executed now with schedule_time equal to current time.
:class:`~airflow.providers.google.cloud.operators.bigquery_dts.BigQueryDataTransferServiceStartTransferRunsOperator`.

Basic usage of the operator:

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_bigquery_dts.py
    :language: python
    :dedent: 4
    :start-after: [START howto_bigquery_start_transfer]
    :end-before: [END howto_bigquery_start_transfer]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.operators.bigquery_dts.BigQueryDataTransferServiceStartTransferRunsOperator`
parameters which allows you to dynamically determine values.

To check if operation succeeded you can use
:class:`~airflow.providers.google.cloud.sensors.bigquery_dts.BigQueryDataTransferServiceTransferRunSensor`.

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_bigquery_dts.py
    :language: python
    :dedent: 4
    :start-after: [START howto_bigquery_dts_sensor]
    :end-before: [END howto_bigquery_dts_sensor]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.sensors.bigquery_dts.BigQueryDataTransferServiceTransferRunSensor`
parameters which allows you to dynamically determine values.

Reference
^^^^^^^^^

For further information, look at:

* `Client Library Documentation <https://googleapis.github.io/google-cloud-python/latest/bigquery_datatransfer/index.html>`__
* `Product Documentation <https://cloud.google.com/bigquery/transfer/>`__
