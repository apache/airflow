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



.. _howto/operator:SnowflakeOperator:

SnowflakeOperator
=================

Use the :class:`SnowflakeOperator <airflow.providers.snowflake.operators.snowflake>` to execute
SQL commands in a `Snowflake <https://docs.snowflake.com/en/>`__ database.


Using the Operator
^^^^^^^^^^^^^^^^^^

Use the ``snowflake_conn_id`` argument to connect to your Snowflake instance where
the connection metadata is structured as follows:

.. list-table:: Snowflake Airflow Connection Metadata
   :widths: 25 25
   :header-rows: 1

   * - Parameter
     - Input
   * - Login: string
     - Snowflake user name
   * - Password: string
     - Password for Snowflake user
   * - Schema: string
     - Set schema to execute SQL operations on by default
   * - Extra: dictionary
     - ``warehouse``, ``account``, ``database``, ``region``, ``role``, ``authenticator``

An example usage of the SnowflakeOperator is as follows:

.. exampleinclude:: /../airflow/providers/snowflake/example_dags/example_snowflake.py
    :language: python
    :start-after: [START howto_operator_snowflake]
    :end-before: [END howto_operator_snowflake]

.. note::

  Parameters that can be passed onto the operator will be given priority over the parameters already given
  in the Airflow connection metadata (such as ``schema``, ``role``, ``database`` and so forth).


.. _howto/operator:S3ToSnowflakeOperator:

S3ToSnowflakeOperator
=====================

Use the :class:`S3ToSnowflakeOperator <airflow.providers.snowflake.transfers.s3_to_snowflake>` to load data stored in `AWS S3 <https://aws.amazon.com/s3/>`__
to a Snowflake table.


Using the Operator
^^^^^^^^^^^^^^^^^^

Similarly to the :class:`SnowflakeOperator <airflow.providers.snowflake.operators.snowflake>`, use the ``snowflake_conn_id`` and
the additional relevant parameters to establish connection with your Snowflake instance.
This operator will allow loading of one or more named files from a specific Snowflake stage (predefined S3 path). In order to do so
pass the relevant file names to the ``s3_keys`` parameter and the relevant Snowflake stage to the ``stage`` parameter.
``file_format`` can be used to either reference an already existing Snowflake file format or a custom string that defines
a file format (see `docs <https://docs.snowflake.com/en/sql-reference/sql/create-file-format.html>`__).

An example usage of the S3ToSnowflakeOperator is as follows: #TODO: currently forces usage of schema parameter

.. exampleinclude:: /../airflow/providers/snowflake/example_dags/example_snowflake.py
    :language: python
    :start-after: [START howto_operator_s3_to_snowflake]
    :end-before: [END howto_operator_s3_to_snowflake]


.. _howto/operator:SnowflakeToSlackOperator:

SnowflakeToSlackOperator
========================

Use the :class:`SnowflakeToSlackOperator <airflow.providers.snowflake.transfers.snowflake_to_slack>` to post messages to predefined Slack
channels.

.. list-table:: Slack Webhook Airflow Connection Metadata
   :widths: 25 25
   :header-rows: 1

   * - Parameter
     - Input
   * - Host: string
     - Slack Webhook URL
   * - Extra: dictionary
     - ``webhook_token`` (optional)



Using the Operator
^^^^^^^^^^^^^^^^^^

Similarly to the :class:`SnowflakeOperator <airflow.providers.snowflake.operators.snowflake>`, use the ``snowflake_conn_id`` and
the additional relevant parameters to establish connection with your Snowflake instance.
This operator will execute a custom query on a selected Snowflake table and publish a Slack message that can be formatted
and contain the resulting dataset (e.g. ASCII formatted dataframe).

An example usage of the SnowflakeToSlackOperator is as follows:

.. exampleinclude:: /../airflow/providers/snowflake/example_dags/example_snowflake.py
    :language: python
    :start-after: [START howto_operator_snowflake_to_slack]
    :end-before: [END howto_operator_snowflake_to_slack]
