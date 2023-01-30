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

.. _howto/operator:SnowflakeToSlackOperator:

SnowflakeToSlackOperator
========================

Use the :class:`~airflow.providers.snowflake.transfers.snowflake_to_slack.snowflake_to_slack` to post messages to predefined Slack
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

Similarly to the :class:`~airflow.providers.snowflake.operators.snowflake.SnowflakeOperator`, use the ``snowflake_conn_id`` and
the additional relevant parameters to establish connection with your Snowflake instance.
This operator will execute a custom query on a selected Snowflake table and publish a Slack message that can be formatted
and contain the resulting dataset (e.g. ASCII formatted dataframe).

An example usage of the SnowflakeToSlackOperator is as follows:

.. exampleinclude:: /../../tests/system/providers/snowflake/example_snowflake_to_slack.py
    :language: python
    :start-after: [START howto_operator_snowflake_to_slack]
    :end-before: [END howto_operator_snowflake_to_slack]
    :dedent: 4
