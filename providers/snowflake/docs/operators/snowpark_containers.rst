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

.. _howto/operator:SnowparkContainerJobOperator:

SnowparkContainerJobOperator
============================

Use the :class:`SnowparkContainerJobOperator
<airflow.providers.snowflake.operators.snowpark_containers.SnowparkContainerJobOperator>` to
submit and monitor container jobs on
`Snowpark Container Services <https://docs.snowflake.com/en/developer-guide/snowpark-container-services/overview>`__.

The operator wraps the Snowflake
`EXECUTE JOB SERVICE <https://docs.snowflake.com/en/sql-reference/sql/execute-job-service>`__
SQL command. It submits the job asynchronously, optionally polls until the job
reaches a terminal state, retrieves container logs, and can drop the service on
completion.

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

To use this operator, you must do a few things:

  * Install the provider package via **pip**.

    .. code-block:: bash

      pip install 'apache-airflow-providers-snowflake'

    Detailed information is available for :doc:`Installation <apache-airflow:installation/index>`.

  * :doc:`Setup a Snowflake Connection </connections/snowflake>`.

  * Create the necessary resources in your Snowflake account. See the
    `Snowpark Container Services documentation <https://docs.snowflake.com/en/developer-guide/snowpark-container-services/overview>`__.

Using the Operator
^^^^^^^^^^^^^^^^^^

Use the ``snowflake_conn_id`` argument to specify the connection used. If not
specified, ``snowflake_default`` will be used.

An example usage of the SnowparkContainerJobOperator is as follows:

.. exampleinclude:: /../../snowflake/tests/system/snowflake/example_snowpark_container_job.py
    :language: python
    :start-after: [START howto_operator_snowpark_container_job]
    :end-before: [END howto_operator_snowpark_container_job]
    :dedent: 4

.. note::

  Parameters that can be passed onto the operator will be given priority over the parameters already given
  in the Airflow connection metadata (such as ``schema``, ``role``, ``database`` and so forth).
