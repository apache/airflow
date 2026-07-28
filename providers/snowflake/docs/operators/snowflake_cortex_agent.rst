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

.. _howto/operator:SnowflakeCortexAgentOperator:

SnowflakeCortexAgentOperator
============================

Use the :class:`~airflow.providers.snowflake.operators.snowflake_cortex_agent.SnowflakeCortexAgentOperator`
to execute `Snowflake Cortex Agents <https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents>`__.

The operator wraps the Snowflake Cortex Agent Run API and executes an existing
Cortex Agent. It returns the JSON response payload from the agent, allowing
responses to be consumed by downstream Airflow tasks through XCom.

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

To use this operator, you must do a few things:

  * Install the provider package via **pip**.

    .. code-block:: bash

      pip install 'apache-airflow-providers-snowflake'

    Detailed information is available for :doc:`Installation <apache-airflow:installation/index>`.

  * :doc:`Setup a Snowflake Connection </connections/snowflake>`.

  * Create a Snowflake Cortex Agent. See the
    `Snowflake Cortex Agents documentation <https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents>`__.

Using the Operator
^^^^^^^^^^^^^^^^^^

Use the ``snowflake_conn_id`` argument to specify the connection used. If not
specified, ``snowflake_default`` will be used.

An example usage of the ``SnowflakeCortexAgentOperator`` is as follows:

.. exampleinclude:: /../../snowflake/tests/system/snowflake/example_snowflake_cortex_agent.py
    :language: python
    :start-after: [START howto_operator_snowflake_cortex_agent]
    :end-before: [END howto_operator_snowflake_cortex_agent]
    :dedent: 4

.. note::

   Parameters passed to the operator take precedence over the corresponding
   values configured in the Airflow connection metadata, such as ``database``,
   ``schema`` and ``role``.
