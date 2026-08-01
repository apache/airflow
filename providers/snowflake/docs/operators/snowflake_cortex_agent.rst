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

Authentication
^^^^^^^^^^^^^^

``SnowflakeCortexAgentHook`` (and the operator built on it) authenticate according to the connection's
``authenticator`` extra:

- **OAuth**: set ``authenticator`` to ``oauth`` and configure a refresh token, a client
  credentials grant, or ``azure_conn_id``, as on the Snowflake connection generally.
- **PAT (Programmatic Access Token)**: set ``authenticator`` to ``programmatic_access_token`` and
  put the PAT value in the connection's Password field.
- **Key-pair JWT**: the default when neither of the above is set. Configure
  ``private_key_file`` or ``private_key_content`` (optionally with a passphrase in Password), and
  set Login to the Snowflake user name the key is registered to.

See the :doc:`Snowflake connection </connections/snowflake>` page for the full field reference.
A hook instance keeps its resolved key-pair JWT and reuses it within its renewal window instead of
signing a new one on every request. This benefits code that calls ``SnowflakeCortexAgentHook``
several times on the same instance; the operator makes one request per ``execute`` call.
.. _howto/operator:SnowflakeCortexAgentCreateOperator:

SnowflakeCortexAgentCreateOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To create a Snowflake Cortex Agent you can use
:class:`~airflow.providers.snowflake.operators.snowflake_cortex_agent.SnowflakeCortexAgentCreateOperator`.

.. exampleinclude:: /../../snowflake/tests/system/snowflake/example_snowflake_cortex_agent.py
    :language: python
    :start-after: [START howto_operator_snowflake_cortex_agent_create]
    :end-before: [END howto_operator_snowflake_cortex_agent_create]
    :dedent: 4

.. _howto/operator:SnowflakeCortexAgentUpdateOperator:

SnowflakeCortexAgentUpdateOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To update an existing Snowflake Cortex Agent you can use
:class:`~airflow.providers.snowflake.operators.snowflake_cortex_agent.SnowflakeCortexAgentUpdateOperator`.

.. exampleinclude:: /../../snowflake/tests/system/snowflake/example_snowflake_cortex_agent.py
    :language: python
    :start-after: [START howto_operator_snowflake_cortex_agent_update]
    :end-before: [END howto_operator_snowflake_cortex_agent_update]
    :dedent: 4

.. _howto/operator:SnowflakeCortexAgentDeleteOperator:

SnowflakeCortexAgentDeleteOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To delete a Snowflake Cortex Agent you can use
:class:`~airflow.providers.snowflake.operators.snowflake_cortex_agent.SnowflakeCortexAgentDeleteOperator`.

.. exampleinclude:: /../../snowflake/tests/system/snowflake/example_snowflake_cortex_agent.py
    :language: python
    :start-after: [START howto_operator_snowflake_cortex_agent_delete]
    :end-before: [END howto_operator_snowflake_cortex_agent_delete]
    :dedent: 4
