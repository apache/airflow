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

``SnowflakeCortexAgentHook`` (and the operator built on it) authenticate the connection's
``authenticator`` extra:

- **OAuth**: set ``authenticator`` to ``oauth`` and configure a refresh token, a client
  credentials grant, or ``azure_conn_id``, as on the Snowflake connection generally.
- **PAT (Programmatic Access Token)**: set ``authenticator`` to ``programmatic_access_token`` and
  put the PAT value in the connection's Password field.
- **Key-pair JWT**: the default when neither of the above is set. Configure
  ``private_key_file`` or ``private_key_content`` (optionally with a passphrase in Password).

See the :doc:`Snowflake connection </connections/snowflake>` page for the full field reference.
The resolved token is cached and renewed for as long as the hook instance lives, so a
long-running agent run reuses the same key-pair JWT within its renewal window instead of signing
a new one on every request.

.. _howto/hook:SnowflakeCortexManagedAgentHook:

Using a Cortex Agent from Common AI
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To let an agent running under Common AI's ``AgentOperator`` consult a Cortex Agent as one of its
tools, use
:class:`~airflow.providers.snowflake.hooks.snowflake_cortex_managed_agent.SnowflakeCortexManagedAgentHook`.
It implements the Common AI managed-agent contract, so ``hook.agent("DATABASE.SCHEMA.NAME")`` can
be passed to a ``ManagedAgentToolset`` or combined with agents on other clouds in a
``FailoverManagedAgentClient``. The hook needs the ``common.ai`` extra of this provider, which
installs ``apache-airflow-providers-common-ai`` and therefore requires Airflow 3.

The agent is ``DATABASE.SCHEMA.NAME`` (quoted identifiers containing their own ``.`` are not
supported). This adoption does not support sessions -- a Cortex thread needs a
``parent_message_id`` the contract has no field for -- so a request carrying ``session_id`` is
refused. ``vendor_options`` may carry ``tool_choice``, ``models``, ``instructions``,
``orchestration``, ``tools``, or ``tool_resources``, the optional payload fields ``run_agent``
accepts; anything else is rejected. A 4xx response (other than 408 or 429, which propagate for
Airflow's task-level retry to handle) is raised as a terminal
``ManagedAgentInvocationError``.

.. code-block:: python

    from airflow.providers.snowflake.hooks.snowflake_cortex_managed_agent import (
        SnowflakeCortexManagedAgentHook,
    )
    from airflow.providers.common.ai.toolsets import ManagedAgentToolset

    claims = SnowflakeCortexManagedAgentHook(snowflake_conn_id="snowflake_default").agent(
        "MY_DB.MY_SCHEMA.CLAIMS_AGENT"
    )
    toolset = ManagedAgentToolset(
        claims,
        tool_name="ask_claims_agent",
        description="Reviews an insurance claim and returns a coverage determination.",
    )
