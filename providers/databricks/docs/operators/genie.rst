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

.. _howto/databricks-genie:

Databricks Genie consultations
==============================

The ``DatabricksGenieHook`` submits a question to a Genie space, waits for the message to finish,
and returns a bounded result with the space, conversation, and message IDs. It uses the
credentials in an Airflow ``databricks`` connection and implements Common AI's public
``BaseManagedAgentHook`` contract.

Install the optional Common AI integration when using its managed-agent toolset:

.. code-block:: bash

    pip install 'apache-airflow-providers-databricks[common.ai]'

.. exampleinclude:: /../../databricks/tests/system/databricks/example_databricks_genie.py
    :language: python
    :dedent: 4
    :start-after: [START howto_hook_databricks_genie_toolset]
    :end-before: [END howto_hook_databricks_genie_toolset]

The hook can also be called directly without Common AI:

.. exampleinclude:: /../../databricks/tests/system/databricks/example_databricks_genie.py
    :language: python
    :dedent: 4
    :start-after: [START howto_hook_databricks_genie_consult]
    :end-before: [END howto_hook_databricks_genie_consult]

The result contains IDs and structured answer, generated query, and query-result fields. Large
responses are capped. A request that starts a conversation or posts a message is sent once; after
a timeout or server failure its outcome may be unknown, so inspect the conversation before
submitting again. Message and query-result reads may be retried. A 403 can mean either that access
was denied or that the resource is missing, because Genie may conceal missing resources this way.

Genie MCP through Unity Gateway is enough when an external model or MCP client should discover and
call Genie directly through the organization's governed MCP endpoint. The native Databricks API
integration is useful when an Airflow-managed agent needs Genie as a typed tool backed by an Airflow
Databricks connection, and when the caller needs the Genie space/conversation/message IDs and
bounded query results in its response. Both paths still enforce Genie and Unity Catalog permissions.

This hook handles consultations only. To ask Genie as a standalone Airflow task that reconnects to
its message after a worker failure, use
:class:`~airflow.providers.databricks.operators.genie.DatabricksGenieOperator`; see
:ref:`howto/operator:DatabricksGenieOperator`.
