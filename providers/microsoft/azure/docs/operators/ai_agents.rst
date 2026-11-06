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


Azure AI Foundry Hosted Agents Operators
========================================

Azure AI Foundry Hosted agents let you run a containerized agent in Microsoft Foundry Agent Service.
Build and push your agent image to Azure Container Registry first, then use these operators to create
agent versions, invoke the hosted endpoint, and clean up the deployment.

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: /operators/_partials/prerequisite_tasks.rst

The operators use the ``azure_ai_agents_default`` connection by default. Configure the Azure AI Foundry
project endpoint in the connection host field or in the ``endpoint`` connection extra. The endpoint
format is:

.. code-block:: text

    https://<aiservices-id>.services.ai.azure.com/api/projects/<project-name>

The container image must be available in Azure Container Registry and must implement one of the
Hosted agent protocols exposed by Microsoft Foundry, such as ``responses`` or ``invocations``.

.. _howto/operator:CreateAzureAIAgentOperator:

CreateAzureAIAgentOperator
--------------------------

To create an Azure AI Hosted agent from a container image, use the
:class:`~airflow.providers.microsoft.azure.operators.ai_agents.CreateAzureAIAgentOperator`.
The operator returns the created Hosted agent version as a serializable dictionary.
Optional agent fields such as ``metadata``, ``description``, and ``blueprint_reference``
can be passed through to the Foundry API.

.. exampleinclude:: /../tests/system/microsoft/azure/example_azure_ai_agents.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_azure_ai_agent_create]
    :end-before: [END howto_operator_azure_ai_agent_create]

.. _howto/operator:UpdateAzureAIAgentOperator:

UpdateAzureAIAgentOperator
--------------------------

Azure AI Hosted agent updates are published as new immutable versions. To create a new version, use the
:class:`~airflow.providers.microsoft.azure.operators.ai_agents.UpdateAzureAIAgentOperator`.
The operator accepts the same parameters as ``CreateAzureAIAgentOperator`` and returns the newly
created Hosted agent version as a serializable dictionary.

.. exampleinclude:: /../tests/system/microsoft/azure/example_azure_ai_agents.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_azure_ai_agent_update]
    :end-before: [END howto_operator_azure_ai_agent_update]

Set ``deferrable=True`` to release the worker slot while the new version deploys; the wait then
runs in the triggerer. The same parameter is available on ``CreateAzureAIAgentOperator``.

.. exampleinclude:: /../tests/system/microsoft/azure/example_azure_ai_agents.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_azure_ai_agent_update_deferrable]
    :end-before: [END howto_operator_azure_ai_agent_update_deferrable]

.. _howto/operator:RunAzureAIAgentOperator:

RunAzureAIAgentOperator
-----------------------

To invoke an Azure AI Hosted agent, use the
:class:`~airflow.providers.microsoft.azure.operators.ai_agents.RunAzureAIAgentOperator`.
The ``responses`` protocol sends the payload to the agent's OpenAI-compatible endpoint, and the
``invocations`` protocol posts it to the agent's invocations endpoint.
Pass ``agent_session_id`` to reuse an existing Hosted agent session with the ``invocations``
protocol and ``user_isolation_key`` to scope endpoint resources to a specific end user.
The operator returns the JSON-compatible protocol response.

.. exampleinclude:: /../tests/system/microsoft/azure/example_azure_ai_agents.py
    :language: python
    :dedent: 8
    :start-after: [START howto_operator_azure_ai_agent_run]
    :end-before: [END howto_operator_azure_ai_agent_run]

.. _howto/operator:DeleteAzureAIAgentOperator:

DeleteAzureAIAgentOperator
--------------------------

To delete an Azure AI Hosted agent and all of its versions, use the
:class:`~airflow.providers.microsoft.azure.operators.ai_agents.DeleteAzureAIAgentOperator`.
Pass ``agent_version`` to delete only one version. Set ``force=True`` to also delete active sessions
associated with the agent or version.
The operator returns the serialized deletion response.

.. exampleinclude:: /../tests/system/microsoft/azure/example_azure_ai_agents.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_azure_ai_agent_delete]
    :end-before: [END howto_operator_azure_ai_agent_delete]

Reference
---------

For further information, look at:

* `Deploy an Azure AI Hosted agent <https://learn.microsoft.com/en-us/azure/foundry/agents/how-to/deploy-hosted-agent>`__
* `Microsoft Foundry Agent Service documentation <https://learn.microsoft.com/en-us/azure/foundry/agents/overview>`__
* `Microsoft Foundry Agents REST API reference <https://ai.azure.com/api-reference/agents/>`__
* `Azure AI Projects Python SDK reference <https://learn.microsoft.com/en-us/python/api/azure-ai-projects/azure.ai.projects.aiprojectclient?view=azure-python-preview>`__
