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


Azure AI Foundry Agents Operators
=================================

Azure AI Foundry Agents lets you create agents that can run against a thread of messages.

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: /operators/_partials/prerequisite_tasks.rst

The operators use the ``azure_ai_agents_default`` connection by default. Configure the Azure AI Foundry
project endpoint in the connection host field or in the ``endpoint`` connection extra. The endpoint
format is:

.. code-block:: text

    https://<aiservices-id>.services.ai.azure.com/api/projects/<project-name>

.. _howto/operator:CreateAzureAIAgentOperator:

CreateAzureAIAgentOperator
--------------------------

To create an Azure AI Agent, use the
:class:`~airflow.providers.microsoft.azure.operators.ai_agents.CreateAzureAIAgentOperator`.
The operator returns the created agent as a serializable dictionary.

.. exampleinclude:: /../tests/system/microsoft/azure/example_azure_ai_agents.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_azure_ai_agent_create]
    :end-before: [END howto_operator_azure_ai_agent_create]

.. _howto/operator:UpdateAzureAIAgentOperator:

UpdateAzureAIAgentOperator
--------------------------

To update an Azure AI Agent, use the
:class:`~airflow.providers.microsoft.azure.operators.ai_agents.UpdateAzureAIAgentOperator`.
The operator returns the updated agent as a serializable dictionary.

.. exampleinclude:: /../tests/system/microsoft/azure/example_azure_ai_agents.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_azure_ai_agent_update]
    :end-before: [END howto_operator_azure_ai_agent_update]

.. _howto/operator:RunAzureAIAgentOperator:

RunAzureAIAgentOperator
-----------------------

To create a thread and run an Azure AI Agent, use the
:class:`~airflow.providers.microsoft.azure.operators.ai_agents.RunAzureAIAgentOperator`.
The operator returns the run as a serializable dictionary. When ``wait_for_completion`` is
``True``, the returned run is in a terminal state. When ``wait_for_completion`` is ``False``,
the returned run is the initial run created by Azure AI Foundry Agents.

``RunAzureAIAgentOperator`` uses ``AgentsClient.create_thread_and_run``. It does not process
agent tool calls or submit tool outputs. Runs that enter ``requires_action`` fail with an
actionable error. Use agents that can complete without tool-output submission, or handle the
tool-call workflow outside this operator.

.. exampleinclude:: /../tests/system/microsoft/azure/example_azure_ai_agents.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_azure_ai_agent_run]
    :end-before: [END howto_operator_azure_ai_agent_run]

The operator can run in deferrable mode so that polling for run completion occurs on the Airflow
Triggerer. Deferrable mode only applies when ``wait_for_completion`` is ``True``. When
``wait_for_completion`` is ``False``, the operator returns the initial run immediately and does
not defer.

.. exampleinclude:: /../tests/system/microsoft/azure/example_azure_ai_agents.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_azure_ai_agent_run_deferrable]
    :end-before: [END howto_operator_azure_ai_agent_run_deferrable]

.. _howto/operator:DeleteAzureAIAgentOperator:

DeleteAzureAIAgentOperator
--------------------------

To delete an Azure AI Agent, use the
:class:`~airflow.providers.microsoft.azure.operators.ai_agents.DeleteAzureAIAgentOperator`.
The operator waits until the agent is no longer retrievable before completing.
The operator does not return a value.

.. exampleinclude:: /../tests/system/microsoft/azure/example_azure_ai_agents.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_azure_ai_agent_delete]
    :end-before: [END howto_operator_azure_ai_agent_delete]

Reference
---------

For further information, look at:

* `Azure AI Foundry Agents documentation <https://learn.microsoft.com/en-us/azure/ai-foundry/agents/>`__
* `AgentsClient API reference <https://learn.microsoft.com/en-us/python/api/azure-ai-agents/azure.ai.agents.agentsclient?view=azure-python>`__
