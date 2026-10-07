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

Databricks Genie Integration
============================

Databricks Genie is an AI/BI conversational assistant powered by Unity Catalog metadata, schemas,
and analytical models. A Genie Space (`space_id`) enables natural-language data querying, automatic
SQL generation, execution against SQL warehouses, and data visualization.

The Databricks provider integrates with Databricks Genie via :class:`~airflow.providers.databricks.hooks.genie.DatabricksGenieHook`.

Unity Gateway MCP vs. Native Genie API
--------------------------------------

* **Unity Gateway MCP (Model Context Protocol)**:
  Exposes discrete Unity Catalog functions and tools as callable endpoints. Use MCP when an Airflow-managed LLM (e.g. via ``pydantic-ai``) needs to execute individual tools or run raw SQL queries under its own reasoning loop.
* **Native Genie API**:
  Integrates directly with a managed Databricks Genie Space that retains stateful multi-turn conversations, domain-specific semantic instructions, curated SQL queries, and benchmark questions. Use Genie when delegating high-level analytical questions to Databricks' autonomous data reasoning agent.

Using DatabricksGenieHook Directly
----------------------------------

You can interact directly with Genie spaces using :class:`~airflow.providers.databricks.hooks.genie.DatabricksGenieHook`:

.. code-block:: python

    from airflow.providers.databricks.hooks.genie import DatabricksGenieHook

    hook = DatabricksGenieHook(databricks_conn_id="databricks_default")
    space_id = "01ef8392-4f3b-1234-9abc-1234567890ab"

    # Start a new conversation
    message = hook.start_conversation(space_id=space_id, content="What was total revenue last quarter?")
    conversation_id = message["conversation_id"]
    message_id = message["id"]

    # Poll until completed
    completed_msg = hook.wait_for_message(
        space_id=space_id,
        conversation_id=conversation_id,
        message_id=message_id,
        timeout=300,
    )

    print("Genie answer:", completed_msg.get("content"))

Using Databricks Genie with Common AI Managed Agents
----------------------------------------------------

With the ``common.ai`` extra installed (``pip install 'apache-airflow-providers-databricks[common.ai]'``),
:class:`~airflow.providers.databricks.hooks.genie.DatabricksGenieHook` adopts the
:class:`~airflow.providers.common.ai.managed_agents.base.BaseManagedAgentHook` contract.

This allows Airflow AI agents to consult a Genie space via :class:`~airflow.providers.common.ai.toolsets.ManagedAgentToolset`:

.. code-block:: python

    from airflow.providers.common.ai.toolsets import ManagedAgentToolset
    from airflow.providers.databricks.hooks.genie import DatabricksGenieHook

    hook = DatabricksGenieHook(databricks_conn_id="databricks_default")
    genie_space = hook.agent("01ef8392-4f3b-1234-9abc-1234567890ab")

    toolset = ManagedAgentToolset(
        genie_space,
        tool_name="consult_sales_genie",
        description="Consults Databricks Genie for sales, revenue, and pipeline analytics.",
    )
