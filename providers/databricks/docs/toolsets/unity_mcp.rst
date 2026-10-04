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

.. _howto/toolset:DatabricksUnityMCPToolset:

Unity AI Gateway MCP Services
=============================

Use :class:`~airflow.providers.databricks.toolsets.unity_mcp.DatabricksUnityMCPToolset` to give an
agent the tools of a Unity AI Gateway MCP Service. One service can front Unity Catalog functions,
Genie spaces, AI Search indexes, or an external MCP server registered in Unity Catalog. The toolset
works with :class:`~airflow.providers.common.ai.operators.agent.AgentOperator`, ``@task.agent``, and
the LangChain bridge of the Common AI provider.

The Dag names the service by its three-level Unity Catalog name, ``catalog.schema.service``, and the
:ref:`Databricks connection <howto/connection:databricks>` to use. The toolset builds the service URL,
``https://<workspace host>/ai-gateway/mcp-services/<catalog.schema.service>``, from the connection's
host, so neither the gateway URL nor a token appears in Dag code, and the connection's token is only
sent to that workspace. Service names may contain only ASCII letters, digits, ``_`` and ``-`` in each
part; any other name is rejected before a request is made.

.. exampleinclude:: /../../databricks/tests/system/databricks/example_databricks_unity_mcp.py
    :language: python
    :start-after: [START howto_toolset_databricks_unity_mcp]
    :end-before: [END howto_toolset_databricks_unity_mcp]

The service name and connection ID are templated when the toolset is passed to ``AgentOperator`` or
``@task.agent``.

Caller identity and authentication
----------------------------------

The gateway runs every tool call as the identity of the connection's credentials, which needs
``EXECUTE`` on the MCP Service in Unity Catalog. It exposes only the tools selected for the service,
and the service's policies apply. Grant that identity only the services the agent should use.

The gateway accepts bearer tokens only, so the connection must use one of these authentication modes
of the Databricks connection:

* a personal access token (the identity is the token's user or service principal);
* service principal OAuth (``service_principal_oauth``);
* Azure AD: a service principal, a managed identity, or ``DefaultAzureCredential``;
* workload identity federation (Kubernetes, AWS IAM, or a supplied token provider).

Username and password authentication is not supported. A token is fetched from the connection for
every request, so OAuth and Azure AD tokens are refreshed during a long agent run and when the toolset
reconnects. Tokens minted for the toolset are masked in task logs.

Errors and retries
------------------

Gateway errors are raised as dedicated exceptions from
:mod:`airflow.providers.databricks.exceptions`:

.. list-table::
    :header-rows: 1

    * - Exception
      - Cause
    * - ``DatabricksUnityMCPAccessDeniedError``
      - HTTP 401 or 403: the credentials are invalid, or the identity lacks ``EXECUTE`` on the service.
    * - ``DatabricksUnityMCPServiceNotFoundError``
      - HTTP 404: the service does not exist on the workspace, or is not visible to the identity.
    * - ``DatabricksUnityMCPThrottledError``
      - HTTP 429. Its ``retry_after`` attribute holds the ``Retry-After`` delay in seconds, when given.
    * - ``DatabricksUnityMCPTransportError``
      - The gateway could not be reached, or the connection dropped.
    * - ``DatabricksUnityMCPError``
      - Any other gateway error, or a failure to get a token from the connection.

The toolset never retries a tool call. A call that fails after it was sent, with a server or transport
error, may or may not have run, and repeating a tool that changes data could apply the change twice;
the error message says so. Use task retries for calls that are safe to repeat.
