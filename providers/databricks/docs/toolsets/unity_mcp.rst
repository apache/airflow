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

Unity Gateway MCP Services
==========================

Use :class:`~airflow.providers.databricks.toolsets.unity_mcp.DatabricksUnityMCPToolset` to give an
agent the tools of a Unity Gateway MCP Service: either one Databricks provides for workspace tools
and SaaS applications, such as ``system.ai.google_calendar``, or an external MCP server registered as
an MCP Service in Unity Catalog. The toolset works with
:class:`~airflow.providers.common.ai.operators.agent.AgentOperator`, ``@task.agent``, and the LangChain
bridge of the Common AI provider.

The Dag names the service by its three-level Unity Catalog name, ``catalog.schema.service``, and the
:ref:`Databricks connection <howto/connection:databricks>` to use. The toolset builds the service URL,
``https://<workspace host>/ai-gateway/mcp-services/<catalog.schema.service>``, from the connection's
host, so neither the gateway URL nor a token appears in Dag code, and the connection's token is only
sent to that workspace, over HTTPS. A connection whose ``schema`` is ``http`` is rejected unless its
host is a loopback address. Requests go through the proxy in the connection's ``proxies`` extra, if
set. Service names may contain only ASCII letters, digits and ``_`` in each part; any other name is
rejected before a request is made.

.. exampleinclude:: /../../databricks/tests/system/databricks/example_databricks_unity_mcp.py
    :language: python
    :start-after: [START howto_toolset_databricks_unity_mcp]
    :end-before: [END howto_toolset_databricks_unity_mcp]

The service name and connection ID are templated when the toolset is passed to ``AgentOperator`` or
``@task.agent``.

Caller identity and authentication
----------------------------------

MCP Services need a workspace enabled for Unity Catalog, in a region where Model Serving is
supported.

The gateway runs every tool call as the identity of the connection's credentials. That identity needs
``EXECUTE`` on the MCP Service, ``USE CATALOG`` and ``USE SCHEMA`` on its parent catalog and schema
(``EXECUTE`` alone is not enough), and an assignment to the workspace. It needs no privilege on the
Unity Catalog connection behind the service. On the built-in ``system.ai`` services, account users
hold these privileges by default (see `MCP Services
<https://docs.databricks.com/aws/en/agents/mcp-tools/mcp-services>`__). The gateway exposes only the
tools selected for the service, and the service's policies apply. Grant the identity only the services
the agent should use.

Built-in services that act on a user's own data, such as ``system.ai.google_calendar`` or
``system.ai.gmail``, need that identity to complete a one-time OAuth login first, for example by
opening the service in Catalog Explorer and clicking **Login**.

The toolset sends the connection's token as a bearer token, so the connection must use one of these
authentication modes of the Databricks connection:

* a personal access token (the identity is the token's user or service principal);
* service principal OAuth (``service_principal_oauth``);
* Azure AD: a service principal, a managed identity, or ``DefaultAzureCredential``;
* workload identity federation (Kubernetes, AWS IAM, or a supplied token provider).

For service principal OAuth, the OAuth secret must allow the ``all-apis`` scope, which the connection
requests; a secret restricted to narrower scopes fails (see `OAuth for service principals
<https://docs.databricks.com/aws/en/dev-tools/auth/oauth-m2m>`__). Username and password
authentication is not supported. A token is fetched from the connection for
every request, so OAuth and Azure AD tokens are refreshed during a long agent run and when the toolset
reconnects. Tokens minted for the toolset are masked in task logs.

Errors and retries
------------------

When the MCP server answers a tool call with an error, such as invalid arguments, the error goes to the
model, which can correct the call and try again.

When the gateway gives no answer of its own, a tool call that was already sent may or may not have
run, and repeating a tool that changes data could apply the change twice. The toolset never retries
it: the task fails with one of these exceptions from :mod:`airflow.providers.databricks.exceptions`,
which are also raised for failures while connecting and listing tools:

.. list-table::
    :header-rows: 1

    * - Exception
      - Cause
    * - ``DatabricksUnityMCPAccessDeniedError``
      - The gateway will not let the identity invoke the service: no service has that name, the
        identity lacks ``EXECUTE`` on the service or ``USE CATALOG`` / ``USE SCHEMA`` on its parents,
        or the credentials are invalid. A missing service is reported like a missing privilege.
    * - ``DatabricksUnityMCPThrottledError``
      - HTTP 429. Its ``retry_after`` attribute holds the ``Retry-After`` delay when the gateway gives
        it as a number of seconds.
    * - ``DatabricksUnityMCPTransportError``
      - The gateway could not be reached, the connection dropped, a request got no answer in time,
        or the MCP handshake did not finish within ``init_timeout``.
    * - ``DatabricksUnityMCPError``
      - Any other gateway error, or a failure to get a token from the connection.

A connection that cannot be used at all, such as one without a host or without token-based
authentication, raises ``ValueError`` when the toolset connects.

Unity Gateway forwards a response only once the MCP server has finished sending it, so connecting
to a server that keeps its response stream open takes as long as the server keeps it open. The toolset
waits ``init_timeout`` seconds (30 by default) for the handshake; raise it for slow services.

The exceptions carry the HTTP status in ``http_status_code`` when it is known. When the agent runs
several tool calls of the toolset at once, the status of a failed call cannot be told apart from the
others', so a call that fails with HTTP 401, 403 or 429 is reported as ``DatabricksUnityMCPError``
without it. Transport errors, timeouts and a denial the gateway reports in its JSON-RPC answer keep
their class. Use task retries for calls that are safe to repeat.

These exception classes and their attributes reach the task in Pydantic AI runs. Through the
LangChain bridge, ``airflow_toolset_to_langchain_tools``, a failed tool call raises
``ToolCallError`` instead, whose message names the exception class and repeats its message;
``http_status_code`` and ``retry_after`` are not carried over.
