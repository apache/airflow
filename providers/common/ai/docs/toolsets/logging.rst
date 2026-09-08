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

Tool call logging: ``LoggingToolset``
=====================================

.. note::

    Experimental: this can change or be removed in a minor release of this provider.
    See :ref:`howto/stability`.

:class:`~airflow.providers.common.ai.toolsets.logging.LoggingToolset` is a
``WrapperToolset`` that intercepts ``call_tool()`` to log each tool invocation
in real time. ``AgentOperator`` applies it automatically (see
``enable_tool_logging``) through
:class:`~airflow.providers.common.ai.toolsets.logging.ToolLoggingCapability`.
Applying the wrapper as a capability means logging covers the complete toolset
that pydantic-ai assembles, including factory-backed toolsets, nested
capabilities, and MCP tools.

You can also use ``LoggingToolset`` directly with any pydantic-ai ``Agent``:

.. code-block:: python

    from airflow.providers.common.ai.toolsets.logging import LoggingToolset
    from airflow.providers.common.ai.toolsets.sql import SQLToolset

    logged_toolset = LoggingToolset(wrapped=SQLToolset(db_conn_id="my_db"))

Each call logs the tool's name and how long it took at INFO, inside a collapsible
``::group::`` block in the task log, and its arguments at DEBUG. A call that raises
is logged with its traceback and the exception is re-raised. Pass ``logger`` to
send the lines to a logger other than the toolset module's own.
