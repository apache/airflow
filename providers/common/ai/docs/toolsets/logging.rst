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

Tool call logging
=================

.. note::

    Experimental: the constructor and the behaviour of
    :class:`~airflow.providers.common.ai.toolsets.logging.ToolLoggingCapability` can change
    or be removed in a minor release of this provider. See :ref:`howto/stability`.

:class:`~airflow.providers.common.ai.toolsets.logging.ToolLoggingCapability` wraps the
assembled function toolset so that each tool invocation is logged in real time.
Logging covers tools supplied through ``toolsets=``,
``agent_params={"tools": [...]}``, and capabilities such as factory-backed
toolsets, nested capabilities, and MCP toolsets. Output tools such as
``final_result`` and provider-native tools, including native MCP, are not
covered by Airflow's real-time tool-call logging. Tools added by another
capability through its own wrapper toolset, such as ToolSearch's
``search_tools`` and CodeMode's ``run_code``, are also not covered. A tool hidden with
``.defer_loading()`` is logged once the model finds it, and its result is masked like
any other (see :doc:`progressive_disclosure`).

``AgentOperator`` adds ``ToolLoggingCapability`` automatically when
``enable_tool_logging=True``. If ``capabilities=`` already contains one,
including inside a combined or wrapper capability, the operator does not add
another, so the logger on the supplied instance is used.

For an agent you build yourself, pass the capability to any pydantic-ai ``Agent``:

.. code-block:: python

    from pydantic_ai import Agent

    from airflow.providers.common.ai.toolsets.logging import ToolLoggingCapability
    from airflow.providers.common.ai.toolsets.sql import SQLToolset

    agent = Agent(
        "openai:gpt-5",
        toolsets=[SQLToolset(db_conn_id="my_db")],
        capabilities=[ToolLoggingCapability()],
    )

Each tool call logs its name and timing at INFO, inside a collapsible
``::group::`` block in the task log, and its arguments at DEBUG. Control-flow
signals that let the model retry, report a failed tool result, defer or skip a
call, or wait for approval are logged at INFO and re-raised. Other exceptions,
including ``SkipToolValidation`` raised by a tool body, are logged at ERROR
with a traceback and re-raised. Pass ``logger`` to ``ToolLoggingCapability`` to send the
lines to a logger other than the module's own.
