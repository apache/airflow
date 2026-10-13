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

:class:`~airflow.providers.common.ai.toolsets.logging.ToolLoggingCapability` wraps the
assembled function toolset so that each tool invocation is logged in real time.
``AgentOperator`` applies it automatically (see ``enable_tool_logging``).
Applying the wrapper as a capability means logging covers the assembled
function toolset, including tools supplied through ``toolsets=``,
``agent_params={"tools": [...]}``, and capabilities such as factory-backed
toolsets, nested capabilities, and MCP toolsets. Output tools such as
``final_result`` and provider-native tools, including native MCP, are not
covered by Airflow's real-time tool-call logging. Tools added by another
capability through its own wrapper toolset, such as ToolSearch's
``search_tools`` and CodeMode's ``run_code``, are also not covered.

``AgentOperator`` adds ``ToolLoggingCapability`` automatically when
``enable_tool_logging=True``. If ``capabilities=`` already contains one,
including inside a combined or wrapper capability, the operator does not add
another, so the logger on the supplied instance is used.

Each tool call logs its name and timing at INFO, inside a collapsible
``::group::`` block in the task log, and its arguments at DEBUG. Control-flow
signals that let the model retry, report a failed tool result, defer or skip a
call, or wait for approval are logged at INFO and re-raised. Other exceptions,
including ``SkipToolValidation`` raised by a tool body, are logged at ERROR
with a traceback and re-raised. Pass ``logger`` to ``ToolLoggingCapability`` to send the
lines to a logger other than the module's own.
