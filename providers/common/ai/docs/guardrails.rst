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

.. _capabilities-passthrough:

Guardrails and capabilities
===========================

pydantic-ai `capabilities <https://ai.pydantic.dev/capabilities/>`__ bundle
tools, lifecycle hooks, instructions, and model settings into composable units.
Common ones include ``Thinking`` (reasoning at a configurable effort level),
``WebSearch``, ``WebFetch``, ``ImageGeneration``, and ``MCP``.
For the current capability catalog and package-specific installation notes, see
the pydantic-ai documentation and the
`pydantic-ai-harness capability matrix <https://github.com/pydantic/pydantic-ai-harness#capability-matrix>`__.

``AgentOperator`` does not yet expose a first-class ``capabilities=`` kwarg,
but anything passed through ``agent_params`` is forwarded to the underlying
``Agent(...)`` constructor.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent_capabilities.py
    :language: python
    :start-after: [START howto_operator_agent_capabilities_thinking]
    :end-before: [END howto_operator_agent_capabilities_thinking]

Capabilities compose with toolsets -- pydantic-ai merges tools from both.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent_capabilities.py
    :language: python
    :start-after: [START howto_operator_agent_capabilities_composed]
    :end-before: [END howto_operator_agent_capabilities_composed]

Guardrail capabilities use the same passthrough pattern. This example uses
``InputGuard`` from ``pydantic-ai-shields`` to reject a prompt before the agent
run starts.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent_capabilities.py
    :language: python
    :start-after: [START howto_operator_agent_capabilities_input_guard]
    :end-before: [END howto_operator_agent_capabilities_input_guard]

.. warning::

    ``agent_params`` is a templated field, which Airflow serializes by calling
    ``str()`` on values it doesn't natively understand. Capability instances
    are not yet round-trip-safe through Dag serialization, so the examples above construct them inside the ``@dag`` function -- not at module level.
