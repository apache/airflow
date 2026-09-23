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

.. _howto/features:

LLM and agent features
======================

These settings change how a model call or an agent run behaves, independent of which operator
you use. Each is a parameter on the operator or decorator.

- :doc:`structured_output` — ``output_type`` returns a typed Pydantic object through XCom
  instead of a string.
- :doc:`message_history` — ``message_history`` carries a conversation across agent runs.
- :doc:`guardrails` — pydantic-ai capabilities and ``pydantic-ai-shields`` guards pass through
  ``agent_params``.
- :doc:`code_mode` — ``code_mode=True`` lets the model call several tools from one Python
  snippet instead of one round trip per call.
- :doc:`approval_gates` — ``require_approval=True`` pauses an LLM operator until a person
  approves, edits or rejects the output.
- :doc:`hitl_review` — ``enable_hitl_review=True`` opens an iterative review loop on an agent,
  with a chat UI and REST API for the reviewer.

The last two are different tools for different jobs: an approval gate is a one-shot decision on
one output, a HITL review is a conversation with a running agent. Each page opens with the
other in a *see also* note.

Making retries cheap with ``durable=True`` is a reliability feature and lives under
:doc:`operations`.

.. toctree::
    :hidden:
    :titlesonly:

    Structured output <structured_output>
    Message history <message_history>
    Guardrails <guardrails>
    Code mode <code_mode>
    Approve outputs <approval_gates>
    Review agent sessions <hitl_review>
