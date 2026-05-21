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

.. _howto/hook:crewai:

``CrewAIHook``
==============

Use :class:`~airflow.providers.common.ai.hooks.crewai.CrewAIHook` to bridge an
Airflow connection to a `CrewAI <https://docs.crewai.com/>`__ ``LLM``. The hook
reads credentials (API key, optional base URL) from a connection of type
``crewai`` and returns a configured ``crewai.LLM`` that you can pass to any
``crewai.Agent``.

CrewAI's ``LLM`` is a LiteLLM wrapper, so model identifiers use LiteLLM's
``provider/name`` form (e.g. ``openai/gpt-4o``, ``anthropic/claude-3-5-sonnet``).
**Note the slash** -- this is distinct from pydantic-ai's colon
(``provider:name``) format, which is why CrewAI needs its own connection type.

Basic Usage
-----------

Pass ``llm_model`` to the constructor (or set ``extra["model"]`` on the
connection) and call ``get_llm()``:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_crewai_hook.py
    :language: python
    :start-after: [START howto_hook_crewai_basic]
    :end-before: [END howto_hook_crewai_basic]

A richer end-to-end demo (multi-agent crew, mapped tasks, HITL review, and
LLM-based report synthesis) lives in ``example_crewai_stock_analysis.py``
under the same ``example_dags`` directory.

Supported providers
-------------------

The hook forwards ``conn.password`` as ``api_key`` and ``conn.host`` as
``base_url`` to ``crewai.LLM``. The combinations that work out of the box
with just ``crewai`` installed:

- ``openai/gpt-4o``, ``openai/gpt-4o-mini``
- ``openai/...`` against an OpenAI-compatible endpoint, with ``host`` pointing
  at Ollama / vLLM / LM Studio
- ``anthropic/claude-3-5-sonnet``, ``anthropic/claude-3-7-sonnet`` (CrewAI
  installs the ``anthropic`` SDK as a runtime dependency).

Other LiteLLM-routed providers (``groq/``, ``mistralai/``, ``deepseek/``,
``gemini/``, ``ollama/``, ``cohere/``, ``huggingface/``, ...) need the CrewAI
``litellm`` extra **and** the vendor's SDK installed alongside CrewAI; this
hook does not pull those in. If you need them, install ``crewai[litellm]``
plus the relevant SDK separately.

Cloud providers with non-standard auth -- ``bedrock/...``, ``vertex_ai/...``,
``azure/...`` -- require AWS region/IAM, GCP project/location, or Azure
endpoint+api_version kwargs that this hook does not forward. The hook raises
``NotImplementedError`` if you pass such a model identifier; per-vendor
subclasses (mirroring the pydantic-ai
:class:`~airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIBedrockHook` /
``PydanticAIVertexHook`` / ``PydanticAIAzureHook`` pattern) are deferred to a
follow-up.

Connection Configuration
------------------------

The hook reads credentials from the Airflow connection of type ``crewai``:

- **password** -- API key (passed as ``api_key`` to ``crewai.LLM``).
- **host** -- Optional base URL (passed as ``base_url``; useful for custom
  OpenAI-compatible endpoints, Ollama, vLLM).
- **extra** JSON -- ``{"model": "openai/gpt-4o"}`` to set a default model
  identifier on the connection.

Parameters
----------

.. list-table::
   :header-rows: 1
   :widths: 25 25 50

   * - Parameter
     - Default
     - Description
   * - ``llm_conn_id``
     - ``crewai_default``
     - Airflow connection ID for the LLM provider.
   * - ``llm_model``
     - ``None`` (falls back to ``extra["model"]`` on the connection)
     - Model identifier in LiteLLM ``provider/name`` form, e.g.
       ``openai/gpt-4o``. Required (constructor or connection extra) when
       calling ``get_llm()``.

Dependencies
------------

Install ``crewai`` separately into your venv after installing the
``common-ai`` provider::

    pip install apache-airflow-providers-common-ai
    pip install "crewai>=1.14.5"

There is intentionally **no** ``[crewai]`` extra on this provider.
``crewai`` 1.14.x ships tight conservative pins (``click~=8.1.7``,
``tomli~=2.0.2``, plus a ``crewai-cli`` co-dependency that pulls
``textual>=7.5`` → ``rich>=14.2``) that conflict with Airflow's own
resolved ``click>=8.3``, ``tomli>=2.4``, and FAB's transitive
``rich<14``. uv can resolve the mismatch via ``override-dependencies``,
but the CI image build runs ``pip check`` afterwards and treats the
metadata conflict as fatal. The cleanest answer is to keep ``crewai``
out of Airflow's lock entirely; users who accept the pin cascade
install ``crewai`` themselves.

For non-OpenAI / non-Anthropic providers, install CrewAI's own
``litellm`` extra plus the vendor SDK::

    pip install "crewai[litellm]>=1.14.5" langchain-mistralai

CrewAI 1.14.x publishes wheels for Python 3.10-3.13; 3.14 isn't supported
upstream yet.
