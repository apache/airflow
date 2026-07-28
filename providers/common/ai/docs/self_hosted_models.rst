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

.. _howto/self_hosted_models:

Self-hosted models
===================

This guide serves an open model on your own infrastructure, points
:class:`~airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook` at it
instead of a cloud provider, runs a ``@task.llm`` task and an
``AgentOperator`` with tool calling against it, and then routes the same
setup through a bearer-token AI gateway instead of talking to the server
directly. Running a model locally keeps prompt and response data inside your
own infrastructure, avoids per-call cost, and works offline.

Before you start
------------------

This guide assumes a working :doc:`apache-airflow:installation/index`
(Airflow 3.0+) already exists. Its job stops at wiring Airflow to a server
that's already running -- it doesn't cover installing or operating the
model-serving stack itself.

Prerequisites
^^^^^^^^^^^^^

You need an OpenAI-compatible model server reachable from where Airflow
runs, and hardware able to run the model you pick. The provider is built on
pydantic-ai, which reaches these servers by speaking the OpenAI-compatible
wire protocol -- that's why the server needs to expose an OpenAI-compatible
API. Any server that does works the same way; see
pydantic-ai's `OpenAI-compatible models docs
<https://pydantic.dev/docs/ai/models/overview/#openai-compatible-providers>`__
for the full list of what qualifies. Ollama, vLLM, and LM Studio are three
common choices, used as the examples throughout this guide:

- `Ollama <https://ollama.com/>`__ -- see its
  `hardware support docs <https://docs.ollama.com/gpu>`__ for GPU/CPU
  requirements.
- `vLLM <https://docs.vllm.ai/>`__ -- a higher-throughput alternative,
  commonly GPU-hosted; see its
  `GPU installation requirements <https://docs.vllm.ai/en/latest/getting_started/installation/gpu/>`__.
- `LM Studio <https://lmstudio.ai/>`__ -- a desktop app with a built-in
  OpenAI-compatible server; see its
  `system requirements <https://lmstudio.ai/docs/app/system-requirements>`__.

This page uses these three as running examples rather than trying to cover
every option; for how to point Airflow at any of the others, see
:ref:`self_hosted_models_other_servers` below.

1. Start an OpenAI-compatible server
--------------------------------------

Ollama is the reference server this guide proves end to end. The vLLM and LM
Studio tabs show equivalent wiring for readers already running one of those
instead, but this guide hasn't exercised them itself.

.. tab-set::

    .. tab-item:: Ollama
        :sync: ollama

        `Install it <https://ollama.com/download>`__ and pull a model:

        .. code-block:: bash

            ollama pull gemma4

        Installing Ollama sets up a background service (the macOS/Windows app,
        or a systemd service on Linux), so the server is already running --
        ``ollama pull`` only downloads the model. Ollama then serves an
        OpenAI-compatible API at ``http://localhost:11434``. On a headless host
        with no service, start it yourself with ``ollama serve``.

    .. tab-item:: vLLM
        :sync: vllm

        Unlike Ollama and LM Studio, vLLM serves a model by its full
        HuggingFace identifier (``organization/model-name``) rather than a
        short tag. Serve one, aliasing the served name to ``gemma4`` to match
        the connection below:

        .. code-block:: bash

            vllm serve Qwen/Qwen2.5-1.5B-Instruct --served-model-name gemma4

        See `vLLM's installation docs <https://docs.vllm.ai/en/latest/getting_started/installation/>`__
        for install and GPU requirements. vLLM then serves an
        OpenAI-compatible API at ``http://localhost:8000``.

    .. tab-item:: LM Studio
        :sync: lmstudio

        Download a model, load it, then start the server:

        .. code-block:: bash

            lms get gemma4
            lms load gemma4
            lms server start

        See `LM Studio's CLI docs <https://lmstudio.ai/docs/cli>`__ for the
        full ``lms`` reference (you can also download and load a model from
        the LM Studio UI instead). LM Studio then serves an OpenAI-compatible
        API at ``http://localhost:1234``.

2. Create the connection
--------------------------

Every LLM call goes through a Pydantic AI connection (``conn_type``
``pydanticai``, default connection id ``pydanticai_default``).

.. tab-set::

    .. tab-item:: Ollama
        :sync: ollama

        For a local, keyless server:

        .. code-block:: bash

            export AIRFLOW_CONN_PYDANTICAI_DEFAULT='{"conn_type": "pydanticai", "host": "http://localhost:11434/v1", "extra": {"model": "ollama:gemma4"}}'

    .. tab-item:: vLLM
        :sync: vllm

        .. code-block:: bash

            export AIRFLOW_CONN_PYDANTICAI_DEFAULT='{"conn_type": "pydanticai", "host": "http://localhost:8000/v1", "extra": {"model": "openai:gemma4"}}'

    .. tab-item:: LM Studio
        :sync: lmstudio

        .. code-block:: bash

            export AIRFLOW_CONN_PYDANTICAI_DEFAULT='{"conn_type": "pydanticai", "host": "http://localhost:1234/v1", "extra": {"model": "openai:gemma4"}}'

        ``http://localhost:1234`` is the default server port shown in `LM
        Studio's OpenAI-compatibility docs
        <https://lmstudio.ai/docs/app/api/endpoints/openai>`__.

- ``host`` -- the server's base URL.
- ``password`` -- left empty for a keyless local server. That triggers two
  separate things: the Airflow hook forwards no key at all when ``password``
  is empty (only ``base_url`` goes to the provider constructor); pydantic-ai's
  provider class then fills in its own placeholder API key and does send
  that placeholder in the actual HTTP request. A server with no auth check
  doesn't care, so the request still succeeds either way.
- ``extra["model"]`` -- ``ollama:<model>`` picks up Ollama-specific tuning;
  vLLM and LM Studio have no dedicated pydantic-ai provider class, so they
  use ``openai:<model>`` instead. See
  :ref:`Model identifier format <self_hosted_models_model_id>` below for
  details.

The examples above all use ``gemma4`` to illustrate the format -- the actual
value must match a model your server is really serving:

- Ollama -- ``ollama list`` shows the tags you've pulled.
- vLLM -- defaults to its ``--model`` startup argument; the
  ``--served-model-name`` flag can set an alias explicitly (e.g. to
  ``gemma4``) if you want the served name to differ from ``--model``.
- LM Studio -- the model identifier shown for whatever you loaded in its UI.
- Any of the three (and any other OpenAI-compatible server) --
  ``curl <host>/v1/models`` lists what the server actually reports as
  available.

See :ref:`howto/connection:pydanticai` for the full connection field
reference.

3. Run your first Dag with ``@task.llm``
-------------------------------------------

The ``@task.llm`` decorator turns a function that returns a prompt string
into a task that sends that prompt to the connection above and returns the
response:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_quickstart.py
    :language: python
    :start-after: [START howto_quickstart_llm]
    :end-before: [END howto_quickstart_llm]

Run it like any other Dag (``airflow dags test quickstart_llm``); the
``summarize`` task completes using the local model, not a cloud provider.

4. Run an agent with tool calling
------------------------------------

Tool calling needs a model that supports pydantic-ai's ``tools`` capability
-- not every small local model does.

.. tab-set::

    .. tab-item:: Ollama
        :sync: ollama

        The connection from the previous steps already points at a model
        that supports it, so this step runs against the same connection
        with no changes.

    .. tab-item:: vLLM
        :sync: vllm

        vLLM needs extra server startup flags to enable tool calling for a
        given model (``--enable-auto-tool-choice`` plus a matching
        ``--tool-call-parser``) -- see its
        `Tool Calling docs <https://docs.vllm.ai/en/latest/features/tool_calling/>`__
        for how to pick the right ones for your model.

    .. tab-item:: LM Studio
        :sync: lmstudio

        LM Studio's built-in server supports tool calling out of the box
        through ``/v1/chat/completions`` -- see its
        `Tool Use docs <https://lmstudio.ai/docs/developer/openai-compat/tools>`__.
        Its docs note that smaller models, or ones not trained for tool use,
        may return improperly formatted tool calls.

``AgentOperator`` can wrap an existing Airflow hook as a tool via
``HookToolset``. This example wraps an HTTP connection named ``my_api`` --
point it at any reachable HTTP endpoint that returns JSON:

.. code-block:: bash

    export AIRFLOW_CONN_MY_API='{"conn_type": "http", "host": "<your reachable API base URL>"}'

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent.py
    :language: python
    :start-after: [START howto_agent_self_hosted]
    :end-before: [END howto_agent_self_hosted]

The hook above is constructed with ``method="GET"`` because the target
endpoint -- the OpenAI-compatible model list (``/v1/models``) every one of
these servers exposes -- is GET-only, while ``HttpHook``
defaults to ``method="POST"``
(:class:`~airflow.providers.http.hooks.http.HttpHook`) -- fixed once the
hook is constructed, not something the model can change per call.

The prompt itself just asks a natural question -- it's the ``system_prompt``
that tells the agent where to find the answer (``GET /v1/models``), so the
model doesn't have to guess a path. That matters because ``HttpHook`` raises
on any response outside the 2xx/3xx range: if the model guessed wrong, the
task would fail outright rather than the agent recovering gracefully.

Routing through an AI gateway
--------------------------------

The same wiring routes through a bearer-token AI gateway instead of talking
to the server directly -- swap three connection fields and nothing else
changes:

- ``host`` -- the gateway's base URL, with no ``/v1`` suffix; pydantic-ai's
  ``openai`` provider appends the API path itself.
- ``password`` -- the bearer token the gateway expects.
- ``extra["model"]`` -- ``openai:<route>``, where ``<route>`` is the route
  name the gateway exposes for the model you want.

Only ``api_key`` and ``base_url`` reach the provider class -- there is no
extension point for custom headers. A gateway that authenticates with a
bearer token works through this connection shape; one that requires
additional custom headers does not.

You can validate this pattern locally with any OpenAI-compatible proxy that
speaks bearer-token auth -- for example, LiteLLM's proxy mode running in
front of a model server. That's a different role than the ``litellm:`` model
prefix covered in the reference section below: here LiteLLM stands in for
the gateway itself, reached through the generic ``openai:<route>`` prefix,
not addressed via its own model-prefix.

Reference
----------

The rest of this page is reference material for the connection shapes
above.

.. _self_hosted_models_model_id:

Model identifier format
^^^^^^^^^^^^^^^^^^^^^^^^

The ``extra`` JSON's ``model`` value keeps the ``provider:model`` format the
hook always expects, but which ``provider`` prefix to use depends on the
endpoint (verified against ``pydantic-ai-slim`` 2.5.0, the version this
guide was developed against, and 2.10.0, the latest at the time of
writing):

- **vLLM** has no dedicated provider class in pydantic-ai -- ``openai:<model>``
  is the only option. pydantic-ai's ``openai`` provider class talks to
  whatever ``host`` points at, so it works against vLLM's OpenAI-compatible
  API even though the weights being served aren't OpenAI's.
- **Ollama** has two valid options: the dedicated ``ollama:<model>`` prefix
  (recommended) applies Ollama-specific model-profile tuning based on the
  model name (``llama``, ``gemma``, ``qwen``, ``deepseek``, ``mistral``,
  ``command``, and ``gpt-oss`` prefixes get tuned defaults); the generic
  ``openai:<model>`` prefix also works, since Ollama exposes an
  OpenAI-compatible API too, but skips that tuning.

Both prefixes need the same ``openai`` extra
(``pip install "apache-airflow-providers-common-ai[openai]"``) -- pydantic-ai's
Ollama provider is built on the same underlying ``openai`` Python package as
its OpenAI provider, and there's no separate ``ollama`` extra.

.. note::

   ``LiteLLMProvider`` is an exception: its constructor takes ``api_base``
   instead of ``base_url``, so passing ``host`` raises a ``TypeError`` that
   the hook catches and silently falls back to environment-variable auth,
   ignoring ``host`` entirely. Point a LiteLLM proxy's OpenAI-compatible
   endpoint via ``openai:<model>`` instead -- this is the same proxy role
   used in the gateway section above, just addressed through the working
   prefix rather than the rejected ``litellm:`` one.

.. _self_hosted_models_other_servers:

Other OpenAI-compatible servers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The same ``openai:<model>`` plus ``host`` setup works against any other
server that exposes an OpenAI-compatible API -- for example
`LM Studio <https://lmstudio.ai/>`__,
`llama.cpp <https://github.com/ggml-org/llama.cpp>`__'s ``llama-server``, or
`LocalAI <https://localai.io/>`__. Point ``host`` at that server's
OpenAI-compatible endpoint the same way as the vLLM example above.

Cross-hook naming differences
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

:class:`~airflow.providers.common.ai.hooks.langchain.LangChainHook` and
:class:`~airflow.providers.common.ai.hooks.llamaindex.LlamaIndexHook` also
support self-hosted endpoints through the same ``host`` connection field, but
the underlying constructor keyword each hook passes it to is not uniform:

.. list-table::
   :header-rows: 1

   * - Hook
     - Connection field
     - Constructor keyword
   * - ``PydanticAIHook``
     - ``host``
     - ``base_url``
   * - ``LangChainHook``
     - ``host``
     - ``base_url``
   * - ``LlamaIndexHook``
     - ``host``
     - ``api_base``

Where to go next
-------------------

- :ref:`howto/connection:pydanticai` -- the full connection field reference.
- :doc:`retry_policies` -- the "Local LLM support" section covers pointing
  ``LLMRetryPolicy`` at a self-hosted endpoint.
- :doc:`examples` -- more runnable Dags against the ``pydanticai``
  connection type; point one at any of the connections on this page.
- pydantic-ai isn't limited to Ollama and vLLM -- any OpenAI-compatible
  endpoint works the same way. See the
  `pydantic-ai install docs <https://ai.pydantic.dev/install/#slim-install>`__
  for the full list of providers it supports.
- `pydantic-ai's OpenAI-compatible models docs
  <https://pydantic.dev/docs/ai/models/overview/#openai-compatible-providers>`__
  -- background on the ``openai`` provider class this guide's
  ``openai:<model>`` examples use.
