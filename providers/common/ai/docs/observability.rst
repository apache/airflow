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

Observability (OpenTelemetry tracing)
=====================================

.. note::

    Experimental: the spans and their attributes can change in a minor release of this
    provider.
    See :ref:`howto/stability`.

pydantic-ai ships native OpenTelemetry instrumentation that emits GenAI spans
for each agent run, model call, and tool call, following the
`OpenTelemetry GenAI semantic conventions <https://opentelemetry.io/docs/specs/semconv/gen-ai/>`__.
When enabled, this provider turns that instrumentation on for every agent it
builds and routes the spans through the OpenTelemetry exporter Airflow already
uses, so they appear in whatever backend your deployment runs (Jaeger, Tempo,
Grafana, Phoenix, Langfuse, an OTLP collector, ...), correlated to the task that
produced them.

This covers all of the LLM operators (:class:`~airflow.providers.common.ai.operators.agent.AgentOperator`,
``@task.agent`` / ``@task.llm`` and the SQL / branch / file-analysis / schema-compare
operators), because they all build their agent through
:meth:`~airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook.create_agent`.

How it works
------------

* **No extra infrastructure.** The provider does not configure an exporter or a
  ``TracerProvider`` of its own. It reuses the global provider that Airflow's
  core tracing installs, so the spans share the exporter and endpoint already
  configured under ``[traces]`` / the standard ``OTEL_EXPORTER_OTLP_*``
  environment variables. If core tracing is not enabled in the worker process,
  no GenAI spans are emitted.
* **Correlation.** The worker opens a task span before the operator runs, so the
  agent's spans nest under it and share its ``trace_id``.
  :class:`~airflow.providers.common.ai.operators.agent.AgentOperator` (and
  ``@task.agent``) additionally stamps the task-instance identity on every GenAI
  span it emits: the five keys core tracing already puts on the task span
  (``airflow.dag_id``, ``airflow.task_id``, ``airflow.dag_run.run_id``,
  ``airflow.task_instance.try_number``, ``airflow.task_instance.map_index``) plus
  ``airflow.task_instance.id`` as the per-attempt run join key. So a span is
  filterable by dag, task, run, attempt, or map index directly, without walking
  up to the parent span (OpenTelemetry children inherit trace context, not
  attributes).
  An automatic retry reuses the task instance's persisted trace context, so all
  attempts share one trace and appear as repeated task-run spans on it,
  distinguished by ``try number``. Only a manual clear or rerun regenerates the
  context and starts a new trace.
* **Run join key.** For an ``AgentOperator`` run, the task-instance id (unique
  per attempt, since Airflow regenerates it on each retry) is passed to
  pydantic-ai as the run's ``run_id``. It surfaces on the run's GenAI spans as
  ``gen_ai.agent.call.id``, and the operator also exposes it, alongside the run's
  token usage, on XCom under the ``run_id`` and ``usage`` keys. ``usage`` is
  this attempt's own usage -- not the cross-attempt cumulative total described
  under ``usage_limits`` in :ref:`howto/operator:agent` -- and it is pushed on
  a failed attempt too, so a downstream ``all_done`` task or failure callback
  can read what the last attempt spent. With ``durable=True``, steps an
  attempt replays from the cache are not part of it. XCom is cleared at the start of every
  attempt, so only the most recent attempt's value survives, not each
  historical attempt's. A downstream task can then reference the
  run (``ti.xcom_pull(task_ids="my_agent", key="run_id")``) and a trace
  backend can join a task's output to its agent trace without parsing logs.
  With ``enable_hitl_review`` the ``run_id`` and ``usage`` reflect the initial
  model run, not the human-feedback regenerations. A run that resumes after a
  tool approval (see :doc:`tool_approval`) continues as
  ``<task-instance id>-resumed``, which is the ``run_id`` the operator pushes;
  ``usage`` covers both parts.
* **Scope.** The ``run_id`` / ``usage`` XComs come only from ``AgentOperator`` and
  ``@task.agent``, and so do the ``airflow.*`` identity attributes, apart from a Strands or
  ADK agent run inside ``agent_framework_tracing`` (see below). The other LLM
  operators still emit GenAI spans correlated to the task span by nesting, but
  without the identity attributes or the run join key.
* **Content is off by default.** Only token counts, model id, latency, tool
  names, and finish reason are recorded. Prompt and completion text is never
  emitted unless you opt in (see below).
* **Cost is already on the span.** pydantic-ai's own instrumentation sets a
  best-effort ``operation.cost`` attribute on the model-call span whenever it
  can price the response -- no provider configuration is needed for this.

.. note::

    The agent-run span reports token usage under
    ``gen_ai.aggregated_usage.*`` while the per-model-call span keeps
    ``gen_ai.usage.*``. This avoids double-counting in backends that sum a
    parent span and its children. Dashboards or alerts that read run-level token
    usage from ``gen_ai.usage.*`` should switch to ``gen_ai.aggregated_usage.*``.

Enabling it
-----------

Enable core tracing and turn on the provider option:

.. code-block:: ini

    [traces]
    otel_on = True

    [common.ai]
    otel_export_enabled = True

Configure the exporter destination with the standard OpenTelemetry environment
variables, for example:

.. code-block:: bash

    # Core tracing defaults the exporter to OTLP/gRPC. For an OTLP/HTTP
    # endpoint (port 4318, ``/v1/traces`` path) also select the HTTP exporter:
    export OTEL_TRACES_EXPORTER="otlp_proto_http"
    export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT="http://otel-collector:4318/v1/traces"

Capturing prompt and completion content
---------------------------------------

By default the spans carry no message text. To also record model inputs and
outputs (``gen_ai.input.messages`` / ``gen_ai.output.messages``), set:

.. code-block:: ini

    [common.ai]
    capture_content = True

.. warning::

    With ``capture_content`` enabled, prompts, completions, and tool arguments are
    exported to your tracing backend **without redaction**. Airflow's secret masking
    applies to logs and rendered template fields, not to OpenTelemetry span
    attributes. The one exception is what a tool returns or raises while it runs in
    an agent this provider builds, which is masked before the span records it (see
    :doc:`agent_security`). Enable it only for debugging in a trusted environment. It has no effect unless ``otel_export_enabled`` is
    ``True``.

Agents built with other frameworks
----------------------------------

.. note::

    Experimental: ``agent_framework_tracing`` can change or be removed in a minor release
    of this provider.
    See :ref:`howto/stability`.

Strands Agents and Google ADK emit OpenTelemetry spans of their own. Run the agent inside
:func:`~airflow.providers.common.ai.tools.tracing.agent_framework_tracing` so those spans
follow the same rules as ``AgentOperator``'s:

.. code-block:: python

    from airflow.providers.common.ai.tools.strands import AirflowTools
    from airflow.providers.common.ai.tools.tracing import agent_framework_tracing

    with agent_framework_tracing():
        agent = Agent(model=model, plugins=[AirflowTools(warehouse)])
        answer = agent(question)

Inside the block the framework leaves prompts, completions and tool arguments and results
out of its spans unless ``[common.ai] otel_export_enabled`` and ``capture_content`` are
both on, using the switch each
framework reads: the ``gen_ai_unredacted_attributes`` token of
``OTEL_SEMCONV_STABILITY_OPT_IN`` for Strands, and ``ADK_CAPTURE_MESSAGE_CONTENT_IN_SPANS``
and ``OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT`` for ADK. A value your deployment
already set wins. Strands reads its switch once per process, when it creates its one tracer,
so create the first ``Agent`` of the task inside the block, not at module level; ADK reads
its switches when a ``TelemetryConfig`` is built, so build that inside the block too.

Every span started inside the block under the worker's tracer provider carries the same
``airflow.*`` identity attributes as ``AgentOperator``'s spans. When no tracer provider made
a span for the task, the Dag run's trace context is not made the parent of the framework's
spans: that context is marked as not sampled, and a parent-based sampler, the OpenTelemetry
default, would drop every span a tracer provider the framework installs starts. When core
tracing or auto-instrumentation made the task's span, the Dag run's sampling decision
holds.

The identity attributes go on spans of the tracer provider that is installed when the block
starts, so set up the framework's own telemetry, such as ``StrandsTelemetry``, before
entering it. They follow the task through ``asyncio``; ADK's synchronous ``Runner.run``
runs the agent on a thread of its own that they do not reach, so use ``run_async``.

Counting tool calls
-------------------

.. note::

    Experimental: the ``common_ai.tool_calls`` metric and its tags can change or be
    removed in a minor release of this provider.
    See :ref:`howto/stability`.

Every call to one of this provider's connection-backed toolsets increments the
``common_ai.tool_calls`` counter through Airflow's metrics, whether the call comes from ``AgentOperator``, a
Pydantic AI agent you build yourself, or another framework through its adapter. It
answers which toolsets are used and from where without reading task logs. The counter
carries three tags:

.. list-table::
   :header-rows: 1
   :widths: 20 80

   * - Tag
     - Values
   * - ``toolset``
     - The toolset class, such as ``SQLToolset`` or ``ObjectStorageToolset``.
   * - ``framework``
     - ``pydantic_ai`` for ``AgentOperator`` and your own Pydantic AI agents;
       ``strands``, ``adk`` or ``langchain`` for a call through that framework's adapter;
       ``none`` for a call to a toolset's ``airflow_tools()`` made without an adapter.
   * - ``outcome``
     - ``executed`` when the call returned, ``failed`` when it raised (including a
       failure the model is asked to correct), and ``replayed`` when
       ``AgentOperator(durable=True)`` served the result from its cache on a retry. A call
       whose arguments fail validation never reaches the toolset and is not counted, and
       neither is a call that pauses the run until a person approves it.

Tags never include arguments, connection IDs, table names or paths. They reach backends
that support them: OpenTelemetry metrics (``[metrics] otel_on``), or StatsD with
``[metrics] statsd_datadog_enabled`` or ``statsd_influxdb_enabled``. The Agent Skills
toolset, toolsets you write yourself and hand-built ``AirflowTool`` objects are not
counted.

See :doc:`configurations-ref` for the full list of options.
