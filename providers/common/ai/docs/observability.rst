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
* **Correlation is automatic.** The worker opens a task span before the operator
  runs, so the agent's spans nest under it and inherit the task's ``trace_id``
  and ``airflow.*`` attributes (dag id, run id, task id, try number, map index).
  An automatic retry reuses the task instance's persisted trace context, so all
  attempts share one trace and appear as repeated task-run spans on it,
  distinguished by ``try number``. Only a manual clear or rerun regenerates the
  context and starts a new trace.
* **Content is off by default.** Only token counts, model id, latency, tool
  names, and finish reason are recorded. Prompt and completion text is never
  emitted unless you opt in (see below).

.. note::

    On pydantic-ai 2.x the agent-run span reports token usage under
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

    With ``capture_content`` enabled, prompts, completions, and tool IO are
    exported to your tracing backend **without redaction**. Airflow's secret masking
    applies to logs and rendered template fields, not to OpenTelemetry span
    attributes, so it does not scrub this content. Enable it only for debugging
    in a trusted environment. It has no effect unless ``otel_export_enabled`` is
    ``True``.

See :doc:`configurations-ref` for the full list of options.
