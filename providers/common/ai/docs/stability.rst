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

.. _howto/stability:

Stable and experimental features
================================

A stable feature keeps its behaviour across minor releases of this provider, from version
1.0.0 on. An experimental feature is documented and maintained, but can change or be removed
in a minor release, as Airflow's :ref:`experimental feature policy <apache-airflow:experimental>`
allows. A breaking change to an experimental feature is announced in the changelog.

Stable features
---------------

A stable feature keeps its public parameters, their defaults and the behaviour described
below until the next major release. A minor release can add an optional parameter.
Changing a default, renaming a parameter or narrowing what a feature does needs a major
release.

Stability covers behaviour, not wording. Log lines, error messages, tool descriptions and
the prompt text this provider sends to a model can change in any release. The toolsets stay
Pydantic AI toolsets, but the Pydantic AI class they inherit from can change.

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Feature
     - What stays the same
   * - ``@task.llm`` and :class:`~airflow.providers.common.ai.operators.llm.LLMOperator`
     - Sends the prompt to the model from ``llm_conn_id`` and returns the output as the
       task's return value: a string by default, or an instance of ``output_type``,
       dumped to a dict when ``serialize_output=True``. Before Airflow 3.3 an instance is
       always dumped to a dict. ``decision_policy`` is experimental; see below.
   * - ``@task.agent`` and :class:`~airflow.providers.common.ai.operators.agent.AgentOperator`
     - Runs an agent with the model from ``llm_conn_id`` and the given toolsets, and
       returns its output under the same rules as ``@task.llm``. ``durable``,
       code mode and per-tool approval are experimental; see below.
   * - ``message_history`` on ``AgentOperator``
     - Seeds the run with the given conversation, as a list of messages or its JSON form,
       and publishes the finished conversation under the ``message_history`` XCom key so a
       later run can continue it. Cannot be combined with ``enable_hitl_review``.
   * - ``@task.llm_branch`` and
       :class:`~airflow.providers.common.ai.operators.llm_branch.LLMBranchOperator`
     - The model picks among the task's direct downstream tasks, one by default or several
       with ``allow_multiple_branches=True``, and only the picked tasks run. The
       descriptions in ``branches``, as strings or as ``BranchOption(description=...)``,
       are sent to the model with the options. ``decision_policy`` and
       ``BranchOption.min_confidence`` are experimental; see below.
   * - :class:`~airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook` and its
       connection types
     - The connection fields documented for each type in :doc:`connections/pydantic_ai`,
       :doc:`connections/pydantic_ai_bedrock`, :doc:`connections/pydantic_ai_vertex` and
       :doc:`connections/pydantic_ai_azure` keep their meaning, so an existing connection
       keeps producing the same model.
   * - :class:`~airflow.providers.common.ai.toolsets.sql.SQLToolset`
     - Exposes ``list_tables``, ``get_schema``, ``query`` and ``check_query``. Read-only
       unless ``allow_writes=True``. When ``allowed_tables`` is set, other tables are
       refused. ``query`` returns at most ``max_rows`` rows, and keeps the rows and column
       names within ``max_result_bytes`` bytes. When ``query`` fails or refuses a
       statement, the model gets the error and can correct it, up to the tool's retry
       limit, after which the task fails.
   * - :class:`~airflow.providers.common.ai.toolsets.hook.HookToolset`
     - Exposes exactly the hook methods in ``allowed_methods``, each named after its method
       with ``tool_name_prefix`` in front, and raises an error when the toolset is created
       if a listed method does not exist on the hook. ``pinned_arguments`` is
       experimental; see below.
   * - :class:`~airflow.providers.common.ai.toolsets.mcp.MCPToolset` and
       :class:`~airflow.providers.common.ai.hooks.mcp.MCPHook`
     - Exposes the tools of the MCP server configured by ``mcp_conn_id``, each named
       ``<tool_prefix>_<name>`` when ``tool_prefix`` is set. The connection fields for each
       transport keep their meaning.
   * - :class:`~airflow.providers.common.ai.policies.retry.LLMRetryPolicy`
     - Returns a retry decision from a model's reading of the exception. Values registered
       as secrets are masked in the exception text before it reaches the model, unless
       ``redact_exception=False`` or a custom ``redactor`` replaces the masking. If the
       model call fails, it falls back to ``fallback_rules`` and then to the task's own
       retry settings; it never fails the task itself.
   * - Output review: ``require_approval``, ``enable_hitl_review`` and the review plugin
     - With ``require_approval=True``, the task defers after generating output and returns
       it only once a person approves it on the Required Actions page. A rejection, or a
       timeout under the default ``on_approval_timeout="fail"``, fails the task, except on
       ``@task.llm_branch``, where it skips the downstream tasks unless
       ``fail_on_reject=True``.

Experimental features
---------------------

Everything this provider ships that is not in the table above is experimental.

.. list-table::
   :header-rows: 1
   :widths: 35 65

   * - Feature
     - Why it is experimental
   * - :class:`~airflow.providers.common.ai.policies.retry.ClassifierRetryPolicy`
       (:doc:`decision_models`)
     - The confidence threshold, the fallback order and the behaviour when the classifier
       is unavailable are still settling.
   * - :class:`~airflow.providers.common.ai.policies.decision.DecisionPolicy`, and
       ``min_confidence`` on
       :class:`~airflow.providers.common.ai.policies.decision.BranchOption`
       (:doc:`approval_gates`)
     - New. The threshold semantics and the recorded decision may change as they are
       used.
   * - ``@task.llm_batch`` and batch adapters (:doc:`operators/llm_batch`)
     - Submission, re-attachment on retry, cancellation and handling of partial results
       are still settling, and ``BatchAdapter`` has no implementation outside this
       provider yet.
   * - ``durable=True`` on ``AgentOperator`` (:doc:`durable_execution`)
     - Which tool results are replayed on retry will change, so that a tool can declare
       whether its result may be reused.
   * - Per-tool approval on ``AgentOperator`` (``tool_approval_timeout``,
       ``on_tool_approval_timeout`` and ``tool_approval_assigned_users``;
       :doc:`tool_approval`)
     - New, and needs Airflow 3.3. How a paused run resumes may change.
   * - Code mode (:doc:`code_mode`), the Agent Skills toolset
       (:doc:`toolsets/skills`) and the ``shields`` extra (used in :doc:`capabilities`)
     - Thin integrations of packages outside this provider whose APIs are still
       changing: ``pydantic-ai-harness``, ``pydantic-ai-skills`` and
       ``pydantic-ai-shields``.
   * - :class:`~airflow.providers.common.ai.toolsets.sandbox.SandboxToolset` and its
       backends (:doc:`sandbox/index`)
     - The sandbox runtime belongs to the backend; ownership and cleanup across worker
       failures are still being designed.
   * - LangChain and LlamaIndex hooks and the LangChain tool bridge
       (:doc:`hooks/langchain`, :doc:`toolsets/langchain`, :doc:`hooks/llamaindex`)
     - Each follows the API of a framework that changes often.
   * - The retrieval operators: ``DocumentLoaderOperator``,
       ``LlamaIndexEmbeddingOperator`` and ``LlamaIndexRetrievalOperator``
       (:doc:`rag_pipelines`)
     - New. The shape of the pipeline, from loading documents through embedding to
       retrieval, has not settled.
   * - :class:`~airflow.providers.common.ai.toolsets.datafusion.DataFusionToolset`
       (:doc:`toolsets/datafusion`)
     - Runs on the DataFusion engine of ``common.sql``, which pins ``datafusion`` below 52
       and follows its API.
   * - Managed agent toolsets (:doc:`toolsets/managed_agent`)
     - A contract for vendor providers that no vendor implements yet.
   * - OpenTelemetry spans (``[common.ai] otel_export_enabled`` and
       ``capture_content``; :doc:`observability`)
     - Span names and attributes come from Pydantic AI's instrumentation and the
       OpenTelemetry GenAI conventions, which are still in development.
   * - :class:`~airflow.providers.common.ai.toolsets.logging.LoggingToolset` used directly
       (:doc:`toolsets/logging`)
     - The wrapper ``AgentOperator`` applies for ``enable_tool_logging``; its constructor
       may change.
   * - ``@task.llm_schema_compare``, ``@task.llm_file_analysis`` and ``@task.llm_sql``
       (:doc:`operators/llm_schema_compare`, :doc:`operators/llm_file_analysis`,
       :doc:`operators/llm_sql`)
     - Each adds its own input handling to ``@task.llm``: schema introspection, file
       sampling, or SQL validation. Their options are still settling.
   * - The framework-neutral tools (:mod:`airflow.providers.common.ai.tools`), including
       the ``airflow_tools()`` method of the toolsets, the Strands plugin and the ADK
       toolset (:doc:`frameworks/index`)
     - Written against Strands 1.56 and ADK 2.9.1. CI does not run the tests of the two
       adapters, because both frameworks exclude dependency versions that Airflow's
       development environment uses.
   * - :class:`~airflow.providers.common.ai.toolsets.object_storage.ObjectStorageToolset`
       (:doc:`toolsets/object_storage`)
     - New; its tools and read limits may change after first use.
   * - ``pinned_arguments`` on
       :class:`~airflow.providers.common.ai.toolsets.hook.HookToolset`
       (:doc:`toolsets/hook`)
     - New; how a pinned argument is matched to each method's parameters may change after
       first use.
   * - The ``common_ai.tool_calls`` metric and
       :func:`~airflow.providers.common.ai.tools.tracing.agent_framework_tracing`
       (:doc:`observability`)
     - The tracing helper follows the agent frameworks' own telemetry, which is still
       changing; the metric's tags may change as more frameworks get adapters.
