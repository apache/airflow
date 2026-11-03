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
       returns its output under the same rules as ``@task.llm``. Code mode is
       experimental; see below.
   * - Per-tool approval on ``AgentOperator`` (:doc:`tool_approval`)
     - On Airflow 3.3 and later, a call to a tool marked with Pydantic AI's approval API
       (``approval_required(...)`` on a toolset, or ``requires_approval=True`` on a
       function tool) pauses the task before the tool runs. **Approve** runs the call and
       the agent carries on; **Reject** tells the agent the call was denied and it carries
       on without it. ``tool_approval_timeout``, ``on_tool_approval_timeout`` (``"fail"``
       by default, or ``"deny"``) and ``tool_approval_assigned_users`` keep their meaning.
       On Airflow 3.4 and later, every later step that calls a marked tool pauses again;
       on Airflow 3.3 a task instance can ask once per Dag run. With ``durable=True``,
       ``enable_hitl_review=True`` or a ``SandboxToolset`` that provisions its own sandbox,
       and on Airflow versions before 3.3, a call to such a tool fails the task; under a
       ``CodeMode`` capability, a call from ``run_code`` does not run and is reported back
       to the model.
   * - ``message_history`` on ``AgentOperator``
     - Seeds the run with the given conversation, as a list of messages or its JSON form,
       and publishes the finished conversation under the ``message_history`` XCom key so a
       later run can continue it. Cannot be combined with ``enable_hitl_review``.
   * - ``emit_message_history`` on ``AgentOperator``
     - Publishes the finished conversation under the ``message_history`` XCom key without
       seeding the run. Defaults to ``True`` when ``message_history`` is set, and ``False``
       stops a seeded run from publishing it. Cannot be combined with ``enable_hitl_review``.
   * - ``durable=True`` on ``AgentOperator`` and ``@task.agent``, and
       :class:`~airflow.providers.common.ai.durable.AirflowDurability`
       (:doc:`durable_execution`)
     - On a retry the agent runs from the start, and each model request and tool call an
       earlier attempt recorded is replayed from the journal kept for the task instance,
       without calling the model or running the tool again, as long as it matches the step
       recorded at the same point; from the first step that differs, every step runs live.
       The journal is kept in the task state store on Airflow 3.3 and later, and under
       ``[common.ai] durable_cache_path`` before. With ``durable=True``, replayed steps
       count nothing against ``usage_limits`` or in the ``usage`` XCom, the agent's
       ``run_id`` stays the first attempt's, and the journal is deleted when the task
       succeeds. In a plain ``@task``, ``AirflowDurability`` deletes a run's steps when
       that run succeeds.
   * - ``@task.llm_branch`` and
       :class:`~airflow.providers.common.ai.operators.llm_branch.LLMBranchOperator`
     - The model picks among the task's direct downstream tasks, one by default or several
       with ``allow_multiple_branches=True``, and only the picked tasks run. The
       descriptions in ``branches``, as strings or as ``BranchOption(description=...)``,
       are sent to the model with the options. ``decision_policy`` and
       ``BranchOption.min_confidence`` are experimental; see below.
   * - ``@task.llm_sql`` and
       :class:`~airflow.providers.common.ai.operators.llm_sql.LLMSQLQueryOperator`
     - Generates SQL from the prompt and returns it without running it. With
       ``validate_sql=True``, the default, the task fails unless the output is a single
       statement that parses for ``dialect`` and is one of ``allowed_sql_types``
       (``SELECT``, ``UNION``, ``INTERSECT`` and ``EXCEPT`` by default); with the default
       types, a write nested inside the statement fails it too. A reviewer's edit under
       ``allow_modifications=True`` is checked the same way. ``db_conn_id`` with
       ``table_names``, ``schema_context`` or ``datasource_config`` give the model the
       schema. ``datasource_config`` reads object storage through the DataFusion engine of
       ``common.sql``, which pins ``datafusion`` below 52.
   * - ``@task.llm_file_analysis`` and
       :class:`~airflow.providers.common.ai.operators.llm_file_analysis.LLMFileAnalysisOperator`
     - Reads the file at ``file_path``, or up to ``max_files`` files under a prefix,
       without changing them, and sends their content to the model with the prompt in one
       request: CSV, Parquet and Avro sampled to ``sample_rows`` rows, and all text cut to
       ``max_text_chars``. Input over ``max_file_size_bytes`` or ``max_total_size_bytes``
       fails the task before the model is called, and so does an image or PDF unless
       ``multi_modal=True``, which attaches it.
   * - ``@task.llm_schema_compare`` and
       :class:`~airflow.providers.common.ai.operators.llm_schema_compare.LLMSchemaCompareOperator`
     - Introspects the schemas of ``data_sources`` and of ``table_names`` on each of
       ``db_conn_ids``, either or both, and returns a
       :class:`~airflow.providers.common.ai.operators.llm_schema_compare.SchemaCompareResult`
       as a dict: ``compatible``, ``summary`` and the ``mismatches``, each with a
       ``severity`` of ``critical``, ``warning`` or ``info``. ``context_strategy="basic"``
       sends columns only; ``"full"``, the default, adds keys and indexes for database
       sources.
       ``data_sources`` reads object storage through the DataFusion engine of
       ``common.sql``, which pins ``datafusion`` below 52.
   * - ``@task.llm_batch`` and
       :class:`~airflow.providers.common.ai.operators.llm_batch.LLMBatchOperator`
     - Submits ``requests`` to the batch API that the ``model_id`` prefix selects, writes
       one JSONL row per request under ``result_path`` and returns the manifest
       (``schema_version: 1``) described in :doc:`operators/llm_batch`, never the results
       themselves. A retry or clear that renders the same ``result_path`` re-attaches to
       the batch already submitted while the requests, model, connection, system prompt,
       ``max_tokens``, ``request_params`` and output schema are unchanged, unless that
       batch was cancelled or failed. On Airflow 3.3 and later, clearing a deferred task
       cancels its batch while ``cancel_on_kill=True``, the default. Adding a batch engine
       through ``BatchAdapter`` is experimental; see below.
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
     - Exposes exactly the hook methods in ``allowed_methods``, each named
       ``<tool_prefix>_<method>`` when ``tool_prefix`` is set, which must be a valid Python
       identifier, and raises an error when the toolset is created
       if a listed method does not exist on the hook. Arguments in ``pinned_arguments``
       are left out of the schema the model sees and passed to every allowed method; a
       call that supplies one is refused, and the toolset raises an error when it is
       created if an allowed method does not take a pinned argument by that name.
   * - :class:`~airflow.providers.common.ai.toolsets.object_storage.ObjectStorageToolset`
     - Exposes ``list_files``, ``get_file_info`` and ``read_file`` for the files under the
       root path, read-only. Paths from the model are relative to the root: absolute
       paths, paths with a scheme and paths that climb out of the root are refused. A file
       the model cannot read, such as a binary file or one larger than ``max_read_bytes``,
       comes back to it as a message instead of failing the task.
   * - :class:`~airflow.providers.common.ai.toolsets.mcp.MCPToolset` and
       :class:`~airflow.providers.common.ai.hooks.mcp.MCPHook`
     - Exposes the tools of the MCP server configured by ``mcp_conn_id``, each named
       ``<tool_prefix>_<name>`` when ``tool_prefix`` is set. The connection fields for each
       transport keep their meaning.
   * - The framework-neutral tool interface (:mod:`airflow.providers.common.ai.tools`):
       ``AirflowTool``, ``ToolResult``, ``ToolCallError``, ``ToolProvider``,
       ``collect_tools`` and the ``airflow_tools()`` method of the toolsets
     - An ``AirflowTool`` keeps its ``name``, ``description``, JSON Schema ``parameters``
       and async ``function``, and ``call()`` masks secrets in what it returns. A
       ``ToolResult`` with ``is_error=True`` is a failure the model can correct;
       ``ToolCallError`` fails the run. ``collect_tools`` raises ``ValueError`` when two
       tools share a name. An adapter for another agent framework can build on this.
   * - :class:`~airflow.providers.common.ai.policies.retry.LLMRetryPolicy`
     - Returns a retry decision from a model's reading of the exception. Values registered
       as secrets are masked in the exception text before it reaches the model, unless
       ``redact_exception=False`` or a custom ``redactor`` replaces the masking. If the
       model call fails, it falls back to ``fallback_rules`` and then to the task's own
       retry settings; it never fails the task itself.
   * - :class:`~airflow.providers.common.ai.policies.retry.ClassifierRetryPolicy` and
       :class:`~airflow.providers.common.ai.policies.retry.ErrorCategory`
     - The model can only answer with one of the names in ``categories``, and that name's
       ``ErrorCategory`` decides whether the task retries and after what ``delay``.
       Without ``categories`` the policy uses
       :data:`~airflow.providers.common.ai.policies.retry.DEFAULT_CATEGORIES`, whose seven
       names and their retry or fail split stay the same. When the model call fails, or
       ``min_confidence`` is set and the answer is under it or carries no confidence, the
       policy uses the decision of ``fallback_policy``, then ``fallback_rules``, then
       the task's own retry settings. ``ErrorCategory.min_confidence`` overrides the
       policy's ``min_confidence`` for that one category. ``min_confidence`` only has an
       effect when the model reports a confidence, and that comes from the ``typesafe``
       extra, which is still experimental. Secrets are masked in the exception text as for
       ``LLMRetryPolicy``.
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
   * - The ``typesafe`` extra (:doc:`decision_models`)
     - It installs ``typesafe-sdk``, which is before its 1.0 release.
   * - :class:`~airflow.providers.common.ai.policies.decision.DecisionPolicy`, and
       ``min_confidence`` on
       :class:`~airflow.providers.common.ai.policies.decision.BranchOption`
       (:doc:`approval_gates`)
     - Whether the confidence gate keeps this shape or becomes part of a wider decision
       policy is still open.
   * - Batch engines from other packages: ``BatchAdapter``, ``register_adapter()`` and the
       ``airflow.providers.common.ai.batch_adapters`` entry point
       (:doc:`operators/llm_batch`)
     - No package outside this provider implements the contract yet, so it has not been
       tested by a second implementation.
   * - Code mode (:doc:`code_mode`) and the ``shields`` extra (used in
       :doc:`capabilities`)
     - Thin integrations of ``pydantic-ai-harness`` and ``pydantic-ai-shields``, which are
       before their 1.0 releases.
   * - The Agent Skills toolset (:doc:`toolsets/skills`)
     - It is built on ``pydantic-ai-skills``, a community package. Whether it moves to
       ``pydantic-ai-harness``'s ``Skills`` once that loads skill resources and scripts is
       still open.
   * - :class:`~airflow.providers.common.ai.toolsets.sandbox.SandboxToolset` and its
       backends (:doc:`sandbox/index`)
     - ``durable=True`` cannot be used with it yet, the ``sbx`` backend leaves its sandbox
       behind when the worker is killed, and the backend contract is still gaining
       capabilities, such as attaching to a sandbox another task owns.
   * - LangChain and LlamaIndex hooks and the LangChain tool bridge
       (:doc:`hooks/langchain`, :doc:`toolsets/langchain`, :doc:`hooks/llamaindex`)
     - Each follows the API of a framework that changes often.
   * - The retrieval operators: ``DocumentLoaderOperator``,
       ``LlamaIndexEmbeddingOperator`` and ``LlamaIndexRetrievalOperator``
       (:doc:`rag_pipelines`)
     - Whether retrieval pipelines belong in this provider, which bridges Airflow to agent
       frameworks rather than replacing them, is still open.
   * - :class:`~airflow.providers.common.ai.toolsets.datafusion.DataFusionToolset`
       (:doc:`toolsets/datafusion`)
     - Runs on the DataFusion engine of ``common.sql``, which pins ``datafusion`` below 52
       and follows its API.
   * - Managed agent toolsets (:doc:`toolsets/managed_agent`)
     - The amazon and google providers implement the contract. The managed-agent operators
       of the other vendor providers are still being brought onto it, which can change it.
   * - OpenTelemetry spans (``[common.ai] otel_export_enabled`` and
       ``capture_content``; :doc:`observability`)
     - Span names and attributes come from Pydantic AI's instrumentation and the
       OpenTelemetry GenAI conventions, which are still in development.
   * - :class:`~airflow.providers.common.ai.toolsets.logging.ToolLoggingCapability`
       (:doc:`toolsets/logging`)
     - The capability ``AgentOperator`` applies for ``enable_tool_logging``; its constructor
       and behaviour may change.
   * - The Strands plugin, the ADK toolset and Strands checkpoint resume
       (:doc:`frameworks/index`)
     - Written against Strands 1.56 and ADK 2.9.1, which change often. Strands marks its
       own checkpointing experimental.
   * - The Anthropic tool runner adapter (:doc:`frameworks/anthropic`)
     - Built on ``tool_runner``, a beta helper of the Anthropic SDK that can change between
       SDK releases.
   * - The OpenAI Agents SDK adapter, trace processor and ``run_durably``
       (:doc:`frameworks/openai_agents`)
     - Written against OpenAI Agents SDK 0.20 and 0.23. The SDK is pre-1.0 and changes
       its tool and runner APIs between minor releases.
   * - The Claude Agent SDK adapter (:doc:`frameworks/claude_agent_sdk`)
     - New; the SDK is pre-1.0, and its in-process MCP server turns a handler's exception
       into an error result the adapter has to work around.
   * - The ``common_ai.tool_calls`` metric and
       :func:`~airflow.providers.common.ai.tools.tracing.agent_framework_tracing`
       (:doc:`observability`)
     - The tracing helper follows the agent frameworks' own telemetry, which is still
       changing; the metric's tags may change as more frameworks get adapters.
