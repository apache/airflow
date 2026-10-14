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

.. NOTE TO CONTRIBUTORS:
    Please, only add notes to the Changelog just below the "Changelog" header when there are some breaking changes
    and you want to add an explanation to the users on how they are supposed to deal with them.
    The changelog is updated and maintained semi-automatically by release manager.

``apache-airflow-providers-common-ai``

Changelog
---------

.. note::
  The worker cannot register a model class for XCom deserialization when ``output_type`` is a
  list of output types such as ``[Summary, int]``, an output marker such as
  ``ToolOutput(Summary)``, an output function, a ``TypeAliasType`` or a ``NewType``. For these
  output types, ``AgentOperator`` and ``@task.agent`` now push each model in the output as a
  JSON-compatible dict (``model_dump(mode="json")``), including models inside a list, tuple or
  dict. On Airflow < 3.3, the worker registers no model class at all, so a model inside a list,
  tuple or dict is now pushed as a JSON-compatible dict for every ``output_type``. A top-level
  model is still dumped with plain ``model_dump()`` when ``serialize_output=True`` is set or on
  Airflow < 3.3.

  The new behaviour changes the type that a downstream task receives after an upgrade. Versions
  1.0.0 and 1.1.0 pushed these models as model instances. They dumped a top-level model with
  plain ``model_dump()`` only in HITL review, with ``serialize_output=True`` or on
  Airflow < 3.3. A downstream task loaded a model instance when the model's class was listed in
  ``[core] allowed_deserialization_classes``, or on Airflow >= 3.3 when another task in the
  same Dag declared the class as its ``output_type``. That downstream task now gets a dict and
  must read ``result["field"]`` instead of ``result.field``. In HITL review on Airflow >= 3.3
  without ``serialize_output=True``, values such as ``datetime``, ``date``, ``timedelta``,
  ``UUID`` and ``Decimal`` in a top-level model now arrive as strings, for example
  ``"PT1M30S"`` for a ``timedelta``. Tuples and sets in that model now arrive as lists. See
  :doc:`structured_output`.

.. warning::
  The following changes are breaking for code written against 1.0.0:

  * ``PydanticAIHook``: when a connection supplies credentials (password or host) and the provider class
    rejects the resulting keyword arguments (``TypeError``), the hook now raises ``ValueError``. In 1.0.0
    it logged a warning and fell back to environment-variable authentication, which could authenticate
    as a different identity than the connection. Fix the connection fields for that provider, or remove
    the credentials from the connection if environment-variable authentication is what you want.
  * ``airflow.providers.common.ai.policies.retry``: importing it without the Airflow ``RetryPolicy``
    support (Airflow older than 3.3) now raises ``AirflowOptionalProviderFeatureException`` instead of
    ``ImportError``. It is not an ``ImportError`` subclass (it derives from ``AirflowException``), so a
    Dag that guards the import with ``except ImportError:`` as in the 1.0.0 example Dag now fails to
    parse on those versions. Catch both, importing the exception from
    ``airflow.providers.common.compat.sdk`` (works on Airflow 2.11 and 3.x)::

      from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

      try:
          from airflow.providers.common.ai.policies.retry import LLMRetryPolicy
      except (ImportError, AirflowOptionalProviderFeatureException):
          LLMRetryPolicy = None

.. note::
  ``LlamaIndexHook`` and ``LangChainHook`` (both experimental, see :doc:`stability`):
  passing ``embed_model`` and ``llm_model`` positionally is deprecated. In 1.0.0 they were the third and
  fourth positional parameters (after ``llm_conn_id`` and ``embed_conn_id``), and the two hooks listed
  them in opposite order (``embed_model`` then ``llm_model`` for ``LlamaIndexHook``, ``llm_model`` then
  ``embed_model`` for ``LangChainHook``). Positional calls still work and emit an
  ``AirflowProviderDeprecationWarning``, but support will be removed in a future release. Pass them by
  keyword, e.g. ``LlamaIndexHook(llm_model="gpt-5")``, which also avoids the meanings swapping between
  the two hooks.

.. note::
  ``LoggingToolset`` is removed from the public API. Importing it from either
  ``airflow.providers.common.ai.toolsets.LoggingToolset`` (exported from the package since
  1.0.0, see #74449 under 1.0.0 Features below) or
  ``airflow.providers.common.ai.toolsets.logging.LoggingToolset`` (public since 0.1.0) now
  raises ``ImportError``. ``AgentOperator``
  still logs tool calls through ``enable_tool_logging``. To log tool calls of an agent you
  build yourself, pass ``capabilities=[ToolLoggingCapability()]`` to the pydantic-ai ``Agent``
  (import it from ``airflow.providers.common.ai.toolsets.logging``); its ``logger`` argument
  sets the logger. See :doc:`stability`.

1.1.0
.....

Features
~~~~~~~~

* ``Add emit_message_history to record an agent transcript without seeding it (#74811)``
* ``Push LLMOperator's token usage and cost to XCom (#74355)``

Bug Fixes
~~~~~~~~~

* ``Use common.compat mask_secret in MCPHook (#75064)``
* ``Preserve SQL table allowlist validation with newer sqlglot and add OpenAI Decisions and System One to the connection vendor list (#73822)``

Misc
~~~~

* ``Remove unused mypy suppressions in common providers (#75257)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Number task loop iterations from one instead of zero (#74773)``
   * ``Stop parallel prek hooks racing on __pycache__ cleanup (#73823)``
   * ``Fix Modal contract test importing a private class from its old location (#75188)``

1.0.0
.....

.. note::
  ``LLMOperator``, the operators built on it (``LLMBranchOperator``, ``LLMSQLQueryOperator``,
  ``LLMSchemaCompareOperator``, ``LLMFileAnalysisOperator``) and their ``@task.llm*``
  decorators no longer accept ``agent_params``. These operators make one model call and run
  no tools in the task; an agent with function tools, toolsets or capabilities belongs in
  ``AgentOperator``, which keeps ``agent_params``. Passing ``agent_params`` to an LLM operator now fails when the Dag is
  parsed. ``agent_params={"model_settings": {...}}`` becomes ``model_settings={...}``,
  ``agent_params={"retries": n}`` becomes ``output_retries=n``, and ``tools``, ``toolsets``,
  ``capabilities`` or ``spec_file`` move to ``AgentOperator`` or ``@task.agent``. See
  :ref:`howto/operator:llm`.

.. note::
  On Airflow >= 3.3, the ``usage_limits`` of the ``LLM*`` operators (``LLMOperator``,
  ``LLMBranchOperator``, ``LLMSQLQueryOperator``, ``LLMSchemaCompareOperator``,
  ``LLMFileAnalysisOperator``) counts usage across every attempt of the task instance
  combined -- the initial run and every retry add to one running total (including the
  implicit ``request_limit=50`` default), instead of each attempt starting fresh. A pause
  for approval and a retry after a reviewer rejects the output share the same total. Scale
  each limit by ``retries + 1``, or set ``usage_limits=None``, to keep the old per-attempt
  headroom. On Airflow < 3.3, or when ``usage_limits`` is ``None``, each attempt is checked
  and counted on its own, unchanged. See :ref:`howto/operator:llm`.

.. note::
  A reviewer's edit on ``LLMOperator``, ``LLMFileAnalysisOperator`` or
  ``LLMSchemaCompareOperator`` (``allow_modifications=True``) is now converted back into
  ``output_type``, and an edit that does not validate fails the task with
  ``ReviewedOutputValidationError`` (a ``ValueError``) instead of returning the edited text as
  a ``str``. Use ``output_type=str`` or ``str | None`` to keep accepting free text; a ``Literal`` or
  a ``str`` ``Enum`` accepts one of its values typed as bare text, and any other edit now fails the
  task. An approval without an edit never fails this way. The review text now shows field names and leaves out computed fields.
  See :doc:`approval_gates`.

.. note::
  Durable execution (``durable=True`` on ``AgentOperator``) is rebuilt on the
  ``AirflowDurability`` capability. The internal ``durable.caching_model``,
  ``durable.caching_toolset`` and ``durable.step_counter`` modules are removed, and the
  provider now needs ``pydantic-ai-slim>=2.36.0``. With ``durable=True``, every
  ``FunctionToolset``, ``MCPToolset`` and ``DynamicToolset`` must be given a unique ``id=``
  (for example ``FunctionToolset(..., id="orders")``), otherwise the operator raises
  ``ValueError`` when it is created. A tool result that cannot be serialized to JSON now fails
  the task instead of being replayed, and ``durable=True`` cannot be combined with an explicit
  ``AirflowDurability`` capability. Durable steps recorded by 0.10.0 are not replayed, so a
  retry that spans the upgrade runs its steps live again.

.. note::
  ``LLMBranchOperator``, ``LLMSQLQueryOperator`` and ``LLMSchemaCompareOperator`` (and their
  decorators) now raise ``TypeError`` when given ``output_type``. They produce a fixed output and
  previously ignored the argument silently; remove it from those tasks. With
  ``enable_hitl_review=True``, the review loop now fails the task after 10 consecutive failures
  to read the review XCom, instead of polling until ``hitl_timeout``.

.. note::
  ``OpenSandboxBackend`` refuses a deny-all or allowlist ``SandboxSpec``, including the
  default ``SandboxSpec()``, unless the OpenSandbox server's egress sidecar runs in ``dns+nft``
  mode. In the server's default ``dns`` mode the sidecar filters name resolution only, and a
  sandbox could still connect to any numeric address. Set ``[egress] mode = "dns+nft"`` in the
  OpenSandbox server configuration, or pass ``SandboxSpec(block_network=False)`` for a sandbox
  that may reach the network. See :ref:`the OpenSandbox backend <sandbox-backend-opensandbox>`.

.. note::
  ``HookToolset`` no longer accepts the deprecated ``tool_name_prefix`` parameter. Pass
  ``tool_prefix`` instead: ``tool_name_prefix="s3_"`` becomes ``tool_prefix="s3"``, since
  ``HookToolset`` adds the underscore itself. ``MCPToolset``'s and ``MCPHook``'s ``tool_prefix``
  now default to ``""`` instead of ``None``, like the other toolsets that take a prefix.
  ``SQLToolset`` and ``DataFusionToolset`` gain a ``tool_prefix`` parameter with the same
  behaviour as ``HookToolset``'s.

.. note::
  On Airflow >= 3.3, ``AgentOperator``'s ``usage_limits`` counts usage across every
  attempt of the task instance combined -- initial run, every retry, and every HITL
  regeneration add to one running total (including the implicit ``request_limit=50``
  default), instead of each attempt starting fresh. Scale each limit by ``retries + 1``,
  or set ``usage_limits=None``, to keep the old per-attempt headroom. On every Airflow
  version, a regeneration shares its count with the run before it only when
  ``usage_limits`` is set. A step replayed with ``durable=True`` no longer counts toward
  ``usage_limits`` or the ``usage`` XCom, on any Airflow version. The ``usage`` XCom is
  now also pushed on a failed attempt, reporting that attempt's own usage (not the
  cross-attempt total). On Airflow < 3.3, or when ``usage_limits`` is ``None``, each
  attempt is checked and counted on its own, unchanged. See :ref:`the cross-attempt usage
  budget <agent-usage-budget>`.

.. note::
  ``get_schema`` on ``SQLToolset`` and ``DataFusionToolset`` now returns a JSON object
  ``{"columns": [{"name", "type"}, ...], "column_count": N}`` instead of a bare JSON array of
  columns. The tool also accepts an optional ``name_contains`` substring filter, and on a table
  with more columns than ``max_columns`` (default 100), or one whose serialized columns exceed
  ``max_result_bytes``, it returns a bounded summary (``column_count``, a ``type_histogram`` and a
  ``sample_columns`` preview, with ``truncated``, ``truncated_by`` and a ``hint``) in place of the
  full list. Update any system prompt or direct ``call_tool("get_schema", ...)`` caller that read
  the old top-level array: check ``result.get("truncated")`` first, then read ``result["columns"]``
  on a full result or ``result["sample_columns"]`` on a summary. A full result carries no
  ``truncated`` key and a summary carries no ``columns`` key, so indexing either one directly
  raises ``KeyError`` on the other kind of result.

.. note::
  ``AgentOperator`` no longer accepts ``code_mode``. Pass the pydantic-ai-harness capability
  instead: replace ``code_mode=True`` with ``capabilities=[CodeMode()]``, using
  ``from pydantic_ai_harness import CodeMode``. The capability takes its own arguments, such
  as ``max_tool_calls``. If the task also passes ``agent_params={"capabilities": [...]}``,
  move those into ``capabilities=`` too, since the two cannot be combined. The import runs
  when the Dag file is parsed, so the ``code-mode`` extra is now needed by the Dag processor
  as well as the workers. The extra's floor is now ``pydantic-ai-harness>=0.35.0``, the first
  release that caps ``pydantic-monty`` below 1.0, which earlier releases cannot run code with.
  See :doc:`code_mode`.

Breaking changes
~~~~~~~~~~~~~~~~

* ``Fail the task when a reviewed output no longer matches 'output_type' (#74473)``
* ``Remove 'agent_params' from LLM operators so they cannot run tools (#74721)``
* ``Add AirflowDurability capability for durable agent replay across task retries (#74385, #74720)``
* ``Bound LLM operator usage_limits across task retries (#74396)``
* ``Reject unsupported output_type and bound HITL XCom pull retries (#74450)``

Features
~~~~~~~~

* ``Replay an OpenAI Agents SDK agent's completed calls when Airflow retries its task (#74770)``
* ``Record OpenAI Agents SDK traces as the Airflow task's OpenTelemetry spans (#74769)``
* ``Let retry policies reschedule a task without using a retry (#74719)``
* ``Allow durable agent execution with a 'CodeMode' capability (#74735)``
* ``Add an OpenAI Agents SDK adapter for 'common.ai' toolsets (#74767)``
* ``Let LLMBatchOperator request_params be templated (#74751)``
* ``Add 'OpenShell' backend for sandbox tools (#73961)``
* ``Add Anthropic tool runner adapter for 'common.ai' toolsets (#73962)``
* ``Let an agent ask for tool approval in more than one step (#74686)``
* ``Add Islo backend for sandbox tools (#71672)``
* ``Add Boat backend for sandbox tools (#71725)``
* ``Resume Strands agents from their last checkpoint when a task retries (#74404)``
* ``Add a Model tab to the task-instance page for 'common.ai' (#74334)``
* ``Rename 'HookToolset' 'tool_name_prefix' to 'tool_prefix' (#74447)``
* ``Log capability tool calls in AgentOperator (#72202)``
* ``Let the HITL review plugin address one loop pass or mapped slot (#74351)``
* ``Export 'DataFusionToolset', 'AgentSkillsToolset' and 'LoggingToolset' from 'common.ai' toolsets (#74449)``
* ``Expose the resolved LLM model name on a dedicated XCom key (#74272)``
* ``Add 'max_retries' to 'AgentSkillsToolset' (#74381)``
* ``Add 'include_traceback' to model-backed retry policies (#74308)``
* ``Let the Modal sandbox backend read credentials from a modal connection (#74302)``
* ``Include prompt cache token counts in AgentOperator's usage XCom (#74277)``
* ``Bound get_schema results for very wide tables in the SQL toolsets (#74017)``
* ``Support Airflow 2.11 in the Common AI provider (#73991)``
* ``Cache repeated agent prompts by default in AgentOperator (#73994)``
* ``Add a vendor-neutral managed-agent hook contract to Common AI (#73532)``
* ``Export files an agent built in a sandbox to object storage (#73990)``
* ``Add first-class 'capabilities' to Common AI 'AgentOperator' (#73984)``
* ``Let Common AI toolsets use the agent's tool retry budget (#73957)``
* ``Add embedding kwargs to common AI hooks and operators (#72002)``
* ``Tell the model how many rows a Parquet or Avro file holds (#73925)``
* ``Count and trace tool calls made by native agent frameworks (#73901)``
* ``Let HookToolset pin arguments the model must not choose (#73900)``
* ``Add 'ObjectStorageToolset' for reading files on object storage (#73899)``
* ``Rework common.ai Strands support into a plugin and add Google ADK (#73898, #74525)``
* ``Name the tools a durable agent retry will run again (#73873)``
* ``Return an approved agent answer as 'output_type' after human review (#73904, #74537)``
* ``Let the sandbox toolset attach to a sandbox another task provisioned (#73559)``
* ``Add Strands Agents support for 'common.ai' toolsets (#73587)``
* ``Add Modal provider with 'ModalHook' and connection type (#73418)``
* ``Add OpenSandbox backend for sandbox tools (#71676, #74504)``
* ``Let a person approve an agent's tool calls before they run (#73586)``

Bug Fixes
~~~~~~~~~

* ``Make Common AI example Dags reach the behaviour they show (#74901)``
* ``Reject duplicate durable toolset ids and fail deferred tools with 'UnsupportedToolDeferralError' (#74902)``
* ``Fix Common AI Model tab not loading on Airflow 3.4 (#74898)``
* ``Keep the pydantic-ai first-run banner out of Common AI task logs (#74900)``
* ``Cap 'pydantic-ai-skills' below 2 for the Common AI skills extra (#74899)``
* ``Fingerprint durable agent steps that hold sets or values that are not JSON (#71575)``
* ``Fix HookToolset calls to hook methods that take a datetime (#74759)``
* ``Stop rejecting valid non-ASCII Anthropic batches as too large (#74662)``
* ``Mask tool output from every capability in Common AI agents (#73993)``
* ``Validate the connection type in PydanticAIHook.get_hook (#74626)``
* ``Decode base64 for bytes-typed 'HookToolset' parameters (#70433)``
* ``Make 'SQLToolset' 'check_query' follow 'allow_writes' like 'query' (#74448)``
* ``Tell the model a pinned hook argument is fixed for every allowed method (#74380)``
* ``Keep retried task attempts and their data under the attempt UUID (#74222)``
* ``Fix Common AI durable retries not replaying tools from a capability without an id (#74314)``
* ``Fix Common AI durable retries and tool-approval resumes on pydantic-ai 2.50+ (#74313)``
* ``Mask secrets in tool results before they reach the model (#73897)``
* ``Account for AgentOperator spend on failed runs and across retries (#73706)``
* ``Stop 'DataFusionToolset' from materializing full results for a capped query (#73384)``
* ``Fix mypy error in common.ai decision helper with pydantic-ai 2.46 (#73652)``

Misc
~~~~

* ``Move Common AI provider from incubation to production lifecycle (#74705)``
* ``Add a generated supported services page for the common.ai provider (#72939)``
* ``Drop support for Python 3.10 (#74157)``
* ``Remove Python 3.10 compatibility shims (#74153)``
* ``Remove 'code_mode' from 'AgentOperator' in favor of the 'CodeMode' capability (#74312)``
* ``Make 'execute_tool' the public method 'AirflowToolset' subclasses implement (#73938)``
* ``Mark which 'common.ai' features are stable and which stay experimental for 1.0 (#73896, #74707, #74704, #74679)``
* ``Require 'common.sql' 2.2.0 for common.ai's SQL extras (#73867)``

Doc-only
~~~~~~~~

* ``Keep the custom-toolset example's version from changing the PyPI path (#74939)``
* ``Fix stale Common AI docs and add a guide to writing a toolset (#74904)``
* ``Document progressive tool disclosure for Common AI toolsets (#74736)``
* ``Walk through the reviewer's side of Common AI approval gates (#74737)``
* ``Run the OpenAI Agents adapter tests in the agent framework CI job (#74768)``
* ``Add a feature map to the Common AI docs (#74723)``
* ``Document how common.ai agent tasks recover from provider and worker failures (#74708)``
* ``Fix the common.ai message_history trimming example and link the memory page (#74706)``
* ``Mark decision_policy experimental on the decision models page and finish the ClassifierRetryPolicy stability row (#74713)``
* ``Add an example of a decision model approving an agent's tool calls (#74697)``
* ``Add a Why Common AI page comparing the provider with a plain task (#74685)``
* ``Clarify what common.ai message_history does and does not cover (#74625)``
* ``Explain why common.ai is built on Pydantic AI (#74680)``
* ``Document agent memory across Dag runs for 'common.ai' (#74681)``
* ``Document hosted OpenAI-compatible services for common.ai (#74620)``
* ``Document that 'SQLToolset' does not check functions without 'allowed_tables' (#74444)``
* ``Explain unknown platform labels in managed-agent metrics (#72786)``
* ``Add toolset overview and MCP tool filtering example to 'common.ai' docs (#74378)``
* ``Link registry modules to the guide section that documents them (#71477)``
* ``Add restricted-agent examples to common.ai toolset guides (#74379)``
* ``Support PAT and key-pair auth in the Snowflake Cortex Agent hook (#73932)``
* ``Update provider READMEs for the Python 3.11 baseline (#74158)``
* ``Lead the 'common.ai' sandbox docs with when to use it and where each piece runs (#74297)``
* ``Correct usage_limits=None docstring on AgentOperator and LLMOperator (#74307)``
* ``Document decision models served over the System One API in common.ai (#74266)``
* ``Add Azure Blob Storage support to the DataFusion object storage layer (#73374)``
* ``Render the HITL review workflow as a Mermaid diagram (#74024)``
* ``Use "Airflow versions" in user-facing docs, not "cores" (#74045)``
* ``Document running your own Pydantic AI agent, and sandboxes in other frameworks (#73902)``
* ``Surface the retry policy decision on task instances page (#73030)``
* ``Update changelog to clarify new LLMBranchOperator "branches" feature (#73850)``
* ``Add connections and agent-security entries to the common.ai sidebar (#73837)``
* ``Persist 'retry_reason' not just for retries but even when a task fails (#73027)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add the Common AI docs fixes to the 1.0.0 changelog (#74937)``
   * ``Prepare ad-hoc providers release 2026-10-17 (#74933)``
   * ``Prepare ad-hoc providers release 2026-10-14 (#74733)``
   * ``Add sqlglot drift tripwires for common.ai read-only SQL validation (#74487)``
   * ``Test that deferred capability ids stay out of Common AI durable fingerprints (#74423)``
   * ``Assert on structured log events in common.ai tests (#74443)``
   * ``Prepare providers release 2026-10-06 (#74429)``
   * ``Apply ruff Python 3.11 fixes to providers (#74155)``
   * ``Remove provider tests that restate 'template_fields' and other class constants (#74359)``
   * ``Add AIP-85 to the AIP progress tracker registry (#74304)``
   * ``Fix flaky AgentOperator durable usage budget tests (#73996)``
   * ``[main] Upgrade important CI environment (#73629)``

0.10.0
......

.. note::
  ``LLMBranchOperator`` and ``LLMOperator`` now push a ``decision`` XCom on every run, next to
  ``return_value``, with the model's pick, the action taken, the confidence and probabilities
  when the model reports them, and the ``decision_policy`` that applied. ``LLMBranchOperator``
  also offers the downstream task IDs to the model in sorted order (it was set order, which
  differed between workers), gained ``branches`` as a template field, and builds its option
  type from pydantic-ai's ``Choices`` on 2.46+ or an equivalent enum whose member names are
  generated; the option values are still the task IDs, so ``do_branch`` receives the same
  strings as before.

.. note::
  ``execute_complete`` on ``LLMOperator``, ``LLMBranchOperator``, ``LLMSQLQueryOperator`` and
  ``LLMSchemaCompareOperator`` gained a keyword argument, ``decision``. ``LLMOperator`` and
  ``LLMBranchOperator`` pass it on resume, so a subclass of either that overrides
  ``execute_complete`` with the old three-argument signature raises ``TypeError`` when the
  reviewed task resumes; add ``decision=None`` to the override. The other two accept the
  keyword but do not pass it yet. A review that was already pending when you upgraded
  resumes without it and is unaffected.

.. note::
  Configuring ``fallback_conn_ids`` on a connection (or the matching operator/decorator
  argument) changes what exception a task raises once every connection in the chain fails:
  it is ``pydantic_ai.exceptions.FallbackExceptionGroup``, not the last provider's own
  exception. An existing ``RetryRule(exception=ModelHTTPError, ...)`` -- in
  ``LLMRetryPolicy.fallback_rules`` or a plain ``ExceptionRetryPolicy`` -- stops matching
  as soon as the connection gains a fallback chain, with no change to the Dag needed to
  trigger it. Match ``pydantic_ai.exceptions.FallbackExceptionGroup`` explicitly as well,
  or inspect its ``.exceptions`` attribute for the original per-model errors. See
  :doc:`retry_policies`, "When the connection also carries a fallback chain".

.. note::
  ``SQLToolset`` now rejects ``allowed_tables=None`` and ``allowed_tables=[]`` with
  ``ValueError``. Up to 0.9.0 both were accepted and exposed every table in the schema, so
  a Dag that builds the list dynamically (a ``Variable.get`` with a ``None`` default, a
  config file, a filtered comprehension) silently handed the agent the whole schema
  whenever the lookup came back empty. Such a Dag now fails at import instead. Exposing
  every table is still the default, but only by omitting the argument: no value you can
  pass requests it, so a runtime lookup can never widen the allow-list by accident. Dags
  that passed ``allowed_tables=None`` explicitly should drop the argument.

.. note::
  The PydanticAI vendor connection types are renamed from ``pydanticai-azure``,
  ``pydanticai-bedrock`` and ``pydanticai-vertex`` to ``pydanticai_azure``,
  ``pydanticai_bedrock`` and ``pydanticai_vertex``. The hyphenated names could never be
  expressed as a connection URI scheme, so a connection stored in a secrets backend or in
  ``AIRFLOW_CONN_*`` never resolved. An existing connection created with a hyphenated
  ``conn_type`` no longer matches its hook and the task fails to find it: re-create it with
  the underscored type (in the UI, pick the same vendor again and save).

Breaking changes
~~~~~~~~~~~~~~~~

* ``Reject allowed_tables=None and allowed_tables=[] in SQLToolset so only omitting it allows every table (#73381, #73452)``
* ``Add a decision policy so an LLM operator asks a person when the model is unsure (#73368)``
* ``Fix PydanticAI vendor connections not resolving from secrets backends (#72853)``

Features
~~~~~~~~

* ``Allow templated connection IDs in agent toolsets (#73578)``
* ``Cancel the agent run when a common.ai LLM or agent task is killed (#73495)``
* ``Add JSON Lines support for DocumentLoaderOperator (#72246)``
* ``Add an address-layer egress allowlist for hosted sandboxes (#73534)``
* ``Add ClassifierRetryPolicy for classifier models and keep LLMRetryPolicy as the text-model policy (#73450, #73501)``
* ``Add a deferrable batch execution mode to common.ai (#72938)``
* ``Add a Modal backend for the sandbox toolset (#72910)``
* ``Add support for TypeSafe Jev classifier models (#73363)``
* ``Add branches to LLMBranchOperator so the model reads what each branch means (#73367, #73368)``
* ``Add connection-driven provider failover for common.ai LLM calls (#72156)``
* ``Support assigned reviewers in LLM approval reviews (#72157)``
* ``Stamp Airflow run identity onto agent runs and traces (#73275)``
* ``Support notifiers in LLM approval reviews (#72159)``
* ``Support timeout defaults in LLM approval reviews (#72155)``
* ``Support per-run cost limits in common.ai LLM and Agent operators (#71403)``

Bug Fixes
~~~~~~~~~

* ``Reject non-string prompts in LLMFileAnalysisOperator before reading files (#71734)``
* ``Reject durable replay and human-in-the-loop review when an agent holds a SandboxToolset (#73529)``
* ``Fix LLMBranchOperator sending branch options in a different order on each worker (#73366)``
* ``Fail LLMOperator approval at parse time on Airflow cores older than 3.1 (#73261)``
* ``Fix 'DocumentLoaderOperator' validation errors naming the wrong argument (#73053)``
* ``Report the Airflow version error first when human-in-the-loop review needs Airflow 3.1 or newer (#73052)``
* ``Restore Dag-parse-time validation for common.ai operator arguments (#70628)``

Misc
~~~~

* ``Require pydantic-ai-slim 2.33.0 or newer so Anthropic models work with the anthropic 1.x SDK (#73511)``
* ``Replace the retired gemini-2.0-flash example model in the Vertex connection placeholders and docs (#73516)``
* ``Update the Azure OpenAI connection placeholders and document when api_version must be omitted (#73024)``
* ``Add TypedDict type hints for AirflowPlugin list fields (#69761)``

Doc-only
~~~~~~~~

* ``Document sandbox enforcement, teardown and cost-limit scope for agents (#73589)``
* ``Add a guide to developing and testing Common AI tasks locally (#73602)``
* ``Fix stale and duplicated content in the 'common.ai' provider docs (#73567)``
* ``Lead the common.ai docs with a runnable quick start and a verifiable result (#73556)``
* ``Nest the 'common.ai' docs sidebar and lead with features and providers (#73548)``
* ``Add use-case pages to the common.ai provider docs (#73542)``
* ``Rename the common.ai retry policies page to cover both policies (#73526)``
* ``Reorganize 'common.ai' provider docs into topic-based navigation (#73523)``
* ``Add a decision guide for choosing between common.ai toolsets (#72936)``
* ``Document GCS data sources in the common.ai SQL toolset guides (#73370)``
* ``Update the LLM schema-compare guide and example for plain database tables (#73273)``
* ``Document how retry policies and durable execution work together (#73160)``
* ``Document usage_limits on all common.ai LLM operators (#73283)``
* ``Make LLM retry policy failure model documentation more accurate (#73158)``
* ``Document what the LLM can and cannot do in retry policies (#72947)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Revert "[main] Upgrade important CI environment (#73308)" (#73621)``
   * ``[main] Upgrade important CI environment (#73308)``

0.9.0
.....

.. note::
  A rejected ``LLMBranchOperator`` review now skips the direct downstream tasks -- teardown tasks
  excepted -- instead of failing the task. Set ``fail_on_reject=True`` to keep failing the task, or
  ``ignore_downstream_trigger_rules=True`` to skip every downstream task rather than only the direct
  ones.

Features
~~~~~~~~

* ``Support bzip2 and xz compressed inputs in LLM file analysis (#70302)``
* ``Add .md file support in LLMFileAnalysisOperator (#71611)``
* ``Add test_connection support to LangChainHook and LlamaIndexHook (#71841)``
* ``Add BaseManagedAgentToolset for vendor-managed AI agents (#71946)``

Bug Fixes
~~~~~~~~~

* ``Add require_approval preflight check to @task.llm_schema_compare (#71688)``
* ``Skip downstream tasks instead of failing when an LLM branch review is rejected (#71073, #72183)``
* ``Fix Vertex AI hook silently discarding credentials when vertexai flag is set (#72012)``

Misc
~~~~

* ``Import TaskInstanceState from airflow.sdk (#72446)``

Doc-only
~~~~~~~~

* ``Fix LlamaIndexHook docs to stop claiming Ollama/vLLM support (#72013)``
* ``Document the missing resource category in common.ai retry policy docs (#72189)``
* ``Document LLMFileAnalysisOperator's inherited LLM and HITL parameters (#71856)``
* ``Fix reversed credential precedence in Bedrock hook docstring (#71826)``
* ``Correct the common-ai toolset list and document the shields extra (#71819)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add drift tripwires for common.ai Vertex model prefix (#72152)``
   * ``Add unit tests for common AI provider exceptions (#72082)``
   * ``Fix common.ai Vertex model example to use a valid pydantic-ai prefix (#72011)``
   * ``Sync connection UI metadata in provider.yaml with hook definitions (#72087)``
   * ``[main] Upgrade important CI environment (#71590)``

0.8.0
.....

.. note::
    The ``query`` tool of ``SQLToolset`` and ``DataFusionToolset`` returns a different
    shape. Rows were a dict per row alongside a ``count`` of every matching row:
    ``{"rows": [{"id": 1, "name": "a"}], "count": 900}``. They are now columnar, and
    ``row_count`` counts the rows actually returned:
    ``{"columns": ["id", "name"], "rows": [[1, "a"]], "row_count": 1}``. A truncated
    result also carries ``truncated_by`` -- ``max_rows`` or the new ``max_result_bytes``
    -- and ``total_rows`` appears only when the driver reports a trustworthy query
    total. The tool's own description states the new shape, so agents adapt without
    changes; update any system prompt that describes the old shape, and any code
    calling ``toolset.call_tool("query", ...)`` directly.

Features
~~~~~~~~

* ``Add SandboxToolset for sandboxed agent shell and file access (#68847)``
* ``Support require_approval in LLMSchemaCompareOperator (#71051)``
* ``Bound SQL toolset query results by size, not just row count (#71317)``
* ``Support custom redaction and message length cap in LLMRetryPolicy (#70830)``

Bug Fixes
~~~~~~~~~

* ``Let the agent retry on a rejected DataFusion query instead of failing the task (#71445)``

Misc
~~~~

* ``Show which LLM providers each Common AI connection type reaches (#70497)``

Doc-only
~~~~~~~~

* ``Document the dedicated pydantic-ai vendor connection types (#71774)``
* ``Document the common-ai MCPHook (#71817)``
* ``Document the common-ai LangChain and LlamaIndex connection types (#71818)``
* ``Fix and expand the common-ai provider's When to use guidance (#71820)``
* ``Add LLMSchemaCompareOperator to the common-ai operator index table (#71738)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``[main] CI: Upgrade important CI environment (#70501)``
   * ``Require a lower bound on every dependency in pyproject.toml (#71378)``
   * ``Pin providers in constraints to the versions published in PyPI (#71324)``

0.7.0
.....

.. note::
    The ``skills`` extra now requires ``pydantic-ai-skills>=1.2.0`` (previously ``>=0.11.0``);
    the file-exclusion support added in #69924 relies on the 1.x API. Every 0.x release is
    excluded, so environments that pin ``pydantic-ai-skills`` below 1.2.0 will fail to resolve
    ``apache-airflow-providers-common-ai[skills]``. To migrate, upgrade ``pydantic-ai-skills``
    to 1.2.0 or newer; if you cannot, stay on ``common.ai`` 0.6.0. Installations that do not use
    the ``skills`` extra are unaffected.

.. note::
    ``SQLToolset(allowed_tables=...)`` now fails closed: ``COPY`` in any form, and any function
    sqlglot cannot type, are refused before the query runs. This closes a bypass where
    ``pg_read_file()``, ``query_to_xml()`` and ``COPY ... FROM PROGRAM`` reached data and files
    outside ``allowed_tables``. Project UDFs and a few common builtins such as
    ``json_build_object`` are also not recognized; pass them in the new ``allowed_functions``
    argument to permit them. The database role remains the real security boundary.

Features
~~~~~~~~

* ``Add pydantic-ai capability matrix references doc to Common AI (#69887)``
* ``Support excluding files from 'common.ai' Agent Skills discovery (#69924)``
* ``Use task state store for 'common.ai' durable execution on Airflow 3.3+ (#68926)``
* ``Support '.txt' files in 'LLMFileAnalysisOperator' (#70431)``
* ``Support cloud URI globs in DocumentLoaderOperator (#70299)``
* ``Support require_approval in LLMBranchOperator (#70651)``
* ``Render multi-branch review choices as a multi-select (#71046)``

Bug Fixes
~~~~~~~~~

* ``Allow Common AI SQL imports without DataFusion (#69990)``
* ``Fix HITL review showing altered agent output to reviewers (#70070)``
* ``Fix 'LLMSQLQueryOperator' not stripping single-line markdown code fences (#70137)``
* ``Harden common.ai SQLToolset allowed_tables against function/COPY bypass (#70134)``
* ``Preserve output_type through human approval in LLM operators (#70075)``
* ``Fix DocumentLoaderOperator ignoring unknown parser on byte input (#70071)``
* ``Reject unsupported require_approval in LLM branch and schema compare operators (#70069)``
* ``Fix '@task.llm_branch' import failure on Task SDK-only workers (#70068)``
* ``Fix common.ai durable execution skipping Toolset-capability tools (#69881)``
* ``Redact secrets from LLMRetryPolicy classification prompts (#70229)``
* ``Validate common AI tool-call arguments to enable pydantic-ai retries (#70096)``
* ``Validate template fields after rendering in common.ai operators (#70338)``

Misc
~~~~

* ``Add dataclasses-json floor to common.ai llamaindex extra (#69755)``
* ``Add a Retry Policies category to the provider registry (#70499)``
* ``Mark asserts under 'TYPE_CHECKING' in 'DocumentLoaderOperator' (#70378)``
* ``Mark KubernetesPodOperator and AgentOperator as durable capable (#70289)``

Doc-only
~~~~~~~~

* ``Add self-hosted model guide for the common.ai provider (#69867)``
* ``Update retired model ids and fill doc gaps in common.ai provider docs (#69711)``
* ``Add quick start guide to common.ai provider docs (#69552)``
* ``Document the dynamic 'system_prompt' pattern for common-ai agents (#69636)``
* ``Add feature-comparison table and toolset links to common.ai provider docs (#69649)``
* ``Add an Examples entry point to the common.ai provider docs (#69650)``
* ``Document when to use common.ai vs vendor-specific AI providers (#69551)``
* ``Document the sql extra required for LLMSQLQueryOperator (#70564)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix toolsets python-modules check for common.ai provider (#70181)``
   * ``Add toolset as a provider module category (#70122)``
   * ``[main] Upgrade important CI environment (#69694)``
   * ``Guard code_mode example DAG on SQLToolset import (#69677)``
   * ``Add AWS services toolset for agents to access 1000+ APIs (#70087)``
   * ``Fix flaky secrets masking test in LLMRetryPolicy (#70823)``
   * ``Revert AWS services toolset for common AI provider (#70695)``
   * ``Use common.compat.sdk for timezone imports in providers (#70492)``
   * ``Mark common.ai provider as not ready for release (#70481)``
   * ``Prepare providers release 2026-07-22 (#70256)``

0.6.0
.....

Features
~~~~~~~~

* ``Add 'env_provider' and 'Extra.env' support to MCP stdio transport (#69225)``

Bug Fixes
~~~~~~~~~

* ``Replace deprecated 'pydantic-ai' MCP classes with 'MCPToolset' in 'common.ai' (#69006)``

Misc
~~~~

* ``Migrate common.ai provider to pydantic-ai 2.x and remove the <2 cap (#69358)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Document each provider's optional extras in its docs index (#69478)``
   * ``Fix inconsistency between generated provider docs and pyproject.toml (#68991)``
   * ``[main] Upgrade important CI environment (#68933)``

0.5.0
.....

.. note::
    ``SQLToolset(allowed_tables=[...])`` now enforces the allow-list on the ``query`` and
    ``check_query`` tools, not only on metadata discovery (#68487). The SQL an agent submits is
    parsed with sqlglot and rejected before execution if it reaches any table outside the list
    (including through subqueries, CTEs, JOINs, set operations and DML). While a list is active,
    constructs a ``schema.table`` list cannot describe are also rejected: quoted identifiers,
    inline comments, cross-database references, ``SHOW``, table-valued functions and dynamic SQL.
    Agents querying a restricted connection must send unquoted, comment-free SQL. This is an
    application-level guardrail and not a substitute for least-privilege database permissions.

Breaking changes
~~~~~~~~~~~~~~~~

* ``Enforce SQLToolset allowed_tables on queries, not just discovery (#68487)``

Features
~~~~~~~~

* ``common.ai: Park approval reviews in awaiting_input on Airflow 3.3+ (#68489)``
* ``Add spec_file support to PydanticAIHook.create_agent (#67788)``
* ``Return common.ai SQLToolset errors to the agent so it self-corrects (#68117)``
* ``Allow DESCRIBE/SHOW in common.ai SQLToolset read-only queries (#68102)``
* ``Support multi-schema introspection in common.ai SQLToolset (#68103)``
* ``Add token_provider for short-lived MCP auth in common.ai (#68104)``
* ``Add code mode (Monty sandbox) to common.ai AgentOperator (#68407)``
* ``Add 'message_history' to 'AgentOperator' for multi-turn agent sessions (#68648)``

Bug Fixes
~~~~~~~~~

* ``Fix 'LlamaIndexEmbeddingOperator' returning 'vector=None' for every chunk (#68491)``
* ``Verify durable cached agent steps match the request before replay (#68372)``

Misc
~~~~

* ``Access AgentRunResult.usage as a property in common.ai logging (#68405)``
* ``Bump pydantic-ai-slim>=1.99.0 (#68105)``

Doc-only
~~~~~~~~

* ``Explain the agent tool boundary in common.ai security docs (#68404)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Bump vite (#68580)``
   * ``Fix common.ai example DAGs failing to parse without the sql extra (#68497)``
   * ``Informatica provider: Add SQL auto-lineage and selective lineage control (#66612)``
   * ``Rename task_store/asset_store to task_state_store/asset_state_store (#68438)``
   * ``[main] Upgrade important CI environment (#68163)``
   * ``Improve AIP progress tracker example for accuracy (#68037)``
   * ``Fix XCom deserialization of Pydantic models in LangChain 10-K example (#67930)``

Breaking change: operators with ``output_type=<BaseModel subclass>``
(``LLMOperator``, ``LLMAgentOperator``, ``LLMFileAnalysisOperator``, and
their ``@task.llm`` / ``@task.agent`` / ``@task.llm_file_analysis`` decorators)
now return the Pydantic model instance through XCom instead of dumping it to
a ``dict``, on Airflow versions whose worker registers operator-declared output
classes for deserialization. Downstream tasks should type-hint the model class
(``def downstream(result: MyModel)``) and use attribute access (``result.field``)
instead of subscript access. The output class must be defined at **module scope**
and bound to an attribute matching its ``__name__``; classes that are nested,
dynamically built, or otherwise non-importable by ``qualname`` cannot be
re-imported and will fail to deserialize at the consumer.

The worker walks the loaded DAG and registers each declared class before any
task runs, so same-DAG downstream tasks (including mapped ``.expand(...)``
producers) deserialize the model without any configuration change. The UI XCom
viewer renders the value via the ``stringify`` path and works without
configuration (it shows ``module.MyModel@version=1(field=value,...)`` rather than
a pretty form). Cross-DAG ``xcom_pull`` consumers still need the class qualified
name added to ``[core] allowed_deserialization_classes`` -- the consumer DAG's
worker only loads its own DAG. On Airflow versions whose worker does not register
declared classes, the operators dump to ``dict`` instead.

0.4.0
.....

.. note::
    This release changes the return type of ``LLMOperator``, ``LLMAgentOperator`` and
    ``LLMFileAnalysisOperator``: structured output is now returned through XCom as Pydantic
    model instances instead of plain ``dict`` objects. Downstream tasks that consume these
    XCom values must be updated accordingly. As this provider is still pre-1.0, the breaking
    change ships in a minor release.

Breaking changes
~~~~~~~~~~~~~~~~~~

* ``Return Pydantic model instances through XCom for structured output (#67644)``

Features
~~~~~~~~

* ``Add 'OpenTelemetry' tracing for 'common.ai' Pydantic AI agents (#67792)``
* ``Add a bridge to expose 'common.ai' toolsets as LangChain tools (#67791)``
* ``Add Agent Skills support to the Common AI provider (#67786)``
* ``Accept Sequence[UserContent] in common.ai TaskFlow decorators (#67389)``
* ``Add LlamaIndex operators to common.ai provider (#67121)``
* ``Add 'DocumentLoaderOperator' to 'common.ai' provider (#67120)``
* ``Add 'Langchain' hook to 'common-ai' provider (#67192)``

Bug Fixes
~~~~~~~~~

* ``Register operator-declared XCom classes from a worker-side DAG walk (#67875)``
* ``common-ai: Honour serialize_output=True on LLMFileAnalysisOperator (#67858)``

Misc
~~~~

* ``Bump common.ai floor to pydantic-ai-slim>=1.71.0 and document capabilities passthrough (#67444)``
* ``Remove further findings from positional session check (#67712)``
* ``Add prek hook to enforce HTTPException is imported from fastapi (#67367)``
* ``Add prek hook enforcing the "example" tag on example DAGs (#67354)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add SEC 10-K analysis example using LangChain for Common.ai provider (#67727)``
   * ``Add SEC 10-K financial analysis example DAG using LlamaIndex for Common.ai provider (#67671)``
   * ``Add AIP progress tracker example DAG for common.ai provider (#67660)``
   * ``[main] CI: Upgrade important CI environment (#67593)``
   * ``[main] CI: Upgrade important CI environment (#67313)``
   * ``Prevent durable storage tests from leaking hook lineage (#67252)``
   * ``Fix LangChain hook tests failing when langchain is not installed (#67237)``

0.3.0
.....

Features
~~~~~~~~

* ``Add 'LLMRetryPolicy' to common-ai provider (#65451)``

Bug Fixes
~~~~~~~~~

* ``Update dependencies to fix dependabot alarms in providers.common.ai (#66628)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``[main] CI: Upgrade important CI environment (#66843)``

0.2.0
.....

Features
~~~~~~~~

* ``Add UsageLimits support to common.ai operators (#66248)``

Doc-only
~~~~~~~~

* ``Add Configuration Reference docs page to Common AI provider (#66024)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Use contextlib.suppress instead of try-except-pass in providers (#66178)``
   * ``[main] CI: Upgrade important CI environment (#66068)``
   * ``fix: update dependencies to fix dependabot alarms in providers.common.ai (#66244)``
   * ``[main] CI: Upgrade important CI environment (#65933)``
   * ``Providers wave 2026-04-21 (#65614)``
   * ``Providers wave 2026-04-21``

0.1.1
.....

Misc
~~~~

* ``Update dependencies to address Dependabot security alarms in providers.common.ai (#65048)``
* ``Bump vite (#64799)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``[main] CI: Upgrade important CI environment (#65521)``
   * ``Isolate non-provider mypy hooks per distribution with dedicated .build/ venvs (#65492)``
   * ``Simple LLM scenario example based on the Airflow survey data (#65172)``

0.1.0
.....

.. note::
  This release of provider is only available for Airflow 3.0+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.
