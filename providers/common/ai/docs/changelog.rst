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
* ``Add branch_descriptions to LLMBranchOperator so the model reads what each branch means (#73367)``
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
