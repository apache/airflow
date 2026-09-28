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

Provider fallback
=================

A single ``llm_conn_id`` gives a task one provider. When that provider is down, the task
fails and retries into the same outage. ``fallback_conn_ids`` gives the connection an
ordered list of other connections to try, so a provider outage moves to the next vendor
inside the same task attempt.

Configure it on the connection
------------------------------

Put the chain in the primary connection's extra:

.. code-block:: json

    {
      "model": "openai:gpt-5",
      "fallback_conn_ids": ["anthropic_prod", "bedrock_dr"]
    }

Every entry is an Airflow connection ID, resolved through the hook registered for its own
connection type. A chain can therefore mix vendors whose credentials live in different
connection fields (``pydanticai`` for OpenAI, ``pydanticai_bedrock`` for a Bedrock
standby) without the Dag knowing anything about either.

That is the point of configuring it here rather than in Dag code: the Dag keeps naming one
connection, and whoever administers the connections owns the failover topology. Changing a
standby provider is a connection edit, not a Dag deployment.

A *bare* model name (e.g. ``"gpt-5"`` rather than ``"openai:gpt-5"``) is forwarded down
the chain as a logical model name: each connection that has no ``model`` of its own
resolves that name against its own platform, so one bare name can reach a primary and
every fallback without repeating it per connection. It does not matter where the primary's
name comes from -- the ``Model`` field on its connection and a ``model_id`` on the operator
or hook are forwarded alike. A fallback with its own ``model`` in
extra always uses that instead -- this is how a fallback pins a spelling the forwarded
name would not produce, such as Bedrock's region-prefixed ``us.anthropic.`` model ids. A
name that already pins a platform (its segment before the first ``:`` is itself a
recognized provider, e.g. ``"openai:gpt-5"``) is *not* forwarded; a fallback with no
``model`` of its own still raises "no model specified" rather than trying a prefixed name
meant for a different provider.

A bare name with no ``:`` of its own (e.g. ``"gpt-5"``) forwards to any fallback
regardless of platform, since nothing about the spelling is vendor-specific. A bare name
that itself contains a ``:`` -- a vendor's own native model id, such as Bedrock's
version-suffixed ``"us.anthropic.claude-opus-4-6-v1:0"`` -- only forwards to a fallback on
the *same* platform: that spelling is only meaningful on the vendor that produced it, so a
Bedrock primary's native id reaches a Bedrock fallback but not an Azure one. For example,
a Bedrock primary with ``fallback_conn_ids: ["bedrock_dr"]`` forwards
``"us.anthropic.claude-opus-4-6-v1:0"`` to ``bedrock_dr`` unchanged; the same primary with
``fallback_conn_ids: ["azure_dr"]`` does not forward it to ``azure_dr`` at all, and
``azure_dr`` raises "no model specified" unless its own extra sets a ``model``. See
:doc:`connections/pydantic_ai_azure`, :doc:`connections/pydantic_ai_bedrock` and
:doc:`connections/pydantic_ai_vertex` for how each vendor connection resolves a bare name.

Configure it on the operator
-----------------------------

``fallback_conn_ids`` is also a parameter on
:class:`~airflow.providers.common.ai.operators.llm.LLMOperator`,
:class:`~airflow.providers.common.ai.operators.agent.AgentOperator`, their subclasses,
and the matching ``@task.llm`` / ``@task.agent`` decorators -- mirroring ``model_id``,
which is settable at the same two layers:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_fallback.py
    :language: python
    :dedent: 0
    :start-after: [START howto_llm_fallback_operator_argument]
    :end-before: [END howto_llm_fallback_operator_argument]

The operator argument overrides the connection's extra field, and passing ``[]``
explicitly disables a chain configured there -- ``None`` (the default) reads whatever
the connection says. Use this when a task should own its own failover order instead of
inheriting it from however the connection is configured.

Configure it in code
--------------------

:class:`~airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook` also takes the list
directly, which is what a task that constructs the hook itself (rather than through an
operator) should use:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_fallback.py
    :language: python
    :dedent: 0
    :start-after: [START howto_llm_fallback_hook_argument]
    :end-before: [END howto_llm_fallback_hook_argument]

The argument wins over the connection's extra, and passing ``[]`` explicitly disables a
chain configured there. Omitting it entirely (``None``) means "use whatever the connection
says", which is why the two are not interchangeable.

Where this sits among the retry layers
--------------------------------------

Three mechanisms handle failure at different time scales, and they compose rather than
replace each other:

.. list-table::
   :header-rows: 1
   :widths: 25 40 35

   * - Scope
     - Mechanism
     - Handles
   * - Within one model call
     - ``fallback_conn_ids``
     - This vendor's API is returning errors; ask the next one (any ``ModelAPIError``,
       transient or not)
   * - Within one task attempt
     - ``timeout`` in pydantic-ai's ``ModelSettings``
     - This vendor is slow rather than down
   * - Across task attempts
     - :doc:`retry_policies` (including ``LLMRetryPolicy``)
     - Whether this failure is worth retrying at all

A chain does not remove the need for the outer layers. It covers the case where another
vendor can answer the same prompt now; a bad prompt, an exhausted quota on every vendor, or
a permanent data error still has to be decided by the retry policy.

Adding a chain changes what the retry layer sees. When every connection in the chain
fails, the exception the task raises is ``pydantic_ai.exceptions.FallbackExceptionGroup``,
not the last provider's own exception, so retry rules matched against a provider-specific
exception type stop matching. Before adding a chain to a connection that Dags already use,
read :doc:`retry_policies` -- the section "When the connection also carries a fallback
chain" spells out what to check.

Costs to know before configuring a long chain
---------------------------------------------

**The timeout multiplies.** pydantic-ai applies a ``ModelSettings`` timeout to each model
in the chain, not to the chain as a whole. A 30-second timeout across three connections is
a 90-second worst case for one call.

**There is no circuit breaker.** Every call tries the primary first. During an outage each
task instance pays the primary's timeout again before failing over, so 500 mapped tasks pay
it 500 times. Keeping the primary's timeout short bounds both of these.

**Chains are not resolved recursively.** If a connection listed as a fallback declares its
own ``fallback_conn_ids``, resolution fails with an error rather than following it. List
every provider directly on the primary; a flat chain is the one you can read off a single
connection.

**A malformed prompt walks the whole chain.** Failover triggers on pydantic-ai's
``ModelAPIError`` family, which includes ``ModelHTTPError`` -- raised for any 4xx as well as
5xx. A malformed prompt is the one error every connection in the chain shares: the same
request body goes to each of them, so all reject it alike before the task finally sees the
failure -- N requests, N timeouts, and N billable calls for a request that was never going to
succeed. An expired key does not cost the same way -- it is per-connection, so the next
connection in the chain presents its own credentials and, if they are still valid, answers
normally; that is the chain doing its job, not a repeated failure. A misspelled model name is
shared across the chain only in the narrower case where the name is bare and every fallback it
reaches configures no ``model`` of its own: a bare name that itself embeds a ``:`` (a vendor's
native id) only forwards to a fallback on the *same* platform, and a name that already pins a
platform is never forwarded at all -- see *Configure it on the connection* above for the full
forwarding rules. Keep chains short, and put deterministic rules for errors like these in
:doc:`retry_policies`.

**Airflow's task-level** ``retries`` **multiplies on top of the chain.** A task with
``retries=5`` gets up to six attempts -- the initial attempt plus five retries -- before
Airflow marks it failed, and each attempt walks the whole chain again if every connection
is still down. Against the three-connection chain in the JSON extra under *Configure it on
the connection* above (the primary plus two fallbacks), that is up to 18 upstream calls, not
3, before the task is finally marked failed.

**A bad fallback connection fails the whole chain, including a healthy primary.** The
primary and every fallback are resolved eagerly, before any of them is called, so a
misspelled fallback ``conn_id`` or a fallback connection missing its ``model`` raises
immediately -- the task never reaches the primary, even though the primary itself would
have answered fine. Run ``test_connection`` on the primary to catch this before it costs a
task; see *Verifying a chain* below.

Verifying a chain
-----------------

Two checks, neither of which requires waiting for a real outage:

*Test the connection.* ``test_connection`` on the primary resolves every connection in the
chain, so a fallback with a missing ``model`` or an unknown connection ID is reported by name
there rather than discovered mid-incident. Credential fields a provider class rejects with a
``TypeError`` are caught by the hook, which retries with the env-var-based provider
constructor and logs a warning either way; if the required env var is also missing, that
retry raises ``pydantic_ai.exceptions.UserError``, which ``test_connection`` does surface
since it wraps the whole resolution in a broad exception handler. What it cannot show is the
opposite case: the env var *is* set on the worker, the retry quietly succeeds, and
``test_connection`` reports success even though the credentials you configured on the
connection were silently ignored -- check the logs for that warning rather than relying on
``test_connection`` alone. It also does not call the provider, so a well-formed but revoked
key still passes -- that is what the drill below is for.

*Drill it.* Point the primary at an endpoint nothing listens on and run the Dag. The task
should still succeed, and the run summary in its log names the model that answered:

.. code-block:: text

    ::group::LLM run complete: model=claude-haiku-4-5-20251001, requests=1, ...

That line is how a failover is noticed at all: it reports the model that actually served
the request, not the chain. Repeat the drill whenever the topology changes.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_fallback.py
    :language: python
    :dedent: 0
    :start-after: [START howto_llm_fallback_connection_driven]
    :end-before: [END howto_llm_fallback_connection_driven]

Scope
-----

``fallback_conn_ids`` is supported only for the pydantic-ai hooks. Failover here is
pydantic-ai's ``FallbackModel``, and the other frameworks do not share that construct:
LangChain's nearest equivalent is ``Runnable.with_fallbacks()`` on the object the hook
returns, and LlamaIndex has none.
