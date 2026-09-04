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
connection fields — ``pydanticai`` for OpenAI, ``pydanticai-bedrock`` for a Bedrock
standby — without the Dag knowing anything about either.

That is the point of configuring it here rather than in Dag code: the Dag keeps naming one
connection, and whoever administers the connections owns the failover topology. Changing a
standby provider is a connection edit, not a Dag deployment.

``model_id`` is deliberately not inherited by the fallbacks. It names a model of the
primary's provider, so each fallback connection supplies its own ``model``.

Configure it in code
--------------------

:class:`~airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook` also takes the list
directly, which is what a task that owns its own failover order should use:

.. exampleinclude:: /../src/airflow/providers/common/ai/example_dags/example_llm_fallback.py
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
     - This vendor is erroring right now; ask the next one
   * - Within one task attempt
     - ``timeout`` in pydantic-ai's ``ModelSettings``
     - This vendor is slow rather than down
   * - Across task attempts
     - :doc:`retry_policies` (including ``LLMRetryPolicy``)
     - Whether this failure is worth retrying at all

A chain does not remove the need for the outer layers. It covers the case where another
vendor can answer the same prompt now; a bad prompt, an exhausted quota on every vendor, or
a permanent data error still has to be decided by the retry policy.

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

Verifying a chain
-----------------

Two checks, neither of which requires waiting for a real outage:

*Test the connection.* ``test_connection`` on the primary resolves every connection in the
chain, so a fallback with a missing ``model`` or unusable credentials is reported by name
there rather than discovered mid-incident.

*Drill it.* Point the primary at an endpoint nothing listens on and run the Dag. The task
should still succeed, and the run summary in its log names the model that answered:

.. code-block:: text

    LLM run complete: model=claude-haiku-4-5-20251001, requests=1, ...

That line is how a failover is noticed at all — it reports the model that actually served
the request, not the chain. Repeat the drill whenever the topology changes.

.. exampleinclude:: /../src/airflow/providers/common/ai/example_dags/example_llm_fallback.py
    :language: python
    :dedent: 0
    :start-after: [START howto_llm_fallback_connection_driven]
    :end-before: [END howto_llm_fallback_connection_driven]

Scope
-----

``fallback_conn_ids`` is currently supported only for the pydantic-ai hooks. Failover here
is pydantic-ai's ``FallbackModel``, and the other frameworks do not share that construct:
LangChain's nearest equivalent is ``Runnable.with_fallbacks()`` on the object the hook
returns, and LlamaIndex has none. Extending the same connection-level contract to them is
deliberately left out of this change rather than approximated.
