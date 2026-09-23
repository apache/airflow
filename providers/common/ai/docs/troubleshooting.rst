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

.. _howto/troubleshooting:

Troubleshooting
===============

The errors below are the ones a first Dag most often hits. Each names the message you
see, the cause, and the fix.

Model and connection errors
---------------------------

``No model specified for connection '...'``
    The connection has no ``model`` in its extra and the operator did not pass
    ``model_id``. Set the **Model** field on the connection (``provider:model`` form) or
    pass ``model_id`` to the operator. See :doc:`connections/pydantic_ai`.

``Connection '...' has no default model provider, so the bare model name '...' cannot be resolved``
    The generic ``pydanticai`` connection type needs a ``provider:`` prefix on the model
    name, for example ``openai:gpt-5`` rather than ``gpt-5``. Add the prefix, or use a
    vendor connection type (:doc:`connections/pydantic_ai_azure`,
    :doc:`connections/pydantic_ai_bedrock`, :doc:`connections/pydantic_ai_vertex`), which
    supply their own platform.

``'...' is not a provider pydantic-ai recognizes``
    The text before the first ``:`` in the model name is not a pydantic-ai provider.
    Check it for a typo. If the vendor's own model id contains a ``:`` (Bedrock-style
    version suffixes, for example), use the matching vendor connection type instead of
    the generic one.

An ``ImportError`` for ``pydantic_ai.models.<vendor>`` or the vendor SDK
    The provider is installed without the extra for that vendor. Install it, quoting the
    package name so the brackets survive the shell:

    .. code-block:: bash

        pip install "apache-airflow-providers-common-ai[openai]"

    :doc:`installation` lists the extras.

``A fallback chain is configured for '...' but no model is set``
    ``fallback_conn_ids`` on the connection needs an explicit primary model. Set the
    **Model** field on the primary connection or ``model_id`` on the operator. A model
    taken from an agent spec file cannot be wrapped in a fallback chain. See
    :doc:`provider_fallback`.

``Fallback connection '...' resolves to ..., which is not a PydanticAIHook``
    Every entry in ``fallback_conn_ids`` must be one of the ``pydanticai`` connection
    types. Chains are also not resolved recursively, so a fallback connection may not
    declare its own ``fallback_conn_ids``; list every vendor directly on the primary.

Operator construction errors
----------------------------

These raise while the Dag file is parsed, so the whole file fails to import. A mapped
task (``.expand()``) is constructed at run time instead, so there the same error surfaces
as a task failure.

``require_approval=True needs Airflow 3.1+`` / ``DecisionPolicy(on_uncertain='review') needs Airflow 3.1+`` / ``approval_assigned_users needs Airflow 3.1+`` / ``Human in the loop functionality needs Airflow 3.1+``
    Human-in-the-loop review, whether through ``require_approval``,
    ``DecisionPolicy(on_uncertain="review")`` or ``enable_hitl_review``, needs Airflow 3.1
    or later. Upgrade the core, or use ``on_uncertain="fail"`` and drop the review flags
    on an older core. See :doc:`approval_gates` and :doc:`hitl_review`.

``durable=True and enable_hitl_review=True cannot be used together`` / ``durable=True and code_mode=True cannot be used together``
    Durable replay assumes a stable step order across attempts, which neither a human
    review loop nor code mode provides. Pick one. See :doc:`durable_execution`.

``message_history and enable_hitl_review=True cannot be used together``
    The post-review transcript is not recoverable today, so the operator refuses rather
    than silently dropping the reviewed turns. See :doc:`message_history`.

``code_mode=True requires the 'code-mode' extra``
    Install ``apache-airflow-providers-common-ai[code-mode]``. See :doc:`code_mode`.

``... does not support decision_policy yet``
    Only ``LLMOperator`` and ``LLMBranchOperator`` honor a ``DecisionPolicy`` with a
    confidence bar. The SQL, schema-compare and file-analysis operators run their own
    ``execute`` and reject one at construction; use ``require_approval=True`` there for an
    unconditional review.

``on_approval_timeout=... needs a review path ... and a positive approval_timeout to fire``
    ``on_approval_timeout`` other than ``"fail"`` only makes sense when a review can open
    (``require_approval=True`` or a reviewing ``decision_policy``) **and**
    ``approval_timeout`` is set to a positive ``timedelta``. Set both or leave the default.

Run-time errors
---------------

``Agent model must be set when durable=True``
    The agent was built without a model, usually because the connection has no
    ``model`` and no ``model_id`` was passed. Durable execution needs the model resolved
    up front so that replayed steps can be matched against their fingerprints. Fix the connection as described
    above.

``durable=True`` on Airflow below 3.3 fails with a ``ValueError`` about ``durable_cache_path``
    On cores older than 3.3 the step cache lives in object storage and
    ``[common.ai] durable_cache_path`` must be set. On 3.3 and later the task state store
    is used and the option is ignored. See :doc:`durable_execution`.

A structured ``output_type`` arrives downstream as a string or fails to deserialize
    The Pydantic class must be defined at module scope under its own ``__name__`` so the
    worker can register it for XCom deserialization, and a consumer in a *different* Dag
    needs the class added to ``[core] allowed_deserialization_classes``. Pass
    ``serialize_output=True`` to receive a plain ``dict`` instead. See
    :doc:`structured_output`.

A review task waits for a long time
    That is expected: the task is waiting for a reviewer. An approval gate on an LLM operator
    releases its worker slot while it waits (it pauses as awaiting input on Airflow 3.3+, and
    defers to the triggerer on older cores); a HITL review on ``AgentOperator`` polls from the
    worker and holds its slot. Set ``approval_timeout`` or ``hitl_timeout`` so an unattended
    review cannot wait forever. See :doc:`approval_gates` and :doc:`hitl_review`.

The provider returns a rate limit or is down
    Three features answer this at different layers. Airflow's own ``retries`` re-run the
    task. :doc:`provider_fallback` fails over to another vendor inside one attempt.
    :doc:`retry_policies` lets a model classify the failure and decide whether a retry is
    worth it. They compose; each page says where it sits among the others.

Still stuck
-----------

Every operator logs a post-run summary with the model name, token usage and the tool
call sequence, and ``AgentOperator`` logs each tool call as it happens. Turn the task
log level to ``DEBUG`` to see tool arguments and the model output. :doc:`observability`
covers exporting the same information as OpenTelemetry traces.
