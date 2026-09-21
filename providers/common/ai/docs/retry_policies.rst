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

LLM Retry Policies
===================

.. note::
    Requires Airflow >= 3.3.0.

``LLMRetryPolicy`` asks a model which kind of failure a task hit, then retries or
fails the task according to a table you own. It works with any LLM provider
supported by pydantic-ai (OpenAI, Anthropic, Bedrock, Vertex, Ollama, etc.), and
with a classifier model such as TypeSafe's Jev, which answers the question in a few
hundred milliseconds and reports how sure it is.

For the core retry policy concepts, see :doc:`apache-airflow:core-concepts/tasks`.
If the task also needs to survive a worker crash without losing its progress,
see :ref:`apache-airflow:concepts-resumable-tasks-retry-policies`.

Setup
-----

1. Install the provider with the LLM backend you need:

   .. code-block:: bash

       pip install 'apache-airflow-providers-common-ai[anthropic]'

2. Create a connection (``Admin > Connections``):

   - **Connection Id**: ``pydanticai_default``
   - **Connection Type**: ``Pydantic AI``
   - **Password**: Your API key
   - **Extra**: ``{"model": "anthropic:claude-haiku-4-5"}``

Usage
-----

.. code-block:: python

    from airflow.providers.common.ai.policies.retry import LLMRetryPolicy
    from airflow.sdk.definitions.retry_policy import RetryAction, RetryRule
    from datetime import timedelta

    llm_policy = LLMRetryPolicy(
        llm_conn_id="pydanticai_default",
        timeout=30.0,  # max seconds to wait for LLM response
        fallback_rules=[  # used when the LLM call fails or the answer is under its bar
            RetryRule(exception=ConnectionError, action=RetryAction.RETRY, retry_delay=timedelta(seconds=10)),
            RetryRule(exception=PermissionError, action=RetryAction.FAIL),
        ],
    )


    @task(retries=5, retry_policy=llm_policy)
    def call_external_api(): ...

How it works
------------

When a task fails, ``LLMRetryPolicy``:

1. Sends the exception message to the configured LLM. By default, the message
   is first masked through Airflow's secrets masker (see ``redactor`` below)
   and truncated to ``max_exception_length`` characters before it is added
   to the prompt.
2. The model picks one of the policy's ``categories``. It sees each category's
   name and description in the output schema and nothing else about it, and it
   cannot answer with a name outside the set.
3. The policy looks the category up in the same table and returns RETRY, with
   that category's delay, or FAIL. If the policy has a confidence bar and the
   model's answer is under it, the answer is discarded instead (see
   `Confidence`_ below).
4. The decision is logged in the task logs and, on a RETRY, written to the task
   instance's ``retry_reason``, as one line such as
   ``category=network confidence=0.91 threshold=0.60 action=retry delay=10s``.

This classification call is a separate LLM request, made by ``LLMRetryPolicy``
itself rather than by an operator -- it is not subject to an operator's
``usage_limits``, and it runs on every task failure regardless
of any cost cap configured on the failing task. It is bounded by ``timeout``
and ``max_exception_length``, but not by a cost limit.

If the LLM call fails (provider down, timeout, bad credentials), or the model
cannot produce one of the categories even after pydantic-ai re-prompts it, or
its answer is under the confidence bar, the policy falls back to
``fallback_rules`` if configured, or to the task's standard retry behaviour.
The category's action and delay are not applied in that case, and the decision's
reason starts with ``LLM classification not applied (model_error)``,
``(below_threshold)`` or ``(missing_confidence)`` so a ``retry_reason`` read later
is not mistaken for a classifier decision or a plain rule match.

This policy decides *between* attempts. Failing over to another vendor *within*
an attempt is a separate mechanism on the connection — see
:doc:`provider_fallback`, which also sets out how the two layers compose.

Categories
----------

``categories`` maps a category name to an
:class:`~airflow.providers.common.ai.policies.retry.ErrorCategory`: what
failures belong there (the ``description`` the model reads), whether it is
retried, after what ``delay``, and how sure the model has to be
(``min_confidence``, covered below). Everything the model is told about a
category, and everything the policy does with it, sits in that one entry, so
the two cannot drift apart.

The default,
:data:`~airflow.providers.common.ai.policies.retry.DEFAULT_CATEGORIES`, is an
example taxonomy, not the taxonomy:

.. list-table::
   :header-rows: 1
   :widths: 20 55 25

   * - Category
     - What belongs there
     - Action
   * - ``rate_limit``
     - API throttling or a quota exceeded.
     - Retry after 60s
   * - ``network``
     - Transient connectivity issue: connection reset, DNS, TLS handshake.
     - Retry after 10s
   * - ``transient``
     - Temporary issue likely to resolve on its own.
     - Retry after 30s
   * - ``auth``
     - Credentials invalid, expired, or missing permissions.
     - Fail
   * - ``data``
     - Schema validation, type mismatch, or bad input data.
     - Fail
   * - ``resource``
     - Resource not found or unavailable, such as a missing table or bucket.
     - Fail
   * - ``permanent``
     - Problem that will not resolve without a code or configuration change.
     - Fail

Pass your own to change any of it. It **replaces** the default rather than
merging into it, so include every category you want offered -- or spread the
default and edit what you need:

.. code-block:: python

    from dataclasses import replace
    from datetime import timedelta

    from airflow.providers.common.ai.policies.retry import DEFAULT_CATEGORIES, ErrorCategory, LLMRetryPolicy

    LLMRetryPolicy(
        llm_conn_id="pydanticai_default",
        categories={
            **DEFAULT_CATEGORIES,
            # The table is created upstream, so a missing one is worth another look.
            "resource": replace(DEFAULT_CATEGORIES["resource"], retry=True, delay=timedelta(minutes=5)),
        },
    )

Or write a taxonomy for your stack. A Snowflake pipeline has kinds of failure the
seven defaults can only approximate, and naming them is what lets the policy act
on them differently:

.. code-block:: python

    snowflake_policy = LLMRetryPolicy(
        llm_conn_id="pydanticai_default",
        categories={
            "queued": ErrorCategory(
                "Statement queued or a concurrency limit reached; the warehouse is busy.",
                delay=timedelta(seconds=120),
            ),
            "warehouse_suspended": ErrorCategory(
                "The warehouse is suspended and will auto-resume.", delay=timedelta(seconds=30)
            ),
            "token_expired": ErrorCategory(
                "A JWT or session token expired; the token rotates on its own.", delay=timedelta(seconds=30)
            ),
            "schema_drift": ErrorCategory(
                "A referenced column, table or view does not exist; a person has to fix the schema.",
                retry=False,
            ),
        },
    )

Write descriptions as the boundary between categories: what belongs here and what
does not. That is the whole of what the model reads about a category; the name
alone gives it very little, and two categories with similar names and no
descriptions are indistinguishable to it. A pick is relative: the model chooses
the best fit among the categories offered, not whether any fits. If "none of
these" or "not enough in the message to tell" is a real outcome for your
pipeline, add a category for it (``retry=True`` with no delay keeps the task's
own behaviour) rather than hoping the model refuses. When you change a
description or the set of categories, treat the confidence values you measured
before as stale: the distribution the model returns is over the options it was
given.

A ``delay`` is ``timedelta``, not seconds, and is used as-is: a task's own
``max_retry_delay`` does not clamp it. ``delay=None`` (the default) means no
override, so the task's own ``retry_delay`` / ``retry_exponential_backoff`` /
``max_retry_delay`` apply instead (see :doc:`apache-airflow:core-concepts/tasks`).
``retry=False`` ends the task straight away even when attempts were left, so a
wrong classification into such a category costs the task the retries it would
otherwise have had; that is what the confidence bar below is for. A bare int
delay, a negative delay, a delay on a ``retry=False`` category, an empty
description, or fewer than two categories raises when the policy is
constructed, at Dag parse time, rather than on the first task failure.

Confidence
----------

A classifier model reports how sure it is of its answer. ``min_confidence`` is
the bar that answer needs for the policy to act on it; under the bar the policy
discards the answer and takes the same path it takes when the model call fails:
``fallback_rules`` if one matches, otherwise the task's own retry behaviour. It
does not substitute a delay of its own.

Each category can carry its own bar. The stakes differ: a wrong ``transient``
costs one more attempt, while a wrong ``permanent`` costs the task every retry it
had left, so the category that ends the task deserves the higher bar.
``jev_default`` is the classifier-model connection from :doc:`classifier_models`.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_retry_policy.py
    :language: python
    :start-after: [START howto_retry_policy_classifier]
    :end-before: [END howto_retry_policy_classifier]

A category bar needs a policy bar to inherit from; setting one without the other
raises at construction. With no bar at all the policy acts on every answer, as
it always has, and the confidence is still logged.

**A missing confidence counts as an unsure one.** A text model reports no
confidence, and so does a response whose metadata was dropped along the way.
With no bar configured that changes nothing. With a bar configured, every such
answer is discarded and the fallback path decides, so swapping the connection
from a classifier model to a text model does not silently switch off a control
you set on purpose. To run a text model, remove the bar.

The confidence is a statistic on the shape of the probability distribution the
model returned: concentrated on one category is high, spread out is low. It is
not the probability that the answer is correct. Pick the bar from the confidence
values your own failures produce: run the policy with no bar first, read the
logged ``confidence=`` values per category, and set the bar where the wrong
answers start. For orientation, a calibration run of the two example taxonomies
on ``jev-1.13.0`` over 31 realistic exception messages put every correct pick at
0.89 or above and three of the four wrong picks between 0.47 and 0.69; the
fourth wrong pick was a ``permanent`` at 0.90, which is why a bar reduces wrong
actions and does not eliminate them. Pin the model version
(``typesafe:jev-1.13.0``, not ``jev-latest``): a bar tuned against one release
is not guaranteed to mean the same thing after the next. See
:doc:`classifier_models` for what these models answer well and badly.

When the connection also carries a fallback chain
--------------------------------------------------

``LLMRetryPolicy`` builds its classifier hook from ``llm_conn_id`` without passing
``fallback_conn_ids``, so if that connection's extra configures a chain (see
:doc:`provider_fallback`), the policy inherits it silently -- editing the connection changes
retry behaviour with no change to the Dag. Two things follow:

* ``timeout`` stops bounding the whole classification call. pydantic-ai applies a
  ``ModelSettings`` timeout to each model in the chain, not to the chain as a whole, so a
  30-second ``timeout`` across a three-connection chain is a 90-second worst case before the
  policy falls back to ``fallback_rules``.
* If every connection in the chain fails, the classification call raises
  ``pydantic_ai.exceptions.FallbackExceptionGroup``. ``evaluate()`` still degrades to
  ``fallback_rules`` correctly -- it catches the broad ``Exception``, and an exception group is
  one -- so the only cost here is that the classification is wasted.

Separately, and regardless of this policy: if the connection **the task itself** uses to call
the LLM (for example ``llm_conn_id`` on ``LLMOperator`` or ``AgentOperator``) carries a fallback
chain, the exception the task raises once that chain is exhausted is
``pydantic_ai.exceptions.FallbackExceptionGroup``, not the last provider's own exception.
``RetryRule`` matches with ``isinstance``, so a rule written as
``RetryRule(exception=ModelHTTPError, ...)`` -- in ``fallback_rules`` here or in a plain
``ExceptionRetryPolicy`` -- stops matching. Match ``pydantic_ai.exceptions.FallbackExceptionGroup``
explicitly as well; its only common ancestor with ``ModelAPIError`` is ``Exception``, too
broad to write a rule against. The original per-model exceptions are still available on
``FallbackExceptionGroup.exceptions``, but ``RetryRule`` only compares the top-level
exception type, so a rule set that told 429s apart from 400s collapses into one rule once
the chain is in play.

What the model can and cannot do
--------------------------------

The model answers one question: which kind of failure is this. It is
given no tools and there is no way to attach any, so it cannot run code, call
an API, read a connection, or reach your data. Beyond your ``instructions``,
it sees only the exception's class name, the exception message (after
redaction and truncation), how many attempts are left, and the category names
and descriptions. The prompt says ``attempt {try_number} of {max_tries}``, so
the model knows the limit and not just where it is right now. That only moves
the category -- an instruction like "after two attempts treat an expired token as
``auth`` rather than ``transient``" works because the model can see which
attempt this is.

It returns the category name and nothing else. It does not decide whether to
retry, it does not choose the delay, and it does not explain itself -- the
first two come from the category's entry in the worker process, and the
explanation is the generated line in the task log, which says what mattered
(the category, the confidence, the bar, the action) rather than what the model
felt like saying. A model cannot return a category the policy does not
recognize, and it cannot return a category paired with an action that
contradicts it.

The decision line is only recorded on a RETRY. It is written to the task
instance's ``retry_reason`` (truncated to 500 characters), then cleared once the
next attempt starts running. On a FAIL it is not written anywhere -- it only
shows up in the task log.

RETRY cannot give a task more attempts than ``retries`` allows. FAIL ends the
task straight away even when attempts were left.

Custom instructions
-------------------

The default instructions say only that the model is classifying a failed
pipeline task and should pick the best-fitting category; the categories and
their meanings travel in the output schema, from ``categories``. Override
``instructions`` to teach the model your stack's error strings when a
description is not enough on its own. Pass ``categories`` alongside: custom
``instructions`` with the default table raises a ``UserWarning`` at Dag parse
time, because a prompt that names categories or delays of its own no longer
defines either.

.. code-block:: python

    SNOWFLAKE_INSTRUCTIONS = (
        "You are classifying failures from Snowflake-backed data pipelines.\n"
        "- 'Statement queued' or 'concurrency limit' -> queued\n"
        "- '000606' or 'is suspended' -> warehouse_suspended\n"
        "- 'JWT token' or 'session token' with 'expired' -> token_expired\n"
        "- '002003' or 'does not exist' -> schema_drift\n"
    )

    snowflake_policy = LLMRetryPolicy(
        llm_conn_id="pydanticai_default",
        instructions=SNOWFLAKE_INSTRUCTIONS,
        categories={...},  # the same names the instructions use
        fallback_rules=[
            RetryRule(
                exception=ConnectionError,
                action=RetryAction.RETRY,
                retry_delay=timedelta(seconds=30),
            ),
        ],
    )


    @task(retries=5, retry_policy=snowflake_policy)
    def query_snowflake(): ...

When writing custom instructions:

- Use the category names from ``categories`` as-is. The model is constrained to
  them, so a name you invent cannot come back. A model that insists on one
  anyway is re-prompted once by pydantic-ai and then gives up, which lands the
  task on ``fallback_rules`` or on its own retry behaviour, having billed two
  calls. Offering a name the schema rejects is therefore worse than offering
  none.
- Be concrete with examples (``"'Warehouse suspended' -> warehouse_suspended"``)
  rather than vague rules ("treat warehouse issues as recoverable").
- Do not spell out delays or "do NOT retry" instructions. The model does not
  decide either one, and telling it to only spends tokens. Put them in the
  category instead.
- A classifier model sends ``instructions`` as the question it scores the
  exception text against, not as rules it follows step by step, so a long rubric
  buys less there than a better description on each category does.

Parameters
----------

.. list-table::
   :header-rows: 1
   :widths: 20 15 65

   * - Parameter
     - Default
     - Description
   * - ``llm_conn_id``
     - (required)
     - Airflow connection ID for the LLM provider.
   * - ``model_id``
     - None
     - Override the model from the connection (e.g., ``"openai:gpt-4o-mini"``).
   * - ``instructions``
     - (built-in)
     - Custom system prompt for error classification. Teaches the model your
       error strings; the categories themselves come from ``categories``.
   * - ``fallback_rules``
     - None
     - List of ``RetryRule`` objects used when the LLM call fails or the answer
       is under its confidence bar.
   * - ``timeout``
     - 30.0
     - Max seconds to wait for the LLM response before falling back.
   * - ``categories``
     - ``DEFAULT_CATEGORIES``
     - Mapping of category name to ``ErrorCategory(description, retry, delay,
       min_confidence)``: what the model chooses between and what the policy
       does with each answer. **Replaces** the default mapping rather than
       merging into it. At least two categories; validated at construction.
   * - ``min_confidence``
     - None
     - The confidence, from 0 to 1, the model's answer needs for the policy to
       act on it. Under the bar, or with a bar set and no confidence reported,
       the answer is discarded and ``fallback_rules`` then the task's own retry
       behaviour apply. A category's own ``min_confidence`` overrides it for
       that category.
   * - ``redactor``
     - None (uses ``redact_registered_secrets``)
     - Callable ``(str) -> str`` applied to the exception's string
       representation before it is added to the classification prompt. The
       default only masks values already registered via ``mask_secret()``
       (e.g. connection passwords Airflow captured while resolving the
       failing task's connections) -- it is not general-purpose PII
       detection and will not catch arbitrary sensitive strings that were
       never registered as secrets. Passing a custom callable **replaces**
       the default masker entirely rather than stacking on top of it.
   * - ``redact_exception``
     - True
     - Whether to redact the exception's string representation before it is
       added to the classification prompt. Set to ``False`` to disable
       redaction entirely. Raises ``ValueError`` at construction time if
       combined with an explicit ``redactor``.
   * - ``max_exception_length``
     - 4096
     - Maximum number of characters of the (already redacted) exception
       message included in the prompt. Longer messages are truncated with a
       trailing ``"... (truncated)"`` marker. Must be a positive integer.

Custom redactors
----------------

The default ``redactor`` only masks values already registered with Airflow's
secrets masker via ``mask_secret()``. It does not detect free-text PII --
email addresses, customer names, account numbers -- that were never
registered as secrets. If your task's exception messages can contain that
kind of data, supply your own ``redactor`` callable. It **replaces** the
default masker rather than running in addition to it, so combine your own
logic with :func:`~airflow.providers.common.ai.policies.retry.redact_registered_secrets`
yourself if you still want known-secret masking too:

.. code-block:: python

    import re

    from airflow.providers.common.ai.policies.retry import redact_registered_secrets

    EMAIL_RE = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")


    def redact_emails_and_secrets(message: str) -> str:
        return redact_registered_secrets(EMAIL_RE.sub("<email>", message))


    llm_policy = LLMRetryPolicy(
        llm_conn_id="pydanticai_default",
        redactor=redact_emails_and_secrets,
        max_exception_length=2048,  # keep long tracebacks from inflating token cost
    )

To disable redaction entirely (for example, if you are certain your
exception messages contain no sensitive data and need the raw text for
accurate classification), pass ``redact_exception=False``:

.. code-block:: python

    LLMRetryPolicy(llm_conn_id="pydanticai_default", redact_exception=False)

Local LLM support
-----------------

By default, the built-in ``redactor`` already masks known secrets before the
exception data reaches the LLM provider. For environments where exception
data must not leave your own infrastructure at all -- even in masked form --
point to a local model via Ollama or vLLM instead, so the classification
never crosses the network boundary. See :ref:`howto/self_hosted_models` for
general self-hosted connection setup:

.. code-block:: python

    LLMRetryPolicy(
        llm_conn_id="ollama_local",  # host=http://localhost:11434
        model_id="ollama:llama3.2",
    )
