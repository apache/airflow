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

Anthropic Operators
===================

The Anthropic provider runs the `Claude Message Batches API
<https://docs.claude.com/en/docs/build-with-claude/batch-processing>`__ from Airflow.
Message Batches process many ``messages.create`` requests asynchronously at 50% of
standard cost; most complete within an hour, with a 24-hour SLA — a good fit for
Airflow's deferrable execution model.

.. note::

    For interactive, single-call or agentic LLM workloads, prefer the vendor-agnostic
    ``apache-airflow-providers-common-ai`` provider with ``model="anthropic:claude-opus-4-8"``.
    This provider focuses on the batch/async surface and direct SDK access that the agent
    abstraction does not model.

.. _howto/operator:AnthropicBatchOperator:

AnthropicBatchOperator
----------------------

:class:`~airflow.providers.anthropic.operators.batch.AnthropicBatchOperator` submits a
Message Batch and waits for it to reach the terminal ``ended`` status. In deferrable mode it
releases the worker slot while an
:class:`~airflow.providers.anthropic.triggers.batch.AnthropicBatchTrigger` polls for
completion.

The operator returns the **batch ID only**. Pull the per-request results with
:meth:`~airflow.providers.anthropic.hooks.anthropic.AnthropicHook.stream_batch_results` and
persist them to object storage — results can be very large and must not be pushed to XCom.
Results are retained for 29 days after the batch is created.

Parameters
""""""""""

* ``requests`` — a list of ``{"custom_id": str, "params": {...}}`` dicts, where ``params`` is a
  ``messages.create`` payload (``model``, ``max_tokens``, ``messages``, ...).
* ``model`` — default model id applied to any request whose ``params`` omits ``model``. When
  unset, those requests fall back to the connection's ``default_model`` (``extra['model']``). Set
  it to choose the batch's model once instead of repeating it in every request; a request that
  sets its own ``model`` always wins, so a batch can still mix models.
* ``conn_id`` — the Anthropic connection ID (default ``anthropic_default``).
* ``deferrable`` — run in deferrable mode (defaults to the ``operators.default_deferrable`` config).
* ``poll_interval`` — seconds between status checks, in both the synchronous and deferrable paths.
* ``timeout`` — seconds to wait for a terminal status; defaults to 24 hours (the batch SLA).
* ``wait_for_completion`` — if ``False``, return the batch ID immediately after submission.
* ``fail_on_partial_error`` — if ``True``, fail the task when any request errored or expired.
  Defaults to ``False`` (succeed and log a warning so successful results are not discarded).

.. warning::

    A task retry re-submits a **new** batch. Prefer ``retries=0`` on this task. The submitted
    ``batch_id`` is pushed to XCom under key ``batch_id`` immediately after submission, so a
    crashed run never loses track of an in-flight batch.

Example
"""""""

.. exampleinclude:: /../tests/system/anthropic/example_anthropic_batch.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_anthropic_batch]
    :end-before: [END howto_operator_anthropic_batch]

.. _howto/sensor:AnthropicBatchSensor:

AnthropicBatchSensor
--------------------

:class:`~airflow.providers.anthropic.sensors.batch.AnthropicBatchSensor` waits for an
already-submitted batch (by ``batch_id``) to reach a terminal status. Pair it with
``AnthropicBatchOperator(wait_for_completion=False)`` for a fire-and-forget submit followed
by a re-entrant await — because the sensor only polls an existing batch, retrying it never
re-submits, which sidesteps the "retry creates a new batch" hazard of a waiting submit task.

It applies the same terminal-status policy as the operator (skip on full cancellation,
``fail_on_partial_error`` to fail on errored/expired requests) and supports ``deferrable``
mode via the shared trigger.

.. code-block:: python

    from airflow.providers.anthropic.operators.batch import AnthropicBatchOperator
    from airflow.providers.anthropic.sensors.batch import AnthropicBatchSensor

    submit = AnthropicBatchOperator(
        task_id="submit",
        requests=requests,
        wait_for_completion=False,  # fire-and-forget; recommend retries=0
    )
    wait = AnthropicBatchSensor(
        task_id="wait",
        batch_id="{{ ti.xcom_pull(task_ids='submit') }}",
        deferrable=True,
    )
    submit >> wait

.. _howto/operator:AnthropicAgentSessionOperator:

AnthropicAgentSessionOperator
-----------------------------

:class:`~airflow.providers.anthropic.operators.agent.AnthropicAgentSessionOperator` runs a
`Managed Agents <https://platform.claude.com/docs/en/managed-agents/overview>`__ session:
Anthropic runs the agent loop server-side while the worker drives a session and waits for it
to finish. Unlike the ``common.ai`` provider (a *local* pydantic-ai loop), the loop and its
tool-execution sandbox run on Anthropic's infrastructure; the worker only orchestrates.

**Agents and environments are created once** (via
:meth:`~airflow.providers.anthropic.hooks.anthropic.AnthropicHook.create_agent` /
:meth:`~airflow.providers.anthropic.hooks.anthropic.AnthropicHook.create_environment`, the
``ant`` CLI, or the Console) and referenced by ID on every run — the operator never creates
an agent per task. Configure the agent for **autonomous** operation (no client-side custom
tools or ``always_ask`` permission) so the session reaches ``idle`` (turn complete) rather
than blocking on input the operator cannot supply.

Provide exactly one of ``message`` (a single user turn) or ``outcome`` (a
``user.define_outcome`` rubric the agent iterates against until satisfied). The operator
returns the **session ID only**; pull artifacts the agent wrote to ``/mnt/session/outputs/``
afterwards via the Files API (``scope_id=<session_id>``).

Parameters
""""""""""

* ``agent_id`` / ``environment_id`` — IDs of a pre-created agent and environment.
* ``message`` — a single user message to start the session (mutually exclusive with ``outcome``).
* ``outcome`` — a ``user.define_outcome`` payload (``description`` + required ``rubric``,
  optional ``max_iterations``); mutually exclusive with ``message``.
* ``conn_id`` — the Anthropic connection ID (default ``anthropic_default``).
* ``deferrable`` — run in deferrable mode (defaults to ``operators.default_deferrable``).
* ``poll_interval`` — seconds between session status checks.
* ``timeout`` — seconds to wait for a terminal status; defaults to 24 hours.
* ``vault_ids`` — vault IDs providing MCP/credential access to the session.
* ``session_resources`` — files, GitHub repos, or memory stores to mount (forwarded to
  ``sessions.create`` as ``resources``; renamed to avoid the reserved ``BaseOperator.resources``).
* ``session_kwargs`` — extra keyword arguments forwarded to ``sessions.create``.

.. note::

    Completion is detected accurately for both modes. A ``message`` run inspects the
    terminal ``session.status_idle`` event's ``stop_reason`` (correlated against the
    kickoff event): ``end_turn`` succeeds; ``requires_action`` and ``retries_exhausted``
    raise an error. An ``outcome`` run is judged from the ``outcome_evaluations`` verdict.
    The agent must still be configured for autonomous operation (no client-side custom
    tools / ``always_ask``).

.. exampleinclude:: /../tests/system/anthropic/example_anthropic_agent.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_anthropic_agent_session]
    :end-before: [END howto_operator_anthropic_agent_session]
