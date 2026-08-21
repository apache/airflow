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
* ``budget`` -- spend ceiling for the session, in US dollars (``25.00``) or as the raw API
  payload (a mapping). Templated, so it can come from a Variable, a params entry, or
  an upstream XCom. See `Session budgets`_ below.
* ``session_resources`` — files, GitHub repos, or memory stores to mount (forwarded to
  ``sessions.create`` as ``resources``; renamed to avoid the reserved ``BaseOperator.resources``).
* ``session_kwargs`` — extra keyword arguments forwarded to ``sessions.create``. Setting
  ``budget`` here as well as via the ``budget`` argument is rejected.

.. note::

    Completion is detected accurately for both modes. A ``message`` run inspects the
    terminal ``session.status_idle`` event's ``stop_reason`` (correlated against the
    kickoff event): ``end_turn`` succeeds; ``requires_action``, ``retries_exhausted`` and
    ``budget_reached`` raise an error. An ``outcome`` run is judged from the
    ``outcome_evaluations`` verdict. The agent must still be configured for autonomous
    operation (no client-side custom tools / ``always_ask``).

Session budgets
"""""""""""""""

``budget`` bounds what a single session may spend. The session stops issuing new model
requests once its tracked list cost reaches the ceiling. A number or numeric string is read
as **US dollars**:

.. code-block:: python

    AnthropicAgentSessionOperator(
        task_id="research",
        agent_id="agt_...",
        environment_id="env_...",
        message="Summarise yesterday's incidents.",
        budget=25.00,  # 25.00 USD
        retries=0,
    )

The field is templated, so the ceiling can come from a Variable
(``budget="{{ var.value.max_agent_spend }}"``) and be changed without editing the Dag.
Amounts are converted through :class:`~decimal.Decimal`, never binary float, and anything
finer than a cent is rejected rather than silently rounded.

Pass a mapping instead to send the raw API payload, for a budget shape the provider has not
caught up with. Today the API accepts only ``type: "limit"`` and ``currency: "USD"``, so the
mapping form is an escape hatch for future additions rather than something needed now:

.. code-block:: python

    budget = {"type": "limit", "max_list_cost": {"amount": "2500", "currency": "USD"}}

To raise or remove a ceiling on a session that is already running, use
:meth:`~airflow.providers.anthropic.hooks.anthropic.AnthropicHook.update_session`. Only the
keywords you pass are sent, because the API distinguishes *omitted* (preserve) from
``None`` (clear):

.. code-block:: python

    hook = AnthropicHook()
    hook.update_session(session_id, budget=50.00)  # raise the ceiling
    hook.update_session(session_id, budget=None)  # remove it entirely

On a ``message`` run, a session that stops this way raises
:class:`~airflow.providers.anthropic.exceptions.AnthropicSessionBudgetExceeded`, a subclass
of ``AnthropicAgentSessionError``, so it can be caught on its own and routed to review
rather than treated as a fault.

.. warning::

    On an ``outcome`` run, completion is judged from ``outcome_evaluations`` before the idle
    event is read, so a budget stop raises nothing. The session stays non-terminal, polling
    continues until ``timeout`` (24 hours by default), and the task then fails with
    ``AnthropicAgentSessionTimeout`` -- a misleading error for a session that stopped
    deliberately. Set a shorter ``timeout`` when combining ``outcome`` with a budget.

.. note::

    On an ``outcome`` run, completion is judged from the session's ``outcome_evaluations``
    before the idle event is consulted, so a budget stop is reported as whatever verdict the
    outcome recorded and raises the generic ``AnthropicAgentSessionError``. Catch
    ``AnthropicSessionBudgetExceeded`` only on ``message`` runs.

.. warning::

    **A budget is a stop trigger, not a spend cap.** The ceiling is checked *between*
    model requests, so a request already in flight runs to completion and the session can
    finish well above the limit -- in testing, by a large multiple of a very small
    ceiling, because a single long generation overshoots before the next request can be
    blocked. Size it as a circuit breaker rather than a guarantee, and read the session's
    ``usage.list_cost`` for what was actually spent.

.. warning::

    A session also stops with ``budget_reached`` when its usage includes a model with **no
    list price**, because a budget cannot measure that spend. Raising the ceiling does not
    unblock that case; remove the budget instead.

.. warning::

    Airflow ``retries`` multiply spend. Each retry starts a **new** session with a **fresh**
    budget, so ``retries=2`` with a $25 ceiling can spend $75. Prefer ``retries=0`` on
    budgeted sessions: the operator archives a budget-stopped session, so there is no
    running session left to raise the ceiling on.

Recording what a session actually spent
"""""""""""""""""""""""""""""""""""""""

Because the ceiling is not a cap, it does not tell you the spend. The operator pushes the
session's usage to XCom under ``usage`` on **both** success and failure, so cost per Dag run
can be queried and a budget-stopped run still records what it consumed:

.. code-block:: python

    {
        "input_tokens": 827,
        "output_tokens": 17065,
        "cache_read_input_tokens": 0,
        "cache_creation": {"ephemeral_5m_input_tokens": 0, "ephemeral_1h_input_tokens": 0},
        "server_tool_use": {"web_search_requests": 0, "web_fetch_requests": 0},
        "active_seconds": 91.2,
        "list_cost": {"amount": "44", "currency": "USD"},
        "try_number": 1,
    }

``amount`` is the API's **minor-unit string** (``"44"`` is $0.44), kept as a string so no
rounding is applied to a cost figure. ``list_cost`` is ``None`` when usage includes a model
with no list price -- which is precisely when a caller has to price the run from the token
counts, so every billable dimension is reported: cache *writes* are billed above base input,
and server tool calls are billed per request. Reading usage is best effort: if it fails, the
task's real outcome is preserved and a warning is logged.

.. warning::

    Airflow clears a task's XCom at the start of every attempt, so ``usage`` holds the
    **final attempt only** and ``try_number`` records which one that was. With retries
    enabled, total spend across attempts is not recoverable from this key; sum it from the
    session records instead. This is the same scenario as the retry warning above, so
    ``retries=0`` keeps both problems away.

Configuring the agent
"""""""""""""""""""""

Agent-level settings are not operator arguments: they belong to the agent, which is created
once and referenced by ID on every run.
:meth:`~airflow.providers.anthropic.hooks.anthropic.AnthropicHook.create_agent` forwards
keyword arguments to the API unchanged, so these need no provider support.

**Pinning the inference region.** Pass ``model`` as a config object instead of a bare id to
confine inference to one region:

.. code-block:: python

    hook.create_agent(
        name="us-only-analyst",
        model={"id": "claude-opus-5", "inference_geo": "us"},
    )

An unsupported value is rejected with a 400 naming the accepted set; see `Data residency
<https://platform.claude.com/docs/en/manage-claude/data-residency>`__ for the regions
Anthropic currently serves and the workspace-level controls. When ``inference_geo`` is unset,
requests fall through to the workspace's
``default_inference_geo``. On an update, ``model`` is whole-object replacement, so omitting
``inference_geo`` clears it rather than preserving it.

The pin is re-checked against the workspace allowlist when the agent is saved, when a
session is created, and on every turn a session serves -- so narrowing the allowlist stops
running sessions, not just new ones.

In a ``multiagent`` configuration the coordinator's pin and every roster member's must all
be set to the same value, or all be unset -- a mismatch is rejected. Following both this and
the roster example below on one agent is the easy way to trip that.

**Adding an advisor.** A coordinator agent can consult a second model mid-turn by adding an
``advisor`` entry to its ``multiagent`` roster:

.. code-block:: python

    hook.create_agent(
        name="coordinator",
        model="claude-opus-5",
        multiagent={
            "type": "coordinator",
            "agents": [
                worker_agent_id,
                {"type": "advisor", "model": "claude-opus-5"},
            ],
        },
    )

``type: "coordinator"`` on the ``multiagent`` object is required and the request is rejected
without it. The roster takes 1 to 20 entries, each an agent ID
string, a versioned ``{"type": "agent", "id": ..., "version": ...}`` reference,
``{"type": "self"}`` for recursive self-invocation, or an ``advisor``. Referenced agents
must exist, must be distinct, must not be archived, and must not themselves set
``multiagent`` (depth limit 1); at most one ``self`` and at most one ``advisor``. The
advisor occupies the roster name ``anthropic.advisor``, and its model must be permitted as
an advisor for the coordinator's own model.

.. exampleinclude:: /../tests/system/anthropic/example_anthropic_agent.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_anthropic_agent_session]
    :end-before: [END howto_operator_anthropic_agent_session]
