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

.. _howto:hitl_review:

Human-in-the-loop (HITL) review for agents
==========================================

HITL Review adds an interactive feedback loop to agentic operators. After the
LLM Agent produces an initial output, a human reviewer can **approve**, **reject**, or
**request changes** through a chat UI. The review ends on a terminal action, or when
``hitl_timeout`` or ``max_hitl_iterations`` is reached. On Airflow 3.3+ the task waits
for the reviewer in the ``awaiting_input`` state and holds no worker or pool slot
while it waits.

This document describes the architecture, workflow, API, XCom schema, and usage.

Overview
--------

.. seealso::
    This page covers the iterative review loop on ``AgentOperator``. For a one-shot
    approve, edit or reject gate on ``LLMOperator`` and its subclasses, see
    :doc:`approval_gates`.

**Components**

- **HITL Review Plugin**: FastAPI app mounted at ``/hitl-review`` on the
  Airflow API server. Provides REST endpoints and a chat UI for reviewers.

**Storage**: The review session, every output and every piece of feedback is
stored in XCom on the task instance. The worker writes the session and the agent
outputs; the plugin writes human feedback and actions. On Airflow 3.3+ the
conversation the agent needs to regenerate is also kept in the task state store
while the task waits.

**Compatibility**: Requires Airflow 3.1+. Uses the Task SDK execution model
where workers communicate via the Execution API and XCom; the plugin runs on
the API server and accesses the metadata database.

**How the task waits**

- **Airflow 3.3+**: each review round is a Human-in-the-loop request on the task
  instance, and the task parks in the ``awaiting_input`` state until the reviewer
  answers it. A parked task holds no worker slot and no pool slot, so a review
  that takes hours costs nothing while it waits. When the reviewer acts in the
  chat UI, the plugin answers the request and the task is scheduled again; the
  next round (regenerating with the feedback, or returning the approved output)
  runs in a new worker process. ``hitl_poll_interval`` is not used.
- **Airflow 3.1 and 3.2**: the task stays ``running`` and polls the human action
  XCom every ``hitl_poll_interval`` seconds, holding its worker slot for the
  whole review.

Because each round is a Human-in-the-loop request, a parked review also appears
on the **Required Actions** page. It can be answered there with *Approve*,
*Request changes* (with feedback) or *Reject*, but the output and the earlier
rounds are only shown in the chat UI.

.. note::
   On Airflow 3.3+ each round is a new run of the task, so ``execution_timeout``
   applies to each round separately and ``hitl_timeout`` is what bounds the review
   as a whole. ``pre_execute`` and ``on_execute_callback`` also run once per round.

   The plugin on the API server resumes the parked task, so upgrade the provider on
   the API server no later than on the workers. A worker on the new provider with an
   API server on the old one parks, and the old plugin's answers never resume it.

Workflow
--------

The operator and the plugin never talk to each other directly. On Airflow 3.3+
each round parks the task and the reviewer's answer schedules it again:

.. mermaid::

    sequenceDiagram
        participant Op as Operator (worker)
        participant A as Airflow (XCom, HITL request, task state)
        participant P as API server / plugin
        participant H as Reviewer

        Op->>A: open HITL request, push agent_session + agent_output_N
        Op->>A: park in awaiting_input (worker slot released)
        H->>P: open chat UI, submit action
        P->>A: write feedback XCom, answer the HITL request
        A->>Op: task scheduled again, resumes with the answer
        Op->>Op: handle the answer (see below), then park for the next round

On Airflow 3.1 and 3.2 the operator stays running and polls the
``airflow_hitl_review_human_action`` XCom instead of parking.

The reviewer's answer resolves to one of five outcomes:

.. mermaid::

    flowchart TD
        A[Read human_action] --> B{action}
        B -->|approve| C[Return output]
        B -->|reject| D[Raise HITLRejectException]
        B -->|changes_requested| E[regenerate_with_feedback]
        E --> F["Push agent_output_N<br/>status: pending_review"]
        F -.loop.-> A
        B -->|"iteration &ge; max_hitl_iterations"| G["Push status:<br/>max_iterations_exceeded"]
        G --> H[Raise HITLMaxIterationsError]
        B -->|hitl_timeout elapsed| I["Push status:<br/>timeout_exceeded"]
        I --> J[Raise HITLTimeoutError]

Using HITL review with ``AgentOperator``
----------------------------------------

Enable the review loop with ``enable_hitl_review=True``:

.. code-block:: python

    from airflow.providers.common.ai.operators.agent import AgentOperator
    from airflow.providers.common.ai.toolsets.sql import SQLToolset
    from datetime import timedelta

    AgentOperator(
        task_id="summarize",
        prompt="Summarize the sales data",
        llm_conn_id="openai",
        toolsets=[SQLToolset(db_conn_id="postgres")],
        enable_hitl_review=True,
        hitl_timeout=timedelta(hours=4),
    )

**Parameters**

- ``enable_hitl_review``: When ``True``, the operator enters the review loop
  after the first generation. Default ``False``.
- ``max_hitl_iterations``: Maximum outputs the reviewer can see (1 = initial
  output plus subsequent regenerations). When the reviewer requests changes at
  iteration ``>= max_hitl_iterations``, the task fails with
  ``HITLMaxIterationsError`` without running the LLM. For example, ``5`` allows
  changes at iterations 1 to 4; the fifth output must be either approved or
  rejected. Default ``5``.
- ``hitl_timeout``: Maximum wall-clock time for all review rounds combined,
  including the time spent regenerating. ``None`` = no wall-clock timeout. Once it
  passes the task fails with ``HITLTimeoutError`` (on Airflow 3.3+ the scheduler
  resumes the parked task to raise it), and ``retries`` apply as for any failure.
  When polling, the task also fails, re-raising the XCom error, if pulling the human
  action XCom fails 10 times in a row.
- ``hitl_poll_interval``: Seconds between XCom polls while waiting for a
  human response on Airflow 3.1 and 3.2. Not used on Airflow 3.3+, where the
  task parks instead. Default ``10``.

**Accessing the chat UI**: The chat loads as a React plugin on the task
instance page. Use the **HITL Review** extra link on the task instance, or
navigate to
``/dags/{dag_id}/runs/{run_id}/tasks/{task_id}/plugin/hitl-review``.

**Example Dag**

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent.py
    :language: python
    :start-after: [START howto_operator_agent_hitl_review]
    :end-before: [END howto_operator_agent_hitl_review]

REST API
--------

The plugin exposes a FastAPI app at ``/hitl-review``. Base URL:

.. code-block:: text

    {AIRFLOW_BASE_URL}/hitl-review

**Common query parameters** (where applicable):

- ``dag_id``: Dag ID.
- ``run_id``: Dag run ID.
- ``task_id``: Task ID.
- ``map_index``: Map index for mapped tasks. Use ``-1`` for non-mapped tasks or index for dynamic mapping.
- ``region_id`` and ``region_index``: Select one loop pass or one mapped slot. Use the ``region_id`` and
  ``region_index`` of the task instance as returned by the public task instance endpoints. Send them
  together; ``region_index`` without ``region_id`` is rejected. Only supported on Airflow 3.4 or later;
  earlier hosts return 400 when either is sent. A task inside a loop addressed without them returns 400,
  and a selection that matches more than one task instance returns 409.

Endpoints
^^^^^^^^^

.. list-table::
   :header-rows: 1
   :widths: 20 20 60

   * - Method
     - Path
     - Description
   * - GET
     - ``/health``
     - Liveness check. Returns ``{"status": "ok"}``.
   * - GET
     - ``/sessions/find``
     - Find the feedback session for a task instance. Returns
       :class:`HITLReviewResponse` or 404 if no session.
   * - POST
     - ``/sessions/feedback``
     - Request changes. Body: ``{"feedback": "..."}``. Session must be
       ``pending_review``. Returns updated session.
   * - POST
     - ``/sessions/approve``
     - Approve the current output. Session must be ``pending_review``.
   * - POST
     - ``/sessions/reject``
     - Reject the output. Session must be ``pending_review``.

Response model: HITLReviewResponse
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    {
        "dag_id": str,
        "run_id": str,
        "task_id": str,
        "status": "pending_review"
        | "changes_requested"
        | "approved"
        | "rejected"
        | "max_iterations_exceeded"
        | "timeout_exceeded",
        "iteration": int,
        "max_iterations": int,
        "prompt": str,
        "current_output": str,
        "conversation": [{"role": "assistant" | "human", "content": str, "iteration": int}],
        "task_completed": bool,
    }

XCom keys and storage
---------------------

All keys use the prefix ``airflow_hitl_review_``.

.. list-table::
   :header-rows: 1
   :widths: 35 15 50

   * - Key
     - Writer
     - Value
   * - ``airflow_hitl_review_agent_session``
     - Worker
     - ``AgentSessionData``: status, iteration, prompt, current_output, and
       ``parked`` (``True`` when the task waits in ``awaiting_input``)
   * - ``airflow_hitl_review_human_action``
     - Plugin
     - ``HumanActionData``: action (approve|reject|changes_requested),
       feedback, iteration
   * - ``airflow_hitl_review_agent_output_1``, ``_2``, …
     - Worker
     - Per-iteration AI output (string or JSON)
   * - ``airflow_hitl_review_human_feedback_1``, ``_2``, …
     - Plugin, and the worker when it resumes
     - Per-iteration human feedback text

Session lifecycle
^^^^^^^^^^^^^^^^^

- **pending_review**: Awaiting human action. Plugin accepts approve, reject,
  or feedback.
- **changes_requested**: Feedback submitted; the worker is regenerating. The
  plugin does not accept new actions until the worker pushes a new output and
  status returns to ``pending_review``.
- **approved** / **rejected**: Terminal. The review has ended.

On Airflow 3.3+ the plugin also refuses (409) an action when the task's request for
the round has already been answered, for example on the Required Actions page.

Chat UI
-------

The plugin provides an interactive chat UI that loads in the task instance page.
The UI:

- Fetches session and conversation from the REST API
- Displays the current output and feedback history
- Submits approve, reject, or feedback via POST endpoints
