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
**request changes** through a chat UI. The operator blocks until a
terminal action, or until a timeout is reached, max_iterations is reached, or
polling the review XCom keeps failing.

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

**Storage**: All state is stored in XCom on the running task instance. The
worker writes session and agent outputs; the plugin writes human feedback and
actions. Both sides read and write the same keys.

**Compatibility**: Requires Airflow 3.1+. Uses the Task SDK execution model
where workers communicate via the Execution API and XCom; the plugin runs on
the API server and accesses the metadata database.

.. important::
   **Worker slot usage**: Each HITL task **holds a worker slot for the entire
   review duration** (until approve, reject, timeout, max_iterations, or
   repeated XCom polling failures). The operator polls
   XCom with ``time.sleep``; it does not defer. With a 10-second poll interval
   and review times of 30+ minutes, the worker is occupied for the duration.

**Why the operator does not defer**: the agent state (message history, tool
results) lives in the worker process. Polling XCom with ``time.sleep`` keeps it
there for the whole review instead of serializing and restoring it across a
defer and resume. The standard provider's ``HITLOperator`` defers because it
carries no in-process state.

Workflow
--------

The operator and the plugin never talk to each other directly — every
arrow below crosses the XCom store. The operator holds its worker slot for
the whole loop; it polls instead of deferring because the agent's message
history and tool state live in-process.

.. mermaid::

    sequenceDiagram
        participant Op as Operator (worker)
        participant X as XCom
        participant P as API server / plugin
        participant H as Reviewer

        Op->>X: push agent_session + agent_output_1
        loop until a terminal action
            Op->>X: poll airflow_hitl_review_human_action
            H->>P: open chat UI, submit action
            P->>X: write human_action + feedback
            X-->>Op: read action
            Op->>Op: handle action (see below)
        end

Once the operator reads a human action, it resolves to one of five outcomes:

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
        hitl_timeout=timedelta(minutes=30),
        hitl_poll_interval=10.0,
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
- ``hitl_timeout``: Maximum wall-clock time to wait for all review rounds.
  ``None`` = no wall-clock timeout. The task still fails, re-raising the XCom error, if polling
  the human action XCom fails 10 times in a row.
- ``hitl_poll_interval``: Seconds between XCom polls while waiting for a
  human response. Default ``10``.

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
     - ``AgentSessionData``: status, iteration, prompt, current_output
   * - ``airflow_hitl_review_human_action``
     - Plugin
     - ``HumanActionData``: action (approve|reject|changes_requested),
       feedback, iteration
   * - ``airflow_hitl_review_agent_output_1``, ``_2``, …
     - Worker
     - Per-iteration AI output (string or JSON)
   * - ``airflow_hitl_review_human_feedback_1``, ``_2``, …
     - Plugin
     - Per-iteration human feedback text

Session lifecycle
^^^^^^^^^^^^^^^^^

- **pending_review**: Awaiting human action. Plugin accepts approve, reject,
  or feedback.
- **changes_requested**: Feedback submitted; worker is regenerating (or
  polling for the next action). Plugin does not accept new actions until the
  worker pushes a new output and status returns to ``pending_review``.
- **approved** / **rejected**: Terminal. Worker has exited the loop.

Chat UI
-------

The plugin provides an interactive chat UI that loads in the task instance page.
The UI:

- Fetches session and conversation from the REST API
- Displays the current output and feedback history
- Submits approve, reject, or feedback via POST endpoints
