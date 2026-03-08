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

Human-in-the-Loop (HITL) Review For Agentic Operators
#####################################################

HITL Review adds an interactive feedback loop to agentic operators. After the
LLM Agent produces an initial output, a human reviewer can **approve**, **reject**, or
**request changes** through a chat UI. The operator blocks until a
terminal action, or until a timeout is reached.

This document describes the architecture, workflow, API, XCom schema, and usage.


Overview
========

**Components**

- **HITLReviewMixin** — Operator mixin that runs the review loop inside
  :meth:`execute`. Polls XCom for human actions and drives regeneration.

- **HITL Review Plugin** — FastAPI app mounted at ``/hitl-review`` on the
  Airflow API server. Provides REST endpoints and a chat UI for reviewers.

- **utils.hitl_review** — Shared models, exceptions, and XCom keys used by
  both the mixin (worker) and plugin (API server).

**Storage** — All state is stored in XCom on the running task instance. The
worker writes session and agent outputs; the plugin writes human feedback and
actions. Both sides read and write the same keys.

**Compatibility** — Requires Airflow 3.1+. Uses the Task SDK execution model
where workers communicate via the Execution API and XCom; the plugin runs on
the API server and accesses the metadata database.

.. important::
   **Worker slot usage** — Each HITL task **holds a worker slot for the entire
   review duration** (until approve, reject, or timeout or max_iterations). The operator polls
   XCom with ``time.sleep``; it does not defer. With a 10-second poll interval
   and review times of 30+ minutes, the worker is occupied doing nothing most
   of the time. Size your worker pool accordingly.

**Implementation: XCom polling vs deferral** — This implementation uses XCom
polling with ``time.sleep`` rather than the deferral/Triggerer pattern (as used
by the standard provider's ``HITLOperator``). Deferral would free the worker
during review but adds cross-process coordination complexity: the agent state
(message history, tool results) lives in the worker process and would need to
be serialized and restored across defer/resume. XCom polling keeps the flow
simple and keeps all agent context in-process. Future versions may have different approaches.


Workflow
========

.. code-block:: text

    [Operator]                    [API Server / Plugin]
         |                                 |
         | 1. Generate output              |
         | 2. Push session + output_1      |
         |    to XCom                      |
         |                                 |
         | 3. Poll XCOM_HUMAN_ACTION       |
         |    (sleep, poll, repeat)        |
         |                                 | 4. Reviewer opens chat UI,
         |                                 |    submits feedback / approve / reject
         |                                 | 5. Plugin writes human action
         |                                 |    to XCom
         | 6. Read action from XCom        |
         |                                 |
         | 7a. approve → return output     |
         | 7b. reject  → raise HITLRejectException
         | 7c. changes_requested           |
         |     → regenerate_with_feedback  |
         |     → push output_2, loop to 3  |
         | 7d. max_iterations reached      |
         |     → raise HITLMaxIterationsError


Using HITL Review with AgentOperator
====================================

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

- ``enable_hitl_review`` — When ``True``, the operator enters the review loop
  after the first generation. Default ``False``.
- ``max_hitl_iterations`` — Maximum generate–review cycles. After this, the
  task fails with ``HITLMaxIterationsError``. Default ``5``.
- ``hitl_timeout`` — Maximum wall-clock time to wait for all review rounds.
  ``None`` = no timeout (blocks until a terminal action).
- ``hitl_poll_interval`` — Seconds between XCom polls while waiting for a
  human response. Default ``10``.

**Extra link** — When HITL review is enabled, the task instance page shows an
**HITL Review** link that opens the chat window for that task.

**Example DAG**

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent.py
    :language: python
    :start-after: [START howto_operator_agent_hitl_review]
    :end-before: [END howto_operator_agent_hitl_review]


REST API
========

The plugin exposes a FastAPI app at ``/hitl-review``. Base URL:

.. code-block:: text

    {AIRFLOW_BASE_URL}/hitl-review

**Common query parameters** (where applicable):

- ``dag_id`` — DAG ID.
- ``run_id`` — DAG run ID.
- ``task_id`` — Task ID.
- ``map_index`` — Map index for mapped tasks. Use ``-1`` for non-mapped tasks.
  If the URL contains an unreplaced placeholder like ``{MAP_INDEX}``, invalid
  values default to ``-1``.


Endpoints
---------

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
   * - GET
     - ``/chat``
     - Serve the interactive chat HTML for a session. Requires
       ``dag_id``, ``run_id``, ``task_id``, ``map_index``.
   * - GET
     - ``/chat-by-task``
     - Same as ``/chat`` but with ``map_index`` handling for dynamic
       task mapping.


Response model: HITLReviewResponse
----------------------------------

.. code-block:: python

    {
        "dag_id": str,
        "run_id": str,
        "task_id": str,
        "status": "pending_review" | "changes_requested" | "approved" | "rejected",
        "iteration": int,
        "max_iterations": int,
        "prompt": str,
        "current_output": str,
        "conversation": [{"role": "assistant" | "human", "content": str, "iteration": int}],
        "task_completed": bool,
    }


XCom keys and storage
=====================

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
-----------------

- **pending_review** — Awaiting human action. Plugin accepts approve, reject,
  or feedback.
- **changes_requested** — Feedback submitted; worker is regenerating (or
  polling for the next action). Plugin does not accept new actions until the
  worker pushes a new output and status returns to ``pending_review``.
- **approved** / **rejected** — Terminal. Worker has exited the loop.


Chat UI
=======

The plugin provides an interactive chat UI that loads as an iframe in the task
instance page. The UI:

- Fetches session and conversation from the REST API
- Displays the current output and feedback history
- Submits approve, reject, or feedback via POST endpoints
