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


Dag-level Retry via on_failure_callback
=======================================

Airflow's built-in retry mechanism operates at the **task** level: each task has its own
``retries`` and ``retry_delay``, and only that single task is re-run on failure.

Tasks should ideally be designed to be **idempotent at the task level** so that the
built-in per-task retry mechanism is sufficient. That is the recommended starting point,
and most workflows should not need anything beyond it.

This how-to is for the cases where, even with task-level idempotency in place, you still
want a coarser unit of retry — effectively a *Dag-level retry*. For example, a multi-step
pipeline whose intermediate state is hard to clean up partially, so re-running from the
beginning is safer than re-running a single failed task.

It shows a pattern that approximates Dag-level retry by combining the
:doc:`on_failure_callback </administration-and-deployment/logging-monitoring/callbacks>`
with the public Airflow REST API: when a task fails, the callback calls the
Clear endpoint to clear the failed Dag run, so the scheduler will re-run it.

This is a **recipe** built on top of existing primitives, not a new feature. It
is opinionated and has trade-offs; read :ref:`Caveats <clear-retry-caveats>`
carefully before using it.

A more general construct for this — sometimes referred to as a
*transactional task group* — is being discussed as a possible additional
feature on the Airflow dev list. If this would benefit your use case, please
join that discussion rather than rely on this recipe long-term.


When this pattern fits
----------------------

Use it when:

- Your unit of work is naturally a Dag run, not a single task.
- Re-running tasks is **idempotent** — running them again does not produce
  duplicate side effects, double-charges, or inconsistent external state.
- A bounded number of full re-runs is acceptable in terms of time and cost.

Avoid it when:

- Tasks have non-idempotent side effects (sending emails, charging payments,
  posting to non-deduplicated APIs) and you have not made them idempotent.
- The retry should be transparent to downstream Dags or assets — clearing a Dag
  run produces a fresh attempt, not a retry of the original.
- The simple per-task ``retries`` setting already covers the failure modes you
  see in practice.


How it works
------------

1. A task fails after exhausting its task-level retries.
2. The task's ``on_failure_callback`` runs in the dag processor.
3. The callback decides whether another Dag-level attempt is allowed (using a
   counter you maintain — see :ref:`Limiting the number of retries
   <clear-retry-limiting>` below).
4. If yes, the callback calls the Airflow REST API to clear the failed Dag run.
5. The scheduler picks up the cleared task instances and re-runs them.

The REST endpoint used is ``POST /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/clear``.


Prerequisites
-------------

- Authenticated access to the Airflow REST API from the dag processor, with permission
  to clear Dag runs on the target Dag.
- Network reachability from the dag processor to the API server.

.. note::

   ``on_failure_callback`` only fires when the failure occurs through normal
   task execution. State changes performed manually (UI, CLI) do **not** invoke
   it.


Basic example: clear the failed Dag run
---------------------------------------

The following example clears the entire Dag run when any task in it fails after
exhausting its task-level retries. ``retries=0`` is set on the task so that the
callback fires on the first failure and the *whole Dag run* takes over the
retry responsibility.

This minimal version always clears on failure. **Do not run it as-is in
production** — it can loop forever if a task keeps failing. Add an attempt
limit before deploying; see :ref:`Limiting the number of retries
<clear-retry-limiting>` below.

.. code-block:: python

    from urllib.parse import quote

    import requests

    from airflow.sdk import DAG, task

    AIRFLOW_API_BASE = "https://airflow.example.com/api/v2"  # your deployment's API base


    def clear_dag_run_on_failure(context):
        dag_run = context["dag_run"]
        dag_id_path = quote(dag_run.dag_id, safe="")
        run_id_path = quote(dag_run.run_id, safe="")
        response = requests.post(
            f"{AIRFLOW_API_BASE}/dags/{dag_id_path}/dagRuns/{run_id_path}/clear",
            headers={"Authorization": "Bearer <token>"},
            json={"dry_run": False},
            timeout=30,
        )
        response.raise_for_status()


    with DAG(
        dag_id="example_dag_level_retry",
        default_args={
            "retries": 0,  # let the Dag-level retry take over
            "on_failure_callback": clear_dag_run_on_failure,
        },
    ):

        @task
        def step_one(): ...

        @task
        def step_two(value): ...

        step_two(step_one())


.. _clear-retry-limiting:

Limiting the number of retries
------------------------------

A naive callback that always clears the run will loop forever if a task keeps
failing. ``dag_run.conf`` is set when the run is created and is *not* mutated
by clearing, so it cannot be used as a retry counter — you must track attempts
yourself. A simple approach is a Variable scoped to the run:

.. code-block:: python

    from airflow.sdk import Variable


    def _attempts_key(dag_id: str, run_id: str) -> str:
        return f"dag_run_attempts::{dag_id}::{run_id}"


    def _read_attempts(dag_id: str, run_id: str) -> int:
        return int(Variable.get(_attempts_key(dag_id, run_id), default=0))


    def _bump_attempts(dag_id: str, run_id: str) -> int:
        new_value = _read_attempts(dag_id, run_id) + 1
        Variable.set(_attempts_key(dag_id, run_id), str(new_value))
        return new_value

Use ``_read_attempts`` before deciding to clear, then ``_bump_attempts`` right
before calling the API. Reset the counter when the Dag run finishes — both on
success and on give-up — so the Variables do not accumulate:

.. code-block:: python

    def cleanup_attempts(context):
        dag_run = context["dag_run"]
        Variable.delete(_attempts_key(dag_run.dag_id, dag_run.run_id))

Wire ``cleanup_attempts`` to ``on_success_callback`` at the Dag level, and call
it from the give-up branch of your ``on_failure_callback`` before returning.

.. note::

   This Variable-based counter is **not** atomic across concurrent callbacks.
   If your Dag has parallel branches that may all fail at once, two callbacks
   could each read the same value and both decide to clear. In practice the
   second clear is a no-op (the first one already reset the run), but you may
   see duplicate API calls. For stricter guarantees, use a backend with
   compare-and-swap semantics.


.. _clear-retry-caveats:

Caveats
-------

- **``UP_FOR_RETRY`` wait between Dag-level retries.** A failed task waits in
  ``UP_FOR_RETRY`` for ``retry_delay`` (default 5 minutes) before transitioning
  to ``FAILED`` and triggering the next clear, so each Dag-level retry is
  delayed by at least that interval.
- **Idempotency is your responsibility.** Every cleared task will run again.
  Anything that produced an external side effect on the failed attempt — a
  row written, a message sent, a file uploaded — will be produced again
  unless your task is idempotent (e.g. uses upserts, deduplication keys, or
  conditional writes).
- **Loop hazard.** A bug in the counter or in the failing task can cause an
  infinite clear-and-retry loop. Always cap the attempts and alert on the cap
  being hit.
- **Cost.** A Dag-level retry re-runs every cleared task from scratch. Make
  sure the time and resource cost is acceptable, and keep the attempt cap low.
- **Running tasks.** If your Dag has parallel branches, one task can fail while
  other tasks in the same Dag run are still running. Design the Dag and its
  side effects so that clearing the run while other work is in progress is safe.
- **Token lifetime.** Tokens issued by most auth managers expire (e.g. the
  simple auth manager's JWT defaults to 24 hours). A static token stored in a
  Variable will silently 401 after expiration; the loop guard prevents bad
  behavior but Dag-level retry stops working. For long-running deployments,
  refresh the credential periodically or use a longer-lived service credential.
