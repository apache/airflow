Wires ``CHECKPOINTED`` (introduced in #66402) through the supervisor →
API server path so the DB row actually transitions to ``checkpointed``
when an operator raises ``AirflowTaskCheckpointed``.

- ``CHECKPOINTED`` is added to ``STATES_SENT_DIRECTLY`` so the supervisor
  does not route it through the terminal-state ``finish()`` endpoint.
- The supervisor's ``TaskState`` handler calls a new
  ``client.task_instances.checkpoint()`` method when the worker reports
  ``CHECKPOINTED``.
- The new client method PATCHes ``/task-instances/{id}/state`` with
  ``TITargetStatePayload(state=CHECKPOINTED)`` — the same shape
  ``DEFERRED`` and ``UP_FOR_RESCHEDULE`` already use.

``checkpoint_data`` persistence is intentionally not added in this PR.
The right shape (XCom-backed, new TaskInstance JSON column, separate
metadata table) is the open AIP-96 question; this PR ships the
state-transition wiring so the DB lands at ``checkpointed`` while the
persistence question is debated.
