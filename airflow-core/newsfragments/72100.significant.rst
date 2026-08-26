Clearing a task now discards its task state store entries

Clearing a task instance discards its ``task_state_store`` entries, so the next attempt starts from
the beginning instead of resuming from a checkpoint or reconnecting to an external job recorded by
the attempt that was cleared.

Retries are unaffected. They keep task state exactly as before, which is what crash recovery relies
on. Only a deliberate clear discards.

**Why**

Clearing means "run this again". A checkpoint records how far a task got, not what it got there
with, so resuming after the code or the upstream data changed left work done before the fix in place
and silently mixed it with the corrected work. Clearing a task whose external job had already
succeeded was worse: the operator read the stored result back and returned in seconds having run
nothing.

**Keeping the old behaviour**

Pass ``keep_task_state=True`` to the clear task instances endpoint, or tick "keep task state" in the
clear dialog. Use it when nothing about the inputs or the code changed and the task should carry on
where it stopped, or when an external job is still running and you want the next attempt to
reconnect rather than submit a duplicate.

Operators with durable execution are worth particular attention. Clearing a *failed* task never runs
``on_kill``, so an external job that outlived its worker is still running, and discarding the stored
id means submitting a second one. The same applies to operators configured to leave their job alive
on kill, such as ``KubernetesPodOperator`` with ``on_kill_action="keep_pod"``.

* Types of change

  * [ ] Dag changes
  * [ ] Config changes
  * [x] API changes
  * [ ] CLI changes
  * [x] Behaviour changes
  * [ ] Plugin changes
  * [ ] Dependency changes
  * [ ] Code interface changes
