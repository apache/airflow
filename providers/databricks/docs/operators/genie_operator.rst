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



.. _howto/operator:DatabricksGenieOperator:

Ask a Databricks Genie space
============================

Use :class:`~airflow.providers.databricks.operators.genie.DatabricksGenieOperator` to ask a Genie space
one question as a standalone task. The operator returns the same bounded result as
:ref:`DatabricksGenieHook <howto/databricks-genie>` through XCom: the space, conversation and message
IDs, the message status, the text answer, generated queries and capped query results.

.. exampleinclude:: /../../databricks/tests/system/databricks/example_databricks_genie.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_databricks_genie]
    :end-before: [END howto_operator_databricks_genie]

By default the operator starts a new conversation. Set ``conversation_id`` to ask a follow-up in an
existing conversation. The operator never deletes or cancels conversations, so a conversation you
supply stays yours.

Set ``deferrable=True`` to release the worker while Genie works. The trigger only reads the message
status; the worker fetches the answer and query results when the task resumes, so the trigger event
never carries Genie output.

.. exampleinclude:: /../../databricks/tests/system/databricks/example_databricks_genie.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_databricks_genie_deferrable]
    :end-before: [END howto_operator_databricks_genie_deferrable]

``timeout`` limits how long one try waits for the answer. Timing out does not cancel the message.

Durable execution
-----------------

On Airflow 3.3 and later the operator is durable by default. It stores the conversation and message
IDs in the :doc:`task state store <apache-airflow:core-concepts/task-state-store>` before it waits, so
a retry after a worker crash, a timeout or a failed trigger reconnects to the same message instead of
asking again. See :doc:`apache-airflow:core-concepts/resumable-tasks` for how resumable tasks work.
On retry the operator checks the stored message:

* A message that is still running is awaited again.
* A completed message returns its answer without asking again.
* A message that failed, was cancelled or whose result expired is asked again in a new message.
* A stored message that no longer exists (Genie answers 404), or that was asked for a different
  space, conversation or question, is asked again.

Set ``durable=False`` to ask again on every try. On Airflow versions before 3.3 there is no task state
store, so every try asks again; the operator logs the IDs right after it submits the question so you
can attach to the message by hand.

Clearing the task
^^^^^^^^^^^^^^^^^

Retries always keep the task state. Clearing does not by default: clearing a task instance from the
UI or the REST API (so ``airflowctl`` too) discards it from Airflow 3.4, and every other clear, such as
``airflow tasks clear`` or clearing a whole Dag run, does so from Airflow 3.5. To keep it, tick
**Keep task state and resume** in the Clear dialog, or send ``"keep_task_state": true`` in the
request. From Airflow 3.5, ``airflow tasks clear --keep-task-state`` keeps it too. On Airflow 3.3 a
clear keeps the task state like a retry. What the next attempt does then depends on what the task
state holds:

.. list-table::
   :header-rows: 1
   :widths: 30 35 35

   * - Task state holds
     - Default clear
     - Clear that keeps task state
   * - A message that is still running
     - Asks the question again in a new message. Genie cannot cancel the first one, so both run.
     - Waits for the running message.
   * - A completed message
     - Asks the question again in a new message.
     - Returns the stored answer without asking.
   * - A failed, cancelled or expired message
     - Asks the question again in a new message.
     - Asks the question again in a new message.
   * - An unconfirmed submission (see below)
     - Asks the question again, so it may be asked twice.
     - Refuses to ask until you attach or set ``resubmit_unconfirmed=True``.
   * - A message hidden behind a 403 (see below)
     - Asks the question again in a new message.
     - Fails the same way.

With ``[state_store] clear_on_success`` on, the stored IDs are deleted when the task succeeds, so
clearing a succeeded task asks again even when task state is kept.

Attaching to a message
^^^^^^^^^^^^^^^^^^^^^^

Set ``conversation_id`` and ``message_id`` to wait for a message that already exists instead of
asking. The operator returns that message's answer and replaces any stored IDs with it. Both fields
are templated. A fixed ``message_id`` in the Dag applies to every later run, which would then return
the same old answer without asking, so pass the IDs for one run through params instead and trigger
or clear that run with them. Empty values ask the question as usual:

.. exampleinclude:: /../../databricks/tests/system/databricks/example_databricks_genie.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_databricks_genie_attach]
    :end-before: [END howto_operator_databricks_genie_attach]

Unconfirmed submissions
^^^^^^^^^^^^^^^^^^^^^^^

Genie's start-conversation and create-message APIs have no idempotency key. When Databricks does not
confirm a submission (a timeout, a connection error, a 5xx response, or a response without IDs) the
message may or may not exist, and listing the conversation cannot reliably tell. The operator then
fails with ``AirflowFailException``, so Airflow does not retry it and the next attempt only comes from
a clear. The durable state remembers the unconfirmed submission, but a default clear discards it and
asks the question again without any check. Inspect the Genie space before clearing, then either:

* attach to the message you found with ``conversation_id`` and ``message_id`` as shown above; the
  operator waits for it and returns its answer without asking,
* clear with task state kept and set ``resubmit_unconfirmed=True`` to accept that the question may be
  asked twice, or
* clear with the default to ask again, accepting the same risk.

A clear that keeps task state and changes neither setting refuses to ask again.

A request that Databricks rejects with a 4xx response created nothing, so Airflow's normal retries
apply to it.

Messages hidden behind a 403
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Genie may also answer 403 for a message that does not exist, so it does not reveal which resources
are there. The operator treats only 404 as a missing message, so a stored message that Genie hides
behind a 403 fails the try instead of being asked again, and retries fail the same way because they
keep the stored IDs. First check that the connection can still read the Genie space and fix its
permissions if it cannot. If access is fine, the stored message is gone: a default clear discards the
stored IDs and asks again, or you can attach to a message you know exists. A new Dag run starts with
empty task state and is not affected.

Cancellation
------------

Genie has no API to cancel a chat message, so killing or timing out the task leaves the message
running in Databricks. With durable execution the next try reconnects to it. With ``durable=False``,
or on Airflow before 3.3 where there is no task state store, the next try asks the question again in
a new message.
