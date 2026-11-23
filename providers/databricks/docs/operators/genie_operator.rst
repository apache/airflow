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

Genie may also answer 403 for a message that does not exist, so it does not reveal which resources
are there. The operator treats only 404 as a missing message, so a stored message that Genie hides
behind a 403 fails the try instead of being asked again. Retrying or clearing the task fails the same
way, because the stored IDs stay in the task state for that DAG run. First check that the connection
can still read the Genie space and fix its permissions if it cannot. If access is fine, the stored
message is gone. To recover that DAG run, either set ``conversation_id`` and ``message_id`` to attach
to a message you know exists, which replaces the stored IDs, or set ``durable=False`` to ask again in
a new message. A new DAG run starts with empty task state and is not affected.

Set ``durable=False`` to ask again on every try. On Airflow versions before 3.3 there is no task state
store, so every try asks again; the operator logs the IDs right after it submits the question so you
can attach to the message by hand.

Genie's start-conversation and create-message APIs have no idempotency key. When Databricks does not
confirm a submission (a timeout, a connection error, a 5xx response, or a response without IDs) the
message may or may not exist, and listing the conversation cannot reliably tell. The operator then fails
with ``AirflowFailException`` instead of retrying, and while the durable state remembers that unconfirmed
submission the next try refuses to ask again. Inspect the Genie space, then either:

* set ``conversation_id`` and ``message_id`` to attach to the message you found; the operator waits for
  it and returns its answer without asking, or
* set ``resubmit_unconfirmed=True`` to accept that the question may be asked twice.

A request that Databricks rejects with a 4xx response created nothing, so Airflow's normal retries
apply to it.

Cancellation
------------

Genie has no API to cancel a chat message, so killing or timing out the task leaves the message
running in Databricks. With durable execution the next try reconnects to it. With ``durable=False``,
or on Airflow before 3.3 where there is no task state store, the next try asks the question again in
a new message.
