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

Human in the Loop (HITL) Operators
==================================

Human-in-the-Loop (HITL) operators enable workflows where task execution
can pause and wait for human input, approval, or intervention before
continuing.

These operators are part of the ``apache-airflow-providers-standard``
package and are intended for modeling approval steps or manual decision
points within automated pipelines.

Available HITL operators
------------------------

The Standard provider includes Human-in-the-Loop (HITL) operators.
For the complete and up-to-date list of available operators and their
parameters, refer to the Python API documentation:

`Python API reference <https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/operators/hitl/index.html>`_

Usage guide
-----------

For an end-to-end guide on using HITL operators, including examples and
recommended patterns, see the HITL tutorial:

* `HITL tutorial <https://airflow.apache.org/docs/apache-airflow/stable/tutorial/hitl.html>`_

.. _howto/operator:request_input:

Asking for input from inside a Python task
------------------------------------------

Sometimes the question only comes up halfway through a task. An agent built with the OpenAI
Agents SDK, Strands or Google ADK, for example, stops in front of a tool call and needs a person
to approve it. :func:`~airflow.providers.standard.operators.hitl.request_input` asks the question
and returns the answer:

.. exampleinclude:: /../../standard/src/airflow/providers/standard/example_dags/example_hitl_request_input.py
    :language: python
    :start-after: [START howto_hitl_request_input]
    :end-before: [END howto_hitl_request_input]

The first time the task reaches ``request_input()``, the question appears on the Required Actions
page and the task stops there. It waits in the ``awaiting_input`` state without holding a worker
slot. When the reviewer answers, the task runs again from the start of its callable, and this
time the same call returns the answer: a dict with ``chosen_options``, ``params_input``,
``responded_at`` (a ``datetime``), ``responded_by_user`` and ``timedout``.

``request_input()`` takes the same request fields as
:class:`~airflow.providers.standard.operators.hitl.HITLOperator` (``subject``, ``options``, ``body``,
``defaults``, ``multiple``, ``params``, ``notifiers``, ``assigned_users``), a ``timeout``, and a
``key``. A task can ask several questions; each run returns every answer it already has and stops
at the first question that has none.

Things to know:

* Code before ``request_input()`` runs again on every run. Keep anything expensive, such as an
  agent framework's saved run state, in ``task_state_store`` so the next run restores it instead of
  redoing it.
* Each answer goes back to the call with the same ``key``, which defaults to ``subject``, and each
  key is asked at most once per try. Pass ``key`` when the subject can change between runs (for
  example when it includes a value computed fresh each run), or when the task can ask the same
  subject more than once, such as one approval per agent tool call: key on the call's id.
* The task fails instead of guessing when the keys don't line up: when it asks a key whose answer
  it has already read, and when a run that resumed with an answer asks a new question, or finishes,
  before reading that answer.
* Every pause carries the answers the task has received so far in this try, including their
  ``params_input``, so keep the number of questions per try small (a handful, not hundreds) and the
  form fields short.
* A side effect between two questions, such as issuing a refund before asking whether to email the
  customer, runs again when the task resumes after the second answer. Record it in
  ``task_state_store`` and skip it when the record is there:

  .. code-block:: python

      if task_state_store.get("refund_id") is None:
          task_state_store.set("refund_id", issue_refund(order_id))

* A retry asks every question again, but ``task_state_store`` keeps its values across retries, so
  the retry still sees what the failed try saved. Delete those values when you no longer need them.
* Without ``defaults``, the task fails with
  :class:`~airflow.providers.standard.exceptions.HITLTimeoutError` when ``timeout`` passes. With
  ``defaults``, it resumes with ``timedout`` set to ``True`` and ``responded_by_user`` set to
  ``None``.
* It works in ``@task``, ``@task.branch``, ``@task.short_circuit`` and ``PythonOperator`` callables,
  sync or async. It can't pause tasks that run their callable in another process
  (``@task.virtualenv``, ``@task.external_python``, ``@task.docker``, ``@task.kubernetes``).
* Ask one question at a time, from the task's own code. A task instance has a single Required
  Actions request, so two questions asked at once (``asyncio.gather``, ``asyncio.TaskGroup``, or
  threads) overwrite each other, and a call from a worker thread doesn't see the answers the task
  resumed with. Don't call it inside code that turns exceptions into a result, such as an agent
  framework's tool-error handler: the errors above only fail the task if they reach it.
* It needs Airflow 3.4+.
* The Required Actions page refuses an answer to a request that the task has since replaced. A
  client answering through the REST API gets the same protection only if it sends the request's
  ``created_at`` (from the GET) as ``request_created_at``; without it, the answer is applied to
  whatever request is waiting.
