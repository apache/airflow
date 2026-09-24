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

.. _approval-gates:

Approval gates for LLM operators
================================

.. seealso::
    This page covers the one-shot approve, edit or reject gate on ``LLMOperator`` and its
    subclasses. For a multi-round review loop on ``AgentOperator`` with a chat UI and REST
    API, see :doc:`hitl_review`.

Set ``require_approval=True`` to pause the task after the LLM generates its
output and wait for a human reviewer to approve or reject it via the Airflow
HITL interface.  Optionally allow the reviewer to edit the output before
approving with ``allow_modifications=True``, and set a deadline with
``approval_timeout``.

Human-in-the-loop review needs Airflow 3.1+, whether ``require_approval`` or a
``decision_policy`` with ``on_uncertain="review"`` opens it. On an older core the
operator raises ``AirflowOptionalProviderFeatureException`` when it is constructed, so the Dag file
fails to import, and with it every Dag defined in that file. A dynamically mapped
task (``.expand()``) is only constructed when it runs, so there the same error
surfaces as a task failure -- still before the model is called.

When ``approval_timeout`` expires without a review, the task fails by default.
Set ``on_approval_timeout="approve"`` to return the generated output instead, so
an unattended pipeline keeps moving.  ``"reject"`` answers the review with a
rejection, which still fails this operator; only
:class:`~airflow.providers.common.ai.operators.llm_branch.LLMBranchOperator`
turns a rejection into a downstream skip.  The chosen option is also
pre-highlighted as the default in the review form, so ``"reject"`` makes
Reject the primary button:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm.py
    :language: python
    :start-after: [START howto_operator_llm_approval]
    :end-before: [END howto_operator_llm_approval]

A pending review is not surfaced as a notification.  Pass
``approval_notifiers`` to tell the reviewers about it through any Airflow
notifier (Slack, email, ...), the way
:class:`~airflow.providers.standard.operators.hitl.HITLOperator` does with
``notifiers``.  The notifiers run once the review is open and can reference
the review ``{{ task.subject }}`` and ``{{ task.body }}`` in their templates,
as the example above does.  The ``@task.llm`` decorator and the operator
subclasses accept the same parameter.  A notifier whose delivery fails is
logged and the task still waits for the review; a template error fails the
task.  A retry re-runs the LLM and re-notifies with the regenerated output,
while the open review keeps the original subject and body.

The default ``body`` contains the rendered prompt and the output.  Where either
is sensitive, template only ``{{ task.subject }}`` and a link to the review
into channels outside Airflow's auth boundary.

By default any user with the permission can answer the review.  Pass
``approval_assigned_users=[{"id": "<auth-manager-user-id>", "name": "<user-name>"}]``
to restrict it to named reviewers, the way
:class:`~airflow.providers.standard.operators.hitl.HITLOperator` does with
``assigned_users``.  ``id`` is the user id reported by the auth manager: with
the default ``SimpleAuthManager`` it is the username from
``simple_auth_manager_users``; under the FAB auth manager it is the numeric
user row id as a string, not the username.  This needs Airflow 3.1+.  On Airflow 3.1.0 through 3.1.5 both
``id`` and ``name`` must match what the auth manager reports, so a wrong
``name`` blocks the assigned reviewer as well as everyone else; from 3.1.6 only
``id`` is compared.  The list is stored when the review is first created:
clearing the task re-runs it against the existing review row, so a changed
list does not take effect.

Reviewing uncertain output
--------------------------

A classifier model such as TypeSafe's reports a confidence for every field
of a structured output, in ``provider_details`` on the model response. It is a
summary of how concentrated the model's probability distribution was, not the
probability that the field is right. ``decision_policy=DecisionPolicy(min_confidence=0.7)``
(import ``DecisionPolicy`` from ``airflow.providers.common.ai.operators.llm``)
is the bar the least confident field has to clear for the operator to return
the output by itself. Only fields that reported a confidence are compared: a
field whose type reports none (a bounded float, where the probability is the
answer) is not gated, and the record's ``confidence`` map shows which fields
were. Under the bar, the policy's ``on_uncertain`` applies: ``"review"``
(default) sends the output to human review through the same approval flow as
``require_approval``, with the same ``approval_timeout``,
``on_approval_timeout`` and notifier settings; ``"fail"`` fails the task with
``LowConfidenceError`` (from ``airflow.providers.common.ai.exceptions``), which
Airflow retries like any other failure unless a retry rule says otherwise. A
text model reports no confidence for any field, which counts as uncertain, so
switching the connection does not silently switch off a control you set.
``require_approval=True`` keeps its meaning and always asks, and
``on_uncertain="review"`` needs Airflow 3.1+ like it does. The policy is
honoured by ``LLMOperator`` and ``LLMBranchOperator``; the SQL, schema-compare
and file-analysis operators run their own ``execute`` and reject a policy with a
bar at construction.

With or without a bar, the operator pushes a ``decision`` XCom carrying the
model name, the per-field ``confidence`` and ``probabilities`` (empty for a text
model), the bar that applied, why the output went to review if it did, who
decided, and the gate configuration in force.
A pending record is checkpointed with the paused task and finalized from that
copy on resume, not from the XCom. See :ref:`LLMBranchOperator <howto/operator:llm_branch>` for the
record's fields; there ``proposed`` and ``action`` name the branches, while
here they are ``null`` and the output itself is the return value.
