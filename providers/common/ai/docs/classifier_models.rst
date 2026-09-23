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

Classifier models
=================

Every other model in this provider writes text. A classifier model does not: you give it
some text and a typed question, and it answers with a value from a set you named in
advance, plus a confidence. Ask it for a string and the request is refused before it
leaves your process.

`TypeSafe <https://typesafe.ai>`__'s Jev is the one pydantic-ai supports, as the
``typesafe:`` provider. Nothing in this provider is specific to it -- it arrives through the same
:class:`~airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook` as every other
model, so a model id is the whole integration.

Setup
-----

1. Install the extra:

   .. code-block:: bash

       pip install 'apache-airflow-providers-common-ai[typesafe]'

   The extra installs the TypeSafe SDK; the model adapter itself is part of pydantic-ai
   from 2.45.0, so make sure ``pydantic-ai-slim>=2.45.0`` is installed as well. The
   provider does not raise its own floor to that release yet, because pydantic-ai's
   ``openai`` extra needs openai 3.x while other Airflow providers still pin openai 2.x.

2. Create a connection (``Admin > Connections``):

   - **Connection Id**: ``jev_default``
   - **Connection Type**: ``Pydantic AI``
   - **Password**: your TypeSafe API key, from your `TypeSafe account <https://typesafe.ai>`__
   - **Extra**: ``{"model": "typesafe:jev-1.13.0"}``

Leave **Host** empty unless you are pointing at a proxy; the provider defaults to
TypeSafe's own endpoint.

Pin the version rather than using ``jev-latest``. A threshold you tuned against one
release is not guaranteed to mean the same thing after the next one, and ``jev-latest``
moves under you.

When a classifier model is the right choice
-------------------------------------------

All four of these have to hold.

**The answer is a label, a number, or a pick, not prose.** A ``bool``, a ``Literal`` or
``Enum`` of strings, a bounded ``float``, a whole-number rubric, or a list of picks. A
``str`` field is refused, so anything that writes a summary, a query, a migration, or a
reply to a person is out.

**You can name the options up front, and there are at most 255.** Downstream task ids,
error categories, severity levels, environments, a repository list. If the set is open, or
is discovered at run time and could grow past the cap, this is the wrong tool. The cap is
per question, so tools attached to an agent get their own 255, with the output type as one
option in that question.

**The decision is on a path where latency or cost is the constraint.** One decision per
Dag run rarely justifies changing models. One per row, per file, or per retrieved document
does, and so does a branch a scheduler is waiting on.

**You want a number to branch on, not a sentence to trust.** A pick, a rubric, and a
yes/no each come back with a confidence, so "act automatically above 0.8, ask a human below
it" becomes something you can write down. A bounded ``float`` is the exception: there the
probability *is* the answer, so nothing separate is reported and you gate on the value
itself. If you would not do anything different with a confidence of 0.55 than with 0.95,
that is a sign a general-purpose model is fine here.

And one case where the answer is neither: if a deterministic rule already sorts the input
correctly, use the rule. A classifier model is cheap, not free, and a rule you can read is
worth more than a probability you have to calibrate.

Where it fits in this provider
------------------------------

.. list-table::
   :header-rows: 1
   :widths: 30 15 55

   * - Surface
     - Fits
     - Why
   * - :class:`~airflow.providers.common.ai.operators.llm_branch.LLMBranchOperator`
     - Yes, with a caveat
     - The downstream task ids are already presented to the model as a constrained set of
       choices, which is exactly the shape a classifier model answers. Setting
       ``model_id`` is the only change, as long as there are two or more downstream tasks
       -- a one-option pick is refused. Describe each branch in ``branches`` and set a
       ``decision_policy`` so an unsure pick goes to a person instead of branching; see
       :doc:`operators/llm_branch`.
   * - :class:`~airflow.providers.common.ai.operators.llm.LLMOperator` /
       :class:`~airflow.providers.common.ai.operators.agent.AgentOperator` with a typed
       ``output_type``
     - Yes
     - A ``Literal``, ``Enum``, ``bool`` or bounded number works. Describe the field, which
       becomes the question, and describe each option, which is what tells them apart. An
       option with no description is read from its name alone.
   * - :doc:`ClassifierRetryPolicy <retry_policies>`
     - Yes
     - The model names one of the policy's ``categories`` and nothing else; retry or
       fail, the delay and the confidence bar come from each category's entry in the
       worker. Set ``min_confidence`` and an unsure answer goes to ``fallback_policy``
       (typically an ``LLMRetryPolicy`` on a text model), then ``fallback_rules``, then
       the task's own retry behaviour, instead of ending the task on the model's say-so.
       ``LLMRetryPolicy`` itself asks for free text, which a classifier model refuses.
       This is the surface where the model's speed and price matter most: it runs on
       every task failure.
   * - Agents with toolsets
     - Partly
     - Which tool the text calls for is itself a pick, so a classifier model can make it.
       What it cannot write is a tool's arguments. A tool taking none it calls itself; one
       taking arguments raises ``ToolCallProposed`` after the request, which is a
       ``ModelAPIError`` rather than a refusal, so ``FallbackModel`` hands those requests to
       a text model behind it and only they cost a full call.

Reading the confidence
----------------------

The confidence lives in ``provider_details`` on the model response. It is reported per
output field, and a bare output type is reported under ``"response"``. A bounded ``float``
field reports none at all, because there the probability is the answer.

Two surfaces act on it for you. :class:`~airflow.providers.common.ai.operators.llm_branch.LLMBranchOperator`
and :class:`~airflow.providers.common.ai.operators.llm.LLMOperator` take a
``decision_policy`` whose ``min_confidence`` sends an unsure answer to a person, or fails
the task, before anything downstream runs on it, and record the confidence, the
probabilities and the bar in the ``decision`` XCom (see :doc:`operators/llm_branch`).
:doc:`ClassifierRetryPolicy <retry_policies>` takes the same ``min_confidence`` and hands an
unsure answer to ``fallback_policy``, then its deterministic fallback rules. In the branch operator and the retry policy, a
per-option bar lets the choice whose wrong pick costs most demand more certainty than the rest.

Outside those, read it yourself. ``AgentOperator`` carries it inside the ``message_history``
transcript when that is enabled, and a hook-level call has it on the result:

.. code-block:: python

    from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook
    from airflow.providers.common.compat.sdk import task
    from typing import Literal


    @task
    def triage(log_line: str) -> dict:
        agent = PydanticAIHook(llm_conn_id="jev_default").create_agent(
            output_type=Literal["transient", "resource", "permanent"],
            instructions="Classify why this Airflow task failed.",
        )
        result = agent.run_sync(log_line)
        details = result.response.provider_details or {}
        confidence = (details.get("confidence") or {}).get("response")
        return {"category": result.output, "confidence": confidence}

Then branch on the returned confidence in a downstream task, so an unsure answer escalates
instead of acting. Use a higher bar for acting automatically than for flagging something
for review: collapsing both into one number is the easier thing to tune and the wrong
shape for the decision.

Worked example
--------------

`example_classifier_model.py <https://github.com/apache/airflow/blob/providers-common-ai/|version|/providers/common/ai/src/airflow/providers/common/ai/example_dags/example_classifier_model.py>`__
has both halves: a branch whose only classifier-specific line is ``model_id``, and a
classification that escalates when the confidence is low.

What it answers badly
---------------------

Read `pydantic-ai's model page <https://pydantic.dev/docs/ai/models/typesafe/>`__ and
`TypeSafe's own documentation <https://docs.typesafe.ai/>`__ before you
trust a number from one of these models. Two of its failure modes matter more than the
rest in a Dag:

- **The text is treated as data, not as hostile.** An injected instruction, a misleading
  framing, or an argument for its own answer can move the result. A guard built on a
  classifier model belongs alongside deterministic checks, not instead of them.
- **Option order is part of what the model sees.** Reordering a ``Literal``'s members can
  change the answer, so a threshold measured against one ordering is measured against
  that ordering only.
