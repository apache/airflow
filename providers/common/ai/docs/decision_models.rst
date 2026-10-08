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

Decision models
===============

.. note::

    Experimental: this can change or be removed in a minor release of this provider.
    See :ref:`howto/stability`.

Every other model in this provider writes text. A decision model does not: you give it
some text and a typed question, and it answers with a value from a set you named in
advance, plus a confidence. Ask it for a string and the request is refused before it
leaves your process.

pydantic-ai reaches decision models through two model prefixes. Nothing in this provider is
specific to either: both arrive through the same
:class:`~airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook` as every other
model, so a model id is the whole integration.

.. list-table::
   :header-rows: 1
   :widths: 18 32 50

   * - Prefix
     - Needs
     - Runs
   * - ``typesafe:``
     - ``pydantic-ai-slim`` 2.45.0 or later (2.46.0 for the examples on this page, which
       describe options with ``UseEnumMemberDocstrings``) and the provider's ``typesafe`` extra
     - `TypeSafe <https://typesafe.ai>`__'s hosted Jev.
   * - ``system-one:``
     - ``pydantic-ai-slim`` 2.53.0 or later, no extra
     - Any server that answers the same ``POST /v1/systemone`` API as Jev, hosted or your
       own. `Ollama <https://docs.ollama.com/capabilities/decision>`__ 0.35 and later serves
       it for the decision models it runs, ``strands-decider serve`` serves AWS's
       `Strands Decider <https://github.com/strands-labs/strands-decider>`__, and
       `Kev <https://github.com/jaredpalmer/kev>`__ is an open-weight model built to serve
       it. `pydantic-ai's System One page <https://pydantic.dev/docs/ai/models/system-one/>`__
       lists more.

Both prefixes need a newer ``pydantic-ai-slim`` than the floor the provider's other extras
set, so check the release you have installed.

Setup
-----

The examples on this page use a connection named ``decision_default``. Create it for the
backend you run.

**TypeSafe Jev**

1. Install the extra, which adds the TypeSafe SDK:

   .. code-block:: bash

       pip install 'apache-airflow-providers-common-ai[typesafe]'

2. Create a connection (``Admin > Connections``):

   - **Connection Id**: ``decision_default``
   - **Connection Type**: ``Pydantic AI``
   - **Password**: your TypeSafe API key, from your `TypeSafe account <https://typesafe.ai>`__
   - **Extra**: ``{"model": "typesafe:jev-1.13.0"}``

   Leave **Host** empty unless you are pointing at a proxy; the provider defaults to
   TypeSafe's own endpoint.

**A System One server**

1. Start the server, or note the URL of a hosted one. For example, to serve Strands Decider
   on your own machine:

   .. code-block:: bash

       pip install strands-decider
       strands-decider serve StrandsAgents/strands-decider-2B-hobson-v19 --port 8000

2. Create a connection (``Admin > Connections``):

   - **Connection Id**: ``decision_default``
   - **Connection Type**: ``Pydantic AI``
   - **Host**: the server's URL, such as ``http://decider.internal:8000``, with or without a
     trailing ``/v1``
   - **Password**: the server's API key, sent as a bearer token. Leave it empty for a server
     that takes none, such as a local Ollama.
   - **Extra**: ``{"model": "system-one:strands-decider-2B-hobson-v19"}``

   The name after ``system-one:`` is sent to the server as the model to answer with. A
   server that runs several, such as Ollama, picks one by it.

3. Describe every option you ask about, and give the question its text. Servers differ in
   what they accept, and Strands Decider 0.1.0 refuses, with an HTTP 422 that fails the task,
   a question that has an option without a description or that has no question text at all.
   Describe each branch in ``branches`` and each member of an ``Enum`` through a docstring;
   the members of a bare ``Literal`` have no descriptions, so use a described ``Enum``
   instead, as the worked example below does. The question text is the operator's
   ``system_prompt`` or the agent's ``instructions``, which default to empty.

Pin the model and its version, as in ``typesafe:jev-1.13.0`` rather than
``typesafe:jev-latest``. A threshold you tuned against one model, or one release of it, is
not guaranteed to mean the same thing on another, so measure it again when you change
either.

When a decision model is the right choice
-----------------------------------------

All four of these have to hold.

**The answer is a label, a number, or a pick, not prose.** A ``bool``, a ``Literal`` or
``Enum`` of strings, a bounded ``float``, a whole-number rubric, or a list of picks. A
``str`` field is refused, so anything that writes a summary, a query, a migration, or a
reply to a person is out.

**You can name the options up front, and there are few enough for the model.** Downstream
task ids, error categories, severity levels, environments, a repository list. If the set is
open, or is discovered at run time and could grow past the cap, this is the wrong tool. Each
model has its own cap per question: Jev takes 255 options, and Ollama takes 26. Tools
attached to an agent count as options in a question of their own, with the output type as
one more. pydantic-ai refuses a question over Jev's cap before sending it; a ``system-one:``
model's cap is the server's, so a question over it comes back as an HTTP error from the
server instead.

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
correctly, use the rule. A decision model is cheap, not free, and a rule you can read is
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
       choices, which is exactly the shape a decision model answers. Pointing
       ``llm_conn_id`` (or ``model_id``) at one is the only change, as long as there are two
       or more downstream tasks -- a one-option pick is refused. Describe each branch in
       ``branches`` and set a
       ``decision_policy`` so an unsure pick goes to a person instead of branching; see
       :doc:`operators/llm_branch`.
   * - :class:`~airflow.providers.common.ai.operators.llm.LLMOperator` /
       :class:`~airflow.providers.common.ai.operators.agent.AgentOperator` with a typed
       ``output_type``
     - Yes
     - A ``Literal``, ``Enum``, ``bool`` or bounded number works. Describe the field, which
       becomes the question, and describe each option, which is what tells them apart. An
       option with no description is read from its name alone, and some servers refuse it.
   * - :doc:`ClassifierRetryPolicy <retry_policies>`
     - Yes
     - The model names one of the policy's ``categories`` and nothing else; retry or
       fail, the delay and the confidence bar come from each category's entry in the
       worker. Set ``min_confidence`` and an unsure answer goes to ``fallback_policy``
       (typically an ``LLMRetryPolicy`` on a text model), then ``fallback_rules``, then
       the task's own retry behaviour, instead of ending the task on the model's say-so.
       ``LLMRetryPolicy`` itself asks for free text, which a decision model refuses.
       This is the surface where the model's speed and price matter most: it runs on
       every task failure.
   * - Agents with toolsets
     - Partly
     - Which tool the text calls for is itself a pick, so a decision model can make it.
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

    from enum import Enum

    from pydantic_ai import UseEnumMemberDocstrings

    from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook
    from airflow.sdk import task


    class FailureCause(UseEnumMemberDocstrings, str, Enum):
        transient = "transient"
        """A fault that clears by itself: a timeout, throttling, a dropped connection."""

        resource = "resource"
        """A dependency is down or unreachable and needs fixing before a retry can work."""

        permanent = "permanent"
        """A bug or bad input that fails the same way however often it is retried."""


    @task
    def triage(log_line: str) -> dict:
        agent = PydanticAIHook(llm_conn_id="decision_default").create_agent(
            output_type=FailureCause,
            instructions="Classify why this Airflow task failed.",
        )
        result = agent.run_sync(log_line)
        details = result.response.provider_details or {}
        confidence = (details.get("confidence") or {}).get("response")
        return {"category": result.output.value, "confidence": confidence}

Then branch on the returned confidence in a downstream task, so an unsure answer escalates
instead of acting. Use a higher bar for acting automatically than for flagging something
for review: collapsing both into one number is the easier thing to tune and the wrong
shape for the decision.

Worked example
--------------

`example_decision_model.py <https://github.com/apache/airflow/blob/providers-common-ai/|version|/providers/common/ai/src/airflow/providers/common/ai/example_dags/example_decision_model.py>`__
has both halves: a branch with nothing specific to decision models but its connection, and a
classification that escalates when the confidence is low.

What it answers badly
---------------------

Read `pydantic-ai's decision model guide <https://pydantic.dev/docs/ai/models/decision/>`__
and the documentation of the model you run (`TypeSafe's <https://docs.typesafe.ai/>`__ for
Jev) before you trust a number from one of these models. Two failure modes documented for
Jev matter more than the rest in a Dag; check them against any other model before relying on
it not to share them:

- **The text is treated as data, not as hostile.** An injected instruction, a misleading
  framing, or an argument for its own answer can move the result. A guard built on a
  decision model belongs alongside deterministic checks, not instead of them.
- **Option order is part of what the model sees.** Reordering a ``Literal``'s members can
  change the answer, so a threshold measured against one ordering is measured against
  that ordering only.
