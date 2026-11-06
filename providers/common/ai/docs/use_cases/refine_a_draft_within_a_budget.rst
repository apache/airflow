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

Refine a draft within a budget
==============================

A model's first draft of a customer status page update is rarely the one you publish. This Dag
has an agent write the update and a second model score it against a style guide. A
:ref:`task loop <apache-airflow:loops>` repeats both until the score reaches 0.9, and stops
sooner once the loop as a whole has spent $5.00 or 500,000 tokens. Each pass shows in the UI with
the reason the loop continued or stopped.

The budget is checked between passes, so it limits how many passes run, not what one pass
spends. To cap a pass, set ``usage_limits`` on each task.

What this demonstrates
----------------------

* :ref:`Task loops <apache-airflow:loops>` -- ``refine.loop(max_iterations=20, until=good_enough)``
  repeats the task group, and ``good_enough`` returns a ``LoopDecision`` with a reason.
* The ``usage`` XCom that ``@task.agent`` and ``@task.llm`` push, read back inside the loop to
  keep a running total of tokens and estimated cost.
* :ref:`howto/operator:llm_usage_limits` on each task, which caps what one pass can spend.
* :doc:`../structured_output` -- the judge's output is validated against ``Score``.

Run it
------

The Dag needs Airflow 3.4.0 or later for task loops, and Common AI 1.1.0 or later, the first
release in which ``@task.llm`` pushes the ``usage`` XCom. Both tasks use the model set on the
``pydanticai_default`` connection.

1. Install the provider with the extra for your vendor:

   .. code-block:: bash

       pip install "apache-airflow-providers-common-ai[anthropic]>=1.1.0"

2. Trigger the Dag:

   .. code-block:: bash

       airflow dags test example_agent_loop_budget

The gate logs its decision for each iteration, and ``publish`` logs the result. From a run with
``anthropic:claude-sonnet-4-6``:

.. code-block:: text

    Loop iteration 1: continue (max_iterations=20): score 0.5; $0.007317 of $5.00, 1,427 of 500,000 tokens
    Loop iteration 2: continue (max_iterations=20): score 0.7; $0.016605 of $5.00, 3,131 of 500,000 tokens
    Loop iteration 3: continue (max_iterations=20): score 0.5; $0.025290 of $5.00, 4,870 of 500,000 tokens
    Loop iteration 4: continue (max_iterations=20): score 0.5; $0.033585 of $5.00, 6,543 of 500,000 tokens
    Loop iteration 5: continue (max_iterations=20): score 0.5; $0.042198 of $5.00, 8,226 of 500,000 tokens
    Loop iteration 6: continue (max_iterations=20): score 0.7; $0.051069 of $5.00, 9,919 of 500,000 tokens
    Loop iteration 7: continue (max_iterations=20): score 0.5; $0.060381 of $5.00, 11,643 of 500,000 tokens
    Loop iteration 8: continue (max_iterations=20): score 0.7; $0.069204 of $5.00, 13,376 of 500,000 tokens
    Loop iteration 9: continue (max_iterations=20): score 0.5; $0.079605 of $5.00, 15,175 of 500,000 tokens
    Loop iteration 10: continue (max_iterations=20): score 0.5; $0.087213 of $5.00, 16,883 of 500,000 tokens
    Loop iteration 11: stop (max_iterations=20): passed: score 0.9; $0.096552 of $5.00, 18,624 of 500,000 tokens
    11 passes, score 0.9, 18624 tokens, $0.096552

The Dag
-------

The whole loop, with the budget in ``tally`` and ``good_enough``:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent_loop_budget.py
    :language: python
    :start-after: [START howto_loop_budget]
    :end-before: [END howto_loop_budget]

Each pass runs ``write_draft``, then ``judge``, then ``tally``. ``tally`` is the loop body's only
terminal task, so what it returns is ``loop.result`` for the gate and ``loop.previous`` for the
next pass: the draft, the judge's feedback, and the running totals.

Inside a loop, ``ti.xcom_pull(..., key="usage")`` returns the ``usage`` XCom of the current
iteration only, so ``tally`` adds this pass's spend to the totals it finds in ``loop.previous``.
Money is added as ``Decimal`` and stored as a string in XCom, so the total does not pick up
``float`` rounding.

How the loop ends
-----------------

Every reason carries the score and both totals, and a reason that stops the loop starts with the
ending:

.. list-table::
   :header-rows: 1
   :widths: 22 40 38

   * - Ending
     - Reason on the last gate
     - Result
   * - Converged
     - ``passed: score 0.9; $0.096552 of $5.00, 18,624 of 500,000 tokens``
     - The gate succeeds and ``publish`` runs.
   * - Dollar budget spent
     - ``dollar budget spent: score 0.9; $0.016362 of $0.01, 3,226 of 500,000 tokens``
     - The gate succeeds and ``publish`` runs.
   * - Token budget spent
     - ``token budget spent: score 0.0; cost unknown, 745 of 600 tokens``
     - The gate succeeds and ``publish`` runs.
   * - ``max_iterations`` reached
     - ``score 0.0; cost unknown, 4,995 of 500,000 tokens``
     - The last gate fails and ``publish`` is ``upstream_failed``.

The converged row is the run above. The dollar row ran the same models with a $0.01 cap and a
threshold of 0.99, so the budget ran out first. The token and ``max_iterations`` rows ran
pydantic-ai's ``test`` model, which has no price, the token row with a 600-token cap.

Reaching ``max_iterations`` fails the last gate with ``LoopMaxIterationsExceeded``, carrying the
last reason:

.. code-block:: text

    Loop condition remained false at max_iterations=20: score 0.0; cost unknown, 4,995 of 500,000 tokens

Stopping on a budget is a successful end of the loop, so ``publish`` runs and returns the last
draft even though it did not pass. Check ``passes[-1]["score"]`` in ``publish`` if a draft below
the threshold should not go out.

A per-task ``usage_limits`` that trips is not a budget stop. The task fails with
``UsageLimitExceeded``, the gate does not run, and the loop fails. The task log shows what the
call spent before it stopped:

.. code-block:: text

    LLM run failed: requests=0, tool_calls=0, input_tokens=118, output_tokens=4, total_tokens=122
    Task failed with exception

The exception reads ``Exceeded the total_tokens_limit of 100 (total_tokens=122)``.

Set each task's limits above what a normal pass spends, so they only catch a runaway call.

What the totals count
---------------------

**Either cap can be overshot by one pass.** The pass that crosses a cap has already run when the
gate checks it: a 600-token cap stopped at 745 after a pass of 250 tokens. The two tasks'
``usage_limits`` bound one pass, give or take one model response per task, since each limit is
checked after every response; the 122 tokens against a limit of 100 above are one such response.

**The dollar total is an estimate from list prices.** ``cost`` in the ``usage`` XCom is
pydantic-ai's ``RunUsage.cost``, which `genai-prices <https://github.com/pydantic/genai-prices>`__
computes from the token counts and the model name at the provider's list prices. The provider is
matched by its API URL first and by its name after that, so a call through a gateway that
``genai-prices`` does not know is still priced at the vendor's list rates. When a provider reports
what it billed, as OpenRouter does, pydantic-ai keeps that amount in the response's
``provider_details`` and the ``usage`` XCom still carries the estimate. Token counts are the ones
the provider reported.

**An unpriced model switches the dollar cap off.** A model ``genai-prices`` does not know, such
as a private fine-tune, a model newer than your ``genai-prices`` data, or pydantic-ai's ``test``
model, reports ``cost=None``. The first pass with such a call sets the loop's ``usd`` to ``None``
for the rest of the loop and logs a warning in ``tally``:

.. code-block:: text

    A model call had no price, so the dollar cap no longer applies to this loop.

From then on the gate's reasons say ``cost unknown``, and only the token cap can stop the loop.
One unpriced model is enough: with a priced writer and the unpriced ``test`` judge, ``usd`` was
``None`` from the first pass and the loop ended on
``token budget spent: score 0.0; cost unknown, 2,063 of 2,000 tokens``.

The same call also leaves that task's own ``cost_limit`` unenforced. The task log warns and the
task carries on:

.. code-block:: text

    A `cost_limit` is set but cannot be enforced because no cost was calculated for this run. ...

Set a token cap even when the budget you care about is dollars. Counting the unknown cost as zero
would report less than was spent, and with no priced model at all the dollar cap would never trip.

**Spend from an earlier try of a task is not in the totals.** The ``usage`` XCom holds the last
try's usage only. In a run where ``write_draft`` failed after its model call and passed on retry,
each try used 127 tokens and the loop counted 127. A retry of ``write_draft`` or ``judge`` after a
try that reached the model leaves that try's spend out of the totals.

Adapting it
-----------

* Give the writer and the judge different models with ``model_id``, for example a cheaper judge.
  Mixing a priced and an unpriced model switches the dollar cap off, as described above.
* Raise ``max_iterations`` for harder drafts. Reaching it fails the loop, so keep it high enough
  that the threshold or a budget normally ends the loop first.
