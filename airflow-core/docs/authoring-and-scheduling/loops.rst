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

.. _loops:

Loops
=====

A loop repeats a group of tasks a number of times decided at runtime,
using the results of each iteration. Each iteration can use the previous
iteration's result. Use a loop for work such as refining an answer, following
a hierarchy, or processing successive batches.

A conditional loop works like a ``do … while`` loop: run the body, then test
whether to run it again. Airflow's ``until`` condition expresses when to stop,
so it corresponds to ``do { body } while not until(result)``. The body runs at
least once. ``max_iterations`` sets an upper bound; the runtime condition
determines how many iterations are needed within that bound.

Tasks within an iteration can run in parallel.

If you are not sure whether you need a loop or mapped tasks, see
:ref:`loops-and-mapped-tasks`.

Create a loop
-------------

Define the body with ``@task_group`` and call ``.loop()`` on the decorated
function. Supply a positive integer ``max_iterations`` to bound the number of
iterations. An optional ``until`` callable decides when to stop early.

Airflow evaluates ``until`` in a gate task downstream of the body's terminal task:

* ``True`` means stop: the condition has been met.
* ``False`` means continue, provided another iteration is allowed.

Airflow creates the next iteration when the gate says to continue.
It does not create all ``max_iterations`` up front:
if the loop stops after four iterations, you see those four, without a tail
of skipped iterations. For a fixed-count loop, the gate continues until the
count is reached.

This example improves an estimate of the square root of two until the error
is small enough:

.. code-block:: python

   from airflow.sdk import dag, task, task_group


   @dag(schedule=None, catchup=False, tags=["example"])
   def refine_estimate():
       @task_group
       def refine():
           @task
           def improve(*, loop):
               previous = loop.previous
               estimate = 1.0 if previous is None else previous["estimate"]
               return (estimate + 2.0 / estimate) / 2.0

           @task
           def evaluate(estimate):
               return {"estimate": estimate, "error": abs(estimate * estimate - 2.0)}

           evaluate(improve())

       def accurate_enough(*, loop):
           return loop.result["error"] < 0.000001

       @task
       def finished():
           print("Refinement finished.")

       refinement = refine.loop(max_iterations=10, until=accurate_enough)
       refinement >> finished()


   refine_estimate()

``evaluate`` returns the result for the iteration. The gate reads it through
``loop.result``. If another iteration runs, ``improve`` reads that same result
through ``loop.previous``. The gate appears as a task in the loop, named after
the condition function: ``refine.accurate_enough`` in this example.

Stopping because ``until`` returned ``True`` means the loop converged.
Reaching ``max_iterations`` while the condition remains ``False`` means the
loop did not converge: the gate task in the final iteration is marked as
failed. Meeting the condition on the last allowed iteration succeeds.

For a fixed-count loop, omit ``until``: ``refine.loop(max_iterations=3)`` runs
three iterations, carrying results between them. Reaching the cap completes
a fixed-count loop successfully. Its gate is named ``__loop_gate`` within the group.

.. code-block:: python

   @dag(schedule=None, catchup=False, tags=["example"])
   def fixed_task_loop():
       @task_group
       def accumulate():
           @task
           def increment(*, loop):
               previous = loop.previous
               return (0 if previous is None else previous) + 1

           increment()

       accumulate.loop(max_iterations=3)


   fixed_task_loop()

For a task-group function with arguments, supply them with ``.partial()`` before
calling ``.loop()``. Use ``.override()`` to configure the group, for example to
give another loop a different ``group_id``.

Read the loop context
---------------------

Declare ``loop`` as a keyword-only parameter on a task function. Airflow
supplies it at execution time; leave it out when calling the task in the Dag
definition.

.. list-table:: Loop context
   :header-rows: 1
   :widths: 25 75

   * - Attribute
     - Meaning
   * - ``loop.index``
     - The current iteration number, starting at zero.
   * - ``loop.max_iterations``
     - The configured limit on the number of iterations.
   * - ``loop.previous``
     - The previous iteration's result, or ``None`` in iteration 0.
   * - ``loop.result``
     - The terminal body task's result for the current iteration, used by the gate.

Use ``loop.index`` for the loop iteration and ``ti.map_index`` for a mapped
task's position. Neither is a task try number. A task can have several tries
within one iteration.

Pass data between iterations
----------------------------

Between iterations, use ``loop.previous``. Check explicitly for ``None`` to
handle the first iteration; a previous result of ``0``, ``False``, or an empty
collection can be valid data.

``include_prior_dates=True`` cannot select a loop iteration from another run.
Publish the result through a task outside the loop if later runs need to
retrieve it with an ordinary XCom pull.

The experimental DagRun wait API also requires an outside-loop result task.
It rejects results selected directly from a loop member, including authored
Dag results, before it starts streaming status updates.

The loop body must have exactly one terminal task definition before the gate.
Its return value becomes ``loop.result`` for the gate and ``loop.previous``
for the next iteration. A mapped terminal task supplies its collection of
results using normal task-mapping semantics.

If the body has several branches, finish with a task that combines their
outputs. For example, if two branches end in ``refine_left`` and
``refine_right``, add this inside the task group:

.. code-block:: python

   @task
   def combine(left, right):
       return {"left": left, "right": right}


   combine(refine_left(), refine_right())

``combine`` is now the single terminal task. The gate can read each result
through ``loop.result["left"]`` and ``loop.result["right"]``; tasks in the
next iteration use the corresponding keys in ``loop.previous``.

Connect a loop to other tasks
------------------------------

Use the object returned by ``.loop()`` in dependencies:

.. code-block:: python

   start() >> refinement >> finish()

With the default ``all_success`` trigger rule, ``finish`` waits for successful
loop completion. Other trigger rules behave normally; ``always`` does not wait
for the loop. A task that must run in every iteration belongs inside the task group.

.. _loops-mapped-tasks:

Mapped tasks inside a loop
--------------------------

A loop iteration can contain mapped tasks. Use mapping to process several
items within the iteration. The next iteration can operate on a different
collection of items.

A mapped task can be the body's single terminal task:

.. code-block:: text

   select items → process each item → gate

The gate reads the collection of mapped results through ``loop.result``. Add
a combining task before the gate if you want to reduce those results to a
single value or structure. A zero-length expansion skips the mapped task and,
under the gate's default trigger rule, skips the gate; no next iteration is
created.

.. code-block:: python

   @dag(schedule=None, catchup=False, tags=["example"])
   def mapped_task_loop():
       @task_group
       def process_batch():
           @task
           def process(value, *, loop, ti):
               print(f"Iteration {loop.index}, mapped position {ti.map_index}")
               return value + loop.index

           process.expand(value=[1, 2])

       def batch_ready(*, loop):
           return min(loop.result) >= 2

       process_batch.loop(max_iterations=3, until=batch_ready)


   mapped_task_loop()

The loop iteration and mapped position are separate "coordinates". Mapped
instance 2 in iteration 0 is distinct from mapped instance 2 in iteration 1.
Mapped instances appear within their loop iteration, so you can inspect their
states, tries, and logs separately.

Mapping a whole loop, nesting loops, and placing a loop inside a mapped task
group are not supported.

Failures, skips, and retries
----------------------------

A retry stays in the same iteration and does not consume another iteration.
Tasks in the body follow normal trigger rules. For example, a final combining
task with ``all_done`` can handle a branch failure and return a result. If that
task succeeds, the gate evaluates its result normally. If the terminal task
fails, the gate's default ``all_success`` rule prevents it from running, and
no next iteration is created.

An exception in ``until`` fails the gate task and appears in its logs. If the
loop reaches its iteration limit without meeting ``until``, the final gate
fails. Downstream tasks with
the default ``all_success`` trigger rule will not run.

Skipping the body's terminal task also skips the gate under its default
``all_success`` trigger rule, so no next iteration is created. Downstream
tasks follow their own trigger rules; with the default, they are skipped too.
If some branches may be skipped but the loop should continue, give the final
combining task a trigger rule that permits those skips and have it return the
iteration's result.

The gate does not independently wait for every task in the body. If the
terminal task uses ``one_success``, for example, it can finish while another
branch is still running. The gate then follows its normal trigger rule and
can start the next iteration while that branch continues in the earlier one.

Manually marking a gate successful completes it without evaluating ``until``
or creating another iteration. This is an explicit override of normal gate
execution. Any later iterations retained after a selective clear remain unchanged.

Iterations and execution history
--------------------------------

The loop view groups tasks and mapped instances by iteration. The gate's state
and logs explain why the loop continued, stopped, or failed. Each task's tries
and logs remain accessible within its iteration.

Clear tasks inside a loop
--------------------------

Use these controls to select how far a clear extends through the loop:

* **Clear downstream**, selected by default, includes downstream tasks.
  Clearing a task this way can also clear the gate in the same iteration.
* **Clear later loop iterations**, selected by default, clears later
  iterations when the selection includes a gate task, whether selected
  directly or through **Clear downstream**. The gate runs again and decides
  whether the loop should continue from that point.

Clearing later iterations does not require the loop to run the same number of
iterations again. The gate can stop earlier or continue further, within the
configured limit.

Rerun part of an iteration
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Suppose each iteration contains this sequence:

.. code-block:: text

   prepare → process → consume → gate

The loop has already run iterations 0 through 4. You clear ``process`` in
iteration 2 with both default options selected:

* ``prepare`` in iteration 2 remains completed.
* ``process``, ``consume``, and the gate in iteration 2 run again.
* Later iterations 3 and 4 are cleared. Whether replacement iterations run
  depends on the new gate decisions.
* Iterations 0 and 1 remain untouched.

If the gate now stops at iteration 2, replacement iterations 3 and 4 are not
created.

Keep later iterations
~~~~~~~~~~~~~~~~~~~~~

Deselect **Clear later loop iterations** to keep the later iterations.
Rerunning an earlier gate then does not change the loop progression that
already followed it: a new stop result does not remove later iterations, and
a new continue result does not create a duplicate next iteration.

Tasks you leave uncleared are not recomputed using the results of the rerun.
