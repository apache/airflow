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

=====
Loops
=====

A loop repeats a group of tasks a number of times decided at runtime,
using the results of each iteration. Each iteration can use the previous
iteration's result. Use a loop for work such as refining an answer, following
a hierarchy, or processing successive batches.

A conditional loop works like a ``do … while`` loop: run the body, then test
whether to run it again. Airflow's ``until`` condition expresses when to stop,
so it corresponds to ``do { body } while not until(result)``. The body runs at
least once. ``max_iterations`` sets an upper limit; the runtime condition
determines how many iterations are needed within that limit.

Iterations run one after another; task instances within an iteration can run in parallel.

If you are not sure whether you need a loop or mapped tasks, see
:ref:`loops-and-mapped-tasks`.

Create a loop
=============

Define the body with ``@task_group`` and call ``.loop()`` on the decorated
function. Supply a positive integer ``max_iterations`` to limit the number of
iterations. An optional ``until`` callable decides when to stop early.

Airflow evaluates ``until`` in a gate task downstream of the body's terminal task.
It must return a ``bool``:

* ``True`` means stop: the condition has been met.
* ``False`` means continue, provided another iteration is allowed.

Any other return value, including the ``None`` that a forgotten ``return`` produces, will result in
the gate task failing, and the loop not continuing.

Airflow creates the next iteration when the gate says to continue.
It does not create all ``max_iterations`` up front:
if the loop stops after four iterations, you see those four, without a tail
of skipped iterations. A fixed-count loop runs exactly ``max_iterations``
iterations.

This example improves an estimate of the square root of two until the error
is small enough:

.. exampleinclude:: /authoring-and-scheduling/examples/example_task_loops.py
   :start-after: [START refine_estimate]
   :end-before: [END refine_estimate]

``evaluate`` returns the result for the iteration. The gate reads it through
``loop.result``. If another iteration runs, ``improve`` reads that same result
through ``loop.previous``. The gate appears as a task in the loop, named after
the condition function: ``refine.accurate_enough`` in this example.

Tasks inside a loop body receive ``loop`` as a context parameter, so a parameter with that name in a
loop body must be keyword-only and cannot have a default other than ``None``.

In a templated field, a Jinja ``{% for %}`` block defines its own ``loop`` that hides this one for
the length of the block. Read the Airflow value before the block and use that inside it, for example
``{% set iteration = loop.index %}``.

When ``until`` has no usable name, the gate is named ``__loop_gate`` instead.
That covers a lambda, a ``functools.partial`` and a callable object. A gate name
that matches a task in the loop body gets a ``__1`` suffix, as with any other
task ID.

Stopping because ``until`` returned ``True`` means the loop converged.
Reaching ``max_iterations`` while the condition remains ``False`` means the
loop did not converge: the gate task in the final iteration is marked as
failed. Meeting the condition on the last allowed iteration marks the gate task as success.

For a fixed-count loop, omit ``until``. The definition ``refine.loop(max_iterations=3)`` runs
three iterations, carrying results between them. Reaching the cap completes
a fixed-count loop successfully. Its gate is named ``__loop_gate`` within the group.

.. exampleinclude:: /authoring-and-scheduling/examples/example_task_loops.py
   :start-after: [START fixed_loop]
   :end-before: [END fixed_loop]

For a task-group function with arguments, supply them with ``.partial()`` before
calling ``.loop()``. Use ``.override()`` to configure the group, for example to
give another loop a different ``group_id``:

.. exampleinclude:: /authoring-and-scheduling/examples/example_task_loops.py
   :start-after: [START partial_override_loop]
   :end-before: [END partial_override_loop]

Read the loop context
=====================

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
     - The terminal body task's result (the XCom pushed under the key ``return_value``) for the current iteration, used by the gate.

Use ``loop.index`` for the loop iteration and ``ti.map_index`` for a mapped
task instance's position. Each task instance can have several tries, each with its own ``ti.try_number``.

Pass data between iterations
============================

Between iterations, use ``loop.previous``. Check ``loop.index == 0`` to handle
the first iteration. Do not test ``loop.previous is None``: it is also ``None``
when the previous terminal task returned nothing, and a previous result of
``0``, ``False``, or an empty collection can be valid data.

The loop body must have exactly one terminal task definition before the gate.
Its return value (the XCom pushed under the ``return_value`` key) becomes ``loop.result`` for the gate and ``loop.previous``
for the next iteration. A mapped terminal task supplies its collection of
results as a sequence (``LazyXComSequence``), the same as other downstream consumers of mapped tasks.

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

Limitations:

* ``include_prior_dates=True`` cannot select a loop iteration from another Dag run. Push the result to XCom through a task outside the loop if later Dag runs need to retrieve it with an ordinary XCom pull.
* The experimental DagRun wait API also requires an outside-loop result task. It rejects results selected directly from a loop member. See :ref:`dag-result`.

Connect a loop to other tasks
=============================

Use the object returned by ``.loop()`` in dependencies:

.. code-block:: python

   refinement >> finished()

With the default ``all_success`` trigger rule, ``finished`` waits for successful
loop completion. Other trigger rules behave normally; ``always`` does not wait
for the loop.

.. _loops-mapped-tasks:

Mapped tasks inside a loop
==========================

A loop iteration can contain mapped tasks. Use mapping to process several
items within the iteration. The next iteration can operate on a different
collection of items.

A mapped task can be the body's single terminal task:

.. code-block:: text

   select items → process each item → gate

The gate reads the collection of mapped results through ``loop.result``. Add
a combining task before the gate if you want to reduce those results to a
single value or structure. A zero-length expansion skips the mapped task and,
under the gate's ``all_success`` rule, skips the gate; no next iteration is
created.

In this example, ``choose_items`` returns the values to map over: ``[1, 2]`` in the
first iteration, then each previous result plus one.

.. exampleinclude:: /authoring-and-scheduling/examples/example_task_loops.py
   :start-after: [START mapped_loop]
   :end-before: [END mapped_loop]

The loop iteration and mapped position are separate "coordinates". In this
example, each iteration has mapped instances 0 and 1, so mapped instance 1 in
iteration 0 is distinct from mapped instance 1 in iteration 1.
Mapped instances appear within their loop iteration, so you can inspect their
states, tries, and logs separately.

Mapping a whole loop, nesting loops, and placing a loop inside a mapped task
group are not supported.

Failures, skips, and retries
============================

A retry stays in the same iteration and does not consume another iteration.
Tasks in the body follow normal trigger rules. For example, a final combining
task with ``all_done`` can handle a branch failure and return a result. If that
task succeeds, the gate evaluates its result normally. If the terminal task
fails, the gate's ``all_success`` rule prevents it from running, and
no next iteration is created. You cannot change the gate task's trigger rule.
The gate has exactly one upstream, the body's terminal task, so set
``trigger_rule`` on that task to change when the gate runs.

An exception in ``until`` fails the gate task and appears in its logs. If the
loop reaches its iteration limit without meeting ``until``, the final gate
fails; downstream tasks with the default ``all_success`` trigger rule will not
run, as described under non-convergence above.

Skipping the body's terminal task also skips the gate under its
``all_success`` trigger rule, so no next iteration is created. Downstream
tasks with the default ``all_success`` rule are skipped too; give a downstream
task ``trigger_rule="none_failed"`` if it should still run when the loop ends
without running:

.. code-block:: python

   @task(trigger_rule="none_failed")
   def finished(): ...


   refinement >> finished()

If some branches may be skipped but the loop should continue, give the final
combining task a trigger rule that permits those skips and have it return the
iteration's result.

Manually marking a gate successful completes it without evaluating ``until``
or creating another iteration. This is an explicit override of normal gate
execution. Downstream tasks then run as they would after any successful gate: they cannot tell a
gate that was marked successful by hand from one that stopped because ``until`` returned ``True``.
Any later iterations retained after a selective clear remain unchanged.

Iterations and execution history
================================

In the Grid, select the loop's task group in a Dag run to open its Task Instances
tab. The **Iteration** filter narrows the table to one iteration, or shows
**All iterations**, and the **Iteration** column shows which iteration each task
instance belongs to. The gate's state and logs explain why the loop continued,
stopped, or failed. Each task's tries and logs remain accessible within its
iteration.

Iterations cleared by a rerun are kept as history but are not shown in the UI
in 3.4.0.

Clear tasks inside a loop
==========================

Use these controls to select how far a clear extends through the loop:

* **Downstream** includes downstream tasks. It starts selected unless you have
  changed your saved clear options. Clearing a task this way can also clear
  the gate in the same iteration.
* **Clear later loop iterations**, selected by default, clears later
  iterations when the selection includes a gate task, whether selected
  directly or through **Downstream**. The gate runs again and decides
  whether the loop should continue from that point.

Clearing later iterations does not require the loop to run the same number of
iterations again. The gate can stop earlier or continue further, within the
configured limit.

Rerun part of an iteration
---------------------------

Suppose each iteration contains this sequence:

.. code-block:: text

   prepare → process → consume → gate

The loop has already run iterations 0 through 4. You clear ``process`` in
iteration 2 with **Downstream** and **Clear later loop iterations** both selected:

* ``prepare`` in iteration 2 remains completed.
* ``process``, ``consume``, and the gate in iteration 2 run again.
* Later iterations 3 and 4 are cleared. Whether replacement iterations run
  depends on the new gate decisions.
* Iterations 0 and 1 remain untouched.

If the gate now stops at iteration 2, replacement iterations 3 and 4 are not
created.

Keep later iterations
---------------------

Deselect **Clear later loop iterations** to keep the later iterations.
Rerunning an earlier gate then does not change the loop progression that
already followed it: a new stop result does not remove later iterations, and
a new continue result does not create a duplicate next iteration.

The choice is the same wherever you clear. The REST clear endpoint takes it as
``include_later_loop_iterations``, which defaults to ``true``, matching the
checked box in the UI. ``airflow tasks clear`` has no option for it: it always
clears later iterations, except with ``--only-failed`` or ``--only-running``,
which leave them in place. To keep later iterations, clear from the UI or the
REST endpoint.

Tasks you leave uncleared are not recomputed using the results of the rerun.
