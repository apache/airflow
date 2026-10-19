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

.. _guides/task_execution_identity:openlineage:

Identifying task executions in events
-------------------------------------

Every OpenLineage event the provider emits for a task tells which execution of the task it describes:
which Dag run, which try, which mapped instance and, inside a :doc:`task loop
<apache-airflow:authoring-and-scheduling/loops>`, which iteration and which rerun of the loop. This page
explains how to read that from a single event, without access to the Airflow metadata database, for example to
group events per execution, deduplicate alerts or tell whether a failure is final.

All the fields below are in the ``airflow`` run facet of task events (``run.facets.airflow``), unless said
otherwise. Dag run events carry the ``dag`` and ``dagRun`` parts in the ``airflowDagRun`` run facet. A field
that doesn't apply to an execution is left out of the event.

Run ids
^^^^^^^

The ``runId`` of an event identifies one attempt, and all the events of that attempt (START, then COMPLETE or
FAIL) share it.

* **Dag run:** a UUIDv7 derived from the OpenLineage namespace, the ``dag_id`` and ``dagRun.clear_number``,
  with the Dag run's logical date (or ``run_after`` when it has none) as its timestamp. Clearing the whole Dag
  run gives it a new ``runId``.
* **Task outside a loop:** a UUIDv7 derived from the namespace, ``dag_id``, ``task_id``, ``try_number`` and
  ``map_index``, with the same timestamp. The same attempt always gets the same ``runId``, so it can be rebuilt
  from those fields.
* **Task inside a loop:** ``taskInstance.id``, the UUID Airflow gives the attempt (Airflow 3.4+). The iterations
  of a loop share every other coordinate, so a derived id would not be unique.

The ``ParentRunFacet`` of a task event points at its Dag run's ``runId``.

Fields
^^^^^^

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Field
     - Meaning
   * - ``dag.dag_id``, ``task.task_id``
     - The Dag and the task. ``job.name`` is ``<dag_id>.<task_id>`` for a task and ``<dag_id>`` for a Dag run.
   * - ``dagRun.run_id``
     - The Dag run. It stays the same when the Dag run is cleared.
   * - ``dagRun.clear_number``
     - How many times the whole Dag run was cleared. Clearing single tasks doesn't change it.
   * - ``taskInstance.id``
     - The UUID of the attempt (Airflow 3+). Every try, and every task instance a clear creates again, gets a
       new one.
   * - ``taskInstance.try_number``
     - The try. It counts per ``region_id`` and ``region_index``, so a task instance that a loop rerun creates
       again starts from the first try.
   * - ``taskInstance.max_tries``
     - The last ``try_number`` that is retried on failure. A FAIL with ``try_number > max_tries`` is final;
       otherwise the task will be retried. Clearing a task instance raises it to give the task its retries again.
   * - ``taskInstance.map_index``
     - The position in a mapped task's expansion, as shown in the UI. Absent for a task that isn't mapped.
   * - ``taskInstance.expanded_ti_count``
     - The number of live instances in the mapped task's expansion (Airflow 3.4+), counted within the loop
       iteration inside a loop. ``map_index`` is one of ``range(expanded_ti_count)``, so it tells how many events
       to expect and is the denominator of "3 of 20 instances failed". Absent for a task that isn't mapped.
   * - ``taskInstance.region_id``, ``taskInstance.region_index``
     - The coordinates Airflow stores the task instance under (Airflow 3.4+). A region is one execution of a
       loop or of a mapped task's expansion. ``region_index`` is the iteration for a task in a loop that isn't
       mapped, and the map index for a mapped task. Absent for a task that is neither mapped nor in a loop.
   * - ``taskInstance.loop``
     - The iteration of the loop the task runs in (Airflow 3.4+), see below. Absent outside a loop.
   * - ``task.task_group``
     - The task's task group. For a loop group it also has ``max_iterations``, ``has_until``, ``gate_task_id``
       and ``terminal_task_id``, also listed in the ``taskGroups`` of the Dag's ``AirflowJobFacet``.

The ``taskInstance.loop`` object:

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Field
     - Meaning
   * - ``group_id``
     - The loop's group id. It can differ from ``task.task_group`` when the task sits in a task group inside
       the loop.
   * - ``index``
     - The zero-based iteration.
   * - ``max_iterations``
     - The loop's limit on iterations.
   * - ``region_id``
     - The loop region that holds the iteration. All the task instances of one pass of the loop share it,
       mapped ones included, whose own ``taskInstance.region_id`` is their expansion's.
   * - ``trajectory``
     - 1 for the loop's first execution, and one more for each rerun of it, see :ref:`loop reruns
       <guides/task_execution_identity:loop-reruns>`. Of two executions of the same iteration, the higher
       trajectory is the current one.
   * - ``forked_from_region_id``
     - The ``loop.region_id`` of the execution this rerun replaces. Absent in trajectory 1.
   * - ``resumes_from_index``
     - The first iteration this execution of the loop holds, 0 in trajectory 1.
   * - ``decision``
     - Only on the COMPLETE and FAIL events of a loop's gate: ``stop`` and the optional ``reason`` the gate
       decided, see :ref:`loop gates <guides/task_execution_identity:loop-gates>`.

Loops can't be nested, so ``loop`` always describes the only loop around the task.

Grouping events
^^^^^^^^^^^^^^^

Each of these keys is built from a single event:

.. list-table::
   :header-rows: 1
   :widths: 35 65

   * - Group
     - Key
   * - A Dag run, across its clears
     - ``(dag_id, dagRun.run_id)``
   * - One execution of a Dag run
     - ``(dag_id, dagRun.run_id, dagRun.clear_number)``
   * - A task in a Dag run, across retries and clears
     - ``(dag_id, dagRun.run_id, task_id, loop.group_id, loop.index, map_index)``, the loop and map parts left
       empty when absent
   * - One attempt
     - ``runId``, or ``taskInstance.id`` on Airflow 3+
   * - One mapped expansion
     - ``(dag_id, dagRun.run_id, task_id, taskInstance.region_id)``, with ``expanded_ti_count`` instances
   * - One iteration of a loop, in one trajectory
     - ``(dag_id, dagRun.run_id, loop.region_id, loop.index)``
   * - One iteration of a loop, across reruns
     - ``(dag_id, dagRun.run_id, loop.group_id, loop.index)``, the current execution being the one with the
       highest ``loop.trajectory``

How each kind of task emits events
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**Plain task.** START when it starts, then COMPLETE or FAIL. A skipped task emits COMPLETE.

**Retries.** Each try is an attempt with its own ``runId``, ``try_number`` and ``taskInstance.id``. A FAIL with
``try_number <= max_tries`` is followed by another try.

**Sensors in reschedule mode and deferred tasks.** Rescheduling or deferring a task doesn't start a new try, so
its events keep one ``try_number`` and ``runId``. A rescheduled sensor emits a single START, while a deferred
task emits START again when it resumes.

**Mapped task.** Each mapped instance has its own ``map_index`` and ``runId``, and every instance of one
expansion carries the same ``taskInstance.region_id`` and ``expanded_ti_count``. Tasks of a mapped task group
are mapped tasks in this sense. A task whose expansion turns out empty doesn't run, so no instance of it emits
events.

**Task in a loop.** Each iteration emits its own events, with ``loop.index`` the iteration. For a task that isn't
mapped, ``taskInstance.region_id`` is the same as ``loop.region_id``.

**Mapped task in a loop.** Each iteration expands the task again: ``map_index`` and ``expanded_ti_count`` are
within the iteration, ``taskInstance.region_id`` is the iteration's expansion and ``loop.region_id`` ties the
instances to the other tasks of the same pass.

.. _guides/task_execution_identity:loop-gates:

**Loop gate.** The gate ends each iteration and decides whether the loop goes on. Its task id is the loop
group's ``gate_task_id``. Read its ``loop.decision`` with ``loop.index``, ``loop.max_iterations`` and the loop
group's ``has_until``:

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Event
     - Meaning
   * - COMPLETE, ``stop`` false
     - The loop goes on to iteration ``index + 1``.
   * - COMPLETE, ``stop`` true, ``has_until``
     - ``until`` ended the loop at iteration ``index``, which can be the last one.
   * - COMPLETE, ``stop`` true, no ``has_until``
     - The loop ran all its iterations: without ``until`` it always stops at ``index == max_iterations - 1``.
   * - FAIL, ``stop`` false
     - ``until`` was still false at the last iteration, so the loop reached ``max_iterations`` and fails.
   * - FAIL without ``decision``
     - The gate failed before deciding, for example because ``until`` raised; the error facet tells why.

.. _guides/task_execution_identity:loop-reruns:

**Loop rerun.** Clearing a loop's gate together with its later iterations reruns the loop from there. Say a
loop ran iterations 0 to 4 and the gate of iteration 2 is cleared:

* iteration 2's cleared tasks run again in place: same ``loop.region_id`` and ``loop.trajectory``, next
  ``try_number``;
* iterations 3 and 4 are replaced by a new execution of the loop, if the gate decides to go on. Its events
  have a new ``loop.region_id``, ``loop.trajectory`` one more, ``loop.forked_from_region_id`` the region
  they replace and ``loop.resumes_from_index`` 3. Their tries count from the start again, so ``loop.index``,
  ``map_index`` and ``try_number`` can repeat those of the replaced executions: ``loop.region_id`` and
  ``loop.trajectory`` tell them apart.

An event from a trajectory greater than 1 tells that the iterations from ``resumes_from_index`` on of the
execution ``forked_from_region_id`` were superseded.

**Dag run cleared.** ``dagRun.clear_number`` grows and the Dag run gets a new ``runId``. The tasks it reruns
get a new try.

**State set by hand.** Marking a task instance successful or failed in the UI or the API emits COMPLETE or FAIL
from the API server, with the same fields as the worker sends, ``loop`` and ``expanded_ti_count`` included
from Airflow 3.4.0 on. A task that never started has no START. A gate marked by hand has no ``loop.decision``.

Limitations
^^^^^^^^^^^

* Loops need Airflow 3.4. Pre-releases of Airflow 3.4.0 give no ``loop`` details beyond ``group_id``,
  ``index`` and ``max_iterations``, and no ``expanded_ti_count``.
* The ``airflowState`` run facet of Dag run events keys task states by ``task_id`` only, so mapped instances
  and loop iterations of one task share one entry.
