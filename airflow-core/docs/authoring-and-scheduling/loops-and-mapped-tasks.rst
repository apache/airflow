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

.. _loops-and-mapped-tasks:

======================
Loops and mapped tasks
======================

A Dag does not have to be static. How much work it does can depend on data that only exists at
runtime: how many files arrived, what the previous step found, or whether an answer is good enough
yet. Airflow has two features for this, and sometimes neither is what you need.

Choose a mechanism
==================

.. list-table::
   :header-rows: 1
   :widths: 30 30 40

   * - You want to
     - Use
     - How it works
   * - Run the same task once for each item in a collection
     - :ref:`Mapped tasks <mapped-tasks>`
     - ``task.expand(...)`` creates one task instance per item once the collection is known. The
       instances can run concurrently.
   * - Repeat a group of tasks, each pass building on the result of the previous one, until a
       condition is met
     - :ref:`Loops <loops>`
     - ``task_group.loop(...)`` creates the next pass of task instances only if the last pass says
       another is needed.
   * - Give a failed task another try
     - Retries, set with ``retries`` on the task. See :doc:`/core-concepts/tasks`.
     - A retry reruns the same task instance. It does not create new tasks, and it does not advance a
       loop.
   * - Create tasks from something known when the Dag file is parsed
     - A Python ``for`` loop in the Dag file. See :doc:`/howto/dynamic-dag-generation`.
     - The Dag has the same shape in every run.

Use them together
=================

A pass of a loop can contain mapped tasks, so each pass can fan out over a different collection of
items. See :ref:`Mapped tasks inside a loop <loops-mapped-tasks>`.

Mapping a whole loop, nesting loops, and placing a loop inside a mapped task group are not supported.
