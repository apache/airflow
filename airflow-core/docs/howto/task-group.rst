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

Grouping tasks with TaskGroups
------------------------------

This page shows how to group tasks in a Dag and how to wire groups together.
For what a TaskGroup is and how it appears in the Graph view, see
:ref:`concepts:taskgroups`.

Group tasks and set dependencies on the group
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create a group with ``TaskGroup`` as a context manager, then set dependencies
on the group as a whole. A dependency on a group applies to its root and leaf
tasks.

.. exampleinclude:: /../src/airflow/example_dags/example_task_group.py
    :language: python
    :start-after: [START howto_task_group]
    :end-before: [END howto_task_group]

Here ``start >> section_1`` makes ``start`` run before the root tasks of
``section_1``, and ``section_1 >> section_2`` makes the leaf tasks of
``section_1`` run before the root tasks of ``section_2``.

Set dependencies between groups
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Groups can depend on each other directly, without naming individual tasks:

.. code-block:: python

    group1() >> group2()

Every leaf task of ``group1`` runs before every root task of ``group2``. Avoid
dependencies that leave a group and come back into it, because they form a
cycle. See :ref:`concepts:taskgroup-cycles`.

Use the ``@task_group`` decorator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The decorator turns a function into a group. The function's docstring becomes
the group's tooltip in the UI unless you pass ``tooltip`` explicitly.

.. exampleinclude:: /../src/airflow/example_dags/example_task_group_decorator.py
    :language: python
    :start-after: [START howto_task_group_decorator]
    :end-before: [END howto_task_group_decorator]

Set defaults for every task in a group
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Pass ``default_args`` to a group to set values for all of its tasks. They
override the Dag-level ``default_args``, and an argument set on a single task
overrides both:

.. code-block:: python

    @task_group(default_args={"retries": 3})
    def group1():
        task1 = EmptyOperator(task_id="task1")  # retries=3
        task2 = BashOperator(task_id="task2", bash_command="echo hi", retries=2)  # retries=2

Turn off ID prefixing
~~~~~~~~~~~~~~~~~~~~~

By default, task and group IDs inside a group are prefixed with the group's ID,
so two groups can each contain a task named ``task1`` without a clash. To drop
the prefix, pass ``prefix_group_id=False``:

.. code-block:: python

    with TaskGroup("section_1", prefix_group_id=False):
        task_1 = EmptyOperator(task_id="task_1")

Without the prefix, every task and group ID in the Dag must be unique on its
own. Reusing an ID makes the Dag fail to parse with a duplicate task ID error.