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

.. _concepts:priority-weight:

Priority Weights
================

``priority_weight`` defines priorities in the executor queue. The default ``priority_weight`` is ``1``, and can be
bumped to any integer. Moreover, each task has a true ``priority_weight`` that is calculated based on its
``weight_rule`` which defines the weighting method used for the effective total priority weight of the task.

Below are the weighting methods. By default, Airflow's weighting method is ``downstream``.


.. grid:: 3

  .. grid-item-card:: ``downstream``
    :shadow: none

    The effective weight of the task is the aggregate sum of all
    downstream descendants. As a result, upstream tasks will have
    higher weight and will be scheduled more aggressively when
    using positive weight values. This is useful when you have
    multiple DAG run instances and desire to have all upstream
    tasks to complete for all runs before each DAG can continue
    processing downstream tasks.

  .. grid-item-card:: ``upstream``
    :shadow: none

    The effective weight is the aggregate sum of all upstream ancestors.
    This is the opposite where downstream tasks have higher weight
    and will be scheduled more aggressively when using positive weight
    values. This is useful when you have multiple DAG run instances
    and prefer to have each DAG complete before starting upstream
    tasks of other DAG runs.

  .. grid-item-card:: ``absolute``
    :shadow: none

    The effective weight is the exact ``priority_weight`` specified
    without additional weighting. You may want to do this when you
    know exactly what priority weight each task should have.
    Additionally, when set to ``absolute``, there is bonus effect of
    significantly speeding up the task creation process as for very
    large Dags.


The ``priority_weight`` parameter can be used in conjunction with :ref:`concepts:pool`.

Custom Weight Rule
------------------

.. versionadded:: 2.9.0

You can implement your own custom weighting method by extending the ``PriorityWeightStrategy`` class and
registering it in a plugin.

.. exampleinclude:: /../../airflow/example_dags/plugins/decreasing_priority_weight_strategy.py
    :language: python
    :dedent: 0
    :start-after: [START custom_priority_weight_strategy]
    :end-before: [END custom_priority_weight_strategy]


Then to use it, you can create an instance of the custom class and provide it in the ``weight_rule`` parameter
of the task or provide the path of the custom class:

.. code-block:: python

    from custom_weight_rule_module import CustomPriorityWeightStrategy

    # provide the class instance
    task1 = BashOperator(task_id="task", bash_command="echo 1", weight_rule=CustomPriorityWeightStrategy())

    # or provide the path of the class
    task1 = BashOperator(
        task_id="task",
        bash_command="echo 1",
        weight_rule="custom_weight_rule_module.CustomPriorityWeightStrategy",
    )

|experimental|
