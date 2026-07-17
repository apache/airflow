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

.. _howto/operator:BranchDateTimeOperator:

BranchDateTimeOperator
======================

Use the :class:`~airflow.providers.standard.operators.datetime.BranchDateTimeOperator` to branch into one of two execution paths
depending on whether the time falls into the range given by two target arguments,

This operator has two modes. First mode is to use current time (machine clock time at the
moment the Dag is executed), and the second mode is to use the ``logical_date`` of the Dag run it is run
with.


Usage with current time
-----------------------

The usages above might be useful in certain situations - for example when Dag is used to perform cleanups
and maintenance and is not really supposed to be used for any Dags that are supposed to be back-filled,
because the "current time" make back-filling non-idempotent, its result depend on the time when the Dag
actually was run. It's also slightly non-deterministic potentially even if it is run on schedule. It can
take some time between when the DAGRun was scheduled and executed and it might mean that even if
the DAGRun was scheduled properly, the actual time used for branching decision will be different than the
schedule time and the branching decision might be different depending on those delays.

.. exampleinclude:: /../src/airflow/providers/standard/example_dags/example_branch_datetime_operator.py
    :language: python
    :start-after: [START howto_branch_datetime_operator]
    :end-before: [END howto_branch_datetime_operator]

The target parameters, ``target_upper`` and ``target_lower``, can receive a ``datetime.datetime``,
a ``datetime.time``, or ``None``. When a ``datetime.time`` object is used, it will be combined with
the current date in order to allow comparisons with it. In the event that ``target_upper`` is set
to a ``datetime.time`` that occurs before the given ``target_lower``, a day will be added to ``target_upper``.
This is done to allow for time periods that span over two dates.

.. exampleinclude:: /../src/airflow/providers/standard/example_dags/example_branch_datetime_operator.py
    :language: python
    :start-after: [START howto_branch_datetime_operator_next_day]
    :end-before: [END howto_branch_datetime_operator_next_day]

If a target parameter is set to ``None``, the operator will perform a unilateral comparison using only
the non-``None`` target. Setting both ``target_upper`` and ``target_lower`` to ``None``
will raise an exception.

Usage with logical date
-----------------------

The usage is much more "data range" friendly. The ``logical_date`` does not change when the Dag is re-run and
it is not affected by execution delays, so this approach is suitable for idempotent Dag runs that might be
back-filled.

.. exampleinclude:: /../src/airflow/providers/standard/example_dags/example_branch_datetime_operator.py
    :language: python
    :start-after: [START howto_branch_datetime_operator_logical_date]
    :end-before: [END howto_branch_datetime_operator_logical_date]

.. _howto/operator:BranchDayOfWeekOperator:

BranchDayOfWeekOperator
=======================

Use the :class:`~airflow.providers.standard.operators.weekday.BranchDayOfWeekOperator` to branch your workflow based on week day value.

.. exampleinclude:: /../src/airflow/providers/standard/example_dags/example_branch_day_of_week_operator.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_day_of_week_branch]
    :end-before: [END howto_operator_day_of_week_branch]
