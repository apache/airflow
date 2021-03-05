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



.. _howto/operator:BranchDayOfWeekOperator:

BranchDayOfWeekOperator
=======================

Use the :class:`~airflow.operators.weekday.BranchDayOfWeekOperator` to branch your workflow based on week day value.

.. exampleinclude:: /../../airflow/example_dags/example_branch_day_of_week_operator.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_day_of_week_branch]
    :end-before: [END howto_operator_day_of_week_branch]
