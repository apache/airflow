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

.. _howto/operator:OnDemandSectionOperator:

OnDemandSectionOperator
=======================

Use the :class:`~airflow.providers.standard.operators.on_demand.OnDemandSectionOperator` to mark downstream work as
optional for normal Dag runs.

When the operator runs, it succeeds and skips its downstream tasks by default. This lets the Dag run finish
without waiting for optional work. Use the ``Run on-demand section`` action in Graph view or the operator's
task instance details to run the optional section for a selected Dag run.

By default, the operator skips all descendants. Set ``ignore_downstream_trigger_rules=False`` to skip only
direct downstream tasks and let later descendants follow their own trigger rules.

For example, a CI/CD Dag can build and validate a release in staging during its normal run while leaving
the production deployment available on demand. After reviewing the staging deployment, an operator can
run the production section from the same Dag run.

.. exampleinclude:: /../src/airflow/providers/standard/example_dags/example_on_demand_deployment.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_on_demand_deployment]
    :end-before: [END howto_operator_on_demand_deployment]
