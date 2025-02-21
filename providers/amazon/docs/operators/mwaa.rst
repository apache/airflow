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

==================================================
Amazon Managed Workflows for Apache Airflow (MWAA)
==================================================

`Amazon Managed Workflows for Apache Airflow (MWAA) <https://aws.amazon.com/managed-workflows-for-apache-airflow/>`__
is a managed service for Apache Airflow that lets you use your current, familiar Apache Airflow platform to orchestrate
your workflows. You gain improved scalability, availability, and security without the operational burden of managing
underlying infrastructure.

Prerequisite Tasks
------------------

.. include:: ../_partials/prerequisite_tasks.rst

Generic Parameters
------------------

.. include:: ../_partials/generic_parameters.rst

Operators
---------

.. _howto/operator:MwaaTriggerDagRunOperator:

Trigger a DAG run in an Amazon MWAA environment
===============================================

To trigger a DAG run in an Amazon MWAA environment you can use the
:class:`~airflow.providers.amazon.aws.operators.mwaa.MwaaTriggerDagRunOperator`

Note: Unlike :class:`~airflow.providers.standard.operators.trigger_dagrun.TriggerDagRunOperator`, this operator is capable of
triggering a DAG in a separate Airflow environment as long as the environment with the DAG being triggered is running on
AWS MWAA.

In the following example, the task ``trigger_dag_run`` triggers a dag run for a DAG with with the ID ``hello_world`` in
the environment ``MyAirflowEnvironment``.

.. exampleinclude:: /../../providers/amazon/tests/system/amazon/aws/example_mwaa.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_mwaa_trigger_dag_run]
    :end-before: [END howto_operator_mwaa_trigger_dag_run]

References
----------

* `AWS boto3 library documentation for MWAA <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/mwaa.html>`__
