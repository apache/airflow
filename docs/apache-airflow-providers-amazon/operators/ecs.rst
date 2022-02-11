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


Amazon Elastic Container Service (ECS) Operators
================================================

`Amazon Elastic Container Service (Amazon ECS) <https://aws.amazon.com/eks/>`__  is a fully
managed container orchestration service that makes it easy for you to deploy, manage, and
scale containerized applications.

Airflow provides operators to run Task Definitions on an ECS cluster.

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include::/operators/_partials/prerequisite_tasks.rst

.. _howto/operator:EcsOperator:

Run a Task
^^^^^^^^^^

To run a task defined in an Amazon ECS cluster you can use
:class:`~airflow.providers.amazon.aws.operators.ecs.EcsOperator`.

Before using EcsOperator *cluster*, *task definition*, and *container* need to be created.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_ecs_fargate.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_ecs]
    :end-before: [END howto_operator_ecs]

More information
----------------

For further information, look at:

* `Boto3 Library Documentation for ECS <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html>`__
