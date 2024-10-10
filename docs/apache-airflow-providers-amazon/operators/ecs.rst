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

======================================
Amazon Elastic Container Service (ECS)
======================================

`Amazon Elastic Container Service (Amazon ECS) <https://aws.amazon.com/ecs/>`__  is a fully
managed container orchestration service that makes it easy for you to deploy, manage, and
scale containerized applications.

Airflow provides operators to run Task Definitions on an ECS cluster.

Prerequisite Tasks
------------------

.. include:: ../_partials/prerequisite_tasks.rst

Generic Parameters
------------------
.. include:: ../_partials/generic_parameters.rst

Operators
---------

.. _howto/operator:EcsCreateClusterOperator:

Create an AWS ECS Cluster
=========================

To create an Amazon ECS cluster you can use
:class:`~airflow.providers.amazon.aws.operators.ecs.EcsCreateClusterOperator`.

All optional parameters to be passed to the Create Cluster API should be
passed in the 'create_cluster_kwargs' dict.

.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_ecs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_ecs_create_cluster]
    :end-before: [END howto_operator_ecs_create_cluster]

.. _howto/operator:EcsDeleteClusterOperator:

Delete an AWS ECS Cluster
=========================

To delete an Amazon ECS cluster you can use
:class:`~airflow.providers.amazon.aws.operators.ecs.EcsDeleteClusterOperator`.


.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_ecs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_ecs_delete_cluster]
    :end-before: [END howto_operator_ecs_delete_cluster]

.. _howto/operator:EcsRegisterTaskDefinitionOperator:

Register a Task Definition
==========================

To register a task definition you can use
:class:`~airflow.providers.amazon.aws.operators.ecs.EcsRegisterTaskDefinitionOperator`.

All optional parameters to be passed to the Register Task Definition API should be
passed in the 'register_task_kwargs' dict.


.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_ecs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_ecs_register_task_definition]
    :end-before: [END howto_operator_ecs_register_task_definition]

.. _howto/operator:EcsDeregisterTaskDefinitionOperator:

Deregister a Task Definition
=============================

To deregister a task definition you can use
:class:`~airflow.providers.amazon.aws.operators.ecs.EcsDeregisterTaskDefinitionOperator`.


.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_ecs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_ecs_deregister_task_definition]
    :end-before: [END howto_operator_ecs_deregister_task_definition]

.. _howto/operator:EcsRunTaskOperator:

Run a Task Definition
=====================

To run a Task Definition defined in an Amazon ECS cluster you can use
:class:`~airflow.providers.amazon.aws.operators.ecs.EcsRunTaskOperator`.

You need to have created your ECS Cluster, and have created a Task Definition before you can use this Operator.
The Task Definition contains details of the containerized application you want to run.

This Operator support running your containers in ECS Clusters that are either Serverless (FARGATE), via EC2,
or via external resources (EXTERNAL).
The parameters you need to configure for this Operator will depend upon which ``launch_type`` you want to use.

.. code-block::

    launch_type="EC2|FARGATE|EXTERNAL"

* If you are using AWS Fargate as your compute resource in your ECS Cluster, set the parameter ``launch_type`` to FARGATE. When using a launch type of FARGATE you will need to provide ``network_configuration`` parameters.
* If you are using EC2 as the compute resources in your ECS Cluster, set the parameter to EC2.
* If you have integrated external resources in your ECS Cluster, for example using ECS Anywhere, and want to run your containers on those external resources, set the parameter to EXTERNAL.

.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_ecs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_ecs_run_task]
    :end-before: [END howto_operator_ecs_run_task]


.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_ecs_fargate.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_ecs]
    :end-before: [END howto_operator_ecs]

Stream logs to AWS CloudWatch
"""""""""""""""""""""""""""""

To stream logs to AWS CloudWatch, you need to define the parameters below.
Using the example above, we would add these additional parameters to enable logging to CloudWatch.
You need to ensure that you have the appropriate level of permissions (see next section).

.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_ecs.py
    :language: python
    :dedent: 8
    :start-after: [START howto_awslogs_ecs]
    :end-before: [END howto_awslogs_ecs]

IAM Permissions
"""""""""""""""

You need to ensure you have the following IAM permissions to run tasks via the EcsRunTaskOperator:

.. code-block::

  {
      "Effect": "Allow",
      "Action": [
          "ecs:RunTask",
          "ecs:DescribeTasks",
      ]
      "Resource": [ "arn:aws:ecs:{aws region}:{aws account number}:task_definition/{task definition family}" ]
  },
  {
      "Effect": "Allow",
      "Action": [
          "iam:PassRole"
      ]
      "Resource": [ "arn:aws:iam::{aws account number}:role/{task execution role name}" ]
  },
  {
      "Effect": "Allow",
      "Action": [
        "ecs:DescribeTasks",
      ],
      "Resource": [ "arn:aws:ecs:{aws region}:{aws account number}:task/{ecs cluster name}/*" ]
  }

If you use the "reattach=True" (the default is False), you need to add further permissions.
You need to add the following additional Actions to the IAM policy.

.. code-block::

        "ecs:DescribeTaskDefinition",
        "ecs:ListTasks"

**CloudWatch Permissions**

If you plan on streaming Apache Airflow logs into AWS CloudWatch, you need to ensure that you have configured the appropriate permissions set.

.. code-block::

                iam.PolicyStatement(
                    actions=[
                        "logs:CreateLogStream",
                        "logs:CreateLogGroup",
                        "logs:PutLogEvents",
                        "logs:GetLogEvents",
                        "logs:GetLogRecord",
                        "logs:GetLogGroupFields",
                        "logs:GetQueryResults"
                    ],
                    effect=iam.Effect.ALLOW,
                    resources=[
                        "arn:aws:logs:{aws region}:{aws account number}:log-group:{aws-log-group-name}:log-stream:{aws-log-stream-name}/\*"
                        ]
                )

Sensors
-------

.. _howto/sensor:EcsClusterStateSensor:

AWS ECS Cluster State Sensor
============================

To poll the cluster state until it reaches a terminal state you can use
:class:`~airflow.providers.amazon.aws.operators.ecs.EcsClusterStateSensor`.

Defaults to EcsClusterStates.ACTIVE as a success state and no failure state,
both can be overridden with provided values.  Raises an AirflowException with
the failure reason if a failed state is provided and that state is reached
before the target state.


.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_ecs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_ecs_cluster_state]
    :end-before: [END howto_sensor_ecs_cluster_state]

.. _howto/sensor:EcsTaskDefinitionStateSensor:

AWS ECS Task Definition State Sensor
=====================================

To poll the task definition state until it reaches a terminal state you can use
:class:`~airflow.providers.amazon.aws.operators.ecs.EcsTaskDefinitionStateSensor`.

Valid states are either EcsTaskDefinitionStates.ACTIVE or EcsTaskDefinitionStates.INACTIVE.
Defaults to EcsTaskDefinitionStates.ACTIVE as the success state, but accepts a parameter
to change that.  Raises an AirflowException with the failure reason if the failed state
is reached before the target state.


.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_ecs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_ecs_task_definition_state]
    :end-before: [END howto_sensor_ecs_task_definition_state]

.. _howto/sensor:EcsTaskStateSensor:

AWS ECS Task State Sensor
=========================

To poll the task state until it reaches a terminal state you can use
:class:`~airflow.providers.amazon.aws.operators.ecs.EcsTaskStateSensor`.

Defaults to EcsTaskStates.RUNNING as the success state and no failure state,
both can be overridden with provided values.  Raises an AirflowException with
the failure reason if a failed state is provided and that state is reached
before the target state.

.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_ecs_fargate.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_ecs_task_state]
    :end-before: [END howto_sensor_ecs_task_state]

Reference
---------

* `AWS boto3 library documentation for ECS <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html>`__
