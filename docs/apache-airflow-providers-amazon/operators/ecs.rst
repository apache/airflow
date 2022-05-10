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

.. include:: _partials/prerequisite_tasks.rst

* You will need to have created your ECS Cluster, and have created a Task Definition before you can use this Operator. The Task Definition contains details of the containerized application you want to run.

.. include::/operators/_partials/prerequisite_tasks.rst

.. _howto/operator:EcsOperator:

Overview
^^^^^^^^

To run a Task Definition defined in an Amazon ECS cluster you can use
:class:`~airflow.providers.amazon.aws.operators.ecs.EcsOperator`.

This Operator support running your containers in ECS Clusters that are either Serverless (FARGATE), via EC2, or via external resources (EXTERNAL). The parameters you need to configure for this Operator will depend upon which ``launch_type`` you want to use.

Launch Types
------------
.. code-block::

    launch_type="EC2|FARGATE|EXTERNAL"

* If you are using AWS Fargate as your compute resource in your ECS Cluster, set the parameter ``launch_type`` to FARGATE. When using a launch type of FARGATE you will need to provide ``network_configuration`` parameters.
* If you are using EC2 as the compute resources in your ECS Cluster, set the parameter to EC2.
* If you have integrated external resources in your ECS Cluster, for example using ECS Anywhere, and want to run your containers on those external resources, set the parameter to EXTERNAL.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_ecs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_ecs]
    :end-before: [END howto_operator_ecs]


.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_ecs_fargate.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_ecs]
    :end-before: [END howto_operator_ecs]


CloudWatch Logging
------------------

To stream logs to AWS CloudWatch, you need to define these parameters. Using the example Operators above, we would add these additional parameters to enable logging to CloudWatch. You will need to ensure that you have the appropriate level of permissions (see next section)

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_ecs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_awslogs_ecs]
    :end-before: [END howto_awslogs_ecs]

IAM Permissions
---------------

You will need to ensure you have the following IAM permissions to run Tasks via this Operator. In this example, the Operator will have permissions to run Tasks on an ECS Cluster called "cluster a" in a specific AWS region and account.

.. code-block::

        {
            "Effect": "Allow",
            "Action": [
                "ecs:RunTask",
                "ecs:DescribeTasks"
            ],
            "Resource": : [ "arn:aws:ecs:{aws region}:{aws account number}:cluster/{custer a}"
        }

If you use the "reattach=True" (the default is False), you will need to add further permissions. You will need to add the following additional Actions to the IAM policy.

.. code-block::

        "ecs:DescribeTaskDefinition",
        "ecs:ListTasks"

**CloudWatch Permissions**

If you plan on streaming Apache Airflow logs into AWS CloudWatch, you will need to ensure that you have configured the appropriate permissions set.

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


More information
----------------

For further information, look at:

* `Boto3 Library Documentation for ECS <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html>`__
