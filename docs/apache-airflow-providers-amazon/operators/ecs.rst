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

Using Operator
--------------


Launch Types
--------------

You can use this Operator to run ECS Tasks in ECS Clusters with launch types of EC2, Fargate and EXTERNAL, using the "launch_type" parameter. Bear in mind that the different launch types will require different parameters to be supplied.

```
launch_type="EC2|Fargate|EXTERNAL"
```

*Example Operator for launch_type of EC2 and EXTERNAL*

```
    hello_world = EcsOperator(
        task_id="hello_world",
        dag=dag,
        aws_conn_id="aws_default",
        cluster="ecs-cluster-name",
        task_definition="ecs-task-definition",
        launch_type="EC2|EXTERNAL",
        overrides={ "containerOverrides": [
            {
                "name": "hello-world-container",
                "command" : [ "echo","hello world from Airflow" ],
            }
        ] },
        tags={
            "Customer": "X",
            "Project": "Y",
            "Application": "Z",
            "Version": "0.0.1",
            "Environment": "Development",
            }
    )
```

*Example Operator for launch_type of Fargate*

With a launch type of Fargate you will need to provide the "network_configuration" parameter.

```
    hello_world = EcsOperator(
        task_id="hello_world",
        dag=dag,
        aws_conn_id="aws_default",
        cluster="ecs-cluster-name",
        task_definition="ecs-task-definition",
        launch_type="FARGATE",
        overrides={ "containerOverrides": [
            {
                "name": "hello-world-container",
                "command" : [ "echo","hello world from Airflow" ],
            }
        ] },
        network_configuration={
            'awsvpcConfiguration': {
                'securityGroups': ['aws security groups'],
                'subnets': ['aws subnets'],
                'assignPublicIp': "ENABLED|DISABLED"
            },
        },
        tags={
            "Customer": "X",
            "Project": "Y",
            "Application": "Z",
            "Version": "0.0.1",
            "Environment": "Development",
            }
    )
```

CloudWatch Logging
------------------

To stream logs to AWS CloudWatch, you can set the following parameters. Using the example Operators above, we would add these additional parameters to enable logging to CloudWatch. You will need to ensure that you have the appropriate level of permissions (see next section)

```
        awslogs_group="/ecs/hello-world-container",
        awslogs_region="aws-region",
        awslogs_stream_prefix="ecs/hello-world-container",
```

IAM Permissions
--------------

**ECS Permissions**

You will need to ensure you have the following IAM permissions to run Tasks via this Operator

```
        {
            "Effect": "Allow",
            "Action": [
                "ecs:RunTask",
                "ecs:DescribeTasks"
            ],
            "Resource": : [
         "arn:aws:ecs:{region}:{aws account number}:cluster/{custer name a}",
         "arn:aws:ecs:{region}:{aws account number}:cluster/{cluster name b}"
        }
```

If you use the "reattach=True" (the default is False), you will need to add further permissions. You will need to add the following additional Actions to the IAM policy.

```
    "ecs:DescribeTaskDefinition",
    "ecs:ListTasks"
```

**CloudWatch Permissions**

If you plan on using awslogs to stream Apache Airflow logs into AWS CloudWatch, you will need to ensure that you have the appropriate permissions set. For example, for the following configured logs in the Operator

```
        awslogs_group="/ecs/hello-world-container",
        awslogs_region="aws-region",
        awslogs_stream_prefix="ecs/hello-world-container",
```

We would need to ensure the following IAM Permissions were added

```
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
                        "arn:aws:logs:{region}:{aws account number}:log-group:/ecs/hello-world-container:log-stream:/ecs/hello-world-container/*"
                        ]
                )
```


More information
----------------

For further information, look at:

* `Boto3 Library Documentation for ECS <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html>`__
