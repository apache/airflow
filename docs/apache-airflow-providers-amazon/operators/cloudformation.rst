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

==================
AWS CloudFormation
==================

`AWS CloudFormation <https://aws.amazon.com/cloudformation/>`__ enables you to create and provision AWS
infrastructure deployments predictably and repeatedly. It helps you leverage AWS products such as Amazon
EC2, Amazon Elastic Block Store, Amazon SNS, Elastic Load Balancing, and Auto Scaling to build highly
reliable, highly scalable, cost-effective applications in the cloud without worrying about creating and
configuring the underlying AWS infrastructure. AWS CloudFormation enables you to use a template file to
create and delete a collection of resources together as a single unit (a stack).

Prerequisite Tasks
------------------

.. include:: ../_partials/prerequisite_tasks.rst

Operators
---------

.. _howto/operator:CloudFormationCreateStackOperator:

Create an AWS CloudFormation stack
==================================

To create a new AWS CloudFormation stack use
:class:`~airflow.providers.amazon.aws.operators.cloud_formation.CloudFormationCreateStackOperator`.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_cloudformation.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloudformation_create_stack]
    :end-before: [END howto_operator_cloudformation_create_stack]

.. _howto/operator:CloudFormationDeleteStackOperator:

Delete an AWS CloudFormation stack
==================================

To delete an AWS CloudFormation stack you can use
:class:`~airflow.providers.amazon.aws.operators.cloud_formation.CloudFormationDeleteStackOperator`.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_cloudformation.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cloudformation_delete_stack]
    :end-before: [END howto_operator_cloudformation_delete_stack]

Sensors
-------

.. _howto/sensor:CloudFormationCreateStackSensor:

Wait on an AWS CloudFormation stack creation state
==================================================

To wait on the state of an AWS CloudFormation stack creation until it reaches a terminal state you can use
:class:`~airflow.providers.amazon.aws.sensors.cloud_formation.CloudFormationCreateStackSensor`

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_cloudformation.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_cloudformation_create_stack]
    :end-before: [END howto_sensor_cloudformation_create_stack]

.. _howto/sensor:CloudFormationDeleteStackSensor:

Wait on an AWS CloudFormation stack deletion state
==================================================

To wait on the state of an AWS CloudFormation stack deletion until it reaches a terminal state you can use
use :class:`~airflow.providers.amazon.aws.sensors.cloud_formation.CloudFormationDeleteStackSensor`

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_cloudformation.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_cloudformation_delete_stack]
    :end-before: [END howto_sensor_cloudformation_delete_stack]

Reference
---------

* `AWS boto3 library documentation for CloudFormation <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/cloudformation.html>`__
