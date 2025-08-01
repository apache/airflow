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

===================================
Amazon Simple Systems Manager (SSM)
===================================

`Amazon Simple Systems Manager (Amazon SSM) <https://aws.amazon.com/systems-manager/>`__ is a service
that helps centrally view, manage, and operate nodes at scale in AWS, on-premises, and multi-cloud
environments. Systems Manager consolidates various tools to help complete common node tasks across AWS
accounts and Regions.
To use Systems Manager, nodes must be managed, which means SSM Agent is installed on the machine and
the agent can communicate with the Systems Manager service.

Prerequisite Tasks
------------------

.. include:: ../_partials/prerequisite_tasks.rst

Generic Parameters
------------------

.. include:: ../_partials/generic_parameters.rst

Operators
---------

.. _howto/operator:SsmRunCommandOperator:

Runs commands on one or more managed nodes
==========================================

To run SSM run command, you can use
:class:`~airflow.providers.amazon.aws.operators.ssm.SsmRunCommandOperator`.

To monitor the state of the command for a specific instance, you can use the "command_executed"
Waiter. Additionally, you can use the following components to track the status of the command execution:
:class:`~airflow.providers.amazon.aws.sensors.ssm.SsmRunCommandCompletedSensor` Sensor,
or the :class:`~airflow.providers.amazon.aws.triggers.ssm.SsmRunCommandTrigger` Trigger.


.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_ssm.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_run_command]
    :end-before: [END howto_operator_run_command]

Sensors
-------

.. _howto/sensor:SsmRunCommandCompletedSensor:

Wait for an Amazon SSM run command
==================================

To wait on the state of an Amazon SSM run command job until it reaches a terminal state you can use
:class:`~airflow.providers.amazon.aws.sensors.SSM.SsmRunCommandCompletedSensor`

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_ssm.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_run_command]
    :end-before: [END howto_sensor_run_command]

IAM Permissions
---------------

You need to ensure the following IAM permissions are granted to allow Airflow to run and monitor SSM Run Command executions:

.. code-block::

    {
      "Effect": "Allow",
      "Action": [
        "ssm:SendCommand",
        "ssm:ListCommandInvocations",
        "ssm:GetCommandInvocation"
      ],
      "Resource": "*"
    }

This policy allows access to all SSM documents and managed instances. For production environments,
it is recommended to restrict the ``Resource`` field to specific SSM document ARNs and, if applicable,
to the ARNs of intended target resources (such as EC2 instances), in accordance with the principle of least privilege.

Reference
---------

* `AWS boto3 library documentation for Amazon SSM <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ssm.html>`__
