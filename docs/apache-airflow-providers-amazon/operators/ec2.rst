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

=========================================
Amazon Amazon Elastic Compute Cloud (EC2)
=========================================

`Amazon Elastic Compute Cloud (Amazon EC2) <https://aws.amazon.com/ec2/>`__ is a web service that provides resizable
computing capacity—literally, servers in Amazon's data centers—that you use to build and host your software systems.

Prerequisite Tasks
------------------

.. include:: _partials/prerequisite_tasks.rst

Operators
---------

.. _howto/operator:EC2StartInstanceOperator:

Start an Amazon EC2 instance
============================

To start an Amazon EC2 instance you can use
:class:`~airflow.providers.amazon.aws.operators.ec2.EC2StartInstanceOperator`.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_ec2.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_ec2_start_instance]
    :end-before: [END howto_operator_ec2_start_instance]

.. _howto/operator:EC2StopInstanceOperator:

Stop an Amazon EC2 instance
===========================

To stop an Amazon EC2 instance you can use
:class:`~airflow.providers.amazon.aws.operators.ec2.EC2StopInstanceOperator`.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_ec2.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_ec2_stop_instance]
    :end-before: [END howto_operator_ec2_stop_instance]

Sensors
-------

.. _howto/sensor:EC2InstanceStateSensor:

Wait on an Amazon EC2 instance state
====================================

To check the state of an Amazon EC2 instance and wait until it reaches the target state you can use
:class:`~airflow.providers.amazon.aws.sensors.ec2.EC2InstanceStateSensor`.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_ec2.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_ec2_instance_state]
    :end-before: [END howto_sensor_ec2_instance_state]

Reference
---------

* `Boto3 Library Documentation for EC2 <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html>`__
