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

===============
Amazon DynamoDB
===============

`Amazon DynamoDB <https://aws.amazon.com/dynamodb/>`__ Amazon DynamoDB is a
fully managed, serverless, key-value NoSQL database designed to run
high-performance applications at any scale. DynamoDB offers built-in security,
continuous backups, automated multi-Region replication, in-memory caching, and
data import and export tools.

Prerequisite Tasks
------------------

.. include:: ../_partials/prerequisite_tasks.rst


Sensors
-------

.. _howto/sensor:DynamoDBValueSensor:

Wait on Amazon DynamoDB item attribute value match
==================================================

Use the :class:`~airflow.providers.amazon.aws.sensors.dynamodb.DynamoDBValueSensor`
to wait for the presence of a matching DynamoDB item's attribute/value pair.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_dynamodb.py
    :language: python
    :start-after: [START howto_sensor_dynamodb]
    :dedent: 4
    :end-before: [END howto_sensor_dynamodb]


Reference
---------

* `AWS boto3 library documentation for DynamoDB <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html>`__
