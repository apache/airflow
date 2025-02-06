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

:py:mod:`airflow.providers.amazon.aws.sensors.dynamodb`
=======================================================

.. py:module:: airflow.providers.amazon.aws.sensors.dynamodb


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.sensors.dynamodb.DynamoDBValueSensor




.. py:class:: DynamoDBValueSensor(table_name, partition_key_name, partition_key_value, attribute_name, attribute_value, sort_key_name = None, sort_key_value = None, aws_conn_id = DynamoDBHook.default_conn_name, region_name = None, **kwargs)


   Bases: :py:obj:`airflow.sensors.base.BaseSensorOperator`

   Waits for an attribute value to be present for an item in a DynamoDB table.

   .. seealso::
       For more information on how to use this sensor, take a look at the guide:
       :ref:`howto/sensor:DynamoDBValueSensor`

   :param table_name: DynamoDB table name
   :param partition_key_name: DynamoDB partition key name
   :param partition_key_value: DynamoDB partition key value
   :param attribute_name: DynamoDB attribute name
   :param attribute_value: DynamoDB attribute value
   :param sort_key_name: (optional) DynamoDB sort key name
   :param sort_key_value: (optional) DynamoDB sort key value
   :param aws_conn_id: aws connection to use
   :param region_name: aws region to use

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('table_name', 'partition_key_name', 'partition_key_value', 'attribute_name', 'attribute_value',...



   .. py:method:: poke(context)

      Test DynamoDB item for matching attribute value.


   .. py:method:: hook()

      Create and return a DynamoDBHook.
