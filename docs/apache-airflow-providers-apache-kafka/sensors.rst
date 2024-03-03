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


Apache Kafka Sensors
====================


.. _howto/sensor:AwaitMessageSensor:

AwaitMessageSensor
------------------------

A sensor that defers until a specific message is published to a Kafka topic.
The sensor will create a consumer reading messages from a Kafka topic until a message fulfilling criteria defined in the
``apply_function`` parameter is found. If the ``apply_function`` returns any data, a ``TriggerEvent`` is raised and the ``AwaitMessageSensor`` completes successfully.

For parameter definitions take a look at :class:`~airflow.providers.apache.kafka.sensors.kafka.AwaitMessageSensor`.

Using the sensor
""""""""""""""""""


.. exampleinclude:: /../../tests/system/providers/apache/kafka/example_dag_hello_kafka.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_await_message]
    :end-before: [END howto_sensor_await_message]


Reference
"""""""""

For further information, see the `Apache Kafka Consumer documentation <https://kafka.apache.org/documentation/#consumerconfigs>`_.


.. _howto/sensor:AwaitMessageTriggerFunctionSensor:

AwaitMessageTriggerFunctionSensor
---------------------------------

Similar to the ``AwaitMessageSensor`` above, this sensor will defer until it consumes a message from a Kafka topic fulfilling the criteria
of its ``apply_function``. Once a positive event is encountered, the ``AwaitMessageTriggerFunctionSensor`` will trigger a callable provided
to ``event_triggered_function``. Afterwards the sensor will be deferred again, continuing to consume messages.

For parameter definitions take a look at :class:`~airflow.providers.apache.kafka.sensors.kafka.AwaitMessageTriggerFunctionSensor`.

Using the sensor
""""""""""""""""""

.. exampleinclude:: /../../tests/system/providers/apache/kafka/example_dag_event_listener.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_await_message_trigger_function]
    :end-before: [END howto_sensor_await_message_trigger_function]


Reference
"""""""""

For further information, see the `Apache Kafka Consumer documentation <https://kafka.apache.org/documentation/#consumerconfigs>`_.
