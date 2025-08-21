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

Redis Message Queue
===================


Redis Queue Provider
--------------------

Implemented by :class:`~airflow.providers.redis.queues.redis.RedisPubSubMessageQueueProvider`


The Redis Queue Provider is a message queue provider that uses
Redis as the underlying message queue system.
It allows you to send and receive messages using Redis in your Airflow workflows.
The provider supports Redis channels.

The queue must be matching this regex:

.. exampleinclude::/../src/airflow/providers/redis/queues/redis.py
   :language: python
   :dedent: 0
   :start-after: [START queue_regexp]
   :end-before: [END queue_regexp]

Queue URI Format:

.. code-block:: text

   redis://<host>:<port>/<channel_list>

Where:

- ``host``: Redis server hostname
- ``port``: Redis server port
- ``channel_list``: Comma-separated list of Redis channels to subscribe to

The ``queue`` parameter is used to configure the underlying
:class:`~airflow.providers.redis.triggers.redis_await_message.AwaitMessageTrigger` class and
passes all kwargs directly to the trigger constructor, if provided.

Channels can also be specified via the Queue URI instead of the ``channels`` kwarg. The provider will extract channels from the URI as follows:

.. exampleinclude:: /../src/airflow/providers/redis/queues/redis.py
   :language: python
   :dedent: 0
   :start-after: [START extract_channels]
   :end-before: [END extract_channels]


Below is an example of how you can configure an Airflow DAG to be triggered by a message in Redis.

.. exampleinclude:: /../tests/system/redis/example_dag_message_queue_trigger.py
   :language: python
   :dedent: 0
   :start-after: [START howto_trigger_message_queue]
   :end-before: [END howto_trigger_message_queue]
