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



.. _howto/operator:WebSocketSensor:

WebSocketSensor
================

Use the :class:`~airflow.providers.standard.sensors.websocket.WebSocketSensor` to wait for a message on a
``ws://`` or ``wss://`` WebSocket connection. This is useful when a remote server accepts a long-lived
connection and replies asynchronously once it has handled the requested work, since a plain HTTP request
is not well suited to that kind of long-lived connection. Requires the ``websocket`` extra
(``apache-airflow-providers-standard[websocket]``).

.. exampleinclude:: /../src/airflow/providers/standard/example_dags/example_sensors.py
    :language: python
    :dedent: 4
    :start-after: [START example_websocket_sensor]
    :end-before: [END example_websocket_sensor]

Also for this job you can use sensor in the deferrable mode:

.. exampleinclude:: /../src/airflow/providers/standard/example_dags/example_sensors.py
    :language: python
    :dedent: 4
    :start-after: [START example_websocket_sensor_async]
    :end-before: [END example_websocket_sensor_async]

A common use case is to send a request over the connection right after it opens (via
``message_to_send``) and then wait for the remote server's asynchronous reply, optionally
passing connection headers such as an auth token via ``header``:

.. exampleinclude:: /../src/airflow/providers/standard/example_dags/example_sensors.py
    :language: python
    :dedent: 4
    :start-after: [START example_websocket_sensor_send_message_async]
    :end-before: [END example_websocket_sensor_send_message_async]

.. warning::
    In deferrable mode, ``message_to_send`` may be sent more than once for a single task
    run. Airflow triggers are not guaranteed to execute exactly once — a triggerer
    restart or redistribution to another host re-runs the trigger from scratch, opening
    a new connection and re-sending ``message_to_send``. If that message has a side
    effect on the remote server, such as starting a job, the server must treat a resend
    as safe — for example by deduplicating on a request id embedded in the message.
