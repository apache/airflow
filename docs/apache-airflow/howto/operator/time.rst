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



.. _howto/operator:TimeDeltaSensor:

TimeDeltaSensor
===============

Use the :class:`~airflow.sensors.time_delta.TimeDeltaSensor` to end sensing after specific time.


.. exampleinclude:: /../../airflow/example_dags/example_sensors.py
    :language: python
    :dedent: 4
    :start-after: [START example_time_delta_sensor]
    :end-before: [END example_time_delta_sensor]


.. _howto/operator:TimeDeltaSensorAsync:

TimeDeltaSensorAsync
====================

Use the :class:`~airflow.sensors.time_delta.TimeDeltaSensorAsync` to end sensing after specific time.
It is an async version of the operator and requires Triggerer to run.


.. exampleinclude:: /../../airflow/example_dags/example_sensors.py
    :language: python
    :dedent: 4
    :start-after: [START example_time_delta_sensor_async]
    :end-before: [END example_time_delta_sensor_async]



.. _howto/operator:TimeSensor:

TimeSensor
==========

Use the :class:`~airflow.sensors.time_sensor.TimeSensor` to end sensing after time specified.

.. exampleinclude:: /../../airflow/example_dags/example_sensors.py
    :language: python
    :dedent: 4
    :start-after: [START example_time_sensors]
    :end-before: [END example_time_sensors]


.. _howto/operator:TimeSensorAsync:

TimeSensorAsync
===============

Use the :class:`~airflow.sensors.time_sensor.TimeSensorAsync` to end sensing after time specified.
It is an async version of the operator and requires Triggerer to run.

.. exampleinclude:: /../../airflow/example_dags/example_sensors.py
    :language: python
    :dedent: 4
    :start-after: [START example_time_sensors_async]
    :end-before: [END example_time_sensors_async]
