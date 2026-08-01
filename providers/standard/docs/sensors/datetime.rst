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

Use the :class:`~airflow.providers.standard.sensors.time_delta.TimeDeltaSensor` to end sensing after specific time.

.. exampleinclude:: /../src/airflow/providers/standard/example_dags/example_sensors.py
    :language: python
    :dedent: 4
    :start-after: [START example_time_delta_sensor]
    :end-before: [END example_time_delta_sensor]

To run the sensor in deferrable mode, set ``deferrable=True``. See :ref:`deferring/writing` for more information.



.. _howto/operator:TimeSensor:

TimeSensor
==========

Use the :class:`~airflow.providers.standard.sensors.time_sensor.TimeSensor` to end sensing after time specified. ``TimeSensor`` can be run in deferrable mode, if a Triggerer is available.

The target moment is "today" (in the Dag's timezone) combined with ``target_time``, evaluated fresh
each time the operator is instantiated -- which, for the default and for ``deferrable=True`` execution,
happens close to the actual task run.

``start_from_trigger`` is not supported by ``TimeSensor``: the target moment can only be correctly
computed at task-execution time, not at Dag-parse time, so it cannot be handed to the triggerer ahead
of time. Setting ``start_from_trigger=True`` raises a ``ValueError``.

.. exampleinclude:: /../src/airflow/providers/standard/example_dags/example_sensors.py
    :language: python
    :dedent: 4
    :start-after: [START example_time_sensors]
    :end-before: [END example_time_sensors]


.. _howto/operator:DayOfWeekSensor:

DayOfWeekSensor
===============

Use the :class:`~airflow.sensors.weekday.DayOfWeekSensor` to sense for day of week.

.. exampleinclude:: /../src/airflow/providers/standard/example_dags/example_sensors.py
    :language: python
    :dedent: 4
    :start-after: [START example_day_of_week_sensor]
    :end-before: [END example_day_of_week_sensor]
