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

.. _howto/sensor:InfluxDB3Sensor:

InfluxDB3Sensor
===============

Use :class:`~airflow.providers.influxdb.sensors.influxdb3.InfluxDB3Sensor` to wait until an
InfluxDB 3.x SQL query returns a truthy first cell. Prefer an efficient existence query that returns
one value and limits the result to one row.

.. exampleinclude:: /../../influxdb/tests/system/influxdb/example_influxdb3.py
    :language: python
    :start-after: [START howto_sensor_influxdb3]
    :end-before: [END howto_sensor_influxdb3]

An empty result, a missing value, numeric or string zero, and an empty string are treated as false.
Set ``fail_on_empty=True`` to fail immediately when the query returns no rows.

Deferrable mode
^^^^^^^^^^^^^^^

Set ``deferrable=True`` to release the worker slot between queries. The
:class:`~airflow.providers.influxdb.triggers.influxdb3.InfluxDB3SensorTrigger` repeats the query
at the configured ``poke_interval`` until the condition is met or Airflow reaches the sensor timeout.
