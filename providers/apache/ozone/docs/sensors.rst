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

Apache Ozone Sensors
====================

Prerequisites
-------------

To use sensors, configure an :doc:`Ozone Connection <connections>` and
make sure the worker runtime can execute the native ``ozone`` CLI.

OzoneKeySensor
--------------

``OzoneKeySensor`` waits for a file or directory to appear in Ozone FS.
It is useful for:

* waiting for marker files such as ``_SUCCESS``;
* waiting for upstream ingestion into a landing path;
* coordinating DAGs that exchange data through Ozone.

The sensor uses ``OzoneFsHook`` internally and checks a target ``ofs://`` or
``o3fs://`` path with the provider's standard connection-driven runtime
configuration.

Behavior notes
--------------

* ``path`` is templated, so Jinja expressions can be used in DAGs.
* ``timeout`` is the timeout in seconds for a single Ozone CLI existence check.
  The sensor stores this value internally as ``cli_timeout`` before calling
  ``OzoneFsHook.key_exists(...)``.
* ``timeout`` is intentionally not forwarded to ``BaseSensorOperator`` and does
  not control the overall waiting horizon of the sensor. Use standard Airflow
  sensor arguments, such as ``poke_interval`` and other ``BaseSensorOperator``
  options, for sensor scheduling behavior.
* retryable CLI errors are treated as "not yet ready" and do not fail the task
  immediately;
* non-retryable CLI errors are raised to Airflow as task failures.

Example:

.. code-block:: python

    from airflow.providers.apache.ozone.sensors.ozone import OzoneKeySensor

    wait_for_marker = OzoneKeySensor(
        task_id="wait_for_marker",
        path="ofs://om-service/analytics/landing/{{ ds_nodash }}/_SUCCESS",
        ozone_conn_id="ozone_default",
        timeout=60,
        poke_interval=30,
    )
