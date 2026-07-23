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



.. _howto/sensor:AssetEventSensor:

AssetEventSensor
================

Use the :class:`~airflow.providers.standard.sensors.asset.AssetEventSensor` to wait for
asset events matching a set of filters to reach an expected count.

.. note::

    This sensor requires **Apache Airflow 3.4+**, because the ``partition_key``,
    ``partition_key_regexp_pattern`` and ``extra`` asset-event filters it relies on are only
    available in Airflow 3.4 and later.

Basic usage
-----------

Point the sensor at an :class:`~airflow.sdk.Asset` (or :class:`~airflow.sdk.AssetAlias`, or the raw
``name`` / ``uri`` / ``alias_name``). By default the sensor succeeds once **at least one** matching
event exists.

.. exampleinclude:: /../src/airflow/providers/standard/example_dags/example_asset_sensor.py
    :language: python
    :dedent: 4
    :start-after: [START example_asset_event_sensor]
    :end-before: [END example_asset_event_sensor]

Filtering events and expected count
-----------------------------------

Events can be narrowed with ``after`` / ``before`` (time range), ``ascending`` / ``limit``
(ordering and cap), ``partition_key`` / ``partition_key_regexp_pattern`` and ``extra`` (key/value
pairs contained in the event ``extra`` field). Use ``expected_count`` to control how many
(processed) events are required to succeed: ``-1`` (the default) means "at least one", ``0`` means
"exactly zero", and any other positive value requires an exact match.

.. exampleinclude:: /../src/airflow/providers/standard/example_dags/example_asset_sensor.py
    :language: python
    :dedent: 4
    :start-after: [START example_asset_event_sensor_filtered]
    :end-before: [END example_asset_event_sensor_filtered]

Post-processing results
-----------------------

Pass a ``process_result`` callable (or a dotted import path to one) to transform, deduplicate or
filter the fetched events **before** the count check. It receives the list of asset events and must
return a list; the processed events are pushed to XCom.

.. exampleinclude:: /../src/airflow/providers/standard/example_dags/example_asset_sensor.py
    :language: python
    :dedent: 0
    :start-after: [START example_asset_event_process_result]
    :end-before: [END example_asset_event_process_result]

Deferrable mode
---------------

Set ``deferrable=True`` to run the sensor in :ref:`deferrable mode <deferring/writing>`. In this
mode ``process_result`` runs in the triggerer, so it must be a **top-level importable function**
(defined in an installed module, not a lambda, nested function, or bound method) and its return
value must be JSON-serializable.

.. exampleinclude:: /../src/airflow/providers/standard/example_dags/example_asset_sensor.py
    :language: python
    :dedent: 4
    :start-after: [START example_asset_event_sensor_async]
    :end-before: [END example_asset_event_sensor_async]
