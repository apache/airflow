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

Use this sensor when an already-running Dag needs an asset between tasks. For example, a
daily consumer can run its preparation tasks immediately, then wait for another Dag to
produce the partition for its own data interval. Moving that dependency to an asset schedule
would change when the entire Dag starts and the data interval used by the consumer.

The sensor declares its target as an inlet, so the dependency is visible in asset lineage
without changing the Dag's schedule. It reads events through ``context["inlet_events"]``.

.. note::

    This sensor requires **Apache Airflow 3.4+**, because the ``partition_key``,
    ``partition_key_regexp_pattern`` and ``extra`` asset-event filters it relies on are only
    available in Airflow 3.4 and later.

Basic usage
-----------

Pass an :class:`~airflow.sdk.Asset` or :class:`~airflow.sdk.AssetAlias` as ``obj``, or use the raw
``name`` / ``uri`` / ``alias_name`` arguments. A name-only or URI-only target is an asset reference
and must resolve to an existing asset when the task context is created. Target identifiers are
static so Airflow can record the inlet; use templated event filters to select a run's events.
By default the sensor succeeds once **at least one** matching event exists.

Choose one target form: ``obj``, ``alias_name``, or ``name`` / ``uri``. The name and URI may be
supplied together, but other combinations are rejected to avoid silently selecting a different asset.

.. exampleinclude:: /../src/airflow/providers/standard/example_dags/example_asset_sensor.py
    :language: python
    :dedent: 4
    :start-after: [START example_asset_event_sensor]
    :end-before: [END example_asset_event_sensor]

Filtering events and expected count
-----------------------------------

Events can be narrowed with ``after`` / ``before`` (time range), ``ascending`` / ``limit``
(ordering and cap), ``partition_key`` / ``partition_key_regexp_pattern`` and ``extra`` (key/value
pairs contained in the event ``extra`` field). Prefer a partition key for the event's partition
identity; ``extra`` can further filter metadata, such as a validation status.

Use ``expected_count`` and ``count_policy`` to control how many processed events are required:

* ``count_policy="minimum"`` (the default) succeeds at or above ``expected_count``, which defaults to one.
* ``count_policy="exact"`` succeeds only at ``expected_count``. Use this policy with zero to check
  that no matching events exist. An exact count can be missed if several events arrive between pokes.

Counts must be non-negative integers, and ``limit`` must be a positive integer when supplied.
With the minimum policy, zero always satisfies the count condition.
If no ``process_result`` callback is provided, a ``limit`` below ``expected_count`` is rejected
at construction instead of waiting until the sensor times out.

.. exampleinclude:: /../src/airflow/providers/standard/example_dags/example_asset_sensor.py
    :language: python
    :dedent: 4
    :start-after: [START example_asset_event_sensor_filtered]
    :end-before: [END example_asset_event_sensor_filtered]

Post-processing results
-----------------------

Pass a ``process_result`` callable (or a dotted import path to one) to transform, deduplicate or
filter the fetched events **before** the count check. It receives the list of asset events and must
return a list; the processed events are pushed to XCom. It runs on every poke, so it should be
idempotent and free of side effects.

The query applies ``limit`` **before** processing. A callback may expand the fetched list, so
``limit < expected_count`` is allowed when a callback is supplied; the count is checked on its
output. Choose the limit with any filtering or deduplication in mind.

With ascending order and a limit, each poke returns the same oldest matching events while those
events remain in the query range. If the callback rejects them, newer acceptable events beyond
the limit are never inspected. Prefer query filters such as ``partition_key``, ``extra`` and time
bounds where possible. For a check of recent events, use ``ascending=False`` with a suitable limit.

.. exampleinclude:: /../src/airflow/providers/standard/example_dags/example_asset_sensor.py
    :language: python
    :dedent: 0
    :start-after: [START example_asset_event_process_result]
    :end-before: [END example_asset_event_process_result]

.. exampleinclude:: /../src/airflow/providers/standard/example_dags/example_asset_sensor.py
    :language: python
    :dedent: 4
    :start-after: [START example_asset_event_sensor_process_result]
    :end-before: [END example_asset_event_sensor_process_result]
