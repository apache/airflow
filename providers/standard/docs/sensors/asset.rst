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

    This sensor requires **Apache Airflow 3.2+**. The ``partition_key``,
    ``partition_key_regexp_pattern`` and ``extra`` filters require
    **Apache Airflow 3.4+**.

When to use a schedule instead
------------------------------

If the dependency does not need to sit in the middle of a run, a schedule is
usually a better fit. See
:doc:`apache-airflow:authoring-and-scheduling/asset-scheduling` for details.

* Schedule the Dag on the asset when the whole run can wait for it.
* Use ``AssetAndTimeSchedule`` (Airflow 3.4+) when a time-scheduled run needs
  the asset before it starts.
* Use a partitioned asset timetable with ``RollupMapper`` to wait for every
  partition in a window.

Use this sensor when some tasks should run immediately and only later tasks
depend on the asset. Here, ``prepare`` starts as soon as the run does, and only
``process`` waits for the partition (``wait_for_partition_events`` is defined
:ref:`below <howto/sensor:AssetEventSensor:filtering>`):

.. exampleinclude:: /../src/airflow/providers/standard/example_dags/example_asset_sensor.py
    :language: python
    :dedent: 4
    :start-after: [START example_asset_event_sensor_mid_run]
    :end-before: [END example_asset_event_sensor_mid_run]

Basic usage
-----------

Pass an :class:`~airflow.sdk.Asset`, :meth:`~airflow.sdk.Asset.ref`, or
:class:`~airflow.sdk.AssetAlias` as ``asset``. A reference must resolve to an
existing asset when the task starts. The target is static so Airflow can record
the inlet; use templated event filters to select a run's events. By default the
sensor succeeds once **at least one** matching event exists.

.. warning::

    Without ``after``, ``before`` or a partition filter, the sensor considers
    the asset's entire event history, so it succeeds immediately if a matching
    event ever existed. Bound the query, for example by time, as below.

.. exampleinclude:: /../src/airflow/providers/standard/example_dags/example_asset_sensor.py
    :language: python
    :dedent: 4
    :start-after: [START example_asset_event_sensor]
    :end-before: [END example_asset_event_sensor]

.. note::

    For asset-triggered runs, ``dag_run.run_after`` can be the timestamp of the
    newest triggering event, and ``after`` is inclusive. If the sensor waits on
    the same asset that triggered the run, that event satisfies it immediately.
    Use ``RunStart()`` to wait for events at or after the actual run start.

A mapped sensor works, but its asset is only recorded as an inlet in lineage if
it is also passed through ``partial(inlets=[...])``.

Run-relative time bounds
------------------------

The ``after`` and ``before`` arguments accept datetime values, ISO strings,
Jinja templates, and these bound objects from
``airflow.providers.standard.sensors.asset``:

* ``RunStart()`` uses ``dag_run.start_date``, the actual start of the Dag run.
* ``LogicalDate()`` uses ``logical_date``.
* ``DataIntervalStart()`` uses ``data_interval_start``.
* ``DataIntervalEnd()`` uses ``data_interval_end``.

Each built-in object accepts a non-negative ``lookback`` duration, with zero as the
default. For example, ``RunStart(lookback=timedelta(hours=1))`` includes events
from one hour before the Dag run started. The basic example above uses this
bound, so an event from that hour can satisfy the sensor immediately.

``RunStart`` uses the actual start even for a backfill. Select a logical date
or data interval explicitly when you need historical bounds. For example:

.. code-block:: python

    from airflow.providers.standard.sensors.asset import (
        AssetEventSensor,
        DataIntervalEnd,
        DataIntervalStart,
    )

    wait_for_interval_events = AssetEventSensor(
        task_id="wait_for_interval_events",
        asset=my_asset,
        after=DataIntervalStart(),
        before=DataIntervalEnd(),
    )

Both bounds are inclusive and filter the asset event's timestamp. An event at
an interval boundary can therefore match two adjacent intervals. These bounds
do not filter the event's partition key.

A bound raises ``ValueError`` if its selected timestamp is missing. For example,
some runs do not have a logical date or data interval. The sensor does not
substitute another timestamp in that case.

Airflow resolves the objects during template rendering, before polling. Their
run-relative values stay fixed while the sensor polls and after rescheduling.

Custom resolvers
~~~~~~~~~~~~~~~~

Pass any object with a ``resolve(context)`` method that returns a datetime or
ISO string. :class:`~airflow.providers.standard.sensors.asset.AssetEventTimeBound`
defines this interface. Inheritance is optional. For example:

.. code-block:: python

    from airflow.providers.standard.sensors.asset import AssetEventSensor, AssetEventTimeBound


    class ParameterBound(AssetEventTimeBound):
        def resolve(self, context):
            return context["params"]["events_after"]


    wait_for_custom_events = AssetEventSensor(
        task_id="wait_for_custom_events",
        asset=my_asset,
        after=ParameterBound(),
        params={"events_after": "2024-01-01T00:00:00+00:00"},
    )

The same example works with ``class ParameterBound:``. Custom resolvers need
only ``resolve(context)``. The ``lookback`` argument is a convenience provided
by the built-in helpers.

Alternatively, pass a template callable that accepts the ``context`` and
``jinja_env`` keyword arguments and returns a datetime or ISO string. Airflow
evaluates the callable during template rendering. Custom resolvers and callables
must return the same bound after rescheduling. A bound based on the current clock
can move the query window and exclude events that arrived between pokes.

.. _howto/sensor:AssetEventSensor:filtering:

Filtering events and expected count
-----------------------------------

Events can be narrowed with ``after``, ``before`` (time range), ``ascending``,
``limit`` (ordering and cap), ``partition_key``,
``partition_key_regexp_pattern``, and ``extra`` (key/value pairs contained in
the event ``extra`` field). Prefer a partition key for the event's partition
identity; ``extra`` can further filter metadata, such as a validation status.

``partition_key_regexp_pattern`` requires a positive
:ref:`[api] regexp_query_timeout <apache-airflow:config:api__regexp_query_timeout>`.
With the default ``0``, regexp filtering is disabled, the query is rejected,
and the task fails.

Use ``expected_count`` and ``count_policy`` to control how many processed
events are required:

* ``count_policy="minimum"`` (the default) succeeds at or above
  ``expected_count``, which defaults to one.
* ``count_policy="exact"`` succeeds only at ``expected_count``. Use this policy
  with zero to check that no matching events exist. An exact count can be
  missed if several events arrive between pokes.

Counts must be non-negative integers, and ``limit`` must be a positive integer
when supplied. With the *minimum* policy, zero always satisfies the count
condition.

If a ``process_result`` callback is not provided, the sensor raises *ValueError*
at construction if ``limit`` is less than ``expected_count`` with the *minimum*
policy, or not greater than ``expected_count`` with the *exact* policy.

.. exampleinclude:: /../src/airflow/providers/standard/example_dags/example_asset_sensor.py
    :language: python
    :dedent: 4
    :start-after: [START example_asset_event_sensor_filtered]
    :end-before: [END example_asset_event_sensor_filtered]

Post-processing results
-----------------------

Pass a ``process_result`` callable to transform, deduplicate or filter the
fetched events before the count check. It receives the list of asset events and
must return a list; the processed events are pushed to XCom. It runs on every
poke, so it should be idempotent and free of side effects.

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
