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


Timetables
==========

For a Dag with a time-based schedule (as opposed to event-driven), the Dag's internal "timetable"
drives scheduling.  The timetable also determines the data interval and the logical date of
each run created for the Dag.

Dags scheduled with a cron expression or ``timedelta`` object are
internally converted to always use a timetable.

If a cron expression or ``timedelta`` is sufficient for your use case, you don't need
to worry about writing a custom timetable because Airflow has default timetables that handle those cases.
But for more complicated scheduling requirements,
you can create your own timetable class and pass that to the Dags ``schedule`` argument.

Some examples of when custom timetable implementations are useful:

* Task runs that occur at different times each day. For example, an astronomer might find it
  useful to run a task at dawn to process data collected from the previous
  night-time period.
* Schedules that don't follow the Gregorian calendar. For example, create a run for
  each month in the `Traditional Chinese Calendar`_. This is conceptually
  similar to the sunrise case, but for a different time scale.
* Rolling windows, or overlapping data intervals. For example, you might want to
  have a run each day, but make each run cover the period of the previous seven
  days. It is possible to hack this with a cron expression, but a custom data
  interval provides a more natural representation.
* Data intervals with "holes" between intervals instead of a contiguous window. Built-in
  data-interval cron and ``timedelta`` timetables always produce contiguous intervals; the
  default trigger timetables use a zero-width interval at each tick. See :ref:`data-interval`.

.. _`Traditional Chinese Calendar`: https://en.wikipedia.org/wiki/Chinese_calendar

Airflow allows you to write custom timetables in plugins and used by
Dags. You can find an example demonstrating a custom timetable in the
:doc:`/howto/timetable` how-to guide.

.. note::

    As a general rule, always access Variables, Connections, or anything else that needs access to
    the database as late as possible in your code. See :ref:`best_practices/timetables`
    for more best practices to follow.

Built-in Timetables
-------------------

Airflow comes with several common timetables built-in to cover the most common use cases. Additional timetables
may be available in plugins.

.. _DeltaTriggerTimetable:

DeltaTriggerTimetable
^^^^^^^^^^^^^^^^^^^^^

A timetable that accepts a :class:`datetime.timedelta` or ``dateutil.relativedelta.relativedelta``, and runs
the Dag once a delta passes.

.. seealso:: `Differences between "trigger" and "data interval" timetables`_

.. code-block:: python

    from datetime import timedelta

    from airflow.timetables.trigger import DeltaTriggerTimetable


    @dag(schedule=DeltaTriggerTimetable(timedelta(days=7)), ...)  # Once every week.
    def example_dag():
        pass

You can also provide a static data interval to the timetable. The optional ``interval`` argument also
should be a :class:`datetime.timedelta` or ``dateutil.relativedelta.relativedelta``. When using these
arguments, a triggered Dag run's data interval spans the specified duration, and *ends* with the trigger time.

.. code-block:: python

    from datetime import UTC, datetime, timedelta

    from dateutil.relativedelta import relativedelta, FR

    from airflow.timetables.trigger import DeltaTriggerTimetable


    @dag(
        # Runs every Friday at 18:00 to cover the work week.
        schedule=DeltaTriggerTimetable(
            relativedelta(weekday=FR(), hour=18),
            interval=timedelta(days=4, hours=9),
        ),
        start_date=datetime(2025, 1, 3, 18, tzinfo=UTC),
        ...,
    )
    def example_dag():
        pass


.. _CronTriggerTimetable:

CronTriggerTimetable
^^^^^^^^^^^^^^^^^^^^

A timetable that accepts a cron expression, and triggers Dag runs according to it.

.. seealso:: `Differences between "trigger" and "data interval" timetables`_

.. code-block:: python

    from airflow.timetables.trigger import CronTriggerTimetable


    @dag(schedule=CronTriggerTimetable("0 1 * * 3", timezone="UTC"), ...)  # At 01:00 on Wednesday
    def example_dag():
        pass

You can also provide a static data interval to the timetable. The optional ``interval`` argument
must be a :class:`datetime.timedelta` or ``dateutil.relativedelta.relativedelta``. When using these arguments, a triggered Dag run's data interval spans the specified duration, and *ends* with the trigger time.

.. code-block:: python

    from datetime import timedelta

    from airflow.timetables.trigger import CronTriggerTimetable


    @dag(
        # Runs every Friday at 18:00 to cover the work week (9:00 Monday to 18:00 Friday).
        schedule=CronTriggerTimetable(
            "0 18 * * 5",
            timezone="UTC",
            interval=timedelta(days=4, hours=9),
        ),
        ...,
    )
    def example_dag():
        pass


.. _MultipleCronTriggerTimetable:

MultipleCronTriggerTimetable
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This is similar to CronTriggerTimetable_ except it takes multiple cron expressions. A Dag run is scheduled whenever any of the expressions matches the time. It is particularly useful when the desired schedule cannot be expressed by one single cron expression.

.. code-block:: python

    from airflow.timetables.trigger import MultipleCronTriggerTimetable


    # At 1:10 and 2:40 each day.
    @dag(schedule=MultipleCronTriggerTimetable("10 1 * * *", "40 2 * * *", timezone="UTC"), ...)
    def example_dag():
        pass

The same optional ``interval`` argument as CronTriggerTimetable_ is also available.

.. code-block:: python

    from datetime import timedelta

    from airflow.timetables.trigger import MultipleCronTriggerTimetable


    @dag(
        schedule=MultipleCronTriggerTimetable(
            "10 1 * * *",
            "40 2 * * *",
            timezone="UTC",
            interval=timedelta(hours=1),
        ),
        ...,
    )
    def example_dag():
        pass


.. _DeltaDataIntervalTimetable:

DeltaDataIntervalTimetable
^^^^^^^^^^^^^^^^^^^^^^^^^^

A timetable that schedules data intervals with a time delta. You can select it by providing a
:class:`DeltaDataIntervalTimetable` to the ``schedule`` parameter of a Dag.

This timetable focuses on the data interval value and does not necessarily align execution dates with
arbitrary bounds, such as the start of day or of hour.

.. seealso:: `Differences between the cron and delta data interval timetables`_

.. code-block:: python

    from datetime import timedelta

    from airflow.sdk import dag, DeltaDataIntervalTimetable


    @dag(schedule=DeltaDataIntervalTimetable(timedelta(minutes=30)))
    def example_dag():
        pass

.. _CronDataIntervalTimetable:

CronDataIntervalTimetable
^^^^^^^^^^^^^^^^^^^^^^^^^

A timetable that accepts a cron expression, creates data intervals according to the interval between each cron
trigger points, and triggers a Dag run at the end of each data interval. You can select it by providing a
:class:`CronDataIntervalTimetable` to the ``schedule`` parameter of a Dag.

.. seealso:: `Differences between "trigger" and "data interval" timetables`_
.. seealso:: `Differences between the cron and delta data interval timetables`_

.. code-block:: python

    from airflow.sdk import dag, CronDataIntervalTimetable


    @dag(schedule=CronDataIntervalTimetable("0 1 * * 3"))  # At 01:00 on Wednesday.
    def example_dag():
        pass

EventsTimetable
^^^^^^^^^^^^^^^

Pass a list of ``datetime``\s for the Dag to run after. This can be useful for timing based on sporting
events, planned communication campaigns, and other schedules that are arbitrary and irregular, but predictable.

The list of events must be finite and of reasonable size as it must be loaded every time the Dag is parsed. Optionally, use
the ``restrict_to_events`` flag to force manual runs of the Dag that use the time of the most recent, or very
first, event for the data interval. Otherwise, manual runs begin with a ``data_interval_start`` and
``data_interval_end`` equal to the time at which the manual run started. You can also name the set of events using the
``description`` parameter, which will be displayed in the Airflow UI.

.. code-block:: python

    from airflow.timetables.events import EventsTimetable


    @dag(
        schedule=EventsTimetable(
            event_dates=[
                pendulum.datetime(2022, 4, 5, 8, 27, tz="America/Chicago"),
                pendulum.datetime(2022, 4, 17, 8, 27, tz="America/Chicago"),
                pendulum.datetime(2022, 4, 22, 20, 50, tz="America/Chicago"),
            ],
            description="My Team's Baseball Games",
            restrict_to_events=False,
        ),
        ...,
    )
    def example_dag():
        pass

.. _asset-timetable-section:

AssetOrTimeSchedule
^^^^^^^^^^^^^^^^^^^

Combining conditional asset expressions with time-based schedules enhances scheduling flexibility.

The ``AssetOrTimeSchedule`` is a specialized timetable that allows for the scheduling of Dags based on both time-based schedules and asset events. It also facilitates the creation of both scheduled runs, as per traditional timetables, and asset-triggered runs, which operate independently.

This feature is particularly useful in scenarios where a Dag needs to run on asset updates and also at periodic intervals. It ensures that the workflow remains responsive to data changes and consistently runs regular checks or updates.

Here's an example of a Dag using ``AssetOrTimeSchedule``:

.. code-block:: python

    from airflow.timetables.assets import AssetOrTimeSchedule
    from airflow.timetables.trigger import CronTriggerTimetable


    @dag(
        schedule=AssetOrTimeSchedule(
            timetable=CronTriggerTimetable("0 1 * * 3", timezone="UTC"), assets=(dag1_asset & dag2_asset)
        ),
        ...,
    )
    def example_dag():
        pass


Timetables comparisons
----------------------

.. _Differences between "trigger" and "data interval" timetables:

Differences between "trigger" and "data interval" timetables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Airflow has two sets of timetables for cron and delta schedules:

* CronTriggerTimetable_ and CronDataIntervalTimetable_ both accept a cron expression.
* DeltaTriggerTimetable_ and DeltaDataIntervalTimetable_ both accept a timedelta or relativedelta.

In Airflow 3, a bare cron string such as ``@daily`` in ``schedule=`` resolves to
CronTriggerTimetable_ by default (``[scheduler] create_cron_data_intervals`` is
``False``). A bare ``timedelta`` resolves to DeltaTriggerTimetable_ by default
(``[scheduler] create_delta_data_intervals`` is also ``False``). Pass an explicit
data-interval timetable class, or set ``create_cron_data_intervals`` to ``True``,
to get contiguous windows instead.

.. note::

    ``[scheduler] create_delta_data_intervals`` is intended to control timedelta
    schedules independently, but is not currently consulted. Those schedules
    follow ``create_cron_data_intervals`` as well, so setting only
    ``create_delta_data_intervals=True`` still yields DeltaTriggerTimetable_.

- A trigger timetable (CronTriggerTimetable_ or DeltaTriggerTimetable_) represents
  each run as a point in time: by default ``data_interval_start`` and
  ``data_interval_end`` are the same (the trigger time). You can optionally pass a
  non-zero ``interval=`` so the data interval ends at the trigger time and spans
  that duration. A data-interval timetable (CronDataIntervalTimetable_ or
  DeltaDataIntervalTimetable_) always uses a contiguous non-zero window between
  consecutive schedule boundaries.
- ``logical_date`` and the timestamp used in ``run_id`` differ between the two
  kinds based on how they handle the data interval, as described in
  :ref:`timetables_run_id_logical_date`.

*Data Interval* Shape
~~~~~~~~~~~~~~~~~~~~~

A trigger timetable uses a *point* (zero-width) data interval by default. This
means that the values of ``data_interval_start`` and ``data_interval_end`` are
the same, the time when a Dag run is triggered. Passing a non-zero
``interval=`` makes the interval end at the trigger time and begin ``interval``
earlier.

For a data interval timetable, the values of ``data_interval_start`` and
``data_interval_end`` are different. ``data_interval_end`` is the time when a
Dag run is triggered (``run_after``), while ``data_interval_start`` is the start
of the contiguous window. ``logical_date`` is ``data_interval_start`` for both
kinds.

*Catchup* behavior
~~~~~~~~~~~~~~~~~~

By default, ``catchup`` is ``False`` (Airflow config
``[scheduler] catchup_by_default``). Missed scheduled run times between
``start_date`` and "now" are not backfilled when a Dag is activated or
re-enabled. The timetable instead selects the most recently applicable
scheduled run time:

- For CronTriggerTimetable_, the latest cron tick that is not after "now" and
  not before ``start_date``. For DeltaTriggerTimetable_, pickup time itself —
  a delta has no wall-clock tick to snap to.
- For a data-interval timetable, the most recently completed interval whose end
  is not after "now".

If you set ``catchup=True``, the scheduler creates Dag runs from the latest
automated run (or ``start_date`` if none) forward to "now", one scheduled run
time (or completed interval) at a time in chronological order. Those runs may
execute concurrently up to the Dag's ``max_active_runs`` (defaulted from
:ref:`config:core__max_active_runs_per_dag`).

Catchup also applies when you pause a Dag for a period and then re-enable it.

See :ref:`dag-catchup` for a worked example with ``@daily``.

.. _timetables_run_id_logical_date:

The time when a Dag run is triggered
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Both trigger and data interval timetables can create the first Dag run
immediately when ``catchup=False`` and ``start_date`` is in the past. What
differs is *which* run is selected and how ``logical_date`` / ``run_id`` are
derived. Without a ``start_date`` (optional when ``catchup=False``), a trigger
timetable with the default ``run_immediately=False`` waits for the next future
tick instead — midnight on February 1st in the example below.

``logical_date`` is always ``data_interval_start``. The timestamp embedded in
``run_id`` comes from ``run_after`` (when the run is eligible to start). For a
zero-width trigger timetable those coincide; for a data-interval timetable
``run_after`` is ``data_interval_end``, so the ``run_id`` timestamp is one
period after ``logical_date``.

For example, suppose there is a cron expression ``@daily`` or ``0 0 * * *``, a
past ``start_date``, and ``catchup=False``. If you enable the Dag at 3PM on
January 31st,

- `CronTriggerTimetable`_ immediately creates a Dag run for the most recent
  tick — midnight on January 31st. ``logical_date``, ``data_interval_start``,
  ``data_interval_end``, and the ``run_id`` timestamp are all that midnight.
- `CronDataIntervalTimetable`_ immediately creates a Dag run for the most
  recently completed interval (midnight January 30 through midnight
  January 31st). ``logical_date`` / ``data_interval_start`` are January 30;
  ``run_after`` and the ``run_id`` timestamp are midnight on January 31st.

The following is another example showing the difference when skipping Dag runs:

Suppose there are two running Dags with a cron expression ``@daily`` or
``0 0 * * *`` that use the two different timetables. If you pause the Dags at
3PM on January 31st and re-enable them at 3PM on February 2nd (still with
``catchup=False``),

- `CronTriggerTimetable`_ skips the missed tick on February 1st. It
  immediately creates a Dag run for midnight on February 2nd (the most recent
  applicable tick). The next future tick is midnight on February 3rd.
- `CronDataIntervalTimetable`_ skips the completed interval that would have
  ended at midnight on February 1st (January 31st through February 1st). It
  immediately creates a Dag run for the most recently completed interval
  (February 1st through February 2nd). The still-open interval ending at the
  next midnight is not created yet.

In these examples, a trigger timetable creates Dag runs at schedule ticks
people typically expect for a workflow clock, while a data-interval timetable
is designed around the contiguous data window each run processes.

Switching between trigger and data interval timetables on an existing Dag
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The two kinds of timetable can disagree on ``logical_date`` for the same
schedule tick: a zero-width trigger run uses the trigger time, while a
data-interval run uses ``data_interval_start``. Switching a Dag from a
trigger timetable to a data-interval timetable when it already has existing
dagruns will skip one scheduled run, because the next run is advanced one
period to avoid colliding with the previous run's ``logical_date``. The
reverse direction (data interval -> trigger) does not skip a run.

This transition can happen without editing a Dag, in two ways:

- Flipping ``[scheduler] create_cron_data_intervals`` changes how every Dag
  with a bare cron string in ``schedule=`` resolves its timetable.
- Crossing a version boundary where the default differs. Airflow 3 defaults
  to ``False``; Airflow 2.x defaults to ``True``.

To keep ``logical_date`` semantics stable across either change, decide which
timetable you want and pin it before the change: set the flag explicitly to
the same value on both sides, or convert affected Dags to use an explicit
timetable instance in ``schedule=`` so the flag no longer applies.


.. _Differences between the cron and delta data interval timetables:

Differences between the cron and delta data interval timetables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Choosing between `DeltaDataIntervalTimetable`_ and `CronDataIntervalTimetable`_ depends on your use case.
If you enable a Dag at 01:05 on February 1st, the following table summarizes the Dag runs created and the
data interval that they cover, depending on 3 arguments: ``schedule``, ``start_date`` and ``catchup``.

.. list-table::
   :header-rows: 1

   * - ``schedule``
     - ``start_date``
     - ``catchup``
     - Intervals covered
     - Remarks

   * - ``*/30 * * * *``
     - ``year-02-01``
     - ``True``
     - * 00:00 - 00:30
       * 00:30 - 01:00
     - Same behavior than using the timedelta object.

   * - ``*/30 * * * *``
     - ``year-02-01``
     - ``False``
     - * 00:30 - 01:00
     -

   * - ``*/30 * * * *``
     - ``year-02-01 00:10``
     - ``True``
     - * 00:30 - 01:00
     - Interval 00:00 - 00:30 is not after the start date, and so is skipped.

   * - ``*/30 * * * *``
     - ``year-02-01 00:10``
     - ``False``
     - * 00:30 - 01:00
     - Whatever the start date, the data intervals are aligned with hour/day/etc. boundaries.

   * - ``datetime.timedelta(minutes=30)``
     - ``year-02-01``
     - ``True``
     - * 00:00 - 00:30
       * 00:30 - 01:00
     - Same behavior than using the cron expression.

   * - ``datetime.timedelta(minutes=30)``
     - ``year-02-01``
     - ``False``
     - * 00:35 - 01:05
     - Interval is not aligned with start date but with the current time.

   * - ``datetime.timedelta(minutes=30)``
     - ``year-02-01 00:10``
     - ``True``
     - * 00:10 - 00:40
     - Interval is aligned with start date. Next one will be triggered in 5 minutes covering 00:40 - 01:10.

   * - ``datetime.timedelta(minutes=30)``
     - ``year-02-01 00:10``
     - ``False``
     - * 00:35 - 01:05
     - Interval is aligned with current time. Next run will be triggered in 30 minutes.
