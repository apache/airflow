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

For DAGs with time-based schedules (as opposed to event-driven), the scheduling
decisions are driven by its internal "timetable".  The timetable also
determines the data interval and the logical date of each run created for the DAG.

DAGs scheduled with a cron expression or ``timedelta`` object are
internally converted to always use a timetable.

If a cron expression or ``timedelta`` is sufficient for your use case, you don't need
to worry about writing a custom timetable because Airflow has default timetables that handle those cases.
But for more complicated scheduling requirements,
you may create your own timetable class and pass that to the DAG's ``schedule`` argument.

Here are some examples of when custom timetable implementations are useful:

* Data intervals with "holes" between. (Instead of continuous, as both the cron
  expression and ``timedelta`` schedules represent.)
* Run tasks at different times each day. For example, an astronomer may find it
  useful to run a task at dawn to process data collected from the previous
  night-time period.
* Schedules not following the Gregorian calendar. For example, create a run for
  each month in the `Traditional Chinese Calendar`_. This is conceptually
  similar to the sunset case above, but for a different time scale.
* Rolling windows, or overlapping data intervals. For example, one may want to
  have a run each day, but make each run cover the period of the previous seven
  days. It is possible to "hack" this with a cron expression, but a custom data
  interval would be a more natural representation.

.. _`Traditional Chinese Calendar`: https://en.wikipedia.org/wiki/Chinese_calendar

As such, Airflow allows for custom timetables to be written in plugins and used by
DAGs. An example demonstrating a custom timetable can be found in the
:doc:`/howto/timetable` how-to guide.

.. note::

    As a general rule, always access Variables, Connections etc or anything that would access
    the database as late as possible in your code. See :ref:`best_practices/timetables`
    for more best practices to follow.

Built-in Timetables
-------------------

Airflow comes with several common timetables built in to cover the most common use cases. Additional timetables
may be available in plugins.

.. _CronTriggerTimetable:

CronTriggerTimetable
^^^^^^^^^^^^^^^^^^^^

A timetable that accepts a cron expression, and triggers DAG runs according to it.

.. seealso:: `Differences between the two cron timetables`_

.. code-block:: python

    from airflow.timetables.trigger import CronTriggerTimetable


    @dag(
        timetable=CronTriggerTimetable("0 1 * * 3", timezone="UTC"),  # At 01:00 on Wednesday
    )
    def example_dag():
        pass

It is also possible to provide a static data interval to the timetable. The optional ``interval`` argument
must be a :class:`datetime.timedelta` or ``dateutil.relativedelta.relativedelta``. If given, a triggered DAG
run's data interval would span the specified duration, and *ends* with the trigger time.

.. code-block:: python

    from datetime import timedelta

    from airflow.timetables.trigger import CronTriggerTimetable


    @dag(
        # Runs every Friday at 18:00 to cover the work week (9:00 Monday to 18:00 Friday).
        timetable=CronTriggerTimetable(
            "0 18 * * 5",
            timezone="UTC",
            interval=timedelta(days=4, hours=9),
        ),
    )
    def example_dag():
        pass


.. _DeltaDataIntervalTimetable:

DeltaDataIntervalTimetable
^^^^^^^^^^^^^^^^^^^^^^^^^^

Schedules data intervals with a time delta. Can be selected by providing a
:class:`datetime.timedelta` or ``dateutil.relativedelta.relativedelta`` to the ``schedule`` parameter of a DAG.

.. code-block:: python

    @dag(schedule=datetime.timedelta(minutes=30))
    def example_dag():
        pass

.. _CronDataIntervalTimetable:

CronDataIntervalTimetable
^^^^^^^^^^^^^^^^^^^^^^^^^

A timetable that accepts a cron expression, creates data intervals according to the interval between each cron
trigger points, and triggers a DAG run at the end of each data interval.

.. seealso:: `Differences between the two cron timetables`_

This can be selected by providing a string that is a valid cron expression to the ``schedule``
parameter of a DAG as described in the :doc:`/concepts/dags` documentation.

.. code-block:: python

    @dag(schedule="0 1 * * 3")  # At 01:00 on Wednesday.
    def example_dag():
        pass

EventsTimetable
^^^^^^^^^^^^^^^

Simply pass a list of ``datetime``\s for the DAG to run after. Useful for timing based on sporting
events, planned communication campaigns, and other schedules that are arbitrary and irregular but predictable.

The list of events must be finite and of reasonable size as it must be loaded every time the DAG is parsed. Optionally,
the ``restrict_to_events`` flag can be used to force manual runs of the DAG to use the time of the most recent (or very
first) event for the data interval, otherwise manual runs will run with a ``data_interval_start`` and
``data_interval_end`` equal to the time at which the manual run was begun. You can also name the set of events using the
``description`` parameter, which will be displayed in the Airflow UI.

.. code-block:: python

    from airflow.timetables.events import EventsTimetable


    @dag(
        timetable=EventsTimetable(
            event_dates=[
                pendulum.datetime(2022, 4, 5, 8, 27, tz="America/Chicago"),
                pendulum.datetime(2022, 4, 17, 8, 27, tz="America/Chicago"),
                pendulum.datetime(2022, 4, 22, 20, 50, tz="America/Chicago"),
            ],
            description="My Team's Baseball Games",
            restrict_to_events=False,
        ),
    )
    def example_dag():
        pass

.. _Differences between the two cron timetables:

Differences between the two cron timetables
-------------------------------------------

There are two timetables `CronTriggerTimetable`_ and `CronDataIntervalTimetable`_ that accepts a cron expression.
There are some differences between the two:
- `CronTriggerTimetable`_ does not take care of *Data Interval*, while `CronDataIntervalTimetable`_ does.
- The time when a DAG run is triggered by `CronTriggerTimetable`_ is more intuitive and more similar to what people
expect cron to behave than that of `CronDataIntervalTimetable`_ (when ``catchup`` is ``False``).

Whether taking care of *Data Interval*
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

`CronTriggerTimetable`_ *does not* care the idea of *data interval*. It means the value of ``data_interval_start``,
``data_interval_end`` and legacy ``execution_date`` are the same - the time when a DAG run is triggered.

On the other hand, `CronDataIntervalTimetable`_ *does* care the idea of *data interval*. It means the value of
``data_interval_start`` and ``data_interval_end`` (and legacy ``execution_date``) are different. They are the start
and end of the interval respectively.

The time when a DAG run is triggered
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

There is no difference between the two when ``catchup`` is ``True``. :ref:`dag-catchup` tells you how DAG runs are
triggered when ``catchup`` is ``True``.

When ``catchup`` is ``False``, there is difference in how a new DAG run is triggered. `CronTriggerTimetable`_ triggers
a new DAG run *after* the current time, while `CronDataIntervalTimetable`_ does *before* the current time (assuming
the value of ``start_date`` is past time).

Here is an example showing how the first DAG run is triggered. Supposes there is a cron expression ``@daily`` or
``0 0 * * *``, which is aimed to run at 12AM every day. If you enable DAGs using the two timetables at 3PM on January
31st, `CronTriggerTimetable`_ will trigger a new DAG run at 12AM on February 1st. `CronDataIntervalTimetable`_, on the other
hand, will immediately trigger a new DAG run which is supposed to trigger at 12AM on January 31st if the DAG had been
enabled beforehand.

This is another example showing the difference in the case of skipping DAG runs. Suppose there are two running DAGs
using the two timetables with a cron expression ``@daily`` or ``0 0 * * *``. If you pause the DAGs at 3PM on January
31st and re-enable them at 3PM on February 2nd, `CronTriggerTimetable`_ skips the DAG runs which are supposed to
trigger on February 1st and 2nd. The next DAG run will be triggered at 12AM on February 3rd. `CronDataIntervalTimetable`_,
on the other hand, skips the DAG runs which are supposed to trigger on February 1st only. A DAG run for February 2nd
is immediately triggered after you re-enable the DAG.

By these examples, you see how `CronTriggerTimetable`_ triggers DAG runs is more intuitive and more similar to what
people expect cron to behave than how `CronDataIntervalTimetable`_ does.
