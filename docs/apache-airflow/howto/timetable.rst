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


Customizing DAG Scheduling with Timetables
==========================================

A DAG's scheduling strategy is determined by its internal "timetable". This
timetable can be created by specifying the DAG's ``schedule_interval`` argument,
as described in :doc:`DAG Run </dag-run>`. The timetable also dictates the data
interval and the logical time of each run created for the DAG.

However, there are situations when a cron expression or simple ``timedelta``
periods cannot properly express the schedule. Some of the examples are:

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


For our example, let's say a company wants to run a job after each weekday to
process data collected during the work day. The first intuitive answer to this
would be ``schedule_interval="0 0 * * 1-5"`` (midnight on Monday to Friday), but
this means data collected on Friday will *not* be processed right after Friday
ends, but on the next Monday, and that run's interval would be from midnight
Friday to midnight *Monday*.

This is, therefore, an example in the "holes" category above; the intended
schedule should not include the two weekend days. What we want is:

* Schedule a run for each Monday, Tuesday, Wednesday, Thursday, and Friday. The
  run's data interval would cover from midnight of each day, to midnight of the
  next day (e.g. 2021-01-01 00:00:00 to 2021-01-02 00:00:00).
* Each run would be created right after the data interval ends. The run covering
  Monday happens on midnight Tuesday and so on. The run covering Friday happens
  on midnight Saturday. No runs happen on midnights Sunday and Monday.

For simplicity, we will only deal with UTC datetimes in this example.


Timetable Registration
----------------------

A timetable must be a subclass of :class:`~airflow.timetables.base.Timetable`,
and be registered as a part of a :doc:`plugin </plugins>`. The following is a
skeleton for us to implement a new timetable:

.. code-block:: python

    from airflow.plugins_manager import AirflowPlugin
    from airflow.timetables.base import Timetable


    class AfterWorkdayTimetable(Timetable):
        pass


    class WorkdayTimetablePlugin(AirflowPlugin):
        name = "workday_timetable_plugin"
        timetables = [AfterWorkdayTimetable]

Next, we'll start putting code into ``AfterWorkdayTimetable``. After the
implementation is finished, we should be able to use the timetable in our DAG
file:

.. exampleinclude:: /../../airflow/example_dags/example_workday_timetable.py
    :language: python
    :dedent: 4
    :start-after: [START howto_timetable_example_dag]
    :end-before: [END howto_timetable_example_dag]


Define Scheduling Logic
-----------------------

When Airflow's scheduler encounters a DAG, it calls one of the two methods to
know when to schedule the DAG's next run.

* ``next_dagrun_info``: The scheduler uses this to learn the timetable's regular
  schedule, i.e. the "one for every workday, run at the end of it" part in our
  example.
* ``infer_data_interval``: When a DAG run is manually triggered (from the web
  UI, for example), the scheduler uses this method to learn about how to
  reverse-infer the out-of-schedule run's data interval.

We'll start with ``infer_data_interval`` since it's the easier of the two:

.. exampleinclude:: /../../airflow/example_dags/plugins/workday.py
    :language: python
    :dedent: 4
    :start-after: [START howto_timetable_infer_data_interval]
    :end-before: [END howto_timetable_infer_data_interval]

The method accepts one argument ``run_after``, a ``pendulum.DateTime`` object
that indicates when the DAG is externally triggered. Since our timetable creates
a data interval for each complete work day, the data interval inferred here
should usually start at the midnight one day prior to ``run_after``, but if
``run_after`` falls on a Sunday or Monday (i.e. the prior day is Saturday or
Sunday), it should be pushed further back to the previous Friday. Once we know
the start of the interval, the end is simply one full day after it. We then
create a :class:`~airflow.timetables.base.DataInterval` object to describe this
interval.

Next is the implementation of ``next_dagrun_info``:

.. exampleinclude:: /../../airflow/example_dags/plugins/workday.py
    :language: python
    :dedent: 4
    :start-after: [START howto_timetable_next_dagrun_info]
    :end-before: [END howto_timetable_next_dagrun_info]

This method accepts two arguments. ``last_automated_dagrun`` is a
``pendulum.DateTime`` object indicating the logical date of this DAG's previous
non-manually-triggered run, or ``None`` if this is the first time ever the DAG
is being scheduled. ``restriction`` encapsulates how the DAG and its tasks
specify the schedule, and contains three attributes:

* ``earliest``: The earliest time the DAG may be scheduled. This is a
  calculated ``pendulum.DateTime`` from all the ``start_date`` arguments from
  the DAG and its tasks, or ``None`` oi there are no ``start_date`` arguments
  found at all.
* ``latest``: Similar to ``earliest``, this is the latest time the DAG may be
  scheduled, calculated from ``end_date`` arguments.
* ``catchup``: A boolean reflecting the DAG's ``catchup`` argument.

.. note::

    Both ``earliest`` and ``latest`` apply to the DAG run's logical date
    (the *start* of the data interval), not when the run will be scheduled
    (usually after the end of the data interval).

If there was a run scheduled previously, we should now schedule for the next
weekday, i.e. plus one day if the previous run was on Monday through Thursday,
or three days if it was on Friday. If there was not a previous scheduled run,
however, we pick the next workday's midnight after ``restriction.earliest``
(unless it *is* a workday's midnight; in which case it's used directly).
``restriction.catchup`` also needs to be considered---if it's ``False``, we
can't schedule before the current time, even if ``start_date`` values are in the
past. Finally, if our calculated data interval is later than
``restriction.latest``, we must respect it and not schedule a run by returning
``None``.

If we decide to schedule a run, we need to describe it with a
:class:`~airflow.timetables.base.DagRunInfo`. This type has two arguments and
attributes:

* ``data_interval``: A :class:`~airflow.timetables.base.DataInterval` instance
  like ``infer_data_interval``'s return value. This describes the next run's
  data interval.
* ``run_after``: A ``pendulum.DateTime`` instance that tells the scheduler when
  the DAG run can be scheduled.

.. note::

    In case you're wondering---yes, the argument and return value of
    ``infer_data_interval`` are also internally combined into a ``DagRunInfo``.

A ``DagRunInfo`` can be created like this:

.. code-block:: python

    info = DagRunInfo(
        data_interval=DataInterval(start=start, end=end),
        run_after=run_after,
    )

But since we typically wan to schedule a run as soon as the data interval ends,
``end`` and ``run_after`` above are generally the same. ``DagRunInfo`` therefore
provides a shortcut for this:

.. code-block:: python

    info = DagRunInfo.interval(start=start, end=end)
    assert info.data_interval.end == info.run_after  # Always True.

For reference, here's our plugin and DAG files in their entirety:

.. exampleinclude:: /../../airflow/example_dags/plugins/workday.py
    :language: python
    :start-after: [START howto_timetable]
    :end-before: [END howto_timetable]

.. exampleinclude:: /../../airflow/example_dags/example_workday_timetable.py
    :language: python
    :start-after: [START howto_timetable]
    :end-before: [END howto_timetable]


Parameterized Timetables
------------------------

Sometimes we need to pass some run-time arguments to the timetable. Continuing
with our ``AfterWorkdayTimetable`` example, maybe we may have DAGs running on
different timezones, and we want to schedule some DAGs at 8am the next day,
instead of on midnight. Instead of creating a separate timetable for each
purpose, we'd want to do something like:

.. code-block:: python

    from datetime import timedelta

    from pendulum import DateTime, Time


    class SometimeAfterWorkdayTimetable(Timetable):
        def __init__(self, schedule_at: Time) -> None:
            self._schedule_at = schedule_at

        def next_dagrun_info(self, last_automated_dagrun, restriction):
            ...
            end = start + timedelta(days=1)
            return DagRunInfo(
                data_interval=DataInterval(start=start, end=end),
                run_after=DateTime.combine(end.date(), self._schedule_at).replace(
                    tzinfo=end.tzinfo
                ),
            )

However, since the timetable is a part of the DAG, we need to tell Airflow how
to serialize it with the context we provide in ``__init__``. This is done by
implementing two additional methods on our timetable class:

.. code-block:: python

    def serialize(self) -> Dict[str, Any]:
        return {"schedule_at": self._schedule_at.isoformat()}


    @classmethod
    def deserialize(cls, value: Dict[str, Any]) -> Timetable:
        return cls(Time.fromisoformat(value["schedule_at"]))

When the DAG is being serialized, ``serialize`` is called to obtain a
JSON-serializable value. That value is passed to ``deserialize`` when the
serialized DAG is accessed by the scheduler to reconstruct the timetable.


Timetable Display in UI
=======================

By default, a custom timetable is displayed by their class name in the UI (e.g.
the *Schedule* column in the "DAGs" table. It is possible to customize this
by overriding the ``summary`` property. This is especially useful for
parameterized timetables to include arguments provided in ``__init__``. For
our ``SometimeAfterWorkdayTimetable`` class, for example, we could have:

.. code-block:: python

    @property
    def summary(self) -> str:
        return f"after each workday, at {self._schedule_at}"

So for a DAG declared like this:

.. code-block:: python

    from pendulum import Time


    with DAG(
        timetable=SometimeAfterWorkdayTimetable(Time(8)),  # 8am.
        ...,
    ) as dag:
        ...

The *Schedule* column would say ``after each workday, at 08:00:00``.


.. seealso::

    Module :mod:`airflow.timetables.base`
        The public interface is heavily documented to explain what they should
        be implemented by subclasses.
