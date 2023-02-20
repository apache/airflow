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

For our example, let's say a company wants to run a job after each weekday to
process data collected during the work day. The first intuitive answer to this
would be ``schedule="0 0 * * 1-5"`` (midnight on Monday to Friday), but
this means data collected on Friday will *not* be processed right after Friday
ends, but on the next Monday, and that run's interval would be from midnight
Friday to midnight *Monday*. What we want is:

* Schedule a run for each Monday, Tuesday, Wednesday, Thursday, and Friday. The
  run's data interval would cover from midnight of each day, to midnight of the
  next day (e.g. 2021-01-01 00:00:00 to 2021-01-02 00:00:00).
* Each run would be created right after the data interval ends. The run covering
  Monday happens on midnight Tuesday and so on. The run covering Friday happens
  on midnight Saturday. No runs happen on midnights Sunday and Monday.

For simplicity, we will only deal with UTC datetimes in this example.

.. note::

    All datetime values returned by a custom timetable **MUST** be "aware", i.e.
    contains timezone information. Furthermore, they must use ``pendulum``'s
    datetime and timezone types.


Timetable Registration
----------------------

A timetable must be a subclass of :class:`~airflow.timetables.base.Timetable`,
and be registered as a part of a :doc:`plugin </authoring-and-scheduling/plugins>`. The following is a
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

.. code-block:: python

    import pendulum

    from airflow import DAG
    from airflow.example_dags.plugins.workday import AfterWorkdayTimetable


    with DAG(
        dag_id="example_after_workday_timetable_dag",
        start_date=pendulum.datetime(2021, 3, 10, tz="UTC"),
        schedule=AfterWorkdayTimetable(),
        tags=["example", "timetable"],
    ):
        ...


Define Scheduling Logic
-----------------------

When Airflow's scheduler encounters a DAG, it calls one of the two methods to
know when to schedule the DAG's next run.

* ``next_dagrun_info``: The scheduler uses this to learn the timetable's regular
  schedule, i.e. the "one for every workday, run at the end of it" part in our
  example.
* ``infer_manual_data_interval``: When a DAG run is manually triggered (from the web
  UI, for example), the scheduler uses this method to learn about how to
  reverse-infer the out-of-schedule run's data interval.

We'll start with ``infer_manual_data_interval`` since it's the easier of the two:

.. exampleinclude:: /../../airflow/example_dags/plugins/workday.py
    :language: python
    :dedent: 4
    :start-after: [START howto_timetable_infer_manual_data_interval]
    :end-before: [END howto_timetable_infer_manual_data_interval]

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
:class:`~airflow.timetables.base.DataInterval` instance indicating the data
interval of this DAG's previous non-manually-triggered run, or ``None`` if this
is the first time ever the DAG is being scheduled. ``restriction`` encapsulates
how the DAG and its tasks specify the schedule, and contains three attributes:

* ``earliest``: The earliest time the DAG may be scheduled. This is a
  ``pendulum.DateTime`` calculated from all the ``start_date`` arguments from
  the DAG and its tasks, or ``None`` if there are no ``start_date`` arguments
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
  describing the next run's data interval.
* ``run_after``: A ``pendulum.DateTime`` instance that tells the scheduler when
  the DAG run can be scheduled.

A ``DagRunInfo`` can be created like this:

.. code-block:: python

    info = DagRunInfo(
        data_interval=DataInterval(start=start, end=end),
        run_after=run_after,
    )

Since we typically want to schedule a run as soon as the data interval ends,
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

.. code-block:: python

    import pendulum

    from airflow import DAG
    from airflow.example_dags.plugins.workday import AfterWorkdayTimetable
    from airflow.operators.empty import EmptyOperator


    with DAG(
        dag_id="example_workday_timetable",
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        schedule=AfterWorkdayTimetable(),
        tags=["example", "timetable"],
    ):
        EmptyOperator(task_id="run_this")


Parameterized Timetables
------------------------

Sometimes we need to pass some run-time arguments to the timetable. Continuing
with our ``AfterWorkdayTimetable`` example, maybe we have DAGs running on
different timezones, and we want to schedule some DAGs at 8am the next day,
instead of on midnight. Instead of creating a separate timetable for each
purpose, we'd want to do something like:

.. code-block:: python

    class SometimeAfterWorkdayTimetable(Timetable):
        def __init__(self, schedule_at: Time) -> None:
            self._schedule_at = schedule_at

        def next_dagrun_info(self, last_automated_dagrun, restriction):
            ...
            end = start + timedelta(days=1)
            return DagRunInfo(
                data_interval=DataInterval(start=start, end=end),
                run_after=DateTime.combine(end.date(), self._schedule_at).replace(tzinfo=UTC),
            )

However, since the timetable is a part of the DAG, we need to tell Airflow how
to serialize it with the context we provide in ``__init__``. This is done by
implementing two additional methods on our timetable class:

.. code-block:: python

    class SometimeAfterWorkdayTimetable(Timetable):
        ...

        def serialize(self) -> dict[str, Any]:
            return {"schedule_at": self._schedule_at.isoformat()}

        @classmethod
        def deserialize(cls, value: dict[str, Any]) -> Timetable:
            return cls(Time.fromisoformat(value["schedule_at"]))

When the DAG is being serialized, ``serialize`` is called to obtain a
JSON-serializable value. That value is passed to ``deserialize`` when the
serialized DAG is accessed by the scheduler to reconstruct the timetable.


Timetable Display in UI
-----------------------

By default, a custom timetable is displayed by their class name in the UI (e.g.
the *Schedule* column in the "DAGs" table). It is possible to customize this
by overriding the ``summary`` property. This is especially useful for
parameterized timetables to include arguments provided in ``__init__``. For
our ``SometimeAfterWorkdayTimetable`` class, for example, we could have:

.. code-block:: python

    @property
    def summary(self) -> str:
        return f"after each workday, at {self._schedule_at}"

So for a DAG declared like this:

.. code-block:: python

    with DAG(
        schedule=SometimeAfterWorkdayTimetable(Time(8)),  # 8am.
        ...,
    ):
        ...

The *Schedule* column would say ``after each workday, at 08:00:00``.


.. seealso::

    Module :mod:`airflow.timetables.base`
        The public interface is heavily documented to explain what should be
        implemented by subclasses.


Timetable Description Display in UI
-----------------------------------

You can also provide a description for your Timetable Implementation
by overriding the ``description`` property.
This is especially useful for providing comprehensive description for your implementation in UI.
For our ``SometimeAfterWorkdayTimetable`` class, for example, we could have:

.. code-block:: python

    description = "Schedule: after each workday"

You can also wrap this inside ``__init__``, if you want to derive description.

.. code-block:: python

    def __init__(self) -> None:
        self.description = "Schedule: after each workday, at f{self._schedule_at}"


This is specially useful when you want to provide comprehensive description which is different from ``summary`` property.

So for a DAG declared like this:

.. code-block:: python

    with DAG(
        schedule=SometimeAfterWorkdayTimetable(Time(8)),  # 8am.
        ...,
    ):
        ...

The *i* icon  would show,  ``Schedule: after each workday, at 08:00:00``.


.. seealso::
    Module :mod:`airflow.timetables.interval`
        check ``CronDataIntervalTimetable`` description implementation which provides comprehensive cron description in UI.

Changing generated ``run_id``
-----------------------------

.. versionadded:: 2.4

Since Airflow 2.4, Timetables are also responsible for generating the ``run_id`` for DagRuns.

For example to have the Run ID show a "human friendly" date of when the run started (that is, the end of the data interval, rather then the start which is the date currently used) you could add a method like this to a custom timetable:

.. code-block:: python

    def generate_run_id(
        self,
        *,
        run_type: DagRunType,
        logical_date: DateTime,
        data_interval: DataInterval | None,
        **extra,
    ) -> str:
        if run_type == DagRunType.SCHEDULED and data_interval:
            return data_interval.end.format("YYYY-MM-DD dddd")
        return super().generate_run_id(
            run_type=run_type, logical_date=logical_date, data_interval=data_interval, **extra
        )


Remember that the RunID is limited to 250 characters, and must be unique within a DAG.
