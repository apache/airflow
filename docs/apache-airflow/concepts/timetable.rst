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

A DAG's scheduling strategy is determined by its internal "timetable". This
timetable can be created by specifying the DAG's ``schedule_interval`` argument,
as described in :doc:`DAG Run </dag-run>`, or by passing a ``timetable`` argument
directly. The timetable also dictates the data interval and the logical time of each
run created for the DAG.

Cron expressions and timedeltas are still supported (using
``CronDataIntervalTimetable`` and ``DeltaDataIntervalTimetable`` under the hood
respectively), however, there are situations where they cannot properly express
the schedule. Some examples are:

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
