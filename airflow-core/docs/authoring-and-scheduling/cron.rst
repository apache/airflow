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


Cron & Time Intervals
======================
You may set your Dag to run on a simple schedule by setting its ``schedule`` argument to either a
`cron expression <https://en.wikipedia.org/wiki/Cron#CRON_expression>`_, a ``datetime.timedelta`` object,
or one of the :ref:`cron-presets`.

.. code-block:: python

    from airflow.sdk import DAG

    import datetime

    dag = DAG("regular_interval_cron_example", schedule="0 0 * * *", ...)

    dag = DAG("regular_interval_cron_preset_example", schedule="@daily", ...)

    dag = DAG("regular_interval_timedelta_example", schedule=datetime.timedelta(days=1), ...)
.. _cron-presets:

Cron Presets
''''''''''''
For more elaborate scheduling requirements, you can implement a :doc:`custom timetable <../authoring-and-scheduling/timetable>`.
Note that Airflow parses cron expressions with the croniter library which supports an extended syntax for cron strings. See their documentation `in github <https://github.com/kiorky/croniter>`_.
For example, you can create a Dag schedule to run at 12AM on the first Monday of the month with their extended cron syntax: ``0 0 * * MON#1``.

.. tip::
    You can use an online editor for CRON expressions such as `Crontab guru <https://crontab.guru/>`_

+----------------+--------------------------------------------------------------------+-----------------+
| preset         | meaning                                                            | cron            |
+================+====================================================================+=================+
| ``None``       | Don't schedule, use for exclusively "externally triggered" Dags    |                 |
+----------------+--------------------------------------------------------------------+-----------------+
| ``@once``      | Schedule once and only once                                        |                 |
+----------------+--------------------------------------------------------------------+-----------------+
| ``@continuous``| Run as soon as the previous run finishes                           |                 |
+----------------+--------------------------------------------------------------------+-----------------+
| ``@hourly``    | Run once an hour at the beginning of the hour                      | ``0 * * * *``   |
+----------------+--------------------------------------------------------------------+-----------------+
| ``@daily``     | Run once a day at midnight (24:00)                                 | ``0 0 * * *``   |
+----------------+--------------------------------------------------------------------+-----------------+
| ``@weekly``    | Run once a week at midnight (24:00) on Sunday                      | ``0 0 * * 0``   |
+----------------+--------------------------------------------------------------------+-----------------+
| ``@monthly``   | Run once a month at midnight (24:00) of the first day of the month | ``0 0 1 * *``   |
+----------------+--------------------------------------------------------------------+-----------------+
| ``@quarterly`` | Run once a quarter at midnight (24:00) on the first day            | ``0 0 1 */3 *`` |
+----------------+--------------------------------------------------------------------+-----------------+
| ``@yearly``    | Run once a year at midnight (24:00) of January 1                   | ``0 0 1 1 *``   |
+----------------+--------------------------------------------------------------------+-----------------+

Your Dag will be instantiated for each schedule along with a corresponding
Dag Run entry in the database backend.

.. _cron-when-runs-fire:

When does a cron schedule fire?
'''''''''''''''''''''''''''''''
A cron expression only says *what times a run is scheduled for*. The timetable that
interprets the expression decides *when the run is actually created* and which
``logical_date`` it gets. This differs between Airflow 2 and Airflow 3, so a bare cron
string alone does not tell you when your Dag runs.

.. list-table::
   :header-rows: 1

   * - ``[scheduler] create_cron_data_intervals``
     - Timetable
     - ``logical_date``
     - Run is created
   * - ``False`` (Airflow 3 default)
     - :ref:`CronTriggerTimetable`
     - the cron time itself
     - at the cron time
   * - ``True`` (Airflow 2.x default)
     - :ref:`CronDataIntervalTimetable`
     - the start of the data interval
     - at the end of the data interval, one cron period later

For example, with ``schedule="0 0 * * *"``, ``CronTriggerTimetable`` creates a run at
midnight on February 1st whose ``logical_date`` is February 1st. At that same moment
``CronDataIntervalTimetable`` instead closes the data interval that began at midnight on
January 31st, so the run it creates has a ``logical_date`` of January 31st.

To pin the behaviour for one Dag regardless of the configuration value, pass a timetable
instance instead of a bare cron string:

.. code-block:: python

    from airflow.timetables.interval import CronDataIntervalTimetable

    dag = DAG(
        "pinned_data_interval_example",
        schedule=CronDataIntervalTimetable("0 0 * * *", timezone="UTC"),
    )

.. seealso::

    - :ref:`config:scheduler__create_cron_data_intervals` for the configuration option
    - :ref:`timetables_run_id_logical_date` for a worked comparison of the two timetables
    - :doc:`../core-concepts/dag-run` for the definition of ``logical_date``
