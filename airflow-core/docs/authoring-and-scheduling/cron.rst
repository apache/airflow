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
| ``@hourly``    | Run once an hour at the end of the hour                            | ``0 * * * *``   |
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
