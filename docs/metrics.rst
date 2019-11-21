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



Metrics
=======

Airflow can be set up to send metrics to `StatsD <https://github.com/etsy/statsd>`__.

Setup
-----

First you must install statsd requirement:

.. code-block:: bash

   pip install 'apache-airflow[statsd]'

Add the following lines to your configuration file e.g. ``airflow.cfg``

.. code-block:: ini

    [scheduler]
    statsd_on = True
    statsd_host = localhost
    statsd_port = 8125
    statsd_prefix = airflow

If you want to avoid send all the available metrics to StatsD, you can configure an allow list of prefixes to send only
the metrics that start with the elements of the list:

.. code-block:: ini

    [scheduler]
    statsd_allow_list = scheduler,executor,dagrun

Counters
--------

======================================= ================================================================
Name                                    Description
======================================= ================================================================
``<job_name>_start``                    Number of started ``<job_name>`` job, ex. ``SchedulerJob``, ``LocalTaskJob``
``<job_name>_end``                      Number of ended ``<job_name>`` job, ex. ``SchedulerJob``, ``LocalTaskJob``
``operator_failures_<operator_name>``   Operator ``<operator_name>`` failures
``operator_successes_<operator_name>``  Operator ``<operator_name>`` successes
``ti_failures``                         Overall task instances failures
``ti_successes``                        Overall task instances successes
``zombies_killed``                      Zombie tasks killed
``scheduler_heartbeat``                 Scheduler heartbeats
``dag_processing.processes``            Number of currently running DAG parsing processes
``scheduler.tasks.killed_externally``   Number of tasks killed externally
======================================= ================================================================

Gauges
------

=================================================== ========================================================================
Name                                                Description
=================================================== ========================================================================
``dagbag_size``                                     DAG bag size
``dag_processing.import_errors``                    Number of errors from trying to parse DAG files
``dag_processing.total_parse_time``                 Seconds taken to scan and import all DAG files once
``dag_processing.last_runtime.<dag_file>``          Seconds spent processing ``<dag_file>`` (in most recent iteration)
``dag_processing.last_run.seconds_ago.<dag_file>``  Seconds since ``<dag_file>`` was last processed
``dag_processing.processor_timeouts``               Number of file processors that have been killed due to taking too long
``executor.open_slots``                             Number of open slots on executor
``executor.queued_tasks``                           Number of queued tasks on executor
``executor.running_tasks``                          Number of running tasks on executor
``pool.open_slots.<pool_name>``                     Number of open slots in the pool
``pool.used_slots.<pool_name>``                     Number of used slots in the pool
``pool.starving_tasks.<pool_name>``                 Number of starving tasks in the pool
=================================================== ========================================================================

Timers
------

=========================================== =================================================
Name                                        Description
=========================================== =================================================
``dagrun.dependency-check.<dag_id>``        Milliseconds taken to check DAG dependencies
``dag.<dag_id>.<task_id>.duration``         Milliseconds taken to finish a task
``dag_processing.last_duration.<dag_file>`` Milliseconds taken to load the given DAG file
``dagrun.duration.success.<dag_id>``        Milliseconds taken for a DagRun to reach success state
``dagrun.duration.failed.<dag_id>``         Milliseconds taken for a DagRun to reach failed state
``dagrun.schedule_delay.<dag_id>``          Milliseconds of delay between the scheduled DagRun
                                            start date and the actual DagRun start date
=========================================== =================================================
