..  Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

..    http://www.apache.org/licenses/LICENSE-2.0

..  Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

Metrics
=======

Configuration
-------------
Airflow can be set up to send metrics to `StatsD <https://github.com/etsy/statsd>`__:

.. code-block:: bash

    [scheduler]
    statsd_on = True
    statsd_host = localhost
    statsd_port = 8125
    statsd_prefix = airflow

Counters
--------

=================================== ================================================================
Name                                Description
=================================== ================================================================
<job_name>_start                    Number of started <job_name> job, ex. SchedulerJob, LocalTaskJob
<job_name>_end                      Number of ended <job_name> job, ex. SchedulerJob, LocalTaskJob
operator_failures_<operator_name>   Operator <operator_name> failures
operator_successes_<operator_name>  Operator <operator_name> successes
ti_failures                         Overall task instances failures
ti_successes                        Overall task instances successes
zombies_killed                      Zombie tasks killed
scheduler_heartbeat                 Scheduler heartbeats
=================================== ================================================================

Gauges
------

===================== =====================================
Name                  Description
===================== =====================================
collect_dags          Seconds taken to scan and import DAGs
dagbag_import_errors  DAG import errors
dagbag_size           DAG bag size
===================== =====================================

Timers
------

================================= =======================================
Name                              Description
================================= =======================================
dagrun.dependency-check.<dag_id>  Seconds taken to check DAG dependencies
================================= =======================================
