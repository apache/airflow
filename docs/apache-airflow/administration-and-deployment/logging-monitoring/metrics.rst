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

First you must install StatsD requirement:

.. code-block:: bash

   pip install 'apache-airflow[statsd]'

Add the following lines to your configuration file e.g. ``airflow.cfg``

.. code-block:: ini

    [metrics]
    statsd_on = True
    statsd_host = localhost
    statsd_port = 8125
    statsd_prefix = airflow

If you want to avoid sending all the available metrics to StatsD, you can configure an allow list of prefixes to send only
the metrics that start with the elements of the list:

.. code-block:: ini

    [metrics]
    statsd_allow_list = scheduler,executor,dagrun

If you want to redirect metrics to different name, you can configure ``stat_name_handler`` option
in ``[metrics]`` section.  It should point to a function that validates the StatsD stat name, applies changes
to the stat name if necessary, and returns the transformed stat name. The function may looks as follow:

.. code-block:: python

    def my_custom_stat_name_handler(stat_name: str) -> str:
        return stat_name.lower()[:32]

If you want to use a custom StatsD client instead of the default one provided by Airflow, the following key must be added
to the configuration file alongside the module path of your custom StatsD client. This module must be available on
your :envvar:`PYTHONPATH`.

.. code-block:: ini

    [metrics]
    statsd_custom_client_path = x.y.customclient

See :doc:`../modules_management` for details on how Python and Airflow manage modules.

.. note::

    For a detailed listing of configuration options regarding metrics,
    see the configuration reference documentation - :ref:`config:metrics`.

Counters
--------

====================================================================== ================================================================
Name                                                                   Description
====================================================================== ================================================================
``<job_name>_start``                                                   Number of started ``<job_name>`` job, ex. ``SchedulerJob``, ``LocalTaskJob``
``<job_name>_end``                                                     Number of ended ``<job_name>`` job, ex. ``SchedulerJob``, ``LocalTaskJob``
``<job_name>_heartbeat_failure``                                       Number of failed Heartbeats for a ``<job_name>`` job, ex. ``SchedulerJob``,
                                                                       ``LocalTaskJob``
``local_task_job.task_exit.<job_id>.<dag_id>.<task_id>.<return_code>`` Number of ``LocalTaskJob`` terminations with a ``<return_code>``
                                                                       while running a task ``<task_id>`` of a DAG  ``<dag_id>``.
``operator_failures_<operator_name>``                                  Operator ``<operator_name>`` failures
``operator_successes_<operator_name>``                                 Operator ``<operator_name>`` successes
``ti_failures``                                                        Overall task instances failures
``ti_successes``                                                       Overall task instances successes
``previously_succeeded``                                               Number of previously succeeded task instances
``zombies_killed``                                                     Zombie tasks killed
``scheduler_heartbeat``                                                Scheduler heartbeats
``dag_processing.processes``                                           Number of currently running DAG parsing processes
``dag_processing.processor_timeouts``                                  Number of file processors that have been killed due to taking too long
``dag_file_processor_timeouts``                                        (DEPRECATED) same behavior as ``dag_processing.processor_timeouts``
``dag_processing.manager_stalls``                                      Number of stalled ``DagFileProcessorManager``
``dag_file_refresh_error``                                             Number of failures loading any DAG files
``scheduler.tasks.killed_externally``                                  Number of tasks killed externally
``scheduler.orphaned_tasks.cleared``                                   Number of Orphaned tasks cleared by the Scheduler
``scheduler.orphaned_tasks.adopted``                                   Number of Orphaned tasks adopted by the Scheduler
``scheduler.critical_section_busy``                                    Count of times a scheduler process tried to get a lock on the critical
                                                                       section (needed to send tasks to the executor) and found it locked by
                                                                       another process.
``sla_missed``                                                         Number of SLA misses
``sla_callback_notification_failure``                                  Number of failed SLA miss callback notification attempts
``sla_email_notification_failure``                                     Number of failed SLA miss email notification attempts
``ti.start.<dag_id>.<task_id>``                                        Number of started task in a given dag. Similar to <job_name>_start but for task
``ti.finish.<dag_id>.<task_id>.<state>``                               Number of completed task in a given dag. Similar to <job_name>_end but for task
``dag.callback_exceptions``                                            Number of exceptions raised from DAG callbacks. When this happens, it
                                                                       means DAG callback is not working.
``celery.task_timeout_error``                                          Number of ``AirflowTaskTimeout`` errors raised when publishing Task to Celery Broker.
``celery.execute_command.failure``                                     Number of non-zero exit code from Celery task.
``task_removed_from_dag.<dag_id>``                                     Number of tasks removed for a given dag (i.e. task no longer exists in DAG)
``task_restored_to_dag.<dag_id>``                                      Number of tasks restored for a given dag (i.e. task instance which was
                                                                       previously in REMOVED state in the DB is added to DAG file)
``task_instance_created-<operator_name>``                              Number of tasks instances created for a given Operator
``triggers.blocked_main_thread``                                       Number of triggers that blocked the main thread (likely due to not being
                                                                       fully asynchronous)
``triggers.failed``                                                    Number of triggers that errored before they could fire an event
``triggers.succeeded``                                                 Number of triggers that have fired at least one event
``dataset.updates``                                                    Number of updated datasets
``dataset.orphaned``                                                   Number of datasets marked as orphans because they are no longer referenced in DAG
                                                                       schedule parameters or task outlets
``dataset.triggered_dagruns``                                          Number of DAG runs triggered by a dataset update
====================================================================== ================================================================

Gauges
------

=================================================== ========================================================================
Name                                                Description
=================================================== ========================================================================
``dagbag_size``                                     Number of DAGs found when the scheduler ran a scan based on it's
                                                    configuration
``dag_processing.import_errors``                    Number of errors from trying to parse DAG files
``dag_processing.total_parse_time``                 Seconds taken to scan and import all DAG files once
``dag_processing.last_run.seconds_ago.<dag_file>``  Seconds since ``<dag_file>`` was last processed
``scheduler.tasks.running``                         Number of tasks running in executor
``scheduler.tasks.starving``                        Number of tasks that cannot be scheduled because of no open slot in pool
``scheduler.tasks.executable``                      Number of tasks that are ready for execution (set to queued)
                                                    with respect to pool limits, DAG concurrency, executor state,
                                                    and priority.
``executor.open_slots``                             Number of open slots on executor
``executor.queued_tasks``                           Number of queued tasks on executor
``executor.running_tasks``                          Number of running tasks on executor
``pool.open_slots.<pool_name>``                     Number of open slots in the pool
``pool.queued_slots.<pool_name>``                   Number of queued slots in the pool
``pool.running_slots.<pool_name>``                  Number of running slots in the pool
``pool.starving_tasks.<pool_name>``                 Number of starving tasks in the pool
``triggers.running``                                Number of triggers currently running (per triggerer)
=================================================== ========================================================================

Timers
------

=================================================== ========================================================================
Name                                                Description
=================================================== ========================================================================
``dagrun.dependency-check.<dag_id>``                Milliseconds taken to check DAG dependencies
``dag.<dag_id>.<task_id>.duration``                 Milliseconds taken to finish a task
``dag_processing.last_duration.<dag_file>``         Milliseconds taken to load the given DAG file
``dagrun.duration.success.<dag_id>``                Seconds taken for a DagRun to reach success state
``dagrun.duration.failed.<dag_id>``                 Milliseconds taken for a DagRun to reach failed state
``dagrun.schedule_delay.<dag_id>``                  Seconds of delay between the scheduled DagRun
                                                    start date and the actual DagRun start date
``scheduler.critical_section_duration``             Milliseconds spent in the critical section of scheduler loop --
                                                    only a single scheduler can enter this loop at a time
``scheduler.critical_section_query_duration``       Milliseconds spent running the critical section task instance query
``scheduler.scheduler_loop_duration``               Milliseconds spent running one scheduler loop
``dagrun.<dag_id>.first_task_scheduling_delay``     Seconds elapsed between first task start_date and dagrun expected start
``collect_db_dags``                                 Milliseconds taken for fetching all Serialized Dags from DB
=================================================== ========================================================================
