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

.. _scheduler:

Scheduler
==========

The Airflow scheduler monitors all tasks and DAGs, then triggers the
task instances once their dependencies are complete. Behind the scenes,
the scheduler spins up a subprocess, which monitors and stays in sync with all
DAGs in the specified DAG directory. Once per minute, by default, the scheduler
collects DAG parsing results and checks whether any active tasks can be triggered.

The Airflow scheduler is designed to run as a persistent service in an
Airflow production environment. To kick it off, all you need to do is
execute the ``airflow scheduler`` command. It uses the configuration specified in
``airflow.cfg``.

The scheduler uses the configured :doc:`Executor <../core-concepts/executor/index>` to run tasks that are ready.

To start a scheduler, simply run the command:

.. code-block:: bash

    airflow scheduler

Your DAGs will start executing once the scheduler is running successfully.

.. note::

    The first DAG Run is created based on the minimum ``start_date`` for the tasks in your DAG.
    Subsequent DAG Runs are created according to your DAG's :doc:`timetable <../authoring-and-scheduling/timetable>`.


For dags with a cron or timedelta schedule, scheduler won't trigger your tasks until the period it covers has ended e.g., A job with ``schedule`` set as ``@daily`` runs after the day
has ended. This technique makes sure that whatever data is required for that period is fully available before the DAG is executed.
In the UI, it appears as if Airflow is running your tasks a day **late**

.. note::

    If you run a DAG on a ``schedule`` of one day, the run with data interval starting on ``2019-11-21`` triggers after ``2019-11-21T23:59``.

    **Let's Repeat That**, the scheduler runs your job one ``schedule`` AFTER the start date, at the END of the interval.

    You should refer to :doc:`../core-concepts/dag-run` for details on scheduling a DAG.

.. note::
    The scheduler is designed for high throughput. This is an informed design decision to achieve scheduling
    tasks as soon as possible. The scheduler checks how many free slots available in a pool and schedule at most that number of tasks instances in one iteration.
    This means that task priority will only come in to effect when there are more scheduled tasks
    waiting than the queue slots. Thus there can be cases where low priority tasks will be schedule before high priority tasks if they share the same batch.
    For more read about that you can reference `this GitHub discussion <https://github.com/apache/airflow/discussions/28809>`__.


DAG File Processing
-------------------

You can have the Airflow Scheduler be responsible for starting the process that turns the Python files contained in the DAGs folder into DAG objects
that contain tasks to be scheduled.

Refer to :doc:`../authoring-and-scheduling/dagfile-processing` for details on how this can be achieved


Triggering DAG with Future Date
-------------------------------

If you want to use 'external trigger' to run future-dated data intervals, set ``allow_trigger_in_future = True`` in ``scheduler`` section in ``airflow.cfg``.
This only has effect if your DAG is defined with ``schedule=None``.
When set to ``False`` (the default value), if you manually trigger a run with future-dated data intervals,
the scheduler will not execute it until its ``data_interval_start`` is in the past.

.. _scheduler:ha:

Running More Than One Scheduler
-------------------------------

.. versionadded: 2.0.0

Airflow supports running more than one scheduler concurrently -- both for performance reasons and for
resiliency.

Overview
""""""""

The :abbr:`HA (highly available)` scheduler is designed to take advantage of the existing metadata database.
This was primarily done for operational simplicity: every component already has to speak to this DB, and by
not using direct communication or consensus algorithm between schedulers (Raft, Paxos, etc.) nor another
consensus tool (Apache Zookeeper, or Consul for instance) we have kept the "operational surface area" to a
minimum.

The scheduler now uses the serialized DAG representation to make its scheduling decisions and the rough
outline of the scheduling loop is:

- Check for any DAGs needing a new DagRun, and create them
- Examine a batch of DagRuns for schedulable TaskInstances or complete DagRuns
- Select schedulable TaskInstances, and whilst respecting Pool limits and other concurrency limits, enqueue
  them for execution

This does however place some requirements on the Database.

.. _scheduler:ha:db_requirements:

Database Requirements
"""""""""""""""""""""

The short version is that users of PostgreSQL 10+ or MySQL 8+ are all ready to go -- you can start running as
many copies of the scheduler as you like -- there is no further set up or config options needed. If you are
using a different database please read on.

To maintain performance and throughput there is one part of the scheduling loop that does a number of
calculations in memory (because having to round-trip to the DB for each TaskInstance would be too slow) so we
need to ensure that only a single scheduler is in this critical section at once - otherwise limits would not
be correctly respected. To achieve this we use database row-level locks (using ``SELECT ... FOR UPDATE``).

This critical section is where TaskInstances go from scheduled state and are enqueued to the executor, whilst
ensuring the various concurrency and pool limits are respected. The critical section is obtained by asking for
a row-level write lock on every row of the Pool table (roughly equivalent to ``SELECT * FROM slot_pool FOR
UPDATE NOWAIT`` but the exact query is slightly different).

The following databases are fully supported and provide an "optimal" experience:

- PostgreSQL 10+
- MySQL 8+

.. warning::

  MariaDB did not implement the ``SKIP LOCKED`` or ``NOWAIT`` SQL clauses until version
  `10.6.0 <https://jira.mariadb.org/browse/MDEV-25433>`_.
  Without these features, running multiple schedulers is not supported and deadlock errors have been reported. MariaDB
  10.6.0 and following may work appropriately with multiple schedulers, but this has not been tested.

.. warning::

  MySQL 5.x does not support ``SKIP LOCKED`` or ``NOWAIT``, and additionally is more prone to deciding
  queries are deadlocked, so running with more than a single scheduler on MySQL 5.x is not supported or
  recommended.

.. note::

  Microsoft SQLServer has not been tested with HA.


Fine-tuning your Scheduler performance
--------------------------------------

What impacts scheduler's performance
""""""""""""""""""""""""""""""""""""

The Scheduler is responsible for two operations:

* continuously parsing DAG files and synchronizing with the DAG in the database
* continuously scheduling tasks for execution

Those two tasks are executed in parallel by the scheduler and run independently of each other in
different processes. In order to fine-tune your scheduler, you need to include a number of factors:

* The kind of deployment you have
    * what kind of filesystem you have to share the DAGs (impacts performance of continuously reading DAGs)
    * how fast the filesystem is (in many cases of distributed cloud filesystem you can pay extra to get
      more throughput/faster filesystem
    * how much memory you have for your processing
    * how much CPU you have available
    * how much networking throughput you have available

* The logic and definition of your DAG structure:
    * how many DAG files you have
    * how many DAGs you have in your files
    * how large the DAG files are (remember dag parser needs to read and parse the file every n seconds)
    * how complex they are (i.e. how fast they can be parsed, how many tasks and dependencies they have)
    * whether parsing your DAG file involves importing a lot of libraries or heavy processing at the top level
      (Hint! It should not. See :ref:`best_practices/top_level_code`)

* The scheduler configuration
   * How many schedulers you have
   * How many parsing processes you have in your scheduler
   * How much time scheduler waits between re-parsing of the same DAG (it happens continuously)
   * How many task instances scheduler processes in one loop
   * How many new DAG runs should be created/scheduled per loop
   * How often the scheduler should perform cleanup and check for orphaned tasks/adopting them

In order to perform fine-tuning, it's good to understand how Scheduler works under-the-hood.
You can take a look at the Airflow Summit 2021 talk
`Deep Dive into the Airflow Scheduler talk <https://youtu.be/DYC4-xElccE>`_ to perform the fine-tuning.

How to approach Scheduler's fine-tuning
"""""""""""""""""""""""""""""""""""""""

Airflow gives you a lot of "knobs" to turn to fine tune the performance but it's a separate task,
depending on your particular deployment, your DAG structure, hardware availability and expectations,
to decide which knobs to turn to get best effect for you. Part of the job when managing the
deployment is to decide what you are going to optimize for. Some users are ok with
30 seconds delays of new DAG parsing, at the expense of lower CPU usage, whereas some other users
expect the DAGs to be parsed almost instantly when they appear in the DAGs folder at the
expense of higher CPU usage for example.

Airflow gives you the flexibility to decide, but you should find out what aspect of performance is
most important for you and decide which knobs you want to turn in which direction.

Generally for fine-tuning, your approach should be the same as for any performance improvement and
optimizations (we will not recommend any specific tools - just use the tools that you usually use
to observe and monitor your systems):

* its extremely important to monitor your system with the right set of tools that you usually use to
  monitor your system. This document does not go into details of particular metrics and tools that you
  can use, it just describes what kind of resources you should monitor, but you should follow your best
  practices for monitoring to grab the right data.
* decide which aspect of performance is most important for you (what you want to improve)
* observe your system to see where your bottlenecks are: CPU, memory, I/O are the usual limiting factors
* based on your expectations and observations - decide what is your next improvement and go back to
  the observation of your performance, bottlenecks. Performance improvement is an iterative process.

What resources might limit Scheduler's performance
""""""""""""""""""""""""""""""""""""""""""""""""""

There are several areas of resource usage that you should pay attention to:

* FileSystem performance. Airflow Scheduler relies heavily on parsing (sometimes a lot) of Python
  files, which are often located on a shared filesystem. Airflow Scheduler continuously reads and
  re-parses those files. The same files have to be made available to workers, so often they are
  stored in a distributed filesystem. You can use various filesystems for that purpose (NFS, CIFS, EFS,
  GCS fuse, Azure File System are good examples). There are various parameters you can control for those
  filesystems and fine-tune their performance, but this is beyond the scope of this document. You should
  observe statistics and usage of your filesystem to determine if problems come from the filesystem
  performance. For example there are anecdotal evidences that increasing IOPS (and paying more) for the
  EFS performance, dramatically improves stability and speed of parsing Airflow DAGs when EFS is used.
* Another solution to FileSystem performance, if it becomes your bottleneck, is to turn to alternative
  mechanisms of distributing your DAGs. Embedding DAGs in your image and GitSync distribution have both
  the property that the files are available locally for Scheduler and it does not have to use a
  distributed filesystem to read the files, the files are available locally for the Scheduler and it is
  usually as fast as it can be, especially if your machines use fast SSD disks for local storage. Those
  distribution mechanisms have other characteristics that might make them not the best choice for you,
  but if your problems with performance come from distributed filesystem performance, they might be the
  best approach to follow.
* Database connections and Database usage might become a problem as you want to increase performance and
  process more things in parallel. Airflow is known for being "database-connection hungry" - the more DAGs
  you have and the more you want to process in parallel, the more database connections will be opened.
  This is generally not a problem for MySQL as its model of handling connections is thread-based, but this
  might be a problem for Postgres, where connection handling is process-based. It is a general consensus
  that if you have even medium size Postgres-based Airflow installation, the best solution is to use
  `PGBouncer <https://www.pgbouncer.org/>`_ as a proxy to your database. The :doc:`helm-chart:index`
  supports PGBouncer out-of-the-box. For MsSQL we have not yet worked out the best practices as support
  for MsSQL is still experimental.
* CPU usage is most important for FileProcessors - those are the processes that parse and execute
  Python DAG files. Since Schedulers triggers such parsing continuously, when you have a lot of DAGs,
  the processing might take a lot of CPU. You can mitigate it by increasing the
  :ref:`config:scheduler__min_file_process_interval`, but this is one of the mentioned trade-offs,
  result of this is that changes to such files will be picked up slower and you will see delays between
  submitting the files and getting them available in Airflow UI and executed by Scheduler. Optimizing
  the way how your DAGs are built, avoiding external data sources is your best approach to improve CPU
  usage. If you have more CPUs available, you can increase number of processing threads
  :ref:`config:scheduler__parsing_processes`, Also Airflow Scheduler scales almost linearly with
  several instances, so you can also add more Schedulers if your Scheduler's performance is CPU-bound.
* Airflow might use quite a significant amount of memory when you try to get more performance out of it.
  Often more performance is achieved in Airflow by increasing the number of processes handling the load,
  and each process requires whole interpreter of Python loaded, a lot of classes imported, temporary
  in-memory storage. A lot of it is optimized by Airflow by using forking and copy-on-write memory used
  but in case new classes are imported after forking this can lead to extra memory pressure.
  You need to observe if your system is using more memory than it has - which results with using swap disk,
  which dramatically decreases performance. Note that Airflow Scheduler in versions prior to ``2.1.4``
  generated a lot of ``Page Cache`` memory used by log files (when the log files were not removed).
  This was generally harmless, as the memory is just cache and could be reclaimed at any time by the system,
  however in version ``2.1.4`` and beyond, writing logs will not generate excessive ``Page Cache`` memory.
  Regardless - make sure when you look at memory usage, pay attention to the kind of memory you are observing.
  Usually you should look at ``working memory``(names might vary depending on your deployment) rather
  than ``total memory used``.

What can you do, to improve Scheduler's performance
"""""""""""""""""""""""""""""""""""""""""""""""""""

When you know what your resource usage is, the improvements that you can consider might be:

* improve the logic, efficiency of parsing and reduce complexity of your top-level DAG Python code. It is
  parsed continuously so optimizing that code might bring tremendous improvements, especially if you try
  to reach out to some external databases etc. while parsing DAGs (this should be avoided at all cost).
  The :ref:`best_practices/top_level_code` explains what are the best practices for writing your top-level
  Python code. The :ref:`best_practices/reducing_dag_complexity` document provides some areas that you might
  look at when you want to reduce complexity of your code.
* improve utilization of your resources. This is when you have a free capacity in your system that
  seems underutilized (again CPU, memory I/O, networking are the prime candidates) - you can take
  actions like increasing number of schedulers, parsing processes or decreasing intervals for more
  frequent actions might bring improvements in performance at the expense of higher utilization of those.
* increase hardware capacity (for example if you see that CPU is limiting you or that I/O you use for
  DAG filesystem is at its limits). Often the problem with scheduler performance is
  simply because your system is not "capable" enough and this might be the only way. For example if
  you see that you are using all CPU you have on machine, you might want to add another scheduler on
  a new machine - in most cases, when you add 2nd or 3rd scheduler, the capacity of scheduling grows
  linearly (unless the shared database or filesystem is a bottleneck).
* experiment with different values for the "scheduler tunables". Often you might get better effects by
  simply exchanging one performance aspect for another. For example if you want to decrease the
  CPU usage, you might increase file processing interval (but the result will be that new DAGs will
  appear with bigger delay). Usually performance tuning is the art of balancing different aspects.
* sometimes you change scheduler behaviour slightly (for example change parsing sort order)
  in order to get better fine-tuned results for your particular deployment.


.. _scheduler:ha:tunables:

Scheduler Configuration options
"""""""""""""""""""""""""""""""

The following config settings can be used to control aspects of the Scheduler.
However you can also look at other non-performance-related scheduler configuration parameters available at
:doc:`../configurations-ref` in ``[scheduler]`` section.

- :ref:`config:scheduler__max_dagruns_to_create_per_loop`

  This changes the number of DAGs that are locked by each scheduler when
  creating DAG runs. One possible reason for setting this lower is if you
  have huge DAGs (in the order of 10k+ tasks per DAG) and are running multiple schedulers, you won't want one
  scheduler to do all the work.

- :ref:`config:scheduler__max_dagruns_per_loop_to_schedule`

  How many DagRuns should a scheduler examine (and lock) when scheduling
  and queuing tasks. Increasing this limit will allow more throughput for
  smaller DAGs but will likely slow down throughput for larger (>500
  tasks for example) DAGs. Setting this too high when using multiple
  schedulers could also lead to one scheduler taking all the DAG runs
  leaving no work for the others.

- :ref:`config:scheduler__use_row_level_locking`

  Should the scheduler issue ``SELECT ... FOR UPDATE`` in relevant queries.
  If this is set to False then you should not run more than a single
  scheduler at once.

- :ref:`config:scheduler__pool_metrics_interval`

  How often (in seconds) should pool usage stats be sent to StatsD (if
  statsd_on is enabled). This is a *relatively* expensive query to compute
  this, so this should be set to match the same period as your StatsD roll-up
  period.

- :ref:`config:scheduler__orphaned_tasks_check_interval`

  How often (in seconds) should the scheduler check for orphaned tasks or dead
  SchedulerJobs.

  This setting controls how a dead scheduler will be noticed and the tasks it
  was "supervising" get picked up by another scheduler. The tasks will stay
  running, so there is no harm in not detecting this for a while.

  When a SchedulerJob is detected as "dead" (as determined by
  :ref:`config:scheduler__scheduler_health_check_threshold`) any running or
  queued tasks that were launched by the dead process will be "adopted" and
  monitored by this scheduler instead.

- :ref:`config:scheduler__dag_dir_list_interval`
  How often (in seconds) to scan the DAGs directory for new files.

- :ref:`config:scheduler__file_parsing_sort_mode`
  The scheduler will list and sort the DAG files to decide the parsing order.

- :ref:`config:scheduler__max_tis_per_query`
  The batch size of queries in the scheduling main loop. If this is too high, SQL query
  performance may be impacted by complexity of query predicate, and/or excessive locking.

  Additionally, you may hit the maximum allowable query length for your db.
  Set this to 0 for no limit (not advised).

- :ref:`config:scheduler__min_file_process_interval`
  Number of seconds after which a DAG file is re-parsed. The DAG file is parsed every
  min_file_process_interval number of seconds. Updates to DAGs are reflected after
  this interval. Keeping this number low will increase CPU usage.

- :ref:`config:scheduler__parsing_processes`
  The scheduler can run multiple processes in parallel to parse DAG files. This defines
  how many processes will run.

- :ref:`config:scheduler__scheduler_idle_sleep_time`
  Controls how long the scheduler will sleep between loops, but if there was nothing to do
  in the loop. i.e. if it scheduled something then it will start the next loop
  iteration straight away. This parameter is badly named (historical reasons) and it will be
  renamed in the future with deprecation of the current name.

- :ref:`config:scheduler__schedule_after_task_execution`
  Should the Task supervisor process perform a "mini scheduler" to attempt to schedule more tasks of
  the same DAG. Leaving this on will mean tasks in the same DAG execute quicker,
  but might starve out other DAGs in some circumstances.
