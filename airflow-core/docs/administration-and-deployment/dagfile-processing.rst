
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

DAG File Processing
-------------------

DAG File Processing refers to the process of reading the python files that define your dags and storing them such that the scheduler can schedule them.

There are two primary components involved in DAG file processing.  The ``DagFileProcessorManager`` is a process executing an infinite loop that determines which files need
to be processed, and the ``DagFileProcessorProcess`` is a separate process that is started to convert an individual file into one or more DAG objects.

The ``DagFileProcessorManager`` runs user codes. As a result, it runs as a standalone process by running the ``airflow dag-processor`` CLI command.

.. image:: /img/dag_file_processing_diagram.png

``DagFileProcessorManager`` has the following steps:

1. Check for new files:  If the elapsed time since the DAG was last refreshed is > :ref:`config:scheduler__dag_dir_list_interval` then update the file paths list
2. Exclude recently processed files:  Exclude files that have been processed more recently than :ref:`min_file_process_interval<config:dag_processor__min_file_process_interval>` and have not been modified
3. Queue file paths: Add files discovered to the file path queue
4. Process files:  Start a new ``DagFileProcessorProcess`` for each file, up to a maximum of :ref:`config:dag_processor__parsing_processes`
5. Collect results: Collect the result from any finished DAG processors
6. Log statistics:  Print statistics and emit ``dag_processing.total_parse_time``

``DagFileProcessorProcess`` has the following steps:

1. Process file: The entire process must complete within :ref:`dag_file_processor_timeout<config:dag_processor__dag_file_processor_timeout>`
2. The DAG files are loaded as Python module: Must complete within :ref:`dagbag_import_timeout<config:core__dagbag_import_timeout>`
3. Process modules:  Find DAG objects within Python module
4. Return DagBag:  Provide the ``DagFileProcessorManager`` a list of the discovered DAG objects


Fine-tuning your DAG processor performance
------------------------------------------

What impacts DAG processor's performance
""""""""""""""""""""""""""""""""""""""""

The DAG processor is responsible for continuously parsing DAG files and synchronizing with the DAG in the database
In order to fine-tune your DAG processor, you need to include a number of factors:

* The kind of deployment you have
    * what kind of filesystem you have to share the dags (impacts performance of continuously reading dags)
    * how fast the filesystem is (in many cases of distributed cloud filesystem you can pay extra to get
      more throughput/faster filesystem)
    * how much memory you have for your processing
    * how much CPU you have available
    * how much networking throughput you have available

* The logic and definition of your DAG structure:
    * how many DAG files you have
    * how many dags you have in your files
    * how large the DAG files are (remember DAG parser needs to read and parse the file every n seconds)
    * how complex they are (i.e. how fast they can be parsed, how many tasks and dependencies they have)
    * whether parsing your DAG file involves importing a lot of libraries or heavy processing at the top level
      (Hint! It should not. See :ref:`best_practices/top_level_code`)

* The DAG processor configuration
   * How many DAG processors you have
   * How many parsing processes you have in your DAG processor
   * How much time DAG processor waits between re-parsing of the same DAG (it happens continuously)
   * How many callbacks you run per DAG processor loop

How to approach DAG processor's fine-tuning
"""""""""""""""""""""""""""""""""""""""""""

Airflow gives you a lot of "knobs" to turn to fine tune the performance but it's a separate task,
depending on your particular deployment, your DAG structure, hardware availability and expectations,
to decide which knobs to turn to get best effect for you. Part of the job when managing the
deployment is to decide what you are going to optimize for. Some users are ok with
30 seconds delays of new DAG parsing, at the expense of lower CPU usage, whereas some other users
expect the dags to be parsed almost instantly when they appear in the dags folder at the
expense of higher CPU usage for example.

Airflow gives you the flexibility to decide, but you should find out what aspect of performance is
most important for you and decide which knobs you want to turn in which direction.

Generally for fine-tuning, your approach should be the same as for any performance improvement and
optimizations (we will not recommend any specific tools - just use the tools that you usually use
to observe and monitor your systems):

* it's extremely important to monitor your system with the right set of tools that you usually use to
  monitor your system. This document does not go into details of particular metrics and tools that you
  can use, it just describes what kind of resources you should monitor, but you should follow your best
  practices for monitoring to grab the right data.
* decide which aspect of performance is most important for you (what you want to improve)
* observe your system to see where your bottlenecks are: CPU, memory, I/O are the usual limiting factors
* based on your expectations and observations - decide what is your next improvement and go back to
  the observation of your performance, bottlenecks. Performance improvement is an iterative process.

What resources might limit DAG processors's performance
"""""""""""""""""""""""""""""""""""""""""""""""""""""""

There are several areas of resource usage that you should pay attention to:

* FileSystem performance. The Airflow DAG processor relies heavily on parsing (sometimes a lot) of Python
  files, which are often located on a shared filesystem. The DAG processor continuously reads and
  re-parses those files. The same files have to be made available to workers, so often they are
  stored in a distributed filesystem. You can use various filesystems for that purpose (NFS, CIFS, EFS,
  GCS fuse, Azure File System are good examples). There are various parameters you can control for those
  filesystems and fine-tune their performance, but this is beyond the scope of this document. You should
  observe statistics and usage of your filesystem to determine if problems come from the filesystem
  performance. For example there are anecdotal evidences that increasing IOPS (and paying more) for the
  EFS performance, dramatically improves stability and speed of parsing Airflow dags when EFS is used.
* Another solution to FileSystem performance, if it becomes your bottleneck, is to turn to alternative
  mechanisms of distributing your dags. Embedding dags in your image and GitSync distribution have both
  the property that the files are available locally for the DAG processor and it does not have to use a
  distributed filesystem to read the files, the files are available locally for the the DAG processor and it is
  usually as fast as it can be, especially if your machines use fast SSD disks for local storage. Those
  distribution mechanisms have other characteristics that might make them not the best choice for you,
  but if your problems with performance come from distributed filesystem performance, they might be the
  best approach to follow.
* Database connections and Database usage might become a problem as you want to increase performance and
  process more things in parallel. Airflow is known for being "database-connection hungry" - the more dags
  you have and the more you want to process in parallel, the more database connections will be opened.
  This is generally not a problem for MySQL as its model of handling connections is thread-based, but this
  might be a problem for Postgres, where connection handling is process-based. It is a general consensus
  that if you have even medium size Postgres-based Airflow installation, the best solution is to use
  `PGBouncer <https://www.pgbouncer.org/>`_ as a proxy to your database. The :doc:`helm-chart:index`
  supports PGBouncer out-of-the-box.
* CPU usage is most important for FileProcessors - those are the processes that parse and execute
  Python DAG files. Since DAG processors typically triggers such parsing continuously, when you have a lot of dags,
  the processing might take a lot of CPU. You can mitigate it by increasing the
  :ref:`config:dag_processor__min_file_process_interval`, but this is one of the mentioned trade-offs,
  result of this is that changes to such files will be picked up slower and you will see delays between
  submitting the files and getting them available in Airflow UI and executed by Scheduler. Optimizing
  the way how your dags are built, avoiding external data sources is your best approach to improve CPU
  usage. If you have more CPUs available, you can increase number of processing threads
  :ref:`config:dag_processor__parsing_processes`.
* Airflow might use quite a significant amount of memory when you try to get more performance out of it.
  Often more performance is achieved in Airflow by increasing the number of processes handling the load,
  and each process requires whole interpreter of Python loaded, a lot of classes imported, temporary
  in-memory storage. A lot of it is optimized by Airflow by using forking and copy-on-write memory used
  but in case new classes are imported after forking this can lead to extra memory pressure.
  You need to observe if your system is using more memory than it has - which results with using swap disk,
  which dramatically decreases performance. Make sure when you look at memory usage, pay attention to the
  kind of memory you are observing. Usually you should look at ``working memory`` (names might vary depending
  on your deployment) rather than ``total memory used``.

What can you do, to improve DAG processor's performance
"""""""""""""""""""""""""""""""""""""""""""""""""""""""

When you know what your resource usage is, the improvements that you can consider might be:

* improve the logic, efficiency of parsing and reduce complexity of your top-level DAG Python code. It is
  parsed continuously so optimizing that code might bring tremendous improvements, especially if you try
  to reach out to some external databases etc. while parsing dags (this should be avoided at all cost).
  The :ref:`best_practices/top_level_code` explains what are the best practices for writing your top-level
  Python code. The :ref:`best_practices/reducing_dag_complexity` document provides some areas that you might
  look at when you want to reduce complexity of your code.
* improve utilization of your resources. This is when you have a free capacity in your system that
  seems underutilized (again CPU, memory I/O, networking are the prime candidates) - you can take
  actions like increasing number of parsing processes might bring improvements in performance at the
  expense of higher utilization of those.
* increase hardware capacity (for example if you see that CPU is limiting you or that I/O you use for
  DAG filesystem is at its limits). Often the problem with DAG processor performance is
  simply because your system is not "capable" enough and this might be the only way, unless a shared database
  or filesystem is a bottleneck.
* experiment with different values for the "DAG processor tunables". Often you might get better effects by
  simply exchanging one performance aspect for another. For example if you want to decrease the
  CPU usage, you might increase file processing interval (but the result will be that new dags will
  appear with bigger delay). Usually performance tuning is the art of balancing different aspects.
* sometimes you change DAG processor behavior slightly (for example change parsing sort order)
  in order to get better fine-tuned results for your particular deployment.

DAG processor Configuration options
"""""""""""""""""""""""""""""""""""

The following config settings can be used to control aspects of the Scheduler.
However, you can also look at other non-performance-related scheduler configuration parameters available at
:doc:`../configurations-ref` in the ``[scheduler]`` section.

- :ref:`config:dag_processor__file_parsing_sort_mode`
  The scheduler will list and sort the DAG files to decide the parsing order.

- :ref:`config:dag_processor__min_file_process_interval`
  Number of seconds after which a DAG file is re-parsed. The DAG file is parsed every
  min_file_process_interval number of seconds. Updates to dags are reflected after
  this interval. Keeping this number low will increase CPU usage.

- :ref:`config:dag_processor__parsing_processes`
  The scheduler can run multiple processes in parallel to parse DAG files. This defines
  how many processes will run.
