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

DAG File Processing refers to the process of turning Python files contained in the DAGs folder into DAG objects that contain tasks to be scheduled.

There are two primary components involved in DAG file processing.  The ``DagFileProcessorManager`` is a process executing an infinite loop that determines which files need
to be processed, and the ``DagFileProcessorProcess`` is a separate process that is started to convert an individual file into one or more DAG objects.

The ``DagFileProcessorManager`` runs user codes. As a result, you can decide to run it as a standalone process in a different host than the scheduler process.
If you decide to run it as a standalone process, you need to set this configuration: ``AIRFLOW__SCHEDULER__STANDALONE_DAG_PROCESSOR=True`` and
run the ``airflow dag-processor`` CLI command, otherwise, starting the scheduler process (``airflow scheduler``) also starts the ``DagFileProcessorManager``.

.. image:: /img/dag_file_processing_diagram.png

``DagFileProcessorManager`` has the following steps:

1. Check for new files:  If the elapsed time since the DAG was last refreshed is > :ref:`config:scheduler__dag_dir_list_interval` then update the file paths list
2. Exclude recently processed files:  Exclude files that have been processed more recently than :ref:`min_file_process_interval<config:scheduler__min_file_process_interval>` and have not been modified
3. Queue file paths: Add files discovered to the file path queue
4. Process files:  Start a new ``DagFileProcessorProcess`` for each file, up to a maximum of :ref:`config:scheduler__parsing_processes`
5. Collect results: Collect the result from any finished DAG processors
6. Log statistics:  Print statistics and emit ``dag_processing.total_parse_time``

``DagFileProcessorProcess`` has the following steps:

1. Process file: The entire process must complete within :ref:`dag_file_processor_timeout<config:core__dag_file_processor_timeout>`
2. The DAG files are loaded as Python module: Must complete within :ref:`dagbag_import_timeout<config:core__dagbag_import_timeout>`
3. Process modules:  Find DAG objects within Python module
4. Return DagBag:  Provide the ``DagFileProcessorManager`` a list of the discovered DAG objects
