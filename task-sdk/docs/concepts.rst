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

Concepts
========

This section covers the fundamental concepts that Dag authors need to understand when working with the Task SDK.

.. note::

    For information about Airflow 3.x architectural changes and database access restrictions, see the "Upgrading to Airflow 3" guide in the main Airflow documentation.

Terminology
-----------
- **Task**: a Python function (decorated with ``@task``) or Operator invocation representing a unit of work in a Dag.
- **Task Execution**: the runtime machinery that executes user tasks in isolated subprocesses, managed via the Supervisor and Execution API.

Task Lifecycle
--------------

Understanding the task lifecycle helps Dag authors write more effective tasks and debug issues:

- **Scheduled**: The Airflow scheduler enqueues the task instance. The Executor assigns a workload token used for subsequent API authentication and validation with the Airflow API Server.
- **Queued**: Workers poll the queue to retrieve and reserve queued task instances.
- **Subprocess Launch**: The worker's Supervisor process spawns a dedicated subprocess (Task Runner) for the task instance, isolating its execution.
- **Run API Call**: The Supervisor sends a ``POST /run`` call to the Execution API to mark the task as running; the API server responds with a ``TIRunContext`` containing essential runtime information including:

  - ``dag_run``: Complete Dag run information (logical date, data intervals, configuration, etc.)
  - ``max_tries``: Maximum number of retry attempts allowed for this task instance
  - ``should_retry``: Boolean flag indicating whether the task should enter retry state or fail immediately on error
  - ``task_reschedule_count``: Number of times this task has been rescheduled
  - ``variables``: List of Airflow variables accessible to the task instance
  - ``connections``: List of Airflow connections accessible to the task instance
  - ``upstream_map_indexes``: Mapping of upstream task IDs to their map indexes for dynamic task mapping scenarios
  - ``next_method``: Method name to call when resuming from a deferred state (set when task resumes from a trigger)
  - ``next_kwargs``: Arguments to pass to the ``next_method`` (can be encrypted for sensitive data)
  - ``xcom_keys_to_clear``: List of XCom keys that need to be cleared and purged by the worker
- **Runtime Dependency Fetching**: During execution, if the task code requests Airflow resources (variables, connections, etc.), it writes a request to STDOUT. The Supervisor receives it and issues a corresponding API call, and writes the API response into the subprocess's STDIN.
- **Heartbeats & Token Renewal**: The Task Runner periodically emits ``POST /heartbeat`` calls through the Supervisor. Each call authenticates via JWT; if the token has expired, the API server returns a refreshed token in the ``Refreshed-API-Token`` header.
- **XCom Operations**: Upon successful task completion (or when explicitly invoked during execution), the Supervisor issues API calls to set or clear XCom entries for inter-task data passing.
- **State Patch**: When the task reaches a terminal (success/failed), deferred, or rescheduled state, the Supervisor invokes ``PATCH /state`` with the final task status and metadata.

Supervisor & Task Runner
------------------------

Within an Airflow worker, a Supervisor process manages the execution of task instances:

- Spawns isolated subprocesses (Task Runners) for each task, following a parent–child model.
- Establishes dedicated STDIN, STDOUT, and log pipes to communicate with each subprocess.
- Proxies Execution API calls: forwards subprocess requests (e.g., variables, connections, XCom operations, state transitions) to the API server and relays responses.
- Monitors subprocess liveness via heartbeats and marks tasks as failed if heartbeats are missed.
- Generates and refreshes JWT tokens on behalf of subprocesses through heartbeat responses to ensure authenticated API calls.

A Task Runner subprocess provides a sandboxed environment where user task code runs:

- Receives startup messages (run parameters) via STDIN from the Supervisor.
- Executes the Python function or operator code in isolation.
- Emits logs through STDOUT and communicates runtime events (heartbeats, XCom messages) via the Supervisor.
- Performs final state transitions by sending authenticated API calls through the Supervisor.
