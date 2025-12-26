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

Edge Provider Architecture
==========================

Airflow consist of several components which are connected like in the following diagram. The Edge Worker which is
deployed outside of the central Airflow cluster is connected via HTTP(s) to the API server of the Airflow cluster:

.. graphviz::

    digraph A{
        rankdir="TB"
        node[shape="rectangle", style="rounded"]


        subgraph cluster {
            label="Cluster";
            {rank = same; dag; database}
            {rank = same; workers; scheduler; api}

            workers[label="(Central) Workers"]
            scheduler[label="Scheduler"]
            api[label="API server"]
            database[label="Database"]
            dag[label="Dag files"]

            api->workers
            api->database

            workers->dag
            workers->database

            scheduler->database
        }

        subgraph edge_worker_subgraph {
            label="Edge site";
            {rank = same; edge_worker; edge_dag}
            edge_worker[label="Edge Worker"]
            edge_dag[label="Dag files (Remote copy)"]

            edge_worker->edge_dag
        }

        edge_worker->api[label="HTTP(s)"]
    }

* **Workers** (Central) - Execute the assigned tasks - most standard setup has local or centralized workers, e.g. via Celery
* **Edge Workers** - Special workers which pull tasks via HTTP(s) as provided as feature via this provider package
* **Scheduler** - Responsible for adding the necessary tasks to the queue. The EdgeExecutor is running as a module inside the scheduler.
* **API server** - HTTP REST API Server provides access to Dag/task status information. The required end-points are
  provided by the Edge provider plugin. The Edge Worker uses this API to pull tasks and send back the results.
* **Database** - Contains information about the status of tasks, Dags, Variables, connections, etc.

In detail the parts of the Edge provider are deployed as follows:

.. image:: img/edge_package.svg
   :alt: Overview and communication of Edge Provider modules

* **EdgeExecutor** - The EdgeExecutor is running inside the core Airflow scheduler. It is responsible for
  scheduling tasks and sending them to the Edge job queue in the database. To activate the EdgeExecutor, you
  need to set the ``executor`` configuration option in the ``airflow.cfg`` file to
  ``airflow.providers.edge3.executors.EdgeExecutor``. For more details see :doc:`edge_executor`. Note that also
  multiple executors can be used in parallel together with the EdgeExecutor.
* **API server** - The API server is providing REST endpoints to the web UI as well
  as serves static files. The Edge provider adds a plugin that provides additional REST API for the Edge Worker
  as well as UI elements to manage workers (not available in Airflow 3.0).
  The API server is responsible for handling requests from the Edge Worker and sending back the results. To
  activate the API server, you need to set the ``api_enabled`` configuration option in ``edge`` section in the
  ``airflow.cfg`` file to ``True``. The API endpoints for edge is not started by default.
  For more details see :doc:`ui_plugin`.
* **Database** - The Airflow meta database is used to store the status of tasks, Dags, Variables, Connections
  etc. The Edge provider uses the database to store the status of the Edge Worker instances and the tasks that
  are assigned to it. The database is also used to store the results of the tasks that are executed by the
  Edge Worker. Setup of needed tables and migration is done automatically when the provider package is deployed.
* **Edge Worker** - The Edge Worker is a lightweight process that runs on the edge device. It is responsible for
  pulling tasks from the API server and executing them. The Edge Worker is a standalone process that can be
  deployed on any machine that has access to the API server. It is designed to be lightweight and easy to
  deploy. The Edge Worker is implemented as a command line tool that can be started with the ``airflow edge worker``
  command. For more details see :doc:`deployment`.

Edge Worker State Model
-----------------------

Each Edge Worker is tracked from the API server such that it is known which worker is currently active. This is
for monitoring as well as administrators as else it is assumed a distributed monitoring and tracking is hard to
achieve. This also allows central management for administrative maintenance.

Workers send regular heartbeats to the API server to indicate that they are still alive. The heartbeats are used to
determine the state of the worker.

The following states are used to track the worker:

.. graphviz::

   digraph edge_worker_state {
      node [shape=circle];

      STARTING[label="starting"];
      IDLE[label="idle"];
      RUNNING[label="running"];
      TERMINATING[label="terminating"];
      OFFLINE[label="offline"];
      UNKNOWN[label="unknown"];
      MAINTENANCE_REQUEST[label="maintenance request"];
      MAINTENANCE_PENDING[label="maintenance pending"];
      MAINTENANCE_MODE[label="maintenance mode"];
      MAINTENANCE_EXIT[label="maintenance exit"];
      OFFLINE_MAINTENANCE[label="offline maintenance"];

      STARTING->IDLE[label="initialization"];
      IDLE->RUNNING[label="new task"];
      RUNNING->IDLE[label="all tasks completed"];
      IDLE->MAINTENANCE_REQUEST[label="triggered by admin"];
      RUNNING->MAINTENANCE_REQUEST[label="triggered by admin"];
      MAINTENANCE_REQUEST->MAINTENANCE_PENDING[label="if running tasks > 0"];
      MAINTENANCE_REQUEST->MAINTENANCE_MODE[label="if running tasks = 0"];
      MAINTENANCE_PENDING->MAINTENANCE_MODE[label="running tasks = 0"];
      MAINTENANCE_PENDING->MAINTENANCE_EXIT[label="triggered by admin"];
      MAINTENANCE_MODE->MAINTENANCE_EXIT[label="triggered by admin"];
      MAINTENANCE_EXIT->RUNNING[label="if running tasks > 0"];
      MAINTENANCE_EXIT->IDLE[label="if running tasks = 0"];
      IDLE->OFFLINE[label="on clean shutdown"];
      RUNNING->TERMINATING[label="on clean shutdown if running tasks > 0"];
      TERMINATING->OFFLINE[label="on clean shutdown if running tasks = 0"];
   }

See also :py:class:`airflow.providers.edge3.models.edge_worker.EdgeWorkerState`
for a documentation of details of all states of the Edge Worker.

Feature Backlog Edge Provider
-----------------------------

The current version of the EdgeExecutor is released with known limitations. It will mature over time.

The following features are known missing and will be implemented in increments:

- API token per worker: Today there is a global API token available only
- Edge Worker Plugin

  - Overview about queues / jobs per queue
  - Allow starting Edge Worker REST API separate to api-server
  - Add some hints how to setup an additional worker

- Edge Worker CLI

  - Use WebSockets instead of HTTP calls for communication
  - Send logs also to TaskFileHandler if external logging services are used
  - Integration into telemetry to send metrics from remote site
  - Publish system metrics with heartbeats (CPU, Disk space, RAM, Load)
  - Be more liberal e.g. on patch version. Currently requires exact version match
    (In current state if versions do not match, the worker will gracefully shut
    down when jobs are completed, no new jobs will be started)

- Tests

  - System tests in GitHub, test the deployment of the worker with a Dag execution
  - Test/Support on Windows for Edge Worker

- Scaling test - Check and define boundaries of workers/jobs. Today it is known to
  scale into a range of ~80 workers. This is not a hard limit but just an experience reported.
- Load tests - impact of scaled execution and code optimization
- Incremental logs during task execution can be served w/o shared log disk on api-server
- Reduce dependencies during execution: Today the worker depends on the airflow core with a lot
  of transitive dependencies. Target is to reduce the dependencies to a minimum like TaskSDK
  and providers only.

- Documentation

  - Provide scripts and guides to install edge components as service (systemd)
  - Extend Helm-Chart for needed support
  - Provide an example docker compose for worker setup
