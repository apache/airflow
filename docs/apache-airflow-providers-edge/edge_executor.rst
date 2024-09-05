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

Edge Executor
=============

.. note::

    The Edge Provider Package is an experimental preview. Features and stability is limited
    and needs to be improved over time. Target is to have full support in Airflow 3.
    Once Airflow 3 support contains Edge Provider, maintenance of the Airflow 2 package will
    be dis-continued.


.. note::

    As of Airflow 2.10.0, you can install the ``edge`` provider package to use this executor.
    This can be done by installing ``apache-airflow-providers-edge`` or by installing Airflow
    with the ``edge`` extra: ``pip install 'apache-airflow[edge]'``.


``EdgeExecutor`` is an option if you want to distribute tasks to workers distributed in different locations.
You can use it also in parallel with other executors if needed. Change your ``airflow.cfg`` to point
the executor parameter to ``EdgeExecutor`` and provide the related settings.

The configuration parameters of the Edge Executor can be found in the Edge provider's :doc:`configurations-ref`.

Here are a few imperative requirements for your workers:

- ``airflow`` needs to be installed, and the CLI needs to be in the path
- Airflow configuration settings should be homogeneous across the cluster
- Operators that are executed on the Edge Worker need to have their dependencies
  met in that context. Please take a look to the respective provider package
  documentations
- The worker needs to have access to its ``DAGS_FOLDER``, and you need to
  synchronize the filesystems by your own means. A common setup would be to
  store your ``DAGS_FOLDER`` in a Git repository and sync it across machines using
  Chef, Puppet, Ansible, or whatever you use to configure machines in your
  environment. If all your boxes have a common mount point, having your
  pipelines files shared there should work as well


Minimum configuration for the Edge Worker to make it running is:

- Section ``[core]``

  - ``executor``: Executor must be set or added to be ``airflow.providers.edge.executors.EdgeExecutor``
  - ``internal_api_secret_key``: An encryption key must be set on webserver and Edge Worker component as
    shared secret to authenticate traffic. It should be a random string like the fernet key
    (but preferably not the same).

- Section ``[edge]``

  - ``api_enabled``: Must be set to true. It is disabled by intend not to expose
    the endpoint by default. This is the endpoint the worker connects to.
    In a future release a dedicated API server can be started.
  - ``api_url``: Must be set to the URL which exposes the web endpoint

To kick off a worker, you need to setup Airflow and kick off the worker
subcommand

.. code-block:: bash

    airflow edge worker

Your worker should start picking up tasks as soon as they get fired in
its direction. To stop a worker running on a machine you can use:

.. code-block:: bash

    airflow edge stop

It will try to stop the worker gracefully by sending ``SIGINT`` signal to main
process as and wait until all running tasks are completed.

If you want to monitor the remote activity and worker, use the UI plugin which
is included in the provider package as install on the webserver and use the
"Admin" - "Edge Worker Hosts" and "Edge Worker Jobs" pages.


Some caveats:

- Tasks can consume resources. Make sure your worker has enough resources to run ``worker_concurrency`` tasks
- Queue names are limited to 256 characters, but each broker backend might have its own restrictions

See :doc:`apache-airflow:administration-and-deployment/modules_management` for details on how Python and Airflow manage modules.

Limitations of Pre-Release
--------------------------

As this provider package is an experimental preview not all functions are support and not fully covered.
If you plan to use the Edge Executor / Worker in the current stage you need to ensure you test properly
before use. The following features have been initially tested and are working:

- Some core operators

  - ``BashOperator``
  - ``PythonOperator``
  - ``@task`` decorator
  - ``@task.branch`` decorator
  - ``@task.virtualenv`` decorator
  - ``@task.bash`` decorator
  - Dynamic Mapped Tasks
  - XCom read/write
  - Variable and Connection access
  - Setup and Teardown tasks

- Some known limitations

  - Tasks that require DB access will fail - no DB connection from remote site is possible
  - This also means that some direct Airflow API via Python is not possible (e.g. airflow.models.*)


Architecture
------------

.. graphviz::

    digraph A{
        rankdir="TB"
        node[shape="rectangle", style="rounded"]


        subgraph cluster {
            label="Cluster";
            {rank = same; dag; database}
            {rank = same; workers; scheduler; web}

            workers[label="(Central) Workers"]
            scheduler[label="Scheduler"]
            web[label="Web server"]
            database[label="Database"]
            dag[label="DAG files"]

            web->workers
            web->database

            workers->dag
            workers->database

            scheduler->dag
            scheduler->database
        }

        subgraph edge_worker_subgraph {
            label="Edge site";
            edge_worker[label="Edge Worker"]
            edge_dag[label="DAG files (Remote)"]

            edge_worker->edge_dag
        }

        edge_worker->web[label="HTTP(s)"]
    }

Airflow consist of several components:

* **Workers** - Execute the assigned tasks - most standard setup has local or centralized workers, e.g. via Celery
* **Edge Workers** - Special workers which pull tasks via HTTP as provided as feature via this provider package
* **Scheduler** - Responsible for adding the necessary tasks to the queue
* **Web server** - HTTP Server provides access to DAG/task status information
* **Database** - Contains information about the status of tasks, DAGs, Variables, connections, etc.


.. _edge_executor:queue:

Queues
------

When using the EdgeExecutor, the workers that tasks are sent to
can be specified. ``queue`` is an attribute of BaseOperator, so any
task can be assigned to any queue. The default queue for the environment
is defined in the ``airflow.cfg``'s ``operators -> default_queue``. This defines
the queue that tasks get assigned to when not specified, as well as which
queue Airflow workers listen to when started.

Workers can listen to one or multiple queues of tasks. When a worker is
started (using command ``airflow edge worker``), a set of comma-delimited queue
names (with no whitespace) can be given (e.g. ``airflow edge worker -q remote,wisconsin_site``).
This worker will then only pick up tasks wired to the specified queue(s).

This can be useful if you need specialized workers, either from a
resource perspective (for say very lightweight tasks where one worker
could take thousands of tasks without a problem), or from an environment
perspective (you want a worker running from a specific location where required
infrastructure is available).

Feature Backlog of MVP to Release Readiness
-------------------------------------------

As noted above the current version of the EdgeExecutor is a MVP (Minimum Viable Product).
It can be used but must be taken with care if you want to use it productively. Just the
bare minimum functions are provided currently and missing features will be added over time.

The target implementation is sketched in
`AIP-69 (Airflow Improvement Proposal for Edge Executor) <https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=301795932>`_
and this AIP will be completed when open features are implemented and it has production grade stability.

The following features are known missing and will be implemented in increments:

- API token per worker: Today there is a global API token available only
- Edge Worker Plugin

  - Overview about queues / jobs per queue
  - Allow starting Edge Worker REST API separate to webserver
  - Administrative maintenance / temporary disable jobs on worker

- Edge Worker CLI

  - Use WebSockets instead of HTTP calls for communication
  - Handle SIG-INT/CTRL+C and gracefully terminate and complete job (``airflow edge stop`` is working though)
  - Send logs also to TaskFileHandler if external logging services are used
  - Integration into telemetry to send metrics from remote site
  - Allow ``airflow edge stop`` to wait until completed to terminated
  - Publish system metrics with heartbeats (CPU, Disk space, RAM, Load)
  - Be more liberal e.g. on patch version. MVP requires exact version match

- Tests

  - Integration tests in Github
  - Test/Support on Windows for Edge Worker

- Scaling test - Check and define boundaries of workers/jobs
- Airflow 3 / AIP-72 Migration

  - Thin deployment based on Task SDK
  - DAG Code push (no need to GIT Sync)
  - Implicit with AIP-72: Move task context generation from Remote to Executor

- Documentation

  - Describe more details on deployment options and tuning
  - Provide scripts and guides to install edge components as service (systemd)
  - Extend Helm-Chart for needed support
