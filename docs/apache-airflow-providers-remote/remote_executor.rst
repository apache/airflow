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

Remote Executor
===============

.. note::

    The Remote Provider Package is an experimental preview. Features and stability is limited
    and needs to be improved over time. Target is to have full support in Airflow 3.
    Once Airflow 3 support contains Remote Provider, maintenance of the Airflow 2 package will
    be dis-continued.


.. note::

    As of Airflow 2.10.0, you can install the ``remote`` provider package to use this executor.
    This can be done by installing ``apache-airflow-providers-remote`` or by installing Airflow
    with the ``remote`` extra: ``pip install 'apache-airflow[remote]'``.


``RemoteExecutor`` is an option if you want to distribute tasks to workers distributed in different locations.
You can use it also in parallel with other executors if needed. Change your ``airflow.cfg`` to point
the executor parameter to ``RemoteExecutor`` and provide the related settings.

The configuration parameters of the Remote Executor can be found in the Remote provider's :doc:`configurations-ref`.

Here are a few imperative requirements for your workers:

- ``airflow`` needs to be installed, and the CLI needs to be in the path
- Airflow configuration settings should be homogeneous across the cluster
- Operators that are executed on the remote worker need to have their dependencies
  met in that context. Please take a look to the respective provider package
  documentations
- The worker needs to have access to its ``DAGS_FOLDER``, and you need to
  synchronize the filesystems by your own means. A common setup would be to
  store your ``DAGS_FOLDER`` in a Git repository and sync it across machines using
  Chef, Puppet, Ansible, or whatever you use to configure machines in your
  environment. If all your boxes have a common mount point, having your
  pipelines files shared there should work as well


To kick off a worker, you need to setup Airflow and kick off the worker
subcommand

.. code-block:: bash

    airflow remote worker

Your worker should start picking up tasks as soon as they get fired in
its direction. To stop a worker running on a machine you can use:

.. code-block:: bash

    airflow remote stop

It will try to stop the worker gracefully by sending ``SIGTERM`` signal to main
process as and wait until all running tasks are completed.

If you want to monitor the remote activity and worker, use the UI plugin which
is included in the provider package as install on the webserver and use the
"Admin" - "Remote Worker Hosts" and "Remote Worker Jobs" pages.


Some caveats:

- Make sure to specify the Airflow backend and credentials in the remote worker configuration.
- Tasks can consume resources. Make sure your worker has enough resources to run ``worker_concurrency`` tasks
- Queue names are limited to 256 characters, but each broker backend might have its own restrictions

See :doc:`apache-airflow:administration-and-deployment/modules_management` for details on how Python and Airflow manage modules.

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

        subgraph remote_worker_subgraph {
            label="Remote site";
            remote_worker[label="Remote Worker"]
            remote_dag[label="DAG files (Remote)"]

            remote_worker->remote_dag
        }

        remote_worker->web[label="HTTP(s)"]
    }

Airflow consist of several components:

* **Workers** - Execute the assigned tasks - most standard setup has local or centralized workers, e.g. via Celery
* **Remote Workers** - Special workers which pull tasks via HTTP as provided as feature via this provider package
* **Scheduler** - Responsible for adding the necessary tasks to the queue
* **Web server** - HTTP Server provides access to DAG/task status information
* **Database** - Contains information about the status of tasks, DAGs, Variables, connections, etc.


.. _remote_executor:queue:

Queues
------

When using the RemoteExecutor, the workers that tasks are sent to
can be specified. ``queue`` is an attribute of BaseOperator, so any
task can be assigned to any queue. The default queue for the environment
is defined in the ``airflow.cfg``'s ``operators -> default_queue``. This defines
the queue that tasks get assigned to when not specified, as well as which
queue Airflow workers listen to when started.

Workers can listen to one or multiple queues of tasks. When a worker is
started (using command ``airflow remote worker``), a set of comma-delimited queue
names (with no whitespace) can be given (e.g. ``airflow remote worker -q remote,wisconsin_site``).
This worker will then only pick up tasks wired to the specified queue(s).

This can be useful if you need specialized workers, either from a
resource perspective (for say very lightweight tasks where one worker
could take thousands of tasks without a problem), or from an environment
perspective (you want a worker running from a specific location where required
infrastructure is available).
