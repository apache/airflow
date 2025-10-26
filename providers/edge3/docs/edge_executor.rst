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

``EdgeExecutor`` is an option if you want to distribute tasks to workers distributed in different locations.
You can use it also in parallel with other executors if needed. Change your ``airflow.cfg`` to point
the executor parameter to ``EdgeExecutor`` and provide the related settings. The ``EdgeExecutor`` is the component
to schedule tasks to the edge workers. The edge workers need to be set-up separately as described in :doc:`deployment`.

The configuration parameters of the Edge Executor can be found in the Edge provider's :doc:`configurations-ref`.

To understand the setup of the Edge Executor, please also take a look to :doc:`architecture`.

 See more details Airflow documentation
  :ref:`apache-airflow:using-multiple-executors-concurrently`.

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
If the ``queue`` attribute is not given then a worker will pick tasks from all queues.

This can be useful if you need specialized workers, either from a
resource perspective (for say very lightweight tasks where one worker
could take thousands of tasks without a problem), or from an environment
perspective (you want a worker running from a specific location where required
infrastructure is available).

When using EdgeExecutor in addition to other executors and EdgeExecutor not being the default executor
(that is to say the first one in the list of executors), be reminded to also define EdgeExecutor
as the executor at task or Dag level in addition to the queues you are targeting.
For more details on multiple executors please see :ref:`apache-airflow:using-multiple-executors-concurrently`.

.. _edge_executor:concurrency_slots:

Concurrency slot handling
-------------------------

Some tasks may need more resources than other tasks, to handle these use case the Edge worker supports
concurrency slot handling. The logic behind this is the same as the pool slot feature
see :doc:`apache-airflow:administration-and-deployment/pools`.
Edge worker reuses ``pool_slots`` of task_instance to keep number if task instance parameter as low as possible.
The ``pool_slots`` value works together with the ``worker_concurrency`` value which is defined during start of worker.
If a task needs more resources, the ``pool_slots`` value can be increased to reduce number of tasks running in parallel.
The value can be used to block other tasks from being executed in parallel on the same worker.
A ``pool_slots`` of 2 and a ``worker_concurrency`` of 3 means
that a worker which executes this task can only execute a job with a ``pool_slots`` of 1 in parallel.
If no ``pool_slots`` is defined for a task the default value is 1. The ``pool_slots`` value only supports
integer values.

Here is an example setting pool_slots for a task:

.. code-block:: python

    import os

    import pendulum

    from airflow import DAG
    from airflow.decorators import task
    from airflow.example_dags.libs.helper import print_stuff
    from airflow.settings import AIRFLOW_HOME

    with DAG(
        dag_id="example_edge_pool_slots",
        schedule=None,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        tags=["example"],
    ) as dag:

        @task(executor="EdgeExecutor", pool_slots=2)
        def task_with_template():
            print_stuff()

        task_with_template()

Current Limitations Edge Executor
---------------------------------

- Some known limitations

  - Log upload will only work if you use a single api-server / webserver instance or they need to share one log file
    volume. Logs are uploaded in chunks and are transferred via API. If you use multiple api-servers / webservers w/o
    a shared log volume the logs will be scattered across the api-server / webserver instances and if you view the
    logs on UI you will only see fractions of the logs.
  - Performance: No extensive performance assessment and scaling tests have been made. The edge executor package is
    optimized for stability. This will be incrementally improved in future releases. Setups have reported stable
    operation with ~80 workers until now. Note that executed tasks require more api-server / webserver API capacity.
