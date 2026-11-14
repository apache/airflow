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

.. _running-airflow-in-docker-localexecutor:

Running Airflow with LocalExecutor in Docker
============================================

The :doc:`main Docker Compose guide </howto/docker-compose/index>` uses the
:doc:`CeleryExecutor <apache-airflow-providers-celery:celery_executor>`, which needs Redis,
a Celery worker and optionally Flower. If you only want to try Airflow, develop Dags, or
run a small instance on a single machine, :doc:`LocalExecutor </core-concepts/executor/local>`
is simpler: the scheduler runs the tasks itself in parallel subprocesses, so PostgreSQL for
the metadata database is the only extra container.

This page describes an alternative ``docker-compose.yaml`` that uses LocalExecutor and
PostgreSQL, with no Redis and no worker.

.. caution::
    This procedure can be useful for learning and exploration.
    However, adapting it for use in real-world situations can be complicated, and the docker
    compose file does not provide any security guarantees required for a production system.
    LocalExecutor is limited to a single machine, and tasks share the scheduler's resources.

.. note::
    This example sets ``AIRFLOW__CORE__AUTH_MANAGER`` to ``FabAuthManager`` so that the account
    created by ``airflow-init`` exists after startup. The official ``apache/airflow`` image ships
    the FAB provider. If you build your own image from a plain ``pip install apache-airflow``,
    install ``apache-airflow-providers-fab`` as well: without it the ``airflow users`` command is
    unavailable and the login described below will fail.

When to use this example
------------------------

Use the LocalExecutor example when:

* you want fewer moving parts than the CeleryExecutor setup,
* tasks are short enough to run alongside the scheduler, or they mostly wait on remote systems
  with :doc:`deferrable operators </authoring-and-scheduling/deferring>`,
* parallelism requirements fit on one machine.

Stay with the CeleryExecutor example or the :doc:`Helm chart <helm-chart:index>` when you need
to scale workers independently of the scheduler, or run tasks on several machines.

Fetching the ``docker-compose.yaml``
------------------------------------

Download the file into an empty directory, then create the directories it mounts and the
``.env`` file that records your host user id:

.. code-block:: bash

    curl -LfO 'https://airflow.apache.org/docs/apache-airflow/|version|/docker-compose-localexecutor.yaml'
    mkdir -p ./dags ./logs ./plugins ./config
    echo -e "AIRFLOW_UID=$(id -u)" > .env

The services in this file are:

* ``postgres`` - the metadata database. It is the only dependency besides the Airflow containers.
* ``airflow-apiserver`` - the API server, available at ``http://localhost:8080``; it also hosts the UI.
* ``airflow-scheduler`` - parses the schedule, queues tasks and, with LocalExecutor, executes them.
* ``airflow-dag-processor`` - parses Dag files separately from the scheduler.
* ``airflow-triggerer`` - runs the event loop for deferrable tasks.
* ``airflow-init`` - migrates the database and creates the first account, then exits.
* ``airflow-cli`` - an optional container for running ``airflow`` CLI commands, enabled with ``--profile debug``.

Initializing the environment
----------------------------

Run the initialization container once. It applies the database migrations and creates the
account with the login ``airflow`` and the password ``airflow``:

.. code-block:: bash

    docker compose up airflow-init

Wait for the container to exit with code 0 before starting the cluster.

Running Airflow
---------------

.. code-block:: bash

    docker compose up

When the containers report healthy, open http://localhost:8080 and log in as ``airflow``.
To run CLI commands, use the ``airflow-cli`` service, for example:

.. code-block:: bash

    docker compose run airflow-cli airflow info

To run a single Dag and watch it succeed, trigger one of the example Dags from the UI. You can
list them, and run one without waiting for its schedule, from the CLI:

.. code-block:: bash

    docker compose run airflow-cli airflow dags list
    docker compose run airflow-cli airflow dags test <dag_id>

Unlike the CeleryExecutor setup, there is no ``airflow-worker`` container and no Flower:
tasks are logged by the scheduler container, so ``docker compose logs airflow-scheduler`` is
where task output appears.

Cleaning up
-----------

.. code-block:: bash

    docker compose down --volumes --remove-orphans
