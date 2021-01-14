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

Running Airflow in Docker
#########################

this quick-start guide will allow you to quickly start Airflow from :doc:`CeleryExecutor </executor/celery>` in Docker. This is the fastest way to start Airflow.

Before you begin
================

Follow these steps to install the necessary tools.

1. Install `Docker Community Edition (CE) <https://docs.docker.com/engine/installation/>`__ on your workstation.
2. Install `Docker Compose <https://docs.docker.com/compose/install/>`__ on your workstation.

``docker-compose.yaml``
=======================

To deploy Airflow on Docker Compose, you should download `docker-compose.yaml <../docker-compose.yaml>`__. This file contains several service definitions:

- ``airflow-scheduler`` - The :doc:`scheduler </scheduler>` monitors all tasks and DAGs, then triggers the
  task instances once their dependencies are complete.
- ``airflow-webserver`` - The webserver available at ``http://localhost:8080``.
- ``airflow-worker`` - The worker that executes the tasks given by the scheduler.
- ``flower`` - `The flower app <https://flower.readthedocs.io/en/latest/>`__ for monitoring the environment. It is available at ``http://localhost:8080``.
- ``postgres`` - The database.
- ``redis`` - `The redis <https://redis.io/>`__ - broker that forwards messages from scheduler to worker.

All these services allow you to run Airflow with :doc:`CeleryExecutor </executor/celery>`. For more information, see: :ref:`architecture`.

Running Airflow
===============

Before starting Airflow for the first time, you need to initialize the database. To do it, run.

.. code-block:: bash

    docker-compose run --rm airflow-webserver \
        airflow db init

Access to the web server requires a user account, to create it, run:

.. code-block:: bash

    docker-compose run --rm airflow-webserver \
        airflow users create \
            --role Admin \
            --username airflow \
            --password airflow \
            --email airflow@airflow.com \
            --firstname airflow \
            --lastname airflow

Now you can start all services:

.. code-block:: bash

    docker-compose up

In the second terminal you can check the condition of the containers and make sure that no containers are in unhealthy condition:

.. code-block:: bash

    $ docker ps
    CONTAINER ID   IMAGE                  COMMAND                  CREATED          STATUS                    PORTS                              NAMES
    247ebe6cf87a   apache/airflow:2.0.0   "/usr/bin/dumb-init …"   3 minutes ago    Up 3 minutes              8080/tcp                           compose_airflow-worker_1
    ed9b09fc84b1   apache/airflow:2.0.0   "/usr/bin/dumb-init …"   3 minutes ago    Up 3 minutes              8080/tcp                           compose_airflow-scheduler_1
    65ac1da2c219   apache/airflow:2.0.0   "/usr/bin/dumb-init …"   3 minutes ago    Up 3 minutes (healthy)    0.0.0.0:5555->5555/tcp, 8080/tcp   compose_flower_1
    7cb1fb603a98   apache/airflow:2.0.0   "/usr/bin/dumb-init …"   3 minutes ago    Up 3 minutes (healthy)    0.0.0.0:8080->8080/tcp             compose_airflow-webserver_1
    74f3bbe506eb   postgres:9.5           "docker-entrypoint.s…"   18 minutes ago   Up 17 minutes (healthy)   5432/tcp                           compose_postgres_1
    0bd6576d23cb   redis:latest           "docker-entrypoint.s…"   10 hours ago     Up 17 minutes (healthy)   0.0.0.0:6379->6379/tcp             compose_redis_1

Testing
=======

Once the cluster has started up, you login to the web interface and try to run some tasks. The webserver available at: ``http://localhost:8080``.

.. image:: /img/dags.png

Notes
=====

By default, the Docker Compose file uses the latest Airflow image (`apache/airflow< <https://hub.docker.com/r/apache/airflow>`__). If you need, you can :ref:`customize and extend it <docker_image>` to your needs.

What's Next?
============

From this point, you can head to the :doc:`/tutorial` section for further examples or the :doc:`/howto/index` section if you're ready to get your hands dirty.
