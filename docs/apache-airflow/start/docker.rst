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

.. _running-airflow-in-docker:

Running Airflow in Docker
#########################

This quick-start guide will allow you to quickly get Airflow up and running with :doc:`CeleryExecutor </executor/celery>` in Docker.  For running airflow in production, we recommend running on kubernetes with the official helm chart.

Customizing the quick-start Docker Compose
==========================================

DO NOT attempt to customize images and the Docker Compose if you do not know exactly what you are doing,
do not know Docker Compose, or are not prepared to debug and resolve problems on your own. If you do not
know Docker Compose and expect Airflow to **just work** beyond following precisely the quick-start,
then please use other ways of running Airflow - for example :doc:`/start/local` for testing and trying
and :doc:`Official Airflow Community Helm Chart<helm-chart:index>` for production purposes.

Even if many users think of Docker Compose as "ready to use", it is really a developer tool, that requires
the user to know very well how docker images, containers, docker compose networking, volumes, naming, image
building works. It is extremely easy to make mistakes that lead to difficult to diagnose problems and if
you are not ready to spend your own time on learning and diagnosing and resolving those problems on your own
do not follow this path. You have been warned.

If you customize, or modify images, the compose file and see problem do not expect you will get a lot of
help with solving those problems in the Airflow support channels. Most of the problems you will experience
are Docker Compose related problems and if you need help in solving them, there are dedicated channels in
`Docker Compose <https://github.com/docker/compose>`_ that you can use.

Production readiness
====================

.. warning::
    DO NOT expect the Docker Compose below will be enough to run production-ready Docker Compose Airflow installation using it.
    This is truly ``quick-start`` docker-compose for you to get Airflow up and running locally and get your hands dirty with
    Airflow. Configuring a Docker-Compose installation that is ready for production requires an intrinsic knowledge of
    Docker Compose, a lot of customization and possibly even writing the Docker Compose file that will suit your needs
    from the scratch. It's probably OK if you want to run Docker Compose-based deployment, but short of becoming a
    Docker Compose expert, it's highly unlikely you will get robust deployment with it.

    If you want to get an easy to configure Docker-based deployment that Airflow Community develops, supports and
    can provide support with deployment, you should consider using Kubernetes and deploying Airflow using
    :doc:`Official Airflow Community Helm Chart<helm-chart:index>`.

Before you begin
================

Follow these steps to install the necessary tools.

1. Install `Docker Community Edition (CE) <https://docs.docker.com/engine/installation/>`__ on your workstation. Depending on the OS, you may need to configure your Docker instance to use 4.00 GB of memory for all containers to run properly. Please refer to the Resources section if using `Docker for Windows <https://docs.docker.com/docker-for-windows/#resources>`__ or `Docker for Mac <https://docs.docker.com/docker-for-mac/#resources>`__ for more information.
2. Install `Docker Compose <https://docs.docker.com/compose/install/>`__ v1.29.1 and newer on your workstation.

Older versions of ``docker-compose`` do not support all the features required by ``docker-compose.yaml`` file, so double check that your version meets the minimum version requirements.

.. warning::
    Default amount of memory available for Docker on MacOS is often not enough to get Airflow up and running.
    If enough memory is not allocated, it might lead to airflow webserver continuously restarting.
    You should at least allocate 4GB memory for the Docker Engine (ideally 8GB). You can check
    and change the amount of memory in `Resources <https://docs.docker.com/docker-for-mac/#resources>`_

    You can also check if you have enough memory by running this command:

    .. code-block:: bash

        docker run --rm "debian:bullseye-slim" bash -c 'numfmt --to iec $(echo $(($(getconf _PHYS_PAGES) * $(getconf PAGE_SIZE))))'


``docker-compose.yaml``
=======================

To deploy Airflow on Docker Compose, you should fetch `docker-compose.yaml <../docker-compose.yaml>`__.

.. jinja:: quick_start_ctx

    .. code-block:: bash

        curl -LfO '{{ doc_root_url }}docker-compose.yaml'

This file contains several service definitions:

- ``airflow-scheduler`` - The :doc:`scheduler </concepts/scheduler>` monitors all tasks and DAGs, then triggers the
  task instances once their dependencies are complete.
- ``airflow-webserver`` - The webserver is available at ``http://localhost:8080``.
- ``airflow-worker`` - The worker that executes the tasks given by the scheduler.
- ``airflow-init`` - The initialization service.
- ``postgres`` - The database.
- ``redis`` - `The redis <https://redis.io/>`__ - broker that forwards messages from scheduler to worker.

Optionally you can enable flower by adding ``--profile flower`` option e.g. ``docker-compose --profile flower up`` or by explicitly targeted on the command line e.g. ``docker-compose up flower``.

- ``flower`` - `The flower app <https://flower.readthedocs.io/en/latest/>`__ for monitoring the environment.
   It is available at ``http://localhost:5555``.

In general, if you want to use airflow locally, your DAGs may try to connect to servers which are running on the host. In order to achieve that, an extra configuration must be added in ``docker-compose.yaml``. For example, on Linux the configuration must be in the section ``services: airflow-worker`` adding ``extra_hosts: - "host.docker.internal:host-gateway"``; and use ``host.docker.internal`` instead of ``localhost``. This configuration vary in different platforms. Please, see documentation for `Windows <https://docs.docker.com/desktop/windows/networking/#use-cases-and-workarounds>`_ and `Mac <https://docs.docker.com/desktop/mac/networking/#use-cases-and-workarounds>`_ for further information.

All these services allow you to run Airflow with :doc:`CeleryExecutor </executor/celery>`. For more information, see :doc:`/concepts/overview`.

Some directories in the container are mounted, which means that their contents are synchronized between your computer and the container.

- ``./dags`` - you can put your DAG files here.
- ``./logs`` - contains logs from task execution and scheduler.
- ``./plugins`` - you can put your :doc:`custom plugins </plugins>` here.

This file uses the latest Airflow image (`apache/airflow <https://hub.docker.com/r/apache/airflow>`__).
If you need to install a new Python library or system library, you can :doc:`build your image <docker-stack:index>`.

Using custom images
===================

When you want to run Airflow locally, you might want to use an extended image, containing some additional dependencies - for
example you might add new python packages, or upgrade airflow providers to a later version. This can be done very easily
by placing a custom Dockerfile alongside your ``docker-compose.yaml``. Then you can use ``docker-compose build`` command
to build your image (you need to do it only once). You can also add the ``--build`` flag to your ``docker-compose`` commands
to rebuild the images on-the-fly when you run other ``docker-compose`` commands.

Examples of how you can extend the image with custom providers, python packages,
apt packages and more can be found in :doc:`Building the image <docker-stack:build>`.

.. _initializing_docker_compose_environment:

Initializing Environment
========================

Before starting Airflow for the first time, You need to prepare your environment, i.e. create the necessary
files, directories and initialize the database.

Setting the right Airflow user
------------------------------

On **Linux**, the quick-start needs to know your host user id and needs to have group id set to ``0``.
Otherwise the files created in ``dags``, ``logs`` and ``plugins`` will be created with ``root`` user.
You have to make sure to configure them for the docker-compose:

.. code-block:: bash

    mkdir -p ./dags ./logs ./plugins
    echo -e "AIRFLOW_UID=$(id -u)" > .env

See :ref:`Docker Compose environment variables <docker-compose-env-variables>`

For other operating systems, you will get warning that ``AIRFLOW_UID`` is not set, but you can
ignore it. You can also manually create the ``.env`` file in the same folder your
``docker-compose.yaml`` is placed with this content to get rid of the warning:

.. code-block:: text

  AIRFLOW_UID=50000

Initialize the database
-----------------------

On **all operating systems**, you need to run database migrations and create the first user account. To do it, run.

.. code-block:: bash

    docker-compose up airflow-init

After initialization is complete, you should see a message like below.

.. parsed-literal::

    airflow-init_1       | Upgrades done
    airflow-init_1       | Admin user airflow created
    airflow-init_1       | |version|
    start_airflow-init_1 exited with code 0

The account created has the login ``airflow`` and the password ``airflow``.

Cleaning-up the environment
===========================

The docker-compose we prepare is a "Quick-start" one. It is not intended to be used in production
and it has a number of caveats - one of them being that the best way to recover from any problem is to clean it
up and restart from the scratch.

The best way to do it is to:

* Run ``docker-compose down --volumes --remove-orphans`` command in the directory you downloaded the
  ``docker-compose.yaml`` file
* remove the whole directory where you downloaded the ``docker-compose.yaml`` file
  ``rm -rf '<DIRECTORY>'``
* re-download the ``docker-compose.yaml`` file
* re-start following the instructions from the very beginning in this guide

Running Airflow
===============

Now you can start all services:

.. code-block:: bash

    docker-compose up

In the second terminal you can check the condition of the containers and make sure that no containers are in unhealthy condition:

.. code-block:: text
    :substitutions:

    $ docker ps
    CONTAINER ID   IMAGE            |version-spacepad| COMMAND                  CREATED          STATUS                    PORTS                              NAMES
    247ebe6cf87a   apache/airflow:|version|   "/usr/bin/dumb-init …"   3 minutes ago    Up 3 minutes (healthy)    8080/tcp                           compose_airflow-worker_1
    ed9b09fc84b1   apache/airflow:|version|   "/usr/bin/dumb-init …"   3 minutes ago    Up 3 minutes (healthy)    8080/tcp                           compose_airflow-scheduler_1
    7cb1fb603a98   apache/airflow:|version|   "/usr/bin/dumb-init …"   3 minutes ago    Up 3 minutes (healthy)    0.0.0.0:8080->8080/tcp             compose_airflow-webserver_1
    74f3bbe506eb   postgres:13      |version-spacepad| "docker-entrypoint.s…"   18 minutes ago   Up 17 minutes (healthy)   5432/tcp                           compose_postgres_1
    0bd6576d23cb   redis:latest     |version-spacepad| "docker-entrypoint.s…"   10 hours ago     Up 17 minutes (healthy)   0.0.0.0:6379->6379/tcp             compose_redis_1

Accessing the environment
=========================

After starting Airflow, you can interact with it in 3 ways;

* by running :doc:`CLI commands </usage-cli>`.
* via a browser using :doc:`the web interface </ui>`.
* using :doc:`the REST API </stable-rest-api-ref>`.

Running the CLI commands
------------------------

You can also run :doc:`CLI commands </usage-cli>`, but you have to do it in one of the defined ``airflow-*`` services. For example, to run ``airflow info``, run the following command:

.. code-block:: bash

    docker-compose run airflow-worker airflow info

If you have Linux or Mac OS, you can make your work easier and download a optional wrapper scripts that will allow you to run commands with a simpler command.

.. jinja:: quick_start_ctx

    .. code-block:: bash

        curl -LfO '{{ doc_root_url }}airflow.sh'
        chmod +x airflow.sh

Now you can run commands easier.

.. code-block:: bash

    ./airflow.sh info

You can also use ``bash`` as parameter to enter interactive bash shell in the container or ``python`` to enter
python container.

.. code-block:: bash

    ./airflow.sh bash

.. code-block:: bash

    ./airflow.sh python

Accessing the web interface
---------------------------

Once the cluster has started up, you can log in to the web interface and try to run some tasks.

The webserver is available at: ``http://localhost:8080``.
The default account has the login ``airflow`` and the password ``airflow``.

Sending requests to the REST API
--------------------------------

`Basic username password authentication <https://tools.ietf.org/html/rfc7617
https://en.wikipedia.org/wiki/Basic_access_authentication>`_ is currently
supported for the REST API, which means you can use common tools to send requests to the API.

The webserver is available at: ``http://localhost:8080``.
The default account has the login ``airflow`` and the password ``airflow``.

Here is a sample ``curl`` command, which sends a request to retrieve a pool list:

.. code-block:: bash

    ENDPOINT_URL="http://localhost:8080/"
    curl -X GET  \
        --user "airflow:airflow" \
        "${ENDPOINT_URL}/api/v1/pools"

Cleaning up
===========

To stop and delete containers, delete volumes with database data and download images, run:

.. code-block:: bash

    docker-compose down --volumes --rmi all

FAQ: Frequently asked questions
===============================

``ModuleNotFoundError: No module named 'XYZ'``
----------------------------------------------

The Docker Compose file uses the latest Airflow image (`apache/airflow <https://hub.docker.com/r/apache/airflow>`__). If you need to install a new Python library or system library, you can :doc:`customize and extend it <docker-stack:index>`.

What's Next?
============

From this point, you can head to the :doc:`/tutorial` section for further examples or the :doc:`/howto/index` section if you're ready to get your hands dirty.

.. _docker-compose-env-variables:

Environment variables supported by Docker Compose
=================================================

Do not confuse the variable names here with the build arguments set when image is built. The
``AIRFLOW_UID`` build arg defaults to ``50000`` when the image is built, so it is
"baked" into the image. On the other hand, the environment variables below can be set when the container
is running, using - for example - result of ``id -u`` command, which allows to use the dynamic host
runtime user id which is unknown at the time of building the image.

+--------------------------------+-----------------------------------------------------+--------------------------+
|   Variable                     | Description                                         | Default                  |
+================================+=====================================================+==========================+
| ``AIRFLOW_IMAGE_NAME``         | Airflow Image to use.                               | apache/airflow:|version| |
+--------------------------------+-----------------------------------------------------+--------------------------+
| ``AIRFLOW_UID``                | UID of the user to run Airflow containers as.       | ``50000``                |
|                                | Override if you want to use use non-default Airflow |                          |
|                                | UID (for example when you map folders from host,    |                          |
|                                | it should be set to result of ``id -u`` call.       |                          |
|                                | When it is changed, a user with the UID is          |                          |
|                                | created with ``default`` name inside the container  |                          |
|                                | and home of the use is set to ``/airflow/home/``    |                          |
|                                | in order to share Python libraries installed there. |                          |
|                                | This is in order to achieve the  OpenShift          |                          |
|                                | compatibility. See more in the                      |                          |
|                                | :ref:`Arbitrary Docker User <arbitrary-docker-user>`|                          |
+--------------------------------+-----------------------------------------------------+--------------------------+

.. note::

    Before Airflow 2.2, the Docker Compose also had ``AIRFLOW_GID`` parameter, but it did not provide any additional
    functionality - only added confusion - so it has been removed.


Those additional variables are useful in case you are trying out/testing Airflow installation via Docker Compose.
They are not intended to be used in production, but they make the environment faster to bootstrap for first time
users with the most common customizations.

+----------------------------------+-----------------------------------------------------+--------------------------+
|   Variable                       | Description                                         | Default                  |
+==================================+=====================================================+==========================+
| ``_AIRFLOW_WWW_USER_USERNAME``   | Username for the administrator UI account.          | airflow                  |
|                                  | If this value is specified, admin UI user gets      |                          |
|                                  | created automatically. This is only useful when     |                          |
|                                  | you want to run Airflow for a test-drive and        |                          |
|                                  | want to start a container with embedded development |                          |
|                                  | database.                                           |                          |
+----------------------------------+-----------------------------------------------------+--------------------------+
| ``_AIRFLOW_WWW_USER_PASSWORD``   | Password for the administrator UI account.          | airflow                  |
|                                  | Only used when ``_AIRFLOW_WWW_USER_USERNAME`` set.  |                          |
+----------------------------------+-----------------------------------------------------+--------------------------+
| ``_PIP_ADDITIONAL_REQUIREMENTS`` | If not empty, airflow containers will attempt to    |                          |
|                                  | install requirements specified in the variable.     |                          |
|                                  | example: ``lxml==4.6.3 charset-normalizer==1.4.1``. |                          |
|                                  | Available in Airflow image 2.1.1 and above.         |                          |
+----------------------------------+-----------------------------------------------------+--------------------------+
