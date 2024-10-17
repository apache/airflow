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

This quick-start guide will allow you to quickly get Airflow up and running with the :doc:`CeleryExecutor <apache-airflow-providers-celery:celery_executor>` in Docker.

.. caution::
    This procedure can be useful for learning and exploration. However, adapting it for use in real-world situations can be complicated and the docker compose file does not provide any security guarantees required for production system. Making changes to this procedure will require specialized expertise in Docker & Docker Compose, and the Airflow community may not be able to help you.

    For that reason, we recommend using Kubernetes with the :doc:`Official Airflow Community Helm Chart<helm-chart:index>` when you are ready to run Airflow in production.

Before you begin
================

This procedure assumes familiarity with Docker and Docker Compose. If you haven't worked with these tools before, you should take a moment to run through the `Docker Quick Start <https://docs.docker.com/get-started/>`__ (especially the section on `Docker Compose <https://docs.docker.com/get-started/08_using_compose/>`__) so you are familiar with how they work.

Follow these steps to install the necessary tools, if you have not already done so.

1. Install `Docker Community Edition (CE) <https://docs.docker.com/engine/installation/>`__ on your workstation. Depending on your OS, you may need to configure Docker to use at least 4.00 GB of memory for the Airflow containers to run properly. Please refer to the Resources section in the `Docker for Windows <https://docs.docker.com/docker-for-windows/#resources>`__ or `Docker for Mac <https://docs.docker.com/docker-for-mac/#resources>`__ documentation for more information.
2. Install `Docker Compose <https://docs.docker.com/compose/install/>`__ v2.14.0 or newer on your workstation.

Older versions of ``docker-compose`` do not support all the features required by the Airflow ``docker-compose.yaml`` file, so double check that your version meets the minimum version requirements.

.. tip::
    The default amount of memory available for Docker on macOS is often not enough to get Airflow up and running.
    If enough memory is not allocated, it might lead to the webserver continuously restarting.
    You should allocate at least 4GB memory for the Docker Engine (ideally 8GB).

    You can check if you have enough memory by running this command:

    .. code-block:: bash

        docker run --rm "debian:bookworm-slim" bash -c 'numfmt --to iec $(echo $(($(getconf _PHYS_PAGES) * $(getconf PAGE_SIZE))))'

.. warning::

    Some operating systems (Fedora, ArchLinux, RHEL, Rocky) have recently introduced Kernel changes that result in
    Airflow in Docker Compose consuming 100% memory when run inside the community Docker implementation maintained
    by the OS teams.

    This is an issue with backwards-incompatible containerd configuration that some of Airflow dependencies
    have problems with and is tracked in a few issues:

    * `Moby issue <https://github.com/moby/moby/issues/43361>`_
    * `Containerd issue <https://github.com/containerd/containerd/>`_

    There is no solution yet from the containerd team, but seems that installing
    `Docker Desktop on Linux <https://docs.docker.com/desktop/install/linux-install/>`_ solves the problem as
    stated in `This comment <https://github.com/moby/moby/issues/43361#issuecomment-1227617516>`_ and allows to
    run Breeze with no problems.



Fetching ``docker-compose.yaml``
================================

.. jinja:: quick_start_ctx

    To deploy Airflow on Docker Compose, you should fetch `docker-compose.yaml <{{ doc_root_url }}docker-compose.yaml>`__.

    .. code-block:: bash

        curl -LfO '{{ doc_root_url }}docker-compose.yaml'

.. important::
   From July 2023 Compose V1 stopped receiving updates.
   We strongly advise upgrading to a newer version of Docker Compose, supplied ``docker-compose.yaml`` may not function accurately within Compose V1.

This file contains several service definitions:

- ``airflow-scheduler`` - The :doc:`scheduler </administration-and-deployment/scheduler>` monitors all tasks and DAGs, then triggers the
  task instances once their dependencies are complete.
- ``airflow-webserver`` - The webserver is available at ``http://localhost:8080``.
- ``airflow-worker`` - The worker that executes the tasks given by the scheduler.
- ``airflow-triggerer`` - The triggerer runs an event loop for deferrable tasks.
- ``airflow-init`` - The initialization service.
- ``postgres`` - The database.
- ``redis`` - `The redis <https://redis.io/>`__ - broker that forwards messages from scheduler to worker.

Optionally, you can enable flower by adding ``--profile flower`` option, e.g. ``docker compose --profile flower up``, or by explicitly specifying it on the command line e.g. ``docker compose up flower``.

- ``flower`` - `The flower app <https://flower.readthedocs.io/en/latest/>`__ for monitoring the environment. It is available at ``http://localhost:5555``.

All these services allow you to run Airflow with :doc:`CeleryExecutor <apache-airflow-providers-celery:celery_executor>`. For more information, see :doc:`/core-concepts/overview`.

Some directories in the container are mounted, which means that their contents are synchronized between your computer and the container.

- ``./dags`` - you can put your DAG files here.
- ``./logs`` - contains logs from task execution and scheduler.
- ``./config`` - you can add custom log parser or add ``airflow_local_settings.py`` to configure cluster policy.
- ``./plugins`` - you can put your :doc:`custom plugins </authoring-and-scheduling/plugins>` here.

This file uses the latest Airflow image (`apache/airflow <https://hub.docker.com/r/apache/airflow>`__).
If you need to install a new Python library or system library, you can :doc:`build your image <docker-stack:index>`.


.. _initializing_docker_compose_environment:

Initializing Environment
========================

Before starting Airflow for the first time, you need to prepare your environment, i.e. create the necessary
files, directories and initialize the database.

Setting the right Airflow user
------------------------------

On **Linux**, the quick-start needs to know your host user id and needs to have group id set to ``0``.
Otherwise the files created in ``dags``, ``logs`` and ``plugins`` will be created with ``root`` user ownership.
You have to make sure to configure them for the docker-compose:

.. code-block:: bash

    mkdir -p ./dags ./logs ./plugins ./config
    echo -e "AIRFLOW_UID=$(id -u)" > .env

See :ref:`Docker Compose environment variables <docker-compose-env-variables>`

For other operating systems, you may get a warning that ``AIRFLOW_UID`` is not set, but you can
safely ignore it. You can also manually create an ``.env`` file in the same folder as
``docker-compose.yaml`` with this content to get rid of the warning:

.. code-block:: text

  AIRFLOW_UID=50000

Initialize the database
-----------------------

On **all operating systems**, you need to run database migrations and create the first user account. To do this, run.

.. code-block:: bash

    docker compose up airflow-init

After initialization is complete, you should see a message like this:

.. parsed-literal::

    airflow-init_1       | Upgrades done
    airflow-init_1       | Admin user airflow created
    airflow-init_1       | |version|
    start_airflow-init_1 exited with code 0

The account created has the login ``airflow`` and the password ``airflow``.

Cleaning-up the environment
===========================

The docker-compose environment we have prepared is a "quick-start" one. It was not designed to be used in production
and it has a number of caveats - one of them being that the best way to recover from any problem is to clean it
up and restart from scratch.

The best way to do this is to:

* Run ``docker compose down --volumes --remove-orphans`` command in the directory you downloaded the
  ``docker-compose.yaml`` file
* Remove the entire directory where you downloaded the ``docker-compose.yaml`` file
  ``rm -rf '<DIRECTORY>'``
* Run through this guide from the very beginning, starting by re-downloading the ``docker-compose.yaml`` file

Running Airflow
===============

Now you can start all services:

.. code-block:: bash

    docker compose up

.. note::
  docker-compose is old syntax. Please check `Stackoverflow <https://stackoverflow.com/questions/66514436/difference-between-docker-compose-and-docker-compose>`__.

In a second terminal you can check the condition of the containers and make sure that no containers are in an unhealthy condition:

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

After starting Airflow, you can interact with it in 3 ways:

* by running :doc:`CLI commands </howto/usage-cli>`.
* via a browser using :doc:`the web interface </ui>`.
* using :doc:`the REST API </stable-rest-api-ref>`.

Running the CLI commands
------------------------

You can also run :doc:`CLI commands <../usage-cli>`, but you have to do it in one of the defined ``airflow-*`` services. For example, to run ``airflow info``, run the following command:

.. code-block:: bash

    docker compose run airflow-worker airflow info

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

Once the cluster has started up, you can log in to the web interface and begin experimenting with DAGs.

The webserver is available at: ``http://localhost:8080``.
The default account has the login ``airflow`` and the password ``airflow``.

Sending requests to the REST API
--------------------------------

`Basic username password authentication <https://en.wikipedia.org/wiki/Basic_access_authentication>`_ is currently
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

    docker compose down --volumes --rmi all

Using custom images
===================

When you want to run Airflow locally, you might want to use an extended image, containing some additional dependencies - for
example you might add new python packages, or upgrade airflow providers to a later version. This can be done very easily
by specifying ``build: .`` in your ``docker-compose.yaml`` and placing a custom Dockerfile alongside your
``docker-compose.yaml``. Then you can use ``docker compose build`` command
to build your image (you need to do it only once). You can also add the ``--build`` flag to your ``docker compose`` commands
to rebuild the images on-the-fly when you run other ``docker compose`` commands.

Examples of how you can extend the image with custom providers, python packages,
apt packages and more can be found in :doc:`Building the image <docker-stack:build>`.

.. note::
   Creating custom images means that you need to maintain also a level of automation as you need to re-create the images
   when either the packages you want to install or Airflow is upgraded. Please do not forget about keeping these scripts.
   Also keep in mind, that in cases when you run pure Python tasks, you can use the
   `Python Virtualenv functions <_howto/operator:PythonVirtualenvOperator>`_ which will
   dynamically source and install python dependencies during runtime. With Airflow 2.8.0 Virtualenvs can also be cached.

Special case - adding dependencies via requirements.txt file
============================================================

Usual case for custom images, is when you want to add a set of requirements to it - usually stored in
``requirements.txt`` file. For development, you might be tempted to add it dynamically when you are
starting the original airflow image, but this has a number of side effects (for example your containers
will start much slower - each additional dependency will further delay your containers start up time).
Also it is completely unnecessary, because docker compose has the development workflow built-in.
You can - following the previous chapter, automatically build and use your custom image when you
iterate with docker compose locally. Specifically when you want to add your own requirement file,
you should do those steps:

1) Comment out the ``image: ...`` line and remove comment from the ``build: .`` line in the
   ``docker-compose.yaml`` file. The relevant part of the docker-compose file of yours should look similar
   to (use correct image tag):

.. code-block:: docker

    #image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:|version|}
    build: .

2) Create ``Dockerfile`` in the same folder your ``docker-compose.yaml`` file is with content similar to:

.. code-block:: docker

    FROM apache/airflow:|version|
    ADD requirements.txt .
    RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt

It is the best practice to install apache-airflow in the same version as the one that comes from the
original image. This way you can be sure that ``pip`` will not try to downgrade or upgrade apache
airflow while installing other requirements, which might happen in case you try to add a dependency
that conflicts with the version of apache-airflow that you are using.

3) Place ``requirements.txt`` file in the same directory.

Run ``docker compose build`` to build the image, or add ``--build`` flag to ``docker compose up`` or
``docker compose run`` commands to build the image automatically as needed.

Special case - Adding a custom config file
==========================================

If you have a custom config file and wish to use it in your Airflow instance, you need to perform the following steps:

1) Remove comment from the ``AIRFLOW_CONFIG: '/opt/airflow/config/airflow.cfg'`` line
   in the ``docker-compose.yaml`` file.

2) Place your custom ``airflow.cfg`` file in the local config folder.

3) If your config file has a different name than ``airflow.cfg``, adjust the filename in
   ``AIRFLOW_CONFIG: '/opt/airflow/config/airflow.cfg'``

Networking
==========

In general, if you want to use Airflow locally, your DAGs may try to connect to servers which are running on the host. In order to achieve that, an extra configuration must be added in ``docker-compose.yaml``. For example, on Linux the configuration must be in the section ``services: airflow-worker`` adding ``extra_hosts: - "host.docker.internal:host-gateway"``; and use ``host.docker.internal`` instead of ``localhost``. This configuration vary in different platforms. Please check the Docker documentation for `Windows <https://docs.docker.com/desktop/windows/networking/#use-cases-and-workarounds>`_ and `Mac <https://docs.docker.com/desktop/mac/networking/#use-cases-and-workarounds>`_ for further information.

Debug Airflow inside docker container using PyCharm
===================================================
.. jinja:: quick_start_ctx

    Prerequisites: Create a project in **PyCharm** and download the (`docker-compose.yaml <{{ doc_root_url }}docker-compose.yaml>`__).

Steps:

1) Modify  ``docker-compose.yaml``

   Add the following section under the ``services`` section:

.. code-block:: yaml

    airflow-python:
    <<: *airflow-common
    profiles:
        - debug
    environment:
        <<: *airflow-common-env
    user: "50000:0"
    entrypoint: [ "/bin/bash", "-c" ]

.. note::

    This code snippet creates a new service named **"airflow-python"** specifically for PyCharm's Python interpreter.
    On a Linux system,  if you have executed the command ``echo -e "AIRFLOW_UID=$(id -u)" > .env``, you need to set ``user: "50000:0"`` in ``airflow-python`` service to avoid PyCharm's ``Unresolved reference 'airflow'`` error.

2) Configure PyCharm Interpreter

   * Open PyCharm and navigate to **Settings** > **Project: <Your Project Name>** > **Python Interpreter**.
   * Click the **"Add Interpreter"** button and choose **"On Docker Compose"**.
   * In the **Configuration file** field, select your ``docker-compose.yaml`` file.
   * In the **Service field**, choose the newly added ``airflow-python`` service.
   * Click **"Next"** and follow the prompts to complete the configuration.

.. image:: /img/add_container_python_interpreter.png
    :alt: Configuring the container's Python interpreter in PyCharm, step diagram

Building the interpreter index might take some time.
3) Add `exec` to docker-compose/command and actions in python service
.. image:: /img/docker-compose-pycharm.png
    :alt: Configuring the container's Python interpreter in PyCharm, step diagram
Once configured, you can debug your Airflow code within the container environment, mimicking your local setup.


FAQ: Frequently asked questions
===============================

``ModuleNotFoundError: No module named 'XYZ'``
----------------------------------------------

The Docker Compose file uses the latest Airflow image (`apache/airflow <https://hub.docker.com/r/apache/airflow>`__). If you need to install a new Python library or system library, you can :doc:`customize and extend it <docker-stack:index>`.

What's Next?
============

From this point, you can head to the :doc:`/tutorial/index` section for further examples or the :doc:`/howto/index` section if you're ready to get your hands dirty.

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
|                                | Override if you want to use non-default Airflow     |                          |
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
