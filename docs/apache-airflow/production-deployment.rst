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

Production Deployment
^^^^^^^^^^^^^^^^^^^^^

It is time to deploy your DAG in production. To do this, first, you need to make sure that the Airflow is itself production-ready.
Let's see what precautions you need to take.

Database backend
================

Airflow comes with an ``SQLite`` backend by default. This allows the user to run Airflow without any external database.
However, such a setup is meant to be used for testing purposes only; running the default setup in production can lead to data loss in multiple scenarios.
If you want to run production-grade Airflow, make sure you :doc:`configure the backend <howto/initialize-database>` to be an external database such as PostgreSQL or MySQL.

You can change the backend using the following config

.. code-block:: ini

 [core]
 sql_alchemy_conn = my_conn_string

Once you have changed the backend, airflow needs to create all the tables required for operation.
Create an empty DB and give airflow's user the permission to ``CREATE/ALTER`` it.
Once that is done, you can run -

.. code-block:: bash

 airflow db upgrade

``upgrade`` keeps track of migrations already applied, so it's safe to run as often as you need.

.. note::

 Do not use ``airflow db init`` as it can create a lot of default connections, charts, etc. which are not required in production DB.


Multi-Node Cluster
==================

Airflow uses :class:`airflow.executors.sequential_executor.SequentialExecutor` by default. However, by its nature, the user is limited to executing at most
one task at a time. ``Sequential Executor`` also pauses the scheduler when it runs a task, hence not recommended in a production setup.
You should use the :class:`Local executor <airflow.executors.local_executor.LocalExecutor>` for a single machine.
For a multi-node setup, you should use the :doc:`Kubernetes executor <../executor/kubernetes>` or the :doc:`Celery executor <../executor/celery>`.


Once you have configured the executor, it is necessary to make sure that every node in the cluster contains the same configuration and dags.
Airflow sends simple instructions such as "execute task X of dag Y", but does not send any dag files or configuration. You can use a simple cronjob or
any other mechanism to sync DAGs and configs across your nodes, e.g., checkout DAGs from git repo every 5 minutes on all nodes.


Logging
=======

If you are using disposable nodes in your cluster, configure the log storage to be a distributed file system (DFS) such as ``S3`` and ``GCS``, or external services such as
Stackdriver Logging, Elasticsearch or Amazon CloudWatch.
This way, the logs are available even after the node goes down or gets replaced. See :doc:`logging-monitoring/logging-tasks` for configurations.

.. note::

    The logs only appear in your DFS after the task has finished. You can view the logs while the task is running in UI itself.


Configuration
=============

Airflow comes bundled with a default ``airflow.cfg`` configuration file.
You should use environment variables for configurations that change across deployments
e.g. metadata DB, password, etc. You can accomplish this using the format :envvar:`AIRFLOW__{SECTION}__{KEY}`

.. code-block:: bash

 AIRFLOW__CORE__SQL_ALCHEMY_CONN=my_conn_id
 AIRFLOW__WEBSERVER__BASE_URL=http://host:port

Some configurations such as the Airflow Backend connection URI can be derived from bash commands as well:

.. code-block:: ini

 sql_alchemy_conn_cmd = bash_command_to_run


Scheduler Uptime
================

Airflow users have for a long time been affected by a
`core Airflow bug <https://issues.apache.org/jira/browse/AIRFLOW-401>`_
that causes the scheduler to hang without a trace.

Until fully resolved, you can mitigate this issue via a few short-term workarounds:

* Set a reasonable run_duration setting in your ``airflow.cfg``. `Example config <https://github.com/astronomer/airflow-chart/blob/63bc503c67e2cd599df0b6f831d470d09bad7ee7/templates/configmap.yaml#L44>`_.
* Add an ``exec`` style health check to your helm charts on the scheduler deployment to fail if the scheduler has not heartbeat in a while. `Example health check definition <https://github.com/astronomer/helm.astronomer.io/pull/200/files>`_.

Production Container Images
===========================

Customizing or extending the Production Image
---------------------------------------------

Before you dive-deeply in the way how the Airflow Image is build, named and why we are doing it the
way we do, you might want to know very quickly how you can extend or customize the existing image
for Apache Airflow. This chapter gives you a short answer to those questions.

The docker image provided (as convenience binary package) in the
`Apache Airflow DockerHub <https://hub.docker.com/repository/docker/apache/airflow>`_ is a bare image
that has not many external dependencies and extras installed. Apache Airflow has many extras
that can be installed alongside the "core" airflow image and they often require some additional
dependencies. The Apache Airflow image provided as convenience package is optimized for size, so
it provides just a bare minimal set of the extras and dependencies installed and in most cases
you want to either extend or customize the image.

Airflow Summit 2020's `Production Docker Image <https://youtu.be/wDr3Y7q2XoI>`_ talk provides more
details about the context, architecture and customization/extension methods for the Production Image.

Extending the image
...................

Extending the image is easiest if you just need to add some dependencies that do not require
compiling. The compilation framework of Linux (so called ``build-essential``) is pretty big, and
for the production images, size is really important factor to optimize for, so our Production Image
does not contain ``build-essential``. If you need compiler like gcc or g++ or make/cmake etc. - those
are not found in the image and it is recommended that you follow the "customize" route instead.

How to extend the image - it is something you are most likely familiar with - simply
build a new image using Dockerfile's ``FROM`` directive and add whatever you need. Then you can add your
Debian dependencies with ``apt`` or PyPI dependencies with ``pip install`` or any other stuff you need.

You should be aware, about a few things:

* The production image of airflow uses "airflow" user, so if you want to add some of the tools
  as ``root`` user, you need to switch to it with ``USER`` directive of the Dockerfile. Also you
  should remember about following the
  `best practises of Dockerfiles <https://docs.docker.com/develop/develop-images/dockerfile_best-practices/>`_
  to make sure your image is lean and small.

.. code-block:: dockerfile

  FROM apache/airflow:2.0.0
  USER root
  RUN apt-get update \
    && apt-get install -y --no-install-recommends \
           my-awesome-apt-dependency-to-add \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
  USER airflow


* PyPI dependencies in Apache Airflow are installed in the user library, of the "airflow" user, so
  you need to install them with the ``--user`` flag and WITHOUT switching to airflow user. Note also
  that using --no-cache-dir is a good idea that can help to make your image smaller.

.. code-block:: dockerfile

  FROM apache/airflow:2.0.0
  RUN pip install --no-cache-dir --user my-awesome-pip-dependency-to-add


* If your apt, or PyPI dependencies require some of the build-essentials, then your best choice is
  to follow the "Customize the image" route. However it requires to checkout sources of Apache Airflow,
  so you might still want to choose to add build essentials to your image, even if your image will
  be significantly bigger.

.. code-block:: dockerfile

  FROM apache/airflow:2.0.0
  USER root
  RUN apt-get update \
    && apt-get install -y --no-install-recommends \
           build-essential my-awesome-apt-dependency-to-add \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
  USER airflow
  RUN pip install --no-cache-dir --user my-awesome-pip-dependency-to-add


* You can also embed your dags in the image by simply adding them with COPY directive of Airflow.
  The DAGs in production image are in /opt/airflow/dags folder.

Customizing the image
.....................

Customizing the image is an alternative way of adding your own dependencies to the image - better
suited to prepare optimized production images.

The advantage of this method is that it produces optimized image even if you need some compile-time
dependencies that are not needed in the final image. You need to use Airflow Sources to build such images
from the `official distribution folder of Apache Airflow <https://downloads.apache.org/airflow/>`_ for the
released versions, or checked out from the GitHub project if you happen to do it from git sources.

The easiest way to build the image image is to use ``breeze`` script, but you can also build such customized
image by running appropriately crafted docker build in which you specify all the ``build-args``
that you need to add to customize it. You can read about all the args and ways you can build the image
in the `<#production-image-build-arguments>`_ chapter below.

Here just a few examples are presented which should give you general understanding of what you can customize.

This builds the production image in version 3.7 with additional airflow extras from 2.0.0 PyPI package and
additional apt dev and runtime dependencies.

.. code-block:: bash

  docker build . \
    --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" \
    --build-arg PYTHON_MAJOR_MINOR_VERSION=3.7 \
    --build-arg AIRFLOW_INSTALLATION_METHOD="apache-airflow" \
    --build-arg AIRFLOW_VERSION="2.0.0" \
    --build-arg AIRFLOW_INSTALL_VERSION="==2.0.0" \
    --build-arg AIRFLOW_CONSTRAINTS_REFERENCE="constraints-2-0" \
    --build-arg AIRFLOW_SOURCES_FROM="empty" \
    --build-arg AIRFLOW_SOURCES_TO="/empty" \
    --build-arg ADDITIONAL_AIRFLOW_EXTRAS="jdbc" \
    --build-arg ADDITIONAL_PYTHON_DEPS="pandas" \
    --build-arg ADDITIONAL_DEV_APT_DEPS="gcc g++" \
    --build-arg ADDITIONAL_RUNTIME_APT_DEPS="default-jre-headless" \
    --tag my-image


the same image can be built using ``breeze`` (it supports auto-completion of the options):

.. code-block:: bash

  ./breeze build-image \
      --production-image  --python 3.7 --install-airflow-version=2.0.0 \
      --additional-extras=jdbc --additional-python-deps="pandas" \
      --additional-dev-apt-deps="gcc g++" --additional-runtime-apt-deps="default-jre-headless"


You can customize more aspects of the image - such as additional commands executed before apt dependencies
are installed, or adding extra sources to install your dependencies from. You can see all the arguments
described below but here is an example of rather complex command to customize the image
based on example in `this comment <https://github.com/apache/airflow/issues/8605#issuecomment-690065621>`_:

.. code-block:: bash

  docker build . -f Dockerfile \
    --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" \
    --build-arg PYTHON_MAJOR_MINOR_VERSION=3.7 \
    --build-arg AIRFLOW_INSTALLATION_METHOD="apache-airflow" \
    --build-arg AIRFLOW_VERSION="2.0.0" \
    --build-arg AIRFLOW_INSTALL_VERSION="==2.0.0" \
    --build-arg AIRFLOW_CONSTRAINTS_REFERENCE="constraints-2-0" \
    --build-arg AIRFLOW_SOURCES_FROM="empty" \
    --build-arg AIRFLOW_SOURCES_TO="/empty" \
    --build-arg ADDITIONAL_AIRFLOW_EXTRAS="slack" \
    --build-arg ADDITIONAL_PYTHON_DEPS="apache-airflow-backport-providers-odbc \
        apache-airflow-backport-providers-odbc \
        azure-storage-blob \
        sshtunnel \
        google-api-python-client \
        oauth2client \
        beautifulsoup4 \
        dateparser \
        rocketchat_API \
        typeform" \
    --build-arg ADDITIONAL_DEV_APT_DEPS="msodbcsql17 unixodbc-dev g++" \
    --build-arg ADDITIONAL_DEV_APT_COMMAND="curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add --no-tty - && curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list" \
    --build-arg ADDITIONAL_DEV_ENV_VARS="ACCEPT_EULA=Y" \
    --build-arg ADDITIONAL_RUNTIME_APT_COMMAND="curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add --no-tty - && curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list" \
    --build-arg ADDITIONAL_RUNTIME_APT_DEPS="msodbcsql17 unixodbc git procps vim" \
    --build-arg ADDITIONAL_RUNTIME_ENV_VARS="ACCEPT_EULA=Y" \
    --tag my-image

Customizing images in high security restricted environments
...........................................................

You can also make sure your image is only build using local constraint file and locally downloaded
wheel files. This is often useful in Enterprise environments where the binary files are verified and
vetted by the security teams.

This builds below builds the production image in version 3.7 with packages and constraints used from the local
``docker-context-files`` rather than installed from PyPI or GitHub. It also disables MySQL client
installation as it is using external installation method.

Note that as a prerequisite - you need to have downloaded wheel files. In the example below we
first download such constraint file locally and then use ``pip download`` to get the .whl files needed
but in most likely scenario, those wheel files should be copied from an internal repository of such .whl
files. Note that ``AIRFLOW_INSTALL_VERSION`` is only there for reference, the apache airflow .whl file
in the right version is part of the .whl files downloaded.

Note that 'pip download' will only works on Linux host as some of the packages need to be compiled from
sources and you cannot install them providing ``--platform`` switch. They also need to be downloaded using
the same python version as the target image.

The ``pip download`` might happen in a separate environment. The files can be committed to a separate
binary repository and vetted/verified by the security team and used subsequently to build images
of Airflow when needed on an air-gaped system.

Preparing the constraint files and wheel files:

.. code-block:: bash

  rm docker-context-files/*.whl docker-context-files/*.txt

  curl -Lo "docker-context-files/constraints-2-0.txt" \
    https://raw.githubusercontent.com/apache/airflow/constraints-2-0/constraints-3.7.txt

  pip download --dest docker-context-files \
    --constraint docker-context-files/constraints-2-0.txt  \
    apache-airflow[async,aws,azure,celery,dask,elasticsearch,gcp,kubernetes,mysql,postgres,redis,slack,ssh,statsd,virtualenv]==2.0.0


Building the image (after copying the files downloaded to the "docker-context-files" directory:

.. code-block:: bash

  ./breeze build-image \
      --production-image --python 3.7 --install-airflow-version=2.0.0 \
      --disable-mysql-client-installation --disable-pip-cache --install-from-local-files-when-building \
      --constraints-location="/docker-context-files/constraints-2-0.txt"

or

.. code-block:: bash

  docker build . \
    --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" \
    --build-arg PYTHON_MAJOR_MINOR_VERSION=3.7 \
    --build-arg AIRFLOW_INSTALLATION_METHOD="apache-airflow" \
    --build-arg AIRFLOW_VERSION="2.0.0" \
    --build-arg AIRFLOW_INSTALL_VERSION="==2.0.0" \
    --build-arg AIRFLOW_CONSTRAINTS_REFERENCE="constraints-2-0" \
    --build-arg AIRFLOW_SOURCES_FROM="empty" \
    --build-arg AIRFLOW_SOURCES_TO="/empty" \
    --build-arg INSTALL_MYSQL_CLIENT="false" \
    --build-arg AIRFLOW_PRE_CACHED_PIP_PACKAGES="false" \
    --build-arg INSTALL_FROM_DOCKER_CONTEXT_FILES="true" \
    --build-arg AIRFLOW_CONSTRAINTS_LOCATION="/docker-context-files/constraints-2-0.txt"


Customizing & extending the image together
..........................................

You can combine both - customizing & extending the image. You can build the image first using
``customize`` method (either with docker command or with ``breeze`` and then you can ``extend``
the resulting image using ``FROM`` any dependencies you want.

Customizing PYPI installation
.............................

You can customize PYPI sources used during image build by adding a docker-context-files/.pypirc file
This .pypirc will never be committed to the repository and will not be present in the final production image.
It is added and used only in the build segment of the image so it is never copied to the final image.

External sources for dependencies
---------------------------------

In corporate environments, there is often the need to build your Container images using
other than default sources of dependencies. The docker file uses standard sources (such as
Debian apt repositories or PyPI repository. However, in corporate environments, the dependencies
are often only possible to be installed from internal, vetted repositories that are reviewed and
approved by the internal security teams. In those cases, you might need to use those different
sources.

This is rather easy if you extend the image - you simply write your extension commands
using the right sources - either by adding/replacing the sources in apt configuration or
specifying the source repository in pip install command.

It's a bit more involved in the case of customizing the image. We do not have yet (but we are working
on it) a capability of changing the sources via build args. However, since the builds use
Dockerfile that is a source file, you can rather easily simply modify the file manually and
specify different sources to be used by either of the commands.


Comparing extending and customizing the image
---------------------------------------------

Here is the comparison of the two types of building images.

+----------------------------------------------------+---------------------+-----------------------+
|                                                    | Extending the image | Customizing the image |
+====================================================+=====================+=======================+
| Produces optimized image                           | No                  | Yes                   |
+----------------------------------------------------+---------------------+-----------------------+
| Use Airflow Dockerfile sources to build the image  | No                  | Yes                   |
+----------------------------------------------------+---------------------+-----------------------+
| Requires Airflow sources                           | No                  | Yes                   |
+----------------------------------------------------+---------------------+-----------------------+
| You can build it with Breeze                       | No                  | Yes                   |
+----------------------------------------------------+---------------------+-----------------------+
| Allows to use non-default sources for dependencies | Yes                 | No [1]                |
+----------------------------------------------------+---------------------+-----------------------+

[1] When you combine customizing and extending the image, you can use external sources
    in the "extend" part. There are plans to add functionality to add external sources
    option to image customization. You can also modify Dockerfile manually if you want to
    use non-default sources for dependencies.

Using the production image
--------------------------

The PROD image entrypoint works as follows:

* In case the user is not "airflow" (with undefined user id) and the group id of the user is set to 0 (root),
  then the user is dynamically added to /etc/passwd at entry using USER_NAME variable to define the user name.
  This is in order to accommodate the
  `OpenShift Guidelines <https://docs.openshift.com/enterprise/3.0/creating_images/guidelines.html>`_

* The ``AIRFLOW_HOME`` is set by default to ``/opt/airflow/`` - this means that DAGs
  are in default in the ``/opt/airflow/dags`` folder and logs are in the ``/opt/airflow/logs``

* The working directory is ``/opt/airflow`` by default.

* If ``AIRFLOW__CORE__SQL_ALCHEMY_CONN`` variable is passed to the container and it is either mysql or postgres
  SQL alchemy connection, then the connection is checked and the script waits until the database is reachable.
  If ``AIRFLOW__CORE__SQL_ALCHEMY_CONN_CMD`` variable is passed to the container, it is evaluated as a
  command to execute and result of this evaluation is used as ``AIRFLOW__CORE__SQL_ALCHEMY_CONN``. The
  ``_CMD`` variable takes precedence over the ``AIRFLOW__CORE__SQL_ALCHEMY_CONN`` variable.

* If no ``AIRFLOW__CORE__SQL_ALCHEMY_CONN`` variable is set then SQLite database is created in
  ${AIRFLOW_HOME}/airflow.db and db reset is executed.

* If first argument equals to "bash" - you are dropped to a bash shell or you can executes bash command
  if you specify extra arguments. For example:

.. code-block:: bash

  docker run -it apache/airflow:master-python3.6 bash -c "ls -la"
  total 16
  drwxr-xr-x 4 airflow root 4096 Jun  5 18:12 .
  drwxr-xr-x 1 root    root 4096 Jun  5 18:12 ..
  drwxr-xr-x 2 airflow root 4096 Jun  5 18:12 dags
  drwxr-xr-x 2 airflow root 4096 Jun  5 18:12 logs

* If first argument is equal to "python" - you are dropped in python shell or python commands are executed if
  you pass extra parameters. For example:

.. code-block:: bash

  > docker run -it apache/airflow:master-python3.6 python -c "print('test')"
  test

* If first argument equals to "airflow" - the rest of the arguments is treated as an airflow command
  to execute. Example:

.. code-block:: bash

   docker run -it apache/airflow:master-python3.6 airflow webserver

* If there are any other arguments - they are simply passed to the "airflow" command

.. code-block:: bash

  > docker run -it apache/airflow:master-python3.6 version
  2.0.0.dev0

* If ``AIRFLOW__CELERY__BROKER_URL`` variable is passed and airflow command with
  scheduler, worker of flower command is used, then the script checks the broker connection
  and waits until the Celery broker database is reachable.
  If ``AIRFLOW__CELERY__BROKER_URL_CMD`` variable is passed to the container, it is evaluated as a
  command to execute and result of this evaluation is used as ``AIRFLOW__CELERY__BROKER_URL``. The
  ``_CMD`` variable takes precedence over the ``AIRFLOW__CELERY__BROKER_URL`` variable.

Production image build arguments
--------------------------------

The following build arguments (``--build-arg`` in docker build command) can be used for production images:

+------------------------------------------+------------------------------------------+------------------------------------------+
| Build argument                           | Default value                            | Description                              |
+==========================================+==========================================+==========================================+
| ``PYTHON_BASE_IMAGE``                    | ``python:3.6-slim-buster``               | Base python image.                       |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``PYTHON_MAJOR_MINOR_VERSION``           | ``3.6``                                  | major/minor version of Python (should    |
|                                          |                                          | match base image).                       |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_VERSION``                      | ``2.0.0.dev0``                           | version of Airflow.                      |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_REPO``                         | ``apache/airflow``                       | the repository from which PIP            |
|                                          |                                          | dependencies are pre-installed.          |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_BRANCH``                       | ``master``                               | the branch from which PIP dependencies   |
|                                          |                                          | are pre-installed initially.             |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_CONSTRAINTS_LOCATION``         |                                          | If not empty, it will override the       |
|                                          |                                          | source of the constraints with the       |
|                                          |                                          | specified URL or file. Note that the     |
|                                          |                                          | file has to be in docker context so      |
|                                          |                                          | it's best to place such file in          |
|                                          |                                          | one of the folders included in           |
|                                          |                                          | .dockerignore.                           |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_CONSTRAINTS_REFERENCE``        | ``constraints-master``                   | Reference (branch or tag) from GitHub    |
|                                          |                                          | where constraints file is taken from     |
|                                          |                                          | It can be ``constraints-master`` but     |
|                                          |                                          | also can be ``constraints-1-10`` for     |
|                                          |                                          | 1.10.* installation. In case of building |
|                                          |                                          | specific version you want to point it    |
|                                          |                                          | to specific tag, for example             |
|                                          |                                          | ``constraints-1.10.14``.                 |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``INSTALL_PROVIDERS_FROM_SOURCES``       | ``false``                                | If set to ``true`` and image is built    |
|                                          |                                          | from sources, all provider packages are  |
|                                          |                                          | installed from sources rather than from  |
|                                          |                                          | packages. It has no effect when          |
|                                          |                                          | installing from PyPI or GitHub repo.     |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_EXTRAS``                       | (see Dockerfile)                         | Default extras with which airflow is     |
|                                          |                                          | installed.                               |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``INSTALL_FROM_PYPI``                    | ``true``                                 | If set to true, Airflow is installed     |
|                                          |                                          | from PyPI. if you want to install        |
|                                          |                                          | Airflow from self-build package          |
|                                          |                                          | you can set it to false, put package in  |
|                                          |                                          | ``docker-context-files`` and set         |
|                                          |                                          | ``INSTALL_FROM_DOCKER_CONTEXT_FILES`` to |
|                                          |                                          | ``true``. For this you have to also keep |
|                                          |                                          | ``AIRFLOW_PRE_CACHED_PIP_PACKAGES`` flag |
|                                          |                                          | set to ``false``.                        |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_PRE_CACHED_PIP_PACKAGES``      | ``false``                                | Allows to pre-cache airflow PIP packages |
|                                          |                                          | from the GitHub of Apache Airflow        |
|                                          |                                          | This allows to optimize iterations for   |
|                                          |                                          | Image builds and speeds up CI builds.    |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``INSTALL_FROM_DOCKER_CONTEXT_FILES``    | ``false``                                | If set to true, Airflow, providers and   |
|                                          |                                          | all dependencies are installed from      |
|                                          |                                          | from locally built/downloaded            |
|                                          |                                          | .whl and .tar.gz files placed in the     |
|                                          |                                          | ``docker-context-files``. In certain     |
|                                          |                                          | corporate environments, this is required |
|                                          |                                          | to install airflow from such pre-vetted  |
|                                          |                                          | packages rather than from PyPI. For this |
|                                          |                                          | to work, also set ``INSTALL_FROM_PYPI``  |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``UPGRADE_TO_NEWER_DEPENDENCIES``        | ``false``                                | If set to true, the dependencies are     |
|                                          |                                          | upgraded to newer versions matching      |
|                                          |                                          | setup.py before installation.            |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_AIRFLOW_EXTRAS``            |                                          | Optional additional extras with which    |
|                                          |                                          | airflow is installed.                    |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_PYTHON_DEPS``               |                                          | Optional python packages to extend       |
|                                          |                                          | the image with some extra dependencies.  |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``DEV_APT_COMMAND``                      | (see Dockerfile)                         | Dev apt command executed before dev deps |
|                                          |                                          | are installed in the Build image.        |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_DEV_APT_COMMAND``           |                                          | Additional Dev apt command executed      |
|                                          |                                          | before dev dep are installed             |
|                                          |                                          | in the Build image. Should start with    |
|                                          |                                          | ``&&``.                                  |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``DEV_APT_DEPS``                         | (see Dockerfile)                         | Dev APT dependencies installed           |
|                                          |                                          | in the Build image.                      |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_DEV_APT_DEPS``              |                                          | Additional apt dev dependencies          |
|                                          |                                          | installed in the Build image.            |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_DEV_APT_ENV``               |                                          | Additional env variables defined         |
|                                          |                                          | when installing dev deps.                |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``RUNTIME_APT_COMMAND``                  | (see Dockerfile)                         | Runtime apt command executed before deps |
|                                          |                                          | are installed in the Main image.         |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_RUNTIME_APT_COMMAND``       |                                          | Additional Runtime apt command executed  |
|                                          |                                          | before runtime dep are installed         |
|                                          |                                          | in the Main image. Should start with     |
|                                          |                                          | ``&&``.                                  |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``RUNTIME_APT_DEPS``                     | (see Dockerfile)                         | Runtime APT dependencies installed       |
|                                          |                                          | in the Main image.                       |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_RUNTIME_APT_DEPS``          |                                          | Additional apt runtime dependencies      |
|                                          |                                          | installed in the Main image.             |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_RUNTIME_APT_ENV``           |                                          | Additional env variables defined         |
|                                          |                                          | when installing runtime deps.            |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_HOME``                         | ``/opt/airflow``                         | Airflow’s HOME (that’s where logs and    |
|                                          |                                          | sqlite databases are stored).            |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_UID``                          | ``50000``                                | Airflow user UID.                        |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_GID``                          | ``50000``                                | Airflow group GID. Note that most files  |
|                                          |                                          | created on behalf of airflow user belong |
|                                          |                                          | to the ``root`` group (0) to keep        |
|                                          |                                          | OpenShift Guidelines compatibility.      |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_USER_HOME_DIR``                | ``/home/airflow``                        | Home directory of the Airflow user.      |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``CASS_DRIVER_BUILD_CONCURRENCY``        | ``8``                                    | Number of processors to use for          |
|                                          |                                          | cassandra PIP install (speeds up         |
|                                          |                                          | installing in case cassandra extra is    |
|                                          |                                          | used).                                   |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``INSTALL_MYSQL_CLIENT``                 | ``true``                                 | Whether MySQL client should be installed |
|                                          |                                          | The mysql extra is removed from extras   |
|                                          |                                          | if the client is not installed.          |
+------------------------------------------+------------------------------------------+------------------------------------------+

There are build arguments that determine the installation mechanism of Apache Airflow for the
production image. There are three types of build:

* From local sources (by default for example when you use ``docker build .``)
* You can build the image from released PyPi airflow package (used to build the official Docker image)
* You can build the image from any version in GitHub repository(this is used mostly for system testing).

+-----------------------------------+------------------------+-----------------------------------------------------------------------------------+
| Build argument                    | Default                | What to specify                                                                   |
+===================================+========================+===================================================================================+
| ``AIRFLOW_INSTALLATION_METHOD``   | ``apache-airflow``     | Should point to the installation method of Apache Airflow. It can be              |
|                                   |                        | ``apache-airflow`` for installation from packages and URL to installation from    |
|                                   |                        | GitHub repository tag or branch or "." to install from sources.                   |
|                                   |                        | Note that installing from local sources requires appropriate values of the        |
|                                   |                        | ``AIRFLOW_SOURCES_FROM`` and ``AIRFLOW_SOURCES_TO`` variables as described below. |
|                                   |                        | Only used when ``INSTALL_FROM_PYPI`` is set to ``true``.                          |
+-----------------------------------+------------------------+-----------------------------------------------------------------------------------+
| ``AIRFLOW_INSTALL_VERSION``       |                        | Optional - might be used for package installation of different Airflow version    |
|                                   |                        | for example"==2.0.0". For consistency, you should also set``AIRFLOW_VERSION``   |
|                                   |                        | to the same value AIRFLOW_VERSION is embedded as label in the image created.      |
+-----------------------------------+------------------------+-----------------------------------------------------------------------------------+
| ``AIRFLOW_CONSTRAINTS_REFERENCE`` | ``constraints-master`` | Reference (branch or tag) from GitHub where constraints file is taken from.       |
|                                   |                        | It can be ``constraints-master`` but also can be``constraints-1-10`` for          |
|                                   |                        | 1.10.*  installations. In case of building specific version                       |
|                                   |                        | you want to point it to specific tag, for example ``constraints-2.0.0``         |
+-----------------------------------+------------------------+-----------------------------------------------------------------------------------+
| ``AIRFLOW_WWW``                   | ``www``                | In case of Airflow 2.0 it should be "www", in case of Airflow 1.10                |
|                                   |                        | series it should be "www_rbac".                                                   |
+-----------------------------------+------------------------+-----------------------------------------------------------------------------------+
| ``AIRFLOW_SOURCES_FROM``          | ``empty``              | Sources of Airflow. Set it to "." when you install airflow from                   |
|                                   |                        | local sources.                                                                    |
+-----------------------------------+------------------------+-----------------------------------------------------------------------------------+
| ``AIRFLOW_SOURCES_TO``            | ``/empty``             | Target for Airflow sources. Set to "/opt/airflow" when                            |
|                                   |                        | you want to install airflow from local sources.                                   |
+-----------------------------------+------------------------+-----------------------------------------------------------------------------------+

This builds production image in version 3.6 with default extras from the local sources (master version
of 2.0 currently):

.. code-block:: bash

  docker build .

This builds the production image in version 3.7 with default extras from 2.0.0 tag and
constraints taken from constraints-2-0 branch in GitHub.

.. code-block:: bash

  docker build . \
    --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" \
    --build-arg PYTHON_MAJOR_MINOR_VERSION=3.7 \
    --build-arg AIRFLOW_INSTALLATION_METHOD="https://github.com/apache/airflow/archive/2.0.0.tar.gz#egg=apache-airflow" \
    --build-arg AIRFLOW_CONSTRAINTS_REFERENCE="constraints-2-0" \
    --build-arg AIRFLOW_BRANCH="v1-10-test" \
    --build-arg AIRFLOW_SOURCES_FROM="empty" \
    --build-arg AIRFLOW_SOURCES_TO="/empty"

This builds the production image in version 3.7 with default extras from 2.0.0 PyPI package and
constraints taken from 2.0.0 tag in GitHub and pre-installed pip dependencies from the top
of v1-10-test branch.

.. code-block:: bash

  docker build . \
    --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" \
    --build-arg PYTHON_MAJOR_MINOR_VERSION=3.7 \
    --build-arg AIRFLOW_INSTALLATION_METHOD="apache-airflow" \
    --build-arg AIRFLOW_VERSION="2.0.0" \
    --build-arg AIRFLOW_INSTALL_VERSION="==2.0.0" \
    --build-arg AIRFLOW_BRANCH="v1-10-test" \
    --build-arg AIRFLOW_CONSTRAINTS_REFERENCE="constraints-2.0.0" \
    --build-arg AIRFLOW_SOURCES_FROM="empty" \
    --build-arg AIRFLOW_SOURCES_TO="/empty"

This builds the production image in version 3.7 with additional airflow extras from 2.0.0 PyPI package and
additional python dependencies and pre-installed pip dependencies from 2.0.0 tagged constraints.

.. code-block:: bash

  docker build . \
    --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" \
    --build-arg PYTHON_MAJOR_MINOR_VERSION=3.7 \
    --build-arg AIRFLOW_INSTALLATION_METHOD="apache-airflow" \
    --build-arg AIRFLOW_VERSION="2.0.0" \
    --build-arg AIRFLOW_INSTALL_VERSION="==2.0.0" \
    --build-arg AIRFLOW_BRANCH="v1-10-test" \
    --build-arg AIRFLOW_CONSTRAINTS_REFERENCE="constraints-2.0.0" \
    --build-arg AIRFLOW_SOURCES_FROM="empty" \
    --build-arg AIRFLOW_SOURCES_TO="/empty" \
    --build-arg ADDITIONAL_AIRFLOW_EXTRAS="mssql,hdfs" \
    --build-arg ADDITIONAL_PYTHON_DEPS="sshtunnel oauth2client"

This builds the production image in version 3.7 with additional airflow extras from 2.0.0 PyPI package and
additional apt dev and runtime dependencies.

.. code-block:: bash

  docker build . \
    --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" \
    --build-arg PYTHON_MAJOR_MINOR_VERSION=3.7 \
    --build-arg AIRFLOW_INSTALLATION_METHOD="apache-airflow" \
    --build-arg AIRFLOW_VERSION="2.0.0" \
    --build-arg AIRFLOW_INSTALL_VERSION="==2.0.0" \
    --build-arg AIRFLOW_CONSTRAINTS_REFERENCE="constraints-2-0" \
    --build-arg AIRFLOW_SOURCES_FROM="empty" \
    --build-arg AIRFLOW_SOURCES_TO="/empty" \
    --build-arg ADDITIONAL_AIRFLOW_EXTRAS="jdbc" \
    --build-arg ADDITIONAL_DEV_APT_DEPS="gcc g++" \
    --build-arg ADDITIONAL_RUNTIME_APT_DEPS="default-jre-headless"



More details about the images
-----------------------------

You can read more details about the images - the context, their parameters and internal structure in the
`IMAGES.rst <https://github.com/apache/airflow/blob/master/IMAGES.rst>`_ document.

.. _production-deployment:kerberos:

Kerberos-authenticated workers
==============================

Apache Airflow has a built-in mechanism for authenticating the operation with a KDC (Key Distribution Center).
Airflow has a separate command ``airflow kerberos`` that acts as token refresher. It uses the pre-configured
Kerberos Keytab to authenticate in the KDC to obtain a valid token, and then refreshing valid token
at regular intervals within the current token expiry window.

Each request for refresh uses a configured principal, and only keytab valid for the principal specified
is capable of retrieving the authentication token.

The best practice to implement proper security mechanism in this case is to make sure that worker
workloads have no access to the Keytab but only have access to the periodically refreshed, temporary
authentication tokens. This can be achieved in docker environment by running the ``airflow kerberos``
command and the worker command in separate containers - where only the ``airflow kerberos`` token has
access to the Keytab file (preferably configured as secret resource). Those two containers should share
a volume where the temporary token should be written by the ``airflow kerberos`` and read by the workers.

In the Kubernetes environment, this can be realized by the concept of side-car, where both Kerberos
token refresher and worker are part of the same Pod. Only the Kerberos side-car has access to
Keytab secret and both containers in the same Pod share the volume, where temporary token is written by
the side-care container and read by the worker container.

This concept is implemented in the development version of the Helm Chart that is part of Airflow source code.


.. spelling::

   pypirc
   dockerignore


Secured Server and Service Access on Google Cloud
=================================================

This section describes techniques and solutions for securely accessing servers and services when your Airflow environment is deployed on Google Cloud, or you connect to Google services, or you are connecting to the Google API.

IAM and Service Accounts
------------------------

You should do not rely on internal network segmentation or firewalling as our primary security mechanisms. To protect your organization's data, every request you make should contain sender identity. In the case of Google Cloud, the identity is provided by `the IAM and Service account <https://cloud.google.com/iam/docs/service-accounts>`__. Each Compute Engine instance has an associated service account identity. It provides cryptographic credentials that your workload can use to prove its identity when making calls to Google APIs or third-party services. Each instance has access only to short-lived credentials. If you use Google-managed service account keys, then the private key is always held in escrow and is never directly accessible.

If you are using Kubernetes Engine, you can use `Workload Identity <https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity>`__ to assign an identity to individual pods.

For more information about service accounts in the Airflow, see :ref:`howto/connection:gcp`

Impersonate Service Accounts
----------------------------

If you need access to other service accounts, you can :ref:`impersonate other service accounts <howto/connection:gcp:impersonation>` to exchange the token with the default identity to another service account. Thus, the account keys are still managed by Google and cannot be read by your workload.

It is not recommended to generate service account keys and store them in the metadata database or the secrets backend. Even with the use of the backend secret, the service account key is available for your workload.

Access to Compute Engine Instance
---------------------------------

If you want to establish an SSH connection to the Compute Engine instance, you must have the network address of this instance and credentials to access it. To simplify this task, you can use :class:`~airflow.providers.google.cloud.hooks.compute.ComputeEngineHook` instead of :class:`~airflow.providers.ssh.hooks.ssh.SSHHook`

The :class:`~airflow.providers.google.cloud.hooks.compute.ComputeEngineHook` support authorization with Google OS Login service. It is an extremely robust way to manage Linux access properly as it stores short-lived ssh keys in the metadata service, offers PAM modules for access and sudo privilege checking and offers nsswitch user lookup into the metadata service as well.

It also solves the discovery problem that arises as your infrastructure grows. You can use the instance name instead of the network address.

Access to Amazon Web Service
----------------------------

Thanks to `Web Identity Federation <https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_providers_oidc.html>`__, you can exchange the Google Cloud Platform identity to the Amazon Web Service identity, which effectively means access to Amazon Web Service platform. For more information, see: :ref:`howto/connection:aws:gcp-federation`

.. spelling::

    nsswitch
    cryptographic
    firewalling
    ComputeEngineHook
