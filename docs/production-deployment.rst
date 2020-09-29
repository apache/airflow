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

This document describes various aspects of the production deployments of Apache Airflow.

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

  FROM apache/airflow:1.10.12
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

  FROM apache/airflow:1.10.12
  RUN pip install --no-cache-dir --user my-awesome-pip-dependency-to-add


* If your apt, or PyPI dependencies require some of the build-essentials, then your best choice is
  to follow the "Customize the image" route. However it requires to checkout sources of Apache Airflow,
  so you might still want to choose to add build essentials to your image, even if your image will
  be significantly bigger.

.. code-block:: dockerfile

  FROM apache/airflow:1.10.12
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
released versions, or checked out from the Github project if you happen to do it from git sources.

The easiest way to build the image image is to use ``breeze`` script, but you can also build such customized
image by running appropriately crafted docker build in which you specify all the ``build-args``
that you need to add to customize it. You can read about all the args and ways you can build the image
in the `<#production-image-build-arguments>`_ chapter below.

Here just a few examples are presented which should give you general understanding of what you can customize.

This builds the production image in version 3.7 with additional airflow extras from 1.10.10 Pypi package and
additional apt dev and runtime dependencies.

.. code-block:: bash

  docker build . \
    --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" \
    --build-arg PYTHON_MAJOR_MINOR_VERSION=3.7 \
    --build-arg AIRFLOW_INSTALL_SOURCES="apache-airflow" \
    --build-arg AIRFLOW_INSTALL_VERSION="==1.10.12" \
    --build-arg AIRFLOW_CONSTRAINTS_REFERENCE="constraints-1-10" \
    --build-arg AIRFLOW_SOURCES_FROM="empty" \
    --build-arg AIRFLOW_SOURCES_TO="/empty" \
    --build-arg ADDITIONAL_AIRFLOW_EXTRAS="jdbc"
    --build-arg ADDITIONAL_PYTHON_DEPS="pandas"
    --build-arg ADDITIONAL_DEV_DEPS="gcc g++"
    --build-arg ADDITIONAL_RUNTIME_DEPS="default-jre-headless"
    --tag my-image


the same image can be built using ``breeze`` (it supports auto-completion of the options):

.. code-block:: bash

  ./breeze build-image \
      --production-image  --python 3.7 --install-airflow-version=1.10.12 \
      --additional-extras=jdbc --additional-python-deps="pandas" \
      --additional-dev-deps="gcc g++" --additional-runtime-deps="default-jre-headless"

Customizing & extending the image together
..........................................

You can combine both - customizing & extending the image. You can build the image first using
``customize`` method (either with docker command or with ``breeze`` and then you can ``extend``
the resulting image using ``FROM`` any dependencies you want.

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

* If ``AIRFLOW__CORE__SQL_ALCHEMY_CONN`` variable is passed to the container and it is either mysql or postgres
  SQL alchemy connection, then the connection is checked and the script waits until the database is reachable.

* If no ``AIRFLOW__CORE__SQL_ALCHEMY_CONN`` variable is set or if it is set to sqlite SQL alchemy connection
  then db reset is executed.

* If ``AIRFLOW__CELERY__BROKER_URL`` variable is passed and scheduler, worker of flower command is used then
  the connection is checked and the script waits until the Celery broker database is reachable.

* The ``AIRFLOW_HOME`` is set by default to ``/opt/airflow/`` - this means that DAGs
  are in default in the ``/opt/airflow/dags`` folder and logs are in the ``/opt/airflow/logs``

* The working directory is ``/opt/airflow`` by default.

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

* If there are any other arguments - they are passed to "airflow" command

.. code-block:: bash

  > docker run -it apache/airflow:master-python3.6
  2.0.0.dev0

Production image build arguments
--------------------------------

The following build arguments (``--build-arg`` in docker build command) can be used for production images:

+------------------------------------------+------------------------------------------+------------------------------------------+
| Build argument                           | Default value                            | Description                              |
+==========================================+==========================================+==========================================+
| ``PYTHON_BASE_IMAGE``                    | ``python:3.6-slim-buster``               | Base python image                        |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``PYTHON_MAJOR_MINOR_VERSION``           | ``3.6``                                  | major/minor version of Python (should    |
|                                          |                                          | match base image)                        |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_VERSION``                      | ``2.0.0.dev0``                           | version of Airflow                       |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_REPO``                         | ``apache/airflow``                       | the repository from which PIP            |
|                                          |                                          | dependencies are pre-installed           |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_BRANCH``                       | ``master``                               | the branch from which PIP dependencies   |
|                                          |                                          | are pre-installed initially              |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_CONSTRAINTS_REFERENCE``        | ``constraints-master``                   | reference (branch or tag) from GitHub    |
|                                          |                                          | repository from which constraints are    |
|                                          |                                          | used. By default it is set to            |
|                                          |                                          | ``constraints-master`` but can be        |
|                                          |                                          | ``constraints-1-10`` for 1.10.* versions |
|                                          |                                          | or it could point to specific version    |
|                                          |                                          | for example ``constraints-1.10.12``      |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_EXTRAS``                       | (see Dockerfile)                         | Default extras with which airflow is     |
|                                          |                                          | installed                                |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_PRE_CACHED_PIP_PACKAGES``      | ``true``                                 | Allows to pre-cache airflow PIP packages |
|                                          |                                          | from the GitHub of Apache Airflow        |
|                                          |                                          | This allows to optimize iterations for   |
|                                          |                                          | Image builds and speeds up CI builds     |
|                                          |                                          | But in some corporate environments it    |
|                                          |                                          | might be forbidden to download anything  |
|                                          |                                          | from public repositories.                |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_AIRFLOW_EXTRAS``            |                                          | Optional additional extras with which    |
|                                          |                                          | airflow is installed                     |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_PYTHON_DEPS``               |                                          | Optional python packages to extend       |
|                                          |                                          | the image with some extra dependencies   |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_DEV_DEPS``                  |                                          | additional apt dev dependencies to       |
|                                          |                                          | install                                  |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_RUNTIME_DEPS``              |                                          | additional apt runtime dependencies to   |
|                                          |                                          | install                                  |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_HOME``                         | ``/opt/airflow``                         | Airflow’s HOME (that’s where logs and    |
|                                          |                                          | sqlite databases are stored)             |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_UID``                          | ``50000``                                | Airflow user UID                         |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_GID``                          | ``50000``                                | Airflow group GID. Note that most files  |
|                                          |                                          | created on behalf of airflow user belong |
|                                          |                                          | to the ``root`` group (0) to keep        |
|                                          |                                          | OpenShift Guidelines compatibility       |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_USER_HOME_DIR``                | ``/home/airflow``                        | Home directory of the Airflow user       |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``CASS_DRIVER_BUILD_CONCURRENCY``        | ``8``                                    | Number of processors to use for          |
|                                          |                                          | cassandra PIP install (speeds up         |
|                                          |                                          | installing in case cassandra extra is    |
|                                          |                                          | used).                                   |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``INSTALL_MYSQL_CLIENT``                 | ``true``                                 | Whether MySQL client should be installed |
|                                          |                                          | The mysql extra is removed from extras   |
|                                          |                                          | if the client is not installed           |
+------------------------------------------+------------------------------------------+------------------------------------------+

There are build arguments that determine the installation mechanism of Apache Airflow for the
production image. There are three types of build:

* From local sources (by default for example when you use ``docker build .``)
* You can build the image from released PyPi airflow package (used to build the official Docker image)
* You can build the image from any version in GitHub repository(this is used mostly for system testing).

+-----------------------------------+-----------------------------------+
| Build argument                    | What to specify                   |
+===================================+===================================+
| ``AIRFLOW_INSTALL_SOURCES``       | Should point to the sources of    |
|                                   | of Apache Airflow. It can be      |
|                                   | either "." for installation from  |
|                                   | local sources, "apache-airflow"   |
|                                   | for installation from packages    |
|                                   | and URL to installation from      |
|                                   | GitHub repository (see below)     |
|                                   | to install from any GitHub        |
|                                   | version                           |
+-----------------------------------+-----------------------------------+
| ``AIRFLOW_INSTALL_VERSION``       | Optional - might be used for      |
|                                   | package installation case to      |
|                                   | set Airflow version for example   |
|                                   | "==1.10.12"                       |
+-----------------------------------+-----------------------------------+
| ``AIRFLOW_CONSTRAINTS_REFERENCE`` | reference (branch or tag) from    |
|                                   | GitHub where constraints file     |
|                                   | is taken from. By default it is   |
|                                   | ``constraints-master`` but can be |
|                                   | ``constraints-1-10`` for 1.10.*   |
|                                   | constraint or if you want to      |
|                                   | point to specific version         |
|                                   | might be ``constraints-1.10.12``  |
+-----------------------------------+-----------------------------------+
| ``SLUGIFY_USES_TEXT_UNIDECODE``   | In case of of installing airflow  |
|                                   | 1.10.2 or 1.10.1 you need to      |
|                                   | set this arg to ``yes``.          |
+-----------------------------------+-----------------------------------+
| ``AIRFLOW_WWW``                   | In case of Airflow 2.0 it should  |
|                                   | be "www", in case of Airflow 1.10 |
|                                   | series it should be "www_rbac".   |
|                                   | See examples below                |
+-----------------------------------+-----------------------------------+
| ``AIRFLOW_SOURCES_FROM``          | Sources of Airflow. Set it to     |
|                                   | "empty" to avoid costly           |
|                                   | Docker context copying            |
|                                   | in case of installation from      |
|                                   | the package or from GitHub URL.   |
|                                   | See examples below                |
+-----------------------------------+-----------------------------------+
| ``AIRFLOW_SOURCES_TO``            | Target for Airflow sources. Set   |
|                                   | to "/empty" to avoid costly       |
|                                   | Docker context copying            |
|                                   | in case of installation from      |
|                                   | the package or from GitHub URL.   |
|                                   | See examples below                |
+-----------------------------------+-----------------------------------+


This builds production image in version 3.6 with default extras from the local sources (master version
of 2.0 currently):

.. code-block:: bash

  docker build .

This builds the production image in version 3.7 with default extras from 1.10.12 tag and
constraints taken from constraints-1-10-12 branch in GitHub.

.. code-block:: bash

  docker build . \
    --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" \
    --build-arg PYTHON_MAJOR_MINOR_VERSION=3.7 \
    --build-arg AIRFLOW_INSTALL_SOURCES="https://github.com/apache/airflow/archive/1.10.12.tar.gz#egg=apache-airflow" \
    --build-arg AIRFLOW_CONSTRAINTS_REFERENCE="constraints-1-10" \
    --build-arg AIRFLOW_BRANCH="v1-10-test" \
    --build-arg AIRFLOW_SOURCES_FROM="empty" \
    --build-arg AIRFLOW_SOURCES_TO="/empty"

This builds the production image in version 3.7 with default extras from 1.10.12 Pypi package and
constraints taken from 1.10.12 tag in GitHub and pre-installed pip dependencies from the top
of v1-10-test branch.

.. code-block:: bash

  docker build . \
    --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" \
    --build-arg PYTHON_MAJOR_MINOR_VERSION=3.7 \
    --build-arg AIRFLOW_INSTALL_SOURCES="apache-airflow" \
    --build-arg AIRFLOW_INSTALL_VERSION="==1.10.12" \
    --build-arg AIRFLOW_BRANCH="v1-10-test" \
    --build-arg AIRFLOW_CONSTRAINTS_REFERENCE="constraints-1.10.12" \
    --build-arg AIRFLOW_SOURCES_FROM="empty" \
    --build-arg AIRFLOW_SOURCES_TO="/empty"

This builds the production image in version 3.7 with additional airflow extras from 1.10.12 Pypi package and
additional python dependencies and pre-installed pip dependencies from 1.10.12 tagged constraints.

.. code-block:: bash

  docker build . \
    --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" \
    --build-arg PYTHON_MAJOR_MINOR_VERSION=3.7 \
    --build-arg AIRFLOW_INSTALL_SOURCES="apache-airflow" \
    --build-arg AIRFLOW_INSTALL_VERSION="==1.10.12" \
    --build-arg AIRFLOW_BRANCH="v1-10-test" \
    --build-arg AIRFLOW_CONSTRAINTS_REFERENCE="constraints-1.10.12" \
    --build-arg AIRFLOW_SOURCES_FROM="empty" \
    --build-arg AIRFLOW_SOURCES_TO="/empty" \
    --build-arg ADDITIONAL_AIRFLOW_EXTRAS="mssql,hdfs"
    --build-arg ADDITIONAL_PYTHON_DEPS="sshtunnel oauth2client"

This builds the production image in version 3.7 with additional airflow extras from 1.10.12 Pypi package and
additional apt dev and runtime dependencies.

.. code-block:: bash

  docker build . \
    --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" \
    --build-arg PYTHON_MAJOR_MINOR_VERSION=3.7 \
    --build-arg AIRFLOW_INSTALL_SOURCES="apache-airflow" \
    --build-arg AIRFLOW_INSTALL_VERSION="==1.10.12" \
    --build-arg AIRFLOW_CONSTRAINTS_REFERENCE="constraints-1-10" \
    --build-arg AIRFLOW_SOURCES_FROM="empty" \
    --build-arg AIRFLOW_SOURCES_TO="/empty" \
    --build-arg ADDITIONAL_AIRFLOW_EXTRAS="jdbc"
    --build-arg ADDITIONAL_DEV_DEPS="gcc g++"
    --build-arg ADDITIONAL_RUNTIME_DEPS="default-jre-headless"


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
