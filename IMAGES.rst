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

.. contents:: :local:

Airflow docker images
=====================

Airflow has two images (build from Dockerfiles):

* CI image (Dockerfile.ci) - used for running tests and local development
* Production image (Dockerfile) - used to run production-ready Airflow installations

Image naming conventions
========================

The images are named as follows:

``apache/airflow:<BRANCH_OR_TAG>-python<PYTHON_MAJOR_MINOR_VERSION>[-ci][-manifest]``

where:

* ``BRANCH_OR_TAG`` - branch or tag used when creating the image. Examples: ``master``, ``v1-10-test``, ``1.10.10``
  The ``master`` and ``v1-10-test`` labels are built from branches so they change over time. The ``1.10.*`` and in
  the future ``2.*`` labels are build from git tags and they are "fixed" once built.
* ``PYTHON_MAJOR_MINOR_VERSION`` - version of python used to build the image. Examples: ``3.5``, ``3.7``
* The ``-ci`` suffix is added for CI images
* The ``-manifest`` is added for manifest images (see below for explanation of manifest images)

Building docker images
======================

The easiest way to build those images is to use `<BREEZE.rst>`_.

Note! Breeze by default builds production image from local sources. You can change it's behaviour by
providing ``--install-airflow-version`` parameter, where you can specify the
tag/branch used to download Airflow package from in github repository. You can
also change the repository itself by adding ``--dockerhub-user`` and ``--dockerhub-repo`` flag values.

You can build the CI image using this command:

.. code-block::

  ./breeze build-image

You can build production image using this command:

.. code-block::

  ./breeze build-image --production-image

By adding ``--python <PYTHON_MAJOR_MINOR_VERSION>`` parameter you can build the
image version for the chosen python version.

The images are build with default extras - different extras for CI and production image and you
can change the extras via the ``--extras`` parameters. You can see default extras used via
``./breeze flags``.

For example if you want to build python 3.7 version of production image with
"all" extras installed you should run this command:

.. code-block::

  ./breeze build-image --python 3.7 --extras "all" --production-image

The command that builds the CI image is optimized to minimize the time needed to rebuild the image when
the source code of Airflow evolves. This means that if you already have the image locally downloaded and
built, the scripts will determine whether the rebuild is needed in the first place. Then the scripts will
make sure that minimal number of steps are executed to rebuild parts of the image (for example,
PIP dependencies) and will give you an image consistent with the one used during Continuous Integration.

The command that builds the production image is optimised for size of the image.

In Breeze by default, the airflow is installed using local sources of Apache Airflow.

You can also build production images from PIP packages via providing ``--install-airflow-version``
parameter to Breeze:

.. code-block::

  ./breeze build-image --python 3.7 --extras=gcp --production-image --install-airflow-version=1.10.9

This will build the image using command similar to:

.. code-block::

    pip install apache-airflow[sendgrid]==1.10.9 \
       --constraint https://raw.githubusercontent.com/apache/airflow/v1-10-test/requirements/requirements-python3.7.txt

This will also download entrypoint script from https://raw.githubusercontent.com/apache/airflow/v1-10-test/entrypoint.sh
url. It is important so that we have matching version of the requirements.

The requirement files and entrypoint only appeared in version 1.10.10 of airflow so if you install
an earlier version -  both constraint and requirements should point to 1.10.10 version.

You can also build production images from specific Git version via providing ``--install-airflow-reference``
parameter to Breeze:

.. code-block::

    pip install https://github.com/apache/airflow/archive/<tag>.tar.gz#egg=apache-airflow \
       --constraint https://raw.githubusercontent.com/apache/airflow/<tag>/requirements/requirements-python3.7.txt

This will also Download entrypoint script from ``https://raw.githubusercontent.com/apache/airflow/<tag>/entrypoint.sh``
url.

Technical details of Airflow images
===================================

The CI image is used by Breeze as shell image but it is also used during CI build.
The image is single segment image that contains Airflow installation with "all" dependencies installed.
It is optimised for rebuild speed (``AIRFLOW_CONTAINER_CI_OPTIMISED_BUILD`` flag set to "true").
It installs PIP dependencies from the current branch first - so that any changes in setup.py do not trigger
reinstalling of all dependencies. There is a second step of installation that re-installs the dependencies
from the latest sources so that we are sure that latest dependencies are installed.

The production image is a multi-segment image. The first segment "airflow-build-image" contains all the
build essentials and related dependencies that allow to install airflow locally. By default the image is
build from a released version of Airflow from Github, but by providing some extra arguments you can also
build it from local sources. This is particularly useful in CI environment where we are using the image
to run Kubernetes tests. See below for the list of arguments that should be provided to build
production image from the local sources.

Manually building the images
----------------------------

You can build the default production image with standard ``docker build`` command but they will only build
default versions of the image and will not use the dockerhub versions of images as cache.


CI images
.........

The following build arguments (``--build-arg`` in docker build command) can be used for CI images:

+------------------------------------------+------------------------------------------+------------------------------------------+
| Build argument                           | Default value                            | Description                              |
+==========================================+==========================================+==========================================+
| ``PYTHON_BASE_IMAGE``                    | ``python:3.6-slim-buster``               | Base python image                        |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_VERSION``                      | ``2.0.0.dev0``                           | version of Airflow                       |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``PYTHON_MAJOR_MINOR_VERSION``           | ``3.6``                                  | major/minor version of Python (should    |
|                                          |                                          | match base image)                        |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``DEPENDENCIES_EPOCH_NUMBER``            | ``2``                                    | increasing this number will reinstall    |
|                                          |                                          | all apt dependencies                     |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``PIP_NO_CACHE_DIR``                     | ``true``                                 | if true, then no pip cache will be       |
|                                          |                                          | stored                                   |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``PIP_VERSION``                          | ``19.0.2``                               | version of PIP to use                    |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``HOME``                                 | ``/root``                                | Home directory of the root user (CI      |
|                                          |                                          | image has root user as default)          |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_HOME``                         | ``/root/airflow``                        | Airflow’s HOME (that’s where logs and    |
|                                          |                                          | sqlite databases are stored)             |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_SOURCES``                      | ``/opt/airflow``                         | Mounted sources of Airflow               |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``PIP_DEPENDENCIES_EPOCH_NUMBER``        | ``3``                                    | increasing that number will reinstall    |
|                                          |                                          | all PIP dependencies                     |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``CASS_DRIVER_NO_CYTHON``                | ``1``                                    | if set to 1 no CYTHON compilation is     |
|                                          |                                          | done for cassandra driver (much faster)  |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_CONTAINER_CI_OPTIMISED_BUILD`` | ``true``                                 | if set then PIP dependencies are         |
|                                          |                                          | installed from repo first before they    |
|                                          |                                          | are reinstalled from local sources. This |
|                                          |                                          | allows for incremental faster builds     |
|                                          |                                          | when requirements change                 |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_REPO``                         | ``apache/airflow``                       | the repository from which PIP            |
|                                          |                                          | dependencies are installed (CI           |
|                                          |                                          | optimised)                               |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_BRANCH``                       | ``master``                               | the branch from which PIP dependencies   |
|                                          |                                          | are installed (CI optimised)             |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_CI_BUILD_EPOCH``               | ``1``                                    | increasing this value will reinstall PIP |
|                                          |                                          | dependencies from the repository from    |
|                                          |                                          | scratch                                  |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_EXTRAS``                       | ``all``                                  | extras to install                        |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_AIRFLOW_EXTRAS``            | ````                                     | additional extras to install             |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_PYTHON_DEPS``               | \```\`                                   | additional python dependencies to        |
|                                          |                                          | install                                  |
+------------------------------------------+------------------------------------------+------------------------------------------+

Here are some examples of how CI images can built manually. CI is always built from local sources.

This builds the CI image in version 3.7 with default extras ("all").

.. code-block::

  docker build . -f Dockerfile.ci --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" \
    --build-arg PYTHON_MAJOR_MINOR_VERSION=3.7


This builds the CI image in version 3.6 with "gcp" extra only.

.. code-block::

  docker build . -f Dockerfile.ci --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" \
    --build-arg PYTHON_MAJOR_MINOR_VERSION=3.6 --build-arg AIRFLOW_EXTRAS=gcp


This builds the CI image in version 3.6 with "apache-beam" extra added.

.. code-block::

  docker build . -f Dockerfile.ci --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" \
    --build-arg PYTHON_MAJOR_MINOR_VERSION=3.6 --build-arg ADDITIONAL_AIRFLOW_EXTRAS="apache-beam"

This builds the CI image in version 3.6 with "mssql" additional package added.

.. code-block::

  docker build . -f Dockerfile.ci --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" \
    --build-arg PYTHON_MAJOR_MINOR_VERSION=3.6 --build-arg ADDITIONAL_PYTHON_DEPS="mssql"



Production images
.................

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
| ``AIRFLOW_ORG``                          | ``apache``                               | Github organisation from which Airflow   |
|                                          |                                          | is installed (when installed from repo)  |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_REPO``                         | ``airflow``                              | Github repository from which Airflow is  |
|                                          |                                          | installed (when installed from repo)     |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_GIT_REFERENCE``                | ``master``                               | reference (branch or tag) from Github    |
|                                          |                                          | repository from which Airflow is         |
|                                          |                                          | installed (when installed from repo)     |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``REQUIREMENTS_GIT_REFERENCE``           | ``master``                               | reference (branch or tag) from Github    |
|                                          |                                          | repository from which requirements are   |
|                                          |                                          | downloaded for constraints (when         |
|                                          |                                          | installed from repo).                    |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_EXTRAS``                       | (see Dockerfile)                         | Default extras with which airflow is     |
|                                          |                                          | installed                                |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_AIRFLOW_EXTRAS``            | ````                                     | Optional additional extras with which    |
|                                          |                                          | airflow is installed                     |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_PYTHON_DEPS``               | ````                                     | Optional python packages to extend       |
|                                          |                                          | the image with some extra dependencies   |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_HOME``                         | ``/opt/airflow``                         | Airflow’s HOME (that’s where logs and    |
|                                          |                                          | sqlite databases are stored)             |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_UID``                          | ``50000``                                | Airflow user UID                         |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_GID``                          | ``50000``                                | Airflow group GID                        |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``PIP_VERSION``                          | ``19.0.2``                               | version of PIP to use                    |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``CASS_DRIVER_BUILD_CONCURRENCY``        | ``8``                                    | Number of processors to use for          |
|                                          |                                          | cassandra PIP install (speeds up         |
|                                          |                                          | installing in case cassandra extra is    |
|                                          |                                          | used).                                   |
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
|                                   | "==1.10.10"                       |
+-----------------------------------+-----------------------------------+
| ``CONSTRAINT_REQUIREMENTS``       | Should point to requirements file |
|                                   | in case of installation from      |
|                                   | the package or from GitHub URL.   |
|                                   | See examples below                |
+-----------------------------------+-----------------------------------+
| ``ENTRYPOINT_FILE``               | Should point to entrypoint.sh     |
|                                   | file in case of installation from |
|                                   | the package or from GitHub URL.   |
|                                   | See examples below                |
+-----------------------------------+-----------------------------------+
| ``AIRFLOW_WWW``                   | In case of Airflow 2.0 it should  |
|                                   | be "www", in case of Airflow 1.10 |
|                                   | series it should be "www_rbac".   |
|                                   | See examples below                |
+-----------------------------------+-----------------------------------+
| ``AIRFLOW_SOURCES_FROM``          | Sources of Airflow. Set it to     |
|                                   | "entrypoint.sh" to avoid costly   |
|                                   | Docker context copying            |
|                                   | in case of installation from      |
|                                   | the package or from GitHub URL.   |
|                                   | See examples below                |
+-----------------------------------+-----------------------------------+
| ``AIRFLOW_SOURCES_TO``            | Target for Airflow sources. Set   |
|                                   | to "/entrypoint" to avoid costly  |
|                                   | Docker context copying            |
|                                   | in case of installation from      |
|                                   | the package or from GitHub URL.   |
|                                   | See examples below                |
+-----------------------------------+-----------------------------------+


This builds production image in version 3.6 with default extras from the local sources:

.. code-block::

  docker build .

This builds the production image in version 3.7 with default extras from 1.10.9 tag and
requirements taken from v1-10-test branch in Github.
Note that versions 1.10.9 and below have no requirements so requirements should be taken from head of
the 1.10.10 tag.

.. code-block::

  docker build . \
    --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" \
    --build-arg PYTHON_MAJOR_MINOR_VERSION=3.7 \
    --build-arg AIRFLOW_INSTALL_SOURCES="https://github.com/apache/airflow/archive/1.10.10.tar.gz#egg=apache-airflow" \
    --build-arg CONSTRAINT_REQUIREMENTS="https://raw.githubusercontent.com/apache/airflow/1.10.10/requirements/requirements-python3.7.txt" \
    --build-arg ENTRYPOINT_FILE="https://raw.githubusercontent.com/apache/airflow/1.10.10/entrypoint.sh" \
    --build-arg AIRFLOW_SOURCES_FROM="entrypoint.sh" \
    --build-arg AIRFLOW_SOURCES_TO="/entrypoint"

This builds the production image in version 3.7 with default extras from 1.10.10 Pypi package and
requirements taken from v1-10-test branch in Github.

.. code-block::

  docker build . \
    --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" \
    --build-arg PYTHON_MAJOR_MINOR_VERSION=3.7 \
    --build-arg AIRFLOW_INSTALL_SOURCES="apache-airflow" \
    --build-arg AIRFLOW_INSTALL_VERSION="==1.10.10" \
    --build-arg CONSTRAINT_REQUIREMENTS="https://raw.githubusercontent.com/apache/airflow/1.10.10/requirements/requirements-python3.7.txt" \
    --build-arg ENTRYPOINT_FILE="https://raw.githubusercontent.com/apache/airflow/1.10.10/entrypoint.sh" \
    --build-arg AIRFLOW_SOURCES_FROM="entrypoint.sh" \
    --build-arg AIRFLOW_SOURCES_TO="/entrypoint"

This builds the production image in version 3.7 with additional airflow extras from 1.10.10 Pypi package and
additional python dependencies.

.. code-block::

  docker build . \
    --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" \
    --build-arg PYTHON_MAJOR_MINOR_VERSION=3.7 \
    --build-arg AIRFLOW_INSTALL_SOURCES="apache-airflow" \
    --build-arg AIRFLOW_INSTALL_VERSION="==1.10.10" \
    --build-arg CONSTRAINT_REQUIREMENTS="https://raw.githubusercontent.com/apache/airflow/1.10.10/requirements/requirements-python3.7.txt" \
    --build-arg ENTRYPOINT_FILE="https://raw.githubusercontent.com/apache/airflow/1.10.10/entrypoint.sh" \
    --build-arg AIRFLOW_SOURCES_FROM="entrypoint.sh" \
    --build-arg AIRFLOW_SOURCES_TO="/entrypoint" \
    --build-arg ADDITIONAL_AIRFLOW_EXTRAS="mssql,hdfs"
    --build-arg ADDITIONAL_PYTHON_DEPS="sshtunnel oauth2client"

Image manifests
---------------

Together with the main CI images we also build and push image manifests. Those manifests are very small images
that contain only results of the docker inspect for the image. This is in order to be able to
determine very quickly if the image in the docker registry has changed a lot since the last time.
Unfortunately docker registry (specifically dockerhub registry) has no anonymous way of querying image
details via API, you need to download the image to inspect it. We overcame it in the way that
always when we build the image we build a very small image manifest and push it to registry together
with the main CI image. The tag for the manifest image is the same as for the image it refers
to with added ``-manifest`` suffix. The manifest image for ``apache/airflow:master-python3.6-ci`` is named
``apache/airflow:master-python3.6-ci-manifest``.

Pulling the Latest Images
-------------------------

Sometimes the image needs to be rebuilt from scratch. This is required, for example,
when there is a security update of the Python version that all the images are based on and new version
of the image is pushed to the repository. In this case it is usually faster to pull the latest
images rather than rebuild them from scratch.

You can do it via the ``--force-pull-images`` flag to force pulling the latest images from the Docker Hub.

For production image:

.. code-block::

  ./breeze build-image --force-pull-images --production-image

For CI image Breeze automatically uses force pulling in case it determines that your image is very outdated,
however uou can also force it with the same flag.

.. code-block::

  ./breeze build-image --force-pull-images

Using the images
================

Both images have entrypoint set as dumb-init with entrypoint.sh script executed (in order to forward
signals). This entrypoint works as follows:

* If ``AIRFLOW__CORE__SQL_ALCHEMY_CONN`` variable is passed to the container and it is either mysql or postgres
  SQL alchemy connection, then the connection is checked and the script waits until the database is reachable.
* If no ``AIRFLOW__CORE__SQL_ALCHEMY_CONN`` variable is set or if it is set to sqlite SQL alchemy connection
  then db reset is executed.
* If ``AIRFLOW__CELERY__BROKER_URL`` variable is passed and scheduler, worker of flower command is used then
  the connection is checked and the script waits until the Celery broker database is reachable.

* If first argument is equal to "bash" - you are dropped in bash shell.
* If there are any arguments they are passed to "airflow" command
