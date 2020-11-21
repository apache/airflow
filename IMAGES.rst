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

  * Production image (Dockerfile) - that can be used to build your own production-ready Airflow installation
    You can read more about building and using the production image in the
    `Production Deployments <docs/production-deployment.rst>`_ document. The image is built using
    `Dockerfile <Dockerfile>`_

  * CI image (Dockerfile.ci) - used for running tests and local development. The image is built using
    `Dockerfile.ci <Dockerfile.ci>`_

Image naming conventions
========================

The images are named as follows:

``apache/airflow:<BRANCH_OR_TAG>-python<PYTHON_MAJOR_MINOR_VERSION>[-ci][-manifest]``

where:

* ``BRANCH_OR_TAG`` - branch or tag used when creating the image. Examples: ``master``, ``v1-10-test``, ``1.10.12``
  The ``master`` and ``v1-10-test`` labels are built from branches so they change over time. The ``1.10.*`` and in
  the future ``2.*`` labels are build from git tags and they are "fixed" once built.
* ``PYTHON_MAJOR_MINOR_VERSION`` - version of python used to build the image. Examples: ``3.5``, ``3.7``
* The ``-ci`` suffix is added for CI images
* The ``-manifest`` is added for manifest images (see below for explanation of manifest images)

We also store (to increase speed of local build/pulls) python images that were used to build
the CI images. Each CI image, when built uses current python version of the base images. Those
python images are regularly updated (with bugfixes/security fixes), so for example python3.8 from
last week might be a different image than python3.8 today. Therefore whenever we push CI image
to airflow repository, we also push the python image that was used to build it this image is stored
as ``apache/airflow:python-3.8-<BRANCH_OR_TAG>``.

Since those are simply snapshots of the existing python images, DockerHub does not create a separate
copy of those images - all layers are mounted from the original python images and those are merely
labels pointing to those.

Building docker images
======================

The easiest way to build those images is to use `<BREEZE.rst>`_.

Note! Breeze by default builds production image from local sources. You can change it's behaviour by
providing ``--install-airflow-version`` parameter, where you can specify the
tag/branch used to download Airflow package from in github repository. You can
also change the repository itself by adding ``--dockerhub-user`` and ``--dockerhub-repo`` flag values.

You can build the CI image using this command:

.. code-block:: bash

  ./breeze build-image

You can build production image using this command:

.. code-block:: bash

  ./breeze build-image --production-image

By adding ``--python <PYTHON_MAJOR_MINOR_VERSION>`` parameter you can build the
image version for the chosen python version.

The images are build with default extras - different extras for CI and production image and you
can change the extras via the ``--extras`` parameters and add new ones with ``--additional-extras``.
You can see default extras used via ``./breeze flags``.

For example if you want to build python 3.7 version of production image with
"all" extras installed you should run this command:

.. code-block:: bash

  ./breeze build-image --python 3.7 --extras "all" --production-image

If you just want to add new extras you can add them like that:

.. code-block:: bash

  ./breeze build-image --python 3.7 --additional-extras "all" --production-image

The command that builds the CI image is optimized to minimize the time needed to rebuild the image when
the source code of Airflow evolves. This means that if you already have the image locally downloaded and
built, the scripts will determine whether the rebuild is needed in the first place. Then the scripts will
make sure that minimal number of steps are executed to rebuild parts of the image (for example,
PIP dependencies) and will give you an image consistent with the one used during Continuous Integration.

The command that builds the production image is optimised for size of the image.

In Breeze by default, the airflow is installed using local sources of Apache Airflow.

You can also build production images from PIP packages via providing ``--install-airflow-version``
parameter to Breeze:

.. code-block:: bash

  ./breeze build-image --python 3.7 --additional-extras=presto \
      --production-image --install-airflow-version=1.10.12

This will build the image using command similar to:

.. code-block:: bash

    pip install \
      apache-airflow[async,aws,azure,celery,dask,elasticsearch,gcp,kubernetes,mysql,postgres,redis,slack,ssh,statsd,virtualenv,presto]==1.10.12 \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-1.10.12/constraints-3.6.txt"

You can also build production images from specific Git version via providing ``--install-airflow-reference``
parameter to Breeze (this time constraints are taken from the ``constraints-master`` branch which is the
HEAD of development for constraints):

.. code-block:: bash

    pip install "https://github.com/apache/airflow/archive/<tag>.tar.gz#egg=apache-airflow" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-master/constraints-3.6.txt"

Using cache during builds
=========================

Default mechanism used in Breeze for building CI images uses images pulled from DockerHub or
GitHub Image Registry. This is done to speed up local builds and CI builds - instead of 15 minutes
for rebuild of CI images, it takes usually less than 3 minutes when cache is used. For CI builds this is
usually the best strategy - to use default "pull" cache. This is default strategy when
`<BREEZE.rst>`_ builds are performed.

For Production Image - which is far smaller and faster to build, it's better to use local build cache (the
standard mechanism that docker uses. This is the default strategy for production images when
`<BREEZE.rst>`_ builds are performed. The first time you run it, it will take considerably longer time than
if you use the pull mechanism, but then when you do small, incremental changes to local sources,
Dockerfile image= and scripts further rebuilds with local build cache will be considerably faster.

You can also disable build cache altogether. This is the strategy used by the scheduled builds in CI - they
will always rebuild all the images from scratch.

You can change the strategy by providing one of the ``--build-cache-local``, ``--build-cache-pulled`` or
even ``--build-cache-disabled`` flags when you run Breeze commands. For example:

.. code-block:: bash

  ./breeze build-image --python 3.7 --build-cache-local

Will build the CI image using local build cache (note that it will take quite a long time the first
time you run it).

.. code-block:: bash

  ./breeze build-image --python 3.7 --production-image --build-cache-pulled

Will build the production image with pulled images as cache.


.. code-block:: bash

  ./breeze build-image --python 3.7 --production-image --build-cache-disabled

Will build the production image from the scratch.

You can also turn local docker caching by setting ``DOCKER_CACHE`` variable to "local", "pulled",
"disabled" and exporting it.

.. code-block:: bash

  export DOCKER_CACHE="local"

or

.. code-block:: bash

  export DOCKER_CACHE="disabled"


Choosing image registry
=======================

By default images are pulled and pushed from and to DockerHub registry when you use Breeze's push-image
or build commands.

Our images are named like that:

.. code-block:: bash

  apache/airflow:<BRANCH_OR_TAG>[-<PATCH>]-pythonX.Y         - for production images
  apache/airflow:<BRANCH_OR_TAG>[-<PATCH>]-pythonX.Y-ci      - for CI images
  apache/airflow:<BRANCH_OR_TAG>[-<PATCH>]-pythonX.Y-build   - for production build stage

For example:

.. code-block:: bash

  apache/airflow:master-python3.6                - production "latest" image from current master
  apache/airflow:master-python3.6-ci             - CI "latest" image from current master
  apache/airflow:v1-10-test-python2.7-ci         - CI "latest" image from current v1-10-test branch
  apache/airflow:1.10.12-python3.6               - production image for 1.10.12 release
  apache/airflow:1.10.12-1-python3.6             - production image for 1.10.12 with some patches applied


You can see DockerHub images at `<https://hub.docker.com/repository/docker/apache/airflow>`_

By default DockerHub registry is used when you push or pull such images.
However for CI builds we keep the images in GitHub registry as well - this way we can easily push
the images automatically after merge requests and use such images for Pull Requests
as cache - which makes it much it much faster for CI builds (images are available in cache
right after merged request in master finishes it's build), The difference is visible especially if
significant changes are done in the Dockerfile.CI.

The images are named differently (in Docker definition of image names - registry URL is part of the
image name if DockerHub is not used as registry). Also GitHub has its own structure for registries
each project has its own registry naming convention that should be followed. The name of
images for GitHub registry are:

.. code-block:: bash

  docker.pkg.github.com/apache/airflow/<BRANCH>-pythonX.Y       - for production images
  docker.pkg.github.com/apache/airflow/<BRANCH>-pythonX.Y-ci    - for CI images
  docker.pkg.github.com/apache/airflow/<BRANCH>-pythonX.Y-build - for production build state

Note that we never push or pull TAG images to GitHub registry. It is only used for CI builds

You can see all the current GitHub images at `<https://github.com/apache/airflow/packages>`_

In order to interact with the GitHub images you need to add ``--github-registry`` flag to the pull/push
commands in Breeze. This way the images will be pulled/pushed from/to GitHub rather than from/to
DockerHub. Images are build locally as ``apache/airflow`` images but then they are tagged with the right
GitHub tags for you.

You can read more about the CI configuration and how CI builds are using DockerHub/GitHub images
in `<CI.rst>`_.

Note that you need to be committer and have the right to push to DockerHub and GitHub and you need to
be logged in. Only committers can push images directly.

Technical details of Airflow images
===================================

The CI image is used by Breeze as shell image but it is also used during CI build.
The image is single segment image that contains Airflow installation with "all" dependencies installed.
It is optimised for rebuild speed. It installs PIP dependencies from the current branch first -
so that any changes in setup.py do not trigger reinstalling of all dependencies.
There is a second step of installation that re-installs the dependencies
from the latest sources so that we are sure that latest dependencies are installed.

The production image is a multi-segment image. The first segment "airflow-build-image" contains all the
build essentials and related dependencies that allow to install airflow locally. By default the image is
build from a released version of Airflow from GitHub, but by providing some extra arguments you can also
build it from local sources. This is particularly useful in CI environment where we are using the image
to run Kubernetes tests. See below for the list of arguments that should be provided to build
production image from the local sources.

The image is primarily optimised for size of the final image, but also for speed of rebuilds - the
'airflow-build-image' segment uses the same technique as the CI builds for pre-installing PIP dependencies.
It first pre-installs them from the right github branch and only after that final airflow installation is
done from either local sources or remote location (PIP or github repository).

Customizing the image
.....................

Customizing the image is an alternative way of adding your own dependencies to the image.

The easiest way to build the image image is to use ``breeze`` script, but you can also build such customized
image by running appropriately crafted docker build in which you specify all the ``build-args``
that you need to add to customize it. You can read about all the args and ways you can build the image
in the `<#ci-image-build-arguments>`_ chapter below.

Here just a few examples are presented which should give you general understanding of what you can customize.

This builds the production image in version 3.7 with additional airflow extras from 1.10.10 Pypi package and
additional apt dev and runtime dependencies.

.. code-block:: bash

  docker build . -f Dockerfile.ci \
    --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" \
    --build-arg PYTHON_MAJOR_MINOR_VERSION=3.7 \
    --build-arg AIRFLOW_INSTALL_SOURCES="apache-airflow" \
    --build-arg AIRFLOW_INSTALL_VERSION="==1.10.12" \
    --build-arg AIRFLOW_CONSTRAINTS_REFERENCE="constraints-1-10" \
    --build-arg AIRFLOW_SOURCES_FROM="empty" \
    --build-arg AIRFLOW_SOURCES_TO="/empty" \
    --build-arg ADDITIONAL_AIRFLOW_EXTRAS="jdbc"
    --build-arg ADDITIONAL_PYTHON_DEPS="pandas"
    --build-arg ADDITIONAL_DEV_APT_DEPS="gcc g++"
    --build-arg ADDITIONAL_RUNTIME_APT_DEPS="default-jre-headless"
    --tag my-image


the same image can be built using ``breeze`` (it supports auto-completion of the options):

.. code-block:: bash

  ./breeze build-image -f Dockerfile.ci \
      --production-image  --python 3.7 --install-airflow-version=1.10.12 \
      --additional-extras=jdbc --additional-python-deps="pandas" \
      --additional-dev-apt-deps="gcc g++" --additional-runtime-apt-deps="default-jre-headless"
You can build the default production image with standard ``docker build`` command but they will only build
default versions of the image and will not use the dockerhub versions of images as cache.


You can customize more aspects of the image - such as additional commands executed before apt dependencies
are installed, or adding extra sources to install your dependencies from. You can see all the arguments
described below but here is an example of rather complex command to customize the image
based on example in `this comment <https://github.com/apache/airflow/issues/8605#issuecomment-690065621>`_:

.. code-block:: bash

  docker build . -f Dockerfile.ci \
    --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" \
    --build-arg PYTHON_MAJOR_MINOR_VERSION=3.7 \
    --build-arg AIRFLOW_INSTALL_SOURCES="apache-airflow" \
    --build-arg AIRFLOW_INSTALL_VERSION="==1.10.12" \
    --build-arg AIRFLOW_CONSTRAINTS_REFERENCE="constraints-1-10" \
    --build-arg AIRFLOW_SOURCES_FROM="empty" \
    --build-arg AIRFLOW_SOURCES_TO="/empty" \
    --build-arg ADDITIONAL_AIRFLOW_EXTRAS="slack" \
    --build-arg ADDITIONAL_PYTHON_DEPS="apache-airflow-backport-providers-odbc \
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

CI image build arguments
........................

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
| ``AIRFLOW_REPO``                         | ``apache/airflow``                       | the repository from which PIP            |
|                                          |                                          | dependencies are pre-installed           |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_BRANCH``                       | ``master``                               | the branch from which PIP dependencies   |
|                                          |                                          | are pre-installed                        |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_CI_BUILD_EPOCH``               | ``1``                                    | increasing this value will reinstall PIP |
|                                          |                                          | dependencies from the repository from    |
|                                          |                                          | scratch                                  |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_CONSTRAINTS_LOCATION``         |                                          | If not empty, it will override the       |
|                                          |                                          | source of the constraints with the       |
|                                          |                                          | specified URL or file. Note that the     |
|                                          |                                          | file has to be in docker context so      |
|                                          |                                          | it's best to place such file in          |
|                                          |                                          | one of the folders included in           |
|                                          |                                          | dockerignore                . for example in the        |
|                                          |                                          | 'docker-context-files'. Note that the    |
|                                          |                                          | location does not work for the first     |
|                                          |                                          | stage of installation when the           |
|                                          |                                          | stage of installation when the           |
|                                          |                                          | ``AIRFLOW_PRE_CACHED_PIP_PACKAGES`` is   |
|                                          |                                          | set to true. Default location from       |
|                                          |                                          | GitHub is used in this case.             |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_LOCAL_PIP_WHEELS``             | ``false``                                | If set to true, Airflow and it's         |
|                                          |                                          | dependencies are installed from locally  |
|                                          |                                          | downloaded .whl files placed in the      |
|                                          |                                          | ``docker-context-files``.                |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_EXTRAS``                       | ``all``                                  | extras to install                        |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``INSTALL_AIRFLOW_VIA_PIP``              | ``false``                                | If set to true, Airflow is installed via |
|                                          |                                          | pip install. if you want to install      |
|                                          |                                          | Airflow from externally provided binary  |
|                                          |                                          | package you can set it to false, place   |
|                                          |                                          | the package in ``docker-context-files``  |
|                                          |                                          | and set ``AIRFLOW_LOCAL_PIP_WHEELS`` to  |
|                                          |                                          | true. You have to also set to true the   |
|                                          |                                          | ``AIRFLOW_PRE_CACHED_PIP_PACKAGES`` flag |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_PRE_CACHED_PIP_PACKAGES``      | ``true``                                 | Allows to pre-cache airflow PIP packages |
|                                          |                                          | from the GitHub of Apache Airflow        |
|                                          |                                          | This allows to optimize iterations for   |
|                                          |                                          | Image builds and speeds up CI builds     |
|                                          |                                          | But in some corporate environments it    |
|                                          |                                          | might be forbidden to download anything  |
|                                          |                                          | from public repositories.                |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_AIRFLOW_EXTRAS``            |                                          | additional extras to install             |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_PYTHON_DEPS``               |                                          | additional python dependencies to        |
|                                          |                                          | install                                  |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``DEV_APT_COMMAND``                      | (see Dockerfile)                         | Dev apt command executed before dev deps |
|                                          |                                          | are installed in the first part of image |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_DEV_APT_COMMAND``           |                                          | Additional Dev apt command executed      |
|                                          |                                          | before dev dep are installed             |
|                                          |                                          | in the first part of the image           |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``DEV_APT_DEPS``                         | (see Dockerfile)                         | Dev APT dependencies installed           |
|                                          |                                          | in the first part of the image           |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_DEV_APT_DEPS``              |                                          | Additional apt dev dependencies          |
|                                          |                                          | installed in the first part of the image |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_DEV_APT_ENV``               |                                          | Additional env variables defined         |
|                                          |                                          | when installing dev deps                 |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``RUNTIME_APT_COMMAND``                  | (see Dockerfile)                         | Runtime apt command executed before deps |
|                                          |                                          | are installed in first part of the image |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_RUNTIME_APT_COMMAND``       |                                          | Additional Runtime apt command executed  |
|                                          |                                          | before runtime dep are installed         |
|                                          |                                          | in the second part of the image          |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``RUNTIME_APT_DEPS``                     | (see Dockerfile)                         | Runtime APT dependencies installed       |
|                                          |                                          | in the second part of the image          |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_RUNTIME_APT_DEPS``          |                                          | Additional apt runtime dependencies      |
|                                          |                                          | installed in second part of the image    |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_RUNTIME_APT_ENV``           |                                          | Additional env variables defined         |
|                                          |                                          | when installing runtime deps             |
+------------------------------------------+------------------------------------------+------------------------------------------+

Here are some examples of how CI images can built manually. CI is always built from local sources.

This builds the CI image in version 3.7 with default extras ("all").

.. code-block:: bash

  docker build . -f Dockerfile.ci --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" \
    --build-arg PYTHON_MAJOR_MINOR_VERSION=3.7


This builds the CI image in version 3.6 with "gcp" extra only.

.. code-block:: bash

  docker build . -f Dockerfile.ci --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" \
    --build-arg PYTHON_MAJOR_MINOR_VERSION=3.6 --build-arg AIRFLOW_EXTRAS=gcp


This builds the CI image in version 3.6 with "apache-beam" extra added.

.. code-block:: bash

  docker build . -f Dockerfile.ci --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" \
    --build-arg PYTHON_MAJOR_MINOR_VERSION=3.6 --build-arg ADDITIONAL_AIRFLOW_EXTRAS="apache-beam"

This builds the CI image in version 3.6 with "mssql" additional package added.

.. code-block:: bash

  docker build . -f Dockerfile.ci --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" \
    --build-arg PYTHON_MAJOR_MINOR_VERSION=3.6 --build-arg ADDITIONAL_PYTHON_DEPS="mssql"

This builds the CI image in version 3.6 with "gcc" and "g++" additional apt dev dependencies added.

.. code-block::

  docker build . -f Dockerfile.ci --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" \
    --build-arg PYTHON_MAJOR_MINOR_VERSION=3.6 --build-arg ADDITIONAL_DEV_APT_DEPS="gcc g++"

This builds the CI image in version 3.6 with "jdbc" extra and "default-jre-headless" additional apt runtime dependencies added.

.. code-block::

  docker build . -f Dockerfile.ci --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" \
    --build-arg PYTHON_MAJOR_MINOR_VERSION=3.6 --build-arg AIRFLOW_EXTRAS=jdbc --build-arg ADDITIONAL_RUNTIME_DEPS="default-jre-headless"

Production images
-----------------

You can find details about using, building, extending and customising the production images in the
`Latest documentation <https://airflow.readthedocs.io/en/latest/production-deployment.html>`_


Image manifests
---------------

Together with the main CI images we also build and push image manifests. Those manifests are very small images
that contain only results of the docker inspect for the image. This is in order to be able to
determine very quickly if the image in the docker registry has changed a lot since the last time.
Unfortunately docker registry (specifically DockerHub registry) has no anonymous way of querying image
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

.. code-block:: bash

  ./breeze build-image --force-pull-images --production-image

For CI image Breeze automatically uses force pulling in case it determines that your image is very outdated,
however uou can also force it with the same flag.

.. code-block:: bash

  ./breeze build-image --force-pull-images

Embedded image scripts
======================

Both images have a set of scripts that can be used in the image. Those are:
 * /entrypoint - entrypoint script used when entering the image
 * /clean-logs - script for periodic log cleaning

Running the CI image
====================

The entrypoint in the CI image contains all the initialisation needed for tests to be immediately executed.
It is copied from ``scripts/in_container/entrypoint_ci.sh``.

The default behaviour is that you are dropped into bash shell. However if RUN_TESTS variable is
set to "true", then tests passed as arguments are executed

The entrypoint performs those operations:

* checks if the environment is ready to test (including database and all integrations). It waits
  until all the components are ready to work

* installs older version of Airflow (if older version of Airflow is requested to be installed
  via ``INSTALL_AIRFLOW_VERSION`` variable.

* Sets up Kerberos if Kerberos integration is enabled (generates and configures Kerberos token)

* Sets up ssh keys for ssh tests and restarts teh SSH server

* Sets all variables and configurations needed for unit tests to run

* Reads additional variables set in ``files/airflow-breeze-config/variables.env`` by sourcing that file

* In case of CI run sets parallelism to 2 to avoid excessive number of processes to run

* In case of CI run sets default parameters for pytest

* In case of running integration/long_running/quarantined tests - it sets the right pytest flags

* Sets default "tests" target in case the target is not explicitly set as additional argument

* Runs system tests if RUN_SYSTEM_TESTS flag is specified, otherwise runs regular unit and integration tests


Using, customising, and extending the production image
======================================================

You can read more about using, customising, and extending the production image in the documentation:

* [Stable docs](https://airflow.apache.org/docs/stable/production-deployment.html)
* [Latest docs from master branch](https://airflow.readthedocs.io/en/latest/production-deployment.html

Alpha versions of 1.10.10 production-ready images
=================================================

The production images have been released for the first time in 1.10.10 release of Airflow as "Alpha" quality
ones. Between 1.10.10 the images are being improved and the 1.10.10 images should be patched and
published several times separately in order to test them with the upcoming Helm Chart.

Those images are for development and testing only and should not be used outside of the
development community.

The images were pushed with tags following the pattern: ``apache/airflow:1.10.10.1-alphaN-pythonX.Y``.
Patch level is an increasing number (starting from 1).

Those are alpha-quality releases however they contain the officially released Airflow ``1.10.10`` code.
The main changes in the images are scripts embedded in the images.

The following versions were pushed:

+-------+--------------------------------+----------------------------------------------------------+
| Patch | Tag pattern                    | Description                                              |
+=======+================================+==========================================================+
| 1     | ``1.10.10.1-alpha1-pythonX.Y`` | Support for parameters added to bash and python commands |
+-------+--------------------------------+----------------------------------------------------------+
| 2     | ``1.10.10-1-alpha2-pythonX.Y`` | Added "/clean-logs" script                               |
+-------+--------------------------------+----------------------------------------------------------+

The commits used to generate those images are tagged with ``prod-image-1.10.10.1-alphaN`` tags.
