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

Airflow Docker images
=====================

Airflow has two main images (build from Dockerfiles):

  * Production image (Dockerfile) - that can be used to build your own production-ready Airflow installation
    You can read more about building and using the production image in the
    `Docker stack <https://airflow.apache.org/docs/docker-stack/index.html>`_ documentation.
    The image is built using `Dockerfile <Dockerfile>`_

  * CI image (Dockerfile.ci) - used for running tests and local development. The image is built using
    `Dockerfile.ci <Dockerfile.ci>`_

PROD image
-----------

The PROD image is a multi-segment image. The first segment "airflow-build-image" contains all the
build essentials and related dependencies that allow to install airflow locally. By default the image is
build from a released version of Airflow from GitHub, but by providing some extra arguments you can also
build it from local sources. This is particularly useful in CI environment where we are using the image
to run Kubernetes tests. See below for the list of arguments that should be provided to build
production image from the local sources.

The image is primarily optimised for size of the final image, but also for speed of rebuilds - the
'airflow-build-image' segment uses the same technique as the CI jobs for pre-installing dependencies.
It first pre-installs them from the right GitHub branch and only after that final airflow installation is
done from either local sources or remote location (PyPI or GitHub repository).

You can read more details about building, extending and customizing the PROD image in the
`Latest documentation <https://airflow.apache.org/docs/docker-stack/index.html>`_

CI image
--------

The CI image is used by `Breeze <BREEZE.rst>`_ as the shell image but it is also used during CI tests.
The image is single segment image that contains Airflow installation with "all" dependencies installed.
It is optimised for rebuild speed. It installs PIP dependencies from the current branch first -
so that any changes in setup.py do not trigger reinstalling of all dependencies.
There is a second step of installation that re-installs the dependencies
from the latest sources so that we are sure that latest dependencies are installed.

Building docker images from current sources
===========================================

The easy way to build the CI/PROD images is to use `<BREEZE.rst>`_. It uses a number of optimization
and caches to build it efficiently and fast when you are developing Airflow and need to update to
latest version.

CI image, airflow package is always built from sources. When you execute the image, you can however use
the ``--use-airflow-version`` flag (or ``USE_AIRFLOW_VERSION`` environment variable) to remove
the preinstalled source version of Airflow and replace it with one of the possible installation methods:

* "none" airflow is removed and not installed
* "wheel" airflow is removed and replaced with "wheel" version available in dist
* "sdist" airflow is removed and replaced with "sdist" version available in dist
* "<VERSION>" airflow is removed and installed from PyPI (with the specified version)

For PROD image by default production image is built from the latest sources when using Breeze, but when
you use it via docker build command, it uses the latest installed version of airflow and providers.
However, you can choose different installation methods as described in
`Building PROD docker images from released PIP packages <#building-prod-docker-images-from-released-packages>`_.
Detailed reference for building production image from different sources can be found in:
`Build Args reference <docs/docker-stack/build-arg-ref.rst#installing-airflow-using-different-methods>`_

You can build the CI image using current sources this command:

.. code-block:: bash

  breeze ci-image build

You can build the PROD image using current sources with this command:

.. code-block:: bash

  breeze prod-image build

By adding ``--python <PYTHON_MAJOR_MINOR_VERSION>`` parameter you can build the
image version for the chosen Python version.

The images are build with default extras - different extras for CI and production image and you
can change the extras via the ``--extras`` parameters and add new ones with ``--additional-extras``.

For example if you want to build Python 3.7 version of production image with
"all" extras installed you should run this command:

.. code-block:: bash

  breeze prod-image build --python 3.7 --extras "all"

If you just want to add new extras you can add them like that:

.. code-block:: bash

  breeze prod-image build --python 3.7 --additional-extras "all"

The command that builds the CI image is optimized to minimize the time needed to rebuild the image when
the source code of Airflow evolves. This means that if you already have the image locally downloaded and
built, the scripts will determine whether the rebuild is needed in the first place. Then the scripts will
make sure that minimal number of steps are executed to rebuild parts of the image (for example,
PIP dependencies) and will give you an image consistent with the one used during Continuous Integration.

The command that builds the production image is optimised for size of the image.

Building PROD docker images from released PIP packages
======================================================

You can also build production images from PIP packages via providing ``--install-airflow-version``
parameter to Breeze:

.. code-block:: bash

  breeze prod-image build --python 3.7 --additional-extras=trino --install-airflow-version=2.0.0

This will build the image using command similar to:

.. code-block:: bash

    pip install \
      apache-airflow[async,amazon,celery,cncf.kubernetes,docker,dask,elasticsearch,ftp,grpc,hashicorp,http,ldap,google,microsoft.azure,mysql,postgres,redis,sendgrid,sftp,slack,ssh,statsd,virtualenv]==2.0.0 \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.0.0/constraints-3.7.txt"

.. note::

   Only ``pip`` installation is currently officially supported.

   While they are some successes with using other tools like `poetry <https://python-poetry.org/>`_ or
   `pip-tools <https://pypi.org/project/pip-tools/>`_, they do not share the same workflow as
   ``pip`` - especially when it comes to constraint vs. requirements management.
   Installing via ``Poetry`` or ``pip-tools`` is not currently supported.

   If you wish to install airflow using those tools you should use the constraint files and convert
   them to appropriate format and workflow that your tool requires.


You can also build production images from specific Git version via providing ``--install-airflow-reference``
parameter to Breeze (this time constraints are taken from the ``constraints-main`` branch which is the
HEAD of development for constraints):

.. code-block:: bash

    pip install "https://github.com/apache/airflow/archive/<tag>.tar.gz#egg=apache-airflow" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-main/constraints-3.7.txt"

You can also skip installing airflow and install it from locally provided files by using
``--install-packages-from-context`` parameter to Breeze:

.. code-block:: bash

  breeze prod-image build --python 3.7 --additional-extras=trino --install-packages-from-context

In this case you airflow and all packages (.whl files) should be placed in ``docker-context-files`` folder.

Using docker cache during builds
================================

Default mechanism used in Breeze for building CI images uses images pulled from
GitHub Container Registry. This is done to speed up local builds and building images for CI runs - instead of
> 12 minutes for rebuild of CI images, it takes usually about 1 minute when cache is used.
For CI images this is usually the best strategy - to use default "pull" cache. This is default strategy when
`<BREEZE.rst>`_ builds are performed.

For Production Image - which is far smaller and faster to build, it's better to use local build cache (the
standard mechanism that docker uses. This is the default strategy for production images when
`<BREEZE.rst>`_ builds are performed. The first time you run it, it will take considerably longer time than
if you use the pull mechanism, but then when you do small, incremental changes to local sources,
Dockerfile image= and scripts further rebuilds with local build cache will be considerably faster.

You can also disable build cache altogether. This is the strategy used by the scheduled builds in CI - they
will always rebuild all the images from scratch.

You can change the strategy by providing one of the ``--build-cache`` flags: ``registry`` (default), ``local``,
or ``disabled`` flags when you run Breeze commands. For example:

.. code-block:: bash

  breeze ci-image build --python 3.7 --docker-cache local

Will build the CI image using local build cache (note that it will take quite a long time the first
time you run it).

.. code-block:: bash

  breeze prod-image build --python 3.7 --docker-cache registry

Will build the production image with cache used from registry.


.. code-block:: bash

  breeze prod-image build --python 3.7 --docker-cache disabled

Will build the production image from the scratch.

You can also turn local docker caching by setting ``DOCKER_CACHE`` variable to ``local``, ``registry``,
``disabled`` and exporting it.

.. code-block:: bash

  export DOCKER_CACHE="registry"

or

.. code-block:: bash

  export DOCKER_CACHE="local"

or

.. code-block:: bash

  export DOCKER_CACHE="disabled"

Naming conventions
==================

By default images we are using cache for images in GitHub Container registry. We are using GitHub
Container Registry as development image cache and CI registry for build images.
The images are all in organization wide "apache/" namespace. We are adding "airflow-" as prefix for
the image names of all Airflow images. The images are linked to the repository
via ``org.opencontainers.image.source`` label in the image.

See https://docs.github.com/en/packages/learn-github-packages/connecting-a-repository-to-a-package

Naming convention for the GitHub packages.

Images with a commit SHA (built for pull requests and pushes). Those are images that are snapshot of the
currently run build. They are built once per each build and pulled by each test job.

.. code-block:: bash

  ghcr.io/apache/airflow/<BRANCH>/ci/python<X.Y>:<COMMIT_SHA>         - for CI images
  ghcr.io/apache/airflow/<BRANCH>/prod/python<X.Y>:<COMMIT_SHA>       - for production images

Thoe image contain inlined cache.

You can see all the current GitHub images at `<https://github.com/apache/airflow/packages>`_

You can read more about the CI configuration and how CI jobs are using GitHub images
in `<CI.rst>`_.

Note that you need to be committer and have the right to refresh the images in the GitHub Registry with
latest sources from main via (./dev/refresh_images.sh).
Only committers can push images directly. You need to login with your Personal Access Token with
"packages" write scope to be able to push to those repositories or pull from them
in case of GitHub Packages.

GitHub Container Registry

.. code-block:: bash

  docker login ghcr.io

Since there are different naming conventions used for Airflow images and there are multiple images used,
`Breeze <BREEZE.rst>`_ provides easy to use management interface for the images. The
`CI system of ours <CI.rst>`_ is designed in the way that it should automatically refresh caches, rebuild
the images periodically and update them whenever new version of base Python is released.
However, occasionally, you might need to rebuild images locally and push them directly to the registries
to refresh them.



Every developer can also pull and run images being result of a specific CI run in GitHub Actions.
This is a powerful tool that allows to reproduce CI failures locally, enter the images and fix them much
faster. It is enough to pass ``--image-tag`` and the registry and Breeze will download and execute
commands using the same image that was used during the CI tests.

For example this command will run the same Python 3.8 image as was used in build identified with
9a621eaa394c0a0a336f8e1b31b35eff4e4ee86e commit SHA  with enabled rabbitmq integration.

.. code-block:: bash

  breeze --image-tag 9a621eaa394c0a0a336f8e1b31b35eff4e4ee86e --python 3.8 --integration rabbitmq

You can see more details and examples in `Breeze <BREEZE.rst>`_

Customizing the CI image
========================

Customizing the CI image allows to add your own dependencies to the image.

The easiest way to build the customized image is to use ``breeze`` script, but you can also build suc
customized image by running appropriately crafted docker build in which you specify all the ``build-args``
that you need to add to customize it. You can read about all the args and ways you can build the image
in the `<#ci-image-build-arguments>`_ chapter below.

Here just a few examples are presented which should give you general understanding of what you can customize.

This builds the production image in version 3.7 with additional airflow extras from 2.0.0 PyPI package and
additional apt dev and runtime dependencies.

As of Airflow 2.3.0, it is required to build images with ``DOCKER_BUILDKIT=1`` variable
(Breeze sets ``DOCKER_BUILDKIT=1`` variable automatically) or via ``docker buildx build`` command if
you have ``buildx`` plugin installed.

.. code-block:: bash

  DOCKER_BUILDKIT=1 docker build . -f Dockerfile.ci \
    --pull \
    --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-bullseye" \
    --build-arg ADDITIONAL_AIRFLOW_EXTRAS="jdbc" \
    --build-arg ADDITIONAL_PYTHON_DEPS="pandas" \
    --build-arg ADDITIONAL_DEV_APT_DEPS="gcc g++" \
    --tag my-image:0.0.1


the same image can be built using ``breeze`` (it supports auto-completion of the options):

.. code-block:: bash

  breeze ci-image build --python 3.7 --additional-extras=jdbc --additional-python-deps="pandas" \
      --additional-dev-apt-deps="gcc g++"

You can customize more aspects of the image - such as additional commands executed before apt dependencies
are installed, or adding extra sources to install your dependencies from. You can see all the arguments
described below but here is an example of rather complex command to customize the image
based on example in `this comment <https://github.com/apache/airflow/issues/8605#issuecomment-690065621>`_:

.. code-block:: bash

  DOCKER_BUILDKIT=1 docker build . -f Dockerfile.ci \
    --pull \
    --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-bullseye" \
    --build-arg AIRFLOW_INSTALLATION_METHOD="apache-airflow" \
    --build-arg ADDITIONAL_AIRFLOW_EXTRAS="slack" \
    --build-arg ADDITIONAL_PYTHON_DEPS="apache-airflow-providers-odbc \
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
    --build-arg ADDITIONAL_DEV_ENV_VARS="ACCEPT_EULA=Y"
    --tag my-image:0.0.1

CI image build arguments
------------------------

The following build arguments (``--build-arg`` in docker build command) can be used for CI images:

+------------------------------------------+------------------------------------------+------------------------------------------+
| Build argument                           | Default value                            | Description                              |
+==========================================+==========================================+==========================================+
| ``PYTHON_BASE_IMAGE``                    | ``python:3.7-slim-bullseye``             | Base Python image                        |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``PYTHON_MAJOR_MINOR_VERSION``           | ``3.7``                                  | major/minor version of Python (should    |
|                                          |                                          | match base image)                        |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``DEPENDENCIES_EPOCH_NUMBER``            | ``2``                                    | increasing this number will reinstall    |
|                                          |                                          | all apt dependencies                     |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_PIP_INSTALL_FLAGS``         |                                          | additional ``pip`` flags passed to the   |
|                                          |                                          | installation commands (except when       |
|                                          |                                          | reinstalling ``pip`` itself)             |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``PIP_NO_CACHE_DIR``                     | ``true``                                 | if true, then no pip cache will be       |
|                                          |                                          | stored                                   |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``HOME``                                 | ``/root``                                | Home directory of the root user (CI      |
|                                          |                                          | image has root user as default)          |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_HOME``                         | ``/root/airflow``                        | Airflow's HOME (that's where logs and    |
|                                          |                                          | sqlite databases are stored)             |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_SOURCES``                      | ``/opt/airflow``                         | Mounted sources of Airflow               |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_REPO``                         | ``apache/airflow``                       | the repository from which PIP            |
|                                          |                                          | dependencies are pre-installed           |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_BRANCH``                       | ``main``                                 | the branch from which PIP dependencies   |
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
|                                          |                                          | .dockerignore. for example in the        |
|                                          |                                          | 'docker-context-files'. Note that the    |
|                                          |                                          | location does not work for the first     |
|                                          |                                          | stage of installation when the           |
|                                          |                                          | stage of installation when the           |
|                                          |                                          | ``AIRFLOW_PRE_CACHED_PIP_PACKAGES`` is   |
|                                          |                                          | set to true. Default location from       |
|                                          |                                          | GitHub is used in this case.             |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_CONSTRAINTS_REFERENCE``        |                                          | reference (branch or tag) from GitHub    |
|                                          |                                          | repository from which constraints are    |
|                                          |                                          | used. By default it is set to            |
|                                          |                                          | ``constraints-main`` but can be          |
|                                          |                                          | ``constraints-2-0`` for 2.0.* versions   |
|                                          |                                          | or it could point to specific version    |
|                                          |                                          | for example ``constraints-2.0.0``        |
|                                          |                                          | is empty, it is auto-detected            |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_EXTRAS``                       | ``all``                                  | extras to install                        |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``UPGRADE_TO_NEWER_DEPENDENCIES``        | ``false``                                | If set to a value different than "false" |
|                                          |                                          | the dependencies are upgraded to newer   |
|                                          |                                          | versions. In CI it is set to build id    |
|                                          |                                          | to make sure subsequent builds are not   |
|                                          |                                          | reusing cached images with same value.   |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_PRE_CACHED_PIP_PACKAGES``      | ``true``                                 | Allows to pre-cache airflow PIP packages |
|                                          |                                          | from the GitHub of Apache Airflow        |
|                                          |                                          | This allows to optimize iterations for   |
|                                          |                                          | Image builds and speeds up CI jobs       |
|                                          |                                          | But in some corporate environments it    |
|                                          |                                          | might be forbidden to download anything  |
|                                          |                                          | from public repositories.                |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_AIRFLOW_EXTRAS``            |                                          | additional extras to install             |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_PYTHON_DEPS``               |                                          | additional Python dependencies to        |
|                                          |                                          | install                                  |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``DEV_APT_COMMAND``                      |                                          | Dev apt command executed before dev deps |
|                                          |                                          | are installed in the first part of image |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_DEV_APT_COMMAND``           |                                          | Additional Dev apt command executed      |
|                                          |                                          | before dev dep are installed             |
|                                          |                                          | in the first part of the image           |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``DEV_APT_DEPS``                         | Empty - install default dependencies     | Dev APT dependencies installed           |
|                                          | (see ``install_os_dependencies.sh``)     | in the first part of the image           |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_DEV_APT_DEPS``              |                                          | Additional apt dev dependencies          |
|                                          |                                          | installed in the first part of the image |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_DEV_APT_ENV``               |                                          | Additional env variables defined         |
|                                          |                                          | when installing dev deps                 |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_PIP_VERSION``                  | ``23.0``                                 | PIP version used.                        |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``PIP_PROGRESS_BAR``                     | ``on``                                   | Progress bar for PIP installation        |
+------------------------------------------+------------------------------------------+------------------------------------------+

Here are some examples of how CI images can built manually. CI is always built from local sources.

This builds the CI image in version 3.7 with default extras ("all").

.. code-block:: bash

  DOCKER_BUILDKIT=1 docker build . -f Dockerfile.ci \
     --pull \
     --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-bullseye" --tag my-image:0.0.1


This builds the CI image in version 3.7 with "gcp" extra only.

.. code-block:: bash

  DOCKER_BUILDKIT=1 docker build . -f Dockerfile.ci \
    --pull \
    --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-bullseye" \
    --build-arg AIRFLOW_EXTRAS=gcp --tag my-image:0.0.1


This builds the CI image in version 3.7 with "apache-beam" extra added.

.. code-block:: bash

  DOCKER_BUILDKIT=1 docker build . -f Dockerfile.ci \
    --pull \
    --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-bullseye" \
    --build-arg ADDITIONAL_AIRFLOW_EXTRAS="apache-beam" --tag my-image:0.0.1

This builds the CI image in version 3.7 with "mssql" additional package added.

.. code-block:: bash

  DOCKER_BUILDKIT=1 docker build . -f Dockerfile.ci \
    --pull \
    --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-bullseye" \
    --build-arg ADDITIONAL_PYTHON_DEPS="mssql" --tag my-image:0.0.1

This builds the CI image in version 3.7 with "gcc" and "g++" additional apt dev dependencies added.

.. code-block::

  DOCKER_BUILDKIT=1 docker build . -f Dockerfile.ci \
    --pull
    --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-bullseye" \
    --build-arg ADDITIONAL_DEV_APT_DEPS="gcc g++" --tag my-image:0.0.1

This builds the CI image in version 3.7 with "jdbc" extra and "default-jre-headless" additional apt runtime dependencies added.

.. code-block::

  DOCKER_BUILDKIT=1 docker build . -f Dockerfile.ci \
    --pull \
    --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-bullseye" \
    --build-arg AIRFLOW_EXTRAS=jdbc \
    --tag my-image:0.0.1

Running the CI image
--------------------

The entrypoint in the CI image contains all the initialisation needed for tests to be immediately executed.
It is copied from ``scripts/docker/entrypoint_ci.sh``.

The default behaviour is that you are dropped into bash shell. However if RUN_TESTS variable is
set to "true", then tests passed as arguments are executed

The entrypoint performs those operations:

* checks if the environment is ready to test (including database and all integrations). It waits
  until all the components are ready to work

* removes and re-installs another version of Airflow (if another version of Airflow is requested to be
  reinstalled via ``USE_AIRFLOW_PYPI_VERSION`` variable.

* Sets up Kerberos if Kerberos integration is enabled (generates and configures Kerberos token)

* Sets up ssh keys for ssh tests and restarts the SSH server

* Sets all variables and configurations needed for unit tests to run

* Reads additional variables set in ``files/airflow-breeze-config/variables.env`` by sourcing that file

* In case of CI run sets parallelism to 2 to avoid excessive number of processes to run

* In case of CI run sets default parameters for pytest

* In case of running integration/long_running/quarantined tests - it sets the right pytest flags

* Sets default "tests" target in case the target is not explicitly set as additional argument

* Runs system tests if RUN_SYSTEM_TESTS flag is specified, otherwise runs regular unit and integration tests
