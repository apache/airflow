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
'airflow-build-image' segment uses the same technique as the CI builds for pre-installing PIP dependencies.
It first pre-installs them from the right GitHub branch and only after that final airflow installation is
done from either local sources or remote location (PIP or GitHub repository).

You can read more details about building, extending and customizing the PROD image in the
`Latest documentation <https://airflow.apache.org/docs/docker-stack/index.html>`_


CI image
--------

The CI image is used by `Breeze <BREEZE.rst>`_ as shell image but it is also used during CI build.
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

  ./breeze build-image

You can build the PROD image using current sources with this command:

.. code-block:: bash

  ./breeze build-image --production-image

By adding ``--python <PYTHON_MAJOR_MINOR_VERSION>`` parameter you can build the
image version for the chosen Python version.

The images are build with default extras - different extras for CI and production image and you
can change the extras via the ``--extras`` parameters and add new ones with ``--additional-extras``.
You can see default extras used via ``./breeze flags``.

For example if you want to build Python 3.7 version of production image with
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

Building PROD docker images from released PIP packages
======================================================

You can also build production images from PIP packages via providing ``--install-airflow-version``
parameter to Breeze:

.. code-block:: bash

  ./breeze build-image --python 3.7 --additional-extras=trino \
      --production-image --install-airflow-version=2.0.0

This will build the image using command similar to:

.. code-block:: bash

    pip install \
      apache-airflow[async,amazon,celery,cncf.kubernetes,docker,dask,elasticsearch,ftp,grpc,hashicorp,http,ldap,google,microsoft.azure,mysql,postgres,redis,sendgrid,sftp,slack,ssh,statsd,virtualenv]==2.0.0 \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.0.0/constraints-3.6.txt"

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
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-main/constraints-3.6.txt"

You can also skip installing airflow and install it from locally provided files by using
``--install-from-docker-context-files`` parameter and ``--disable-pypi-when-building`` to Breeze:

.. code-block:: bash

  ./breeze build-image --python 3.7 --additional-extras=trino \
      --production-image --disable-pypi-when-building --install-from-docker-context-files

In this case you airflow and all packages (.whl files) should be placed in ``docker-context-files`` folder.

Using cache during builds
=========================

Default mechanism used in Breeze for building CI images uses images pulled from
GitHub Container Registry. This is done to speed up local builds and CI builds - instead of 15 minutes
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

Naming conventions
==================

By default images are pulled and pushed from and to Github Container registry when you use Breeze's push-image
or build commands.

We are using GitHub Container Registry as build cache.The images are all in organization wide "apache/"
namespace. We are adding "airflow-" as prefix for image names of all Airflow images.
The images are linked to the repository via ``org.opencontainers.image.source`` label in the image.

See https://docs.github.com/en/packages/learn-github-packages/connecting-a-repository-to-a-package

Naming convention for the GitHub packages.

Images with a commit SHA (built for pull requests and pushes)

.. code-block:: bash

  ghcr.io/apache/airflow/<BRANCH>/ci/python<X.Y>:<COMMIT_SHA>         - for CI images
  ghcr.io/apache/airflow/<BRANCH>/prod/python<X.Y>:<COMMIT_SHA>       - for production images

We do not push Base Python images and prod-build images when we prepare COMMIT builds, because those
images are never rebuilt locally, so there is no need to store base images specific for those builds.

Latest images (pushed when main merge succeeds):

.. code-block:: bash

  ghcr.io/apache/airflow/<BRANCH>/python:<X.Y>-slim-buster        - for base Python images
  ghcr.io/apache/airflow/<BRANCH>/ci/python<X.Y>:latest           - for CI images
  ghcr.io/apache/airflow/<BRANCH>/ci-manifest/python<X.Y>:latest  - for CI Manifest images
  ghcr.io/apache/airflow/<BRANCH>/prod/python<X.Y>:latest         - for production images
  ghcr.io/apache/airflow/<BRANCH>/prod-build/python<X.Y>:latest   - for production build stage

You can see all the current GitHub images at `<https://github.com/apache/airflow/packages>`_

You can read more about the CI configuration and how CI builds are using GitHub images
in `<CI.rst>`_.

Note that you need to be committer and have the right to push to GitHub and you need to
be logged in to the registry. Only committers can push images directly. You need to login with your
Personal Access Token with "packages" write scope to be able to push to those repositories or pull from them
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

This can be done with ``Breeze`` command line which has easy-to-use tool to manage those images. For
example:

Force building Python 3.6 CI image using local cache and pushing it container registry:

.. code-block:: bash

  ./breeze build-image --python 3.6 --force-build-images --check-if-base-python-image-updated --build-cache-local
  ./breeze push-image --python 3.6

Building Python 3.8 CI image using cache pulled from GitHub Container Registry and pushing it back:

.. code-block:: bash

  ./breeze build-image --python 3.8
  ./breeze push-image --python 3.8

You can also pull and run images being result of a specific CI run in GitHub Actions. This is a powerful
tool that allows to reproduce CI failures locally, enter the images and fix them much faster. It is enough
to pass ``--github-image-id`` and the registry and Breeze will download and execute commands using
the same image that was used during the CI build.

For example this command will run the same Python 3.8 image as was used in build identified with
9a621eaa394c0a0a336f8e1b31b35eff4e4ee86e commit SHA  with enabled Kerberos integration.

.. code-block:: bash

  ./breeze --github-image-id 9a621eaa394c0a0a336f8e1b31b35eff4e4ee86e \
    --python 3.8 --integration kerberos

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

.. code-block:: bash

  docker build . -f Dockerfile.ci \
    --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" \
    --build-arg ADDITIONAL_AIRFLOW_EXTRAS="jdbc"
    --build-arg ADDITIONAL_PYTHON_DEPS="pandas"
    --build-arg ADDITIONAL_DEV_APT_DEPS="gcc g++"
    --build-arg ADDITIONAL_RUNTIME_APT_DEPS="default-jre-headless"
    --tag my-image:0.0.1


the same image can be built using ``breeze`` (it supports auto-completion of the options):

.. code-block:: bash

  ./breeze build-image -f Dockerfile.ci \
      --production-image  --python 3.7 \
      --additional-extras=jdbc --additional-python-deps="pandas" \
      --additional-dev-apt-deps="gcc g++" --additional-runtime-apt-deps="default-jre-headless"

You can customize more aspects of the image - such as additional commands executed before apt dependencies
are installed, or adding extra sources to install your dependencies from. You can see all the arguments
described below but here is an example of rather complex command to customize the image
based on example in `this comment <https://github.com/apache/airflow/issues/8605#issuecomment-690065621>`_:

.. code-block:: bash

  docker build . -f Dockerfile.ci \
    --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" \
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
    --build-arg ADDITIONAL_DEV_ENV_VARS="ACCEPT_EULA=Y" \
    --build-arg ADDITIONAL_RUNTIME_APT_COMMAND="curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add --no-tty - && curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list" \
    --build-arg ADDITIONAL_RUNTIME_APT_DEPS="msodbcsql17 unixodbc git procps vim" \
    --build-arg ADDITIONAL_RUNTIME_ENV_VARS="ACCEPT_EULA=Y" \
    --tag my-image:0.0.1

CI image build arguments
------------------------

The following build arguments (``--build-arg`` in docker build command) can be used for CI images:

+------------------------------------------+------------------------------------------+------------------------------------------+
| Build argument                           | Default value                            | Description                              |
+==========================================+==========================================+==========================================+
| ``PYTHON_BASE_IMAGE``                    | ``python:3.6-slim-buster``               | Base Python image                        |
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
| ``UPGRADE_TO_NEWER_DEPENDENCIES``        | ``false``                                | If set to true, the dependencies are     |
|                                          |                                          | upgraded to newer versions matching      |
|                                          |                                          | setup.py before installation.            |
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
| ``ADDITIONAL_PYTHON_DEPS``               |                                          | additional Python dependencies to        |
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
| ``AIRFLOW_PIP_VERSION``                  | ``21.1``                                 | PIP version used.                        |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``PIP_PROGRESS_BAR``                     | ``on``                                   | Progress bar for PIP installation        |
+------------------------------------------+------------------------------------------+------------------------------------------+

Here are some examples of how CI images can built manually. CI is always built from local sources.

This builds the CI image in version 3.7 with default extras ("all").

.. code-block:: bash

  docker build . -f Dockerfile.ci --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" --tag my-image:0.0.1


This builds the CI image in version 3.6 with "gcp" extra only.

.. code-block:: bash

  docker build . -f Dockerfile.ci --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" \
    --build-arg AIRFLOW_EXTRAS=gcp --tag my-image:0.0.1


This builds the CI image in version 3.6 with "apache-beam" extra added.

.. code-block:: bash

  docker build . -f Dockerfile.ci --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" \
    --build-arg ADDITIONAL_AIRFLOW_EXTRAS="apache-beam" --tag my-image:0.0.1

This builds the CI image in version 3.6 with "mssql" additional package added.

.. code-block:: bash

  docker build . -f Dockerfile.ci --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" \
    --build-arg ADDITIONAL_PYTHON_DEPS="mssql" --tag my-image:0.0.1

This builds the CI image in version 3.6 with "gcc" and "g++" additional apt dev dependencies added.

.. code-block::

  docker build . -f Dockerfile.ci --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" \
    --build-arg ADDITIONAL_DEV_APT_DEPS="gcc g++" --tag my-image:0.0.1

This builds the CI image in version 3.6 with "jdbc" extra and "default-jre-headless" additional apt runtime dependencies added.

.. code-block::

  docker build . -f Dockerfile.ci --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" \
    --build-arg AIRFLOW_EXTRAS=jdbc --build-arg ADDITIONAL_RUNTIME_DEPS="default-jre-headless" \
    --tag my-image:0.0.1

CI Image manifests
------------------

Together with the main CI images we also build and push image manifests. Those manifests are very small images
that contain only content of randomly generated file at the 'crucial' part of the CI image building.
This is in order to be able to determine very quickly if the image in the docker registry has changed a
lot since the last time. Unfortunately docker registry has no anonymous
way of querying image details via API. You really need to download the image to inspect it.
We workaround it in the way that always when we build the image we build a very small image manifest
containing randomly generated UUID and push it to registry together with the main CI image.
The tag for the manifest image reflects the image it refers to with added ``-manifest`` suffix.
The manifest image for ``ghcr.io/apache/airflow/main/ci/python3.6`` is named
``ghcr.io/apache/airflow/main/ci-manifest/python3.6``.

The image is quickly pulled (it is really, really small) when important files change and the content
of the randomly generated UUID is compared with the one in our image. If the contents are different
this means that the user should rebase to latest main and rebuild the image with pulling the image from
the repo as this will likely be faster than rebuilding the image locally.

The random UUID is generated right after pre-cached pip install is run - and usually it means that
significant changes have been made to apt packages or even the base Python image has changed.

Working with the images
=======================

Pulling the Latest Images
-------------------------

Sometimes the image needs to be refreshed from the GitHub Container Registry - because you have an outdated
version. You can do it via the ``--force-pull-images`` flag to force pulling the latest images.

For production image:

.. code-block:: bash

  ./breeze build-image --force-pull-images --production-image

For CI image Breeze automatically uses force pulling in case it determines that your image is very outdated,
however uou can also force it with the same flag.

.. code-block:: bash

  ./breeze build-image --force-pull-images

Refreshing Base Python images
-----------------------------

Python base images are updated from time-to-time, usually as a result of implementing security fixes.
When you build your image locally using ``docker build`` you use the version of image that you have locally.
For the CI builds using ``breeze`` we use the image that is stored in our repository in order to use cache
efficiently. However CI push build have ``CHECK_IF_BASE_PYTHON_IMAGE_UPDATED`` variable set to ``true``
which checks if the image has been released and will pull it and rebuild it if needed

.. code-block:: bash

    #/bin/bash
    export GITHUB_REPOSITORY="apache/airflow"
    export FORCE_ANSWER_TO_QUESTIONS="true"
    export CI="true"

    for python_version in "3.6" "3.7" "3.8"
    do
            ./breeze build-image --python ${python_version} --build-cache-local \
                    --check-if-python-base-image-updated --verbose
            ./breeze build-image --python ${python_version} --build-cache-local \
                    --production-image --verbose
            ./breeze push-image
            ./breeze push-image --production-image
    done

Running the CI image
--------------------

The entrypoint in the CI image contains all the initialisation needed for tests to be immediately executed.
It is copied from ``scripts/in_container/entrypoint_ci.sh``.

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
