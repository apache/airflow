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

.. _build:build_image:

Building the image
==================

Before you dive-deeply in the way how the Airflow Image is built, let us first explain why you might need
to build the custom container image and we show a few typical ways you can do it.

Quick start scenarios of image extending
----------------------------------------

The most common scenarios where you want to build your own image are adding a new ``apt`` package,
adding a new ``PyPI`` dependency (either individually or via requirements.txt) and embedding DAGs
into the image.

Example Dockerfiles for those scenarios are below, and you can read further
for more complex cases which might involve either extending or customizing the image. You will find
more information about more complex scenarios below, but if your goal is to quickly extend the Airflow
image with new provider, package, etc. then here is a quick start for you.

Adding new ``apt`` package
..........................

The following example adds ``vim`` to the Airflow image. When adding packages via ``apt`` you should
switch to the ``root`` user when running the ``apt`` commands, but do not forget to switch back to the
``airflow`` user after installation is complete.

.. exampleinclude:: docker-examples/extending/add-apt-packages/Dockerfile
    :language: Dockerfile
    :start-after: [START Dockerfile]
    :end-before: [END Dockerfile]


Adding new ``PyPI`` packages individually
.........................................

The following example adds ``lxml`` python package from PyPI to the image. When adding packages via
``pip`` you need to use the ``airflow`` user rather than ``root``. Attempts to install ``pip`` packages
as ``root`` will fail with an appropriate error message.

.. note::
   In the example below, we also add apache-airflow package to be installed - in the very same version
   that the image version you used it from. This is not strictly necessary, but it is a good practice
   to always install the same version of apache-airflow as the one you are using. This way you can
   be sure that the version you are using is the same as the one you are extending. In some cases where
   your new packages have conflicting dependencies, ``pip`` might decide to downgrade or upgrade
   apache-airflow for you, so adding it explicitly is a good practice - this way if you have conflicting
   requirements, you will get an error message with conflict information, rather than a surprise
   downgrade or upgrade of airflow. If you upgrade airflow base image, you should also update the version
   to match the new version of airflow.

.. note::
   Creating custom images means that you need to maintain also a level of automation as you need to re-create the images
   when either the packages you want to install or Airflow is upgraded. Please do not forget about keeping these scripts.
   Also keep in mind, that in cases when you run pure Python tasks, you can use the
   `Python Virtualenv functions <https://airflow.apache.org/docs/apache-airflow/stable/howto/operator/python.html#pythonvirtualenvoperator>`_
   which will dynamically source and install python dependencies during runtime. With Airflow 2.8.0 Virtualenvs can also be cached.

.. exampleinclude:: docker-examples/extending/add-pypi-packages/Dockerfile
    :language: Dockerfile
    :start-after: [START Dockerfile]
    :end-before: [END Dockerfile]

Adding packages from requirements.txt
.....................................

The following example adds few python packages from ``requirements.txt`` from PyPI to the image.
Note that similarly when adding individual packages, you need to use the ``airflow`` user rather than
``root``. Attempts to install ``pip`` packages as ``root`` will fail with an appropriate error message.

.. note::
   In the example below, we also add apache-airflow package to be installed - in the very same version
   that the image version you used it from. This is not strictly necessary, but it is a good practice
   to always install the same version of apache-airflow as the one you are using. This way you can
   be sure that the version you are using is the same as the one you are extending. In some cases where
   your new packages have conflicting dependencies, ``pip`` might decide to downgrade or upgrade
   apache-airflow for you, so adding it explicitly is a good practice - this way if you have conflicting
   requirements, you will get an error message with conflict information, rather than a surprise
   downgrade or upgrade of airflow. If you upgrade airflow base image, you should also update the version
   to match the new version of airflow.


.. exampleinclude:: docker-examples/extending/add-requirement-packages/Dockerfile
    :language: Dockerfile
    :start-after: [START Dockerfile]
    :end-before: [END Dockerfile]

.. exampleinclude:: docker-examples/extending/add-requirement-packages/requirements.txt
    :language: text


Embedding DAGs
..............

The following example adds ``test_dag.py`` to your image in the ``/opt/airflow/dags`` folder.

.. exampleinclude:: docker-examples/extending/embedding-dags/Dockerfile
    :language: Dockerfile
    :start-after: [START Dockerfile]
    :end-before: [END Dockerfile]


.. exampleinclude:: docker-examples/extending/embedding-dags/test_dag.py
    :language: Python
    :start-after: [START dag]
    :end-before: [END dag]

Add Airflow configuration with environment variables
....................................................

The following example adds airflow configuration to the image. ``airflow.cfg`` file in
``$AIRFLOW_HOME`` directory contains Airflow's configuration. You can set options with environment variables for those Airflow's configuration by using this format:
:envvar:`AIRFLOW__{SECTION}__{KEY}` (note the double underscores).


.. exampleinclude:: docker-examples/extending/add-airflow-configuration/Dockerfile
    :language: Dockerfile
    :start-after: [START Dockerfile]
    :end-before: [END Dockerfile]


Extending vs. customizing the image
-----------------------------------

You might want to know very quickly whether you need to extend or customize the existing image
for Apache Airflow. This chapter gives you a short answer to those questions.

Here is the comparison of the two approaches:

+----------------------------------------------------+-----------+-------------+
|                                                    | Extending | Customizing |
+====================================================+===========+=============+
| Uses familiar 'FROM' pattern of image building     | Yes       | No          |
+----------------------------------------------------+-----------+-------------+
| Requires only basic knowledge about images         | Yes       | No          |
+----------------------------------------------------+-----------+-------------+
| Builds quickly                                     | Yes       | No          |
+----------------------------------------------------+-----------+-------------+
| Produces image heavily optimized for size          | No        | Yes         |
+----------------------------------------------------+-----------+-------------+
| Can build from custom airflow sources (forks)      | No        | Yes         |
+----------------------------------------------------+-----------+-------------+
| Can build on air-gapped system                     | No        | Yes         |
+----------------------------------------------------+-----------+-------------+

TL;DR; If you have a need to build custom image, it is easier to start with "Extending". However, if your
dependencies require compilation steps or when your require to build the image from security vetted
packages, switching to "Customizing" the image provides much more optimized images. For example,
if we compare equivalent images built by "Extending" and "Customization", they end up being
1.1GB and 874MB respectively - a 20% improvement in size for the Customized image.

.. note::

  You can also combine both - customizing & extending the image in one. You can build your
  optimized base image first using ``customization`` method (for example by your admin team) with all
  the heavy compilation required dependencies and you can publish it in your registry and let others
  ``extend`` your image using ``FROM`` and add their own lightweight dependencies. This reflects well
  the split where typically "Casual" users will Extend the image and "Power-users" will customize it.

Airflow Summit 2020's `Production Docker Image <https://youtu.be/wDr3Y7q2XoI>`_ talk provides more
details about the context, architecture and customization/extension methods for the Production Image.

Why customizing the image ?
---------------------------

The Apache Airflow community, releases Docker Images which are ``reference images`` for Apache Airflow.
However, Airflow has more than 60 community managed providers (installable via extras) and some of the
default extras/providers installed are not used by everyone, sometimes others extras/providers
are needed, sometimes (very often actually) you need to add your own custom dependencies,
packages or even custom providers.

In Kubernetes and Docker terms this means that you need another image with your specific requirements.
This is why you should learn how to build your own Docker (or more properly Container) image.
You might be tempted to use the ``reference image`` and dynamically install the new packages while
starting your containers, but this is a bad idea for multiple reasons - starting from fragility of the build
and ending with the extra time needed to install those packages - which has to happen every time every
container starts. The only viable way to deal with new dependencies and requirements in production is to
build and use your own image. You should only use installing dependencies dynamically in case of
"hobbyist" and "quick start" scenarios when you want to iterate quickly to try things out and later
replace it with your own images.

Building images primer
----------------------

.. note::
  The ``Dockerfile`` does not strictly follow the `SemVer <https://semver.org/>`_ approach of
  Apache Airflow when it comes to features and backwards compatibility. While Airflow code strictly
  follows it, the ``Dockerfile`` is really a way to conveniently package Airflow using standard container
  approach, occasionally there are some changes in the building process or in the entrypoint of the image
  that require slight adaptation. Details of changes and adaptation needed can be found in the
  :doc:`Changelog <changelog>`.

There are several most-typical scenarios that you will encounter and here is a quick recipe on how to achieve
your goal quickly. In order to understand details you can read further, but for the simple cases using
typical tools here are the simple examples.

In the simplest case building your image consists of those steps:

1) Create your own ``Dockerfile`` (name it ``Dockerfile``) where you add:

* information what your image should be based on (for example ``FROM: apache/airflow:|airflow-version|-python3.8``

* additional steps that should be executed in your image (typically in the form of ``RUN <command>``)

2) Build your image. This can be done with ``docker`` CLI tools and examples below assume ``docker`` is used.
   There are other tools like ``kaniko`` or ``podman`` that allow you to build the image, but ``docker`` is
   so far the most popular and developer-friendly tool out there. Typical way of building the image looks
   like follows (``my-image:0.0.1`` is the custom tag of your image containing version).
   In case you use some kind of registry where you will be using the image from, it is usually named
   in the form of ``registry/image-name``. The name of the image has to be configured for the deployment
   method your image will be deployed. This can be set for example as image name in the
   :doc:`apache-airflow:howto/docker-compose/index` or in the :doc:`helm-chart:index`.

.. code-block:: shell

   docker build . -f Dockerfile --pull --tag my-image:0.0.1

3) [Optional] Test the image. Airflow contains tool that allows you to test the image. This step, however,
   requires locally checked out or extracted Airflow sources. If you happen to have the sources you can
   test the image by running this command (in airflow root folder). The output will tell you if the image
   is "good-to-go".

.. code-block:: shell

   ./scripts/ci/tools/verify_docker_image.sh PROD my-image:0.0.1

4) Once you build the image locally you have usually several options to make them available for your deployment:

* For ``docker-compose`` deployment, if you've already built your image, and want to continue
  building the image manually when needed with ``docker build``, you can edit the
  docker-compose.yaml and replace the "apache/airflow:<version>" image with the
  image you've just built ``my-image:0.0.1`` - it will be used from your local Docker
  Engine cache. You can also simply set ``AIRFLOW_IMAGE_NAME`` variable to
  point to your image and ``docker-compose`` will use it automatically without having
  to modify the file.

* Also for ``docker-compose`` deployment, you can delegate image building to the docker-compose.
  To do that - open your ``docker-compose.yaml`` file and search for the phrase "In order to add custom dependencies".
  Follow these instructions of commenting the "image" line and uncommenting the "build" line.
  This is a standard docker-compose feature and you can read about it in
  `Docker Compose build reference <https://docs.docker.com/compose/reference/build/>`_.
  Run ``docker-compose build`` to build the images. Similarly as in the previous case, the
  image is stored in Docker engine cache and Docker Compose will use it from there.
  The ``docker-compose build`` command uses the same ``docker build`` command that
  you can run manually under-the-hood.

* For some - development targeted - Kubernetes deployments you can load the images directly to
  Kubernetes clusters. Clusters such as ``kind`` or ``minikube`` have dedicated ``load`` method to load the
  images to the cluster.

* Last but not least - you can push your image to a remote registry which is the most common way
  of storing and exposing the images, and it is most portable way of publishing the image. Both
  Docker-Compose and Kubernetes can make use of images exposed via registries.


Extending the image
-------------------

Extending the image is easiest if you just need to add some dependencies that do not require
compiling. The compilation framework of Linux (so called ``build-essential``) is pretty big, and
for the production images, size is really important factor to optimize for, so our Production Image
does not contain ``build-essential``. If you need a compiler like gcc or g++ or make/cmake etc. - those
are not found in the image and it is recommended that you follow the "customize" route instead.

How to extend the image - it is something you are most likely familiar with - simply
build a new image using Dockerfile's ``FROM`` directive and add whatever you need. Then you can add your
Debian dependencies with ``apt`` or PyPI dependencies with ``pip install`` or any other stuff you need.

Base images
...........

There are two types of images you can extend your image from:

1) Regular Airflow image that contains the most common extras and providers, and all supported backend
   database clients for AMD64 platform and Postgres for ARM64 platform.

2) Slim Airflow image, which is a minimal image, contains all supported backends database clients installed
   for AMD64 platform and Postgres for ARM64 platform, but contains no extras or providers, except
   the 4 default providers.

.. note:: Database clients and database providers in slim images
    Slim images come with database clients preinstalled for your convenience, however the default
    providers included do not include any database provider. You will still need to manually install
    any database provider you need

.. note:: Differences of slim image vs. regular image.

    The slim image is small comparing to regular image (~500 MB vs ~1.1GB) and you might need to add a
    lot more packages and providers in order to make it useful for your case (but if you use only a
    small subset of providers, it might be a good starting point for you).

    The slim images might have dependencies in different versions than those used when providers are
    preinstalled, simply because core Airflow might have less limits on the versions on its own.
    When you install some providers they might require downgrading some dependencies if the providers
    require different limits for the same dependencies.

Naming conventions for the images:

+----------------+-----------------------+---------------------------------+--------------------------------------+
| Image          | Python                | Standard image                  | Slim image                           |
+================+=======================+=================================+======================================+
| Latest default | 3.8                   | apache/airflow:latest           | apache/airflow:slim-latest           |
+----------------+-----------------------+---------------------------------+--------------------------------------+
| Default        | 3.8                   | apache/airflow:X.Y.Z            | apache/airflow:slim-X.Y.Z            |
+----------------+-----------------------+---------------------------------+--------------------------------------+
| Latest         | 3.8,3.9,3.10,3.11     | apache/airflow:latest-pythonN.M | apache/airflow:slim-latest-pythonN.M |
+----------------+-----------------------+---------------------------------+--------------------------------------+
| Specific       | 3.8,3.9,3.10,3.11     | apache/airflow:X.Y.Z-pythonN.M  | apache/airflow:slim-X.Y.Z-pythonN.M  |
+----------------+-----------------------+---------------------------------+--------------------------------------+

* The "latest" image is always the latest released stable version available.

.. spelling:word-list::

     pythonN

Important notes for the base images
-----------------------------------

You should be aware, about a few things

* The production image of airflow uses "airflow" user, so if you want to add some of the tools
  as ``root`` user, you need to switch to it with ``USER`` directive of the Dockerfile and switch back to
  ``airflow`` user when you are done. Also you should remember about following the
  `best practices of Dockerfiles <https://docs.docker.com/develop/develop-images/dockerfile_best-practices/>`_
  to make sure your image is lean and small.

* You can use regular ``pip install`` commands (and as of Dockerfile coming in Airflow 2.9 also
  ``uv pip install`` - experimental) to install PyPI packages. Regular ``install`` commands should be used,
  however you should remember to add ``apache-airflow==${AIRFLOW_VERSION}`` to the command to avoid
  accidentally upgrading or downgrading the version of Apache Airflow. Depending on the scenario you might
  also use constraints file. As of Dockerfile available in Airflow 2.9.0, the constraints file used to
  build the image is available in ``${HOME}/constraints.txt.``

* The PyPI dependencies in Apache Airflow are installed in the ``~/.local`` virtualenv, of the "airflow" user,
  so PIP packages are installed to ``~/.local`` folder as if the ``--user`` flag was specified when running
  PIP. This has the effect that when you create a virtualenv with ``--system-site-packages`` flag, the
  virtualenv created will automatically have all the same packages installed as local airflow installation.
  Note also that using ``--no-cache-dir`` in ``pip`` or ``--no-cache`` in ``uv`` is a good idea that can
  help to make your image smaller.

* If your apt, or PyPI dependencies require some of the ``build-essential`` or other packages that need
  to compile your python dependencies, then your best choice is to follow the "Customize the image" route,
  because you can build a highly-optimized (for size) image this way. However, it requires you to use
  the Dockerfile that is released as part of Apache Airflow sources (also available at
  `Dockerfile <https://github.com/apache/airflow/blob/main/Dockerfile>`_).

* You can also embed your dags in the image by simply adding them with COPY directive of Airflow.
  The DAGs in production image are in ``/opt/airflow/dags`` folder.

* You can build your image without any need for Airflow sources. It is enough that you place the
  ``Dockerfile`` and any files that are referred to (such as DAG files) in a separate directory and run
  a command ``docker build . --pull --tag my-image:my-tag`` (where ``my-image`` is the name you want to name it
  and ``my-tag`` is the tag you want to tag the image with.

* If your way of extending image requires to create writable directories, you MUST remember about adding
  ``umask 0002`` step in your RUN command. This is necessary in order to accommodate our approach for
  running the image with an arbitrary user. Such user will always run with ``GID=0`` -
  the entrypoint will prevent non-root GIDs. You can read more about it in
  :ref:`arbitrary docker user <arbitrary-docker-user>` documentation for the entrypoint. The
  ``umask 0002`` is set as default when you enter the image, so any directories you create by default
  in runtime, will have ``GID=0`` and will be group-writable.

Examples of image extending
---------------------------

Example of setting own Airflow Provider packages
................................................

The :ref:`Airflow Providers <providers:community-maintained-providers>` are released independently of core
Airflow and sometimes you might want to upgrade specific providers only to fix some problems or
use features available in that provider version. Here is an example of how you can do it

.. exampleinclude:: docker-examples/extending/custom-providers/Dockerfile
    :language: Dockerfile
    :start-after: [START Dockerfile]
    :end-before: [END Dockerfile]

Example of adding Airflow Provider package and ``apt`` package
..............................................................

The following example adds ``apache-spark`` airflow-providers which requires both ``java`` and
python package from PyPI.

.. exampleinclude:: docker-examples/extending/add-providers/Dockerfile
    :language: Dockerfile
    :start-after: [START Dockerfile]
    :end-before: [END Dockerfile]

Example of adding ``apt`` package
.................................

The following example adds ``vim`` to the airflow image.

.. exampleinclude:: docker-examples/extending/add-apt-packages/Dockerfile
    :language: Dockerfile
    :start-after: [START Dockerfile]
    :end-before: [END Dockerfile]

Example of adding ``PyPI`` package
..................................

The following example adds ``lxml`` python package from PyPI to the image.

.. exampleinclude:: docker-examples/extending/add-pypi-packages/Dockerfile
    :language: Dockerfile
    :start-after: [START Dockerfile]
    :end-before: [END Dockerfile]

Example of adding ``PyPI`` package with constraints
...................................................

The following example adds ``lxml`` python package from PyPI to the image with constraints that were
used to install airflow. This allows you to use the version of packages that you know were tested with the
given version of Airflow. You can also use it if you do not want to use potentially newer versions
that were released after the version of Airflow you are using.

.. exampleinclude:: docker-examples/extending/add-pypi-packages-constraints/Dockerfile
    :language: Dockerfile
    :start-after: [START Dockerfile]
    :end-before: [END Dockerfile]


Example of adding ``PyPI`` package with uv
..........................................

The following example adds ``lxml`` python package from PyPI to the image using ``uv``. This is an
experimental feature as ``uv`` is a very fast but also very new tool in the Python ecosystem.

.. exampleinclude:: docker-examples/extending/add-pypi-packages-uv/Dockerfile
    :language: Dockerfile
    :start-after: [START Dockerfile]
    :end-before: [END Dockerfile]


Example of adding packages from requirements.txt
................................................

The following example adds few python packages from ``requirements.txt`` from PyPI to the image.
Note that similarly when adding individual packages, you need to use the ``airflow`` user rather than
``root``. Attempts to install ``pip`` packages as ``root`` will fail with an appropriate error message.

.. note::
   In the example below, we also add apache-airflow package to be installed - in the very same version
   that the image version you used it from. This is not strictly necessary, but it is a good practice
   to always install the same version of apache-airflow as the one you are using. This way you can
   be sure that the version you are using is the same as the one you are extending. In some cases where
   your new packages have conflicting dependencies, ``pip`` might decide to downgrade or upgrade
   apache-airflow for you, so adding it explicitly is a good practice - this way if you have conflicting
   requirements, you will get an error message with conflict information, rather than a surprise
   downgrade or upgrade of airflow. If you upgrade airflow base image, you should also update the version
   to match the new version of airflow.

.. exampleinclude:: docker-examples/extending/add-requirement-packages/Dockerfile
    :language: Dockerfile
    :start-after: [START Dockerfile]
    :end-before: [END Dockerfile]

.. exampleinclude:: docker-examples/extending/add-requirement-packages/requirements.txt
    :language: text


Example when writable directory is needed
.........................................

The following example adds a new directory that is supposed to be writable for any arbitrary user
running the container.

.. exampleinclude:: docker-examples/extending/writable-directory/Dockerfile
    :language: Dockerfile
    :start-after: [START Dockerfile]
    :end-before: [END Dockerfile]


Example when you add packages requiring compilation
...................................................

The following example adds ``mpi4py`` package which requires both ``build-essential`` and ``mpi compiler``.

.. exampleinclude:: docker-examples/extending/add-build-essential-extend/Dockerfile
    :language: Dockerfile
    :start-after: [START Dockerfile]
    :end-before: [END Dockerfile]

The size of this image is ~ 1.1 GB when build. As you will see further, you can achieve 20% reduction in
size of the image in case you use "Customizing" rather than "Extending" the image.

Example when you want to embed DAGs
...................................

The following example adds ``test_dag.py`` to your image in the ``/opt/airflow/dags`` folder.

.. exampleinclude:: docker-examples/extending/embedding-dags/Dockerfile
    :language: Dockerfile
    :start-after: [START Dockerfile]
    :end-before: [END Dockerfile]


.. exampleinclude:: docker-examples/extending/embedding-dags/test_dag.py
    :language: Python
    :start-after: [START dag]
    :end-before: [END dag]

Example of changing airflow configuration using environment variables
.....................................................................

The following example adds airflow configuration changes to the airflow image.

.. exampleinclude:: docker-examples/extending/add-airflow-configuration/Dockerfile
    :language: Dockerfile
    :start-after: [START Dockerfile]
    :end-before: [END Dockerfile]

Customizing the image
---------------------

.. warning::

    In Dockerfiles released in Airflow 2.8.0, images are based on ``Debian Bookworm`` images as base images.

.. note::
    You can usually use the latest ``Dockerfile`` released by Airflow to build previous Airflow versions.
    Note, however, that there are slight changes in the Dockerfile and entrypoint scripts that can make it
    behave slightly differently, depending which Dockerfile version you used. Details of what has changed
    in each of the released versions of Docker image can be found in the :doc:`Changelog <changelog>`.

Prerequisites for building customized docker image:

* You need to enable `Buildkit <https://docs.docker.com/develop/develop-images/build_enhancements/>`_ to
  build the image. This can be done by setting ``DOCKER_BUILDKIT=1`` as an environment variable
  or by installing `the buildx plugin <https://docs.docker.com/buildx/working-with-buildx/>`_
  and running ``docker buildx build`` command. Docker Desktop has ``Buildkit`` enabled by default.

* You need to have a new Docker installed to handle ``1.4`` syntax of the Dockerfile.
  Docker version ``23.0.0`` and above are known to work.


Before attempting to customize the image, you need to download flexible and customizable ``Dockerfile``.
You can extract the officially released version of the Dockerfile from the
`released sources <https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-sources.html>`_.
You can also conveniently download the latest released version
`from GitHub <https://raw.githubusercontent.com/apache/airflow/|airflow-version|/Dockerfile>`_. You can save it
in any directory - there is no need for any other files to be present there. If you wish to use your own
files (for example custom configuration of ``pip`` or your own ``requirements`` or custom dependencies,
you need to use ``DOCKER_CONTEXT_FILES`` build arg and place the files in the directory pointed at by
the arg (see :ref:`Using docker context files <using-docker-context-files>` for details).

Customizing the image is an optimized way of adding your own dependencies to the image - better
suited to prepare highly optimized (for size) production images, especially when you have dependencies
that require to be compiled before installing (such as ``mpi4py``).

It also allows more sophisticated usages, needed by "Power-users" - for example using forked version
of Airflow, or building the images from security-vetted sources.

The big advantage of this method is that it produces optimized image even if you need some compile-time
dependencies that are not needed in the final image.

The disadvantage it that building the image takes longer and it requires you to use
the Dockerfile that is released as part of Apache Airflow sources.

The disadvantage is that the pattern of building Docker images with ``--build-arg`` is less familiar
to developers of such images. However, it is quite well-known to "power-users". That's why the
customizing flow is better suited for those users who have more familiarity and have more custom
requirements.

The image also usually builds much longer than the equivalent "Extended" image because instead of
extending the layers that are already coming from the base image, it rebuilds the layers needed
to add extra dependencies needed at early stages of image building.

When customizing the image you can choose a number of options how you install Airflow:

* From the PyPI releases (default)
* From the custom installation sources - using additional/replacing the original apt or PyPI repositories
* From local sources. This is used mostly during development.
* From tag or branch, or specific commit from a GitHub Airflow repository (or fork). This is particularly
  useful when you build image for a custom version of Airflow that you keep in your fork and you do not
  want to release the custom Airflow version to PyPI.
* From locally stored binary packages for Airflow, Airflow Providers and other dependencies. This is
  particularly useful if you want to build Airflow in a highly-secure environment where all such packages
  must be vetted by your security team and stored in your private artifact registry. This also
  allows to build airflow image in an air-gaped environment.
* Side note. Building ``Airflow`` in an ``air-gaped`` environment sounds pretty funny, doesn't it?

You can also add a range of customizations while building the image:

* base python image you use for Airflow
* version of Airflow to install
* extras to install for Airflow (or even removing some default extras)
* additional apt/python dependencies to use while building Airflow (DEV dependencies)
* add ``requirements.txt`` file to ``docker-context-files`` directory to add extra requirements
* additional apt/python dependencies to install for runtime version of Airflow (RUNTIME dependencies)
* additional commands and variables to set if needed during building or preparing Airflow runtime
* choosing constraint file to use when installing Airflow

Additional explanation is needed for the last point. Airflow uses constraints to make sure
that it can be predictably installed, even if some new versions of Airflow dependencies are
released (or even dependencies of our dependencies!). The docker image and accompanying scripts
usually determine automatically the right versions of constraints to be used based on the Airflow
version installed and Python version. For example 2.0.2 version of Airflow installed from PyPI
uses constraints from ``constraints-2.0.2`` tag). However, in some cases - when installing airflow from
GitHub for example - you have to manually specify the version of constraints used, otherwise
it will default to the latest version of the constraints which might not be compatible with the
version of Airflow you use.

You can also download any version of Airflow constraints and adapt it with your own set of
constraints and manually set your own versions of dependencies in your own constraints and use the version
of constraints that you manually prepared.

You can read more about constraints in :doc:`apache-airflow:installation/installing-from-pypi`

Note that if you place ``requirements.txt`` in the ``docker-context-files`` folder, it will be
used to install all requirements declared there. It is recommended that the file
contains specified version of dependencies to add with ``==`` version specifier, to achieve
stable set of requirements, independent if someone releases a newer version. However, you have
to make sure to update those requirements and rebuild the images to account for latest security fixes.

.. _using-docker-context-files:

Using docker-context-files
--------------------------

When customizing the image, you can optionally make Airflow install custom binaries or provide custom
configuration for your pip in ``docker-context-files``. In order to enable it, you need to add
``--build-arg DOCKER_CONTEXT_FILES=docker-context-files`` build arg when you build the image.
You can pass any subdirectory of your docker context, it will always be mapped to ``/docker-context-files``
during the build.

You can use ``docker-context-files`` for the following purposes:

* you can place ``requirements.txt`` and add any ``pip`` packages you want to install in the
  ``docker-context-file`` folder. Those requirements will be automatically installed during the build.

.. note::
   In the example below, we also add apache-airflow package to be installed - in the very same version
   that the image version you used it from. This is not strictly necessary, but it is a good practice
   to always install the same version of apache-airflow as the one you are using. This way you can
   be sure that the version you are using is the same as the one you are extending. In some cases where
   your new packages have conflicting dependencies, ``pip`` might decide to downgrade or upgrade
   apache-airflow for you, so adding it explicitly is a good practice - this way if you have conflicting
   requirements, you will get an error message with conflict information, rather than a surprise
   downgrade or upgrade of airflow. If you upgrade airflow base image, you should also update the version
   to match the new version of airflow.


.. exampleinclude:: docker-examples/customizing/own-requirements.sh
    :language: bash
    :start-after: [START build]
    :end-before: [END build]

* you can place ``pip.conf`` (and legacy ``.piprc``) in the ``docker-context-files`` folder and they
  will be used for all ``pip`` commands (for example you can configure your own sources
  or authentication mechanisms)

.. exampleinclude:: docker-examples/customizing/custom-pip.sh
    :language: bash
    :start-after: [START build]
    :end-before: [END build]


* you can place ``.whl`` packages that you downloaded and install them with
  ``INSTALL_PACKAGES_FROM_CONTEXT`` set to ``true`` . It's useful if you build the image in
  restricted security environments (see: :ref:`image-build-secure-environments` for details):

.. exampleinclude:: docker-examples/restricted/restricted_environments.sh
    :language: bash
    :start-after: [START download]
    :end-before: [END download]

.. note::
  You can also pass ``--build-arg DOCKER_CONTEXT_FILES=.`` if you want to place your ``requirements.txt``
  in the main directory without creating a dedicated folder. However, it is a good practice to keep any files
  that you copy to the image context in a sub-folder. This makes it easier to separate things that
  are used on the host from those that are passed in Docker context. Of course, by default when you run
  ``docker build .`` the whole folder is available as "Docker build context" and sent to the docker
  engine, but the ``DOCKER_CONTEXT_FILES`` are always copied to the ``build`` segment of the image so
  copying all your local folder might unnecessarily increase time needed to build the image and your
  cache will be invalidated every time any of the files in your local folder change.

.. warning::
  BREAKING CHANGE! As of Airflow 2.3.0 you need to specify additional flag:
  ``--build-arg DOCKER_CONTEXT_Files=docker-context-files`` in order to use the files placed
  in ``docker-context-files``. Previously that switch was not needed. Unfortunately this change is needed
  in order to enable ``Dockerfile`` as standalone Dockerfile without any extra files. As of Airflow 2.3.0
  the ``Dockerfile`` that is released with Airflow does not need any extra folders or files and can
  be copied and used from any folder. Previously you needed to copy Airflow sources together with the
  Dockerfile as some scripts were needed to make it work. With Airflow 2.3.0, we are using ``Buildkit``
  features that enable us to make the ``Dockerfile`` a completely standalone file that can be used "as-is".

Examples of image customizing
-----------------------------

.. _image-build-pypi:


Building from PyPI packages
...........................

This is the basic way of building the custom images from sources.

The following example builds the production image in version ``3.8`` with latest PyPI-released Airflow,
with default set of Airflow extras and dependencies. The latest PyPI-released Airflow constraints are used automatically.

.. exampleinclude:: docker-examples/customizing/stable-airflow.sh
    :language: bash
    :start-after: [START build]
    :end-before: [END build]

The following example builds the production image in version ``3.8`` with default extras from ``2.3.0`` Airflow
package. The ``2.3.0`` constraints are used automatically.

.. exampleinclude:: docker-examples/customizing/pypi-selected-version.sh
    :language: bash
    :start-after: [START build]
    :end-before: [END build]

The following example builds the production image in version ``3.8`` with additional airflow extras
(``mssql,hdfs``) from ``2.3.0`` PyPI package, and additional dependency (``oauth2client``).

.. exampleinclude:: docker-examples/customizing/pypi-extras-and-deps.sh
    :language: bash
    :start-after: [START build]
    :end-before: [END build]


The following example adds ``mpi4py`` package which requires both ``build-essential`` and ``mpi compiler``.

.. exampleinclude:: docker-examples/customizing/add-build-essential-custom.sh
    :language: bash
    :start-after: [START build]
    :end-before: [END build]

The above image is equivalent of the "extended" image from previous chapter but its size is only
874 MB. Comparing to 1.1 GB of the "extended image" this is about 230 MB less, so you can achieve ~20%
improvement in size of the image by using "customization" vs. extension. The saving can increase in case you
have more complex dependencies to build.


.. _image-build-optimized:

Building optimized images
.........................

The following example builds the production image in version ``3.8`` with additional airflow extras from
PyPI package but it includes additional apt dev and runtime dependencies.

The dev dependencies are those that require ``build-essential`` and usually need to involve recompiling
of some python dependencies so those packages might require some additional DEV dependencies to be
present during recompilation. Those packages are not needed at runtime, so we only install them for the
"build" time. They are not installed in the final image, thus producing much smaller images.
In this case pandas requires recompilation so it also needs gcc and g++ as dev APT dependencies.
The ``jre-headless`` does not require recompiling so it can be installed as the runtime APT dependency.

.. exampleinclude:: docker-examples/customizing/pypi-dev-runtime-deps.sh
    :language: bash
    :start-after: [START build]
    :end-before: [END build]

.. _image-build-uv:

Building prod images using UV as the package installer
......................................................

The following example builds the production image in default settings, but uses ``uv`` to build the image.
This is an experimental feature as ``uv`` is a very fast but also very new tool in the Python ecosystem.

.. exampleinclude:: docker-examples/customizing/use-uv.sh
    :language: bash
    :start-after: [START build]
    :end-before: [END build]

.. _image-build-mysql:

Building images with MySQL client
.................................

.. warning::

  By default Airflow images as of Airflow 2.8.0 use "MariaDB" client by default on both "X86_64" and "ARM64"
  platforms. However, you can also build images with MySQL client. The following example builds the
  production image in default Python version with "MySQL" client.

.. exampleinclude:: docker-examples/customizing/mysql-client.sh
    :language: bash
    :start-after: [START build]
    :end-before: [END build]


.. _image-build-github:


Building from GitHub
....................

This method is usually used for development purpose. But in case you have your own fork you can point
it to your forked version of source code without having to release it to PyPI. It is enough to have
a branch or tag in your repository and use the tag or branch in the URL that you point the installation to.

In case of GitHub builds you need to pass the constraints reference manually in case you want to use
specific constraints, otherwise the default ``constraints-main`` is used.

The following example builds the production image in version ``3.8`` with default extras from the latest main version and
constraints are taken from latest version of the constraints-main branch in GitHub.

.. exampleinclude:: docker-examples/customizing/github-main.sh
    :language: bash
    :start-after: [START build]
    :end-before: [END build]

The following example builds the production image with default extras from the
latest ``v2-*-test`` version and constraints are taken from the latest version of
the ``constraints-2-*`` branch in GitHub (for example ``v2-2-test`` branch matches ``constraints-2-2``).
Note that this command might fail occasionally as only the "released version" constraints when building a
version and "main" constraints when building main are guaranteed to work.

.. exampleinclude:: docker-examples/customizing/github-v2-2-test.sh
    :language: bash
    :start-after: [START build]
    :end-before: [END build]

You can also specify another repository to build from. If you also want to use different constraints
repository source, you must specify it as additional ``CONSTRAINTS_GITHUB_REPOSITORY`` build arg.

The following example builds the production image using ``potiuk/airflow`` fork of Airflow and constraints
are also downloaded from that repository.

.. exampleinclude:: docker-examples/customizing/github-different-repository.sh
    :language: bash
    :start-after: [START build]
    :end-before: [END build]

.. _image-build-custom:

Using custom installation sources
.................................

You can customize more aspects of the image - such as additional commands executed before apt dependencies
are installed, or adding extra sources to install your dependencies from. You can see all the arguments
described below but here is an example of rather complex command to customize the image
based on example in `this comment <https://github.com/apache/airflow/issues/8605#issuecomment-690065621>`_:

In case you need to use your custom PyPI package indexes, you can also customize PYPI sources used during
image build by adding a ``docker-context-files/pip.conf`` file when building the image.
This ``pip.conf`` will not be committed to the repository (it is added to ``.gitignore``) and it will not be
present in the final production image. It is added and used only in the build segment of the image.
Therefore this ``pip.conf`` file can safely contain list of package indexes you want to use,
usernames and passwords used for authentication. More details about ``pip.conf`` file can be found in the
`pip configuration <https://pip.pypa.io/en/stable/topics/configuration/>`_.

If you used the ``.piprc`` before (some older versions of ``pip`` used it for customization), you can put it
in the ``docker-context-files/.piprc`` file and it will be automatically copied to ``HOME`` directory
of the ``airflow`` user.

Note, that those customizations are only available in the ``build`` segment of the Airflow image and they
are not present in the ``final`` image. If you wish to extend the final image and add custom ``.piprc`` and
``pip.conf``, you should add them in your own Dockerfile used to extend the Airflow image.

Such customizations are independent of the way how airflow is installed.

.. note::
  Similar results could be achieved by modifying the Dockerfile manually (see below) and injecting the
  commands needed, but by specifying the customizations via build-args, you avoid the need of
  synchronizing the changes from future Airflow Dockerfiles. Those customizations should work with the
  future version of Airflow's official ``Dockerfile`` at most with minimal modifications od parameter
  names (if any), so using the build command for your customizations makes your custom image more
  future-proof.

The following - rather complex - example shows capabilities of:

* Adding airflow extras (slack, odbc)
* Adding PyPI dependencies (``azure-storage-blob, oauth2client, beautifulsoup4, dateparser, rocketchat_API,typeform``)
* Adding custom environment variables while installing ``apt`` dependencies - both DEV and RUNTIME
  (``ACCEPT_EULA=Y'``)
* Adding custom curl command for adding keys and configuring additional apt sources needed to install
  ``apt`` dependencies (both DEV and RUNTIME)
* Adding custom ``apt`` dependencies, both DEV (``msodbcsql17 unixodbc-dev g++) and runtime msodbcsql17 unixodbc git procps vim``)

.. exampleinclude:: docker-examples/customizing/custom-sources.sh
    :language: bash
    :start-after: [START build]
    :end-before: [END build]

.. _image-build-secure-environments:

Build images in security restricted environments
................................................

You can also make sure your image is only built using local constraint file and locally downloaded
wheel files. This is often useful in Enterprise environments where the binary files are verified and
vetted by the security teams. It is also the most complex way of building the image. You should be an
expert of building and using Dockerfiles in order to use it and have to have specific needs of security if
you want to follow that route.

This builds below builds the production image  with packages and constraints used from the local
``docker-context-files`` rather than installed from PyPI or GitHub. It also disables MySQL client
installation as it is using external installation method.

Note that as a prerequisite - you need to have downloaded wheel files. In the example below we
first download such constraint file locally and then use ``pip download`` to get the ``.whl`` files needed
but in most likely scenario, those wheel files should be copied from an internal repository of such .whl
files. Note that ``AIRFLOW_VERSION_SPECIFICATION`` is only there for reference, the apache airflow ``.whl`` file
in the right version is part of the ``.whl`` files downloaded.

Note that 'pip download' will only works on Linux host as some of the packages need to be compiled from
sources and you cannot install them providing ``--platform`` switch. They also need to be downloaded using
the same python version as the target image.

The ``pip download`` might happen in a separate environment. The files can be committed to a separate
binary repository and vetted/verified by the security team and used subsequently to build images
of Airflow when needed on an air-gaped system.

Example of preparing the constraint files and wheel files. Note that ``mysql`` dependency is removed
as ``mysqlclient`` is installed from Oracle's ``apt`` repository and if you want to add it, you need
to provide this library from your repository if you want to build Airflow image in an "air-gaped" system.

.. exampleinclude:: docker-examples/restricted/restricted_environments.sh
    :language: bash
    :start-after: [START download]
    :end-before: [END download]

After this step is finished, your ``docker-context-files`` folder will contain all the packages that
are needed to install Airflow from.

Those downloaded packages and constraint file can be pre-vetted by your security team before you attempt
to install the image. You can also store those downloaded binary packages in your private artifact registry
which allows for the flow where you will download the packages on one machine, submit only new packages for
security vetting and only use the new packages when they were vetted.

On a separate (air-gaped) system, all the PyPI packages can be copied to ``docker-context-files``
where you can build the image using the packages downloaded by passing those build args:

* ``INSTALL_PACKAGES_FROM_CONTEXT="true"``  - to use packages present in ``docker-context-files``
* ``AIRFLOW_PRE_CACHED_PIP_PACKAGES="false"``  - to not pre-cache packages from PyPI when building image
* ``AIRFLOW_CONSTRAINTS_LOCATION=/docker-context-files/YOUR_CONSTRAINT_FILE.txt`` - to downloaded constraint files
* (Optional) ``INSTALL_MYSQL_CLIENT="false"`` if you do not want to install ``MySQL``
  client from the Oracle repositories.
* (Optional) ``INSTALL_MSSQL_CLIENT="false"`` if you do not want to install ``MsSQL``
  client from the Microsoft repositories.
* (Optional) ``INSTALL_POSTGRES_CLIENT="false"`` if you do not want to install ``Postgres``
  client from the Postgres repositories.

Note, that the solution we have for installing python packages from local packages, only solves the problem
of "air-gaped" python installation. The Docker image also downloads ``apt`` dependencies and ``node-modules``.
Those types of dependencies are more likely to be available in your "air-gaped" system via transparent
proxies and it should automatically reach out to your private registries. However, in the future the
solution might be applied to both of those installation steps.

You can also use techniques described in the previous chapter to make ``docker build`` use your private
apt sources or private PyPI repositories (via ``.pypirc``) available which can be security-vetted.

If you fulfill all the criteria, you can build the image on an air-gaped system by running command similar
to the below:

.. exampleinclude:: docker-examples/restricted/restricted_environments.sh
    :language: bash
    :start-after: [START build]
    :end-before: [END build]

Modifying the Dockerfile
........................

The build arg approach is a convenience method if you do not want to manually modify the ``Dockerfile``.
Our approach is flexible enough to be able to accommodate most requirements and
customizations out-of-the-box. When you use it, you do not need to worry about adapting the image every
time a new version of Airflow is released. However, sometimes it is not enough if you have very
specific needs and want to build a very custom image. In such case you can simply modify the
``Dockerfile`` manually as you see fit and store it in your forked repository. However, you will have to
make sure to rebase your changes whenever new version of Airflow is released, because we might modify
the approach of our Dockerfile builds in the future and you might need to resolve conflicts
and rebase your changes.

There are a few things to remember when you modify the ``Dockerfile``:

* We are using the widely recommended pattern of ``.dockerignore`` where everything is ignored by default
  and only the required folders are added through exclusion (!). This allows to keep docker context small
  because there are many binary artifacts generated in the sources of Airflow and if they are added to
  the context, the time of building the image would increase significantly. If you want to add any new
  folders to be available in the image you must add them here with leading ``!``

  .. code-block:: text

      # Ignore everything
      **

      # Allow only these directories
      !airflow
      ...


* The ``docker-context-files`` folder is automatically added to the context of the image, so if you want
  to add individual files, binaries, requirement files etc you can add them there. The
  ``docker-context-files`` is copied to the ``/docker-context-files`` folder of the build segment of the
  image, so it is not present in the final image - which makes the final image smaller in case you want
  to use those files only in the ``build`` segment. You must copy any files from the directory manually,
  using COPY command if you want to get the files in your final image (in the main image segment).


More details
------------

Build Args reference
....................

The detailed ``--build-arg`` reference can be found in :doc:`build-arg-ref`.


The architecture of the images
..............................

You can read more details about the images - the context, their parameters and internal structure in the
`Images documentation <https://github.com/apache/airflow/blob/main/dev/breeze/doc/ci/02_images.md>`_.


Pip packages caching
....................

To enable faster iteration when building the image locally (especially if you are testing different combination of
python packages), pip caching has been enabled. The caching id is based on four different parameters:

1. ``PYTHON_BASE_IMAGE``: Avoid sharing same cache based on python version and target os
2. ``AIRFLOW_PIP_VERSION``
3. ``TARGETARCH``: Avoid sharing architecture specific cached package
4. ``PIP_CACHE_EPOCH``: Enable changing cache id by passing ``PIP_CACHE_EPOCH`` as ``--build-arg``
