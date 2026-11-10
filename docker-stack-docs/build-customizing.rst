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

.. _build:customizing:

Customizing the image
=====================

Customizing the image means building it with the ``Dockerfile`` that Airflow releases, choosing what goes
into it with ``--build-arg`` options instead of adding layers on top of a published image. The quick start
in :ref:`build:selected-extras` shows the most common case - choosing the extras and adding packages that
need compiling. This page covers everything else you can do with the ``Dockerfile``.

.. note::
    Since Airflow 3.4.0 the images are based on the Docker Hardened Image for Python on Debian 13
    "trixie", see :ref:`image-hardened-base`.

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
  allows to build Airflow image in an air-gaped environment.
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
uses constraints from ``constraints-2.0.2`` tag). However, in some cases - when installing Airflow from
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
   downgrade or upgrade of airflow. If you upgrade Airflow base image, you should also update the version
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
  ``INSTALL_DISTRIBUTIONS_FROM_CONTEXT`` set to ``true`` . It's useful if you build the image in
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

.. _image-build-pypi:


Building from PyPI packages
---------------------------

This is the basic way of building the custom images from sources.

The following example builds the production image for the default Python version with latest PyPI-released Airflow,
with default set of Airflow extras and dependencies. The latest PyPI-released Airflow constraints are used automatically.

.. exampleinclude:: docker-examples/customizing/stable-airflow.sh
    :language: bash
    :start-after: [START build]
    :end-before: [END build]

The following example builds the production image for the default Python version with default extras from ``2.3.0`` Airflow
package. The ``2.3.0`` constraints are used automatically.

.. exampleinclude:: docker-examples/customizing/pypi-selected-version.sh
    :language: bash
    :start-after: [START build]
    :end-before: [END build]

The following example builds the production image for the default Python version with additional Airflow extras
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
-------------------------

The following example builds the production image for the default Python version with additional Airflow extras from
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
------------------------------------------------------

The following example builds the production image in default settings, but uses ``uv`` to build the image.
This is an experimental feature as ``uv`` is a very fast but also very new tool in the Python ecosystem.

.. exampleinclude:: docker-examples/customizing/use-uv.sh
    :language: bash
    :start-after: [START build]
    :end-before: [END build]

.. _image-build-mysql:

Building images with MySQL client
---------------------------------

.. warning::

   As of Airflow 3.2.0 - our images only support installation of "MariaDB" client from default Debian
   repositories. If you need to use "MySQL" client from Oracle, you need to extend the images and add
   your own ways of installing the client. The main reason for that is that Oracle's
   ``MySQL`` client installation is broken due to expiring GPG keys and every 2 years the old
   packages stop installing unless you manually update the keys (but there is a period of time when the
   packages are not installable at all). MariaDB client is a drop-in replacement for MySQL client
   and it works perfectly well with MySQL server.

.. _image-build-custom:

Using custom installation sources
---------------------------------

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

Such customizations are independent of the way how Airflow is installed.

.. note::
  Similar results could be achieved by modifying the Dockerfile manually (see below) and injecting the
  commands needed, but by specifying the customizations via build-args, you avoid the need of
  synchronizing the changes from future Airflow Dockerfiles. Those customizations should work with the
  future version of Airflow's official ``Dockerfile`` at most with minimal modifications od parameter
  names (if any), so using the build command for your customizations makes your custom image more
  future-proof.

The following - rather complex - example shows capabilities of:

* Adding Airflow extras (slack, odbc)
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
