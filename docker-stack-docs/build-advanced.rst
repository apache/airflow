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

.. _build:advanced:

Advanced build cases
====================

:doc:`build` covers the standard ways of building a small production image. This page helps you find
the right approach when they are not enough. Find what you want to do in the list below and follow the
link.

.. _build:advanced-decision:

What do you want to do?
-----------------------

* **Add providers, Python packages, apt packages, Dags or configuration that install without errors**
  - extend the image, see :doc:`build-extending`.

* **Add packages that fail to install with compilation errors, or get the smallest possible image**
  - customize the image with additional dev and runtime ``apt`` dependencies, see
  :ref:`image-build-optimized`.

* **Choose the extras, the Airflow version or the Python version the image is built with** - customize
  the image, see :ref:`image-build-pypi` and :doc:`build-arg-ref`.

* **Install from your own PyPI index or apt repositories, or authenticate to them** - customize the image
  with ``pip.conf`` or custom installation sources, see :ref:`using-docker-context-files` and
  :ref:`image-build-custom`.

* **Build Airflow from a fork, a branch or a commit** - customize the image with the ``AIRFLOW_INSTALLATION_METHOD``
  and ``AIRFLOW_VERSION_SPECIFICATION`` build arguments, see :doc:`build-arg-ref`.

* **Build in an air-gapped environment or from packages vetted by your security team** - see
  :ref:`image-build-secure-environments`.

* **Build an image that has to run in a FIPS-compliant environment** - see :ref:`image-build-fips`.

* **Understand why something behaves differently since Airflow 3.4.0** - for example ``apt`` installing
  a second Python, C extensions failing to compile, or renamed Debian packages - see
  :ref:`image-hardened-base` and :ref:`image-debian-trixie`.

* **Keep using the Debian "bookworm" based images with python.org Python** - build the deprecated
  ``legacy`` flavor, see :doc:`legacy-images`.

* **Add tools such as the Google Cloud SDK, the Hadoop stack or Go** - see :ref:`build:recipes`.

* **Change something that no build argument covers** - modify the ``Dockerfile``, see
  :ref:`build:modifying-dockerfile`.

Restricted environments
-----------------------

.. _image-build-secure-environments:

Build images in security restricted environments
................................................

You can also make sure your image is only built using local constraint file and locally downloaded
wheel files. This is often useful in Enterprise environments where the binary files are verified and
vetted by the security teams. It is also the most complex way of building the image. You should be an
expert of building and using Dockerfiles in order to use it and have to have specific needs of security if
you want to follow that route.

The example below builds the production image with packages and constraints used from the local
``docker-context-files`` rather than installed from PyPI or GitHub. It also disables MySQL client
installation as it is using external installation method.

Note that as a prerequisite - you need to have downloaded wheel files. In the example below we
first download such constraint file locally and then use ``pip download`` to get the ``.whl`` files needed
but in most likely scenario, those wheel files should be copied from an internal repository of such .whl
files. Note that ``AIRFLOW_VERSION_SPECIFICATION`` is only there for reference, the Apache Airflow ``.whl`` file
in the right version is part of the ``.whl`` files downloaded.

Note that 'pip download' will only works on Linux host as some of the packages need to be compiled from
sources and you cannot install them providing ``--platform`` switch. They also need to be downloaded using
the same python version as the target image.

The ``pip download`` might happen in a separate environment. The files can be committed to a separate
binary repository and vetted/verified by the security team and used subsequently to build images
of Airflow when needed on an air-gapped system.

Example of preparing the constraint files and wheel files. Note that ``mysql`` dependency is removed
as ``mysqlclient`` is installed from Oracle's ``apt`` repository and if you want to add it, you need
to provide this library from your repository if you want to build Airflow image in an "air-gapped" system.

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

On a separate (air-gapped) system, all the PyPI packages can be copied to ``docker-context-files``
where you can build the image using the packages downloaded by passing those build args:

* ``INSTALL_DISTRIBUTIONS_FROM_CONTEXT="true"``  - to use packages present in ``docker-context-files``
* ``AIRFLOW_CONSTRAINTS_LOCATION=/docker-context-files/YOUR_CONSTRAINT_FILE.txt`` - to downloaded constraint files
* (Optional) ``INSTALL_MYSQL_CLIENT="false"`` if you do not want to install ``MySQL``
  client from the Oracle repositories.
* (Optional) ``INSTALL_MSSQL_CLIENT="false"`` if you do not want to install ``MsSQL``
  client from the Microsoft repositories.
* (Optional) ``INSTALL_POSTGRES_CLIENT="false"`` if you do not want to install ``Postgres``
  client from the Postgres repositories.

Note, that the solution we have for installing python packages from local packages, only solves the problem
of "air-gapped" python installation. The Docker image also downloads ``apt`` dependencies and ``node-modules``.
Those types of dependencies are more likely to be available in your "air-gapped" system via transparent
proxies and it should automatically reach out to your private registries. However, in the future the
solution might be applied to both of those installation steps.

You can also use techniques described in the previous chapter to make ``docker build`` use your private
apt sources or private PyPI repositories (via ``.pypirc``) available which can be security-vetted.

If you fulfill all the criteria, you can build the image on an air-gapped system by running command similar
to the below:

.. exampleinclude:: docker-examples/restricted/restricted_environments.sh
    :language: bash
    :start-after: [START build]
    :end-before: [END build]

.. _image-build-fips:

Build images in FIPS-compliant environments
...........................................

Airflow images are based on the `Docker Hardened Images <https://dhi.io>`_ for Python, and Docker
publishes FIPS-validated variants of those images. Those variants are only available with a paid
Docker subscription, so they cannot be the default, but you can point the build at one with the
``BASE_IMAGE`` build argument.

.. code-block:: bash

    docker build . --build-arg BASE_IMAGE="dhi.io/python:3.13.16-debian13-fips-dev" --tag my-image:my-tag

.. note::

   While building on a FIPS-validated base image is necessary for FIPS compliance, it is not sufficient
   to make the image fully FIPS compliant. There might be other reasons for FIPS incompatibility
   (for example usage of non-FIPS compliant algorithms in the software installed in the image).
   You should verify the compliance of the image yourself.

The base image
--------------

.. _image-hardened-base:

Properties of the hardened base image
.....................................

Since Airflow 3.4.0 the images are built on the Debian 13 "trixie" ``-dev`` variant of the
`Docker Hardened Image <https://dhi.io>`_ for Python (``dhi.io/python``, mirrored to
``ghcr.io/apache/airflow/base/python``) instead of compiling Python on ``debian:bookworm-slim``. Docker
publishes the definitions these images are built from in the
`docker-hardened-images/catalog <https://github.com/docker-hardened-images/catalog>`_ repository
(``package/python`` for the Python build, ``image/python`` for the image). The differences from the
previous images that can matter when you extend or run the image are:

* **Python is patched.** Docker applies its own patches on top of the upstream CPython release
  (``package/python/patch`` in the catalog). One of them makes ``pkgutil.get_data()`` reject resource
  paths that are absolute or contain ``..`` (CVE-2026-3479) - upstream CPython only documents that
  restriction and does not enforce it. Libraries that load bundled data through ``../`` paths fail with
  ``ValueError: resource must be a relative path with no parent directory components``; for example
  ``moto`` before 5.2.2.
* **Threads get a 1 MiB stack.** The hardened Python is compiled with
  ``-DTHREAD_STACK_SIZE=0x100000``, while the previous images used the ``glibc`` default (the 8 MiB
  ``ulimit -s``). On Python 3.12 and 3.13 the interpreter's C recursion guard is a fixed depth that
  assumes the larger stack, so deeply nested data - for example a JSON body parsed in a web server worker
  thread - can crash the process instead of raising ``RecursionError``. The image restores the 8 MiB
  default for threads started by Python with a ``airflow-thread-stack-size.pth`` file in the base
  Python's ``site-packages``; delete that file if you need the smaller stacks.
* **Python is built without profile-guided optimization.** The previous images were built with
  ``--enable-optimizations`` and ``--with-lto``; the hardened Python uses only ``--with-lto``, so CPU-bound Python code
  can run measurably slower.
* **Python lives under** ``/usr``. The Debian 13 hardened images install Python as Debian packages
  (``python-3.13``, ``libpython-3.13``, ...), so it is ``/usr/bin/python3`` with the standard library in
  ``/usr/lib/python3.13`` (the Debian 12 ones used ``/opt/python``). ``/usr/python`` and the
  ``/usr/local/bin`` links point to it, so the paths used before keep working. The standard library ships without ``.pyc`` files, so the build
  compiles it into the image.
* **The OS is minimal.** The base ships no compiler, ``curl``, ``wget``, ``git``, ``gzip``, ``which`` or
  ``ldconfig``, no ``dash`` (``/bin/sh`` is ``bash``) and a stripped ``/etc`` - for example without
  ``/etc/shells`` or the PAM ``common-*`` files. The build restores what Airflow and the Debian packages
  it installs need; a custom image that relied on something else being present has to install it.
* **Packages also come from Docker's Debian repository.** Next to Debian's own repositories, the Debian 13
  images configure ``http://dhi.io/deb/debian/main`` (``/etc/apt/sources.list.d/dhi.sources``, signed with
  ``/usr/share/keyrings/dhi-deb-main.gpg``), which serves Docker's rebuilds of Debian packages. Their
  versions carry a ``+dhi<N>`` suffix, so apt prefers them over Debian's builds - in the image and in
  images extending it. The repository can be used anonymously: unlike pulling the ``dhi.io`` images,
  installing packages from it needs no ``docker login`` or any other credentials.
* **Installing apt packages can pull in a second Python.** The Debian 13 images register their own Python
  with ``dpkg`` (``python-3.12``, ``libpython-3.12``, ...), and Docker's package repository serves the
  other Python versions as packages too. A Debian package that depends on ``python3`` - directly or, as
  ``libenchant-2-dev`` does, through its build tools - therefore makes apt install another hardened Python
  version (for example ``python-3.13`` into a Python 3.12 image), and on the Debian 12 images it pulls in
  Debian's own ``python3``. The second interpreter does not replace the image's Python, but it can take
  over ``/usr/bin/python3`` and its headers, and C extensions built afterwards then compile against the
  wrong Python. The image build fails when this happens, but a custom image that installs extra apt
  packages is not checked. Check what ``apt-get install --simulate`` would install before adding packages
  that might depend on ``python3``, and prefer the runtime library packages over their ``-dev`` variants.
* **The image's Python packages are held.** On the Debian 13 images, Debian's own ``python3`` packages
  conflict with the image's ``python-3.X`` packages, so a package that apt can only satisfy with them -
  for example NodeSource's ``nodejs`` - would make apt remove the image's Python without asking. The
  image marks its Python packages with ``apt-mark hold``, so apt reports the ``python3`` conflict and the
  installation fails instead. Install that software some other way - for Node.js, copy the ``node`` binary
  from the official ``node`` image - rather than releasing the hold.
* **C extensions are compiled more strictly.** The Debian 13 images come with GCC 14, which turns
  incompatible pointer types, integer conversions and implicit function declarations into errors, and the
  hardened Python passes ``-Werror=implicit-function-declaration`` on to every C extension it builds. Old
  source distributions that compiled with warnings before - for example ``xmlsec`` 1.3.14 or ``ibm-db``
  3.2.0 - fail to build. Prefer versions with wheels or newer releases; if you have to build such a
  package, setting ``CFLAGS="-Wno-error=incompatible-pointer-types -Wno-error=int-conversion
  -Wno-error=implicit-function-declaration"`` for the installation turns the errors back into warnings.
* **apt does not refresh the shared library cache.** The base images ship ``libc-bin`` without its
  ``dpkg`` trigger, so installing a shared library does not run ``ldconfig``, and anything that looks
  libraries up through the cache - such as ``ctypes.util.find_library()`` - does not find it. The image
  adds ``/etc/apt/apt.conf.d/99airflow-ldconfig``, which runs ``ldconfig`` after every ``dpkg`` run,
  including those of images extending it.
* **Some OS files are modified.** ``/etc/os-release`` identifies the system as "Docker Hardened Images
  (Debian)", and ``/etc/debian_version`` differs from the Debian package's copy, so an ``apt-get upgrade``
  that touches ``base-files`` would stop at ``dpkg``'s interactive configuration-file prompt. The image
  ships ``/etc/dpkg/dpkg.cfg.d/airflow-keep-conffiles`` (``force-confdef`` and ``force-confold``), so
  ``dpkg`` keeps the image's copy of a configuration file without asking - also in images that extend
  it. A package upgrade therefore does not replace a configuration file that differs from the
  package's version.
* **The tags are rebuilt in place, so the default is pinned to a digest.** Docker republishes the same tags
  as CVEs are fixed, and Airflow refreshes its mirror every six hours, so building from a tag alone can pick
  up a newer base at any time. The default ``BASE_IMAGE`` therefore names the digest as well, and Airflow's
  regular dependency upgrade moves it - in a pull request that has to pass the full CI before the new base
  is used. Pass a tag without a digest to build on the newest mirrored base instead. The mirror also copies
  the newest Python patch release Docker publishes, and the same upgrade moves the pinned patch level to it,
  so the images follow Docker's releases rather than python.org's.
* **The OS dependencies come from pinned images too.** A digest pins the base image, but not ``dhi.io``'s apt
  repository: it moves on - and drops the package versions it replaced - whenever Docker rebuilds its
  images, so installing the OS dependencies from it can fail for a commit that built fine before. Airflow's
  CI and release builds therefore start their OS dependencies - the database clients included - from
  images in which ``install_os_dependencies.sh`` already ran, built daily on the mirrored base images and
  pinned to a digest by the same upgrade, passed as ``DEV_OS_DEPS_IMAGE`` and ``RUNTIME_OS_DEPS_IMAGE``. The
  script records what it installed with and does nothing when nothing differs, so a build with other apt
  dependencies or database clients still installs them. A plain ``docker build`` leaves both at
  ``BASE_IMAGE`` and installs everything as before.
  The ``AIRFLOW_OS_DEPS_IMAGE`` environment variable of an image names the image its OS dependencies came
  from, and ``/usr/share/airflow/os-dependencies.base-image`` the base image they were installed on - which
  can be an older digest than ``BASE_IMAGE``.
* **Packages come from Docker's apt repository first.** The image keeps stock Debian's apt sources beside
  ``dhi.io``, and ``/etc/apt/preferences.d/airflow-prefer-dhi`` ranks ``dhi.io`` higher, so apt installs
  Docker's build of a package whenever ``dhi.io`` publishes it and falls back to Debian's otherwise - also
  in images that extend it. The priority stays below 1000, so apt still favors ``dhi.io``'s build but can
  take a newer version from another repository when a dependency needs it (for example PGDG's ``libpq5``).

.. _image-debian-trixie:

Debian trixie
.............

Together with the hardened base, the default images moved from Debian 12 "bookworm" to Debian 13
"trixie". Besides the newer system libraries, this shows up in two places when you extend the image:

* Several library packages were renamed in trixie's 64-bit ``time_t`` transition - for example
  ``libssl3`` is ``libssl3t64`` and ``libldap-2.5-0`` is ``libldap2`` - so ``RUNTIME_APT_DEPS`` or
  ``ADDITIONAL_RUNTIME_APT_DEPS`` that name the bookworm packages have to be updated.
* The MySQL-compatible client comes from MariaDB 11.8, the first MariaDB LTS published for trixie. The
  ``mysql`` named commands are provided by ``mariadb-client-compat``.

The images built before Airflow 3.4.0 - on ``debian:bookworm-slim``, with Python compiled from
python.org sources - can still be built as the deprecated ``legacy`` flavor. Those images are not
published; see :doc:`legacy-images` for how to build one.

.. _build:recipes:

Recipes
-------

Users sometimes share interesting ways of using the Docker images. We encourage users to contribute these
recipes to the documentation in case they prove useful to other members of the community by
submitting a pull request. The sections below capture this knowledge.

Google Cloud SDK installation
.............................

Some operators, such as :class:`~airflow.providers.google.cloud.operators.kubernetes_engine.GKEStartPodOperator`,
require the installation of `Google Cloud SDK <https://cloud.google.com/sdk>`__ (includes ``gcloud``).
You can also run these commands with BashOperator.

Create a new Dockerfile like the one shown below.

.. exampleinclude:: /docker-images-recipes/gcloud.Dockerfile
    :language: dockerfile

Then build a new image.

.. code-block:: bash

  docker build . \
    --pull \
    --build-arg BASE_AIRFLOW_IMAGE="apache/airflow:2.0.2" \
    --tag my-airflow-image:0.0.1


Apache Hadoop Stack installation
................................

Airflow is often used to run tasks on Hadoop cluster. It required Java Runtime Environment (JRE) to run.
Below are the steps to take tools that are frequently used in Hadoop-world:

- Java Runtime Environment (JRE)
- Apache Hadoop
- Apache Hive
- `Cloud Storage connector for Apache Hadoop <https://cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage>`__


Create a new Dockerfile like the one shown below.

.. exampleinclude:: /docker-images-recipes/hadoop.Dockerfile
    :language: dockerfile

Then build a new image.

.. code-block:: bash

  docker build . \
    --pull \
    --build-arg BASE_AIRFLOW_IMAGE="apache/airflow:2.0.2" \
    --tag my-airflow-image:0.0.1

Apache Beam Go Stack installation
.................................

To be able to run Beam Go Pipeline with the :class:`~airflow.providers.apache.beam.operators.beam.BeamRunGoPipelineOperator`,
you will need Go in your container. Install Airflow with ``apache-airflow-providers-google>=6.5.0`` and ``apache-airflow-providers-apache-beam>=3.2.0``

Create a new Dockerfile like the one shown below.

.. exampleinclude:: /docker-images-recipes/go-beam.Dockerfile
    :language: dockerfile

Then build a new image.

.. code-block:: bash

  docker build . \
    --pull \
    --build-arg BASE_AIRFLOW_IMAGE="apache/airflow:2.2.5" \
    --tag my-airflow-image:0.0.1

.. _build:modifying-dockerfile:

Modifying the Dockerfile
------------------------

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

.. _build:dependency-caching:

Dependency caching
------------------

When you customize the image, the ``Dockerfile`` keeps the packages that ``pip`` and ``uv`` download and
build in a `BuildKit cache mount <https://docs.docker.com/build/cache/optimize/#use-cache-mounts>`_. The
cache lives in your builder, not in the image, so it does not make the image bigger - but it makes the
next build faster: even when a change invalidates the layer that installs Python packages, the packages
that did not change are taken from the cache instead of being downloaded or compiled again.

This matters when you build the image repeatedly - for example while you try out different
``ADDITIONAL_PYTHON_DEPS``, extras or a ``requirements.txt`` in ``docker-context-files``, or when the
dependencies you add have to be compiled. It does not help on CI runners that start with an empty builder
every time, unless you keep the builder's cache between runs.

The cache is identified by two values:

1. ``TARGETARCH`` - set by Docker to the architecture you build for, so packages built for one architecture
   are never used for another.
2. ``DEPENDENCY_CACHE_EPOCH`` - a build argument. Pass a new value to start with an empty cache, for example
   when you suspect a broken or stale package in it, after changing the system libraries your packages are
   compiled against, or when you want to check that the image still builds from scratch.

You can also remove all cache mounts of your builder with ``docker builder prune --filter type=exec.cachemount``.

More details
------------

* :doc:`build-arg-ref` - all build arguments of the ``Dockerfile``.
* `Images documentation <https://github.com/apache/airflow/blob/main/dev/breeze/doc/ci/02_images.md>`_ -
  the architecture of the images: their context, parameters and internal structure.
