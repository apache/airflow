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

.. _build:small-production-image:

Quick start
===========

The published images are a good way to try Airflow out, but in production you should run your own
image - see :ref:`build:build_image`. There are three standard ways of building it. Each step below is a little more work than
the one before, so take the first one that fits:

1. **Do you only add providers, pure-Python packages, Dags or configuration?** Extend the slim image -
   see :ref:`build:extend-slim`. This is the right choice for most production images.

2. **Do you need many of the providers of the regular image, or are you trying things out?** Extend the
   regular image - see :ref:`build:extend-regular`.

3. **Does installing your packages fail with compilation errors, or do you need build tools such as**
   ``build-essential`` **to install them?** Build the image from the ``Dockerfile`` with the extras and
   packages you need - see :ref:`build:selected-extras`. This also gives you the smallest image.

The first two ways are called *extending* the image, the third one *customizing* it.

.. _build:extend-slim:

Step 1: extend the slim image
.............................

Write a ``Dockerfile`` that starts ``FROM`` the slim image and installs the providers you need. This is
the familiar ``FROM`` pattern, needs no Airflow sources and builds in seconds.

.. exampleinclude:: docker-examples/extending/slim-with-providers/Dockerfile
    :language: Dockerfile
    :start-after: [START Dockerfile]
    :end-before: [END Dockerfile]

Two things in this example are worth copying into every image you build:

* ``apache-airflow==${AIRFLOW_VERSION}`` keeps Airflow at the version of the image. Without it, ``pip``
  can silently upgrade or downgrade Airflow to satisfy a dependency of the packages you add; with it, a
  conflict fails the build with a clear error instead. ``AIRFLOW_VERSION`` is set in the image.
* The ``--constraint`` file pins the providers and their dependencies to the versions released and
  tested with that Airflow version, so the image builds the same way every time. Leave it out only when
  you want newer versions of the providers than the ones released with your Airflow version.

Then build the image:

.. code-block:: shell

   docker build . --pull --tag my-company/airflow:0.0.1

.. _build:extend-regular:

Step 2: extend the regular image
................................

Extending the regular image works the same way - only the ``FROM`` line changes. The providers of the
regular image are already installed, so you only add your own packages:

.. exampleinclude:: docker-examples/extending/add-pypi-packages/Dockerfile
    :language: Dockerfile
    :start-after: [START Dockerfile]
    :end-before: [END Dockerfile]

The image is about twice as big as an extended slim image, but you do not have to know in advance which
providers you will use. :doc:`build-extending` has the rules to follow when extending an image and
examples of adding ``apt`` packages, ``requirements.txt`` files, Dags and configuration.

.. _build:when-compilation-fails:

When adding packages fails with compilation errors
..................................................

Many Python packages ship as pre-built binary wheels and install in seconds. Some do not - for your
Python version, your platform, or at all - and ``pip`` then tries to build them from sources. The
published images contain no compiler and no development libraries, so that fails with errors such as:

.. code-block:: text

   error: command 'gcc' failed: No such file or directory

.. code-block:: text

   fatal error: mpi.h: No such file or directory

.. code-block:: text

   Error: pg_config executable not found.

.. code-block:: text

   Exception: Can not find valid pkg-config name.

.. code-block:: text

   error: can't find Rust compiler

They all end with ``ERROR: Failed building wheel for <package>`` or
``error: subprocess-exited-with-error``.

You can fix this while extending the image - install ``build-essential`` and the ``-dev`` packages of
the libraries as ``root`` before you install the package (see
:ref:`the example <build:more-extending-examples>`). But the compiler and the development libraries,
hundreds of megabytes of them, then stay in your image, although nothing needs them once the package
is built. Customizing the image avoids that: they are installed only in the build stage of the
``Dockerfile``, and only the built packages end up in the final image.

.. _build:selected-extras:

Step 3: build the image with the extras and packages you need
.............................................................

The ``Dockerfile`` that Airflow releases builds the reference images, and its build arguments decide
what goes into the image:

* ``AIRFLOW_EXTRAS`` - the extras, and therefore the providers, to install. Choosing only the extras you
  need gives an image as small as an extended slim image, with the constraints for those providers
  applied automatically.
* ``ADDITIONAL_PYTHON_DEPS`` - other Python packages to install.
* ``ADDITIONAL_DEV_APT_DEPS`` - ``apt`` packages needed only to build them, such as the ``-dev``
  packages of the libraries they compile against. The compiler itself is always present in the build
  stage.
* ``ADDITIONAL_RUNTIME_APT_DEPS`` - ``apt`` packages the installed packages need when they run.

Download the `Dockerfile <https://raw.githubusercontent.com/apache/airflow/|airflow-version|/Dockerfile>`_
to an empty directory and run:

.. exampleinclude:: docker-examples/customizing/selected-extras.sh
    :language: bash
    :start-after: [START build]
    :end-before: [END build]

The saving can be large: for the ``mpi4py`` package used here, the customized image is about 20%
smaller than the same packages added by extending the regular image, and it grows with every package
that needs compiling.

The published slim images are built the same way, with no extras at all and the constraints without
providers:

.. code-block:: shell
   :substitutions:

   docker build . \
     --build-arg AIRFLOW_VERSION="|airflow-version|" \
     --build-arg AIRFLOW_EXTRAS="" \
     --build-arg AIRFLOW_CONSTRAINTS="constraints-no-providers" \
     --tag my-company/airflow:slim-|airflow-version|

This method needs `BuildKit <https://docs.docker.com/build/buildkit/>`_ (the default in Docker Desktop
and Docker Engine 23.0 and later) and takes longer to build, because it installs Airflow from scratch.
All the build arguments are listed in :doc:`build-arg-ref`, and :doc:`build-customizing` shows what else
you can do with them.

Extending vs. customizing the image
...................................

+----------------------------------------------------+-----------+-------------+
|                                                    | Extending | Customizing |
+====================================================+===========+=============+
| Uses familiar 'FROM' pattern of image building     | Yes       | No          |
+----------------------------------------------------+-----------+-------------+
| Requires only basic knowledge about images         | Yes       | No          |
+----------------------------------------------------+-----------+-------------+
| Builds quickly                                     | Yes       | No          |
+----------------------------------------------------+-----------+-------------+
| Small image when based on slim / minimal extras    | Yes       | Yes         |
+----------------------------------------------------+-----------+-------------+
| Small image with dependencies that need compiling  | No        | Yes         |
+----------------------------------------------------+-----------+-------------+
| Can build from custom Airflow sources (forks)      | No        | Yes         |
+----------------------------------------------------+-----------+-------------+
| Can build on air-gapped system                     | No        | Yes         |
+----------------------------------------------------+-----------+-------------+

You can also combine both: a platform team customizes an optimized base image and publishes it in the
company registry, and Dag authors extend it with ``FROM``.


Where to go next
----------------

* :doc:`build-publishing` - building, testing and using your image in your deployment.
* :doc:`build-extending` - rules for extending the image and more examples.
* :doc:`build-customizing` - everything you can do with the ``Dockerfile`` and its build arguments.
* :doc:`build-advanced` - which approach to follow for custom package sources, restricted, air-gapped
  or FIPS environments, forks of Airflow, and the properties of the hardened base image.
* :doc:`build-arg-ref` - all build arguments of the ``Dockerfile``.
* :doc:`legacy-images` - the deprecated ``legacy`` image flavor, which is not published but which you
  can build yourself.
* :ref:`build:recipes` - recipes for adding tools such as the Google Cloud SDK.

Airflow Summit 2020's `Production Docker Image <https://youtu.be/wDr3Y7q2XoI>`_ talk provides more
details about the context, architecture and customization/extension methods for the Production Image.
