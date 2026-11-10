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

.. _build:extending:

Extending the image
===================

Extending the image means writing a ``Dockerfile`` that starts ``FROM`` one of the published images and
adds what you need on top of it. It needs no Airflow sources, uses the familiar ``FROM`` pattern and
builds in seconds - it is the right choice for most users. The basic flow is shown in
:ref:`build:small-production-image`; this page has the rules to follow and more examples.

If installing your packages fails with compilation errors, extending is not the best fit - see
:ref:`build:when-compilation-fails` and :doc:`build-customizing`.

Rules to follow when extending the image
----------------------------------------

* The image runs as the ``airflow`` user. Switch to ``root`` with the ``USER`` directive only for
  commands that need it (such as ``apt-get``), and switch back to ``airflow`` afterwards.

* Python packages are installed in the ``~/.local`` virtualenv of the ``airflow`` user. Install yours
  with plain ``pip install`` (or ``uv pip install``) as the ``airflow`` user, and always include
  ``apache-airflow==${AIRFLOW_VERSION}``. The constraints the image was built with are in
  ``${HOME}/constraints.txt`` - in the slim image those are the constraints without providers.

* Keep the image small: use ``pip install --no-cache-dir`` (or ``uv`` with ``--no-cache``), clean the
  ``apt`` lists in the same ``RUN`` command that installs packages, and follow the
  `Dockerfile best practices <https://docs.docker.com/develop/develop-images/dockerfile_best-practices/>`_.

* If you create directories that the running container needs to write to, run ``umask 0002`` in the
  same ``RUN`` command. The image can run as an arbitrary user, which always has ``GID=0``, so such
  directories have to be group-writable. See :ref:`arbitrary docker user <arbitrary-docker-user>`.

* The image contains no compiler. If a dependency needs ``build-essential`` or other build tools,
  customize the image instead - see :ref:`build:when-compilation-fails`.

* You do not need Airflow sources to extend the image: put the ``Dockerfile`` and the files it copies
  (for example your Dags) in a directory and run ``docker build`` there.

.. note::
  The ``Dockerfile`` does not strictly follow the `SemVer <https://semver.org/>`_ approach of Apache
  Airflow when it comes to features and backwards compatibility. Occasionally there are changes in the
  build process or in the entrypoint of the image that require slight adaptation of your images. Check
  the :doc:`Changelog <changelog>` when you upgrade.

Common cases of extending
-------------------------

The examples below start from a regular image, but they work the same way from a slim one.

PyPI packages
.............

When adding packages with ``pip``, use the ``airflow`` user (the default), not ``root`` - installing
``pip`` packages as ``root`` fails with an error message. Add ``apache-airflow==${AIRFLOW_VERSION}`` for
the reasons explained in the quick start.

.. exampleinclude:: docker-examples/extending/add-pypi-packages/Dockerfile
    :language: Dockerfile
    :start-after: [START Dockerfile]
    :end-before: [END Dockerfile]

Dependencies in ``requirements.txt``
....................................

.. exampleinclude:: docker-examples/extending/add-requirement-packages/Dockerfile
    :language: Dockerfile
    :start-after: [START Dockerfile]
    :end-before: [END Dockerfile]

.. exampleinclude:: docker-examples/extending/add-requirement-packages/requirements.txt
    :language: text

Pin versions in ``requirements.txt`` with ``==`` so the image does not change when a new version of a
package is released, and rebuild the image regularly to pick up security fixes.

``apt`` packages
................

When adding packages with ``apt``, switch to the ``root`` user, and switch back to the ``airflow`` user
when the installation is complete. Clean up the ``apt`` lists in the same ``RUN`` command to keep the
image small.

.. exampleinclude:: docker-examples/extending/add-apt-packages/Dockerfile
    :language: Dockerfile
    :start-after: [START Dockerfile]
    :end-before: [END Dockerfile]

Embedding Dags
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

Airflow configuration
.....................

``airflow.cfg`` in ``$AIRFLOW_HOME`` holds Airflow's configuration. You can set any of its options with
an environment variable named :envvar:`AIRFLOW__{SECTION}__{KEY}` (note the double underscores).

.. exampleinclude:: docker-examples/extending/add-airflow-configuration/Dockerfile
    :language: Dockerfile
    :start-after: [START Dockerfile]
    :end-before: [END Dockerfile]

.. _build:more-extending-examples:

More extending examples
-----------------------

The examples below cover less common cases.

Provider versions
.................

The :ref:`Airflow Providers <providers:community-maintained-providers>` are released independently of core
Airflow and sometimes you might want to upgrade specific providers only to fix some problems or
use features available in that provider version. Here is an example of how you can do it

.. exampleinclude:: docker-examples/extending/custom-providers/Dockerfile
    :language: Dockerfile
    :start-after: [START Dockerfile]
    :end-before: [END Dockerfile]

Provider with ``apt`` package
.............................

The following example adds ``apache-spark`` airflow-providers which requires both ``java`` and
python package from PyPI.

.. exampleinclude:: docker-examples/extending/add-providers/Dockerfile
    :language: Dockerfile
    :start-after: [START Dockerfile]
    :end-before: [END Dockerfile]

PyPI with constraints
.....................

The following example adds ``lxml`` python package from PyPI to the image with constraints that were
used to install airflow. This allows you to use the version of packages that you know were tested with the
given version of Airflow. You can also use it if you do not want to use potentially newer versions
that were released after the version of Airflow you are using.

.. exampleinclude:: docker-examples/extending/add-pypi-packages-constraints/Dockerfile
    :language: Dockerfile
    :start-after: [START Dockerfile]
    :end-before: [END Dockerfile]


PyPI with uv
............

The following example adds ``lxml`` python package from PyPI to the image using ``uv``. This is an
experimental feature as ``uv`` is a very fast but also very new tool in the Python ecosystem.

.. exampleinclude:: docker-examples/extending/add-pypi-packages-uv/Dockerfile
    :language: Dockerfile
    :start-after: [START Dockerfile]
    :end-before: [END Dockerfile]

Python venv for external tasks
..............................

The following example creates a separate Python virtualenv inside the image and installs packages
from ``requirements.txt`` into it, leaving Airflow's own Python environment untouched. This is the
typical setup for the
:ref:`ExternalPythonOperator <apache-airflow-providers-standard:howto/operator:externalpythonoperator>`
(or the ``@task.external_python`` decorator), which executes tasks in a pre-existing, immutable
Python environment. Use ``/opt/airflow/venv/bin/python`` as the operator's ``python`` argument
to run tasks in this venv.

The venv is created without ``--system-site-packages``, so the packages installed inside it
are isolated from Airflow's own dependencies.

.. exampleinclude:: docker-examples/extending/add-python-venv/Dockerfile
    :language: Dockerfile
    :start-after: [START Dockerfile]
    :end-before: [END Dockerfile]

.. exampleinclude:: docker-examples/extending/add-python-venv/requirements.txt
    :language: text

Writable directory
..................

The following example adds a new directory that is supposed to be writable for any arbitrary user
running the container.

.. exampleinclude:: docker-examples/extending/writable-directory/Dockerfile
    :language: Dockerfile
    :start-after: [START Dockerfile]
    :end-before: [END Dockerfile]


Packages requiring compilation
..............................

The following example adds ``mpi4py`` package which requires both ``build-essential`` and ``mpi compiler``.

.. exampleinclude:: docker-examples/extending/add-build-essential-extend/Dockerfile
    :language: Dockerfile
    :start-after: [START Dockerfile]
    :end-before: [END Dockerfile]

The size of this image is ~ 1.1 GB when built, because the compiler and the development libraries stay
in it. Customizing the image builds the same image about 20% smaller - see :ref:`image-build-pypi`.
