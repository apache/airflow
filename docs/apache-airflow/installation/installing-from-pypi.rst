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

Installation from PyPI
----------------------

This page describes installations using the ``apache-airflow`` package `published in
PyPI <https://pypi.org/project/apache-airflow/>`__.

Installation tools
''''''''''''''''''

Only ``pip`` installation is currently officially supported.

.. note::

  While there are some successes with using other tools like `poetry <https://python-poetry.org/>`_ or
  `pip-tools <https://pypi.org/project/pip-tools/>`_, they do not share the same workflow as
  ``pip`` - especially when it comes to constraint vs. requirements management.
  Installing via ``Poetry`` or ``pip-tools`` is not currently supported. If you wish to install airflow
  using those tools you should use the constraints and convert them to appropriate
  format and workflow that your tool requires.

  There are known issues with ``bazel`` that might lead to circular dependencies when using it to install
  Airflow. Please switch to ``pip`` if you encounter such problems. ``Bazel`` community works on fixing
  the problem in `this PR <https://github.com/bazelbuild/rules_python/pull/1166>`_ so it might be that
  newer versions of ``bazel`` will handle it.

Typical command to install airflow from scratch in a reproducible way from PyPI looks like below:

.. code-block:: bash

    pip install "apache-airflow[celery]==|version|" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-|version|/constraints-3.9.txt"


Typically, you can add other dependencies and providers as separate command after the reproducible
installation - this way you can upgrade or downgrade the dependencies as you see fit, without limiting them to
constraints. Good practice for those is to extend such ``pip install`` command with the ``apache-airflow``
pinned to the version you have already installed to make sure it is not accidentally
upgraded or downgraded by ``pip``.

.. code-block:: bash

    pip install "apache-airflow==|version|" apache-airflow-providers-google==10.1.0


Those are just examples, see further for more explanation why those are the best practices.

.. note::

   Generally speaking, Python community established practice is to perform application installation in a
   virtualenv created with ``virtualenv`` or ``venv`` tools. You can also use ``pipx`` to install Airflow® in a
   application dedicated virtual environment created for you. There are also other tools that can be used
   to manage your virtualenv installation and you are free to choose how you are managing the environments.
   Airflow has no limitation regarding to the tool of your choice when it comes to virtual environment.

   The only exception where you might consider not using virtualenv is when you are building a container
   image with only Airflow installed - this is for example how Airflow is installed in the official Container
   image.

.. _installation:constraints:

Constraints files
'''''''''''''''''

Why we need constraints
=======================

Airflow® installation can be tricky because Airflow is both a library and an application.

Libraries usually keep their dependencies open and applications usually pin them, but we should do neither
and both at the same time. We decided to keep our dependencies as open as possible
(in ``pyproject.toml``) so users can install different version of libraries if needed. This means that
from time to time plain ``pip install apache-airflow`` will not work or will produce an unusable
Airflow installation.

Reproducible Airflow installation
=================================

In order to have a reproducible installation, we also keep a set of constraint files in the
``constraints-main``, ``constraints-2-0``, ``constraints-2-1`` etc. orphan branches and then we create a tag
for each released version e.g. :subst-code:`constraints-|version|`.

This way, we keep a tested set of dependencies at the moment of release. This provides you with the ability
of having the exact same installation of airflow + providers + dependencies as was known to be working
at the moment of release - frozen set of dependencies for that version of Airflow. There is a separate
constraints file for each version of Python that Airflow supports.

You can create the URL to the file substituting the variables in the template below.

.. code-block::

  https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt

where:

- ``AIRFLOW_VERSION`` - Airflow version (e.g. :subst-code:`|version|`) or ``main``, ``2-0``, for latest development version
- ``PYTHON_VERSION`` Python version e.g. ``3.9``, ``3.10``

The examples below assume that you want to use install airflow in a reproducible way with the ``celery`` extra,
but you can pick your own set of extras and providers to install.

.. code-block:: bash

    pip install "apache-airflow[celery]==|version|" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-|version|/constraints-3.9.txt"


.. note::

    The reproducible installation guarantees that this initial installation steps will always work for you -
    providing that you use the right Python version and that you have appropriate Operating System dependencies
    installed for the providers to be installed. Some of the providers require additional OS dependencies to
    be installed such as ``build-essential`` in order to compile libraries, or for example database client
    libraries in case you install a database provider, etc.. You need to figure out which system dependencies
    you need when your installation fails and install them before retrying the installation.

Upgrading and installing dependencies (including providers)
===========================================================

**The reproducible installation above should not prevent you from being able to upgrade or downgrade
providers and other dependencies to other versions**

You can, for example, install new versions of providers and dependencies after the release to use the latest
version and up-to-date with latest security fixes - even if you do not want upgrade airflow core version.
Or you can downgrade some dependencies or providers if you want to keep previous versions for compatibility
reasons. Installing such dependencies should be done without constraints as a separate pip command.

When you do such an upgrade, you should make sure to also add the ``apache-airflow`` package to the list of
packages to install and pin it to the version that you have, otherwise you might end up with a
different version of Airflow than you expect because ``pip`` can upgrade/downgrade it automatically when
performing dependency resolution.


.. code-block:: bash

    pip install "apache-airflow[celery]==|version|" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-|version|/constraints-3.9.txt"
    pip install "apache-airflow==|version|" apache-airflow-providers-google==10.1.1

You can also downgrade or upgrade other dependencies this way - even if they are not compatible with
those dependencies that are stored in the original constraints file:

.. code-block:: bash

    pip install "apache-airflow[celery]==|version|" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-|version|/constraints-3.9.txt"
    pip install "apache-airflow[celery]==|version|" dbt-core==0.20.0

.. warning::

    Not all dependencies can be installed this way - you might have dependencies conflicting with basic
    requirements of Airflow or other dependencies installed in your system. However, by skipping constraints
    when you install or upgrade dependencies, you give ``pip`` a chance to resolve the conflicts for you,
    while keeping dependencies within the limits that Apache Airflow, providers and other dependencies require.
    The resulting combination of those dependencies and the set of dependencies that come with the
    constraints might not be tested before, but it should work in most cases as we usually add
    requirements, when Airflow depends on particular versions of some dependencies. In cases you cannot
    install some dependencies in the same environment as Airflow - you can attempt to use other approaches.
    See :ref:`best practices for handling conflicting/complex Python dependencies <best_practices/handling_conflicting_complex_python_dependencies>`


Verifying installed dependencies
================================

You can also always run the ``pip check`` command to test if the set of your Python packages is
consistent and not conflicting.


.. code-block:: bash

    > pip check
    No broken requirements found.


When you see such message and the exit code from ``pip check`` is 0, you can be sure, that there are no
conflicting dependencies in your environment.


Using your own constraints
==========================

When you decide to install your own dependencies, or want to upgrade or downgrade providers, you might want
to continue being able to keep reproducible installation of Airflow and those dependencies via a single command.
In order to do that, you can produce your own constraints file and use it to install Airflow instead of the
one provided by the community.

.. code-block:: bash

    pip install "apache-airflow[celery]==|version|" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-|version|/constraints-3.9.txt"
    pip install "apache-airflow==|version|" dbt-core==0.20.0
    pip freeze > my-constraints.txt


Then you can use it to create reproducible installations of your environment in a single operation via
a local constraints file:

.. code-block:: bash

    pip install "apache-airflow[celery]==|version|" --constraint "my-constraints.txt"


Similarly as in case of Airflow original constraints, you can also host your constraints at your own
repository or server and use it remotely from there.

Fixing Constraints at release time
''''''''''''''''''''''''''''''''''

The released "versioned" constraints are mostly ``fixed`` when we release Airflow version and we only
update them in exceptional circumstances. For example when we find out that the released constraints might prevent
Airflow from being installed consistently from the scratch.

In normal circumstances, the constraint files are not going to change if new version of Airflow
dependencies are released - not even when those versions contain critical security fixes.
The process of Airflow releases is designed around upgrading dependencies automatically where
applicable but only when we release a new version of Airflow, not for already released versions.

Between the releases you can upgrade dependencies on your own and you can keep your own constraints
updated as described in the previous section.

The easiest way to keep-up with the latest released dependencies is to upgrade to the latest released
Airflow version. Whenever we release a new version of Airflow, we upgrade all dependencies to the latest
applicable versions and test them together, so if you want to keep up with those tests - staying up-to-date
with latest version of Airflow is the easiest way to update those dependencies.

Installation and upgrade scenarios
''''''''''''''''''''''''''''''''''

In order to simplify the installation, we have prepared examples of how to upgrade Airflow and providers.

Installing Airflow® with extras and providers
=============================================

If you need to install extra dependencies of Airflow®, you can use the script below to make an installation
a one-liner (the example below installs Postgres and Google providers, as well as ``async`` extra).

.. code-block:: bash
    :substitutions:

    AIRFLOW_VERSION=|version|
    PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
    CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
    pip install "apache-airflow[async,postgres,google]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

Note, that it will install the versions of providers that were available at the moment this version of Airflow
has been released. You need to run separate ``pip`` commands without constraints, if you want to upgrade
provider packages in case they were released afterwards.

Upgrading Airflow together with providers
=========================================

You can upgrade airflow together with extras (providers available at the time of the release of Airflow
being installed. This will bring ``apache-airflow`` and all providers to the versions that were
released and tested together when the version of Airflow you are installing was released.

.. code-block:: bash
    :substitutions:

    AIRFLOW_VERSION=|version|
    PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
    CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
    pip install "apache-airflow[postgres,google]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

.. _installing-from-pypi-managing-providers-separately-from-airflow-core:

Managing providers separately from Airflow core
===============================================

In order to add new features, implement bug-fixes or simply maintain backwards compatibility, you might need
to install, upgrade or downgrade any of the providers - separately from the Airflow Core package. We release
providers independently from the core of Airflow, so often new versions of providers are released before
Airflow is, also if you do not want yet to upgrade Airflow to the latest version, you might want to
install just some (or all) newly released providers separately.

As you saw above, when installing the providers separately, you should not use any constraint files.

If you build your environment automatically, You should run provider's installation as a
separate command after Airflow has been installed (usually with constraints).
Constraints are only effective during the ``pip install`` command they were used with.

It is the best practice to install apache-airflow in the same version as the one that comes from the
original image. This way you can be sure that ``pip`` will not try to downgrade or upgrade apache
airflow while installing other requirements, which might happen in case you try to add a dependency
that conflicts with the version of apache-airflow that you are using:

.. code-block:: bash

    pip install "apache-airflow==|version|" "apache-airflow-providers-google==8.0.0"

.. note::

    Installing, upgrading, downgrading providers separately is not guaranteed to work with all
    Airflow versions or other providers. Some providers have minimum-required version of Airflow and some
    versions of providers might have limits on dependencies that are conflicting with limits of other
    providers or other dependencies installed. For example google provider before 10.1.0 version had limit
    of protobuf library ``<=3.20.0`` while for example ``google-ads`` library that is supported by google
    has requirement for protobuf library ``>=4``. In such cases installing those two dependencies alongside
    in a single environment will not work. In such cases you can attempt to use other approaches.
    See :ref:`best practices for handling conflicting/complex Python dependencies <best_practices/handling_conflicting_complex_python_dependencies>`


Managing just Airflow core without providers
============================================

If you don't want to install any providers you have, just install or upgrade Apache Airflow, you can simply
install airflow in the version you need. You can use the special ``constraints-no-providers`` constraints
file, which is smaller and limits the dependencies to the core of Airflow only, however this can lead to
conflicts if your environment already has some of the dependencies installed in different versions and
in case you have other providers installed. This command, however, gives you the latest versions of
dependencies compatible with just airflow core at the moment Airflow was released.

.. code-block:: bash
    :substitutions:

    AIRFLOW_VERSION=|version|
    PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
    # For example: 3.9
    CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-no-providers-${PYTHON_VERSION}.txt"
    # For example: https://raw.githubusercontent.com/apache/airflow/constraints-|version|/constraints-no-providers-3.9.txt
    pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"


.. note::

    Airflow uses `Scarf <https://about.scarf.sh/>`__ to collect basic usage data during operation.
    Check the :ref:`Usage data collection FAQ <usage-data-collection>` for more information about the data collected and how to opt-out.

Troubleshooting
'''''''''''''''

This section describes how to troubleshoot installation issues with PyPI installation.

The 'airflow' command is not recognized
=======================================

If the ``airflow`` command is not getting recognized (can happen on Windows when using WSL), then
ensure that ``~/.local/bin`` is in your ``PATH`` environment variable, and add it in if necessary:

.. code-block:: bash

    PATH=$PATH:~/.local/bin

You can also start airflow with ``python -m airflow``

Symbol not found: ``_Py_GetArgcArgv``
=====================================

If you see ``Symbol not found: _Py_GetArgcArgv`` while starting or importing ``airflow``, this may mean that you are using an incompatible version of Python.
For a homebrew installed version of Python, this is generally caused by using Python in ``/usr/local/opt/bin`` rather than the Frameworks installation (e.g. for ``python 3.9``: ``/usr/local/opt/python@3.9/Frameworks/Python.framework/Versions/3.9``).

The crux of the issue is that a library Airflow depends on, ``setproctitle``, uses a non-public Python API
which is not available from the standard installation ``/usr/local/opt/`` (which symlinks to a path under ``/usr/local/Cellar``).

An easy fix is just to ensure you use a version of Python that has a dylib of the Python library available. For example:

.. code-block:: bash

  # Note: these instructions are for python3.9 but can be loosely modified for other versions
  brew install python@3.9
  virtualenv -p /usr/local/opt/python@3.9/Frameworks/Python.framework/Versions/3.9/bin/python3 .toy-venv
  source .toy-venv/bin/activate
  pip install apache-airflow
  python
  >>> import setproctitle
  # Success!

Alternatively, you can download and install Python directly from the `Python website <https://www.python.org/>`__.
