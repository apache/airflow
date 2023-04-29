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

While there are some successes with using other tools like `poetry <https://python-poetry.org/>`_ or
`pip-tools <https://pypi.org/project/pip-tools/>`_, they do not share the same workflow as
``pip`` - especially when it comes to constraint vs. requirements management.
Installing via ``Poetry`` or ``pip-tools`` is not currently supported. If you wish to install airflow
using those tools you should use the constraints and convert them to appropriate
format and workflow that your tool requires.

Typical command to install airflow from PyPI looks like below:

.. code-block::

    pip install "apache-airflow[celery]==|version|" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-|version|/constraints-3.8.txt"

This is an example, see further for more explanation.

.. _installation:constraints:

Constraints files
'''''''''''''''''

Airflow installation can be tricky sometimes because Airflow is both a library and an application.
Libraries usually keep their dependencies open and applications usually pin them, but we should do neither
and both at the same time. We decided to keep our dependencies as open as possible
(in ``setup.cfg`` and ``setup.py``) so users can install different
version of libraries if needed. This means that from time to time plain ``pip install apache-airflow`` will
not work or will produce an unusable Airflow installation.

In order to have a repeatable installation (and only for that reason), we also keep a set of "known-to-be-working" constraint files in the
``constraints-main``, ``constraints-2-0``, ``constraints-2-1`` etc. orphan branches and then we create a tag
for each released version e.g. :subst-code:`constraints-|version|`. This way, we keep a tested and working set of dependencies.

Those "known-to-be-working" constraints are per major/minor Python version. You can use them as constraint
files when installing Airflow from PyPI. Note that you have to specify the correct Airflow
and Python versions in the URL.

You can create the URL to the file substituting the variables in the template below.

.. code-block::

  https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt

where:

- ``AIRFLOW_VERSION`` - Airflow version (e.g. :subst-code:`|version|`) or ``main``, ``2-0``, for latest development version
- ``PYTHON_VERSION`` Python version e.g. ``3.8``, ``3.9``

There is also a ``constraints-no-providers`` constraint file, which contains just constraints required to
install Airflow core. This allows to install and upgrade airflow separately and independently from providers.

You can create the URL to the file substituting the variables in the template below.

.. code-block::

  https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-no-providers-${PYTHON_VERSION}.txt

You can also use "latest" as version when you install "latest" stable version of Airflow. The "latest"
constraints always points to the "latest" released Airflow version constraints:

.. code-block::

  https://raw.githubusercontent.com/apache/airflow/constraints-latest/constraints-3.8.txt


Fixing Constraint files at release time
'''''''''''''''''''''''''''''''''''''''

The released "versioned" constraints are mostly ``fixed`` when we release Airflow version and we only
update them in exceptional circumstances. For example when we find out that the released constraints might prevent
Airflow from being installed consistently from the scratch. In normal circumstances, the constraint files
are not going to change if new version of Airflow dependencies are released - not even when those
versions contain critical security fixes. The process of Airflow releases is designed around upgrading
dependencies automatically where applicable but only when we release a new version of Airflow,
not for already released versions.

If you want to make sure that Airflow dependencies are upgraded to the latest released versions containing
latest security fixes, you should implement your own process to upgrade those yourself when
you detect the need for that. Airflow usually does not upper-bound versions of its dependencies via
requirements, so you should be able to upgrade them to the latest versions - usually without any problems.

Obviously - since we have no control over what gets released in new versions of the dependencies, we
cannot give any guarantees that tests and functionality of those dependencies will be compatible with
Airflow after you upgrade them - testing if Airflow still works with those is in your hands,
and in case of any problems, you should raise issue with the authors of the dependencies that are problematic.
You can also - in such cases - look at the `Airflow issues <https://github.com/apache/airflow/issues>`_
`Airflow Pull Requests <https://github.com/apache/airflow/pulls>`_ and
`Airflow Discussions <https://github.com/apache/airflow/discussions>`_, searching for similar
problems to see if there are any fixes or workarounds found in the ``main`` version of Airflow and apply them
to your deployment.

The easiest way to keep-up with the latest released dependencies is however, to upgrade to the latest released
Airflow version. Whenever we release a new version of Airflow, we upgrade all dependencies to the latest
applicable versions and test them together, so if you want to keep up with those tests - staying up-to-date
with latest version of Airflow is the easiest way to update those dependencies.

Installation and upgrade scenarios
''''''''''''''''''''''''''''''''''

In order to simplify the installation, we have prepared examples of how to upgrade Airflow and providers.

Installing Airflow with extras and providers
============================================

If you need to install extra dependencies of Airflow, you can use the script below to make an installation
a one-liner (the example below installs Postgres and Google providers, as well as ``async`` extra).

.. code-block:: bash
    :substitutions:

    AIRFLOW_VERSION=|version|
    PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
    CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
    pip install "apache-airflow[async,postgres,google]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

Note, that it will install the versions of providers that were available at the moment this version of Airflow
has been prepared. You need to follow next steps if you want to upgrade provider packages in case they were
released afterwards.


Upgrading Airflow with providers
================================

You can upgrade airflow together with extras (providers available at the time of the release of Airflow
being installed.


.. code-block:: bash
    :substitutions:

    AIRFLOW_VERSION=|version|
    PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
    CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
    pip install "apache-airflow[postgres,google]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

Installing/upgrading/downgrading providers separately from Airflow core
=======================================================================

In order to add new features, implement bug-fixes or simply maintain backwards compatibility, you might need
to install, upgrade or downgrade any of the providers you need. We release providers independently from the
core of Airflow, so often new versions of providers are released before Airflow is, also if you do not want
yet to upgrade Airflow to the latest version, you might want to install newly released providers separately.
For installing the providers you should not use any constraint files (the constraints are for installing
Airflow with providers, not to install providers separately).

You should run provider's installation as a separate command after Airflow has been installed (usually
with constraints). Constraints are only effective during the ``pip install`` command they were used with.

.. code-block:: bash

    pip install "apache-airflow-providers-google==8.0.0"

Note, that installing, upgrading, downgrading providers separately is not guaranteed to work with all
Airflow versions or other providers. Some providers have minimum-required version of Airflow and some
versions of providers might have conflicting requirements with Airflow or other dependencies you
might have installed.


Installation and upgrade of Airflow core
========================================

If you don't want to install any extra providers, initially you can use the command set below.

.. code-block:: bash
    :substitutions:

    AIRFLOW_VERSION=|version|
    PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
    # For example: 3.8
    CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-no-providers-${PYTHON_VERSION}.txt"
    # For example: https://raw.githubusercontent.com/apache/airflow/constraints-|version|/constraints-no-providers-3.8.txt
    pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"


Troubleshooting
'''''''''''''''

This section describes how to troubleshoot installation issues with PyPI installation.

Airflow command is not recognized
=================================

If the ``airflow`` command is not getting recognized (can happen on Windows when using WSL), then
ensure that ``~/.local/bin`` is in your ``PATH`` environment variable, and add it in if necessary:

.. code-block:: bash

    PATH=$PATH:~/.local/bin

You can also start airflow with ``python -m airflow``

Symbol not found: ``_Py_GetArgcArgv``
=====================================

If you see ``Symbol not found: _Py_GetArgcArgv`` while starting or importing Airflow, this may mean that you are using an incompatible version of Python.
For a homebrew installed version of Python, this is generally caused by using Python in ``/usr/local/opt/bin`` rather than the Frameworks installation (e.g. for ``python 3.8``: ``/usr/local/opt/python@3.8/Frameworks/Python.framework/Versions/3.8``).

The crux of the issue is that a library Airflow depends on, ``setproctitle``, uses a non-public Python API
which is not available from the standard installation ``/usr/local/opt/`` (which symlinks to a path under ``/usr/local/Cellar``).

An easy fix is just to ensure you use a version of Python that has a dylib of the Python library available. For example:

.. code-block:: bash

  # Note: these instructions are for python3.8 but can be loosely modified for other versions
  brew install python@3.8
  virtualenv -p /usr/local/opt/python@3.8/Frameworks/Python.framework/Versions/3.8/bin/python3 .toy-venv
  source .toy-venv/bin/activate
  pip install apache-airflow
  python
  >>> import setproctitle
  # Success!

Alternatively, you can download and install Python directly from the `Python website <https://www.python.org/>`__.
