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


Installation
------------

.. contents:: :local:


Prerequisites
-------------

Airflow is tested with:

* Python: 3.6, 3.7, 3.8

* Databases:

  * PostgreSQL:  9.6, 10, 11, 12, 13
  * MySQL: 5.7, 8
  * SQLite: 3.15.0+

* Kubernetes: 1.16.9, 1.17.5, 1.18.6

**Note:** MySQL 5.x versions are unable to or have limitations with
running multiple schedulers -- please see the "Scheduler" docs. MariaDB is not tested/recommended.

**Note:** SQLite is used in Airflow tests. Do not use it in production. We recommend
using the latest stable version of SQLite for local development.

Getting Airflow
'''''''''''''''

Airflow is published as ``apache-airflow`` package in PyPI. Installing it however might be sometimes tricky
because Airflow is a bit of both a library and application. Libraries usually keep their dependencies open and
applications usually pin them, but we should do neither and both at the same time. We decided to keep
our dependencies as open as possible (in ``setup.cfg`` and ``setup.py``) so users can install different
version of libraries if needed. This means that from time to time plain ``pip install apache-airflow`` will
not work or will produce unusable Airflow installation.

In order to have repeatable installation, however, starting from **Airflow 1.10.10** and updated in
**Airflow 1.10.13** we also keep a set of "known-to-be-working" constraint files in the
``constraints-master``, ``constraints-2-0`` and ``constraints-1-10`` orphan branches.
Those "known-to-be-working" constraints are per major/minor python version. You can use them as constraint
files when installing Airflow from PyPI. Note that you have to specify correct Airflow version
and python versions in the URL.

  **Prerequisites**

  On Debian based Linux OS:

  .. code-block:: bash

      sudo apt-get update
      sudo apt-get install build-essential


1. Installing just Airflow

.. note::

   On November 2020, new version of PIP (20.3) has been released with a new, 2020 resolver. This resolver
   does not yet work with Apache Airflow and might lead to errors in installation - depends on your choice
   of extras. In order to install Airflow you need to either downgrade pip to version 20.2.4
   ``pip install --upgrade pip==20.2.4`` or, in case you use Pip 20.3, you need to add option
   ``--use-deprecated legacy-resolver`` to your pip install command.


.. code-block:: bash

    AIRFLOW_VERSION=2.0.0
    PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
    # For example: 3.6
    CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
    # For example: https://raw.githubusercontent.com/apache/airflow/constraints-2.0.0/constraints-3.6.txt
    pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

Please note that with respect to Python 3 support, Airflow 2.0.0 has been
tested with Python 3.6, 3.7, and 3.8, but does not yet support Python 3.9.

2. Installing with extras (for example postgres, google)

.. note::

   On November 2020, new version of PIP (20.3) has been released with a new, 2020 resolver. This resolver
   does not yet work with Apache Airflow and might lead to errors in installation - depends on your choice
   of extras. In order to install Airflow you need to either downgrade pip to version 20.2.4
   ``pip install --upgrade pip==20.2.4`` or, in case you use Pip 20.3, you need to add option
   ``--use-deprecated legacy-resolver`` to your pip install command.


.. code-block:: bash

    AIRFLOW_VERSION=2.0.0
    PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
    CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
    pip install "apache-airflow[postgres,google]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

Most of the extras are linked to a corresponding providers package. For example "amazon" extra
has a corresponding ``apache-airflow-providers-amazon`` providers package to be installed. When you install
Airflow with such extras, the necessary provider packages are installed automatically (latest versions from
PyPI for those packages). However you can freely upgrade and install provider packages independently from
the main Airflow installation.

Python versions support
'''''''''''''''''''''''

As of Airflow 2.0 we agreed to certain rules we follow for Python support. They are based on the official
release schedule of Python, nicely summarized in the
`Python Developer's Guide <https://devguide.python.org/#status-of-python-branches>`_

1. We end support for Python versions when they reach EOL (For Python 3.6 it means that we will stop supporting it
   on 23.12.2021).

2. The "oldest" supported version of Python is the default one. "Default" is only meaningful in terms of
   "smoke tests" in CI PRs which are run using this default version.

3. We support a new version of Python after it is officially released, as soon as we manage to make
   it works in our CI pipeline (which might not be immediate) and release a new version of Airflow
   (non-Patch version) based on this CI set-up.


Requirements
''''''''''''

You need certain system level requirements in order to install Airflow. Those are requirements that are known
to be needed for Linux system (Tested on Ubuntu Buster LTS) :

.. code-block:: bash

   sudo apt-get install -y --no-install-recommends \
           freetds-bin \
           krb5-user \
           ldap-utils \
           libffi6 \
           libsasl2-2 \
           libsasl2-modules \
           libssl1.1 \
           locales  \
           lsb-release \
           sasl2-bin \
           sqlite3 \
           unixodbc

You also need database client packages (Postgres or MySQL) if you want to use those databases.

If the ``airflow`` command is not getting recognized (can happen on Windows when using WSL), then
ensure that ``~/.local/bin`` is in your ``PATH`` environment variable, and add it in if necessary:

.. code-block:: bash

    PATH=$PATH:~/.local/bin

.. _installation:extra_packages:

Extra Packages
''''''''''''''

The ``apache-airflow`` PyPI basic package only installs what's needed to get started.
Subpackages can be installed depending on what will be useful in your
environment. For instance, if you don't need connectivity with Postgres,
you won't have to go through the trouble of installing the ``postgres-devel``
yum package, or whatever equivalent applies on the distribution you are using.

Behind the scenes, Airflow does conditional imports of operators that require
these extra dependencies.

For the list of the subpackages and what they enable, see: :doc:`extra-packages-ref`.

.. _installation:provider_packages:

Provider packages
'''''''''''''''''

Unlike Apache Airflow 1.10, the Airflow 2.0 is delivered in multiple, separate, but connected packages.
The core of Airflow scheduling system is delivered as ``apache-airflow`` package and there are around
60 providers packages which can be installed separately as so called "Airflow Provider packages".
The default Airflow installation doesn't have many integrations and you have to install them yourself.

You can even develop and install your own providers for Airflow. For more information,
see: :doc:`apache-airflow-providers:index`

For the list of the provider packages and what they enable, see: :doc:`apache-airflow-providers:packages-ref`.


Initializing Airflow Database
'''''''''''''''''''''''''''''

Airflow requires a database to be initialized before you can run tasks. If
you're just experimenting and learning Airflow, you can stick with the
default SQLite option. If you don't want to use SQLite, then take a look at
:doc:`howto/initialize-database` to setup a different database.

After configuration, you'll need to initialize the database before you can
run tasks:

.. code-block:: bash

    airflow db init

Docker image
''''''''''''

Airflow is also distributed as a Docker image (OCI Image). For more information, see: :doc:`production-deployment`

Troubleshooting
'''''''''''''''

This section describes how to troubleshoot installation issues.

``Symbol not found: _Py_GetArgcArgv``
"""""""""""""""""""""""""""""""""""""

If you see ``Symbol not found: _Py_GetArgcArgv`` while starting or importing Airflow, this may mean that you are using an incompatible version of Python.
For a homebrew installed version of Python, this is generally caused by using Python in ``/usr/local/opt/bin`` rather than the Frameworks installation (e.g. for ``python 3.7``: ``/usr/local/opt/python@3.7/Frameworks/Python.framework/Versions/3.7``).

The crux of the issue is that a library Airflow depends on, ``setproctitle``, uses a non-public Python API
which is not available from the standard installation ``/usr/local/opt/`` (which symlinks to a path under ``/usr/local/Cellar``).

An easy fix is just to ensure you use a version of Python that has a dylib of the Python library available. For example:

.. code-block:: bash

  # Note: these instructions are for python3.7 but can be loosely modified for other versions
  brew install python@3.7
  virtualenv -p /usr/local/opt/python@3.7/Frameworks/Python.framework/Versions/3.7/bin/python3 .toy-venv
  source .toy-venv/bin/activate
  pip install apache-airflow
  python
  >>> import setproctitle
  # Success!

Alternatively, you can download and install Python directly from the `Python website <https://www.python.org/>`__.
