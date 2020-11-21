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

Getting Airflow
'''''''''''''''

Airflow is published as ``apache-airflow`` package in PyPI. Installing it however might be sometimes tricky
because Airflow is a bit of both a library and application. Libraries usually keep their dependencies open and
applications usually pin them, but we should do neither and both at the same time. We decided to keep
our dependencies as open as possible (in ``setup.cfg`` and ``setup.py``) so users can install different
version of libraries if needed. This means that from time to time plain ``pip install apache-airflow`` will
not work or will produce unusable Airflow installation.

In order to have repeatable installation, however, starting from **Airflow 1.10.10** and updated in
**Airflow 1.10.12** we also keep a set of "known-to-be-working" constraint files in the
``constraints-master`` and ``constraints-1-10`` orphan branches.
Those "known-to-be-working" constraints are per major/minor python version. You can use them as constraint
files when installing Airflow from PyPI. Note that you have to specify correct Airflow version
and python versions in the URL.

  **Prerequisites**

  On Debian based Linux OS:

  .. code-block:: bash

      sudo apt-get update
      sudo apt-get install build-essential


1. Installing just airflow

.. code-block:: bash

    AIRFLOW_VERSION=1.10.12
    PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
    # For example: 3.6
    CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
    # For example: https://raw.githubusercontent.com/apache/airflow/constraints-1.10.12/constraints-3.6.txt
    pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

2. Installing with extras (for example postgres, google)

.. code-block:: bash

    AIRFLOW_VERSION=1.10.12
    PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
    CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
    pip install "apache-airflow[postgres,google]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

Most of the extras are linked to a corresponding providers package. For example "amazon" extra
has a corresponding ``apache-airflow-providers-amazon`` providers package to be installed. When you install
Airflow with such extras, the necessary provider packages are installed automatically (latest versions from
PyPI for those packages). However you can freely upgrade and install provider packages independently from
the main Airflow installation.

.. note:: Automated installation of Provider packages does not work in Airflow 2.0.0b1 - for this version
          you have to install provider packages manually. As of Airflow 2.0.0b2 the corresponding
          provider packages are installed together with the extras.

Read more about it in the :ref:`Provider Packages <installation:provider_packages>` section.

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
For more information, see: :doc:`apache-airflow-providers:index`

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
