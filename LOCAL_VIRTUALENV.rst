
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

Local Virtual Environment (virtualenv)
======================================

Use the local virtualenv development option in the combination with the `Breeze
<BREEZE.rst#aout-airflow-breeze>`_ development environment. This option helps
you benefit from the infrastructure provided
by your IDE (for example, IntelliJ PyCharm/IntelliJ Idea) and work in the
environment where all necessary dependencies and tests are available and set up
within Docker images.

But you can also use the local virtualenv as a standalone development option if you
develop Airflow functionality that does not incur large external dependencies and
CI test coverage.

These are examples of the development options available with the local virtualenv in your IDE:

* local debugging;
* Airflow source view;
* autocompletion;
* documentation support;
* unit tests.

This document describes minimum requirements and insructions for using a standalone version of the local virtualenv.

Prerequisites
=============

Required Software Packages
--------------------------

Use system-level package managers like yum, apt-get for Linux, or
Homebrew for macOS to install required software packages:

* Python (2.7, 3.5 or 3.6)
* MySQL
* libxml

Refer to the `Dockerfile.ci <Dockerfile.ci>`__ for a comprehensive list
of required packages.

Extra Packages
--------------

You can also install extra packages (like ``[gcp]``, etc) via
``pip install -e [EXTRA1,EXTRA2 ...]``. However, some of them may
have additional install and setup requirements for your local system.

For example, if you have a trouble installing the mysql client on macOS and get
an error as follows:

.. code:: text

    ld: library not found for -lssl

you should set LIBRARY\_PATH before running ``pip install``:

.. code:: bash

    export LIBRARY_PATH=$LIBRARY_PATH:/usr/local/opt/openssl/lib/

You are STRONGLY encouraged to also install and use `pre-commit hooks <TESTING.rst#pre-commit-hooks>`_
for your local virtualenv development environment. Pre-commit hooks can speed up your
development cycle a lot.

The full list of extras is available in `<setup.py>`_.

Creating a Local virtualenv
===========================

To use your IDE for Airflow development and testing, you need to configure a virtual
environment. Ideally you should set up virtualenv for all Python versions that Airflow
supports (2.7, 3.5, 3.6).

To create and initialize the local virtualenv:

1. Create an environment with one of the two options:

   - Option 1: consider using one of the following utilities to create virtual environments and easily switch between them with the ``workon`` command:

    - `pyenv <https://github.com/pyenv/pyenv>`_
    - `pyenv-virtualenv <https://github.com/pyenv/pyenv-virtualenv>`_
    - `virtualenvwrapper <https://virtualenvwrapper.readthedocs.io/en/latest/>`_

    ``mkvirtualenv <ENV_NAME> --python=python<VERSION>``

   - Option 2: create a local virtualenv with Conda

    - install `miniconda3 <https://docs.conda.io/en/latest/miniconda.html>`_

    .. code-block:: bash

      conda create -n airflow python=3.6
      conda activate airflow

2. Install Python PIP requirements:

   .. code-block:: bash

    pip install -U -e ".[devel,<OTHER EXTRAS>]" # for example: pip install -U -e ".[devel,gcp,postgres]"

Note: when you first initialize database (the next step), you may encounter some problems.
This is because airflow by default will try to load in example dags where some of them requires dependencies ``gcp`` and ``postgres``.
You can solve the problem by:

- installing the extras i.e. ``[devel,gcp,postgres]`` or
- disable the example dags with environment variable: ``export AIRFLOW__CORE__LOAD_EXAMPLES=False`` or
- simply ignore the error messages and proceed

*In addition to above, you may also encounter problems during database migration.*
*This is a known issue and please see the progress here:* `AIRFLOW-6265 <https://issues.apache.org/jira/browse/AIRFLOW-6265>`_

3. Create the Airflow sqlite database:

   .. code-block:: bash

    # if necessary, start with a clean AIRFLOW_HOME, e.g.
    # rm -rf ~/airflow
    airflow db init

4. Select the virtualenv you created as the project's default virtualenv in your IDE.

Note that if you have the Breeze development environment installed, the ``breeze``
script can automate initializing the created virtualenv (steps 2 and 3).
Simply enter the Breeze environment by using ``workon`` and, once you are in it, run:

.. code-block:: bash

  ./breeze initialize-local-virtualenv

Running Tests
-------------

Running tests is described in `TESTING.rst <TESTING.rst>`_.

While most of the tests are typical unit tests that do not
require external components, there are a number of Integration tests. You can technically use local
virtualenv to run those tests, but it requires to set up a number of
external components (databases/queues/kubernetes and the like). So, it is
much easier to use the `Breeze <BREEZE.rst>`__ development environment
for Integration tests.

Note: Soon we will separate the integration and system tests out via pytest
so that you can clearly know which tests are unit tests and can be run in
the local virtualenv and which should be run using Breeze.

Connecting to database
----------------------

When analyzing the situation, it is helpful to be able to directly query the database. You can do it using
the built-in Airflow command:

.. code:: bash

    airflow db shell
