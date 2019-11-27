
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
============================================

Use the local virtualenv development option in the combination with the _`Breeze <BREEZE.rst#aout-airflow-breeze>`_
development environment. This option helps you benefit from the infrastructure provided
by your IDE (for example, IntelliJ's PyCharm/Idea) and work in the enviroment where all necessary dependencies and tests are 
available and set up within Docker images.

But you can also use the local virtualenv as a standalone development option of
you develop Airflow functionality that does not incur large external dependencies and 
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

* Python (3.5 or 3.6)
* MySQL
* libxml

Refer to the `Dockerfile <Dockerfile>`__ for a comprehensive list
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

You are STRONGLY encouraged to also install and use `pre-commit hooks <CONTRIBUTING.rst#pre-commit-hooks>`_ 
for your local virtualenv development environment. Pre-commit hooks can speed up your 
development cycle a lot.

The full list of extras is available in `<setup.py>`_.

Creating a Local virtualenv
===========================

To use your IDE for Airflow development and testing, you need to configure a virtual 
environment. Ideally you should set up virtualenv for all Python versions that Airflow
supports (3.5, 3.6). 

Consider using one of the following utilities to create virtual environments and easily 
switch between them with the ``workon`` command:

- `pyenv <https://github.com/pyenv/pyenv>`_
- `pyenv-virtualenv <https://github.com/pyenv/pyenv-virtualenv>`_
- `virtualenvwrapper <https://virtualenvwrapper.readthedocs.io/en/latest/>`_

To create and initialize the local virtualenv:

1. Create an environment as follows:

   ``mkvirtualenv <ENV_NAME> --python=python<VERSION>``

2. Install Python PIP requirements:

   ``pip install -e ".[devel]"``

3. Create the Airflow sqlite database:

   ``airflow db init``

4. Select the virtualenv you created as the project's default virtualenv in your IDE.

Note that if you have the Breeze development environment installed, the ``breeze`` 
script can automate initializing the created virtualenv (steps 2 and 3).
Simply enter the Breeze environment by using ``workon`` and, once you are in it, run:

.. code-block:: bash

  ./breeze --initialize-local-virtualenv

Debugging and Running Tests
===========================

When you set up the local virtualenv, you can use the usual **Run Test** option of the IDE, have all the
autocomplete and documentation support from IDE as well as you can debug and click-through
the sources of Airflow, which is very helpful during development.

Local and Remote Debugging
--------------------------

One of the great benefits of using the local virtualenv is an option to run
local debugging in your IDE graphical interface. You can also use ``ipdb``
if you prefer _`console debugging <BREEZE.rst#debugging-with-ipdb>`_.

When you run example DAGs, even if you run them using unit tests within IDE, they are run in a separate
container. This makes it a little harder to use with IDE built-in debuggers.
Fortunately, IntelliJ/PyCharm provides an effective remote debugging feature (but only in paid versions).
See additional details on
`remote debugging <https://www.jetbrains.com/help/pycharm/remote-debugging-with-product.html>`_.

You can set up your remote debugging session as follows:

.. image:: images/setup_remote_debugging.png
    :align: center
    :alt: Setup remote debugging

Note that on macOS, you have to use a real IP address of your host rather than default
localhost because on macOS the container runs in a virtual machine with a different IP address.

Make sure to configure source code mapping in the remote debugging configuration to map
your local sources to the ``/opt/airflow`` location of the sources within the container:

.. image:: images/source_code_mapping_ide.png
    :align: center
    :alt: Source code mapping

Running Unit Tests via IDE
--------------------------

Usually you can run most of the unit tests (those that do not have dependencies such as 
Postgres/MySQL/Hadoop/etc.) directly from the IDE:

.. image:: images/running_unittests.png
    :align: center
    :alt: Running unit tests

Some of the core tests use dags defined in ``tests/dags`` folder. Those tests should have
``AIRFLOW__CORE__UNIT_TEST_MODE`` set to True. You can set it up in your test configuration:

.. image:: images/airflow_unit_test_mode.png
    :align: center
    :alt: Airflow Unit test mode

Running Tests via Script
------------------------

You can also use the ``run-tests`` script that provides a Python
testing framework with more than 300 tests including integration, unit, and
system tests. 

The script is in the path in the Breeze environment but you need to prepend 
it with ``./`` when running in the local virtualenv: ``./run-tests``.

This script has several flags that can be useful for your testing.

.. code:: text

    Usage: run-tests [FLAGS] [TESTS_TO_RUN] -- <EXTRA_NOSETEST_ARGS>

    Runs tests specified (or all tests if no tests are specified).

    Flags:

    -h, --help
            Shows this help message.

    -i, --with-db-init
            Forces database initialization before tests.

    -s, --nocapture
            Doesn't capture stdout when running the tests. This is useful if you are
            debugging with ipdb and want to drop into the console with it
            by adding this line to source code:

                import ipdb; ipdb.set_trace()

    -v, --verbose
            Provides verbose output showing coloured output of tests being run and summary
            of the tests (in a manner similar to the tests run in the CI environment).

You can pass extra parameters to ``nose``, by adding ``nose`` arguments after
``--``. For example, to just execute the "core" unit tests and add ipdb
set\_trace method, you can run the following command:

.. code:: bash

    ./run-tests tests.core:TestCore --nocapture --verbose

or a single test method without colors or debug logs:

.. code:: bash

    ./run-tests tests.core:TestCore.test_check_operators

Note that the first time it runs, the ``./run_tests`` script 
performs a database initialization. If you run further tests without
leaving the environment, the database will not be initialized. But you
can always force the database initialization with the ``--with-db-init``
(``-i``) switch. The script will inform you what you can do when it is
run.

In general, the ``run-tests`` script can be used to run unit, integration and system tests. Currently, when you run tests not supported in the local virtualenv, the script may either fail or provide an error message.

Running Unit Tests from the IDE
-----------------------------------

Once you created the local virtualenv and selected it as the default project's environment, 
running unit tests from the IDE is as simple as:

.. figure:: images/run_unittests.png
   :alt: Run unittests


Running Integration Tests
-------------------------

While most of the tests are typical unit tests that do not
require external components, there are a number of integration and
system tests. You can technically use local
virtualenv to run those tests, but it requires to set up a number of
external components (databases/queues/kubernetes and the like). So, it is
much easier to use the `Breeze development environment <BREEZE.rst>`_
for integration and system tests.

Note: Soon we will separate the integration and system tests out
so that you can clearly know which tests are unit tests and can be run in
the local virtualenv and which should be run using Breeze.

Connecting to database
----------------------

When analyzing the situation, it is helpful to be able to directly query the database. You can do it using
the built-in Airflow command:

.. code:: bash

    airflow db shell
