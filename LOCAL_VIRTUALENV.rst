
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


Local virtualenv environment
============================

Installation
------------

Install Python (3.5 or 3.6), MySQL, and libxml by using system-level
package managers like yum, apt-get for Linux, or Homebrew for Mac OS at
first. Refer to the `Dockerfile <Dockerfile>`__ for a comprehensive list
of required packages.

In order to use your IDE you need you can use the virtual environment.
Ideally you should setup virtualenv for all python versions that Airflow
supports (3.5, 3.6). An easy way to create the virtualenv is to use
`virtualenvwrapper <https://virtualenvwrapper.readthedocs.io/en/latest/>`__
- it allows you to easily switch between virtualenvs using ``workon``
command and mange your virtual environments more easily. Typically
creating the environment can be done by:

.. code:: bash

    mkvirtualenv <ENV_NAME> --python=python<VERSION>

Then you need to install python PIP requirements. Typically it can be
done with: ``pip install -e ".[devel]"``.

After creating the virtualenv, run this command to create the Airflow
sqlite database:

.. code:: bash

    airflow db init


Creating virtualenv can be automated with `Breeze environment <BREEZE.rst#configuring-local-virtualenv>`_

Once initialization is done, you should select the virtualenv you
initialized as the project's default virtualenv in your IDE.

After setting it up - you can use the usual "Run Test" option of the IDE
and have the autocomplete and documentation support from IDE as well as
you can debug and view the sources of Airflow - which is very helpful
during development.

Installing other extras
-----------------------

You can also other extras (like ``[mysql]``, ``[gcp]`` etc. via
``pip install -e [EXTRA1,EXTRA2 ...]``. However some of the extras have additional
system requirements and you might need to install additional packages on your
local machine.

For example if you have trouble installing mysql client on MacOS and you have
an error similar to

.. code:: text

    ld: library not found for -lssl

you should set LIBRARY\_PATH before running ``pip install``:

.. code:: bash

    export LIBRARY_PATH=$LIBRARY_PATH:/usr/local/opt/openssl/lib/

The full list of extras is available in `<setup.py>`_


Running individual tests
------------------------

Once you activate virtualenv (or enter docker container) as described
below you should be able to run ``run-tests`` at will (it is in the path
in Docker environment but you need to prepend it with ``./`` in local
virtualenv (``./run-tests``).

Note that this script has several flags that can be useful for your
testing.

.. code:: text

    Usage: run-tests [FLAGS] [TESTS_TO_RUN] -- <EXTRA_NOSETEST_ARGS>

    Runs tests specified (or all tests if no tests are specified)

    Flags:

    -h, --help
            Shows this help message.

    -i, --with-db-init
            Forces database initialization before tests

    -s, --nocapture
            Don't capture stdout when running the tests. This is useful if you are
            debugging with ipdb and want to drop into console with it
            by adding this line to source code:

                import ipdb; ipdb.set_trace()

    -v, --verbose
            Verbose output showing coloured output of tests being run and summary
            of the tests - in a manner similar to the tests run in the CI environment.

You can pass extra parameters to nose, by adding nose arguments after
``--``

For example, in order to just execute the "core" unit tests and add ipdb
set\_trace method, you can run the following command:

.. code:: bash

    ./run-tests tests.core:TestCore --nocapture --verbose

or a single test method without colors or debug logs:

.. code:: bash

    ./run-tests tests.core:TestCore.test_check_operators

Note that ``./run_tests`` script runs tests but the first time it runs,
it performs database initialisation. If you run further tests without
leaving the environment, the database will not be initialized, but you
can always force database initialization with ``--with-db-init``
(``-i``) switch. The scripts will inform you what you can do when they
are run.

Running tests directly from the IDE
-----------------------------------

Once you configure your tests to use the virtualenv you created. running
tests from IDE is as simple as:

.. figure:: images/run_unittests.png
   :alt: Run unittests


Running integration tests
-------------------------

Note that while most of the tests are typical "unit" tests that do not
require external components, there are a number of tests that are more
of "integration" or even "system" tests. You can technically use local
virtualenv to run those tests, but it requires to setup a number of
external components (databases/queues/kubernetes and the like) so it is
much easier to use the `Breeze development environment <BREEZE.rst>`_
for those tests.

Note - soon we will separate the integration and system tests out
so that you can clearly know which tests are unit tests and can be run in
the local virtualenv and which should be run using Breeze.
