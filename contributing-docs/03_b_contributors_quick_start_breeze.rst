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

**********************************
Contributor's Quick Start - Breeze
**********************************

**The outline for this document in GitHub is available at top-right corner button (with 3-dots and 3 lines).**

`Breeze <../dev/breeze/doc/README.rst>`_ is a Docker-based Airflow development environment. As it is what's
used in Airflow's continuous integration (CI) process, it's a great tool to use to both develop Airflow and
test your changes against Airflow's CI image.


Pre-requisites
##############

It is recommended to run Breeze with at least 4GB RAM, 2 cores, and 20GB of disk space. It is possible to get
away with 2.5GB RAM and 20GB of disk space for some development tasks.

Installation
############

1. Install `Docker Desktop <https://docs.docker.com/get-docker/>`_
2. Install `Docker Compose <https://docs.docker.com/compose/install/>`_
3. Install pipx: ``pip install --user "pipx>=1.4.1"``
   1. MacOS users may want to consider using brew to install pipx: ``brew install pipx``
4. Add pipx to your PATH:
  1. If you installed pipx with pip on MacOS, run ``python -m pipx ensurepath``
  2. Otherwise, run ``pipx ensurepath``
5. Install Breeze: ``pipx install -e ./dev/breeze``
  3. Optionally configure autocomplete: ``breeze setup autocomplete``

For more detailed Breeze installation instructions, see the `Breeze installation guide <../dev/breeze/doc/01_installation.rst`_.
For troubleshooting Breeze installation, see the `Breeze troubleshooting guide <../dev/breeze/doc/04_troubleshooting.rst`_.

Running Airflow with Breeze
###########################

TODO: what happens if you run breeze start-airflow the first time you ever run breeze start-airflow?

For the quickest of quick starts, simply run ``breeze start-airflow``. This will spin up the Airflow database
(sqlite), the Airflow scheduler with the LocalExecutor, the Airflow triggerer, and the Airflow webserver --
everything you need for many simple Airflow development tasks.

This, however, is a very bare-bones Airflow environment. Many changes will require a database
(``breeze start-airflow --backend postgres``), a specific executor (``breeze start-airflow --executor CeleryExecutor``),
or some combination of configurations. To see what's configurable, run ``breeze start-airflow --help``.

Using Breeze and tmux
---------------------

Starting an Airflow environment using ``breeze start-airflow`` starts a `tmux <https://github.com/tmux/tmux/wiki>`_ session.
tmux opens four different windows in your terminal: The top-left is a shell within the container, the top-right runs and logs
the triggerer, the bottom-left runs and logs the scheduler, and the bottom-right runs and logs the webserver.

Navigating tmux can be a little tricky at first. Your search engine is your best friend for finding out how to do things
in tmux. One tip, though, is to hit ctrl+b then [ to enter "copy mode". This will enable you to scroll up and down in
a particular window. Hit q to exit copy mode. Click or tap on one of the four tmux windows to interact with that window.

To exit tmux and stop Airflow, run ``stop_airflow`` in the top-left window.

Following are some of important topics of `Breeze documentation <../dev/breeze/doc/README.rst>`__:

* `Breeze Installation <../dev/breeze/doc/01_installation.rst>`__
* `Installing Additional tools to the Docker Image <../dev/breeze/doc/02-customizing.rst#additional-tools-in-breeze-container>`__
* `Regular developer tasks <../dev/breeze/doc/03_developer_tasks.rst>`__
* `Cleaning the environment <../dev/breeze/doc/03_developer_tasks.rst#breeze-cleanup>`__
* `Troubleshooting Breeze environment <../dev/breeze/doc/04_troubleshooting.rst>`__

Running tests with Breeze
-------------------------

Many changes to the Airflow code base will require, at a minimum, unit tests, either a new one or updating an existing one. All tests exist within the ``tests/``
directory in the root of the Airflow project.

One advantage with Breeze is that all of the tests are run in the same environment as Airflow's continuous integration (CI)
pipeline.

Unit tests
^^^^^^^^^^

You can run unit tests either from inside the Breeze environment using ``pytest path/to/test.py``
or by using the ``breeze`` command from your terminal.

Unit tests from inside Breeze
"""""""""""""""""""""""""""""

After creating a Breeze environment with ``breeze start-airflow``, you can run unit tests directly from the Breeze
container using `pytest <https://docs.pytest.org/en/8.2.x/>`_ The below example will run all of the tests in the file
``tests/utils/test_dates.py``:

.. code-block:: bash

   root@63528318c8b1:/opt/airflow# pytest tests/utils/test_dates.py
   ============================================================= test session starts ==============================================================
   platform linux -- Python 3.8.16, pytest-7.2.1, pluggy-1.0.0 -- /usr/local/bin/python
   cachedir: .pytest_cache
   rootdir: /opt/airflow, configfile: pytest.ini
   plugins: timeouts-1.2.1, capture-warnings-0.0.4, cov-4.0.0, requests-mock-1.10.0, rerunfailures-11.1.1, anyio-3.6.2, instafail-0.4.2, time-machine-2.9.0, asyncio-0.20.3, httpx-0.21.3, xdist-3.2.0
   asyncio: mode=strict
   setup timeout: 0.0s, execution timeout: 0.0s, teardown timeout: 0.0s
   collected 12 items

   tests/utils/test_dates.py::TestDates::test_days_ago PASSED                                                                               [  8%]
   tests/utils/test_dates.py::TestDates::test_parse_execution_date PASSED                                                                   [ 16%]
   tests/utils/test_dates.py::TestDates::test_round_time PASSED                                                                             [ 25%]
   tests/utils/test_dates.py::TestDates::test_infer_time_unit PASSED                                                                        [ 33%]
   tests/utils/test_dates.py::TestDates::test_scale_time_units PASSED                                                                       [ 41%]
   tests/utils/test_dates.py::TestUtilsDatesDateRange::test_no_delta PASSED                                                                 [ 50%]
   tests/utils/test_dates.py::TestUtilsDatesDateRange::test_end_date_before_start_date PASSED                                               [ 58%]
   tests/utils/test_dates.py::TestUtilsDatesDateRange::test_both_end_date_and_num_given PASSED                                              [ 66%]
   tests/utils/test_dates.py::TestUtilsDatesDateRange::test_invalid_delta PASSED                                                            [ 75%]
   tests/utils/test_dates.py::TestUtilsDatesDateRange::test_positive_num_given PASSED                                                       [ 83%]
   tests/utils/test_dates.py::TestUtilsDatesDateRange::test_negative_num_given PASSED                                                       [ 91%]
   tests/utils/test_dates.py::TestUtilsDatesDateRange::test_delta_cron_presets PASSED                                                       [100%]

   ============================================================== 12 passed in 0.24s ==============================================================

You can also run specific tests one by one. For example, ``pytest tests/utils/test_dates.py::TestDates::test_days_ago``
would only run the test ``test_days_ago``. This can be really useful for quickly testing new tests.

Unit tests with the ``breeze`` command
""""""""""""""""""""""""""""""""""""""

You don't need to be inside a Breeze environment in order to use Breeze to run unit tests. For example,
``breeze testing tests tests/utils/test_dates.py`` will run all of the tests within the file ``tests/utils/test_dates.py``.

Other tests
^^^^^^^^^^^

Some tests require a database or specific executor to be configured. These tests are beyond the scope of the quick start.
More information can be found in the `contributor's guide for testing <09_testing.rst>`_. Here's a sneak peak, though,
which will run all tests against a Postgres database:

.. code-block:: bash

   breeze --backend postgres --postgres-version 15 --python 3.8 --db-reset testing tests --test-type All
