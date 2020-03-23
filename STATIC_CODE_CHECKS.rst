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

Static Code Checks
==================

The static code checks in Airflow are used to verify that the code meets certain quality standards.
All the static code checks can be run through pre-commit hooks.

Some of the static checks in pre-commits require Breeze Docker images to be installed locally.
The pre-commit hooks perform all the necessary installation when you run them
for the first time. See the table below to identify which pre-commit checks require the Breeze Docker images.

Sometimes your image is outdated and needs to be rebuilt because some dependencies have been changed.
In such cases, the Docker-based pre-commit will inform you that you should rebuild the image.

You can also run some static code checks via `Breeze <BREEZE.rst#aout-airflow-breeze>`_ environment
using available bash scripts.

Pre-commit Hooks
----------------

Pre-commit hooks help speed up your local development cycle and place less burden on the CI infrastructure.
Consider installing the pre-commit hooks as a necessary prerequisite.


This table lists pre-commit hooks used by Airflow and indicates which hooks
require Breeze Docker images to be installed locally:

=================================== ================================================================ ============
**Hooks**                             **Description**                                                 **Breeze**
=================================== ================================================================ ============
``base-operator``                     Checks that BaseOperator is imported properly
----------------------------------- ---------------------------------------------------------------- ------------
``build``                             Builds image for check-apache-licence, mypy, flake8.                 *
----------------------------------- ---------------------------------------------------------------- ------------
``check-apache-license``              Checks compatibility with Apache License requirements.               *
----------------------------------- ---------------------------------------------------------------- ------------
``check-executables-have-shebangs``   Checks that executables have shebang.
----------------------------------- ---------------------------------------------------------------- ------------
``check-hooks-apply``                 Checks which hooks are applicable to the repository.
----------------------------------- ---------------------------------------------------------------- ------------
``check-merge-conflict``              Checks if a merge conflict is committed.
----------------------------------- ---------------------------------------------------------------- ------------
``check-xml``                         Checks XML files with xmllint.
----------------------------------- ---------------------------------------------------------------- ------------
``debug-statements``                  Detects accidenatally committed debug statements.
----------------------------------- ---------------------------------------------------------------- ------------
``detect-private-key``                Detects if private key is added to the repository.
----------------------------------- ---------------------------------------------------------------- ------------
``doctoc``                            Refreshes the table of contents for md files.
----------------------------------- ---------------------------------------------------------------- ------------
``end-of-file-fixer``                 Makes sure that there is an empty line at the end.
----------------------------------- ---------------------------------------------------------------- ------------
``flake8``                            Runs flake8.                                                         *
----------------------------------- ---------------------------------------------------------------- ------------
``forbid-tabs``                       Fails if tabs are used in the project.
----------------------------------- ---------------------------------------------------------------- ------------
``insert-license``                    Adds licenses for most file types.
----------------------------------- ---------------------------------------------------------------- ------------
``isort``                             Sorts imports in python files.
----------------------------------- ---------------------------------------------------------------- ------------
``lint-dockerfile``                   Lints a dockerfile.
----------------------------------- ---------------------------------------------------------------- ------------
``mixed-line-ending``                 Detects if mixed line ending is used (\r vs. \r\n).
----------------------------------- ---------------------------------------------------------------- ------------
``mypy``                              Runs mypy.                                                           *
----------------------------------- ---------------------------------------------------------------- ------------
``pydevd``                            Check for accidentally commited pydevd statements.
----------------------------------- ---------------------------------------------------------------- ------------
``python-no-log-warn``                Checks if there are no deprecate log warn.
----------------------------------- ---------------------------------------------------------------- ------------
``rst-backticks``                     Checks if RST files use double backticks for code.
----------------------------------- ---------------------------------------------------------------- ------------
``setup-order``                       Checks for an order of dependencies in setup.py
----------------------------------- ---------------------------------------------------------------- ------------
``shellcheck``                        Checks shell files with shellcheck.
----------------------------------- ---------------------------------------------------------------- ------------
``update-breeze-file``                Update output of breeze command in BREEZE.rst.
----------------------------------- ---------------------------------------------------------------- ------------
``yamllint``                          Checks yaml files with yamllint.
=================================== ================================================================ ============

The pre-commit hooks only check the files you are currently working on and make
them fast. Yet, these checks use exactly the same environment as the CI tests
use. So, you can be sure your modifications will also work for CI if they pass
pre-commit hooks.

We have integrated the fantastic `pre-commit <https://pre-commit.com>`__ framework
in our development workflow. To install and use it, you need Python 3.6 locally.

It is the best to use pre-commit hooks when you have your local virtualenv for
Airflow activated since then pre-commit hooks and other dependencies are
automatically installed. You can also install the pre-commit hooks manually
using ``pip install``.

The pre-commit hooks require the Docker Engine to be configured as the static
checks are executed in the Docker environment. You should build the images
locally before installing pre-commit checks as described in `BREEZE.rst <BREEZE.rst>`__.
In case you do not have your local images built, the
pre-commit hooks fail and provide instructions on what needs to be done.

Prerequisites for Pre-commit Hooks
..................................

The pre-commit hooks use several external linters that need to be installed before pre-commit is run.

Each of the checks installs its own environment, so you do not need to install those, but there are some
checks that require locally installed binaries. On Linux, you typically install
them with ``sudo apt install``, on macOS - with ``brew install``.

The current list of prerequisites is limited to ``xmllint``:

- on Linux, install via ``sudo apt install libxml2-utils``;

- on macOS, install via ``brew install libxml2``.

Enabling Pre-commit Hooks
.........................

To turn on pre-commit checks for ``commit`` operations in git, enter:

.. code-block:: bash

    pre-commit install


To install the checks also for ``pre-push`` operations, enter:

.. code-block:: bash

    pre-commit install -t pre-push


For details on advanced usage of the install method, use:

.. code-block:: bash

   pre-commit install --help


Using Pre-commit Hooks
......................

After installation, pre-commit hooks are run automatically when you commit the
code. But you can run pre-commit hooks manually as needed.

-   Run all checks on your staged files by using:

.. code-block:: bash

    pre-commit run


-   Run only mypy check on your staged files by using:

.. code-block:: bash

    pre-commit run mypy


-   Run only mypy checks on all files by using:

.. code-block:: bash

    pre-commit run mypy --all-files


-   Run all checks on all files by using:

.. code-block:: bash

    pre-commit run --all-files


-   Skip one or more of the checks by specifying a comma-separated list of
    checks to skip in the SKIP variable:

.. code-block:: bash

    SKIP=mypy pre-commit run --all-files


You can always skip running the tests by providing ``--no-verify`` flag to the
``git commit`` command.

To check other usage types of the pre-commit framework, see `Pre-commit website <https://pre-commit.com/>`__.

Running Static Code Checks via Breeze
-------------------------------------

The static code checks can be launched using the Breeze environment.

You run the static code checks via ``./breeze static-check`` or ``./breeze static-check-all-files`` commands.
The former ones run appropriate checks only for files changed and staged locally, the latter ones
run checks on all files.

You can see the list of available static checks either via ``--help`` flag or by using the autocomplete
option. Note that the ``all`` static check runs all configured static checks.

Run the ``mypy`` check for the currently staged changes:

.. code-block:: bash

     ./breeze static-check mypy

Run the ``mypy`` check for all files:

.. code-block:: bash

     ./breeze static-check-all-files mypy

Run the ``flake8`` check for the ``tests.core.py`` file with verbose output:

.. code-block:: bash

     ./breeze static-check flake8 -- --files tests/core.py --verbose

Run the ``flake8`` check for the ``tests.core`` package with verbose output:

.. code-block:: bash

     ./breeze static-check mypy -- --files tests/hooks/test_druid_hook.py

Run all tests for the currently staged files:

.. code-block:: bash

     ./breeze static-check all

Run all tests for all files:

.. code-block:: bash

     ./breeze static-check-all-files all


The ``license`` check is run via the same Docker image containing the
Apache RAT verification tool that checks for Apache-compatibility of licenses within the codebase.
It does not take pre-commit parameters as extra arguments.

.. code-block:: bash

     ./breeze static-check-all-files licenses

Running Static Code Checks via Scripts from the Host
....................................................

You can trigger the static checks from the host environment, without entering the Docker container. To do
this, run the following scripts (the same is done in Travis CI):

* `<scripts/ci/ci_check_license.sh>`_ - checks the licenses.
* `<scripts/ci/ci_docs.sh>`_ - checks that documentation can be built without warnings.
* `<scripts/ci/ci_flake8.sh>`_ - runs Flake8 source code style enforcement tool.
* `<scripts/ci/ci_lint_dockerfile.sh>`_ - runs lint checker for the dockerfiles.
* `<scripts/ci/ci_mypy.sh>`_ - runs a check for mypy type annotation consistency.

The scripts may ask you to rebuild the images, if needed.

You can force rebuilding the images by deleting the ``.build`` directory. This directory keeps cached
information about the images already built and you can safely delete it if you want to start from scratch.

After documentation is built, the HTML results are available in the ``docs/_build/html``
folder. This folder is mounted from the host so you can access those files on your host as well.

Running Static Code Checks in the Docker Container
..................................................

If you are already in the Breeze Docker environment (by running the ``./breeze`` command),
you can also run the same static checks via run_scripts:

* Mypy: ``./scripts/ci/in_container/run_mypy.sh airflow tests``
* Flake8: ``./scripts/ci/in_container/run_flake8.sh``
* License check: ``./scripts/ci/in_container/run_check_licence.sh``
* Documentation: ``./scripts/ci/in_container/run_docs_build.sh``

Running Static Code Checks for Selected Files
.............................................

In all static check scripts, both in the container and host versions, you can also pass a module/file path as
parameters of the scripts to only check selected modules or files. For example:

In the Docker container:

.. code-block::

  ./scripts/ci/in_container/run_mypy.sh ./airflow/example_dags/

or

.. code-block::

  ./scripts/ci/in_container/run_mypy.sh ./airflow/example_dags/test_utils.py

On the host:

.. code-block::

  ./scripts/ci/ci_mypy.sh ./airflow/example_dags/

.. code-block::

  ./scripts/ci/ci_mypy.sh ./airflow/example_dags/test_utils.py
