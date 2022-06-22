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

Static code checks
==================

The static code checks in Airflow are used to verify that the code meets certain quality standards.
All the static code checks can be run through pre-commit hooks.

The pre-commit hooks perform all the necessary installation when you run them
for the first time. See the table below to identify which pre-commit checks require the Breeze Docker images.

You can also run some static code checks via `Breeze <BREEZE.rst#aout-airflow-breeze>`_ environment
using available bash scripts.

Pre-commit hooks
----------------

Pre-commit hooks help speed up your local development cycle and place less burden on the CI infrastructure.
Consider installing the pre-commit hooks as a necessary prerequisite.

The pre-commit hooks by default only check the files you are currently working on and make
them fast. Yet, these checks use exactly the same environment as the CI tests
use. So, you can be sure your modifications will also work for CI if they pass
pre-commit hooks.

We have integrated the fantastic `pre-commit <https://pre-commit.com>`__ framework
in our development workflow. To install and use it, you need at least Python 3.6 locally.

Installing pre-commit hooks
...........................

It is the best to use pre-commit hooks when you have your local virtualenv for
Airflow activated since then pre-commit hooks and other dependencies are
automatically installed. You can also install the pre-commit hooks manually
using ``pip install``.

.. code-block:: bash

    pip install pre-commit

After installation, pre-commit hooks are run automatically when you commit the code and they will
only run on the files that you change during your commit, so they are usually pretty fast and do
not slow down your iteration speed on your changes. There are also ways to disable the ``pre-commits``
temporarily when you commit your code with ``--no-verify`` switch or skip certain checks that you find
to much disturbing your local workflow. See `Available pre-commit checks<#available-pre-commit-checks>`_
and `Using pre-commit <#using-pre-commit>`_

.. note:: Additional prerequisites might be needed

    The pre-commit hooks use several external linters that need to be installed before pre-commit is run.
    Each of the checks installs its own environment, so you do not need to install those, but there are some
    checks that require locally installed binaries. On Linux, you typically install
    them with ``sudo apt install``, on macOS - with ``brew install``.

The current list of prerequisites is limited to ``xmllint``:

- on Linux, install via ``sudo apt install libxml2-utils``
- on macOS, install via ``brew install libxml2``

Some pre-commit hooks also require the Docker Engine to be configured as the static
checks are executed in the Docker environment (See table in the
Available pre-commit checks<#available-pre-commit-checks>`_ . You should build the images
locally before installing pre-commit checks as described in `BREEZE.rst <BREEZE.rst>`__.

Sometimes your image is outdated and needs to be rebuilt because some dependencies have been changed.
In such cases, the Docker-based pre-commit will inform you that you should rebuild the image.

In case you do not have your local images built, the pre-commit hooks fail and provide
instructions on what needs to be done.

Enabling pre-commit hooks
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

Available pre-commit checks
...........................

This table lists pre-commit hooks used by Airflow. The ``Breeze`` column indicates which hooks
require Breeze Docker images to be installed locally.

.. note:: Disabling particular checks

    In case you have a problem with running particular ``pre-commit`` check you can still continue using the
    benefits of having ``pre-commit`` installed, with some of the checks disabled. In order to disable
    checks you need to set ``SKIP`` environment variable to coma-separated list of checks to skip. For example
    when you want to skip all checks that require Breeze Docker image to be installed, you should be able to
    do it by setting ``export SKIP=bat-in-container-tests,build,flake8,mypy``. You can also add this to your
    ``.bashrc`` or ``.zshrc`` if you do not want to set it manually every time you enter the terminal.

==================================== ================================================================ ============
**Checks**                            **Description**                                                  **Breeze**
==================================== ================================================================ ============
``airflow-config-yaml``                Checks that airflow config YAML is 1-1 with the code
------------------------------------ ---------------------------------------------------------------- ------------
``airflow-providers-available``        Checks that providers are properly declared by extras
------------------------------------ ---------------------------------------------------------------- ------------
``airflow-provider-yaml-files-ok``     Checks that providers YAML files are valid
------------------------------------ ---------------------------------------------------------------- ------------
``autoflake``                          Remove unused imports and unnecessary code
------------------------------------ ---------------------------------------------------------------- ------------
``base-operator``                      Checks that BaseOperator is imported properly
------------------------------------ ---------------------------------------------------------------- ------------
``bats-tests``                         Runs BATS bash unit tests
------------------------------------ ---------------------------------------------------------------- ------------
``bats-in-container-tests``            Run in Breeze container bats tests                                   *
------------------------------------ ---------------------------------------------------------------- ------------
``black``                              Runs Black (the uncompromising Python code formatter)
------------------------------------ ---------------------------------------------------------------- ------------
``blacken-docs``                       Run black on python code blocks in documentation files
------------------------------------ ---------------------------------------------------------------- ------------
``boring-cyborg``                      Checks for Boring Cyborg configuration consistency
------------------------------------ ---------------------------------------------------------------- ------------
``build``                              Builds image for mypy, flake8                                        *
------------------------------------ ---------------------------------------------------------------- ------------
``build-providers-dependencies``       Regenerates the JSON file with cross-provider dependencies
------------------------------------ ---------------------------------------------------------------- ------------
``check-apache-license``               Checks compatibility with Apache License requirements
------------------------------------ ---------------------------------------------------------------- ------------
``check-builtin-literals``             Require literal syntax when initializing Python builtin types
------------------------------------ ---------------------------------------------------------------- ------------
``check-executables-have-shebangs``    Checks that executables have shebang
------------------------------------ ---------------------------------------------------------------- ------------
``check-extras-order``                 Checks that extras in Dockerfile are sorted
------------------------------------ ---------------------------------------------------------------- ------------
``check-hooks-apply``                  Checks which hooks are applicable to the repository
------------------------------------ ---------------------------------------------------------------- ------------
``check-integrations``                 Checks if integration list is synchronized in code
------------------------------------ ---------------------------------------------------------------- ------------
``check-merge-conflicts``              Checks that merge conflicts are not being committed
------------------------------------ ---------------------------------------------------------------- ------------
``check-xml``                          Checks XML files with xmllint
------------------------------------ ---------------------------------------------------------------- ------------
``daysago-import-check``               Checks if daysago is properly imported
------------------------------------ ---------------------------------------------------------------- ------------
``debug-statements``                   Detects accidentally committed debug statements
------------------------------------ ---------------------------------------------------------------- ------------
``detect-private-key``                 Detects if private key is added to the repository
------------------------------------ ---------------------------------------------------------------- ------------
``doctoc``                             Refreshes the table of contents for MD files
------------------------------------ ---------------------------------------------------------------- ------------
``dont-use-safe-filter``               Don't use safe in templates
------------------------------------ ---------------------------------------------------------------- ------------
``no-providers-in-core-examples``      Don't use providers imports in core example DAGs
------------------------------------ ---------------------------------------------------------------- ------------
``no-relative-imports``                Use absolute imports, not relative
------------------------------------ ---------------------------------------------------------------- ------------
``end-of-file-fixer``                  Makes sure that there is an empty line at the end.
------------------------------------ ---------------------------------------------------------------- ------------
``fix-encoding-pragma``                Removes encoding header from python files
------------------------------------ ---------------------------------------------------------------- ------------
``pyupgrade``                          Runs pyupgrade
------------------------------------ ---------------------------------------------------------------- ------------
``flake8``                             Runs flake8                                                          *
------------------------------------ ---------------------------------------------------------------- ------------
``flynt``                              Runs flynt
------------------------------------ ---------------------------------------------------------------- ------------
``forbid-tabs``                        Fails if tabs are used in the project
------------------------------------ ---------------------------------------------------------------- ------------
``helm-lint``                          Verifies if helm lint passes for the chart
------------------------------------ ---------------------------------------------------------------- ------------
``identity``                           Prints inputs to the static check hooks for troubleshooting
------------------------------------ ---------------------------------------------------------------- ------------
``incorrect-use-of-LoggingMixin``      Checks if LoggingMixin is properly imported
------------------------------------ ---------------------------------------------------------------- ------------
``insert-license``                     Adds licenses for most file types
------------------------------------ ---------------------------------------------------------------- ------------
``isort``                              Sorts imports in python files
------------------------------------ ---------------------------------------------------------------- ------------
``json-schema``                        Checks that the files complies with the JSON Schema spec
------------------------------------ ---------------------------------------------------------------- ------------
``language-matters``                   Check for language that we do not accept as community
------------------------------------ ---------------------------------------------------------------- ------------
``lint-dockerfile``                    Lints a dockerfile
------------------------------------ ---------------------------------------------------------------- ------------
``lint-openapi``                       Lints openapi specification
------------------------------------ ---------------------------------------------------------------- ------------
``markdownlint``                       Lints Markdown files
------------------------------------ ---------------------------------------------------------------- ------------
``mermaid``                            Generates diagrams from mermaid files
------------------------------------ ---------------------------------------------------------------- ------------
``mixed-line-ending``                  Detects if mixed line ending is used (\r vs. \r\n)
------------------------------------ ---------------------------------------------------------------- ------------
``mypy``                               Runs mypy                                                            *
------------------------------------ ---------------------------------------------------------------- ------------
``persist-credentials-disabled``       Check that workflow files have persist-credentials disabled
------------------------------------ ---------------------------------------------------------------- ------------
``pre-commit-descriptions``            Check if all pre-commits are described in docs
------------------------------------ ---------------------------------------------------------------- ------------
``pre-commit-hook-names``              Check that hook names are not overly long
------------------------------------ ---------------------------------------------------------------- ------------
``pretty-format-json``                 Formats json files
------------------------------------ ---------------------------------------------------------------- ------------
``provide-create-sessions``            Make sure provide-session and create-session imports are OK
------------------------------------ ---------------------------------------------------------------- ------------
``providers-changelogs``               Updates documentation for providers changelogs
------------------------------------ ---------------------------------------------------------------- ------------
``providers-subpackages-init-file``    Check that providers' subpackages __init__.py files are there
------------------------------------ ---------------------------------------------------------------- ------------
``providers-init-file``                Check that provider's __init__.py file is removed
------------------------------------ ---------------------------------------------------------------- ------------
``provider-yamls``                     Checks that provider.yaml files have the correct content
------------------------------------ ---------------------------------------------------------------- ------------
``pydevd``                             Check for accidentally committed pydevd statements
------------------------------------ ---------------------------------------------------------------- ------------
``pydocstyle``                         Runs pydocstyle
------------------------------------ ---------------------------------------------------------------- ------------
``python-no-log-warn``                 Checks if there are no deprecate log warn
------------------------------------ ---------------------------------------------------------------- ------------
``restrict-start_date``                'start_date' should not be in default_args in example_dags
------------------------------------ ---------------------------------------------------------------- ------------
``rst-backticks``                      Checks if RST files use double backticks for code
------------------------------------ ---------------------------------------------------------------- ------------
``setup-order``                        Checks for an order of dependencies in setup.py
------------------------------------ ---------------------------------------------------------------- ------------
``setup-extra-packages``               Checks if all the libraries in setup.py are listed in docs
------------------------------------ ---------------------------------------------------------------- ------------
``shellcheck``                         Checks shell files with shellcheck
------------------------------------ ---------------------------------------------------------------- ------------
``sort-in-the-wild``                   Sort INTHEWILD.md alphabetically
------------------------------------ ---------------------------------------------------------------- ------------
``sort-spelling-wordlist``             Sort alphabetically and uniquify spelling_wordlist.txt
------------------------------------ ---------------------------------------------------------------- ------------
``stylelint``                          Checks CSS files with stylelint
------------------------------------ ---------------------------------------------------------------- ------------
``trailing-whitespace``                Removes trailing whitespace at end of line
------------------------------------ ---------------------------------------------------------------- ------------
``ui-lint``                            Static checks of airflow/ui/ folder
------------------------------------ ---------------------------------------------------------------- ------------
``update-breeze-file``                 Update output of breeze command in BREEZE.rst
------------------------------------ ---------------------------------------------------------------- ------------
``update-extras``                      Updates extras in the documentation
------------------------------------ ---------------------------------------------------------------- ------------
``update-local-yml-file``              Updates mounts in local.yml file
------------------------------------ ---------------------------------------------------------------- ------------
``update-setup-cfg-file``              Update setup.cfg file with all licenses
------------------------------------ ---------------------------------------------------------------- ------------
``update-supported-versions``          Updates supported versions in documentation
------------------------------------ ---------------------------------------------------------------- ------------
``update-versions``                    Updates latest versions in the documentation
------------------------------------ ---------------------------------------------------------------- ------------
``verify-db-migrations-documented``    Verify DB Migrations have been documented
------------------------------------ ---------------------------------------------------------------- ------------
``www-lint``                           Static checks of js in airflow/www/static/js/ folder
------------------------------------ ---------------------------------------------------------------- ------------
``yesqa``                              Removes unnecessary noqa statements
------------------------------------ ---------------------------------------------------------------- ------------
``yamllint``                           Checks YAML files with yamllint
==================================== ================================================================ ============

Using pre-commit
................

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


-   Run all checks only on files modified in the last locally available commit in your checked out branch:

.. code-block:: bash

    pre-commit run --source=HEAD^ --origin=HEAD


-   Show files modified automatically by pre-commit when pre-commits automatically fix errors

.. code-block:: bash

    pre-commit run --show-diff-on-failure

-   Skip one or more of the checks by specifying a comma-separated list of
    checks to skip in the SKIP variable:

.. code-block:: bash

    SKIP=mypy,flake8,build pre-commit run --all-files


You can always skip running the tests by providing ``--no-verify`` flag to the
``git commit`` command.

To check other usage types of the pre-commit framework, see `Pre-commit website <https://pre-commit.com/>`__.

Running static code checks via Breeze
-------------------------------------

The static code checks can be launched using the Breeze environment.

You run the static code checks via ``./breeze static-check`` or commands.

You can see the list of available static checks either via ``--help`` flag or by using the autocomplete
option. Note that the ``all`` static check runs all configured static checks.

Run the ``mypy`` check for the currently staged changes:

.. code-block:: bash

     ./breeze static-check mypy

Run the ``mypy`` check for all files:

.. code-block:: bash

     ./breeze static-check mypy -- --all-files

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

     ./breeze static-check all -- --all-files

Run all tests for last commit :

.. code-block:: bash

     ./breeze static-check all -- --from-ref HEAD^ --to-ref HEAD


The ``license`` check is run via a separate script and a separate Docker image containing the
Apache RAT verification tool that checks for Apache-compatibility of licenses within the codebase.
It does not take pre-commit parameters as extra arguments.

.. code-block:: bash

     ./breeze static-check licenses

Running Static Code Checks via scripts from the Host
....................................................

You can trigger the static checks from the host environment, without entering the Docker container. To do
this, run the following scripts:

* `<scripts/ci/docs/ci_docs.sh>`_ - checks that documentation can be built without warnings.
* `<scripts/ci/static_checks/check_license.sh>`_ - checks the licenses.
* `<scripts/ci/static_checks/flake8.sh>`_ - runs Flake8 source code style enforcement tool.
* `<scripts/ci/static_checks/lint_dockerfile.sh>`_ - runs lint checker for the dockerfiles.
* `<scripts/ci/static_checks/mypy.sh>`_ - runs a check for Mypy type annotation consistency.

The scripts may ask you to rebuild the images, if needed.

You can force rebuilding the images by deleting the ``.build`` directory. This directory keeps cached
information about the images already built and you can safely delete it if you want to start from scratch.

After documentation is built, the HTML results are available in the ``docs/_build/html``
folder. This folder is mounted from the host so you can access those files on your host as well.

Running static code checks in the Docker container
..................................................

If you are already in the Breeze Docker environment (by running the ``./breeze`` command),
you can also run the same static checks via run_scripts:

* Mypy: ``./scripts/in_container/run_mypy.sh airflow tests``
* Flake8: ``./scripts/in_container/run_flake8.sh``
* License check: ``./scripts/in_container/run_check_licence.sh``
* Documentation: ``./scripts/in_container/run_docs_build.sh``

Running static code checks for selected files
.............................................

In all static check scripts, both in the container and host versions, you can also pass a module/file path as
parameters of the scripts to only check selected modules or files. For example:

In the Docker container:

.. code-block::

  ./scripts/in_container/run_mypy.sh ./airflow/example_dags/

or

.. code-block::

  ./scripts/in_container/run_mypy.sh ./airflow/example_dags/test_utils.py

On the host:

.. code-block::

  ./scripts/ci/static_checks/mypy.sh ./airflow/example_dags/

.. code-block::

  ./scripts/ci/static_checks/mypy.sh ./airflow/example_dags/test_utils.py
