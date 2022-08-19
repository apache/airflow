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
in our development workflow. To install and use it, you need at least Python 3.7 locally.

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

This table lists pre-commit hooks used by Airflow. The ``Image`` column indicates which hooks
require Breeze Docker image to be build locally.

.. note:: Disabling particular checks

  In case you have a problem with running particular ``pre-commit`` check you can still continue using the
  benefits of having ``pre-commit`` installed, with some of the checks disabled. In order to disable
  checks you might need to set ``SKIP`` environment variable to coma-separated list of checks to skip. For example
  when you want to skip some checks (flake/mypy for example), you should be able to do it by setting
  ``export SKIP=run-flake8,run-mypy``. You can also add this to your ``.bashrc`` or ``.zshrc`` if you
  do not want to set it manually every time you enter the terminal.

  In case you do not have breeze image configured locally, you can also disable all checks that require
  the image by setting ``SKIP_IMAGE_PRE_COMMITS`` to "true". This will mark the tests as "green" automatically
  when run locally (note that those checks will anyway run in CI).

  .. BEGIN AUTO-GENERATED STATIC CHECK LIST

+--------------------------------------------------------+------------------------------------------------------------------+---------+
| ID                                                     | Description                                                      | Image   |
+========================================================+==================================================================+=========+
| black                                                  | Run Black (the uncompromising Python code formatter)             |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| blacken-docs                                           | Run black on python code blocks in documentation files           |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| check-airflow-2-2-compatibility                        | Check that providers are 2.2 compatible.                         |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| check-airflow-config-yaml-consistent                   | Checks for consistency between config.yml and default_config.cfg |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| check-apache-license-rat                               | Check if licenses are OK for Apache                              |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| check-base-operator-partial-arguments                  | Check BaseOperator and partial() arguments                       |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| check-base-operator-usage                              | * Check BaseOperator[Link] core imports                          |         |
|                                                        | * Check BaseOperator[Link] other imports                         |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| check-boring-cyborg-configuration                      | Checks for Boring Cyborg configuration consistency               |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| check-breeze-top-dependencies-limited                  | Breeze should have small number of top-level dependencies        |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| check-builtin-literals                                 | Require literal syntax when initializing Python builtin types    |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| check-changelog-has-no-duplicates                      | Check changelogs for duplicate entries                           |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| check-daysago-import-from-utils                        | Make sure days_ago is imported from airflow.utils.dates          |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| check-decorated-operator-implements-custom-name        | Check @task decorator implements custom_operator_name            |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| check-docstring-param-types                            | Check that docstrings do not specify param types                 |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| check-example-dags-urls                                | Check that example dags url include provider versions            |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| check-executables-have-shebangs                        | Check that executables have shebang                              |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| check-extra-packages-references                        | Checks setup extra packages                                      |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| check-extras-order                                     | Check order of extras in Dockerfile                              |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| check-for-inclusive-language                           | Check for language that we do not accept as community            |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| check-hooks-apply                                      | Check if all hooks apply to the repository                       |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| check-incorrect-use-of-LoggingMixin                    | Make sure LoggingMixin is not used alone                         |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| check-integrations-are-consistent                      | Check if integration list is consistent in various places        |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| check-lazy-logging                                     | Check that all logging methods are lazy                          |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| check-merge-conflict                                   | Check that merge conflicts are not being committed               |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| check-newsfragments-are-valid                          | Check newsfragments are valid                                    |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| check-no-providers-in-core-examples                    | No providers imports in core example DAGs                        |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| check-no-relative-imports                              | No relative imports                                              |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| check-persist-credentials-disabled-in-github-workflows | Check that workflow files have persist-credentials disabled      |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| check-pre-commit-information-consistent                | Update information re pre-commit hooks and verify ids and names  |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| check-provide-create-sessions-imports                  | Check provide_session and create_session imports                 |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| check-provider-yaml-valid                              | Validate providers.yaml files                                    |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| check-providers-init-file-missing                      | Provider init file is missing                                    |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| check-providers-subpackages-init-file-exist            | Provider subpackage init files are there                         |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| check-pydevd-left-in-code                              | Check for pydevd debug statements accidentally left              |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| check-revision-heads-map                               | Check that the REVISION_HEADS_MAP is up-to-date                  |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| check-safe-filter-usage-in-html                        | Don't use safe in templates                                      |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| check-setup-order                                      | Check order of dependencies in setup.cfg and setup.py            |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| check-start-date-not-used-in-defaults                  | 'start_date' not to be defined in default_args in example_dags   |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| check-system-tests-present                             | Check if system tests have required segments of code             |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| check-system-tests-tocs                                | Check that system tests is properly added                        |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| check-xml                                              | Check XML files with xmllint                                     |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| codespell                                              | Run codespell to check for common misspellings in files          |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| compile-www-assets                                     | Compile www assets                                               |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| compile-www-assets-dev                                 | Compile www assets in dev mode                                   |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| create-missing-init-py-files-tests                     | Create missing init.py files in tests                            |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| debug-statements                                       | Detect accidentally committed debug statements                   |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| detect-private-key                                     | Detect if private key is added to the repository                 |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| doctoc                                                 | Add TOC for md and rst files                                     |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| end-of-file-fixer                                      | Make sure that there is an empty line at the end                 |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| fix-encoding-pragma                                    | Remove encoding header from python files                         |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| flynt                                                  | Run flynt string format converter for Python                     |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| identity                                               | Print input to the static check hooks for troubleshooting        |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| insert-license                                         | * Add license for all SQL files                                  |         |
|                                                        | * Add license for all rst files                                  |         |
|                                                        | * Add license for all CSS/JS/PUML/TS/TSX files                   |         |
|                                                        | * Add license for all JINJA template files                       |         |
|                                                        | * Add license for all shell files                                |         |
|                                                        | * Add license for all Python files                               |         |
|                                                        | * Add license for all XML files                                  |         |
|                                                        | * Add license for all YAML files                                 |         |
|                                                        | * Add license for all md files                                   |         |
|                                                        | * Add license for all other files                                |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| isort                                                  | Run isort to sort imports in Python files                        |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| lint-chart-schema                                      | Lint chart/values.schema.json file                               |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| lint-css                                               | stylelint                                                        |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| lint-dockerfile                                        | Lint dockerfile                                                  |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| lint-helm-chart                                        | Lint Helm Chart                                                  |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| lint-javascript                                        | ESLint against airflow/ui                                        |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| lint-json-schema                                       | * Lint JSON Schema files with JSON Schema                        |         |
|                                                        | * Lint NodePort Service with JSON Schema                         |         |
|                                                        | * Lint Docker compose files with JSON Schema                     |         |
|                                                        | * Lint chart/values.schema.json file with JSON Schema            |         |
|                                                        | * Lint chart/values.yaml file with JSON Schema                   |         |
|                                                        | * Lint airflow/config_templates/config.yml file with JSON Schema |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| lint-markdown                                          | Run markdownlint                                                 |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| lint-openapi                                           | * Lint OpenAPI using spectral                                    |         |
|                                                        | * Lint OpenAPI using openapi-spec-validator                      |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| mixed-line-ending                                      | Detect if mixed line ending is used (\r vs. \r\n)                |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| pretty-format-json                                     | Format json files                                                |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| pydocstyle                                             | Run pydocstyle                                                   |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| python-no-log-warn                                     | Check if there are no deprecate log warn                         |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| pyupgrade                                              | Upgrade Python code automatically                                |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| replace-bad-characters                                 | Replace bad characters                                           |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| rst-backticks                                          | Check if RST files use double backticks for code                 |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| run-flake8                                             | Run flake8                                                       | *       |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| run-mypy                                               | * Run mypy for dev                                               | *       |
|                                                        | * Run mypy for core                                              |         |
|                                                        | * Run mypy for providers                                         |         |
|                                                        | * Run mypy for /docs/ folder                                     |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| run-shellcheck                                         | Check Shell scripts syntax correctness                           |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| static-check-autoflake                                 | Remove all unused code                                           |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| trailing-whitespace                                    | Remove trailing whitespace at end of line                        |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| ts-compile-and-lint-javascript                         | TS types generation and ESLint against current UI files          |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| update-breeze-cmd-output                               | Update output of breeze commands in BREEZE.rst                   |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| update-breeze-readme-config-hash                       | Update Breeze README.md with config files hash                   |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| update-extras                                          | Update extras in documentation                                   |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| update-in-the-wild-to-be-sorted                        | Sort INTHEWILD.md alphabetically                                 |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| update-inlined-dockerfile-scripts                      | Inline Dockerfile and Dockerfile.ci scripts                      |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| update-local-yml-file                                  | Update mounts in the local yml file                              |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| update-migration-references                            | Update migration ref doc                                         | *       |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| update-providers-dependencies                          | Update cross-dependencies for providers packages                 |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| update-setup-cfg-file                                  | Update setup.cfg file with all licenses                          |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| update-spelling-wordlist-to-be-sorted                  | Sort alphabetically and uniquify spelling_wordlist.txt           |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| update-supported-versions                              | Updates supported versions in documentation                      |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| update-vendored-in-k8s-json-schema                     | Vendor k8s definitions into values.schema.json                   |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| update-version                                         | Update version to the latest version in the documentation        |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| yamllint                                               | Check YAML files with yamllint                                   |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+
| yesqa                                                  | Remove unnecessary noqa statements                               |         |
+--------------------------------------------------------+------------------------------------------------------------------+---------+

  .. END AUTO-GENERATED STATIC CHECK LIST

Using pre-commit
................

After installation, pre-commit hooks are run automatically when you commit the
code. But you can run pre-commit hooks manually as needed.

-   Run all checks on your staged files by using:

.. code-block:: bash

    pre-commit run

-   Run only mypy check on your staged files by using:

.. code-block:: bash

    pre-commit run run-mypy

-   Run only mypy checks on all files by using:

.. code-block:: bash

    pre-commit run run-mypy --all-files


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

    SKIP=run-mypy,run-flake8 pre-commit run --all-files


You can always skip running the tests by providing ``--no-verify`` flag to the
``git commit`` command.

To check other usage types of the pre-commit framework, see `Pre-commit website <https://pre-commit.com/>`__.

Running static code checks via Breeze
-------------------------------------

The static code checks can be launched using the Breeze environment.

You run the static code checks via ``breeze static-check`` or commands.

You can see the list of available static checks either via ``--help`` flag or by using the autocomplete
option. Note that the ``all`` static check runs all configured static checks.

Run the ``mypy`` check for the currently staged changes:

.. code-block:: bash

     breeze static-checks --type run-mypy

Run the ``mypy`` check for all files:

.. code-block:: bash

     breeze static-checks --type run-mypy --all-files

Run the ``flake8`` check for the ``tests.core.py`` file with verbose output:

.. code-block:: bash

     breeze static-checks --type run-flake8 --file tests/core.py --verbose

Run the ``flake8`` check for the ``tests.core`` package with verbose output:

.. code-block:: bash

     breeze static-checks --type run-flake8 --file tests/core/* --verbose

Run all checks for the currently staged files:

.. code-block:: bash

     breeze static-checks --type all

Run all checks for all files:

.. code-block:: bash

    breeze static-checks --type all --all-files

Run all checks for last commit :

.. code-block:: bash

     breeze static-checks --type all --last-commit

Debugging pre-commit check scripts requiring image
--------------------------------------------------

Those commits that use Breeze docker image might sometimes fail, depending on your operating system and
docker setup, so sometimes it might be required to run debugging with the commands. This is done via
two environment variables ``VERBOSE`` and ``DRY_RUN``. Setting them to "true" will respectively show the
commands to run before running them or skip running the commands.

Note that you need to run pre-commit with --verbose command to get the output regardless of the status
of the static check (normally it will only show output on failure).

Printing the commands while executing:

.. code-block:: bash

     VERBOSE="true" pre-commit run --verbose run-flake8

Just performing dry run:

.. code-block:: bash

     DRY_RUN="true" pre-commit run --verbose run-flake8
