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

Contributions
=============

Contributions are welcome and are greatly appreciated! Every little bit helps,
and credit will always be given.

Report Bugs
-----------

Report bugs through `Apache
Jira <https://issues.apache.org/jira/browse/AIRFLOW>`__.

Please report relevant information and preferably code that exhibits the
problem.

Fix Bugs
--------

Look through the JIRA issues for bugs. Anything is open to whoever wants to
implement it.

Implement Features
------------------

Look through the `Apache
JIRA <https://issues.apache.org/jira/browse/AIRFLOW>`__ for features.

Any unassigned "Improvement" issue is open to whoever wants to implement it.

We've created the operators, hooks, macros and executors we needed, but we've
made sure that this part of Airflow is extensible. New operators, hooks, macros
and executors are very welcomed!

Improve Documentation
---------------------

Airflow could always use better documentation, whether as part of the official
Airflow docs, in docstrings, ``docs/*.rst`` or even on the web as blog posts or
articles.

Submit Feedback
---------------

The best way to send feedback is to open an issue on `Apache
JIRA <https://issues.apache.org/jira/browse/AIRFLOW>`__.

If you are proposing a new feature:

-   Explain in detail how it would work.
-   Keep the scope as narrow as possible to make it easier to implement.
-   Remember that this is a volunteer-driven project, and that contributions are
    welcome :)

Documentation
=============

The latest API documentation is usually available
`here <https://airflow.apache.org/>`__.

To generate a local version:

1.  Set up an Airflow development environment.

2.  Install the ``doc`` extra.

.. code-block:: bash

    pip install -e '.[doc]'


3.  Generate and serve the documentation as follows:

.. code-block:: bash

    cd docs
    ./build.sh
    ./start_doc_server.sh


Pull Request Guidelines
=======================

Before you submit a pull request (PR) from your forked repo, check that it meets
these guidelines:

-   Include tests, either as doctests, unit tests, or both, to your pull
    request.

    The airflow repo uses `Travis CI <https://travis-ci.org/apache/airflow>`__ to
    run the tests and `codecov <https://codecov.io/gh/apache/airflow>`__ to track
    coverage. You can set up both for free on your fork (see
    `Travis CI Testing Framework <#travis-ci-testing-framework>`__ section below).
    It will help you make sure you do not break the build with your PR and
    that you help increase coverage.

-   `Rebase your fork <http://stackoverflow.com/a/7244456/1110993>`__, squash
    commits, and resolve all conflicts.

-   When merging PRs, wherever possible try to use **Squash and Merge** instead of **Rebase and Merge**.

-   Make sure every pull request has an associated
    `JIRA <https://issues.apache.org/jira/browse/AIRFLOW/?selectedTab=com.atlassian.jira.jira-projects-plugin:summary-panel>`__
    ticket. The JIRA link should also be added to the PR description.

-   Preface your commit's subject & PR title with **[AIRFLOW-XXX] COMMIT_MSG** where *XXX*
    is the JIRA number. For example: [AIRFLOW-5574] Fix Google Analytics script loading.
    We compose Airflow release notes from all commit titles in a release. By placing the JIRA number in the
    commit title and hence in the release notes, we let Airflow users look into
    JIRA and GitHub PRs for more details about a particular change.

-   Add an `Apache License <http://www.apache.org/legal/src-headers.html>`__ header
    to all new files.

    If you have `pre-commit hooks <#pre-commit-hooks>`_ enabled, they automatically add
    license headers during commit.

-   If your pull request adds functionality, make sure to update the docs as part
    of the same PR. Doc string is often sufficient. Make sure to follow the
    Sphinx compatible standards.

-   Make sure the pull request works for Python 3.5, 3.6 and 3.7.

-   Run tests locally before opening PR.

    As Airflow grows as a project, we try to enforce a more consistent style and
    follow the Python community guidelines. We currently enforce most of
    `PEP8 <https://www.python.org/dev/peps/pep-0008/>`__ and a few other linting
    rules described in `Running static code checks <BREEZE.rst#running-static-code-checks>`__ section.

-   Adhere to guidelines for commit messages described in this `article <http://chris.beams.io/posts/git-commit/>`__.
    This makes the lives of those who come after you a lot easier.



Development Environments
========================

There are two environments, available on Linux and macOS, that you can use to
develop Apache Airflow:

-   `Local virtualenv development environment <#local-virtualenv-development-environment>`_
    that supports running unit tests and can be used in your IDE.

-   `Breeze Docker-based development environment <#breeze-development-environment>`_ that provides
    an end-to-end CI solution with all software dependencies covered.

The table below summarizes differences between the two environments:


========================= ================================ =====================================
**Property**              **Local virtualenv**             **Breeze environment**
========================= ================================ =====================================
Test coverage             - (-) unit tests only            - (+) integration and unit tests
------------------------- -------------------------------- -------------------------------------
Setup                     - (+) automated with breeze cmd  - (+) automated with breeze cmd
------------------------- -------------------------------- -------------------------------------
Installation difficulty   - (-) depends on the OS setup    - (+) works whenever Docker works
------------------------- -------------------------------- -------------------------------------
Team synchronization      - (-) difficult to achieve       - (+) reproducible within team
------------------------- -------------------------------- -------------------------------------
Reproducing CI failures   - (-) not possible in many cases - (+) fully reproducible
------------------------- -------------------------------- -------------------------------------
Ability to update         - (-) requires manual updates    - (+) automated update via breeze cmd
------------------------- -------------------------------- -------------------------------------
Disk space and CPU usage  - (+) relatively lightweight     - (-) uses GBs of disk and many CPUs
------------------------- -------------------------------- -------------------------------------
IDE integration           - (+) straightforward            - (-) via remote debugging only
========================= ================================ =====================================


Typically, you are recommended to use both of these environments depending on your needs.

Local virtualenv Development Environment
----------------------------------------

All details about using and running local virtualenv environment for Airflow can be found
in `LOCAL_VIRTUALENV.rst <LOCAL_VIRTUALENV.rst>`__.

Benefits:

-   Packages are installed locally. No container environment is required.

-   You can benefit from local debugging within your IDE.

-   With the virtualenv in your IDE, you can benefit from autocompletion and running tests directly from the IDE.

Limitations:

-   You have to maintain your dependencies and local environment consistent with
    other development environments that you have on your local machine.

-   You cannot run tests that require external components, such as mysql,
    postgres database, hadoop, mongo, cassandra, redis, etc.

    The tests in Airflow are a mixture of unit and integration tests and some of
    them require these components to be set up. Local virtualenv supports only
    real unit tests. Technically, to run integration tests, you can configure
    and install the dependencies on your own, but it is usually complex.
    Instead, you are recommended to use
    `Breeze development environment <#breeze-development-environment>`__ with all required packages
    pre-installed.

-   You need to make sure that your local environment is consistent with other
    developer environments. This often leads to a "works for me" syndrome. The
    Breeze container-based solution provides a reproducible environment that is
    consistent with other developers.

Possible extensions:

-   You are **STRONGLY** encouraged to also install and use `pre-commit hooks <#pre-commit-hooks>`_
    for your local virtualenv development environment.
    Pre-commit hooks can speed up your development cycle a lot.

Breeze Development Environment
------------------------------

All details about using and running Airflow Breeze can be found in
`BREEZE.rst <BREEZE.rst>`__.

The Airflow Breeze solution is intended to ease your local development as "*It's
a Breeze to develop Airflow*".

Benefits:

-   Breeze is a complete environment that includes external components, such as
    mysql database, hadoop, mongo, cassandra, redis, etc., required by some of
    Airflow tests. Breeze provides a preconfigured Docker Compose environment
    where all these services are available and can be used by tests
    automatically.

-   Breeze environment is almost the same as used in `Travis CI <https://travis-ci.com/>`__ automated builds.
    So, if the tests run in your Breeze environment, they will work in Travis CI as well.

Limitations:

-   Breeze environment takes significant space in your local Docker cache. There
    are separate environments for different Python and Airflow versions, and
    each of the images takes around 3GB in total.

-   Though Airflow Breeze setup is automated, it takes time. The Breeze
    environment uses pre-built images from DockerHub and it takes time to
    download and extract those images. Building the environment for a particular
    Python version takes less than 10 minutes.

-   Breeze environment runs in the background taking precious resources, such as
    disk space and CPU. You can stop the environment manually after you use it
    or even use a ``bare`` environment to decrease resource usage.

**NOTE:** Breeze CI images are not supposed to be used in production environments.
They are optimized for repeatability of tests, maintainability and speed of building rather
than production performance. The production images are not yet officially published.

Pylint Checks
=============

We are in the process of fixing code flagged with pylint checks for the whole Airflow project.
This is a huge task so we implemented an incremental approach for the process.
Currently most of the code is excluded from pylint checks via scripts/ci/pylint_todo.txt.
We have an open JIRA issue AIRFLOW-4364 which has a number of sub-tasks for each of
the modules that should be made compatible. Fixing problems identified with pylint is one of
straightforward and easy tasks to do (but time-consuming), so if you are a first-time
contributor to Airflow, you can choose one of the sub-tasks as your first issue to fix.

To fix a pylint issue, do the following:

1.  Remove module/modules from the
    `scripts/ci/pylint_todo.txt <scripts/ci/pylint_todo.txt>`__.

2.  Run `scripts/ci/ci_pylint_main.sh <scripts/ci/ci_pylint_main.sh>`__ and
`scripts/ci/ci_pylint_tests.sh <scripts/ci/ci_pylint_tests.sh>`__.

3.  Fix all the issues reported by pylint.

4.  Re-run `scripts/ci/ci_pylint_main.sh <scripts/ci/ci_pylint_main.sh>`__ and
`scripts/ci/ci_pylint_tests.sh <scripts/ci/ci_pylint_tests.sh>`__.

5.  If you see "success", submit a PR following
    `Pull Request guidelines <#pull-request-guidelines>`__.
 

These are guidelines for fixing errors reported by pylint:

-   Fix the errors rather than disable pylint checks. Often you can easily
    refactor the code (IntelliJ/PyCharm might be helpful when extracting methods
    in complex code or moving methods around).

-   If disabling a particular problem, make sure to disable only that error by
    using the symbolic name of the error as reported by pylint.

.. code-block:: python

    import airflow.*  # pylint: disable=wildcard-import


-   If there is a single line where you need to disable a particular error,
    consider adding a comment to the line that causes the problem. For example:

.. code-block:: python

    def  MakeSummary(pcoll, metric_fn, metric_keys): # pylint: disable=invalid-name


-   For multiple lines/block of code, to disable an error, you can surround the
    block with ``pylint:disable/pylint:enable`` comment lines. For example:

.. code-block:: python

    # pylint: disable=too-few-public-methods
    class  LoginForm(Form):
        """Form for the user"""
        username = StringField('Username', [InputRequired()])
        password = PasswordField('Password', [InputRequired()])
    # pylint: enable=too-few-public-methods


Pre-commit Hooks
================

Pre-commit hooks help speed up your local development cycle, either in the local virtualenv or Breeze,
and place less burden on the CI infrastructure. Consider installing the pre-commit
hooks as a necessary prerequisite.

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
----------------------------------

The pre-commit hooks use several external linters that need to be installed before pre-commit is run.

Each of the checks installs its own environment, so you do not need to install those, but there are some
checks that require locally installed binaries. On Linux, you typically install
them with ``sudo apt install``, on macOS - with ``brew install``.

The current list of prerequisites:

-   ``xmllint``:
    on Linux, install via ``sudo apt install xmllint``;
    on macOS, install via ``brew install xmllint``

Enabling Pre-commit Hooks
-------------------------

To turn on pre-commit checks for ``commit`` operations in git, enter:

.. code-block:: bash

    pre-commit install


To install the checks also for ``pre-push`` operations, enter:

.. code-block:: bash

    pre-commit install -t pre-push


For details on advanced usage of the install method, use:

.. code-block:: bash

   pre-commit install --help


Using Docker Images for Pre-commit Hooks
----------------------------------------

Before running the pre-commit hooks, you must first build the Docker images as
described in `BREEZE.rst <BREEZE.rst>`__.

Sometimes your image is outdated and needs to be rebuilt because some
dependencies have been changed. In such case the Docker-based pre-commit will
inform you that you should rebuild the image.

Supported Pre-commit Hooks
--------------------------

In Airflow, we have the following checks:

=================================== =======================================================
**Hooks**                             **Description**
=================================== =======================================================
``check-apache-license``              Checks compatibility with Apache License requirements.
----------------------------------- -------------------------------------------------------
``check-executables-have-shebangs``   Checks that executables have shebang.
----------------------------------- -------------------------------------------------------
``check-hooks-apply``                 Checks which hooks are applicable to the repository.
----------------------------------- -------------------------------------------------------
``check-merge-conflict``              Checks if a merge conflict is committed.
----------------------------------- -------------------------------------------------------
``check-xml``                         Checks XML files with xmllint.
----------------------------------- -------------------------------------------------------
``detect-private-key``                Detects if private key is added to the repository.
----------------------------------- -------------------------------------------------------
``doctoc``                            Refreshes the table of contents for md files.
----------------------------------- -------------------------------------------------------
``end-of-file-fixer``                 Makes sure that there is an empty line at the end.
----------------------------------- -------------------------------------------------------
``flake8``                            Runs flake8.
----------------------------------- -------------------------------------------------------
``forbid-tabs``                       Fails if tabs are used in the project.
----------------------------------- -------------------------------------------------------
``insert-license``                    Adds licenses for most file types.
----------------------------------- -------------------------------------------------------
``isort``                             Sorts imports in python files.
----------------------------------- -------------------------------------------------------
``lint-dockerfile``                   Lints a dockerfile.
----------------------------------- -------------------------------------------------------
``mixed-line-ending``                 Detects if mixed line ending is used (\r vs. \r\n).
----------------------------------- -------------------------------------------------------
``mypy``                              Runs mypy.
----------------------------------- -------------------------------------------------------
``pylint``                            Runs pylint.
----------------------------------- -------------------------------------------------------
``shellcheck``                        Checks shell files with shellcheck.
----------------------------------- -------------------------------------------------------
``yamllint``                          Checks yaml files with yamllint.
=================================== =======================================================


Using Pre-commit Hooks
----------------------

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

    SKIP=pylint,mypy pre-commit run --all-files


You can always skip running the tests by providing ``--no-verify`` flag to the
``git commit`` command.

To check other usage types of the pre-commit framework, see `Pre-commit website <https://pre-commit.com/>`__.

Travis CI Testing Framework
===========================

Airflow test suite is based on Travis CI framework as running all of the tests
locally requires significant setup. You can set up Travis CI in your fork of
Airflow by following the `Travis
CI Getting Started
guide <https://docs.travis-ci.com/user/getting-started/>`__.


There are two different options available for running Travis CI, and they are
set up on GitHub as separate components:

-   **Travis CI GitHub App** (new version)
-   **Travis CI GitHub Services** (legacy version)

Using Travis CI GitHub App (new version)
----------------------------------------

-   Once `installed <https://github.com/apps/travis-ci/installations/new/permissions?target_id=47426163>`__,
    configure the Travis CI GitHub App at
    `Configure Travis CI <https://github.com/settings/installations>`__.

-   Set repository access to either "All repositories" for convenience, or "Only
    select repositories" and choose ``USERNAME/airflow`` in the drop-down menu.

-   Access Travis CI for your fork at `<https://travis-ci.com/USERNAME/airflow>`__.

Using Travis CI GitHub Services (legacy version)
------------------------------------------------

**NOTE:** The apache/airflow project is still using the legacy version.

Travis CI GitHub Services version uses an Authorized OAuth App.

1.  Once installed, configure the Travis CI Authorized OAuth App at
    `Travis CI OAuth APP <https://github.com/settings/connections/applications/88c5b97de2dbfc50f3ac>`__.

2.  If you are a GitHub admin, click the **Grant** button next to your
    organization; otherwise, click the **Request** button. For the Travis CI
    Authorized OAuth App, you may have to grant access to the forked
    ``ORGANIZATION/airflow`` repo even though it is public.

3.  Access Travis CI for your fork at
    `<https://travis-ci.org/ORGANIZATION/airflow>`_.

Creating New Projects in Travis CI
----------------------------------

If you need to create a new project in Travis CI, use travis-ci.com for both
private repos and open source.

The travis-ci.org site for open source projects is now legacy and you should not use it.

..
    There is a second Authorized OAuth App available called "Travis CI for Open Source" used
    for the legacy travis-ci.org service Don't use it for new projects.

More information:

-  `Open Source on travis-ci.com <https://docs.travis-ci.com/user/open-source-on-travis-ci-com/>`__.
-  `Legacy GitHub Services to GitHub Apps Migration Guide <https://docs.travis-ci.com/user/legacy-services-to-github-apps-migration-guide/>`__.
-  `Migrating Multiple Repositories to GitHub Apps Guide <https://docs.travis-ci.com/user/travis-migrate-to-apps-gem-guide/>`__.

Metadata Database Updates
==============================

When developing features, you may need to persist information to the metadata
database. Airflow has `Alembic <https://github.com/sqlalchemy/alembic>`__ built-in
module to handle all schema changes. Alembic must be installed on your
development machine before continuing with migration.


.. code-block:: bash

    # starting at the root of the project
    $ pwd
    ~/airflow
    # change to the airflow directory
    $ cd airflow
    $ alembic revision -m "add new field to db"
       Generating
    ~/airflow/airflow/migrations/versions/12341123_add_new_field_to_db.py


node / npm Javascript Environment Setup
================================================

``airflow/www/`` contains all npm-managed, front-end assets. Flask-Appbuilder
itself comes bundled with jQuery and bootstrap. While they may be phased out
over time, these packages are currently not managed with npm.

Make sure you are using recent versions of node and npm. No problems have been
found with node\>=8.11.3 and npm\>=6.1.3.

Installing npm and its packages
-------------------------------

Make sure npm is available in your environment.

To install it on macOS:

1.  Run the following commands (taken from `this source <https://gist.github.com/DanHerbert/9520689>`__):

.. code-block:: bash

    brew install node --without-npm
    echo prefix=~/.npm-packages >> ~/.npmrc
    curl -L https://www.npmjs.com/install.sh | sh


2.  Add ``~/.npm-packages/bin`` to your ``PATH`` so that commands you install
    globally are usable.

3.  Set up your ``.bashrc`` file and then ``source ~/.bashrc`` to reflect the
    change.

    For example:

.. code-block:: bash

    export PATH="$HOME/.npm-packages/bin:$PATH"


    You can also follow  _`general npm installation
    instructions <https://docs.npmjs.com/downloading-and-installing-node-js-and-npm>`__.

4.  Install third party libraries defined in ``package.json`` by running the
    following commands within the ``airflow/www/`` directory:


.. code-block:: bash

    # from the root of the repository, move to where our JS package.json lives
    cd airflow/www/
    # run npm install to fetch all the dependencies
    npm install


These commands install the libraries in a new ``node_modules/`` folder within
``www/``.

Should you add or upgrade an npm package, which involves changing
``package.json``, you'll need to re-run ``npm install`` and push the newly generated
``package-lock.json`` file so that we get a reproducible build.


Generating Bundled Files with npm
---------------------------------

To parse and generate bundled files for Airflow, run either of the following
commands:

.. code-block:: bash

    # Compiles the production / optimized js & css
    npm run prod

    # Starts a web server that manages and updates your assets as you modify them
    npm run dev


Javascript Style Guide
~~~~~~~~~~~~~~~~~~~~~~

We try to enforce a more consistent style and follow the JS community
guidelines.

Once you add or modify any javascript code in the project, please make sure it
follows the guidelines defined in `Airbnb
JavaScript Style Guide <https://github.com/airbnb/javascript>`__.

Apache Airflow uses `ESLint <https://eslint.org/>`__ as a tool for identifying and
reporting on patterns in JavaScript. To use it, run any of the following
commands:

.. code-block:: bash

    # Check JS code in .js and .html files, and report any errors/warnings
    npm run lint

    # Check JS code in .js and .html files, report any errors/warnings and fix them if possible
    npm run lint:fix

Resources & links
=================
- `Airflow’s official documentation <http://airflow.apache.org/>`__

- Mailing lists:

  - Developer’s mailing list `<dev-subscribe@airflow.apache.org>`_

  - All commits mailing list: `<commits-subscribe@airflow.apache.org>`_

  - Airflow users mailing list: `<users-subscribe@airflow.apache.org>`_

- `Issues on Apache’s Jira <https://issues.apache.org/jira/browse/AIRFLOW>`__

- `Slack (chat) <https://apache-airflow-slack.herokuapp.com/>`__

- `More resources and links to Airflow related content on the Wiki <https://cwiki.apache.org/confluence/display/AIRFLOW/Airflow+Links>`__
