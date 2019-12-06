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
JIRA <https://issues.apache.org/jira/browse/AIRFLOW>`__.

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
    `Travis CI Testing Framework <TESTING.rst#travis-ci-testing-framework>`__ usage guidelines).
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

    If you have `pre-commit hooks <STATIC_CODE_CHECKS.rst#pre-commit-hooks>`__ enabled, they automatically add
    license headers during commit.

-   If your pull request adds functionality, make sure to update the docs as part
    of the same PR. Doc string is often sufficient. Make sure to follow the
    Sphinx compatible standards.

-   Make sure your code fulfils all the
    `static code checks <STATIC_CODE_CHECKS.rst#pre-commit-hooks>`__ we have in our code. The easiest way
    to make sure of that is to use `pre-commit hooks <STATIC_CODE_CHECKS.rst#pre-commit-hooks>`__

-   Run tests locally before opening PR.

-   Make sure the pull request works for Python 3.6 and 3.7.

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

Static code checks
==================

We check our code quality via static code checks. See
`STATIC_CODE_CHECKS.rst <STATIC_CODE_CHECKS.rst>`_ for details.

Your code must pass all the static code checks in Travis CI in order to be eligible for Code Review.
The easiest way to make sure your code is good before pushing is to use pre-commit checks locally
as described in the static code checks documentation.

Test Infrastructure
===================

We support the following types of tests:

* **Unit tests** are Python ``nose`` tests launched with ``run-tests``.
  Unit tests are available both in the `Breeze environment <BREEZE.rst>`_
  and `local virtualenv <LOCAL_VIRTUALENV.rst>`_.

* **Integration tests** are available in the Breeze development environment
  that is also used for Airflow Travis CI tests. Integration test are special tests that require
  additional services running, such as Postgres,Mysql, Kerberos, etc. These tests are not yet
  clearly marked as integration tests but soon they will be clearly separated by the ``pytest`` annotations.

* **System tests** are automatic tests that use external systems like
  Google Cloud Platform. These tests are intended for an end-to-end DAG execution.

For details on running different types of Airflow tests, see `TESTING.rst <TESTING.rst>`_.

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
    You can also follow  `general npm installation
    instructions <https://docs.npmjs.com/downloading-and-installing-node-js-and-npm>`__.
    For example:

.. code-block:: bash

    export PATH="$HOME/.npm-packages/bin:$PATH"

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

Contribution Workflow Example
==============================

Typically, you start your first contribution by reviewing open tickets
at `Apache JIRA <https://issues.apache.org/jira/browse/AIRFLOW>`__.

For example, you want to have the following sample ticket assigned to you:
`AIRFLOW-5934: Add extra CC: to the emails sent by Aiflow <https://issues.apache.org/jira/browse/AIRFLOW-5934>`_.

In general, your contribution includes the following stages:

.. image:: images/workflow.png
    :align: center
    :alt: Contribution Workflow

1. Make your own `fork <https://help.github.com/en/github/getting-started-with-github/fork-a-repo>`__ of
   the Apache Airflow `main repository <https://github.com/apache/airflow>`__.

2. Create a `local virtualenv <LOCAL_VIRTUALENV.rst>`_,
   initialize the `Breeze environment <BREEZE.rst>`__, and
   install `pre-commit framework <STATIC_CODE_CHECKS.rst#pre-commit-hooks>`__.
   If you want to add more changes in the future, set up your own `Travis CI
   fork <https://github.com/PolideaInternal/airflow/blob/more-gsod-improvements/TESTING.rst#travis-ci-testing-framework>`__.

3. Join `devlist <https://lists.apache.org/list.html?dev@airflow.apache.org>`__
   and set up a `Slack account <https://apache-airflow-slack.herokuapp.com>`__.

4. Make the change and create a `Pull Request from your fork <https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request-from-a-fork>`__.

5. Ping @ #development slack, comment @people. Be annoying. Be considerate.

Step 1: Fork the Apache Repo
----------------------------
From the `apache/airflow <https://github.com/apache/airflow>`_ repo,
`create a fork <https://help.github.com/en/github/getting-started-with-github/fork-a-repo>`_:

.. image:: images/fork.png
    :align: center
    :alt: Creating a fork


Step 2: Configure Your Environment
----------------------------------
Configure the Docker-based Breeze development environment and run tests.

You can use the default Breeze configuration as follows:

1. Install the latest versions of the Docker Community Edition
   and Docker Compose and add them to the PATH.

2. Enter Breeze: ``./breeze``

   Breeze starts with downloading the Airflow CI image from
   the Docker Hub and installing all required dependencies.

3. Enter the Docker environment and mount your local sources
   to make them immediately visible in the environment.

4. Create a local virtualenv, for example:

.. code-block:: bash

   mkvirtualenv myenv --python=python3.6

5. Initialize the created environment:

.. code-block:: bash

   ./breeze --initialize-local-virtualenv

6. Open your IDE (for example, PyCharm) and select the virtualenv you created
   as the project's default virtualenv in your IDE.

Step 3: Connect with People
---------------------------

For effective collaboration, make sure to join the following Airflow groups:

- Mailing lists:

  - Developer’s mailing list `<dev-subscribe@airflow.apache.org>`_
    (quite substantial traffic on this list)

  - All commits mailing list: `<commits-subscribe@airflow.apache.org>`_
    (very high traffic on this list)

  - Airflow users mailing list: `<users-subscribe@airflow.apache.org>`_
    (reasonably small traffic on this list)

- `Issues on Apache’s JIRA <https://issues.apache.org/jira/browse/AIRFLOW>`__

- `Slack (chat) <https://apache-airflow-slack.herokuapp.com/>`__

Step 4: Prepare PR
------------------

1. Update the local sources to address the JIRA ticket.

   For example, to address this example JIRA ticket, do the following:

   * Read about `email configuration in Airflow <https://airflow.readthedocs.io/en/latest/concepts.html#email-configuration>`__.

   * Find the class you should modify. For the example ticket, this is `email.py <https://github.com/apache/airflow/blob/master/airflow/utils/email.py>`__.

   * Find the test class where you should add tests. For the example ticket, this is `test_email.py <https://github.com/apache/airflow/blob/master/tests/utils/test_email.py>`__.

   * Modify the class and add necessary code and unit tests.

   * Run the unit tests from the `IDE <TESTING.rst#running-unit-tests-from-ide>`__ or `local virtualenv <TESTING.rst#running-unit-tests-from-local-virtualenv>`__ as you see fit.

   * Run the tests in `Breeze <TESTING.rst#running-unit-tests-inside-breeze>`__.

   * Run and fix all the `static checks <STATIC_CODE_CHECKS>`__. If you have
     `pre-commits installed <STATIC_CODE_CHECKS.rst#pre-commit-hooks>`__,
     this step is automatically run while you are committing your code. If not, you can do it manually
     via ``git add`` and then ``pre-commit run``.

2. Rebase your fork, squash commits, and resolve all conflicts.

3. Re-run static code checks again.

4. Create a pull request with the following title for the sample ticket:
   ``[AIRFLOW-5934] Added extra CC: field to the Airflow emails.``

Make sure to follow other PR guidelines described in `this document <#pull-request-guidelines>`_.


Step 5: Pass PR Review
----------------------

.. image:: images/review.png
    :align: center
    :alt: PR Review

Note that committers will use **Squash and Merge** instead of **Rebase and Merge**
when merging PRs and your commit will be squashed to single commit.

Resources & Links
=================
- `Airflow’s official documentation <http://airflow.apache.org/>`__

- `More resources and links to Airflow related content on the Wiki <https://cwiki.apache.org/confluence/display/AIRFLOW/Airflow+Links>`__
