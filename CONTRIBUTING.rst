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

Get Mentoring Support
---------------------

If you are new to the project, you might need some help in understanding how the dynamics
of the community works and you might need to get some mentorship from other members of the
community - mostly committers. Mentoring new members of the community is part of committers
job so do not be afraid of asking committers to help you. You can do it
via comments in your Pull Request, asking on a devlist or via Slack. For your convenience,
we have a dedicated #newbie-questions Slack channel where you can ask any questions
you want - it's a safe space where it is expected that people asking questions do not know
a lot about Airflow (yet!).

If you look for more structured mentoring experience, you can apply to Apache Software Foundation's
`Official Mentoring Programme <http://community.apache.org/mentoringprogramme.html>`_. Feel free
to follow it and apply to the programme and follow up with the community.

Report Bugs
-----------

Report bugs through `GitHub <https://github.com/apache/airflow/issues>`__.

Please report relevant information and preferably code that exhibits the
problem.

Fix Bugs
--------

Look through the GitHub issues for bugs. Anything is open to whoever wants to
implement it.

Implement Features
------------------

Look through the `GitHub issues labeled "kind:feature"
<https://github.com/apache/airflow/labels/kind%3Afeature>`__ for features.

Any unassigned feature request issue is open to whoever wants to implement it.

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

The best way to send feedback is to `open an issue on GitHub <https://github.com/apache/airflow/issues/new/choose>`__.

If you are proposing a new feature:

-   Explain in detail how it would work.
-   Keep the scope as narrow as possible to make it easier to implement.
-   Remember that this is a volunteer-driven project, and that contributions are
    welcome :)


Roles
=============

There are several roles within the Airflow Open-Source community.


PMC Member
-----------
The PMC (Project Management Committee) is a group of maintainers that drives changes in the way that
Airflow is managed as a project.

Considering Apache, the role of the PMC is primarily to ensure that Airflow conforms to Apache's processes
and guidelines.

Committers/Maintainers
----------------------

Committers are community members that have write access to the project’s repositories, i.e., they can modify the code,
documentation, and website by themselves and also accept other contributions.

The official list of committers can be found `here <https://airflow.apache.org/docs/stable/project.html#committers>`__.

Additionally, committers are listed in a few other places (some of these may only be visible to existing committers):

* https://whimsy.apache.org/roster/ppmc/airflow
* https://github.com/orgs/apache/teams/airflow-committers/members

Committers are responsible for:

* Championing one or more items on the `Roadmap <https://cwiki.apache.org/confluence/display/AIRFLOW/Airflow+Home>`__
* Reviewing & Merging Pull-Requests
* Scanning and responding to GitHub issues
* Responding to questions on the dev mailing list (dev@airflow.apache.org)

Becoming a Committer
--------------------

There is no strict protocol for becoming a committer.
Candidates for new committers are typically people that are active contributors and community members.

The key aspects of a committer are:

* Consistent contributions over the past 6 months
* Understanding of Airflow Core or has displayed a holistic understanding of a particular part and made
  contributions towards a more strategic goal
* Understanding of contributor/committer guidelines: `Contributors' Guide <https://github.com/apache/airflow/blob/master/CONTRIBUTING.rst>`__
* Quality of the commits
* Visibility in community discussions (dev mailing list, Slack and GitHub)
* Testing Release Candidates


Contributors
------------

A contributor is anyone who wants to contribute code, documentation, tests, ideas, or anything to the
Apache Airflow project.

Contributors are responsible for:

* Fixing bugs
* Adding features
* Championing one or more items on the `Roadmap <https://cwiki.apache.org/confluence/display/AIRFLOW/Airflow+Home>`__.

Contribution Workflow
=====================

Typically, you start your first contribution by reviewing open tickets
at `GitHub issues <https://github.com/apache/airflow/issues>`__.

If you create pull-request, you don't have to create an issue first, but if you want, you can do it.
Creating an issue will allow you to collect feedback or share plans with other people.

For example, you want to have the following sample ticket assigned to you:
`#7782: Add extra CC: to the emails sent by Airflow <https://github.com/apache/airflow/issues/7782>`_.

In general, your contribution includes the following stages:

.. image:: images/workflow.png
    :align: center
    :alt: Contribution Workflow

1. Make your own `fork <https://help.github.com/en/github/getting-started-with-github/fork-a-repo>`__ of
   the Apache Airflow `main repository <https://github.com/apache/airflow>`__.

2. Create a `local virtualenv <LOCAL_VIRTUALENV.rst>`_,
   initialize the `Breeze environment <BREEZE.rst>`__, and
   install `pre-commit framework <STATIC_CODE_CHECKS.rst#pre-commit-hooks>`__.
   If you want to add more changes in the future, set up your fork and enable GitHub Actions.

3. Join `devlist <https://lists.apache.org/list.html?dev@airflow.apache.org>`__
   and set up a `Slack account <https://s.apache.org/airflow-slack>`__.

4. Make the change and create a `Pull Request from your fork <https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request-from-a-fork>`__.

5. Ping @ #development slack, comment @people. Be annoying. Be considerate.

Step 1: Fork the Apache Airflow Repo
------------------------------------
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

   ./breeze initialize-local-virtualenv --python 3.6

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

- `Issues on GitHub <https://github.com/apache/airflow/issues>`__

- `Slack (chat) <https://s.apache.org/airflow-slack>`__

Step 4: Prepare PR
------------------

1. Update the local sources to address the issue.

   For example, to address this example issue, do the following:

   * Read about `email configuration in Airflow <https://airflow.readthedocs.io/en/latest/howto/email-config.html>`__.

   * Find the class you should modify. For the example GitHub issue,
     this is `email.py <https://github.com/apache/airflow/blob/master/airflow/utils/email.py>`__.

   * Find the test class where you should add tests. For the example ticket,
     this is `test_email.py <https://github.com/apache/airflow/blob/master/tests/utils/test_email.py>`__.

   * Make sure your fork's master is synced with Apache Airflow's master before you create a branch. See
     `How to sync your fork <#how-to-sync-your-fork>`_ for details.

   * Create a local branch for your development. Make sure to use latest
     ``apache/master`` as base for the branch. See `How to Rebase PR <#how-to-rebase-pr>`_ for some details
     on setting up the ``apache`` remote. Note, some people develop their changes directly in their own
     ``master`` branches - this is OK and you can make PR from your master to ``apache/master`` but we
     recommend to always create a local branch for your development. This allows you to easily compare
     changes, have several changes that you work on at the same time and many more.
     If you have ``apache`` set as remote then you can make sure that you have latest changes in your master
     by ``git pull apache master`` when you are in the local ``master`` branch. If you have conflicts and
     want to override your locally changed master you can override your local changes with
     ``git fetch apache; git reset --hard apache/master``.

   * Modify the class and add necessary code and unit tests.

   * Run the unit tests from the `IDE <TESTING.rst#running-unit-tests-from-ide>`__
     or `local virtualenv <TESTING.rst#running-unit-tests-from-local-virtualenv>`__ as you see fit.

   * Run the tests in `Breeze <TESTING.rst#running-unit-tests-inside-breeze>`__.

   * Run and fix all the `static checks <STATIC_CODE_CHECKS.rst>`__. If you have
     `pre-commits installed <STATIC_CODE_CHECKS.rst#pre-commit-hooks>`__,
     this step is automatically run while you are committing your code. If not, you can do it manually
     via ``git add`` and then ``pre-commit run``.

2. Rebase your fork, squash commits, and resolve all conflicts. See `How to rebase PR <#how-to-rebase-pr>`_
   if you need help with rebasing your change. Remember to rebase often if your PR takes a lot of time to
   review/fix. This will make rebase process much easier and less painful and the more often you do it,
   the more comfortable you will feel doing it.

3. Re-run static code checks again.

4. Make sure your commit has a good title and description of the context of your change, enough
   for the committer reviewing it to understand why you are proposing a change. Make sure to follow other
   PR guidelines described in `pull request guidelines <#pull-request-guidelines>`_.
   Create Pull Request! Make yourself ready for the discussion!

5. Depending on "scope" of your changes, your Pull Request might go through one of few paths after approval.
   We run some non-standard workflow with high degree of automation that allows us to optimize the usage
   of queue slots in Github Actions. Our automated workflows determine the "scope" of changes in your PR
   and send it through the right path:

   * In case of a "no-code" change, approval will generate a comment that the PR can be merged and no
     tests are needed. This is usually when the change modifies some non-documentation related rst
     files (such as this file). No python tests are run and no CI images are built for such PR. Usually
     it can be approved and merged few minutes after it is submitted (unless there is a big queue of jobs).

   * In case of change involving python code changes or documentation changes, a subset of full test matrix
     will be executed. This subset of tests perform relevant tests for single combination of python, backend
     version and only builds one CI image and one PROD image. Here the scope of tests depends on the
     scope of your changes:

     * when your change does not change "core" of Airflow (Providers, CLI, WWW, Helm Chart) you will get the
       comment that PR is likely ok to be merged without running "full matrix" of tests. However decision
       for that is left to committer who approves your change. The committer might set a "full tests needed"
       label for your PR and ask you to rebase your request or re-run all jobs. PRs with "full tests needed"
       run full matrix of tests.

     * when your change changes the "core" of Airflow you will get the comment that PR needs full tests and
       the "full tests needed" label is set for your PR. Additional check is set that prevents from
       accidental merging of the request until full matrix of tests succeeds for the PR.

     * when your change has "upgrade to newer dependencies" label set, constraints will be automatically
       upgraded to latest constraints matching your setup.py. This is useful in case you want to force
       upgrade to a latest version of dependencies. You can ask committers to set the label for you
       when you need it in your PR.

   More details about the PR workflow be found in `PULL_REQUEST_WORKFLOW.rst <PULL_REQUEST_WORKFLOW.rst>`_.


Step 5: Pass PR Review
----------------------

.. image:: images/review.png
    :align: center
    :alt: PR Review

Note that committers will use **Squash and Merge** instead of **Rebase and Merge**
when merging PRs and your commit will be squashed to single commit.

You need to have review of at least one committer (if you are committer yourself, it has to be
another committer). Ideally you should have 2 or more committers reviewing the code that touches
the core of Airflow.


Pull Request Guidelines
=======================

Before you submit a pull request (PR) from your forked repo, check that it meets
these guidelines:

-   Include tests, either as doctests, unit tests, or both, to your pull
    request.

    The airflow repo uses `GitHub Actions <https://help.github.com/en/actions>`__ to
    run the tests and `codecov <https://codecov.io/gh/apache/airflow>`__ to track
    coverage. You can set up both for free on your fork. It will help you make sure you do not
    break the build with your PR and that you help increase coverage.

-   Follow our project's `Coding style and best practices`_.

    These are things that aren't currently enforced programmatically (either because they are too hard or just
    not yet done.)

-   `Rebase your fork <http://stackoverflow.com/a/7244456/1110993>`__, squash
    commits, and resolve all conflicts.

-   When merging PRs, wherever possible try to use **Squash and Merge** instead of **Rebase and Merge**.

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

-   Make sure the pull request works for Python 2.7, 3.5, 3.6, 3.7 and 3.8.

-   Adhere to guidelines for commit messages described in this `article <http://chris.beams.io/posts/git-commit/>`__.
    This makes the lives of those who come after you a lot easier.

Airflow Git Branches
====================

All new development in Airflow happens in the ``master`` branch. All PRs should target that branch.
We also have a ``v1-10-test`` branch that is used to test ``1.10.x`` series of Airflow and where committers
cherry-pick selected commits from the master branch.
Cherry-picking is done with the ``-x`` flag.

The ``v1-10-test`` branch might be broken at times during testing. Expect force-pushes there so
committers should coordinate between themselves on who is working on the ``v1-10-test`` branch -
usually these are developers with the release manager permissions.

Once the branch is stable, the ``v1-10-stable`` branch is synchronized with ``v1-10-test``.
The ``v1-10-stable`` branch is used to release ``1.10.x`` releases.

The general approach is that cherry-picking a commit that has already had a PR and unit tests run
against main is done to ``v1-10-test`` branch, but PRs from contributors towards 1.10 should target
``v1-10-stable`` branch.

The ``v1-10-test`` branch and ``v1-10-stable`` ones are merged just before the release and that's the
time when they converge.

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

-   You are **STRONGLY** encouraged to also install and use `pre-commit hooks <STATIC_CODE_CHECKS.rst#pre-commit-hooks>`_
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

-   Breeze environment is almost the same as used in the CI automated builds.
    So, if the tests run in your Breeze environment, they will work in the CI as well.
    See `<CI.rst>`_ for details about Airflow CI.

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


Airflow dependencies
====================

Extras
------

There are a number of extras that can be specified when installing Airflow. Those
extras can be specified after the usual pip install - for example
``pip install -e .[ssh]``. For development purpose there is a ``devel`` extra that
installs all development dependencies. There is also ``devel_ci`` that installs
all dependencies needed in the CI environment.

This is the full list of those extras:

  .. START EXTRAS HERE

all, all_dbs, amazon, apache.atlas, apache.cassandra, apache.druid, apache.hdfs, apache.hive,
apache.pinot, apache.presto, apache.webhdfs, async, atlas, aws, azure, azure_blob_storage,
azure_container_instances, azure_cosmos, azure_data_lake, azure_secrets, cassandra, celery, cgroups,
cloudant, cncf.kubernetes, crypto, dask, databricks, datadog, devel, devel_all, devel_azure,
devel_ci, devel_hadoop, doc, docker, druid, elasticsearch, emr, gcp, gcp_api, github_enterprise,
google, google_auth, grpc, hashicorp, hdfs, hive, jdbc, jira, kerberos, kubernetes, ldap,
microsoft.azure, microsoft.mssql, microsoft.winrm, mongo, mssql, mysql, oracle, papermill, password,
pinot, postgres, presto, qds, rabbitmq, redis, s3, salesforce, samba, segment, sendgrid, sentry,
slack, snowflake, ssh, statsd, vertica, virtualenv, webhdfs, winrm

  .. END EXTRAS HERE

Provider packages
-----------------

Airflow 2.0 is split into core and providers. They are delivered as separate packages:

* ``apache-airflow`` - core of Apache Airflow
* ``apache-airflow-providers-*`` - More than 50 provider packages to communicate with external services

In Airflow 1.10 all those providers were installed together within one single package and when you installed
airflow locally, from sources, they were also installed. In Airflow 2.0, providers are separated out,
and not installed together with the core, unless you set ``INSTALL_PROVIDERS_FROM_SOURCES`` environment
variable to ``true``.

In Breeze - which is a development environment, ``INSTALL_PROVIDERS_FROM_SOURCES`` variable is set to true,
but you can add ``--skip-installing-airflow-providers`` flag to Breeze to skip installing providers when
building the images.

One watch-out - providers are still always installed (or rather available) if you install airflow from
sources using ``-e`` (or ``--editable``) flag. In such case airflow is read directly from the sources
without copying airflow packages to the usual installation location, and since 'providers' folder is
in this airflow folder - the providers package is importable.

Some of the packages have cross-dependencies with other providers packages. This typically happens for
transfer operators where operators use hooks from the other providers in case they are transferring
data between the providers. The list of dependencies is maintained (automatically with pre-commits)
in the ``airflow/providers/dependencies.json``. Pre-commits are also used to generate dependencies.
The dependency list is automatically used during pypi packages generation.

Cross-dependencies between provider packages are converted into extras - if you need functionality from
the other provider package you can install it adding [extra] after the
``apache-airflow-backport-providers-PROVIDER`` for example:
``pip install apache-airflow-backport-providers-google[amazon]`` in case you want to use GCP
transfer operators from Amazon ECS.

If you add a new dependency between different providers packages, it will be detected automatically during
pre-commit phase and pre-commit will fail - and add entry in dependencies.json so that the package extra
dependencies are properly added when package is installed.

You can regenerate the whole list of provider dependencies by running this command (you need to have
``pre-commits`` installed).

.. code-block:: bash

  pre-commit run build-providers-dependencies


Here is the list of packages and their extras:


  .. START PACKAGE DEPENDENCIES HERE

========================== ===========================
Package                    Extras
========================== ===========================
amazon                     apache.hive,google,imap,mongo,mysql,postgres,ssh
apache.druid               apache.hive
apache.hive                amazon,microsoft.mssql,mysql,presto,samba,vertica
apache.livy                http
dingding                   http
discord                    http
google                     amazon,apache.cassandra,cncf.kubernetes,facebook,microsoft.azure,microsoft.mssql,mysql,postgres,presto,salesforce,sftp
hashicorp                  google
microsoft.azure            google,oracle
microsoft.mssql            odbc
mysql                      amazon,presto,vertica
opsgenie                   http
postgres                   amazon
sftp                       ssh
slack                      http
snowflake                  slack
========================== ===========================

  .. END PACKAGE DEPENDENCIES HERE

Backport providers
------------------

You can also build backport provider packages for Airflow 1.10. They aim to provide a bridge when users
of Airflow 1.10 want to migrate to Airflow 2.0. The backport packages are named similarly to the
provider packages, but with "backport" added:

* ``apache-airflow-backport-provider-*``

Those backport providers are automatically refactored to work with Airflow 1.10.* and have a few
limitations described in those packages.

Dependency management
=====================

Airflow is not a standard python project. Most of the python projects fall into one of two types -
application or library. As described in
[StackOverflow Question](https://stackoverflow.com/questions/28509481/should-i-pin-my-python-dependencies-versions)
decision whether to pin (freeze) dependency versions for a python project depends on the type. For
applications, dependencies should be pinned, but for libraries, they should be open.

For application, pinning the dependencies makes it more stable to install in the future - because new
(even transitive) dependencies might cause installation to fail. For libraries - the dependencies should
be open to allow several different libraries with the same requirements to be installed at the same time.

The problem is that Apache Airflow is a bit of both - application to install and library to be used when
you are developing your own operators and DAGs.

This - seemingly unsolvable - puzzle is solved by having pinned constraints files. Those are available
as of airflow 1.10.10 and further improved with 1.10.12 (moved to separate orphan branches)

Pinned constraint files
=======================

By default when you install ``apache-airflow`` package - the dependencies are as open as possible while
still allowing the apache-airflow package to install. This means that ``apache-airflow`` package might fail to
install in case a direct or transitive dependency is released that breaks the installation. In such case
when installing ``apache-airflow``, you might need to provide additional constraints (for
example ``pip install apache-airflow==1.10.2 Werkzeug<1.0.0``)

However we now have ``constraints-<PYTHON_MAJOR_MINOR_VERSION>.txt`` files generated
automatically and committed to orphan ``constraints-master`` and ``constraint-1-10`` branches based on
the set of all latest working and tested dependency versions. Those
``constraints-<PYTHON_MAJOR_MINOR_VERSION>.txt`` files can be used as
constraints file when installing Apache Airflow - either from the sources:

.. code-block:: bash

  pip install -e . \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-1-10/constraints-3.6.txt"


or from the pypi package:

.. code-block:: bash

  pip install apache-airflow \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-1-10/constraints-3.6.txt"


This works also with extras - for example:

.. code-block:: bash

  pip install .[ssh] \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-master/constraints-3.6.txt"


As of apache-airflow 1.10.12 it is also possible to use constraints directly from GitHub using specific
tag/hash name. We tag commits working for particular release with constraints-<version> tag. So for example
fixed valid constraints 1.10.12 can be used by using ``constraints-1.10.12`` tag:

.. code-block:: bash

  pip install apache-airflow[ssh]==1.10.12 \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-1.10.12/constraints-3.6.txt"

There are different set of fixed constraint files for different python major/minor versions and you should
use the right file for the right python version.

The ``constraints-<PYTHON_MAJOR_MINOR_VERSION>.txt`` will be automatically regenerated by CI cron job
every time after the ``setup.py`` is updated and pushed if the tests are successful. There are separate
jobs for each python version.

Backport providers packages
---------------------------

**NOTE:** In case of problems with installation / development of backport packages
check `troubleshooting installing backport packages <https://github
.com/apache/airflow#troubleshooting-installing-backport-packages>`_.

Since we are developing new operators in the master branch, we prepared backport packages ready to be
installed for Airflow 1.10.* series. Those backport operators (the tested ones) are going to be released
in PyPi and we are going to maintain the list at
`Backported providers package page <https://cwiki.apache.org/confluence/display/AIRFLOW/Backported+providers+packages+for+Airflow+1.10.*+series>`_

Some of the packages have cross-dependencies with other providers packages. This typically happens for
transfer operators where operators use hooks from the other providers in case they are transferring
data between the providers. The list of dependencies is maintained (automatically with pre-commits)
in the ``airflow/providers/dependencies.json``. Pre-commits are also used to generate dependencies.
The dependency list is automatically used during pypi packages generation.

Cross-dependencies between provider packages are converted into extras - if you need functionality from
the other provider package you can install it adding [extra] after the apache-airflow-providers-PROVIDER
for example ``pip install apache-airflow-providers-google[amazon]`` in case you want to use GCP's
transfer operators from Amazon ECS.

If you add a new dependency between different providers packages, it will be detected automatically during
pre-commit phase and pre-commit will fail - and add entry in dependencies.json so that the package extra
dependencies are properly added when package is installed.

You can regenerate the whole list of provider dependencies by running this command (you need to have
``pre-commits`` installed).

.. code-block:: bash

  pre-commit run build-providers-dependencies


Here is the list of packages and their extras:


  .. START PACKAGE DEPENDENCIES HERE

========================== ===========================
Package                    Extras
========================== ===========================
amazon                     apache.hive,google,imap,mongo,postgres,ssh
apache.druid               apache.hive
apache.hive                amazon,microsoft.mssql,mysql,presto,samba,vertica
apache.livy                http
dingding                   http
discord                    http
google                     amazon,apache.cassandra,cncf.kubernetes,microsoft.azure,microsoft.mssql,mysql,postgres,presto,sftp
hashicorp                  google
microsoft.azure            oracle
microsoft.mssql            odbc
mysql                      amazon,presto,vertica
opsgenie                   http
postgres                   amazon
sftp                       ssh
slack                      http
========================== ===========================

  .. END PACKAGE DEPENDENCIES HERE

Documentation
=============

The latest API documentation (for the master branch) is usually available
`here <https://airflow.readthedocs.io/en/latest/>`__.

To generate a local version you can use `<BREEZE.rst>`_.

The documentation build consists of verifying consistency of documentation and two steps:

* spell checking
* building documentation

You can only run one of the steps via ``--spellcheck-only`` or ``--docs-only``.

.. code-block:: bash

    ./breeze build-docs

or just to run spell-check

.. code-block:: bash

     ./breeze build-docs -- --spellcheck-only

or just to run documentation building

.. code-block:: bash

     ./breeze build-docs

Also documentation is available as downloadable artifact in GitHub Actions after the CI builds your PR.

**Known issues:**

If you are creating a new directory for new integration in the ``airflow.providers`` package,
you should also update the ``docs/autoapi_templates/index.rst`` file.

If you are creating new ``hooks``, ``sensors``, ``operators`` directory in
the ``airflow.providers`` package, you should also update
the ``docs/operators-and-hooks-ref.rst`` file.

If you are creating ``example_dags`` directory, you need to create ``example_dags/__init__.py`` with Apache
license or copy another ``__init__.py`` file that contains the necessary license.

Static code checks
==================

We check our code quality via static code checks. See
`STATIC_CODE_CHECKS.rst <STATIC_CODE_CHECKS.rst>`_ for details.

Your code must pass all the static code checks in the CI in order to be eligible for Code Review.
The easiest way to make sure your code is good before pushing is to use pre-commit checks locally
as described in the static code checks documentation.

.. _coding_style:

Coding style and best practices
===============================

Most of our coding style rules are enforced programmatically by flake8 and pylint (which are run automatically
on every pull request), but there are some rules that are not yet automated and are more Airflow specific or
semantic than style

Database Session Handling
-------------------------

**Explicit is better than implicit.** If a function accepts a ``session`` parameter it should not commit the
transaction itself. Session management is up to the caller.

To make this easier there is the ``create_session`` helper:

.. code-block:: python

    from airflow.utils.session import create_session

    def my_call(*args, session):
      ...
      # You MUST not commit the session here.

    with create_session() as session:
        my_call(*args, session=session)

If this function is designed to be called by "end-users" (i.e. DAG authors) then using the ``@provide_session`` wrapper is okay:

.. code-block:: python

    from airflow.utils.session import provide_session

    ...

    @provide_session
    def my_method(arg, arg, session=None)
      ...
      # You SHOULD not commit the session here. The wrapper will take care of commit()/rollback() if exception

Naming Conventions for provider packages
----------------------------------------

In Airflow 2.0 we standardized and enforced naming for provider packages, modules and classes.
those rules (introduced as AIP-21) were not only introduced but enforced using automated checks
that verify if the naming conventions are followed. Here is a brief summary of the rules, for
detailed discussion you can go to [AIP-21 Changes in import paths](https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-21%3A+Changes+in+import+paths)

The rules are as follows:

* Provider packages are all placed in 'airflow.providers'

* Providers are usually direct sub-packages of the 'airflow.providers' package but in some cases they can be
  further split into sub-packages (for example 'apache' package has 'cassandra', 'druid' ... providers ) out
  of which several different provider packages are produced (apache.cassandra, apache.druid). This is
  case when the providers are connected under common umbrella but very loosely coupled on the code level.

* In some cases the package can have sub-packages but they are all delivered as single provider
  package (for example 'google' package contains 'ads', 'cloud' etc. sub-packages). This is in case
  the providers are connected under common umbrella and they are also tightly coupled on the code level.

* Typical structure of provider package:
    * example_dags -> example DAGs are stored here (used for documentation and System Tests)
    * hooks -> hooks are stored here
    * operators -> operators are stored here
    * sensors -> sensors are stored here
    * secrets -> secret backends are stored here
    * transfers -> transfer operators are stored here

* Module names do not contain word "hooks", "operators" etc. The right type comes from
  the package. For example 'hooks.datastore' module contains DataStore hook and 'operators.datastore'
  contains DataStore operators.

* Class names contain 'Operator', 'Hook', 'Sensor' - for example DataStoreHook, DataStoreExportOperator

* Operator name usually follows the convention: ``<Subject><Action><Entity>Operator``
  (BigQueryExecuteQueryOperator) is a good example

* Transfer Operators are those that actively push data from one service/provider and send it to another
  service (might be for the same or another provider). This usually involves two hooks. The convention
  for those ``<Source>To<Destination>Operator``. They are not named *TransferOperator nor *Transfer.

* Operators that use external service to perform transfer (for example CloudDataTransferService operators
  are not placed in "transfers" package and do not have to follow the naming convention for
  transfer operators.

* It is often debatable where to put transfer operators but we agreed to the following criteria:

  * We use "maintainability" of the operators as the main criteria - so the transfer operator
    should be kept at the provider which has highest "interest" in the transfer operator

  * For Cloud Providers or Service providers that usually means that the transfer operators
    should land at the "target" side of the transfer

* Secret Backend name follows the convention: ``<SecretEngine>Backend``.

* Tests are grouped in parallel packages under "tests.providers" top level package. Module name is usually
  ``test_<object_to_test>.py``,

* System tests (not yet fully automated but allowing to run e2e testing of particular provider) are
  named with _system.py suffix.

Test Infrastructure
===================

We support the following types of tests:

* **Unit tests** are Python tests launched with ``pytest``.
  Unit tests are available both in the `Breeze environment <BREEZE.rst>`_
  and `local virtualenv <LOCAL_VIRTUALENV.rst>`_.

* **Integration tests** are available in the Breeze development environment
  that is also used for Airflow's CI tests. Integration test are special tests that require
  additional services running, such as Postgres, Mysql, Kerberos, etc.

* **System tests** are automatic tests that use external systems like
  Google Cloud. These tests are intended for an end-to-end DAG execution.

For details on running different types of Airflow tests, see `TESTING.rst <TESTING.rst>`_.

Metadata Database Updates
=========================

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


Node.js Environment Setup
=========================

``airflow/www/`` contains all yarn-managed, front-end assets. Flask-Appbuilder
itself comes bundled with jQuery and bootstrap. While they may be phased out
over time, these packages are currently not managed with yarn.

Make sure you are using recent versions of node and yarn. No problems have been
found with node\>=8.11.3 and yarn\>=1.19.1.

Installing yarn and its packages
--------------------------------

Make sure yarn is available in your environment.

To install yarn on macOS:

1.  Run the following commands (taken from `this source <https://gist.github.com/DanHerbert/9520689>`__):

.. code-block:: bash

    brew install node
    brew install yarn
    yarn config set prefix ~/.yarn


2.  Add ``~/.yarn/bin`` to your ``PATH`` so that commands you are installing
    could be used globally.

3.  Set up your ``.bashrc`` file and then ``source ~/.bashrc`` to reflect the
    change.

.. code-block:: bash

    export PATH="$HOME/.yarn/bin:$PATH"

4.  Install third-party libraries defined in ``package.json`` by running the
    following commands within the ``airflow/www/`` directory:


.. code-block:: bash

    # from the root of the repository, move to where our JS package.json lives
    cd airflow/www/
    # run yarn install to fetch all the dependencies
    yarn install


These commands install the libraries in a new ``node_modules/`` folder within
``www/``.

Should you add or upgrade a node package, run
``yarn add --dev <package>`` for packages needed in development or
``yarn add <package>`` for packages used by the code.
Then push the newly generated ``package.json`` and ``yarn.lock`` file so that we
could get a reproducible build. See the `Yarn docs
<https://yarnpkg.com/en/docs/cli/add#adding-dependencies->`_ for more details.


Generate Bundled Files with yarn
--------------------------------

To parse and generate bundled files for Airflow, run either of the following
commands:

.. code-block:: bash

    # Compiles the production / optimized js & css
    yarn run prod

    # Starts a web server that manages and updates your assets as you modify them
    yarn run dev


Follow JavaScript Style Guide
-----------------------------

We try to enforce a more consistent style and follow the JS community
guidelines.

Once you add or modify any JavaScript code in the project, please make sure it
follows the guidelines defined in `Airbnb
JavaScript Style Guide <https://github.com/airbnb/javascript>`__.

Apache Airflow uses `ESLint <https://eslint.org/>`__ as a tool for identifying and
reporting on patterns in JavaScript. To use it, run any of the following
commands:

.. code-block:: bash

    # Check JS code in .js and .html files, and report any errors/warnings
    yarn run lint

    # Check JS code in .js and .html files, report any errors/warnings and fix them if possible
    yarn run lint:fix

How to sync your fork
=====================

When you have your fork, you should periodically synchronize the master of your fork with the
Apache Airflow master. In order to do that you can ``git pull --rebase`` to your local git repository from
apache remote and push the master (often with ``--force`` to your fork). There is also an easy
way using ``Force sync master from apache/airflow`` workflow. You can go to "Actions" in your repository and
choose the workflow and manually trigger the workflow using "Run workflow" command.

This will force-push the master from apache/airflow to the master in your fork. Note that in case you
modified the master in your fork, you might loose those changes.


How to rebase PR
================

A lot of people are unfamiliar with the rebase workflow in Git, but we think it is an excellent workflow,
providing a better alternative to the merge workflow. We've therefore written a short guide for those who would like to learn it.

As opposed to the merge workflow, the rebase workflow allows us to
clearly separate your changes from the changes of others. It puts the responsibility of rebasing on the
author of the change. It also produces a "single-line" series of commits on the master branch. This
makes it easier to understand what was going on and to find reasons for problems (it is especially
useful for "bisecting" when looking for a commit that introduced some bugs).

First of all, we suggest you read about the rebase workflow here:
`Merging vs. rebasing <https://www.atlassian.com/git/tutorials/merging-vs-rebasing>`_. This is an
excellent article that describes all the ins/outs of the rebase workflow. I recommend keeping it for future reference.

The goal of rebasing your PR on top of ``apache/master`` is to "transplant" your change on top of
the latest changes that are merged by others. It also allows you to fix all the conflicts
that arise as a result of other people changing the same files as you and merging the changes to ``apache/master``.

Here is how rebase looks in practice:

1. You first need to add the Apache project remote to your git repository. In this example, we will be adding the remote
as "apache" so you can refer to it easily:

* If you use ssh: ``git remote add apache git@github.com:apache/airflow.git``
* If you use https: ``git remote add apache https://github.com/apache/airflow.git``

2. You then need to make sure that you have the latest master fetched from the ``apache`` repository. You can do this
   via:

   ``git fetch apache`` (to fetch apache remote)

   ``git fetch --all``  (to fetch all remotes)

3. Assuming that your feature is in a branch in your repository called ``my-branch`` you can easily check
   what is the base commit you should rebase from by:

   ``git merge-base my-branch apache/master``

   This will print the HASH of the base commit which you should use to rebase your feature from.
   For example: ``5abce471e0690c6b8d06ca25685b0845c5fd270f``. You can also find this commit hash manually if you want
   better control.

   Run:

   ``git log``

   And find the first commit that you DO NOT want to "transplant".

   Performing:

   ``git rebase HASH``

   Will "transplant" all commits after the commit with the HASH.

4. Check out your feature branch locally via:

   ``git checkout my-branch``

5. Rebase:

   ``git rebase HASH --onto apache/master``

   For example:

   ``git rebase 5abce471e0690c6b8d06ca25685b0845c5fd270f --onto apache/master``

6. If you have no conflicts - that's cool. You rebased. You can now run ``git push --force-with-lease`` to
   push your changes to your repository. That should trigger the build in our CI if you have a
   Pull Request (PR) opened already.

7. While rebasing you might have conflicts. Read carefully what git tells you when it prints information
   about the conflicts. You need to solve the conflicts manually. This is sometimes the most difficult
   part and requires deliberately correcting your code and looking at what has changed since you developed your
   changes.

   There are various tools that can help you with this. You can use:

   ``git mergetool``

   You can configure different merge tools with it. You can also use IntelliJ/PyCharm's excellent merge tool.
   When you open a project in PyCharm which has conflicts, you can go to VCS > Git > Resolve Conflicts and there
   you have a very intuitive and helpful merge tool. For more information, see
   `Resolve conflicts <https://www.jetbrains.com/help/idea/resolving-conflicts.html.>`_.

8. After you've solved your conflict run:

   ``git rebase --continue``

   And go either to point 6. or 7, depending on whether you have more commits that cause conflicts in your PR (rebasing applies each
   commit from your PR one-by-one).

How to communicate
==================

Apache Airflow is a Community within Apache Software Foundation. As the motto of
the Apache Software Foundation states "Community over Code" - people in the
community are far more important than their contribution.

This means that communication plays a big role in it, and this chapter is all about it.

In our communication, everyone is expected to follow the `ASF Code of Conduct <https://www.apache.org/foundation/policies/conduct>`_.

We have various channels of communication - starting from the official devlist, comments
in the Pull Requests, Slack, wiki.

All those channels can be used for different purposes.
You can join the channels via links at the `Airflow Community page <https://airflow.apache.org/community/>`_

* The `Apache Airflow devlist <https://lists.apache.org/list.html?dev@airflow.apache.org>`_ for:
   * official communication
   * general issues, asking community for opinion
   * discussing proposals
   * voting
* The `Airflow CWiki <https://cwiki.apache.org/confluence/display/AIRFLOW/Airflow+Home?src=breadcrumbs>`_ for:
   * detailed discussions on big proposals (Airflow Improvement Proposals also name AIPs)
   * helpful, shared resources (for example Apache Airflow logos
   * information that can be re-used by others (for example instructions on preparing workshops)
* GitHub `Pull Requests (PRs) <https://github.com/apache/airflow/pulls>`_ for:
   * discussing implementation details of PRs
   * not for architectural discussions (use the devlist for that)
* The deprecated `JIRA issues <https://issues.apache.org/jira/projects/AIRFLOW/issues/AIRFLOW-4470?filter=allopenissues>`_ for:
   * checking out old but still valuable issues that are not on GitHub yet
   * mentioning the JIRA issue number in the title of the related PR you would like to open on GitHub

**IMPORTANT**
We don't create new issues on JIRA anymore. The reason we still look at JIRA issues is that there are valuable tickets inside of it. However, each new PR should be created on `GitHub issues <https://github.com/apache/airflow/issues>`_ as stated in `Contribution Workflow Example <https://github.com/apache/airflow/blob/master/CONTRIBUTING.rst#contribution-workflow-example>`_

* The `Apache Airflow Slack <https://s.apache.org/airflow-slack>`_ for:
   * ad-hoc questions related to development (#development channel)
   * asking for review (#development channel)
   * asking for help with PRs (#how-to-pr channel)
   * troubleshooting (#troubleshooting channel)
   * group talks (including SIG - special interest groups) (#sig-* channels)
   * notifications (#announcements channel)
   * random queries (#random channel)
   * regional announcements (#users-* channels)
   * newbie questions (#newbie-questions channel)
   * occasional discussions (wherever appropriate including group and 1-1 discussions)

The devlist is the most important and official communication channel. Often at Apache project you can
hear "if it is not in the devlist - it did not happen". If you discuss and agree with someone from the
community on something important for the community (including if it is with committer or PMC member) the
discussion must be captured and reshared on devlist in order to give other members of the community to
participate in it.

We are using certain prefixes for email subjects for different purposes. Start your email with one of those:
  * ``[DISCUSS]`` - if you want to discuss something but you have no concrete proposal yet
  * ``[PROPOSAL]`` - if usually after "[DISCUSS]" thread discussion you want to propose something and see
    what other members of the community think about it.
  * ``[AIP-NN]`` - if the mail is about one of the Airflow Improvement Proposals
  * ``[VOTE]`` - if you would like to start voting on a proposal discussed before in a "[PROPOSAL]" thread

Voting is governed by the rules described in `Voting <https://www.apache.org/foundation/voting.html>`_

We are all devoting our time for community as individuals who except for being active in Apache Airflow have
families, daily jobs, right for vacation. Sometimes we are in different time zones or simply are
busy with day-to-day duties that our response time might be delayed. For us it's crucial
to remember to respect each other in the project with no formal structure.
There are no managers, departments, most of us is autonomous in our opinions, decisions.
All of it makes Apache Airflow community a great space for open discussion and mutual respect
for various opinions.

Disagreements are expected, discussions might include strong opinions and contradicting statements.
Sometimes you might get two committers asking you to do things differently. This all happened in the past
and will continue to happen. As a community we have some mechanisms to facilitate discussion and come to
a consensus, conclusions or we end up voting to make important decisions. It is important that these
decisions are not treated as personal wins or looses. At the end it's the community that we all care about
and what's good for community, should be accepted even if you have a different opinion. There is a nice
motto that you should follow in case you disagree with community decision "Disagree but engage". Even
if you do not agree with a community decision, you should follow it and embrace (but you are free to
express your opinion that you don't agree with it).

As a community - we have high requirements for code quality. This is mainly because we are a distributed
and loosely organised team. We have both - contributors that commit one commit only, and people who add
more commits. It happens that some people assume informal "stewardship" over parts of code for some time -
but at any time we should make sure that the code can be taken over by others, without excessive communication.
Setting high requirements for the code (fairly strict code review, static code checks, requirements of
automated tests, pre-commit checks) is the best way to achieve that - by only accepting good quality
code. Thanks to full test coverage we can make sure that we will be able to work with the code in the future.
So do not be surprised if you are asked to add more tests or make the code cleaner -
this is for the sake of maintainability.

Here are a few rules that are important to keep in mind when you enter our community:

 * Do not be afraid to ask questions
 * The communication is asynchronous - do not expect immediate answers, ping others on slack
   (#development channel) if blocked
 * There is a #newbie-questions channel in slack as a safe place to ask questions
 * You can ask one of the committers to be a mentor for you, committers can guide within the community
 * You can apply to more structured `Apache Mentoring Programme <https://community.apache.org/mentoringprogramme.html>`_
 * It’s your responsibility as an author to take your PR from start-to-end including leading communication
   in the PR
 * It’s your responsibility as an author to ping committers to review your PR - be mildly annoying sometimes,
   it’s OK to be slightly annoying with your change - it is also a sign for committers that you care
 * Be considerate to the high code quality/test coverage requirements for Apache Airflow
 * If in doubt - ask the community for their opinion or propose to vote at the devlist
 * Discussions should concern subject matters - judge or criticise the merit but never criticise people
 * It’s OK to express your own emotions while communicating - it helps other people to understand you
 * Be considerate for feelings of others. Tell about how you feel not what you think of others

Committer Responsibilities
==========================

Committers are more than contributors. While it's important for committers to maintain standing by
committing code, their key role is to build and foster a healthy and active community.
This means that committers should:

* Review PRs in a timely and reliable fashion
* They should also help to actively whittle down the PR backlog
* Answer questions (i.e. on the dev list, in PRs, in GitHub Issues, slack, etc...)
* Take on core changes/bugs/feature requests
* Some changes are important enough that a committer needs to ensure it gets done. This is especially
  the case if no one from the community is taking it on.
* Improve processes and tooling
* Refactoring code

Commit Policy
=============

The following commit policy passed by a vote 8(binding FOR) to 0 against on May 27, 2016 on the dev list
and slightly modified and consensus reached in October 2020:

* Commits need a +1 vote from a committer who is not the author
* Do not merge a PR that regresses linting or does not pass CI tests (unless we have
  justification such as clearly transient error).
* When we do AIP voting, both PMC and committer +1s are considered as binding vote.

Resources & Links
=================
- `Airflow’s official documentation <http://airflow.apache.org/>`__

- `More resources and links to Airflow related content on the Wiki <https://cwiki.apache.org/confluence/display/AIRFLOW/Airflow+Links>`__
