
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

Creating issues and pull requests
=================================

This document describes various way you can contribute to Apache Airflow project - by creating
issues and pull requests.

.. contents:: :local:

Report Bugs
-----------

Report bugs through `GitHub <https://github.com/apache/airflow/issues>`__.

Please report relevant information and preferably code that exhibits the problem.

Fix Bugs
--------

Look through the GitHub issues for bugs. Anything is open to whoever wants to implement it.

Issue reporting and resolution process
--------------------------------------

An unusual element of the Apache Airflow project is that you can open a PR to
fix an issue or make an enhancement, without needing to open an issue first.
This is intended to make it as easy as possible to contribute to the project.

If you however feel the need to open an issue (usually a bug or feature request)
consider starting with a `GitHub Discussion <https://github.com/apache/airflow/discussions>`_ instead.
In the vast majority of cases discussions are better than issues - you should only open
issues if you are sure you found a bug and have a reproducible case,
or when you want to raise a feature request that will not require a lot of discussion.
If you have a very important topic to discuss, start a discussion on the
`Devlist <https://lists.apache.org/list.html?dev@airflow.apache.org>`_ instead.

The Apache Airflow project uses a set of labels for tracking and triaging issues, as
well as a set of priorities and milestones to track how and when the enhancements and bug
fixes make it into an Airflow release. This is documented as part of
the `Issue reporting and resolution process <ISSUE_TRIAGE_PROCESS.rst>`_,

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

See the `Docs README <https://github.com/apache/airflow/blob/main/docs/README.rst>`__ for more information about contributing to Airflow docs.

Submit Feedback
---------------

The best way to send feedback is to `open an issue on GitHub <https://github.com/apache/airflow/issues/new/choose>`__.

If you are proposing a new feature:

-   Explain in detail how it would work.
-   Keep the scope as narrow as possible to make it easier to implement.
-   Remember that this is a volunteer-driven project, and that contributions are
    welcome :)

Pull Request guidelines
=======================

Before you submit a Pull Request (PR) from your forked repo, check that it meets
these guidelines:

-   Include tests, either as doctests, unit tests, or both, to your pull request.

    The airflow repo uses `GitHub Actions <https://help.github.com/en/actions>`__ to
    run the tests and `codecov <https://codecov.io/gh/apache/airflow>`__ to track
    coverage. You can set up both for free on your fork. It will help you make sure you do not
    break the build with your PR and that you help increase coverage.
    Also we advise to install locally `pre-commit hooks <static_code_checks.rst#pre-commit-hooks>`__ to
    apply various checks, code generation and formatting at the time you make a local commit - which
    gives you near-immediate feedback on things you need to fix before you push your code to the PR, or in
    many case it will even fix it for you locally so that you can add and commit it straight away.

-   Follow our project's `Coding style and best practices`_. Usually we attempt to enforce the practices by
    having appropriate pre-commits. There are checks amongst them that aren't currently enforced
    programmatically (either because they are too hard or just not yet done).

-   We prefer that you ``rebase`` your PR (and do it quite often) rather than merge. It leads to
    easier reviews and cleaner changes where you know exactly what changes you've done. You can learn more
    about rebase vs. merge workflow in `Rebase and merge your pull request <https://github.blog/2016-09-26-rebase-and-merge-pull-requests/>`__
    and `Rebase your fork <http://stackoverflow.com/a/7244456/1110993>`__. Make sure to resolve all conflicts
    during rebase.

-   When merging PRs, Maintainer will use **Squash and Merge** which means then your PR will be merged as one
    commit, regardless of the number of commits in your PR. During the review cycle, you can keep a commit
    history for easier review, but if you need to, you can also squash all commits to reduce the
    maintenance burden during rebase.

-   Add an `Apache License <http://www.apache.org/legal/src-headers.html>`__ header to all new files. If you
    have ``pre-commit`` installed, pre-commit will do it automatically for you. If you hesitate to install
    pre-commit for your local repository - for example because it takes a few seconds to commit your changes,
    this one thing might be a good reason to convince anyone to install pre-commit.

-   If your PR adds functionality, make sure to update the docs as part of the same PR, not only
    code and tests. Docstring is often sufficient. Make sure to follow the Sphinx compatible standards.

-   Make sure your code fulfills all the
    `static code checks <static_code_checks.rst#static-code-checks>`__ we have in our code. The easiest way
    to make sure of that is - again - to install `pre-commit hooks <static_code_checks.rst#pre-commit-hooks>`__

-   Make sure your PR is small and focused on one change only - avoid adding unrelated changes, mixing
    adding features and refactoring. Keeping to that rule will make it easier to review your PR and will make
    it easier for release managers if they decide that your change should be cherry-picked to release it in a
    bug-fix release of Airflow. If you want to add a new feature and refactor the code, it's better to split the
    PR to several smaller PRs. It's also quite a good and common idea to keep a big ``Draft`` PR if you have
    a bigger change that you want to make and then create smaller PRs from it that are easier to review and
    merge and cherry-pick. It takes a long time (and a lot of attention and focus of a reviewer to review
    big PRs so by splitting it to smaller PRs you actually speed up the review process and make it easier
    for your change to be eventually merged.

-   Run relevant tests locally before opening PR. Often tests are placed in the files that are corresponding
    to the changed code (for example for ``airflow/cli/cli_parser.py`` changes you have tests in
    ``tests/cli/test_cli_parser.py``). However there are a number of cases where the tests that should run
    are placed elsewhere - you can either run tests for the whole ``TEST_TYPE`` that is relevant (see
    ``breeze testing tests --help`` output for available test types) or you can run all tests, or eventually
    you can push your code to PR and see results of the tests in the CI.

-   You can use any supported python version to run the tests, but the best is to check
    if it works for the oldest supported version (Python 3.8 currently). In rare cases
    tests might fail with the oldest version when you use features that are available in newer Python
    versions. For that purpose we have ``airflow.compat`` package where we keep back-ported
    useful features from newer versions.

-   Adhere to guidelines for commit messages described in this `article <http://chris.beams.io/posts/git-commit/>`__.
    This makes the lives of those who come after you (and your future self) a lot easier.

.. _coding_style:

Coding style and best practices
===============================

Most of our coding style rules are enforced programmatically by ruff and mypy, which are run automatically
with static checks and on every Pull Request (PR), but there are some rules that are not yet automated and
are more Airflow specific or semantic than style.

Don't Use Asserts Outside Tests
-------------------------------

Our community agreed that to various reasons we do not use ``assert`` in production code of Apache Airflow.
For details check the relevant `mailing list thread <https://lists.apache.org/thread.html/bcf2d23fcd79e21b3aac9f32914e1bf656e05ffbcb8aa282af497a2d%40%3Cdev.airflow.apache.org%3E>`_.

In other words instead of doing:

.. code-block:: python

    assert some_predicate()

you should do:

.. code-block:: python

    if not some_predicate():
        handle_the_case()

The one exception to this is if you need to make an assert for typechecking (which should be almost a last resort) you can do this:

.. code-block:: python

    if TYPE_CHECKING:
        assert isinstance(x, MyClass)


Database Session Handling
-------------------------

**Explicit is better than implicit.** If a function accepts a ``session`` parameter it should not commit the
transaction itself. Session management is up to the caller.

To make this easier, there is the ``create_session`` helper:

.. code-block:: python

    from sqlalchemy.orm import Session

    from airflow.utils.session import create_session


    def my_call(x, y, *, session: Session):
        ...
        # You MUST not commit the session here.


    with create_session() as session:
        my_call(x, y, session=session)

.. warning::
  **DO NOT** add a default to the ``session`` argument **unless** ``@provide_session`` is used.

If this function is designed to be called by "end-users" (i.e. DAG authors) then using the ``@provide_session`` wrapper is okay:

.. code-block:: python

    from sqlalchemy.orm import Session

    from airflow.utils.session import NEW_SESSION, provide_session


    @provide_session
    def my_method(arg, *, session: Session = NEW_SESSION):
        ...
        # You SHOULD not commit the session here. The wrapper will take care of commit()/rollback() if exception

In both cases, the ``session`` argument is a `keyword-only argument`_. This is the most preferred form if
possible, although there are some exceptions in the code base where this cannot be used, due to backward
compatibility considerations. In most cases, ``session`` argument should be last in the argument list.

.. _`keyword-only argument`: https://www.python.org/dev/peps/pep-3102/


Don't use time() for duration calculations
-----------------------------------------

If you wish to compute the time difference between two events with in the same process, use
``time.monotonic()``, not ``time.time()`` nor ``timezone.utcnow()``.

If you are measuring duration for performance reasons, then ``time.perf_counter()`` should be used. (On many
platforms, this uses the same underlying clock mechanism as monotonic, but ``perf_counter`` is guaranteed to be
the highest accuracy clock on the system, monotonic is simply "guaranteed" to not go backwards.)

If you wish to time how long a block of code takes, use ``Stats.timer()`` -- either with a metric name, which
will be timed and submitted automatically:

.. code-block:: python

    from airflow.stats import Stats

    ...

    with Stats.timer("my_timer_metric"):
        ...

or to time but not send a metric:

.. code-block:: python

    from airflow.stats import Stats

    ...

    with Stats.timer() as timer:
        ...

    log.info("Code took %.3f seconds", timer.duration)

For full docs on ``timer()`` check out `airflow/stats.py`_.

If the start_date of a duration calculation needs to be stored in a database, then this has to be done using
datetime objects. In all other cases, using datetime for duration calculation MUST be avoided as creating and
diffing datetime operations are (comparatively) slow.

Naming Conventions for provider packages
----------------------------------------

In Airflow 2.0 we standardized and enforced naming for provider packages, modules and classes.
those rules (introduced as AIP-21) were not only introduced but enforced using automated checks
that verify if the naming conventions are followed. Here is a brief summary of the rules, for
detailed discussion you can go to `AIP-21 Changes in import paths <https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-21%3A+Changes+in+import+paths>`_

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
