
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

Pull Requests
=============

This document describes how you can create Pull Requests and describes coding standards we use when
implementing them.

**The outline for this document in GitHub is available at top-right corner button (with 3-dots and 3 lines).**

Pull Request guidelines
-----------------------

Before you submit a Pull Request (PR) from your forked repo, check that it meets
these guidelines:

-   Include tests, either as doctests, unit tests, or both, to your pull request.

    The Airflow repo uses `GitHub Actions <https://help.github.com/en/actions>`__ to
    run the tests and `codecov <https://codecov.io/gh/apache/airflow>`__ to track
    coverage. You can set up both for free on your fork. It will help you make sure you do not
    break the build with your PR and that you help increase coverage.
    Also we advise to install locally `prek hooks <08_static_code_checks.rst#prek-hooks>`__ to
    apply various checks, code generation and formatting at the time you make a local commit - which
    gives you near-immediate feedback on things you need to fix before you push your code to the PR, or in
    many case it will even fix it for you locally so that you can add and commit it straight away.

-   Follow our project's `Coding style and best practices`_. Usually we attempt to enforce the practices by
    having appropriate prek hooks. There are checks amongst them that aren't currently enforced
    programmatically (either because they are too hard or just not yet done).

-   Maintainers will not merge a PR that regresses linting or does not pass CI tests (unless you have good
    justification that it a transient error or something that is being fixed in other PR).

-   Maintainers will not merge PRs that have unresolved conversation.

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
    have ``prek`` installed, prek will do it automatically for you. If you hesitate to install
    prek for your local repository - for example because it takes a few seconds to commit your changes,
    this one thing might be a good reason to convince anyone to install prek.

-   If your PR adds functionality, make sure to update the docs as part of the same PR, not only
    code and tests. Docstring is often sufficient. Make sure to follow the Sphinx compatible standards.

-   Make sure your code fulfills all the
    `static code checks <08_static_code_checks.rst#static-code-checks>`__ we have in our code. The easiest way
    to make sure of that is - again - to install `prek hooks <08_static_code_checks.rst#prek-hooks>`__

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
    ``breeze testing core-tests --help`` or ``breeze testing providers-tests --help`` output for
    available test types for each of the testing commands) or you can run all tests, or eventually
    you can push your code to PR and see results of the tests in the CI.

-   You can use any supported python version to run the tests, but the best is to check
    if it works for the oldest supported version (Python 3.10 currently). In rare cases
    tests might fail with the oldest version when you use features that are available in newer Python
    versions. For that purpose we have ``airflow.compat`` package where we keep back-ported
    useful features from newer versions.

-   Adhere to guidelines for commit messages described in this `article <https://cbea.ms/git-commit/>`__.
    This makes the lives of those who come after you (and your future self) a lot easier.

Gen-AI Assisted contributions
-----------------------------

Generally, it's fine to use Gen-AI tools to help you create Pull Requests for Apache Airflow as long as you
adhere to the following guidelines:

* Follow the `Apache Software Foundation (ASF) Generative Tooling guideliens <https://www.apache.org/legal/generative-tooling.html>`__.
* Ensure that you review and understand all code generated by Gen-AI tools before including it in your PR -
  do not blindly trust the generated code.
* Make sure that the generated code adheres to the project's coding standards and best practices described
  above
* Make sure to run all relevant static checks and tests locally to verify that the generated code works as
  intended and does not introduce any issues.
* State in your PR description that you have used Gen-AI tools to assist in creating the PR.
* Be prepared to explain and justify the use of Gen-AI tools if asked by project maintainers or reviewers.
* Remember that the final responsibility for the code in your PR lies with you, regardless of whether
  it was generated by a tool or written by you.
* By contributing code generated by Gen-AI tools, you agree to comply with the project's licensing terms
  and any applicable laws and regulations.
* Remember that when you blindly copy & paste hallucinated code from Gen-AI tools, you are not improving your
  skills as a developer and you might be introducing security and stability risks to the project and maintainers
  when spot such repeated behaviours will close all such PRs and might block the contributor from further
  contributions. Your personal reputation goes south with such behaviour, so if your goal is to increase
  your value as a developer, avoid such practices.

Requirement to resolve all conversations
----------------------------------------

We have decided to enable the requirement to resolve all the open conversations in a
PR in order to make it merge-able. You will see in the status of the PR that it needs to have all the
conversations resolved before it can be merged.

The goal is to make it easier to see when there are some conversations that are not
resolved for everyone involved in the PR - author, reviewers and maintainers who try to figure out if
the PR is ready to merge and - eventually - merge it. The goal is also to use conversations more as a "soft" way
to request changes and limit the use of ``Request changes`` status to only those cases when the maintainer
is sure that the PR should not be merged in the current state. This leads to faster review/merge
cycle and less problems with stalled PRs that have ``Request changes`` status but all the issues are
already solved.

.. _coding_style:

Coding style and best practices
-------------------------------

Most of our coding style rules are enforced programmatically by ruff and mypy, which are run automatically
with static checks and on every Pull Request (PR), but there are some rules that are not yet automated and
are more Airflow specific or semantic than style.

Don't Use Asserts Outside Tests
...............................

Our community agreed that to various reasons we do not use ``assert`` in production code of Apache Airflow.
For details check the relevant `mailing list thread <https://lists.apache.org/thread.html/bcf2d23fcd79e21b3aac9f32914e1bf656e05ffbcb8aa282af497a2d%40%3Cdev.airflow.apache.org%3E>`_.

In other words instead of doing:

.. code-block:: python

    assert some_predicate()

you should do:

.. code-block:: python

    if not some_predicate():
        handle_the_case()

The one exception to this is if you need to make an assert for type checking (which should be almost a last resort) you can do this:

.. code-block:: python

    if TYPE_CHECKING:
        assert isinstance(x, MyClass)


Database Session Handling
.........................

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

If this function is designed to be called by "end-users" (i.e. Dag authors) then using the ``@provide_session`` wrapper is okay:

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
..........................................

If you wish to compute the time difference between two events with in the same process, use
``time.monotonic()``, not ``time.time()`` nor ``timezone.utcnow()``.

If you are measuring duration for performance reasons, then ``time.perf_counter()`` should be used. (On many
platforms, this uses the same underlying clock mechanism as monotonic, but ``perf_counter`` is guaranteed to be
the highest accuracy clock on the system, monotonic is simply "guaranteed" to not go backwards.)

If you wish to time how long a block of code takes, use ``Stats.timer()`` -- either with a metric name, which
will be timed and submitted automatically:

.. code-block:: python

    from airflow.observability.stats import Stats

    ...

    with Stats.timer("my_timer_metric"):
        ...

or to time but not send a metric:

.. code-block:: python

    from airflow.observability.stats import Stats

    ...

    with Stats.timer() as timer:
        ...

    log.info("Code took %.3f seconds", timer.duration)

For full docs on ``timer()`` check out `airflow/stats.py`_.

If the start_date of a duration calculation needs to be stored in a database, then this has to be done using
datetime objects. In all other cases, using datetime for duration calculation MUST be avoided as creating and
diffing datetime operations are (comparatively) slow.

Templated fields in Operator's __init__ method
..............................................

Airflow Operators might have some fields added to the list of ``template_fields``. Such fields should be
set in the constructor (``__init__`` method) of the operator and usually their values should
come from the ``__init__`` method arguments. The reason for that is that the templated fields
are evaluated at the time of the operator execution and when you pass arguments to the operator
in the Dag, the fields that are set on the class just before the ``execute`` method is called
are processed through templating engine and the fields values are set to the result of applying the
templating engine to the fields (in case the field is a structure such as dict or list, the templating
engine is applied to all the values of the structure).

That's why we expect two things in case of ``template fields``:

* with a few exceptions, only self.field = field should be happening in the operator's constructor
* validation of the fields should be done in the ``execute`` method, not in the constructor because in
  the constructor, the field value might be a templated value, not the final value.

The exceptions are cases where we want to assign empty default value to a mutable field (list or dict)
or when we have a more complex structure which we want to convert into a different format (say dict or list)
but where we want to keep the original strings in the converted structure.

In such cases we can usually do something like this

.. code-block:: python

    def __init__(self, *, my_field: list[str] = None, **kwargs):
        super().__init__(**kwargs)
        my_field = my_field or []
        self.my_field = my_field

The reason for doing it is that we are working on a cleaning up our code to have
`prek hook <../scripts/ci/prek/validate_operators_init.py>`__
that will make sure all the cases where logic (such as validation and complex conversion)
is not done in the constructor are detected in PRs.

Don't raise AirflowException directly
..............................................

Our community has decided to stop adding new ``raise AirflowException`` and to adopt the following practices when an exception is necessary. For details check the relevant `mailing list thread <https://lists.apache.org/thread/t8bnhyqy77kq4fk7fj3fmjd5wo9kv6w0>`_.

1. In most cases, we should prioritize using Python's standard exceptions (e.g., ``ValueError``, ``TypeError``, ``OSError``)
   instead of wrapping everything in ``AirflowException``.
2. Within ``airflow-core``, we should define and utilize more specific exception classes under ``airflow-core/src/airflow/exceptions.py``.
3. For provider-specific implementations, exceptions should be defined within ``providers/<provider>/src/airflow/providers/<provider>/exceptions.py``.

The use of points 2 and 3 should only be considered when point 1 is inappropriate, which should be a rare occurrence.

In other words instead of doing:

.. code-block:: python

   if key not in conf:
       raise AirflowException(f"Required key {key} is missing")

you should do:

.. code-block:: python

   if key not in conf:
       raise ValueError(f"Required key {key} is missing")

-----------

If you want to learn what are the options for your development environment, follow to the
`Development environments <06_development_environments.rst>`__ document.
