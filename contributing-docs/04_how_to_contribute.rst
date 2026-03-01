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

How to contribute
=================

There are various ways how you can contribute to Apache Airflow. Here is a short overview of
some of those ways that involve creating issues and pull requests on GitHub.

**The outline for this document in GitHub is available at top-right corner button (with 3-dots and 3 lines).**

Report Bugs
-----------

Report bugs through `GitHub Issues <https://github.com/apache/airflow/issues>`__.

Please report relevant information and preferably code that exhibits the problem.

Report security issues
----------------------

If you want to report a security finding, please follow the
`Security policy <https://github.com/apache/airflow/security/policy>`_

Issue reporting and resolution process
======================================

.. note::
   **TL;DR: Quick Summary**

   * **No Issue Needed:** You can open a PR directly without opening an issue first.
   * **Discussion First:** If you aren't sure about a bug or feature, start a
     `GitHub Discussion <https://github.com/apache/airflow/discussions>`_ instead.
   * **No Assignments:** we do **not** assign issues to non-maintainers or people who maintainer did not
     reach out about the issue. Please do not ask to be assigned; simply comment "working on it" and
     submit your PR.
   * **Parallel Work is fine:** Multiple people can work on the same issue and it's fine while not necessarily
     default. When it happens - we will merge the best implementation, and encourage learning and
     community feedback.

An unusual element of the Apache Airflow project (compared, for example, to commercial
development) is that you can open a PR to fix an issue or make an enhancement without needing
to open an issue first. This is intended to make it as easy as possible to contribute to the
project.

If you're not sure whether it's a bug or a feature, consider starting
with a `GitHub Discussion <https://github.com/apache/airflow/discussions>`_ instead.

If you have a significant topic to discuss where important behaviour of Airflow seems like a bug
or needs an improvement, start a thread on the
`Devlist <https://lists.apache.org/list.html?dev@airflow.apache.org>`_ instead.

The Apache Airflow project uses a set of labels for tracking and triaging issues, as well as
a set of priorities and milestones to track how and when enhancements and bug fixes make it
into an Airflow release. This is documented as part of the
`Issue reporting and resolution process <../ISSUE_TRIAGE_PROCESS.rst>`_.

Contribute code changes
-----------------------

Note that we do not usually assign issues to people. Maintainers only self-assign issues when
they want to ensure they are working on something where they have unique knowledge or a
specific desire to work on a specific part.

Starting **March 2026**, we do not expect people to ask to be assigned to issues, and we will
not assign them even if asked. Contributors are still welcome to work on those issues and comment
that they are "working on it", but we will not formally assign them. We used this system
previously, but found it did not work well in a landscape where:

* Users treat assignment as a "badge".
* Users are unsure if they can follow through on the work.
* Users attempt to use Gen-AI to solve the issue and fail to deliver or lose interest.

Consequently, we do not assign issues to anyone who is not a maintainer, unless a maintainer
knows them personally and has agreed to the work via direct communication - this requires
maintainer to actively reach out to the person before, you should not ask to be assigned.

The "no assignment" policy is not intended to discourage contributors — quite the opposite. We
want to ensure we do not have a backlog of "assigned" issues that are not actually being
addressed. **This often prevents others from working on them if they want.**

Working in parallel on the same issue is fine, but not the default. When this leads to
several PRs fixing the same issue, we will merge the best implementation and close the others,
and we encourage cooperation and learning from each other in the process. This is a positive outcome,
as it allows contributors to:

1. Receive feedback on their implementation.
2. Learn from different perspectives and coding styles.
3. Foster community building.

Any feature or bug request is open to whoever wants to address it, even if someone else has
already commented or submitted a linked PR. If you wish to contribute to an issue that already
has an open PR, you are encouraged to review that PR and help lead it to completion. However,
it is also perfectly fine to submit your own PR if your solution is different. We embrace this
"better PR wins" approach as the most effective way to encourage learning and ensure the best
result for the project.

Fix Bugs
........

Look through the `GitHub issues labeled "kind:bug" <https://github.com/apache/airflow/labels/kind%3Abug>`__
for bugs.

Anything is open to whoever wants to implement it. Easy to fix bugs are usually marked with the
``good first issue`` label, but you can also look for other issues that are not marked as ``good first issue``
- they might be still easy to fix, and they might be a good way
to get started with the project.

Implement Features
..................

Look through the `GitHub issues labeled "kind:feature" <https://github.com/apache/airflow/labels/kind%3Afeature>`__
for features.

Anything is open to whoever wants to implement it. Easy to implement features are usually marked with the
``good first issue`` label, but you can also look for other issues that are not marked as ``good first issue``
- they might be still easy to implement, and they might be a good way to get started with the project.

We've created the operators, hooks, macros and executors we needed, but we've made sure that this part of
Airflow (providers) is easily extensible. New operators, hooks, macros and executors are very welcome!

Improve Documentation
---------------------

Airflow could always use better documentation, whether as part of the official Airflow docs, in docstrings,
``/docs/`` folders in the projects or even on the web as blog posts or articles.

See the `Docs README <https://github.com/apache/airflow/blob/main/docs/README.md>`__ for more information
about contributing to Airflow docs.

Submit Feedback
---------------

The best way to send feedback is to `open an issue on GitHub <https://github.com/apache/airflow/issues/new/choose>`__.

If you are proposing a new feature:

-   Explain in detail how it would work.
-   Keep the scope as narrow as possible to make it easier to implement.
-   Remember that this is a volunteer-driven project, and that contributions are welcome, and sometimes
    the fastest way to get a new feature implemented/bug fixed is to implement it yourself and submit a
    PR for it.

-------------------------------

If you want to know more about creating Pull Requests (PRs), reading pull request guidelines
and learn about coding standards we have, follow to the `Pull Request <05_pull_requests.rst>`_ document.
