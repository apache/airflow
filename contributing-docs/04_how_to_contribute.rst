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

Report bugs through `GitHub <https://github.com/apache/airflow/issues>`__.

Please report relevant information and preferably code that exhibits the problem.

Report security issues
----------------------

If you want to report a security finding, please follow the
`Security policy <https://github.com/apache/airflow/security/policy>`_


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
the `Issue reporting and resolution process <../ISSUE_TRIAGE_PROCESS.rst>`_,

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

-------------------------------

If you want to know more about creating Pull Requests (PRs), reading pull request guidelines
and learn about coding standards we have, follow to the `Pull Request <05_pull_requests.rst>`_ document.
