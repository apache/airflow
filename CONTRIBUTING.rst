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

Contributions primer
====================

.. contents:: :local:

Contributions are welcome and are greatly appreciated! Every little bit helps, and credit will always be given.

This document explains general contribution rules and links to detailed documentation that aim to explain
the subject of contributions if you have not contributed to any Open Source project, but it will also help
people who have contributed to other projects learn about the rules of that community.

You will find all information about contributing to Airflow in `Detailed contributing documentation <./contribution-docs/README.rst>`__.

New Contributor
---------------
If you are a new contributor, please follow the `Contributors Quick Start <./contribution-docs/contributors_quick_start.rst>`__
guide to get a gentle step-by-step introduction to setting up the development environment and making your
first contribution.

Get Mentoring Support
---------------------

If you are new to the project, you might need some help in understanding how the dynamics
of the community works and you might need to get some mentorship from other members of the
community - mostly Airflow committers (maintainers). Mentoring new members of the community is part of
maintainers job so do not be afraid of asking them to help you. You can do it
via comments in your PR, asking on a devlist or via Slack. For your convenience,
we have a dedicated ``#development-first-pr-support`` Slack channel where you can ask any questions
about making your first Pull Request (PR) contribution to the Airflow codebase - it's a safe space
where it is expected that people asking questions do not know a lot Airflow (yet!).
If you need help with Airflow see the Slack channel #troubleshooting.

To check on how mentoring works for the projects under Apache Software Foundation's
`Apache Community Development - Mentoring <https://community.apache.org/mentoring/>`_.

Commit Policy
-------------

The following commit policy passed by a vote 8(binding) FOR to 0 against on May 27, 2016 on the dev list
and slightly modified and consensus reached in October 2020:

* Commits need a +1 vote from a committer who is not the author
* Do not merge a PR that regresses linting or does not pass CI tests (unless we have
  justification such as clearly transient error).
* When we do AIP voting, both PMC and committer +1s are considered as binding vote.

Resources & Links
-----------------

- `Airflow's official documentation <https://airflow.apache.org/>`__
