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

Roles in Airflow project
========================

There are several roles within the Airflow Open-Source community.

For detailed information for each role, see: `Committers and PMC members <../COMMITTERS.rst>`__.

**The outline for this document in GitHub is available at top-right corner button (with 3-dots and 3 lines).**

PMC Member
----------

The PMC (Project Management Committee) is a group of maintainers that drives changes in the way that
Airflow is managed as a project.

Considering Apache, the role of the PMC member is primarily to ensure that Airflow conforms to Apache's processes
and guidelines.

Committers/Maintainers
----------------------

You will often see the term "committer" or "maintainer" in the context of the Airflow project. This is a person
who has write access to the Airflow repository and can merge pull requests. Committers (also known as maintainers)
are also responsible for reviewing pull requests and guiding contributors to make their first contribution.
They are also responsible for making sure that the project is moving forward and that the quality of the
code is maintained.

The term "committer" and "maintainer" is used interchangeably. The term "committer" is the official term used by the
Apache Software Foundation, while "maintainer" is more commonly used in the Open Source community and is used
in context of GitHub in a number of guidelines and documentation, so this document will mostly use "maintainer",
when speaking about Github, Pull Request, Github Issues and Discussions. On the other hand, "committer" is more
often used in devlist discussions, official communications, Airflow website and every time when we formally
refer to the role.

The official list of committers can be found `here <https://airflow.apache.org/docs/apache-airflow/stable/project.html#committers>`__.

Additionally, committers are listed in a few other places (some of these may only be visible to existing committers):

* https://whimsy.apache.org/roster/committee/airflow
* https://github.com/orgs/apache/teams/airflow-committers/members

Committers are responsible for:

* Championing one or more items on the `Roadmap <https://cwiki.apache.org/confluence/display/AIRFLOW/Airflow+Home>`__
* Reviewing & Merging Pull-Requests
* Scanning and responding to GitHub issues
* Responding to questions on the dev mailing list (dev@airflow.apache.org)

Release managers
----------------

The task of release managers is to prepare and release Airflow artifacts (airflow, providers, Helm Chart, Python client.
The release managers are usually PMC members and the process of releasing is described in the `dev <dev>`__
documentation where we keep information and tools used for releasing.

Contributors
------------

A contributor is anyone who wants to contribute code, documentation, tests, ideas, or anything to the
Apache Airflow project.

Contributors are responsible for:

* Fixing bugs
* Adding features
* Championing one or more items on the `Roadmap <https://cwiki.apache.org/confluence/display/AIRFLOW/Airflow+Home>`__.

Security Team
-------------

Security issues in Airflow are handled by the Airflow Security Team. The team consists
of selected PMC members that are interested in looking at, discussing and fixing
security issues, but it can also include committers and non-committer contributors that are
not PMC members yet and have been approved by the PMC members in a vote. You can request to
be added to the team by sending a message to private@airflow.apache.org. However, the team
should be small and focused on solving security issues, so the requests will be evaluated
on a case-by-case basis and the team size will be kept relatively small, limited to only actively
security-focused contributors.

There are certain expectations from the members of the security team:

* They are supposed to be active in assessing, discussing, fixing and releasing the
  security issues in Airflow. While it is perfectly understood that as volunteers, we might have
  periods of lower activity, prolonged lack of activity and participation will result in removal
  from the team, pending PMC decision (the decision on removal can be taken by `LAZY CONSENSUS <https://community.apache.org/committers/lazyConsensus.html>`_ among
  all the PMC members on private@airflow.apache.org mailing list).

* They are not supposed to reveal the information about pending and unfixed security issues to anyone
  (including their employers) unless specifically authorized by the security team members, specifically
  if diagnosing and solving the issue might involve the need of external experts - for example security
  experts that are available through Airflow stakeholders. The intent about involving 3rd parties has
  to be discussed and agreed upon at security@airflow.apache.org.

* They have to have an `ICLA <https://www.apache.org/licenses/contributor-agreements.html>`_ signed with
  Apache Software Foundation.

* The security team members might inform 3rd parties about fixes, for example in order to assess if the fix
  is solving the problem or in order to assess its applicability to be applied by 3rd parties, as soon
  as a PR solving the issue is opened in the public airflow repository.

* In case of critical security issues, the members of the security team might iterate on a fix in a
  private repository and only open the PR in the public repository once the fix is ready to be released,
  with the intent of minimizing the time between the fix being available and the fix being released. In this
  case the PR might be sent to review and comment to the PMC members on private list, in order to request
  an expedited voting on the release. The voting for such release might be done on the
  private@airflow.apache.org mailing list and should be made public at the dev@apache.airflow.org
  mailing list as soon as the release is ready to be announced.

* The security team members working on the fix might be mentioned as remediation developers in the CVE
  including their job affiliation if they want to.

* Community members acting as release managers are by default members of the security team and unless they
  want to, they do not have to be involved in discussing and solving the issues. They are responsible for
  releasing the CVE information (announcement and publishing to security indexes) as part of the
  release process. This is facilitated by the security tool provided by the Apache Software Foundation.

* Severity of the issue is determined based on the criteria described in the
  `Severity Rating blog post <https://security.apache.org/blog/severityrating/>`_  by the Apache Software
  Foundation Security team.

Handling security issues is something of a chore, it takes vigilance, requires quick reaction and responses
and often requires to act outside of the regular "day" job. This means that not everyone can keep up with
being part of the security team for long while being engaged and active. While we do not expect all the
security team members to be active all the time, and - since we are volunteers, it's perfectly understandable
that work, personal life, family and generally life might not help with being active. And this is not a
considered as being failure, it's more stating the fact of life.

Also prolonged time of being exposed to handling "other's" problems and discussing similar kinds of problem
and responses might be tiring and might lead to burnout.

However, for those who have never done that before, participation in the security team might be an interesting
experience and a way to learn a lot about security and security issue handling. We have a lot of
established processes and tools that make the work of the security team members easier, so this can be
treated as a great learning experience for some community members. And knowing that this is not
a "lifetime" assignment, but rather a temporary engagement might make it easier for people to decide to
join the security team.

That's why we've introduced rotation of the security team members.

Periodically - every 3-4 months (depending on actual churn of the security issues that are reported to us),
we re-evaluate the engagement and activity of the security team members, and we ask them if they want to
continue being part of the security team, taking into account their engagement since the last team refinement.
Generally speaking if the engagement during the last period was marginal, the person is considered as a
candidate for removing from the team and it requires a deliberate confirmation of re-engagement to take
the person off-the-list.

At the same time we open up the possibility to other people in the community to join the team and make
a "call for new security team members" where community members can volunteer to join the security team.
Such volunteering should happen on the private@ list. The current members of the security team as well
as PMC members can also nominate other community members to join the team and those new team members
have to be well recognized and trusted by the community and accepted by the PMC.

The proposal of team refinement is passed to the PMC as LAZY CONSENSUS (or VOTE if consensus cannot
be reached). In case the consensus cannot be reached for the whole list, we can split it and ask for
lazy consensus for each person separately.

-------------

You can follow this with the `How to communicate <02_how_to_communicate.rst>`__ document to learn more how
to communicate with the Airflow community members.
