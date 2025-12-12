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

Issue reporting and resolution process
======================================

This document explains the issue tracking and triage process within Apache
Airflow including labels, milestones, and priorities as well as the process
of resolving issues.

An unusual element of the Apache Airflow project is that you can open a PR
to fix an issue or make an enhancement, without needing to open an issue first.
This is intended to make it as easy as possible to contribute to the project.

Usually, users report `Issues <https://github.com/apache/airflow/issues>`_ where they describe
the issues they think are Airflow issues and should be solved. There are two kinds of issues:

* Bugs - when the user thinks the reported issue is a bug in Airflow
* Features - when there are small features that the user would like to see in Airflow

We have `templates <https://github.com/apache/airflow/tree/main/.github/ISSUE_TEMPLATE>`_ for both types
of issues defined in Airflow.

However, important part of our issue reporting process are
`GitHub Discussions <https://github.com/apache/airflow/discussions>`_ . Issues should represent
clear, small feature requests or reproducible bugs which can/should be either implemented or fixed.
Users are encouraged to open discussions rather than issues if there are no clear, reproducible
steps, or when they have troubleshooting problems, and one of the important points of issue triaging is
to determine if the issue reported should be rather a discussion. Converting an issue to a discussion
while explaining the user why is an important part of triaging process.

Responding to issues/discussions (relatively) quickly
'''''''''''''''''''''''''''''''''''''''''''''''''''''

It is vital to provide rather quick feedback to issues and discussions opened by our users, so that they
feel listened to rather than ignored. Even if the response is "we are not going to work on it because ...",
or "converting this issue to discussion because ..." or "closing because it is a duplicate of #xxx", it is
far more welcoming than leaving issues and discussions unanswered. Sometimes issues and discussions are
answered by other users (and this is cool) but if an issue/discussion is not responded to for a few days or
weeks, this gives an impression that the user was ignored and that the Airflow project is unwelcoming.

We strive to provide relatively quick responses to all such issues and discussions. Users should exercise
patience while waiting for those (knowing that people might be busy, on vacations etc.) however they should
not wait weeks until someone looks at their issues.


Issue Triage Team
''''''''''''''''''

While many of the issues can be responded to by other users and committers, the committer team is not
big enough to handle all such requests and sometimes they are busy with implementing or resolving more important issues.
Therefore, some people who are regularly contributing and helping other users and shown their deep interest
in the project can be invited to join the triage team.
`the .asf.yaml <.asf.yaml>`_ file in the ``collaborators`` section.


The triage team members do not have committer privileges but they can assign, edit, and close issues and pull requests without having capabilities to merge the code. They can
also convert issues into discussions and back. The expectation for the issue triage team is that they
spend a bit of their time on those efforts. Triaging means not only assigning the labels but often responding
to the issues and answering user concerns or if additional input is needed - tagging the committers or other community members who might be able to help provide more complete answers.

Being an active and helpful member of the "Issue Triage Team" is actually one of the paths towards
becoming a committer. By actively helping the users, triaging the issues, responding to them and
involving others (when needed) shows that you are not only willing to help our users and the community,
but are also ready to learn about parts of the projects you are not actively contributing to - all of that
are super valuable components of being eligible to `become a committer <COMMITTERS.rst>`_.

If you are a member of the triage team and not able to make any commitment, it's best to ask to have yourself
removed from the triage team.

Every committer is pretty much automatically part of the "Issue Triage Team" - so if you are committer,
feel free to follow the process for every issue you stumble upon.

Joining Issue Triage Team
'''''''''''''''''''''''''

Joining the team is only by invitation from PMC member.
A [LAZY CONSENSUS] vote must be passed in the PMC private list prior to invitation.
The thread should present data points for candidate already helping and responding to issues, other activities in the project may also be considered.
Activity in areas/project that is external to Apache-Airflow open source will not be considered.
Note: the numbers of people in the triage team is limited by ASF. See `list <https://github.com/apache/airflow/blob/main/.asf.yaml#L143>`_

An exception to the above rule would be re-adding previous member of the team.
In that case adding may be done directly by PMC member without the need for [LAZY CONSENSUS].
The PR to add should clarify that the person is a previous member and quote the original PR that added.

Removal from Issue Triage Team
''''''''''''''''''''''''''''''

Inactive members will be removed without notice.
Due to the members limit we and the number of issues in the project we are looking only for active members.
Note that removal from the team doesn't have any negative impact on you. It's very OK if you moved towards other kind of contributions.
Removal from the team is not permanent. See above paragraph of Joining the team for clarification about the protocol.

Actions that can be taken by the issue triager
''''''''''''''''''''''''''''''''''''''''''''''

There are several actions an issue triager might take:

* Closing and issue with "invalid" label explaining why it is closed in case the issue is invalid. This
  should be accompanied by information that we can always re-open an issue if our understanding was wrong
  or if the user provides more information.

* Converting an issue to a discussion, if it is not very likely it is an Airflow issue or when it is not
  responsible, or when it is a bigger feature proposal requiring discussion or when it's really users
  troubleshooting or when the issue description is not at all clear. This also involves inviting the user
  to a discussion if more information might change it.

* Assigning the issue to a milestone, if the issue seems important enough that it should likely be looked
  at before the next release but there is not enough information or doubts on why and what can be fixed.
  Usually we assign to the the next bugfix release - then, no matter what the issue will be looked at
  by the release manager and it might trigger additional actions during the release preparation.
  This is usually followed by one of the actions below.

* Fixing the issue in a PR if you see it is easy to fix. This is a great way also to learn and
  contribute to parts that you usually are not contributing to, and sometimes it is surprisingly easy.

* Assigning "good first issue" label if an issue is clear but not important to be fixed immediately, This
  often lead to contributors picking up the issues when they are interested. This can be followed by assigning
  the user who comments "I want to work on this" in the issue (which is most welcome).

* Asking the user for additional information if it is needed to perform further investigations. This should
  be accompanied by assigning ``pending response`` label so that we can clearly see the issues that need
  extra information.

* Calling other people who might be knowledgeable in the area by @-mentioning them in a comment.

* Assigning other labels to the issue as described below.


Labels
''''''

Since Apache Airflow uses "GitHub Issues" and "GitHub Discussions" as the
issue tracking systems, the use of labels is extensive. Though issue
labels tend to change over time based on components within the project,
the majority of the ones listed below should stand the test of time.

The intention with the use of labels with the Apache Airflow project is
that they should ideally be non-temporal in nature and primarily used
to indicate the following elements:

**Kind**

The "kind" labels indicate "what kind of issue it is". The most
commonly used "kind" labels are: bug, feature, documentation, or task.

Therefore, when reporting an issue, the label of ``kind:bug`` is to
indicate a problem with the functionality, whereas the label of
``kind:feature`` is a desire to extend the functionality.

There has been discussion within the project about whether to separate
the desire for "new features" from "enhancements to existing features",
but in practice most "feature requests" are actually enhancement requests,
so we decided to combine them both into ``kind:feature``.

The ``kind:task`` is used to categorize issues which are
identified elements of work to be done, primarily as part of a larger
change to be done as part of an AIP or something which needs to be cleaned
up in the project.

Issues of ``kind:documentation`` are for changes which need to be
made to the documentation within the project.


**Area**

The "area" set of labels should indicate the component of the code
referenced by the issue. At a high level, the biggest areas of the project
are: Airflow Core and Airflow Providers, which are referenced by ``area:core``
and ``area:providers``. This is especially important since these are now
being released and versioned independently.

There are more detailed areas of the Core Airflow project such as Scheduler, API Server,
API, UI, Logging, and Metrics, which are all conceptually under the
"Airflow Core" area of the project.

Similarly within Airflow Providers, the larger providers such as Apache, AWS, Azure,
and Google who have many hooks and operators within them, have labels directly
associated with them such as ``provider:amazon-aws``, ``provider:microsoft-azure``, and ``provider:google``.

These make it easier for developers working on a single provider to
track issues for that provider.

Note: each provider has its own unique label. It is possible for issue to be tagged with more than 1 provider label.

Most issues need a combination of "kind" and "area" labels to be actionable.
For example:

* Feature request for an additional API would have ``kind:feature`` and ``area:API``
* Bug report on the User Interface would have ``kind:bug`` and ``area:UI``
* Documentation request on the Kubernetes Executor, would have ``kind:documentation`` and ``area:kubernetes``

Response to issues
''''''''''''''''''

Once an issue has been created on the Airflow project, someone from the
Airflow team or the Airflow community typically responds to this issue.
This response can have multiple elements.

**Priority**

After significant discussion about the different priority schemes currently
being used across various projects, we decided to use a priority scheme based
on the Kubernetes project, since the team felt it was easier for people to
understand.

Therefore, the priority labels used are:

* ``priority:critical``: Showstopper bug that should be resolved immediately and a patch issued as soon as possible. Typically, this is because it affects most users and would take down production systems.
* ``priority:high``: A high priority bug that affects many users and should be resolved quickly, but can wait for the next scheduled patch release.
* ``priority:medium``: A bug that should be fixed before the next release, but would not block a release if found during the release process.
* ``priority:low``: A bug with a simple workaround or a nuisance that does not stop mainstream functionality.


It's important to use priority labels effectively so we can triage incoming issues
appropriately and make sure that when we release a new version of Airflow,
we can ship a release confident that there are no "production blocker" issues in it.

This applies to both Core Airflow as well as the Airflow Providers. With the separation
of the Providers release from Core Airflow, a ``priority:critical`` bug in a single
provider could trigger an unplanned patch release of the Airflow Providers.


**Milestones**

The key temporal element in the issue triage process is the concept of milestones.
This is critical for release management purposes and will be used represent upcoming
release targets.

Issues currently being resolved will get assigned to one of the upcoming releases.
For example a feature request may be targeted for the next feature release milestone
such as ``2.x``, where a bug may be targeted for the next patch release milestone
such as ``2.x.y``.

In the interest of being precise, when an issue is tagged with a milestone, it
represents that it will be considered for that release, not that it is committed to
a release. Once a PR is created to fix that issue and when that PR is tagged with a
milestone, it implies that the PR is intended to released in that milestone.

Please note that Airflow Core and Airflow Providers are now released and
versioned separately. The use of milestones as described above is directed towards
Airflow Core releases.


**Transient Labels**

Sometimes, there is more information needed to either understand the issue or
to be able to reproduce the issue. Typically, this may require a response to the
issue creator asking for more information, with the issue then being tagged with
the label ``pending-response``.
Also, during this stage, additional labels may be added to the issue to help
classification and triage, such as ``affected_version`` and ``area``.

New issues are automatically assigned with ``needs-triage`` label. This labels goal
is to help us detect issues that are waiting for initial triage. The label will be removed by the triager
once the issue is accepted (and assigned with relevant kind and area labels). This sometimes can take a while as we might
ask for other members of the community for consultation or ask for further information from the issue author.
Removing the ``needs-triage`` label means that the issue has been accepted and awaits implementation (no further triage action required),
as long as the ``needs-triage`` label remains the triage team will keep an eye on the issue and check periodically
if it needs to be accepted or closed/converted to GitHub Discussion.
``needs-triage`` label may also be applied manually by committers if they think a further action from the triage team is required.

**area Label**

``area:providers`` must be present for any provider issue. The ``provider:x`` is specifying the exact provider.
We have ``provider:x`` for any provider that we own.
``area:helm-chart`` must be for any helm chart issue.
``area:core`` must be for any core issue. Additional labels like ``area:scheduler``, ``area:UI`` is specifying the exact
core area relevant.

This method allow us to quickly filter issues by the 3 major components of our code base: core, providers and helm-chart.

**affected version Label**

The ``affected_version:x`` will never be present with ``kind:feature`` as feature requests are not related to specific Airflow version.
For bugs, The ``affected_version:x`` is expected to be used with core issues thus normally it appears with ``area:core`` label.
When issue is reproducible on multiple Airflow versions we apply only the latest version with the label.
This policy is best effort, we should try to have as little exceptions as possible.

**Good First Issue**

Issues which are relatively straight forward to solve, will be tagged with
the ``good first issue`` label.

The intention here is to galvanize contributions from new and inexperienced
contributors who are looking to contribute to the project. This has been successful
in other open source projects and early signs are that this has been helpful in the
Airflow project as well.

Ideally, these issues only require one or two files to be changed. The intention
here is that incremental changes to existing files are a lot easier for a new
contributor as compared to adding something completely new.

Another possibility here is to add "how to fix" in the comments of such issues, so
that new contributors have a running start when then pick up these issues.


**Timeliness**

For the sake of quick responses, the general "soft" rule within the Airflow project
is that if there is no assignee, anyone can take an issue to solve.

However, this depends on timely resolution of the issue by the assignee. The
expectation is as follows:

* If there is no activity on the issue for 2 weeks, the assignee will be reminded about the issue and asked if they are still working on it.
* If there is no activity even after 1 more week, the issue will be unassigned, so that someone else can pick it up and work on it.


There is a similar process when additional information is requested from the
issue creator. After the pending-response label has been assigned, if there is no
further information for a period of 1 month, the issue will be automatically closed.


**Invalidity**

At times issues are marked as invalid and later closed because of one of the
following situations:

* The issue is a duplicate of an already reported issue. In such cases, the latter issue is marked as ``duplicate``.
* Despite attempts to reproduce the issue to resolve it, the issue cannot be reproduced by the Airflow team based on the given information. In such cases, the issue is marked as ``Can't Reproduce``.
* In some cases, the original creator realizes that the issue was incorrectly reported and then marks it as ``invalid``. Also, a committer could mark it as ``invalid`` if the issue being reported is for an unsupported operation or environment.
* In some cases, the issue may be legitimate, but may not be addressed in the short to medium term based on current project priorities or because this will be irrelevant because of an upcoming change. The committer could mark this as ``wontfix`` to set expectations that it won't be directly addressed in the near term.

**GitHub Discussions**

Issues should represent clear feature requests which can/should be implemented. If the idea is vague or can be solved with easier steps
we normally convert such issues to discussions in the Ideas category.
Issues that seems more like support requests are also converted to discussions in the Q&A category.
We use judgment about which Issues to convert to discussions, it's best to always clarify with a comment why the issue is being converted.
Note that we can always convert discussions back to issues.


**Stale Policy**

As time passes bug reports that have been accepted may be out dated.
Bot will scan older bug reports and if the report is inactive it will comment
and ask the author to recheck if the bug is still reproducible on latest version.
If the issue is reconfirmed triage team will check if labels needs to be updated (for example: ``reported_version`` label)
If no one respond after some time, we will consider the issue as resolved (may have already been fixed) and bot will resolve the issue.
The exact timeframes for each one of the actions is subject to change from time to time.
The updated values can be checked in ``.github/workflow`` where we define the bots policy.
