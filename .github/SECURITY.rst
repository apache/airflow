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

Security Model
--------------

In the Airflow security model, the system administrators are fully trusted.
They are the only ones who can upload new DAGs, which gives them the ability
to execute any code on the server.

Authenticated web interface and API users with Admin/Op permissions are trusted,
but to a lesser extent: they can configure the DAGs which gives them some control,
but not arbitrary code execution.

Authenticated Web interface and API users with 'regular' permissions are trusted
to the point where they can impact resource consumption and pause/unpause configured DAGs,
but not otherwise influence their functionality.

Reporting Vulnerabilities
-------------------------

**⚠️ Please do not file GitHub issues for security vulnerabilities as they are public! ⚠️**

The Apache Software Foundation takes security issues very seriously. Apache
Airflow specifically offers security features and is responsive to issues
around its features. If you have any concern around Airflow Security or believe
you have uncovered a vulnerability, we suggest that you get in touch via the
e-mail address security@airflow.apache.org. In the message, try to provide a
description of the issue and ideally a way of reproducing it. The security team
will get back to you after assessing the description.

Note that this security address should be used only for undisclosed
vulnerabilities. Dealing with fixed issues or general questions on how to use
the security features should be handled regularly via the user and the dev
lists. Please report any security problems to the project security address
before disclosing it publicly.

The `ASF Security team's page <https://www.apache.org/security/>`_ describes
how vulnerability reports are handled, and includes PGP keys if you wish to use
that.


Handling security issues in Airflow
-----------------------------------

The security issues in Airflow are handled by the Airflow Security Team. The team consists
of selected PMC members that are interested in looking at, discussing about and fixing the
security issues, but it can also include committers and non-committer contributors that are
not PMC members yet and have been approved by the PMC members in a vote. You can request to
be added to the team by sending a message to private@airflow.apache.org. However, the team
should be small and focused on solving security issues, so the requests will be evaluated
on-case-by-case and the team size will be kept relatively small, limited to only actively
security-focused contributors.

There are certain expectations from the members of the security team:

* They are supposed to be active in assessing, discussing, fixing and releasing the
  security issues in Airflow. While it is perfectly understood that as volunteers, we might have
  periods of lower activity, prolonged lack of activity and participation will result in removal
  from the team, pending PMC decision (the decision on removal can be taken by LAZY CONSENSUS among
  all the PMC members on private@airflow.apache.org mailing list).

* They are not supposed to reveal the information about pending and unfixed security issues to anyone
  (including their employers) unless specifically authorised by the security team members, specifically
  if diagnosing and solving the issue might involve the need of external experts - for example security
  experts that are available through Airflow stakeholders. The intent about involving 3rd parties has
  to be discussed and agreed up at security@airflow.apache.org.

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
  ``private@airflow.apache.org`` mailing list and should be made public at the ``dev@apache.airflow.org``
  mailing list as soon as the release is ready to be announced.

* The security team members working on the fix might be mentioned as remediation developers in the CVE
  including their job affiliation if they want to.

* Community members acting as release managers are by default members of the security team and unless they
  want to, they do not have to be involved in discussing and solving the issues. They are responsible for
  releasing the CVE information (announcement and publishing to security indexes) as part of the
  release process. This is facilitated by the security tool provided by the Apache Software Foundation.

Releasing Airflow with security patches
---------------------------------------

Apache Airflow uses strict `SemVer <https://semver.org>`_ versioning policy, which means that we strive for
any release of a given ``MAJOR`` Version (version "2" currently) to be backwards compatible. When we
release ``MINOR`` version, the development continues in the ``main`` branch where we prepare the next
``MINOR`` version, but we release ``PATCHLEVEL`` releases with selected bugfixes (including security
bugfixes) cherry-picked to the latest released ``MINOR`` line of Apache Airflow. At the moment, when we
release a new ``MINOR`` version, we stop releasing ``PATCHLEVEL`` releases for the previous ``MINOR`` version.

For example, when we released  ``2.6.0`` version on April 30, 2023, until we release ``2.7.0`` version,
all the security patches will be cherry-picked and released in ``2.6.*`` versions only. There will be no
``2.5.*`` versions  released after ``2.6.0`` has been released.

This means that in order to apply security fixes with Apache Airflow software released by us, you
MUST upgrade to the latest ``MINOR`` version of Airflow.
