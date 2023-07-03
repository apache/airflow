<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
-->

This document contains information on how to report security vulnerabilities in Apache Airflow and
how the security issues reported to Apache Airflow security team are handled. If you would like
to learn about the security model of Airflow head to
[Airflow Security](https://airflow.apache.org/docs/apache-airflow/stable/security/)

## Reporting Vulnerabilities

**⚠️ Please do not file GitHub issues for security vulnerabilities as they are public! ⚠️**

The Apache Software Foundation takes security issues very seriously. Apache
Airflow specifically offers security features and is responsive to issues
around its features. If you have any concern around Airflow Security or believe
you have uncovered a vulnerability, we suggest that you get in touch via the
e-mail address [security@airflow.apache.org](mailto:security@airflow.apache.org).
In the message, try to provide a description of the issue and ideally a way of
reproducing it. The security team will get back to you after assessing the report.

Note that this security address should be used only for undisclosed
vulnerabilities. Dealing with fixed issues or general questions on how to use
the security features should be handled regularly via the user and the dev
lists. Please report any security problems to the project security address
before disclosing it publicly.

Before reporting vulnerabilities, please make sure to read and understand the
[security model](https://airflow.apache.org/docs/apache-airflow/stable/security/) of Airflow, because
some of the potential security vulnerabilities that are valid for projects that are publicly accessible
from the Internet, are not valid for Airflow. Airflow is not designed to be used by untrusted users, and some
trusted users are trusted enough to do a variety of operations that could be considered as vulnerabilities
in other products/circumstances. Therefore, some potential security vulnerabilities do not
apply to Airflow, or have a different severity than some generic scoring systems (for example `CVSS`)
calculation suggests.

The [ASF Security team's page](https://www.apache.org/security/) describes
how vulnerability reports are handled in general by all ASF projects, and includes PGP keys if
you wish to use them when you report the issues.

## Security vulnerabilities in Airflow and Airflow community managed providers

Airflow core package is released separately from provider packages. While Airflow comes with ``constraints``
which describe which version of providers have been tested when the version of Airflow was released, the
users of Airflow are advised to install providers independently from Airflow core when they want to apply
security fixes found and released in providers. Therefore, the issues found and fixed in providers do
not apply to the Airflow core package. There are also Airflow providers released by 3rd-parties, but the
Airflow community is not responsible for releasing and announcing security vulnerabilities in them, this
is handled entirely by the 3rd-parties that release their own providers.

## Handling security issues in Airflow

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

* They have to have an [ICLA](https://www.apache.org/licenses/contributor-agreements.html) signed with
  Apache Software Foundation.

* The security team members might inform 3rd parties about fixes, for example in order to assess if the fix
  is solving the problem or in order to assess its applicability to be applied by 3rd parties, as soon
  as a PR solving the issue is opened in the public airflow repository.

* In case of critical security issues, the members of the security team might iterate on a fix in a
  private repository and only open the PR in the public repository once the fix is ready to be released,
  with the intent of minimizing the time between the fix being available and the fix being released. In this
  case the PR might be sent to review and comment to the PMC members on private list, in order to request
  an expedited voting on the release. The voting for such release might be done on the
  `private@airflow.apache.org` mailing list and should be made public at the `dev@apache.airflow.org`
  mailing list as soon as the release is ready to be announced.

* The security team members working on the fix might be mentioned as remediation developers in the CVE
  including their job affiliation if they want to.

* Community members acting as release managers are by default members of the security team and unless they
  want to, they do not have to be involved in discussing and solving the issues. They are responsible for
  releasing the CVE information (announcement and publishing to security indexes) as part of the
  release process. This is facilitated by the security tool provided by the Apache Software Foundation.

* Severity of the issue is determined based on the criteria described in the
  [Severity Rating blog post](https://security.apache.org/blog/severityrating/)  by the Apache Software
  Foundation Security team
