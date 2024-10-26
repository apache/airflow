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

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->

- [What should be and should NOT be reported ?](#what-should-be-and-should-not-be-reported-)
- [How to report the issue ?](#how-to-report-the-issue-)
- [Is this really a security vulnerability ?](#is-this-really-a-security-vulnerability-)
- [How do we assess severity of the issue ?](#how-do-we-assess-severity-of-the-issue-)
- [What happens after you report the issue ?](#what-happens-after-you-report-the-issue-)
- [Does CVE in Airflow Providers impact Airflow core package ?](#does-cve-in-airflow-providers-impact-airflow-core-package-)
- [Where do I find more information about Airflow Security ?](#where-do-i-find-more-information-about-airflow-security-)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

This document contains information on how to report security vulnerabilities in Apache Airflow and
how security issues reported to the Apache Airflow security team are handled. If you would like
to learn more, head to the
[Airflow security](https://airflow.apache.org/docs/apache-airflow/stable/security/) documentation.

**⚠️ Please do not file GitHub issues for security vulnerabilities as they are public! ⚠️**

The Apache Software Foundation takes security issues very seriously. Apache
Airflow specifically offers security features and is responsive to issues
around its features. If you have any concern around Airflow Security or believe
you have uncovered a vulnerability, we suggest that you get in touch via the
e-mail address [security@airflow.apache.org](mailto:security@airflow.apache.org).

Before sending the report, however, please read the following guidelines first. The guidelines should
answer the most common questions you might have about reporting vulnerabilities.

### What should be and should NOT be reported ?

**Only** use the security e-mail address to report undisclosed security vulnerabilities in Apache
Airflow and to manage the process of fixing such vulnerabilities. We do not accept regular
bug reports or other security-related queries at this address. We will ignore mail
sent to this address that does not relate to an undisclosed security problem
in the Apache Airflow project. Please follow regular communication channels described in
the [Airflow Community](https://airflow.apache.org/community/index.html) page for inquiries, questions and other discussions related
to the process or issues.

Specifically, we will ignore results of security scans that contain a list of dependencies of Airflow
with dependencies in Airflow Docker reference image - there is a page that describes how the
[Airflow reference Image is fixed at release time](https://airflow.apache.org/docs/docker-stack/index.html#fixing-images-at-release-time) and providing helpful instructions explaining
how you can build your own image and manage dependencies of Airflow in your own image.

### How to report the issue ?

Please send one plain-text email for each vulnerability you are reporting including an explanation
of how it affects Airflow security. We may ask that you resubmit your report if you send it as an image,
movie, HTML, or PDF attachment when you could as easily describe it with plain text.

### Is this really a security vulnerability ?

Before reporting vulnerabilities, please make sure to read and understand the [security model](https://airflow.apache.org/docs/apache-airflow/stable/security/security_model.html)
of Airflow, because some of the potential security vulnerabilities that are valid for projects that are
publicly accessible from the Internet, are not valid for Airflow.

Airflow is not designed to be used by untrusted users, and some trusted users are trusted enough to do a
variety of operations that could be considered as vulnerabilities in other products/circumstances.


Again. Please make sure to read and understand the [security model](https://airflow.apache.org/docs/apache-airflow/stable/security/security_model.html)
of Airflow - this might help to save your time for reporting and our time for assessing the issues when
they are clearly expected, according to the Security model. You also avoid disappointment when you spend
a lot of time on preparing the issue report to follow the guidelines above and what you get as a response is
"this is not a security issue" or "this is invalid security issue" or "this is expected behaviour". You
save time for yourself and for the Airflow Security team by reading and understanding the security model
before reporting the issue.

### How do we assess severity of the issue ?

Severity of the issue is determined based on the criteria described in
the [Severity Rating blog post](https://security.apache.org/blog/severityrating/) by the Apache Software Foundation Security team.

Due to reasons explained in the previous chapter, some potential security vulnerabilities
do not apply to Airflow, or have a different severity than some generic scoring systems
(for example `CVSS`) calculation suggests. So we are not using any generic scoring system.


### What happens after you report the issue ?

The Airflow Security Team will get back to you after assessing the report. You will usually get
confirmation that the issue is being worked (or that we quickly assessed it as invalid) within several
business days. Note that this is an Open-Source projects and members of the security team are volunteers,
so please make sure to be patient. If you do not get a response within a week, please send a kind reminder
to the security team about a lack of response; however, reminders should only be for the initial response
and not for updates on the assessment or remediation. We will usually let you know the CVE number that will
be assigned to the issue and the severity of the issue as well as release the issue is scheduled to be fixed
after we assess the issue (which might take longer or shorter time depending on the issue complexity and
potential impact, severity, whether we want to address a whole class issues in a single fix and a number
of other factors). You should subscribe  and monitor the `users@airflow.apache.org` mailing
list or `announce@apache.org` to see the announcement of the security issue - including the CVE number
and the severity of the issue once the issue is fixed and release is public. Note that the
`announce@apache.org` mailing list is moderated, and it might take a few hours for the announcement to
appear there, so `users@airflow.apache.org` is the best place to monitor for the announcement.

Security issues in Airflow are handled by the Airflow Security Team. Details about the Airflow Security
Team and how members of it are chosen can be found in the
[Contributing documentation](https://github.com/apache/airflow/blob/main/contributing-docs/01_roles_in_airflow_project.rst#security-team).

### Does CVE in Airflow Providers impact Airflow core package ?

Airflow core package is released separately from provider packages. While Airflow comes with ``constraints``
which describe which version of providers have been tested when the version of Airflow was released, the
users of Airflow are advised to install providers independently of Airflow core when they want to apply
security fixes found and released in providers. Therefore, the issues found and fixed in providers do
not apply to the Airflow core package. There are also Airflow providers released by 3rd-parties, but the
Airflow community is not responsible for releasing and announcing security vulnerabilities in them, this
is handled entirely by the 3rd-parties that release their own providers.

### Where do I find more information about Airflow Security ?

If you wish to know more about the ASF security process,
the [ASF Security team's page](https://www.apache.org/security/) describes
how vulnerability reports are handled in general by all ASF projects, and includes PGP keys if
you wish to use them when you report the issues.
