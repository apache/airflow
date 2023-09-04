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
how security issues reported to the Apache Airflow security team are handled. If you would like
to learn more, head to the
[Airflow security](https://airflow.apache.org/docs/apache-airflow/stable/security/) documentation.

## Reporting Vulnerabilities

**⚠️ Please do not file GitHub issues for security vulnerabilities as they are public! ⚠️**

The Apache Software Foundation takes security issues very seriously. Apache
Airflow specifically offers security features and is responsive to issues
around its features. If you have any concern around Airflow Security or believe
you have uncovered a vulnerability, we suggest that you get in touch via the
e-mail address [security@airflow.apache.org](mailto:security@airflow.apache.org).

**Only** use the security e-mail address to report undisclosed security vulnerabilities in Apache
Airflow and to manage the process of fixing such vulnerabilities. We do not accept regular
bug reports or other security-related queries at this address. We will ignore mail
sent to this address that does not relate to an undisclosed security problem
in the Apache Airflow project. Please follow regular communication channels described in
the [Airflow Community](https://airflow.apache.org/community/index.html) page for
inquiries, questions and other discussions related to the process or issues.

Specifically, we will ignore results of security scans that contain a list of
dependencies of Airflow with dependencies in Airflow Docker reference image - there
is a page that describes how the
[Airflow reference Image is fixed at release time](https://airflow.apache.org/docs/docker-stack/index.html#fixing-images-at-release-time)
and providing helpful instructions explaining how you can build your
own image and manage dependencies of Airflow in your own image.

Please send one plain-text email for each vulnerability you are reporting including an explanation
of how it affects Airflow security. We may ask that you resubmit your report if you send it as an image,
movie, HTML, or PDF attachment when you could as easily describe it with plain text.

Before reporting vulnerabilities, please make sure to read and understand the
[security model](https://airflow.apache.org/docs/apache-airflow/stable/security/security_model.html) of Airflow, because
some of the potential security vulnerabilities that are valid for projects that are publicly accessible
from the Internet, are not valid for Airflow.


Airflow is not designed to be used by untrusted users, and some trusted users are trusted enough to do a
variety of operations that could be considered as vulnerabilities in other products/circumstances.
Therefore, some potential security vulnerabilities do not apply to Airflow, or have a different severity
than some generic scoring systems (for example `CVSS`) calculation suggests. Severity of the issue is
determined based on the criteria described in the
[Severity Rating blog post](https://security.apache.org/blog/severityrating/) by the Apache Software
Foundation Security team.

The [Airflow Security Team](https://github.com/apache/airflow/blob/main/CONTRIBUTING.rst#security-team) will get back to you after assessing the report.

If you wish to know more about the ASF security process, the
[ASF Security team's page](https://www.apache.org/security/) describes
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

## How are security issues handled in Airflow

Security issues in Airflow are handled by the Airflow Security Team. Details about the Airflow Security Team and how members of it are chosen can be found in the [Contributing documentation](https://github.com/apache/airflow/blob/main/CONTRIBUTING.rst#security-team).
