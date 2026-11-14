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

.. _image-public-cves:

Discussing public CVEs in the image
===================================

The security scans report public vulnerabilities in 3rd-party components of Airflow. Since those are
already public vulnerabilities, this is something you can talk about but others also are talking about.
So you can do research on your own first. Try to find discussions about the issues, how others were handling
it and possibly even try to explore, whether the vulnerability can be exploited in Airflow or not.
This is a very valuable contribution to the community you can do in order to help others to
understand the impact of the vulnerability on Airflow. We highly appreciate our commercial users do it,
because Airflow is maintained by volunteers, so if you or your company can spend some time and skills of
security researchers to help the community to understand the impact of the vulnerability on Airflow, it
could be a fantastic contribution to the community and way to give back to the project that your company uses
for free.

You are free to discuss it publicly, open a `GitHub Discussion <https://github.com/apache/airflow/discussions>`_
mentioning your findings and research you've done so far. Ideally (as a way to contribute to Airflow) you
should explain the findings of your own security team in your company to help to research and understand
the impact of the vulnerability on Airflow (and your way of using it).
Again - strong suggestion is to open ONE discussion per vulnerability. You should NOT post scan results in
bulk - this is not helpful for a discussion, and you will not get meaningful answers if you will attempt to
discuss all the issues in one discussion thread.

Yes - we know it's the easy way to copy & paste your result and ask others what to do, but doing it is
going to likely result in silence because such actions in the community as seen as pretty selfish way of
getting your problems solved by tapping into time of other volunteers, without spending your time on making it
easier for them to help. If you really want to get help from the community, focus your discussion on
particular CVE, provide your findings - including analyzing your report in detail and finding which
binaries and base images exactly are causing the scanner to report the vulnerability. Remember that only
you have access to your scanner and you should bring as much helpful information so that others can
comment on it. Show that you have done your homework and that you bring valuable information to the community.

Opening a GitHub Discussion for this kind of issues is also a great way to communicate with the
maintainers and security team in an open and transparent way - without reverting to the private security
mailing list (which serves different purpose as explained above). If after such a discussion there will be
a way to remove such a vulnerability from the scanned image - great, you can even contribute a PR to the
Dockerfile to remove the vulnerability from the image. Maybe such a discussion will lead to a PR to allow
Airflow to upgrade to newer dependency that fixes the vulnerability or remove it altogether, or maybe
there is already a way to mitigate it or maybe there is already a PR that someone works on to fix it.
All this can (and should) be discussed publicly and transparently in a GitHub Discussion, not via private
security email, nor GitHub Issues which are exclusively about Airflow Issues not 3rd-party components
public security issues.
