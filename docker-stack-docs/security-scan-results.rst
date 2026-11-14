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

.. _image-vulnerabilities:

Handling security scan results
==============================

When your security scanner reports critical or high vulnerabilities in the image - see
:doc:`security-release-time` for why it does - this page tells you what you should not do and what
your options are.

What you should NOT do
----------------------

Do NOT send private email to the Airflow Security Team with scan results and asking what to do.
The Security team at Airflow takes care exclusively about undisclosed vulnerabilities in Airflow itself, not
in the dependencies or in the base image. The security email should only be used to report privately any
security issues that can be exploited via Airflow. This is nicely explained in our
`Security Policy <https://github.com/apache/airflow/security/policy>`__ where you can find more details
including the need to provide reproducible scenarios and submitting ONE issue per email. NEVER submit multiple
vulnerabilities in one email - those are rejected immediately, as they make the process of handling the issue
way harder for everyone, including the reporters.

Also DO NOT open a GitHub Issue with the scan results and asking what to do. The GitHub Issues are for
reporting bugs and feature requests to Airflow itself, not for asking for help with the security scans on
3rd party components.

Your options
------------

You have four options:

1. Build your own custom image following the examples we share there - using the latest base image and
   possibly manually bumping dependencies you want to bump. There are quite a few examples
   in :ref:`Building your image <build:build_image>` which you can follow. You can use "slim" image as a base
   for your images and rather than basing your image on the "reference" image that has a number of extras
   and providers installed, you can only install what you actually need and upgrade some dependencies that
   otherwise would not be possible to upgrade - because some of the provider libraries have limits and
   did not manage to upgrade yet and we have no control over that. This is the most flexible way to
   build your image and you can build your process to combine it with quickly upgrading to latest Airflow
   versions (see point 2. below).

2. Wait for a new version of Airflow and upgrade to it. Airflow images are updated to latest "non-conflicting"
   dependencies and use latest "base" image at release time, so what you have in the reference images
   at the moment we publish the image / release the version is what is "latest and greatest"
   available at the moment with the base platform we use (see :ref:`image-hardened-base`).
   This is one of good strategies you can take - build a process to upgrade your Airflow version regularly
   - quickly after it has been released by the community, this will help you to keep up with the latest
   security fixes in the dependencies.

3. If the base platform we use (currently the Docker Hardened Image for Python on Debian 13 "trixie")
   does not contain the latest versions you want
   and you want to use other base images, you can take a look at what system dependencies are installed
   and scripts in the latest ``Dockerfile`` of Airflow and take inspiration from it and build your own image
   or copy it and modify it to your needs. See the
   `Dockerfile <https://github.com/apache/airflow/blob/|airflow-version|/Dockerfile>`__ for the latest version.

4. Research if the vulnerability affects you or not. Even if there is a dependency with high or critical
   vulnerability, it does not mean that it can be exploited in Airflow (or specifically in the way you are
   using Airflow). If you do have a reproducible scenario how a vulnerability can be exploited in Airflow, you should -
   of course - privately report it to the security team. But if you do not have reproducible
   scenario, please do some research and try to understand the impact of the vulnerability on Airflow. That
   research might result in a  public GitHub Discussion where you can discuss the impact of the
   vulnerability if you research will indicate Airflow might not be impacted or private security email if
   you find a reproducible scenario on how to exploit it.
