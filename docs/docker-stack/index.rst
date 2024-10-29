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

 .. WARNING:
    IF YOU ARE UPDATING THIS FILE, CONSIDER UPDATING README.MD TOO.

.. image:: /img/docker-logo.png
    :width: 100

Docker Image for Apache Airflow
===============================

.. toctree::
    :hidden:

    Home <self>
    build
    entrypoint
    changelog
    recipes

.. toctree::
    :hidden:
    :caption: References

    build-arg-ref

For the ease of deployment in production, the community releases a production-ready reference container
image.


The Apache Airflow community, releases Docker Images which are ``reference images`` for Apache Airflow.
Every time a new version of Airflow is released, the images are prepared in the
`apache/airflow DockerHub <https://hub.docker.com/r/apache/airflow>`_
for all the supported Python versions.

You can find the following images there (Assuming Airflow version :subst-code:`|airflow-version|`):

* :subst-code:`apache/airflow:latest`              - the latest released Airflow image with default Python version (3.8 currently)
* :subst-code:`apache/airflow:latest-pythonX.Y`    - the latest released Airflow image with specific Python version
* :subst-code:`apache/airflow:|airflow-version|`           - the versioned Airflow image with default Python version (3.8 currently)
* :subst-code:`apache/airflow:|airflow-version|-pythonX.Y` - the versioned Airflow image with specific Python version

Those are "reference" regular images. They contain the most common set of extras, dependencies and providers that are
often used by the users and they are good to "try-things-out" when you want to just take Airflow for a spin,

You can also use "slim" images that contain only core airflow and are about half the size of the "regular" images
but you need to add all the :doc:`apache-airflow:extra-packages-ref` and providers that you need separately
via :ref:`Building the image <build:build_image>`.

* :subst-code:`apache/airflow:slim-latest`              - the latest released Airflow image with default Python version (3.8 currently)
* :subst-code:`apache/airflow:slim-latest-pythonX.Y`    - the latest released Airflow image with specific Python version
* :subst-code:`apache/airflow:slim-|airflow-version|`           - the versioned Airflow image with default Python version (3.8 currently)
* :subst-code:`apache/airflow:slim-|airflow-version|-pythonX.Y` - the versioned Airflow image with specific Python version

The Apache Airflow image provided as convenience package is optimized for size, and
it provides just a bare minimal set of the extras and dependencies installed and in most cases
you want to either extend or customize the image. You can see all possible extras in :doc:`apache-airflow:extra-packages-ref`.
The set of extras used in Airflow Production image are available in the
`Dockerfile <https://github.com/apache/airflow/blob/|airflow-version|/Dockerfile>`__.

However, Airflow has more than 60 community-managed providers (installable via extras) and some of the
default extras/providers installed are not used by everyone, sometimes others extras/providers
are needed, sometimes (very often actually) you need to add your own custom dependencies,
packages or even custom providers. You can learn how to do it in :ref:`Building the image <build:build_image>`.

The production images are build in DockerHub from released version and release candidates. There
are also images published from branches but they are used mainly for development and testing purpose.
See `Airflow Git Branching <https://github.com/apache/airflow/blob/main/contributing-docs/working-with-git#airflow-git-branches>`_
for details.

Fixing images at release time
=============================

The released "versioned" reference images are mostly ``fixed`` when we release Airflow version and we only
update them in exceptional circumstances. For example when we find out that there are dependency errors
that might prevent important Airflow or embedded provider's functionalities working. In normal circumstances,
the images are not going to change after release, even if new version of Airflow dependencies are released -
not even when those versions contain critical security fixes. The process of Airflow releases is designed
around upgrading dependencies automatically where applicable but only when we release a new version of Airflow,
not for already released versions.

What should I do if my security scan shows critical and high vulnerabilities in the image?
==========================================================================================

We often hear questions that our users use various security scanners on the image and find out that
there are some critical and high vulnerabilities in the image - not coming from Airflow but for some other
components. In general, this is normal and expected that such vulnerabilities are found in the image after
it's been released and fixed - precisely because we are NOT updating the images after they are released as
explained above. Also sometimes even the latest releases contain vulnerabilities that are not yet fixed
in the base image we use or in the dependencies we use and cannot upgrade, because some of our providers
have limits and did not manage to upgrade yet and we have no control over that. So it is possible
that even the most recent release of our image there are some High and Critical vulnerabilities that
are not yet fixed.

**What can you do in such case?**

First of all - you should know what you should NOT do.

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

So what are your options?

You have four options:

1. Build your own custom image following the examples we share there - using the latest base image and
   possibly manually bumping dependencies you want to bump. There are quite a few examples
   in :ref:`Building the image <build:build_image>` which you can follow. You can use "slim" image as a base
   for your images and rather than basing your image on the "reference" image that has a number of extras
   and providers installed, you can only install what you actually need and upgrade some dependencies that
   otherwise would not be possible to upgrade - because some of the provider libraries have limits and
   did not manage to upgrade yet and we have no control over that. This is the most flexible way to
   build your image and you can build your process to combine it with quickly upgrading to latest Airflow
   versions (see point 2. below).

2. Wait for a new version of Airflow and upgrade to it. Airflow images are updated to latest "non-conflicting"
   dependencies and use latest "base" image at release time, so what you have in the reference images
   at the moment we publish the image / release the version is what is "latest and greatest"
   available at the moment with the base platform we use (Debian Bookworm is the reference image we use).
   This is one of good strategies you can take - build a process to upgrade your Airflow version regularly
   - quickly after it has been released by the community, this will help you to keep up with the latest
   security fixes in the dependencies.

3. If the base platform we use (currently Debian Bookworm) does not contain the latest versions you want
   and you want to use other base images, you can take a look at what system dependencies are installed
   and scripts in the latest ``Dockerfile`` of airflow and take inspiration from it and build your own image
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


**How do I discuss publicly about public CVEs in the image?**

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

Support
=======

The reference Docker Image supports the following platforms and database:


Intel platform (x86_64)
-----------------------

* Postgres Client
* MySQL Client
* MSSQL Client

ARM platform (aarch64)
----------------------

ARM support is experimental, might change any time.

* Postgres Client
* MySQL Client (MySQL 8)
* MSSQL Client

Note that MySQL on arm has experimental support through MariaDB client library.

Usage
=====

The :envvar:`AIRFLOW_HOME` is set by default to ``/opt/airflow/`` - this means that Dags
are by default in the ``/opt/airflow/dags`` folder and logs are in the ``/opt/airflow/logs``

The working directory is ``/opt/airflow`` by default.

If no :envvar:`AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` variable is set then SQLite database is created in
``${AIRFLOW_HOME}/airflow.db``.

For example commands that start Airflow see: :ref:`entrypoint:commands`.

Airflow requires many components to function as it is a distributed application. You may therefore also be interested
in launching Airflow in the Docker Compose environment, see: :doc:`apache-airflow:howto/docker-compose/index`.

You can use this image in :doc:`Helm Chart <helm-chart:index>` as well.
