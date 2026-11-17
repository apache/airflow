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

Docker Image for Apache Airflow
===============================

.. image:: /img/docker-logo.png
    :width: 100

.. raw:: html

    <div style="margin-bottom: 1.5em"></div>

.. toctree::
    :hidden:
    :caption: Airflow Docker Image

    Overview <self>
    Image variants <image-variants>
    Runtime environment <entrypoint>
    Working with the image <testing>
    changelog

.. toctree::
    :hidden:
    :caption: Building your image

    Why own image <why-build-your-own-image>
    build
    Extending <build-extending>
    Customizing <build-customizing>
    Advanced cases <build-advanced>
    Publishing <build-publishing>
    Build arguments <build-arg-ref>
    legacy-images

.. toctree::
    :hidden:
    :caption: Image and security

    Image update policy <security-release-time>
    Handling scan results <security-scan-results>
    Public CVEs <security-public-cves>

For the ease of deployment in production, the community releases a production-ready reference container
image.


The Apache Airflow community, releases Docker Images which are ``reference images`` for Apache Airflow.
Every time a new version of Airflow is released, the images are prepared in the
`DockerHub of Apache Airflow <https://hub.docker.com/r/apache/airflow>`_
for all the supported Python versions. The images we release are multi-platform AMD/ARM images.

Where to start:

* :doc:`image-variants` - the regular and slim images published for every release, and their names.
* :doc:`entrypoint` - the folders and environment variables of the image, and the commands that
  start Airflow.
* :doc:`why-build-your-own-image` - why you should build your own image for production, and how.
* :doc:`build` - a quick start for building a small production image.
* :doc:`security-release-time` - why security scanners report vulnerabilities in the images, and what
  to do about it.

Python versions
===============

The images are published for every Python version supported by the Airflow release - see
:doc:`image-variants` for the image names.

The default Python version for the reference image - one that you get when you do not specify version -
is the newest supported Python version at the time of the Airflow release that supports
all providers released in the regular reference image.

For example, even when we release Airflow 3.0 with Python 3.13 support, but some of the providers that
are installed by default in the regular reference image do not support Python 3.13,
the "default" Python version for the image should be Python 3.12.

The same applies to the "slim" images.

Supported databases
===================

The reference Docker Image includes the clients for the following databases:

* Postgres Client
* MySQL Client (8+)
* MSSQL Client

Note that MySQL on ``ARM`` platform has experimental support through MariaDB client library.
