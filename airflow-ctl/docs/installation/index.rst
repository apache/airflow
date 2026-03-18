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


Installation of airflowctl
----------------------------

.. contents:: :local:

.. toctree::
    :maxdepth: 1
    :caption: Installation
    :hidden:

    Prerequisites <prerequisites>
    Installing from sources <installing-from-sources>
    Installing from PyPI <installing-from-pypi>

This page describes installations options that you might use when considering how to install Airflow®.
Airflow consists of many components, often distributed among many physical or virtual machines, therefore
installation of Airflow might be quite complex, depending on the options you choose.


Using released sources
''''''''''''''''''''''

More details: :doc:`installing-from-sources`

**When this option works best**

* This option is best if you expect to build all your software from sources.
* Apache Airflow is one of the projects that belong to the `Apache Software Foundation <https://www.apache.org/>`__.
  It is a requirement for all ASF projects that they can be installed using official sources released via `Official Apache Downloads <https://dlcdn.apache.org/>`__.
* This is the best choice if you have a strong need to `verify the integrity and provenance of the software <https://www.apache.org/dyn/closer.cgi#verify>`__

**Intended users**

* Users who are familiar with installing and building software from sources and are conscious about integrity and provenance
  of the software they use down to the lowest level possible.

**What are you expected to handle**

* You are expected to build and install airflow and its components on your own.
* You should develop and handle the deployment for all components of Airflow.
* You are responsible for setting up database, creating and managing database schema with ``airflow db`` commands,
  automated startup and recovery, maintenance, cleanup and upgrades of Airflow and the Airflow Providers.
* You need to setup monitoring of your system allowing you to observe resources and react to problems.
* You are expected to configure and manage appropriate resources for the installation (memory, CPU, etc) based
  on the monitoring of your installation and feedback loop. See the notes about requirements.

**What Apache Airflow Community provides for that method**

* You have `instructions <https://github.com/apache/airflow/blob/main/INSTALL>`__ on how to build the software but due to various environments
  and tools you might want to use, you might expect that there will be problems which are specific to your deployment and environment
  you will have to diagnose and solve.

**Where to ask for help**

* The ``#user-troubleshooting`` channel on slack can be used for quick general troubleshooting questions. The
  `GitHub discussions <https://github.com/apache/airflow/discussions>`__ if you look for longer discussion and have more information to share.

* The ``#user-best-practices`` channel on slack can be used to ask for and share best practices on using and deploying airflow.

* If you can provide description of a reproducible problem with Airflow software, you can open issue at `GitHub issues <https://github.com/apache/airflow/issues>`_

* If you want to contribute back to Airflow, the ``#contributors`` slack channel for building the Airflow itself


Using PyPI
'''''''''''

More details:  :doc:`/installation/installing-from-pypi`

**When this option works best**

* This installation method is useful when you are not familiar with Containers and Docker and want to install
  Apache Airflow on physical or virtual machines and you are used to installing and running software using custom
  deployment mechanism.

* The only officially supported mechanism of installation is via ``pip`` using constraint mechanisms. The constraint
  files are managed by Apache Airflow release managers to make sure that you can repeatably install Airflow from PyPI with all Providers and
  required dependencies.

* In case of PyPI installation you could also verify integrity and provenance of the packages
  downloaded from PyPI as described at the installation page, but software you download from PyPI is pre-built
  for you so that you can install it without building, and you do not build the software from sources.

**Intended users**

* Users who are familiar with installing and configuring Python applications, managing Python environments,
  dependencies and running software with their custom deployment mechanisms.

**What are you expected to handle**

* You are expected to install airflowctl.
* You should run the Airflow API server.
* You need to setup monitoring of your system allowing you to observe resources and react to problems.

**What Apache Airflow Community provides for that method**

* You have :doc:`/installation/installing-from-pypi`
  on how to install the software but due to various environments and tools you might want to use, you might
  expect that there will be problems which are specific to your deployment and environment you will have to
  diagnose and solve.
* You have :doc:`/start` where you can see an example of Quick Start with running Airflow
  locally which you can use to start Airflow quickly for local testing and development.
  However, this is just for inspiration. Do not expect :doc:`/start` is ready for production installation,
  you need to build your own production-ready deployment if you follow this approach.

**Where to ask for help**

* The ``#user-troubleshooting`` channel on Airflow Slack for quick general
  troubleshooting questions. The `GitHub discussions <https://github.com/apache/airflow/discussions>`__
  if you look for longer discussion and have more information to share.
* The ``#user-best-practices`` channel on slack can be used to ask for and share best
  practices on using and deploying airflow.
* If you can provide description of a reproducible problem with Airflow software, you can open
  issue at `GitHub issues <https://github.com/apache/airflow/issues>`__
