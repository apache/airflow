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


Installation
------------

.. contents:: :local:

.. toctree::
    :maxdepth: 1
    :caption: Installation
    :hidden:

    Prerequisites <prerequisites>
    Dependencies <dependencies>
    Supported versions <supported-versions>
    Installing from sources <installing-from-sources>
    Installing from PyPI <installing-from-pypi>
    Setting up the database <setting-up-the-database>
    Upgrading <upgrading>

This page describes installations options that you might use when considering how to install Airflow.
Airflow consists of many components, often distributed among many physical or virtual machines, therefore
installation of Airflow might be quite complex, depending on the options you choose.

You should also check-out the :doc:`Prerequisites <prerequisites>` that must be fulfilled when installing Airflow
as well as  :doc:`Supported versions <supported-versions>` to know what are the policies for supporting
Airflow, Python and Kubernetes.

Airflow requires additional :doc:`Dependencies <dependencies>` to be installed - which can be done
via extras and providers.

When you install Airflow, you need to :doc:`setup the database <setting-up-the-database>` which must
also be kept updated when Airflow is upgraded.

.. warning::

  As of June 2021 Airflow 1.10 is end-of-life and is not going to receive any fixes even critical
  security fixes. Follow the :doc:`/upgrading-from-1-10/index` to learn
  how to upgrade the end-of-life 1.10 to Airflow 2.

Using released sources
''''''''''''''''''''''

More details: :doc:`installing-from-sources`

**When this option works best**

* This option is best if you expect to build all your software from sources.
* Apache Airflow is one of the projects that belong to the `Apache Software Foundation <https://www.apache.org/>`__ .
  It is a requirement for all ASF projects that they can be installed using official sources released via `Official Apache Downloads <http://ws.apache.org/mirrors.cgi/>`__ .
* This is the best choice if you have a strong need to `verify the integrity and provenance of the software <https://www.apache.org/dyn/closer.cgi#verify>`__

**Intended users**

* Users who are familiar with installing and building software from sources and are conscious about integrity and provenance
  of the software they use down to the lowest level possible.

**What are you expected to handle**

* You are expected to build and install airflow and its components on your own.
* You should develop and handle the deployment for all components of Airflow.
* You are responsible for setting up database, creating and managing database schema with ``airflow db`` commands,
  automated startup and recovery, maintenance, cleanup and upgrades of Airflow and the Airflow Providers.

**What Apache Airflow Community provides for that method**

* You have `instructions <https://github.com/apache/airflow/blob/main/INSTALL>`__ on how to build the software but due to various environments
  and tools you might want to use, you might expect that there will be problems which are specific to your deployment and environment
  you will have to diagnose and solve.

**Where to ask for help**

* The ``#development`` slack channel for building the software.

* The ``#troubleshooting`` slack is a channel for quick general troubleshooting questions. The
  `GitHub discussions <https://github.com/apache/airflow/discussions>`__ if you look for longer discussion and have more information to share.

* If you can provide description of a reproducible problem with Airflow software, you can open issue at `GitHub issues <https://github.com/apache/airflow/issues>`_

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

* In case of PyPI installation you could also verify integrity and provenance of the packages of the packages
  downloaded from PyPI as described at the installation page, but software you download from PyPI is pre-built
  for you so that you can install it without building, and you do not build the software from sources.

**Intended users**

* Users who are familiar with installing and configuring Python applications, managing Python environments,
  dependencies and running software with their custom deployment mechanisms.

**What are you expected to handle**

* You are expected to install Airflow - all components of it - on your own.
* You should develop and handle the deployment for all components of Airflow.
* You are responsible for setting up database, creating and managing database schema with ``airflow db`` commands,
  automated startup and recovery, maintenance, cleanup and upgrades of Airflow and Airflow Providers.

**What Apache Airflow Community provides for that method**

* You have :doc:`/installation/installing-from-pypi`
  on how to install the software but due to various environments and tools you might want to use, you might
  expect that there will be problems which are specific to your deployment and environment you will have to
  diagnose and solve.
* You have :doc:`/start` where you can see an example of Quick Start with running Airflow
  locally which you can use to start Airflow quickly for local testing and development.
  However this is just an inspiration. Do not expect this docker-compose is ready for production installation,
  you need to build your own production-ready deployment in this approach.

**Where to ask for help**

* The ``#troubleshooting`` channel on Airflow Slack for quick general
  troubleshooting questions. The `GitHub discussions <https://github.com/apache/airflow/discussions>`__
  if you look for longer discussion and have more information to share.
* If you can provide description of a reproducible problem with Airflow software, you can open
  issue at `GitHub issues <https://github.com/apache/airflow/issues>`__


Using Production Docker Images
''''''''''''''''''''''''''''''

More details: :doc:`docker-stack:index`

**When this option works best**

This installation method is useful when you are familiar with Container/Docker stack. It provides a capability of
running Airflow components in isolation from other software running on the same physical or virtual machines with easy
maintenance of dependencies.

The images are built by Apache Airflow release managers and they use officially released packages from PyPI
and official constraint files- same that are used for installing Airflow from PyPI.

**Intended users**

* Users who are familiar with Containers and Docker stack and understand how to build their own container images.
* Users who understand how to install providers and dependencies from PyPI with constraints if they want to extend or customize the image.
* Users who know how to create deployments using Docker by linking together multiple Docker containers and maintaining such deployments.

**What are you expected to handle**

* You are expected to be able to customize or extend Container/Docker images if you want to
  add extra dependencies. You are expected to put together a deployment built of several containers
  (for example using docker-compose) and to make sure that they are linked together.
* You are responsible for setting up database, creating and managing database schema with ``airflow db`` commands,
  automated startup and recovery, maintenance, cleanup and upgrades of Airflow and the Airflow Providers.
* You are responsible to manage your own customizations and extensions for your custom dependencies.
  With the Official Airflow Docker Images, upgrades of Airflow and Airflow Providers which
  are part of the reference image are handled by the community - you need to make sure to pick up
  those changes when released by upgrading the base image. However you are responsible in creating a
  pipeline of building your own custom images with your own added dependencies and Providers and need to
  repeat the customization step and building your own image when new version of Airflow image is released.
* You should choose the right deployment mechanism. There a number of available options of
  deployments of containers. You can use your own custom mechanism, custom Kubernetes deployments,
  custom Docker Compose, custom Helm charts etc., and you should choose it based on your experience
  and expectations.

**What Apache Airflow Community provides for that method**

* You have instructions: :doc:`docker-stack:build` on how to build and customize your image.
* You have :doc:`/howto/docker-compose/index` where you can see an example of Quick Start which
  you can use to start Airflow quickly for local testing and development. However this is just an inspiration.
  Do not expect to use this ``docker-compose.yml`` file for production installation, you need to get familiar
  with Docker Compose and its capabilities and build your own production-ready deployment with it if
  you choose Docker Compose for your deployment.
* The Docker Image is managed by the same people who build Airflow, and they are committed to keep
  it updated whenever new features and capabilities of Airflow are released.

**Where to ask for help**

* For quick questions with the Official Docker Image there is the ``#production-docker-image`` channel in Airflow Slack.
* The ``#troubleshooting`` channel on Airflow Slack for quick general
  troubleshooting questions. The `GitHub discussions <https://github.com/apache/airflow/discussions>`__
  if you look for longer discussion and have more information to share.
* If you can provide description of a reproducible problem with Airflow software, you can open
  issue at `GitHub issues <https://github.com/apache/airflow/issues>`__

Using Official Airflow Helm Chart
'''''''''''''''''''''''''''''''''

More details: :doc:`helm-chart:index`

**When this option works best**

* This installation method is useful when you are not only familiar with Container/Docker stack but also when you
  use Kubernetes and want to install and maintain Airflow using the community-managed Kubernetes installation
  mechanism via Helm chart.
* It provides not only a capability of running Airflow components in isolation from other software
  running on the same physical or virtual machines and managing dependencies, but also it provides capabilities of
  easier maintaining, configuring and upgrading Airflow in the way that is standardized and will be maintained
  by the community.
* The Chart uses the Official Airflow Production Docker Images to run Airflow.

**Intended users**

* Users who are familiar with Containers and Docker stack and understand how to build their own container images.
* Users who understand how to install providers and dependencies from PyPI with constraints if they want to extend or customize the image.
* Users who manage their infrastructure using Kubernetes and manage their applications on Kubernetes using Helm Charts.

**What are you expected to handle**

* You are expected to be able to customize or extend Container/Docker images if you want to
  add extra dependencies. You are expected to put together a deployment built of several containers
  (for example using Docker Compose) and to make sure that they are linked together.
* You are responsible for setting up database.
* The Helm Chart manages your database schema, automates startup, recovery and restarts of the
  components of the application and linking them together, so you do not have to worry about that.
* You are responsible to manage your own customizations and extensions for your custom dependencies.
  With the Official Airflow Docker Images, upgrades of Airflow and Airflow Providers which
  are part of the reference image are handled by the community - you need to make sure to pick up
  those changes when released by upgrading the base image. However you are responsible in creating a
  pipeline of building your own custom images with your own added dependencies and Providers and need to
  repeat the customization step and building your own image when new version of Airflow image is released.

**What Apache Airflow Community provides for that method**

* You have instructions: :doc:`docker-stack:build` on how to build and customize your image.
* You have :doc:`helm-chart:index` - full documentation on how to configure and install the Helm Chart.
* The Helm Chart is managed by the same people who build Airflow, and they are committed to keep
  it updated whenever new features and capabilities of Airflow are released.

**Where to ask for help**

* For quick questions with the Official Docker Image there is the ``#production-docker-image`` channel in Airflow Slack.
* For quick questions with the official Helm Chart there is the ``#helm-chart-official`` channel in Slack.
* The ``#troubleshooting`` channel on Airflow Slack for quick general
  troubleshooting questions. The `GitHub discussions <https://github.com/apache/airflow/discussions>`__
  if you look for longer discussion and have more information to share.
* If you can provide description of a reproducible problem with Airflow software, you can open
  issue at `GitHub issues <https://github.com/apache/airflow/issues>`__


Using Managed Airflow Services
''''''''''''''''''''''''''''''

Follow the  `Ecosystem <https://airflow.apache.org/ecosystem/>`__ page to find all Managed Services for Airflow.

**When this option works best**

* When you prefer to have someone else manage Airflow installation for you, there are Managed Airflow Services
  that you can use.

**Intended users**

* Users who prefer to get Airflow managed for them and want to pay for it.

**What are you expected to handle**

* The Managed Services usually provide everything you need to run Airflow. Please refer to documentation of
  the Managed Services for details.

**What Apache Airflow Community provides for that method**

* Airflow Community does not provide any specific documentation for managed services.
  Please refer to the documentation of the Managed Services for details.

**Where to ask for help**

* Your first choice should be support that is provided by the Managed services. There are a few
  channels in the Apache Airflow Slack that are dedicated to different groups of users and if you have
  come to conclusion the question is more related to Airflow than the managed service,
  you can use those channels.

Using 3rd-party images, charts, deployments
'''''''''''''''''''''''''''''''''''''''''''

Follow the  `Ecosystem <https://airflow.apache.org/ecosystem/>`__ page to find all 3rd-party deployment options.

**When this option works best**

* Those installation methods are useful in case none of the official methods mentioned before work for you,
  or you have historically used those. It is recommended though that whenever you consider any change,
  you should consider switching to one of the methods that are officially supported by the Apache Airflow
  Community or Managed Services.

**Intended users**

* Users who historically used other installation methods or find the official methods not sufficient for other reasons.

**What are you expected to handle**

* Depends on what the 3rd-party provides. Look at the documentation of the 3rd-party.

**What Apache Airflow Community provides for that method**

* Airflow Community does not provide any specific documentation for 3rd-party methods.
  Please refer to the documentation of the Managed Services for details.

**Where to ask for help**

* Depends on what the 3rd-party provides. Look at the documentation of the 3rd-party deployment you use.
