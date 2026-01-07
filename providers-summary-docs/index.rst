
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

Providers
---------

Apache Airflow 2 is built in modular way. The "Core" of Apache Airflow provides core scheduler
functionality which allow you to write some basic tasks, but the capabilities of Apache Airflow can
be extended by installing additional packages, called ``providers``.

Providers can contain operators, hooks, sensor, and transfer operators to communicate with a
multitude of external systems, but they can also extend Airflow core with new capabilities.

You can install those providers separately in order to interface with a given service. The providers
for ``Apache Airflow`` are designed in the way that you can write your own providers easily. The
``Apache Airflow Community`` develops and maintain more than 80 providers, but you are free to
develop your own providers - the providers you build have exactly the same capability as the providers
written by the community, so you can release and share those providers with others.


If you want to learn how to build your own custom provider, you can find all the information about it at
:doc:`/howto/create-custom-providers`.


The full list of all the community managed providers is available at
`Providers Index <https://airflow.apache.org/docs/#providers-packages-docs-apache-airflow-providers-index-html>`_.

You can also see index of all the community provider's operators and hooks in
:doc:`/operators-and-hooks-ref/index`

Extending Airflow core functionality
------------------------------------

Providers give you the capability of extending core Airflow with extra capabilities. The Core airflow
provides basic and solid functionality of scheduling, the providers extend its capabilities. Here we
describe all the custom capabilities.

Airflow automatically discovers which providers add those additional capabilities and, once you install
provider package and re-start Airflow, those become automatically available to Airflow Users.

The summary of all the core functionalities that can be extended are available in
:doc:`/core-extensions/index`.

Configuration
'''''''''''''

Providers can have their own configuration options which allow you to configure how they work:

You can see all community-managed providers with their own configuration in
:doc:`/core-extensions/configurations`

Command Line Interface
''''''''''''''''''''''

.. note::
   The Airflow Core version must be ``3.2.0`` or newer to be able to use CLI commands provided by providers.

Providers can add their own custom CLI commands to Airflow CLI. Those commands will be available
once you install the provider package.

You can see all community-managed providers with their own CLI commands in
:doc:`/core-extensions/cli-commands`.

Custom connections
''''''''''''''''''

The providers can add custom connection types, extending connection form and handling custom form field
behaviour for the connections defined by the provider.

You can see all the custom connections available via community-managed providers in
:doc:`/core-extensions/connections`.

Extra links
'''''''''''

The providers can add extra custom links to operators delivered by the provider. Those will be visible in
task details view of the task.

You can see all the extra links available via community-managed providers in
:doc:`/core-extensions/extra-links`.


Logging
'''''''

The providers can add additional task logging capabilities. By default ``Apache Airflow`` saves logs for
tasks locally and make them available to Airflow UI via internal http server. However, providers
can add extra logging capabilities, where Airflow Logs can be written to a remote service and
retrieved from those services.

You can see all task loggers available via community-managed providers in
:doc:`/core-extensions/logging`.


Secret backends
'''''''''''''''

Airflow has the capability of reading connections, variables and configuration from Secret Backends rather
than from its own Database.

You can see all the secret backends available via community-managed providers in
:doc:`/core-extensions/secrets-backends`.

Notifications
'''''''''''''

The providers can add custom notifications, that allow you to configure the way how you would like to receive
notifications about the status of your tasks/dags.

You can see all the notifications available via community-managed providers in
:doc:`/core-extensions/notifications`.


Installing and upgrading providers
----------------------------------

Separate providers give the possibilities that were not available in 1.10:

1. You can upgrade to latest version of particular providers without the need of Apache Airflow core upgrade.

2. You can downgrade to previous version of particular provider in case the new version introduces
   some problems, without impacting the main Apache Airflow core package.

3. You can release and upgrade/downgrade providers incrementally, independent from each other. This
   means that you can incrementally validate each of the provider package update in your environment,
   following the usual tests you have in your environment.


Types of providers
------------------

Providers have the same capacity - no matter if they are provided by the community or if they are
third-party providers. This chapter explains how community managed providers are versioned and released
and how you can create your own providers.

.. _providers:community-maintained-providers:

Community maintained providers
''''''''''''''''''''''''''''''

From the point of view of the community, Airflow is delivered in multiple, separate packages.
The core of Airflow scheduling system is delivered as ``apache-airflow`` package and there are more than
80 providers which can be installed separately as so called ``Airflow providers``.
Those packages are available as ``apache-airflow-providers`` packages - for example there is an
``apache-airflow-providers-amazon`` or ``apache-airflow-providers-google`` package).

Community maintained providers are released and versioned separately from the Airflow releases. We are
following the `Semver <https://semver.org/>`_ versioning scheme for the packages. Some versions of the
providers might depend on particular versions of Airflow, but the general approach we have is that
unless there is a good reason, new version of providers should work with recent versions of Airflow 2.x.
Details will vary per-provider and if there is a limitation for particular version of particular provider,
constraining the Airflow version used, it will be included as limitation of dependencies in the provider
package.

Each community provider has corresponding extra which can be used when installing Airflow to install the
provider together with ``Apache Airflow`` - for example you can install Airflow with those extras:
``apache-airflow[google,amazon]`` (with correct constraints -see :doc:`apache-airflow:installation/index`) and you
will install the appropriate versions of the ``apache-airflow-providers-amazon`` and
``apache-airflow-providers-google`` packages together with ``Apache Airflow``.

Some of the community  providers have cross-provider dependencies as well. Those are not required
dependencies, they might simply enable certain features (for example transfer operators often create
dependency between different providers. Again, the general approach here is that the providers are backwards
compatible, including cross-dependencies. Any kind of breaking changes and requirements on particular versions of other
providers are automatically documented in the release notes of every provider.

.. note::
    For Airflow 1.10 We also provided ``apache-airflow-backport-providers`` packages that could be installed
    with those versions Those were the same providers as for 2.0 but automatically back-ported to work for
    Airflow 1.10. The last release of backport providers was done on March 17, 2021 and the backport
    providers will no longer be released, since Airflow 1.10 has reached End-Of-Life as of June 17, 2021.


If you want to contribute to ``Apache Airflow``, you can see how to build and extend community
managed providers in
``https://github.com/apache/airflow/blob/main/providers/MANAGING_PROVIDERS_LIFECYCLE.rst``.

.. toctree::
    :hidden:
    :maxdepth: 2

    Providers <self>
    Installing from PyPI <installing-from-pypi>
    Installing from sources <installing-from-sources>
    Create custom providers <howto/create-custom-providers>
    Packages <packages-ref>
    Operators and hooks <operators-and-hooks-ref/index>
    Core Extensions <core-extensions/index>
