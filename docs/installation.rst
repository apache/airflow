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

.. contents:: :local:

Installation
------------

Getting Airflow
'''''''''''''''

Airflow is published as ``apache-airflow`` package in PyPI. Installing it however might be sometimes tricky
because Airflow is a bit of both a library and application. Libraries usually keep their dependencies open and
applications usually pin them, but we should do neither and both at the same time. We decided to keep
our dependencies as open as possible (in ``setup.py``) so users can install different version of libraries
if needed. This means that from time to time plain ``pip install apache-airflow`` will not work or will
produce unusable Airflow installation.

In order to have repeatable installation, however, starting from **Airflow 1.10.10** and updated in
**Airflow 1.10.12** we also keep a set of "known-to-be-working" constraint files in the
``constraints-master`` and ``constraints-1-10`` orphan branches.
Those "known-to-be-working" constraints are per major/minor python version. You can use them as constraint
files when installing Airflow from PyPI. Note that you have to specify correct Airflow version
and python versions in the URL.

  **Prerequisites**

  On Debian based Linux OS:

  .. code-block:: bash

      sudo apt-get update
      sudo apt-get install build-essential


1. Installing just airflow

.. code-block:: bash

    AIRFLOW_VERSION=1.10.12
    PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
    # For example: 3.6
    CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
    # For example: https://raw.githubusercontent.com/apache/airflow/constraints-1.10.12/constraints-3.6.txt
    pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

2. Installing with extras (for example postgres, google)

.. code-block:: bash

    AIRFLOW_VERSION=1.10.12
    PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
    CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
    pip install "apache-airflow[postgres,google]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

Most of the extras are linked to a corresponding providers package. For example "amazon" extra
has a corresponding ``apache-airflow-providers-amazon`` providers package to be installed. When you install
Airflow with such extras, the necessary provider packages are installed automatically (latest versions from
PyPI for those packages). However you can freely upgrade and install provider packages independently from
the main Airflow installation.

.. note:: Automated installation of Provider packages does not work in Airflow 2.0.0b1 - for this version
          you have to install provider packages manually. As of Airflow 2.0.0b2 the corresponding
          provider packages are installed together with the extras.

Read more about it in the `Provider Packages <#provider-packages>`_ section.

Requirements
''''''''''''

You need certain system level requirements in order to install Airflow. Those are requirements that are known
to be needed for Linux system (Tested on Ubuntu Buster LTS) :

.. code-block:: bash

   sudo apt-get install -y --no-install-recommends \
           freetds-bin \
           krb5-user \
           ldap-utils \
           libffi6 \
           libsasl2-2 \
           libsasl2-modules \
           libssl1.1 \
           locales  \
           lsb-release \
           sasl2-bin \
           sqlite3 \
           unixodbc

You also need database client packages (Postgres or MySQL) if you want to use those databases.

If the ``airflow`` command is not getting recognized (can happen on Windows when using WSL), then
ensure that ``~/.local/bin`` is in your ``PATH`` environment variable, and add it in if necessary:

.. code-block:: bash

    PATH=$PATH:~/.local/bin

Extra Packages
''''''''''''''

The ``apache-airflow`` PyPI basic package only installs what's needed to get started.
Subpackages can be installed depending on what will be useful in your
environment. For instance, if you don't need connectivity with Postgres,
you won't have to go through the trouble of installing the ``postgres-devel``
yum package, or whatever equivalent applies on the distribution you are using.

Behind the scenes, Airflow does conditional imports of operators that require
these extra dependencies.

Here's the list of the subpackages and what they enable:


**Fundamentals:**

+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| subpackage          | install command                                     | enables                                                              |
+=====================+=====================================================+======================================================================+
| all                 | ``pip install 'apache-airflow[all]'``               | All Airflow features known to man                                    |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| all_dbs             | ``pip install 'apache-airflow[all_dbs]'``           | All databases integrations                                           |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| devel               | ``pip install 'apache-airflow[devel]'``             | Minimum dev tools requirements                                       |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| devel_hadoop        | ``pip install 'apache-airflow[devel_hadoop]'``      | Airflow + dependencies on the Hadoop stack                           |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| doc                 | ``pip install 'apache-airflow[doc]'``               | Packages needed to build docs                                        |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| password            | ``pip install 'apache-airflow[password]'``          | Password authentication for users                                    |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+


**Apache Software:**

+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| subpackage          | install command                                     | enables                                                              |
+=====================+=====================================================+======================================================================+
| atlas               | ``pip install 'apache-airflow[apache.atlas]'``      | Apache Atlas to use Data Lineage feature                             |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| beam                | ``pip install 'apache-airflow[apache.beam]'``       | Apache Beam operators & hooks                                        |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| cassandra           | ``pip install 'apache-airflow[apache.cassandra]'``  | Cassandra related operators & hooks                                  |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| druid               | ``pip install 'apache-airflow[apache.druid]'``      | Druid related operators & hooks                                      |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| hdfs                | ``pip install 'apache-airflow[apache.hdfs]'``       | HDFS hooks and operators                                             |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| hive                | ``pip install 'apache-airflow[apache.hive]'``       | All Hive related operators                                           |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| kylin               | ``pip install 'apache-airflow[apache.kylin]'``      | All Kylin related operators & hooks                                  |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| presto              | ``pip install 'apache-airflow[apache.presto]'``     | All Presto related operators & hooks                                 |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| spark               | ``pip install 'apache-airflow[apache.spark]'``      | All Spark related operators & hooks                                  |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| webhdfs             | ``pip install 'apache-airflow[webhdfs]'``           | HDFS hooks and operators                                             |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+


**Services:**

+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| subpackage          | install command                                     | enables                                                              |
+=====================+=====================================================+======================================================================+
| aws                 | ``pip install 'apache-airflow[amazon]'``            | Amazon Web Services                                                  |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| azure               | ``pip install 'apache-airflow[microsoft.azure]'``   | Microsoft Azure                                                      |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| cloudant            | ``pip install 'apache-airflow[cloudant]'``          | Cloudant hook                                                        |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| databricks          | ``pip install 'apache-airflow[databricks]'``        | Databricks hooks and operators                                       |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| datadog             | ``pip install 'apache-airflow[datadog]'``           | Datadog hooks and sensors                                            |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| facebook            | ``pip install 'apache-airflow[facebook]'``          | Facebook Social                                                      |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| gcp                 | ``pip install 'apache-airflow[google]'``            | Google Cloud                                                         |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| github_enterprise   | ``pip install 'apache-airflow[github_enterprise]'`` | GitHub Enterprise auth backend                                       |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| google_auth         | ``pip install 'apache-airflow[google_auth]'``       | Google auth backend                                                  |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| hashicorp           | ``pip install 'apache-airflow[hashicorp]'``         | Hashicorp Services (Vault)                                           |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| jira                | ``pip install 'apache-airflow[jira]'``              | Jira hooks and operators                                             |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| pagerduty           | ``pip install 'apache-airflow[pagerduty]'``         | Pagerduty hook                                                       |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| plexus              | ``pip install 'apache-airflow[plexus]'``            | Plexus service of CoreScientific.com AI platform                     |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| qds                 | ``pip install 'apache-airflow[qds]'``               | Enable QDS (Qubole Data Service) support                             |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| salesforce          | ``pip install 'apache-airflow[salesforce]'``        | Salesforce hook                                                      |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| sendgrid            | ``pip install 'apache-airflow[sendgrid]'``          | Send email using sendgrid                                            |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| segment             | ``pip install 'apache-airflow[segment]'``           | Segment hooks and sensors                                            |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| sentry              | ``pip install 'apache-airflow[sentry]'``            | Sentry service for application logging and monitoring                |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| slack               | ``pip install 'apache-airflow[slack]'``             | :class:`airflow.providers.slack.operators.slack.SlackAPIOperator`    |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| snowflake           | ``pip install 'apache-airflow[snowflake]'``         | Snowflake hooks and operators                                        |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| vertica             | ``pip install 'apache-airflow[vertica]'``           | Vertica hook support as an Airflow backend                           |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| yandexcloud         | ``pip install 'apache-airflow[yandexcloud]'``       | Yandex.Cloud hooks and operators                                     |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+


**Software:**

+---------------------+-----------------------------------------------------+------------------------------------------------------------------------------------+
| subpackage          | install command                                     | enables                                                                            |
+=====================+=====================================================+====================================================================================+
| async               | ``pip install 'apache-airflow[async]'``             | Async worker classes for Gunicorn                                                  |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------------------+
| celery              | ``pip install 'apache-airflow[celery]'``            | CeleryExecutor                                                                     |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------------------+
| dask                | ``pip install 'apache-airflow[dask]'``              | DaskExecutor                                                                       |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------------------+
| docker              | ``pip install 'apache-airflow[docker]'``            | Docker hooks and operators                                                         |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------------------+
| elasticsearch       | ``pip install 'apache-airflow[elasticsearch]'``     | Elasticsearch hooks and Log Handler                                                |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------------------+
| exasol              | ``pip install 'apache-airflow[exasol]'``            | Exasol hooks and operators                                                         |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------------------+
| kubernetes          | ``pip install 'apache-airflow[cncf.kubernetes]'``   | Kubernetes Executor and operator                                                   |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------------------+
| mongo               | ``pip install 'apache-airflow[mongo]'``             | Mongo hooks and operators                                                          |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------------------+
| mssql (deprecated)  | ``pip install 'apache-airflow[microsoft.mssql]'``   | Microsoft SQL Server operators and hook,                                           |
|                     |                                                     | support as an Airflow backend.  Uses pymssql.                                      |
|                     |                                                     | Will be replaced by subpackage ``odbc``.                                           |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------------------+
| mysql               | ``pip install 'apache-airflow[mysql]'``             | MySQL operators and hook, support as an Airflow                                    |
|                     |                                                     | backend. The version of MySQL server has to be                                     |
|                     |                                                     | 5.6.4+. The exact version upper bound depends                                      |
|                     |                                                     | on version of ``mysqlclient`` package. For                                         |
|                     |                                                     | example, ``mysqlclient`` 1.3.12 can only be                                        |
|                     |                                                     | used with MySQL server 5.6.4 through 5.7.                                          |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------------------+
| odbc                | ``pip install 'apache-airflow[odbc]'``              | ODBC data sources including MS SQL Server.  Can use MsSqlOperator,                 |
|                     |                                                     | or as metastore database backend.  Uses pyodbc.                                    |
|                     |                                                     | See :ref:`howto/connection/odbc` for more info.                                    |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------------------+
| oracle              | ``pip install 'apache-airflow[oracle]'``            | Oracle hooks and operators                                                         |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------------------+
| pinot               | ``pip install 'apache-airflow[pinot]'``             | Pinot DB hook                                                                      |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------------------+
| postgres            | ``pip install 'apache-airflow[postgres]'``          | PostgreSQL operators and hook, support as an                                       |
|                     |                                                     | Airflow backend                                                                    |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------------------+
| rabbitmq            | ``pip install 'apache-airflow[rabbitmq]'``          | RabbitMQ support as a Celery backend                                               |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------------------+
| redis               | ``pip install 'apache-airflow[redis]'``             | Redis hooks and sensors                                                            |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------------------+
| samba               | ``pip install 'apache-airflow[samba]'``             | :class:`airflow.providers.apache.hive.transfers.hive_to_samba.HiveToSambaOperator` |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------------------+
| singularity         | ``pip install 'apache-airflow[singularity]'``       | Singularity container operator                                                     |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------------------+
| statsd              | ``pip install 'apache-airflow[statsd]'``            | Needed by StatsD metrics                                                           |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------------------+
| tableau             | ``pip install 'apache-airflow[tableau]'``           | Tableau visualization integration                                                  |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------------------+
| virtualenv          | ``pip install 'apache-airflow[virtualenv]'``        | Running python tasks in local virtualenv                                           |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------------------+


**Other:**

+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| subpackage          | install command                                     | enables                                                              |
+=====================+=====================================================+======================================================================+
| cgroups             | ``pip install 'apache-airflow[cgroups]'``           | Needed To use CgroupTaskRunner                                       |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| grpc                | ``pip install 'apache-airflow[grpc]'``              | Grpc hooks and operators                                             |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| jdbc                | ``pip install 'apache-airflow[jdbc]'``              | JDBC hooks and operators                                             |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| kerberos            | ``pip install 'apache-airflow[kerberos]'``          | Kerberos integration for Kerberized Hadoop                           |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| ldap                | ``pip install 'apache-airflow[ldap]'``              | LDAP authentication for users                                        |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| papermill           | ``pip install 'apache-airflow[papermill]'``         | Papermill hooks and operators                                        |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| ssh                 | ``pip install 'apache-airflow[ssh]'``               | SSH hooks and Operator                                               |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| winrm               | ``pip install 'apache-airflow[microsoft.winrm]'``   | WinRM hooks and operators                                            |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+

Provider packages
-----------------

Provider packages context
'''''''''''''''''''''''''

Unlike Apache Airflow 1.10, the Airflow 2.0 is delivered in multiple, separate, but connected packages.
The core of Airflow scheduling system is delivered as ``apache-airflow`` package and there are around
60 providers packages which can be installed separately as so called "Airflow Provider packages".
Those provider packages are separated per-provider (for example ``amazon``, ``google``, ``salesforce``
etc.)  Those packages are available as ``apache-airflow-providers`` packages - separately per each provider
(for example there is an ``apache-airflow-providers-amazon`` or ``apache-airflow-providers-google`` package.

You can install those provider packages separately in order to interface with a given provider. For those
providers that have corresponding extras, the provider packages (latest version from PyPI) are installed
automatically when Airflow is installed with the extra.

Providers are released and versioned separately from the Airflow releases. We are following the
`Semver <https://semver.org/>`_ versioning scheme for the packages. Some versions of the provider
packages might depend on particular versions of Airflow, but the general approach we have is that unless
there is a good reason, new version of providers should work with recent versions of Airflow 2.x. Details
will vary per-provider and if there is a limitation for particular version of particular provider,
constraining the Airflow version used, it will be included as limitation of dependencies in the provider
package.

Some of the providers have cross-provider dependencies as well. Those are not required dependencies, they
might simply enable certain features (for example transfer operators often create dependency between
different providers. Again, the general approach here is that the providers are backwards compatible,
including cross-dependencies. Any kind of breaking changes and requirements on particular versions of other
provider packages are automatically documented in the release notes of every provider.

.. note::
    We also provide ``apache-airflow-backport-providers`` packages that can be installed for Airflow 1.10.
    Those are the same providers as for 2.0 but automatically back-ported to work for Airflow 1.10. Those
    backport providers are going to be updated and released for 3 months after Apache Airflow 2.0 release.

Provider packages functionality
'''''''''''''''''''''''''''''''

Separate provider packages provide the possibilities that were not available in 1.10:

1. You can upgrade to latest version of particular providers without the need of Apache Airflow core upgrade.

2. You can downgrade to previous version of particular provider in case the new version introduces
   some problems, without impacting the main Apache Airflow core package.

3. You can release and upgrade/downgrade provider packages incrementally, independent from each other. This
   means that you can incrementally validate each of the provider package update in your environment,
   following the usual tests you have in your environment.


Q&A for Airflow and Providers
-----------------------------

Upgrading Airflow 2.0 and Providers
'''''''''''''''''''''''''''''''''''

Q. **When upgrading to a new Airflow version such as 2.0, but possibly 2.0.1 and beyond, is the best practice
   to also upgrade provider packages at the same time?**

A. It depends on your use case. If you have automated or semi-automated verification of your installation,
   that you can run a new version of Airflow including all provider packages, then definitely go for it.
   If you rely more on manual testing, it is advised that you upgrade in stages. Depending on your choice
   you can either upgrade all used provider packages first, and then upgrade Airflow Core or the other way
   round. The first approach - when you first upgrade all providers is probably safer, as you can do it
   incrementally, step-by-step replacing provider by provider in your environment.

Using Backport Providers in Airflow 1.10
''''''''''''''''''''''''''''''''''''''''

Q. **I have an Airflow version (1.10.12) running and it is stable. However, because of a Cloud provider change,
   I would like to upgrade the provider package. If I don't need to upgrade the Airflow version anymore,
   how do I know that this provider version is compatible with my Airflow version?**


A. Backport Provider Packages (those are needed in 1.10.* Airflow series) are going to be released for
   3 months after the release. We will stop releasing new updates to the backport providers afterwards.
   You will be able to continue using the provider packages that you already use and unless you need to
   get some new release of the provider that is only released for 2.0, there is no need to upgrade
   Airflow. This might happen if for example the provider is migrated to use newer version of client
   libraries or when new features/operators/hooks are added to it. Those changes will only be
   backported to 1.10.* compatible backport providers up to 3 months after releasing Airflow 2.0.
   Also we expect more providers, changes and fixes added to the existing providers to come after the
   3 months pass. Eventually you will have to upgrade to Airflow 2.0 if you would like to make use of those.
   When it comes to compatibility of providers with different Airflow 2 versions, each
   provider package will keep its own dependencies, and while we expect those providers to be generally
   backwards-compatible, particular versions of particular providers might introduce dependencies on
   specific Airflow versions.

Customizing Provider Packages
'''''''''''''''''''''''''''''

Q. **I have an older version of my provider package which we have lightly customized and is working
   fine with my MSSQL installation. I am upgrading my Airflow version. Do I need to upgrade my provider,
   or can I keep it as it is.**

A. It depends on the scope of customization. There is no need to upgrade the provider packages to later
   versions unless you want to upgrade to Airflow version that introduces backwards-incompatible changes.
   Generally speaking, with Airflow 2 we are following the `Semver <https://semver.org/>`_  approach where
   we will introduce backwards-incompatible changes in Major releases, so all your modifications (as long
   as you have not used internal Airflow classes) should work for All Airflow 2.* versions.


Initializing Airflow Database
-----------------------------

Airflow requires a database to be initialized before you can run tasks. If
you're just experimenting and learning Airflow, you can stick with the
default SQLite option. If you don't want to use SQLite, then take a look at
:doc:`howto/initialize-database` to setup a different database.

After configuration, you'll need to initialize the database before you can
run tasks:

.. code-block:: bash

    airflow db init
