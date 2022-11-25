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

Reference for package extras
''''''''''''''''''''''''''''

Here's the list of all the extra dependencies of Apache Airflow.

The entries with ``*`` in the ``Preinstalled`` column indicate that those extras (providers) are always
pre-installed when Airflow is installed.

.. note::
  You can disable automated installation of the providers with extras when installing Airflow. You need to
  have ``INSTALL_PROVIDERS_FROM_SOURCES`` environment variable to ``true`` before running ``pip install``
  command. Contributors need to set it, if they are installing Airflow locally, and want to develop
  providers directly via Airflow sources. This variable is automatically set in ``Breeze``
  development environment. Setting this variable is not needed in editable mode (``pip install -e``).

Core Airflow extras
-------------------

These are core airflow extras that extend capabilities of core Airflow. They usually do not install provider
packages (with the exception of ``celery`` and ``cncf.kubernetes`` extras), they just install necessary
python dependencies for the provided package.

+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| extra               | install command                                     | enables                                                                    |
+=====================+=====================================================+============================================================================+
| async               | ``pip install 'apache-airflow[async]'``             | Async worker classes for Gunicorn                                          |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| celery              | ``pip install 'apache-airflow[celery]'``            | CeleryExecutor (also installs the celery provider package!)                |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| cgroups             | ``pip install 'apache-airflow[cgroups]'``           | Needed To use CgroupTaskRunner                                             |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| cncf.kubernetes     | ``pip install 'apache-airflow[cncf.kubernetes]'``   | Kubernetes Executor (also installs the Kubernetes provider package)        |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| dask                | ``pip install 'apache-airflow[dask]'``              | DaskExecutor                                                               |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| deprecated_api      | ``pip install 'apache-airflow[deprecated_api]'``    | Deprecated, experimental API that is replaced with the new REST API        |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| github_enterprise   | ``pip install 'apache-airflow[github_enterprise]'`` | GitHub Enterprise auth backend                                             |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| google_auth         | ``pip install 'apache-airflow[google_auth]'``       | Google auth backend                                                        |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| kerberos            | ``pip install 'apache-airflow[kerberos]'``          | Kerberos integration for Kerberized services (Hadoop, Presto, Trino)       |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| ldap                | ``pip install 'apache-airflow[ldap]'``              | LDAP authentication for users                                              |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| leveldb             | ``pip install 'apache-airflow[leveldb]'``           | Required for use leveldb extra in google provider                          |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| pandas              | ``pip install 'apache-airflow[pandas]'``            | Install Pandas library compatible with Airflow                             |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| password            | ``pip install 'apache-airflow[password]'``          | Password authentication for users                                          |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| rabbitmq            | ``pip install 'apache-airflow[rabbitmq]'``          | RabbitMQ support as a Celery backend                                       |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| sentry              | ``pip install 'apache-airflow[sentry]'``            | Sentry service for application logging and monitoring                      |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| statsd              | ``pip install 'apache-airflow[statsd]'``            | Needed by StatsD metrics                                                   |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| virtualenv          | ``pip install 'apache-airflow[virtualenv]'``        | Running python tasks in local virtualenv                                   |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+


Providers extras
----------------

These providers extras are simply convenience extras to install provider packages so that you can install the providers with simple command - including
provider package and necessary dependencies in single command, which allows PIP to resolve any conflicting dependencies. This is extremely useful
for first time installation where you want to repeatably install version of dependencies which are 'valid' for both airflow and providers installed.

For example the below command will install:

  * apache-airflow
  * apache-airflow-providers-amazon
  * apache-airflow-providers-google
  * apache-airflow-providers-apache-spark

with a consistent set of dependencies based on constraint files provided by Airflow Community at the time |version| version was released.

.. code-block:: bash
    :substitutions:

    pip install apache-airflow[google,amazon,apache.spark]==|version| \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-|version|/constraints-3.7.txt"

Note, that this will install providers in the versions that were released at the time of Airflow |version| release. You can later
upgrade those providers manually if you want to use latest versions of the providers.


Apache Software extras
======================

These are extras that add dependencies needed for integration with other Apache projects (note that ``apache.atlas`` and
``apache.webhdfs`` do not have their own providers - they only install additional libraries that can be used in
custom bash/python providers).

+---------------------+-----------------------------------------------------+------------------------------------------------+
| extra               | install command                                     | enables                                        |
+=====================+=====================================================+================================================+
| apache.atlas        | ``pip install 'apache-airflow[apache.atlas]'``      | Apache Atlas                                   |
+---------------------+-----------------------------------------------------+------------------------------------------------+
| apache.beam         | ``pip install 'apache-airflow[apache.beam]'``       | Apache Beam operators & hooks                  |
+---------------------+-----------------------------------------------------+------------------------------------------------+
| apache.cassandra    | ``pip install 'apache-airflow[apache.cassandra]'``  | Cassandra related operators & hooks            |
+---------------------+-----------------------------------------------------+------------------------------------------------+
| apache.drill        | ``pip install 'apache-airflow[apache.drill]'``      | Drill related operators & hooks                |
+---------------------+-----------------------------------------------------+------------------------------------------------+
| apache.druid        | ``pip install 'apache-airflow[apache.druid]'``      | Druid related operators & hooks                |
+---------------------+-----------------------------------------------------+------------------------------------------------+
| apache.hdfs         | ``pip install 'apache-airflow[apache.hdfs]'``       | HDFS hooks and operators                       |
+---------------------+-----------------------------------------------------+------------------------------------------------+
| apache.hive         | ``pip install 'apache-airflow[apache.hive]'``       | All Hive related operators                     |
+---------------------+-----------------------------------------------------+------------------------------------------------+
| apache.kylin        | ``pip install 'apache-airflow[apache.kylin]'``      | All Kylin related operators & hooks            |
+---------------------+-----------------------------------------------------+------------------------------------------------+
| apache.livy         | ``pip install 'apache-airflow[apache.livy]'``       | All Livy related operators, hooks & sensors    |
+---------------------+-----------------------------------------------------+------------------------------------------------+
| apache.pig          | ``pip install 'apache-airflow[apache.pig]'``        | All Pig related operators & hooks              |
+---------------------+-----------------------------------------------------+------------------------------------------------+
| apache.pinot        | ``pip install 'apache-airflow[apache.pinot]'``      | All Pinot related hooks                        |
+---------------------+-----------------------------------------------------+------------------------------------------------+
| apache.spark        | ``pip install 'apache-airflow[apache.spark]'``      | All Spark related operators & hooks            |
+---------------------+-----------------------------------------------------+------------------------------------------------+
| apache.sqoop        | ``pip install 'apache-airflow[apache.sqoop]'``      | All Sqoop related operators & hooks            |
+---------------------+-----------------------------------------------------+------------------------------------------------+
| apache.webhdfs      | ``pip install 'apache-airflow[apache.webhdfs]'``    | HDFS hooks and operators                       |
+---------------------+-----------------------------------------------------+------------------------------------------------+


External Services extras
========================

These are extras that add dependencies needed for integration with external services - either cloud based or on-premises.

+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| extra               | install command                                     | enables                                             |
+=====================+=====================================================+=====================================================+
| airbyte             | ``pip install 'apache-airflow[airbyte]'``           | Airbyte hooks and operators                         |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| alibaba             | ``pip install 'apache-airflow[alibaba]'``           | Alibaba Cloud                                       |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| amazon              | ``pip install 'apache-airflow[amazon]'``            | Amazon Web Services                                 |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| asana               | ``pip install 'apache-airflow[asana]'``             | Asana hooks and operators                           |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| atlassian.jira      | ``pip install 'apache-airflow[atlassian.jira]'``    | Jira hooks and operators                            |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| azure               | ``pip install 'apache-airflow[microsoft.azure]'``   | Microsoft Azure                                     |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| cloudant            | ``pip install 'apache-airflow[cloudant]'``          | Cloudant hook                                       |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| databricks          | ``pip install 'apache-airflow[databricks]'``        | Databricks hooks and operators                      |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| datadog             | ``pip install 'apache-airflow[datadog]'``           | Datadog hooks and sensors                           |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| dbt.cloud           | ``pip install 'apache-airflow[dbt.cloud]'``         | dbt Cloud hooks and operators                       |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| dingding            | ``pip install 'apache-airflow[dingding]'``          | Dingding hooks and sensors                          |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| discord             | ``pip install 'apache-airflow[discord]'``           | Discord hooks and sensors                           |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| facebook            | ``pip install 'apache-airflow[facebook]'``          | Facebook Social                                     |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| google              | ``pip install 'apache-airflow[google]'``            | Google Cloud                                        |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| hashicorp           | ``pip install 'apache-airflow[hashicorp]'``         | Hashicorp Services (Vault)                          |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| opsgenie            | ``pip install 'apache-airflow[opsgenie]'``          | OpsGenie hooks and operators                        |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| pagerduty           | ``pip install 'apache-airflow[pagerduty]'``         | Pagerduty hook                                      |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| plexus              | ``pip install 'apache-airflow[plexus]'``            | Plexus service of CoreScientific.com AI platform    |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| qubole              | ``pip install 'apache-airflow[qubole]'``            | Enable QDS (Qubole Data Service) support            |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| salesforce          | ``pip install 'apache-airflow[salesforce]'``        | Salesforce hook                                     |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| sendgrid            | ``pip install 'apache-airflow[sendgrid]'``          | Send email using sendgrid                           |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| segment             | ``pip install 'apache-airflow[segment]'``           | Segment hooks and sensors                           |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| slack               | ``pip install 'apache-airflow[slack]'``             | Slack hooks and operators                           |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| snowflake           | ``pip install 'apache-airflow[snowflake]'``         | Snowflake hooks and operators                       |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| tableau             | ``pip install 'apache-airflow[tableau]'``           | Tableau hooks and operators                         |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| tabular             | ``pip install 'apache-airflow[tabular]'``           | Tabular hooks                                       |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| telegram            | ``pip install 'apache-airflow[telegram]'``          | Telegram hooks and operators                        |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| vertica             | ``pip install 'apache-airflow[vertica]'``           | Vertica hook support as an Airflow backend          |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| yandex              | ``pip install 'apache-airflow[yandex]'``            | Yandex.cloud hooks and operators                    |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| zendesk             | ``pip install 'apache-airflow[zendesk]'``           | Zendesk hooks                                       |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+


Locally installed software extras
=================================

These are extras that add dependencies needed for integration with other software packages installed usually as part of the deployment of Airflow.

+---------------------+-----------------------------------------------------+-------------------------------------------+
| extra               | install command                                     | enables                                   |
+=====================+=====================================================+===========================================+
| arangodb            | ``pip install 'apache-airflow[arangodb]'``          | ArangoDB operators, sensors and hook      |
+---------------------+-----------------------------------------------------+-------------------------------------------+
| docker              | ``pip install 'apache-airflow[docker]'``            | Docker hooks and operators                |
+---------------------+-----------------------------------------------------+-------------------------------------------+
| elasticsearch       | ``pip install 'apache-airflow[elasticsearch]'``     | Elasticsearch hooks and Log Handler       |
+---------------------+-----------------------------------------------------+-------------------------------------------+
| exasol              | ``pip install 'apache-airflow[exasol]'``            | Exasol hooks and operators                |
+---------------------+-----------------------------------------------------+-------------------------------------------+
| github              | ``pip install 'apache-airflow[github]'``            | GitHub operators and hook                 |
+---------------------+-----------------------------------------------------+-------------------------------------------+
| influxdb            | ``pip install 'apache-airflow[influxdb]'``          | Influxdb operators and hook               |
+---------------------+-----------------------------------------------------+-------------------------------------------+
| jenkins             | ``pip install 'apache-airflow[jenkins]'``           | Jenkins hooks and operators               |
+---------------------+-----------------------------------------------------+-------------------------------------------+
| mongo               | ``pip install 'apache-airflow[mongo]'``             | Mongo hooks and operators                 |
+---------------------+-----------------------------------------------------+-------------------------------------------+
| microsoft.mssql     | ``pip install 'apache-airflow[microsoft.mssql]'``   | Microsoft SQL Server operators and hook.  |
+---------------------+-----------------------------------------------------+-------------------------------------------+
| mysql               | ``pip install 'apache-airflow[mysql]'``             | MySQL operators and hook                  |
+---------------------+-----------------------------------------------------+-------------------------------------------+
| neo4j               | ``pip install 'apache-airflow[neo4j]'``             | Neo4j operators and hook                  |
+---------------------+-----------------------------------------------------+-------------------------------------------+
| odbc                | ``pip install 'apache-airflow[odbc]'``              | ODBC data sources including MS SQL Server |
+---------------------+-----------------------------------------------------+-------------------------------------------+
| openfaas            | ``pip install 'apache-airflow[openfaas]'``          | OpenFaaS hooks                            |
+---------------------+-----------------------------------------------------+-------------------------------------------+
| oracle              | ``pip install 'apache-airflow[oracle]'``            | Oracle hooks and operators                |
+---------------------+-----------------------------------------------------+-------------------------------------------+
| postgres            | ``pip install 'apache-airflow[postgres]'``          | PostgreSQL operators and hook             |
+---------------------+-----------------------------------------------------+-------------------------------------------+
| presto              | ``pip install 'apache-airflow[presto]'``            | All Presto related operators & hooks      |
+---------------------+-----------------------------------------------------+-------------------------------------------+
| redis               | ``pip install 'apache-airflow[redis]'``             | Redis hooks and sensors                   |
+---------------------+-----------------------------------------------------+-------------------------------------------+
| samba               | ``pip install 'apache-airflow[samba]'``             | Samba hooks and operators                 |
+---------------------+-----------------------------------------------------+-------------------------------------------+
| singularity         | ``pip install 'apache-airflow[singularity]'``       | Singularity container operator            |
+---------------------+-----------------------------------------------------+-------------------------------------------+
| trino               | ``pip install 'apache-airflow[trino]'``             | All Trino related operators & hooks       |
+---------------------+-----------------------------------------------------+-------------------------------------------+


Other extras
============

These are extras that provide support for integration with external systems via some - usually - standard protocols.

+---------------------+-----------------------------------------------------+--------------------------------------+--------------+
| extra               | install command                                     | enables                              | Preinstalled |
+=====================+=====================================================+======================================+==============+
| common.sql          | ``pip install 'apache-airflow[common.sql]'``        | Core SQL Operators                   |      *       |
+---------------------+-----------------------------------------------------+--------------------------------------+--------------+
| ftp                 | ``pip install 'apache-airflow[ftp]'``               | FTP hooks and operators              |      *       |
+---------------------+-----------------------------------------------------+--------------------------------------+--------------+
| grpc                | ``pip install 'apache-airflow[grpc]'``              | Grpc hooks and operators             |              |
+---------------------+-----------------------------------------------------+--------------------------------------+--------------+
| http                | ``pip install 'apache-airflow[http]'``              | HTTP hooks, operators and sensors    |      *       |
+---------------------+-----------------------------------------------------+--------------------------------------+--------------+
| imap                | ``pip install 'apache-airflow[imap]'``              | IMAP hooks and sensors               |      *       |
+---------------------+-----------------------------------------------------+--------------------------------------+--------------+
| jdbc                | ``pip install 'apache-airflow[jdbc]'``              | JDBC hooks and operators             |              |
+---------------------+-----------------------------------------------------+--------------------------------------+--------------+
| microsoft.psrp      | ``pip install 'apache-airflow[microsoft.psrp]'``    | PSRP hooks and operators             |              |
+---------------------+-----------------------------------------------------+--------------------------------------+--------------+
| microsoft.winrm     | ``pip install 'apache-airflow[microsoft.winrm]'``   | WinRM hooks and operators            |              |
+---------------------+-----------------------------------------------------+--------------------------------------+--------------+
| papermill           | ``pip install 'apache-airflow[papermill]'``         | Papermill hooks and operators        |              |
+---------------------+-----------------------------------------------------+--------------------------------------+--------------+
| sftp                | ``pip install 'apache-airflow[sftp]'``              | SFTP hooks, operators and sensors    |              |
+---------------------+-----------------------------------------------------+--------------------------------------+--------------+
| sqlite              | ``pip install 'apache-airflow[sqlite]'``            | SQLite hooks and operators           |      *       |
+---------------------+-----------------------------------------------------+--------------------------------------+--------------+
| ssh                 | ``pip install 'apache-airflow[ssh]'``               | SSH hooks and operators              |              |
+---------------------+-----------------------------------------------------+--------------------------------------+--------------+

Bundle extras
-------------

These are extras that install one ore more extras as a bundle. Note that these extras should only be used for "development" version
of Airflow - i.e. when Airflow is installed from sources. Because of the way how bundle extras are constructed they might not
work when airflow is installed from 'PyPI`.

If you want to install Airflow from PyPI with "all" extras (which should basically be never needed - you almost never need all extras from Airflow),
you need to list explicitly all the non-bundle extras that you want to install.

+---------------------+-----------------------------------------------------+------------------------------------------------------------------------+
| extra               | install command                                     | enables                                                                |
+=====================+=====================================================+========================================================================+
| all                 | ``pip install 'apache-airflow[all]'``               | All Airflow user facing features (no devel and doc requirements)       |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------+
| all_dbs             | ``pip install 'apache-airflow[all_dbs]'``           | All database integrations                                              |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------+
| devel               | ``pip install 'apache-airflow[devel]'``             | Minimum development dependencies (without Hadoop, Kerberos, providers) |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------+
| devel_hadoop        | ``pip install 'apache-airflow[devel_hadoop]'``      | Adds Hadoop stack libraries to ``devel`` dependencies                  |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------+
| devel_all           | ``pip install 'apache-airflow[devel_all]'``         | Everything needed for development including Hadoop and providers       |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------+
| devel_ci            | ``pip install 'apache-airflow[devel_ci]'``          | All dependencies required for CI tests (same as ``devel_all``)         |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------+

Doc extras
----------

This is the extra that is needed to generated documentation for Airflow. This is used for development time only

+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| extra               | install command                                     | enables                                                              |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+
| doc                 | ``pip install 'apache-airflow[doc]'``               | Packages needed to build docs (included in ``devel``)                |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------+


Deprecated 1.10 extras
----------------------

These are the extras that have been deprecated in 2.0 and will be removed in Airflow 3.0.0. They were
all replaced by new extras, which have naming consistent with the names of provider packages.

The ``crypto`` extra is not needed any more, because all crypto dependencies are part of airflow package,
so there is no replacement for ``crypto`` extra.

+---------------------+-----------------------------+
| Deprecated extra    | Extra to be used instead    |
+=====================+=============================+
| atlas               | apache.atlas                |
+---------------------+-----------------------------+
| aws                 | amazon                      |
+---------------------+-----------------------------+
| azure               | microsoft.azure             |
+---------------------+-----------------------------+
| cassandra           | apache.cassandra            |
+---------------------+-----------------------------+
| crypto              |                             |
+---------------------+-----------------------------+
| druid               | apache.druid                |
+---------------------+-----------------------------+
| gcp                 | google                      |
+---------------------+-----------------------------+
| gcp_api             | google                      |
+---------------------+-----------------------------+
| hdfs                | apache.hdfs                 |
+---------------------+-----------------------------+
| hive                | apache.hive                 |
+---------------------+-----------------------------+
| kubernetes          | cncf.kubernetes             |
+---------------------+-----------------------------+
| mssql               | microsoft.mssql             |
+---------------------+-----------------------------+
| pinot               | apache.pinot                |
+---------------------+-----------------------------+
| qds                 | qubole                      |
+---------------------+-----------------------------+
| s3                  | amazon                      |
+---------------------+-----------------------------+
| spark               | apache.spark                |
+---------------------+-----------------------------+
| webhdfs             | apache.webhdfs              |
+---------------------+-----------------------------+
| winrm               | microsoft.winrm             |
+---------------------+-----------------------------+
