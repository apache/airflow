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

Airflow distribution packages
-----------------------------

With Airflow 3, Airflow is now split into several independent and isolated distribution packages on top of
already existing ``providers`` and the dependencies are isolated and simplified across those distribution
packages.

While the original installation methods via ``apache-airflow`` distribution package and extras still
work as previously and it installs complete Airflow installation ready to serve as scheduler, webserver, triggerer
and worker, the ``apache-airflow`` package is now a meta-package that installs all the other distribution
packages, it's also possible to install only the distribution packages that are needed for a specific
component you want to run Airflow with.

The following distribution packages are available:

+----------------------------+------------------------------------------------------------------+----------------------------------------------------------+
| Distribution package       | Purpose                                                          |                      Optional extras                     |
+----------------------------+------------------------------------------------------------------+----------------------------------------------------------+
| apache-airflow-core        | This is the core distribution package that contains              | * Core extras that add optional functionality to Airflow |
|                            | the Airflow scheduler, webserver, triggerer code.                |   core system - enhancing its functionality across       |
|                            |                                                                  |   multiple providers.                                    |
|                            |                                                                  |                                                          |
|                            |                                                                  | * Group ``all`` extra that installs all optional         |
|                            |                                                                  |   functionalities together.                              |
+----------------------------+------------------------------------------------------------------+----------------------------------------------------------+
| apache-airflow-task-sdk    | This is the distribution package that is needed                  | * No optional extras                                     |
|                            | to run tasks in the worker                                       |                                                          |
+----------------------------+------------------------------------------------------------------+----------------------------------------------------------+
| apache-airflow-providers-* | Those are distribution packages that contain                     | * Each provider distribution packages might have its     |
|                            | integrations of Airflow with external systems,                   |   own optional extras                                    |
|                            | 3rd-party software and services. Usually they provide            |                                                          |
|                            | operators, hooks, sensors, triggers, but also                    |                                                          |
|                            | different types of extensions such as logging                    |                                                          |
|                            | handlers, executors, and other functionalities                   |                                                          |
|                            | that are tied to particular service or system.                   |                                                          |
+----------------------------+------------------------------------------------------------------+----------------------------------------------------------+
| apache-airflow             | This is the meta-distribution-package that installs (mandatory): | * Any of the core extras                                 |
|                            |                                                                  | * Any of the provider packages via extras                |
|                            | * ``apache-airflow-core`` (always the same version  as the       |                                                          |
|                            |   ``apache-airflow``)                                            | This is backwards-compatible with previous installation  |
|                            | * ``apache-airflow-task-sdk`` (latest)                           | methods in Airflow 2.                                    |
|                            |                                                                  |                                                          |
|                            |                                                                  | Group extras:                                            |
|                            |                                                                  |                                                          |
|                            |                                                                  | * ``all-core`` - extra that installs all extras of the   |
|                            |                                                                  |   ``apache-airflow-core`` package                        |
|                            |                                                                  |                                                          |
|                            |                                                                  | * ``all`` - extra that installs all core extras and      |
|                            |                                                                  |   all provider packages (without their optional extras). |
+----------------------------+------------------------------------------------------------------+----------------------------------------------------------+

As mentioned above, Airflow has a number of optional "extras" that you can use to add features to your
installation when you are installing Airflow. Those extras are a good way for the users to manage their
installation, but also they are useful for contributors to Airflow when they want to contribute some of
the features - including optional integrations of Airflow - via providers.

Here's the list of all the extra dependencies of Apache Airflow.

Core Airflow extras
-------------------

These are core Airflow extras that extend capabilities of core Airflow. They do not install provider
packages, they just install necessary
python dependencies for the provided package. The same extras are available as ``airflow-core`` package extras.

+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| extra               | install command                                     | enables                                                                    |
+=====================+=====================================================+============================================================================+
| async               | ``pip install 'apache-airflow[async]'``             | Async worker classes for Gunicorn                                          |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| graphviz            | ``pip install 'apache-airflow[graphviz]'``          | Graphviz renderer for converting Dag to graphical output                   |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| kerberos            | ``pip install 'apache-airflow[kerberos]'``          | Kerberos integration for Kerberized services (Hadoop, Presto, Trino)       |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| memray              | ``pip install 'apache-airflow[memray]'``            | Required for memory profiling with memray                                  |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| otel                | ``pip install 'apache-airflow[otel]'``              | Required for OpenTelemetry metrics                                         |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| sentry              | ``pip install 'apache-airflow[sentry]'``            | Sentry service for application logging and monitoring                      |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| standard            | ``pip install apache-airflow[standard]'``           | Standard hooks and operators                                               |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| statsd              | ``pip install 'apache-airflow[statsd]'``            | Needed by StatsD metrics                                                   |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+

Meta-airflow package extras
---------------------------

Airflow 3 is released in several packages. The ``apache-airflow`` package is a meta-package that installs
all the other packages when you run Airflow as a standalone installation, and it also has several extras
that are not extending Airflow core functionality, but they are useful for the users who want to install
other packages that can be used by airflow or some of its providers.

+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| extra               | install command                                     | enables                                                                    |
+=====================+=====================================================+============================================================================+
| aiobotocore         | ``pip install 'apache-airflow[aiobotocore]'``       | Support for asynchronous (deferrable) operators for Amazon integration     |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| amazon-aws-auth     | ``pip install apache-airflow[amazon-aws-auth]``     | Amazon-aws-auth AWS authentication                                         |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| cloudpickle         | ``pip install apache-airflow[cloudpickle]``         | Cloudpickle hooks and operators                                            |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| github-enterprise   | ``pip install 'apache-airflow[github-enterprise]'`` | GitHub Enterprise auth backend                                             |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| google-auth         | ``pip install 'apache-airflow[google-auth]'``       | Google auth backend                                                        |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| graphviz            | ``pip install 'apache-airflow[graphviz]'``          | Graphviz renderer for converting Dag to graphical output                   |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| ldap                | ``pip install 'apache-airflow[ldap]'``              | LDAP authentication for users                                              |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| leveldb             | ``pip install 'apache-airflow[leveldb]'``           | Required for use leveldb extra in google provider                          |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| pandas              | ``pip install 'apache-airflow[pandas]'``            | Install Pandas library compatible with Airflow                             |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| polars              | ``pip install 'apache-airflow[polars]'``            | Polars hooks and operators                                                 |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| rabbitmq            | ``pip install 'apache-airflow[rabbitmq]'``          | RabbitMQ support as a Celery backend                                       |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| s3fs                | ``pip install 'apache-airflow[s3fs]'``              | Support for S3 as Airflow FS                                               |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| saml                | ``pip install 'apache-airflow[saml]'``              | Support for SAML authentication in Amazon provider                         |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| uv                  | ``pip install 'apache-airflow[uv]'``                | Install uv - fast, Rust-based package installer (experimental)             |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+


Providers extras
----------------

These providers extras are simply convenience extras to install providers so that you can install the providers with simple command - including
provider package and necessary dependencies in single command, which allows PIP to resolve any conflicting dependencies. This is extremely useful
for first time installation where you want to repeatably install version of dependencies which are 'valid' for both Airflow and providers installed.

For example the below command will install:

  * apache-airflow
  * apache-airflow-core
  * apache-airflow-task-sdk
  * apache-airflow-providers-amazon
  * apache-airflow-providers-google
  * apache-airflow-providers-apache-spark

with a consistent set of dependencies based on constraint files provided by Airflow Community at the time |version| version was released.

.. code-block:: bash
    :substitutions:

    pip install apache-airflow[google,amazon,apache-spark]==|version| \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-|version|/constraints-3.10.txt"

Note, that this will install providers in the versions that were released at the time of Airflow |version| release. You can later
upgrade those providers manually if you want to use latest versions of the providers.

Also, those extras are ONLY available in the ``apache-airflow`` distribution package as they are a convenient way to install
all the ``airflow`` packages together - similarly to what happened in Airflow 2. When you are installing ``airflow-core`` or
``airflow-task-sdk`` separately, if you want to install providers, you need to install them separately as
``apache-airflow-providers-*`` distribution packages.

Apache Software extras
======================

These are extras that add dependencies needed for integration with other Apache projects (note that ``apache.atlas`` and
``apache.webhdfs`` do not have their own providers - they only install additional libraries that can be used in
custom bash/python providers).

+---------------------+-----------------------------------------------------+------------------------------------------------+
| extra               | install command                                     | enables                                        |
+=====================+=====================================================+================================================+
| apache-atlas        | ``pip install 'apache-airflow[apache-atlas]'``      | Apache Atlas                                   |
+---------------------+-----------------------------------------------------+------------------------------------------------+
| apache-beam         | ``pip install 'apache-airflow[apache-beam]'``       | Apache Beam operators & hooks                  |
+---------------------+-----------------------------------------------------+------------------------------------------------+
| apache-cassandra    | ``pip install 'apache-airflow[apache-cassandra]'``  | Cassandra related operators & hooks            |
+---------------------+-----------------------------------------------------+------------------------------------------------+
| apache-drill        | ``pip install 'apache-airflow[apache-drill]'``      | Drill related operators & hooks                |
+---------------------+-----------------------------------------------------+------------------------------------------------+
| apache-druid        | ``pip install 'apache-airflow[apache-druid]'``      | Druid related operators & hooks                |
+---------------------+-----------------------------------------------------+------------------------------------------------+
| apache-flink        | ``pip install 'apache-airflow[apache-flink]'``      | Flink related operators & hooks                |
+---------------------+-----------------------------------------------------+------------------------------------------------+
| apache-hdfs         | ``pip install 'apache-airflow[apache-hdfs]'``       | HDFS hooks and operators                       |
+---------------------+-----------------------------------------------------+------------------------------------------------+
| apache-hive         | ``pip install 'apache-airflow[apache-hive]'``       | All Hive related operators                     |
+---------------------+-----------------------------------------------------+------------------------------------------------+
| apache-iceberg      | ``pip install 'apache-airflow[apache-iceberg]'``    | Apache Iceberg hooks                           |
+---------------------+-----------------------------------------------------+------------------------------------------------+
| apache-impala       | ``pip install 'apache-airflow[apache-impala]'``     | All Impala related operators & hooks           |
+---------------------+-----------------------------------------------------+------------------------------------------------+
| apache-kafka        | ``pip install 'apache-airflow[apache-kafka]'``      | All Kafka related operators & hooks            |
+---------------------+-----------------------------------------------------+------------------------------------------------+
| apache-kylin        | ``pip install 'apache-airflow[apache-kylin]'``      | All Kylin related operators & hooks            |
+---------------------+-----------------------------------------------------+------------------------------------------------+
| apache-livy         | ``pip install 'apache-airflow[apache-livy]'``       | All Livy related operators, hooks & sensors    |
+---------------------+-----------------------------------------------------+------------------------------------------------+
| apache-pig          | ``pip install 'apache-airflow[apache-pig]'``        | All Pig related operators & hooks              |
+---------------------+-----------------------------------------------------+------------------------------------------------+
| apache-pinot        | ``pip install 'apache-airflow[apache-pinot]'``      | All Pinot related hooks                        |
+---------------------+-----------------------------------------------------+------------------------------------------------+
| apache-spark        | ``pip install 'apache-airflow[apache-spark]'``      | All Spark related operators & hooks            |
+---------------------+-----------------------------------------------------+------------------------------------------------+
| apache-tinkerpop    | ``pip install apache-airflow[apache-tinkerpop]``    | Apache-tinkerpop hooks and operators           |
+---------------------+-----------------------------------------------------+------------------------------------------------+
| apache-webhdfs      | ``pip install 'apache-airflow[apache-webhdfs]'``    | HDFS hooks and operators                       |
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
| apprise             | ``pip install 'apache-airflow[apprise]'``           | Apprise Notification                                |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| amazon              | ``pip install 'apache-airflow[amazon]'``            | Amazon Web Services                                 |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| asana               | ``pip install 'apache-airflow[asana]'``             | Asana hooks and operators                           |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| atlassian-jira      | ``pip install 'apache-airflow[atlassian-jira]'``    | Jira hooks and operators                            |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| microsoft-azure     | ``pip install 'apache-airflow[microsoft-azure]'``   | Microsoft Azure                                     |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| cloudant            | ``pip install 'apache-airflow[cloudant]'``          | Cloudant hook                                       |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| cohere              | ``pip install 'apache-airflow[cohere]'``            | Cohere hook and operators                           |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| databricks          | ``pip install 'apache-airflow[databricks]'``        | Databricks hooks and operators                      |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| datadog             | ``pip install 'apache-airflow[datadog]'``           | Datadog hooks and sensors                           |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| dbt-cloud           | ``pip install 'apache-airflow[dbt-cloud]'``         | dbt Cloud hooks and operators                       |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| dingding            | ``pip install 'apache-airflow[dingding]'``          | Dingding hooks and sensors                          |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| discord             | ``pip install 'apache-airflow[discord]'``           | Discord hooks and sensors                           |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| facebook            | ``pip install 'apache-airflow[facebook]'``          | Facebook Social                                     |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| github              | ``pip install 'apache-airflow[github]'``            | GitHub operators and hook                           |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| google              | ``pip install 'apache-airflow[google]'``            | Google Cloud                                        |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| hashicorp           | ``pip install 'apache-airflow[hashicorp]'``         | Hashicorp Services (Vault)                          |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| openai              | ``pip install 'apache-airflow[openai]'``            | Open AI hooks and operators                         |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| opsgenie            | ``pip install 'apache-airflow[opsgenie]'``          | OpsGenie hooks and operators                        |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| pagerduty           | ``pip install 'apache-airflow[pagerduty]'``         | Pagerduty hook                                      |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| pgvector            | ``pip install 'apache-airflow[pgvector]'``          | pgvector operators and hook                         |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| pinecone            | ``pip install 'apache-airflow[pinecone]'``          | Pinecone Operators and Hooks                        |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| qdrant              | ``pip install 'apache-airflow[qdrant]'``            | Qdrant Operators and Hooks                          |
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
| weaviate            | ``pip install 'apache-airflow[weaviate]'``          | Weaviate hook and operators                         |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| yandex              | ``pip install 'apache-airflow[yandex]'``            | Yandex.cloud hooks and operators                    |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| ydb                 | ``pip install 'apache-airflow[ydb]'``               | YDB hooks and operators                             |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| zendesk             | ``pip install 'apache-airflow[zendesk]'``           | Zendesk hooks                                       |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+

Locally installed software extras
=================================

These are extras that add dependencies needed for integration with other software packages installed usually as part of the deployment of Airflow.
Some of those enable Airflow to use executors to run tasks with them - other than via the built-in LocalExecutor.

+---------------------+-----------------------------------------------------+-----------------------------------------------------------------+----------------------------------------------+
| extra               | install command                                     | brings                                                          | enables executors                            |
+=====================+=====================================================+=================================================================+==============================================+
| arangodb            | ``pip install 'apache-airflow[arangodb]'``          | ArangoDB operators, sensors and hook                            |                                              |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------+----------------------------------------------+
| celery              | ``pip install 'apache-airflow[celery]'``            | Celery dependencies and sensor                                  | CeleryExecutor, CeleryKubernetesExecutor     |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------+----------------------------------------------+
| cncf-kubernetes     | ``pip install 'apache-airflow[cncf-kubernetes]'``   | Kubernetes client libraries, KubernetesPodOperator & friends    | KubernetesExecutor, LocalKubernetesExecutor  |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------+----------------------------------------------+
| docker              | ``pip install 'apache-airflow[docker]'``            | Docker hooks and operators                                      |                                              |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------+----------------------------------------------+
| edge3               | ``pip install 'apache-airflow[edge3]'``             | Connect Edge Workers via HTTP to the scheduler                  | EdgeExecutor                                 |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------+----------------------------------------------+
| elasticsearch       | ``pip install 'apache-airflow[elasticsearch]'``     | Elasticsearch hooks and Log Handler                             |                                              |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------+----------------------------------------------+
| exasol              | ``pip install 'apache-airflow[exasol]'``            | Exasol hooks and operators                                      |                                              |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------+----------------------------------------------+
| fab                 | ``pip install 'apache-airflow[fab]'``               | FAB auth manager                                                |                                              |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------+----------------------------------------------+
| git                 | ``pip install 'apache-airflow[git]'``               | Git bundle and hook                                             |                                              |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------+----------------------------------------------+
| github              | ``pip install 'apache-airflow[github]'``            | GitHub operators and hook                                       |                                              |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------+----------------------------------------------+
| influxdb            | ``pip install 'apache-airflow[influxdb]'``          | Influxdb operators and hook                                     |                                              |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------+----------------------------------------------+
| jenkins             | ``pip install 'apache-airflow[jenkins]'``           | Jenkins hooks and operators                                     |                                              |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------+----------------------------------------------+
| mongo               | ``pip install 'apache-airflow[mongo]'``             | Mongo hooks and operators                                       |                                              |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------+----------------------------------------------+
| microsoft-mssql     | ``pip install 'apache-airflow[microsoft-mssql]'``   | Microsoft SQL Server operators and hook.                        |                                              |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------+----------------------------------------------+
| mysql               | ``pip install 'apache-airflow[mysql]'``             | MySQL operators and hook                                        |                                              |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------+----------------------------------------------+
| neo4j               | ``pip install 'apache-airflow[neo4j]'``             | Neo4j operators and hook                                        |                                              |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------+----------------------------------------------+
| odbc                | ``pip install 'apache-airflow[odbc]'``              | ODBC data sources including MS SQL Server                       |                                              |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------+----------------------------------------------+
| openfaas            | ``pip install 'apache-airflow[openfaas]'``          | OpenFaaS hooks                                                  |                                              |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------+----------------------------------------------+
| oracle              | ``pip install 'apache-airflow[oracle]'``            | Oracle hooks and operators                                      |                                              |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------+----------------------------------------------+
| postgres            | ``pip install 'apache-airflow[postgres]'``          | PostgreSQL operators and hook                                   |                                              |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------+----------------------------------------------+
| presto              | ``pip install 'apache-airflow[presto]'``            | All Presto related operators & hooks                            |                                              |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------+----------------------------------------------+
| redis               | ``pip install 'apache-airflow[redis]'``             | Redis hooks and sensors                                         |                                              |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------+----------------------------------------------+
| samba               | ``pip install 'apache-airflow[samba]'``             | Samba hooks and operators                                       |                                              |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------+----------------------------------------------+
| singularity         | ``pip install 'apache-airflow[singularity]'``       | Singularity container operator                                  |                                              |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------+----------------------------------------------+
| teradata            | ``pip install 'apache-airflow[teradata]'``          | Teradata hooks and operators                                    |                                              |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------+----------------------------------------------+
| trino               | ``pip install 'apache-airflow[trino]'``             | All Trino related operators & hooks                             |                                              |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------+----------------------------------------------+


Other extras
============

These are extras that provide support for integration with external systems via some - usually - standard protocols.

The entries with ``*`` in the ``Preinstalled`` column indicate that those extras (providers) are always
pre-installed when Airflow is installed.


+---------------------+-----------------------------------------------------+--------------------------------------+--------------+
| extra               | install command                                     | enables                              | Preinstalled |
+=====================+=====================================================+======================================+==============+
| common-compat       | ``pip install 'apache-airflow[common-compat]'``     | Compatibility code for old Airflow   |              |
+---------------------+-----------------------------------------------------+--------------------------------------+--------------+
| common-io           | ``pip install 'apache-airflow[common-io]'``         | Core IO Operators                    |              |
+---------------------+-----------------------------------------------------+--------------------------------------+--------------+
| common-messaging    | ``pip install 'apache-airflow[common-messaging]'``  | Core Messaging Operators             |              |
+---------------------+-----------------------------------------------------+--------------------------------------+--------------+
| common-sql          | ``pip install 'apache-airflow[common-sql]'``        | Core SQL Operators                   |      *       |
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
| keycloak            | ``pip install apache-airflow[keycloak]``            | Keycloak hooks and operators         |              +
+---------------------+-----------------------------------------------------+--------------------------------------+--------------+
| microsoft-psrp      | ``pip install 'apache-airflow[microsoft-psrp]'``    | PSRP hooks and operators             |              |
+---------------------+-----------------------------------------------------+--------------------------------------+--------------+
| microsoft-winrm     | ``pip install 'apache-airflow[microsoft-winrm]'``   | WinRM hooks and operators            |              |
+---------------------+-----------------------------------------------------+--------------------------------------+--------------+
| openlineage         | ``pip install 'apache-airflow[openlineage]'``       | Sending OpenLineage events           |              |
+---------------------+-----------------------------------------------------+--------------------------------------+--------------+
| opensearch          | ``pip install 'apache-airflow[opensearch]'``        | Opensearch hooks and operators       |              |
+---------------------+-----------------------------------------------------+--------------------------------------+--------------+
| papermill           | ``pip install 'apache-airflow[papermill]'``         | Papermill hooks and operators        |              |
+---------------------+-----------------------------------------------------+--------------------------------------+--------------+
| sftp                | ``pip install 'apache-airflow[sftp]'``              | SFTP hooks, operators and sensors    |              |
+---------------------+-----------------------------------------------------+--------------------------------------+--------------+
| smtp                | ``pip install 'apache-airflow[smtp]'``              | SMTP hooks and operators             |              |
+---------------------+-----------------------------------------------------+--------------------------------------+--------------+
| sqlite              | ``pip install 'apache-airflow[sqlite]'``            | SQLite hooks and operators           |      *       |
+---------------------+-----------------------------------------------------+--------------------------------------+--------------+
| ssh                 | ``pip install 'apache-airflow[ssh]'``               | SSH hooks and operators              |              |
+---------------------+-----------------------------------------------------+--------------------------------------+--------------+

Group extras
------------

The group extras are convenience extras. Such extra installs many optional dependencies together.
It is not recommended to use it in production, but it is useful for CI, development and testing purposes.

+--------------+----------------------------------------------+---------------------------------------------------+
| extra        | install command                              | enables                                           |
+==============+==============================================+===================================================+
| all          | ``pip install apache-airflow[all]``          | All optional dependencies including all providers |
+--------------+----------------------------------------------+---------------------------------------------------+
| all-core     | ``pip install apache-airflow[all-core]``     | All optional core dependencies                    |
+--------------+----------------------------------------------+---------------------------------------------------+
| all-task-sdk | ``pip install apache-airflow[all-task-sdk]`` | All optional task SDK dependencies                |
+--------------+----------------------------------------------+---------------------------------------------------+
