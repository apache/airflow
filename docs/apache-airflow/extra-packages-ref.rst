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

Airflow has a number of optional "extras" that you can use to add features to your installation when you
are installing Airflow. Those extras are a good way for the users to manage their installation, but also
they are useful for contributors to airflow when they want to contribute some of the featuers - including
optional integrations of Airflow - via providers.

,, warning::

    Traditionally in Airflow some of the extras used `.` and `_` to separate the parts of the extra name.
    This was not PEP-685 normalized name and we opted to change it to to `-` for all our extras, Expecting that
    PEP-685 will be implemented in full by `pip` and other tools. Currently the normalization is happening
    anyway, but `pip` shows warning when `_` or `-` are used, due to old packaging version used (January 2023).
    The work is in progress to change it in `this issue <https://github.com/pypa/pip/issues/11445>` so this
    is anticipated that it will be fixed soon.

    TODO(potiuk): decide whether to do it. In the current proposal we changed everything to `_`.


Here's the list of all the extra dependencies of Apache Airflow.

Core Airflow extras
-------------------

These are core airflow extras that extend capabilities of core Airflow. They usually do not install provider
packages (with the exception of ``celery`` and ``cncf.kubernetes`` extras), they just install necessary
python dependencies for the provided package.

+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| extra               | install command                                     | enables                                                                    |
+=====================+=====================================================+============================================================================+
| aiobotocore         | ``pip install 'apache-airflow[aiobotocore]'``       | Support for asynchronous (deferrable) operators for Amazon integration     |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| async               | ``pip install 'apache-airflow[async]'``             | Async worker classes for Gunicorn                                          |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| cgroups             | ``pip install 'apache-airflow[cgroups]'``           | Needed To use CgroupTaskRunner                                             |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| deprecated_api      | ``pip install 'apache-airflow[deprecated_api]'``    | Deprecated, experimental API that is replaced with the new REST API        |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| github_enterprise   | ``pip install 'apache-airflow[github_enterprise]'`` | GitHub Enterprise auth backend                                             |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| google_auth         | ``pip install 'apache-airflow[google_auth]'``       | Google auth backend                                                        |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| graphviz            | ``pip install 'apache-airflow[graphvis]'``          | Enables exporting DAGs to .dot graphical output                            |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| kerberos            | ``pip install 'apache-airflow[kerberos]'``          | Kerberos integration for Kerberized services (Hadoop, Presto, Trino)       |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| ldap                | ``pip install 'apache-airflow[ldap]'``              | LDAP authentication for users                                              |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| leveldb             | ``pip install 'apache-airflow[leveldb]'``           | Required for use leveldb extra in google provider                          |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| otel                | ``pip install 'apache-airflow[otel]'``              | Required for OpenTelemetry metrics                                         |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| pandas              | ``pip install 'apache-airflow[pandas]'``            | Install Pandas library compatible with Airflow                             |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| password            | ``pip install 'apache-airflow[password]'``          | Password authentication for users                                          |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| rabbitmq            | ``pip install 'apache-airflow[rabbitmq]'``          | RabbitMQ support as a Celery backend                                       |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| sentry              | ``pip install 'apache-airflow[sentry]'``            | Sentry service for application logging and monitoring                      |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| s3fs                | ``pip install 'apache-airflow[s3fs]'``              | Support for S3 as Airflow FS                                               |
+---------------------+-----------------------------------------------------+----------------------------------------------------------------------------+
| saml                | ``pip install 'apache-airflow[saml]'``              | Support for SAML authentication in Airflow                                 |
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

    pip install apache-airflow[google,amazon,apache-spark]==|version| \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-|version|/constraints-3.8.txt"

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
| apache_atlas        | ``pip install 'apache-airflow[apache_atlas]'``      | Apache Atlas                                   |
+---------------------+-----------------------------------------------------+------------------------------------------------+
| apache_beam         | ``pip install 'apache-airflow[apache_beam]'``       | Apache Beam operators & hooks                  |
+---------------------+-----------------------------------------------------+------------------------------------------------+
| apache_cassandra    | ``pip install 'apache-airflow[apache_cassandra]'``  | Cassandra related operators & hooks            |
+---------------------+-----------------------------------------------------+------------------------------------------------+
| apache_drill        | ``pip install 'apache-airflow[apache_drill]'``      | Drill related operators & hooks                |
+---------------------+-----------------------------------------------------+------------------------------------------------+
| apache_druid        | ``pip install 'apache-airflow[apache_druid]'``      | Druid related operators & hooks                |
+---------------------+-----------------------------------------------------+------------------------------------------------+
| apache_flink        | ``pip install 'apache-airflow[apache_flink]'``      | Flink related operators & hooks                |
+---------------------+-----------------------------------------------------+------------------------------------------------+
| apache_hdfs         | ``pip install 'apache-airflow[apache_hdfs]'``       | HDFS hooks and operators                       |
+---------------------+-----------------------------------------------------+------------------------------------------------+
| apache_hive         | ``pip install 'apache-airflow[apache_hive]'``       | All Hive related operators                     |
+---------------------+-----------------------------------------------------+------------------------------------------------+
| apache_impala       | ``pip install 'apache-airflow[apache_impala]'``     | All Impala related operators & hooks           |
+---------------------+-----------------------------------------------------+------------------------------------------------+
| apache_kafka        | ``pip install 'apache-airflow[apache_kafka]'``      | All Kafka related operators & hooks            |
+---------------------+-----------------------------------------------------+------------------------------------------------+
| apache_kylin        | ``pip install 'apache-airflow[apache_kylin]'``      | All Kylin related operators & hooks            |
+---------------------+-----------------------------------------------------+------------------------------------------------+
| apache_livy         | ``pip install 'apache-airflow[apache_livy]'``       | All Livy related operators, hooks & sensors    |
+---------------------+-----------------------------------------------------+------------------------------------------------+
| apache_pig          | ``pip install 'apache-airflow[apache_pig]'``        | All Pig related operators & hooks              |
+---------------------+-----------------------------------------------------+------------------------------------------------+
| apache_pinot        | ``pip install 'apache-airflow[apache_pinot]'``      | All Pinot related hooks                        |
+---------------------+-----------------------------------------------------+------------------------------------------------+
| apache_spark        | ``pip install 'apache-airflow[apache_spark]'``      | All Spark related operators & hooks            |
+---------------------+-----------------------------------------------------+------------------------------------------------+
| apache_webhdfs      | ``pip install 'apache-airflow[apache_webhdfs]'``    | HDFS hooks and operators                       |
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
| atlassian_jira      | ``pip install 'apache-airflow[atlassian_jira]'``    | Jira hooks and operators                            |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| microsoft_azure     | ``pip install 'apache-airflow[microsoft_azure]'``   | Microsoft Azure                                     |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| cloudant            | ``pip install 'apache-airflow[cloudant]'``          | Cloudant hook                                       |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| cohere              | ``pip install 'apache-airflow[cohere]'``            | Cohere hook and operators                           |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| databricks          | ``pip install 'apache-airflow[databricks]'``        | Databricks hooks and operators                      |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| datadog             | ``pip install 'apache-airflow[datadog]'``           | Datadog hooks and sensors                           |
+---------------------+-----------------------------------------------------+-----------------------------------------------------+
| dbt_cloud           | ``pip install 'apache-airflow[dbt_cloud]'``         | dbt Cloud hooks and operators                       |
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
| cncf_kubernetes     | ``pip install 'apache-airflow[cncf_kubernetes]'``   | Kubernetes client libraries, KubernetesPodOperator & friends    | KubernetesExecutor, LocalKubernetesExecutor  |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------+----------------------------------------------+
| docker              | ``pip install 'apache-airflow[docker]'``            | Docker hooks and operators                                      |                                              |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------+----------------------------------------------+
| elasticsearch       | ``pip install 'apache-airflow[elasticsearch]'``     | Elasticsearch hooks and Log Handler                             |                                              |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------+----------------------------------------------+
| exasol              | ``pip install 'apache-airflow[exasol]'``            | Exasol hooks and operators                                      |                                              |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------+----------------------------------------------+
| fab                 | ``pip install 'apache-airflow[fab]'``               | FAB auth manager                                                |                                              |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------+----------------------------------------------+
| github              | ``pip install 'apache-airflow[github]'``            | GitHub operators and hook                                       |                                              |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------+----------------------------------------------+
| influxdb            | ``pip install 'apache-airflow[influxdb]'``          | Influxdb operators and hook                                     |                                              |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------+----------------------------------------------+
| jenkins             | ``pip install 'apache-airflow[jenkins]'``           | Jenkins hooks and operators                                     |                                              |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------+----------------------------------------------+
| mongo               | ``pip install 'apache-airflow[mongo]'``             | Mongo hooks and operators                                       |                                              |
+---------------------+-----------------------------------------------------+-----------------------------------------------------------------+----------------------------------------------+
| microsoft_mssql     | ``pip install 'apache-airflow[microsoft_mssql]'``   | Microsoft SQL Server operators and hook.                        |                                              |
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
| common_io           | ``pip install 'apache-airflow[common_io]'``         | Core IO Operators                    |              |
+---------------------+-----------------------------------------------------+--------------------------------------+--------------+
| common_sql          | ``pip install 'apache-airflow[common_sql]'``        | Core SQL Operators                   |      *       |
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
| microsoft_psrp      | ``pip install 'apache-airflow[microsoft_psrp]'``    | PSRP hooks and operators             |              |
+---------------------+-----------------------------------------------------+--------------------------------------+--------------+
| microsoft_winrm     | ``pip install 'apache-airflow[microsoft_winrm]'``   | WinRM hooks and operators            |              |
+---------------------+-----------------------------------------------------+--------------------------------------+--------------+
| openlineage         | ``pip install 'apache-airflow[openlineage]'``       | Sending OpenLineage events           |              |
+---------------------+-----------------------------------------------------+--------------------------------------+--------------+
| opensearch         | ``pip install 'apache-airflow[opensearch]'``         | Opensearch hooks and operators       |              |
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

Production Bundle extras
-------------------------

These are extras that install one or more extras as a bundle.

+---------------------+-----------------------------------------------------+------------------------------------------------------------------------+
| extra               | install command                                     | enables                                                                |
+=====================+=====================================================+========================================================================+
| all                 | ``pip install 'apache-airflow[all]'``               | All Airflow user facing features (no devel and doc requirements)       |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------+
| all_dbs             | ``pip install 'apache-airflow[all_dbs]'``           | All database integrations                                              |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------+

Development extras
------------------

Generally none of the ``devel`` extras install providers - they expect the providers to be used from sources
and those extras only make sense in editable mode. Users of Airflow should not be using them, unless they
start contributing back and install airflow from sources.

Those extras are only available in Airflow when it is installed in editable mode from sources
(``pip install -e .``).

Devel extras
============

The devel extras do not install dependencies for features of Airflow, but add functionality that is needed to
develop Airflow, such as running tests, static checks. They do not install provider packages - even if they might be related
to some providers (like ``devel_amazon``) but they might be needed if you want to test code of thoe corresponding
provider.

Even if some ``devel`` extras relate to providers - they do not install provider packages - for example
``devel_amazon`` does not install amazon provider) but they might be needed if you want to test code of
the corresponding provider (for example running mypy checks or running tests).

+---------------------+-----------------------------------------+------------------------------------------------------+
| extra               | install command                         | enables                                              |
+=====================+=========================================+======================================================+
| devel_amazon        | pip install -e '.[devel_amazon]'        | Adds all test libraries needed to test amazon        |
+---------------------+-----------------------------------------+------------------------------------------------------+
| devel_azure         | pip install -e '.[devel_azure]'         | Adds all test libraries needed to test azure         |
+---------------------+-----------------------------------------+------------------------------------------------------+
| devel_breeze        | pip install -e '.[devel_breeze]'        | Adds all test libraries needed to test breeze        |
+---------------------+-----------------------------------------+------------------------------------------------------+
| devel_debuggers     | pip install -e '.[devel_debuggers]'     | Adds all test libraries needed to test debuggers     |
+---------------------+-----------------------------------------+------------------------------------------------------+
| devel_deltalake     | pip install -e '.[devel_deltalake]'     | Adds all test libraries needed to test deltalake     |
+---------------------+-----------------------------------------+------------------------------------------------------+
| devel_devscripts    | pip install -e '.[devel_devscripts]'    | Adds all test libraries needed to test devscripts    |
+---------------------+-----------------------------------------+------------------------------------------------------+
| devel_duckdb        | pip install -e '.[devel_duckdb]'        | Adds all test libraries needed to test duckdb        |
+---------------------+-----------------------------------------+------------------------------------------------------+
| devel_iceberg       | pip install -e '.[devel_iceberg]'       | Adds all test libraries needed to test iceberg       |
+---------------------+-----------------------------------------+------------------------------------------------------+
| devel_mongo         | pip install -e '.[devel_mongo]'         | Adds all test libraries needed to test mongo         |
+---------------------+-----------------------------------------+------------------------------------------------------+
| devel_mypy          | pip install -e '.[devel_mypy]'          | Adds all test libraries needed to test mypy          |
+---------------------+-----------------------------------------+------------------------------------------------------+
| devel_sentry        | pip install -e '.[devel_sentry]'        | Adds all test libraries needed to test sentry        |
+---------------------+-----------------------------------------+------------------------------------------------------+
| devel_static_checks | pip install -e '.[devel_static_checks]' | Adds all test libraries needed to test static_checks |
+---------------------+-----------------------------------------+------------------------------------------------------+
| devel_tests         | pip install -e '.[devel_tests]'         | Adds all test libraries needed to test tests         |
+---------------------+-----------------------------------------+------------------------------------------------------+

Editable provider extras
========================

In order to test providers when installing Airflow in editable, development mode, you need to install
dependencies of the providers. This is done by installing the ``editable`` extra with ``pip install -e``.
Those extras are not available in the released PyPI wheel packages, they are only available when Airflow
is installed locally in editable mode.

+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| extra                           | install command                                     | enables                                                    |
+=================================+=====================================================+============================================================+
| editable_airbyte                | pip install -e '.[editable_airbyte]'                | Adds all libraries needed by the airbyte provider          |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_alibaba                | pip install -e '.[editable_alibaba]'                | Adds all libraries needed by the alibaba provider          |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_amazon                 | pip install -e '.[editable_amazon]'                 | Adds all libraries needed by the amazon provider           |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_apache_beam            | pip install -e '.[editable_apache_beam]'            | Adds all libraries needed by the apache_beam provider      |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_apache_cassandra       | pip install -e '.[editable_apache_cassandra]'       | Adds all libraries needed by the apache_cassandra provider |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_apache_drill           | pip install -e '.[editable_apache_drill]'           | Adds all libraries needed by the apache_drill provider     |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_apache_druid           | pip install -e '.[editable_apache_druid]'           | Adds all libraries needed by the apache_druid provider     |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_apache_flink           | pip install -e '.[editable_apache_flink]'           | Adds all libraries needed by the apache_flink provider     |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_apache_hdfs            | pip install -e '.[editable_apache_hdfs]'            | Adds all libraries needed by the apache_hdfs provider      |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_apache_hive            | pip install -e '.[editable_apache_hive]'            | Adds all libraries needed by the apache_hive provider      |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_apache_impala          | pip install -e '.[editable_apache_impala]'          | Adds all libraries needed by the apache_impala provider    |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_apache_kafka           | pip install -e '.[editable_apache_kafka]'           | Adds all libraries needed by the apache_kafka provider     |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_apache_kylin           | pip install -e '.[editable_apache_kylin]'           | Adds all libraries needed by the apache_kylin provider     |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_apache_livy            | pip install -e '.[editable_apache_livy]'            | Adds all libraries needed by the apache_livy provider      |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_apache_pig             | pip install -e '.[editable_apache_pig]'             | Adds all libraries needed by the apache_pig provider       |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_apache_pinot           | pip install -e '.[editable_apache_pinot]'           | Adds all libraries needed by the apache_pinot provider     |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_apache_spark           | pip install -e '.[editable_apache_spark]'           | Adds all libraries needed by the apache_spark provider     |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_apprise                | pip install -e '.[editable_apprise]'                | Adds all libraries needed by the apprise provider          |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_arangodb               | pip install -e '.[editable_arangodb]'               | Adds all libraries needed by the arangodb provider         |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_asana                  | pip install -e '.[editable_asana]'                  | Adds all libraries needed by the asana provider            |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_atlassian_jira         | pip install -e '.[editable_atlassian_jira]'         | Adds all libraries needed by the atlassian_jira provider   |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_celery                 | pip install -e '.[editable_celery]'                 | Adds all libraries needed by the celery provider           |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_cloudant               | pip install -e '.[editable_cloudant]'               | Adds all libraries needed by the cloudant provider         |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_cncf_kubernetes        | pip install -e '.[editable_cncf_kubernetes]'        | Adds all libraries needed by the cncf_kubernetes provider  |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_cohere                 | pip install -e '.[editable_cohere]'                 | Adds all libraries needed by the cohere provider           |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_common_io              | pip install -e '.[editable_common_io]'              | Adds all libraries needed by the common_io provider        |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_common_sql             | pip install -e '.[editable_common_sql]'             | Adds all libraries needed by the common_sql provider       |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_databricks             | pip install -e '.[editable_databricks]'             | Adds all libraries needed by the databricks provider       |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_datadog                | pip install -e '.[editable_datadog]'                | Adds all libraries needed by the datadog provider          |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_dbt_cloud              | pip install -e '.[editable_dbt_cloud]'              | Adds all libraries needed by the dbt_cloud provider        |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_dingding               | pip install -e '.[editable_dingding]'               | Adds all libraries needed by the dingding provider         |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_discord                | pip install -e '.[editable_discord]'                | Adds all libraries needed by the discord provider          |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_docker                 | pip install -e '.[editable_docker]'                 | Adds all libraries needed by the docker provider           |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_elasticsearch          | pip install -e '.[editable_elasticsearch]'          | Adds all libraries needed by the elasticsearch provider    |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_exasol                 | pip install -e '.[editable_exasol]'                 | Adds all libraries needed by the exasol provider           |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_fab                    | pip install -e '.[editable_fab]'                    | Adds all libraries needed by the fab provider              |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_facebook               | pip install -e '.[editable_facebook]'               | Adds all libraries needed by the facebook provider         |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_ftp                    | pip install -e '.[editable_ftp]'                    | Adds all libraries needed by the ftp provider              |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_github                 | pip install -e '.[editable_github]'                 | Adds all libraries needed by the github provider           |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_google                 | pip install -e '.[editable_google]'                 | Adds all libraries needed by the google provider           |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_grpc                   | pip install -e '.[editable_grpc]'                   | Adds all libraries needed by the grpc provider             |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_hashicorp              | pip install -e '.[editable_hashicorp]'              | Adds all libraries needed by the hashicorp provider        |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_http                   | pip install -e '.[editable_http]'                   | Adds all libraries needed by the http provider             |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_imap                   | pip install -e '.[editable_imap]'                   | Adds all libraries needed by the imap provider             |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_influxdb               | pip install -e '.[editable_influxdb]'               | Adds all libraries needed by the influxdb provider         |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_jdbc                   | pip install -e '.[editable_jdbc]'                   | Adds all libraries needed by the jdbc provider             |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_jenkins                | pip install -e '.[editable_jenkins]'                | Adds all libraries needed by the jenkins provider          |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_microsoft_azure        | pip install -e '.[editable_microsoft_azure]'        | Adds all libraries needed by the microsoft_azure provider  |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_microsoft_mssql        | pip install -e '.[editable_microsoft_mssql]'        | Adds all libraries needed by the microsoft_mssql provider  |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_microsoft_psrp         | pip install -e '.[editable_microsoft_psrp]'         | Adds all libraries needed by the microsoft_psrp provider   |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_microsoft_winrm        | pip install -e '.[editable_microsoft_winrm]'        | Adds all libraries needed by the microsoft_winrm provider  |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_mongo                  | pip install -e '.[editable_mongo]'                  | Adds all libraries needed by the mongo provider            |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_mysql                  | pip install -e '.[editable_mysql]'                  | Adds all libraries needed by the mysql provider            |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_neo4j                  | pip install -e '.[editable_neo4j]'                  | Adds all libraries needed by the neo4j provider            |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_odbc                   | pip install -e '.[editable_odbc]'                   | Adds all libraries needed by the odbc provider             |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_openai                 | pip install -e '.[editable_openai]'                 | Adds all libraries needed by the openai provider           |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_openfaas               | pip install -e '.[editable_openfaas]'               | Adds all libraries needed by the openfaas provider         |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_openlineage            | pip install -e '.[editable_openlineage]'            | Adds all libraries needed by the openlineage provider      |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_opensearch             | pip install -e '.[editable_opensearch]'             | Adds all libraries needed by the opensearch provider       |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_opsgenie               | pip install -e '.[editable_opsgenie]'               | Adds all libraries needed by the opsgenie provider         |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_oracle                 | pip install -e '.[editable_oracle]'                 | Adds all libraries needed by the oracle provider           |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_pagerduty              | pip install -e '.[editable_pagerduty]'              | Adds all libraries needed by the pagerduty provider        |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_papermill              | pip install -e '.[editable_papermill]'              | Adds all libraries needed by the papermill provider        |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_pgvector               | pip install -e '.[editable_pgvector]'               | Adds all libraries needed by the pgvector provider         |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_pinecone               | pip install -e '.[editable_pinecone]'               | Adds all libraries needed by the pinecone provider         |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_postgres               | pip install -e '.[editable_postgres]'               | Adds all libraries needed by the postgres provider         |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_presto                 | pip install -e '.[editable_presto]'                 | Adds all libraries needed by the presto provider           |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_redis                  | pip install -e '.[editable_redis]'                  | Adds all libraries needed by the redis provider            |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_salesforce             | pip install -e '.[editable_salesforce]'             | Adds all libraries needed by the salesforce provider       |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_samba                  | pip install -e '.[editable_samba]'                  | Adds all libraries needed by the samba provider            |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_segment                | pip install -e '.[editable_segment]'                | Adds all libraries needed by the segment provider          |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_sendgrid               | pip install -e '.[editable_sendgrid]'               | Adds all libraries needed by the sendgrid provider         |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_sftp                   | pip install -e '.[editable_sftp]'                   | Adds all libraries needed by the sftp provider             |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_singularity            | pip install -e '.[editable_singularity]'            | Adds all libraries needed by the singularity provider      |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_slack                  | pip install -e '.[editable_slack]'                  | Adds all libraries needed by the slack provider            |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_smtp                   | pip install -e '.[editable_smtp]'                   | Adds all libraries needed by the smtp provider             |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_snowflake              | pip install -e '.[editable_snowflake]'              | Adds all libraries needed by the snowflake provider        |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_sqlite                 | pip install -e '.[editable_sqlite]'                 | Adds all libraries needed by the sqlite provider           |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_ssh                    | pip install -e '.[editable_ssh]'                    | Adds all libraries needed by the ssh provider              |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_tableau                | pip install -e '.[editable_tableau]'                | Adds all libraries needed by the tableau provider          |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_tabular                | pip install -e '.[editable_tabular]'                | Adds all libraries needed by the tabular provider          |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_telegram               | pip install -e '.[editable_telegram]'               | Adds all libraries needed by the telegram provider         |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_trino                  | pip install -e '.[editable_trino]'                  | Adds all libraries needed by the trino provider            |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_vertica                | pip install -e '.[editable_vertica]'                | Adds all libraries needed by the vertica provider          |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_weaviate               | pip install -e '.[editable_weaviate]'               | Adds all libraries needed by the weaviate provider         |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_yandex                 | pip install -e '.[editable_yandex]'                 | Adds all libraries needed by the yandex provider           |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+
| editable_zendesk                | pip install -e '.[editable_zendesk]'                | Adds all libraries needed by the zendesk provider          |
+---------------------------------+-----------------------------------------------------+------------------------------------------------------------+

Doc extras
==========

Those are the extras that are needed to generated documentation for Airflow. This is used for development time only

+---------------------+-----------------------------------------------------+------------------------------------------------------------------------+
| extra               | install command                                     | enables                                                                |
+=====================+=====================================================+========================================================================+
| doc                 | ``pip install -e '.[doc]'``                         | Packages needed to build docs (included in ``devel``)                  |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------+
| doc_gen             | ``pip install -e '.[doc_gen]'``                     | Packages needed to generate er diagrams (included in ``devel_all``)    |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------+

Bundle devel extras
===================

Those are extras that bundle devel, editable and doc extras together to make it easy to install them together in a single installation. Some of the
extras are more difficult to install on certain systems (such as ARM MacBooks) because they require system level dependencies to be installed.

+---------------------+-----------------------------------------------------+------------------------------------------------------------------------+
| extra               | install command                                     | enables                                                                |
+=====================+=====================================================+========================================================================+
| devel               | ``pip install -e '.[devel]'``                       | Minimum development dependencies (without Hadoop, Kerberos, providers) |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------+
| devel_hadoop        | ``pip install -e '.[devel_hadoop]'``                | Adds Hadoop stack libraries ``devel`` dependencies                     |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------+
| devel_all_dbs       | ``pip install -e '.[devel_all_dbs]'``               | Adds all libraries (editable extras) needed to test database providers |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------+
| devel_all           | ``pip install -e '.[devel_all]'``                   | Everything needed for development including Hadoop, all devel extras,  |
|                     |                                                     | all doc extras, all editable extras. Generally: all dependencies       |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------+
| devel_ci            | ``pip install -e '.[devel_ci]'``                    | All dependencies required for CI tests (same as ``devel_all``)         |
+---------------------+-----------------------------------------------------+------------------------------------------------------------------------+


Deprecated 1.10 extras
----------------------

These are the extras that have been deprecated in 2.0 and will be removed in Airflow 3.0.0. They were
all replaced by new extras, which have naming consistent with the names of provider packages.

The ``crypto`` extra is not needed any more, because all crypto dependencies are part of airflow package,
so there is no replacement for ``crypto`` extra.

+---------------------+-----------------------------+
| Deprecated extra    | Extra to be used instead    |
+=====================+=============================+
| atlas               | apache_atlas                |
+---------------------+-----------------------------+
| aws                 | amazon                      |
+---------------------+-----------------------------+
| azure               | microsoft_azure             |
+---------------------+-----------------------------+
| cassandra           | apache_cassandra            |
+---------------------+-----------------------------+
| crypto              |                             |
+---------------------+-----------------------------+
| druid               | apache_druid                |
+---------------------+-----------------------------+
| gcp                 | google                      |
+---------------------+-----------------------------+
| gcp_api             | google                      |
+---------------------+-----------------------------+
| hdfs                | apache_hdfs                 |
+---------------------+-----------------------------+
| hive                | apache_hive                 |
+---------------------+-----------------------------+
| kubernetes          | cncf_kubernetes             |
+---------------------+-----------------------------+
| mssql               | microsoft_mssql             |
+---------------------+-----------------------------+
| pinot               | apache_pinot                |
+---------------------+-----------------------------+
| s3                  | amazon                      |
+---------------------+-----------------------------+
| spark               | apache_spark                |
+---------------------+-----------------------------+
| webhdfs             | apache_webhdfs              |
+---------------------+-----------------------------+
| winrm               | microsoft_winrm             |
+---------------------+-----------------------------+
